const fs = require('fs');
const path = require('path');
const auth0Service = require('../services/auth0Service');
const checkpointService = require('../services/checkpointService');
const failedUserService = require('../services/failedUserService');
const { statusQueue, retryQueue } = require('../queues');
const config = require('../config');
const logger = require('../logger');

async function statusProcessor(job) {
  const { chunkId, auth0JobId, attempts, isRetry, user: retryUser } = job.data;

  let jobStatus;
  try {
    jobStatus = await auth0Service.getJobStatus(auth0JobId);
  } catch (err) {
    logger.error('Failed to fetch Auth0 job status — will re-poll', { auth0JobId, error: err.message });
    await requeuePoll(job.data);
    return { status: 'poll-error-retry' };
  }

  logger.info('Auth0 job status received', {
    auth0JobId,
    chunkId,
    status: jobStatus.status,
    summary: jobStatus.summary,
  });

  if (jobStatus.status === 'pending' || jobStatus.status === 'processing') {
    if (attempts >= config.migration.statusPollMaxAttempts) {
      logger.error('Auth0 job timed out — escalating all users to retry', { auth0JobId, chunkId });
      await handleJobFailure(chunkId, isRetry, retryUser, 'Auth0 job timed out after 2 hours');
      await checkpointService.markChunkProcessed(chunkId);
      return { status: 'timeout' };
    }
    await requeuePoll(job.data);
    return { status: 'polling', attempts: attempts + 1 };
  }

  if (jobStatus.status === 'failed') {
    logger.error('Auth0 job failed entirely — escalating all users to retry', { auth0JobId, chunkId });
    await handleJobFailure(chunkId, isRetry, retryUser, 'Auth0 job failed');
    await checkpointService.markChunkProcessed(chunkId);
    return { status: 'failed' };
  }

  if (jobStatus.status === 'completed') {
    const summary = jobStatus.summary || {};
    logger.info('Auth0 job completed', { auth0JobId, chunkId, summary });

    if (summary.failed > 0) {
      let errors = [];
      try {
        errors = await auth0Service.getJobErrors(auth0JobId);
      } catch (err) {
        logger.error('Could not fetch job errors — retrying all users in chunk', { auth0JobId, error: err.message });
        await handleJobFailure(chunkId, isRetry, retryUser, 'Could not fetch individual errors after job completion');
        await checkpointService.markChunkProcessed(chunkId);
        return { status: 'error-fetch-failed' };
      }

      if (errors.length === 0) {
        // Auth0 reported failures but returned no error records — retry the whole chunk
        // to avoid silently losing users.
        logger.warn('Auth0 reported failed users but returned empty error list — retrying whole chunk', {
          auth0JobId,
          chunkId,
          failedCount: summary.failed,
        });
        await handleJobFailure(chunkId, isRetry, retryUser, 'Auth0 reported failures but returned no error details');
        await checkpointService.markChunkProcessed(chunkId);
        return { status: 'empty-errors-fallback' };
      }

      logger.warn(`${errors.length} users failed in job — queuing for retry`, { auth0JobId, chunkId });
      for (const error of errors) {
        if (error.user) {
          await retryQueue.add('retry-user', {
            user: error.user,
            failureReason: `${error.code}: ${error.message}`,
            chunkId,
          });
        }
      }
    }

    await checkpointService.markChunkProcessed(chunkId);
    return { status: 'completed', summary };
  }

  logger.warn('Unrecognised Auth0 job status — re-polling', { auth0JobId, status: jobStatus.status });
  await requeuePoll(job.data);
  return { status: 'unknown-repolling' };
}

async function requeuePoll(jobData) {
  const { chunkId, auth0JobId, attempts, isRetry, user } = jobData;
  await statusQueue.add(
    'poll-status',
    { chunkId, auth0JobId, attempts: attempts + 1, isRetry, user },
    { delay: config.migration.statusPollIntervalMs }
  );
}

// Handles a fully-failed Auth0 job. For normal chunks, re-reads the chunk file and
// queues each user to the retry queue. For retry jobs, the temp file has already been
// deleted — use the user object stored in the job data, or escalate to manual review.
async function handleJobFailure(chunkId, isRetry, retryUser, reason) {
  if (isRetry) {
    if (retryUser) {
      logger.warn('Retry job failed — escalating user to manual review', { chunkId, reason });
      await failedUserService.appendUsers([retryUser], `Retry job failed: ${reason}`);
    } else {
      logger.error('Retry job failed and no user data in job — cannot escalate', { chunkId, reason });
    }
    return;
  }
  await loadChunkAndRetry(chunkId, reason);
}

async function loadChunkAndRetry(chunkId, reason) {
  const chunksDir = config.migration.chunksDir;
  const files = fs.readdirSync(chunksDir);
  const chunkFile = files.find((f) => f.includes(chunkId));

  if (!chunkFile) {
    logger.error('Cannot locate chunk file to re-queue users', { chunkId, chunksDir });
    return;
  }

  const users = JSON.parse(fs.readFileSync(path.join(chunksDir, chunkFile), 'utf-8'));
  logger.info(`Queuing ${users.length} users from failed chunk for retry`, { chunkId, count: users.length });

  for (const user of users) {
    await retryQueue.add('retry-user', { user, failureReason: reason, chunkId });
  }
}

module.exports = statusProcessor;
