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
    // BUG FIX (Bug 5): Network/timeout errors must also respect the poll budget.
    // Without this check, 119 consecutive network errors exhaust the budget so
    // the very first successful fetch of a still-processing job immediately
    // triggers the timeout escalation path.
    if (attempts >= config.migration.statusPollMaxAttempts) {
      logger.error('Auth0 job timed out (fetch errors) — escalating all users to retry', { auth0JobId, chunkId });
      await handleJobFailure(chunkId, isRetry, retryUser, 'Auth0 job status fetch failed too many times');
      await checkpointService.releaseActiveAuth0Job(auth0JobId);
      await checkpointService.markChunkProcessed(chunkId);
      return { status: 'timeout-fetch-errors' };
    }
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
      await checkpointService.releaseActiveAuth0Job(auth0JobId);
      await checkpointService.markChunkProcessed(chunkId);
      return { status: 'timeout' };
    }
    await requeuePoll(job.data);
    return { status: 'polling', attempts: attempts + 1 };
  }

  if (jobStatus.status === 'failed') {
    logger.error('Auth0 job failed entirely — escalating all users to retry', { auth0JobId, chunkId });
    await handleJobFailure(chunkId, isRetry, retryUser, 'Auth0 job failed');
    await checkpointService.releaseActiveAuth0Job(auth0JobId);
    await checkpointService.markChunkProcessed(chunkId);
    return { status: 'failed' };
  }

  if (jobStatus.status === 'completed') {
    const summary = jobStatus.summary || {};
    logger.info('Auth0 job completed', { auth0JobId, chunkId, summary });

    let errors = [];

    if (summary.failed > 0) {
      try {
        errors = await auth0Service.getJobErrors(auth0JobId);
      } catch (err) {
        logger.error('Could not fetch job errors — retrying all users in chunk', { auth0JobId, error: err.message });
        await handleJobFailure(chunkId, isRetry, retryUser, 'Could not fetch individual errors after job completion');
        await checkpointService.releaseActiveAuth0Job(auth0JobId);
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
        await checkpointService.releaseActiveAuth0Job(auth0JobId);
        await checkpointService.markChunkProcessed(chunkId);
        return { status: 'empty-errors-fallback' };
      }

      logger.warn(`${errors.length} users failed in job — queuing for retry`, { auth0JobId, chunkId });
      for (let i = 0; i < errors.length; i++) {
        const error = errors[i];
        if (error.user) {
          const detail = (error.errors || [])[0] || {};
          const failureReason = detail.code
            ? `${detail.code}: ${detail.message}`
            : JSON.stringify(error.errors || error);
          // BUG FIX (Bug 3): Use an index-based fallback when email is missing.
          // Previously, all no-email users from the same chunk got the same jobId
          // (retry-${chunkId}-${chunkId}), causing BullMQ to silently drop all but
          // the first. The index suffix `noemail-${i}` ensures each is unique.
          const emailKey = (error.user.email || '').toLowerCase() || `noemail-${i}`;
          await retryQueue.add(
            'retry-user',
            { user: error.user, failureReason, chunkId },
            { jobId: `retry-${emailKey}-${chunkId}` }
          );
        }
      }
    }

    // Record which users succeeded in Auth0 so gap detection can verify
    // full coverage on restart. We only do this on the normal completion
    // path (not on early-return error fallbacks above) because in those
    // paths all users are being re-queued to retry and will be recorded
    // when they eventually succeed.
    await recordChunkSuccesses(chunkId, errors);

    // Release the slot BEFORE marking processed so a waiting importer can
    // pick it up as soon as this Auth0 job is confirmed done.
    await checkpointService.releaseActiveAuth0Job(auth0JobId);
    await checkpointService.markChunkProcessed(chunkId);
    return { status: 'completed', summary };
  }

  logger.warn('Unrecognised Auth0 job status — re-polling', { auth0JobId, status: jobStatus.status });
  await requeuePoll(job.data);
  return { status: 'unknown-repolling' };
}

// Derives the list of successfully imported users for a given chunk by
// subtracting known Auth0 failures from the full chunk user list, then
// records those emails in the per-user success SET in Redis.
async function recordChunkSuccesses(chunkId, failedErrors) {
  const chunksDir = config.migration.chunksDir;
  let chunkPath;
  try {
    const files = fs.readdirSync(chunksDir);
    const file = files.find(f => f.includes(chunkId));
    if (!file) {
      logger.warn('Cannot find chunk file for success recording — skipping', { chunkId });
      return;
    }
    chunkPath = path.join(chunksDir, file);
  } catch (err) {
    logger.warn('Cannot scan chunks dir for success recording', { chunkId, error: err.message });
    return;
  }

  let allUsers;
  try {
    allUsers = JSON.parse(fs.readFileSync(chunkPath, 'utf-8'));
  } catch (err) {
    logger.warn('Cannot read chunk file for success recording', { chunkId, error: err.message });
    return;
  }

  const failedEmails = new Set(
    (failedErrors || [])
      .map(e => (e.user?.email || '').toLowerCase())
      .filter(Boolean)
  );

  const successEmails = allUsers
    .map(u => (u.email || '').toLowerCase())
    .filter(e => e && !failedEmails.has(e));

  if (successEmails.length > 0) {
    await checkpointService.recordSuccessfulUsers(successEmails);
    logger.info('Recorded successful imports', { chunkId, count: successEmails.length });
  }
}

async function requeuePoll(jobData) {
  const { chunkId, auth0JobId, attempts, isRetry, user } = jobData;
  // BUG FIX (Bug 4): Deterministic jobId prevents a duplicate poll chain if this
  // job is stalled/re-run by BullMQ while the next poll is already in the delayed
  // queue. BullMQ silently ignores an add() with an ID already in waiting/delayed.
  await statusQueue.add(
    'poll-status',
    { chunkId, auth0JobId, attempts: attempts + 1, isRetry, user },
    { delay: config.migration.statusPollIntervalMs, jobId: `poll-${auth0JobId}` }
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

  for (let i = 0; i < users.length; i++) {
    const user = users[i];
    // BUG FIX (Bug 3): index-based fallback prevents multiple no-email users in
    // the same chunk from colliding on the same deterministic jobId.
    const emailKey = (user.email || '').toLowerCase() || `noemail-${i}`;
    await retryQueue.add(
      'retry-user',
      { user, failureReason: reason, chunkId },
      { jobId: `retry-${emailKey}-${chunkId}` }
    );
  }
}

module.exports = statusProcessor;
