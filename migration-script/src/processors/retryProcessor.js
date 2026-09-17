const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const auth0Service = require('../services/auth0Service');
const checkpointService = require('../services/checkpointService');
const failedUserService = require('../services/failedUserService');
const { statusQueue } = require('../queues');
const config = require('../config');
const logger = require('../logger');

async function retryProcessor(job) {
  const { user, failureReason, chunkId } = job.data;

  // Use email as the retry-count key; fall back to a stable hash if no email
  const userKey = user.email || JSON.stringify(user);
  const retryCount = await checkpointService.incrementUserRetryCount(userKey);

  logger.info('Retry attempt for user', {
    userKey,
    retryCount,
    maxRetries: config.migration.maxUserRetries,
    failureReason,
  });

  if (retryCount > config.migration.maxUserRetries) {
    logger.warn('User exceeded max retries — escalating to manual review', {
      userKey,
      retryCount,
    });
    await failedUserService.appendUsers([user], `Exceeded ${config.migration.maxUserRetries} retries. Last reason: ${failureReason}`);
    return { status: 'manual-review', userKey };
  }

  // Create a single-user chunk file and upload it as a fresh Auth0 import job
  const retryChunkId = uuidv4();
  const tmpPath = path.join(config.migration.chunksDir, `retry-${retryChunkId}.json`);
  fs.writeFileSync(tmpPath, JSON.stringify([user]), 'utf-8');

  let auth0Job;
  try {
    auth0Job = await auth0Service.createImportJob(tmpPath, {
      upsert: true,
      externalId: `retry-${retryChunkId}`,
    });
  } catch (err) {
    logger.error('Retry upload to Auth0 failed', { userKey, error: err.message });
    throw err;
  } finally {
    try { fs.unlinkSync(tmpPath); } catch (_) {}
  }

  logger.info('Retry job created in Auth0', { auth0JobId: auth0Job.id, userKey, retryChunkId });

  await checkpointService.storeAuth0JobId(retryChunkId, auth0Job.id);
  // Pass the user object so statusProcessor can escalate to manual review if the
  // retry job fails — the temp file is deleted above and can't be re-read later.
  await statusQueue.add(
    'poll-status',
    { chunkId: retryChunkId, auth0JobId: auth0Job.id, attempts: 0, isRetry: true, user },
    { delay: config.migration.statusPollIntervalMs }
  );

  return { status: 'retrying', auth0JobId: auth0Job.id, retryCount };
}

module.exports = retryProcessor;
