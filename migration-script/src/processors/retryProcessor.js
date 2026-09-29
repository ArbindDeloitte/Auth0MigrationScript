'use strict';
const checkpointService = require('../services/checkpointService');
const failedUserService = require('../services/failedUserService');
const { flushRetryStagingBatch, RETRY_BATCH_SIZE } = require('../services/retryBatchService');
const config = require('../config');
const logger = require('../logger');

async function retryProcessor(job) {
  const { user, failureReason } = job.data;

  // Use email as the retry-count key; fall back to stringified user if no email.
  const userKey = user.email || JSON.stringify(user);

  // Increment per-user retry count — this is the source of truth for escalation.
  // Incremented here, before staging, so the count is always accurate even if the
  // process crashes between staging and batch submission.
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
    await failedUserService.appendUsers(
      [user],
      `Exceeded ${config.migration.maxUserRetries} retries. Last reason: ${failureReason}`
    );
    return { status: 'manual-review', userKey };
  }

  // Stage the user for batch processing instead of creating a single-user Auth0 job.
  // pushToRetryStaging returns the new list length after the push.
  const stagingCount = await checkpointService.pushToRetryStaging(user);

  logger.info('User staged for batch retry', { userKey, retryCount, stagingCount });

  // Trigger a batch flush once the staging list has enough users for a full batch.
  if (stagingCount >= RETRY_BATCH_SIZE) {
    try {
      const result = await flushRetryStagingBatch();
      if (result) {
        logger.info('Retry batch submitted to Auth0', {
          batchChunkId: result.batchChunkId,
          userCount: result.userCount,
        });
      }
      // result === null means a concurrent worker already flushed — that is fine.
    } catch (err) {
      logger.error('Retry batch flush failed', { error: err.message });
      // Non-fatal: users remain in staging list or in the chunk file.
      // The next retryProcessor invocation or the final flush in waitForCompletion
      // will pick them up.
    }
  }

  return { status: 'staged', userKey, retryCount, stagingCount };
}

module.exports = retryProcessor;
