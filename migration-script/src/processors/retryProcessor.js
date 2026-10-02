'use strict';
const checkpointService = require('../services/checkpointService');
const { flushRetryStagingBatch, RETRY_BATCH_SIZE } = require('../services/retryBatchService');
const config = require('../config');
const logger = require('../logger');

// Errors that will never succeed on retry regardless of how many attempts are made.
// Sending these straight to manual review avoids burning retry cycles per user.
// ONE_OF_MISSING: schema requires one of a set of fields — a data quality issue that
// retrying the same payload won't fix. It also fires when custom_password_hash is absent
// (Auth0 strips it from error responses), but statusProcessor now reads the original user
// from the chunk file to prevent that. Remaining ONE_OF_MISSING hits are genuine data gaps.
const UNRECOVERABLE_CODES = new Set([
  'MAX_LENGTH',
  'MISSING_REQUIRED',
  'INVALID_FORMAT',
  'SCHEMA_VIOLATION',
  'ONE_OF_MISSING',
  // Property collision — same payload will always fail, data must be fixed at source
  'NON_UNIQUE_PROPERTY_VALUE',
  // User already exists — statusProcessor should catch these, but guard here too
  'ALREADY_EXISTS',
  'USER_ALREADY_EXISTS',
  'DUPLICATE',
  'DUPLICATED_USER',
  'CONFLICT_USERNAME',
]);
function isUnrecoverableError(reason) {
  if (!reason) return false;
  const upper = reason.toUpperCase();
  return UNRECOVERABLE_CODES.has(upper.split(':')[0].trim())
    || upper.includes('STRING IS TOO LONG')
    || upper.includes('TOO LONG')
    || upper.includes('MAX_LENGTH')
    || upper.includes('MISSING REQUIRED')
    || upper.includes('INVALID FORMAT')
    || upper.includes('ONE_OF_MISSING')
    || upper.includes('NON_UNIQUE_PROPERTY_VALUE')
    || upper.includes('NON UNIQUE PROPERTY')
    || upper.includes('ALREADY EXISTS')
    || upper.includes('ALREADY EXIST')
    || upper.includes('DUPLICATED_USER');
}

async function retryProcessor(job, token) {
  const { user, failureReason } = job.data;

  // Use email as the retry-count key; fall back to stringified user if no email.
  const userKey = user.email || JSON.stringify(user);

  // Unrecoverable errors (e.g. MAX_LENGTH on username) will fail on every attempt.
  // Skip the retry queue entirely and escalate straight to manual review.
  if (isUnrecoverableError(failureReason)) {
    logger.warn('Unrecoverable error — buffering user for manual review', {
      userKey,
      failureReason,
    });
    await checkpointService.pushToManualReviewPending([user], `Unrecoverable: ${failureReason}`);
    return { status: 'manual-review-unrecoverable', userKey };
  }

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
    logger.warn('User exceeded max retries — buffering for manual review', {
      userKey,
      retryCount,
    });
    await checkpointService.pushToManualReviewPending(
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
  // Pass job+token so flushRetryStagingBatch can extend this job's lock while it
  // waits for an Auth0 slot — prevents lock expiry on the retry worker (60s).
  if (stagingCount >= RETRY_BATCH_SIZE) {
    try {
      const result = await flushRetryStagingBatch(false, job, token);
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
