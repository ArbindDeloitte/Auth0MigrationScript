const fs = require('fs');
const path = require('path');
const auth0Service = require('../services/auth0Service');
const checkpointService = require('../services/checkpointService');
const { statusQueue, retryQueue } = require('../queues');
const config = require('../config');
const logger = require('../logger');

// Auth0 error codes returned when upsert:false and a user already exists in the connection.
// We treat these as "confirmed" — the user IS in Auth0 — rather than retrying them.
// DUPLICATED_USER is Auth0's code when the same user appears twice in a single import file
// or when a user already exists and upsert=false. Both mean the user is in Auth0.
const ALREADY_EXISTS_CODES = new Set([
  'ALREADY_EXISTS',
  'USER_ALREADY_EXISTS',
  'DUPLICATE',
  'DUPLICATED_USER',
  'CONFLICT_USERNAME',
]);
function isAlreadyExistsError(detail) {
  const code = (detail.code || '').toUpperCase();
  const msg  = (detail.message || '').toLowerCase();
  return ALREADY_EXISTS_CODES.has(code)
    || msg.includes('already exists')
    || msg.includes('already exist')     // Auth0 sometimes omits the trailing 's'
    || msg.includes('already been used')
    || msg.includes('duplicate');
}

// Errors that will never succeed on retry — skip the retry queue, go straight to manual review.
//
// ONE_OF_MISSING: Auth0 requires one of a set of field combinations (e.g. custom_password_hash).
//   The email-index fallback below restores the full user from Redis when Auth0 strips it from
//   the error response. Any remaining ONE_OF_MISSING hits are genuine source-data gaps.
// NON_UNIQUE_PROPERTY_VALUE: a property (username, email) collides with an existing user in Auth0.
//   Retrying the same payload always fails — the data must be fixed at source.
const DIRECT_MANUAL_REVIEW_CODES = new Set([
  'ONE_OF_MISSING',
  'NON_UNIQUE_PROPERTY_VALUE',
]);
function isDirectManualReviewError(detail) {
  const code = (detail.code || '').toUpperCase();
  const msg  = (detail.message || '').toLowerCase();
  return DIRECT_MANUAL_REVIEW_CODES.has(code)
    || msg.includes('one_of_missing')
    || msg.includes('non_unique_property_value')
    || msg.includes('non unique property');
}

async function statusProcessor(job) {
  const { chunkId, auth0JobId, attempts, isRetry, user: retryUser } = job.data;

  let jobStatus;
  try {
    jobStatus = await auth0Service.getJobStatus(auth0JobId);
  } catch (err) {
    logger.error('Failed to fetch Auth0 job status — will re-poll', { auth0JobId, error: err.message });
    if (attempts >= config.migration.statusPollMaxAttempts) {
      logger.error('Auth0 job timed out (fetch errors) — escalating all users to retry', { auth0JobId, chunkId });
      await checkpointService.releaseActiveAuth0Job(auth0JobId);
      await handleJobFailure(chunkId, isRetry, retryUser, 'Auth0 job status fetch failed too many times');
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
      await checkpointService.releaseActiveAuth0Job(auth0JobId);
      await handleJobFailure(chunkId, isRetry, retryUser, 'Auth0 job timed out after 2 hours');
      await checkpointService.markChunkProcessed(chunkId);
      return { status: 'timeout' };
    }
    await requeuePoll(job.data);
    return { status: 'polling', attempts: attempts + 1 };
  }

  if (jobStatus.status === 'failed') {
    logger.error('Auth0 job failed entirely — escalating all users to retry', { auth0JobId, chunkId });
    await checkpointService.releaseActiveAuth0Job(auth0JobId);
    await handleJobFailure(chunkId, isRetry, retryUser, 'Auth0 job failed');
    await checkpointService.markChunkProcessed(chunkId);
    return { status: 'failed' };
  }

  if (jobStatus.status === 'completed') {
    const summary = jobStatus.summary || {};
    logger.info('Auth0 job completed', { auth0JobId, chunkId, summary });

    // Release the Auth0 slot immediately — the job is done on Auth0's side.
    await checkpointService.releaseActiveAuth0Job(auth0JobId);

    // Read the chunk file once up-front. This serves two purposes:
    //   1. Auth0 strips custom_password_hash from job error responses (security policy).
    //      If we retried with error.user, every user would fail ONE_OF_MISSING.
    //      We use the original full user object from the chunk file instead.
    //   2. recordChunkSuccesses needs the full user list — re-using the same read
    //      avoids a second file scan.
    const chunkUserMap = loadChunkUserMap(chunkId);

    let errors = [];
    // Declared outside the if block so recordChunkSuccesses can always reference it.
    const realFailedErrors = [];

    if (summary.failed > 0) {
      try {
        errors = await auth0Service.getJobErrors(auth0JobId);
      } catch (err) {
        logger.error('Could not fetch job errors — retrying all users in chunk', { auth0JobId, error: err.message });
        await handleJobFailure(chunkId, isRetry, retryUser, 'Could not fetch individual errors after job completion');
        await checkpointService.markChunkProcessed(chunkId);
        return { status: 'error-fetch-failed' };
      }

      if (errors.length === 0) {
        logger.warn('Auth0 reported failed users but returned empty error list — retrying whole chunk', {
          auth0JobId,
          chunkId,
          failedCount: summary.failed,
        });
        await handleJobFailure(chunkId, isRetry, retryUser, 'Auth0 reported failures but returned no error details');
        await checkpointService.markChunkProcessed(chunkId);
        return { status: 'empty-errors-fallback' };
      }

      const alreadyExistsEmails = [];
      const alreadyExistsUsers = [];      // tracked for manual review visibility
      const directManualReviewItems = []; // ONE_OF_MISSING / NON_UNIQUE — skip retry
      const retryBulkJobs = [];

      logger.warn(`${errors.length} users failed in job — classifying errors`, { auth0JobId, chunkId });

      // Pre-fetch full user objects from the Redis email index for any user not
      // present in the chunk file map. Auth0 strips custom_password_hash from error
      // responses, so we must restore it from our own source before retrying.
      // The index was built at chunk-creation time and always has the full object.
      const missingFromChunkEmails = errors
        .filter(e => e.user && !chunkUserMap?.has((e.user.email || '').toLowerCase()))
        .map(e => (e.user.email || '').toLowerCase())
        .filter(Boolean);

      let emailIndexFallbacks = new Map();
      if (missingFromChunkEmails.length > 0) {
        const indexedUsers = await checkpointService.getIndexedUsersByEmails(missingFromChunkEmails);
        for (let j = 0; j < missingFromChunkEmails.length; j++) {
          if (indexedUsers[j]) emailIndexFallbacks.set(missingFromChunkEmails[j], indexedUsers[j]);
        }
      }

      for (let i = 0; i < errors.length; i++) {
        const error = errors[i];
        if (!error.user) continue;

        const detail = (error.errors || [])[0] || {};
        const emailKey = (error.user.email || '').toLowerCase() || `noemail-${i}`;

        // 3-way fallback for the full user (with custom_password_hash):
        //   1. chunk file map  — primary source, written at import time
        //   2. Redis email index — written at chunk-creation time; survives if the chunk file is gone
        //   3. error.user      — Auth0 error response; never has custom_password_hash
        const originalUser = chunkUserMap?.get(emailKey)
          || emailIndexFallbacks.get(emailKey)
          || error.user;

        if (!chunkUserMap?.has(emailKey) && !emailIndexFallbacks.has(emailKey)) {
          logger.warn('User not found in chunk file or email index — falling back to Auth0 error.user (no password hash)', {
            chunkId, email: emailKey,
          });
        }

        if (isAlreadyExistsError(detail)) {
          // User is already in Auth0 — mark confirmed. Also flag in manual review so
          // the team can see which source users were already present before migration.
          if (emailKey) alreadyExistsEmails.push(emailKey);
          alreadyExistsUsers.push(originalUser);
        } else if (isDirectManualReviewError(detail)) {
          // ONE_OF_MISSING or NON_UNIQUE_PROPERTY_VALUE — retrying the same payload
          // will always fail. Route directly to manual review, skip the retry queue.
          realFailedErrors.push(error);
          const failureReason = detail.code
            ? `${detail.code}: ${detail.message}`
            : JSON.stringify(error.errors || error);
          directManualReviewItems.push({ user: originalUser, reason: failureReason });
        } else {
          realFailedErrors.push(error);
          const failureReason = detail.code
            ? `${detail.code}: ${detail.message}`
            : JSON.stringify(error.errors || error);
          retryBulkJobs.push({
            name: 'retry-user',
            data: { user: originalUser, failureReason, chunkId },
            opts: { jobId: `retry-${emailKey}-${chunkId}` },
          });
        }
      }

      if (alreadyExistsEmails.length > 0) {
        await checkpointService.recordSuccessfulUsers(alreadyExistsEmails);
        await checkpointService.pushToManualReviewPending(
          alreadyExistsUsers,
          'User already exists in Auth0 — confirmed present, no action needed'
        );
        logger.info('Already-in-Auth0 users recorded as confirmed and flagged for manual review', {
          chunkId, count: alreadyExistsEmails.length,
        });
      }

      if (directManualReviewItems.length > 0) {
        // Group by reason for a single pushToManualReviewPending call per distinct reason.
        const byReason = new Map();
        for (const { user, reason } of directManualReviewItems) {
          if (!byReason.has(reason)) byReason.set(reason, []);
          byReason.get(reason).push(user);
        }
        for (const [reason, users] of byReason) {
          await checkpointService.pushToManualReviewPending(users, reason);
        }
        logger.warn(`${directManualReviewItems.length} users routed directly to manual review (unrecoverable)`, { chunkId });
      }

      if (retryBulkJobs.length > 0) {
        await retryQueue.addBulk(retryBulkJobs);
        logger.warn(`${retryBulkJobs.length} users queued for retry`, { chunkId });
      }
    }

    await recordChunkSuccesses(chunkId, realFailedErrors, chunkUserMap);
    await checkpointService.markChunkProcessed(chunkId);
    return { status: 'completed', summary };
  }

  logger.warn('Unrecognised Auth0 job status — re-polling', { auth0JobId, status: jobStatus.status });
  await requeuePoll(job.data);
  return { status: 'unknown-repolling' };
}

// Reads the chunk file for chunkId and returns a Map<lowercaseEmail, fullUser>.
// Returns null if the file cannot be found or parsed — callers fall back gracefully.
function loadChunkUserMap(chunkId) {
  const chunksDir = config.migration.chunksDir;
  try {
    const files = fs.readdirSync(chunksDir);
    const file = files.find(f => f.includes(chunkId));
    if (!file) return null;
    const allUsers = JSON.parse(fs.readFileSync(path.join(chunksDir, file), 'utf-8'));
    const map = new Map();
    for (const u of allUsers) {
      const email = (u.email || '').toLowerCase();
      if (email) map.set(email, u);
    }
    return map;
  } catch {
    return null;
  }
}

// Derives the list of successfully imported users for a given chunk by
// subtracting known Auth0 failures from the full chunk user list, then
// records those emails in the per-user success SET in Redis.
// Accepts a pre-loaded chunkUserMap (from loadChunkUserMap) to avoid re-reading
// the file when statusProcessor already loaded it for the retry lookup above.
async function recordChunkSuccesses(chunkId, failedErrors, chunkUserMap = null) {
  let allUsers;

  if (chunkUserMap) {
    allUsers = [...chunkUserMap.values()];
  } else {
    // Fallback: load the file ourselves (e.g. called from a path that didn't pre-load)
    const chunksDir = config.migration.chunksDir;
    try {
      const files = fs.readdirSync(chunksDir);
      const file = files.find(f => f.includes(chunkId));
      if (!file) {
        logger.warn('Cannot find chunk file for success recording — skipping', { chunkId });
        return;
      }
      allUsers = JSON.parse(fs.readFileSync(path.join(chunksDir, file), 'utf-8'));
    } catch (err) {
      logger.warn('Cannot read chunk file for success recording', { chunkId, error: err.message });
      return;
    }
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
  // IMPORTANT: requeuePoll is called while the CURRENT status poll job is still
  // "active" in BullMQ. Using attempt+1 in the jobId gives each hop a unique id
  // that cannot collide with the currently running job.
  const nextAttempts = attempts + 1;
  await statusQueue.add(
    'poll-status',
    { chunkId, auth0JobId, attempts: nextAttempts, isRetry, user },
    {
      delay: config.migration.statusPollIntervalMs,
      jobId: `poll-${auth0JobId}-${nextAttempts}`,
      removeOnComplete: true,
    }
  );
}

// Handles a fully-failed Auth0 job. For normal chunks, re-reads the chunk file and
// queues each user to the retry queue. For retry jobs, pushes the user to the
// manual-review pending buffer (fast Redis write, flushed to Excel at completion).
async function handleJobFailure(chunkId, isRetry, retryUser, reason) {
  if (isRetry) {
    if (retryUser) {
      logger.warn('Retry job failed — buffering user for manual review', { chunkId, reason });
      await checkpointService.pushToManualReviewPending([retryUser], `Retry job failed: ${reason}`);
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

  await retryQueue.addBulk(
    users.map((user, i) => {
      const emailKey = (user.email || '').toLowerCase() || `noemail-${i}`;
      return {
        name: 'retry-user',
        data: { user, failureReason: reason, chunkId },
        opts: { jobId: `retry-${emailKey}-${chunkId}` },
      };
    })
  );
}

module.exports = statusProcessor;
