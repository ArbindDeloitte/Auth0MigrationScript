'use strict';
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const auth0Service = require('./auth0Service');
const checkpointService = require('./checkpointService');
const { statusQueue } = require('../queues');
const config = require('../config');
const logger = require('../logger');

// Keep well under Auth0's 480KB per-file limit. With ~550 bytes/user average
// (SHA-512 base64 hash + salt + all fields), 1000 users ≈ 550KB — over the limit.
// 400 users × 550 bytes ≈ 220KB, leaving plenty of headroom.
const RETRY_BATCH_SIZE = 400;
const SLOT_POLL_MS = 10_000;
// Minimum pop size to commit as a real batch. Concurrent flushes race on the
// RPUSH return value — multiple workers can all see stagingCount >= 400 and
// call flush simultaneously. The first caller pops ~400 users; the others pop
// only the small remainder. Pushing those back avoids per-user Auth0 jobs.
// Use forceFlush=true at end-of-migration cleanup.
const MIN_FLUSH_SIZE = 20;

// Atomically pops up to RETRY_BATCH_SIZE users from the staging list,
// writes a batch file, uploads it to Auth0, and queues a status poll.
// Returns null if the staging list was empty or the pop was too small (race).
async function flushRetryStagingBatch(forceFlush = false, callerJob = null, callerToken = null) {
  const users = await checkpointService.popRetryStagingBatch(RETRY_BATCH_SIZE);
  if (users.length === 0) return null;

  // Race guard: a concurrent flush already claimed the real batch; we only got
  // the small leftover. Push them back so they accumulate into a full batch.
  if (!forceFlush && users.length < MIN_FLUSH_SIZE) {
    await checkpointService.pushBatchToRetryStaging(users);
    logger.debug('Concurrent flush race — pushed small pop back to staging', { count: users.length });
    return null;
  }

  // Deduplicate by email (case-insensitive) before writing the file.
  // On restart, the same user can appear in the Redis staging list twice if they
  // were pushed to staging in a prior run before a flush completed. This is the
  // last safe point to deduplicate — after the pop they are no longer in Redis.
  const seen = new Set();
  const unique = [];
  for (const u of users) {
    const key = (u.email || '').toLowerCase() || u.username || JSON.stringify(u);
    if (!seen.has(key)) {
      seen.add(key);
      unique.push(u);
    }
  }
  if (unique.length < users.length) {
    logger.warn('Duplicate users removed from retry batch', {
      original: users.length,
      unique: unique.length,
      duplicatesRemoved: users.length - unique.length,
    });
  }
  const dedupedUsers = unique;

  // Immediately mark a batch as in-flight so the completion check in
  // waitForCompletion does not declare "done" during the window between
  // popRetryStagingBatch and statusQueue.add. Decremented after the status
  // job is queued (or on error, so the counter never leaks).
  await checkpointService.incrementBatchInFlight();

  const batchChunkId = uuidv4();
  const batchPath = path.join(config.migration.chunksDir, `retry-batch-${batchChunkId}.json`);

  // BUG FIX (Bug 1): Write the file BEFORE any further async work.
  // If writeFileSync throws (e.g. disk full), push users back to staging so
  // they are not permanently lost from the queue.
  try {
    fs.writeFileSync(batchPath, JSON.stringify(dedupedUsers), 'utf-8');
  } catch (writeErr) {
    await checkpointService.pushBatchToRetryStaging(dedupedUsers);
    await checkpointService.decrementBatchInFlight();
    logger.error('Retry batch file write failed — users pushed back to staging', {
      batchChunkId,
      userCount: dedupedUsers.length,
      error: writeErr.message,
    });
    throw writeErr;
  }

  logger.info('Flushing retry staging batch to Auth0', {
    batchChunkId,
    userCount: dedupedUsers.length,
  });

  // Wait for an available Auth0 import slot — same gate as importProcessor.
  // If called from inside a retryProcessor BullMQ job, extend its lock on every
  // wait iteration so it doesn't expire while parked here (lockDuration = 60s,
  // each iteration is 10s, so 7+ iterations would exceed the lock without this).
  const MAX_AUTH0_SLOTS = config.migration.maxConcurrentJobs;
  let slotWaits = 0;
  while (true) {
    const active = await checkpointService.getActiveAuth0JobCount();
    if (active < MAX_AUTH0_SLOTS) break;
    if (slotWaits === 0) {
      logger.info('No Auth0 slot available — retry batch waiting', { batchChunkId, active });
    }
    await new Promise(r => setTimeout(r, SLOT_POLL_MS));
    slotWaits++;
    if (callerJob && callerToken) {
      await callerJob.extendLock(callerToken, 60_000).catch(() => {});
    }
  }

  let auth0Job;
  try {
    auth0Job = await auth0Service.createImportJob(batchPath, {
      upsert: config.migration.importUpsert,
      externalId: `retry-batch-${batchChunkId}`,
    });
  } catch (err) {
    // Decrement the in-flight counter before throwing — no status job will
    // be created, so the completion check must not be held waiting for one.
    // The chunk file is kept on disk; recoverOrphanedChunks will re-queue it
    // on the next restart.
    await checkpointService.decrementBatchInFlight();
    logger.error('Retry batch upload to Auth0 failed — batch file kept for orphan recovery on restart', {
      batchChunkId,
      userCount: dedupedUsers.length,
      error: err.message,
    });
    throw err;
  }

  logger.info('Retry batch job created in Auth0', {
    auth0JobId: auth0Job.id,
    batchChunkId,
    userCount: dedupedUsers.length,
  });

  await checkpointService.storeAuth0JobId(batchChunkId, auth0Job.id);
  await checkpointService.trackActiveAuth0Job(auth0Job.id);

  // BUG FIX (Bug 2): statusQueue.add must be in a try/catch. If it throws
  // after trackActiveAuth0Job succeeds, the active slot would be permanently
  // occupied (slot-wait loop spins forever) and batchInFlight would stay ≥ 1
  // (completion check hangs forever). Release both on failure.
  // The chunk file + stored auth0JobId ensure orphan recovery handles this on
  // the next restart.
  //
  // BUG FIX (Bug 4): Deterministic jobId `poll-${auth0JobId}` prevents a
  // second poll chain from spawning if this status job is stalled and re-run
  // while a newly-queued duplicate is already in the delayed queue.
  try {
    await statusQueue.add(
      'poll-status',
      { chunkId: batchChunkId, auth0JobId: auth0Job.id, attempts: 0, isRetry: false },
      { delay: config.migration.statusPollIntervalMs, jobId: `poll-${auth0Job.id}-0`, removeOnComplete: true }
    );
  } catch (queueErr) {
    await checkpointService.releaseActiveAuth0Job(auth0Job.id);
    await checkpointService.decrementBatchInFlight();
    logger.error('Failed to queue status poll — active slot and inflight counter released; orphan recovery will handle on restart', {
      batchChunkId,
      auth0JobId: auth0Job.id,
      error: queueErr.message,
    });
    throw queueErr;
  }

  // Status job is now durably queued — safe to clear the in-flight counter.
  await checkpointService.decrementBatchInFlight();

  return { batchChunkId, auth0JobId: auth0Job.id, userCount: dedupedUsers.length };
}

module.exports = { flushRetryStagingBatch, RETRY_BATCH_SIZE };
