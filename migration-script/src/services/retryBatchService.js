'use strict';
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const auth0Service = require('./auth0Service');
const checkpointService = require('./checkpointService');
const { statusQueue } = require('../queues');
const config = require('../config');
const logger = require('../logger');

const RETRY_BATCH_SIZE = 1000;
const SLOT_POLL_MS = 10_000;

// Atomically pops up to RETRY_BATCH_SIZE users from the staging list,
// writes a batch file, uploads it to Auth0, and queues a status poll.
// Returns null if the staging list was empty (another worker already flushed).
async function flushRetryStagingBatch() {
  const users = await checkpointService.popRetryStagingBatch(RETRY_BATCH_SIZE);
  if (users.length === 0) return null;

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
    fs.writeFileSync(batchPath, JSON.stringify(users), 'utf-8');
  } catch (writeErr) {
    await checkpointService.pushBatchToRetryStaging(users);
    await checkpointService.decrementBatchInFlight();
    logger.error('Retry batch file write failed — users pushed back to staging', {
      batchChunkId,
      userCount: users.length,
      error: writeErr.message,
    });
    throw writeErr;
  }

  logger.info('Flushing retry staging batch to Auth0', {
    batchChunkId,
    userCount: users.length,
  });

  // Wait for an available Auth0 import slot — same gate as importProcessor.
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
  }

  let auth0Job;
  try {
    auth0Job = await auth0Service.createImportJob(batchPath, {
      upsert: true,
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
      userCount: users.length,
      error: err.message,
    });
    throw err;
  }

  logger.info('Retry batch job created in Auth0', {
    auth0JobId: auth0Job.id,
    batchChunkId,
    userCount: users.length,
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
      { delay: config.migration.statusPollIntervalMs, jobId: `poll-${auth0Job.id}` }
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

  return { batchChunkId, auth0JobId: auth0Job.id, userCount: users.length };
}

module.exports = { flushRetryStagingBatch, RETRY_BATCH_SIZE };
