const fs = require('fs');
const path = require('path');
const config = require('./config');
const logger = require('./logger');
const { closeRedisConnection, getRedisConnection } = require('./redis');
const redisDataService = require('./services/redisDataService');
const { createChunksFromRedis } = redisDataService;
const checkpointService = require('./services/checkpointService');
const failedUserService = require('./services/failedUserService');
const auth0Service = require('./services/auth0Service');
const {
  importQueue,
  statusQueue,
  retryQueue,
  createImportWorker,
  createStatusWorker,
  createRetryWorker,
  closeQueues,
} = require('./queues');
const importProcessor = require('./processors/importProcessor');
const statusProcessor = require('./processors/statusProcessor');
const retryProcessor = require('./processors/retryProcessor');
const { flushRetryStagingBatch } = require('./services/retryBatchService');

const isFreshRun  = process.argv.includes('--fresh');
// --verify: run gap detection only. If all users are accounted for, report and
// exit without creating new chunk files or queuing any import jobs. If a gap
// exists, re-queue only the missing users and process just those via retryQueue.
const isVerifyRun = process.argv.includes('--verify');

// Module-level so the signal handler can reach these even if main() hasn't returned.
let activeWorkers       = [];
let _heartbeatInterval  = null;

async function main() {
  logger.info('Auth0 User Migration starting', {
    maxConcurrentJobs: config.migration.maxConcurrentJobs,
    freshRun: isFreshRun,
    verifyRun: isVerifyRun,
  });

  ensureOutputDirs();

  if (isFreshRun) {
    logger.warn('--fresh flag detected: resetting checkpoint and source offset in Redis');
    await checkpointService.reset();
    await redisDataService.resetOffset();
  }

  await preflight();

  // Start workers
  const importWorker = createImportWorker(importProcessor);
  const statusWorker = createStatusWorker(statusProcessor);
  const retryWorker = createRetryWorker(retryProcessor);
  activeWorkers = [importWorker, statusWorker, retryWorker];

  attachWorkerEvents('import', importWorker);
  attachWorkerEvents('status', statusWorker);
  attachWorkerEvents('retry', retryWorker);

  // When an import job exhausts ALL BullMQ retries, escalate its users.
  importWorker.on('failed', async (job, err) => {
    if (!job) return;
    const isLastAttempt = job.attemptsMade >= (job.opts?.attempts ?? 1);
    if (!isLastAttempt) return;

    const { chunkId, chunkPath } = job.data;
    const httpStatus = err.response?.status;

    logger.warn('Import job permanently failed — escalating users from chunk', {
      chunkId,
      httpStatus,
      escalateTo: httpStatus === 400 ? 'manual-review' : 'retry-queue',
    });

    let users = [];
    try {
      users = JSON.parse(fs.readFileSync(chunkPath, 'utf-8'));
    } catch (readErr) {
      logger.error('Cannot read chunk file to escalate users', { chunkId, error: readErr.message });
      return;
    }

    if (httpStatus === 400) {
      const reason = `Chunk rejected by Auth0 (400): ${JSON.stringify(err.response?.data ?? err.message)}`;
      await failedUserService.appendUsers(users, reason);
      logger.warn(`${users.length} users written to manual review`, { chunkId });
    } else {
      const failureReason = `Chunk upload failed (${httpStatus ?? 'network'}): ${err.message}`;
      await retryQueue.addBulk(
        users.map((user, i) => {
          const emailKey = (user.email || '').toLowerCase() || `noemail-${i}`;
          return {
            name: 'retry-user',
            data: { user, failureReason, chunkId },
            opts: { jobId: `retry-${emailKey}-${chunkId}` },
          };
        })
      );
      logger.warn(`${users.length} users queued for individual retry`, { chunkId });
    }
  });

  // Reset stale in-flight counter from any prior crashed run. Any batch that
  // was mid-flight when the process died will be recovered as an orphaned chunk
  // file — the counter itself is always stale after a restart.
  await checkpointService.resetBatchInFlight();

  // Flush any manual-review users that were buffered to Redis in a prior run
  // that crashed before the end-of-migration flush reached the Excel file.
  await flushManualReviewPending();

  // ── Verify-only mode (--verify) ──────────────────────────────────────────
  // Checks whether all source users are accounted for without creating new
  // chunk files or queuing any import jobs. Use this after a run where most
  // users were already in Auth0 to confirm the migration is complete, rather
  // than re-processing the entire source list.
  if (isVerifyRun) {
    const [importedCount, manualCount, totalUsers] = await Promise.all([
      checkpointService.getSuccessfulUserCount(),
      checkpointService.getManualReviewUserCount(),
      redisDataService.getTotalUsers(),
    ]);
    const gap = totalUsers - importedCount - manualCount;

    logger.info('=== VERIFY RUN ===', { totalUsers, importedCount, manualCount, gap });

    if (gap <= 0) {
      logger.info('All users accounted for — migration is complete', {
        totalUsers,
        importedCount,
        manualCount,
      });
      await flushManualReviewPending();
      const finalStatus = manualCount > 0 ? 'completed_with_manual_review' : 'completed';
      await checkpointService.setMigrationStatus(finalStatus);
      logger.info('=== MIGRATION COMPLETE (verified) ===', {
        totalUsers, importedCount, manualReviewCount: manualCount, status: finalStatus,
      });
      await gracefulShutdown(activeWorkers);
      return;
    }

    logger.warn(`Gap of ${gap} users found — re-queuing only missing users and processing`, {
      totalUsers, importedCount, manualCount, gap,
    });
    await requeueGapUsers(totalUsers);
    // Let the workers drain the gap users then complete normally.
    await waitForCompletion(activeWorkers, []);
    return;
  }
  // ── End verify-only mode ─────────────────────────────────────────────────

  // Load existing checkpoint
  const processedChunks = await checkpointService.getProcessedChunks();
  const existingStatus = await checkpointService.getMigrationStatus();

  logger.info('Checkpoint loaded', {
    alreadyProcessed: processedChunks.length,
    status: existingStatus?.status || 'none',
  });

  // Gap detection: compare per-user success records against the source list.
  // Runs before new chunks are queued so any re-queued gap users are processed
  // alongside (or before) fresh import work.
  await detectAndRecoverGap();

  const processedSet = new Set(processedChunks);

  // Recover chunk files from a prior run that were written but never completed.
  const orphanedChunks = await recoverOrphanedChunks(config.migration.chunksDir, processedSet);
  if (orphanedChunks.length > 0) {
    logger.warn('Recovered orphaned chunks from a prior run', { count: orphanedChunks.length });
  }

  // Stream users from Redis and chunk into <=480KB JSON files
  logger.info('Reading users from Redis and chunking...');
  const newChunks = await createChunksFromRedis(config.migration.chunksDir);
  const allChunks = [...orphanedChunks.map(c => c.chunk), ...newChunks];

  if (allChunks.length === 0) {
    const [sc, rc] = await Promise.all([
      statusQueue.getJobCounts('active', 'waiting', 'delayed'),
      retryQueue.getJobCounts('active', 'waiting', 'delayed'),
    ]);
    const pendingFromPriorRun =
      Object.values(sc).reduce((a, b) => a + b, 0) +
      Object.values(rc).reduce((a, b) => a + b, 0);
    const stagingCount = await checkpointService.getRetryStagingCount();

    if (pendingFromPriorRun === 0 && stagingCount === 0) {
      logger.warn('No users found in Redis source list and no pending jobs — nothing to do', {
        key: config.migration.redisSourceKey,
      });
      await gracefulShutdown(activeWorkers);
      return;
    }

    logger.info('No new chunks, but pending retry/staging work remains — continuing to completion monitor', {
      pendingFromPriorRun,
      stagingCount,
    });
    await checkpointService.resetActiveAuth0Jobs();
  } else {
    await checkpointService.syncToCurrentRun(allChunks.map(c => c.chunkId));
  }

  await checkpointService.setTotalChunks(allChunks.length);
  await checkpointService.setMigrationStatus('running');

  // Heartbeat — lets the dashboard distinguish "running" from "script crashed with
  // stale status". Written every 10 s with a 30 s TTL; deleted on graceful shutdown.
  const _hbKey = 'migration:heartbeat';
  const _writeHeartbeat = () => getRedisConnection().set(_hbKey, Date.now().toString(), 'EX', 30);
  _writeHeartbeat();
  _heartbeatInterval = setInterval(_writeHeartbeat, 10_000);

  for (const { chunk, existingAuth0JobId } of orphanedChunks) {
    if (existingAuth0JobId) {
      logger.info('Orphaned chunk already submitted to Auth0 — re-queuing status poll', {
        chunkId: chunk.chunkId,
        auth0JobId: existingAuth0JobId,
      });
      await statusQueue.add(
        'poll-status',
        { chunkId: chunk.chunkId, auth0JobId: existingAuth0JobId, attempts: 0 },
        { delay: config.migration.statusPollIntervalMs, jobId: `poll-${chunk.chunkId}`, removeOnComplete: true }
      );
    } else {
      await importQueue.add('import-chunk', chunk, { jobId: chunk.chunkId });
    }
  }

  const chunksToQueue = newChunks.filter(chunk => {
    if (processedSet.has(chunk.chunkId)) {
      logger.info('Skipping already-processed chunk', { chunkId: chunk.chunkId });
      return false;
    }
    return true;
  });

  if (chunksToQueue.length > 0) {
    await importQueue.addBulk(
      chunksToQueue.map(chunk => ({ name: 'import-chunk', data: chunk, opts: { jobId: chunk.chunkId } }))
    );
  }
  const queued = chunksToQueue.length;

  logger.info('Chunks enqueued', {
    total: allChunks.length,
    orphanedRecovered: orphanedChunks.length,
    alreadyDone: processedChunks.length,
    queued,
  });

  if (queued === 0 && orphanedChunks.length === 0 && processedChunks.length === allChunks.length) {
    logger.info('All chunks already imported — monitoring status and retry queues for completion');
  }

  await waitForCompletion(activeWorkers, allChunks);
}

async function preflight() {
  logger.info('Running pre-flight checks...');
  try {
    await auth0Service._getToken();
    logger.info('Auth0 Management API token: OK');
  } catch (err) {
    throw new Error(`Pre-flight failed — could not obtain Auth0 token: ${err.message}`);
  }
  const totalUsers = await redisDataService.getTotalUsers();
  if (totalUsers === 0) {
    logger.warn('Pre-flight: Redis source list is empty');
  } else {
    logger.info(`Pre-flight: ${totalUsers} users in Redis source list`);
  }
}

// ── Gap detection & recovery ──────────────────────────────────────────────────
// Compares the per-user success SET against the full source list. Any user
// that is neither imported nor in manual review is re-queued to retryQueue.
//
// This only runs when success tracking data exists (importedCount > 0), which
// means at least one Auth0 job has already completed in a prior run with the
// new tracking code. On a completely fresh run (importedCount === 0) it is
// skipped to avoid re-queuing the entire source list as "gaps".
async function detectAndRecoverGap() {
  // BUG FIX (Bug 7): If prior-run jobs are still in retryQueue/statusQueue, those
  // users are not yet in importedSet/manualSet. Re-queuing them from gap detection
  // would create a second job with a different jobId (gap-${email}) alongside the
  // existing job (retry-${email}-${chunkId}), causing incrementUserRetryCount to
  // fire twice and prematurely escalating users to manual review.
  // Skip gap detection until queues are fully drained — the prior work will resolve
  // those users on its own.
  const [sc, rc] = await Promise.all([
    statusQueue.getJobCounts('active', 'waiting', 'delayed'),
    retryQueue.getJobCounts('active', 'waiting', 'delayed'),
  ]);
  const pendingFromPriorRun =
    Object.values(sc).reduce((a, b) => a + b, 0) +
    Object.values(rc).reduce((a, b) => a + b, 0);
  if (pendingFromPriorRun > 0) {
    logger.info('Gap detection deferred — prior run work still in queues', { pendingFromPriorRun });
    return;
  }

  const [importedCount, totalUsers] = await Promise.all([
    checkpointService.getSuccessfulUserCount(),
    redisDataService.getTotalUsers(),
  ]);

  if (totalUsers === 0) return; // nothing in Redis — nothing to check
  if (importedCount === 0) {
    logger.info('Gap detection skipped — no per-user success records yet (first run or post-fresh)', {
      totalUsers,
    });
    return;
  }

  const manualCount = await checkpointService.getManualReviewUserCount();
  const gap = totalUsers - importedCount - manualCount;

  logger.info('Gap detection', { totalUsers, importedCount, manualCount, gap });

  if (gap <= 0) {
    logger.info('Gap detection: all source users are accounted for');
    return;
  }

  logger.warn('Gap detected — scanning source list to re-queue missing users', {
    totalUsers,
    importedCount,
    manualCount,
    gap,
  });

  await requeueGapUsers(totalUsers);
}

async function requeueGapUsers(totalUsers) {
  // Load both outcome sets into memory for O(1) per-user lookup during the scan.
  const [importedEmails, manualEmails] = await Promise.all([
    checkpointService.getAllImportedEmails(),
    checkpointService.getAllManualReviewEmails(),
  ]);

  const importedSet = new Set(importedEmails.map(e => e.toLowerCase()));
  const manualSet   = new Set(manualEmails.map(e => e.toLowerCase()));

  const SCAN_BATCH = 500;
  // addBulk submits many jobs in one Redis pipeline instead of one round-trip per
  // user. Without this, 94K sequential awaits take ~90+ seconds at startup.
  const QUEUE_BATCH = 500;
  let pendingBulk = [];
  let requeued = 0;
  let scanned = 0;

  const flushBulk = async () => {
    if (pendingBulk.length === 0) return;
    await retryQueue.addBulk(pendingBulk);
    requeued += pendingBulk.length;
    pendingBulk = [];
  };

  for (let start = 0; start < totalUsers; start += SCAN_BATCH) {
    const rawUsers = await redisDataService.getSourceUsersBatch(start, SCAN_BATCH);

    for (const raw of rawUsers) {
      let record;
      try { record = JSON.parse(raw); } catch { continue; }

      const email = (record.email || '').toLowerCase();
      if (!email) continue;
      if (importedSet.has(email) || manualSet.has(email)) continue;

      // Map the raw source record to Auth0 format before queuing
      let user;
      try {
        user = redisDataService.mapSourceToAuth0(record);
      } catch (mapErr) {
        logger.warn('Gap recovery: cannot map source record — skipping', {
          email,
          error: mapErr.message,
        });
        continue;
      }

      pendingBulk.push({
        name: 'retry-user',
        data: {
          user,
          failureReason: 'Gap recovery: user not found in Auth0 success record or manual review',
          chunkId: 'gap-recovery',
        },
        opts: { jobId: `gap-${email}` }, // deterministic: won't duplicate if already queued
      });

      if (pendingBulk.length >= QUEUE_BATCH) {
        await flushBulk();
      }
    }

    scanned += rawUsers.length;
    if (scanned % 10_000 === 0) {
      logger.info('Gap recovery scan progress', { scanned, totalUsers, requeued });
    }
  }

  await flushBulk(); // flush any remaining users under the batch threshold
  logger.warn('Gap recovery scan complete', { scanned, requeued });
}

// ── Orphan recovery ───────────────────────────────────────────────────────────
async function recoverOrphanedChunks(chunksDir, processedSet) {
  if (!fs.existsSync(chunksDir)) return [];

  const results = [];
  for (const file of fs.readdirSync(chunksDir)) {
    const match = file.match(/^chunk-\d+-([a-f0-9-]{36})\.json$/)
               || file.match(/^retry-batch-([a-f0-9-]{36})\.json$/);
    if (!match) continue;
    const chunkId = match[1];
    if (processedSet.has(chunkId)) continue;

    const chunkPath = path.join(chunksDir, file);
    let userCount = 0;
    try {
      const users = JSON.parse(fs.readFileSync(chunkPath, 'utf-8'));
      userCount = users.length;
    } catch {
      logger.warn('Could not read orphaned chunk file — skipping', { file });
      continue;
    }

    const existingAuth0JobId = await checkpointService.getAuth0JobId(chunkId);
    results.push({ chunk: { chunkId, chunkPath, userCount }, existingAuth0JobId });
  }
  return results;
}

// ── Completion monitor ────────────────────────────────────────────────────────
async function waitForCompletion(workers, allChunks) {
  return new Promise((resolve) => {
    const CHECK_INTERVAL_MS = 60_000;

    const check = async () => {
      const [ic, sc, rc] = await Promise.all([
        importQueue.getJobCounts('active', 'waiting', 'delayed'),
        statusQueue.getJobCounts('active', 'waiting', 'delayed'),
        retryQueue.getJobCounts('active', 'waiting', 'delayed'),
      ]);

      const pending =
        Object.values(ic).reduce((a, b) => a + b, 0) +
        Object.values(sc).reduce((a, b) => a + b, 0) +
        Object.values(rc).reduce((a, b) => a + b, 0);

      const batchInFlight = await checkpointService.getBatchInFlightCount();

      logger.info('Queue health check', {
        importQueue: ic,
        statusQueue: sc,
        retryQueue: rc,
        pending,
        batchInFlight,
      });

      // batchInFlight > 0 means a retry batch was popped from staging but its
      // status-poll job has not yet been added to statusQueue. Treat this the
      // same as pending > 0 to avoid declaring done prematurely.
      if (pending === 0 && batchInFlight === 0) {
        const stagingCount = await checkpointService.getRetryStagingCount();
        if (stagingCount > 0) {
          logger.info('Queues empty but staging list has users — flushing final batch', { stagingCount });
          try {
            const result = await flushRetryStagingBatch(true); // forceFlush — remaining users may be < MIN_FLUSH_SIZE
            if (result) {
              logger.info('Final retry batch submitted', {
                batchChunkId: result.batchChunkId,
                userCount: result.userCount,
              });
            }
          } catch (err) {
            logger.error('Final retry batch flush failed', { error: err.message });
          }
          return; // interval continues — status job now makes pending > 0
        }

        clearInterval(timer);
        await onComplete(allChunks, workers);
        resolve();
      }
    };

    const timer = setInterval(check, CHECK_INTERVAL_MS);
    check().catch((err) => logger.error('Completion check error', { error: err.message }));
  });
}

// Reads the Redis manual-review pending buffer, groups entries by reason, and
// writes them to the Excel file in one batch per reason group. Called at startup
// (to recover from a crash before the previous flush) and at completion.
// Deduplicates by email within each group to handle the case where a user was
// pushed to the buffer multiple times (e.g. an orphaned chunk re-processed on restart).
async function flushManualReviewPending() {
  const pending = await checkpointService.popAllManualReviewPending();
  if (pending.length === 0) return;

  // Group by reason
  const byReason = new Map();
  for (const { user, reason } of pending) {
    if (!byReason.has(reason)) byReason.set(reason, new Map());
    const emailKey = (user.email || '').toLowerCase() || JSON.stringify(user);
    byReason.get(reason).set(emailKey, user); // Map deduplicates by email
  }

  let totalWritten = 0;
  for (const [reason, userMap] of byReason) {
    const users = [...userMap.values()];
    await failedUserService.appendUsers(users, reason);
    totalWritten += users.length;
  }

  logger.info('Manual review pending buffer flushed to Excel', {
    total: pending.length,
    written: totalWritten,
    duplicatesRemoved: pending.length - totalWritten,
  });
}

async function onComplete(allChunks, workers) {
  // BUG FIX (Bug 6): Use the Redis SET (migration:manual:users) for manualCount —
  // the same source detectAndRecoverGap uses — so the gap calculation is consistent.
  // failedUserService.readAll() is an expensive Excel read and returns a different
  // count if the Excel file and the Redis SET ever diverge (e.g. manual Redis flush).
  const [manualCount, processed, importedCount, totalUsers] = await Promise.all([
    checkpointService.getManualReviewUserCount(),
    checkpointService.getProcessedChunks(),
    checkpointService.getSuccessfulUserCount(),
    redisDataService.getTotalUsers(),
  ]);

  const gap = totalUsers - importedCount - manualCount;

  if (gap > 0) {
    logger.error('MIGRATION COMPLETE BUT GAP REMAINS — re-queuing gap users', {
      totalUsers,
      importedCount,
      manualCount,
      gap,
    });
    // Re-queue gap users and let the workers drain before calling onComplete again.
    await requeueGapUsers(totalUsers);
    // waitForCompletion will call onComplete again once the queues drain.
    return;
  }

  // Write any buffered manual-review users to Excel before reporting final counts.
  await flushManualReviewPending();

  const finalStatus = manualCount > 0 ? 'completed_with_manual_review' : 'completed';
  await checkpointService.setMigrationStatus(finalStatus);

  logger.info('=== MIGRATION COMPLETE ===', {
    totalChunks: allChunks.length,
    processedChunks: processed.length,
    totalUsers,
    importedCount,
    manualReviewCount: manualCount,
    gap: 0,
    status: finalStatus,
    manualReviewFile: manualCount > 0 ? config.migration.manualReviewFile : null,
  });

  if (manualCount > 0) {
    logger.warn(
      `${manualCount} user(s) in manual review after ${config.migration.maxUserRetries} retries.`,
      { file: config.migration.manualReviewFile }
    );
  }

  await gracefulShutdown(workers);
}

async function gracefulShutdown(workers) {
  // Stop the heartbeat immediately so the dashboard sees "stopped" without
  // waiting for the 30 s TTL to expire.
  if (typeof _heartbeatInterval !== 'undefined') clearInterval(_heartbeatInterval);
  try { await getRedisConnection().del('migration:heartbeat'); } catch { /* best-effort */ }

  logger.info('Draining and closing workers...');
  const DRAIN_TIMEOUT_MS = 15_000;

  // Give workers up to 15s to finish active jobs gracefully, then force-close.
  // Without a timeout, a worker stuck in an infinite slot-wait loop (importProcessor)
  // blocks close() forever and the process never exits.
  await Promise.all(workers.map((w) =>
    Promise.race([
      w.close(),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('drain timeout')), DRAIN_TIMEOUT_MS)
      ),
    ]).catch(async (err) => {
      if (err.message === 'drain timeout') {
        logger.warn('Worker did not drain in time — force closing', { worker: w.name });
        await w.close(true).catch(() => {});
      }
    })
  ));

  await closeQueues();
  await closeRedisConnection();
  logger.info('Shutdown complete');
}

function ensureOutputDirs() {
  const dirs = [
    config.migration.outputDir,
    config.migration.chunksDir,
    `${config.migration.outputDir}/logs`,
  ];
  for (const dir of dirs) {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  }
}

function attachWorkerEvents(name, worker) {
  worker.on('completed', (job, result) =>
    logger.info(`[${name}] Job completed`, { jobId: job.id, result })
  );
  worker.on('failed', (job, err) =>
    logger.error(`[${name}] Job failed`, {
      jobId: job?.id,
      error: err.message,
      auth0Error: err.response?.data ?? null,
      stack: err.stack,
    })
  );
  worker.on('error', (err) =>
    logger.error(`[${name}] Worker error`, { error: err.message })
  );
  worker.on('stalled', (jobId) =>
    logger.warn(`[${name}] Job stalled — will be re-queued`, { jobId })
  );
}

const shutdown = async () => {
  logger.info('Shutdown signal received — draining workers');
  if (activeWorkers.length > 0) {
    await gracefulShutdown(activeWorkers);
  }
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('uncaughtException', (err) => {
  logger.error('Uncaught exception', { error: err.message, stack: err.stack });
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled rejection', { reason: String(reason) });
  process.exit(1);
});

main().catch((err) => {
  logger.error('Fatal error during migration startup', { error: err.message, stack: err.stack });
  process.exit(1);
});
