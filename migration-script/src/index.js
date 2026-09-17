const fs = require('fs');
const path = require('path');
const config = require('./config');
const logger = require('./logger');
const { closeRedisConnection } = require('./redis');
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

const isFreshRun = process.argv.includes('--fresh');

// Module-level so the signal handler can drain workers even if main() hasn't returned.
let activeWorkers = [];

async function main() {
  logger.info('Auth0 User Migration starting', {
    maxConcurrentJobs: config.migration.maxConcurrentJobs,
    freshRun: isFreshRun,
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

  // Load existing checkpoint
  const processedChunks = await checkpointService.getProcessedChunks();
  const existingStatus = await checkpointService.getMigrationStatus();

  logger.info('Checkpoint loaded', {
    alreadyProcessed: processedChunks.length,
    status: existingStatus?.status || 'none',
  });

  const processedSet = new Set(processedChunks);

  // Recover chunk files from a prior run that were written but never completed.
  // These are orphaned when the process crashes after writing a chunk file but
  // before that chunk's Auth0 job is marked processed.
  const orphanedChunks = await recoverOrphanedChunks(config.migration.chunksDir, processedSet);
  if (orphanedChunks.length > 0) {
    logger.warn('Recovered orphaned chunks from a prior run', { count: orphanedChunks.length });
  }

  // Stream users from Redis and chunk into <=480KB JSON files
  logger.info('Reading users from Redis and chunking...');
  const newChunks = await createChunksFromRedis(config.migration.chunksDir);
  const allChunks = [...orphanedChunks.map(c => c.chunk), ...newChunks];

  if (allChunks.length === 0) {
    logger.warn('No users found in Redis source list — nothing to do', { key: config.migration.redisSourceKey });
    await gracefulShutdown(activeWorkers);
    return;
  }

  await checkpointService.setTotalChunks(allChunks.length);
  await checkpointService.setMigrationStatus('running');

  // Re-enqueue orphaned chunks: if the chunk already has an Auth0 job ID stored,
  // re-add a status poll instead of re-uploading (avoids duplicate imports).
  for (const { chunk, existingAuth0JobId } of orphanedChunks) {
    if (existingAuth0JobId) {
      logger.info('Orphaned chunk already submitted to Auth0 — re-queuing status poll', {
        chunkId: chunk.chunkId,
        auth0JobId: existingAuth0JobId,
      });
      await statusQueue.add(
        'poll-status',
        { chunkId: chunk.chunkId, auth0JobId: existingAuth0JobId, attempts: 0 },
        { delay: config.migration.statusPollIntervalMs, jobId: `poll-${chunk.chunkId}` }
      );
    } else {
      await importQueue.add('import-chunk', chunk, { jobId: chunk.chunkId });
    }
  }

  // Enqueue only the new chunks that have not yet been processed
  let queued = 0;
  for (const chunk of newChunks) {
    if (processedSet.has(chunk.chunkId)) {
      logger.info('Skipping already-processed chunk', { chunkId: chunk.chunkId });
      continue;
    }
    await importQueue.add('import-chunk', chunk, { jobId: chunk.chunkId });
    queued++;
  }

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

// Scans the chunks directory for files that were written in a prior run but never
// marked processed. Returns chunks with their stored Auth0 job ID (if any).
async function recoverOrphanedChunks(chunksDir, processedSet) {
  if (!fs.existsSync(chunksDir)) return [];

  const results = [];
  for (const file of fs.readdirSync(chunksDir)) {
    const match = file.match(/^chunk-\d+-([a-f0-9-]{36})\.json$/);
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

      logger.info('Queue health check', { importQueue: ic, statusQueue: sc, retryQueue: rc, pending });

      if (pending === 0) {
        clearInterval(timer);
        await onComplete(allChunks, workers);
        resolve();
      }
    };

    const timer = setInterval(check, CHECK_INTERVAL_MS);
    check().catch((err) => logger.error('Completion check error', { error: err.message }));
  });
}

async function onComplete(allChunks, workers) {
  const manualReviewUsers = await failedUserService.readAll();
  const processed = await checkpointService.getProcessedChunks();

  const finalStatus = manualReviewUsers.length > 0 ? 'completed_with_manual_review' : 'completed';
  await checkpointService.setMigrationStatus(finalStatus);

  logger.info('=== MIGRATION COMPLETE ===', {
    totalChunks: allChunks.length,
    processedChunks: processed.length,
    manualReviewCount: manualReviewUsers.length,
    status: finalStatus,
    manualReviewFile: manualReviewUsers.length > 0 ? config.migration.manualReviewFile : null,
  });

  if (manualReviewUsers.length > 0) {
    logger.warn(
      `${manualReviewUsers.length} user(s) could not be imported after ${config.migration.maxUserRetries} retries.`,
      { file: config.migration.manualReviewFile }
    );
  }

  await gracefulShutdown(workers);
}

async function gracefulShutdown(workers) {
  logger.info('Draining and closing workers...');
  await Promise.all(workers.map((w) => w.close()));
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
    logger.error(`[${name}] Job failed`, { jobId: job?.id, error: err.message, stack: err.stack })
  );
  worker.on('error', (err) =>
    logger.error(`[${name}] Worker error`, { error: err.message })
  );
  worker.on('stalled', (jobId) =>
    logger.warn(`[${name}] Job stalled — will be re-queued`, { jobId })
  );
}

// Signal handlers use activeWorkers so they can drain in-flight jobs before exit.
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
