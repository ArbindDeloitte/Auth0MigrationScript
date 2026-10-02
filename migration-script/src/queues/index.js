const { Queue, Worker } = require('bullmq');
const { createRedisConnection } = require('../redis');
const config = require('../config');

const QUEUE = {
  IMPORT: 'auth0-import',
  STATUS: 'auth0-status',
  RETRY: 'auth0-retry-users',
};

// BullMQ requires separate IORedis connections for queue producers and workers —
// a shared connection can deadlock because workers use BLPOP which blocks the connection.
// auth0Service handles 429 internally with up to 8 retries respecting Retry-After.
// These BullMQ retries are a final safety net for network drops or Auth0 outages.
// Base delay 60s → 60s, 120s, 240s, 480s, 960s across 5 attempts.
const importQueue = new Queue(QUEUE.IMPORT, {
  connection: createRedisConnection(),
  defaultJobOptions: {
    attempts: 5,
    backoff: { type: 'exponential', delay: 60_000 },
    removeOnComplete: { count: 200 },
    removeOnFail: { count: 500 },
  },
});

const statusQueue = new Queue(QUEUE.STATUS, {
  connection: createRedisConnection(),
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 10000 },
    removeOnComplete: { count: 200 },
    removeOnFail: { count: 500 },
  },
});

const retryQueue = new Queue(QUEUE.RETRY, {
  connection: createRedisConnection(),
  defaultJobOptions: {
    attempts: 1, // retry logic handled internally by retryProcessor
    removeOnComplete: { count: 200 },
    removeOnFail: { count: 500 },
  },
});

function createImportWorker(processorFn) {
  return new Worker(QUEUE.IMPORT, processorFn, {
    connection: createRedisConnection(),
    concurrency: config.migration.maxConcurrentJobs,
    // Import jobs spin in a slot-wait loop (10s × N iterations) before uploading
    // to Auth0. Default 30s lock expires during that wait — use 5 min so the job
    // stays alive while waiting for a free Auth0 slot. The loop also extends the
    // lock on every iteration as an extra safety net.
    lockDuration: 300_000,
  });
}

function createStatusWorker(processorFn) {
  return new Worker(QUEUE.STATUS, processorFn, {
    connection: createRedisConnection(),
    concurrency: config.migration.maxConcurrentJobs * 2,
    lockDuration: 60_000,
  });
}

function createRetryWorker(processorFn) {
  return new Worker(QUEUE.RETRY, processorFn, {
    connection: createRedisConnection(),
    // Higher concurrency so the 94K gap-recovery backlog drains quickly.
    // retryProcessor only does Redis ops — no Auth0 calls — so high concurrency is safe.
    concurrency: 20,
    lockDuration: 60_000,
  });
}

async function closeQueues() {
  await Promise.all([importQueue.close(), statusQueue.close(), retryQueue.close()]);
}

module.exports = {
  importQueue,
  statusQueue,
  retryQueue,
  createImportWorker,
  createStatusWorker,
  createRetryWorker,
  closeQueues,
  QUEUE,
};
