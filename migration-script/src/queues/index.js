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
const importQueue = new Queue(QUEUE.IMPORT, {
  connection: createRedisConnection(),
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 5000 },
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
  });
}

function createStatusWorker(processorFn) {
  return new Worker(QUEUE.STATUS, processorFn, {
    connection: createRedisConnection(),
    // Status workers can run more concurrently than active Auth0 jobs
    concurrency: config.migration.maxConcurrentJobs * 2,
  });
}

function createRetryWorker(processorFn) {
  return new Worker(QUEUE.RETRY, processorFn, {
    connection: createRedisConnection(),
    concurrency: config.migration.maxConcurrentJobs,
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
