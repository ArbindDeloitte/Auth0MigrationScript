const auth0Service = require('../services/auth0Service');
const checkpointService = require('../services/checkpointService');
const { statusQueue } = require('../queues');
const config = require('../config');
const logger = require('../logger');

async function importProcessor(job) {
  const { chunkId, chunkPath, userCount } = job.data;

  // Idempotency guard: if this chunk was already successfully processed, skip
  const alreadyDone = await checkpointService.isChunkProcessed(chunkId);
  if (alreadyDone) {
    logger.info('Chunk already processed — skipping', { chunkId });
    return { skipped: true };
  }

  logger.info('Uploading chunk to Auth0', { chunkId, userCount, chunkPath });
  await job.updateProgress(10);

  let auth0Job;
  try {
    auth0Job = await auth0Service.createImportJob(chunkPath, {
      upsert: true,
      externalId: chunkId,
    });
  } catch (err) {
    // 429 means Auth0's concurrent job limit is hit — BullMQ will retry with backoff
    if (err.response?.status === 429) {
      logger.warn('Auth0 concurrent job limit hit — will retry', { chunkId });
    }
    throw err;
  }

  logger.info('Auth0 import job created', { chunkId, auth0JobId: auth0Job.id });
  await checkpointService.storeAuth0JobId(chunkId, auth0Job.id);
  await job.updateProgress(80);

  await statusQueue.add(
    'poll-status',
    { chunkId, auth0JobId: auth0Job.id, attempts: 0 },
    { delay: config.migration.statusPollIntervalMs }
  );

  await job.updateProgress(100);
  return { auth0JobId: auth0Job.id, chunkId };
}

module.exports = importProcessor;
