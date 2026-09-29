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

  // Wait for an available Auth0 import slot before submitting.
  // Slot limit is driven by MAX_CONCURRENT_AUTH0_JOBS in .env (default 2).
  // Auth0 allows up to 10 concurrent import jobs per tenant.
  const MAX_AUTH0_SLOTS = config.migration.maxConcurrentJobs;
  const SLOT_POLL_MS = 10_000;
  let slotWaits = 0;
  while (true) {
    const active = await checkpointService.getActiveAuth0JobCount();
    if (active < MAX_AUTH0_SLOTS) break;
    if (slotWaits === 0) {
      logger.info('No Auth0 import slot available — waiting', { chunkId, active });
    }
    await new Promise(r => setTimeout(r, SLOT_POLL_MS));
    slotWaits++;
  }
  if (slotWaits > 0) {
    logger.info('Auth0 import slot acquired', { chunkId, waitedMs: slotWaits * SLOT_POLL_MS });
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
    const status = err.response?.status;
    const body   = err.response?.data;
    // 429 is now retried with Retry-After inside auth0Service; if it still reaches
    // here after MAX_RATE_LIMIT_RETRIES the job should fail so BullMQ can retry later
    if (status === 400) {
      logger.error('Auth0 rejected chunk (400) — check user payload', { chunkId, auth0Error: body });
    } else {
      logger.error('Auth0 import job creation failed', { chunkId, status, auth0Error: body });
    }
    throw err;
  }

  logger.info('Auth0 import job created', { chunkId, auth0JobId: auth0Job.id });
  await checkpointService.storeAuth0JobId(chunkId, auth0Job.id);
  // Claim the slot — released by statusProcessor when the Auth0 job finishes
  await checkpointService.trackActiveAuth0Job(auth0Job.id);
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
