const { getRedisConnection } = require('../redis');
const logger = require('../logger');

const KEY = {
  processedChunks:  'migration:checkpoint:processed_chunks',
  auth0Jobs:        'migration:checkpoint:auth0_jobs',
  activeAuth0Jobs:  'migration:active:auth0_jobs',
  userRetryCount:   (id) => `migration:retry:${id}`,
  retryStaging:     'migration:retry:staging',
  migrationStatus:  'migration:status',
  totalChunks:      'migration:checkpoint:total_chunks',
  // Per-user outcome tracking — populated as Auth0 jobs complete.
  // Used by gap detection on startup to find users not yet imported.
  successUsers:     'migration:success:users',
  manualUsers:      'migration:manual:users',
  // Counter incremented before a retry batch is popped from staging and
  // decremented after its status-poll job is queued. Prevents the completion
  // check from declaring "done" during the narrow window between staging pop
  // and statusQueue.add.
  batchInFlight:       'migration:retry:batch:inflight',
  // Durable write-buffer for manual-review Excel writes.
  // Workers push {user, reason} here instantly (Redis RPUSH) instead of writing
  // to Excel inline. Flushed to Excel in one batch at startup and at completion.
  manualReviewPending: 'migration:manual-review:pending',
  // Email → full user JSON (HASH). Written once at chunk-creation time so
  // statusProcessor can look up the original user (with custom_password_hash)
  // when a chunk file is unavailable after a crash or mid-run restart.
  sourceEmailIndex: 'migration:source:email:index',
};

// SADD chunk size: ioredis (and Redis 5.x) support large vararg commands but we
// split into groups of 500 to stay well within any per-command limit.
const SADD_CHUNK = 500;

class CheckpointService {
  get redis() {
    return getRedisConnection();
  }

  async markChunkProcessed(chunkId) {
    await this.redis.sadd(KEY.processedChunks, chunkId);
  }

  async isChunkProcessed(chunkId) {
    return !!(await this.redis.sismember(KEY.processedChunks, chunkId));
  }

  async getProcessedChunks() {
    return this.redis.smembers(KEY.processedChunks);
  }

  async storeAuth0JobId(chunkId, auth0JobId) {
    await this.redis.hset(KEY.auth0Jobs, chunkId, auth0JobId);
  }

  async getAuth0JobId(chunkId) {
    return this.redis.hget(KEY.auth0Jobs, chunkId);
  }

  // ── Active Auth0 slot tracking ───────────────────────────────────────────
  async trackActiveAuth0Job(auth0JobId) {
    await this.redis.sadd(KEY.activeAuth0Jobs, auth0JobId);
  }

  async releaseActiveAuth0Job(auth0JobId) {
    await this.redis.srem(KEY.activeAuth0Jobs, auth0JobId);
  }

  async getActiveAuth0JobCount() {
    return this.redis.scard(KEY.activeAuth0Jobs);
  }

  // ── Cross-run cleanup ────────────────────────────────────────────────────
  async syncToCurrentRun(currentChunkIds) {
    const currentSet = new Set(currentChunkIds);

    const [existingProcessed, existingJobKeys] = await Promise.all([
      this.redis.smembers(KEY.processedChunks),
      this.redis.hkeys(KEY.auth0Jobs),
    ]);

    const staleProcessed = existingProcessed.filter(id => !currentSet.has(id));
    const staleJobKeys   = existingJobKeys.filter(id => !currentSet.has(id));

    if (staleProcessed.length > 0 || staleJobKeys.length > 0) {
      const pipe = this.redis.pipeline();
      if (staleProcessed.length > 0) pipe.srem(KEY.processedChunks, ...staleProcessed);
      if (staleJobKeys.length > 0)   pipe.hdel(KEY.auth0Jobs, ...staleJobKeys);
      await pipe.exec();
      logger.info('Cleaned stale cross-run checkpoint entries', {
        staleProcessed: staleProcessed.length,
        staleJobKeys: staleJobKeys.length,
      });
    }

    await this.redis.del(KEY.activeAuth0Jobs);
  }

  async incrementUserRetryCount(userIdentifier) {
    return this.redis.incr(KEY.userRetryCount(userIdentifier));
  }

  async getUserRetryCount(userIdentifier) {
    const val = await this.redis.get(KEY.userRetryCount(userIdentifier));
    return parseInt(val || '0', 10);
  }

  // ── Retry staging ────────────────────────────────────────────────────────
  async pushToRetryStaging(user) {
    return this.redis.rpush(KEY.retryStaging, JSON.stringify(user));
  }

  async getRetryStagingCount() {
    return this.redis.llen(KEY.retryStaging);
  }

  async popRetryStagingBatch(size) {
    const pipe = this.redis.multi();
    pipe.lrange(KEY.retryStaging, 0, size - 1);
    pipe.ltrim(KEY.retryStaging, size, -1);
    const results = await pipe.exec();
    // ioredis MULTI/EXEC returns [[err, val], [err, val]]. Check both commands.
    if (results[0][0]) throw results[0][0];
    if (results[1][0]) throw results[1][0];
    const rawUsers = results[0][1];
    if (!rawUsers || rawUsers.length === 0) return [];
    return rawUsers.map(u => JSON.parse(u));
  }

  // Pushes a batch of users back to the staging list (used when a file write or
  // Auth0 submission fails after an atomic pop, so users are not permanently lost).
  async pushBatchToRetryStaging(users) {
    if (!users || users.length === 0) return;
    const pipe = this.redis.pipeline();
    for (const user of users) {
      pipe.rpush(KEY.retryStaging, JSON.stringify(user));
    }
    await pipe.exec();
  }

  // ── Batch-in-flight counter ──────────────────────────────────────────────
  // Tracks retry batches that have been popped from staging but whose Auth0
  // status-poll job has not yet been added to the queue. The completion check
  // must see this as > 0 to avoid declaring the migration done prematurely.
  async incrementBatchInFlight() {
    return this.redis.incr(KEY.batchInFlight);
  }

  async decrementBatchInFlight() {
    const val = await this.redis.decr(KEY.batchInFlight);
    if (val < 0) await this.redis.set(KEY.batchInFlight, '0');
    return Math.max(0, val);
  }

  async getBatchInFlightCount() {
    const val = await this.redis.get(KEY.batchInFlight);
    return Math.max(0, parseInt(val || '0', 10));
  }

  // Resets the in-flight counter to 0 on startup. Any batch that was in-flight
  // when the process crashed will have left a chunk file that orphan recovery
  // handles — this counter is always stale after a restart.
  async resetBatchInFlight() {
    await this.redis.set(KEY.batchInFlight, '0');
  }

  // ── Per-user outcome tracking ────────────────────────────────────────────
  // recordSuccessfulUsers is called by statusProcessor after each completed
  // Auth0 job. It records every email that Auth0 inserted or updated.
  // The SET accumulates across the full migration run and survives restarts.
  async recordSuccessfulUsers(emails) {
    if (!emails || emails.length === 0) return;
    const lower = emails.map(e => e.toLowerCase()).filter(Boolean);
    if (lower.length === 0) return;
    const pipe = this.redis.pipeline();
    for (let i = 0; i < lower.length; i += SADD_CHUNK) {
      pipe.sadd(KEY.successUsers, ...lower.slice(i, i + SADD_CHUNK));
    }
    await pipe.exec();
  }

  async getSuccessfulUserCount() {
    return this.redis.scard(KEY.successUsers);
  }

  async isUserImported(email) {
    return !!(await this.redis.sismember(KEY.successUsers, email.toLowerCase()));
  }

  // Returns all emails in the success SET — used by gap recovery to build an
  // in-memory lookup. For very large migrations (2M+) this is ~80 MB in memory
  // on the Node side but avoids N individual SISMEMBER calls.
  async getAllImportedEmails() {
    return this.redis.smembers(KEY.successUsers);
  }

  // recordManualReviewUsers is called by failedUserService.appendUsers so the
  // Redis SET always mirrors what is in the Excel file.
  async recordManualReviewUsers(emails) {
    if (!emails || emails.length === 0) return;
    const lower = emails.map(e => e.toLowerCase()).filter(Boolean);
    if (lower.length === 0) return;
    const pipe = this.redis.pipeline();
    for (let i = 0; i < lower.length; i += SADD_CHUNK) {
      pipe.sadd(KEY.manualUsers, ...lower.slice(i, i + SADD_CHUNK));
    }
    await pipe.exec();
  }

  async getManualReviewUserCount() {
    return this.redis.scard(KEY.manualUsers);
  }

  async isUserInManualReview(email) {
    return !!(await this.redis.sismember(KEY.manualUsers, email.toLowerCase()));
  }

  async getAllManualReviewEmails() {
    return this.redis.smembers(KEY.manualUsers);
  }

  // ── Manual-review write buffer ───────────────────────────────────────────
  // Push users+reason to the durable pending list. Also immediately records
  // emails in the Redis SET so gap detection is always accurate regardless of
  // when the Excel flush happens.
  async pushToManualReviewPending(users, reason) {
    if (!users || users.length === 0) return;
    const pipe = this.redis.pipeline();
    for (const user of users) {
      pipe.rpush(KEY.manualReviewPending, JSON.stringify({ user, reason }));
    }
    await pipe.exec();
    const emails = users.map(u => (u.email || '').toLowerCase()).filter(Boolean);
    if (emails.length > 0) {
      await this.recordManualReviewUsers(emails);
    }
  }

  // Atomically reads and clears the pending list. Returns [{user, reason}, ...].
  // MULTI/EXEC makes the LRANGE+DEL atomic so a crash between the two commands
  // cannot leave entries in Redis that will cause duplicate Excel rows on restart.
  async popAllManualReviewPending() {
    const pipe = this.redis.multi();
    pipe.lrange(KEY.manualReviewPending, 0, -1);
    pipe.del(KEY.manualReviewPending);
    const results = await pipe.exec();
    if (results[0][0]) throw results[0][0];
    const items = results[0][1] || [];
    return items.map(item => {
      try { return JSON.parse(item); } catch { return null; }
    }).filter(Boolean);
  }

  // ── Source email index ───────────────────────────────────────────────────
  // Written at chunk-creation time (excelService / redisDataService) so every
  // user's full object (including custom_password_hash) is retrievable by email
  // even if the chunk file has been deleted or is unreadable.
  async bulkIndexUsersByEmail(users) {
    if (!users || users.length === 0) return;
    const pipe = this.redis.pipeline();
    for (const user of users) {
      const email = (user.email || '').toLowerCase();
      if (email) pipe.hset(KEY.sourceEmailIndex, email, JSON.stringify(user));
    }
    await pipe.exec();
  }

  // Returns the full user objects for the given emails, in the same order.
  // Null entries mean the email was not found in the index.
  async getIndexedUsersByEmails(emails) {
    if (!emails || emails.length === 0) return [];
    const lower = emails.map(e => e.toLowerCase());
    const raw = await this.redis.hmget(KEY.sourceEmailIndex, ...lower);
    return raw.map(v => {
      if (!v) return null;
      try { return JSON.parse(v); } catch { return null; }
    });
  }

  // ── Active-slot reset (retry-only restart path) ──────────────────────────
  async resetActiveAuth0Jobs() {
    await this.redis.del(KEY.activeAuth0Jobs);
  }

  // ── Status & totals ──────────────────────────────────────────────────────
  async setTotalChunks(total) {
    await this.redis.set(KEY.totalChunks, String(total));
  }

  async getTotalChunks() {
    const val = await this.redis.get(KEY.totalChunks);
    return parseInt(val || '0', 10);
  }

  async setMigrationStatus(status) {
    await this.redis.set(
      KEY.migrationStatus,
      JSON.stringify({ status, updatedAt: new Date().toISOString() })
    );
  }

  async getMigrationStatus() {
    const val = await this.redis.get(KEY.migrationStatus);
    return val ? JSON.parse(val) : null;
  }

  // ── Full reset (--fresh flag) ────────────────────────────────────────────
  async reset() {
    const retryKeys = [];
    let cursor = '0';
    do {
      const [next, keys] = await this.redis.scan(cursor, 'MATCH', 'migration:retry:*', 'COUNT', 100);
      retryKeys.push(...keys);
      cursor = next;
    } while (cursor !== '0');

    const pipe = this.redis.pipeline();
    pipe.del(KEY.processedChunks);
    pipe.del(KEY.auth0Jobs);
    pipe.del(KEY.activeAuth0Jobs);
    pipe.del(KEY.migrationStatus);
    pipe.del(KEY.totalChunks);
    pipe.del(KEY.retryStaging);
    pipe.del(KEY.successUsers);
    pipe.del(KEY.manualUsers);
    pipe.del(KEY.batchInFlight);
    pipe.del(KEY.manualReviewPending);
    pipe.del(KEY.sourceEmailIndex);
    if (retryKeys.length > 0) pipe.del(...retryKeys);
    await pipe.exec();
    logger.info('Checkpoint reset', { retryKeysCleared: retryKeys.length });
  }
}

module.exports = new CheckpointService();
