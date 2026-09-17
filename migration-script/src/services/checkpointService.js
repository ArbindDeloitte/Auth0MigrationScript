const { getRedisConnection } = require('../redis');
const logger = require('../logger');

const KEY = {
  processedChunks: 'migration:checkpoint:processed_chunks',
  auth0Jobs: 'migration:checkpoint:auth0_jobs',
  userRetryCount: (id) => `migration:retry:${id}`,
  migrationStatus: 'migration:status',
  totalChunks: 'migration:checkpoint:total_chunks',
};

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

  async incrementUserRetryCount(userIdentifier) {
    return this.redis.incr(KEY.userRetryCount(userIdentifier));
  }

  async getUserRetryCount(userIdentifier) {
    const val = await this.redis.get(KEY.userRetryCount(userIdentifier));
    return parseInt(val || '0', 10);
  }

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

  async reset() {
    // Scan and delete all per-user retry count keys so --fresh runs start clean.
    // Without this, users that exhausted retries in a prior run would be immediately
    // escalated to manual review without getting new attempts.
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
    pipe.del(KEY.migrationStatus);
    pipe.del(KEY.totalChunks);
    if (retryKeys.length > 0) pipe.del(...retryKeys);
    await pipe.exec();
    logger.info('Checkpoint reset', { retryKeysCleared: retryKeys.length });
  }
}

module.exports = new CheckpointService();
