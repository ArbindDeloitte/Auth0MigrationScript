const { getRedisConnection } = require('../redis');
const logger = require('../logger');

// Redis key where populated users are stored as JSON strings in a List
// Populate with: RPUSH migration:source:users '{"email":"...","name":"..."}'
const SOURCE_KEY = process.env.REDIS_SOURCE_KEY || 'migration:source:users';
const OFFSET_KEY = 'migration:source:offset';
const BATCH_SIZE = 500;

class RedisDataService {
  get redis() {
    return getRedisConnection();
  }

  async getTotalUsers() {
    return this.redis.llen(SOURCE_KEY);
  }

  async getCurrentOffset() {
    const val = await this.redis.get(OFFSET_KEY);
    return parseInt(val || '0', 10);
  }

  async saveOffset(offset) {
    await this.redis.set(OFFSET_KEY, String(offset));
  }

  async resetOffset() {
    await this.redis.del(OFFSET_KEY);
  }

  // Streams users from the Redis list in batches starting from saved offset.
  // Source list is never modified — safe to re-run and resume.
  async *streamUsers() {
    const total = await this.getTotalUsers();
    if (total === 0) {
      logger.warn('No users found in Redis source list', { key: SOURCE_KEY });
      return;
    }

    let offset = await this.getCurrentOffset();
    logger.info('Starting Redis user stream', { key: SOURCE_KEY, total, resumingFrom: offset });

    while (offset < total) {
      const end = Math.min(offset + BATCH_SIZE - 1, total - 1);
      const rawItems = await this.redis.lrange(SOURCE_KEY, offset, end);

      for (const raw of rawItems) {
        let user;
        try {
          user = JSON.parse(raw);
        } catch {
          logger.warn('Skipping malformed user record in Redis', { raw: raw.slice(0, 100) });
          continue;
        }

        if (!user.email) {
          logger.warn('Skipping user record missing email', { record: user });
          continue;
        }

        yield user;
      }

      offset = end + 1;
      await this.saveOffset(offset);
      logger.info('Redis stream progress', { processed: offset, total });
    }

    logger.info('Redis user stream complete', { total });
  }
}

const instance = new RedisDataService();

// Reads all users from Redis (resuming from saved offset) and chunks them
// into ≤480KB JSON files — mirrors the interface excelService.createChunks had.
async function createChunksFromRedis(chunksDir) {
  const fs = require('fs');
  const path = require('path');
  const { v4: uuidv4 } = require('uuid');
  const config = require('../config');

  if (!fs.existsSync(chunksDir)) fs.mkdirSync(chunksDir, { recursive: true });

  const chunks = [];
  let currentBatch = [];
  let currentSizeBytes = 2; // JSON array brackets
  let chunkIndex = 0;

  const flushChunk = () => {
    if (currentBatch.length === 0) return;
    const chunkId = uuidv4();
    const chunkPath = path.join(chunksDir, `chunk-${chunkIndex}-${chunkId}.json`);
    const content = JSON.stringify(currentBatch);
    fs.writeFileSync(chunkPath, content, 'utf-8');
    chunks.push({ chunkId, chunkPath, userCount: currentBatch.length });
    logger.info(`Chunk ${chunkIndex} written`, {
      chunkId,
      users: currentBatch.length,
      bytes: Buffer.byteLength(content, 'utf-8'),
    });
    chunkIndex++;
    currentBatch = [];
    currentSizeBytes = 2;
  };

  for await (const user of instance.streamUsers()) {
    const userJson = JSON.stringify(user);
    const addedBytes = Buffer.byteLength(
      currentBatch.length > 0 ? ',' + userJson : userJson,
      'utf-8'
    );

    if (currentSizeBytes + addedBytes > config.migration.maxChunkBytes && currentBatch.length > 0) {
      flushChunk();
    }

    currentBatch.push(user);
    currentSizeBytes += addedBytes;
  }

  flushChunk();
  logger.info('Chunking from Redis complete', { totalChunks: chunks.length });
  return chunks;
}

module.exports = instance;
module.exports.createChunksFromRedis = createChunksFromRedis;
