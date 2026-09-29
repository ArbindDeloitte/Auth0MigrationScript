const { getRedisConnection } = require('../redis');
const logger = require('../logger');

// Redis key where source users are stored as JSON strings in a List.
// Expected record shape:
//   { first_name, last_name, email, password_hash, language_preference, uid }
// Populate with: RPUSH migration:source:users '{"email":"...","uid":"...","first_name":"...",...}'
const SOURCE_KEY = process.env.REDIS_SOURCE_KEY || 'migration:source:users';
const OFFSET_KEY = 'migration:source:offset';
const BATCH_SIZE = 500;

// LDAP hash prefix → { auth0Algorithm, hashBytes, salted }
// hashBytes = fixed digest length in bytes; salted = digest || salt blob after stripping prefix
const LDAP_HASH_FORMATS = {
  '{SHA}':     { algorithm: 'sha1',   hashBytes: 20, salted: false },
  '{SSHA}':    { algorithm: 'sha1',   hashBytes: 20, salted: true  },
  '{SHA256}':  { algorithm: 'sha256', hashBytes: 32, salted: false },
  '{SSHA256}': { algorithm: 'sha256', hashBytes: 32, salted: true  },
  '{SHA512}':  { algorithm: 'sha512', hashBytes: 64, salted: false },
  '{SSHA512}': { algorithm: 'sha512', hashBytes: 64, salted: true  },
};

// Parses any supported LDAP password hash string into the Auth0 custom_password_hash shape.
// Salted formats (SSHA*): base64blob decodes to digest_bytes || salt_bytes.
// Auth0 requires digest and salt as separate base64 values.
function parseLdapPasswordHash(raw) {
  const upperRaw = raw.toUpperCase();
  const key = Object.keys(LDAP_HASH_FORMATS).find(k => upperRaw.startsWith(k));

  if (!key) {
    // Unknown prefix — pass the value through as-is and let Auth0 reject it with a real error
    return { algorithm: 'sha512', hash: { value: raw, encoding: 'base64' } };
  }

  const { algorithm, hashBytes, salted } = LDAP_HASH_FORMATS[key];
  const b64 = raw.slice(key.length);
  const buf  = Buffer.from(b64, 'base64');

  if (!salted) {
    return {
      algorithm,
      hash: { value: buf.toString('base64'), encoding: 'base64' },
    };
  }

  const hash = buf.slice(0, hashBytes);
  const salt = buf.slice(hashBytes);
  return {
    algorithm,
    hash: { value: hash.toString('base64'), encoding: 'base64' },
    salt: { value: salt.toString('base64'), position: 'suffix', encoding: 'base64' },
  };
}

// Maps a raw source record (OUD/LADWP format) to the Auth0 user import shape.
// Source fields: first_name, last_name, email, password_hash, language_preference, uid
function mapSourceToAuth0(record) {
  const firstName = (record.first_name || '').trim();
  const lastName = (record.last_name || '').trim();
  const fullName = [firstName, lastName].filter(Boolean).join(' ');

  const user = {
    email: record.email,
    email_verified: false,
  };

  if (firstName) user.given_name = firstName;
  if (lastName) user.family_name = lastName;
  if (fullName) user.name = fullName;

  // UID is stored as Auth0 username (must be unique per connection)
  if (record.uid) user.username = String(record.uid);

  // password_hash: map to Auth0 custom_password_hash.
  // If the ETL already produced a structured object, use it directly.
  // Otherwise parse the LDAP prefix (SHA/SSHA/SHA256/SSHA256/SHA512/SSHA512).
  if (record.password_hash) {
    const raw = record.password_hash;
    user.custom_password_hash =
      typeof raw === 'object' ? raw : parseLdapPasswordHash(raw);
  }

  // Language preference stored as user_metadata so it is accessible post-migration
  if (record.language_preference) {
    user.user_metadata = { language: record.language_preference };
  }

  // requireEmailChange flagged users get it recorded in app_metadata
  if (record.requireEmailChange === true) {
    user.app_metadata = { requireEmailChange: true };
  }

  return user;
}

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

  // Reads a slice of the source list without advancing the offset checkpoint.
  // Used by gap recovery to scan for users not yet in Auth0 or manual review.
  async getSourceUsersBatch(start, count) {
    return this.redis.lrange(SOURCE_KEY, start, start + count - 1);
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
        let record;
        try {
          record = JSON.parse(raw);
        } catch {
          logger.warn('Skipping malformed user record in Redis', { raw: raw.slice(0, 100) });
          continue;
        }

        if (!record.email) {
          logger.warn('Skipping user record missing email', { record });
          continue;
        }

        if (!record.uid) {
          logger.warn('User record missing uid — username will be omitted', { email: record.email });
        }

        yield mapSourceToAuth0(record);
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
module.exports.mapSourceToAuth0 = mapSourceToAuth0;
