require('dotenv').config();

const config = {
  auth0: {
    domain: process.env.AUTH0_DOMAIN,
    clientId: process.env.AUTH0_MGMT_CLIENT_ID,
    apiKey: process.env.AUTH0_MGMT_API_KEY,
    connectionId: process.env.AUTH0_CONNECTION_ID,
    get audience() {
      return `https://${config.auth0.domain}/api/v2/`;
    },
  },
  migration: {
    maxConcurrentJobs: parseInt(process.env.MAX_CONCURRENT_AUTH0_JOBS || '2', 10),
    // 480KB — safe buffer below Auth0's 500KB hard limit
    maxChunkBytes: 480 * 1024,
    maxUserRetries: parseInt(process.env.MAX_USER_RETRIES || '3', 10),
    redisSourceKey: process.env.REDIS_SOURCE_KEY || 'migration:source:users',
    outputDir: process.env.OUTPUT_DIR || './output',
    chunksDir: process.env.CHUNKS_DIR || './output/chunks',
    // Manual review file is now Excel (.xlsx) so the team can open and act on it directly
    manualReviewFile: process.env.MANUAL_REVIEW_FILE || './output/manual-review.xlsx',
    statusPollIntervalMs: parseInt(process.env.STATUS_POLL_INTERVAL_MS || '30000', 10),
    statusPollMaxAttempts: parseInt(process.env.STATUS_POLL_MAX_ATTEMPTS || '240', 10),
    // false = existing users surface as ALREADY_EXISTS errors (accurate tracking)
    // true  = existing users are silently updated (overwrites metadata)
    importUpsert: process.env.AUTH0_IMPORT_UPSERT === 'true',
    // Auth0 enforces a username length limit (default 15). Users whose username
    // exceeds this are skipped from the import and sent to manual review.
    usernameMaxLength: parseInt(process.env.AUTH0_USERNAME_MAX_LENGTH || '15', 10),
  },
  redis: {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    password: process.env.REDIS_PASSWORD || undefined,
    // Required by BullMQ — must be null, not a number
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
  },
};

const required = [
  ['AUTH0_DOMAIN', config.auth0.domain],
  ['AUTH0_MGMT_CLIENT_ID', config.auth0.clientId],
  ['AUTH0_MGMT_API_KEY', config.auth0.apiKey],
  ['AUTH0_CONNECTION_ID', config.auth0.connectionId],
];

for (const [key, val] of required) {
  if (!val) throw new Error(`Missing required environment variable: ${key}`);
}

module.exports = config;
