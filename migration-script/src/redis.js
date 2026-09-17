const { Redis } = require('ioredis');
const config = require('./config');
const logger = require('./logger');

let connection;

function getRedisConnection() {
  if (!connection) {
    connection = _newConn();
  }
  return connection;
}

// Creates a fresh IORedis instance — use this for BullMQ Queues and Workers
// so each entity owns its connection and BLPOP blocking doesn't starve producers.
function createRedisConnection() {
  return _newConn();
}

function _newConn() {
  const conn = new Redis({
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password,
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
  });
  conn.on('error', (err) =>
    logger.error('Redis connection error', { error: err.message })
  );
  conn.on('connect', () =>
    logger.info('Redis connected', { host: config.redis.host, port: config.redis.port })
  );
  return conn;
}

async function closeRedisConnection() {
  if (connection) {
    await connection.quit();
    connection = null;
  }
}

module.exports = { getRedisConnection, createRedisConnection, closeRedisConnection };
