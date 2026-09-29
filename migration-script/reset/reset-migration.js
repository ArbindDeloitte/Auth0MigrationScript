'use strict';
/**
 * reset-migration.js
 *
 * Clears all migration checkpoint state in Redis, drains BullMQ queues,
 * and deletes orphaned chunk files so the migration can be re-run from
 * scratch against the same source users already in Redis.
 *
 * Run from the migration-script directory:
 *   node reset/reset-migration.js
 */

const fs   = require('fs');
const path = require('path');
const Redis = require('ioredis');
const { Queue } = require('bullmq');

const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);
const CHUNKS_DIR = path.resolve(__dirname, '../output/chunks');

const CHECKPOINT_KEYS = [
  'migration:checkpoint:processed_chunks',
  'migration:checkpoint:total_chunks',
  'migration:checkpoint:auth0_jobs',
  'migration:active:auth0_jobs',
  'migration:status',
  'migration:source:offset',
  'migration:retry:staging',
  'migration:success:users',
  'migration:manual:users',
  'migration:retry:batch:inflight',
];

const QUEUE_NAMES = ['auth0-import', 'auth0-status', 'auth0-retry-users'];

async function drainQueue(name, redisOpts) {
  const q = new Queue(name, { connection: new Redis(redisOpts) });
  try {
    const counts = await q.getJobCounts('waiting', 'active', 'delayed', 'completed', 'failed');
    const total  = Object.values(counts).reduce((a, b) => a + b, 0);
    if (total === 0) {
      console.log(`  ${name}: empty — nothing to clear`);
    } else {
      // Clean all job states
      await Promise.all([
        q.clean(0, 100_000, 'completed'),
        q.clean(0, 100_000, 'failed'),
        q.clean(0, 100_000, 'wait'),
        q.clean(0, 100_000, 'delayed'),
        q.clean(0, 100_000, 'active'),
      ]);
      await q.drain();
      console.log(`  ${name}: cleared  (was: waiting=${counts.waiting} active=${counts.active} completed=${counts.completed} failed=${counts.failed} delayed=${counts.delayed})`);
    }
  } catch (err) {
    console.log(`  ${name}: error — ${err.message}`);
  } finally {
    await q.close();
  }
}

async function main() {
  const redisOpts = { host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: null };
  const redis = new Redis(redisOpts);

  console.log(`\nConnecting to Redis ${REDIS_HOST}:${REDIS_PORT}...`);

  // ── 1. Delete fixed checkpoint keys ──────────────────────────────────────
  console.log('\n[1/4] Clearing checkpoint keys...');
  for (const key of CHECKPOINT_KEYS) {
    const deleted = await redis.del(key);
    console.log(`  ${deleted ? 'DELETED' : 'not found'} → ${key}`);
  }

  // ── 2. Scan + delete all per-user retry counters ──────────────────────────
  console.log('\n[2/4] Scanning for per-user retry counters (migration:retry:*)...');
  const retryKeys = [];
  let cursor = '0';
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', 'migration:retry:*', 'COUNT', 100);
    retryKeys.push(...keys);
    cursor = next;
  } while (cursor !== '0');

  if (retryKeys.length > 0) {
    await redis.del(...retryKeys);
    console.log(`  Deleted ${retryKeys.length} retry counter key(s).`);
  } else {
    console.log('  None found.');
  }

  // ── 3. Drain BullMQ queues ────────────────────────────────────────────────
  console.log('\n[3/4] Draining BullMQ queues...');
  for (const name of QUEUE_NAMES) {
    await drainQueue(name, redisOpts);
  }

  // ── 4. Delete orphaned chunk files ────────────────────────────────────────
  console.log('\n[4/4] Removing orphaned chunk files...');
  if (fs.existsSync(CHUNKS_DIR)) {
    const files = fs.readdirSync(CHUNKS_DIR).filter(f => f.endsWith('.json'));
    if (files.length === 0) {
      console.log('  No chunk files found.');
    } else {
      for (const file of files) {
        fs.unlinkSync(path.join(CHUNKS_DIR, file));
        console.log(`  Deleted → ${file}`);
      }
    }
  } else {
    console.log('  Chunks directory does not exist — nothing to clean.');
  }

  // ── Summary ───────────────────────────────────────────────────────────────
  const userCount = await redis.llen('migration:source:users');
  console.log('\n═══════════════════════════════════════════');
  console.log('  Reset complete.');
  console.log(`  Source users still in Redis: ${userCount}`);
  console.log('  All checkpoint state cleared.');
  console.log('  All queues drained.');
  console.log('  Ready to re-run: node src/index.js');
  console.log('═══════════════════════════════════════════\n');

  await redis.quit();
}

main().catch(err => {
  console.error('\nFailed:', err.message);
  process.exit(1);
});
