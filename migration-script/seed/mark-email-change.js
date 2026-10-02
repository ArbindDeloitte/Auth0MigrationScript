'use strict';
/**
 * mark-email-change.js
 * Marks requireEmailChange = true on 500 randomly chosen users
 * already in the Redis source list.
 *
 * Run from the migration-script directory:
 *   node seed/mark-email-change.js
 */

require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') });

const Redis = require('ioredis');

const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);
const SOURCE_KEY = process.env.REDIS_SOURCE_KEY || 'migration:source:users';
const TARGET     = parseInt(process.env.MARK_COUNT || '500', 10);

// Fisher-Yates shuffle, returns first `n` elements
function sampleIndices(total, n) {
  const arr = Array.from({ length: total }, (_, i) => i);
  for (let i = total - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr.slice(0, n);
}

async function main() {
  const redis = new Redis({ host: REDIS_HOST, port: REDIS_PORT, password: process.env.REDIS_PASSWORD || undefined, maxRetriesPerRequest: null });

  console.log(`\nConnecting to Redis ${REDIS_HOST}:${REDIS_PORT}...`);
  const total = await redis.llen(SOURCE_KEY);
  console.log(`Total records in Redis: ${total}`);

  if (total === 0) {
    console.error('No records found. Run seed-users.js first.');
    await redis.quit();
    process.exit(1);
  }

  const count  = Math.min(TARGET, total);
  const indices = sampleIndices(total, count);
  console.log(`Marking ${count} users with requireEmailChange = true...\n`);

  let updated = 0;
  let skipped = 0;

  // Process in batches using pipeline to reduce round-trips
  const BATCH = 50;
  for (let b = 0; b < indices.length; b += BATCH) {
    const slice = indices.slice(b, b + BATCH);

    // 1. Read the raw JSON for each index in this batch
    const pipeline = redis.pipeline();
    for (const idx of slice) pipeline.lindex(SOURCE_KEY, idx);
    const reads = await pipeline.exec(); // [[err, value], ...]

    // 2. Parse, patch, write back
    const writePipeline = redis.pipeline();
    for (let i = 0; i < slice.length; i++) {
      const [err, raw] = reads[i];
      if (err || !raw) { skipped++; continue; }
      let record;
      try { record = JSON.parse(raw); } catch { skipped++; continue; }

      if (record.requireEmailChange === true) { skipped++; continue; } // already marked

      record.requireEmailChange = true;
      writePipeline.lset(SOURCE_KEY, slice[i], JSON.stringify(record));
      updated++;
    }
    await writePipeline.exec();

    process.stdout.write(`\r  Processed ${Math.min(b + BATCH, indices.length)} / ${count}`);
  }

  console.log(`\n\n═══════════════════════════════════════════`);
  console.log(`  requireEmailChange = true set on: ${updated} users`);
  console.log(`  Already marked / skipped:          ${skipped}`);
  console.log(`  Total source records:              ${total}`);
  console.log(`═══════════════════════════════════════════\n`);

  await redis.quit();
}

main().catch(err => {
  console.error('\nFailed:', err.message);
  process.exit(1);
});
