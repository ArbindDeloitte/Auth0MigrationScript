'use strict';
/**
 * export-redis-csv.js
 *
 * Exports all source users from Redis to a CSV file, with their migration status:
 *   confirmed     — email is in migration:success:users SET (imported to Auth0)
 *   manual-review — email is in migration:manual:users SET (could not import)
 *   pending       — email is in neither SET (not yet confirmed in Auth0)
 *
 * Usage:
 *   node export-redis-csv.js
 *   node export-redis-csv.js --out ./output/export.csv
 *   node export-redis-csv.js --status confirmed        (only confirmed users)
 *   node export-redis-csv.js --status manual-review
 *   node export-redis-csv.js --status pending
 */

require('dotenv').config();
const fs   = require('fs');
const path = require('path');
const Redis = require('ioredis');

// ── CLI args ─────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const getArg = (flag) => {
  const i = args.indexOf(flag);
  return i !== -1 && args[i + 1] ? args[i + 1] : null;
};
const outFile    = getArg('--out') || './output/migration-export.csv';
const filterStatus = getArg('--status') || null; // confirmed | manual-review | pending | null (all)

// ── Redis keys ────────────────────────────────────────────────────────────────
const SOURCE_KEY  = process.env.REDIS_SOURCE_KEY || 'migration:source:users';
const SUCCESS_KEY = 'migration:success:users';
const MANUAL_KEY  = 'migration:manual:users';

// ── Config ────────────────────────────────────────────────────────────────────
const BATCH_SIZE  = 1000; // users read from Redis per LRANGE call
const PIPE_SIZE   = 200;  // SISMEMBER pairs piped per round-trip

// ── CSV helpers ───────────────────────────────────────────────────────────────
function csvEscape(val) {
  if (val == null) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function toCSVRow(cols) {
  return cols.map(csvEscape).join(',');
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const redis = new Redis({
    host:                process.env.REDIS_HOST || '127.0.0.1',
    port:                parseInt(process.env.REDIS_PORT || '6379', 10),
    password:            process.env.REDIS_PASSWORD || undefined,
    maxRetriesPerRequest: null,
    enableReadyCheck:    false,
  });

  redis.on('error', (err) => {
    console.error('Redis error:', err.message);
    process.exit(1);
  });

  try {
    // ── 1. Totals ─────────────────────────────────────────────────────────────
    const totalSource   = await redis.llen(SOURCE_KEY);
    const totalConfirmed = await redis.scard(SUCCESS_KEY);
    const totalManual   = await redis.scard(MANUAL_KEY);
    const totalPending  = Math.max(0, totalSource - totalConfirmed - totalManual);

    console.log(`Source users : ${totalSource.toLocaleString()}`);
    console.log(`Confirmed    : ${totalConfirmed.toLocaleString()}`);
    console.log(`Manual review: ${totalManual.toLocaleString()}`);
    console.log(`Pending      : ${totalPending.toLocaleString()}`);
    if (filterStatus) console.log(`Filter       : ${filterStatus}`);
    console.log('');

    // ── 2. Prepare output file ────────────────────────────────────────────────
    const outDir = path.dirname(outFile);
    if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

    const ws = fs.createWriteStream(outFile, { encoding: 'utf8' });
    const header = [
      'email', 'first_name', 'last_name', 'username',
      'language_preference', 'require_email_change', 'status',
    ];
    ws.write(toCSVRow(header) + '\n');

    // ── 3. Stream source list in batches ──────────────────────────────────────
    let written = 0;
    let offset  = 0;

    while (offset < totalSource) {
      const end  = Math.min(offset + BATCH_SIZE - 1, totalSource - 1);
      const raws = await redis.lrange(SOURCE_KEY, offset, end);
      offset += raws.length;

      if (raws.length === 0) break;

      // Pipeline SISMEMBER for both SETs: 2 calls per user
      const pipe = redis.pipeline();
      for (const raw of raws) {
        let email = '';
        try { email = (JSON.parse(raw).email || '').toLowerCase().trim(); } catch {}
        pipe.sismember(SUCCESS_KEY, email);
        pipe.sismember(MANUAL_KEY,  email);
      }
      const results = await pipe.exec();

      // Build rows
      for (let i = 0; i < raws.length; i++) {
        let user;
        try { user = JSON.parse(raws[i]); } catch { continue; }

        const email    = (user.email || '').toLowerCase().trim();
        const inSuccess = results[i * 2]?.[1]     === 1;
        const inManual  = results[i * 2 + 1]?.[1] === 1;

        const status = inSuccess ? 'confirmed'
                     : inManual  ? 'manual-review'
                     : 'pending';

        if (filterStatus && status !== filterStatus) continue;

        ws.write(toCSVRow([
          email,
          user.given_name  || user.first_name  || '',
          user.family_name || user.last_name   || '',
          user.uid || user.username || '',
          user.user_metadata?.language || user.language_preference || '',
          user.app_metadata?.requireEmailChange === true ? 'true' : 'false',
          status,
        ]) + '\n');
        written++;
      }

      const pct = ((offset / totalSource) * 100).toFixed(1);
      process.stdout.write(`\rProcessed ${offset.toLocaleString()} / ${totalSource.toLocaleString()} (${pct}%) — written ${written.toLocaleString()} rows`);
    }

    ws.end();
    await new Promise((res, rej) => { ws.on('finish', res); ws.on('error', rej); });

    console.log(`\n\nExported ${written.toLocaleString()} rows → ${path.resolve(outFile)}`);

  } finally {
    redis.disconnect();
  }
}

main().catch((err) => {
  console.error('Export failed:', err.message);
  process.exit(1);
});
