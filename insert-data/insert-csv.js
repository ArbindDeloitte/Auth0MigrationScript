'use strict';
require('dotenv').config({ path: require('path').resolve(__dirname, '../migration-script/.env') });

const fs         = require('fs');
const path       = require('path');
const Redis      = require('ioredis');
const { parse }  = require('csv-parse');

// ─── Config ───────────────────────────────────────────────────────────────────
const REDIS_HOST   = process.env.REDIS_HOST       || '127.0.0.1';
const REDIS_PORT   = parseInt(process.env.REDIS_PORT || '6379', 10);
const SOURCE_KEY   = process.env.REDIS_SOURCE_KEY || 'migration:source:users';
const CSV_DIR      = path.resolve(__dirname, 'csv-files');
const OUTPUT_DIR   = path.resolve(__dirname, 'output');
const BATCH_SIZE   = 5000;   // optimised for localhost Redis
const MAX_PARALLEL = 4;      // files processed concurrently

const checkpointKey = (filename) => `migration:csv:import:${filename}`;

// ─── Redis ────────────────────────────────────────────────────────────────────
const redis = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});
redis.on('error', (err) => console.error('[Redis]', err.message));

// ─── Field mapping ────────────────────────────────────────────────────────────
function mapRow(row) {
  const subEmail     = (row.SUBADDRESSED_EMAIL || '').trim();
  const useSubEmail  = subEmail.length > 0;
  const email        = (useSubEmail ? subEmail : row.MAIL).trim().toLowerCase();

  return {
    uid:                 (row.UID                  || '').trim(),
    email,
    first_name:          (row.GIVENNAME            || '').trim(),
    last_name:           (row.SN                   || '').trim(),
    password_hash:       (row.USERPASSWORD         || '').trim(),
    language_preference: (row.OUD_PREFERREDLANGUAGE || '').trim().toLowerCase().startsWith('spanish') ? 'es' : 'en',
    requireEmailChange:  useSubEmail,
  };
}

// ─── CSV helpers ──────────────────────────────────────────────────────────────
function streamCSV(filePath, onRow) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(parse({ columns: true, skip_empty_lines: true, trim: true, relax_column_count: true }))
      .on('data', onRow)
      .on('end', resolve)
      .on('error', reject);
  });
}

// ─── Mode: --validate ─────────────────────────────────────────────────────────
// Scans every row and reports data quality issues. Saves invalid rows to a CSV.
async function runValidate(csvFiles) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const invalidPath = path.join(OUTPUT_DIR, 'invalid-rows.csv');
  const invalidOut  = fs.createWriteStream(invalidPath);
  invalidOut.write('file,row,uid,email,reason\n');

  let totalRows     = 0;
  let validRows     = 0;
  let invalidRows   = 0;
  let emailChanged  = 0;
  const seenUIDs    = new Map(); // uid → first file:row it appeared in

  console.log('\n[ --validate ] Scanning CSV files for data issues...\n');

  for (const filename of csvFiles) {
    let rowIndex = 0;
    console.log(`  Validating: ${filename}`);

    await streamCSV(path.join(CSV_DIR, filename), (row) => {
      rowIndex++;
      totalRows++;
      const record = mapRow(row);
      const issues = [];

      if (!record.uid)   issues.push('missing UID');
      if (!record.email) issues.push('missing email');
      if (record.uid && seenUIDs.has(record.uid)) {
        issues.push(`duplicate UID (first seen in ${seenUIDs.get(record.uid)})`);
      }

      if (issues.length > 0) {
        invalidRows++;
        const reason = issues.join('; ');
        invalidOut.write(`"${filename}",${rowIndex},"${record.uid}","${record.email}","${reason}"\n`);
      } else {
        validRows++;
        if (record.uid) seenUIDs.set(record.uid, `${filename}:${rowIndex}`);
        if (record.requireEmailChange) emailChanged++;
      }
    });

    console.log(`    Rows scanned: ${rowIndex.toLocaleString()}`);
  }

  invalidOut.end();

  console.log('\n═══════════════════════════════════════════');
  console.log('  Validation Report');
  console.log('─────────────────────────────────────────');
  console.log(`  Total rows       : ${totalRows.toLocaleString()}`);
  console.log(`  Valid rows       : ${validRows.toLocaleString()}`);
  console.log(`  Invalid rows     : ${invalidRows.toLocaleString()}${invalidRows > 0 ? ` → see ${invalidPath}` : ''}`);
  console.log(`  requireEmailChange=true : ${emailChanged.toLocaleString()}`);
  console.log('═══════════════════════════════════════════\n');
  console.log(invalidRows > 0
    ? '  Fix the invalid rows before inserting.\n'
    : '  All rows valid. Safe to run: node insert-csv.js --dry-run\n'
  );
}

// ─── Mode: --dry-run ──────────────────────────────────────────────────────────
// Runs the full mapping + batching logic but skips all Redis writes.
// Shows exactly what would be inserted and how long it would take.
async function runDryRun(csvFiles) {
  console.log('\n[ --dry-run ] Parsing without writing to Redis...\n');

  let totalRows    = 0;
  let skippedRows  = 0;
  let emailChanged = 0;
  const start      = Date.now();

  for (const filename of csvFiles) {
    let rowIndex  = 0;
    let fileValid = 0;
    console.log(`  Parsing: ${filename}`);

    await streamCSV(path.join(CSV_DIR, filename), (row) => {
      rowIndex++;
      const record = mapRow(row);
      if (!record.email) { skippedRows++; return; }
      totalRows++;
      fileValid++;
      if (record.requireEmailChange) emailChanged++;
    });

    console.log(`    ${fileValid.toLocaleString()} valid rows`);
  }

  const elapsedSec  = ((Date.now() - start) / 1000).toFixed(1);
  const batchCount  = Math.ceil(totalRows / BATCH_SIZE);
  // Estimate: ~5ms per batch on localhost Redis
  const estInsertMs = batchCount * 5;
  const estSec      = (estInsertMs / 1000).toFixed(0);

  console.log('\n═══════════════════════════════════════════');
  console.log('  Dry-run Summary (no data written)');
  console.log('─────────────────────────────────────────');
  console.log(`  Rows that WOULD be inserted : ${totalRows.toLocaleString()}`);
  console.log(`  Rows that WOULD be skipped  : ${skippedRows.toLocaleString()} (no email)`);
  console.log(`  requireEmailChange=true     : ${emailChanged.toLocaleString()}`);
  console.log(`  Batches (${BATCH_SIZE} rows each)       : ${batchCount.toLocaleString()}`);
  console.log(`  Parse time (this run)       : ${elapsedSec}s`);
  console.log(`  Estimated insert time       : ~${estSec}s`);
  console.log(`  Target Redis key            : ${SOURCE_KEY}`);
  console.log('═══════════════════════════════════════════\n');
  console.log('  Counts look right? Run: node insert-csv.js\n');
}

// ─── Mode: insert ─────────────────────────────────────────────────────────────
async function processFile(filePath, filename) {
  const savedOffset = parseInt(await redis.get(checkpointKey(filename)) || '0', 10);
  if (savedOffset > 0) {
    console.log(`  [${filename}] Resuming from row ${savedOffset.toLocaleString()}`);
  }

  return new Promise((resolve, reject) => {
    let rowIndex      = 0;
    let batch         = [];
    let totalInserted = savedOffset;
    let skipped       = 0;
    let emailChanged  = 0;
    const startTime   = Date.now();
    let lastLog       = Date.now();

    const parser = fs.createReadStream(filePath).pipe(
      parse({ columns: true, skip_empty_lines: true, trim: true, relax_column_count: true })
    );

    parser.on('data', (row) => {
      rowIndex++;

      if (rowIndex <= savedOffset) { skipped++; return; }

      const record = mapRow(row);
      if (!record.email) return; // no email = cannot import into Auth0
      if (record.requireEmailChange) emailChanged++;

      batch.push(JSON.stringify(record));

      if (batch.length >= BATCH_SIZE) {
        parser.pause();
        const toFlush = batch.splice(0, BATCH_SIZE);

        flushBatch(filename, toFlush, rowIndex).then((count) => {
          totalInserted = count;
          // Progress: rows/sec + ETA every 2 seconds
          const now = Date.now();
          if (now - lastLog >= 2000) {
            const elapsed  = (now - startTime) / 1000;
            const rps      = Math.round((totalInserted - savedOffset) / elapsed);
            process.stdout.write(`\r  [${filename}] ${totalInserted.toLocaleString()} rows | ${rps.toLocaleString()} rows/sec    `);
            lastLog = now;
          }
          parser.resume();
        }).catch((err) => parser.destroy(err));
      }
    });

    parser.on('end', async () => {
      try {
        if (batch.length > 0) {
          totalInserted = await flushBatch(filename, batch, rowIndex);
        }
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        process.stdout.write('\n');
        console.log(`  [${filename}] Done — ${totalInserted.toLocaleString()} rows inserted in ${elapsed}s (${emailChanged.toLocaleString()} requireEmailChange)`);
        resolve({ inserted: totalInserted, emailChanged });
      } catch (err) {
        reject(err);
      }
    });

    parser.on('error', reject);
  });
}

async function flushBatch(filename, records, upToRow) {
  const pipe = redis.pipeline();
  pipe.rpush(SOURCE_KEY, ...records);
  pipe.set(checkpointKey(filename), String(upToRow));
  await pipe.exec();
  return parseInt(await redis.get(checkpointKey(filename)) || '0', 10);
}

// Run up to MAX_PARALLEL files concurrently
async function runInsert(csvFiles) {
  console.log(`\nInserting into Redis key: ${SOURCE_KEY}`);
  console.log(`Batch size: ${BATCH_SIZE.toLocaleString()} rows | Parallelism: ${MAX_PARALLEL} files\n`);

  let grandInserted    = 0;
  let grandEmailChange = 0;
  const startTime      = Date.now();

  // Process files in parallel batches of MAX_PARALLEL
  for (let i = 0; i < csvFiles.length; i += MAX_PARALLEL) {
    const batch = csvFiles.slice(i, i + MAX_PARALLEL);
    const results = await Promise.all(
      batch.map(filename => processFile(path.join(CSV_DIR, filename), filename))
    );
    results.forEach(r => {
      grandInserted    += r.inserted;
      grandEmailChange += r.emailChanged;
    });
  }

  const totalSec    = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalInRedis = await redis.llen(SOURCE_KEY);

  console.log('\n═══════════════════════════════════════════');
  console.log('  Insert complete.');
  console.log(`  Rows inserted this run     : ${grandInserted.toLocaleString()}`);
  console.log(`  requireEmailChange=true    : ${grandEmailChange.toLocaleString()}`);
  console.log(`  Total in Redis now         : ${totalInRedis.toLocaleString()}`);
  console.log(`  Time taken                 : ${totalSec}s`);
  console.log(`  Redis key                  : ${SOURCE_KEY}`);
  console.log('═══════════════════════════════════════════\n');
}

// ─── Reset helpers ────────────────────────────────────────────────────────────
async function resetCheckpoints() {
  const keys = [];
  let cursor = '0';
  do {
    const [next, found] = await redis.scan(cursor, 'MATCH', 'migration:csv:import:*', 'COUNT', 100);
    keys.push(...found);
    cursor = next;
  } while (cursor !== '0');

  if (keys.length === 0) {
    console.log('  No CSV checkpoint keys found.');
  } else {
    await redis.del(...keys);
    console.log(`  Deleted ${keys.length} checkpoint key(s):`);
    keys.forEach(k => console.log(`    ${k}`));
  }
}

async function resetSourceList() {
  const before = await redis.llen(SOURCE_KEY);
  await redis.del(SOURCE_KEY);
  console.log(`  Deleted ${SOURCE_KEY} (had ${before.toLocaleString()} entries)`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  const args       = process.argv.slice(2);
  const doValidate = args.includes('--validate');
  const doDryRun   = args.includes('--dry-run');
  const doReset    = args.includes('--reset') || args.includes('--reset-all');
  const doResetAll = args.includes('--reset-all');

  console.log(`\nConnecting to Redis ${REDIS_HOST}:${REDIS_PORT} (key: ${SOURCE_KEY})...`);
  await redis.ping();
  const existingCount = await redis.llen(SOURCE_KEY);
  console.log(`Redis OK — ${SOURCE_KEY} currently has ${existingCount.toLocaleString()} records`);

  // ── Reset mode ──────────────────────────────────────────────────────────────
  if (doReset) {
    console.log('\n' + (doResetAll
      ? '[ --reset-all ] Clearing checkpoints + source list...'
      : '[ --reset     ] Clearing CSV checkpoints only...'
    ));
    await resetCheckpoints();
    if (doResetAll) await resetSourceList();
    console.log('\nReset complete. Re-run without --reset to insert.\n');
    await redis.quit();
    return;
  }

  // ── Discover CSV files ───────────────────────────────────────────────────────
  const csvFiles = fs.readdirSync(CSV_DIR)
    .filter(f => f.toLowerCase().endsWith('.csv'))
    .sort();

  if (csvFiles.length === 0) {
    console.log(`\nNo CSV files found in ${CSV_DIR}`);
    await redis.quit();
    return;
  }

  console.log(`\nFound ${csvFiles.length} CSV file(s): ${csvFiles.join(', ')}`);

  if (doValidate)    await runValidate(csvFiles);
  else if (doDryRun) await runDryRun(csvFiles);
  else               await runInsert(csvFiles);

  await redis.quit();
}

main().catch((err) => {
  console.error('\nFatal error:', err.message);
  process.exit(1);
});
