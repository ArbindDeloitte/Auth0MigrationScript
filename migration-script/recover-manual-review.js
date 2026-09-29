'use strict';
/**
 * recover-manual-review.js
 *
 * One-time recovery: reads all retry-batch chunk files, identifies users whose
 * Redis retry counter is >= 4 (exceeded maxRetries=3), and writes them to the
 * manual-review Excel file using the FIXED failedUserService.
 *
 * Run from the migration-script directory AFTER stopping the migration script:
 *   node recover-manual-review.js
 */
require('dotenv').config();

const fs    = require('fs');
const path  = require('path');
const Redis = require('ioredis');
const ExcelJS = require('exceljs');

const CHUNKS_DIR        = path.resolve(__dirname, './output/chunks');
const MANUAL_REVIEW_FILE = path.resolve(__dirname, './output/manual-review.xlsx');
const MAX_RETRIES       = parseInt(process.env.MAX_USER_RETRIES || '3', 10);

const COLUMNS = [
  { header: 'Email',               key: 'email',               width: 36 },
  { header: 'Username (UID)',       key: 'username',            width: 20 },
  { header: 'First Name',          key: 'given_name',           width: 18 },
  { header: 'Last Name',           key: 'family_name',          width: 18 },
  { header: 'Language Preference', key: 'language_preference',  width: 20 },
  { header: 'Email Verified',      key: 'email_verified',       width: 14 },
  { header: 'User Metadata',       key: 'user_metadata',        width: 40 },
  { header: 'App Metadata',        key: 'app_metadata',         width: 40 },
  { header: 'Failure Reason',      key: '_failureReason',       width: 50 },
  { header: 'Added At',            key: '_addedAt',             width: 22 },
];

async function main() {
  const redis = new Redis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    password: process.env.REDIS_PASSWORD || undefined,
    maxRetriesPerRequest: 3,
  });

  console.log('\n[1/4] Scanning retry-batch chunk files...');
  const files = fs.readdirSync(CHUNKS_DIR).filter(f => f.startsWith('retry-batch-') && f.endsWith('.json'));
  console.log(`  Found ${files.length} retry-batch files`);

  // Collect unique users by email (latest occurrence wins)
  const usersByEmail = new Map();
  for (const file of files) {
    const raw = fs.readFileSync(path.join(CHUNKS_DIR, file), 'utf-8');
    let users;
    try { users = JSON.parse(raw); } catch { console.warn(`  Skipping unreadable file: ${file}`); continue; }
    for (const user of users) {
      const key = (user.email || '').toLowerCase();
      if (key) usersByEmail.set(key, user);
    }
  }
  console.log(`  Unique users across all batch files: ${usersByEmail.size}`);

  console.log(`\n[2/4] Checking Redis retry counters (threshold > ${MAX_RETRIES})...`);
  const failed = [];
  const emailList = [...usersByEmail.keys()];

  // Batch the Redis lookups in groups of 500
  const BATCH = 500;
  for (let i = 0; i < emailList.length; i += BATCH) {
    const batch = emailList.slice(i, i + BATCH);
    const redisKeys = batch.map(e => `migration:retry:${e}`);
    const vals = await redis.mget(...redisKeys);
    batch.forEach((email, j) => {
      const count = parseInt(vals[j] || '0', 10);
      if (count > MAX_RETRIES) {
        failed.push(usersByEmail.get(email));
      }
    });
    if (i % 2000 === 0) process.stdout.write(`  Checked ${Math.min(i + BATCH, emailList.length)} / ${emailList.length}\r`);
  }
  console.log(`\n  Users exceeding max retries: ${failed.length}`);

  if (failed.length === 0) {
    console.log('\n  Nothing to recover. Exiting.');
    await redis.quit();
    return;
  }

  console.log('\n[3/4] Writing recovered users to manual-review.xlsx...');

  // Create the file fresh (the existing file has only an empty header from the bug)
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('Manual Review');
  sheet.columns = COLUMNS;
  sheet.getRow(1).font = { bold: true };
  sheet.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9E1F2' } };

  const addedAt = new Date().toISOString();
  const reason  = `Exceeded ${MAX_RETRIES} retries. Last reason: MAX_LENGTH: Error in username property - String is too long, maximum 15`;

  for (const user of failed) {
    sheet.addRow({
      email:               user.email || '',
      username:            user.username || '',
      given_name:          user.given_name || '',
      family_name:         user.family_name || '',
      language_preference: user.user_metadata?.language || '',
      email_verified:      user.email_verified !== undefined ? String(user.email_verified) : '',
      user_metadata:       user.user_metadata ? JSON.stringify(user.user_metadata) : '',
      app_metadata:        user.app_metadata  ? JSON.stringify(user.app_metadata)  : '',
      _failureReason:      reason,
      _addedAt:            addedAt,
    });
  }

  await workbook.xlsx.writeFile(MANUAL_REVIEW_FILE);
  console.log(`  Written ${failed.length} users to ${MANUAL_REVIEW_FILE}`);

  console.log('\n[4/4] Verifying...');
  const verify = new ExcelJS.Workbook();
  await verify.xlsx.readFile(MANUAL_REVIEW_FILE);
  const ws = verify.getWorksheet('Manual Review');
  const rowCount = ws ? Math.max(0, ws.rowCount - 1) : 0;
  console.log(`  Verified rowCount (excluding header): ${rowCount}`);

  await redis.quit();
  console.log('\n  Recovery complete. Restart the dashboard to see results.\n');
}

main().catch(err => {
  console.error('\nFailed:', err.message);
  process.exit(1);
});
