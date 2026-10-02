'use strict';
require('dotenv').config();

const express = require('express');
const Redis   = require('ioredis');
const { Queue } = require('bullmq');
const ExcelJS = require('exceljs');
const path    = require('path');
const fs      = require('fs');

const PORT              = parseInt(process.env.DASHBOARD_PORT || '3001');
const SOURCE_KEY        = process.env.REDIS_SOURCE_KEY || 'migration:source:users';
const MANUAL_REVIEW_PATH = path.resolve(
  __dirname,
  process.env.MANUAL_REVIEW_FILE || '../migration-script/output/manual-review.xlsx'
);

// ─── Redis ──────────────────────────────────────────────────────────────────
const baseRedisOpts = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
  lazyConnect: true,
};

const redis = new Redis(baseRedisOpts);
redis.on('error', (err) => process.stderr.write(`[Redis] ${err.message}\n`));

// ─── BullMQ queues (read-only monitoring) ───────────────────────────────────
function mkQueue(name) {
  return new Queue(name, { connection: new Redis(baseRedisOpts) });
}

let importQueue, statusQueue, retryQueue;
try {
  importQueue = mkQueue('auth0-import');
  statusQueue = mkQueue('auth0-status');
  retryQueue  = mkQueue('auth0-retry-users');
} catch (e) {
  process.stderr.write(`[Queues] ${e.message}\n`);
}

async function queueCounts(q) {
  if (!q) return { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 };
  try {
    return await q.getJobCounts('waiting', 'active', 'delayed', 'completed', 'failed');
  } catch {
    return { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 };
  }
}

// ─── Rate sampling (in-memory circular buffer) ──────────────────────────────
const MAX_SAMPLES = 40;
const samples = []; // { t: ms, o: offset }

// Track when offset first became > 0 so we can compute a clean final rate
let migStartTime   = null; // ms timestamp
let migStartOffset = null; // offset value at start
let capturedFinalRate = null; // { upm, ups, durationMs, totalUsers } — set once on completion

function addSample(offset) {
  const o = parseInt(offset || '0');
  const t = Date.now();

  // Record migration start (first tick where offset is > 0)
  if (migStartOffset === null && o > 0) {
    migStartOffset = 0;
    migStartTime   = t;
  }

  samples.push({ t, o });
  if (samples.length > MAX_SAMPLES) samples.shift();
}

// Use a short active window: find the oldest sample within the last 2 min
// whose offset differs from the latest, avoiding stale idle samples.
function currentUPM() {
  if (samples.length < 2) return 0;
  const last = samples[samples.length - 1];

  // Walk backwards to find a sample with a different offset (migration was moving)
  let ref = null;
  const cutoff = last.t - 120_000; // 2-minute window
  for (let i = samples.length - 2; i >= 0; i--) {
    if (samples[i].o !== last.o) {
      ref = samples[i];
      // Prefer a ref within the 2-min window, but take the nearest if none in range
      if (samples[i].t >= cutoff) break;
    }
  }
  if (!ref) return 0;

  const dt = last.t - ref.t;
  if (dt <= 0) return 0;
  return Math.max(0, Math.round(((last.o - ref.o) / dt) * 60_000));
}

// Called when migration reaches a terminal state; freezes the overall rate.
function captureFinalRate(finalOffset) {
  if (capturedFinalRate) return capturedFinalRate;
  if (!migStartTime || !finalOffset) return null;
  const durationMs = Date.now() - migStartTime;
  if (durationMs <= 0) return null;
  const totalUsers = finalOffset - (migStartOffset || 0);
  const upm = Math.round((totalUsers / durationMs) * 60_000);
  const ups = Math.round((totalUsers / durationMs) * 1000 * 10) / 10; // 1 dp
  capturedFinalRate = { upm, ups, durationMs, totalUsers };
  return capturedFinalRate;
}

// ─── Manual-review cache (expensive xlsx read) ──────────────────────────────
let mrCache = { count: 0, ts: 0 };
const MR_TTL = 30_000;

async function getManualReviewCount() {
  if (Date.now() - mrCache.ts < MR_TTL) return mrCache.count;
  if (!fs.existsSync(MANUAL_REVIEW_PATH)) { mrCache = { count: 0, ts: Date.now() }; return 0; }
  try {
    const wb = new ExcelJS.Workbook();
    await wb.xlsx.readFile(MANUAL_REVIEW_PATH);
    const ws = wb.getWorksheet('Manual Review');
    const count = ws ? Math.max(0, ws.rowCount - 1) : 0;
    mrCache = { count, ts: Date.now() };
    return count;
  } catch { return mrCache.count; }
}

// ─── Express app ────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// GET /api/stats — full migration snapshot
app.get('/api/stats', async (req, res) => {
  try {
    const [totalUsers, offset, processedChunks, totalChunks, statusRaw, auth0Jobs,
           importedInAuth0, manualInAuth0, heartbeatRaw] =
      await Promise.all([
        redis.llen(SOURCE_KEY).catch(() => 0),
        redis.get('migration:source:offset').catch(() => '0'),
        redis.scard('migration:checkpoint:processed_chunks').catch(() => 0),
        redis.get('migration:checkpoint:total_chunks').catch(() => '0'),
        redis.get('migration:status').catch(() => null),
        redis.hlen('migration:checkpoint:auth0_jobs').catch(() => 0),
        redis.scard('migration:success:users').catch(() => 0),
        redis.scard('migration:manual:users').catch(() => 0),
        redis.get('migration:heartbeat').catch(() => null),
      ]);

    const offsetNum    = parseInt(offset || '0');
    const totalChunksN = parseInt(totalChunks || '0');
    addSample(offsetNum);

    let statusObj = null;
    try { statusObj = JSON.parse(statusRaw); } catch { /* ignore */ }

    const [ic, sc, rc, manualReviewCount] = await Promise.all([
      queueCounts(importQueue),
      queueCounts(statusQueue),
      queueCounts(retryQueue),
      getManualReviewCount(),
    ]);

    const upm        = currentUPM();
    const remaining  = totalUsers - offsetNum;
    const etaMins    = upm > 0 ? Math.round(remaining / upm) : null;

    const currentStatus = statusObj?.status || 'idle';
    const isTerminal = currentStatus === 'completed' || currentStatus === 'completed_with_manual_review';
    const finalRate  = isTerminal ? captureFinalRate(offsetNum) : null;
    // heartbeat is written every 10s with a 30s TTL by the migration script.
    // If the key is missing or older than 20s the script is not running.
    const heartbeatAge  = heartbeatRaw ? Date.now() - parseInt(heartbeatRaw) : Infinity;
    const scriptRunning = heartbeatAge < 20_000;

    res.json({
      source: {
        total:       totalUsers,
        read:        offsetNum,
        readPercent: totalUsers > 0 ? Math.round((offsetNum / totalUsers) * 1000) / 10 : 0,
      },
      chunks: {
        total:            totalChunksN,
        processed:        processedChunks,
        auth0JobsTracked: auth0Jobs,
        processedPercent: totalChunksN > 0
          ? Math.round((processedChunks / totalChunksN) * 1000) / 10 : 0,
      },
      // importedInAuth0: confirmed in Auth0 (migration:success:users SET)
      // manualInAuth0: escalated to manual review (migration:manual:users SET)
      // pendingCount: source users not yet in either SET
      importedInAuth0,
      importedInAuth0Percent: totalUsers > 0
        ? Math.round((importedInAuth0 / totalUsers) * 1000) / 10 : 0,
      manualInAuth0,
      manualInAuth0Percent: totalUsers > 0
        ? Math.round((manualInAuth0 / totalUsers) * 1000) / 10 : 0,
      pendingCount: Math.max(0, totalUsers - importedInAuth0 - manualInAuth0),
      status:          currentStatus,
      statusUpdatedAt: statusObj?.updatedAt || null,
      scriptRunning,
      manualReviewCount,
      queues: { import: ic, status: sc, retry: rc },
      rate: { usersPerMinute: upm, etaMinutes: etaMins, finalRate },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/users?page=1&limit=20&search=&filter=all|confirmed|manual-review|pending|email-change&sort=&sortDir=asc|desc
app.get('/api/users', async (req, res) => {
  const page    = Math.max(1, parseInt(req.query.page  || '1'));
  const limit   = Math.min(100, Math.max(1, parseInt(req.query.limit || '20')));
  const search  = (req.query.search  || '').toLowerCase().trim();
  const filter  = req.query.filter   || 'all';
  const sort    = req.query.sort     || '';   // email | uid | first_name | last_name | language | email_change | status
  const sortDir = req.query.sortDir  === 'desc' ? 'desc' : 'asc';

  try {
    const total      = await redis.llen(SOURCE_KEY);
    const readOffset = parseInt(await redis.get('migration:source:offset').catch(() => '0') || '0');

    // Fast path only when no search, no filter, and no sort.
    // Pipeline-check both the success SET and the manual SET for each user on the page.
    if (!search && filter === 'all' && !sort) {
      const start    = (page - 1) * limit;
      const rawItems = await redis.lrange(SOURCE_KEY, start, start + limit - 1);
      const users    = rawItems.map((raw, idx) => parseUser(raw, start + idx));

      // Two SISMEMBER calls per user (success + manual), batched into one pipeline.
      const pipe = redis.pipeline();
      for (const u of users) {
        const key = (u.email || '').toLowerCase();
        pipe.sismember('migration:success:users', key);
        pipe.sismember('migration:manual:users',  key);
      }
      const results = await pipe.exec();
      users.forEach((u, i) => {
        u._imported      = results[i * 2]?.[1]     === 1;
        u._manualReview  = results[i * 2 + 1]?.[1] === 1;
      });

      return res.json({
        total, page, limit,
        totalPages: Math.ceil(total / limit),
        users, readOffset,
      });
    }

    // Filtered / searched / sorted path — scan up to 10k records.
    // Load both SETs upfront for O(1) per-user lookups during the scan.
    const [successEmails, manualEmails] = await Promise.all([
      redis.smembers('migration:success:users').catch(() => []),
      redis.smembers('migration:manual:users').catch(() => []),
    ]);
    const successSet = new Set(successEmails.map(e => e.toLowerCase()));
    const manualSet  = new Set(manualEmails.map(e => e.toLowerCase()));

    const SCAN_CAP = 10_000;
    const matches  = [];
    let scanned    = 0;
    const batchSz  = 500;

    while (scanned < SCAN_CAP) {
      const batch = await redis.lrange(SOURCE_KEY, scanned, scanned + batchSz - 1);
      if (!batch.length) break;
      for (let i = 0; i < batch.length; i++) {
        const u   = parseUser(batch[i], scanned + i);
        const key = (u.email || '').toLowerCase();
        u._imported     = key ? successSet.has(key) : false;
        u._manualReview = key ? manualSet.has(key)  : false;
        if (filter === 'confirmed'     && !u._imported)          continue;
        if (filter === 'pending'       && (u._imported || u._manualReview)) continue;
        if (filter === 'manual-review' && !u._manualReview)      continue;
        if (filter === 'email-change'  && !u.requireEmailChange) continue;
        if (search) {
          const hay = [u.email, u.uid, u.first_name, u.last_name, u.language_preference]
            .filter(Boolean).join(' ').toLowerCase();
          if (!hay.includes(search)) continue;
        }
        matches.push(u);
      }
      scanned += batch.length;
      if (batch.length < batchSz) break;
    }

    // Apply sort
    if (sort) {
      const dir = sortDir === 'desc' ? -1 : 1;
      matches.sort((a, b) => {
        let av, bv;
        switch (sort) {
          case 'email':        av = (a.email  || '').toLowerCase(); bv = (b.email  || '').toLowerCase(); break;
          case 'uid':          av = (a.uid    || '').toLowerCase(); bv = (b.uid    || '').toLowerCase(); break;
          case 'first_name':   av = (a.first_name || '').toLowerCase(); bv = (b.first_name || '').toLowerCase(); break;
          case 'last_name':    av = (a.last_name  || '').toLowerCase(); bv = (b.last_name  || '').toLowerCase(); break;
          case 'language':     av = (a.language_preference || '').toLowerCase(); bv = (b.language_preference || '').toLowerCase(); break;
          case 'email_change': av = a.requireEmailChange ? 1 : 0; bv = b.requireEmailChange ? 1 : 0; break;
          case 'status':       av = a._imported ? 2 : (a._manualReview ? 1 : 0); bv = b._imported ? 2 : (b._manualReview ? 1 : 0); break;
          default: return 0;
        }
        if (av < bv) return -1 * dir;
        if (av > bv) return  1 * dir;
        return 0;
      });
    }

    const start = (page - 1) * limit;
    res.json({
      total: matches.length,
      page, limit,
      totalPages: Math.ceil(matches.length / limit),
      users:     matches.slice(start, start + limit),
      readOffset,
      searchScanned: scanned,
      searchLimited: scanned >= SCAN_CAP,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

function parseUser(raw, index) {
  try {
    const u = JSON.parse(raw);
    return {
      ...u,
      password_hash: u.password_hash ? '[encrypted]' : null,
      _index:       index,
      _imported:    false,  // set by caller after SISMEMBER check against migration:success:users
      _manualReview: false, // set by caller after SISMEMBER check against migration:manual:users
    };
  } catch {
    return { _index: index, _error: true, _raw: String(raw).slice(0, 60) };
  }
}

// POST /api/users — add a new source record
app.post('/api/users', async (req, res) => {
  const { email, uid, first_name, last_name, password_hash, language_preference, requireEmailChange } = req.body;
  if (!email?.trim()) return res.status(400).json({ error: 'email is required' });
  if (!uid?.trim())   return res.status(400).json({ error: 'uid is required' });

  const record = { email: email.trim(), uid: uid.trim() };
  if (first_name?.trim())         record.first_name         = first_name.trim();
  if (last_name?.trim())          record.last_name          = last_name.trim();
  if (password_hash?.trim())      record.password_hash      = password_hash.trim();
  if (language_preference?.trim()) record.language_preference = language_preference.trim();
  if (requireEmailChange === true) record.requireEmailChange = true;

  try {
    await redis.rpush(SOURCE_KEY, JSON.stringify(record));
    const total = await redis.llen(SOURCE_KEY);
    res.json({ success: true, total, record });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Manual-review helpers ────────────────────────────────────────────────────
// Shared failure-reason categorisation used by both endpoints.
function categoriseReason(reason) {
  const r = String(reason || '');
  if (/exceeded.*retr|max.*retr/i.test(r))                      return 'exceeded-retries';
  if (/DUPLICATED_USER|ALREADY_EXISTS/i.test(r))                 return 'duplicate';
  if (/MAX_LENGTH|username.*long|too.*long/i.test(r))            return 'username-length';
  if (/ONE_OF_MISSING|MISSING_REQUIRED|NON_UNIQUE/i.test(r))     return 'missing-field';
  return 'other';
}

const CATEGORY_LABELS = {
  'exceeded-retries': 'Exceeded Retries',
  'duplicate':        'Duplicate User',
  'username-length':  'Username Too Long',
  'missing-field':    'Missing / Non-Unique Field',
  'other':            'Other',
};

// Read all rows from the xlsx and return them (with category attached).
// Cached for 30 s shared with the existing mrCache TTL.
let mrRowsCache = { rows: null, ts: 0 };

async function readManualReviewRows() {
  if (mrRowsCache.rows && Date.now() - mrRowsCache.ts < MR_TTL) return mrRowsCache.rows;
  if (!fs.existsSync(MANUAL_REVIEW_PATH)) { mrRowsCache = { rows: [], ts: Date.now() }; return []; }
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.readFile(MANUAL_REVIEW_PATH);
  const ws = wb.getWorksheet('Manual Review');
  if (!ws) { mrRowsCache = { rows: [], ts: Date.now() }; return []; }
  const rows = [];
  ws.eachRow((row, rowNum) => {
    if (rowNum === 1) return;
    const v = (n) => { const c = row.getCell(n).value; return c ?? null; };
    const reason = v(9);
    rows.push({
      email:               v(1),
      username:            v(2),
      given_name:          v(3),
      family_name:         v(4),
      language_preference: v(5),
      email_verified:      v(6),
      failure_reason:      reason,
      added_at:            v(10),
      _category:           categoriseReason(reason),
    });
  });
  mrRowsCache = { rows, ts: Date.now() };
  // Keep existing count cache in sync
  mrCache = { count: rows.length, ts: Date.now() };
  return rows;
}

// GET /api/manual-review/summary — aggregated breakdown (must be before the paginated route)
app.get('/api/manual-review/summary', async (req, res) => {
  try {
    const rows = await readManualReviewRows();
    const byCategory = {};
    const byLanguage = {};
    for (const r of rows) {
      byCategory[r._category] = (byCategory[r._category] || 0) + 1;
      const lang = String(r.language_preference || 'Unknown').split('-')[0].trim() || 'Unknown';
      byLanguage[lang] = (byLanguage[lang] || 0) + 1;
    }
    res.json({ total: rows.length, byCategory, byLanguage, categoryLabels: CATEGORY_LABELS });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/manual-review?page=1&limit=20&search=&filter=all
app.get('/api/manual-review', async (req, res) => {
  const page   = Math.max(1, parseInt(req.query.page  || '1'));
  const limit  = Math.min(100, Math.max(1, parseInt(req.query.limit || '20')));
  const search = (req.query.search || '').toLowerCase().trim();
  // filter = 'all' | single cat | comma-separated cats e.g. 'exceeded-retries,duplicate'
  const filterRaw = req.query.filter || 'all';
  const filterCats = filterRaw === 'all' ? null
    : new Set(filterRaw.split(',').map(s => s.trim()).filter(Boolean));

  if (!fs.existsSync(MANUAL_REVIEW_PATH)) {
    return res.json({ total: 0, page, limit, totalPages: 0, users: [], grandTotal: 0 });
  }

  try {
    const allRows = await readManualReviewRows();

    let rows = allRows;
    if (filterCats && filterCats.size > 0) rows = rows.filter(r => filterCats.has(r._category));
    if (search) {
      rows = rows.filter(r => {
        const hay = [r.email, r.username, r.given_name, r.family_name, r.language_preference, r.failure_reason]
          .filter(Boolean).join(' ').toLowerCase();
        return hay.includes(search);
      });
    }

    const start = (page - 1) * limit;
    res.json({
      total:      rows.length,
      grandTotal: allRows.length,
      page, limit,
      totalPages: Math.ceil(rows.length / limit),
      users:      rows.slice(start, start + limit),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events — Server-Sent Events for live updates
const sseClients = new Set();

app.get('/api/events', (req, res) => {
  res.set({
    'Content-Type':  'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection':    'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.flushHeaders();
  res.write(': connected\n\n');
  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

// Push a lightweight heartbeat to SSE clients every 10s
setInterval(async () => {
  if (!sseClients.size) return;
  try {
    const [total, offset, processed, totalChunks, statusRaw, importedInAuth0, manualInAuth0, hbRaw] =
      await Promise.all([
        redis.llen(SOURCE_KEY).catch(() => 0),
        redis.get('migration:source:offset').catch(() => '0'),
        redis.scard('migration:checkpoint:processed_chunks').catch(() => 0),
        redis.get('migration:checkpoint:total_chunks').catch(() => '0'),
        redis.get('migration:status').catch(() => null),
        redis.scard('migration:success:users').catch(() => 0),
        redis.scard('migration:manual:users').catch(() => 0),
        redis.get('migration:heartbeat').catch(() => null),
      ]);

    const offsetNum = parseInt(offset || '0');
    addSample(offsetNum);

    let statusObj2 = null;
    try { statusObj2 = JSON.parse(statusRaw); } catch { /* ignore */ }
    const status = statusObj2?.status || 'idle';
    const isTerminal2 = status === 'completed' || status === 'completed_with_manual_review';
    const sseUPM = currentUPM();
    const sseFinalRate = isTerminal2 ? captureFinalRate(offsetNum) : null;
    const sseScriptRunning = hbRaw ? (Date.now() - parseInt(hbRaw)) < 20_000 : false;

    const payload = JSON.stringify({
      source:  { total, read: offsetNum },
      chunks:  { total: parseInt(totalChunks || '0'), processed },
      status,
      scriptRunning: sseScriptRunning,
      importedInAuth0,
      manualInAuth0,
      pendingCount: Math.max(0, total - importedInAuth0 - manualInAuth0),
      rate:    { usersPerMinute: sseUPM, finalRate: sseFinalRate },
    });

    for (const client of sseClients) {
      try { client.write(`data: ${payload}\n\n`); } catch { sseClients.delete(client); }
    }
  } catch { /* ignore */ }
}, 10_000);

// Keepalive ping every 25s to prevent proxy timeouts
setInterval(() => {
  for (const client of sseClients) {
    try { client.write(': ping\n\n'); } catch { sseClients.delete(client); }
  }
}, 25_000);

// POST /api/reset — wipe all Redis migration state (equivalent to --fresh flag)
app.post('/api/reset', async (req, res) => {
  try {
    // Scan for all per-user retry count keys
    const retryKeys = [];
    let cursor = '0';
    do {
      const [next, keys] = await redis.scan(cursor, 'MATCH', 'migration:retry:*', 'COUNT', 100);
      retryKeys.push(...keys);
      cursor = next;
    } while (cursor !== '0');

    const pipe = redis.pipeline();
    pipe.del('migration:checkpoint:processed_chunks');
    pipe.del('migration:checkpoint:auth0_jobs');
    pipe.del('migration:active:auth0_jobs');
    pipe.del('migration:status');
    pipe.del('migration:checkpoint:total_chunks');
    pipe.del('migration:retry:staging');
    pipe.del('migration:success:users');
    pipe.del('migration:manual:users');
    pipe.del('migration:retry:batch:inflight');
    pipe.del('migration:manual-review:pending');
    pipe.del('migration:source:email:index');
    if (retryKeys.length > 0) pipe.del(...retryKeys);
    await pipe.exec();

    // Optionally obliterate BullMQ queues
    if (req.body && req.body.drainQueues) {
      await Promise.allSettled([
        importQueue?.obliterate({ force: true }),
        statusQueue?.obliterate({ force: true }),
        retryQueue?.obliterate({ force: true }),
      ]);
    }

    // Reset in-memory rate tracking
    samples.length  = 0;
    migStartTime    = null;
    migStartOffset  = null;
    capturedFinalRate = null;
    mrCache         = { count: 0, ts: 0 };

    res.json({ success: true, retryKeysCleared: retryKeys.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Start ───────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log('');
  console.log('  Auth0 Migration Dashboard');
  console.log(`  http://localhost:${PORT}`);
  console.log('');
  console.log(`  Redis: ${baseRedisOpts.host}:${baseRedisOpts.port}`);
  console.log(`  Source key: ${SOURCE_KEY}`);
  console.log('');
});
