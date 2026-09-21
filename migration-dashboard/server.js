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

function addSample(offset) {
  const o = parseInt(offset || '0');
  samples.push({ t: Date.now(), o });
  if (samples.length > MAX_SAMPLES) samples.shift();
}

function currentUPM() {
  if (samples.length < 2) return 0;
  const a = samples[0], b = samples[samples.length - 1];
  const dt = b.t - a.t;
  if (dt <= 0) return 0;
  return Math.max(0, Math.round(((b.o - a.o) / dt) * 60_000));
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
    const [totalUsers, offset, processedChunks, totalChunks, statusRaw, auth0Jobs] =
      await Promise.all([
        redis.llen(SOURCE_KEY).catch(() => 0),
        redis.get('migration:source:offset').catch(() => '0'),
        redis.scard('migration:checkpoint:processed_chunks').catch(() => 0),
        redis.get('migration:checkpoint:total_chunks').catch(() => '0'),
        redis.get('migration:status').catch(() => null),
        redis.hlen('migration:checkpoint:auth0_jobs').catch(() => 0),
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

    const upm       = currentUPM();
    const remaining = totalUsers - offsetNum;
    const etaMins   = upm > 0 ? Math.round(remaining / upm) : null;

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
      status:          statusObj?.status   || 'idle',
      statusUpdatedAt: statusObj?.updatedAt || null,
      manualReviewCount,
      queues: { import: ic, status: sc, retry: rc },
      rate: { usersPerMinute: upm, etaMinutes: etaMins },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/users?page=1&limit=20&search=&filter=all|imported|pending
app.get('/api/users', async (req, res) => {
  const page   = Math.max(1, parseInt(req.query.page  || '1'));
  const limit  = Math.min(100, Math.max(1, parseInt(req.query.limit || '20')));
  const search = (req.query.search || '').toLowerCase().trim();
  const filter = req.query.filter || 'all'; // all | imported | pending

  try {
    const total      = await redis.llen(SOURCE_KEY);
    const readOffset = parseInt(await redis.get('migration:source:offset').catch(() => '0') || '0');

    if (!search && filter === 'all') {
      // Fast direct pagination via LRANGE
      const start    = (page - 1) * limit;
      const rawItems = await redis.lrange(SOURCE_KEY, start, start + limit - 1);
      const users    = rawItems.map((raw, idx) => parseUser(raw, start + idx, readOffset));
      return res.json({
        total, page, limit,
        totalPages: Math.ceil(total / limit),
        users, readOffset,
      });
    }

    // Filtered/searched path — scan up to 10k records
    const SCAN_CAP = 10_000;
    const matches  = [];
    let scanned    = 0;
    const batchSz  = 500;

    while (scanned < SCAN_CAP) {
      const batch = await redis.lrange(SOURCE_KEY, scanned, scanned + batchSz - 1);
      if (!batch.length) break;
      for (let i = 0; i < batch.length; i++) {
        const u = parseUser(batch[i], scanned + i, readOffset);
        if (filter === 'imported' && !u._imported) continue;
        if (filter === 'pending'  &&  u._imported) continue;
        if (search) {
          const hay = [u.email, u.uid, u.first_name, u.last_name, u.language_preference]
            .filter(Boolean).join(' ').toLowerCase();
          if (!hay.includes(search)) continue;
        }
        matches.push(u);
        if (matches.length >= 500) break; // cap results
      }
      scanned += batch.length;
      if (batch.length < batchSz || matches.length >= 500) break;
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

function parseUser(raw, index, readOffset) {
  try {
    const u = JSON.parse(raw);
    return {
      ...u,
      password_hash: u.password_hash ? '[encrypted]' : null,
      _index:    index,
      _imported: index < readOffset,
    };
  } catch {
    return { _index: index, _error: true, _raw: String(raw).slice(0, 60) };
  }
}

// POST /api/users — add a new source record
app.post('/api/users', async (req, res) => {
  const { email, uid, first_name, last_name, password_hash, language_preference } = req.body;
  if (!email?.trim()) return res.status(400).json({ error: 'email is required' });
  if (!uid?.trim())   return res.status(400).json({ error: 'uid is required' });

  const record = { email: email.trim(), uid: uid.trim() };
  if (first_name?.trim())         record.first_name         = first_name.trim();
  if (last_name?.trim())          record.last_name          = last_name.trim();
  if (password_hash?.trim())      record.password_hash      = password_hash.trim();
  if (language_preference?.trim()) record.language_preference = language_preference.trim();

  try {
    await redis.rpush(SOURCE_KEY, JSON.stringify(record));
    const total = await redis.llen(SOURCE_KEY);
    res.json({ success: true, total, record });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/manual-review?page=1&limit=20
app.get('/api/manual-review', async (req, res) => {
  const page  = Math.max(1, parseInt(req.query.page  || '1'));
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit || '20')));

  if (!fs.existsSync(MANUAL_REVIEW_PATH)) {
    return res.json({ total: 0, page, limit, totalPages: 0, users: [] });
  }

  try {
    const wb = new ExcelJS.Workbook();
    await wb.xlsx.readFile(MANUAL_REVIEW_PATH);
    const ws = wb.getWorksheet('Manual Review');
    if (!ws) return res.json({ total: 0, page, limit, totalPages: 0, users: [] });

    const rows = [];
    ws.eachRow((row, rowNum) => {
      if (rowNum === 1) return;
      const v = (n) => { const c = row.getCell(n).value; return c ?? null; };
      rows.push({
        email:               v(1),
        username:            v(2),
        given_name:          v(3),
        family_name:         v(4),
        language_preference: v(5),
        email_verified:      v(6),
        failure_reason:      v(9),
        added_at:            v(10),
      });
    });

    const start = (page - 1) * limit;
    res.json({
      total: rows.length,
      page, limit,
      totalPages: Math.ceil(rows.length / limit),
      users: rows.slice(start, start + limit),
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
    const [total, offset, processed, totalChunks, statusRaw] = await Promise.all([
      redis.llen(SOURCE_KEY).catch(() => 0),
      redis.get('migration:source:offset').catch(() => '0'),
      redis.scard('migration:checkpoint:processed_chunks').catch(() => 0),
      redis.get('migration:checkpoint:total_chunks').catch(() => '0'),
      redis.get('migration:status').catch(() => null),
    ]);

    const offsetNum = parseInt(offset || '0');
    addSample(offsetNum);

    let status = 'idle';
    try { status = JSON.parse(statusRaw)?.status || 'idle'; } catch { /* ignore */ }

    const payload = JSON.stringify({
      source:  { total, read: offsetNum },
      chunks:  { total: parseInt(totalChunks || '0'), processed },
      status,
      rate:    { usersPerMinute: currentUPM() },
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
