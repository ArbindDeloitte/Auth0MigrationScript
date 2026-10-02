# LADWP — Auth0 CIAM Migration Project

## Project Overview
Migrates ~2 million users from Oracle OUD (Unified Directory) to Auth0 (CIAM) for LADWP.
Reference docs: `CIAM_Auth0_Migration_Project_Plan.xlsx`, `CIAM_Auth0_Migration_Design_v2.docx`.

---

## Project Structure

```
Auth0MigrationScript/
├── CLAUDE.md
├── CIAM_Auth0_Migration_Project_Plan.xlsx
├── CIAM_Auth0_Migration_Design_v2.docx
│
├── insert-data/                            # Loads CSV source data into Redis
│   ├── insert-csv.js                       # CSV → Redis pipeline (validate / dry-run / insert)
│   └── csv-files/                          # Source CSV files (e.g. SampleBulkUpload.csv)
│
├── migration-script/                       # BullMQ migration pipeline
│   ├── env.template                        # All env vars with descriptions
│   └── src/
│       ├── index.js                        # Orchestrator: chunk → queue → monitor → gap-recover
│       ├── config.js                       # Validated config — fails fast on missing env vars
│       ├── redis.js                        # IORedis factory (separate connections per queue/worker)
│       ├── logger.js                       # Winston: JSON to file + pretty console
│       ├── services/
│       │   ├── auth0Service.js             # M2M token + import / status / errors API calls
│       │   ├── redisDataService.js         # Redis List reader + offset checkpoint + chunker
│       │   ├── excelService.js             # Legacy Excel source reader + chunker
│       │   ├── checkpointService.js        # All Redis state (SETs, HASHes, LISTs, counters)
│       │   └── failedUserService.js        # proper-lockfile + ExcelJS atomic write to manual-review.xlsx
│       ├── processors/
│       │   ├── importProcessor.js          # Upload chunk to Auth0 → add to status queue
│       │   ├── statusProcessor.js          # Poll Auth0 → classify errors → retry or escalate
│       │   └── retryProcessor.js           # Increment counter → re-upload or escalate to manual review
│       └── queues/
│           └── index.js                    # BullMQ Queue + Worker definitions
│
└── migration-dashboard/                    # Express.js live monitoring dashboard
    ├── server.js                           # REST API + SSE; reads Redis + manual-review.xlsx
    └── public/index.html                   # Single-page dashboard (Chart.js, no build step)
```

---

## Migration Approach

### Phase 1 — Bulk Migration (~15 days)
1. Export all ~2M users from Oracle OUD via `export-ldif`
2. ETL parses LDIF, maps OUD attributes to Redis JSON format
3. `insert-data/insert-csv.js` loads CSVs into Redis List (`migration:source:users`)
4. Migration script streams from Redis → chunks (≤480 KB) → uploads to Auth0 in parallel
5. Failed users routed to `output/manual-review.xlsx` after 3 retries

### Phase 2 — Delta Import (after change freeze)
- `ldapsearch` with `modifyTimestamp >= <Day 0 snapshot>` for delta records
- Updated users → re-import with `upsert=true`
- New users → import as new records
- Deleted users → block or delete via Auth0 Management API

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Node.js ≥ 18 |
| Job queue | BullMQ v5 (Bull v4 is EOL) |
| Queue backend | Redis via IORedis v5 — separate connections per Queue and Worker |
| Source data | Oracle OUD → CSV → Redis List |
| Auth0 API | Management API, M2M Client Credentials (re-authenticates; no refresh tokens) |
| Manual review output | ExcelJS + proper-lockfile atomic writes → `manual-review.xlsx` |
| Logging | Winston (JSON file + pretty console) |
| Dashboard | Express.js + Chart.js SPA on port 3001 |

---

## Auth0 Hard Limits

| Constraint | Limit | Design Response |
|---|---|---|
| Max file size per import job | 500 KB | Chunks sized to ≤ 480 KB |
| Concurrent import jobs per tenant | 2 (default) | Controlled by `MAX_CONCURRENT_AUTH0_JOBS` |
| Job completion timeout | 2 hours | Status poller caps at 240 × 30 s = 2 h |
| Per-user retry threshold | 3 (configurable) | After 3 failures → `manual-review.xlsx` |
| Username max length | 15 chars (default) | Oversized UIDs skipped at chunk time → manual review |

---

## Full Attribute Mapping

### 1. CSV (OUD source) → Redis

| CSV Column | Redis JSON Field | Transformation |
|---|---|---|
| `UID` | `uid` | Trimmed as-is |
| `MAIL` | `email` | Lowercased; used only when `SUBADDRESSED_EMAIL` is empty |
| `SUBADDRESSED_EMAIL` | `email` | Lowercased; takes priority over `MAIL` when non-empty |
| `GIVENNAME` | `first_name` | Trimmed |
| `SN` | `last_name` | Trimmed |
| `USERPASSWORD` | `password_hash` | Raw LDAP hash string (e.g. `{SSHA512}…`) |
| `OUD_PREFERREDLANGUAGE` | `language_preference` | `"Spanish*"` → `"es"`, everything else → `"en"` |
| *(derived)* | `requireEmailChange` | `true` if `SUBADDRESSED_EMAIL` was non-empty, else `false` |

### 2. Redis → Auth0 import payload (`redisDataService.js`)

| Redis Field | Auth0 Field | Notes |
|---|---|---|
| `uid` | `username` | Routed to manual review if > `AUTH0_USERNAME_MAX_LENGTH` (default 15) |
| `email` | `email` | Required |
| `first_name` | `given_name` | Omitted if blank |
| `last_name` | `family_name` | Omitted if blank |
| `first_name` + `last_name` | `name` | Concatenated full name |
| `password_hash` | `custom_password_hash` | LDAP prefix parsed → `{ algorithm, hash, salt }` object |
| `language_preference` | `user_metadata.language` | `"en"` or `"es"` |
| *(hardcoded)* | `email_verified` | Always `false` |
| `requireEmailChange` | `app_metadata.duplicateEmail` | `true` when email was sub-addressed |
| *(hardcoded)* | `app_metadata.emailChanged` | Always `false` (updated externally post-migration) |

> **Note:** `password_hash` is never written to `manual-review.xlsx` — Auth0 strips it from error
> responses for security. It is preserved in `migration:source:email:index` (Redis HASH) so retry
> jobs can look up the original user object by email.

### 3. Auth0 / pipeline → Manual Review Excel (`manual-review.xlsx`)

| Col # | Header | Source |
|---|---|---|
| 1 | Email | `user.email` |
| 2 | Username (UID) | `user.username` |
| 3 | First Name | `user.given_name` |
| 4 | Last Name | `user.family_name` |
| 5 | Language Preference | `user.user_metadata.language` |
| 6 | Email Verified | `user.email_verified` (stringified) |
| 7 | User Metadata | `user.user_metadata` (JSON string) |
| 8 | App Metadata | `user.app_metadata` (JSON string — includes `duplicateEmail`, `emailChanged`) |
| 9 | Failure Reason | Free-text reason set by the migration script |
| 10 | Added At | ISO timestamp when row was appended |

### LDAP Password Hash Parsing

| LDAP Prefix | Algorithm | Auth0 `custom_password_hash` shape |
|---|---|---|
| `{SHA}` | sha1 | `{ algorithm, hash: { value, encoding: "base64" } }` |
| `{SSHA}` | sha1 | `{ algorithm, hash: { value, encoding }, salt: { value, position: "suffix", encoding } }` |
| `{SHA256}` | sha256 | Same as `{SHA}` |
| `{SSHA256}` | sha256 | Same as `{SSHA}` |
| `{SHA512}` | sha512 | Same as `{SHA}` |
| `{SSHA512}` | sha512 | Same as `{SSHA}` — used in LADWP source data |

---

## Redis Keys Reference

| Key | Type | Purpose |
|---|---|---|
| `migration:source:users` | LIST | Source user records (JSON strings) — never modified |
| `migration:source:offset` | STRING | Stream read offset for resuming |
| `migration:source:email:index` | HASH | `email → full user JSON` — fallback for retries |
| `migration:checkpoint:processed_chunks` | SET | Chunk IDs successfully completed |
| `migration:checkpoint:auth0_jobs` | HASH | `chunkId → auth0JobId` |
| `migration:checkpoint:total_chunks` | STRING | Total chunks created this run |
| `migration:active:auth0_jobs` | SET | Currently running Auth0 job IDs (slot counter) |
| `migration:status` | STRING | JSON `{ status, updatedAt }` — overall migration state |
| `migration:success:users` | SET | Emails of successfully imported users |
| `migration:manual:users` | SET | Emails of users in manual review |
| `migration:manual-review:pending` | LIST | Write buffer: `{user, reason}` JSON — flushed to Excel at startup/completion |
| `migration:retry:staging` | LIST | Users awaiting next retry batch |
| `migration:retry:batch:inflight` | STRING | Counter: batches popped but not yet queued |
| `migration:retry:{email}` | STRING | Per-user retry count |
| `migration:csv:import:{filename}` | STRING | Row offset checkpoint per CSV file (insert-data) |

---

## BullMQ Queues

| Queue name | Concurrency | Retries | Purpose |
|---|---|---|---|
| `auth0-import` | `MAX_CONCURRENT_AUTH0_JOBS` (default 2) | 5, 60 s exp backoff | Upload chunk file to Auth0 |
| `auth0-status` | `MAX_CONCURRENT_AUTH0_JOBS × 2` | 3, 10 s exp backoff | Poll Auth0 job status |
| `auth0-retry-users` | 20 | 1 | Re-upload individual failed users |

---

## Failure Classification (`statusProcessor.js`)

| Auth0 Error Code | Category | Action |
|---|---|---|
| `ALREADY_EXISTS`, `DUPLICATED_USER` | Duplicate | Record as success + add to manual review |
| `ONE_OF_MISSING`, `NON_UNIQUE` | Direct manual review | Skip retry queue → manual review |
| `MAX_LENGTH`, `MISSING_REQUIRED` | Unrecoverable | Skip retry queue → manual review |
| Any other error | Recoverable | Push to `auth0-retry-users` queue |
| Retry count > `MAX_USER_RETRIES` | Exhausted | → manual review |

---

## Dashboard (`migration-dashboard/`)

Express server on **port 3001**. Start with `node server.js` from `migration-dashboard/`.

### Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/stats` | Live KPIs: imported, manual review, queues, rate, ETA |
| GET | `/api/manual-review` | Paginated manual review rows; `?filter=cat1,cat2&search=…&page=&limit=` |
| GET | `/api/manual-review/summary` | Category + language breakdown for charts and filter pills |
| GET | `/api/source-records` | Paginated source users from Redis |
| POST | `/api/reset` | Wipe all Redis migration keys; optional `drainQueues` body param |
| GET | `/api/events` | SSE stream — pushes stats every 10 s |

### Manual Review filter categories (failure reason classification)

| Category key | Matches |
|---|---|
| `exceeded-retries` | `/exceeded.*retr\|max.*retr/i` |
| `duplicate` | `/DUPLICATED_USER\|ALREADY_EXISTS/i` |
| `username-length` | `/MAX_LENGTH\|username.*long\|too.*long/i` |
| `missing-field` | `/ONE_OF_MISSING\|MISSING_REQUIRED\|NON_UNIQUE/i` |
| `other` | anything else |

---

## Running the Migration Script

```bash
# Install dependencies
cd migration-script && npm install

# First run or intentional reset (wipes all Redis checkpoint keys)
node src/index.js --fresh

# Resume after interruption
node src/index.js

# Verify-only mode (gap check, no new imports)
node src/index.js --verify

# Monitor logs
tail -f output/logs/migration.log
```

## Loading Source Data (insert-data)

```bash
cd insert-data && npm install

# Validate CSVs — reports data quality issues
node insert-csv.js --validate

# Dry run — shows row counts and ETA without writing to Redis
node insert-csv.js --dry-run

# Insert into Redis
node insert-csv.js

# Reset CSV checkpoints (re-insert same files)
node insert-csv.js --reset

# Reset checkpoints AND clear Redis source list
node insert-csv.js --reset-all
```

## Running the Dashboard

```bash
cd migration-dashboard && npm install
node server.js
# Open http://localhost:3001
```

---

## Required Environment Variables

See `migration-script/env.template` for full list.

| Variable | Default | Description |
|---|---|---|
| `AUTH0_DOMAIN` | *(required)* | Auth0 tenant domain |
| `AUTH0_MGMT_CLIENT_ID` | *(required)* | M2M client ID |
| `AUTH0_MGMT_API_KEY` | *(required)* | M2M client secret |
| `AUTH0_CONNECTION_ID` | *(required)* | Database connection ID |
| `REDIS_HOST` | `127.0.0.1` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | *(none)* | Redis auth password |
| `REDIS_SOURCE_KEY` | `migration:source:users` | Source list key |
| `MAX_CONCURRENT_AUTH0_JOBS` | `2` | Parallel Auth0 import slots |
| `MAX_USER_RETRIES` | `3` | Failures before manual review |
| `STATUS_POLL_INTERVAL_MS` | `30000` | Auth0 job poll interval |
| `STATUS_POLL_MAX_ATTEMPTS` | `240` | Max polls per job (240 × 30 s = 2 h) |
| `AUTH0_IMPORT_UPSERT` | `false` | `true` = overwrite existing users silently |
| `AUTH0_USERNAME_MAX_LENGTH` | `15` | UIDs longer than this → manual review |
| `OUTPUT_DIR` | `./output` | Root output directory |
| `MANUAL_REVIEW_FILE` | `./output/manual-review.xlsx` | Manual review Excel path |

---

## Output Files

| Path | Description |
|---|---|
| `output/chunks/chunk-{n}-{uuid}.json` | Auth0 import chunk files (≤ 480 KB) |
| `output/logs/migration.log` | Full Winston log (JSON) |
| `output/logs/error.log` | Errors only |
| `output/manual-review.xlsx` | Users requiring manual action — open directly in Excel |
| `insert-data/output/invalid-rows.csv` | Validation failures from `--validate` mode |

---

## Key Design Decisions

1. **Redis for all checkpoint state** — already required by BullMQ; O(1) SET operations; atomic
2. **Disk-based chunk files** — enables safe resume; re-read on job failure; no memory pressure
3. **Polling, not webhooks** — Auth0 import jobs have no webhook support
4. **Retry at user level, not chunk level** — avoids duplicate imports; uses `GET /api/v2/jobs/{id}/errors`
5. **Atomic Excel writes** — `proper-lockfile` + write-to-tmp-then-rename; handles EBUSY on Windows
6. **Durable manual-review buffer** — Redis LIST (`migration:manual-review:pending`) buffers Excel writes; flushed at startup and completion so no entries are lost on crash
7. **Source email index** — Redis HASH preserves full user JSON (including `custom_password_hash`) keyed by email; used as fallback when chunk files are unavailable
8. **Concurrency as env var** — `MAX_CONCURRENT_AUTH0_JOBS` — no deployment needed to scale
9. **Separate Redis connections per Queue/Worker** — prevents `BLPOP` blocking the shared connection
10. **Language normalisation at insert time** — `OUD_PREFERREDLANGUAGE` → `"es"` (Spanish*) or `"en"` (all others) in `insert-csv.js` before Redis write
11. **`app_metadata` always written** — every Auth0 user gets `{ duplicateEmail: bool, emailChanged: false }`; `duplicateEmail: true` flags users whose email was sub-addressed in OUD

---

## Key Constraints

- Redis AOF persistence required (`appendonly yes`, `appendfsync everysec`) — Redis loss = checkpoint loss
- Never delete Redis keys manually mid-migration — use `--fresh` flag intentionally
- `AUTH0_IMPORT_UPSERT=false` by default — existing users surface as `ALREADY_EXISTS` for accurate tracking; set `true` only if overwriting metadata is acceptable
- `manual-review.xlsx` must be closed in Excel before the migration script runs — otherwise the atomic rename fails with `EBUSY` (script retries 5× / 3 s each)
