# LAWPD — Auth0 CIAM Migration Project

## Project Overview
This project handles the migration of ~2 million users from Oracle OUD (Unified Directory) to Auth0 (Customer Identity and Access Management / CIAM) for LAWPD.
The project plan is documented in `CIAM_Auth0_Migration_Project_Plan.xlsx`.

## Project Structure
```
LAWPD/
├── CLAUDE.md                               # This file
├── CIAM_Auth0_Migration_Project_Plan.xlsx  # Migration project plan
├── CIAM_Auth0_Migration_Design_v2.docx     # Architecture design document (Word)
└── migration-script/                       # BullMQ-based user migration pipeline
    ├── package.json
    ├── env.template                         # All env vars with descriptions
    └── src/
        ├── index.js                         # Orchestrator: stream → chunk → queue → monitor
        ├── config.js                        # Validated config — fails fast on missing env vars
        ├── redis.js                         # IORedis factory (separate connections per queue/worker)
        ├── logger.js                        # Winston: JSON to file + pretty console
        ├── services/
        │   ├── auth0Service.js              # M2M token refresh + import/status/errors API calls
        │   ├── redisDataService.js          # Redis List reader + offset checkpoint + chunker
        │   ├── checkpointService.js         # SADD / SMEMBERS / HSET progress state
        │   └── failedUserService.js         # proper-lockfile + ExcelJS atomic write
        ├── processors/
        │   ├── importProcessor.js           # Upload chunk to Auth0 → add to status queue
        │   ├── statusProcessor.js           # Poll Auth0 → extract errors → handle timeout
        │   └── retryProcessor.js            # Increment counter → re-upload or escalate
        └── queues/
            └── index.js                     # Queue + Worker definitions with separate Redis connections
```

## Migration Approach

### Phase 1 — Bulk Migration (~15 days)
- Export all ~2M users from Oracle OUD via `export-ldif` (runs on the OUD server, not over network)
- ETL script parses LDIF, decodes SSHA512 password hashes, maps OUD attributes to Auth0 fields
- Load transformed users as JSON into Redis List (`migration:source:users`)
- Migration script streams from Redis, chunks into ≤480KB files, uploads to Auth0 in parallel

### Phase 2 — Delta Import (Change Freeze)
- OUD is frozen (no new writes after cutover)
- Delta query: `ldapsearch` with `modifyTimestamp >= <Day 0 snapshot timestamp>`
- Three delta categories:
  - **Updated users** — attribute changes within the mapping list → re-import with `upsert=true`
  - **Newly created users** — created after Day 0 snapshot → import as new records
  - **Deleted users** — soft-deleted/tombstone in OUD → block or delete via Auth0 Management API
- Only attributes within the agreed mapping list matter — changes to unmapped OUD attributes have no effect in Auth0

### Delta Timeline Estimation
Compare two LDIF snapshots at least 7 days apart, filter for mapped attribute changes, calculate daily change rate, extrapolate across the 15-day bulk window.

## Tech Stack
- **Runtime:** Node.js ≥18
- **Job Queue:** BullMQ v5 (successor to Bull v4, which is end-of-life)
- **Queue Backend:** Redis (IORedis v5) — separate connections per Queue and Worker
- **Source Directory:** Oracle OUD via LDAP / export-ldif
- **Auth:** Auth0 Management API, M2M Client Credentials flow (no refresh tokens — re-authenticates directly)
- **Output:** manual-review.xlsx (ExcelJS + proper-lockfile atomic writes)
- **Logging:** Winston (JSON to file + pretty console)

## Auth0 Hard Limits
| Constraint | Limit | Design Response |
|---|---|---|
| Max file size per import job | 500 KB | Chunks sized to ≤480 KB |
| Concurrent import jobs per tenant | 2 (default) | Controlled by `MAX_CONCURRENT_AUTH0_JOBS` env var |
| Job completion timeout | 2 hours | Status poller caps at 240 × 30s = 2h |
| Job data TTL | 24 hours | Not reached due to 2h cap |
| Per-user retry threshold | 3 (configurable) | After 3 failures → manual-review.xlsx |

## Source Record → Auth0 Attribute Mapping
Redis source records use these fields (set by the ETL):

| Source Field (Redis JSON) | Auth0 Field | Notes |
|---|---|---|
| `email` | `email` | Required |
| `uid` | `username` | Used as the Auth0 username; must be unique per connection |
| `first_name` | `given_name` | |
| `last_name` | `family_name` | |
| `first_name` + `last_name` | `name` | Concatenated; computed by migration script |
| `password_hash` | `custom_password_hash` | Base64-encoded SHA-512 hash string, or ETL-structured `{ algorithm, hash, salt }` object |
| `language_preference` | `user_metadata.language` | e.g. `"en"`, `"es"` |

### Legacy OUD → ETL mapping (for reference)
| OUD LDAP Attribute | Redis source field |
|---|---|
| `mail` | `email` |
| `givenName` | `first_name` |
| `sn` | `last_name` |
| `uid` | `uid` |
| `userPassword` (SSHA512) | `password_hash` (decoded by ETL) |
| `preferredLanguage` | `language_preference` |

## OUD Admin Confirmations Required
1. Is `modifyTimestamp` enabled and populated on all user entries?
2. Are deletes soft-deleted (tombstone) or hard-deleted?
3. Is there a separate OAM audit log for change tracking?
4. What is the exact date/time the bulk LDIF snapshot was taken?

## Key Design Decisions
1. **Redis for checkpoint** — already required by BullMQ, O(1) SADD/SMEMBERS, atomic
2. **Disk-based chunk files** — enables safe resume, re-read on job failure, no memory pressure
3. **Polling not webhooks** — Auth0 import jobs don't support webhooks
4. **Retry at user level not chunk level** — avoids duplicate imports; uses `GET /api/v2/jobs/{id}/errors`
5. **Atomic Excel writes** — proper-lockfile + write-to-tmp-then-rename
6. **Concurrency as env var** — `MAX_CONCURRENT_AUTH0_JOBS` — no deployment needed to scale
7. **Separate Redis connections per Queue/Worker** — prevents BLPOP blocking shared connection

## Running the Script
```bash
# First run or intentional reset
node src/index.js --fresh

# Resume after any interruption
node src/index.js

# Monitor progress
tail -f output/logs/migration.log
```

## Required Environment Variables
See `migration-script/env.template` for full list. Key vars:
- `AUTH0_DOMAIN`, `AUTH0_MGMT_CLIENT_ID`, `AUTH0_MGMT_API_KEY`, `AUTH0_CONNECTION_ID`
- `MAX_CONCURRENT_AUTH0_JOBS` — default 2
- `REDIS_SOURCE_KEY` — default `migration:source:users`
- `MAX_USER_RETRIES` — default 3
- `STATUS_POLL_INTERVAL_MS` — default 30000

## Output Files
- `output/chunks/` — chunk-{n}-{uuid}.json files
- `output/logs/error.log` and `migration.log`
- `output/manual-review.xlsx` — users that failed 3× — open directly in Excel

## Key Constraints
- Auth0 hard limits drive all architectural decisions — see table above
- Redis AOF persistence required (`appendonly yes`, `appendfsync everysec`) — Redis loss = checkpoint loss
- Never delete Redis keys manually mid-migration — use `--fresh` flag only intentionally
- `upsert: true` on all imports — document this risk; existing users will be overwritten on re-import

## Design Document
Architecture diagrams (Mermaid) published at:
https://claude.ai/code/artifact/50cde968-f653-434b-86c9-275836b0f0d6
