# Session Goal: Full Ingestion Pipeline & Cron Automation Audit (Data Substrate Verification)

We are beginning our comprehensive audit of Fastly Log Analytics across both architectures (Standard and High-Scale) and all 4 environments (Local Standard, Local High-Scale, GCE Standard, Elevation High-Scale).

Per our architectural rule **"Data Substrate Precedes Presentation"**, this session focuses exclusively on auditing, verifying, and optimizing our **ingestion pipelines, background automation, and cron jobs** before we audit user-facing pages.

---

### Step 1: Interactive Inquiry & Clarification (Do This First)
Before writing code or running large refactors, read:
- `docs/cron/README.md` (the catalog of all 25 jobs across Standard and High-Scale)
- `Current_Project_State_and_Current_Goals.md`
- `AGENTS.md` (Ingest Pipeline, Architecture, and Traps & Gotchas)
- `scripts/dev/audit_environments.py` (`make audit`, `make audit-watch`)

Then, **ask me clarifying questions** to align on expectations and operational requirements:
1. Clarify how each cron job should ideally behave under both Standard (APScheduler/DuckDB) and High-Scale (Celery/RedBeat/ClickHouse/DuckLake) modes.
2. Discuss expected execution schedules, retry policies, and timeout boundaries for each job.
3. Review any specific retention, compaction, or backfill edge cases you want confirmed.

---

### Step 2: Fresh Look & Best-Practice Research
Take a fresh look at all 25 background jobs and perform best-practice research to confirm:
- Are we running all the proper background jobs in each environment, or are there redundant, obsolete, or missing crons?
- Are jobs properly partitioned between the Standard scheduler (`backend/cron/scheduler.py`) and the High-Scale distributed engine (`backend/high_scale/`, RedBeat, and Celery workers)?
- In High-Scale mode, ensure no cron job attempts write operations on read-only ephemeral serving connections (e.g. resolve any `optimize` or `metadata_sync` read-only connection warnings by delegating them to worker tasks).
- Are compaction bin-packing, DuckLake flush durability (`ducklake_flush_inlined_data`), snapshot expiry, and hourly rollup consolidation operating optimally?

---

### Step 3: End-to-End Environment Audit & Testing
- Run `python3 scripts/dev/audit_environments.py` (or `make audit`) to snapshot all 4 live tiers:
  - **Local Standard** (`http://127.0.0.1:80`, Service: `ZU15BvY2LX7WcEp43T9VwU`)
  - **Local High-Scale** (`http://127.0.0.1:8081`, Service: `qI4D8yXXFYOIpZEMrkJy65`)
  - **GCE Standard** (`http://127.0.0.1:8001`, Service: `cVnu9mYB3Cvmob3lsqjQU3`)
  - **Elevation High-Scale** (`http://127.0.0.1:8002`, Service: `ZEZ4mcAjoSFDTg7tpkDKV2`)
- Inspect the last 24h of cron runs in SQLite/Postgres metadata to ensure zero `error` or `warning` states.
- Confirm active ingestion health: verify streaming ingestion on active services and confirm zero uningested `.gz` backlogs in FOS.

---

### Step 4: Contiguous Fixes, Deployment & Validation
- Implement any required fixes, logging improvements, or cron schedule alignments.
- Follow our contiguous deployment mandate: commit, push to `release/v3.0.0-beta2`, deploy to all 4 environments via `bash scripts/dev/deploy_test_all.sh`, and let the 5-minute real-time audit watch verify that all tiers remain green with zero warnings.
- Update `docs/cron/README.md`, `AGENTS.md`, and `Current_Project_State_and_Current_Goals.md` upon completion.
