# Upgrading to v3.0.0

## Overview

v3.0.0 replaces the commit-path storage engine (Apache Iceberg/pyiceberg → DuckDB's **DuckLake**), adds a distributed ingest data plane, and adds an optional Postgres metadata backend. The design is recorded in [ADR-14](adr/14-ducklake-replacement.md), [ADR-15](adr/15-multi-writer-topology.md) and [ADR-16](adr/16-ingest-ledger.md).

**For most operators the upgrade is: pull the new code, restart. The app migrates itself.** There is no export step, no downtime window, and no data copy.

Read [Before you upgrade](#before-you-upgrade) first — it is short, and one item can lose you query history if you skip it.

---

## Which path applies to you

| | You are here if | What you do |
|---|---|---|
| **Single node** (default) | You run one instance — the v2.x default. `INGEST_MODE` unset. | Pull, restart. Nothing else. |
| **Scaled ingest** (opt-in) | You want ingestion to fan out across many workers. | Provision Postgres + valkey, set three env vars, restart. |

Single node remains the default and is fully supported. Scaled ingest is opt-in; you are not required to move to it, and **nothing about your v2.x deployment stops working if you don't**.

---

## Before you upgrade

### 1. Take a backup

Back up `data/` (per-service DuckDB files, metadata SQLite, config JSON) and your `configs/` directory. Adoption registers existing parquet **in place** and does not modify or delete your Iceberg data, so a rollback is possible — but take the backup anyway.

### 2. Know what "adoption" will bring across

On first boot, each service's pre-3.0 log history is adopted into DuckLake. The adopter enumerates **the legacy Iceberg table's own data files from object storage** and registers those `s3://` paths directly. Nothing is copied, no egress is incurred, and the operation is idempotent.

It deliberately does **not** union in the local `cache/.../data/` tree: that directory is a byte-for-byte mirror of the same FOS objects the manifests name (`sync_data` writes each one there), so adopting both would double-count every row inside your cache window. Object storage is the source of truth; the local mirror is only a fallback for services whose legacy table is genuinely absent.

### 3. Check your retention settings

No action needed for the vast majority of deployments — this matters only if you changed the defaults.

Adoption reads the Iceberg **table**, so it is not bounded by your local cache. But if you have previously run with `cache_retention_days` shorter than `data_retention_days`, verify your Iceberg table still contains the history you expect before upgrading. Anything already aged out of the table itself cannot be adopted, because it no longer exists.

### 4. Note what does not carry forward

- **Analyst "independent instance" (Path A) is unsupported against a v3 service.** The old catalog was reconstructible from bucket contents alone, which is what let an analyst run a self-sufficient copy with only read-only FOS credentials. A DuckLake catalog is not bucket-resident, so an analyst with only bucket read access will see an empty dashboard. See [ADR-17](adr/17-analyst-path-a-ducklake.md). Path B (live shared instance) is unaffected.
- **The serving tier is single-node.** Ingest scales horizontally; the process that answers API requests does not, because the per-service DuckDB file is process-exclusive. Do not run more than one backend replica. See [ADR-18](adr/18-serving-tier-single-pod.md).

---

## Path A — single node

```bash
git pull                      # or pull the new image
make stack-restart            # or your own restart
```

That is the whole upgrade. On startup:

1. **SQLite metadata migrations apply automatically**, as they always have.
2. **Legacy history is adopted into DuckLake**, once per service, in a background thread. It never blocks startup.

### Verifying it worked

Adoption is recorded as a `ducklake_adopt` row in `cron_runs`, visible in the Cron UI. A successful run looks like:

```json
{"adopted_files": 412, "skipped_files": 0, "rows_adopted": 1840233, "source": "iceberg_table"}
```

A service created **on** v3 has no legacy data, and correctly reports a clean no-op — this is success, not a failure:

```json
{"adopted_files": 0, "skipped_files": 0, "rows_adopted": 0}
```

Then confirm the app is healthy:

```bash
curl -s 'http://localhost/api/health?deep=1'   # expect "status": "ok"
```

---

## Path B — scaled ingest (`INGEST_MODE=celery`)

Only take this path if you actually need ingestion to scale across workers. It requires two datastores you provide.

### Prerequisites

- **Postgres** — for the DuckLake catalog and the metadata backend. They may share one instance and even one database (every DuckLake table is `ducklake_`-prefixed and cannot collide with the metadata schema).
- **valkey/Redis** — the Celery broker and the SSE backplane.

### Configuration

```bash
INGEST_MODE=celery
DUCKLAKE_CATALOG=postgresql://USER:PASS@HOST:5432/DBNAME
METADATA_DSN=postgresql://USER:PASS@HOST:5432/DBNAME
CELERY_BROKER_URL=redis://HOST:6379/0
```

All four are required. The backend and every worker **refuse to boot** without them rather than degrading silently — a file-based catalog cannot serve concurrent worker writers, and per-node SQLite metadata cannot serialize a cron lease across processes.

The Postgres schema is created at startup, by the backend and by each worker, idempotently and safely when several pods boot at once. `scripts/setup_pg_schema.py` still exists and remains the explicit command for provisioning a database ahead of a deploy.

### Sizing note

`METADATA_PG_POOL_MAX` (default 64) must exceed the process's thread ceiling, because one connection is held per thread for that thread's lifetime. Keep `METADATA_PG_POOL_MAX × (number of processes)` below your server's `max_connections`, and remember DuckLake's own catalog connections draw on the same budget.

### Kubernetes

The Helm chart defaults to single-node and installs cleanly with no flags. Selecting celery mode without the DSNs fails at `helm template`/`helm install` time with a message naming the missing value. See the chart README for the full value set.

---

## If something goes wrong

**Adoption failed.** The failure is recorded as an `error` `ducklake_adopt` row in `cron_runs` with the message, and startup continues. It is not latched as complete, so the next restart retries. To retry immediately:

```bash
curl -X POST 'http://localhost/api/admin/ducklake/migrate?service=SERVICE_ID'
```

To opt out of automatic adoption entirely and drive it by hand, set `FLA_SKIP_LEGACY_ADOPTION=1`.

**Adoption is safe to re-run.** It dedupes against files already registered, so a second run adopts nothing and leaves your row count unchanged.

**Postgres metadata queries fail with `relation "cron_runs" does not exist`.** The schema was not created. Run `scripts/setup_pg_schema.py` with `METADATA_DSN` set.

**Rolling back to SQLite metadata.** `scripts/rollback_pg_to_sqlite.py` exists for this. Your per-service SQLite files are not deleted when you move to Postgres — they simply stop being read — so unsetting `METADATA_DSN` reverts the metadata backend.

**Rolling back to v2.x entirely.** Adoption registers parquet in place and never modifies or deletes the Iceberg table, so your v2 data is intact. Restore your `data/` backup and redeploy the previous version.

---

## What changed that you may notice

- **Two cron jobs were renamed**: `sync_{id}` → `log_discovery_{id}` and `commit_{id}` → `log_ingest_{id}`. Historical `cron_runs` rows under the old names are still read, so your history is not lost.
- **Committed data now lands as parquet under `s3://{bucket}/{prefix}/ducklake/`.** The old `iceberg/` prefix is left untouched.
- **Retention and snapshot expiry now operate on DuckLake.** If you run a shared catalog, note that snapshot retention is catalog-global rather than per-service — one service's `keep_snapshot_days` applies to every service sharing that catalog.
