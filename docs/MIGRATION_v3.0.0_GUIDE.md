# Upgrading from v2.x to v3.0.0

## Overview

v3.0.0 replaces the commit-path storage engine (Apache Iceberg/pyiceberg → DuckDB's **DuckLake**), adds a distributed ingest data plane, and standardizes operational metadata and the DuckLake catalog on **PostgreSQL 16** across all deployment modes (`METADATA_DSN` and `DUCKLAKE_CATALOG`). The design is recorded in [ADR-14](adr/14-ducklake-replacement.md), [ADR-15](adr/15-multi-writer-topology.md), [ADR-16](adr/16-ingest-ledger.md), and [ADR-22](adr/22-postgres-only-metadata.md).

**The upgrade is: tear down logging on v2, upgrade the code, and provision each service fresh.** v3 does not read a v2 Iceberg table, a v2 local cache, or a v2 per-service SQLite file, and it does not migrate them. History collected under v2 is not carried into v3; logging resumes from the moment you provision.

---

## Which deployment applies to you

| | You are here if | What you do |
|---|---|---|
| **Standard** (default) | You run one instance. `DEPLOYMENT_MODE=standard` (with PostgreSQL 16 included in the compose stack for `METADATA_DSN` and `DUCKLAKE_CATALOG`). | Tear down on v2, pull, restart, provision. |
| **High-scale** (opt-in) | You want ingestion to fan out across many workers with ClickHouse. | The same, plus external Postgres + valkey + ClickHouse and the env vars in Path B below. |

---

## Before you upgrade

### 1. Decide whether you need the v2 history

A fresh v3 service starts empty. If you need the v2 data, export what you need while v2 is still running (the Query page, or an extract from your own tooling). v3 offers no import path for it.

### 2. Back up what you want to keep

Back up your `configs/` directory if you want a record of per-service settings (log-field choices, schedules). Teardown deletes the service's local config, so you provision each service again and re-enter the settings in the wizard.

### 3. Tear down each service on v2

Use the Teardown action in the admin UI or the CLI (`uv run python -m backend.provision.cli teardown --service-id <ID> --yes`; see the Teardown section of the README for flags such as `--remove-data`). Teardown removes the Fastly logging endpoint and generated VCL from your service, the analytics-owned CDN service, the FOS access keys, and local state.

### 4. Note what does not carry forward

- **Analyst "independent instance" (Path A) is not available on v3.** It depended on rebuilding the old catalog from a read-only bucket key, and a DuckLake catalog is not bucket-resident. Use live shared-instance access (remote sharing) instead. See [ADR-17](adr/17-analyst-path-a-ducklake.md).
- **The serving tier is single-node.** Ingest scales horizontally; the process that answers API requests does not, because the per-service DuckDB file is process-exclusive. Do not run more than one backend replica. See [ADR-18](adr/18-serving-tier-single-pod.md).

---

## Path A — standard deployment

```bash
git pull                      # or pull the new image
make stack-restart            # or your own restart
```

On startup the PostgreSQL metadata schema applies automatically via `pg_schema.py` upon connecting to `METADATA_DSN`. Then provision each service again from the Provision Wizard (or `python -m backend.provision.cli provision`). A new service creates its DuckLake table during provisioning or on its first commit, with nothing to migrate.

Confirm the app is healthy:

```bash
curl -s 'http://localhost/api/health?deep=1'   # expect "status": "ok"
```

---

## Path B — high-scale deployment (`DEPLOYMENT_MODE=high_scale`)

Only take this path if you actually need ingestion to scale across workers. It requires datastores you provide.

### Prerequisites

- **Postgres** — for the DuckLake catalog and the metadata backend. They may share one instance and even one database (every DuckLake table is `ducklake_`-prefixed and cannot collide with the metadata schema).
- **valkey/Redis** — the Celery broker and the SSE backplane.
- **ClickHouse** — the high-scale analytical engine.

### Configuration

```bash
DEPLOYMENT_MODE=high_scale
DUCKLAKE_CATALOG=postgresql://USER:PASS@HOST:5432/DBNAME
METADATA_DSN=postgresql://USER:PASS@HOST:5432/DBNAME
CELERY_BROKER_URL=redis://HOST:6379/0
```

All four are required for `high_scale` (and `DUCKLAKE_CATALOG` + `METADATA_DSN` are required across all modes per [ADR-22](adr/22-postgres-only-metadata.md)). The backend and every worker **refuse to boot** without them rather than degrading silently — file-based catalogs and SQLite metadata are retired in v3.

The Postgres schema is created at startup, by the backend and by each worker, idempotently and safely when several pods boot at once. `scripts/setup_pg_schema.py` still exists and remains the explicit command for provisioning a database ahead of a deploy.

### Sizing note

`METADATA_PG_POOL_MAX` (default 64) must exceed the process's thread ceiling, because one connection is held per thread for that thread's lifetime. Keep `METADATA_PG_POOL_MAX × (number of processes)` below your server's `max_connections`, and remember DuckLake's own catalog connections draw on the same budget.

### Kubernetes

The Helm chart defaults to `standard` mode. Per [ADR-22](adr/22-postgres-only-metadata.md), any configured `config.ducklakeCatalog` or `secrets.metadataDsn` must be a `postgresql://` DSN; selecting `high_scale` mode without the DSNs fails at `helm template`/`helm install` time with a message naming the missing value. See the chart README for the full value set.

---

## If something goes wrong

**Postgres metadata queries fail with `relation "cron_runs" does not exist`.** The schema was not created. Run `scripts/setup_pg_schema.py` with `METADATA_DSN` set.

**Rolling back to SQLite metadata.** Per ADR-22, v3.0.0 standardizes all metadata and catalogs exclusively on PostgreSQL 16; zero-dependency SQLite is no longer supported in v3. To roll back to SQLite, roll back to v2.x.

**Rolling back to v2.x entirely.** v3 data is not readable by v2. Tear the service down on v3, redeploy the v2.x release, and provision the service again on v2.

---

## What changed that you may notice

- **A cron job was renamed**: `sync_{id}` → `log_discovery_{id}`. Historical `cron_runs` rows under the old name are still read, so history in the Cron UI is not lost.
- **Where committed data lives depends on the mode.** In Standard it is local parquet under `data/services/{service_id}/ducklake/`. In High-scale it is parquet under `s3://{bucket}/{prefix}/ducklake/`. FOS no longer holds an Iceberg table.
- **Retention and snapshot expiry now operate on DuckLake.** If you run a shared catalog, note that snapshot retention is catalog-global rather than per-service: one service's `keep_snapshot_days` applies to every service sharing that catalog.
