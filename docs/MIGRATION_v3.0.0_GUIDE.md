# Moving from v2 to v3 (tear down and reinstall)

## Overview

v3.0.0 replaces the v2 storage and metadata stack with a single, unified architecture across all deployments: **ClickHouse** for analytical serving, **Fastly Object Storage (FOS) Archive Parquet** as the durable source of truth, a stateless ingest worker (`python -m backend.high_scale.worker`), and **PostgreSQL 16** (`METADATA_DSN`) for operational metadata and the ingest control plane. The design is recorded in [ADR-16](adr/16-ingest-ledger.md), [ADR-21](adr/21-high-scale-serving-architecture.md), and [ADR-22](adr/22-postgres-only-metadata.md).

**There is no upgrade path.** Tear down each service on v2, install v3 fresh, and re-provision. v3 does not read a v2 Iceberg table, a v2 local cache, or a v2 per-service SQLite file, and it does not migrate them. All v2 data is discarded; logging resumes from the moment you provision.

---

## Unified Architecture Across All Deployments

Whether you run on a laptop, a single production Linux VM with Docker Compose, or a multi-node Kubernetes cluster via Helm, v3 runs the same unified stack:

| Topology | How it runs | What you do |
|---|---|---|
| **Laptop / Single-Host Docker Compose** | `docker compose up --build` (or with `-f docker-compose.prod.yml` in production) runs `postgres` (PostgreSQL 16), `clickhouse` (2–4 GiB limit), `worker` (`python -m backend.high_scale.worker`, 2 GiB limit, `HIGH_SCALE_WORKER_THREADS=1`, `HIGH_SCALE_MAX_BATCH_BYTES=6291456`, `HIGH_SCALE_MAX_BATCH_OBJECTS=10`), `backend`, `frontend`, and `caddy`. | Tear down on v2, install v3 fresh, provision. |
| **Kubernetes (Helm)** | `deploy/chart/fastly-log-analytics` + ClickHouse + PostgreSQL 16 with horizontally scaled stateless `worker` pods. | Tear down on v2, deploy the v3 Helm chart with `METADATA_DSN` and ClickHouse configured, provision. |

---

## Before you move

### 1. Decide whether you need the v2 history

A fresh v3 service starts empty. If you need the v2 data, export what you need while v2 is still running (the Query page, or an extract from your own tooling). v3 offers no import path for it.

### 2. Back up what you want to keep

Back up your `configs/` directory if you want a record of per-service settings (log-field choices, schedules). Teardown deletes the service's local config, so you provision each service again and re-enter the settings in the wizard.

### 3. Tear down each service on v2

Use the Teardown action in the admin UI or the CLI (`uv run python -m backend.provision.cli teardown --service-id <ID> --remove-data --yes`; when `--yes` is passed, `--remove-data` is required to also remove local data and metadata rows). Teardown removes the Fastly logging endpoint and generated VCL from your service, the analytics-owned CDN service, the FOS access keys, and local state.

### 4. Note what does not carry forward

- **The analyst "independent instance" (Path A) is removed in v3.** Use live shared-instance access (remote sharing) instead. See [ADR-21](adr/21-high-scale-serving-architecture.md) and [ADR-22](adr/22-postgres-only-metadata.md).
- **SQLite metadata and file-backed local engines are retired.** PostgreSQL 16 (`METADATA_DSN`) and ClickHouse (`CLICKHOUSE_URL`) are used across every environment.

---

## Docker Compose & Manual Deployment

Install v3 from a fresh checkout (or image) and start it as described in the README (`docker compose up --build` for local evaluation, `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build` for single-host production, or `./run.sh` + `uv run python -m backend.high_scale.worker` for manual installs).

### Configuration

```bash
METADATA_DSN=postgresql://USER:PASS@HOST:5432/fla
CLICKHOUSE_URL=http://HOST:8123
CLICKHOUSE_USER=fla
CLICKHOUSE_PASSWORD=fla_local_dev
CLICKHOUSE_DATABASE=fla
```

On startup the PostgreSQL metadata and control-plane schemas apply automatically via `pg_schema.py` and `postgres_control` upon connecting to `METADATA_DSN`, and ClickHouse tables/views bootstrap automatically. `scripts/setup_pg_schema.py` also remains available for provisioning a database ahead of a deploy.

Provision each service again from the Provision Wizard (`token → service → storage → ngwaf → fields → execute`) or `uv run python -m backend.provision.cli provision`.

Confirm the app is healthy:

```bash
curl -s 'http://localhost/api/health?deep=1'   # expect "status": "ok"
```

---

## Sizing note

`METADATA_PG_POOL_MAX` (default 64 on the API backend, 6 on stateless workers) bounds per-process PostgreSQL connections because one connection is held per thread for that thread's lifetime. Keep total connections across backend and worker replicas below your PostgreSQL server's `max_connections`.

## Kubernetes

See [the chart README](../deploy/chart/fastly-log-analytics/README.md) for the Helm value set. The worker deployment defaults to the lightweight pod profile (`1 vCPU / 2 GiB`, `HIGH_SCALE_WORKER_THREADS=1`, `HIGH_SCALE_MAX_BATCH_BYTES=6291456`, `HIGH_SCALE_MAX_BATCH_OBJECTS=10`) and scales horizontally with replica count.

---

## Troubleshooting

**Postgres metadata queries fail with `relation "cron_runs" does not exist`.** The schema was not created. Run `scripts/setup_pg_schema.py` with `METADATA_DSN` set.

**Going back to v2.** v3 data is not readable by v2. Tear the service down on v3, install the v2 release, and provision the service again on v2.

---

## What changed that you may notice

- **A cron job was renamed**: `sync_{id}` → `log_discovery_{id}`. Cron history starts empty on a fresh install.
- **Committed data lives in FOS Archive Parquet and ClickHouse.** Canonical archive batches are stored under `s3://{bucket}/{prefix}/archive/` with ClickHouse (`request_facts`, `rum_vitals`, `rum_errors`) as the analytical serving store. FOS no longer holds an Iceberg table.
