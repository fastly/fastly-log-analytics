# Fastly Log Analytics — Comprehensive Cron Jobs Reference

This document serves as the authoritative, exhaustive specification for every scheduled background task and cron job in Fastly Log Analytics. It details the scheduling mechanics, architecture behaviors, role permissions, execution pipelines, data safety locks, telemetry capture, and testing/verification procedures across both `DEPLOYMENT_MODE=standard` and `DEPLOYMENT_MODE=high_throughput`.

---

## Table of Contents

1. [Architectural Scheduling Topology](#1-architectural-scheduling-topology)
2. [Roles & Permission Model for Cron Jobs](#2-roles--permission-model-for-cron-jobs)
3. [Complete Job Inventory & Matrix](#3-complete-job-inventory--matrix)
4. [Per-Service Ingest & Data Lifecycle Jobs](#4-per-service-ingest--data-lifecycle-jobs)
   - [4.1 log_discovery_{service_id}](#41-log_discovery_service_id)
   - [4.2 commit_{service_id}](#42-commit_service_id)
   - [4.3 local_compact_{service_id}](#43-local_compact_service_id)
   - [4.4 partial_hour_merge_{service_id}](#44-partial_hour_merge_service_id)
   - [4.5 rollup_heal_{service_id}](#45-rollup_heal_service_id)
   - [4.6 rollup_compact_{service_id}](#46-rollup_compact_service_id)
   - [4.7 optimize_{service_id}](#47-optimize_service_id)
   - [4.8 expire_{service_id}](#48-expire_service_id)
   - [4.9 full_sync_{service_id} (Daily Full Sweep)](#49-full_sync_service_id-daily-full-sweep)
   - [4.10 gap_heal_{service_id}](#410-gap_heal_service_id)
   - [4.11 metadata_cleanup_{service_id}](#411-metadata_cleanup_service_id)
   - [4.12 alerts_evaluation_{service_id}](#412-alerts_evaluation_service_id)
   - [4.13 insights_prewarmer_{service_id}](#413-insights_prewarmer_service_id)
   - [4.14 sync_metadata_{service_id} (Analyst Path A Sync)](#414-sync_metadata_service_id-analyst-path-a-sync)
   - [4.15 ledger_sweep_{service_id} (High-Scale Crash Net)](#415-ledger_sweep_service_id-high-scale-crash-net)
5. [RUM (Real User Monitoring) Ingest Jobs](#5-rum-real-user-monitoring-ingest-jobs)
   - [5.1 rum_sync_{service_id} & rum_commit_{service_id} (Standard)](#51-rum_sync_service_id--rum_commit_service_id-standard)
   - [5.2 rum_discovery_{service_id} & ledger_rum_sweep_{service_id} (High-Scale)](#52-rum_discovery_service_id--ledger_rum_sweep_service_id-high-scale)
6. [Global / System-Wide Scheduled Jobs](#6-global--system-wide-scheduled-jobs)
   - [6.1 metric_snapshot](#61-metric_snapshot)
   - [6.2 rdns_enrichment](#62-rdns_enrichment)
   - [6.3 bot_data_refresh](#63-bot_data_refresh)
   - [6.4 ngwaf_sync_{service_id}](#64-ngwaf_sync_service_id)
   - [6.5 share_audit_purge](#65-share_audit_purge)
   - [6.6 duckdb_recycle](#66-duckdb_recycle)
7. [Observability, Telemetry & Audit Contract for Cron Jobs](#7-observability-telemetry--audit-contract-for-cron-jobs)
8. [Automated Verification & Testing Runbook](#8-automated-verification--testing-runbook)

---

## 1. Architectural Scheduling Topology

Fastly Log Analytics operates under two distinct deployment topologies governed by `DEPLOYMENT_MODE` (see [ADR-14](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/adr/14-ducklake-replacement.md), [ADR-15](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/adr/15-multi-writer-topology.md), and [ADR-16](file:///Users/drew.michael/Projects/fastly-log-analytics/docs/adr/16-ingest-ledger.md)):

### Standard Mode (`DEPLOYMENT_MODE=standard`)
- **Engine:** Python `APScheduler` (`BackgroundScheduler`) runs in-process within the FastAPI web backend.
- **Data Path:** Scheduled jobs directly read and write to the local DuckDB database (`data/services/{id}.duckdb`), the local Parquet buffer (`cache/{bucket}/`), and the local SQLite operational database (`data/services/{id}.metadata.db`).
- **Concurrency:** Thread pool executor within the single web process. Write operations hold per-service file locks.

### High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)
- **Split Scheduler Architecture:**
  1. **RedBeat / Celery Workers:** Distributed jobs that touch FOS (Fastly Object Storage) and the database-backed DuckLake catalog (Postgres) are routed to RedBeat and executed by a Celery worker fleet (`_REDBEAT_JOB_PREFIXES`: `log_discovery_`, `commit_`, `ledger_sweep_`, `full_sync_`, `gap_heal_`, `rum_discovery_`, `ledger_rum_sweep_`).
  2. **Pod-Local APScheduler:** Jobs that read or write pod-local state, caches, or process metrics (`local_compact_`, `partial_hour_merge_`, `rollup_heal_`, `rollup_compact_`, `insights_prewarmer_`, `alerts_evaluation_`, `metric_snapshot`, `duckdb_recycle`) execute on the web pod's in-process APScheduler. They **never** run on Celery workers, preventing multi-process lock contention on local files.

---

## 2. Roles & Permission Model for Cron Jobs

| Role | Execution Capability | Description & Restrictions |
|---|---|---|
| **Admin (`read_write`)** | Full Control (All Jobs) | Runs the complete ingest, compaction, optimization, snapshot expiration, and metadata cleanup pipeline. Owns FOS write operations. |
| **Analyst Path A (JSON Join)** | Read-Only Local Jobs | Runs independent copy of the app with read-only FOS credentials. Ingest/commit/optimize are **disabled**. Runs `sync_metadata_{id}` to pull table state, `local_compact_{id}` to maintain local query speed, and `alerts_evaluation_{id}` locally. |
| **Analyst Path B (Remote Share)** | Zero Cron Execution | Connects over HTTPS to the admin's running process. All cron jobs are executed by the host Admin backend. Analysts observe results via the dashboard with strict read-only guarantees. |

---

## 3. Complete Job Inventory & Matrix

| Job Name / Pattern | Schedule | Standard Mode | High-Scale Mode | Role Scope | Category |
|---|---|---|---|---|---|
| `log_discovery_{id}` | Derived (`log_period // 2`) | APScheduler | RedBeat + Celery | Admin | Raw Ingest |
| `commit_{id}` | Every 5 min (configurable) | APScheduler | RedBeat + Celery | Admin | DuckLake Commit |
| `local_compact_{id}` | Every 2 min | APScheduler | Pod APScheduler | Admin & Analyst A | Local Performance |
| `partial_hour_merge_{id}` | Every 30 sec | APScheduler | Pod APScheduler | Admin & Analyst A | Speed Layer |
| `rollup_heal_{id}` | Hourly at :05 | APScheduler | Pod APScheduler | Admin | Rollup Self-Heal |
| `rollup_compact_{id}` | Daily 02:00 UTC | APScheduler | Pod APScheduler | Admin | Rollup Compaction |
| `optimize_{id}` | Daily 04:00 UTC | APScheduler | RedBeat / Worker | Admin | Durability & Parquet Rewrite |
| `expire_{id}` | Hourly (configurable) | APScheduler | Worker / Pod | Admin | Retention & Expiry |
| `full_sync_{id}` | Daily 03:30 UTC | APScheduler | RedBeat + Celery | Admin | Missing Log Discovery |
| `gap_heal_{id}` | Every 30 min | APScheduler | RedBeat + Celery | Admin | Accounting Self-Heal |
| `metadata_cleanup_{id}` | Daily 03:15 UTC | APScheduler | Pod APScheduler | Admin | SQLite Housekeeping |
| `alerts_evaluation_{id}` | Every `log_period` sec | APScheduler | Pod APScheduler | Admin & Analyst A | Monitoring |
| `insights_prewarmer_{id}` | Every 240 sec | APScheduler | Pod APScheduler | Admin & Analyst A | Cache Prewarming |
| `sync_metadata_{id}` | Every `log_period` sec | APScheduler | N/A | Analyst Path A | Catalog Sync |
| `ledger_sweep_{id}` | Every 15 min | N/A | RedBeat + Celery | Admin | Worker Crash Recovery |
| `rum_sync_{id}` | Every 60 sec | APScheduler | N/A | Admin | RUM Ingest |
| `rum_commit_{id}` | Every 5 min | APScheduler | N/A | Admin | RUM Commit |
| `rum_discovery_{id}` | Every 60 sec | N/A | RedBeat + Celery | Admin | Distributed RUM Ingest |
| `ledger_rum_sweep_{id}` | Every 15 min | N/A | RedBeat + Celery | Admin | RUM Crash Recovery |
| `metric_snapshot` | Every 60 sec | APScheduler | Pod APScheduler | Global / Admin | Host Health & Trends |
| `rdns_enrichment` | Every 5 min | APScheduler | Pod APScheduler | Global / Admin | IP Intelligence |
| `bot_data_refresh` | Daily 02:00 UTC | APScheduler | Pod APScheduler | Global / Admin | Verified Bot Feeds |
| `ngwaf_sync_{id}` | Every 5 min | APScheduler | Pod APScheduler | Admin | Security Bot Signals |
| `share_audit_purge` | Daily 03:45 UTC | APScheduler | Pod APScheduler | Global / Admin | Security Compliance |
| `duckdb_recycle` | Configurable (60m) | APScheduler | Pod APScheduler | Process-Wide | Memory Leak Guard |

---

## 4. Per-Service Ingest & Data Lifecycle Jobs

### 4.1 `log_discovery_{service_id}`
- **Purpose:** Ingests raw `.gz` Fastly logs from FOS into local Parquet buffers (Standard) or registers them in the distributed state ledger (High-Scale).
- **Schedule:** Configurable via `cron_sync.interval_seconds` / `log_period` (default: every `log_period // 2` seconds, minimum 5s).
- **Execution Lifecycle:**
  - **Standard Mode:**
    1. Issues `LIST` on FOS bucket under `raw/request/**/*.gz`.
    2. Filters out files already recorded in SQLite `ingested_files`.
    3. Downloads `.gz` chunks concurrently, decompresses JSON, extracts custom VCL fields, and converts rows into Parquet files in `cache/{bucket}/`.
    4. Updates the session-scoped DuckDB `logs` view to immediately expose newly arrived rows.
    5. Logs ingested filenames to SQLite `ingested_files`.
    6. Triggers throttled post-ingest refresh: `update_top_values` (autocomplete reservoir sample) and `reconcile_fastly_stats` (billing reconciliation) once every 60s.
  - **High-Scale Mode:**
    1. Issues `LIST` on FOS prefix.
    2. Inserts new keys into Postgres `ingest_ledger` with status `discovered`.
    3. Atomically claims batches with `UPDATE ingest_ledger SET status = 'claimed' ... RETURNING ...`.
    4. Dispatches asynchronous Celery conversion tasks (`convert_batch`).
- **Telemetry & Logging:** Stored in `cron_runs` table (`run_id`, `status`, `duration_s`, `files_ingested`, `rows_ingested`, `bytes_ingested`). Section timings tracked in `usage_log.db`.

### 4.2 `commit_{service_id}`
- **Purpose:** Flushes transient Parquet buffer rows into durable long-term storage (DuckLake table).
- **Schedule:** Every `commit_interval_mins` (default 5 min, jitter 30s).
- **Execution Lifecycle:**
  - **Standard Mode:**
    1. Acquires per-service exclusive commit lock.
    2. Identifies all uncommitted Parquet files in `cache/{bucket}/`.
    3. Writes consolidated Parquet data files to FOS `ducklake/` prefix.
    4. Commits transaction to DuckLake metadata catalog.
    5. Unlinks committed local buffer files.
  - **High-Scale Mode:**
    1. Scans `ingest_ledger` for batches in `claimed` state where conversion completed.
    2. Calls `ducklake_merge_adjacent_files` on Postgres DuckLake catalog.
    3. Updates `ingest_ledger` rows to `committed`.
- **Telemetry & Logging:** Stored in `cron_runs` (`committed_files`, `committed_rows`, `bytes_written`).

### 4.3 `local_compact_{service_id}`
- **Purpose:** Local Parquet compaction to maintain blazing-fast DuckDB scan speeds without incurring cloud storage write costs or file fragmentation.
- **Schedule:** Every 2 minutes (jitter 15s).
- **Execution Lifecycle:**
  1. Inspects local hourly directories in `cache/{bucket}/`.
  2. Identifies partitions containing multiple small Parquet files.
  3. Sequentially bin-packs files into compacted chunks capped at `<= 256MB` (`_MAX_PARTITION_BYTES`).
  4. Consolidates partitions older than 1 day into daily files (`daily_YYYY-MM-DD_<uuid>.parquet`).
  5. Consolidates daily files older than 30 days into weekly files (`weekly/`).
  6. Atomically replaces uncompacted files with the new merged files.
- **Safety:** Local filesystem operations only. Completely safe for read-only Analyst Path A instances.

### 4.4 `partial_hour_merge_{service_id}`
- **Purpose:** Top-N rollup speed layer. Incrementally folds newly arrived rows from the active open hour into a pod-local partial-hour rollup file so live Top-N queries scan only the delta since the last tick rather than the entire open hour from scratch.
- **Schedule:** Every 30 seconds (jitter 5s).
- **Execution Lifecycle:**
  1. Scans delta rows added to local buffer / active partition since last run.
  2. Aggregates Top-N dimensions (status, ASN, datacenter, content_type, URLs).
  3. Writes/updates `rollups/{service_id}/partial_hour_*.parquet`.
- **Safety:** Pod-local cache only.

### 4.5 `rollup_heal_{service_id}`
- **Purpose:** Rollup self-heal. Rebuilds Top-N rollup hour bundles for recently closed hours that may have been missed during ingest boundary transitions or server restarts.
- **Schedule:** Hourly at :05 past the hour.
- **Execution Lifecycle:**
  1. Checks closed hours in the last 24h window.
  2. If any hour bundle is missing under `rollups/{service_id}/hourly/`, executes an aggregate query against DuckDB/DuckLake.
  3. Generates the missing hour bundle Parquet file.
- **Telemetry:** Records `cron_runs` entry (`rebuilt_hours`).

### 4.6 `rollup_compact_{service_id}`
- **Purpose:** Consolidates 24 hourly rollup Parquet files for completed days into a single unified daily rollup file (`daily_*.parquet`) for fast 30-day dashboard queries.
- **Schedule:** Daily at 02:00 UTC.
- **Execution Lifecycle:**
  1. Identifies completed days older than UTC today.
  2. Reads all 24 hourly bundle Parquet files for each completed day.
  3. Unions and re-aggregates into `rollups/{service_id}/daily/{date}.parquet`.
  4. Unlinks the redundant hourly files to reclaim disk space.

### 4.7 `optimize_{service_id}`
- **Purpose:** Durability flush and cloud storage file rewrite.
- **Schedule:** Daily at 04:00 UTC.
- **Execution Lifecycle:**
  1. `CALL ducklake_flush_inlined_data('lake')`: Promotes small inlined catalog commits to durable S3 Parquet data files. (Crucial durability requirement: without this step, data stays inlined in catalog metadata only).
  2. `CALL ducklake_rewrite_data_files('lake')`: Rewrites fragmented cloud Parquet files into optimal sizes.
- **Telemetry:** Records `cron_runs` entry with `rewritten_files` and `bytes_saved`.

### 4.8 `expire_{service_id}`
- **Purpose:** Enforces data retention policies and cleans up orphaned cloud snapshots and local caches.
- **Schedule:** Hourly (default `expire_interval_mins = 60`, configurable).
- **Execution Lifecycle:**
  1. **Retention Delete:** `DELETE FROM lake.<table> WHERE timestamp < cutoff` based on `data_retention_days` and `rum_retention_days` (note: `0` means keep forever).
  2. **Snapshot Expiry:** `ducklake_expire_snapshots('lake', older_than => ?)` for `keep_snapshot_days`.
  3. **File Cleanup:** `ducklake_cleanup_old_files('lake', older_than => ?)` unlinks unreferenced Parquet files.
  4. **Filesystem Purge:** Purges local disk cache and rollup files older than `cache_retention_days` and `rollup_retention_months`.

### 4.9 `full_sync_{service_id}` (Daily Full Sweep)
- **Purpose:** Deep recovery sweep. Discovers any late-arriving or skipped log files in FOS that short-interval prefix discovery may have missed.
- **Schedule:** Daily at 03:30 UTC (runs before `optimize` at 04:00 UTC).
- **Execution Lifecycle:**
  1. Issues an unconstrained `LIST` across the raw log prefix in FOS.
  2. Compares keys against `ingested_files` (Standard) or `ingest_ledger` (High-Scale).
  3. Queues missing keys for immediate ingestion.

### 4.10 `gap_heal_{service_id}`
- **Purpose:** Automated gap healing on sustained loss detection.
- **Schedule:** Every 30 minutes (configurable via `cron_gap_heal.interval_minutes`).
- **Execution Lifecycle:**
  1. Computes edge-vs-ingested accounting via `compute_log_accounting` against Fastly `/stats/service`.
  2. If >= 2 consecutive completed hours show a gap >= 5%, triggers `_run_full_sweep`.
  3. Applies adaptive throttling to prevent repeated heavy sweeps.

### 4.11 `metadata_cleanup_{service_id}`
- **Purpose:** Housekeeping for per-service SQLite metadata databases.
- **Schedule:** Daily at 03:15 UTC.
- **Execution Lifecycle:**
  1. Deletes records older than retention thresholds from `usage_log.db` and `metadata.db`.
  2. Purges raw usage calls, ingested file records (> 1d), and old cron runs (> 7d).
  3. Runs SQLite `PRAGMA wal_checkpoint(TRUNCATE)` and `VACUUM` if fragmented.

### 4.12 `alerts_evaluation_{service_id}`
- **Purpose:** Evaluates user-defined alert thresholds against live streaming logs.
- **Schedule:** Every `log_period` seconds (default 60s).
- **Execution Lifecycle:**
  1. Checks if service has active alerts (`_service_has_alerts`). If zero alerts, skips evaluation.
  2. Runs analytical query over recent time window (e.g. 5m) for error rates, status spikes, or latency.
  3. Updates alert state in SQLite (`OK` vs `ALERTING`).
  4. Dispatches webhook or email notifications on state transition.

### 4.13 `insights_prewarmer_{service_id}`
- **Purpose:** Prewarms analytical query caches for the Insights page so operators experience instant page loads.
- **Schedule:** Every 240 seconds (under the 300s cache TTL).
- **Execution Lifecycle:**
  1. Executes default 1h vs 168h baseline difference queries for top dimensions (Status, Datacenter, ASN, URL).
  2. Populates in-memory / disk cache.

### 4.14 `sync_metadata_{service_id}` (Analyst Path A Sync)
- **Purpose:** Pulls updated DuckLake metadata from FOS into the local engine for independent Analyst instances.
- **Schedule:** Every `interval_seconds` (matches log period).
- **Execution Lifecycle:**
  1. Downloads latest `admin_state.json` and catalog state from FOS.
  2. Refreshes local DuckDB table pointers.
  3. Ensures analyst view reflects recent admin commits.

### 4.15 `ledger_sweep_{service_id}` (High-Scale Crash Net)
- **Purpose:** Crash-net recovery for distributed Celery worker tasks.
- **Schedule:** Every 15 minutes.
- **Execution Lifecycle:**
  1. Scans `ingest_ledger` in Postgres for tasks stuck in `claimed` state beyond timeout (e.g. worker pod OOM/evicted).
  2. Reclaims expired claims back to `discovered` or transitions permanently failing files to `quarantine` / `dead_letter`.
  3. Re-dispatches conversion tasks with queue-depth backpressure checks.

---

## 5. RUM (Real User Monitoring) Ingest Jobs

### 5.1 `rum_sync_{service_id}` & `rum_commit_{service_id}` (Standard)
- **Purpose:** Ingests browser Core Web Vitals and JS errors from Fastly RUM log streaming.
- **Schedule:** `rum_sync` every 60s; `rum_commit` every 5 min.
- **Execution Lifecycle:**
  1. `rum_sync` parses incoming gzipped beacon logs from `raw/rum/**/*.gz`.
  2. Extracts CWV metrics (`LCP`, `FID`, `CLS`, `INP`, `TTFB`) and JS exception stacks.
  3. `rum_commit` flushes beacons into DuckLake `client_vitals` and `client_errors` tables.

### 5.2 `rum_discovery_{service_id}` & `ledger_rum_sweep_{service_id}` (High-Scale)
- **Purpose:** High-throughput distributed ledger ingest for high-volume RUM beacons.
- **Schedule:** `rum_discovery` every 60s; `ledger_rum_sweep` every 15 min.
- **Execution Lifecycle:**
  1. Routes beacon discovery through Postgres `ingest_ledger`.
  2. Celery workers parse and batch-insert into DuckLake.
  3. Sweep job recovers stranded beacon batches.

---

## 6. Global / System-Wide Scheduled Jobs

### 6.1 `metric_snapshot`
- **Purpose:** Collects host and process vitals for the Admin Trends tab and System Health sparklines.
- **Schedule:** Every 60 seconds (jitter 5s).
- **Execution Lifecycle:**
  1. Samples CPU usage, RAM utilization, and disk I/O via `psutil`.
  2. Samples DuckDB pool active/idle connections, queue depth, and thread wait times (`app.thread_wait_ms`).
  3. Samples SQLite pool metrics and Celery queue lengths.
  4. Stores data points in SQLite `system_metrics`.

### 6.2 `rdns_enrichment`
- **Purpose:** Asynchronous reverse DNS hostname resolution for top client IPs.
- **Schedule:** Every 5 minutes (jitter 15s).
- **Execution Lifecycle:**
  1. Identifies top unseen client IP addresses across recent logs.
  2. Executes non-blocking async DNS PTR queries.
  3. Stores hostnames in `rdns_cache.db` to power bot identification and ISP analytics.

### 6.3 `bot_data_refresh`
- **Purpose:** Daily update of verified search engine and bot IP ranges.
- **Schedule:** Daily at 02:00 UTC.
- **Execution Lifecycle:**
  1. Fetches published CIDR feeds from Googlebot, Bingbot, Cloudflare, Applebot, etc.
  2. Compiles updated IP lookup radices into `ngwaf_bot_cache.db`.

### 6.4 `ngwaf_sync_{service_id}`
- **Purpose:** Synchronizes Fastly Next-Gen WAF (Signal Sciences) security signals.
- **Schedule:** Every 5 minutes (jitter 15s).
- **Execution Lifecycle:**
  1. Calls Fastly NGWAF API for workspace security events and flagged IP tags.
  2. Updates local NGWAF cache for cross-referencing with CDN access logs.

### 6.5 `share_audit_purge`
- **Purpose:** Purges expired remote-share session tokens, invites, and audit logs.
- **Schedule:** Daily at 03:45 UTC.
- **Execution Lifecycle:**
  1. Queries `remote_share.db`.
  2. Deletes expired sessions and audit logs older than `share_audit_retention_days` (default 90 days).

### 6.6 `duckdb_recycle`
- **Purpose:** Prevents memory leaks caused by DuckDB's C++ parquet metadata cache.
- **Schedule:** Configurable via `DUCKDB_RECYCLE_INTERVAL_MIN` (e.g. every 60-120 minutes).
- **Execution Lifecycle:**
  1. Closes idle pooled DuckDB connections.
  2. Re-initializes fresh connection pool instances with clean memory heaps.

---

## 7. Observability, Telemetry & Audit Contract for Cron Jobs

Every cron job execution must be fully instrumented and observable:

1. **`cron_runs` Table in SQLite:**
   - Every execution records: `run_id`, `service_id`, `job_id`, `status` (`success`, `warning`, `error`), `started_at`, `duration_s`, `details_json`.
   - Result counts (files ingested, rows committed, bytes compacted) must be logged in `details_json`.
2. **`cron_progress` In-Memory Stream:**
   - Long-running jobs (`log_discovery`, `commit`, `full_sync`) emit granular step events exposed via Server-Sent Events (SSE) to the Admin UI.
3. **OpenTelemetry & Structured Logging:**
   - Cron executions run within an OTel root span (`cron.<job_name>`) with tags for `service_id` and execution outcome.
   - Structured JSON logs capture error tracebacks and performance metrics.
4. **Zero Dark Work:**
   - Any database query, FOS call, or external API request executed by a cron job must be instrumented and attributed.

---

## 8. Automated Verification & Testing Runbook

When testing cron jobs in any environment or AI session:

### Verification Checklist:
- [ ] **1. Scheduler Registration:**
  - Verify `scheduler.get_jobs()` or `redbeat_schedule_entries()` lists all expected jobs for configured services.
- [ ] **2. Manual Execution API:**
  - Trigger manual execution via POST endpoints:
    - `POST /api/admin/sync/{service_id}` -> triggers `log_discovery`
    - `POST /api/admin/commit/{service_id}` -> triggers `commit`
    - `POST /api/admin/compact/{service_id}` -> triggers `local_compact`
    - `POST /api/admin/optimize/{service_id}` -> triggers `optimize`
  - Assert HTTP 200 and verify job transitions to `success` in `cron_runs`.
- [ ] **3. Ingestion & Idempotency:**
  - Ingest a batch of test `.gz` logs. Verify row count matches expected records.
  - Re-run ingest on the same bucket prefix. Verify `files_ingested = 0` and duplicate rows = 0.
- [ ] **4. Crash Recovery (High-Scale):**
  - Manually insert a stuck `claimed` row into `ingest_ledger`.
  - Trigger `ledger_sweep_{id}`. Verify status resets to `discovered` or transitions to `quarantine`.
- [ ] **5. Snapshot Expiry & Retention:**
  - Run `expire_{id}` on a test service with historical snapshots.
  - Verify old snapshots are deleted and live table queries continue to return valid data.
