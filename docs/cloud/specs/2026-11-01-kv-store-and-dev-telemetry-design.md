# KV Store Cost Optimization & Developer Telemetry Gate Design

**Date:** 2026-11-01
**Status:** Approved

## 1. Goals

1. **Gate performance and usage auditing telemetry behind a developer-only flag (`FLP_DEV_TELEMETRY`)**, keeping production installs lean by default while preserving essential operational and security logs (`admin_audit`, `cron_runs`, `platform_errors`).
2. **Eliminate KV Store outbox (`logq_*`) traffic** by moving `cron_runs` and `ingest_files` from `Tier::Outbox` (KV Store) to `Tier::Cache` (shielded Fastly Core Cache at `iad-va-us`), flushed to Parquet by `log_flush`.
3. **Cache read-mostly control-plane keys (`admin:install`, `svc:<sid>`, `settings:operational_logs`) in Fastly Core Cache** with write-through invalidation to eliminate >90% of baseline KV Class B `GET` operations.
4. **Execute `raw_delete` inline at the end of `coordinator::commit()`** when `delete_after == true` and backend call budget permits, skipping the `ingest:deleteq:<plan_id>` KV lifecycle in the common case.
5. **Attribute telemetry & operational log costs on `/usage` and in the Cost Calculator**, displaying an asterisk/sub-label (`* $X.XX from telemetry`) alongside total incurred and estimated monthly costs so operators can see their true production pipeline cost.

---

## 2. Architecture & Components

### 2.1 Developer Telemetry Gate (`FLP_DEV_TELEMETRY`)
- **Dataset classification**:
  - **Always-on production logs**: `admin_audit`, `cron_runs`, `platform_errors`.
  - **Developer-only telemetry (off by default)**: `fos_usage`, `api_telemetry` (and client-side `browser_query_telemetry` beacons).
  - *Amendment (2026-11-10):* `ingest_files` moved to always-on. It backs the Logs page's Ingestion History tab and the cost-calculator prefill on both deployments, and the unified UI has no developer-telemetry toggle, so gating it left that tab empty on Cloud.
- **Unlock check (`telemetry::dev_telemetry_available(runtime)`)**:
  - Checks `runtime.config("dev_telemetry")` (`"1"` or `"true"`, case-insensitive) with fallback to compile-time `option_env!("FLP_DEV_TELEMETRY")`.
  - `app/infra/provision.py` and `app/tools/release.py` propagate `FLP_DEV_TELEMETRY` from `.env` / environment into the `flp_config` Config Store (`dev_telemetry`) when present.
- **Settings & Runtime behavior**:
  - `OperationalLogSettings` adds `#[serde(default)] pub dev_telemetry_enabled: bool` (default: `false`).
  - `telemetry::effective_dev_telemetry(runtime, settings)` returns `dev_telemetry_available(runtime) && settings.dev_telemetry_enabled`.
  - When `effective_dev_telemetry` is `false`:
    - `write_log_at` drops `WritableLogKind::IngestFiles`, `WritableLogKind::FosUsage`, and `WritableLogKind::ApiTelemetry` before serializing or buffering.
    - `coordinator::commit` and `retention::raw_delete` skip constructing per-file `IngestFileRow` records.
    - `POST /api/telemetry/query` returns `204 No Content` immediately.
  - `GET /api/admin/settings/operational-logs` includes `"dev_telemetry_available": bool` in its JSON response.
  - When `dev_telemetry_available == false`, `/admin` (`AdminOperationalLogging`), `/logs?tab=retention` (`RetentionCleanup`), and `/logs` (`LogsClient`) hide the developer telemetry toggle and dev-only dataset tabs (`ingest_files`, `fos_usage`, `api_telemetry`), showing the 3 always-on datasets (`cron_runs`, `admin_audit`, `platform_errors`).

### 2.2 Core Cache Buffering for `cron_runs` and `ingest_files`
- `LogKind::CronRuns` and `LogKind::IngestFiles` return `Tier::Cache` from `LogKind::tier()`.
- Because `/_tick` and all admin routes execute through `flp-front` at the `iad-va-us` shield POP, `flush_request_buffer` appends `cron_runs` and `ingest_files` to the shielded Core Cache minute buckets (`flp:log:v1:<kind>:<sid>:<epoch_min>:<shard>`) with 0 KV Store operations.
- `log_flush` flushes closed Core Cache minute buckets for all `Tier::Cache` kinds (`cron_runs`, `ingest_files`, `fos_usage`, `api_telemetry`, `platform_errors`) to `processed/ops/<kind>/` Parquet files.
- In `flush_admin_audit`, `kv_list("audit:")` is skipped when `audit:seq` has not advanced past `cursor.admin_audit_seq`.

### 2.3 Core Cache Read-Through / Write-Through for Control-Plane Config Keys
- `app/src/admin/kv.rs` caches `admin:install`, `svc:<sid>`, and `settings:operational_logs` in Core Cache under `flp:kvcache:v1:<key>` with a 300-second TTL (`CacheOverride::ttl(300)`).
- Reads (`admin_get`) check Core Cache first for cacheable config keys; on miss, they read from KV Store and populate Core Cache (including a sentinel for missing keys).
- Writes (`admin_cas`) and deletes (`kv_delete`) on cacheable config keys write through to KV Store and immediately update or purge the corresponding `flp:kvcache:v1:<key>` entry in Core Cache.

### 2.4 Inline `raw_delete` on Ingest Commit
- In `coordinator::commit()`, after `admin_cas("ingest:watermark", ...)` succeeds:
  - When `delete_after == true` and `runtime.remaining_Read_budget() >= 3`, the coordinator calls `storage::delete_objects` inline for the plan's raw keys and `_inflight` shard manifests.
  - If all raw keys and `_inflight` manifests are deleted with zero errors, `ingest:deleteq:<plan_id>` is never written to KV Store.
  - If `delete_after == false`, call budget is insufficient, or `delete_objects` returns any failed keys, the remaining keys are written to `ingest:deleteq:<plan_id>` for the scheduled `raw_delete` job to retry.

### 2.5 Telemetry Cost Attribution on `/usage` and Cost Calculator
- `incurredCost` in `app/web/lib/usage/summary.ts` computes `telemetryUsd` from:
  - Operational log storage (`ops_*` / `cron_runs`, `ingest_files`, `fos_usage`, `admin_audit`, `api_telemetry`, `platform_errors` datasets),
  - Operational log FOS Class A & Class B calls (`cron:log_flush`, `cron:log_compact`, `cron:ops_retention`, `api:logs`, and `ops_*` datasets),
  - Operational log Compute requests and vCPU ms (`log_flush`, `log_compact`, `ops_retention`, `storage_reconcile`, and `/api/logs` / `/api/telemetry` routes).
- `UsageStatCards` displays `* $X.XX from telemetry` under the **Estimated Incurred Cost** headline (with a tooltip breaking down telemetry vs. production pipeline cost).
- `estimate.ts` models v2 plan-batched KV operations and computes `telemetryUsd` so `CostCalculator` also displays `* $X.XX from telemetry` beside the monthly estimate.
