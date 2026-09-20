> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `clickhouse_backup_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `clickhouse_backup_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `clickhouse_backup_{service_id}`
- **Category:** High-Scale Fact Store Durability & Incremental Table Backup
- **Purpose:** Executes native ClickHouse incremental table backups via `BACKUP TABLE ... TO Disk(...)` across high-scale fact tables and minute dimension rollups.
- **Why It Runs:** In `DEPLOYMENT_MODE=high_throughput`, ClickHouse acts as the primary analytical fact store and serving accelerator. While authoritative raw logs reside in FOS, replaying tens of millions of raw requests during disaster recovery takes hours. Incremental table backups create fast-restore snapshots directly on high-performance storage disks, reducing RTO (Recovery Time Objective) from hours to minutes.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 01:00 UTC (`hour=1, minute=0`).
- **Timing Rationale:** Runs before `rollup_compact` (02:00 UTC) and `optimize` (04:00 UTC) when cluster ingestion volume is typically lowest.
- **Configurable Overrides:** `clickhouse.backup_interval_hours` (default: 24).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | Disabled | ClickHouse is not deployed in standard single-pod mode. | N/A |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) or Worker | Connects to ClickHouse cluster via `ClickHouseClient`, issues native backup DDL, records receipts in PostgreSQL manifest. | Non-blocking table snapshots (ClickHouse MergeTree native). |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/clickhouse/backup` | Backup receipts and disk status in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | ClickHouse backup is strictly an infrastructure duty. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side backup daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite & Allowlist Verification:**
   - Confirms `CLICKHOUSE_HOST` is configured.
   - Retrieves table allowlist from `backend.core.clickhouse_client.CLICKHOUSE_HIGH_SCALE_TABLES`:
     - `request_facts`, `high_scale_batch_publications`, `cmcd_projection_facts`
     - Minute dimension tables: `origin_minute_summary`, `network_minute_dimensions`, `security_minute_dimensions`, `performance_minute_dimensions`
     - RUM fact tables: `rum_vitals_facts`, `rum_error_facts`
2. **Incremental Backup Execution:**
   - For each allowlisted table, issues:
     ```sql
     BACKUP TABLE default.{table} TO Disk('backups', '{service_id}/{table}/{backup_id}')
     ```
   - Uses hardlinks under the hood where supported, making subsequent incremental backups instantaneous and consuming zero extra disk space for unmodified parts.
3. **Durability Receipt Generation:**
   - Constructs a `BackupReceipt` containing: `service_id`, `backup_id`, `table_name`, `parts_count`, `compressed_bytes`, `timestamp`.
   - Records receipt in PostgreSQL `clickhouse_backup_receipts`.
4. **Retention Cleanup of Stale Backups:**
   - Queries older backups beyond the retention cutoff (default: 7 days).
   - Issues `DROP BACKUP ...` or cleans disk directory paths safely.
5. **Telemetry & Log Recording:**
   - Records execution status and backup sizes in `cron_runs`.
   - Updates Prometheus metric `app_clickhouse_last_backup_timestamp_seconds`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **ClickHouse Operations:** Every `BACKUP TABLE` query must register via `query_registry.register("ClickHouse", ...)` and appear in the Live Query Monitor.
  - **Postgres Operations:** Receipt storage queries must record duration.
  - **Storage Metrics:** Must log `compressed_bytes_written` and `deduplicated_bytes_saved`.
- **Timing & Resource Budgets:**
  - Incremental backup execution: < 15 seconds per table on live clusters.
  - Overall job execution: < 3 minutes.
- **Audit Checklist:**
  - Confirm that only allowlisted tables are backed up (prevent accidental backing up of temp tables).
  - Verify that backup creation does not increase query latency for concurrent dashboard readers.

---

## 7. Failure Modes & Recovery Runbooks
- **Backup Disk Full:** If disk space < 15%, backup aborts gracefully and logs warning to prevent filling the root partition.
- **Non-Authoritative Durability Rule:** If a ClickHouse backup fails or is corrupted, FOS raw logs remain fully authoritative. Data can always be replayed via `/api/admin/clickhouse/replay`.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Backup:** `POST /api/admin/clickhouse/backup`.
- **Inspect Cluster Status:** `GET /api/admin/clickhouse/status`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Trigger `POST /api/admin/clickhouse/backup`; confirm HTTP 200.
- [ ] 2. Verify in ClickHouse: `system.backups` shows completed backup receipt.
- [ ] 3. Verify `query_registry` records the `BACKUP TABLE` query under engine `ClickHouse`.
- [ ] 4. Confirm in `cron_runs`: status `success` with valid `compressed_bytes`.
- [ ] 5. Verify Prometheus metric `app_clickhouse_last_backup_timestamp_seconds` updates to current time.
