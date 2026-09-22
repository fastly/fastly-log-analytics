> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `local_compact_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `local_compact_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `local_compact_{service_id}`
- **Category:** Local Storage Compaction & Query Accelerator
- **Purpose:** Compaction of small, fragmented local Parquet files into size-capped consolidated Parquet partitions (hourly, daily, weekly tiers) to maintain high-throughput vectorized DuckDB scan speeds.
- **Why It Runs:** Continuous streaming ingest produces dozens of small files per hour. Unchecked, this causes file descriptor exhaustion, excessive seek latency, and DuckDB scan slowdowns. Local compaction bin-packs small files into optimal chunks (<= 256MB) strictly on local disk without generating expensive cloud FOS API charges.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 1 minute (`minutes=1`).
- **Configurable Overrides:** Environment variables:
  - `LOCAL_COMPACT_INTERVAL_MIN` (default: 1)
  - `LOCAL_COMPACT_DAILY_TIER_DAYS` (default: 1)
  - `LOCAL_COMPACT_WEEKLY_TIER_DAYS` (default: 30)
- **Jitter & Misfire Policy:**
  - Jitter: 10 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Scans local `cache/{bucket}/` and `data/parquet/`, merges small files via DuckDB, writes compacted files, atomic rename. | Local file lock per service partition. Explicitly ALLOWED under `FLA_DEV_NO_CRONS=1` (local-safe). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Scans `ingest_ledger` for hours committed in the last 15 minutes and executes `recompute_touched_hours()` to maintain fresh serving-pod Top-N rollups. Skipped on worker nodes. | Pod-local lock; read-only access to DuckLake via ephemeral connection. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/compact/{service_id}` | Full compaction stats in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Compaction runs locally to keep analyst queries fast. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Benefits transparently from server-side compaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Partition Discovery:** Scans local directory tree for hourly partition directories containing more than `_MIN_FILES_TO_COMPACT` (default: 3).
2. **Sequential Bin-Packing:** Groups files within an hourly partition into size-capped bins (target: <= 256MB) to preserve vectorized execution parallelism.
3. **DuckDB Merge Execution:**
   - Issues `COPY (SELECT * FROM read_parquet([...])) TO 'compacted_temp.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)`.
   - Preserves row order and schema definitions.
4. **Atomic Swap & Unlink:**
   - Atomically renames temporary file to `compacted_YYYY-MM-DD_HH_<uuid>.parquet`.
   - Safely removes the original fragmented input files.
5. **Tier Promotion (Daily & Weekly):**
   - Partitions older than 1 day are consolidated into `daily/daily_YYYY-MM-DD_<uuid>.parquet`.
   - Daily partitions older than 30 days are consolidated into `weekly/weekly_YYYY-WXX_<uuid>.parquet`.
6. **View Cache Notification:** Signals DuckDB connection pool that partition file lists have changed.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Under no circumstances may `local_compact` issue any S3 / FOS network calls.
  - **DuckDB Statements:** All `COPY ... TO` compaction statements are recorded with bytes read, bytes written, and execution duration.
  - **Metrics Recorded:** Compaction events record: `source_files_count`, `compacted_files_count`, `bytes_before`, `bytes_after`, `duration_ms`.
- **Timing & Resource Budgets:**
  - Partition scan: < 50ms.
  - Compaction write throughput: > 100 MB/s.
  - Memory consumption: Strictly capped by DuckDB thread memory limit.
- **Audit Checklist:**
  - Verify zero Class A or Class B FOS API calls in `usage_log.db`.
  - Confirm atomic replacement prevents read errors for concurrent dashboard queries.
  - Verify compacted files do not exceed `_MAX_PARTITION_BYTES` (256MB).

---

## 7. Failure Modes & Recovery Runbooks
- **Disk Full (ENOSPC):** Pre-checks available disk before merge; aborts if free space < 2x target bin size.
- **Process Crash Mid-Merge:** Temporary `.tmp` files are swept on next startup; original source files remain untouched until atomic rename succeeds.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Compaction:** `POST /api/admin/compact/{service_id}`.
- **Inspect Compaction Status:** `GET /api/admin/compaction-status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Generate 10 small synthetic Parquet files in an hourly partition directory.
- [ ] 2. Trigger `POST /api/admin/compact/{service_id}`; confirm HTTP 200.
- [ ] 3. Verify the 10 small files are consolidated into a single size-capped file.
- [ ] 4. Verify `usage_log.db` confirms zero outbound FOS calls were made.
- [ ] 5. Confirm concurrent queries against `/api/dashboard/bundle` succeed without `FileNotFoundError`.
- [ ] 6. Under `FLA_DEV_NO_CRONS=1`, verify `local_compact` is registered and functions normally.
