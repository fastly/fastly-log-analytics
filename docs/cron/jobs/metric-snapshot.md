> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `metric_snapshot` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `metric_snapshot`

## 1. Overview & Objectives
- **Job Identifier:** `metric_snapshot`
- **Category:** Host, Runtime & Connection Pool Observability
- **Purpose:** Samples container-level and host-level operational vitals every 60 seconds (CPU utilization, resident memory RSS, disk capacity, DuckDB connection pool saturation, thread wait queue times) and stores them into SQLite `system_metrics.db` to drive the System Health sparklines and Trends tab (`/admin/trends`).
- **Why It Runs:** Real-time visibility into internal application health is essential for diagnosing query bottlenecks, memory growth, and disk exhaustion. Without high-frequency sampling, resource contention or DuckDB connection starvation cannot be correlated with analytical query loads.

---

## 2. Scheduling & Cadence
- **Trigger Type:** High-frequency interval timer (`interval`)
- **Default Schedule:** Every 60 seconds (`seconds=60`).
- **Scope:** Process-global singleton job (runs once per web backend process).
- **Jitter & Misfire Policy:**
  - Jitter: 5 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Samples OS vitals via `psutil`, probes DuckDB pool status, writes to SQLite `data/system/system_metrics.db`. | Local SQLite write lock. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Samples pod-local runtime vitals and connection pools; never scheduled to Celery workers (avoids sampling worker processes). | Pod-local SQLite write lock. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | Internal trigger | Trends and System Health in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Monitors local analyst process health. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | System-wide daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **OS Metric Probing (`psutil`):**
   - Samples process CPU percent (`proc.cpu_percent(interval=None)`).
   - Samples resident memory (`proc.memory_info().rss / (1024 * 1024)` MB).
   - Probes disk utilization for the data volume (`psutil.disk_usage(...)`).
2. **DuckDB Pool Instrumentation Probe:**
   - Probes DuckDB connection pool metrics: `active_connections`, `idle_connections`, `waiting_threads`.
   - Records maximum thread wait time (`app.thread_wait_ms`).
3. **SQLite Metric Insertion:**
   - Inserts record into `system_metrics.db`:
     ```sql
     INSERT INTO metric_snapshots (timestamp, cpu_pct, memory_rss_mb, disk_used_pct, duckdb_active_conns, duckdb_wait_ms)
     VALUES (?, ?, ?, ?, ?, ?);
     ```
4. **Historical Pruning (Bounded Retention):**
   - Deletes snapshots older than 14 days (`DELETE FROM metric_snapshots WHERE timestamp < ?`) to maintain a lightweight SQLite footprint.
5. **Telemetry & Log Recording:**
   - Emits operational metrics to Prometheus (`process_resident_memory_bytes`, `process_cpu_seconds_total`).

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local system probe; zero cloud API calls.
  - **SQLite Operations:** Every snapshot write must execute via `ThreadLocalPool` with instrumented timings.
  - **Overhead Budget:** Total execution runtime must remain < 50ms per tick.
- **Timing & Resource Budgets:**
  - `psutil` sampling duration: < 10ms.
  - SQLite insert duration: < 5ms.
- **Audit Checklist:**
  - Verify that metric sampling does not introduce GIL pauses.
  - Confirm historical snapshots older than 14 days are pruned automatically.

---

## 7. Failure Modes & Recovery Runbooks
- **SQLite Write Contention:** ThreadLocalPool handles retries transparently in WAL mode.
- **Corrupt Metrics DB:** Recreated automatically on boot if schema validation fails.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Inspect Trends:** `GET /api/admin/trends/metrics`.
- **System Vitals:** `GET /api/admin/system/vitals`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Allow `metric_snapshot` to tick or trigger sampling.
- [ ] 2. Call `GET /api/admin/trends/metrics`; verify recent timestamps exist.
- [ ] 3. Verify CPU, memory RSS, and DuckDB pool metrics are populated with realistic non-zero values.
- [ ] 4. Confirm in `system_metrics.db`: snapshot rows exist.
- [ ] 5. Confirm zero FOS Class A/B calls in `usage_log.db`.
