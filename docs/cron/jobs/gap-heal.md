> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `gap_heal_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `gap_heal_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `gap_heal_{service_id}`
- **Category:** Autonomous Loss Detection & Reactive Self-Healing
- **Purpose:** Periodically evaluates Fastly log accounting metrics against actual ingested events. If sustained log loss is detected (>= 2 consecutive completed hourly buckets with >= 5% loss between Fastly edge billable counts and ingested rows), it automatically triggers a targeted full sweep (`_run_full_sweep`) to heal the gap.
- **Why It Runs:** Network blips, queue overflows, or edge delivery anomalies can result in lost log bursts. Waiting for the daily 03:30 UTC full sweep could leave analytics inaccurate for up to 24 hours. The gap-heal evaluator continuously audits delivery fidelity and reacts dynamically to restore complete data within minutes.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 30 minutes (`interval_minutes = 30`).
- **Configurable Overrides:**
  - `provisioning.cron_gap_heal.enabled` (default: true).
  - `provisioning.cron_gap_heal.interval_minutes` (default: 30).
  - Adaptive backoff throttle: `_gap_heal_throttle_hours` (prevents spamming full sweeps if gap is caused by upstream CDN drops).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Queries Fastly `/stats/service` and compares with DuckDB row counts; triggers `_run_full_sweep` if sustained loss occurs. | Read-only accounting query; acquires ingest lock only if sweep is triggered. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat / Celery Worker | Compares Fastly API metrics against `ingest_ledger` / DuckLake counts; enqueues sweep tasks. | Distributed PostgreSQL locking. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/gap-heal/{service_id}` | Accounting loss alerts in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Gap healing requires Admin credentials and Fastly API access. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Benefits from healed data; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite Check:** Verifies service has a valid `logging_service_id` or `service_id` configured. Checks `FLA_DEV_NO_CRONS=1`.
2. **Accounting Computation:**
   - Calls `compute_log_accounting(service_id, lookback_hours=12)`.
   - Queries Fastly Stats API for edge-recorded request counts in 1-hour buckets.
   - Queries DuckDB / DuckLake for ingested request counts in the same hourly buckets.
3. **Loss Analysis:**
   - Compares Edge Count vs Ingested Count across completed buckets (ignores the active, in-progress hour).
   - Detects `SustainedLossAlert`: triggered if >= 2 consecutive buckets exhibit >= 5% deficit (`loss_pct >= 5.0`).
4. **Adaptive Throttle Check:**
   - Checks timestamp of last triggered heal sweep. If within adaptive throttle cooldown window (default 2-4 hours), skips re-triggering to prevent infinite sweep loops when loss is caused by edge filtering (e.g. VCL drops).
5. **Reactive Full Sweep Invocation:**
   - If sustained loss detected and throttle permits: logs warning and executes `_run_full_sweep(service_id, force=True)`.
6. **Telemetry & Log Recording:**
   - Records evaluation outcome in `cron_runs` (`loss_detected: bool`, `max_gap_pct`, `sweep_triggered: bool`).

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **Fastly API Calls:** Requests to Fastly `/stats/service/{id}` must log response times and rate limit headers.
  - **DuckDB Accounting Queries:** Vectorized count queries across hourly partitions must be tracked in `telemetry_queries`.
  - **Audit Logging:** Every gap heal evaluation must record its findings in `metadata.db`.
- **Timing & Resource Budgets:**
  - Accounting evaluation: < 500ms total runtime.
  - Fastly API call latency: < 300ms.
- **Audit Checklist:**
  - Verify that active, incomplete hours are excluded from loss calculations.
  - Verify adaptive backoff prevents cascading full sweeps during genuine upstream drop events.

---

## 7. Failure Modes & Recovery Runbooks
- **Fastly API Timeout / 500:** Logs warning, retries on next 30m tick; does not trigger false-positive sweep.
- **Zero Log Ingest on Dead Service:** If Fastly reports 0 requests, gap is 0%; no sweep triggered.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Gap Check & Heal:** `POST /api/admin/gap-heal/{service_id}`.
- **Inspect Accounting Data:** `GET /api/admin/log-accounting?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Simulate a sustained gap by querying `compute_log_accounting` with mock API data.
- [ ] 2. Trigger `POST /api/admin/gap-heal/{service_id}`; verify HTTP 200.
- [ ] 3. Verify in logs: `SustainedLossAlert` is detected.
- [ ] 4. Verify in logs: `_run_full_sweep` is automatically triggered.
- [ ] 5. Confirm subsequent immediate invocation is suppressed by the adaptive throttle cooldown.
- [ ] 6. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute.
