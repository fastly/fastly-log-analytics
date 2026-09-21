# Background Job Specification: `gap_heal_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `gap_heal_{service_id}`
- **Category:** Autonomous Loss Detection & Reactive Self-Healing
- **Purpose:** Periodically evaluates Fastly log accounting metrics against actual ingested events. If sustained log loss is detected (>= 2 consecutive completed hourly buckets with >= 5% loss between Fastly edge billable counts and ingested rows), it automatically triggers a targeted full sweep (`_run_full_sweep`) to heal the gap.
- **Why It Runs:** Network blips, queue overflows, or edge delivery anomalies can result in lost log bursts. Waiting for scheduled full sweeps could leave analytics inaccurate for hours. The gap-heal evaluator continuously audits delivery fidelity and reacts dynamically to restore complete data within minutes.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 30 minutes (`interval_minutes = 30`).
- **Timing Rationale:** Fastly Stats API updates every ~5-15 minutes, and hourly buckets finalize on the hour. A 30-minute cadence ensures quick detection of closed hourly loss runs without redundant API traffic.
- **Configurable Overrides:**
  - `provisioning.cron_gap_heal.enabled` (default: true).
  - `provisioning.cron_gap_heal.interval_minutes` (default: 30; dynamic scheduler rescheduling supported).
- **Adaptive Severity Bands & Throttle Cooldowns:**
  - `critical` (>= 80% gap or >= 500k lost lines): 0h throttle (fires immediately on every tick), expands sweep budget to 100k files / 1800s.
  - `severe` (>= 50% gap or >= 100k lost lines): 0.25h (15 min) throttle, expands sweep budget to 50k files / 1500s.
  - `elevated` (>= 10% gap or >= 10k lost lines): 2h throttle, default sweep budget (20k files / 900s).
  - `mild` (floor / baseline): 4h throttle, default sweep budget (20k files / 900s).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Queries Fastly `/stats/service` and compares with DuckDB row counts; triggers `_run_full_sweep` if sustained loss occurs. | Read-only accounting query. Active-request deferral (`should_defer_cron("gap_heal", service_id)`). Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Workers / Web Pod | Compares Fastly API metrics against `ingest_ledger` / DuckLake counts; enqueues sweep tasks via `discover_prefix`. | Distributed PostgreSQL locking. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/gap-heal/{service_id}` | Accounting loss alerts in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Gap healing requires Admin credentials and Fastly API access. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Benefits from healed data; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite & Politeness Check:**
   - Verifies service has a valid `logging_service_id` or `service_id` configured and is not read-only.
   - Checks `FLA_DEV_NO_CRONS=1` (aborts if set).
   - Evaluates `should_defer_cron("gap_heal", service_id)`. If active user queries are running in DuckDB, yields to preserve dashboard query latency.
2. **Accounting Computation:**
   - Calls `compute_log_accounting(src, hours=24, by="hour")`.
   - Queries Fastly Stats API for edge-recorded billable request counts in 1-hour buckets.
   - Queries DuckDB / DuckLake for ingested request counts in the same hourly buckets.
3. **Loss Analysis:**
   - Compares Edge Count vs Ingested Count across completed buckets (ignores the active, in-progress hour).
   - Detects `SustainedLossAlert`: triggered if >= 2 consecutive buckets exhibit >= 5% deficit (`loss_pct >= 0.05`).
   - If no sustained loss: records `status="success"` in `cron_runs` and exits.
4. **Severity Classification & Adaptive Throttle Check:**
   - Evaluates `_gap_heal_severity(sustained.max_gap_pct, sustained.total_lost_lines)` to select the severity band (`mild`, `elevated`, `severe`, `critical`).
   - Checks elapsed hours since last successful trigger for `service_id`.
   - If within the throttle cooldown: logs `status="warning"` with `throttled` summary and exits without re-triggering.
5. **Reactive Full Sweep Invocation:**
   - If sustained loss detected and throttle permits:
     - Logs `status="warning"` and emits status event to `cron_progress`.
     - Marks trigger timestamp in `_GAP_HEAL_LAST_TRIGGER[service_id]`.
     - Synchronously triggers `_run_full_sweep(service_id, max_files=band.sweep_max_files, max_seconds=band.sweep_max_seconds)`.
6. **Duration Finalization:**
   - In `finally:` block, calls `end_progress(run_id)` and `finalize_cron_duration(src, run_id, start_time_exec)` to guarantee complete duration capture.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **Fastly API Calls:** Requests to Fastly `/stats/service/{id}` are cached with 45s TTL to prevent rate limit exhaustion.
  - **DuckDB Accounting Queries:** Count queries across hourly partitions use short-TTL cache (45s) and indexed rollups.
  - **Audit Logging:** Every gap heal evaluation records its findings in `cron_runs` (warning when sustained loss is detected or throttled).
- **Timing & Resource Budgets:**
  - Accounting evaluation: < 500ms total runtime (when cached).
  - Fastly API call latency: < 300ms.

---

## 7. Failure Modes & Recovery Runbooks
- **Fastly API Timeout / 500:** Logs error, retries on next 30m tick; does not trigger false-positive sweep.
- **Zero Log Ingest on Dead Service:** If Fastly reports 0 requests, gap is 0%; no sweep triggered.
- **Active Dashboard Queries:** `should_defer_cron` yields gap heal evaluation while users are actively running queries.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Gap Check & Heal:** `POST /api/admin/gap-heal/{service_id}`.
- **Inspect Accounting Data:** `GET /api/admin/log-accounting?service_id={service_id}`.

---

## 9. Automated Verification Matrix
- [x] 1. Active request politeness deferral verified via `test_run_gap_heal_defers_when_active_requests_present`.
- [x] 2. Sustained loss detection triggering `_run_full_sweep` verified via `test_run_gap_heal_triggers_full_sweep_on_sustained_loss`.
- [x] 3. Warning status logged for both triggered and throttled loss verified via `test_run_gap_heal_triggers_full_sweep_on_sustained_loss` and `test_run_gap_heal_respects_throttle_window`.
- [x] 4. Severity bands classification verified via `test_gap_heal_severity_bands`.
- [x] 5. Critical loss throttle bypass and widened budget verified via `test_run_gap_heal_critical_bypasses_throttle_and_widens_sweep`.
- [x] 6. Severe loss 15-minute throttle verified via `test_run_gap_heal_severe_uses_15min_throttle`.
- [x] 7. Dynamic rescheduling on `interval_minutes` config change verified via `test_sync_jobs_reschedules_gap_heal_when_interval_changed`.
- [x] 8. Kill switch protection verified via `test_run_gap_heal_refuses_when_kill_switch_on`.
