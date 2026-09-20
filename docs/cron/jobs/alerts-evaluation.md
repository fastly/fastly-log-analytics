> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `alerts_evaluation_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `alerts_evaluation_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `alerts_evaluation_{service_id}`
- **Category:** Alerting & Real-Time Anomaly Notification
- **Purpose:** Periodically evaluates user-configured alert threshold rules (error rates, bandwidth spikes, latency thresholds, 4xx/5xx ratios) against recent log windows in DuckDB/DuckLake and dispatches webhook notifications (Slack, PagerDuty, generic webhooks).
- **Why It Runs:** Real-time visibility into edge anomalies requires proactive alerting. Without scheduled evaluation, anomalies are only discovered when a human actively loads a dashboard page.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Runs at the service `log_period` interval (default: every 60s, or matched to `cron_sync.interval_seconds`).
- **Registration Gate (Important):** Gated strictly on `_service_has_alerts(service_id)`. If a service has zero active alerts configured, this job is never registered, avoiding wasted CPU and empty logs.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Queries session DuckDB `logs` view over lookback window (e.g. last 5-15 min). | Shared read-only DuckDB connection. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Queries ephemeral DuckDB over DuckLake or ClickHouse facts/aggregates. | Read-only queries; never runs on Celery workers. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/alerts/test/{alert_id}` | Full alert CRUD and incident history. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Evaluates alerts locally on analyst instance. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side alert daemon; notifications sent to configured webhooks. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Gate Check:** Checks `count_alerts(service_id)`. If 0, skips evaluation and unregisters job.
2. **Alert Rule Fetch:** Reads active alert configurations from `metadata.db` table `alerts`.
3. **DuckDB Metrics Query:**
   - For each active alert, constructs a parameterized DuckDB query bounded by `timestamp >= NOW() - INTERVAL '5 MINUTE'`.
   - Aggregates target metric: e.g. `COUNT(*) FILTER (WHERE status >= 500) * 100.0 / COUNT(*)`.
4. **Threshold Comparison & Debouncing:**
   - Compares metric against threshold (`operator`: `>`, `<`, `>=`, `<=`).
   - Checks cooldown / deduplication window to prevent alert fatigue.
5. **Webhook Dispatch:**
   - If threshold breached and cooldown expired: constructs JSON payload.
   - Dispatches HTTP POST to destination webhook (Slack, Teams, PagerDuty) with 5s timeout.
6. **State & Incident Update:**
   - Updates `last_triggered_at` and `incident_state` in SQLite `alerts` table.
   - Emits log to `cron_runs`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **DuckDB Analytical Queries:** Every threshold query must be recorded in `telemetry_queries` with execution duration.
  - **Webhook Dispatches:** Every outbound HTTP call to webhook endpoints must record latency, HTTP status code, and payload size.
  - **SQLite Operations:** Updates to `alerts` table must use `ThreadLocalPool`.
- **Timing & Resource Budgets:**
  - Threshold query duration: < 100ms per alert rule.
  - Overall job execution: < 1.0 second across all rules.
- **Audit Checklist:**
  - Confirm lookback window is indexed and leverages zone maps / partition pruning.
  - Verify webhook failures do not block subsequent alert evaluations.
  - Verify deduplication prevents notification storming.

---

## 7. Failure Modes & Recovery Runbooks
- **Webhook Endpoint Unreachable (500/timeout):** Logs failure in `cron_runs`, records non-fatal incident in `alerts`, retries on next tick.
- **Corrupt Alert Query SQL:** Handled safely via Pydantic validator and SQL syntax pre-validation; corrupt alerts are marked `error` and skipped.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Test Alert Webhook:** `POST /api/alerts/test/{alert_id}`.
- **List Service Alerts:** `GET /api/alerts/{service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Create a synthetic alert with a low threshold (e.g. error count > 0).
- [ ] 2. Insert synthetic error logs into the service.
- [ ] 3. Trigger alert evaluation; verify webhook payload is generated.
- [ ] 4. Confirm in `alerts` table: `last_triggered_at` is updated.
- [ ] 5. Confirm in `cron_runs`: execution status `success`.
- [ ] 6. Delete all alerts; verify `alerts_evaluation` job is automatically unregistered.
