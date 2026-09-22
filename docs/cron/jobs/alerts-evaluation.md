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
- **Registration Gate (Important):** Gated strictly on `_service_has_alerts(service_id)` and `cron_alerts.enabled`. If a service has zero active alerts configured or if alerts are disabled, this job is not registered, avoiding wasted CPU and empty logs.
- **Configurable Overrides:**
  - `provisioning.cron_alerts.enabled` (default: `true`).
  - `provisioning.cron_alerts.interval_seconds` (overrides default `log_period`).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Queries session DuckDB `logs` view over lookback window (e.g. last 5-15 min). | Read-only DuckDB connection. Active request politeness gate (`should_defer_cron`) yields to interactive dashboard queries. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Queries ephemeral DuckDB over DuckLake or ClickHouse facts/aggregates. | Ephemeral read-only connection; never runs on Celery workers. Yields if active interactive dashboard queries are executing. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/alerts/test/{alert_id}` | Full alert CRUD and incident history. Dispatches webhook notifications to external channels. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Evaluates alerts locally against cached dataset on analyst instance. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side alert daemon; notifications sent to configured webhooks. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Source & Politeness Check:**
   - Loads source via `get_source_for_service(service_id)`.
   - Checks `should_defer_cron("alerts", service_id)`. If active user queries are currently hitting DuckDB, defers until next interval to avoid query contention.
2. **Alert Rule Fetch & Early Exit:**
   - Reads active alert configurations from `metadata.db` table `alerts`.
   - Filters to `enabled_alerts = [a for a in alerts if a["enabled"]]`. If empty, logs `skipped` with summary "No alerts configured" and exits without opening DuckDB.
3. **Execution & Progress Initialization:**
   - Opens read-only DuckDB connection `get_connection(src, read_only=True)`.
   - Calls `start_cron_run(src, "alerts")` and initializes `start_progress` in `cron_progress`.
4. **DuckDB Metrics Query:**
   - For each active alert, calls `alert_repo.evaluate_alert`:
     - Queries `MAX(timestamp)` from `logs`. If logs are older than 30 minutes, skips evaluation.
     - Constructs parameterized DuckDB query over the configured lookback window.
     - Evaluates threshold expression against target metric.
   - If triggered: captures `(alert, webhook_url, payload, max_ts)` and emits progress event to `cron_progress`.
5. **Durable Timestamp Update & Export:**
   - Before firing webhooks, calls `update_last_triggered` and `export_admin_state` so quiet-period debouncing persists even if an outbound webhook network request hangs.
6. **Notification Dispatch:**
   - Posts legacy webhook payload if configured.
   - Posts to modern notification channels (Slack mrkdwn, PagerDuty Events API, generic HTTP webhooks) with timeouts.
   - Collects any webhook/channel transmission failures in `webhook_failures`.
7. **Status & Telemetry Recording:**
   - Status is recorded as `"warning"` if any alert triggered (`n_trig > 0`) or if any webhook failed to deliver (`webhook_failures`). Otherwise records `"success"`.
   - Emits done event to `cron_progress`.
   - Logs execution summary and counts in `cron_runs`.
   - In guaranteed `finally:` block, calls `end_progress(run_id)` and `finalize_cron_duration(src, run_id, start)`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **DuckDB Analytical Queries:** Every threshold query is instrumented via `track_query` with latency attribution under category `"alerts"`.
  - **Webhook Dispatches:** Outbound HTTP calls use bounded timeouts (5s) and catch network errors to prevent crashing the evaluation loop.
  - **SQLite Operations:** Updates to `alerts` table execute through `ThreadLocalPool`.
- **Timing & Resource Budgets:**
  - Threshold query duration: < 100ms per alert rule.
  - Overall job execution: < 1.0 second across all rules.
- **Audit Checklist:**
  - Confirm lookback window leverages partition pruning.
  - Verify webhook failures do not block subsequent alert evaluations.
  - Verify timestamp persistence precedes webhook dispatch to eliminate notification storms on network timeouts.

---

## 7. Failure Modes & Recovery Runbooks
- **Active Dashboard Load:** Politeness gate defers evaluation to preserve UI interactivity.
- **Webhook Endpoint Timeout / 5xx:** Recorded in `webhook_failures`; cron run logged as `"warning"`; remaining alerts continue evaluation.
- **Corrupt Alert Query SQL:** Caught per-alert; logs error for individual alert while remaining alerts continue evaluation.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Test Alert Webhook:** `POST /api/alerts/test/{alert_id}`.
- **List Service Alerts:** `GET /api/alerts/{service_id}`.

---

## 9. Automated Verification Matrix
- [x] 1. Active request politeness deferral verified via `test_run_service_alerts_evaluation_defers_when_active_requests_present`.
- [x] 2. Warning status on triggered alerts verified via `test_run_service_alerts_evaluation_status_warning_when_alert_triggers`.
- [x] 3. Warning status on webhook dispatch failures verified via `test_run_service_alerts_evaluation_status_warning_when_webhook_fails`.
- [x] 4. Live progress events to `cron_progress` verified via `test_run_service_alerts_evaluation_emits_progress_events`.
- [x] 5. Dynamic rescheduling on `cron_alerts.interval_seconds` change verified via `test_sync_jobs_reschedules_alerts_evaluation_when_interval_changed`.
- [x] 6. Job disabled when `cron_alerts.enabled = False` verified via `test_sync_jobs_skips_alerts_evaluation_when_disabled`.
- [x] 7. Unregistered when 0 alerts configured verified via `test_run_service_alerts_evaluation_skips_when_no_alerts_configured`.
