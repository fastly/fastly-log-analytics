# Alerts (`/alerts`)

Admin page for threshold and anomaly alert rules over a service's request logs, with state tracking and Slack, PagerDuty and webhook notifications.

Source: [`frontend/app/alerts/`](../../frontend/app/alerts/): `page.tsx` is a server wrapper (`force-dynamic`) that prefetches the alert list; `_sections/AlertsClient.tsx`, `AlertsList.tsx`, `CreateAlertForm.tsx`, `CreateAlertFormFields.tsx`, `AlertPreview.tsx`.

## Roles

Admin only. `/api/alerts` is in the analyst blocklist (`_ANALYST_BLOCKED_PREFIXES` in [`backend/utils/remote_access.py`](../../backend/utils/remote_access.py)), `AppLayout` redirects analysts away from `/alerts`, and the mutating handlers additionally reject analyst sessions with `403 admin_only`.

## UI

- KPI cards: Total Rules, Alerting, Healthy (OK), Muted.
- Configured Alerts card with a status filter (`all`, alerting, ok, muted), per-rule enable/mute toggle, Test, Edit and Delete actions, and an empty state. Header actions: Evaluate Rules, Refresh Now and Create Alert.
- Rule editor: name, category (reliability, performance, traffic, caching), metric, scope (`all`, `edge`, `origin`), evaluation type (`absolute`, `relative_increase`, `relative_decrease`, `anomaly_zscore`), operator (`>`, `<`, `>=`, `<=`), threshold, window and comparison period (minutes), status codes for the specific-status metrics, z-score threshold and baseline days, and notification channels (Slack, PagerDuty, webhook). A live preview chart (`/api/alerts/preview`) helps tune the threshold.
- Metrics: `requests`, `5xx`, `5xx_rate`, `4xx`, `4xx_rate`, `specific_status`, `specific_status_rate`, `p95_latency`, `hit_rate`, `bandwidth`, `ttfb`.

## Backend

Routes in [`backend/routers/alerts.py`](../../backend/routers/alerts.py) (prefix `/api/alerts`); models `Alert`, `AlertChannel` in [`backend/models/common/bootstrap_alerts.py`](../../backend/models/common/bootstrap_alerts.py).

- `GET /api/alerts/` (all alerts, `limit` up to 2000, `status` filter) and `GET /api/alerts/{service_id}`.
- `POST /api/alerts/` create; `PUT /api/alerts/{alert_id}` update; `PATCH /api/alerts/{alert_id}/enabled`; `DELETE /api/alerts/{alert_id}`.
- `POST /api/alerts/evaluate`: evaluate a service's enabled rules now. `POST /api/alerts/preview`: historical series for a candidate rule.
- `POST /api/alerts/{alert_id}/test` and `POST /api/alerts/{service_id}/{alert_id}/test`: send (or dry-run) a test notification.

`validate_webhook_url` applies on create and update for the webhook URL and every channel URL: only `http`/`https` with a hostname; loopback, link-local, private, multicast, reserved and unspecified IPs and metadata/internal hostnames (`.internal`, `.local`, ...) are rejected with `400 invalid_webhook_url`.

Alerts are stored in the PostgreSQL `alerts` table ([`backend/core/metadata/alerts_db.py`](../../backend/core/metadata/alerts_db.py)) and are per instance (not synced in `admin_state.json`). Continuous evaluation is the `alerts_evaluation_{service_id}` cron ([spec](../cron/jobs/alerts-evaluation.md)).

## Tests

- Frontend: [`frontend/__tests__/app/alerts.test.tsx`](../../frontend/__tests__/app/alerts.test.tsx)
- E2E: `Alerts & Notifications Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts) (list, status filter, rule lifecycle, SSRF rejection)
