# Page Specification: Alerts & Notifications (`/alerts`)

> [!IMPORTANT] **Page Specification Live-Certified (v3.0.0-beta3)**
> This specification defines the verified contract, architecture behaviors, role permissions, SSRF protections, and automated test cases for `/alerts`. Certified across Local Standard, Remote Standard GCE, and High-Scale Elevation across Admin and Analyst Path B roles.

---

## 1. Overview & Objectives

The **Alerts** page manages automated anomaly detection rules, threshold triggers (e.g. 5xx rate > 5%, latency p95 > 500ms, origin failure spikes), alert state history (`OK`, `ALERTING`, `MUTED`), notification destinations (Slack, PagerDuty, Webhook), and active incident escalation.

### Key Tenets:
- **Proactive Operational Warning:** Evaluates threshold rules continuously via background `alerts_evaluation_{service_id}` jobs and on-demand manual evaluation (`POST /api/alerts/evaluate`).
- **Incident State & KPI Summary:** Tracks rule state transitions (`OK`, `ALERTING`, `MUTED`), last evaluated values, and trigger conditions across a 4-card KPI summary banner (`Total Rules`, `Active Alerts`, `Healthy Rules`, `Muted / Disabled`).
- **SSRF & RBAC Hardening:** Enforces strict `require_admin` RBAC on all alert mutations and validates all webhook/Slack/PagerDuty URLs against SSRF (loopback, link-local, RFC1918 private IPs, and cloud metadata endpoints) unless `FLA_ALLOW_PRIVATE_WEBHOOKS=1` is explicitly enabled in test environments.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores (`useServiceStore`, `useTimeRangeStore`, `useFilterStore`).

---

## 2. Routes & URL Schema

- **Primary Route:** `/alerts`
- **Supported Query Parameters:**
  - `service` / `service_id`: Fastly Service ID.
  - `status`: Filter by state (`all`, `alerting`, `ok`, `muted`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Create, edit, mute/enable, evaluate, test notification channels, and delete alert rules; configure Webhook, Slack, and PagerDuty destinations. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Active alerts and rule configurations viewable; rule creation, editing, muting, evaluation, webhook testing, and deletion are blocked (`403 Forbidden`) and disabled/hidden in UI. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Local alert evaluation against local engine. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Alert Engine** | Evaluated by in-process APScheduler job `alerts_evaluation_{id}` (`backend/cron/jobs/alerts.py`) against DuckDB/DuckLake `logs` view. | Evaluated by serving-pod APScheduler job `alerts_evaluation_{id}` against shared DuckLake / ClickHouse analytical store. |
| **Alert State DB** | Unified PostgreSQL 16 (`METADATA_DSN`, `alerts` table per ADR-22 via `backend/core/metadata/alerts.py`). | Unified PostgreSQL 16 (`METADATA_DSN`, `alerts` table per ADR-22 via `backend/core/metadata/alerts.py`). |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/alerts/` & `GET /api/alerts/{service_id}`:
   - Returns list of configured rules (`AlertListResponse`), current status (`ok`, `alerting`, `muted`), last evaluated value, and notification channels. Supports optional `?status=all|alerting|ok|muted` filter.
2. `POST /api/alerts/` (Admin only):
   - Creates or updates an alert rule (`AlertMutationResponse`) after validating webhook/channel URLs against SSRF.
3. `PUT /api/alerts/{alert_id}` (Admin only):
   - Updates an existing alert rule (`AlertMutationResponse`) with path/payload ID consistency and SSRF validation.
4. `PATCH /api/alerts/{alert_id}/enabled` (Admin only):
   - Toggles an alert rule's `enabled` state (`AlertToggleResponse`).
5. `DELETE /api/alerts/{alert_id}` (Admin only):
   - Deletes a configured alert rule (`AlertDeleteResponse`).
6. `POST /api/alerts/evaluate` (Admin only):
   - Evaluates all enabled alert rules for a service on demand (`AlertEvaluateResponse`), returning per-rule triggered states and current metric values.
7. `POST /api/alerts/preview` (Admin only):
   - Computes historical time-series preview data (`AlertPreviewResponse`) against the configured metric and window for interactive threshold tuning.
8. `POST /api/alerts/{alert_id}/test` & `POST /api/alerts/{service_id}/{alert_id}/test` (Admin only):
   - Validates and dispatches (or dry-runs with `dry_run=true`) a test notification across configured Webhook, Slack, or PagerDuty channels (`AlertTestWebhookResponse`).

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID` and executes inside `process_context_scope` (`alerts:evaluate`, `alerts:preview`).
- DuckDB analytical queries and PostgreSQL metadata statements are recorded in `telemetry_queries` (`_debug_queries` / `_debug_calls`) and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Alert KPI Summary Grid (`min-h-[104px] [contain-intrinsic-size:104px] [content-visibility:auto]`) and Configured Rules Card (`min-h-[380px] [contain-intrinsic-size:380px] [content-visibility:auto]`) reserve layout geometry prior to data hydration.
- **In-Place Skeletons:** Rules table displays contextual loading copy (`Fetching configured alert rules...`) inside pre-allocated `CardContent`.
- **Dimmed Updates:** Background refetches dim existing content (`opacity-40 pointer-events-none transition-opacity duration-200`) with zero layout shift (`CLS <= 0.05`).

---

## 7. Interactive Workflows & Edge Cases

- **Create / Edit Alert Sheet (`AlertEditor`):** Allows configuring rule name, metric (`5xx_rate`, `4xx_rate`, `origin_error_rate`, `waf_block_rate`, `hit_ratio`, `ttfb`, `p95_ttfb`, `origin_ttfb`, `requests`, `rps`, `bandwidth`), operator (`>`, `<`, `>=`, `<=`), threshold, evaluation window (`window_min`), filter query, and multi-channel notifications (`webhook`, `slack`, `pagerduty`), with live threshold chart preview.
- **Status Filter Tabs:** Operators can filter rules by `All`, `Alerting`, `Healthy` (`ok`), and `Muted` (`muted`) via `data-testid="alerts-status-filter"`.
- **Rule Muting & Testing:** Operators can toggle rules enabled/muted in-place (`toggle-alert-{id}`) and trigger dry-run or live channel verification (`test-alert-{id}`).
- **Zero Alerts State:** Displays `"No alert rules configured"` (`data-testid="alerts-empty-state"`) with an `"Add Alert"` CTA for Admins.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 | <= 0.05 | PerformanceObserver |
| **API Latency (List)** | < 150 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **Query Efficiency** | Single PostgreSQL select; 0 redundant queries | Single PostgreSQL select; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /alerts` returns HTTP 200.
- [x] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS <= 0.05.
- [x] **3. Alerts List Contract:** Verify configured rules and active incident statuses (`all`, `alerting`, `ok`, `muted`) return accurately.
- [x] **4. Rule Creation:** (Admin) Create, update (`PUT`), and toggle (`PATCH`) a test alert rule and verify it appears in the list.
- [x] **5. Test Webhook & Evaluation:** (Admin) Trigger test notification (`POST /api/alerts/{alert_id}/test`) and on-demand evaluation (`POST /api/alerts/evaluate`) and verify HTTP 200 responses.
- [x] **6. Role Verification:** Confirm Analyst Path B receives HTTP 403 on mutations and observes read-only UI gating.
- [x] **7. Telemetry & Query Audit:** Verify all DuckDB and PostgreSQL queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright E2E Suite:** `frontend/e2e/pages/alerts.spec.ts` (7 contract tests: route & zero-CLS reservations, status filter tabs, Admin CRUD + toggle + evaluate + test webhook lifecycle, SSRF validation, Analyst Path B 403 boundary, warm FCP/CLS performance budget, and telemetry audit).
- **Frontend Unit Tests:** `frontend/__tests__/app/alerts.test.tsx` (4 tests covering empty state, KPI summary + status tabs, PATCH toggle + POST test webhook, and Analyst Path B read-only gating).
- **Backend Unit Tests:** `tests/routers/test_alerts_and_views.py` (22 tests covering CRUD, `PUT` update, status filtering, `/evaluate`, `/preview`, `/test` webhook dispatch, SSRF private-IP rejection, and Analyst Path B 403 enforcement).
