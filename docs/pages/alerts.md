# Page Specification: Alerts & Notifications (`/alerts`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/alerts`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Alerts** page manages automated anomaly detection rules, threshold triggers (e.g. 5xx rate > 5%, latency p95 > 500ms, origin failure spikes), alert state history (`OK`, `ALERTING`, `MUTED`), notification destinations (Slack, PagerDuty, Webhook, Email), and active incident escalation.

### Key Tenets:
- **Proactive Operational Warning:** Evaluates threshold rules continuously via background `alerts_evaluation_{service_id}` jobs.
- **Incident State History:** Tracks state transitions with timestamps, evaluated values, and trigger conditions.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/alerts`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `status`: Filter by state (`all`, `alerting`, `ok`, `muted`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Create, edit, mute, delete alert rules; configure webhook destinations. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Active alerts and incident history viewable; rule creation/editing disabled. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Local alert evaluation against local engine. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Alert Engine** | Evaluated by in-process APScheduler job `alerts_evaluation_{id}`. | Evaluated by pod-local APScheduler against shared lake. |
| **Alert State DB** | Per-service SQLite `metadata.db` (`alerts` & `alert_history` tables). | Shared Postgres or per-service SQLite database. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/alerts/{service_id}`:
   - Returns list of configured rules, current state, last evaluated value, and notification targets.
2. `POST /api/alerts/{service_id}` (Admin only):
   - Creates or updates an alert rule.
3. `POST /api/alerts/{service_id}/{alert_id}/test` (Admin only):
   - Sends a test webhook notification to verify integration.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Alert Status Summary, Rules List Table, and Incident History Feed enforce container intrinsic sizes (`contain-intrinsic-size: 350px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Fetching configured alert rules...`, `Checking incident status...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Create Alert Modal:** Allows selecting metric (5xx rate, TTFB, requests), threshold condition (> or <), evaluation window (1m, 5m, 15m), and webhook URL.
- **Rule Muting:** Operators can temporarily mute an alert for 1h, 4h, or 24h during maintenance.
- **Zero Alerts State:** Displays a helpful "No alert rules configured" banner with an "Add Alert" button for Admins.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (List)** | < 150 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **Query Efficiency** | Single SQLite select; 0 redundant queries | Single DB select; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /alerts` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Alerts List Contract:** Verify configured rules and active incident statuses return accurately.
- [ ] **4. Rule Creation:** (Admin) Create a test alert rule and verify it appears in the list.
- [ ] **5. Test Webhook:** (Admin) Trigger test notification and verify HTTP 200 response.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes disabled create/edit buttons.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/alerts.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Injected simulated error condition to trigger active alerting state.
