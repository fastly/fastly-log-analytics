# Page Specification: Real-Time Session Stream (`/sessions/stream`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/sessions/stream`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Real-Time Session Stream** page visualizes live client session state transitions as they occur at the edge. It highlights escalating threat scores, bot challenge failures, rapid path traversal, and real-time score threshold crossings.

### Key Tenets:
- **Streaming State Machine:** Watches session score transitions in real time via Server-Sent Events.
- **Immediate Escalation Detection:** Alerts operators when a session crosses into High/Critical severity.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/sessions/stream`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `min_score`: Filter to sessions exceeding this minimum score threshold (e.g. `0.70`).
  - Standard drill-down filters: `ja4`, `country`, `pop`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Live session events, unmasked client IPs, full request attributes. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Live stream viewable; client IPs masked (if enabled). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Streams from local buffer/session scoring pipeline. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Stream Transport** | In-process SSE stream from session scorer engine. | Redis Pub/Sub stream bridge connected to Celery workers. |
| **State Tracking** | In-memory session sliding window. | Valkey / Redis session state store. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/sessions/stream/{service_id}` (Server-Sent Events):
   - Streams live JSON events: `session_id`, `client_ip`, `ja4`, `current_score`, `score_delta`, `trigger_reason`.

### Telemetry Attribution:
- Connection and event throughput logged to `usage_log.db` and telemetry spans.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Real-Time Flow Visualizer, Escalation Alert Feed, and Stream Controls Toolbar enforce container intrinsic sizes (`contain-intrinsic-size: 500px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Subscribing to session threat events...`).
- **Zero CLS Stream:** Uses `@tanstack/react-virtual` to append incoming session events smoothly without document layout shift.

---

## 7. Interactive Workflows & Edge Cases

- **Threshold Slider:** Dragging the minimum score slider filters incoming events dynamically on the client side.
- **Pause / Freeze:** Instant pause button freezes the stream to inspect an escalating session.
- **Session Focus:** Clicking any session event opens the historical drill-down drawer.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **Event Delivery Lag** | < 1000 ms | < 1000 ms | Timestamp diff |
| **Telemetry Capture** | 100% streams captured | 100% streams captured | Stream connection audit |
| **DOM Memory Cap** | Max 500 streamed items | Max 500 streamed items | Memory profiling |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /sessions/stream` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. SSE Event Stream:** Verify `/api/sessions/stream/{service_id}` receives live session score transitions.
- [ ] **4. Threshold Filtering:** Adjust score threshold and verify lower-scored sessions are filtered out.
- [ ] **5. Role Verification:** Confirm Analyst Path B observes masked client IPs in the stream.
- [ ] **6. Telemetry & Query Audit:** Verify SSE connection is recorded in `usage_log.db`.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/sessions-stream.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Simulated escalating attack bot whose threat score increases every 5 requests.
