# Page Specification: Live Log Streaming (`/streaming`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/streaming`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Live Log Streaming** page delivers a high-speed, interactive terminal-style live tail of incoming CDN edge log lines via Server-Sent Events (SSE). It supports pause/resume, client-side regex search, JSON inspect modal, and column formatting.

### Key Tenets:
- **Low-Latency Streaming:** Pushes raw edge request events as they land in ingest buffers.
- **Interactive Inspection:** Clicking any streamed line opens the deep JSON inspector showing all captured Fastly VCL variables.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/streaming`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `filter`: Live regex/string search pattern.
  - Standard drill-down filters: `status`, `pop`, `client_ip`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full raw JSON log records, unmasked client IPs, full request headers. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Live stream viewable; client IPs masked (if enabled by invite policy). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Streams from local buffer/lake. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Stream Transport** | FastAPI SSE streaming endpoint (`/api/stream/{service_id}`). | FastAPI SSE backed by Redis Pub/Sub stream bridge. |
| **Buffer Source** | Reads newest rows from local memory ring buffer. | Reads newest events from Valkey / Redis stream. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/stream/{service_id}` (Server-Sent Events):
   - Streams JSON chunks with request fields (timestamp, client_ip, method, url, status, pop, time_elapsed).

### Telemetry Attribution:
- Connection establishment and stream duration recorded in `usage_log.db` and telemetry logs.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Terminal Stream Box, Stream Controls Toolbar, and Inspector Drawer enforce container intrinsic sizes (`contain-intrinsic-size: 600px`).
- **In-Place Skeletons:** Renders a connected stream skeleton (`Connecting to live Fastly log stream...`).
- **Zero CLS Log Append:** Appending log rows utilizes virtualized list rendering (`@tanstack/react-virtual`) to eliminate DOM jitter and scroll jump.

---

## 7. Interactive Workflows & Edge Cases

- **Pause / Resume:** Spacebar or UI button pauses stream without dropping the SSE connection; resumed stream flushes recent buffer.
- **JSON Inspector Modal:** Clicking any log line opens the slide-out drawer with syntax-highlighted VCL JSON.
- **Stream Disconnect Recovery:** Automatically reconnects with exponential backoff if SSE stream breaks.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **Stream Event Latency** | < 1000 ms from edge ingest | < 1000 ms from edge ingest | Timestamp diff |
| **Telemetry Capture** | 100% streams captured | 100% streams captured | `usage_log.db` audit |
| **Memory Leak Guard** | Max 1000 DOM lines capped | Max 1000 DOM lines capped | Chrome memory profiling |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /streaming` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. SSE Connection:** Verify `/api/stream/{service_id}` establishes connection and receives valid JSON events.
- [ ] **4. Pause/Resume:** Toggle pause; verify stream freezes in place without UI errors.
- [ ] **5. Line Inspector:** Click a streamed row; verify inspector drawer opens with full JSON attributes.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes masked client IPs in streamed lines.
- [ ] **7. Telemetry & Query Audit:** Verify SSE stream connection is recorded in `usage_log.db`.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/streaming.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Continuous streaming traffic with varying HTTP status codes.
