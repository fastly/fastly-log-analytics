# Page Specification: Admin Live Share Management (`/admin/share`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/admin/share`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Admin Live Share Management** page provides complete administrative control over remote live sharing (Analyst Path B). It manages share invite tokens, passcode generation (Argon2id), active analyst sessions, session revocation, compliance audit trails, Terms of Service (TOS) policy settings, and client IP masking policies.

### Key Tenets:
- **Zero-FOS-Credential Analyst Access:** Grants time-limited read-only access to running instances without exposing storage keys.
- **Session Governance:** Real-time visibility into active analyst sessions with instant revocation capability.
- **Strict RBAC:** Accessible **exclusively** to Admin (`read_write`) users. Analysts receive HTTP 403.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/share`
- **Supported Query Parameters:**
  - `tab`: Selected view (`invites`, `sessions`, `audit`, `settings`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Create invites, revoke sessions, view analyst audit logs, toggle IP masking. |
| **Analyst Path B (Remote Share)** | Blocked (403) | Route access rejected. |
| **Analyst Path A (JSON Join)** | Blocked (403) | Route access rejected. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Share DB** | Local SQLite singleton `data/system/remote_share.db`. | Shared Postgres or SQLite remote share database. |
| **Session Tracking** | In-memory token registry + SQLite session table. | Centralized session state table with TTL expiration. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/share/invites`:
   - Returns active and expired invite tokens, service targets, and expiry timestamps.
2. `POST /api/share/invites`:
   - Creates a new share invite with optional passcode and expiry.
3. `GET /api/share/sessions`:
   - Returns currently active analyst sessions.
4. `DELETE /api/share/sessions/{session_id}`:
   - Revokes an active analyst session immediately.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Share Status Summary, Create Invite Drawer, Active Sessions Table, and Compliance Audit Log enforce container intrinsic sizes (`contain-intrinsic-size: 400px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Inspecting active analyst sessions...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Create Invite Modal:** Administrator specifies service, expiration (1h, 24h, 7d, never), optional passcode, and whether client IPs should be masked.
- **Instant Revocation:** Clicking "Revoke" on an active session immediately invalidates the session cookie and disconnects the analyst.
- **Zero Active Sessions State:** Displays a calm "No analysts currently connected" badge.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Invites List)** | < 100 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright auth audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /admin/share` returns HTTP 200 for Admin; returns HTTP 403 for Analyst.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Invite Creation:** Create a test invite and verify token and link are generated.
- [ ] **4. Session Revocation:** Revoke a test analyst session and verify analyst receives HTTP 401 on next call.
- [ ] **5. Role Verification:** Confirm Analyst Path B receives 403.
- [ ] **6. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/admin-share.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Simulated analyst invite creation, login, and revocation cycle.
