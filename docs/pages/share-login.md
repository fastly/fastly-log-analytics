# Page Specification: Analyst Share Login & Portal (`/share-login`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/share-login`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Analyst Share Login** page is the secure entry point for external analysts accessing Fastly Log Analytics via Analyst Path B (Remote Live Share). It enforces invite token validation, passcode authentication (Argon2id), Terms of Service (TOS) acceptance gating, rate-limiting lockout protection, and session cookie generation.

### Key Tenets:
- **Zero-FOS-Credential Security:** Analysts never receive or store cloud storage credentials.
- **Strict Compliance & Audit:** Mandatory TOS acceptance recorded in SQLite `remote_share.db` before any dashboard route is unlocked.
- **Shared Primitives:** Conforms strictly to authentication layout primitives and theme tokens.

---

## 2. Routes & URL Schema

- **Primary Route:** `/share-login`
- **Supported Query Parameters:**
  - `token`: Pre-filled invite token (e.g. `?token=abc-123`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Unauthenticated User** | Login Interface | Can submit token, passcode, and TOS acceptance. |
| **Authenticated Analyst** | Session Granted | Redirected to `/dashboard` with session cookie; read-only role assigned. |
| **Admin** | Bypass | Admins access dashboard directly without share login. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Auth Database** | Local SQLite singleton `data/system/remote_share.db`. | Shared Postgres or SQLite remote share database. |
| **Password Hashing** | Argon2id (with legacy scrypt verification fallback). | Argon2id password hashing. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/share/invite/{token}`:
   - Validates invite token, returns service name, invite expiry, and whether passcode is required.
2. `POST /api/share/login`:
   - Payload: `{"invite_token": "...", "passcode": "...", "accept_tos": true}`.
   - Sets HTTP-only `analyst_session` cookie; returns session details.

### Telemetry Attribution:
- Authentication attempts and lockouts recorded in `remote_share.db` audit log.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Auth Card Container, Form Input Fields, and TOS Agreement Box enforce container intrinsic sizes (`contain-intrinsic-size: 450px`).
- **In-Place Skeletons:** Renders auth card skeleton (`Verifying share invite token...`).
- **Zero CLS State Transitions:** Error banners appear in reserved alert spaces without shifting input boxes.

---

## 7. Interactive Workflows & Edge Cases

- **TOS Acceptance Gate:** The "Enter Dashboard" button remains disabled until the operator checks the TOS acceptance box.
- **Rate Limit Lockout:** 5 consecutive incorrect passcodes trigger a 15-minute IP/token lockout with countdown timer.
- **Expired Invite:** Display clean error alert if the invite has expired or been revoked by the admin.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 300 ms / < 600 ms | < 300 ms / < 600 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **Auth Verification Latency** | < 200 ms (Argon2id) | < 200 ms (Argon2id) | HAR log / API timing |
| **Telemetry Capture** | 100% auth audits recorded | 100% auth audits recorded | `remote_share.db` audit |
| **Brute Force Protection** | Max 5 attempts / 15 min | Max 5 attempts / 15 min | Rate limit verification |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /share-login` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Invite Token Validation:** Valid token loads service details; invalid token displays error alert.
- [ ] **4. TOS Enforced:** Submit button disabled until TOS checkbox is checked.
- [ ] **5. Successful Login:** Correct passcode and TOS check sets session cookie and redirects to `/dashboard`.
- [ ] **6. Lockout Defense:** 5 invalid passcode attempts trigger temporary lockout.
- [ ] **7. Telemetry & Query Audit:** Verify auth attempts are recorded in `remote_share.db` audit table.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/share-login.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Automated test cases for valid login, wrong password, expired token, and lockout.
