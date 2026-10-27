# Page Specification: Admin Live Share Management (`/admin/share`)

> [!IMPORTANT] **Page Specification Status: Live-certified**
> Audited, hardened, and live-certified across Local Standard (`https://localhost:9443`), Remote Standard GCE (`https://localhost:9444`), and Remote High-Scale Elevation (`https://localhost:9445`) on `release/v3.0.0-beta4`.

---

## 1. Overview & Objectives

The **Admin Live Share Management** page (`/admin/share`, implemented in `frontend/app/admin/share/page.tsx` with `SharingControlPanel`, `InvitationsPanel`, `SessionsPanel`, and `AuditLogPanel` under `frontend/components/share-dashboard/`) provides complete administrative control over remote live sharing (Analyst). It governs direct-mode public endpoint activation, analyst invitation CRUD, Argon2id passcode and SSO/OIDC authentication policies, one-shot invite claim tokens, active analyst session monitoring, instant session revocation and panic shutoff, compliance audit trails, Terms of Service (TOS) publishing, and remote frontend domain deployment/teardown.

### Key Tenets:
- **Zero-FOS-Credential Analyst Access:** Grants time-limited, service-scoped read-only access to running instances without exposing Fastly Object Storage keys or admin routes.
- **Session Governance & Panic Shutoff:** Real-time visibility into active analyst sessions with instant per-session revocation (`/revoke` / `/boot`) and emergency panic shutoff (`/panic` stops sharing and revokes all active sessions).
- **Strict RBAC:** Accessible **exclusively** to Admin (`read_write`) operators via loopback or mTLS admin gateway. Analyst sessions receive `403 Forbidden` (`{"error": "admin_only"}`) on every `/api/admin/share/*` endpoint.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/share`
- **Supported Query Parameters:**
  - `tab`: Selected view (`invites`, `sessions`, `audit`; defaults to `invites`, invalid values normalize to `invites`). Switching tabs updates `?tab=` via `window.history.replaceState` (`?tab=` omitted when returning to `invites`).
  - `service`: Optional active `service_id` used to contextualize the header `Deploy Remote Frontend` / `Teardown Remote Frontend` dialog (`DeployRemoteFrontendDialog` / `TeardownRemoteFrontendDialog`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Start/stop/panic live sharing, configure public endpoint & max concurrent sessions, create/update/revoke/reactivate/delete invites, regenerate passcodes & claim links, revoke active sessions, inspect/filter/export/purge audit logs, publish TOS versions, and deploy/teardown remote frontend domains. |
| **Analyst (Remote Share)** | Blocked (`403 Forbidden`) | Rejected by `require_admin` and `RemoteAccessGuardMiddleware` (`{"error": "admin_only"}`) across all `/api/admin/share/*` endpoints. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Share Metadata Store (ADR-22)** | Unified PostgreSQL 16 (`METADATA_DSN`) tables: `remote_invites`, `invite_services`, `remote_sessions`, `remote_share_audit_logs`, `remote_invite_claim_tokens`, `share_settings`, `share_tos_versions` (`backend/core/share_db/`). | Unified PostgreSQL 16 (`METADATA_DSN`) tables: `remote_invites`, `invite_services`, `remote_sessions`, `remote_share_audit_logs`, `remote_invite_claim_tokens`, `share_settings`, `share_tos_versions` (`backend/core/share_db/`). |
| **Tunnel & Session Runtime** | `TunnelManager` (`backend/utils/tunnel/manager.py`) managing direct-mode HTTPS `public_endpoint`, in-memory + PostgreSQL `remote_sessions` state, and login brute-force rate limiters (`rate_limiter.py`). | `TunnelManager` (`backend/utils/tunnel/manager.py`) on the single serving pod backed by shared PostgreSQL `METADATA_DSN` tables. |
| **Live Telemetry Stream** | Multiplexed admin SSE stream (`GET /api/admin/events/stream` `share` channel) + 5-min fallback poll (`GET /api/admin/share/live`). | Multiplexed admin SSE stream (`GET /api/admin/events/stream` `share` channel) + 5-min fallback poll (`GET /api/admin/share/live`). |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit (`backend/routers/share_admin.py` & `backend/routers/sharing_domain.py`):
1. `GET /api/admin/share/status` (`ShareStatusResponse`):
   - Mount-time composite snapshot returning `sharing_active`, `public_endpoint`, `public_url`, `forward_port`, `started_at`, `max_concurrent_sessions`, `active_session_count`, `services`, `invites`, `sessions`, `audit_logs`, `rate_limits`, and `telemetry`.
2. `GET /api/admin/share/live` (`ShareLiveResponse`):
   - Lean live-state snapshot returning `sharing_active`, `public_url`, `active_session_count`, `rate_limits`, and `telemetry`.
3. `POST /api/admin/share/start`, `POST /api/admin/share/stop`, `POST /api/admin/share/panic`:
   - Starts direct-mode sharing on an HTTPS `public_endpoint` (optionally issuing one-shot claim tokens for active invites), stops sharing, or executes an emergency panic shutoff (stops sharing and boots all active sessions).
4. `GET /api/admin/share/invites`, `POST /api/admin/share/invites`, `PATCH /api/admin/share/invites/{invite_id}`:
   - Lists, creates (with Argon2id passcode hashing or OIDC `auth_provider` binding and one-shot `claim_url`), or updates analyst invitations.
5. `POST /api/admin/share/invites/{invite_id}/revoke`, `POST /api/admin/share/invites/{invite_id}/reactivate`, `DELETE /api/admin/share/invites/{invite_id}`:
   - Revokes an invite (booting its live sessions), reactivates a previously revoked invite, or permanently deletes a revoked invite.
6. `POST /api/admin/share/invites/{invite_id}/regenerate-passcode`, `POST /api/admin/share/invites/{invite_id}/reset-passcode`, `POST /api/admin/share/invites/{invite_id}/reissue-link`:
   - Rotates an invite's Argon2id passcode (returning the new plaintext passcode once and booting live sessions) or reissues a fresh 24-hour one-shot claim token.
7. `POST /api/admin/share/sessions/{session_id}/revoke` & `POST /api/admin/share/sessions/{session_id}/boot`:
   - Immediately revokes a live analyst session and records a `SESSION_BOOT` audit event.
8. `GET /api/admin/share/audit-logs`, `DELETE /api/admin/share/audit-logs`, `POST /api/admin/share/purge`:
   - Queries filtered compliance audit events (`email`, `event_type`, `since`, `until`, `limit`), clears audit logs, or runs retention purge across audit logs, expired invites, claim tokens, and idle sessions.
9. `GET /api/admin/share/settings` & `PATCH /api/admin/share/settings` (`ShareSettingsResponse`):
   - Reads or updates `max_concurrent_analyst_sessions` in PostgreSQL `share_settings`.
10. `GET /api/admin/share/tos`, `POST /api/admin/share/tos`, `PUT /api/admin/share/tos` (`ShareTosResponse`):
    - Reads or publishes a new Terms of Service version in PostgreSQL `share_tos_versions`.
11. `GET /api/admin/share/wordphrase` & `GET /api/admin/share/oauth-providers`:
    - Generates high-entropy wordphrase passcodes and lists configured OIDC providers for the invite creation form.
12. `POST /api/admin/share/deploy-domain` & `DELETE /api/admin/share/deploy-domain/{service_id}`:
    - SSE streams that provision or tear down a Fastly remote-sharing frontend domain.

### Telemetry Attribution:
- Every client request from `/admin/share` propagates `X-Page-Load-ID` (`frontend/lib/api.ts` includes `"/api/admin/share"` in `SERVICELESS_PATH_PREFIXES` so global live-share queries execute without waiting on a per-service store selection).
- PostgreSQL statements executed via `backend/core/share_db/` are instrumented through `backend/core/metadata/pg_connection.py` and `sqlite_profiler.record_statement`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Route-Level Skeleton (`frontend/app/admin/share/loading.tsx`):** Mirrors the exact `PageHeader` + amber security `Alert` + `admin-share-control-reservation` (`min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto]`) + `admin-share-panel-reservation` (`min-h-[360px] [contain-intrinsic-size:auto_360px] [content-visibility:auto]`) geometry of `page.tsx`, wrapped in `<React.Suspense fallback={<Loading />}>`.
- **Stable Test Hooks (`data-testid`):**
  - `admin-share-root`, `admin-share-refresh-btn`, `admin-share-status-error`, `admin-share-action-error`
  - `admin-share-control-reservation`, `admin-share-control-panel`, `admin-share-status-badge`, `admin-share-start-btn`, `admin-share-stop-btn`, `admin-share-panic-btn`
  - `admin-share-tabs`, `admin-share-tab-invites`, `admin-share-tab-sessions`, `admin-share-tab-audit`, `admin-share-panel-reservation`
  - `admin-share-invites-panel`, `admin-share-sessions-panel`, `admin-share-audit-panel`
- **Dimmed Updates:** Manual or background refreshes dim the active reservation containers (`opacity-40 pointer-events-none`) without collapsing layout (`CLS <= 0.05`).

---

## 7. Interactive Workflows & Edge Cases

- **Sharing Control Panel (`SharingControlPanel.tsx`):** Displays live `ACTIVE` / `OFF` badge, active/max session counter, public endpoint input (`https://...`), `Start Sharing` / `Stop Sharing` / `Panic` controls, live uptime and heartbeat 401 telemetry, and failed-login lockout banners.
- **Invitations Panel (`InvitationsPanel.tsx`):** Supports creating invites with service multi-select, expiration presets, passcode or OIDC SSO authentication, one-shot claim link copy, passcode reset/regeneration, revoke/reactivate/delete lifecycle actions, and direct drill-through to the `Audit` tab filtered by the analyst's email.
- **Sessions Panel (`SessionsPanel.tsx`):** Displays active analyst sessions with per-minute query sparklines, service scope badges, IP/Geo/ASN attribution, session TTL extension (`+1h`), and instant session revocation (`Boot`).
- **Audit Log Panel (`AuditLogPanel.tsx`):** Filters PostgreSQL `remote_share_audit_logs` by email, event type, and time window (`1h`, `24h`, `7d`, `30d`, `all`), and supports CSV/JSON export and audit log clearing.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP (Warm Load)** | < 1200 ms | < 1200 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (0.00 observed) | <= 0.05 (0.00 observed) | PerformanceObserver (`clsInitScript`) |
| **API Latency (`/api/admin/share/status`)** | < 150 ms (p95) | < 150 ms (p95) | Playwright response timing |
| **Telemetry Capture** | `X-Page-Load-ID` on 100% of calls | `X-Page-Load-ID` on 100% of calls | Playwright request header audit |
| **RBAC Enforcement** | 100% Analyst requests `403` | 100% Analyst requests `403` | `test_remote_share_admin.py` & route tripwire |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /admin/share` returns HTTP 200 for Admin; Analyst receives HTTP 403 (`{"error": "admin_only"}`) on all `/api/admin/share/*` endpoints.
- [x] **2. Zero Layout Shift:** Verified pre-allocated `admin-share-control-reservation` and `admin-share-panel-reservation` containers with `CLS <= 0.05` (`0.00` measured).
- [x] **3. Invite Lifecycle:** Verified invite creation, listing (`GET /api/admin/share/invites`), revocation (`/revoke`), reactivation (`/reactivate`), passcode regeneration (`/regenerate-passcode`), and deletion.
- [x] **4. Session Revocation:** Verified active session revocation (`POST /api/admin/share/sessions/{session_id}/revoke` and `/boot`) immediately invalidates the analyst session.
- [x] **5. Role Verification:** Confirmed Analyst receives `403 Forbidden` (`{"error": "admin_only"}`) across all 18 `/api/admin/share/*` routes.
- [x] **6. Telemetry & Multi-Tier Certification:** Verified `X-Page-Load-ID` propagation and live E2E execution across Local Standard (`:9443`), Remote Standard GCE (`:9444`), and Remote High-Scale Elevation (`:9445`).

---

## 10. Automated Test Suite & Traffic Generation

- **Backend Unit & RBAC Tests:** `tests/routers/test_remote_share_admin.py`, `tests/remote_access/test_share_admin_routes.py`, `tests/remote_access/test_route_table_tripwire.py`.
- **Frontend Unit Tests:** `frontend/__tests__/app/admin/share.test.tsx`.
- **Playwright E2E Suite:** `frontend/e2e/pages/admin-share.spec.ts` (5 contract tests verified locally and across all 3 live mTLS tiers).
