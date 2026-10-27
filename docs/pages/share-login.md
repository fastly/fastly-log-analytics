# Page Specification: Analyst Share Login & Portal (`/share-login`)

> [!IMPORTANT] **Page Specification Status: Live-certified**
> Audited, hardened, and live-certified across Local Standard, Remote Standard GCE, and High-Scale Elevation on `release/v3.0.0-beta3`. Verified with backend unit tests (`tests/remote_access/`), OpenAPI contract snapshot (`tests/test_openapi_snapshot.py`), Vitest component tests (`frontend/__tests__/app/share-login.test.tsx` & `frontend/__tests__/app/share-login/*.test.tsx`), and Playwright E2E integration tests (`frontend/e2e/pages/share-login.spec.ts`).

---

## 1. Overview & Objectives

The **Analyst Share Login** page (`/share-login`) and its companion **Terms of Access** gate (`/share-login/acknowledge`) form the authentication and compliance entry point for external analysts accessing Fastly Log Analytics via **Analyst (Remote Live Share)**. It enforces one-shot invite claim token validation (`POST /api/share/claim/{token}`), Argon2id passcode authentication (`POST /api/share/login`), optional OAuth/OIDC SSO flows (`GET /api/share/oauth/authorize` and `GET /api/share/oauth/callback`), two-cookie Terms of Service (TOS) isolation (`analyst_pending_session_id` upgraded and rotated to `analyst_session_id` via `POST /api/share/acknowledge`), brute-force rate-limiting lockout protection (`429`), IP whitelist enforcement (`403`), global session capacity limits (`503`), and open-redirect sanitization (`safeReturnTarget`).

### Key Tenets:
- **Zero-FOS-Credential Security:** Analysts never receive or store cloud storage credentials; all access is scoped to the invited `service_ids` via server-side `RequestContext` and `RemoteAccessMiddleware`.
- **Two-Cookie Session Rotation & TOS Isolation:** When `tos_pending: true`, `POST /api/share/login` issues an HTTP-only `analyst_pending_session_id` cookie (`SameSite=Strict`, `Path=/`, `Max-Age=86400`) that is rejected by all protected analytics endpoints until the analyst acknowledges the current published TOS version at `/share-login/acknowledge` (`POST /api/share/acknowledge`), which atomically records acceptance in PostgreSQL (`remote_invites`), rotates the session ID to prevent session fixation across the TOS boundary, sets `analyst_session_id`, and deletes `analyst_pending_session_id`.
- **Zero-CLS Layout Reservations:** Card shells (`[contain-intrinsic-size:auto_380px]`), form containers (`[contain-intrinsic-size:auto_240px]`), and contextual loading skeletons (`Verifying share session & security policies...`) prevent layout shifts during auth-config probing and login submission (`CLS <= 0.05`).

---

## 2. Routes & URL Schema

- **Primary Routes:**
  - `/share-login` — Analyst sign-in portal (passcode & OAuth/OIDC SSO).
  - `/share-login/acknowledge` — Terms of Access review and acknowledgment gate.
- **Supported Query Parameters:**
  - `token` / `invite`: Optional one-shot invite claim token (e.g. `?token=abc-123`). Redeemed via `POST /api/share/claim/{token}` on mount to pre-fill the analyst's email and display a verified invitation banner (`data-testid="share-login-invite-banner"`), or surface an expired/invalid alert on HTTP `404`/`410`.
  - `return` / `next`: Post-authentication in-app redirect path (e.g. `?return=/query` or `?next=/performance`). Strictly validated via `safeReturnTarget` to block protocol-relative (`//evil.example`), backslash (`/\\evil.example`), whitespace-smuggled (`/\t/evil.example`), absolute external URLs (`https://evil.example`), and `/share-login` self-loops, falling back to `/dashboard`.
  - `oauth_error`: Fixed allowlist error code (`unverified_email`, `not_invited`, `wrong_domain`, `idp_unavailable`) mapped to non-enumerating user-facing copy (raw query parameter is never reflected).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Unauthenticated User** | Public Auth Surface Only | Can reach `/share-login`, `/share-login/acknowledge`, `GET /api/share/auth-config`, `POST /api/share/claim/{token}`, `POST /api/share/login`, `POST /api/share/logout`, and `/api/share/oauth/*`. All other `/api/*` endpoints return `401 {"error": "unauthenticated"}`. |
| **Pending-TOS Analyst (`analyst_pending_session_id`)** | TOS Gate Only | Can reach `GET /api/share/tos`, `POST /api/share/acknowledge`, `GET /api/share/heartbeat`, and `POST /api/share/logout`. Protected analytics endpoints (`/api/sources`, `/api/dashboard/*`, etc.) reject the pending cookie with `401`. |
| **Authenticated Analyst (`analyst_session_id`)** | Scoped Read-Only Analytics | Redirected to validated `return`/`next` target or `/dashboard`; scoped strictly to invited `service_ids`; optional IP masking (`mask_ips_for`) enforced server-side; all `/api/admin/*` and mutation routes blocked (`403`). |
| **Admin (`read_write`)** | Direct Loopback / Admin Tunnel | Bypasses analyst login gate on loopback; `ServerFooter` renders `Admin View • ...` on loopback while remaining suppressed (`null`) on public Caddy-proxied analyst traffic (`x-proxied-by-caddy: true`). |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Auth & Session Store** | Unified PostgreSQL 16 (`METADATA_DSN`, ADR-22) tables: `remote_invites`, `remote_sessions`, `remote_share_audit_logs`, `share_settings`, `share_tos_versions`, `share_claim_tokens` + in-memory `TunnelManager` cache with on-demand PostgreSQL session rehydration. | Unified PostgreSQL 16 (`METADATA_DSN`, ADR-22) shared across serving pods with on-demand session rehydration in `TunnelManager.validate_session`. |
| **Password Hashing** | Argon2id (`argon2-cffi`) with transparent verification fallback and rehash-on-login for legacy scrypt hashes. | Argon2id (`argon2-cffi`) with transparent verification fallback and rehash-on-login for legacy scrypt hashes. |
| **Brute-Force Rate Limiting** | Per-IP sliding-window rate limiter (`TunnelManager._rate_limiter`): 5 failed attempts trigger a 15-minute lockout (`HTTP 429` with `Retry-After` header and `retry_after_s` countdown). Successful logins do not clear prior IP failures (preventing interleaved brute-force bypass). | Same per-IP sliding-window rate limiter and audit logging in `remote_share_audit_logs`. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit (`backend/routers/share_auth.py` & `backend/routers/share_oauth.py`):
1. `GET /api/share/auth-config` (`AuthConfigResponse`):
   - Unauthenticated. Returns `{ passcode_enabled: bool, oauth_enabled: bool, oauth_provider_label: str | null, providers: [{ id, display_name }] }`. Frontend fails open to the passcode form if this probe fails.
2. `POST /api/share/claim/{token}` (`ShareClaimResponse`):
   - Unauthenticated one-shot reveal. Returns `{ name, email, passcode, share_url, expires_at }` on `200 OK`, `404 {"detail": {"error": "invalid_or_used"}}` if unknown/already consumed, or `410 {"detail": {"error": "invite_gone"}}` if the underlying invite was revoked or expired.
3. `POST /api/share/login` (`ShareLoginPayload` -> `ShareLoginResponse`):
   - Payload: `{"email": "...", "passcode": "..."}`.
   - Verifies Argon2id passcode, IP CIDR whitelist (`403 ip_not_whitelisted`), rate limit (`429 rate_limited` / `locked`), and atomic global session cap (`503 capacity_exceeded`).
   - Sets HTTP-only `analyst_pending_session_id` (when `tos_pending=True`) or `analyst_session_id` (when `tos_pending=False`), deletes the opposite cookie, and returns `{ ok: true, name, email, tos_pending, tos, service_ids, redirect }` (never echoing the bearer `session_id` in the JSON body).
4. `GET /api/share/tos` (`TosDocument`):
   - Session-gated (`analyst_pending_session_id` or `analyst_session_id`). Returns `{ version, text }` for the latest published TOS in `share_tos_versions`.
5. `POST /api/share/acknowledge` (`TosAckPayload` -> `ShareAcknowledgeResponse`):
   - Payload: `{"version": "..."}`. Validates exact match against the current published TOS version (`400 invalid_tos_version` on mismatch), records `tos_version` and `tos_accepted_at` on `remote_invites`, writes `TOS_ACCEPTED` to `remote_share_audit_logs`, rotates the session ID via `mgr.rotate_session_id()`, sets `analyst_session_id`, and deletes `analyst_pending_session_id`.
6. `GET /api/share/heartbeat` (`ShareHeartbeatResponse`):
   - Session liveness probe (`{ ok: true }`). Bumps `last_active_time` only when `X-User-Active: 1` is present so abandoned background tabs still expire at the idle timeout.
7. `POST /api/share/logout` (`ShareLogoutResponse`):
   - Boots the active session in `TunnelManager` and PostgreSQL `remote_sessions`, clears both session cookies, and returns `{ ok: true }`.
8. `GET /api/share/oauth/authorize` & `GET /api/share/oauth/callback`:
   - OIDC PKCE (`S256`) + state + nonce authorization flow issuing the same two-cookie TOS-isolated session upon IdP callback verification.

### Telemetry & Audit Attribution:
- All frontend `/api/share/*` requests go through `analystFetch` (`frontend/lib/analystFetch.ts`), which attaches the active `X-Page-Load-ID` correlation header (`getCurrentPageLoadId()`).
- Every authentication event (`LOGIN_SUCCESS`, `LOGIN_FAIL`, `TOS_ACCEPTED`, `SESSION_BOOT`, `INVITE_CREATE`, `INVITE_REVOKE`, `INVITE_DELETE`) is recorded in PostgreSQL `remote_share_audit_logs` with sanitized email (`safe_audit_email`, stripped of control characters and bounded to 254 chars) and client IP.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Zero-CLS Layout Reservations:**
  - `ShareLoginPage` (`frontend/app/share-login/page.tsx`): Card container (`data-testid="share-login-card"`) reserves `min-h-[380px] [contain-intrinsic-size:auto_380px] [content-visibility:auto]`; card content (`data-testid="share-login-card-content"`) reserves `min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto]`.
  - `ShareLoginForm` (`frontend/app/share-login/ShareLoginForm.tsx`): Form container (`data-testid="share-login-form-container"`) reserves `min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto]`.
  - `AcknowledgePage` (`frontend/app/share-login/acknowledge/page.tsx`): Card container (`data-testid="share-acknowledge-card"`) reserves `min-h-[360px] [contain-intrinsic-size:auto_360px] [content-visibility:auto]`.
- **In-Place Contextual Skeletons:**
  - Route-level `loading.tsx` (`AuthCardSkeleton`) and mount-time `configState === 'loading'` (`data-testid="share-login-loading-skeleton"`) render `"Verifying share session & security policies..."` inside pre-allocated intrinsic containers.
- **Dimmed Busy Transitions:**
  - While `POST /api/share/login` or `POST /api/share/acknowledge` is in-flight, form inputs (`data-testid="share-login-fields"`, `data-testid="share-acknowledge-actions"`) transition smoothly with `opacity-40 pointer-events-none transition-opacity` without collapsing layout height.

---

## 7. Interactive Workflows & Edge Cases

- **Invite Claim Prefill (`?token=` / `?invite=`):** Redeems the one-shot claim token on mount, pre-filling the Email input and rendering `Invitation verified for {name || email}`. Displays `"This invitation link has expired or is no longer valid."` on `404`/`410`.
- **Passcode Visibility Toggle:** Accessible `"Reveal passcode"` / `"Hide passcode"` icon button toggles the passcode input between `type="password"` and `type="text"`.
- **Per-Status Error Feedback:**
  - `401`: `"Invalid email or passcode."`
  - `403` (`ip_not_whitelisted`): `"Your IP address is not on the approved list for this invitation."`
  - `429`: `"Too many failed attempts — temporarily locked out. Retry in {N}s."` with live 1-second countdown and disabled submit button.
  - `503`: `"The dashboard is at capacity. Try again shortly."`
- **TOS Acknowledgment Gate (`/share-login/acknowledge`):** Server-rendered (RSC) TOS text with client `AcknowledgeFallback` resilience; clicking `"I acknowledge"` (`data-testid="share-acknowledge-submit"`) posts the exact TOS version string and performs a full document navigation (`window.location.assign`) to flush pre-login React Query bootstrap caches.
- **Open-Redirect Protection:** `safeReturnTarget` validates `?return=` and `?next=` on both `/share-login` and `/share-login/acknowledge`.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / Warm Reload** | < 400 ms | < 400 ms | Web Vitals / Playwright (`share-login.spec.ts`) |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (0.000 measured) | <= 0.05 (0.000 measured) | `PerformanceObserver` (`layout-shift`) |
| **Auth Verification Latency** | < 250 ms (Argon2id) | < 250 ms (Argon2id) | Playwright E2E / API timing |
| **Telemetry & Audit Capture** | 100% auth events recorded | 100% auth events recorded | `remote_share_audit_logs` + `X-Page-Load-ID` |
| **Brute Force Protection** | Max 5 attempts / 15 min | Max 5 attempts / 15 min | `test_share_auth_routes.py` & `test_rate_limiter.py` |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility & Footer Indicator:** `GET /share-login` returns HTTP 200, mounts `[data-testid="share-login-card"]`, and renders `ServerFooter` (`Admin View`) on loopback while suppressing it for remote Caddy analysts.
- [x] **2. Zero Layout Shift:** Pre-allocated `[contain-intrinsic-size:...]` and `[content-visibility:auto]` containers achieve `CLS = 0.000` (`<= 0.05`) and warm `FCP < 400ms`.
- [x] **3. Invite Token Validation:** Valid `?token=` claims invite and pre-fills email; invalid/expired/revoked token displays destructive alert.
- [x] **4. Two-Cookie TOS Isolation & Upgrade:** `tos_pending: true` sets `analyst_pending_session_id`, blocks `/api/sources` (`401`), routes to `/share-login/acknowledge`, validates TOS version on `POST /api/share/acknowledge`, and rotates cookie to `analyst_session_id`.
- [x] **5. Successful Login & Redirect:** Valid credentials + TOS acknowledgment redirect to validated `?next=` / `?return=` target or `/dashboard` and pass `GET /api/share/heartbeat`.
- [x] **6. Lockout & Security Defenses:** 5 invalid attempts trigger HTTP `429` lockout with `Retry-After`; off-whitelist IPs receive `403`; capacity cap returns `503`; hostile `?next=//evil.example` open redirects are neutralized.
- [x] **7. Telemetry & Audit Trail:** `X-Page-Load-ID` propagated via `analystFetch`; all auth mutations recorded in PostgreSQL `remote_share_audit_logs`.

---

## 10. Automated Test Suite & Traffic Generation

- **Backend Unit & Route Tests:** `tests/remote_access/test_share_auth_routes.py`, `tests/remote_access/test_route_table_tripwire.py`, `tests/remote_access/test_middleware.py`, `tests/remote_access/test_rate_limiter.py`.
- **Frontend Unit Tests (Vitest):** `frontend/__tests__/app/share-login.test.tsx`, `frontend/__tests__/app/share-login/page.test.tsx`, `frontend/__tests__/app/share-login/acknowledge.test.tsx`, `frontend/__tests__/app/share-login/ShareLoginForm.oauth.test.tsx`, `frontend/__tests__/app/share-login/safeReturnTarget.test.ts`.
- **Playwright E2E Suite:** `frontend/e2e/pages/share-login.spec.ts` (5 end-to-end contract tests).
