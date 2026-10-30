# Analyst Share Login (`/share-login`)

Sign-in and terms-of-access flow for remote analysts using a live shared instance (see [admin/share.md](admin/share.md) for the admin side). Analysts get no storage credentials; every request runs through the admin's process, scoped to the invited services by `RemoteAccessMiddleware` and `RequestContext`.

Source: [`frontend/app/share-login/page.tsx`](../../frontend/app/share-login/page.tsx) (sign-in) and [`frontend/app/share-login/acknowledge/page.tsx`](../../frontend/app/share-login/acknowledge/page.tsx) (terms gate, server-rendered with the current terms fetched through `fetchTosServerSide`, `force-dynamic`); both have a `loading.tsx`.

## URL parameters

- `token` or `invite`: one-shot invite claim token. Redeemed through `POST /api/share/claim/{token}` on mount; it pre-fills the email and shows "Invitation verified for ...". An invalid or used token shows "This invitation link has expired or is no longer valid."
- `return` or `next`: in-app path to open after sign-in, validated by `safeReturnTarget` (rejects protocol-relative, backslash and other open-redirect forms) on both pages; default `/dashboard`.
- `oauth_error`: one of a fixed allow-list of codes (for example `unverified_email`, `not_invited`, `wrong_domain`, `idp_unavailable`) mapped to fixed copy; the raw value is never reflected.

## Flow and behavior

1. The page probes `GET /api/share/auth-config` to decide which modes to show (passcode form, SSO provider buttons). If the probe fails the page falls back to the passcode form.
2. Passcode sign-in posts `{email, passcode}` to `POST /api/share/login`. Errors shown: 401 "Invalid email or passcode.", 403 `ip_not_whitelisted` "Your IP address is not on the approved list for this invitation.", 429 "Too many failed attempts — temporarily locked out." with a countdown, 503 "The dashboard is at capacity. Try again shortly." A passcode visibility toggle is provided.
3. If the invite's terms are not yet accepted (`tos_pending`), the server sets the `analyst_pending_session_id` cookie instead of `analyst_session_id` and the page routes to `/share-login/acknowledge`. The pending cookie only reaches the terms and session endpoints; protected analytics routes reject it. Acknowledging posts the exact terms version, after which the session id is rotated into the full `analyst_session_id` cookie and the pending cookie is cleared.
4. Both cookies are HTTP-only, `SameSite=Strict`, `Path=/`, 24-hour max age, and `Secure` when the public endpoint is HTTPS.
5. SSO (OAuth/OIDC with PKCE, state and nonce) is an opt-in alternative: `GET /api/share/oauth/authorize` and `/oauth/callback` are top-level navigations that end in a redirect to this page or the app, and converge on the same two-cookie session. Providers are configured through the gitignored `data/system/oauth_providers.json` and `OAUTH_<KEY>_CLIENT_ID` / `_CLIENT_SECRET`; the flow is inert unless `OAUTH_FLOW_STATE_SECRET` is set. `SHARE_PASSCODE_LOGIN_ENABLED=0` disables passcode login at the endpoint.

## Backend

Routes are in [`backend/routers/share_admin/sessions_and_tos.py`](../../backend/routers/share_admin/sessions_and_tos.py) (`share_auth_router`, prefix `/api/share`) and [`backend/routers/share_admin/invites.py`](../../backend/routers/share_admin/invites.py) (`share_oauth_router`); models in [`backend/models/admin/sharing.py`](../../backend/models/admin/sharing.py).

| Endpoint | Notes |
|---|---|
| `GET /api/share/auth-config` | Unauthenticated: `passcode_enabled`, `providers`. |
| `POST /api/share/claim/{token}` | Unauthenticated one-shot reveal (`ShareClaimResponse`); 404 `invalid_or_used`, 410 `invite_gone` for a revoked or deleted invite. |
| `POST /api/share/login` | Passcode login. Checks the invite's CIDR whitelist, the per-IP rate limit, and the global session cap. Returns `{ok, name, email, tos_pending, tos, service_ids, redirect}`; the session id is never in the body. |
| `GET /api/share/tos`, `POST /api/share/acknowledge` | Current terms; accept (`400 invalid_tos_version` on mismatch). |
| `GET /api/share/heartbeat` | Liveness; refreshes the idle deadline only when the request carries `X-User-Active: 1`. |
| `POST /api/share/logout` | Ends the session and clears both cookies. |
| `GET /api/share/oauth/authorize`, `/oauth/callback` | SSO. |

Passcodes are hashed with argon2id (legacy scrypt hashes are verified and rehashed on login). The per-IP limiter locks an address for 5 minutes after 5 failures within 60 seconds (`LOGIN_FAILURE_*` in [`backend/utils/tunnel/__init__.py`](../../backend/utils/tunnel/__init__.py)), returning 429 with `Retry-After`. Sessions, invites, terms versions and the audit trail (`remote_share_audit_logs`, including login success/failure, terms acceptance and session events) are PostgreSQL tables in [`backend/core/share_db/`](../../backend/core/share_db/). Admin-side paths (`/api/admin/share/*`) are blocked for analysts.

## Tests

- Frontend: [`frontend/__tests__/app/share-login.test.tsx`](../../frontend/__tests__/app/share-login.test.tsx)
- Backend: [`tests/remote_access/test_middleware.py`](../../tests/remote_access/test_middleware.py), [`tests/remote_access/test_oauth.py`](../../tests/remote_access/test_oauth.py), [`tests/remote_access/test_share_db.py`](../../tests/remote_access/test_share_db.py)
- E2E: `Analyst Share Login Page Contract` and `analyst OAuth login` in [`frontend/e2e/zz-analyst-share-login-lockout.spec.ts`](../../frontend/e2e/zz-analyst-share-login-lockout.spec.ts)
