# Route access matrix

Enforced in `compute/cloud/src/shell.rs`; the table and registry guard live in `compute/cloud/tests/route_auth.rs`
(a new route without a declared class fails the test).

Per-page access (roles, nav, and the API routes each page calls with the role each needs) was in the
`## Access` section of every FLP page doc; the shared [page docs](../pages/README.md) do not carry
those sections. The audit test `frontend/lib/cloud-engine/tools/check-page-access.test.ts` now checks
only the nav gating of Data Management and Usage & Cost, and the role mapping and log redaction in
`main.rs` and `auth/mod.rs`.

Gate order: front key, setup gate, Origin and JSON checks for non-GET, then session. Sessions are
HMAC-checked and the role is reloaded from the store on every request.

| Class | Routes | Anonymous | Analyst (no PII) | Analyst | Admin |
|---|---|---|---|---|---|
| Public | `/api/auth/*`, `/api/setup`, `/api/setup/accept`, static pages, `/404` | yes | yes | yes | yes |
| Session | `/api/services` (redacted for non-admins), `/api/log-fields/catalog`, `/api/rum/versions`, `/api/log-extents`, `/api/files`, `POST /api/telemetry/query` (browser query beacon: 4 KiB cap, allowlisted fields, 60 per session per minute, same-origin POST rules) | 401 | yes | yes | yes |
| No-PII denied | `/api/file` | 401 | 403 | yes | yes |
| Admin | `/api/sync-status`, `/api/cron-runs*`, `/api/admin/*` (including `/api/admin/operational-logging`, `/api/admin/logs/*`, `/api/admin/usage/*`), `/api/logs/:kind/live`, `/api/provision/*` | 401 | 403 | 403 | yes |
| Front key | `/fos/<service>/<key>` | needs `x-fastly-key` | n/a | n/a | n/a |
| Tick | `/_tick`, `/_flush` (exact paths only; `/_tick/` and `/_flush/` return 403) | VCL-bucketed; a continuation with the CDN key (`x-flp-continue` + `x-fastly-key`, which only `flp` holds) passes the cache | n/a | n/a | n/a |

`/api/admin/audit-logs` and `/api/sync-status` `audit_logs` return Service History only (service and infrastructure changes). User and admin actions are read from `/api/logs/admin_audit/live` and the archived `admin_audit` dataset.

`/api/files?dataset=ops:*` and `/api/file` keys under the operational-log root are admin-only
(403 for analysts, no-PII and anonymous 401), even though the routes are otherwise Session or
No-PII classes. Operational-settings changes and every `/api/admin/logs/*` and
`/api/admin/usage/*` action append an admin audit entry; rejected or no-op requests do not.

Non-admin `/api/services` keeps only `service_id`, `name`, `status`, `logging_enabled`,
`created_at` and `updated_at`.

## Live evidence (isolated install, 2026-10-30)

Anonymous (every API route in the page docs' Access tables, 43 probes): all Session, No-PII
denied and Admin routes return 401; public auth and setup routes answer (200, or 422 on an empty
body); `/_tick/` 403. Anonymous `/admin`, `/provision`, `/services`, `/log-fields` (and `.html`,
`.txt`) redirect 303 to `/sign-in`; `/setup` redirects to `/admin`; unbuilt pages (`/control-room`,
`/cloud-files`, `/admin/queue`) return 404. The admin session gets 200 on the admin routes it
read-probed (`sync-status`, `cron-runs`, `admin/raw-tree`, `storage-tree`, `ingested-files`,
`logs/status`, `audit-logs`).

Not yet live: `/logs` and `/usage` are admin pages in the repo (anonymous 303, analysts 403), but
the isolated install predates that change and still serves their HTML (200) to anonymous visitors.
The pages hold no data and every data route is 401; deploying the new build closes it.

## Not yet done

- Live wrong-role calls (analyst and no-PII sessions) against a deployed install.
- Page-level wrong-role checks for analysts (needs a non-admin session; none was minted).
- Residual: reliance on Fastly stripping a client `Fastly-FF`; `GET /api/setup` lists store names
  until setup completes.

`/api/sync-status` and `/api/cron-runs` are served from Core Cache, but only after
the session and admin role check; the cache key includes the service id, so no
cached body reaches an unauthorized caller.

## Edge-cached page shells

Page HTML and RSC `.txt` files are the build's static export: no nonces, no
per-user, per-session or per-install data. The front VCL caches them for 60
seconds, keyed by path (query dropped), host and the deployed build SHA. Deploy
also purges `flp-html`. A response with `Set-Cookie`, a redirect or any
non-200 status is never cached.

- Analytics pages (`/dashboard`, `/origin`, `/sessions`, `/query` and the rest)
  were already public in Compute (`public, max-age=60`). Serving them from cache
  changes nothing about exposure. Their data stays behind the APIs above, and the
  app sends signed-out visitors to `/share-login`.
- Admin pages (`/admin*`, `/logs*`, `/provision*`, `/log-fields*`, `/services*`)
  come from cache only when the edge verifies the request's `flp_cdn` credential.
  The credential must have a valid HMAC, the admin role, an unexpired time, the
  current `auth_epoch`, and a binding to the presented `flp_session`. That is the
  same check that already admits ops parquet. Every other request reaches
  Compute, which still returns the 303, 403 and cookie clearing. Cached admin
  copies are kept apart in the cache key and keep `private, no-store` and
  `Vary: Cookie` for browsers. A role change or revocation bumps the epoch, so
  the next navigation falls back to Compute. The credential lasts at most 180
  seconds.
- `/api`, `/auth`, `/setup` and `/share-login` always pass to Compute.
