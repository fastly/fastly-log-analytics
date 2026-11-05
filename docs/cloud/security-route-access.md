# Route access matrix

Enforced in `app/src/shell.rs`; the table and registry guard live in `app/tests/route_auth.rs`
(a new route without a declared class fails the test).

Per-page access (roles, nav, and the API routes each page calls with the role each needs) is in the
`## Access` section of every page doc; see [pages/README.md](pages/README.md). The audit test
`app/web/tools/check-page-access.test.ts` checks those sections against this table, `shell.rs`, and
the web nav and role gating.

Gate order: front key, setup gate, Origin and JSON checks for non-GET, then session. Sessions are
HMAC-checked and the role is reloaded from the store on every request.

| Class | Routes | Anonymous | Analyst (no PII) | Analyst | Admin |
|---|---|---|---|---|---|
| Public | `/api/auth/*`, `/api/setup`, `/api/setup/accept`, static pages, `/404` | yes | yes | yes | yes |
| Session | `/api/services` (redacted for non-admins), `/api/log-fields/catalog`, `/api/rum/versions`, `/api/log-extents`, `/api/files`, `POST /api/telemetry/query` (browser query beacon: 4 KiB cap, allowlisted fields, 60 per session per minute, same-origin POST rules) | 401 | yes | yes | yes |
| No-PII denied | `/api/file` | 401 | 403 | yes | yes |
| Admin | `/api/sync-status`, `/api/cron-runs*`, `/api/admin/*` (including `/api/admin/operational-logging`, `/api/admin/logs/*`, `/api/admin/usage/*`), `/api/logs/:kind/live`, `/api/provision/*` | 401 | 403 | 403 | yes |
| Front key | `/fos/<service>/<key>` | needs `x-fastly-key` | n/a | n/a | n/a |
| Tick | `/_tick`, `/_flush` (exact paths only; `/_tick/` and `/_flush/` return 403) | VCL-bucketed | n/a | n/a | n/a |

`/api/admin/audit-logs` and `/api/sync-status` `audit_logs` return Service History only (service and infrastructure changes). User and admin actions are read from `/api/logs/admin_audit/live` and the archived `admin_audit` dataset; see the runbook's "Service History vs Audit".

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
