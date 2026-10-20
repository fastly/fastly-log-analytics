# High-scale analyst SQL console: least-privilege ClickHouse identities

On a high-scale deployment the free-form SQL console (`POST /api/query`) runs on ClickHouse. Analysts are an
adversary in this repo's trust model, so their SQL never runs as the application's ClickHouse user.

## How the boundary is enforced

The SQL guard in `backend/high_scale/query.py` (comment/quote/escape rejection, function allow-list, table
filters, wrapped row cap) is the **second** line of defence. The first is the database
([`backend/high_scale/console_access.py`](../../backend/high_scale/console_access.py)). Each service gets its own
ClickHouse user:

| Control | What it does |
|---|---|
| `GRANT SELECT` on `request_facts`, `rum_vitals_facts`, `rum_error_facts`, `high_scale_batch_publications` | No other table, no inserts, no DDL, no `remote()` / `url()` / `file()` / `s3()` sources, no `system.query_log`. |
| Settings profile `fla_console_ro` | `readonly = 1`; memory, threads and timeout are capped with `MAX`, so a query cannot raise its own limits. Only the settings the client sends are `CHANGEABLE_IN_READONLY`. |
| Restrictive row policy per table | Pins every readable table to the user's own `service_id`. No query text and no `SETTINGS` clause can remove it. |

The analyst's time window is still applied by the console through `additional_table_filters`; that narrows rows
inside the service the policy already restricts.

Credentials are derived: `password = HMAC-SHA256(CLICKHOUSE_CONSOLE_SECRET, "console:<service_id>")`. Nothing per
service is stored. The user name is `fla_console_<first 16 hex of sha256(service_id)>`.

## Configuration

| Variable | Meaning |
|---|---|
| `CLICKHOUSE_CONSOLE_SECRET` | At least 32 bytes. Unset means analysts get **no** console (403 `console_unavailable_for_analysts`). Admins are unaffected. |

Set it as a secret on the backend deployment only (the workers do not need it). It is read at process start.

## Provisioning

The backend provisions every registered high-scale service at startup (`provision_configured_services`), using the
application's own ClickHouse user, which therefore needs access management (the bundled ClickHouse chart sets
`CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`). It is idempotent. A failure is logged and leaves that service without
an analyst console; it never blocks startup.

Statement order is deliberate: **the row policy is created before any `SELECT` is granted.** A user with grants
but no policy can read every service, so a half-finished provision must leave a grantless user, never a leaky one.

On first use per process the app also checks the identity (`count() ... WHERE service_id != '<svc>'` must be 0)
and refuses to serve if it is not, so even a mis-provisioned user fails closed.

## Rotating the secret

1. Change `CLICKHOUSE_CONSOLE_SECRET` and restart the backend. Startup re-runs `ALTER USER ... IDENTIFIED` for every
   service, so the old credentials stop working immediately.
2. Open analyst consoles reconnect on their next request.

## Adding a service

Nothing to do: it is provisioned the next time the backend starts. Until then analysts on that service see
`console_unavailable_for_analysts`.

## Verifying

- `RUN_HS_CONSOLE_ACCESS_INTEGRATION=1 uv run pytest tests/high_scale/test_console_access_integration.py` starts a
  real `clickhouse-server` container and proves, through the production client, that an identity sees only its own
  service even when the SQL guard is bypassed, that other tables / table functions / DDL / limit overrides are
  refused by the server, and that rotation locks out the old credentials. Re-run it whenever the ClickHouse image
  is bumped.
- `tests/high_scale/test_console_access.py` pins the generated DDL (order, grants, escaping, profile caps).
- `tests/high_scale/test_query_console_scope.py` and the `sql_console` cases in
  `test_clickhouse_readers_integration.py` pin the SQL guard.

## Rollback

Unset `CLICKHOUSE_CONSOLE_SECRET` and restart: analysts lose the console immediately and nothing else changes. The
users, profile and policies are inert without the secret; remove them with
`DROP USER fla_console_<hash>` / `DROP SETTINGS PROFILE fla_console_ro` if desired.
