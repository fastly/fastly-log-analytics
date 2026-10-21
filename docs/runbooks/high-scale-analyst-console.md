# High-scale analyst SQL console: least-privilege ClickHouse identities

On a high-scale deployment the free-form SQL console (`POST /api/query`) runs on ClickHouse. Analysts are an
adversary in this repo's trust model, so their SQL never runs as the application's ClickHouse user.

## How the boundary is enforced

The SQL guard in `backend/high_scale/query.py` (comment/quote/escape rejection, function allow-list, table
filters, wrapped row cap) is the **second** line of defence. The first is the database
([`backend/high_scale/console_access.py`](../../backend/high_scale/console_access.py)). Each service gets **two**
ClickHouse users, `masked` (for invites with `mask_ips`) and `raw`, and each user:

| Control | What it does |
|---|---|
| Reads only its own three **views** (`request_facts`, `rum_vitals_facts`, `rum_error_facts`) | The users have `GRANT SELECT` on those views and nothing else: no base table, no publication table, no other identity's views, no `system.query_log` / `system.users`, no `remote()` / `url()` / `file()` / `s3()` sources, no inserts, no DDL. |
| Views fix the service and the publication fence | Each view is `WHERE service_id = '<svc>'` plus the visible-batch fence and runs with `SQL SECURITY DEFINER`, so the identity never needs access to the base tables. No query text and no `SETTINGS` clause can reach another service. |
| Masked views redact in the data | In the `masked` views `client_ip` and every `custom_fields` / `cmcd` key in `ip`, `client_ip`, `ip_address`, `remote_addr`, `cookie_session`, `cid`, `client_id` read `'[redacted]'` (empty stays empty). The data never leaves ClickHouse unmasked, so `replaceAll(client_ip, ...)`, `substring`, `reverse`, `extract`, `mapValues(custom_fields)` and every other string trick return only redacted text. The masked user has no grant on the raw view. |
| Settings profile `fla_console_ro` | `readonly = 1`; memory, threads and timeout are capped with `MAX`; per-user memory (2 GiB) and result size (64 MiB) ceilings cannot be changed. Only the settings the client sends are `CHANGEABLE_IN_READONLY`. |

The analyst's time window is still applied by the console through `additional_table_filters` keyed on the view
name; it narrows rows inside the service the view already restricts. The router picks the identity from the
invite's `mask_ips` policy (`mask_ips_for(analyst_session)`): masked invites can only ever query the masked views.

Credentials are derived: `password = HMAC-SHA256(CLICKHOUSE_CONSOLE_SECRET, "console:<service_id>:<m|r>")`. Nothing
per service is stored. User names are `fla_console_<first 16 hex of sha256(service_id)>_m` / `_r`; views are
`fla_cv_<same hash>_<m|r>_<table>`.

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

Statement order is deliberate: **each user's views are created and verified present before that user is granted
anything.** A half-finished provision leaves a grantless user, never a leaky one. Provisioning also removes the
pre-view identity (`fla_console_<hash>` with base-table grants and a row policy), re-creates the views with
`CREATE OR REPLACE` (so a tampered view is repaired), and re-applies the profile and password.

On first use per process the app also checks each identity: every view must return no row for another
`service_id`, and a masked identity must see only `'[redacted]'`/empty `client_ip`. If either check fails it refuses
to serve, so even a mis-provisioned user fails closed.

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
- `tests/high_scale/test_console_access.py` pins the generated DDL (views, redaction, grants, order, escaping,
  profile caps) and the runtime identity checks.
- `tests/high_scale/test_query_console_scope.py` and the `sql_console` cases in
  `test_clickhouse_readers_integration.py` pin the SQL guard.

## Rollback

Unset `CLICKHOUSE_CONSOLE_SECRET` and restart: analysts lose the console immediately and nothing else changes. The
users, views and profile are inert without the secret; remove them with `DROP USER fla_console_<hash>_m`,
`DROP USER fla_console_<hash>_r`, the `fla_cv_<hash>_*` views and `DROP SETTINGS PROFILE fla_console_ro` if desired.
