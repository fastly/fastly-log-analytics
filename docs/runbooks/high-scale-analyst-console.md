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
| `CLICKHOUSE_CONSOLE_SECRET` | At least 32 bytes with at least 8 distinct byte values. Unset means analysts get **no** console (403 `console_unavailable_for_analysts`). Admins are unaffected. |
| `CLICKHOUSE_PROVISION_USER` / `CLICKHOUSE_PROVISION_PASSWORD` | Optional startup-only account used for provisioning instead of the application user (see "Keeping access management away from the application user"). |

Set it as a secret on the backend deployment only (the workers do not need it). It is read at process start.

## Provisioning

The backend provisions every registered high-scale service at startup (`provision_configured_services`), using the
application's own ClickHouse user, which therefore needs access management (the bundled ClickHouse chart sets
`CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`). It is idempotent. A failure is logged and leaves that service without
an analyst console; it never blocks startup.

Statement order is deliberate: **each user's views are created and verified present before that user is granted
anything.** A half-finished provision leaves a grantless user, never a leaky one. Provisioning also removes the
pre-view identity (`fla_console_<hash>` with base-table grants and a row policy), drops and re-creates the views (so
a tampered view is repaired), revokes any stray grants from the identities, and re-applies the profile and password.
A column added to a base table later is invisible to both identities until provisioning runs again (views fix their
column list when created), and a masked view refuses to be created over a column it cannot redact.

On first use per process the app also checks each identity: every view must return no row for another
`service_id`, and a masked identity must see only `'[redacted]'`/empty `client_ip`. If either check fails it refuses
to serve, so even a mis-provisioned user fails closed.

## Keeping access management away from the application user (recommended)

By default the application's own ClickHouse user provisions the identities, which requires it to hold access
management (the ClickHouse chart sets `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`). Anything that can run SQL as that
user could then mint users and grants. To avoid it, give provisioning its own account that is used only during
backend startup and then closed:

1. **ClickHouse chart** (`deploy/chart/clickhouse`): set `provisioner.enabled=true`, `provisioner.password=<secret>`
   and `auth.accessManagement=false`. The chart then defines `fla_provisioner` in `users.d` (password read from
   the `CLICKHOUSE_PROVISIONER_PASSWORD` env var, which comes from the auth secret) holding exactly
   `GRANT ACCESS MANAGEMENT ON *.*` and `GRANT SELECT, CREATE VIEW, DROP VIEW ON <database>.* WITH GRANT OPTION`,
   and starts the application user without access management. **Applying this restarts ClickHouse**, so roll it
   out in a maintenance window.
2. **Backend**: set `CLICKHOUSE_PROVISION_USER=fla_provisioner` and `CLICKHOUSE_PROVISION_PASSWORD` (a secret,
   backend only). Startup provisioning then runs on a separate connection as that account and closes it; if the
   variables are unset the application client is used (the previous behaviour).

The provisioner never gets `DROP TABLE`, `INSERT` or `CREATE TABLE`: it can read the data tables (its views run with
its rights) and manage access, and nothing else. For that reason views are dropped and created rather than
`CREATE OR REPLACE`d (which works through a temporary table and needs `DROP TABLE`); the brief gap between the two
statements fails closed. Do not combine `<access_management>` with `<grants>` in the `users.d` XML: ClickHouse
refuses to start with that user definition. `tests/high_scale/test_console_provisioner_chart_integration.py` renders
the chart with `helm`, boots a real server from the rendered configuration and proves all of the above.

### Elevation (company `standard-chart` release)

Elevation's ClickHouse (`fastly-log-analytics-clickhouse`) is a release of the company `standard-chart` deployed
by the platform tooling, not of `deploy/chart/clickhouse`, so apply the same change through that release's values
(`app.components[name=clickhouse]`), mirroring how `logger-override.xml` is already injected:

```yaml
configs:
  provisioner-user.xml: |        # same XML the chart renders and the integration test boots
    <clickhouse>
      <users>
        <fla_provisioner>
          <password from_env="CLICKHOUSE_PROVISIONER_PASSWORD"/>
          <networks><ip>::/0</ip></networks>
          <profile>default</profile>
          <quota>default</quota>
          <grants>
            <query>GRANT ACCESS MANAGEMENT ON *.* WITH GRANT OPTION</query>
            <query>GRANT SELECT, CREATE VIEW, DROP VIEW ON fastly_log_analytics.* WITH GRANT OPTION</query>
          </grants>
        </fla_provisioner>
      </users>
    </clickhouse>
containers:
  - envs:
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "0"
      CLICKHOUSE_PROVISIONER_PASSWORD: "<from your secret mechanism>"
    image:
      tag: "26.9.1.1629"          # pin: the release uses `latest` with pullPolicy Always
    mounts:
      - path: /etc/clickhouse-server/users.d/provisioner-user.xml
        sub_path: provisioner-user.xml
        volume_name: clickhouse-config-override
```

Things to know before applying it:

- The release uses `update_strategy: Recreate`, so ClickHouse is down for the restart (ingest retries; serving
  returns errors). Data is on a PVC and is kept.
- The image is `latest` with `pullPolicy: Always`: **any** restart pulls whatever is newest. Pin the tag you are
  running (`SELECT version()`), as above, or the restart is also an unplanned ClickHouse upgrade over the data volume.
- Order: (1) apply the ClickHouse values and confirm `fla_provisioner` can log in; (2) only then create the Secret
  `fla-provisioner-secret` (keys `CLICKHOUSE_PROVISION_USER`, `CLICKHOUSE_PROVISION_PASSWORD`) in the namespace;
  (3) run `./scripts/dev/deploy_test_all.sh`, which wires that secret into the backend when it exists. Pointing the
  backend at an account that does not exist yet means provisioning fails (logged) and analysts get no console.
- Once step 3 is verified, the application user no longer holds access management.

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
