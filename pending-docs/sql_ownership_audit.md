# SQL Ownership Audit — Phase 5a

Goal: every SQL string in the backend lives in `backend/repositories/_sql/`
as a named, parameterised template. Routers and `backend/core/` never
contain inline SQL except for low-level engine plumbing (DuckDB
configuration, SQLite migrations, telemetry-proxy usage_log writes).

This audit is the input to Phase 5a's mechanical extraction. The numbers
below are from a `2026-06-09` grep of `con.execute` / `conn.execute` /
`cur.execute` call sites.

## Headline counts

| Layer | execute call sites | comment |
|---|---|---|
| `backend/core/` | 258 | expected — DuckDB/SQLite engine plumbing |
| `backend/repositories/` | 15 | most route through `track_query`, OK |
| `backend/routers/` | **4** | **LEAK — must move in Phase 5a** |

## The 4 router-layer leaks (Phase 5a primary target)

| File:line | Notes |
|---|---|
| [backend/routers/admin.py:1310](../backend/routers/admin.py#L1310) | `catchup_row = con.execute(...)` — admin's cron-progress catchup query; move to `backend/repositories/cron.py` |
| [backend/routers/session_scoring.py:704](../backend/routers/session_scoring.py#L704) | `con.execute(sql, params)` — already a helper-style call inside a router-local function; move the helper + its SQL fragments into a new `backend/repositories/session_scoring.py` (currently routes-only logic) |
| [backend/routers/services/core.py:153](../backend/routers/services/core.py#L153) | `row = con.execute(...)` — service-config row lookup; should be a repository call |

The `session_scoring.py:690` match is a docstring (`"""params is passed
through to con.execute..."""`), not an actual call site.

## Router → `backend.core.*` imports (Phase 5b target)

Phase 5b §5b.1 forbids routers from importing `backend.core.*` directly.
Today's count: **117 imports across 11 router files**. Notable hotspots:

| Router | Imports from core |
|---|---|
| `bootstrap.py` | 7 — duckdb, log_fields, INSIGHT_DEFINITIONS |
| `provision.py` | 8 — duckdb, iceberg, metadata_db, fastly.client |
| `admin.py` | 5+ — duckdb, metadata_db, log_fields |
| `query.py` | 2 |
| `alerts.py` | 2 — duckdb, metrics |
| `services/core.py` | 3 |
| `services/audit.py` | 2 |
| `session_scoring.py` | 4 |
| `usage.py` | 2 |
| `views.py` | 2 |
| `debug.py` | 3 |

Phase 5b enforces this via a `ruff` custom rule (or a CI grep gate). Adding
the gate now would fail CI on every commit — defer until Phase 5b ships
the repository facades that absorb these imports.

## Per-repository SQL extraction targets (Phase 5a)

For each repository file, audit its inline SQL strings and move them into
`backend/repositories/_sql/<file_stem>.py` as named constants. Function
signatures stay; the function body calls `con.execute(SQL_NAME, params)`
where `SQL_NAME` is imported from the `_sql/` module.

Files in priority order (most-SQL first):

| File | Inline SQL fragments (est.) | Phase 5a target |
|---|---|---|
| [_base.py](../backend/repositories/_base.py) (1,171 lines) | ~25 | Yes — biggest payoff |
| [dashboard.py](../backend/repositories/dashboard.py) (1,114 lines) | ~30 | Yes |
| [origin.py](../backend/repositories/origin.py) (1,257 lines) | ~20 | Yes |
| [security.py](../backend/repositories/security.py) | ~15 | Yes |
| [network.py](../backend/repositories/network.py) | ~10 | Yes |
| [performance.py](../backend/repositories/performance.py) | ~10 | Yes |
| [sessions.py](../backend/repositories/sessions.py) | ~8 | Yes |
| [alerts.py](../backend/repositories/alerts.py) | ~6 | Yes |
| [views.py](../backend/repositories/views.py) | ~6 | Yes |
| [query.py](../backend/repositories/query.py) | ~4 | Yes (admin-grade SQL — handle with care) |
| [usage.py](../backend/repositories/usage.py) | ~5 | Yes |
| [cron.py](../backend/repositories/cron.py) | ~5 | Yes |
| [insights/registry.py](../backend/repositories/insights/registry.py) | ~3 | Defer to Phase 5b (insights restructure decision pending) |
| [insights/repository.py](../backend/repositories/insights/repository.py) | ~5 | Defer to Phase 5b |
| [insights/definitions.py](../backend/repositories/insights/definitions.py) (1,291 lines) | ~30 | Defer to Phase 5b (data-driven vs split decision) |
| [utils/pagination.py](../backend/repositories/utils/pagination.py) | ~2 | Inline-utility-shaped; pull into `_sql/pagination.py` |
| [utils/filters.py](../backend/repositories/utils/filters.py) | ~5 | Same |

Insights logic is deferred to Phase 5b because Phase 5b's open question
(`definitions.py` shape — split-by-section vs convert-to-data-driven YAML)
affects the SQL layout.

## What stays inline

Inline SQL is acceptable in:

- **`backend/core/duckdb.py`** — engine setup (`SET memory_limit`, `INSTALL`,
  `LOAD`, etc.) and view-binding SQL. Not user-facing.
- **`backend/core/iceberg.py`** — manifest reads + view rebuild. Phase 4
  carves this into `iceberg/view.py`; SQL stays inline there.
- **`backend/core/sqlite_migrations.py`** — schema migrations. By design.
- **`backend/core/metadata_db.py`** + `backend/core/share_db.py` — local
  SQLite CRUD. Phase 5b carves both into per-concern modules; their SQL
  stays inline in those carved modules (one concern per file = locality is
  already correct).
- **`backend/utils/telemetry_proxy.py`** — usage_log writes. Internal.
- **Migrations and one-off scripts** under `scripts/`.

## Mechanical extraction recipe

For each repository function with inline SQL:

1. Pull the SQL string into `backend/repositories/_sql/<file>.py`:

   ```python
   # backend/repositories/_sql/dashboard.py
   AGGREGATES_BY_WINDOW = """
       SELECT
         date_trunc({granularity}, timestamp) AS bucket,
         sum(bytes) AS bytes,
         count() AS rows
       FROM {table}
       WHERE timestamp BETWEEN {start} AND {end}
       GROUP BY bucket
       ORDER BY bucket
   """
   ```

2. Document the template at the top of the constant: window shape, filter
   placeholders expected, output column list.

3. In the repository, replace the inline string with the import:

   ```python
   from backend.repositories._sql import dashboard as SQL

   def get_aggregates(con, ctx, start, end, granularity):
       sql = SQL.AGGREGATES_BY_WINDOW.format(
           table=ctx.cached_temps["live_hour"],
           granularity=granularity,
           start=as_iso(start),
           end=as_iso(end),
       )
       return con.execute(sql).fetchdf()
   ```

4. Add a test in `tests/repositories/_sql/test_<file>.py` asserting the
   rendered SQL contains the expected fragments for a sample set of inputs.
   These tests run fast (no DuckDB needed for string-level checks).

5. The existing repository tests continue to pass unchanged because
   function signatures haven't moved.

## Verification

- **No router contains `.execute(`** outside doc strings.
- **Repository functions** call SQL templates by name (no multi-line `"""`
  blocks inside function bodies).
- **`_sql/` modules** contain only string constants and docstrings (no
  imports of DuckDB / pydantic / FastAPI).
- **Coverage gate** stays at the per-phase floor (Phase 5a ratchet target
  is `cov-fail-under=83%` per cleanup_plan.md).

## Out of scope for Phase 5a

- Splitting the repository files (that's Phase 5b §5b.3).
- Moving connections into the new `RequestContext` (Phase 2 already
  shipped that; routers migrate as they get touched).
- Routing the SQL through a query builder (sqlglot was skipped per
  plan §"Library swaps explicitly skipped").
