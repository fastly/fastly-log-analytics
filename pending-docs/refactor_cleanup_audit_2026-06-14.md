# Refactor / Cleanup Audit & Remediation Plan

**Date:** 2026-06-14
**Branch:** `refactor/cleanup`
**Scope:** 100% of tracked files at HEAD (967 files, 222 backend Python, 422 frontend TS/TSX, 236 tests, plus root configs)
**Contract:** Research-only audit. **No behavior changes** are proposed unless explicitly flagged.
**Fresh-install assumption:** Per project decision, all installs provision from latest code. Any code that only exists to handle upgrades from older states is removable.

## Execution log

**2026-06-14 — Session 1 (calibration scope):** Pre-flight + PR-1 only. Operator declined the per-bucket prod-deploy cadence for the full 14-bucket sweep in a single agent session; remaining buckets will be picked up in follow-up sessions. Pre-flight corrections landed:
- (a) Frontend types regen command confirmed as `cd frontend && npm run gen:types` (runs `uv run python3 ../scripts/generate_openapi.py openapi.json && node ../scripts/refresh_api_types.js`). Section 7 updated.
- (b) Five cited pytest paths didn't exist at HEAD and have been replaced with real ones (`tests/core/test_metadata_db_pool.py`, `tests/core/test_share_db_connection.py`, `tests/core/test_usage_log_db.py`, `tests/cron/test_commit.py|test_sync.py`, `tests/utils/tunnel/`, `tests/routers/test_share_admin*.py`, `tests/repositories/test__base*.py`, `tests/core/test_sqlite_migrations*.py`, `tests/routers/admin/`). Section 8 PR scope tables and per-finding test lines updated.
- (c) Golden-payload capture moved to PR-3 prep (capturing them in pre-flight is wasted: dev data drifts before PR-3, breaking the diff). Documented at PR-3 entry.
- (d) `mypy-baseline.txt` (0 bytes) is **active scaffolding** per `.pre-commit-config.yaml:23-31` and `pyproject.toml:81-83` — the local pre-commit `mypy` hook pipes `mypy backend/` through `mypy-baseline filter`, and an empty baseline means "fail on any net-new mypy error." Keep.

PR-14 added to Implementation Plan (core-3 / core-5 / core-9 / core-15, deliberate behavior changes per executor mandate).

---

## Methodology

An 18-lane parallel audit fanned out over the tree, each lane responsible for a partition of the codebase:

| Lane | Scope |
|---|---|
| b1 | `backend/repositories/**` (and `_sql/`) |
| b2 | `backend/utils/**` |
| b3 | `backend/routers/**` |
| b4 | `backend/core/**` (config, ingest, duckdb, log_fields, etc.) |
| b5 | `backend/core/iceberg/**`, `metadata/**`, `rollups/**`, `share_db/**` |
| b6 | `backend/cron/**`, `scoring/**`, `provision/**`, `services/**`, `main.py`, `deps.py` |
| b7 | `backend/models/**` |
| b8 | `tests/**` |
| f1 | `frontend/components/ui/**` |
| f2 | `frontend/components/**` (feature components) |
| f3 | `frontend/app/**` (pages & sections) |
| f4 | `frontend/hooks/**` |
| f5 | `frontend/lib/**` |
| f6 | `frontend/stores/**`, edge files (proxy, ssr, preload manifest) |
| f7 | `frontend/__tests__/**` |
| x1 | Backend ↔ frontend type sync (openapi.json, api.generated.ts, api.ts, models/*) |
| x2 | Legacy / migration / upgrade-support code |
| x3 | Tracked-file hygiene, build, CI, Docker, scripts |

Each finding was then handed to an independent **adversarial verifier** agent whose job was to refute the claim by reading the cited files and lines. Only findings with verdict `confirmed` or `partially_confirmed` survived into this report.

**Totals after verification:** 123 confirmed, 48 partially confirmed, 2 refuted, 0 stale. After deduplication and severity filtering, 49 substantive findings remain (this document).

---

## Headline summary for the implementer

If you read nothing else:

1. The single biggest cleanup target is the **origin repository's per-card live/temp template duplication** ([b1](#b1)). ~700 LOC removable; high-confidence behavior-preserving; needs full origin test pass.
2. Three "deferred for one release" legacy paths from v2.0.0 have expired and should be removed: **usage_log legacy schema** ([b3-usage-log](#b3-usage-log)), **scrypt passcode path** ([b3-scrypt](#b3-scrypt)), **tunnel legacy fields** ([u7](#u7)).
3. Three half-built migration scaffolds need a **delete-or-finish** decision in one sitting: `backend/utils/retry.py` ([u1](#u1)), `backend/core/settings.py` ([core-1](#core-1)), `backend/utils/cdn.py` ([u4](#u4)). Recommendation throughout: delete.
4. Backend ↔ frontend type sync is in good shape; no wire-shape drift findings surfaced. The openapi.json + api.generated.ts regen pipeline is doing its job.
5. Tracked-file hygiene is also in good shape; no secrets, no stale generated artifacts, no missing .gitignore entries.

The four xs-effort P0 items ([b5](#b5), [c1](#c1), [b8](#b8), [r9](#r9)) make a clean first commit — ~60 lines deleted, zero behavior risk, full test coverage already in place.

---

## Index of findings

| ID | Title | Category | Effort | Behavior-preserving | PR |
|---|---|---|---|---|---|
| [b1](#b1) | Origin per-card live/temp template pairs | Redundancy | l | yes | PR-3 |
| [b2](#b2) | `_phase(name, t0)` boilerplate in 8+ repos | Reuse | s | yes | PR-5 |
| [b3](#b3) | `_hour_had_any_data` walk duplicated | Reuse | s | yes | PR-6 |
| [b3-pool](#b3-pool) | 3 near-identical SQLite thread-local pools | Redundancy | l | yes (with `on_borrow` hook) | PR-9 |
| [b3-usage-log](#b3-usage-log) | 138-LOC dead `usage_log` DDL + triggers | Legacy removal | s | yes (fresh installs) | PR-7 |
| [b3-scrypt](#b3-scrypt) | Scrypt passcode legacy path | Legacy removal | s | yes (fresh installs) | PR-8 |
| [b3-describe](#b3-describe) | DESCRIBE+stale-view-retry across 4 rollup writers | Reuse | xs | yes | PR-6 |
| [b3-discover](#b3-discover) | `discover_closed_hours` in 3 places | Reuse | xs | yes | PR-6 |
| [b3-migration](#b3-migration) | 2 duplicate `apply_pending` migration runners | Reuse | xs | yes | PR-6 |
| [b3-rollups-hour-token](#b3-rollups-hour-token) | `parse_hour_token` repeated 5+ times | Reuse | xs | yes | PR-6 |
| [b3-iceberg-cdn-purge](#b3-iceberg-cdn-purge) | `purge_surrogate_key` 2 sites with drifted exceptions | Reuse | xs | yes | PR-10 |
| [b3-iceberg-pointer-key](#b3-iceberg-pointer-key) | Slash-vs-dot namespace fallback hardcoded 5 places | Reuse | xs | yes | PR-10 |
| [b3-iceberg-package-proxy](#b3-iceberg-package-proxy) | 2 module-class-swap shims | Reuse | xs | yes | PR-10 |
| [b4](#b4) | Origin TS metric builder duplicated | Reuse | s | yes (folds into b1) | PR-3 |
| [b5](#b5) | `empty_schema_response` re-imported 30+ times | Reuse | xs | yes | PR-1 |
| [b7](#b7) | `TLS/H2/OH_FINGERPRINTS` byte-identical except column | Redundancy | s | yes | PR-12 |
| [b8](#b8) | `ORIGIN_TIMESERIES` near-duplicate of `TIME_SERIES` | Redundancy | xs | yes (with `timestamp IS NOT NULL` no-op note) | PR-1 |
| [b9](#b9) | Origin bespoke `_response_cache` vs `BoundedTTLCache` | Reuse | m | yes | PR-11 |
| [b10](#b10) | `SUMMARY_GROUPING_SETS` consumed by positional indices | Bad logic | s | yes | PR-12 |
| [b11](#b11) | Dashboard cache disabled but key-build still runs | Inefficiency | xs | yes | PR-12 |
| [c1](#c1) | 6 `try: pass / except: pass` blocks in cron jobs | Dead code | xs | yes | PR-1 |
| [c3](#c3) | `finalize_cron_duration` boilerplate in 5 `finally:` | Reuse | s | yes | PR-10 |
| [c4](#c4) | `invalidate_service(name)` in dashboard repo | Reuse | s | yes | PR-10 |
| [c5](#c5) | `refresh_view_and_warm_pool` in commit.py + sync.py | Reuse | s | latent bug fix in sync.py | PR-10 |
| [c8](#c8) | `shim_attr(name, fallback)` self-import dance in 4 sites | Reuse | xs | yes | PR-12 |
| [c10](#c10) | v2.0 tombstone comments in `deps.py` | Legacy removal | xs | yes | PR-12 |
| [c12](#c12) | `backend/services/__init__.py` empty | File structure | xs | yes | (skip) |
| [core-1](#core-1) | `backend/core/settings.py` — 1 of 11 consumers | Legacy removal | s | yes | PR-4 |
| [core-3](#core-3) | `_ORPHAN_THRESHOLD_MINS = 5` vs `60` | Bad logic | xs | **no** — operational decision | PR-14 |
| [core-5](#core-5) | `_safe_weakref` defined twice, divergent behavior | Bad logic | xs | **no** — fixes instrumentation | PR-14 |
| [core-9](#core-9) | Promote `_TASK_TO_CRON_KEY` | Reuse | xs | **no** — fixes latent retention bug | PR-14 |
| [core-10](#core-10) | `_atomic_write_json` helper duplicated | Reuse | xs | yes | PR-12 |
| [core-11](#core-11) | `_get_cfg_field` triplet | Reuse | xs | yes | PR-12 |
| [core-13](#core-13) | Dual fastly name fetcher | Reuse | s | partially (latency) | PR-12 |
| [core-14](#core-14) | `log_fields.py` Phase 7 docstring stale | Cleanup | xs | yes | PR-12 |
| [core-15](#core-15) | `_cleanup_temp_tables` runs unconditionally | Inefficiency | xs | **no** — removes safety net | PR-14 |
| [m1](#m1) | `backend/models/lake.py` not a models module | File structure | s | yes | PR-13 |
| [m4](#m4) | `LogExtentsMixin` opportunity | Reuse | xs | yes | PR-12 |
| [m5](#m5) | `OkResponse` mixin opportunity | Reuse | xs | yes | PR-12 |
| [r3](#r3) | `load_service_config / 404` written 16+ times | Reuse | s | yes | PR-10 |
| [r5](#r5) | `client_ip(request, default=...)` — 11+ sites | Reuse | s | yes | PR-12 |
| [r6](#r6) | SSE headers inlined in compaction.py | Reuse | xs | yes | PR-12 |
| [r7](#r7) | `_phase` pattern in 4 routers | Reuse | s | yes (folds into b2) | PR-5 |
| [r8](#r8) | `start_or_resume_cron` triple | Reuse | s | yes | PR-10 |
| [r9](#r9) | `_resolve_source` duplicates `get_source` body | Reuse | xs | yes | PR-1 |
| [u1](#u1) | `backend/utils/retry.py` — 0 production callers | Legacy removal | s | yes (delete path) | PR-4 |
| [u2](#u2) | `iso_z_now()` ignored in 9 utils sites | Reuse | s | yes (byte-identical) | PR-2 |
| [u4](#u4) | `backend/utils/cdn.py` — imported only by tests | Legacy removal | s | yes (delete path) | PR-4 |
| [u5](#u5) | `_iceberg_meta_prefix(source)` in state_sync.py | Reuse | xs | yes | PR-12 |
| [u6](#u6) | `_run_falco_lint` extraction | Reuse | s | yes (fixes a documented bug) | PR-12 |
| [u7](#u7) | `TunnelState.use_tunnel` / `tunnel_url` expired | Legacy removal | s | yes | PR-8 |
| [u8](#u8) | Replace `share_db.iso_z_now()` re-exports in tunnel | Reuse | xs | yes | PR-2 |
| [u9](#u9) | `sync_admin_state` misplaced in `utils/` | File structure | s | yes | PR-13 |
| [u10](#u10) | Promote `_is_full_miss` / `build_cdn_miss_synth_row` | Reuse | xs | yes | PR-12 |

`PR-N` references the PR boundaries in [§4 Implementation Plan](#4-implementation-plan).

---

## Section 1 — Redundancy & Duplication

### <a id="b1"></a>b1 · Origin repository: per-card live/temp template pairs

**Category:** redundancy | **Severity:** high | **Effort:** l | **Behavior-preserving:** yes
**Files:**
- [backend/repositories/origin.py:161-655](backend/repositories/origin.py#L161) — live functions
- [backend/repositories/origin.py:757-993](backend/repositories/origin.py#L757) — temp mirrors
- [backend/repositories/_sql/origin.py:20-220](backend/repositories/_sql/origin.py#L20) — live SQL templates
- [backend/repositories/_sql/origin.py:284-475](backend/repositories/_sql/origin.py#L284) — `TEMP_*` SQL templates

**Evidence.** `_sql/origin.py` declares 8 live templates and 7 `TEMP_*` mirrors. They differ only in `{lat_val}` → hardcoded `lat_us` and `{table}/{where}` → `{temp_table}`. The Python halves at `origin.py:203-212` (`get_summary`) and `origin.py:770-783` (`_origin_summary_from_temp`) compute the same five conditional expressions (`ost_5xx`, `ottlb_p50`, …) with `actual_cols` → `actual_cols_set` as the only difference. The `N-8` comment at lines 197-202 is mirrored at 767-769 with the explicit acknowledgment "same fix as get_summary above" — drift has already happened once.

**Why it matters.** Every change to an origin endpoint must be applied in two places. The comment trail proves drift has already occurred. ~450 LOC of dual-maintenance burden in `origin.py` plus ~450 LOC of duplicated SQL in `_sql/origin.py`.

**Remediation.**

Two-commit sequence:

**Commit 1: Unify SQL templates with `{lat_val}` parametrisation.**
- Add a `{lat_val}` placeholder to every `SUMMARY`, `TIMESERIES`, `SLOW_URLS`, `STATUS_CODES`, `PATH_BREAKDOWN`, `POP_LATENCY`, `IP_HEALTH` template in `_sql/origin.py`.
- Delete the `TEMP_*` mirrors (7 templates).
- Note: `TEMP_SUMMARY_BY_EDGE` does not have a live equivalent (the live path folds it into a `GROUPING SETS` query). Preserve it explicitly as a single template named `SUMMARY_BY_EDGE_FROM_TEMP`.

**Commit 2: Collapse the Python pairs.**
- Replace each pair (`get_summary` + `_origin_summary_from_temp`, etc.) with a single function parameterised by `(table, where, params, lat_val, actual_cols)`. Live callers pass `table='{table}'`, `lat_val=origin_latency_us_expr(actual_cols)`. Temp callers pass `table='{temp_table}'`, `where='1=1'`, `lat_val='lat_us'`.
- The router dispatching layer changes shape minimally — same number of public functions, same response payloads.

**Tests to run.**
```
pytest tests/repositories/test_origin*.py tests/repositories/_sql/test_origin*.py -v
```

**Manual verification (per `verify-dev-first` memory).** After running on dev (13002/18002), hit each origin endpoint with a known service id and diff response JSON against a stash taken before the change. Endpoints to check: `/api/origin/aggregates`, `/api/origin/timeseries`, `/api/origin/slow-urls`, `/api/origin/status-codes`, `/api/origin/path-breakdown`, `/api/origin/pop-latency`, `/api/origin/ip-health`.

**Dependencies / unblocks.** Subsumes [b4](#b4) (origin TS metric builder).

---

### <a id="b3-pool"></a>b3-pool · Three near-identical SQLite thread-local connection pools

**Category:** redundancy | **Severity:** medium | **Effort:** l | **Behavior-preserving:** yes (with `on_borrow` hook)
**Files:**
- [backend/core/metadata/base.py:25-258](backend/core/metadata/base.py#L25)
- [backend/core/share_db/connection.py:31-200](backend/core/share_db/connection.py#L31)
- [backend/core/metadata/usage_log_db.py:59-208](backend/core/metadata/usage_log_db.py#L59)

**Evidence.** Same module-globals (`_local`, `_init_lock`, `_initialized`, `_all_connections`, `_all_connections_lock`), same `_connections()` helper shape, same 10s lock timeout, same 5 PRAGMAs (`journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`, `cache_size=-64000`, `busy_timeout=30000`), same teardown helpers. Drift already exists:
- `share_db/connection.py:122` uses bare `sqlite3.connect(...)` with no `factory=InstrumentedConnection`, so share_db writes never show in the Live Query Monitor.
- `share_db` was missing `cache_size=-64000` until a retrofit.
- `usage_log_db.py:212-215` comment: *"Exact copies of the table / index / trigger definitions that used to live in backend.core.metadata.base._SCHEMA."*

**Why it matters.** Three places to update for any pool behavior change. Observability drift is already live — share_db queries are invisible to the Query Monitor. ~250 LOC delta on extraction.

**Remediation.**

**Commit 1: Extract `backend/core/sqlite_pool.py`.**
```python
# backend/core/sqlite_pool.py
class ThreadLocalPool:
    """Shared shape for the three per-service SQLite pools."""

    _PRAGMAS = (
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA foreign_keys=ON",
        "PRAGMA cache_size=-64000",
        "PRAGMA busy_timeout=30000",
    )

    def __init__(
        self, *,
        label: str,
        key_fn: Callable[[Any], str],
        path_fn: Callable[[Any], str],
        init_schema_fn: Callable[[sqlite3.Connection], None],
        use_instrumented: bool = True,
        connect_fn: Callable[..., sqlite3.Connection] | None = None,
        on_borrow: Callable[[sqlite3.Connection], None] | None = None,
    ):
        ...

    def get(self, *args) -> sqlite3.Connection:
        ...
```

**Commit 2: Migrate `usage_log_db` first** (newest, fewest tests). `_pool = ThreadLocalPool(label="usage_log", ...)`; `get_con = _pool.get`.

**Commit 3: Migrate `metadata/base`.** Largest test surface; do this second so any pool-behavior regression catches early.

**Commit 4: Migrate `share_db/connection`** with `connect_fn=` for the quarantine logic at [connection.py:61-127](backend/core/share_db/connection.py#L61) and `on_borrow=` for the per-borrow `PRAGMA foreign_keys=ON` re-assertion at [connection.py:142-149](backend/core/share_db/connection.py#L142). Crucially, this migration **enables `InstrumentedConnection` for share_db**, which is a visible behavior change (share_db queries appear in Live Query Monitor). Call this out in the commit message and verify via `/admin/queries`.

**Tests to run after each commit.**
```
pytest tests/core/test_metadata_db_concurrency.py tests/core/test_metadata_db_crud.py tests/core/test_metadata_db_audit.py tests/remote_access/test_share_db.py tests/routers/test_usage_log.py -v
pytest tests/ -k "pool" -v
```

**Manual verification.** After PR lands on dev, open Live Query Monitor and confirm share_db queries (e.g. share-login activity) now appear.

---

### <a id="b7"></a>b7 · `TLS/H2/OH_FINGERPRINTS` byte-identical except column

**Category:** redundancy | **Severity:** low | **Effort:** s | **Behavior-preserving:** yes
**Files:**
- [backend/repositories/_sql/security.py:127-184](backend/repositories/_sql/security.py#L127)
- [backend/repositories/security.py:399-431](backend/repositories/security.py#L399)

**Evidence.** Three SQL constants differ only in the column name. The sibling `FINGERPRINT_COVERAGE` at `_sql/security.py:192-196` is already parameterised with `{col}` and called three times — its own docstring says *"Cheaper to ship one template + call it three times."* The same rationale is applied inconsistently across the file.

**Remediation.**
```python
# backend/repositories/_sql/security.py
FINGERPRINT_TOP_N = '''
    SELECT "{col}" AS fingerprint,
           count(DISTINCT ip) AS ip_count,
           count(*) AS req_count
    FROM {temp_table}
    WHERE "{col}" IS NOT NULL AND "{col}" != ''
    GROUP BY 1 ORDER BY 3 DESC LIMIT 20
'''
# (delete TLS_FINGERPRINTS, H2_FINGERPRINTS, OH_FINGERPRINTS and the corresponding __all__ entries)

# backend/repositories/security.py:399-431
_FP_KEY = {"tls_ciphers_sha": "tls_fingerprints",
           "h2_fingerprint": "h2_fingerprints",
           "oh_fingerprint": "oh_fingerprints"}

for col, result_key in _FP_KEY.items():
    if col in actual_cols and "ip" in actual_cols:
        q = SQL.FINGERPRINT_TOP_N.format(col=col, temp_table=temp_table)
        res = runner.execute(q).fetchall()
        results[result_key] = [
            {"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res
        ]
        fingerprint_coverage[col] = _coverage_for(col)
    else:
        results[result_key] = []
```

Keep the explicit result-key map — the mapping is not derivable by suffix manipulation.

**Tests.** `pytest tests/repositories/test_security*.py`

---

### <a id="b8"></a>b8 · `ORIGIN_TIMESERIES` near-duplicate of `TIME_SERIES`

**Category:** redundancy | **Severity:** low | **Effort:** xs | **Behavior-preserving:** yes (with one tiny no-op predicate)
**Files:**
- [backend/repositories/_sql/dashboard.py:89-95](backend/repositories/_sql/dashboard.py#L89)
- [backend/repositories/_sql/performance.py:13-19](backend/repositories/_sql/performance.py#L13)
- [backend/repositories/performance.py:301-307](backend/repositories/performance.py#L301)

**Evidence.** `TIME_SERIES` has an `{extra_where}` slot used at [backend/repositories/dashboard.py:691-697](backend/repositories/dashboard.py#L691) to inject precisely the `AND <col> IS NOT NULL` clause that `ORIGIN_TIMESERIES` hardcodes. Differences: (a) `TIME_SERIES` carries `AND timestamp IS NOT NULL` (no-op on these tables); (b) metric column substitution boundary differs trivially.

**Remediation.**
```python
# performance.py:301-307
from backend.repositories._sql import dashboard as SQL_DASHBOARD

sql = SQL_DASHBOARD.TIME_SERIES.format(
    extra_where=f' AND "{metric_col}" IS NOT NULL',
    ...,
)
```
Delete `ORIGIN_TIMESERIES` from `_sql/performance.py` and remove its `__all__` entry.

**Tests.** `pytest tests/repositories/_sql/test_performance.py tests/repositories/test_performance*.py`

---

### <a id="b3-usage-log"></a>b3-usage-log · 138 LOC of dead `usage_log` DDL + 3 triggers in `metadata/base.py`

**Category:** legacy removal | **Severity:** medium | **Effort:** s | **Behavior-preserving:** yes for fresh installs
**Files:**
- [backend/core/metadata/base.py:442-579](backend/core/metadata/base.py#L442)
- [backend/core/metadata/usage_log_db.py:139-146](backend/core/metadata/usage_log_db.py#L139), [216-301](backend/core/metadata/usage_log_db.py#L216), [310-407](backend/core/metadata/usage_log_db.py#L310)
- [backend/core/metadata/usage_log.py:24-37](backend/core/metadata/usage_log.py#L24)
- [backend/core/metadata/ingest_log.py:415-432](backend/core/metadata/ingest_log.py#L415)
- [backend/core/sqlite_migrations.py:113-144](backend/core/sqlite_migrations.py#L113)

**Evidence.** `usage_log_db.py:1-46` docstring states explicitly: *"The legacy table in metadata.db is left intact for one release as a rollback backstop; readers and writers no longer touch it. The next release can drop it."* Every writer now uses `_ul(service_id) → _usage_log_db.get_con(service_id)`. The legacy `metadata.db.usage_log` table is created with three INSERT/DELETE/UPDATE triggers that would silently double-count if any future writer forgot to use `_ul`. `_migration_003_rebuild_usage_log_hourly_summary` runs as a 0-row no-op on every fresh init.

**Also broken today:** `get_latest_reconciliation_ts` ([ingest_log.py:415-432](backend/core/metadata/ingest_log.py#L415)) reads `metadata.db.usage_log` — silently always returns `None` on fresh installs because `fastly.reconciliation` rows now land in the new DB via `usage_log.py:311`. Fixing this is technically a behavior change (zero callers will notice, but if any did they were getting wrong data).

**Remediation.**

**Commit 1.** Delete the 138-line DDL block at `base.py:442-579`. Removes table + 3 triggers + 4 indexes.

**Commit 2.** Delete `migrate_from_metadata_db` and its invocation at `usage_log_db.py:139-146`. Delete the entire function body at `usage_log_db.py:310-407`.

**Commit 3.** Delete `_migration_003_rebuild_usage_log_hourly_summary` from `sqlite_migrations.py:113-144` and key `3:` from the `MIGRATIONS` registry.

**Commit 4.** Rewrite `get_latest_reconciliation_ts` to read from `usage_log_db.open_readonly(service_id)`:
```python
def get_latest_reconciliation_ts(service_id: str) -> str | None:
    with _usage_log_db.open_readonly(service_id) as con:
        row = con.execute(
            "SELECT MAX(timestamp) FROM fastly_reconciliation"
        ).fetchone()
    return row[0] if row else None
```
Verify the new table name against `_usage_log_db._SCHEMA`.

**Tests to run.**
```
pytest tests/core/test_metadata*.py tests/core/test_metadata_db_migrations.py tests/routers/test_usage_log.py -v
```

**Rollback.** Reverting this PR re-creates the legacy table on next startup via the `CREATE TABLE IF NOT EXISTS` paths.

---

### <a id="b5"></a>b5 · `empty_schema_response` re-imported 30+ times

**Category:** reuse | **Severity:** low | **Effort:** xs | **Behavior-preserving:** yes
**Files:**
- [backend/repositories/origin.py:176](backend/repositories/origin.py#L176), [:352](backend/repositories/origin.py#L352), [:443](backend/repositories/origin.py#L443) (+6 more in `origin.py`)
- [backend/repositories/performance.py:34](backend/repositories/performance.py#L34) (+4 more)
- [backend/repositories/security.py:40](backend/repositories/security.py#L40) (+1 more)

**Evidence.** `sessions.py:13` already does this at module top — every other repo file inlines the import 6+ times despite `_base` being a top-level import already. At [performance.py:283-290](backend/repositories/performance.py#L283), the import inside the `else` is dead because the next-but-one line `return empty_schema_response(...)` is at the same indent as `else`.

**Remediation.** Extend the existing top-level `from backend.repositories._base import (...)` block in each file to include `empty_schema_response` and `origin_latency_us_expr`. Delete all inline imports. Mechanical edit.

**Tests.** `pytest tests/repositories/ -v`

---

### Smaller redundancy items (effort: xs each)

- <a id="b3-iceberg-cdn-purge"></a>**b3-iceberg-cdn-purge** · `purge_surrogate_key(source, key)` — 2 iceberg sites with drifted exception handling. Extract a helper. Test: `pytest tests/core/test_iceberg*.py`.
- <a id="b3-iceberg-pointer-key"></a>**b3-iceberg-pointer-key** · Slash-vs-dot namespace fallback hardcoded in 5 places. Extract `_iceberg_root_prefix` + `_metadata_pointer_candidates`.
- <a id="b3-iceberg-package-proxy"></a>**b3-iceberg-package-proxy** · 2 module-class-swap shims share shape. Extract `install_mirroring_proxy(package, primary, secondary, mirrored)`.

---

## Section 2 — Reusability / Extract-to-Shared

### <a id="b2"></a>b2 · `_phase(name, t0)` boilerplate copy-pasted 8+ times

**Category:** reuse | **Severity:** medium | **Effort:** s | **Behavior-preserving:** yes
**Files:**
- [backend/repositories/dashboard.py:153-160](backend/repositories/dashboard.py#L153) — thunk form `_timed(name, fn)`
- [backend/repositories/sessions.py:453-456](backend/repositories/sessions.py#L453)
- [backend/repositories/network.py:54-62](backend/repositories/network.py#L54), [:555-560](backend/repositories/network.py#L555)
- [backend/repositories/security.py:25-30](backend/repositories/security.py#L25), [:200-205](backend/repositories/security.py#L200), [:287-293](backend/repositories/security.py#L287)
- [backend/repositories/query.py:36-39](backend/repositories/query.py#L36) — uses `time.monotonic()` instead of `perf_counter()`

**Evidence.** Two inconsistencies in the wild: `dashboard.py` uses a `_timed(name, fn)` thunk form (data shape identical), and `query.py:39` uses `time.monotonic()` while the others use `time.perf_counter()`. Four more copies in routers — see [r7](#r7).

**Remediation.**
```python
# backend/repositories/_base.py
class SectionTimer:
    def __init__(self) -> None:
        self.entries: list[dict] = []

    def mark(self, name: str, t0: float) -> None:
        self.entries.append({
            "section": name,
            "time_ms": round((time.perf_counter() - t0) * 1000, 2),
        })

    def call(self, name: str, fn):
        t0 = time.perf_counter()
        try:
            return fn()
        finally:
            self.mark(name, t0)
```

Migrate one callsite per commit. Standardise on `perf_counter()` (drop `query.py`'s `monotonic()` — sub-noise-floor for wall-clock telemetry).

**Tests.** `pytest tests/repositories/ -v` per migrated file.

---

### <a id="r7"></a>r7 · Same pattern in 4 routers

**Category:** reuse | **Severity:** low | **Effort:** s | **Behavior-preserving:** yes
**Files:**
- [backend/routers/bootstrap.py:32-39](backend/routers/bootstrap.py#L32) — uses `monotonic()`
- [backend/routers/dashboard.py:68-92](backend/routers/dashboard.py#L68) — inlines instead of abstracting
- [backend/routers/usage.py:74-77](backend/routers/usage.py#L74)
- [backend/routers/services/core.py:484-487](backend/routers/services/core.py#L484)

**Remediation.** If [b2](#b2) is being done, use the same `SectionTimer` class from `_base.py`. Otherwise, mirror class in `backend/utils/router_utils.py` and standardise on `perf_counter()`. Land both together in PR-5.

---

### <a id="b3"></a>b3 · `_hour_had_any_data` + per-field rollup root listdir implemented twice

**Category:** reuse | **Severity:** medium | **Effort:** s | **Behavior-preserving:** yes
**Files:**
- [backend/repositories/_base.py:1289-1336](backend/repositories/_base.py#L1289)
- [backend/repositories/sessions.py:48-90](backend/repositories/sessions.py#L48)

**Evidence.** Both files contain the same load-bearing "fall back to raw if writer behind, skip if hour was empty" walk. Comments in both cross-reference each other (*"mirrors `QueryRunner.try_time_series_from_rollup`"*) — the author already worried about drift.

**Remediation.**
```python
# backend/repositories/_base.py
def collect_hourly_bundle_paths(
    src: dict, st: datetime, et: datetime,
    bundled_root: str, bundle_filename: str,
) -> tuple[list[str], bool] | None:
    """Walk [st, et) by UTC hour, return (paths, crosses_active) or None
    if any closed hour with per-field data has no bundle (writer behind)."""
    hour_per_field_root = _rollups_root(src)
    try:
        field_dirs = [f for f in os.listdir(hour_per_field_root) if f.startswith("field=")]
    except OSError:
        field_dirs = []
    def _hour_had_any_data(h: str) -> bool:
        return any(
            os.path.isdir(os.path.join(hour_per_field_root, f, f"hour={h}"))
            for f in field_dirs
        )
    active_hour_str = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    paths: list[str] = []
    cursor = st.replace(minute=0, second=0, microsecond=0)
    crosses_active = False
    while cursor < et:
        hour_str = cursor.strftime("%Y-%m-%d-%H")
        if hour_str >= active_hour_str:
            crosses_active = True
            break
        path = os.path.join(bundled_root, f"hour={hour_str}", bundle_filename)
        if not os.path.isfile(path):
            if _hour_had_any_data(hour_str):
                return None
            cursor += timedelta(hours=1)
            continue
        paths.append(path)
        cursor += timedelta(hours=1)
    return paths, crosses_active

# Callsite (sessions.py)
result = collect_hourly_bundle_paths(src, st, et, bundled_root, SESSIONS_BUNDLE_FILENAME)
if result is None:
    return None
paths, crosses_active = result
```

**Tests.** `pytest tests/repositories/test_sessions*.py tests/repositories/test_base*.py`.

---

### <a id="b4"></a>b4 · Origin TS metric builder duplicated

**Category:** reuse | **Severity:** low | **Effort:** s | **Behavior-preserving:** yes (folds into [b1](#b1))
**Files:** [backend/repositories/origin.py:359-389](backend/repositories/origin.py#L359), [backend/repositories/origin.py:845-867](backend/repositories/origin.py#L845)

**Remediation.** Extract `build_origin_ts_metric(actual_cols, metric, percentile, bucket_minutes) -> tuple[str,str,str,str] | None` near `origin_latency_us_expr` in `_base.py`. Disappears entirely if [b1](#b1) is done; valuable independently if [b1](#b1) is deferred.

---

### <a id="b9"></a>b9 · `_response_cache` infra in `origin.py` is bespoke; `BoundedTTLCache` exists

**Category:** reuse | **Severity:** low | **Effort:** m | **Behavior-preserving:** yes
**Files:**
- [backend/repositories/origin.py:31-100](backend/repositories/origin.py#L31)
- [backend/repositories/dashboard.py:38-55](backend/repositories/dashboard.py#L38) (cache disabled but pattern matches)
- [backend/repositories/insights/repository.py:18-29](backend/repositories/insights/repository.py#L18)
- [backend/utils/bounded_cache.py](backend/utils/bounded_cache.py)

**Evidence.** `BoundedTTLCache`'s own docstring: *"Drop-in replacement for the ad-hoc dict[key, (timestamp, value)] cache pattern scattered through the codebase."* Origin's bespoke cache was missed during the original migration. Dashboard cache is currently disabled (`DASHBOARD_CACHE_TTL = 0`), so origin is the only live response cache in the lane.

**Remediation.** Wrap `BoundedTTLCache` in a small `ResponseCache`:
```python
# backend/repositories/_base.py or a new file
class ResponseCache:
    def __init__(self, ttl: int, max_entries: int = 256):
        self._inner = BoundedTTLCache(ttl=ttl, max_entries=max_entries)

    def key(self, *parts) -> str:
        return hashlib.sha256(
            json.dumps(parts, sort_keys=True, default=str).encode()
        ).hexdigest()

    def get(self, key: str) -> dict | None:
        v = self._inner.get(key)
        if v is None:
            return None
        return {**v, "is_cached": True}

    def put(self, key: str, value: dict) -> None:
        stripped = {k: v for k, v in value.items()
                    if k not in ("debug_queries", "debug_calls", "is_cached")}
        self._inner.put(key, stripped)
```

Migrate origin's ~6 endpoints incrementally.

**Tests.** `pytest tests/repositories/test_origin*.py tests/utils/test_bounded_cache.py`. Spot-check cache hit/miss markers in admin/queries.

---

### <a id="u2"></a>u2 · `iso_z_now()` ignored in 9 utils sites

**Category:** reuse | **Severity:** low | **Effort:** s | **Behavior-preserving:** yes (byte-identical)
**Files:**
- [backend/utils/date_utils.py:23-25](backend/utils/date_utils.py#L23) — defines `iso_z_now()` and `iso_z(dt)`
- [backend/utils/bot_sources.py:173](backend/utils/bot_sources.py#L173)
- [backend/utils/ngwaf_bot_cache.py:102](backend/utils/ngwaf_bot_cache.py#L102), [:133](backend/utils/ngwaf_bot_cache.py#L133), [:146](backend/utils/ngwaf_bot_cache.py#L146)
- [backend/utils/rdns_cache.py:598-599](backend/utils/rdns_cache.py#L598) — `_now()` is literally a renamed copy
- [backend/utils/system_jobs.py:16](backend/utils/system_jobs.py#L16)
- [backend/utils/telemetry.py:266](backend/utils/telemetry.py#L266)
- [backend/utils/ngwaf.py:206](backend/utils/ngwaf.py#L206)

**Evidence.** `ngwaf_bot_cache.py:130-133` already imports `parse_iso_utc` from `date_utils` and then formats by hand with `strftime`. `rdns_cache._now()` is literally a renamed copy of `iso_z_now`.

**Remediation.** Mechanical replacement. Example:
```python
# Before (ngwaf_bot_cache.py:130-133)
from backend.utils.date_utils import parse_iso_utc

_pts = parse_iso_utc(latest_timestamp)
next_ts = (
    (_pts + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    if _pts else latest_timestamp
)

# After
from backend.utils.date_utils import iso_z, parse_iso_utc

_pts = parse_iso_utc(latest_timestamp)
next_ts = iso_z(_pts + timedelta(seconds=1)) if _pts else latest_timestamp
```
Delete `rdns_cache._now()` after migration.

**Tests.** `pytest tests/utils/`.

---

### <a id="u8"></a>u8 · Tunnel modules import `iso_z_now` via `share_db` re-export

**Category:** reuse | **Severity:** low | **Effort:** xs | **Behavior-preserving:** yes
**Remediation.** Replace 6 `share_db.iso_z_now()` callsites in `backend/utils/tunnel/*` with direct `from backend.utils.date_utils import iso_z_now`.

---

### <a id="b3-describe"></a>b3-describe · DESCRIBE+stale-view-retry across 4 rollup writers

**Category:** reuse | **Severity:** low | **Effort:** xs | **Behavior-preserving:** yes
**Files:**
- [backend/core/rollups/recompute.py:228-240](backend/core/rollups/recompute.py#L228)
- [backend/core/rollups/time_series.py:80-105](backend/core/rollups/time_series.py#L80)
- [backend/core/rollups/sessions.py:88-115](backend/core/rollups/sessions.py#L88)
- [backend/core/rollups/wellknown_bots.py:103-117](backend/core/rollups/wellknown_bots.py#L103)

**Remediation.** Add to `backend/core/rollups/_common.py`:
```python
def describe_columns(
    con, source, table_ident: str, *, logger=None, log_label: str = "",
) -> set[str] | None:
    """DESCRIBE table; retry once with view refresh on stale-view error.
    Returns set of column names, or None if table doesn't exist."""
    ...
```

**Why it matters.** A future rollup added without this pattern silently skips the stale-view self-heal.

---

### <a id="b3-discover"></a>b3-discover · `discover_closed_hours` walk in 3 places

**Files:** [backend/core/rollups/hour_bundles.py:290-308](backend/core/rollups/hour_bundles.py#L290), [time_series.py:212-230](backend/core/rollups/time_series.py#L212), [sessions.py:246-264](backend/core/rollups/sessions.py#L246)
**Remediation.** `discover_closed_hours(rollup_hour_root) -> list[str]` in `_common.py`.

---

### <a id="b3-migration"></a>b3-migration · Two duplicate `apply_pending` migration runners

**Files:** [backend/core/sqlite_migrations.py:260-289](backend/core/sqlite_migrations.py#L260), [backend/core/share_db/schema.py:150-171](backend/core/share_db/schema.py#L150)
**Remediation.** `run_pending_migrations(con, migrations, *, log_prefix)`. Each existing function becomes a one-liner.

---

### <a id="b3-rollups-hour-token"></a>b3-rollups-hour-token · `parse_hour_token(h)` repeated 5+ times

**Remediation.** Extract `parse_hour_token(h) -> datetime | None` into `backend/core/rollups/_common.py`.

---

### <a id="r3"></a>r3 · `load_config / 404` preamble written 16+ times

**Category:** reuse | **Severity:** medium | **Effort:** s | **Behavior-preserving:** yes
**Files:**
- [backend/routers/services/core.py:143-145](backend/routers/services/core.py#L143) (+13 more sites in same file)
- [backend/routers/admin/compaction.py:88-90](backend/routers/admin/compaction.py#L88)
- [backend/routers/provision.py:1012-1014](backend/routers/provision.py#L1012)

**Evidence.** Two drift instances already exist: [services/core.py:87](backend/routers/services/core.py#L87) raises `ValueError`, [services/core.py:874](backend/routers/services/core.py#L874) yields a JSON-encoded SSE error.

**Remediation.**
```python
# backend/utils/router_utils.py
def load_service_config(service_id: str) -> dict:
    cfg = load_config(service_id)
    if not cfg:
        raise HTTPException(404, detail=f"Service {service_id!r} not found")
    return cfg
```
Migrate the 16 sites in one PR. Leave `... or {}` callsites alone (they intentionally want the empty-dict behavior).

**Tests.** `pytest tests/routers/ -v`.

---

### <a id="r8"></a>r8 · `start_cron_run / RuntimeError → look up active_run → 503` triple

**Category:** reuse | **Severity:** low | **Effort:** s | **Behavior-preserving:** yes
**Files:** [backend/routers/admin/ingest.py:31-52](backend/routers/admin/ingest.py#L31), [:54-80](backend/routers/admin/ingest.py#L54), [backend/routers/admin/iceberg.py:44-66](backend/routers/admin/iceberg.py#L44)

**Remediation.**
```python
# backend/utils/router_utils.py (or backend/cron_progress.py)
def start_or_resume_cron(
    source: dict, task: str, target,
    target_kwargs: dict | None = None,
    success_msg: str = "", in_progress_msg: str = "",
) -> dict:
    try:
        run_id = start_cron_run(source, task)
        start_progress(run_id, service_id=source["name"], task=task)
        threading.Thread(
            target=target, args=(source["name"],),
            kwargs={"run_id": run_id, **(target_kwargs or {})}, daemon=True,
        ).start()
        return {"ok": True, "message": success_msg, "run_id": run_id}
    except RuntimeError as e:
        for entry in list_active_runs():
            if entry.get("service_id") == source["name"] and entry.get("task") == task:
                return {"ok": True, "message": in_progress_msg, "run_id": entry["run_id"]}
        raise HTTPException(503, detail={"error": str(e), "busy": True})

# Callsite (iceberg.py)
return start_or_resume_cron(
    source, "commit", _run_commit,
    target_kwargs={"force": True},
    success_msg="Commit started.",
    in_progress_msg="Commit already running.",
)
```

**Tests.** `pytest tests/routers/ -k "admin and (ingest or iceberg)"`.

---

### <a id="r9"></a>r9 · `_resolve_source` is a near-identical copy of `get_source` body

**Category:** reuse | **Severity:** low | **Effort:** xs | **Behavior-preserving:** yes
**Files:** [backend/deps.py:61-73](backend/deps.py#L61), [backend/core/request_context.py:170-185](backend/core/request_context.py#L170)

**Evidence.** The 400-detail body MUST stay identical because the frontend checks `error.no_service`. Two definitions = drift risk.

**Remediation.** Extract `_resolve_source_or_400(service_id)` in `deps.py`; `get_source` wraps it; `request_context.py` imports it directly.

**Tests.** `pytest tests/test_deps.py tests/core/test_request_context.py`.

---

### <a id="c5"></a>c5 · `refresh_view_and_warm_pool` in commit.py + sync.py

**Category:** reuse | **Severity:** medium | **Effort:** s | **Behavior-preserving:** latent bug fix in sync.py
**Files:** [backend/cron/jobs/commit.py:142-165](backend/cron/jobs/commit.py#L142), [backend/cron/jobs/sync.py:250-283](backend/cron/jobs/sync.py#L250)

**Latent bug discovered during verification.** The two callsites differ in where the success log sits relative to try/except. `commit.py` puts it inside (so failure means no status log). `sync.py` puts it outside (today, on failure, sync still emits a misleading "View refresh + warm: Xms" status event). The proposed helper fixes the sync.py mis-log.

**Remediation.** Extract `refresh_view_and_warm_pool(source, *, log_prefix)` to `backend/core/rollups/_common.py` or `backend/cron/decorators.py`. Place the success log inside the try/except so failure → no misleading status event. Document the bug fix in the commit message.

**Tests.** `pytest tests/cron/ tests/core/test_iceberg*.py` (commit.py / sync.py have no test-file-level coverage; iceberg + cron suites exercise the helper).

---

### <a id="c3"></a>c3 · `finalize_cron_duration` boilerplate in 5 `finally:` blocks

**Remediation.** Extract a helper that takes `(run_id, t_start, status, error=None)` and writes the duration row. Five callsites in `backend/cron/jobs/*`.

---

### <a id="c4"></a>c4 · `invalidate_service(name)` reaching into private cache

**Files:** 3 callsites reach `backend.repositories.dashboard._dashboard_cache` directly.
**Remediation.** Add a public `invalidate_service(name: str) -> None` in `backend/repositories/dashboard.py`. Migrate the 3 callers.

---

### <a id="c8"></a>c8 · `shim_attr(name, fallback)` self-import dance in 4 sites

**Remediation.** Extract a helper for the `import backend.scheduler as _shim` pattern with paragraph-long comments. 4 callsites.

---

### Smaller reusability items (xs)

- <a id="u5"></a>**u5** · `_iceberg_meta_prefix(source)` for 4× repeated prefix block in `state_sync.py`.
- <a id="u10"></a>**u10** · Promote `_is_full_miss` to public; extract `build_cdn_miss_synth_row` (two telemetry modules build the same synth row with drifted prefixes).
- <a id="r5"></a>**r5** · `client_ip(request, default=...)` — 11+ sites with 4 different "no-client" markers (`0.0.0.0`, `127.0.0.1`, `unknown`, `admin`). [backend/utils/remote_access.py:272-284](backend/utils/remote_access.py#L272) already has a `get_client_ip(request, *, is_remote)` helper with a vestigial `is_remote` param; absorb the inlinings at `main.py:585`, `remote_access.py:251,682`, and 8 others.
- <a id="r6"></a>**r6** · [admin/compaction.py:250-258](backend/routers/admin/compaction.py#L250) inlines `SSE_HEADERS` instead of importing.
- <a id="m4"></a>**m4** · `LogExtentsMixin` — `earliest_log_at`/`latest_log_at` pair in 4 responses.
- <a id="m5"></a>**m5** · `OkResponse` mixin — `ok: bool = True` in 6 ack responses.
- <a id="core-10"></a>**core-10** · `_atomic_write_json(path, data)` helper — `save_config` and `save_usage_logging_config` paste identical mkstemp/replace blocks.
- <a id="core-11"></a>**core-11** · `_get_cfg_field(field, sid, default)` — 4 near-identical fastly accessors.
- <a id="core-13"></a>**core-13** · Replace `config.fetch_service_name` urllib body with a call to `backend.core.fastly.client.fastly()` (retries + telemetry already there). **Verifier note:** worst-case latency changes from 5s to ~44s (timeout + retry backoff). Caller is behind a name cache and falls back to cached/config name, so acceptable — but the latency profile shifts.
- <a id="u6"></a>**u6** · Extract `_run_falco_lint(vcl_text, ...)` — `vcl_utils.lint_log_format` and `vcl_validator.lint_vcl` are two parsers with one already documented as buggy. Fixing as part of extraction is technically a bug fix; document in commit message.

---

## Section 3 — Bad Logic or Inefficient Code

### <a id="b10"></a>b10 · `SUMMARY_GROUPING_SETS` consumed by positional indices in 2 places (offset-by-N footgun)

**Category:** bad logic | **Severity:** medium | **Effort:** s | **Behavior-preserving:** yes
**Files:** [backend/repositories/origin.py:280-318](backend/repositories/origin.py#L280), [backend/repositories/origin.py:819-833](backend/repositories/origin.py#L819)

**Evidence.**
```python
# origin.py:280-302
# Column order: 0=edge_group, 1=is_total, 2=requests, 3=total_misses, ...
row = (rollup_row[3], rollup_row[4], rollup_row[5], ...)
```
Same shape rebuilt at `_origin_summary_from_temp:819` — but the TEMP variant lacks `edge_group`, `is_total`, `requests`, so its indices start at `0=total_misses`. Adding a column to one query without the other lands the rest on the wrong fields silently — no shape change, just wrong values.

**Remediation.** Switch to `cursor.description`-based dict building (the pattern `sessions.py:395-408` already uses):
```python
cur = runner.execute(SQL.SUMMARY_GROUPING_SETS.format(...), params)
cols = [d[0] for d in cur.description]
rows = [dict(zip(cols, r)) for r in cur.fetchall()]
rollup_row = next((r for r in rows if r["is_total"] == 1), None)
edge_rows = [r for r in rows if r["is_total"] == 0] if has_edge else []

payload = {
    "has_data": True,
    "total_misses": rollup_row["total_misses"],
    ...
    "by_leg": [
        {"edge": r["edge_group"], "requests": r["requests"],
         "p50_ms": r["ottfb_p50_ms"], "p95_ms": r["ottfb_p95_ms"]}
        for r in edge_rows
    ],
}
```
The `AS` aliases already present in `_sql/origin.py:20-58` become the contract.

**Tests.** Full origin test suite. **Highly recommend** golden-payload diff on dev port.

**Dependencies.** If [b1](#b1) is done first, this collapses to one site, making the change trivially safer.

---

### <a id="b11"></a>b11 · Dashboard cache hard-disabled but `sha256(json.dumps(...))` runs on every request

**Category:** inefficiency | **Severity:** low | **Effort:** xs | **Behavior-preserving:** yes
**Files:** [backend/repositories/dashboard.py:38-55](backend/repositories/dashboard.py#L38), [:119-147](backend/repositories/dashboard.py#L119), [:802-804](backend/repositories/dashboard.py#L802)

**Evidence.** `DASHBOARD_CACHE_TTL = 0` disables read+write, but the SHA + payload-build happens unconditionally. Small per-request cost.

**Remediation.** Hoist the `if DASHBOARD_CACHE_TTL > 0:` gate around the key-build block too, not just the read/write. Keeps the rollback hatch the comment intentionally preserves.

**Tests.** `pytest tests/repositories/test_dashboard*.py`.

---

### Deferred behavior-changing items (now in PR-14)

These are real findings — each is a deliberate behavior change. Folded into PR-14 per executor mandate; commit messages must call out the change.

- <a id="core-3"></a>**core-3** · `_ORPHAN_THRESHOLD_MINS = 5` in [backend/core/duckdb.py:38](backend/core/duckdb.py#L38) vs `60` in [backend/core/metadata/base.py:85](backend/core/metadata/base.py#L85). `_duckdb_status.py:352` reads the `5`; `metadata/cron_log.py:15,29,447` reads the `60`. **Operational decision required** before consolidating: status-busy-check vs cron orphan reaper have different threshold rationales.
- <a id="core-5"></a>**core-5** · `_safe_weakref` defined twice with divergent behavior. [query_registry.py:448-475](backend/core/query_registry.py#L448) falls back to a strong-ref closure (sqlite3 connections, which can't be weakref'd, stay tracked); [query_instrumentation.py:344-352](backend/core/query_instrumentation.py#L344) returns `None`. If instrumentation ever wraps a sqlite3 cursor, the memory probe silently no-ops. Unifying fixes the instrumentation path.
- <a id="core-9"></a>**core-9** · Promote `_TASK_TO_CRON_KEY` to module scope. Fixes a latent retention bug where `start_cron_run` uses the bad ternary form that `log_cron_run` was patched away from. Affects ~8 non-sync task types.
- <a id="core-15"></a>**core-15** · `_cleanup_temp_tables` runs unconditionally on every checkin. The docstring acknowledges it's "belt-and-suspenders for the failure paths" — but the failure path is unreachable because `release(con, errored=True)` discards the connection without calling cleanup. The sweep only catches a clean-exit path that forgot to use the `temp_table` context manager. **Removing the sweep gives up a fictional safety net.**

---

## Section 4 — Legacy / Migration / Upgrade-Support Code to Remove (Fresh-Install Contract)

### <a id="u1"></a>u1 · `backend/utils/retry.py` — wrappers defined, zero production callers

**Category:** legacy removal | **Severity:** medium | **Effort:** s | **Behavior-preserving:** yes (delete path)
**Files:**
- [backend/utils/retry.py](backend/utils/retry.py)
- Ad-hoc retry loops still alive at: [backend/utils/rdns_cache.py:46](backend/utils/rdns_cache.py#L46), [:324-329](backend/utils/rdns_cache.py#L324), [backend/core/fastly/client.py:29](backend/core/fastly/client.py#L29), [backend/core/duckdb.py:275](backend/core/duckdb.py#L275), [:399](backend/core/duckdb.py#L399), [backend/core/iceberg/sync.py:294](backend/core/iceberg/sync.py#L294), [:427](backend/core/iceberg/sync.py#L427), [backend/provision/fos_setup.py:302](backend/provision/fos_setup.py#L302)

**Evidence.** Module docstring states *"Adoption is incremental — call sites migrate from ad-hoc for attempt in range(...) loops to these decorators as they're touched."* Today: zero production importers. `rdns_cache.py:324-329` hand-rolls the exact policy `sqlite_busy_retry` produces.

**Recommendation: Path B (delete).**
- Delete `http_api_retry`, `generic_network_retry`, `HttpRetryable`, `_before_sleep_log`, and `backend/utils/retry.py` itself.
- Delete `tests/utils/test_retry.py`.
- Retract the docstring checklist (the module is gone).

**Why not Path A.** Adopting `@sqlite_busy_retry` in `rdns_cache._bulk_update_async` adds `[retry] attempt=N ...` WARNING logs that today are silent. Log-volume change only, but operations alerting keyed on `[retry]` would start firing.

---

### <a id="core-1"></a>core-1 · `backend/core/settings.py` — 1 consumer out of 11 documented

**Category:** legacy removal | **Severity:** medium | **Effort:** s | **Behavior-preserving:** yes (delete path)
**Files:**
- [backend/core/settings.py](backend/core/settings.py) — defines `Settings()`
- [backend/routers/admin_queries.py:30](backend/routers/admin_queries.py#L30) — only live consumer (reads `query_monitor_enabled`)
- All other documented consumers still `os.environ.get(...)`-ing directly: `backend/main.py`, [backend/utils/structlog_config.py:53](backend/utils/structlog_config.py#L53), [backend/core/request_telemetry.py:64](backend/core/request_telemetry.py#L64), [backend/core/iceberg/buffer.py:445](backend/core/iceberg/buffer.py#L445), [backend/core/share_db/connection.py:51](backend/core/share_db/connection.py#L51)

**Evidence.** The Phase 3.5 migration runway never landed. `_validate_proxy_headers_required_in_strict_mode` validator at lines 184-204 is unreached at runtime.

**Recommendation: Path B (delete).**
- Inline the one consumed field at `admin_queries.py:41`:
  ```python
  query_monitor_enabled = os.environ.get("QUERY_MONITOR_ENABLED", "true").lower() != "false"
  ```
  (Use the exact default/parsing from `settings.py`.)
- Delete `backend/core/settings.py`.
- Delete `tests/core/test_settings.py`.

---

### <a id="u4"></a>u4 · `backend/utils/cdn.py` — imported only by its test

**Category:** legacy removal | **Severity:** low | **Effort:** s | **Behavior-preserving:** yes (delete path)
**Files:** [backend/utils/cdn.py](backend/utils/cdn.py), [tests/utils/test_cdn.py](tests/utils/test_cdn.py); inline CDN GET sites at [backend/state_sync.py:260-302](backend/state_sync.py#L260), [backend/core/iceberg/_core.py:728-735](backend/core/iceberg/_core.py#L728), [backend/models/lake.py:63-75](backend/models/lake.py#L63)

**Recommendation: Path B (delete).** Three in-tree CDN GET sites independently build URLs by hand and continue to work. `build_cdn_url`'s `parse_qs` preservation is strictly more correct than the inline recipe, but no caller is using it. Delete `backend/utils/cdn.py` + `tests/utils/test_cdn.py`.

---

### <a id="u7"></a>u7 · `TunnelState.use_tunnel` / `tunnel_url` — "one release" deferral expired

**Category:** legacy removal | **Severity:** medium | **Effort:** s | **Behavior-preserving:** yes
**Files:**
- [backend/utils/tunnel/state.py:27-37](backend/utils/tunnel/state.py#L27)
- [backend/utils/tunnel/manager.py:374-411](backend/utils/tunnel/manager.py#L374)
- [backend/utils/remote_access.py:302-304](backend/utils/remote_access.py#L302), [:325](backend/utils/remote_access.py#L325)
- [backend/routers/share_admin.py:68-69](backend/routers/share_admin.py#L68), [:115](backend/routers/share_admin.py#L115), [:125](backend/routers/share_admin.py#L125)
- Frontend: `frontend/components/share-dashboard/SharingControlPanel.tsx:59`

**Evidence.** v2.0.0 shipped (commit `63e7d15`). The legacy fields are now always `False` / `None`. `share_admin.py` still publishes them in `/api/share/status` and accepts them in the start-sharing POST body.

**Remediation (4 commits).**
1. Delete the two fields from `TunnelState` dataclass at `state.py:27-37`.
2. Drop `manager.py` `use_tunnel` parameter + the `if use_tunnel:` guards at `:374-411`.
3. Drop `remote_access.py` dead branches at `:302-304` and `:325`.
4. Drop `share_admin.py` response keys (`use_tunnel`, `tunnel_url`) at `:68-69` and `:115`; drop the corresponding payload field at `:125`.

**Frontend follow-up.** Update `SharingControlPanel.tsx:59` alongside (it references the response keys). Regenerate `frontend/types/api.generated.ts` and `frontend/openapi.json` via the existing regen script.

**Persistence compat.** Old persisted JSON parses cleanly since `json.load` ignores extras.

**Tests.** `pytest tests/utils/test_tunnel_state.py tests/remote_access/test_tunnel.py tests/remote_access/test_share_admin_routes.py`. Manual: dev share-login flow (per `prod-verify-paths` memory: admin at SSH tunnel, analyst via `/share-login`).

---

### <a id="b3-scrypt"></a>b3-scrypt · Scrypt passcode verify + rehash-on-login dead for fresh installs

**Category:** legacy removal | **Severity:** medium | **Effort:** s | **Behavior-preserving:** yes
**Files:**
- [backend/core/share_db/passcode.py:44-86](backend/core/share_db/passcode.py#L44), [:112-138](backend/core/share_db/passcode.py#L112)
- [backend/core/share_db/invites.py:139-207](backend/core/share_db/invites.py#L139)
- [backend/core/share_db/schema.py:124-138](backend/core/share_db/schema.py#L124)
- [backend/core/share_db/settings.py:19](backend/core/share_db/settings.py#L19) — `PASSCODE_DEFAULT_ALGO_KEY` constant
- [backend/core/share_db/__init__.py:84](backend/core/share_db/__init__.py#L84) — re-export
- [tests/remote_access/test_share_db.py:66](tests/remote_access/test_share_db.py#L66), [:130-180](tests/remote_access/test_share_db.py#L130)

**Evidence.** Cutover happened pre-2.0. Fresh installs have no scrypt rows.

**Remediation (3 commits).**
1. `passcode.py`: delete `_verify_scrypt`, the scrypt parameter block, the scrypt branch in `verify_passcode`, the scrypt branch in `needs_rehash`.
2. `invites.py`: delete the rehash-on-login block at `:188-203`.
3. Tests + schema migration 003 + the `PASSCODE_DEFAULT_ALGO_KEY` constant + the re-export in `__init__.py`. Delete `_migration_003_passcode_algo_marker` (no code reads the marker — confirmed via grep).

**Leave alone:** the argon2id backup-envelope scrypt key derivation at [invites.py:323-400](backend/core/share_db/invites.py#L323) — separate, still-active code path.

**Tests.** `pytest tests/remote_access/test_share_db.py`. Manual: passcode creation + verification on a fresh local share_db.

---

### <a id="c10"></a>c10 · v2.0 "removed at v2.0 cut" tombstone comments

**Category:** cleanup | **Severity:** low | **Effort:** xs | **Behavior-preserving:** yes
**Files:** [backend/deps.py:187-192](backend/deps.py#L187), [:231-236](backend/deps.py#L231)

Two 6-line tombstone blocks describing symbols that no longer exist. Per the project's `public-comms-style` convention, v2.0 scaffolding noise shouldn't carry into long-lived source. The regression test at [tests/test_deps.py:236](tests/test_deps.py#L236) and the migration narrative at [backend/core/request_context.py:22-23](backend/core/request_context.py#L22) already carry the contract.

**Remediation.** Delete both tombstone blocks (12 lines).

---

### <a id="c1"></a>c1 · Six `try: pass / except: pass` no-op blocks in cron jobs

**Category:** dead code | **Severity:** low | **Effort:** xs | **Behavior-preserving:** yes
**Files:**
- [backend/cron/jobs/optimize.py:33-36](backend/cron/jobs/optimize.py#L33)
- [backend/cron/jobs/sync.py:67-70](backend/cron/jobs/sync.py#L67)
- [backend/cron/jobs/commit.py:51-54](backend/cron/jobs/commit.py#L51)
- [backend/cron/jobs/metadata.py:74-77](backend/cron/jobs/metadata.py#L74), [:328-331](backend/cron/jobs/metadata.py#L328), [:523-526](backend/cron/jobs/metadata.py#L523)

Residue from removed instrumentation calls. The `try` body is bare `pass` — the `except` is unreachable.

**Remediation.** Single commit removing 24 lines across 5 files.

**Tests.** `pytest tests/cron/`.

---

### <a id="core-14"></a>core-14 · `log_fields.py` Phase 7 docstring contradicts sibling

**Files:** [backend/core/log_fields.py:7-21](backend/core/log_fields.py#L7), [backend/core/field_registry.py:9-15](backend/core/field_registry.py#L9)

`log_fields.py` claims a Phase 7 migration is "in progress" and refers to `pending-docs/phase_7_field_registry_migration.md` (which doesn't exist — pending-docs/ gets squashed at merge). `field_registry.py` documents the duality as "intentional, not in-flight migration." `_log_fields_data.py:1-18` agrees.

**Remediation.** Drop the Phase 7 paragraph from `log_fields.py:7-21`, the pending-docs reference, and the "after the final caller migrates" plan. Align with the `intentional duality` framing.

---

## Section 5 — Files That Should Not Be Tracked

**No findings.** Audit surfaced no tracked secrets, no accidentally-committed local artifacts, and no generated outputs that should be regenerated. The hygiene of the tree is good.

### <a id="c12"></a>c12 · `backend/services/__init__.py` empty, single-module package

Package contains exactly one module (`service_manager.py`). All callers use the explicit deep import path. Empty `__init__.py` is legal but unhelpful. **Recommendation: skip.** Promotion to `backend/service_manager.py` updates ~13 import sites for marginal benefit.

### m9 · `backend/models/__init__.py` empty

Informational only — every caller deep-imports today.

---

## Section 6 — Suggested File-Structure Changes

### <a id="m1"></a>m1 · `backend/models/lake.py` is not a models module

**Category:** file structure | **Severity:** low | **Effort:** s | **Behavior-preserving:** yes
**Files:**
- [backend/models/lake.py:1-204](backend/models/lake.py#L1)
- Import sites: [backend/routers/provision.py:406](backend/routers/provision.py#L406), [backend/routers/services/core.py:69](backend/routers/services/core.py#L69), [backend/state_sync.py:267](backend/state_sync.py#L267), [backend/core/iceberg/_core.py:728](backend/core/iceberg/_core.py#L728)

**Evidence.** Zero `class` definitions. Four `def`s: `_safe_cdn_url`, `fetch_lake_info`, `_fetch_direct`, `_fetch_with_temp_cache`. Imports `from backend.core import iceberg as db_iceberg`, then `backend.core.iceberg._core` turns around and imports `_safe_cdn_url` back from `backend.models.lake` — layering inversion (core depends on models).

**Remediation.** Move to `backend/core/iceberg/lake_info.py`. Update the 4 import sites. Removes the cross-layer dependency.

**Tests.** `pytest tests/core/test_iceberg*.py tests/routers/test_provision*.py`.

---

### <a id="u9"></a>u9 · `sync_admin_state` doesn't belong in `utils/`

**Category:** file structure | **Severity:** low | **Effort:** s | **Behavior-preserving:** yes
**Files:** [backend/utils/router_utils.py:110-136](backend/utils/router_utils.py#L110)

`router_utils.py` mixes 4 concerns; `sync_admin_state` deferred-imports `backend.state_sync` and `backend.scheduler` (both layered above `utils/`). **Verifier note:** the move would NOT create an immediate cycle today — `state_sync.py` and `scheduler.py` don't import from `utils/`. The deferred-import is forward-looking insurance; the move is hygiene, not bug-prevention.

**Remediation.** Move `sync_admin_state` to a new `backend/routers/_state_sync.py` (or keep close to its 2 callers in `alerts.py` and `views.py`). Leave HTTPException mapping, debug request renderer, and SSE helpers in `router_utils.py` — those are genuine leaf utilities.

---

## Section 7 — Backend ↔ Frontend Type Sync

The audit surfaced **no confirmed wire-shape drift findings**. The openapi.json + api.generated.ts regen pipeline is doing its job.

The only adjacent observation: [frontend/app/origin/page.tsx:35-58](frontend/app/origin/page.tsx#L35) comment notes that the granular `/api/origin/{summary,timeseries,slow-urls,...}` endpoints exist "for rollback safety but should no longer fire from this page" — confirmed by grep returning zero frontend calls. That is a **backend-only cleanup opportunity** (folded into [b1](#b1)), not a wire-sync issue.

After PR-8 (tunnel legacy fields) and PR-3 (origin consolidation), regenerate frontend types:
```
# From frontend/
npm run gen:types
```
This runs `uv run python3 ../scripts/generate_openapi.py openapi.json && node ../scripts/refresh_api_types.js`. Confirm `frontend/types/api.generated.ts` shrinks and `frontend/openapi.json` no longer mentions `use_tunnel`, `tunnel_url`, or the deleted granular origin endpoints.

---

## Section 8 — Implementation Plan

### Pre-flight (one-time)

1. **Open decisions to make before starting** (1 hour, before any code):
   - [ ] [core-3](#core-3): pick one canonical value for `_ORPHAN_THRESHOLD_MINS` (5 or 60) — operational call.
   - [ ] [u1](#u1), [u4](#u4), [core-1](#core-1): confirm Path B (delete) for all three migration scaffolds. **Recommendation is delete.** Path A for any of them requires a separate decision because of log-volume changes.
   - [ ] [c5](#c5): acknowledge the latent sync.py status-log-on-failure bug fix lands with the refactor.
   - [ ] [u6](#u6): acknowledge the falco lint extraction also fixes a documented parser bug.
   - [ ] Defer-list confirmation: [core-5](#core-5), [core-9](#core-9), [core-15](#core-15) — these are real findings but introduce behavior changes; leave them out of the cleanup branch.

2. **Capture golden payloads** for behavior-preservation diffing. On dev (port 18002), hit each endpoint that PR-3 touches and stash JSON to `local-docs/` (per `local-only-docs` memory; never push):
   ```
   curl -s 'http://localhost:18002/api/origin/aggregates?service_id=<sid>&start=...&end=...' > local-docs/origin-aggregates.before.json
   # repeat for: timeseries, slow-urls, status-codes, path-breakdown, pop-latency, ip-health
   ```
   Re-run after PR-3 and diff: `diff <(jq -S . before.json) <(jq -S . after.json)`. Expect empty diff.

3. **Confirm test suite is green at HEAD before starting**:
   ```
   pytest -x
   cd frontend && npm test
   ```

---

### PR boundaries

Each PR is sized to land in 1 day or less with full test pass. Order matters where noted.

#### PR-1 · "Easy wins" cleanup (xs effort, zero risk)

**Findings:** [b5](#b5), [c1](#c1), [b8](#b8), [r9](#r9)
**LOC delta:** ~60 lines removed
**Tests:** `pytest tests/repositories/ tests/cron/ tests/test_deps.py tests/core/test_request_context.py`
**Why first:** Validates the workflow without committing to anything risky. Clears `git diff` noise so larger PRs read cleanly.

#### PR-2 · `iso_z_now()` adoption sweep

**Findings:** [u2](#u2), [u8](#u8)
**Order:** Land utility migrations across `backend/utils/*` first (u2), then tunnel re-export cleanup (u8). Byte-identical output throughout.
**Tests:** `pytest tests/utils/`

#### PR-3 · Origin repository consolidation

**Findings:** [b1](#b1), [b4](#b4)
**LOC delta:** ~700 lines removed across `origin.py` + `_sql/origin.py`
**Risk:** Medium. Largest behavior surface.
**Pre-req:** Golden payloads captured **immediately before this PR** (not at pre-flight — dev data drifts across PRs and breaks the diff). Start dev (`./run.sh --dev`), resync from cloud if local data is sparse, then `curl` each affected endpoint and stash JSON to `local-docs/origin-payloads-before/`. Re-capture into `local-docs/origin-payloads-after/` after PR-3 commits land; diff with `jq -S`.
**Order:**
- Commit A: SQL template unification with `{lat_val}` placeholder.
- Commit B: Python function collapse.
- Commit C (optional): delete the rollback-safety routes if frontend no longer calls them. Confirm via:
  ```
  grep -rE 'api/origin/(summary|timeseries|slow-urls|status-codes|path-breakdown|pop-latency|ip-health)' frontend/
  ```
**Tests:** Full `tests/repositories/test_origin*` + `tests/repositories/_sql/test_origin*` + golden-payload diff on dev.

#### PR-4 · Delete three migration scaffolds

**Findings:** [u1](#u1), [u4](#u4), [core-1](#core-1)
**Order:** One commit per module.
- Commit A: Delete `backend/utils/retry.py` + tests.
- Commit B: Delete `backend/utils/cdn.py` + tests.
- Commit C: Inline `query_monitor_enabled` into `admin_queries.py:41`, delete `backend/core/settings.py` + tests.
**Tests:** Full suite; specifically `pytest tests/utils/ tests/core/ tests/routers/`.

#### PR-5 · `SectionTimer` consolidation

**Findings:** [b2](#b2), [r7](#r7)
**Approach:** Add `SectionTimer` class to `_base.py`, migrate one callsite per commit (12 callsites total). Standardise on `perf_counter()`.
**Tests:** Run per-file tests after each migration.

#### PR-6 · Rollups `_common.py` extractions

**Findings:** [b3](#b3), [b3-describe](#b3-describe), [b3-discover](#b3-discover), [b3-migration](#b3-migration), [b3-rollups-hour-token](#b3-rollups-hour-token)
**Approach:** Five small extractions, can bundle as one commit each or one PR for all five.
**Tests:** `pytest tests/core/test_rollups* tests/repositories/test_sessions*.py tests/core/test_metadata_db_migrations.py`.

#### PR-7 · Delete legacy `usage_log` schema

**Finding:** [b3-usage-log](#b3-usage-log)
**LOC delta:** ~300 lines removed
**Risk:** Low (fresh-install contract). Includes one latent bug fix (`get_latest_reconciliation_ts` was always returning None on fresh installs).
**Order:** 4 commits as listed in the finding.
**Tests:** `pytest tests/core/test_metadata*.py tests/core/test_metadata_db_migrations.py tests/routers/test_usage_log.py`.

#### PR-8 · Tunnel legacy fields + scrypt passcode

**Findings:** [u7](#u7), [b3-scrypt](#b3-scrypt)
**Order:**
- Commits 1-4: Tunnel field deletion (per u7).
- Commits 5-7: Scrypt cleanup (per b3-scrypt).
- Commit 8: Frontend regenerate types (`npm run generate-api-types` or equivalent), update `SharingControlPanel.tsx:59`.
**Tests:** `pytest tests/utils/test_tunnel_state.py tests/remote_access/test_tunnel.py tests/remote_access/test_share_admin_routes.py tests/remote_access/test_share_db.py`. Manual: full share-login + analyst flow on dev (per `prod-verify-paths`).

#### PR-9 · `ThreadLocalPool` extraction

**Finding:** [b3-pool](#b3-pool)
**LOC delta:** ~250 lines removed
**Risk:** Medium (changes share_db observability — share_db queries become visible in Live Query Monitor; document in PR description).
**Order:** 4 commits as listed in the finding (extract → migrate usage_log_db → migrate metadata/base → migrate share_db/connection with `connect_fn` + `on_borrow`).
**Tests:** `pytest tests/core/test_metadata*.py tests/remote_access/test_share_db.py tests/routers/test_usage_log.py`. Manual on dev: open `/admin/queries`, hit share-login, confirm share_db queries now appear.

#### PR-10 · Router & cron helpers

**Findings:** [r3](#r3), [r8](#r8), [c3](#c3), [c4](#c4), [c5](#c5), [b3-iceberg-cdn-purge](#b3-iceberg-cdn-purge), [b3-iceberg-pointer-key](#b3-iceberg-pointer-key)
**Approach:** Multiple small extractions; one commit per helper.
**Tests:** `pytest tests/routers/ tests/cron/ tests/core/test_iceberg*.py`.

#### PR-11 · `ResponseCache` for origin

**Finding:** [b9](#b9)
**Approach:** Wrap `BoundedTTLCache` in `ResponseCache`. Migrate origin's ~6 endpoints incrementally.
**Tests:** `pytest tests/repositories/test_origin*.py tests/utils/test_bounded_cache.py`. Verify cache hit/miss markers in `/admin/queries`.

#### PR-12 · Smaller cleanups

**Findings:** [b7](#b7), [b10](#b10), [b11](#b11), [c8](#c8), [c10](#c10), [core-10](#core-10), [core-11](#core-11), [core-13](#core-13), [core-14](#core-14), [m4](#m4), [m5](#m5), [r5](#r5), [r6](#r6), [u5](#u5), [u6](#u6), [u10](#u10), [b3-iceberg-package-proxy](#b3-iceberg-package-proxy)
**Approach:** Single PR with one commit per finding. All xs/s effort, all behavior-preserving (except core-13's latency profile shift — document in commit message).
**Tests:** Run full backend suite at the end.

#### PR-13 · File-structure moves

**Findings:** [m1](#m1), [u9](#u9)
**Approach:** Two import-renaming commits. Mechanical.
**Tests:** Full backend suite; verify no circular imports with `python -c "import backend.main"`.

#### PR-14 · Deferred behavior-changing items (now in scope)

**Findings:** [core-3](#core-3), [core-5](#core-5), [core-9](#core-9), [core-15](#core-15)
**Behavior-preserving:** **no** — each is a deliberate behavior change; commit messages must call out the change.
**Approach (one commit per finding):**
- core-3: pick one canonical `_ORPHAN_THRESHOLD_MINS` after reading both call sites ([backend/core/_duckdb_status.py:352](backend/core/_duckdb_status.py#L352) reads `5`; [backend/core/metadata/cron_log.py:15,29,447](backend/core/metadata/cron_log.py#L15) reads `60`). Document operational rationale in commit message.
- core-5: promote the registry-version `_safe_weakref` (with strong-ref fallback) over the instrumentation-version (returns None). Fixes a silent memory-probe no-op when wrapping sqlite3 cursors.
- core-9: lift `_TASK_TO_CRON_KEY` to module scope, mirroring the corrected ternary `log_cron_run` uses. Fixes a latent retention bug affecting ~8 non-sync task types.
- core-15: delete the per-checkin `_cleanup_temp_tables` sweep. Failure-path "safety net" is unreachable (`release(con, errored=True)` discards the connection without calling cleanup).
**Tests:** `pytest tests/core/test_duckdb*.py tests/core/test_metadata_db_concurrency.py tests/core/test_metadata_db_audit.py tests/cron/`.

---

### Suggested calendar shape

| Day | PR | Effort |
|---|---|---|
| 1 (AM) | PR-1, PR-2 | xs+s |
| 1 (PM) | PR-4 | s×3 |
| 2 | PR-3 | l (high care) |
| 3 (AM) | PR-5, PR-6 | s, xs×5 |
| 3 (PM) | PR-7 | s |
| 4 | PR-8 | s+s + frontend regen |
| 5 | PR-9 | l |
| 6 (AM) | PR-10 | s×7 |
| 6 (PM) | PR-11 | m |
| 7 | PR-12, PR-13 | xs×17 + s×2 |

Roughly one work week for the full sweep if the implementer is the project owner; ~2 weeks for a new contributor with onboarding.

---

### Rollback strategy

- All PRs are behavior-preserving except where flagged. Reverting any single PR restores prior behavior.
- PR-7 (legacy usage_log) has a one-way safety contract: reverting **after** any user has accumulated data in the new pool is fine (legacy table re-creates empty); the only loss is the rollback-window data, but there is no such data because the legacy table has been read-only since v2.0.
- PR-9 (ThreadLocalPool) changes share_db observability. Rollback restores invisibility — verify ops alerting expectations before rollback.
- PR-3 (origin consolidation) and PR-12's b10 (cursor.description) should be deployed with the golden-payload diff in hand. Rollback if any field mismatches.

---

### Verification checklist (per PR)

- [ ] `pytest <scoped-test-paths>` green
- [ ] `pytest` (full) green
- [ ] `pre-commit run --all-files` clean
- [ ] Local dev (per `verify-dev-first` memory): `./run.sh --dev` and exercise the affected endpoints on `localhost:13002` (frontend) + `localhost:18002` (backend); ports come from `.env` (BACKEND_PORT=18002, FRONTEND_PORT=13002)
- [ ] If frontend types changed (PR-8, possibly PR-3): regenerate, commit the regenerated files
- [ ] If touching share_db / tunnel / share-login: dev sandbox scrub confirmed (per `dev-sandbox-scrub` memory — disable crons under `provisioning.*`, clear `cdn_url`)
- [ ] Branch resource ceiling: do NOT spawn parallel subagents while dev server runs (per `resource-limits` memory)
- [ ] After PR-9 only: open `/admin/queries` on dev and confirm share_db queries appear

---

### Production deploy

After all PRs land on `refactor/cleanup` and squash-merge to `main`:

1. Push `main` to origin.
2. SSH to the GCE VM (per `gce-deploy-rebuild` memory).
3. Pre-flight: `git fetch && git reset --hard origin/main` (in case of any force-push history).
4. Run `~/restart.sh` (git pull + docker compose --build + health check).
5. Hard-refresh the browser for frontend cache.
6. Verify both paths per `prod-verify-paths`:
   - Admin via SSH-tunneled `localhost:3001` (Caddy-marker gate).
   - Analyst via the Fastly URL `/share-login` (email + passcode flow).
7. Confirm `/admin/queries` shows share_db queries (PR-9 validation).
8. Tail logs for ~10 minutes: `docker compose logs -f backend caddy` and confirm no new ERRORs or unexpected WARNINGs.

---

## Appendix A — Verifier notes worth carrying

- **u1** Path A's "behavior preserving" is overstated — adopting `@sqlite_busy_retry` adds `[retry] attempt=N` WARNING logs to `_bulk_update_async` that today are silent. Path B (delete) has no such caveat.
- **c5** `commit.py` puts the success log inside the try/except (failure → no status log). `sync.py` puts it outside (failure today still emits a misleading "View refresh + warm: Xms" status event). The proposed helper fixes this latent bug.
- **core-13** Worst-case latency changes from 5s to ~44s (timeout + retry backoff). Caller is behind a name cache and falls back to cached/config name, so acceptable.
- **b3-pool** `share_db` re-asserts `PRAGMA foreign_keys=ON` on every borrow with a closed-connection fallback. The other two pools don't. The `ThreadLocalPool` extraction needs an `on_borrow` hook to preserve that.
- **b3-usage-log** `get_latest_reconciliation_ts` silently returns `None` on fresh installs today — fixing it is a behavior fix (zero callers will notice).
- **u9** Moving `sync_admin_state` to `backend/routers/` would NOT create an immediate cycle today. The deferred-import is forward-looking insurance; the move is hygiene, not bug-prevention.
- **r5** Real duplicate count for `client_ip` is ~11+, not 8 — additional inlinings at `main.py:585`, `remote_access.py:251,682`.

---

## Appendix B — Cross-cutting observations

1. **The `_phase` helper pattern is the cleanest one-shot cleanup in the entire audit** — combining [b2](#b2) and [r7](#r7) into a single `SectionTimer` lifted to `_base.py` removes 12+ copies in a single commit.
2. **Three migration scaffolds at the same level of unadoption** ([u1](#u1), [u4](#u4), [core-1](#core-1)) suggest the v2.0 cleanup tried to bite off more migration runway than ended up landing. The "delete or commit" round handles all three in one sitting.
3. **Comments in this codebase are unusually honest about duplication.** `usage_log_db.py:212-215` says "Exact copies"; `_origin_summary_from_temp` calls out "same fix as get_summary above"; `_safe_weakref` in instrumentation claims to mirror the registry version; the share_db migration_003 comment notes the scrypt fallback. The audit's job was largely to enumerate what the author already flagged in-tree. Most of the proposed work is small commits informed by comments already in the source.

---

## Appendix C — Methodology audit trail

Workflow: `fla-fullrepo-audit` (single run)
Lanes: 18 (8 backend, 7 frontend, 3 cross-cutting)
Subagents: 193 (1 inventory + 18 finders + 174 verifiers)
Subagent tokens: 8.79M
Tool uses: 2,811
Wall time: ~32 minutes
Verification verdict distribution: 123 confirmed / 48 partially confirmed / 2 refuted / 0 stale

Findings without a verified `confirmed` or `partially_confirmed` verdict were dropped before this document was authored. Every cited file path and line range was checked against the working tree at HEAD of `refactor/cleanup`.
