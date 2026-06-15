"""Shared helpers used by every repository module.

Import from here instead of redefining in each file:

    from backend.repositories._base import _safe_table, _get_schema, safe_iso, QueryRunner
"""

from __future__ import annotations

import contextlib
import heapq
import re
import time
from typing import Any

import duckdb


class SectionTimer:
    """Per-request wall-clock timer that builds the ``_section_timings``
    list the perf harness reads.

    Replaces the per-function ``_phase(name, t0)`` / ``_timed(name, fn)``
    closures that several repos and routers each defined inline. Pass
    an existing list to ``entries`` to share the sink with a caller (for
    helpers that take an optional ``section_timings`` argument).
    """

    __slots__ = ("entries",)

    def __init__(self, entries: list[dict] | None = None) -> None:
        self.entries: list[dict] = entries if entries is not None else []

    def mark(self, name: str, t0: float) -> None:
        self.entries.append({"section": name, "time_ms": round((time.perf_counter() - t0) * 1000, 2)})

    def call(self, name: str, fn):
        t0 = time.perf_counter()
        try:
            return fn()
        finally:
            self.mark(name, t0)


# Pre-compile once; called per ``runner.execute`` invocation.
_PARQUET_LIST_RE = re.compile(r"read_parquet\(\[\s*('[^']+'\s*(?:,\s*'[^']+'\s*)*)\]")


# Cache for ``QueryRunner.get_schema_cols``, keyed on
# ``(service_id, log_format_hash)``. The schema only changes when an
# admin edits the log format (which mints a new ``format_hash`` on the
# saved config — see ``backend.routers.services.core``); a new
# ``format_hash`` produces a cache miss naturally, so no explicit
# invalidation hook is needed. Cap at 64 entries to bound memory in the
# pathological case where format_hash churns (e.g., test fixtures).
#
# Why this exists: SUMMARIZE-over-the-Iceberg-view walks the manifest
# list, which sits on FOS in production. The perf audit clocked
# ``get_schema_cols`` at 2.8s p50 on a cold prod connection vs <1ms on
# warm local — the same SUMMARIZE that takes <1ms once the manifests
# are in-process burns seconds per request when it isn't cached.
_schema_cols_cache: dict[tuple[str, str], list[str]] = {}
_SCHEMA_COLS_CACHE_MAX_ENTRIES = 64


def _schema_cols_cache_key(src: dict) -> tuple[str, str] | None:
    """Return the cache key for ``src``, or ``None`` if we shouldn't cache.

    We need BOTH a stable service id AND a format_hash. Missing either
    means the source dict is malformed or pre-dates the format_hash
    field — fall through to the uncached path rather than risk caching
    under a key we can't invalidate.
    """
    sid = src.get("service_id") or src.get("name")
    fmt = (src.get("log_fields") or {}).get("format_hash")
    if not sid or not fmt:
        return None
    return (sid, fmt)


def clear_schema_cols_cache(service_id: str | None = None) -> None:
    """Drop cached schema columns.

    With ``service_id=None``, clears everything. With a specific id,
    drops entries for that service across all format_hashes (useful in
    tests). Production code shouldn't need to call this — the
    format_hash-keyed cache invalidates itself on log_format changes.
    """
    global _schema_cols_cache
    if service_id is None:
        _schema_cols_cache.clear()
    else:
        _schema_cols_cache = {k: v for k, v in _schema_cols_cache.items() if k[0] != service_id}


# Cache for ``os.listdir`` on the rollup directory tree. The dir_enum
# pass inside ``QueryRunner.execute_top_n_rollups`` calls listdir once
# per (field) at the field-hour and field-day roots, plus once at the
# bundled-hour root. On prod that's ~80 listdirs returning ~375 entries
# each per request and lands at 1.3-3 s of pure stat work — sometimes
# the bulk of the request — per the perf audit (F5).
#
# The cron sync rebuilds the rollup tree at most every minute, so a
# 60 s TTL captures changes without ever serving rollup output that's
# more than one tick stale. Bounded by entry count so unbounded service
# / hour churn can't blow the cache.
_listdir_cache: dict[str, tuple[float, list[str]]] = {}
_LISTDIR_CACHE_TTL_S = 60.0
_LISTDIR_CACHE_MAX_ENTRIES = 4096


def _cached_listdir(path: str) -> list[str]:
    """Return ``os.listdir(path)`` cached for ``_LISTDIR_CACHE_TTL_S``.

    Returns ``[]`` on any OSError (matching the existing call-site
    behaviour around the rollup tree — callers treat missing/empty
    directories the same). The cache is intentionally simple: no
    per-entry expiry sweep, just a flat-clear when full.
    """
    import time as _time

    now = _time.monotonic()
    cached = _listdir_cache.get(path)
    if cached is not None and (now - cached[0]) < _LISTDIR_CACHE_TTL_S:
        return cached[1]
    try:
        import os as _os

        entries = _os.listdir(path)
    except OSError:
        entries = []
    if len(_listdir_cache) >= _LISTDIR_CACHE_MAX_ENTRIES:
        _listdir_cache.clear()
    _listdir_cache[path] = (now, entries)
    return entries


def clear_listdir_cache() -> None:
    """Drop the cached rollup listdir entries. Used by tests + the
    sync writer's commit hook when fresh files have been written."""
    _listdir_cache.clear()


def collect_hourly_bundle_paths(
    src: dict,
    st,
    et,
    bundled_root: str,
    bundle_filename: str,
) -> tuple[list[str], bool] | None:
    """Walk ``[st, et)`` by UTC hour, return ``(paths, crosses_active)``.

    Returns the list of per-hour bundle paths that exist on disk plus a
    ``crosses_active`` flag set when the window extends into (or past)
    the live hour. Returns ``None`` if any closed hour has per-field
    rollup data but no bundle on disk — that's the writer-behind case
    where serving the rollup path would undercount, so the caller falls
    back to raw.

    Shared between :meth:`QueryRunner.try_time_series_from_rollup` and
    :func:`backend.repositories.sessions._collect_sessions_rollup_paths`.
    The two callsites used to maintain identical walk logic with
    cross-referenced "mirrors X" comments; the dual maintenance is now
    one helper. The per-field listdir is done inline (callers do not
    pre-supply it).
    """
    import os
    from datetime import UTC, datetime, timedelta

    from backend.core.rollups import _rollups_root

    hour_per_field_root = _rollups_root(src)
    try:
        field_dirs = [f for f in _cached_listdir(hour_per_field_root) if f.startswith("field=")]
    except OSError:
        field_dirs = []

    # Pre-collect the union of all hour=… entries across every field dir
    # in one pass. The previous shape probed os.path.isdir per (hour,
    # field) inside _hour_had_any_data — on a 7-day window with ~70
    # fields that's 168 × 70 ≈ 11.8k isdir syscalls per /api/sessions
    # request, often the dominant cost of rollup_paths_collect. The
    # union set turns each hour check into an O(1) lookup, and the
    # per-field listdir hits the 60 s ``_cached_listdir`` cache so
    # back-to-back requests skip the I/O entirely.
    all_rollup_hours: set[str] = set()
    for f in field_dirs:
        try:
            for entry in _cached_listdir(os.path.join(hour_per_field_root, f)):
                if entry.startswith("hour="):
                    all_rollup_hours.add(entry[len("hour=") :])
        except OSError:
            continue

    def _hour_had_any_data(h: str) -> bool:
        return h in all_rollup_hours

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


def _compact_sql_for_debug(sql: str) -> str:
    """Replace explicit ``read_parquet([...long file list...])`` literals
    with ``read_parquet([N files])`` for transport in the debug-panel
    payload.

    The dashboard's per-request SQL embeds hundreds of buffer/rollup
    parquet paths in a single ``read_parquet`` call. Shipping those
    verbatim made ``_debug_queries`` ~220 KB of the response (60% of
    total) — pure network + JSON-parse cost on every dashboard refresh
    when the operator has ``DEBUG_RESPONSES=true`` set. The path list
    isn't useful to a human reading the debug panel; the count is.

    Compacting cuts the field to ~tens of bytes per query without
    losing the SQL shape an operator cares about for tuning.
    """

    def _replace(m: re.Match) -> str:
        # Count items by quote pairs — cheap and exact.
        count = m.group(1).count("'") // 2
        return f"read_parquet([{count} files]"

    return _PARQUET_LIST_RE.sub(_replace, sql)


@contextlib.contextmanager
def _attach_sqlite(con: duckdb.DuckDBPyConnection, sqlite_path: str, alias: str):
    """Context manager that ATTACHes a SQLite file to a DuckDB connection read-only.

    Yields True if the ATTACH succeeded, False otherwise (file missing, SQLite
    extension unavailable, etc.). DETACHes on exit. Callers should treat a False
    yield as "the bridge isn't available — fall back to a Python-side query".
    """
    import os

    attached = False
    if sqlite_path and os.path.exists(sqlite_path):
        try:
            escaped = sqlite_path.replace("'", "''")
            con.execute(f"ATTACH '{escaped}' AS {alias} (TYPE SQLITE, READ_ONLY)")
            attached = True
        except Exception:
            pass
    try:
        yield attached
    finally:
        if attached:
            try:
                con.execute(f"DETACH {alias}")
            except Exception:
                pass


@contextlib.contextmanager
def attach_ngwaf_cache(con: duckdb.DuckDBPyConnection, schema_cols: list[str], alias: str = "ngwaf_cache"):
    """Attach the NGWAF bot cache SQLite database when the schema warrants it."""
    if "waf_req_id" not in schema_cols:
        yield False
        return
    from backend import config as svcconfig

    with _attach_sqlite(con, svcconfig.ngwaf_db_path(), alias) as attached:
        yield attached


@contextlib.contextmanager
def attach_metadata_db(con: duckdb.DuckDBPyConnection, service_id: str, alias: str = "meta"):
    """Attach the per-service metadata SQLite file (alerts, views, asn_names, …).

    Use ``meta.<table_name>`` to reference rows from inside a DuckDB query
    (e.g. JOINing the log table against `meta.asn_names`). Yields False if the
    file doesn't exist yet — callers should fall back gracefully.
    """
    from backend.core import metadata_db

    with _attach_sqlite(con, metadata_db.db_path(service_id), alias) as attached:
        yield attached


def _safe_table(name: str) -> str:
    """Convert a source name to a valid SQL table identifier."""
    from backend.core.duckdb import _safe_table_name

    return _safe_table_name(name)


def _get_schema(con: duckdb.DuckDBPyConnection, src: dict) -> list[dict]:
    """Return the schema for the given source's log table."""
    from backend.core.duckdb import get_schema

    return get_schema(con, src)


from backend.utils.date_utils import safe_iso  # noqa: E402, F401 — re-export


def _is_stale_view_error(e: Exception) -> bool:
    """Return True when the error indicates an Iceberg view that references a deleted buffer file."""
    msg = str(e)
    return (
        "No files found" in msg
        or "Catalog Error: Table with name" in msg
        or "does not exist" in msg
        or "No such file or directory" in msg
    )


def optional_col(col: str, actual_cols, default: str = "NULL") -> str:
    """Return a quoted column reference if the column exists, else a SQL default expression.

    Escapes internal double quotes (DuckDB identifier-quote escape: `"` → `""`)
    so a hostile column name (admin-defined custom log fields can contain
    arbitrary characters) cannot break out of the quoted identifier into raw
    SQL. See audit finding 004.
    """
    return '"{}"'.format(col.replace('"', '""')) if col in actual_cols else default


VALID_CHART_INTERVALS: frozenset[str] = frozenset({"1 second", "1 minute", "1 hour", "1 day"})


def safe_interval(requested: str, default: str = "1 minute") -> str:
    """Return requested if it is a known-safe interval string, else default."""
    return requested if requested in VALID_CHART_INTERVALS else default


CANONICAL_METRICS = {
    "hit_rate": "ROUND(COUNT(*) FILTER (WHERE {cache_col} IN ('HIT', 'HIT-STALE')) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "requests": "COUNT(*)",
    "avg_ttfb": "ROUND(AVG(ttfb) * 1000.0, 2)",
    "p95_ttfb": "ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttfb) * 1000.0, 2)",
    "5xx_rate": "ROUND(COUNT(*) FILTER (WHERE status >= 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "4xx_rate": "ROUND(COUNT(*) FILTER (WHERE status >= 400 AND status < 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "avg_resp_bytes": "ROUND(AVG(resp_bytes), 2)",
    "total_resp_bytes": "SUM(resp_bytes)",
    "throughput": "ROUND(COALESCE(MEDIAN(CASE WHEN ({cache_col} ILIKE 'HIT%%') AND {elapsed_col} > 0 THEN {resp_bytes_col} * 1e6 / NULLIF(CAST({elapsed_col} AS DOUBLE), 0) ELSE NULL END), 0), 2)",
    "req_size": "ROUND(COALESCE(MEDIAN(CAST({header_bytes_col} AS DOUBLE) + CAST({req_bytes_col} AS DOUBLE)), 0), 2)",
    "ttfb_ms": "ROUND(COALESCE(MEDIAN(CASE WHEN ttfb IS NOT NULL AND ttfb > 0 THEN ttfb * 1000.0 ELSE NULL END), 0), 2)",
}


# ── Canonical SQL expression builders ─────────────────────────────────────────


def time_bucket_select(interval: str, ts_col: str = "timestamp") -> str:
    """Return 'time_bucket(INTERVAL <interval>, <ts_col>) AS bucket'.

    Validates the interval via safe_interval to prevent injection.
    """
    safe_iv = safe_interval(interval)
    return f"time_bucket(INTERVAL '{safe_iv}', {ts_col}) AS bucket"


def percentile_ms_expr(col: str, p: float = 0.95, filter_expr: str = "") -> str:
    """Return PERCENTILE_CONT expression that converts microseconds → milliseconds."""
    filter_clause = f" FILTER ({filter_expr})" if filter_expr else ""
    return f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {col}){filter_clause} / 1000.0"


def error_rate_expr(status_col: str = "status", threshold: int = 500, filter_expr: str = "") -> str:
    """Return percentage of rows where status_col >= threshold."""
    filter_clause = f" FILTER ({filter_expr})" if filter_expr else ""
    return f"ROUND(SUM(CASE WHEN {status_col} >= {threshold} THEN 1 ELSE 0 END){filter_clause} * 100.0 / NULLIF(count(*){filter_clause}, 0), 2)"


def origin_latency_us_expr(actual_cols: set[str] | list[str]) -> str:
    """Unified expression resolving ottfb (us) vs ttfb (s) vs nothing."""
    if "ottfb" in actual_cols and "ttfb" in actual_cols:
        return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
    elif "ottfb" in actual_cols:
        return '"ottfb"'
    elif "ttfb" in actual_cols:
        return '"ttfb" * 1000000.0'
    else:
        return "NULL"


def empty_schema_response(**extra_fields) -> dict:
    """Return a safe empty response dict when the log table has no schema yet.

    Callers merge in response-specific empty collections via extra_fields.
    Example: empty_schema_response(rows=[], series=[])
    """
    return {"has_data": False, "total": 0, **extra_fields}


def get_source_extent(
    runner: QueryRunner,
    src: dict,
    orig_table_name: str,
) -> tuple[int, str | None, str | None]:
    """Return (total_rows, earliest_log_at, latest_log_at) for a source.

    Checks the svcconfig status cache first; falls back to a live COUNT query.
    """
    try:
        from backend import config as svcconfig

        cached = svcconfig.get_status(src["name"])
        if cached:
            return (
                cached.get("local_rows", 0),
                cached.get("earliest_log_at"),
                cached.get("latest_log_at"),
            )
        row = runner.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {orig_table_name}").fetchone()
        if row:
            return row[0], safe_iso(row[1]), safe_iso(row[2])
    except Exception:
        try:
            count = runner.execute(f"SELECT count(*) FROM {orig_table_name}").fetchone()[0]
            return count, None, None
        except Exception:
            pass
    return 0, None, None


# Mapping from rollup-supported chart_metric to the SQL expression that
# computes the SAME value over RAW rows. Used by
# ``QueryRunner.try_time_series_from_rollup`` when the requested window
# crosses the active hour, so the live slice produces buckets that align
# numerically with the rollup-served buckets.
#
# Returns ``None`` for metrics not in :attr:`QueryRunner._TS_ROLLUP_METRIC_SQL`.
def _live_metric_sql_from_raw(chart_metric: str) -> str | None:
    return {
        "requests": "COUNT(*)",
        "5xx": "ROUND(COUNT(*) FILTER (WHERE status >= 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
        "4xx": "ROUND(COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499) * 100.0 / NULLIF(COUNT(*), 0), 2)",
        "hit_rate": "ROUND(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    }.get(chart_metric)


class QueryRunner:
    """
    Centralises query execution, debug tracking, schema fallback, and stale-view
    recovery logic. Reduces boilerplate in repository functions.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, src: dict):
        self.con = con
        self.src = src
        self.debug_queries: list[dict] = []
        self.actual_cols: set[str] = set()

        from backend.core.iceberg import inject_view_debug

        inject_view_debug(self.debug_queries, src)

    @property
    def debug_calls(self) -> list[dict]:
        """Return the list of calls tracked in the current context."""
        try:
            from backend.utils.telemetry import get_tracked_calls

            return get_tracked_calls()
        except ImportError:
            return []

    def execute(self, q: str, p: list | None = None):
        """Execute a query and track its execution time.

        Self-heals on stale-view errors: if the connection's bound view
        references a buffer parquet file that no longer exists (the sync
        cron deleted it between the view bind and this query), refresh
        the view once and retry. Belt-and-suspenders alongside the pool's
        checkout fingerprint — that catches the common case, this catches
        the race where a commit lands while a query is in flight.

        ``execute_with_retry`` below also does this, but most callers use
        plain ``execute()``, so the retry needs to live here too. The
        cost when nothing's stale is a single Python try/except — no SQL,
        no extra round-trip.
        """
        t0 = time.time()
        try:
            res = self.con.execute(q, p if p is not None else [])
        except Exception as e:
            if not _is_stale_view_error(e):
                raise
            try:
                from backend.core import iceberg as db_iceberg

                # Bust the cached view SQL FIRST. ``force=True`` below
                # skips the lock-free fast path, but its lock-acquire
                # timeout fallback (iceberg.py:2913-2926) re-executes
                # ``_view_cache[source_key][3]`` — the SAME stale SQL
                # that referenced the missing buffer file. Re-executing
                # that cached SQL just re-binds the dead paths into
                # this connection, and the retry of the original query
                # raises the same IOException again. Clearing the cache
                # makes that fallback's ``if cached and cached[3]`` check
                # False, so it falls through to persistent-view / extended-
                # lock-wait paths that actually have a chance to produce
                # fresh SQL.
                #
                # Mirrors the get_sync_status self-heal pattern at
                # backend/core/duckdb.py:1284. ``keep_snapshot_cache=True``
                # preserves the snapshot/path cache so a transient
                # catalog-load blip (FOS rate limit, network) doesn't
                # collapse the view to "WHERE false".
                #
                # 2026-06-05 prod incident: the dashboard surfaced
                # "No files found ... batch_0398ac66102f151b.parquet"
                # to all users for ~30 min because this clear call was
                # missing. The self-heal was firing but the lock was
                # contended by the every-10s sync cron, so the cached-SQL
                # fallback fired and re-bound the same stale paths.
                db_iceberg.clear_source_caches(self.src.get("name", "default"), keep_snapshot_cache=True)
                db_iceberg.update_iceberg_view(self.con, self.src, force=True)
            except Exception:
                # Refresh itself failed — surface the ORIGINAL error so
                # callers see the real symptom, not the rebind side-effect.
                raise e
            res = self.con.execute(q, p if p is not None else [])
        self.debug_queries.append(
            {"sql": _compact_sql_for_debug(q.strip()), "time_ms": round((time.time() - t0) * 1000, 2)}
        )
        return res

    def get_schema_cols(self) -> list[str]:
        """Get schema columns, retrying and refreshing the view if needed.

        Result is cached per ``(service_id, log_format_hash)`` so the
        SUMMARIZE-over-Iceberg-view cost is paid once per format
        revision instead of per request. See ``_schema_cols_cache``
        above for the rationale (2.8s p50 cold on prod).
        """
        cache_key = _schema_cols_cache_key(self.src)
        if cache_key is not None and cache_key in _schema_cols_cache:
            cached = _schema_cols_cache[cache_key]
            self.actual_cols = set(cached)
            return cached

        actual_cols = [col["name"] for col in _get_schema(self.con, self.src)]
        if not actual_cols:
            # The connection's bound view is stale — most likely the sync
            # cron deleted a buffer file the cached view SQL still references,
            # so the SUMMARIZE inside ``_get_schema`` raised IOException and
            # fell through to the "no schema" branch. ``force=True`` skips
            # the lock-free fast path AND skips the lock-acquire-timeout
            # fallback that re-executes the SAME stale cached SQL —
            # without it, the connection keeps re-binding the dead view
            # and the dashboard serves an empty response indefinitely until
            # the process restarts. Witnessed in prod 2026-06-09 when an
            # otherwise-healthy backend started returning ``total_rows=0``
            # for KLJP on every dashboard request despite the sync cron
            # logging successful view refreshes — the cron updates ITS
            # write connection's view but the pool's read-only connections
            # were stuck with the pre-delete cached SQL.
            try:
                from backend.core import iceberg as db_iceberg

                # Mirror execute()'s self-heal: bust the cached view SQL
                # FIRST so the lock-timeout fallback in update_iceberg_view
                # (iceberg.py:3306-3312) can't re-execute the SAME stale
                # SQL when ingest is holding the per-service lock. Without
                # this, the self-heal "succeeds" but the view stays bound
                # to the dead buffer path — _get_schema returns [] again,
                # the caller short-circuits via empty_schema_response, and
                # the dashboard shows "No data available" on a 200.
                # ``keep_snapshot_cache=True`` matches the execute() pattern:
                # preserves the snapshot/path cache so a transient catalog
                # blip doesn't collapse the view to "WHERE false".
                db_iceberg.clear_source_caches(self.src.get("name", "default"), keep_snapshot_cache=True)
                db_iceberg.update_iceberg_view(self.con, self.src, force=True)
                actual_cols = [col["name"] for col in _get_schema(self.con, self.src)]
            except Exception:
                pass
        self.actual_cols = set(actual_cols)
        # Only cache non-empty results. An empty result here means the
        # self-heal path also failed — caching empty would pin the
        # "no schema" answer until the next format_hash change, which
        # is exactly the prod incident the self-heal exists to prevent.
        if actual_cols and cache_key is not None:
            if len(_schema_cols_cache) >= _SCHEMA_COLS_CACHE_MAX_ENTRIES:
                _schema_cols_cache.clear()
            _schema_cols_cache[cache_key] = actual_cols
        return actual_cols

    def execute_with_retry(self, sql: str, params: list | None = None):
        """Execute a query with one stale-view retry.

        Returns the cursor on success, or None on permanent failure after retry.
        Non-stale errors are re-raised immediately.
        """
        try:
            return self.execute(sql, params)
        except Exception as e:
            if not _is_stale_view_error(e):
                raise
        try:
            from backend.core import iceberg as db_iceberg

            db_iceberg.update_iceberg_view(self.con, self.src)
            return self.execute(sql, params)
        except Exception:
            return None

    def create_temp_table(self, sql: str, params: list | None = None) -> bool:
        """Execute a CREATE TEMP TABLE statement with one stale-view retry.

        A background commit job may delete a buffer file between the time our
        connection was opened and when we run the query. When that happens,
        re-initialising the Iceberg view and retrying once is sufficient to
        pick up the new snapshot.

        Returns True on success, False on permanent failure after retry.
        Non-stale errors are re-raised immediately.
        """
        try:
            self.execute(sql, params)
            return True
        except Exception as e:
            if not _is_stale_view_error(e):
                raise
        # Stale view — refresh and retry once.
        try:
            from backend.core import iceberg as db_iceberg

            db_iceberg.update_iceberg_view(self.con, self.src)
            self.execute(sql, params)
            return True
        except Exception:
            return False

    def telemetry(self) -> dict:
        """Return the standard debug telemetry dict for response construction."""
        return {"debug_queries": self.debug_queries, "debug_calls": self.debug_calls}

    def create_filtered_temp_table(
        self,
        cols: list[str],
        actual_cols: list[str] | set[str],
        source_table: str,
        where_clause: str,
        params: list | None = None,
    ) -> str | None:
        """Build and execute a CREATE TEMP TABLE from a filtered column list.

        Returns the temp table name on success, None on failure.
        """
        import uuid as _uuid

        # Escape internal double quotes so a hostile column name cannot break
        # out of the quoted identifier (audit finding 004).
        select_cols = ['"{}"'.format(c.replace('"', '""')) for c in cols if c in actual_cols]
        if not select_cols:
            return None
        temp_name = f"t_{_uuid.uuid4().hex}"
        sql = (
            f"CREATE TEMP TABLE {temp_name} AS SELECT {', '.join(select_cols)} FROM {source_table} WHERE {where_clause}"
        )
        if not self.create_temp_table(sql, params):
            return None
        return temp_name

    def _create_active_hour_temp_direct(
        self,
        fields: list[str],
        actual_cols: list[str] | set[str],
        live_start: Any,
        live_end: Any,
    ) -> str | None:
        """Build the active-hour temp by reading buffer + active hourly hive
        partition parquets directly, bypassing the bound iceberg view.

        Why: profiling on 2026-06-08 showed `live_active_hour` inside
        execute_top_n_rollups taking ~700ms per request — almost entirely
        view-traversal overhead, not data read. The active hour's rows
        live in ~4 buffer parquets (87 rows) and at most a handful of
        ``data/timestamp_hour=<active>/*.parquet`` files post-commit.
        Reading those directly takes ~6ms vs ~700ms via the view.

        Returns the temp table name on success, ``None`` if there's
        nothing to read (caller should skip the live merge) or if the
        direct read fails (caller should fall back to the view-based
        ``create_filtered_temp_table`` path for correctness).
        """
        import os
        import uuid as _uuid

        from backend.core.duckdb import _cache_dir

        try:
            cache_dir = _cache_dir(self.src)
        except Exception:
            return None

        buffer_dir = os.path.join(cache_dir, "buffer")
        active_hour_token = live_start.strftime("%Y-%m-%d-%H")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour_token}")

        # Probe for any parquet files in either location. listdir is faster
        # than glob.glob and bounded — buffer ~4 files, hourly ~1-30.
        def _has_parquets(d: str) -> bool:
            try:
                for f in os.listdir(d):
                    if f.endswith(".parquet") and not f.startswith(".tmp_"):
                        return True
            except OSError:
                pass
            return False

        buffer_exists = _has_parquets(buffer_dir)
        hourly_exists = _has_parquets(hourly_dir)
        if not buffer_exists and not hourly_exists:
            # Nothing on disk for the active hour. Caller will report
            # empty live_res — semantically correct (no current-hour rows).
            return None

        # Project timestamp + every requested field that actually exists
        # in the schema. Keeping the projection narrow lets DuckDB skip
        # parquet column blocks we don't need.
        select_parts = ['"timestamp"']
        seen: set[str] = {"timestamp"}
        for f in fields:
            if f in actual_cols and f not in seen:
                # Escape internal double quotes (audit finding 004).
                select_parts.append('"{}"'.format(f.replace('"', '""')))
                seen.add(f)
        cols_sql = ", ".join(select_parts)
        where = (
            f"timestamp >= TIMESTAMPTZ '{live_start.isoformat()}' AND timestamp < TIMESTAMPTZ '{live_end.isoformat()}'"
        )

        branches: list[str] = []
        if buffer_exists:
            buffer_glob = os.path.join(buffer_dir, "*.parquet").replace("'", "''")
            branches.append(f"SELECT {cols_sql} FROM read_parquet('{buffer_glob}', union_by_name=true) WHERE {where}")
        if hourly_exists:
            hourly_glob = os.path.join(hourly_dir, "*.parquet").replace("'", "''")
            branches.append(f"SELECT {cols_sql} FROM read_parquet('{hourly_glob}', union_by_name=true) WHERE {where}")

        temp_name = f"t_active_direct_{_uuid.uuid4().hex}"
        sql = f"CREATE TEMP TABLE {temp_name} AS " + " UNION ALL ".join(branches)
        try:
            self.con.execute(sql)
        except Exception:
            # Schema mismatch, missing column, etc. Caller falls back.
            try:
                self.con.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
            except Exception:
                pass
            return None
        return temp_name

    @contextlib.contextmanager
    def temp_table(
        self,
        cols: list[str],
        actual_cols: list[str] | set[str],
        source_table: str,
        where_clause: str,
        params: list | None = None,
    ):
        """Yield a filtered TEMP TABLE name (or None on creation failure), dropping it on exit.

        Compared to the bare ``create_filtered_temp_table`` + manual DROP pattern, the
        context manager guarantees the DROP runs even if an intermediate query raises.
        """
        name = self.create_filtered_temp_table(cols, actual_cols, source_table, where_clause, params)
        try:
            yield name
        finally:
            if name is not None:
                try:
                    self.execute(f'DROP TABLE IF EXISTS "{name}"')
                except Exception:
                    pass

    def execute_top_n_rollups(
        self,
        fields: list[str],
        start_time: str | None,
        end_time: str | None,
        limit: int = 10,
        per_field_limits: dict[str, int] | None = None,
        _phase_log: list[dict] | None = None,
    ) -> tuple[list[tuple[str, Any, int]], list[str]]:
        """Compute per-field top-N from rollup parquets + the live active
        hour from the base table. Returns merged (field, value, count)
        tuples truncated to ``per_field_limits.get(field, limit)`` per field.

        per_field_limits lets a caller request a wider top-N for specific
        fields without bloating the others — e.g. ``{"country": 500}`` to
        get up to 500 countries for a choropleth while keeping other
        panels at the default top-10. Internally the live-active-hour
        branch fetches max(all_limits) rows so the merge has enough data
        to satisfy the widest field's truncation.

        Freshness contract: the rollup file enumeration explicitly skips
        any hour >= the current UTC hour (the active hour is still
        receiving writes and cannot be rolled up safely). To avoid
        under-counting the most recent traffic, a separate
        ``execute_top_n_batch`` query runs against the live base table
        clamped to ``[active_hour_start, active_hour_end) ∩ [start, end]``
        and the result is merged into the rollup output before
        truncation. So the returned top-N IS current — the rollup file
        exclusion is implementation, not staleness.
        """
        import os
        from datetime import UTC, datetime, timedelta

        from backend.core.duckdb import _cache_dir
        from backend.core.rollups import _is_safe_ident, _safe_table_for
        from backend.utils.date_utils import parse_iso_utc

        # Optional phase-log instrumentation. Caller passes a list; we
        # append {"section": "top_n_rollups:<phase>", "time_ms": N} per
        # phase. None = no-op. Negligible overhead.
        def _phase(name: str, ms: float) -> None:
            if _phase_log is not None:
                _phase_log.append({"section": f"top_n_rollups:{name}", "time_ms": round(ms, 2)})

        cache_dir = _cache_dir(self.src)
        rollup_dir = os.path.join(cache_dir, "rollups", "hour")
        if not os.path.exists(rollup_dir):
            return [], fields

        # Defense-in-depth: field names land in a SQL IN-list as quoted
        # literals AND the service name lands in the base-table identifier.
        # Both should already be safe (FIELDS + validate_custom_field
        # constrain custom names; service IDs are Fastly-format slugs), but
        # we re-validate here so a future caller can't pierce the boundary.
        safe_fields = [f for f in fields if _is_safe_ident(f)]
        if not safe_fields:
            return [], fields
        base_table = _safe_table_for(self.src)
        if not base_table:
            # Service name failed the identifier safelist; refuse to query.
            return [], fields

        # Parse bounds
        st_dt = parse_iso_utc(start_time) if start_time else None
        et_dt = parse_iso_utc(end_time) if end_time else None

        hour_cond = ""
        if st_dt:
            st_str = st_dt.strftime("%Y-%m-%d-%H")
            hour_cond += f" AND hour >= '{st_str}'"
        if et_dt:
            # Half-open semantics: a request ending exactly on an hour
            # boundary (e.g. ``end_time=2026-06-04T15:00:00``) should
            # EXCLUDE the 15:00 hour rollup (which covers [15:00, 16:00)).
            # Subtracting 1 microsecond before strftime keeps mid-hour
            # ends inclusive of the surrounding hour while making exact
            # boundaries exclusive — matching how the live-hour query
            # below uses ``timestamp < et_dt``.
            et_inclusive = (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H")
            hour_cond += f" AND hour <= '{et_inclusive}'"

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        active_dt_end = active_dt + timedelta(hours=1)
        active_str = active_dt.strftime("%Y-%m-%d-%H")

        # Glob `rollups/hour/**/*.parquet` was the obvious shape but it has
        # DuckDB enumerate every file under the tree before the WHERE clause
        # can prune ANYTHING. On a service with N fields × H hours of rollups
        # that's N*H file stats up front, dominating wall time (witnessed
        # 2026-06-04: ~2.8s on 18,648 files for a 24h query that should be
        # reading ~1,700). Hive-partition pruning kicks in AFTER the glob
        # expands, not before.
        #
        # Instead: enumerate the exact (field, hour) combinations we want in
        # Python (cheap directory listdir per field, bounded by safe_fields ×
        # hours-in-window), then pass DuckDB an explicit file list. Skips
        # the glob, hands DuckDB only the files it needs.
        st_str_floor = st_dt.strftime("%Y-%m-%d-%H") if st_dt else None
        # End cutoff for the directory-list filter — `et_inclusive` was
        # already computed above for the SQL fallback path. Use the same
        # bounds here so the half-open semantics match.
        if et_dt:
            et_str_floor = (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H")
        else:
            et_str_floor = None

        # Per-day compacted root (item 17). When a per-day parquet
        # exists for a closed day, prefer it over the 24 per-hour parquet
        # files for that day — same data, ~24x fewer file opens. Active
        # day stays on per-hour because compaction can't run on a day
        # that's still receiving writes.
        #
        # Per-day and per-hour files MUST be enumerated into separate
        # lists and read via two ``read_parquet([...], hive_partitioning=1)``
        # calls UNION ALL'd. They live under different hive partition
        # keys (``day=YYYY-MM-DD`` vs ``hour=YYYY-MM-DD-HH``); mixing
        # them in one read_parquet call raises ``Binder Error: Hive
        # partition mismatch ... key "day" not found`` and the whole
        # top-N read returns empty. That's the 2026-06-06 prod
        # incident — after the first successful day-compaction
        # the dashboard top-N tabs went blank.
        day_root = os.path.join(cache_dir, "rollups", "day")
        bundled_hour_root = os.path.join(cache_dir, "rollups", "hour_bundled")
        bundled_day_root = os.path.join(cache_dir, "rollups", "day_bundled")
        active_day = active_str[:10]
        day_paths: list[str] = []
        hour_paths: list[str] = []
        # Track which hours are satisfied by a per-hour bundled file so
        # the per-field walk below skips them. Hour bundling collapses
        # ~40 per-field files into one per-hour file, cutting parquet
        # file-opens on a 24h query from ~984 to ~24.
        # Bundled-hour parquets have `field` as a regular column (the
        # PER-FIELD per-hour parquets have it in the hive path), so they
        # need a separate read_parquet branch to avoid schema-mismatch
        # errors when UNION ALL'd with the per-field branch.
        bundled_hour_paths: list[str] = []
        bundled_hours: set[str] = set()
        # Per-day bundled files: one parquet per closed day containing
        # all fields' top-N. When present, replaces ~40 per-field-day
        # files (or 24 per-field-hour files) for that day. Built by the
        # daily rollup_compact_daily cron via backend.core.rollups.
        # bundle_days(); reader prefers it over per-field-day files.
        # Same schema as bundled_hour (field/value/count as columns,
        # no hive partitioning on the projection).
        bundled_day_paths: list[str] = []
        bundled_days_set: set[str] = set()

        # Per-day rollups cover [day 00:00 UTC, +24h). When the request
        # window starts or ends mid-day, including the boundary day's
        # per-day file would over-count rows outside the user's window
        # (e.g. a request starting at 17:36 would pull in counts from
        # 00:00-17:36 too). Only use a per-day file when its entire
        # 24h is contained in the request window; boundary days fall
        # back to per-hour rollups for their in-window hours.
        def _day_fully_in_window(day_str: str) -> bool:
            try:
                day_start = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=UTC)
            except ValueError:
                return False
            day_end = day_start + timedelta(days=1)
            if st_dt and st_dt > day_start:
                return False
            if et_dt and et_dt < day_end:
                return False
            return True

        # Pre-pass: collect days where AT LEAST ONE safe field has a
        # usable per-day file (in-window, fully-contained, closed,
        # parquet present). The bundled-hour walk below skips bundled
        # files whose day is in this set, preventing the day-vs-bundled
        # double count that fires on hour-aligned closed-day-only
        # windows. Per-field per-hour fallback (for fields without a
        # day file for that day) still works because the per-field walk
        # uses its OWN per-field covered_days set, not this global one.
        day_covered_by_any_field: set[str] = set()
        for field in safe_fields:
            field_day_dir = os.path.join(day_root, f"field={field}")
            if not os.path.isdir(field_day_dir):
                continue
            day_entries = _cached_listdir(field_day_dir)
            for day_entry in day_entries:
                if not day_entry.startswith("day="):
                    continue
                day = day_entry[len("day=") :]
                if len(day) != 10 or day in day_covered_by_any_field:
                    continue
                if day >= active_day:
                    continue
                if st_str_floor and day < st_str_floor[:10]:
                    continue
                if et_str_floor and day > et_str_floor[:10]:
                    continue
                if not _day_fully_in_window(day):
                    continue
                day_dir = os.path.join(field_day_dir, day_entry)
                try:
                    if any(f.endswith(".parquet") and not f.startswith(".tmp_") for f in os.listdir(day_dir)):
                        day_covered_by_any_field.add(day)
                except OSError:
                    continue

        # Bundled-day walk (preferred over per-field-day for windows
        # where the whole day fits). When present, replaces ~40 per-
        # field-day file opens with 1. Active day skipped — bundling
        # only runs for closed days. Days NOT fully contained in the
        # window fall through to per-field-hour for the in-window
        # portion (same fall-through as per-field-day).
        if os.path.isdir(bundled_day_root):
            for day_entry in _cached_listdir(bundled_day_root):
                if not day_entry.startswith("day="):
                    continue
                day = day_entry[len("day=") :]
                if len(day) != 10:
                    continue
                if day >= active_day:
                    continue
                if st_str_floor and day < st_str_floor[:10]:
                    continue
                if et_str_floor and day > et_str_floor[:10]:
                    continue
                if not _day_fully_in_window(day):
                    continue
                bundle_path = os.path.join(bundled_day_root, day_entry, "all_fields.parquet")
                if os.path.isfile(bundle_path):
                    bundled_day_paths.append(bundle_path)
                    bundled_days_set.add(day)

        if os.path.isdir(bundled_hour_root):
            for hour_entry in _cached_listdir(bundled_hour_root):
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                if st_str_floor and hour < st_str_floor:
                    continue
                if et_str_floor and hour > et_str_floor:
                    continue
                if hour >= active_str:
                    # Active hour served live, not from any bundle.
                    continue
                if hour[:10] in bundled_days_set:
                    # Day bundle covers this hour (and every field for
                    # this day). Including the hour bundle would
                    # double-count via UNION ALL.
                    continue
                if hour[:10] in day_covered_by_any_field:
                    # Day file covers this hour for at least one
                    # field; including the bundled file would
                    # double-count that field via the UNION ALL.
                    # Fields without a day file for this day fall
                    # through to per-field per-hour in the loop
                    # below (their covered_days won't include this
                    # day).
                    continue
                bundle_path = os.path.join(bundled_hour_root, hour_entry, "all_fields.parquet")
                if os.path.isfile(bundle_path):
                    bundled_hour_paths.append(bundle_path)
                    bundled_hours.add(hour)

        _t_dir_enum = time.perf_counter()
        for field in safe_fields:
            field_hour_dir = os.path.join(rollup_dir, f"field={field}")
            field_day_dir = os.path.join(day_root, f"field={field}")
            if not os.path.isdir(field_hour_dir):
                continue
            # Track which (field, day) tuples we satisfied from the
            # per-day compacted file; the per-hour walk below skips
            # those hours.
            # Track which days are covered for this field. Seeded by
            # `bundled_days_set` so the per-day-bundle suppresses both
            # the per-field-day file AND the per-field-hour fallback
            # for that day (the bundle's one row per (field, value)
            # already aggregates the field's whole day).
            covered_days: set[str] = set(bundled_days_set)
            if os.path.isdir(field_day_dir):
                day_entries = _cached_listdir(field_day_dir)
                for day_entry in day_entries:
                    if not day_entry.startswith("day="):
                        continue
                    day = day_entry[len("day=") :]
                    if len(day) != 10:
                        continue
                    if day in bundled_days_set:
                        # Already served by the bundled-day file.
                        continue
                    if day >= active_day:
                        # Active day is still being written — read per-hour.
                        continue
                    if st_str_floor and day < st_str_floor[:10]:
                        continue
                    if et_str_floor and day > et_str_floor[:10]:
                        continue
                    if not _day_fully_in_window(day):
                        # Boundary day — using the per-day file would over-
                        # count rows outside the requested window. Fall
                        # through to per-hour rollups for the in-window
                        # hours of this day.
                        continue
                    day_dir = os.path.join(field_day_dir, day_entry)
                    for fname in _cached_listdir(day_dir):
                        if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                            day_paths.append(os.path.join(day_dir, fname))
                            covered_days.add(day)
            hour_entries = _cached_listdir(field_hour_dir)
            for hour_entry in hour_entries:
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                # Lexicographic string compare is correct here because the
                # YYYY-MM-DD-HH format is fixed-width.
                if st_str_floor and hour < st_str_floor:
                    continue
                if et_str_floor and hour > et_str_floor:
                    continue
                if hour >= active_str:
                    # Active hour is served live, not from rollups.
                    continue
                if hour[:10] in covered_days:
                    # Per-day file already covers this hour.
                    continue
                if hour in bundled_hours:
                    # Per-hour bundle already covers this (field, hour).
                    continue
                hour_dir = os.path.join(field_hour_dir, hour_entry)
                for fname in _cached_listdir(hour_dir):
                    if fname.endswith(".parquet"):
                        hour_paths.append(os.path.join(hour_dir, fname))

        _phase("dir_enum", (time.perf_counter() - _t_dir_enum) * 1000)
        _phase("dir_enum:n_day_files", float(len(day_paths)))
        _phase("dir_enum:n_hour_files", float(len(hour_paths)))
        _phase("dir_enum:n_bundled_hour_files", float(len(bundled_hour_paths)))
        _phase("dir_enum:n_bundled_day_files", float(len(bundled_day_paths)))

        _t_rolled = time.perf_counter()
        if not day_paths and not hour_paths and not bundled_hour_paths and not bundled_day_paths:
            rolled_res: list = []
        else:
            # Inline each path list as its OWN read_parquet call and
            # UNION ALL the results so SUM(count) aggregates across
            # both sources. ``CAST(count AS BIGINT)`` normalises the
            # type — per-hour files store count as BIGINT but the
            # compaction COPY writes DOUBLE (DuckDB SUM(BIGINT) →
            # DOUBLE in some configurations); UNION ALL requires
            # matching types per column.
            branches = []
            if day_paths:
                paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in day_paths)
                branches.append(
                    f"SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet([{paths_sql}], hive_partitioning=1)"
                )
            if hour_paths:
                paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in hour_paths)
                branches.append(
                    f"SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet([{paths_sql}], hive_partitioning=1)"
                )
            if bundled_hour_paths:
                # Bundled parquets have `field` as a column already (the
                # bundler SELECTs it from the per-field source files).
                # hive_partitioning=0 because the only hive segment here
                # is `hour=...` which we don't need for the projection.
                paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in bundled_hour_paths)
                branches.append(
                    f"SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet([{paths_sql}], hive_partitioning=0)"
                )
            if bundled_day_paths:
                # Same shape as bundled_hour (field/value/count as
                # columns, no hive partitioning on the projection).
                paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in bundled_day_paths)
                branches.append(
                    f"SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet([{paths_sql}], hive_partitioning=0)"
                )
            _max_limit = max([limit] + list((per_field_limits or {}).values()))
            q = (
                "SELECT field, value, SUM(count) AS c FROM ("
                + " UNION ALL ".join(branches)
                + f") GROUP BY field, value QUALIFY ROW_NUMBER() OVER (PARTITION BY field ORDER BY c DESC) <= {_max_limit}"
            )
            try:
                rolled_res = self.execute(q).fetchall()
            except Exception:
                rolled_res = []
        _phase("rolled_res", (time.perf_counter() - _t_rolled) * 1000)

        # We also need to get the live active hour stats from the base table
        _t_live = time.perf_counter()
        live_res: list[tuple] = []
        # Defined here so the partial-day block below can reuse them
        # without re-fetching if the active-hour block populated them.
        actual_cols: list[str] = []
        schema_types: dict[str, str] = {}

        # Clamp the live window to the intersection of (active hour) and
        # (requested window). Without this, a partial-hour request like
        # [14:30, 15:30] where active_dt=15:00 would query the FULL active
        # hour [15:00, 16:00) — over-counting rows from [15:30, 16:00) that
        # fall outside the user's window. Most users hit hour-aligned
        # windows (last 1h, 6h, 24h) so this only matters for custom date
        # ranges that don't snap to hour boundaries, but the over-count is
        # a real correctness gap when it does fire.
        live_start = max(active_dt, st_dt) if st_dt else active_dt
        live_end = min(active_dt_end, et_dt) if et_dt else active_dt_end
        live_where = f"timestamp >= '{live_start.isoformat()}' AND timestamp < '{live_end.isoformat()}'"
        # We only query the active hour if it overlaps with the requested time window
        should_query_live = True
        if et_dt and et_dt <= active_dt:
            should_query_live = False
        if st_dt and st_dt >= active_dt_end:
            should_query_live = False

        if should_query_live:
            # We run a standard execute_top_n_batch query on the base table for just the active hour
            try:
                actual_cols = self.get_schema_cols()
                # _get_schema is module-local (line ~106); the prior code
                # imported it from backend.core.duckdb which does NOT
                # export this symbol — the ImportError silently broke the
                # live merge for an indeterminate time, so the per-field
                # top-N was missing the current hour entirely. Use the
                # module-local function directly.
                schema_types = {col["name"]: col["type"] for col in _get_schema(self.con, self.src)}

                # To prevent creating a massive UNION, we'll create a temp table for just the live hour.
                # Live branch must fetch up to the WIDEST per-field limit so the
                # final per-field truncation has enough data — fetching only
                # `limit` here would under-count any field whose per_field_limit > limit.
                _live_limit = max([limit] + list((per_field_limits or {}).values()))
                # Fast path: read buffer + active hourly partition directly,
                # skipping the iceberg view (~700ms saved per request on the
                # 2026-06-08 baseline). Falls back to the view-based path if
                # the direct read fails (schema mismatch, missing dirs, etc).
                # Use the safe_fields list (validated at line 741) for temp
                # construction so a hostile field name can never reach the
                # SQL builder via this fast path — audit finding 004.
                tmp_name = self._create_active_hour_temp_direct(safe_fields, actual_cols, live_start, live_end)
                if tmp_name is None:
                    tmp_name = self.create_filtered_temp_table(safe_fields, actual_cols, base_table, live_where)
                if tmp_name:
                    try:
                        # Filter to columns present in the temp's projection.
                        # Virtual fields (waf_sig_ind, edge_score_reason_ind)
                        # have rollup parquets but no live column — including
                        # them here would build SQL referencing a missing
                        # column and BinderException out the entire UNION
                        # ALL, silently dropping the live-hour merge for
                        # the real fields too.
                        live_fields = [f for f in safe_fields if f in actual_cols]
                        if live_fields:
                            live_res, _ = self.execute_top_n_batch(
                                live_fields, tmp_name, actual_cols, schema_types, limit=_live_limit
                            )
                    finally:
                        try:
                            self.execute(f'DROP TABLE IF EXISTS "{tmp_name}"')
                        except Exception:
                            pass
            except Exception:
                pass
        _phase("live_active_hour", (time.perf_counter() - _t_live) * 1000)

        # Combine rolled and live, bucketed by field. The prior
        # implementation kept a flat (field, value) keyed dict and then
        # re-scanned the whole dict per field at sort time, making the
        # merge O(N × F) — at ~50k combined rows × 12 fields = 600k
        # filter iterations, this Python work was ~880ms (the single
        # biggest phase inside top_n_rollups, larger than the SQL
        # read itself). Bucketing by field once is O(N) and brings
        # the merge down to <50ms.
        _t_merge = time.perf_counter()
        by_field: dict[str, dict[Any, int]] = {}
        for field, value, count in rolled_res:
            bucket = by_field.setdefault(field, {})
            bucket[value] = bucket.get(value, 0) + count
        for field, value, count in live_res:
            bucket = by_field.setdefault(field, {})
            bucket[value] = bucket.get(value, 0) + count

        # Sort and limit. Per-field limits override the global default for
        # specific fields (e.g. country at 500 for choropleth).
        top_results = []
        _pfl = per_field_limits or {}
        for field in fields:
            bucket_opt: dict[Any, int] | None = by_field.get(field)
            if not bucket_opt:
                continue
            bucket = bucket_opt
            _field_limit = _pfl.get(field, limit)
            # Use heapq.nlargest when truncating to a small slice of a
            # large bucket — avoids the full O(N log N) sort for the
            # common case (10-of-thousands).
            items = bucket.items()
            if _field_limit < len(bucket):
                top_items = heapq.nlargest(_field_limit, items, key=lambda x: x[1])
            else:
                top_items = sorted(items, key=lambda x: x[1], reverse=True)
            for val, count in top_items:
                top_results.append((field, val, count))
        _phase("merge_sort", (time.perf_counter() - _t_merge) * 1000)

        return top_results, fields

    # Chart metrics that the 1-minute time-series rollup can serve directly.
    # SQL keys MUST match the ChartMetric Literal in backend/models/dashboard.py.
    # Each expression's numerator/denominator must produce the same value as
    # the equivalent raw expression in CANONICAL_METRICS so rollup-served and
    # raw-served buckets stay consistent across an active-hour split.
    # Percentile / median metrics (p50/p95/p99 latency, throughput, req_size,
    # ttfb median) are excluded — they require sketch-based re-aggregation
    # which DuckDB doesn't ship with — and fall through to the raw scan.
    _TS_ROLLUP_METRIC_SQL: dict[str, str] = {
        "requests": "CAST(SUM(requests) AS BIGINT)",
        "5xx": "ROUND(SUM(status_5xx) * 100.0 / NULLIF(SUM(requests), 0), 2)",
        "4xx": "ROUND(SUM(status_4xx) * 100.0 / NULLIF(SUM(requests), 0), 2)",
        "hit_rate": "ROUND(SUM(hits) * 100.0 / NULLIF(SUM(requests), 0), 2)",
    }

    # Intervals the reader will re-aggregate up to from the 1-minute rollup.
    # "1 second" is excluded because the rollup is per-minute (no intra-minute
    # resolution to give back). Other intervals fall through to raw.
    _TS_ROLLUP_INTERVALS: frozenset[str] = frozenset({"1 minute", "1 hour", "1 day"})

    def try_time_series_from_rollup(
        self,
        chart_metric: str,
        interval: str,
        start_time: str | None,
        end_time: str | None,
        table_name: str,
        where_clause: str,
        params: list,
    ) -> list[dict] | None:
        """Serve the dashboard time_series chart from per-hour rollup parquets
        when eligible, falling back transparently to ``None`` otherwise (the
        caller then runs its existing raw query).

        Eligibility:
          * ``chart_metric`` in :attr:`_TS_ROLLUP_METRIC_SQL`.
          * ``interval`` in :attr:`_TS_ROLLUP_INTERVALS`.
          * Both ``start_time`` and ``end_time`` parse as ISO-8601 UTC.
          * Every closed hour in the requested window has a
            ``time_series.parquet`` on disk (a single missing closed hour
            disqualifies the whole window — falling back is safer than
            rendering an undercount).

        Active-hour handling: hours at or after the current UTC hour aren't
        rolled up (the bundler skips them — see
        :func:`backend.core.rollups.build_time_series_bundles`). If the
        window includes the active hour we run the live SQL for that hour
        only and UNION ALL it with the rollup-served portion, so the chart
        is always current to the second.

        Returns the same shape as the inline raw block in
        ``dashboard.py:get_aggregates``:
        ``[{"time": iso_string, "value": float}, ...]``, ordered by bucket.
        ``None`` means "not eligible — caller should run its raw query".
        """
        import os
        from datetime import UTC, datetime, timedelta

        from backend.core.rollups import TIME_SERIES_BUNDLE_FILENAME, _hour_bundled_root
        from backend.utils.date_utils import parse_iso_utc

        if chart_metric not in self._TS_ROLLUP_METRIC_SQL:
            return None
        if interval not in self._TS_ROLLUP_INTERVALS:
            return None
        if not start_time or not end_time:
            return None
        # parse_iso_utc is the project-standard helper — it always returns
        # tz-aware UTC, which is what the bundle directory names and the
        # active_hour_str comparison below both assume. Using raw
        # datetime.fromisoformat here is what caused the 2026-06-11 missing-
        # tail bug: cursor kept the request's input tz (CDT for a FE in
        # Central) and looked up bundles by CDT-named hours, missing the
        # last 5 hours of a 24h window.
        st = parse_iso_utc(start_time)
        et = parse_iso_utc(end_time)
        if st is None or et is None:
            return None
        if et <= st:
            return None

        if (et - st) > timedelta(days=366):
            return None

        bundled_root = _hour_bundled_root(self.src)
        if not os.path.isdir(bundled_root):
            return None

        # st is UTC (parse_iso_utc guarantees it). collect_hourly_bundle_paths
        # returns None when a closed hour has per-field rollup data but no
        # bundle on disk — that's the writer-behind case where serving the
        # rollup path would undercount, so we fall back to raw.
        active_hour_str = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        active_hour_dt = datetime.strptime(active_hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
        collected = collect_hourly_bundle_paths(self.src, st, et, bundled_root, TIME_SERIES_BUNDLE_FILENAME)
        if collected is None:
            return None
        rollup_paths, crosses_active = collected

        if not rollup_paths and not crosses_active:
            # Window is in the past but no rollup files exist for it (the
            # backfill hasn't been run, or every hour predates retention).
            return None

        metric_sql = self._TS_ROLLUP_METRIC_SQL[chart_metric]
        # Bucket is TIMESTAMPTZ in the bundle parquets (older notes about
        # "naive TIMESTAMP" referred to a since-removed schema). Use
        # TIMESTAMPTZ literals so the comparison is unambiguous regardless
        # of DuckDB's session timezone — without the explicit offset, a
        # session tz like CDT silently shifts the filter by 5 hours and
        # drops bundles at the window's edges.
        st_tz = st.astimezone(UTC).isoformat()
        et_tz = et.astimezone(UTC).isoformat()

        select_clauses: list[str] = []
        if rollup_paths:
            paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in rollup_paths)
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', bucket) AS out_bucket, "
                f"       {metric_sql} AS value "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE bucket >= TIMESTAMPTZ '{st_tz}' "
                f"  AND bucket < TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY 1"
            )

        if crosses_active:
            # Live SQL for the [max(st, active_hour_start), et) slice. Read
            # from the per-request table (TEMP table or base view) using
            # the same metric-derivation logic as the rollup branch so the
            # buckets align exactly. The where_clause already encodes any
            # filter — we further constrain by the live-slice timestamps.
            live_start = max(st, active_hour_dt)
            live_end = et
            live_st_tz = live_start.astimezone(UTC).isoformat()
            live_et_tz = live_end.astimezone(UTC).isoformat()

            metric_for_live = _live_metric_sql_from_raw(chart_metric)
            if metric_for_live is None:
                # Can't reconstruct the live aggregation for this metric.
                # Better to fall back fully than show a chart missing the
                # most-recent buckets.
                return None
            live_clause = (
                f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS out_bucket, "
                f"       {metric_for_live} AS value "
                f"FROM {table_name} "
                f"WHERE {where_clause} "
                f"  AND timestamp >= TIMESTAMPTZ '{live_st_tz}' "
                f"  AND timestamp <  TIMESTAMPTZ '{live_et_tz}' "
                f"GROUP BY 1"
            )
            select_clauses.append(live_clause)

        if not select_clauses:
            return []

        # UNION ALL: the rollup and live windows don't overlap by
        # construction (cursor stops at active_hour_str), so SUM-style
        # metrics don't need an outer aggregation. Just sort.
        unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
        final_sql = f"SELECT out_bucket, value FROM ({unioned}) WHERE out_bucket IS NOT NULL ORDER BY 1"

        try:
            rows = self.execute(final_sql, params if crosses_active else []).fetchall()
        except duckdb.Error as e:
            # Any read failure (stale view, missing column, schema drift
            # in older bundles, …) drops us to the raw path. Logged at
            # debug — the caller will produce a working result anyway.
            import logging as _logging

            _logging.getLogger(__name__).debug("[time_series_rollup] read failed, falling back to raw: %s", e)
            return None

        out: list[dict] = []
        for r in rows:
            if r[0] is None:
                continue
            out.append(
                {
                    "time": safe_iso(r[0]),
                    "value": float(r[1]) if r[1] is not None else 0.0,
                }
            )
        return out

    def execute_top_n_batch(
        self, fields: list[str], table_name: str, actual_cols: list[str], schema_types: dict[str, str], limit: int = 10
    ) -> tuple[list[tuple], list[str]]:
        """
        Generates and executes a single optimized UNION ALL query for multiple Top-N fields.
        Returns (fetchall_results, field_order).
        """
        from backend.core.rollups import _is_safe_ident
        from backend.repositories.utils.filters import resolve_col

        top_queries = []
        field_order = []

        # Fields whose underlying values are stored as FLOAT but represent
        # integer seconds (Fastly's obj.ttl / obj.age). Floating-point jitter
        # at ingest produces near-duplicate keys (3600.027, 3600.028, …) that
        # split GROUP BY into many tiny buckets. Round to integer to collapse
        # them — also drops the "3600.027" display in TopTenTable.
        INT_AGGREGATE_FIELDS = {"ttl", "age"}

        for field in fields:
            if not _is_safe_ident(field):
                continue
            sql_col = resolve_col(field, actual_cols)
            col_type = schema_types.get(sql_col, "VARCHAR")

            if col_type == "VARCHAR":
                where_filter = f"{sql_col} IS NOT NULL AND {sql_col} != ''"
                select_val = sql_col
            elif field in INT_AGGREGATE_FIELDS:
                where_filter = f"{sql_col} IS NOT NULL"
                select_val = f"CAST(CAST(ROUND({sql_col}) AS INTEGER) AS VARCHAR)"
            else:
                where_filter = f"{sql_col} IS NOT NULL"
                select_val = f"CAST({sql_col} AS VARCHAR)"

            field_order.append(field)
            top_queries.append(f"""
                (SELECT '{field}' as field, {select_val} as value, count(*) as c
                FROM {table_name}
                WHERE {where_filter}
                GROUP BY 1, 2 ORDER BY 3 DESC LIMIT {limit})
            """)

        if not top_queries:
            return [], []

        union_q = " UNION ALL ".join(top_queries)
        return self.execute(union_q).fetchall(), field_order
