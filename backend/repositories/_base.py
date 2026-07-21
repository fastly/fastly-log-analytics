"""Shared helpers used by every repository module.

Import from here instead of redefining in each file:

    from backend.repositories._base import _safe_table, _get_schema, safe_iso, QueryRunner
"""

from __future__ import annotations

import contextlib
import heapq
import logging
import re
import time
from typing import TYPE_CHECKING, Any

import duckdb

from backend.core.rollups._common import quote_path_list

if TYPE_CHECKING:
    from datetime import datetime

_logger = logging.getLogger(__name__)

# Rate-limit table for empty-rollup warnings — (service_id, field) → monotonic
# timestamp of last warning. Bounded growth by capping; reset on the next
# successful emission. 5-minute throttle is enough to surface the
# silent-degraded-panel scenario in operator logs without spamming on every
# dashboard refresh for a service that legitimately has no data for a field.
_EMPTY_ROLLUP_WARN_TS: dict[tuple[str, str], float] = {}
_EMPTY_ROLLUP_WARN_INTERVAL_S = 300.0

# Missing-hour live heal (execute_top_n_rollups): closed hours of NOT-yet-
# day-compacted days that have no hour bundle (and no per-field hour files)
# are live-queried so top-N stays correct through rollup-writer gaps. The
# cap bounds the worst-case live scan when the writer has been down for a
# long stretch — beyond it the oldest gap hours are dropped (the counts
# degrade exactly as they did before the fallback existed) and the warning
# below tells the operator to run the deep backfill instead.
_MISSING_HOUR_HEAL_CAP = 48
_MISSING_HOUR_HEAL_WARN_TS: dict[str, float] = {}

# Fields excluded from the LIVE active-hour top-up in execute_top_n_rollups.
# The live merge tops up each field's rollup top-N with the current (not-yet-
# rolled-up) hour, computed at query time. That cost is dominated by per-field
# GROUP-BYs over near-unique columns — on a busy hour `rid`/`waf_req_id` alone
# build 300k+-entry hash tables. None of the fields below are rendered as a
# dashboard top-N panel (they're per-request identifiers, raw measurements, the
# time axis, or the raw column backing a virtual card), so skipping their live
# top-up is invisible to the UI: the ROLLUP path still returns them through the
# last closed hour, only the current partial-hour merge is skipped.
#
# IMPORTANT: this is a deny-list of NON-rendered fields only. Every field the
# dashboard renders — including custom `edge_score*` / `edge_sid` panels — MUST
# stay out of this set so its panel keeps current-hour freshness.
_LIVE_TOPN_SKIP_FIELDS: frozenset[str] = frozenset(
    {
        "waf_req_id",  # per-request NGWAF id (near-unique; joined elsewhere, never a facet)
        "rid",  # request id (near-unique)
        "prid",  # parent request id (near-unique)
        "_source_file",  # internal FOS provenance column
        "timestamp",  # the time axis, not a top-N facet
        "elapsed",  # raw per-request latency (ms); shown via chart metrics, not a facet
        "ttfb",  # raw per-request time-to-first-byte
        "req_bytes",  # raw per-request byte counts (served via the chart-metric temp)
        "req_header_bytes",
        "resp_bytes",
        "lat",  # raw coordinates — geo facets are city/region/country/metro
        "lon",
        "waf_sig",  # raw col backing the virtual waf_sig_ind card (served via _exploded_top_n)
    }
)


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
    from backend.core import metadata as metadata_db

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
    """Return True when the error indicates an Iceberg view that references
    a deleted buffer file.

    Finding 005 (2026-06-15): the prior implementation matched purely on
    substrings of ``str(e)``. The DuckDB error tunnel ``ConversionException``
    embeds the offending input value into its message, so a user-supplied
    filter value containing one of the canonical stale-view phrases (e.g.
    ``"No files found"``) would spoof a stale-view detection and trigger
    the expensive synchronous view rebuild + Iceberg catalog refresh.
    Hammered, that's a credentialed-DoS vector against the per-service
    iceberg lock.

    The detection now also requires the exception to be a genuine
    ``IOException`` / ``CatalogException`` from DuckDB — the two classes
    actually raised for "the underlying parquet vanished" and "the view
    references a missing table". Substring matching is kept as a
    secondary gate so we only retry for the specific stale-view shapes
    we know about, not every IO blip (e.g. a 500 from FOS is also an
    IOException but the right response is to surface it, not silently
    rebuild).
    """
    msg = str(e)
    looks_stale = (
        "No files found" in msg
        or "Catalog Error: Table with name" in msg
        or "does not exist" in msg
        or "No such file or directory" in msg
    )
    if not looks_stale:
        return False
    try:
        import duckdb as _duckdb_mod
    except Exception:
        # Falling back to the substring-only behaviour if duckdb is somehow
        # not importable preserves the prior contract — no functional
        # regression, only the tightened class check is skipped.
        return True
    return isinstance(e, (_duckdb_mod.IOException, _duckdb_mod.CatalogException))


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
    "throughput": "ROUND(COALESCE(MEDIAN(CASE WHEN starts_with({cache_col}, 'HIT') AND {elapsed_col} > 0 THEN {resp_bytes_col} * 1e6 / NULLIF(CAST({elapsed_col} AS DOUBLE), 0) ELSE NULL END), 0), 2)",
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


def percentile_ms_expr(col: str, p: float = 0.95, filter_expr: str = "", *, approx: bool = False) -> str:
    """Return PERCENTILE_CONT expression that converts microseconds → milliseconds.

    Pass ``approx=True`` to use DuckDB's ``approx_quantile`` (T-Digest sketch)
    instead of the exact sort-based ``PERCENTILE_CONT``. The sketch is a
    streaming O(N) pass with ~1 % typical error on the tail — fine for
    top-N "directional" tables (Slowest URLs / ASNs) where the page
    presents columns as comparative, not as SLA reports. Three exact
    sorted-percentile calls on the same temp rows is the dominant cost
    on `/api/performance/aggregates`; switching to approx collapses
    them to a single streaming sketch per call.
    """
    filter_clause = f" FILTER ({filter_expr})" if filter_expr else ""
    if approx:
        # approx_quantile accepts FILTER the same way as other aggregates.
        return f"approx_quantile({col}, {p}){filter_clause} / 1000.0"
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


def _latest_log_in_window(
    latest_log_at: str | None,
    start_time: str | None,
    end_time: str | None,
) -> bool:
    """Return True when the all-time latest log falls inside ``[start, end]``.

    ``start_time`` / ``end_time`` are open bounds when ``None`` (the
    default-range dashboard request passes neither). The comparison is a
    lexicographic ISO-8601 string compare — both ``latest_log_at`` (from
    the status cache via ``get_source_extent``) and the window bounds are
    UTC ISO strings, which sort chronologically. A normalisation pass via
    ``parse_iso_utc`` guards against trailing-Z vs +00:00 / fractional-
    second differences that would break a naive string compare.

    Returns False when ``latest_log_at`` is None (no data at all) — the
    self-heal must NOT fire in that case.
    """
    if not latest_log_at:
        return False
    from backend.utils.date_utils import parse_iso_utc

    try:
        latest_dt = parse_iso_utc(latest_log_at)
    except Exception:
        return False
    if latest_dt is None:
        return False
    if start_time:
        try:
            st_dt = parse_iso_utc(start_time)
            if st_dt is not None and latest_dt < st_dt:
                return False
        except Exception:
            pass
    if end_time:
        try:
            et_dt = parse_iso_utc(end_time)
            if et_dt is not None and latest_dt > et_dt:
                return False
        except Exception:
            pass
    return True


def should_self_heal_stale_view(
    *,
    windowed_count: int,
    filters,
    latest_log_at: str | None,
    start_time: str | None,
    end_time: str | None,
) -> bool:
    """Decide whether a zero-row window is a STALE VIEW vs a legitimately
    empty window.

    Tight trigger (cheap, false-positive-resistant): fire ONLY when

      * the windowed result is empty (``windowed_count == 0``), AND
      * no user filters are applied, AND
      * the all-time latest log (from the status cache) is non-null AND
        falls inside the queried ``[start_time, end_time]``.

    Rationale: if the window contains the all-time-latest log but the view
    returns nothing, the view MUST be stale — the latest log should be in
    it. A legitimately low-traffic / gap window will NOT contain the
    all-time latest log, so this returns False and no rebuild fires. This
    keeps the rebuild rare in prod (only when the view is actually broken)
    and reliable for the fresh/single-log case where the status cache
    already shows data but the cached view SQL hasn't been rebuilt yet
    (dev runs no crons, so it never self-corrects otherwise).
    """
    if windowed_count != 0:
        return False
    if filters:
        return False
    return _latest_log_in_window(latest_log_at, start_time, end_time)


def force_rebuild_view(con: duckdb.DuckDBPyConnection, src: dict) -> None:
    """Force ONE synchronous rebuild of the per-service Iceberg view.

    Busts the cached view SQL first (``keep_snapshot_cache=True`` mirrors
    the get_sync_status / QueryRunner self-heal pattern — preserve the
    snapshot/path cache so a transient catalog blip can't collapse the
    view to "WHERE false"), then force-rebuilds under the per-service
    lock. Swallows errors: a failed rebuild leaves the caller on the
    existing (empty) path rather than 500ing.
    """
    try:
        from backend.core import iceberg as db_iceberg

        db_iceberg.clear_source_caches(src.get("name", "default"), keep_snapshot_cache=True)
        db_iceberg.update_iceberg_view(con, src, force=True)
    except Exception:
        _logger.debug("[self-heal] force view rebuild failed", exc_info=True)


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
        # Request-scoped sharing of the direct active-hour temps (see
        # begin_shared_active_hour_temps). None = sharing disabled: every
        # _create_active_hour_temp_direct caller owns + drops its own temp.
        self._shared_active_temps: list[tuple[frozenset[str], Any, Any, str]] | None = None
        # File count of the most recent direct active-hour read — telemetry
        # only (surfaced as live_active_hour:n_files).
        self._last_active_direct_n_files: int = 0

        from backend.core.iceberg import inject_view_debug

        inject_view_debug(self.debug_queries, src)

    def begin_shared_active_hour_temps(self) -> None:
        """Enable request-scoped reuse of the direct active-hour temps.

        The dashboard rollup path reads the active hour up to THREE times
        per request — the top-N live merge (wide projection), the
        conn_requests histogram top-up, and the chart's live slice — and
        each direct read pays a per-file open over the buffer + active
        hourly partition (46 files on prod 2026-07-07). Within this scope
        the first temp whose projected columns cover a later caller's
        needs (same [start, end) window) is reused, and every shared temp
        is dropped together in :meth:`end_shared_active_hour_temps`.

        Callers inside the scope must release via
        :meth:`release_active_direct_temp` instead of a bare DROP.
        """
        self._shared_active_temps = []

    def end_shared_active_hour_temps(self) -> None:
        """Drop every shared active-hour temp and disable sharing.

        Safe to call unconditionally (no-op when sharing was never
        enabled) — callers put it in a ``finally`` so pooled connections
        never accumulate session temps across requests.
        """
        temps, self._shared_active_temps = self._shared_active_temps, None
        for _cols, _ws, _we, name in temps or []:
            try:
                self.execute(f'DROP TABLE IF EXISTS "{name}"')
            except Exception:
                pass

    def release_active_direct_temp(self, name: str) -> None:
        """Drop a direct active-hour temp unless the shared scope owns it.

        Inside :meth:`begin_shared_active_hour_temps` the scope-exit drop
        is authoritative; outside it this is a plain guarded DROP.
        """
        if self._shared_active_temps is not None and any(n == name for _c, _s, _e, n in self._shared_active_temps):
            return
        try:
            self.execute(f'DROP TABLE IF EXISTS "{name}"')
        except Exception:
            pass

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
        from datetime import timedelta as _timedelta

        from backend.core.duckdb import _cache_dir

        try:
            cache_dir = _cache_dir(self.src)
        except Exception:
            return None

        buffer_dir = os.path.join(cache_dir, "buffer")
        active_hour_token = live_start.strftime("%Y-%m-%d-%H")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour_token}")

        # Columns the temp will physically carry: timestamp + every
        # requested field that actually exists in the schema.
        projected: list[str] = ["timestamp"]
        for f in fields:
            if f in actual_cols and f not in projected:
                projected.append(f)
        projected_set = frozenset(projected)

        # Shared-scope reuse (see begin_shared_active_hour_temps): an
        # earlier temp over the SAME window whose projection covers this
        # caller's columns serves as-is — zero file opens.
        if self._shared_active_temps is not None:
            for cols_have, ws, we, name in self._shared_active_temps:
                if ws == live_start and we == live_end and projected_set <= cols_have:
                    self._last_active_direct_n_files = 0
                    return name

        # Enumerate candidate parquets EXPLICITLY (not a glob) so buffer
        # files that cannot contain live-window rows are pruned by mtime:
        # a buffer parquet is write-once, so every row in it is stamped no
        # later than the file's close time — a file finalized before
        # (live_start - skew margin) cannot hold rows >= live_start. On
        # prod (2026-07-07) the buffer held 42 files spanning >1h while
        # the live read needed only the tail; opening all of them via the
        # old directory glob was the dominant cost of this temp (~166ms
        # measured via live_active_hour:temp_create). The margin absorbs
        # edge-vs-VM clock skew; correctness only needs "no file whose
        # rows could reach live_start is dropped".
        mtime_floor = (live_start - _timedelta(seconds=300)).timestamp()

        # TOMBSTONED buffer parquets MUST be excluded: their rows were
        # already committed into the hourly partitions this read also
        # scans, and the file only lingers on disk for the sweep grace
        # window (so views bound BEFORE the commit stay readable — see
        # tombstone_buffer_files). Reading them here double-counted up to
        # ~10 min of the freshest rows in every active-hour live slice —
        # on prod (2026-07-07) 40 of 55 buffer files were tombstoned. The
        # view path already filters via buffer_files(); this closes the
        # same hole for the direct read. A commit landing between this
        # marker scan and the read leaves a sub-second race — down from
        # the ~10-minute exposure.
        try:
            from backend.core.iceberg.buffer import _tombstoned_parquet_paths

            tombstoned = _tombstoned_parquet_paths(buffer_dir)
        except Exception:
            tombstoned = set()

        def _list_parquets(d: str, *, prune_mtime: bool) -> list[str]:
            out: list[str] = []
            try:
                with os.scandir(d) as it:
                    for e in it:
                        if not e.name.endswith(".parquet") or e.name.startswith(".tmp_"):
                            continue
                        if e.path in tombstoned:
                            continue
                        if prune_mtime:
                            try:
                                if e.stat().st_mtime < mtime_floor:
                                    continue
                            except OSError:
                                pass  # can't stat → keep (correctness over speed)
                        out.append(e.path)
            except OSError:
                pass
            return out

        buffer_files = _list_parquets(buffer_dir, prune_mtime=True)
        # Hourly-partition files are already scoped to the active hour —
        # no mtime pruning (compaction may rewrite them with fresh mtimes
        # anyway), and tombstones only ever mark buffer files.
        hourly_files = _list_parquets(hourly_dir, prune_mtime=False)
        self._last_active_direct_n_files = len(buffer_files) + len(hourly_files)
        if not buffer_files and not hourly_files:
            # Nothing on disk for the active hour. Caller will report
            # empty live_res — semantically correct (no current-hour rows).
            return None

        # Escape internal double quotes (audit finding 004).
        cols_sql = ", ".join('"{}"'.format(c.replace('"', '""')) for c in projected)
        where = (
            f"timestamp >= TIMESTAMPTZ '{live_start.isoformat()}' AND timestamp < TIMESTAMPTZ '{live_end.isoformat()}'"
        )

        def _create_sql(union_by_name: str) -> str:
            branches: list[str] = []
            if buffer_files:
                paths_sql = quote_path_list(buffer_files)
                branches.append(
                    f"SELECT {cols_sql} FROM read_parquet([{paths_sql}], union_by_name={union_by_name}) WHERE {where}"
                )
            if hourly_files:
                paths_sql = quote_path_list(hourly_files)
                branches.append(
                    f"SELECT {cols_sql} FROM read_parquet([{paths_sql}], union_by_name={union_by_name}) WHERE {where}"
                )
            return " UNION ALL ".join(branches)

        temp_name = f"t_active_direct_{_uuid.uuid4().hex}"
        # union_by_name=false first: with =true DuckDB reconciles every
        # file's FULL schema by name before projecting, and on the ~90-col
        # log files that bind cost (~6ms/file × 27 files on prod
        # 2026-07-07, regardless of how narrow the projection is) dominated
        # this temp. Plain multi-file reads validate that schemas MATCH and
        # raise on any drift — never a silent positional mis-bind — so the
        # =true retry only pays its reconciliation cost in the rare
        # mid-schema-change buffer window.
        try:
            self.con.execute(f"CREATE TEMP TABLE {temp_name} AS " + _create_sql("false"))
        except Exception:
            try:
                self.con.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
            except Exception:
                pass
            try:
                self.con.execute(f"CREATE TEMP TABLE {temp_name} AS " + _create_sql("true"))
            except Exception:
                # Missing column, unreadable file, etc. Caller falls back.
                try:
                    self.con.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
                except Exception:
                    pass
                return None
        if self._shared_active_temps is not None:
            self._shared_active_temps.append((projected_set, live_start, live_end, temp_name))
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
        actual_cols: list[str] | None = None,
        schema_types: dict[str, str] | None = None,
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

        Robustness contract (missing-hour live heal): closed in-window
        hours of not-yet-day-compacted days that have NO rollup coverage
        (no hour bundle, no per-field hour file) are live-queried the same
        way and merged in, bounded by ``_MISSING_HOUR_HEAL_CAP`` hours.
        Zero-cost in steady state (the hourly ``rollup_heal`` cron keeps
        the bundle tree complete); it only pays while the writer is
        behind, so a rollup-writer gap degrades to a slower-but-correct
        read instead of silently under-counted panels.
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
        # Every day token with ANY day-rollup presence on disk, ignoring the
        # window checks below. Consumed by the missing-hour live heal: a day
        # that has been day-compacted had its per-hour files deleted BY
        # DESIGN, so its boundary hours falling outside a fully-contained
        # window are a known, bounded read gap — NOT a writer failure — and
        # must not trigger a per-request live scan.
        days_with_day_files_any: set[str] = set()
        for field in safe_fields:
            field_day_dir = os.path.join(day_root, f"field={field}")
            if not os.path.isdir(field_day_dir):
                continue
            day_entries = _cached_listdir(field_day_dir)
            for day_entry in day_entries:
                if not day_entry.startswith("day="):
                    continue
                day = day_entry[len("day=") :]
                if len(day) != 10:
                    continue
                days_with_day_files_any.add(day)
                if day in day_covered_by_any_field:
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
                days_with_day_files_any.add(day)
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
        # Hour tokens served by at least one per-field hour file (the pre-
        # bundle intermediate state). Consumed by the missing-hour live heal
        # below — such hours have rollup coverage, just not bundled yet.
        per_field_hours_seen: set[str] = set()
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
                        per_field_hours_seen.add(hour)

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
            # Bundled files carry EVERY field's rows (the bundler folds the
            # whole per-field tree into one parquet), while the per-field
            # branches below are already field-scoped by the directory
            # enumeration above. Without this IN-list a single-field caller
            # (e.g. the security UA-rollup read) pays the GROUP BY + window
            # sort over every field in the bundle — 217ms of the 2026-07-06
            # trace vs the one field it consumed. The filter can't prune
            # bundle IO (rows aren't sorted by field) but it collapses the
            # aggregation hash table and the QUALIFY partitions to just the
            # requested fields. safe_fields is _is_safe_ident-validated;
            # quotes escaped anyway (defense-in-depth, see the note above).
            field_in_list = ", ".join("'" + f.replace("'", "''") + "'" for f in safe_fields)
            if day_paths:
                paths_sql = quote_path_list(day_paths)
                branches.append(
                    f"SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet([{paths_sql}], hive_partitioning=1)"
                )
            if hour_paths:
                paths_sql = quote_path_list(hour_paths)
                branches.append(
                    f"SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet([{paths_sql}], hive_partitioning=1)"
                )
            if bundled_hour_paths:
                # Bundled parquets have `field` as a column already (the
                # bundler SELECTs it from the per-field source files).
                # hive_partitioning=0 because the only hive segment here
                # is `hour=...` which we don't need for the projection.
                paths_sql = quote_path_list(bundled_hour_paths)
                branches.append(
                    f"SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
                    f"WHERE field IN ({field_in_list})"
                )
            if bundled_day_paths:
                # Same shape as bundled_hour (field/value/count as
                # columns, no hive partitioning on the projection).
                paths_sql = quote_path_list(bundled_day_paths)
                branches.append(
                    f"SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
                    f"WHERE field IN ({field_in_list})"
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
        # Callers (dashboard repo) already computed these once for the
        # request; the kwargs above let them seed both so we skip the
        # duplicate get_schema_cols() / _get_schema() round-trip below.
        actual_cols_seed = actual_cols
        schema_types_seed = schema_types
        actual_cols = []
        schema_types = {}

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
                # Reuse caller-supplied seeds when present (dashboard repo
                # already paid this cost once for the request); fall back
                # to the schema lookups otherwise.
                actual_cols = actual_cols_seed if actual_cols_seed is not None else self.get_schema_cols()
                # _get_schema is module-local (line ~106); the prior code
                # imported it from backend.core.duckdb which does NOT
                # export this symbol — the ImportError silently broke the
                # live merge for an indeterminate time, so the per-field
                # top-N was missing the current hour entirely. Use the
                # module-local function directly.
                if schema_types_seed is not None:
                    schema_types = schema_types_seed
                else:
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
                #
                # Narrow the LIVE top-up to fields the dashboard actually renders
                # as facet panels: skip per-request identifiers / raw metrics
                # (_LIVE_TOPN_SKIP_FIELDS) whose live current-hour top-N nobody
                # sees but whose near-unique GROUP-BYs dominate the cost. The
                # rollup path above is untouched (still uses safe_fields), so a
                # skipped field is simply rollup-only — no rendered panel loses
                # current-hour freshness. Narrowing here also keeps the
                # high-cardinality columns out of the active-hour temp itself.
                live_topn_fields = [f for f in safe_fields if f not in _LIVE_TOPN_SKIP_FIELDS]
                _t_lt = time.perf_counter()
                tmp_name = self._create_active_hour_temp_direct(live_topn_fields, actual_cols, live_start, live_end)
                if tmp_name is None:
                    tmp_name = self.create_filtered_temp_table(live_topn_fields, actual_cols, base_table, live_where)
                _phase("live_active_hour:temp_create", (time.perf_counter() - _t_lt) * 1000)
                _phase("live_active_hour:n_files", float(self._last_active_direct_n_files))
                if tmp_name:
                    try:
                        # Diagnostic row count (in-memory temp, ~free): tells
                        # the perf harness whether a slow live merge is data
                        # volume (busy active hour) or per-field batch cost.
                        try:
                            _n_live = self.execute(f'SELECT COUNT(*) FROM "{tmp_name}"').fetchone()[0]
                            _phase("live_active_hour:n_rows", float(_n_live))
                        except Exception:
                            pass
                        # Filter to columns present in the temp's projection.
                        # Virtual fields (waf_sig_ind, edge_score_reason_ind)
                        # have rollup parquets but no live column — including
                        # them here would build SQL referencing a missing
                        # column and BinderException out the entire UNION
                        # ALL, silently dropping the live-hour merge for
                        # the real fields too.
                        live_fields = [f for f in live_topn_fields if f in actual_cols]
                        if live_fields:
                            _t_lb = time.perf_counter()
                            live_res, _ = self.execute_top_n_batch(
                                live_fields, tmp_name, actual_cols, schema_types, limit=_live_limit
                            )
                            _phase("live_active_hour:batch", (time.perf_counter() - _t_lb) * 1000)
                    finally:
                        # Deferred to scope-exit when the shared active-hour
                        # scope owns this temp (dashboard rollup path).
                        self.release_active_direct_temp(tmp_name)
            except Exception:
                pass
        _phase("live_active_hour", (time.perf_counter() - _t_live) * 1000)

        # ── Missing-hour live heal ────────────────────────────────────────
        # Closed hours of NOT-yet-day-compacted days that have no rollup
        # coverage (no hour bundle, no per-field hour file) are live-queried
        # here so the returned top-N stays window-correct through rollup-
        # writer gaps. Diagnosed 2026-07-06: the per-sync recompute only
        # fires when a sync batch ingests rows stamped in an already-closed
        # hour, so on bursty services every closed hour of the current day
        # was silently absent until the nightly deep pass — total_rows (a
        # live COUNT) disagreed with every card's field_total by the same
        # factor. Steady-state cost is ZERO: the hourly rollup_heal cron
        # keeps the bundle tree complete — real bundles for hours with
        # data, empty SENTINEL bundles for verified-zero-row hours (which
        # would otherwise be indistinguishable from writer gaps and re-
        # scanned forever) — so `missing` is empty. The scan only fires
        # while the writer is behind, bounded by _MISSING_HOUR_HEAL_CAP
        # hours.
        #
        # Hours of day-compacted days are deliberately excluded
        # (days_with_day_files_any): their per-hour files were deleted by
        # design, so a window that cuts through such a day would otherwise
        # pay a per-request day-scan forever — that boundary sliver keeps
        # its pre-existing fast-but-partial behavior.
        _t_heal = time.perf_counter()
        heal_res: list[tuple] = []
        heal_floor_dt = active_dt - timedelta(hours=_MISSING_HOUR_HEAL_CAP)
        if st_dt and st_dt > heal_floor_dt:
            heal_floor_dt = st_dt

        # Lazy per-day verification for the day-compacted exclusion: a dir
        # entry alone isn't proof of compaction — a crashed compaction can
        # leave an empty day dir, and treating that as "compacted" would
        # permanently exclude the day from the heal (re-creating the silent
        # undercount this fallback exists to close). Verified only for heal
        # CANDIDATE days (≤ a few per request) and memoized, so the cheap
        # dir-presence set stays the prefilter and the walks above pay
        # nothing extra.
        _day_rollup_presence: dict[str, bool] = {}

        def _day_has_real_day_rollup(day: str) -> bool:
            cached = _day_rollup_presence.get(day)
            if cached is not None:
                return cached
            present = os.path.isfile(os.path.join(bundled_day_root, f"day={day}", "all_fields.parquet"))
            if not present:
                for f_ in safe_fields:
                    d_ = os.path.join(day_root, f"field={f_}", f"day={day}")
                    if not os.path.isdir(d_):
                        continue
                    try:
                        if any(n_.endswith(".parquet") and not n_.startswith(".tmp_") for n_ in os.listdir(d_)):
                            present = True
                            break
                    except OSError:
                        continue
            _day_rollup_presence[day] = present
            return present

        missing_hours: list[str] = []
        missing_hour_dts: list[datetime] = []
        cur_dt = heal_floor_dt.replace(minute=0, second=0, microsecond=0)
        while cur_dt < active_dt:
            h = cur_dt.strftime("%Y-%m-%d-%H")
            h_dt = cur_dt
            cur_dt += timedelta(hours=1)
            if et_str_floor and h > et_str_floor:
                break
            if h in bundled_hours or h in per_field_hours_seen:
                continue
            if h[:10] in days_with_day_files_any and _day_has_real_day_rollup(h[:10]):
                continue
            missing_hours.append(h)
            missing_hour_dts.append(h_dt)
        if missing_hours:
            _now_mono = time.monotonic()
            _svc_key = self.src.get("name") or self.src.get("service_id") or "default"
            if _now_mono - _MISSING_HOUR_HEAL_WARN_TS.get(_svc_key, 0.0) >= _EMPTY_ROLLUP_WARN_INTERVAL_S:
                _MISSING_HOUR_HEAL_WARN_TS[_svc_key] = _now_mono
                _logger.warning(
                    "[top_n_rollups] live-healing %d un-rolled closed hour(s) for service=%r "
                    "(%s..%s) — expected when the writer is <1 heal tick behind; persistent "
                    "occurrences mean the rollup_heal cron is failing (check cron_runs), and "
                    "gaps older than %dh need the daily deep pass or scripts/backfill_rollups.py",
                    len(missing_hours),
                    _svc_key,
                    missing_hours[0],
                    missing_hours[-1],
                    _MISSING_HOUR_HEAL_CAP,
                )
            try:
                if not actual_cols:
                    actual_cols = actual_cols_seed if actual_cols_seed is not None else self.get_schema_cols()
                if not schema_types:
                    if schema_types_seed is not None:
                        schema_types = schema_types_seed
                    else:
                        schema_types = {col["name"]: col["type"] for col in _get_schema(self.con, self.src)}
                heal_topn_fields = [f for f in safe_fields if f not in _LIVE_TOPN_SKIP_FIELDS]
                # Clamp to the requested window so a mid-hour window edge
                # (custom range) can't over-count rows outside it; the IN
                # list prunes to exactly the un-rolled hours in between.
                heal_start_dt = max(missing_hour_dts[0], st_dt) if st_dt else missing_hour_dts[0]
                heal_end_dt = missing_hour_dts[-1] + timedelta(hours=1)
                if et_dt and et_dt < heal_end_dt:
                    heal_end_dt = et_dt
                hours_in_sql = ", ".join(f"'{h}'" for h in missing_hours)
                heal_where = (
                    f"timestamp >= '{heal_start_dt.isoformat()}' "
                    f"AND timestamp < '{heal_end_dt.isoformat()}' "
                    f"AND strftime(timestamp, '%Y-%m-%d-%H') IN ({hours_in_sql})"
                )
                _heal_limit = max([limit] + list((per_field_limits or {}).values()))
                heal_tmp = self.create_filtered_temp_table(heal_topn_fields, actual_cols, base_table, heal_where)
                if heal_tmp:
                    try:
                        heal_fields = [f for f in heal_topn_fields if f in actual_cols]
                        if heal_fields:
                            heal_res, _ = self.execute_top_n_batch(
                                heal_fields, heal_tmp, actual_cols, schema_types, limit=_heal_limit
                            )
                    finally:
                        try:
                            self.execute(f'DROP TABLE IF EXISTS "{heal_tmp}"')
                        except Exception:
                            pass
            except Exception:
                # Best-effort: a failed heal leaves the pre-fallback
                # behavior (rollups + active hour only), never a 500.
                _logger.debug("[top_n_rollups] missing-hour live heal failed", exc_info=True)
        _phase("live_missing_hours", (time.perf_counter() - _t_heal) * 1000)
        _phase("live_missing_hours:n_hours", float(len(missing_hours)))

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
        for field, value, count in heal_res:
            bucket = by_field.setdefault(field, {})
            bucket[value] = bucket.get(value, 0) + count

        # Sort and limit. Per-field limits override the global default for
        # specific fields (e.g. country at 500 for choropleth).
        top_results = []
        _pfl = per_field_limits or {}
        _service_key = self.src.get("name") or self.src.get("service_id") or "default"
        for field in fields:
            bucket_opt: dict[Any, int] | None = by_field.get(field)
            if not bucket_opt:
                # Silent-skip surface: when a caller requests a field that has
                # NO rollup or live entries in the window, the dashboard panel
                # for that field renders empty with no operator trail. This is
                # indistinguishable from "the field truly had zero traffic"
                # vs. "the rollup writer hasn't backfilled this field yet" —
                # see e.g. fingerprint fields added 2026-06-10 that lacked
                # 30-day history. Warn (rate-limited to once per
                # _EMPTY_ROLLUP_WARN_INTERVAL_S per (service, field)) so
                # operators can correlate empty panels with missing rollups.
                #
                # Skip the warning for _LIVE_TOPN_SKIP_FIELDS: these are
                # deliberately omitted from the live top-up and have no rendered
                # panel, so an empty merged result is expected (often un-rolled
                # too) — warning here is a false positive, not a backfill gap.
                if field not in _LIVE_TOPN_SKIP_FIELDS:
                    _ts_key = (_service_key, field)
                    _now = time.monotonic()
                    _last = _EMPTY_ROLLUP_WARN_TS.get(_ts_key, 0.0)
                    if _now - _last >= _EMPTY_ROLLUP_WARN_INTERVAL_S:
                        _EMPTY_ROLLUP_WARN_TS[_ts_key] = _now
                        _logger.warning(
                            "[top_n_rollups] empty result for field=%r (service=%r, window=[%s,%s]) — "
                            "panel will render empty; check rollup backfill coverage if unexpected",
                            field,
                            _service_key,
                            start_time,
                            end_time,
                        )
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

    # Chart metrics the 1-minute time-series rollup can serve. Keys MUST match
    # the ChartMetric Literal in backend/models/dashboard.py.
    #
    # Each metric is expressed as a raw (numerator, denominator) pair over BOTH
    # the rollup columns and the raw rows. The reader UNIONs the closed-hour
    # rollup slice with the active-hour live slice, then re-aggregates in a
    # single outer ``GROUP BY out_bucket`` that rebuilds the metric from the
    # SUMMED raw num/den. Carrying num/den (rather than a per-branch pre-divided
    # rate) is what keeps a bucket that STRADDLES the closed/active boundary
    # correct: at ``1 day`` granularity, today's closed hours (rollup) and the
    # active hour (live) fall in the same day bucket and must combine into one
    # value — not be emitted twice. ``kind`` selects the outer rebuild:
    # ``count`` → ``CAST(SUM(num) AS BIGINT)``; ``rate`` →
    # ``ROUND(SUM(num) * 100.0 / NULLIF(SUM(den), 0), 2)``.
    #
    # The num/den expressions must reproduce the equivalent raw expression in
    # CANONICAL_METRICS so rollup-served and raw-served buckets agree to the
    # value. Percentile / median metrics (p50/p95/p99 latency, throughput,
    # req_size, ttfb median) are excluded — they need sketch-based re-aggregation
    # DuckDB doesn't ship — and fall through to the raw scan.
    # ``live_cols`` lists the raw columns the num_live/den_live expressions
    # touch (beyond ``timestamp``) — the direct active-hour read below
    # projects exactly these into its temp.
    _TS_ROLLUP_METRIC_PARTS: dict[str, dict[str, Any]] = {
        "requests": {
            "kind": "count",
            "num_rollup": "SUM(requests)",
            "den_rollup": "0",
            "num_live": "COUNT(*)",
            "den_live": "0",
            "live_cols": (),
        },
        "5xx": {
            "kind": "rate",
            "num_rollup": "SUM(status_5xx)",
            "den_rollup": "SUM(requests)",
            "num_live": "COUNT(*) FILTER (WHERE status >= 500)",
            "den_live": "COUNT(*)",
            "live_cols": ("status",),
        },
        "4xx": {
            "kind": "rate",
            "num_rollup": "SUM(status_4xx)",
            "den_rollup": "SUM(requests)",
            "num_live": "COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499)",
            "den_live": "COUNT(*)",
            "live_cols": ("status",),
        },
        "hit_rate": {
            "kind": "rate",
            "num_rollup": "SUM(hits)",
            "den_rollup": "SUM(requests)",
            "num_live": "COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE'))",
            "den_live": "COUNT(*)",
            "live_cols": ("cache",),
        },
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
        unfiltered_window: bool = False,
    ) -> list[dict] | None:
        """Serve the dashboard time_series chart from per-hour rollup parquets
        when eligible, falling back transparently to ``None`` otherwise (the
        caller then runs its existing raw query).

        Eligibility:
          * ``chart_metric`` in :attr:`_TS_ROLLUP_METRIC_PARTS`.
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
        only — preferring a direct buffer + active-hourly-partition read
        over the bound iceberg view (see the ``crosses_active`` block) —
        UNION ALL it with the rollup-served portion, and re-aggregate
        in an outer ``GROUP BY out_bucket`` so the chart is always current to
        the second AND a coarse bucket (e.g. ``1 day``) that straddles the
        closed/active boundary combines into one value instead of being
        emitted twice.

        Returns the same shape as the inline raw block in
        ``dashboard.py:get_aggregates``:
        ``[{"time": iso_string, "value": float}, ...]``, ordered by bucket.
        ``None`` means "not eligible — caller should run its raw query".
        """
        import os
        from datetime import UTC, datetime, timedelta

        from backend.core.rollups import TIME_SERIES_BUNDLE_FILENAME, _hour_bundled_root
        from backend.utils.date_utils import parse_iso_utc

        if chart_metric not in self._TS_ROLLUP_METRIC_PARTS:
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

        parts = self._TS_ROLLUP_METRIC_PARTS[chart_metric]
        # Bucket is TIMESTAMPTZ in the bundle parquets (older notes about
        # "naive TIMESTAMP" referred to a since-removed schema). Use
        # TIMESTAMPTZ literals so the comparison is unambiguous regardless
        # of DuckDB's session timezone — without the explicit offset, a
        # session tz like CDT silently shifts the filter by 5 hours and
        # drops bundles at the window's edges.
        st_tz = st.astimezone(UTC).isoformat()
        et_tz = et.astimezone(UTC).isoformat()

        # Each branch emits raw (num, den) per bucket; the outer query below
        # re-aggregates. Both branches MUST expose the same column shape
        # (out_bucket, num, den) for the UNION ALL.
        select_clauses: list[str] = []
        if rollup_paths:
            paths_sql = quote_path_list(rollup_paths)
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', bucket) AS out_bucket, "
                f"       {parts['num_rollup']} AS num, {parts['den_rollup']} AS den "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE bucket >= TIMESTAMPTZ '{st_tz}' "
                f"  AND bucket < TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY 1"
            )

        direct_live_tmp: str | None = None
        live_needs_params = False
        if crosses_active:
            # Live SQL for the [max(st, active_hour_start), et) slice, using
            # the same metric-derivation logic as the rollup branch so the
            # buckets align exactly.
            live_start = max(st, active_hour_dt)
            live_end = et
            live_st_tz = live_start.astimezone(UTC).isoformat()
            live_et_tz = live_end.astimezone(UTC).isoformat()

            # Fast path: read buffer + active hourly partition directly,
            # bypassing the bound iceberg view — same trick as the top-N
            # live branch. Reading the 1-hour slice through the view pays
            # its manifest/union/CASE-projection overhead every load (~60-90ms
            # observed on prod 2026-07-06 vs ~6ms direct). Gated on the
            # caller declaring ``unfiltered_window=True`` (the dashboard's
            # ``use_rollups`` path): the direct read applies ONLY its own
            # timestamp clamp, so a where_clause carrying row filters would
            # be silently ignored on the live slice. Falls back to the
            # view-based scan when the direct read is unavailable (no files
            # on disk, schema mismatch, ...).
            if unfiltered_window:
                live_cols = parts["live_cols"]
                try:
                    # Schema lookup only when the metric projects extra
                    # columns — the default `requests` metric needs just
                    # `timestamp`, which the direct read always includes.
                    _cols: list[str] = self.get_schema_cols() if live_cols else []
                    if all(c in _cols for c in live_cols):
                        direct_live_tmp = self._create_active_hour_temp_direct(
                            list(live_cols), _cols, live_start, live_end
                        )
                except Exception:
                    direct_live_tmp = None
            if direct_live_tmp is not None:
                live_source, live_where = direct_live_tmp, "1=1"
            else:
                live_source, live_where = table_name, where_clause
                live_needs_params = True

            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS out_bucket, "
                f"       {parts['num_live']} AS num, {parts['den_live']} AS den "
                f"FROM {live_source} "
                f"WHERE {live_where} "
                f"  AND timestamp >= TIMESTAMPTZ '{live_st_tz}' "
                f"  AND timestamp <  TIMESTAMPTZ '{live_et_tz}' "
                f"GROUP BY 1"
            )

        if not select_clauses:
            return []

        # Re-aggregate the UNION in an outer GROUP BY out_bucket. The rollup
        # (closed hours) and live (active hour) slices are disjoint at the HOUR
        # grain, but once time_bucket() re-buckets to a COARSER grain (1 day)
        # the active day appears in BOTH slices and must be summed into one row
        # — not emitted twice. Rebuilding the metric from SUM(num)/SUM(den)
        # here (instead of combining per-branch pre-divided rates) keeps rate
        # metrics correct across that seam.
        if parts["kind"] == "count":
            value_expr = "CAST(SUM(num) AS BIGINT)"
        else:  # rate
            value_expr = "ROUND(SUM(num) * 100.0 / NULLIF(SUM(den), 0), 2)"

        unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
        final_sql = (
            f"SELECT out_bucket, {value_expr} AS value "
            f"FROM ({unioned}) "
            f"WHERE out_bucket IS NOT NULL "
            f"GROUP BY out_bucket "
            f"ORDER BY out_bucket"
        )

        try:
            rows = self.execute(final_sql, params if live_needs_params else []).fetchall()
        except duckdb.Error as e:
            # Any read failure (stale view, missing column, schema drift
            # in older bundles, …) drops us to the raw path. Logged at
            # debug — the caller will produce a working result anyway.
            import logging as _logging

            _logging.getLogger(__name__).debug("[time_series_rollup] read failed, falling back to raw: %s", e)
            return None
        finally:
            if direct_live_tmp is not None:
                self.release_active_direct_temp(direct_live_tmp)

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

    # Minimum window-hours below which the rollup read isn't worth the
    # closed-hour enumeration. The slow_urls panel hits raw under 48 h
    # in well under a second on most services, so the rollup path is
    # for the 7 d / 30 d cases that actually hurt.
    _SLOW_URLS_ROLLUP_MIN_HOURS = 48

    def _eligible_rollup_window(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        require_top_asns: list[int] | None = None,
        min_hours: int | None = None,
    ) -> tuple[datetime, datetime] | None:
        """Shared eligibility preamble for the ``try_*_from_rollup`` readers.

        Returns the parsed ``(st, et)`` UTC window when every guard passes,
        else ``None`` (caller falls back to its live/TEMP path). Guards, in
        order: unfiltered only; both bounds present; optional non-empty
        ``require_top_asns`` (network readers pass their top-N list); both
        bounds parse as ISO-8601 UTC with ``et > st``; window spans at least
        ``min_hours`` (default :attr:`_SLOW_URLS_ROLLUP_MIN_HOURS` — readers
        whose fallback is a panel-dedicated raw scan rather than an
        already-materialized temp pass a lower floor) and at most 366 days.
        Every guard is a side-effect-free ``return None``, so the
        short-circuit order is equivalent to the per-method preambles it
        replaces.
        """
        from datetime import timedelta

        from backend.utils.date_utils import parse_iso_utc

        if has_filters:
            return None
        if not start_time or not end_time:
            return None
        if require_top_asns is not None and not require_top_asns:
            return None
        st = parse_iso_utc(start_time)
        et = parse_iso_utc(end_time)
        if st is None or et is None or et <= st:
            return None
        if (et - st) < timedelta(hours=min_hours if min_hours is not None else self._SLOW_URLS_ROLLUP_MIN_HOURS):
            return None
        if (et - st) > timedelta(days=366):
            return None
        return st, et

    def try_slow_urls_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        min_requests: int,
        limit: int,
    ) -> dict | None:
        """Serve the /origin slow_urls panel from per-hour rollup parquets
        when eligible, returning ``None`` so the caller falls back to its
        existing TEMP-table path.

        Eligibility:
          * ``has_filters`` is False (the rollup is built unfiltered;
            applying filter chips at read time would require either
            per-filter-permutation rollups or full re-scan, both out of
            scope for this round).
          * ``start_time`` / ``end_time`` parse as ISO-8601 UTC.
          * Window spans at least
            :attr:`_SLOW_URLS_ROLLUP_MIN_HOURS` hours (raw is fast enough
            below that).
          * Every closed hour in the window has a ``slow_urls.parquet``
            (a single missing closed hour disqualifies the whole window
            — conservative, matches :meth:`try_time_series_from_rollup`).

        Active-hour handling: hours at or after the current UTC hour
        aren't rolled up (writer skips them). The panel is a rank by
        p95 over a multi-day window — the most-recent hour shifts the
        bottom of the list by at most a few URLs and is dominated by
        the bulk of the period. We intentionally do NOT merge live
        SQL for the active hour here: doing so would force a raw scan
        anyway, defeating the speedup. Documented in the response.

        Returns a payload shaped like the TEMP-table path's output:
        ``{"has_data": bool, "rows": [{"url", "requests", "p50_ms",
        "p95_ms", "p99_ms"}, ...], "_approx": True}``. The ``_approx``
        flag tells the caller to bubble up a "30 d approximate" hint
        in the panel response. ``None`` means caller should run its
        existing TEMP-table path.

        Aggregation across hours uses a request-weighted average of
        per-hour p95 — biased relative to the true 30-day p95 (DuckDB
        doesn't ship sketch-combine, see ``_base.py:1387`` comment),
        but the ranking is preserved for the URLs that dominate the
        panel.
        """
        import os
        from datetime import UTC, datetime, timedelta

        from backend.core.rollups import SLOW_URLS_BUNDLE_FILENAME, _hour_bundled_root

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        bundled_root = _hour_bundled_root(self.src)
        if not os.path.isdir(bundled_root):
            return None

        # Walk closed hours. Cheap to inline here vs. reusing
        # collect_hourly_bundle_paths because slow_urls doesn't need
        # the active-hour merge (see docstring above) — we just want
        # the list of available closed-hour rollup files, and "skip
        # the active hour entirely" matches the writer's contract.
        #
        # Unlike try_time_series_from_rollup, which falls back on a
        # single missing closed hour (chart undercount would mislead),
        # the slow_urls panel is a top-N ranking and missing a few
        # hours doesn't change which URLs dominate. We tolerate gaps
        # but require at least 50% closed-hour coverage so a freshly-
        # backfilling service doesn't serve a misleading sample.
        active_hour_str = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        rollup_paths: list[str] = []
        total_closed_hours = 0
        cur = st.replace(minute=0, second=0, microsecond=0)
        if cur < st:
            cur = cur + timedelta(hours=1)
        while cur < et:
            hour_str = cur.strftime("%Y-%m-%d-%H")
            if hour_str >= active_hour_str:
                break
            total_closed_hours += 1
            path = os.path.join(bundled_root, f"hour={hour_str}", SLOW_URLS_BUNDLE_FILENAME)
            if os.path.exists(path):
                rollup_paths.append(path)
            cur = cur + timedelta(hours=1)

        if not rollup_paths:
            # No files at all — fall back to raw.
            return None
        if total_closed_hours > 0 and (len(rollup_paths) / total_closed_hours) < 0.5:
            # Less than half coverage — backfill is still bootstrapping;
            # the partial sample would mislead a 30 d top-N ranking. Let
            # raw serve until coverage improves.
            return None

        # Single SQL read over all per-hour parquets. Request-weighted
        # average so a URL that's slow for a few requests in one hour
        # doesn't outrank a URL that's consistently slow across the
        # period.
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT url, "
            f"       CAST(SUM(requests) AS BIGINT) AS requests, "
            f"       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) AS p50_us_w, "
            f"       SUM(p95_us * requests) / NULLIF(SUM(requests), 0) AS p95_us_w, "
            f"       SUM(p99_us * requests) / NULLIF(SUM(requests), 0) AS p99_us_w "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY url "
            f"HAVING SUM(requests) >= ? "
            f"ORDER BY p95_us_w DESC "
            f"LIMIT ?"
        )
        try:
            rows = self.execute(sql, [min_requests, limit]).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[slow_urls_rollup] read failed, falling back to raw: %s", e)
            return None

        out_rows = []
        for r in rows:
            url, requests, p50_us, p95_us, p99_us = r[0], r[1], r[2], r[3], r[4]
            out_rows.append(
                {
                    "url": url,
                    "requests": int(requests or 0),
                    "p50_ms": (float(p50_us) / 1000.0) if p50_us is not None else None,
                    "p95_ms": (float(p95_us) / 1000.0) if p95_us is not None else None,
                    "p99_ms": (float(p99_us) / 1000.0) if p99_us is not None else None,
                }
            )
        return {"has_data": len(out_rows) > 0, "rows": out_rows, "_approx": True}

    def _collect_rollup_paths(
        self,
        st: datetime,
        et: datetime,
        bundle_filename: str,
        *,
        skip_partial_start: bool = True,
    ) -> list[str] | None:
        """Collect day-prefer / hour-fallback rollup parquet paths for ``[st, et)``.

        Walks closed UTC hours in the window: for each fully-covered day (24
        closed hours in-window) it prefers the per-day compacted file
        ``day_bundled/day=D/<bundle_filename>``, else falls back to the
        per-hour ``hour_bundled/hour=H/<bundle_filename>``. Returns ``None``
        on any eligibility miss — no hour-bundled root, no paths found, or
        < 50 % closed-hour coverage — so the caller falls back to live SQL.

        ``skip_partial_start``: when ``st`` is mid-hour, skip that partial
        leading hour (the default — the rollups are whole-hour grains, so a
        partial leading hour would over-count). ``try_verified_bots_ts`` passes
        ``False`` because it trims precisely in SQL via a ``bucket_ts >= st``
        predicate.

        Shared by the five day-prefer rollup readers (origin_summary,
        network_rtt, network_speed, verified_bots_ts, perf_latency); the
        per-feature SQL + result shaping stays in each caller.
        """
        import os
        from datetime import UTC, datetime, timedelta

        from backend.core.rollups._common import _day_bundled_root, _hour_bundled_root

        hour_root = _hour_bundled_root(self.src)
        if not os.path.isdir(hour_root):
            return None
        day_root = _day_bundled_root(self.src)

        active_hour_str = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        day_buckets: dict[str, list[str]] = {}
        total_closed_hours = 0
        cur = st.replace(minute=0, second=0, microsecond=0)
        if skip_partial_start and cur < st:
            cur = cur + timedelta(hours=1)
        while cur < et:
            hour_str = cur.strftime("%Y-%m-%d-%H")
            if hour_str >= active_hour_str:
                break
            total_closed_hours += 1
            day_buckets.setdefault(cur.strftime("%Y-%m-%d"), []).append(hour_str)
            cur = cur + timedelta(hours=1)

        rollup_paths: list[str] = []
        covered_hours = 0
        for day_str, hours_in_day in sorted(day_buckets.items()):
            day_file = os.path.join(day_root, f"day={day_str}", bundle_filename)
            # Use the per-day compacted file only when the window spans the
            # ENTIRE UTC day (24 in-window hours); a partial day would pick up
            # out-of-range hours. Per-hour fallback keeps the window correct.
            if len(hours_in_day) == 24 and os.path.isfile(day_file):
                rollup_paths.append(day_file)
                covered_hours += 24
            else:
                for hour_str in hours_in_day:
                    p = os.path.join(hour_root, f"hour={hour_str}", bundle_filename)
                    if os.path.isfile(p):
                        rollup_paths.append(p)
                        covered_hours += 1

        if not rollup_paths:
            return None
        if total_closed_hours > 0 and (covered_hours / total_closed_hours) < 0.5:
            return None
        return rollup_paths

    def try_origin_summary_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        actual_cols: set[str] | list[str],
    ) -> dict | None:
        """Serve the /origin summary panel from per-hour rollup parquets
        when eligible, returning ``None`` so the caller falls back to its
        existing TEMP-table SQL.

        Same eligibility posture as :meth:`try_slow_urls_from_rollup`:
        unfiltered only, window ≥ 48 h, ≥50% closed-hour coverage.

        Aggregation: counts (``requests``, ``total_misses``,
        ``total_passes``, ``ost_5xx_count``, ``ost_total_count``,
        ``ottlb_count``, etc.) are exact SUMs. ``origin_error_rate``
        is the cross-hour SUM(5xx_count)/SUM(total_count) ratio — that
        is also exact across hours. Percentiles (``ottfb_p{50,75,95,
        99}_ms``, ``ottlb_p{50,95}_ms``, ``cdn_overhead_p50_ms``,
        ``obytes_p50``) are request-weighted averages of per-hour
        values — biased relative to the true cross-hour percentile
        but preserves the headline values the panel reads off.

        Returns the same dict shape as :func:`_shape_summary`, with an
        added ``_approx: True`` marker. ``None`` means caller should
        run its existing path.
        """
        from backend.core.rollups._common import ORIGIN_SUMMARY_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, ORIGIN_SUMMARY_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT "
            f"  CAST(SUM(requests) AS BIGINT)                                AS requests, "
            f"  CAST(SUM(total_misses) AS BIGINT)                            AS total_misses, "
            f"  CAST(SUM(total_passes) AS BIGINT)                            AS total_passes, "
            f"  SUM(lat_us_count)                                            AS lat_us_count, "
            f"  SUM(ottfb_p50_us * lat_us_count) / NULLIF(SUM(lat_us_count), 0) AS ottfb_p50_us, "
            f"  SUM(ottfb_p75_us * lat_us_count) / NULLIF(SUM(lat_us_count), 0) AS ottfb_p75_us, "
            f"  SUM(ottfb_p95_us * lat_us_count) / NULLIF(SUM(lat_us_count), 0) AS ottfb_p95_us, "
            f"  SUM(ottfb_p99_us * lat_us_count) / NULLIF(SUM(lat_us_count), 0) AS ottfb_p99_us, "
            f"  SUM(ottlb_p50_us * ottlb_count) / NULLIF(SUM(ottlb_count), 0)   AS ottlb_p50_us, "
            f"  SUM(ottlb_p95_us * ottlb_count) / NULLIF(SUM(ottlb_count), 0)   AS ottlb_p95_us, "
            f"  SUM(cdn_ovh_p50_us * cdn_ovh_count) / NULLIF(SUM(cdn_ovh_count), 0) AS cdn_ovh_p50_us, "
            f"  SUM(ost_5xx_count) * 1.0 / NULLIF(SUM(ost_total_count), 0)   AS origin_error_rate, "
            f"  SUM(obytes_p50 * obytes_count) / NULLIF(SUM(obytes_count), 0)  AS obytes_p50 "
            f"FROM read_parquet([{paths_sql}])"
        )
        try:
            row = self.execute(sql).fetchone()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[origin_summary_rollup] read failed, falling back to raw: %s", e)
            return None

        if row is None or row[0] is None or row[0] == 0:
            # No data in the rolled-up window — let raw serve so the
            # caller's has_data=False return shape matches what would
            # come back from the live SQL path.
            return None

        # Honour the same actual-cols gating the live path uses: if the
        # source schema lacks ottlb/obytes/etc., return NULL there so
        # the response shape matches the live path for this service.
        cols_set = set(actual_cols)
        ottfb_p50_us = row[4]
        if ottfb_p50_us is None:
            return None
        ottlb_p50_ms = (float(row[8]) / 1000.0) if (row[8] is not None and "ottlb" in cols_set) else None
        ottlb_p95_ms = (float(row[9]) / 1000.0) if (row[9] is not None and "ottlb" in cols_set) else None
        cdn_ovh_p50_ms = (
            (float(row[10]) / 1000.0)
            if (row[10] is not None and "elapsed" in cols_set and "ottlb" in cols_set)
            else None
        )
        origin_error_rate = float(row[11]) if (row[11] is not None and "ost" in cols_set) else None
        obytes_p50 = float(row[12]) if (row[12] is not None and "obytes" in cols_set) else None

        # Match the dict shape :func:`backend.repositories.origin._shape_summary`
        # returns on the has_data=True path — the FE consumers (Origin
        # Aggregates card) read these keys verbatim and adding/dropping
        # one would silently break the panel. ``requests`` is computed
        # for ranking purposes but intentionally NOT in the response
        # (the live path doesn't expose it either).
        return {
            "has_data": True,
            "total_misses": int(row[1]) if row[1] is not None else None,
            "total_passes": int(row[2]) if row[2] is not None else None,
            "ottfb_p50_ms": float(row[4]) / 1000.0,
            "ottfb_p75_ms": (float(row[5]) / 1000.0) if row[5] is not None else None,
            "ottfb_p95_ms": (float(row[6]) / 1000.0) if row[6] is not None else None,
            "ottfb_p99_ms": (float(row[7]) / 1000.0) if row[7] is not None else None,
            "ottlb_p50_ms": ottlb_p50_ms,
            "ottlb_p95_ms": ottlb_p95_ms,
            "cdn_overhead_p50_ms": cdn_ovh_p50_ms,
            "origin_error_rate": origin_error_rate,
            "obytes_p50": obytes_p50,
            "_approx": True,
        }

    def try_origin_pop_latency_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        limit: int,
    ) -> dict | None:
        """Serve the /api/origin/aggregates ``pop_latency`` panel from the
        per-hour + per-day origin_pop rollup (per-(pop, hour) ``lat_us``
        percentiles).

        Same eligibility posture as :meth:`try_origin_summary_from_rollup`
        (unfiltered, >= 48 h, <= 366 d, >= 50 % closed-hour coverage,
        day-prefer + hour-fallback walk). Percentiles are request-weight-
        averaged across hours/files (biased → ``_approx``); counts SUM exact.
        Ranks by p95 DESC LIMIT, mirroring POP_LATENCY's live ORDER BY.

        Returns the same dict shape as
        :func:`backend.repositories.origin._origin_pop_latency_from_temp`
        (``{has_data, requires_group_c, median_p95_ms, rows:[{pop, requests,
        p50_ms, p95_ms, elevated}], _approx}``) or ``None`` on any eligibility
        miss / read error.
        """
        from backend.core.rollups._common import ORIGIN_POP_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, ORIGIN_POP_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT pop, "
            f"       CAST(SUM(requests) AS BIGINT) AS requests, "
            f"       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) AS p50_us_w, "
            f"       SUM(p95_us * requests) / NULLIF(SUM(requests), 0) AS p95_us_w "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY pop "
            f"ORDER BY p95_us_w DESC "
            f"LIMIT ?"
        )
        try:
            rows = self.execute(sql, [limit]).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[origin_pop_rollup] read failed, falling back: %s", e)
            return None

        if not rows:
            return None

        # median_p95_ms + elevated flag computed exactly as the live/TEMP path
        # (origin._origin_pop_latency_from_temp): p95 values are us here, so
        # convert to ms first to keep the threshold math identical.
        p95_ms_list = [(float(r[3]) / 1000.0) for r in rows if r[3] is not None]
        valid_p95s = sorted(p95_ms_list)
        median_p95 = valid_p95s[len(valid_p95s) // 2] if valid_p95s else 0
        out_rows = []
        for r in rows:
            p95_ms = (float(r[3]) / 1000.0) if r[3] is not None else None
            out_rows.append(
                {
                    "pop": r[0],
                    "requests": int(r[1] or 0),
                    "p50_ms": (float(r[2]) / 1000.0) if r[2] is not None else None,
                    "p95_ms": p95_ms,
                    "elevated": p95_ms is not None and median_p95 is not None and p95_ms > median_p95 * 2,
                }
            )
        return {
            "has_data": True,
            "requires_group_c": False,
            "median_p95_ms": median_p95,
            "rows": out_rows,
            "_approx": True,
        }

    def try_origin_ip_health_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        limit: int,
    ) -> dict | None:
        """Serve the /api/origin/aggregates ``ip_health`` panel from the
        per-hour + per-day origin_ip rollup (per-(oip, hour) ``lat_us``
        percentiles + carried 5xx/total counts).

        Same eligibility posture as :meth:`try_origin_pop_latency_from_rollup`.
        Percentiles are request-weight-averaged (biased → ``_approx``);
        ``error_pct`` is EXACT across hours = SUM(ost_5xx_count) /
        SUM(ost_total_count). Re-applies the live IP_HEALTH window-level
        ``HAVING SUM(requests) >= 10`` + ``ORDER BY error_pct DESC LIMIT`` (the
        writer pre-cut each hour at >= 5 + top-K; see origin_dims docstring for
        the per-hour-floor approximation).

        Returns the same dict shape as
        :func:`backend.repositories.origin._origin_ip_health_from_temp`
        (``{has_data, rows:[{oip, requests, p50_ms, p95_ms, error_pct}],
        _approx}``) or ``None`` on any eligibility miss / read error.
        """
        from backend.core.rollups._common import ORIGIN_IP_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, ORIGIN_IP_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        # error_pct mirrors IP_HEALTH's ROUND(... , 1); HAVING + ORDER + LIMIT
        # re-applied at the window grain (the live HAVING is COUNT >= 10).
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT oip, "
            f"       CAST(SUM(requests) AS BIGINT) AS requests, "
            f"       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) AS p50_us_w, "
            f"       SUM(p95_us * requests) / NULLIF(SUM(requests), 0) AS p95_us_w, "
            f"       ROUND(SUM(ost_5xx_count) * 100.0 / NULLIF(SUM(ost_total_count), 0), 1) AS error_pct "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY oip "
            f"HAVING SUM(requests) >= 10 "
            f"ORDER BY error_pct DESC "
            f"LIMIT ?"
        )
        try:
            rows = self.execute(sql, [limit]).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[origin_ip_rollup] read failed, falling back: %s", e)
            return None

        if not rows:
            return None

        out_rows = [
            {
                "oip": r[0],
                "requests": int(r[1] or 0),
                "p50_ms": (float(r[2]) / 1000.0) if r[2] is not None else None,
                "p95_ms": (float(r[3]) / 1000.0) if r[3] is not None else None,
                "error_pct": float(r[4]) if r[4] is not None else None,
            }
            for r in rows
        ]
        return {"has_data": len(out_rows) > 0, "rows": out_rows, "_approx": True}

    def try_origin_path_breakdown_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> dict | None:
        """Serve the /api/origin/aggregates ``path_breakdown`` panel from the
        per-hour + per-day origin_path rollup (per-(edge, hour) ``lat_us``
        percentiles).

        Same eligibility posture as :meth:`try_origin_pop_latency_from_rollup`.
        Percentiles are request-weight-averaged (biased → ``_approx``); counts
        SUM exact. NO top-K / HAVING (2 rows max). ``shielding_detected`` is
        derived from the presence of an ``edge = false`` row, matching
        :func:`backend.repositories.origin._origin_path_breakdown_from_temp`.

        Returns the same dict shape as the temp path
        (``{has_data, shielding_detected, rows:[{edge, requests, p50_ms,
        p95_ms}], _approx}``) or ``None`` on any eligibility miss / read error.
        """
        from backend.core.rollups._common import ORIGIN_PATH_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, ORIGIN_PATH_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT edge, "
            f"       CAST(SUM(requests) AS BIGINT) AS requests, "
            f"       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) AS p50_us_w, "
            f"       SUM(p95_us * requests) / NULLIF(SUM(requests), 0) AS p95_us_w "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY edge"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[origin_path_rollup] read failed, falling back: %s", e)
            return None

        if not rows:
            return None

        shielding_detected = any(r[0] is False for r in rows)
        out_rows = [
            {
                "edge": r[0],
                "requests": int(r[1] or 0),
                "p50_ms": (float(r[2]) / 1000.0) if r[2] is not None else None,
                "p95_ms": (float(r[3]) / 1000.0) if r[3] is not None else None,
            }
            for r in rows
        ]
        return {
            "has_data": len(out_rows) > 0,
            "shielding_detected": shielding_detected,
            "rows": out_rows,
            "_approx": True,
        }

    def try_origin_status_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> dict | None:
        """Serve the /api/origin/aggregates ``status_codes`` panel from the
        existing ``all_fields.parquet`` per-field bundle (no dedicated writer —
        ``ost`` is already a dashboard FIELDS column maintained by the count
        rollup + backfill).

        Reads ``field='ost' AND value IS NOT NULL`` from the closed-hour /
        closed-day bundles, normalizes ``value`` into the STATUS_CODES bucket
        (``CASE WHEN TRY_CAST(value AS INTEGER) BETWEEN 100 AND 599 THEN that
        ELSE -1 END``), ``SUM(count)`` per status, and computes ``pct =
        count * 100 / SUM(count) OVER ()``. The cross-hour math is a pure SUM
        of integer counts → **EXACT** (no ``_approx`` flag).

        Same eligibility posture as :meth:`try_origin_summary_from_rollup`
        (unfiltered, >= 48 h, <= 366 d, >= 50 % closed-hour coverage,
        day-prefer + hour-fallback walk; active hour skipped — closed hours
        only, so no temp is needed). Returns the same dict shape as
        :func:`backend.repositories.origin._origin_status_codes_from_temp`
        (``{has_data, rows:[{status, count, pct}]}``) or ``None`` on any
        eligibility miss / read error.
        """
        from backend.core.rollups._common import DAY_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        # all_fields.parquet is the shared per-field bundle (schema:
        # field, hour, value, count). Day-prefer / hour-fallback walk.
        rollup_paths = self._collect_rollup_paths(st, et, DAY_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT status, "
            f"       CAST(SUM(count) AS BIGINT) AS count, "
            f"       SUM(count) * 100.0 / SUM(SUM(count)) OVER () AS pct "
            f"FROM ("
            f"  SELECT CASE "
            f"           WHEN TRY_CAST(value AS INTEGER) BETWEEN 100 AND 599 "
            f"             THEN TRY_CAST(value AS INTEGER) "
            f"           ELSE -1 "
            f"         END AS status, "
            f"         count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  WHERE field = 'ost' AND value IS NOT NULL"
            f") "
            f"GROUP BY status "
            f"ORDER BY count DESC"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[origin_status_rollup] read failed, falling back: %s", e)
            return None

        if not rows:
            return None

        return {
            "has_data": True,
            "rows": [
                {"status": int(r[0]), "count": int(r[1]), "pct": float(r[2]) if r[2] is not None else None}
                for r in rows
            ],
        }

    def try_origin_latency_ts_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        bucket_minutes: float,
        metric: str,
        percentile: str,
        split_by_leg: bool,
        table_name: str,
        where_clause: str,
        params: list,
    ) -> dict | None:
        """Serve the /api/origin/aggregates ``timeseries`` panel from the
        minute-granular origin_latency_ts rollup, filling the in-progress
        (active) UTC hour live from the base table.

        Hybrid shape — no existing reader did time-series + percentile. It
        combines :meth:`try_time_series_from_rollup`'s active-hour-merge
        re-bucket shape with the request-weighted percentile merge math of
        :meth:`try_slow_urls_from_rollup` (``SUM(p_us*cnt)/SUM(cnt)``).

        Eligibility → return ``None`` (caller falls to the temp path):
          * ``has_filters`` (rollups are unfiltered).
          * ``split_by_leg`` True (rollup aggregates over edge; the page
            sends ``split_by_leg=false`` so this only ever hits on a non-
            default request).
          * ``bucket_minutes < 1`` (sub-minute — the rollup is minute-grain,
            no intra-minute resolution to give back) or a non-integer-minute
            bucket.
          * ``_eligible_rollup_window`` (unfiltered, >= 48 h, <= 366 d).
          * Any closed hour in the window missing its rollup file — FAIL-CLOSED
            (a time-series undercount misleads; matches
            :meth:`try_time_series_from_rollup`, NOT slow_urls' 50 % floor).

        Per-metric columns: ``metric == 'ttfb'`` → ``ttfb_count`` /
        ``ttfb_p{50,95,99}_us``; ``metric == 'ttlb'`` → the ``ttlb_*`` block.
        The percentile column is picked from ``percentile``.

        Returns the same dict shape as
        :func:`backend.repositories.origin._origin_timeseries_from_temp`
        (``{has_data, series:[{time, miss_count, value}], _approx}``) or
        ``None`` on any eligibility miss / read error. Percentiles are
        request-weighted approximations across minutes → ``_approx: True``.
        """
        import os
        from datetime import UTC, datetime

        from backend.core.rollups import ORIGIN_LATENCY_TS_BUNDLE_FILENAME, _hour_bundled_root

        if split_by_leg:
            return None
        if metric not in ("ttfb", "ttlb"):
            return None
        if percentile not in ("p50", "p95", "p99"):
            return None
        # Minute-grain rollup: sub-minute buckets have no resolution to give
        # back, and a non-integer-minute bucket can't re-bucket exactly.
        try:
            bm = float(bucket_minutes)
        except (TypeError, ValueError):
            return None
        if bm < 1 or bm != int(bm):
            return None
        n_min = int(bm)

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        bundled_root = _hour_bundled_root(self.src)
        if not os.path.isdir(bundled_root):
            return None

        # Fail-closed on any missing closed hour (collect_hourly_bundle_paths
        # returns None when a closed hour had data but no bundle on disk). The
        # crosses_active flag tells us whether to splice in the live tail.
        collected = collect_hourly_bundle_paths(self.src, st, et, bundled_root, ORIGIN_LATENCY_TS_BUNDLE_FILENAME)
        if collected is None:
            return None
        rollup_paths, crosses_active = collected
        if not rollup_paths and not crosses_active:
            return None

        cnt_col = f"{metric}_count"
        pcol = f"{metric}_{percentile}_us"

        st_tz = st.astimezone(UTC).isoformat()
        et_tz = et.astimezone(UTC).isoformat()

        # Each branch emits raw (out_bucket, num, den) per re-bucketed slot; the
        # outer query re-aggregates with SUM(num)/SUM(den) so a coarse bucket
        # straddling the closed/active boundary combines into one row.
        select_clauses: list[str] = []
        if rollup_paths:
            paths_sql = quote_path_list(rollup_paths)
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{n_min} minutes', bucket_ts) AS out_bucket, "
                f"       SUM({pcol} * {cnt_col}) AS num, SUM({cnt_col}) AS den "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE bucket_ts >= TIMESTAMPTZ '{st_tz}' "
                f"  AND bucket_ts <  TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY 1"
            )

        if crosses_active:
            # Live SQL for the [active_hour_start, et) slice over the base
            # table, mirroring _origin_timeseries_from_temp's lat derivation
            # for this metric so the buckets align exactly with the rollup.
            # First per-MINUTE percentile + count (request-weight unit), then
            # re-bucket to the caller's width — matching the rollup branch's
            # weighting so the outer SUM(num)/SUM(den) composes correctly.
            active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
            live_start = max(st, active_hour_start)
            live_st_tz = live_start.astimezone(UTC).isoformat()
            live_et_tz = et.astimezone(UTC).isoformat()

            cols = self.get_schema_cols()
            lat = self._origin_ts_live_lat_expr(metric, set(cols))
            if lat is None:
                # The live tail can't be computed (metric column absent); the
                # rollup-only result would be missing the active hour, so fail
                # closed rather than undercount the chart's tail.
                return None
            pct_val = {"p50": 0.5, "p95": 0.95, "p99": 0.99}[percentile]
            per_min_pct = f"MEDIAN({lat})" if percentile == "p50" else f"APPROX_QUANTILE({lat}, {pct_val})"

            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{n_min} minutes', m) AS out_bucket, "
                f"       SUM(p_us * cnt) AS num, SUM(cnt) AS den "
                f"FROM ("
                f"  SELECT date_trunc('minute', timestamp) AS m, "
                f"         {per_min_pct} AS p_us, COUNT(*) AS cnt "
                f"  FROM {table_name} "
                f"  WHERE {where_clause} "
                f"    AND timestamp >= TIMESTAMPTZ '{live_st_tz}' "
                f"    AND timestamp <  TIMESTAMPTZ '{live_et_tz}' "
                f"    AND ({lat}) IS NOT NULL "
                f"  GROUP BY 1"
                f") GROUP BY 1"
            )

        if not select_clauses:
            return {"has_data": False, "series": [], "_approx": True}

        unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
        final_sql = (
            f"SELECT out_bucket, "
            f"       ROUND(SUM(num) / NULLIF(SUM(den), 0) / 1000.0, 3) AS value, "
            f"       CAST(SUM(den) AS BIGINT) AS miss_count "
            f"FROM ({unioned}) "
            f"WHERE out_bucket IS NOT NULL "
            f"GROUP BY out_bucket "
            f"ORDER BY out_bucket"
        )

        try:
            rows = self.execute(final_sql, params if crosses_active else []).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[origin_latency_ts_rollup] read failed, falling back to raw: %s", e)
            return None

        series: list[dict] = []
        for r in rows:
            if r[0] is None:
                continue
            series.append(
                {
                    "time": safe_iso(r[0]),
                    "miss_count": int(r[2]) if r[2] is not None else 0,
                    "value": float(r[1]) if r[1] is not None else None,
                }
            )
        return {"has_data": len(series) > 0, "series": series, "_approx": True}

    @staticmethod
    def _origin_ts_live_lat_expr(metric: str, cols: set[str]) -> str | None:
        """Live-tail latency-us expression for the origin timeseries reader's
        active-hour merge, matching the origin_latency_ts WRITER's per-metric
        lat (ttfb = COALESCE("ottfb","ttfb"*1e6); ttlb = "ottlb"). Returns
        ``None`` when the metric's column is absent.
        """
        if metric == "ttfb":
            if "ottfb" in cols and "ttfb" in cols:
                return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
            if "ottfb" in cols:
                return '"ottfb"'
            if "ttfb" in cols:
                return '"ttfb" * 1000000.0'
            return None
        # ttlb
        if "ottlb" in cols:
            return '"ottlb"'
        return None

    def try_network_rtt_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        top_asns: list[int],
        has_filters: bool,
    ) -> dict[int, dict[str, float | None]] | None:
        """Serve the /api/network-health ``rtt_percentiles_query`` panel
        from per-hour + per-day network_rtt parquets when eligible.

        Same posture as :meth:`try_origin_summary_from_rollup`: unfiltered
        only, window ≥ 48 h, ≥ 50% closed-hour coverage. ``top_asns`` is
        the FE's already-computed top-N (passed in so the rollup query
        can WHERE-prune to the ASNs the panel actually renders, keeping
        the read tight).

        Returns ``{asn: {"p95_rtt_us": float|None, "p99_rtt_us":
        float|None}}`` mirroring the live path's dict shape. Counts
        SUM exact across hours; p95/p99 are request-weighted averages
        of per-hour percentiles. Returns ``None`` on any eligibility
        miss (caller falls back to live SQL).
        """
        from backend.core.rollups._common import NETWORK_RTT_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters, require_top_asns=top_asns)
        if win is None:
            return None
        st, et = win

        # Same day-prefer / hour-fallback walk as the other percentile
        # rollups. Network_rtt has no day-level compaction yet, but the
        # shared walk keeps the structure so adding one later is just a
        # writer drop-in.
        rollup_paths = self._collect_rollup_paths(st, et, NETWORK_RTT_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        # Parameterise the top-N ASN list; never interpolate ints into SQL.
        asn_placeholders = ", ".join(["?"] * len(top_asns))
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT asn, "
            f"  SUM(p95_us * rtt_count) / NULLIF(SUM(rtt_count), 0) AS p95_us, "
            f"  SUM(p99_us * rtt_count) / NULLIF(SUM(rtt_count), 0) AS p99_us "
            f"FROM read_parquet([{paths_sql}]) "
            f"WHERE asn IN ({asn_placeholders}) "
            f"GROUP BY asn"
        )
        try:
            rows = self.execute(sql, top_asns).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[network_rtt_rollup] read failed, falling back: %s", e)
            return None

        out: dict[int, dict[str, float | None]] = {}
        for row in rows:
            asn_v = int(row[0])
            out[asn_v] = {
                "p95_rtt_us": round(float(row[1]), 0) if row[1] is not None else None,
                "p99_rtt_us": round(float(row[2]), 0) if row[2] is not None else None,
            }
        return out

    def try_network_speed_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        top_asns: list[int],
        has_filters: bool,
    ) -> list[tuple[int, str, int]] | None:
        """Serve the /api/network-health ``speed_distribution_query``
        panel from per-hour + per-day network_speed parquets.

        Same eligibility posture as :meth:`try_network_rtt_from_rollup`:
        unfiltered, >= 48 h, >= 50% closed-hour coverage. Math is EXACT
        across hours (SUM of integer counts), so unlike the percentile
        rollups no ``_approx`` flag is needed.

        Returns rows shaped as ``(asn, c_speed, cnt)`` matching the
        live SQL's row shape; the live caller iterates them into a
        dict keyed by asn. Returns ``None`` on any eligibility miss.
        """
        from backend.core.rollups._common import NETWORK_SPEED_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters, require_top_asns=top_asns)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, NETWORK_SPEED_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        asn_placeholders = ", ".join(["?"] * len(top_asns))
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT asn, c_speed, CAST(SUM(count) AS BIGINT) AS cnt "
            f"FROM read_parquet([{paths_sql}]) "
            f"WHERE asn IN ({asn_placeholders}) "
            f"GROUP BY asn, c_speed "
            f"ORDER BY asn, cnt DESC"
        )
        try:
            rows = self.execute(sql, top_asns).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[network_speed_rollup] read failed, falling back: %s", e)
            return None
        return [(int(r[0]), str(r[1]), int(r[2])) for r in rows]

    def try_network_heatmap_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list | None:
        """Serve the /api/network-health ``heatmap`` section (and the
        derived ``leaderboard`` / ``summary`` / ``buckets`` sections)
        from per-hour + per-day network_heatmap parquets when eligible.

        Same eligibility posture as :meth:`try_network_rtt_from_rollup`:
        unfiltered only, window ≥ 48 h, ≥ 50% closed-hour coverage.

        Each parquet row covers exactly one (asn, hour) pair. The reader
        returns one row per (asn, hour) with the hour as the heatmap
        bucket — hourly bucket granularity on 30 d windows (720 buckets
        instead of the 5-min live resolution of 8640), which is still
        meaningful for trend/health analysis.

        Returns rows shaped as
        ``(asn, bucket_ts, throughput_bps, rtt_med_us, rtt_baseline_us,
           rtt_congestion_us, avg_ploss, jitter_us, error_pct, reqs)``
        matching positions ``r[0]–r[9]`` in the live heatmap loop, or
        ``None`` on any eligibility miss / read error.
        """
        from backend.core.rollups._common import NETWORK_HEATMAP_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, NETWORK_HEATMAP_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        st_iso = st.isoformat()
        et_iso = et.isoformat()
        sql = (
            f"SELECT"
            f"  asn,"
            f"  hour_ts                                                             AS bucket_ts,"
            f"  resp_bytes_sum / 3600.0                                            AS throughput_bps,"
            f"  rtt_p50_us                                                         AS rtt_med_us,"
            f"  rtt_min_p50_us                                                     AS rtt_baseline_us,"
            f"  rtt_p50_us - COALESCE(rtt_min_p50_us, rtt_p50_us)                AS rtt_congestion_us,"
            f"  ploss_sum / NULLIF(ploss_count, 0)                                AS avg_ploss,"
            f"  rtt_var_p50_us                                                     AS jitter_us,"
            f"  errors * 100.0 / NULLIF(reqs, 0)                                  AS error_pct,"
            f"  CAST(reqs AS BIGINT)                                               AS reqs"
            f" FROM read_parquet([{paths_sql}])"
            f" WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
            f" ORDER BY reqs DESC"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[network_heatmap_rollup] read failed, falling back: %s", e)
            return None

        if not rows:
            return None
        return list(rows)

    def try_network_geo_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        map_asn: str = "all",
        has_filters: bool,
    ) -> tuple[list, list] | None:
        """Serve the /api/network-health ``map_buckets`` + ``cities`` +
        ``metro_leaderboard`` sections from per-hour network_geo parquets.

        Same eligibility posture as :meth:`try_network_heatmap_from_rollup`
        (unfiltered, >= 48 h, >= 50% closed-hour coverage). Also returns
        ``None`` when ``map_asn != "all"`` because the geo rollup is built
        across all ASNs and cannot serve per-ASN map drill-down.

        Returns ``(map_rows, metro_rows)`` on hit:
          - ``map_rows``: list of tuples shaped as
            ``(country, city, lat, lon, metro, bucket_ts, rtt_med, avg_ploss,
               error_pct, reqs)`` — matching positions ``r[0]–r[9]`` in the
            live ``MAP_BY_COUNTRY_BUCKET`` loop (top 5000 by reqs).
          - ``metro_rows``: list of tuples shaped as
            ``(country, city, region, metro, rtt_med_us, avg_ploss, error_pct,
               reqs)`` — matching positions ``r[0]–r[7]`` in the live
            ``METRO_LEADERBOARD`` loop (top 100). ``region`` is always ``''``
            (the city display falls back to city + country in format_city_label).

        Returns ``None`` on any eligibility miss / read error.
        """
        from backend.core.rollups._common import NETWORK_GEO_BUNDLE_FILENAME

        if map_asn != "all":
            return None  # per-ASN map drill-down not supported by geo rollup

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, NETWORK_GEO_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        st_iso = st.isoformat()
        et_iso = et.isoformat()

        # Map rows: per (geocell, hour) — one heatmap time-bucket per hour.
        # Row shape matches MAP_BY_COUNTRY_BUCKET output:
        #   (country, city, lat, lon, metro, bucket_ts, rtt_med, avg_ploss, error_pct, reqs)
        map_sql = (
            f"SELECT"
            f"  country,"
            f"  city,"
            f"  lat,"
            f"  lon,"
            f"  metro,"
            f"  hour_ts                                           AS bucket_ts,"
            f"  rtt_sum / NULLIF(rtt_count, 0)                  AS rtt_med_us,"
            f"  ploss_sum / NULLIF(ploss_count, 0)              AS avg_ploss,"
            f"  errors * 100.0 / NULLIF(reqs, 0)                AS error_pct,"
            f"  CAST(reqs AS BIGINT)                             AS reqs"
            f" FROM read_parquet([{paths_sql}])"
            f" WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
            f" ORDER BY hour_ts, reqs DESC"
            f" LIMIT 5000"
        )

        # Metro rows: aggregated across all hours — no time dimension.
        # Row shape matches METRO_LEADERBOARD output:
        #   (country, city, region, metro, rtt_med_us, avg_ploss, error_pct, reqs)
        metro_sql = (
            f"SELECT"
            f"  country,"
            f"  city,"
            f"  CAST('' AS VARCHAR)                                   AS region,"
            f"  metro,"
            f"  SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0)             AS rtt_med_us,"
            f"  SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0)         AS avg_ploss,"
            f"  SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0)           AS error_pct,"
            f"  CAST(SUM(reqs) AS BIGINT)                             AS total_reqs"
            f" FROM read_parquet([{paths_sql}])"
            f" WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
            f" GROUP BY country, city, metro"
            f" ORDER BY total_reqs DESC"
            f" LIMIT 100"
        )
        try:
            map_rows = self.execute(map_sql).fetchall()
            metro_rows = self.execute(metro_sql).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[network_geo_rollup] read failed, falling back: %s", e)
            return None

        if not map_rows and not metro_rows:
            return None
        return list(map_rows), list(metro_rows)

    def try_verified_bots_ts_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        temp_table: str,
        bucket_seconds: int,
        has_filters: bool,
    ) -> list[tuple] | None:
        """Serve the /api/security/aggregates ``verified_bots_ts`` panel
        from the minute-granular verified_bots_ts rollup, filling the
        in-progress (active) UTC hour live from the temp table.

        Eligibility mirrors :meth:`try_network_speed_from_rollup`
        (unfiltered, >= 48 h, <= 366 d, >= 50 % closed-hour coverage,
        day-prefer + hour-fallback walk) plus a ``bucket_seconds`` gate:
        re-bucketing from stored minute granularity is EXACT only when the
        caller's bucket is a whole number of minutes (verified empirically),
        so non-multiples of 60 fall through to the live path.

        The result is built in one SQL statement: a ``UNION ALL`` of the
        rollup (closed hours, re-bucketed from ``bucket_ts``) and a scoped
        live query over the active hour (``timestamp >= active_hour``) from
        the temp table, with an outer ``GROUP BY (bucket, bot_type) SUM`` so
        coarse buckets that straddle the closed/active boundary merge
        correctly. The two ranges are disjoint (split at ``active_hour``) —
        no double-count — and a historical window makes the live branch
        match zero temp rows automatically. The live branch mirrors
        :data:`backend.repositories._sql.security.VERIFIED_BOTS_TS`; keep
        the two in sync.

        Math is EXACT (integer SUM), so no ``_approx`` flag. Returns rows
        shaped ``(bucket_ts, bot_type, count)`` matching the live SQL's row
        shape, or ``None`` on any eligibility miss / read error.
        """
        from datetime import UTC, datetime, timedelta

        from backend.core.rollups._common import VERIFIED_BOTS_TS_BUNDLE_FILENAME
        from backend.utils.date_utils import parse_iso_utc

        if has_filters:
            return None
        if not start_time or not end_time:
            return None
        try:
            n = int(bucket_seconds)
        except (TypeError, ValueError):
            return None
        # Minute-granular re-bucketing is only exact for whole-minute buckets.
        if n < 60 or n % 60 != 0:
            return None
        st = parse_iso_utc(start_time)
        et = parse_iso_utc(end_time)
        if st is None or et is None or et <= st:
            return None
        if (et - st) < timedelta(hours=self._SLOW_URLS_ROLLUP_MIN_HOURS):
            return None
        if (et - st) > timedelta(days=366):
            return None

        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

        # skip_partial_start=False: floor st to the hour so the partial
        # boundary hour is INCLUDED — the WHERE bucket_ts trim below cuts it
        # precisely, and the active hour is filled by the live tail.
        rollup_paths = self._collect_rollup_paths(st, et, VERIFIED_BOTS_TS_BUNDLE_FILENAME, skip_partial_start=False)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        st_iso = st.isoformat()
        et_iso = et.isoformat()
        active_iso = active_hour_start.isoformat()
        sql = (
            f"SELECT bucket, bot_type, CAST(SUM(count) AS BIGINT) AS count FROM ("
            f"  SELECT time_bucket(INTERVAL '{n} seconds', bucket_ts) AS bucket, "
            f"         bot_type, SUM(count) AS count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  WHERE bucket_ts >= TIMESTAMPTZ '{st_iso}' AND bucket_ts < TIMESTAMPTZ '{et_iso}' "
            f"  GROUP BY 1, 2 "
            f"  UNION ALL "
            f"  SELECT time_bucket(INTERVAL '{n} seconds', timestamp) AS bucket, "
            f"         replace(tag, 'VERIFIED-BOT.', '') AS bot_type, COUNT(*) AS count "
            f"  FROM ("
            f"    SELECT timestamp, unnest(string_split(waf_sig, ',')) AS tag "
            f"    FROM {temp_table} "
            f"    WHERE waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%' "
            f"      AND timestamp >= TIMESTAMPTZ '{active_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'"
            f"  ) sub "
            f"  WHERE tag LIKE 'VERIFIED-BOT.%' "
            f"  GROUP BY 1, 2"
            f") GROUP BY bucket, bot_type ORDER BY bucket, bot_type"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[verified_bots_ts_rollup] read failed, falling back: %s", e)
            return None
        return [(r[0], str(r[1]), int(r[2])) for r in rows]

    def try_perf_latency_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        dimension: str,
        sort_by: str,
        has_filters: bool,
        min_requests: int,
        limit: int,
    ) -> dict | None:
        """Serve /api/performance/aggregates' ``top_urls`` / ``top_asns``
        panels from the perf_latency rollup (per-(value, hour) ``elapsed``
        percentiles).

        ``dimension`` ∈ {"url", "asn"} selects the parquet file. Same
        eligibility as :meth:`try_network_speed_from_rollup` (unfiltered,
        >= 48 h, <= 366 d, >= 50 % closed-hour coverage, day-prefer +
        hour-fallback walk). Percentiles are request-weight-averaged across
        hours/files (biased → ``_approx``); ``avg`` is exact
        (elapsed_sum / elapsed_count). Re-ranks by ``sort_by``
        (avg/p50/p95/p99, default p99 — same whitelist the live query uses).

        Returns ``{"rows": [{value, requests, avg_ms, p50_ms, p95_ms,
        p99_ms}], "_approx": True}`` or ``None`` on any eligibility miss /
        read error.
        """
        from backend.core.rollups._common import (
            PERF_TOP_ASNS_BUNDLE_FILENAME,
            PERF_TOP_URLS_BUNDLE_FILENAME,
        )

        filename = PERF_TOP_URLS_BUNDLE_FILENAME if dimension == "url" else PERF_TOP_ASNS_BUNDLE_FILENAME
        # Whitelist → safe to interpolate; default p99 mirrors the live sort.
        sort_col = {"avg": "avg_us", "p50": "p50_us_w", "p95": "p95_us_w", "p99": "p99_us_w"}.get(sort_by, "p99_us_w")
        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, filename)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT value, "
            f"       CAST(SUM(requests) AS BIGINT) AS requests, "
            f"       SUM(elapsed_sum) / NULLIF(SUM(elapsed_count), 0) AS avg_us, "
            f"       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) AS p50_us_w, "
            f"       SUM(p95_us * requests) / NULLIF(SUM(requests), 0) AS p95_us_w, "
            f"       SUM(p99_us * requests) / NULLIF(SUM(requests), 0) AS p99_us_w "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY value "
            f"HAVING SUM(requests) > ? "
            f"ORDER BY {sort_col} DESC "
            f"LIMIT ?"
        )
        try:
            rows = self.execute(sql, [min_requests, limit]).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[perf_latency_rollup] read failed, falling back: %s", e)
            return None

        def _ms(v: Any) -> float | None:
            return (float(v) / 1000.0) if v is not None else None

        out_rows = [
            {
                "value": r[0],
                "requests": int(r[1] or 0),
                "avg_ms": _ms(r[2]),
                "p50_ms": _ms(r[3]),
                "p95_ms": _ms(r[4]),
                "p99_ms": _ms(r[5]),
            }
            for r in rows
        ]
        return {"rows": out_rows, "_approx": True}

    def try_security_req_size_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list[dict] | None:
        """Serve /api/security/aggregates' ``req_size`` panel from per-hour +
        per-day security_req_size parquets (req_header_bytes histogram buckets).

        Same eligibility posture as :meth:`try_network_speed_from_rollup`
        (unfiltered, >= 48 h, <= 366 d, >= 50 % closed-hour coverage, day-prefer
        + hour-fallback walk). Math is EXACT across hours — ``count`` SUMs and
        the bucket order is by MIN(min_val), the same lower-bound ordering the
        live ``REQ_HEADER_SIZE_DIST`` (``ORDER BY min_val``) produces. No
        ``_approx`` flag.

        Returns ``[{"bucket": str, "count": int}, ...]`` ordered by min_val
        ascending (matching the live SQL's row shape after the caller drops
        min_val), or ``None`` on any eligibility miss / read error.
        """
        from backend.core.rollups._common import SECURITY_REQ_SIZE_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, SECURITY_REQ_SIZE_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS c, MIN(min_val) AS mv "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bucket "
            f"ORDER BY mv"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[security_req_size_rollup] read failed, falling back: %s", e)
            return None
        if not rows:
            return None
        return [{"bucket": r[0], "count": int(r[1] or 0)} for r in rows]

    def try_security_conn_reuse_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list[dict] | None:
        """Serve /api/security/aggregates' ``conn_reuse`` panel from per-hour +
        per-day security_conn_reuse parquets (conn_requests reuse buckets).

        Same eligibility + EXACT-count posture as
        :meth:`try_security_req_size_from_rollup`. Bucket order is by
        MIN(min_val) ascending, matching the live ``CONN_REUSE_DIST``
        (``ORDER BY min_val``). No ``_approx`` flag.

        Returns ``[{"bucket": str, "count": int}, ...]`` ordered by min_val
        ascending, or ``None`` on any eligibility miss / read error.
        """
        from backend.core.rollups._common import SECURITY_CONN_REUSE_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, SECURITY_CONN_REUSE_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS c, MIN(min_val) AS mv "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bucket "
            f"ORDER BY mv"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[security_conn_reuse_rollup] read failed, falling back: %s", e)
            return None
        if not rows:
            return None
        return [{"bucket": r[0], "count": int(r[1] or 0)} for r in rows]

    # security_conn_reuse.parquet bucket labels → the dashboard conn_requests
    # histogram's label space. Bucket EDGES line up exactly (1 / 2–5 / 6–20 /
    # >20 — both writers replicate the same ``conn_requests > 0`` floor), so
    # folding the rollup's finer '21-100'/'>100' tail into '21+' is exact.
    # Dashboard labels use en-dash (U+2013) — the FE matches them exactly.
    _CONN_REUSE_TO_DASHBOARD_BUCKET: dict[str, str] = {
        "1 (None)": "1",
        "2-5": "2–5",
        "6-20": "6–20",
        "21-100": "21+",
        ">100": "21+",
    }
    _CONN_REQUESTS_BUCKET_ORDER: tuple[str, ...] = ("1", "2–5", "6–20", "21+")

    def try_conn_requests_hist_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        actual_cols: list[str] | set[str] | None = None,
    ) -> dict | None:
        """Serve the dashboard ``conn_requests`` histogram from the per-hour/
        per-day ``security_conn_reuse`` parquets + a live active-hour top-up.

        Reuses /api/security/aggregates' conn_reuse rollup wholesale — same
        backing column, same ``conn_requests > 0`` floor, exactly-aligned
        bucket edges (see :attr:`_CONN_REUSE_TO_DASHBOARD_BUCKET`) — so the
        dashboard panel needs no writer/backfill machinery of its own.

        Differences from :meth:`try_security_conn_reuse_from_rollup`:

        * ``min_hours=2`` instead of the 48h default. The security panel's
          fallback reads an already-materialized catalog temp, so short
          windows ride along ~free; the dashboard fallback is a dedicated
          window scan, so the rollup should win at the default 24h window.
        * The active hour ∩ window is merged live via the same direct
          buffer+hourly-partition read the top-N live branch uses, so the
          histogram stays current like the other dashboard cards. When the
          direct read isn't available the top-up is skipped rather than
          escalated to a view scan — matching the security panel's shipped
          posture (it serves closed hours only).

        Window edge: a mid-hour window START loses its partial leading hour
        (whole-hour rollup grain, ``skip_partial_start``) — ≤1h on a ≥2h
        window; the distribution shape is what the card communicates.

        Returns ``{"top": [{"value", "count"}, ...], "total": int}`` in
        fixed bucket order — the exact shape the dashboard's live
        ``CONN_REQUESTS_BUCKET`` block produces — or ``None`` on any
        eligibility miss (caller falls back to that live scan).
        """
        from datetime import UTC, datetime, timedelta

        from backend.core.rollups._common import SECURITY_CONN_REUSE_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters, min_hours=2)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, SECURITY_CONN_REUSE_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS c FROM read_parquet([{paths_sql}]) GROUP BY bucket"
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            _logger.debug("[conn_requests_hist_rollup] read failed, falling back: %s", e)
            return None
        if not rows:
            return None

        counts: dict[str, int] = {}
        for bucket, cnt in rows:
            label = self._CONN_REUSE_TO_DASHBOARD_BUCKET.get(bucket, bucket)
            counts[label] = counts.get(label, 0) + int(cnt or 0)

        # Live top-up: [active hour ∩ window). Closed hours are all rollup-
        # served (_collect_rollup_paths stops at the active hour).
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(active_dt, st)
        live_end = min(active_dt + timedelta(hours=1), et)
        if live_start < live_end:
            cols = actual_cols if actual_cols is not None else self.get_schema_cols()
            if "conn_requests" in cols:
                tmp = self._create_active_hour_temp_direct(["conn_requests"], cols, live_start, live_end)
                if tmp is not None:
                    try:
                        from backend.repositories._sql import dashboard as _dash_sql

                        live_q = _dash_sql.CONN_REQUESTS_BUCKET.format(
                            requests_metric=CANONICAL_METRICS["requests"],
                            table_name=tmp,
                            where_clause="1=1",
                        )
                        for bucket, cnt in self.execute(live_q).fetchall():
                            counts[bucket] = counts.get(bucket, 0) + int(cnt or 0)
                    except duckdb.Error as e:
                        _logger.debug("[conn_requests_hist_rollup] live top-up failed (skipped): %s", e)
                    finally:
                        self.release_active_direct_temp(tmp)

        known = [b for b in self._CONN_REQUESTS_BUCKET_ORDER if b in counts]
        # Unknown labels (writer drift) are appended rather than dropped so
        # their counts stay visible instead of silently vanishing.
        extras = [b for b in counts if b not in self._CONN_REQUESTS_BUCKET_ORDER]
        top = [{"value": b, "count": counts[b]} for b in known + extras]
        return {"top": top, "total": sum(counts.values())}

    def try_ngwaf_top_bots_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        n: int,
    ) -> list[dict] | None:
        """Serve ``get_top_bots``' ``ngwaf_bots`` panel from the per-hour/
        per-day ``ngwaf_bots`` parquets + a live active-hour top-up.

        The rollup writer already joined each closed hour's ``waf_req_id``s
        against the SQLite bot cache (see
        :mod:`backend.core.rollups.ngwaf_bots`), so this reader SUMs tiny
        aggregated parquets instead of the per-request direct join over the
        raw window (~115ms on prod 24h, 2026-07-07). EXACT SUM; no
        ``_approx``.

        ``min_hours=2`` (not the 48h default): the fallback is a dedicated
        window join, so the rollup should win at the default 24h window.

        Active hour ∩ window merges live via the direct buffer read +
        ``ngwaf_top`` join — the caller has already ATTACHed the cache (this
        reader is only invoked from the ``ngwaf_attached`` branch). A failed
        top-up is skipped, not escalated (bounded staleness ≤1h, same
        posture as the conn_requests histogram reader).

        Returns ``[{"name", "category", "request_count"}, ...]`` sorted by
        count descending, capped at ``n`` — the exact shape the direct-join
        branch produces. An EMPTY list is a valid result (covered window,
        zero bots); ``None`` means ineligible/no coverage → caller falls
        back to the direct join.
        """
        from datetime import UTC, datetime, timedelta

        from backend.core.rollups._common import NGWAF_BOTS_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters, min_hours=2)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, NGWAF_BOTS_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT bot_name, category, CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bot_name, category"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            _logger.debug("[ngwaf_bots_rollup] read failed, falling back: %s", e)
            return None

        counts: dict[tuple[str, str], int] = {}
        for bot_name, category, cnt in rows:
            key = (bot_name, category)
            counts[key] = counts.get(key, 0) + int(cnt or 0)

        # Live top-up: [active hour ∩ window). Closed hours are all rollup-
        # served (_collect_rollup_paths stops at the active hour).
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(active_dt, st)
        live_end = min(active_dt + timedelta(hours=1), et)
        if live_start < live_end:
            cols = self.get_schema_cols()
            if "waf_req_id" in cols:
                tmp = self._create_active_hour_temp_direct(["waf_req_id"], cols, live_start, live_end)
                if tmp is not None:
                    try:
                        live_q = (
                            f"SELECT nb.bot_name, nb.category, CAST(COUNT(*) AS BIGINT) AS c "
                            f'FROM "{tmp}" t '
                            f"INNER JOIN ngwaf_top.ngwaf_bots nb USING (waf_req_id) "
                            f"WHERE nb.bot_name IS NOT NULL "
                            f"GROUP BY 1, 2"
                        )
                        for bot_name, category, cnt in self.execute(live_q).fetchall():
                            key = (bot_name, category)
                            counts[key] = counts.get(key, 0) + int(cnt or 0)
                    except duckdb.Error as e:
                        _logger.debug("[ngwaf_bots_rollup] live top-up failed (skipped): %s", e)
                    finally:
                        self.release_active_direct_temp(tmp)

        ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[: max(0, int(n))]
        return [{"name": bn, "category": cat, "request_count": c} for (bn, cat), c in ranked]

    def try_security_top_ips_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list[dict] | None:
        """Serve /api/security/aggregates' top-IPs-by-max-header panel from
        per-hour + per-day security_topips parquets.

        Same eligibility posture as :meth:`try_security_req_size_from_rollup`.
        The cross-hour merge is MAX-of-MAX (NOT SUM) — re-ranked top-10 by
        max_header DESC, matching the live ``TOP_IPS_BY_MAX_HEADER``
        (``MAX(req_header_bytes) ... ORDER BY 2 DESC LIMIT 10``). EXACT; no
        ``_approx`` flag.

        Returns ``[{"ip": str, "max_header": int}, ...]`` top-10 by max_header
        descending, or ``None`` on any eligibility miss / read error.
        """
        from backend.core.rollups._common import SECURITY_TOPIPS_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, SECURITY_TOPIPS_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT ip, CAST(MAX(max_header) AS BIGINT) AS mh "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY ip "
            f"ORDER BY mh DESC "
            f"LIMIT 10"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[security_top_ips_rollup] read failed, falling back: %s", e)
            return None
        if not rows:
            return None
        return [{"ip": r[0], "max_header": int(r[1] or 0)} for r in rows]

    def try_security_coverage_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> tuple[int, int] | None:
        """Serve /api/security/aggregates' TLS-fingerprint coverage counts from
        per-hour + per-day security_cov parquets.

        Same eligibility posture as :meth:`try_security_req_size_from_rollup`.
        ``total_rows`` and ``tls_populated`` SUM exactly across hours, matching
        the live ``FINGERPRINT_COVERAGE_BULK`` over ``tls_ciphers_sha``. EXACT.

        Returns ``(total_rows, tls_populated)`` summed across the window, or
        ``None`` on any eligibility miss / read error / zero-or-NULL total
        (so the caller's no-coverage shape matches the live path).
        """
        from backend.core.rollups._common import SECURITY_COV_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, SECURITY_COV_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = f"SELECT SUM(total_rows), SUM(tls_populated) FROM read_parquet([{paths_sql}])"
        try:
            row = self.execute(sql).fetchone()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[security_coverage_rollup] read failed, falling back: %s", e)
            return None
        if row is None or row[0] is None or int(row[0]) == 0:
            return None
        return int(row[0]), int(row[1] or 0)

    def try_perf_ttl_dist_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list[dict] | None:
        """Serve /api/performance/aggregates' ``ttl_dist`` histogram panel from
        per-hour + per-day perf_ttl_dist parquets (the ``ttl`` histogram buckets).

        Same eligibility posture as :meth:`try_network_speed_from_rollup`
        (unfiltered, >= 48 h, <= 366 d, >= 50 % closed-hour coverage, day-prefer
        + hour-fallback walk) but with NO ``top_asns`` requirement. Math is EXACT
        across hours — ``count`` SUMs and the bucket order is by MIN(min_ttl), the
        same lower-bound ordering the live ttl_dist histogram (``ORDER BY
        min_ttl``) produces. No ``_approx`` flag.

        Closed-hours-only via :meth:`_collect_rollup_paths` (it skips the active
        hour); no active-hour merge — the live <48 h path covers narrow windows.

        Returns ``[{"bucket": str, "count": int}, ...]`` ordered by min_ttl
        ascending (matching the live SQL's row shape after the caller drops
        min_ttl), or ``None`` on any eligibility miss / read error.
        """
        from backend.core.rollups._common import PERF_TTL_DIST_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, PERF_TTL_DIST_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS count "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bucket "
            f"ORDER BY MIN(min_ttl)"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.Error as e:
            import logging as _logging

            _logging.getLogger(__name__).debug("[perf_ttl_dist_rollup] read failed, falling back: %s", e)
            return None
        return [{"bucket": r[0], "count": int(r[1])} for r in rows]

    def execute_ip_spread_rollups(
        self,
        fields: list[str],
        start_time: str | None,
        end_time: str | None,
        *,
        _phase_log: list[dict] | None = None,
    ) -> tuple[dict[tuple[str, str], int], dict[str, dict]]:
        """Merge HLL IP-spread sketches across the requested window.

        Returns ``({(field, value): merged_ip_count}, {field: meta})``
        where ``merged_ip_count`` is the HLL cardinality estimate over
        the union of all per-hour sketches for that (field, value)
        within the window. The meta dict carries per-field bookkeeping
        the caller can surface to operators / FE:

          - ``coverage_hours``: number of in-window hours from which
            this field had at least one ip_spread parquet row. Lets
            the caller render an "approximate (partial coverage)"
            hint when the writer was still backfilling.
          - ``capped_values``: count of (field, value) pairs where any
            input sketch had ``sample_capped=True`` (the writer's
            IP_SAMPLE_CAP fired). At the boundary the per-hour
            distinct count exceeded the cap, so the merged HLL is a
            lower-bound estimate.

        Skips the active hour (the writer hasn't materialized it yet —
        the caller is expected to use the live temp scan for the
        active-hour slice when it matters, mirroring the count
        rollup's active-hour handling in ``execute_top_n_rollups``).

        Returns ``({}, {})`` when no ip_spread parquets exist for any
        of the requested fields in the window — the caller's signal to
        fall back to the live ``count(DISTINCT ip)`` SQL path."""
        import os
        from datetime import UTC, datetime, timedelta

        from backend.core.duckdb import _cache_dir
        from backend.core.rollups._common import (
            IP_SPREAD_BUNDLE_FILENAME,
            _hour_bundled_root,
            _ip_spread_root,
            _is_safe_ident,
        )
        from backend.utils.date_utils import parse_iso_utc
        from backend.utils.hll import HyperLogLog

        def _phase(name: str, ms: float) -> None:
            if _phase_log is not None:
                _phase_log.append({"section": f"ip_spread_rollup:{name}", "time_ms": round(ms, 2)})

        # Defense in depth: same field-name guard the count rollup
        # uses. Stops any future caller from injecting through the
        # fields parameter into the SQL filter below.
        safe_fields = [f for f in fields if _is_safe_ident(f)]
        if not safe_fields:
            return {}, {}

        st_dt = parse_iso_utc(start_time) if start_time else None
        et_dt = parse_iso_utc(end_time) if end_time else None
        active_str = datetime.now(UTC).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d-%H")

        cache_dir = _cache_dir(self.src)
        ip_spread_per_field_root = _ip_spread_root(self.src)
        bundled_hour_root = _hour_bundled_root(self.src)

        # Pre-pass: discover which closed hours fall in the requested
        # window AND have at least one ip_spread source on disk. Returns
        # early if neither tree exists yet (cold pool, no writer ticks
        # have run for any field in this service).
        if not os.path.isdir(ip_spread_per_field_root) and not os.path.isdir(bundled_hour_root):
            return {}, {}

        if st_dt is not None:
            st_str = st_dt.strftime("%Y-%m-%d-%H")
        else:
            st_str = None
        if et_dt is not None:
            # Half-open semantics match execute_top_n_rollups: an
            # end_time on an exact hour boundary excludes that hour.
            et_inclusive = (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H")
        else:
            et_inclusive = None

        def _in_window(hour: str) -> bool:
            if hour >= active_str:
                return False
            if st_str is not None and hour < st_str:
                return False
            if et_inclusive is not None and hour > et_inclusive:
                return False
            return True

        # Bundled paths first (one file per hour, multi-field). Per-
        # field paths supply hours that DON'T have a bundle yet (cold
        # writer hours, or the bundler hasn't caught up to the latest
        # recompute tick). Hive-partitioning differs between the two
        # trees (bundled has field as a regular column; per-field has
        # it in the path), so they need separate read_parquet calls
        # UNION ALL'd — same constraint that the count rollup reader
        # documents at backend/repositories/_base.py:914-921.
        import time as _time

        _t = _time.perf_counter()
        bundled_paths: list[str] = []
        bundled_hours: set[str] = set()
        if os.path.isdir(bundled_hour_root):
            try:
                for entry in os.listdir(bundled_hour_root):
                    if not entry.startswith("hour="):
                        continue
                    hour = entry[len("hour=") :]
                    if not _in_window(hour):
                        continue
                    bundle_path = os.path.join(bundled_hour_root, entry, IP_SPREAD_BUNDLE_FILENAME)
                    if os.path.isfile(bundle_path):
                        bundled_paths.append(bundle_path)
                        bundled_hours.add(hour)
            except OSError:
                pass

        per_field_paths: list[str] = []
        per_field_hours_by_field: dict[str, set[str]] = {f: set() for f in safe_fields}
        if os.path.isdir(ip_spread_per_field_root):
            try:
                for field_entry in os.listdir(ip_spread_per_field_root):
                    if not field_entry.startswith("field="):
                        continue
                    field = field_entry[len("field=") :]
                    if field not in safe_fields:
                        continue
                    field_dir = os.path.join(ip_spread_per_field_root, field_entry)
                    try:
                        for hour_entry in os.listdir(field_dir):
                            if not hour_entry.startswith("hour="):
                                continue
                            hour = hour_entry[len("hour=") :]
                            if not _in_window(hour):
                                continue
                            # Bundle wins — skip per-field for hours
                            # already covered by all_fields_ip.parquet
                            # so we don't double-count the same data.
                            if hour in bundled_hours:
                                continue
                            hour_dir = os.path.join(field_dir, hour_entry)
                            for fname in os.listdir(hour_dir):
                                if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                                    continue
                                per_field_paths.append(os.path.join(hour_dir, fname))
                                per_field_hours_by_field[field].add(hour)
                    except OSError:
                        continue
            except OSError:
                pass
        _phase("enumerate", (_time.perf_counter() - _t) * 1000.0)

        if not bundled_paths and not per_field_paths:
            return {}, {}

        # Build SQL — at most two SELECT branches UNION ALL'd. Field
        # filter goes on the column directly because the bundled
        # parquets carry field as a regular column. The per-field
        # branch reads it from the hive path via hive_partitioning=1.
        field_filter_sql = "(" + ", ".join("'" + f.replace("'", "''") + "'" for f in safe_fields) + ")"

        branches: list[str] = []
        if bundled_paths:
            paths_sql = quote_path_list(bundled_paths)
            branches.append(
                f"SELECT field, value, ip_sketch, sample_capped "
                f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
                f"WHERE field IN {field_filter_sql}"
            )
        if per_field_paths:
            paths_sql = quote_path_list(per_field_paths)
            branches.append(
                f"SELECT field, value, ip_sketch, sample_capped "
                f"FROM read_parquet([{paths_sql}], hive_partitioning=1) "
                f"WHERE field IN {field_filter_sql}"
            )

        unioned = " UNION ALL ".join(f"({b})" for b in branches)

        _t = _time.perf_counter()
        try:
            rows = self.execute(unioned).fetchall()
        except duckdb.Error as e:
            # A read failure here drops the caller to its live fallback
            # — never let an ip_spread bug take down the security tab.
            import logging as _logging

            _logging.getLogger(__name__).debug("[ip_spread_rollup] read failed: %s", e)
            return {}, {}
        _phase("read", (_time.perf_counter() - _t) * 1000.0)

        # Fold sketches by (field, value) in Python. HLL merge is
        # O(m) = O(256) per pair so the per-row cost is constant; the
        # total time scales with the number of rows we pulled, which
        # is bounded by ``top_K_per_field × len(safe_fields) × hours``.
        _t = _time.perf_counter()
        merged: dict[tuple[str, str], HyperLogLog] = {}
        any_capped: dict[tuple[str, str], bool] = {}
        for field, value, sketch_bytes, capped in rows:
            if not isinstance(sketch_bytes, (bytes, bytearray)) or not sketch_bytes:
                continue
            try:
                hll = HyperLogLog.from_bytes(sketch_bytes)
            except ValueError:
                # Malformed BLOB — skip rather than poison the whole
                # merge. Logged at debug because this implies on-disk
                # corruption that the next recompute will overwrite.
                continue
            key = (field, value)
            if key not in merged:
                merged[key] = HyperLogLog(precision=hll.precision)
                any_capped[key] = bool(capped)
            else:
                any_capped[key] = any_capped[key] or bool(capped)
            try:
                merged[key].merge(hll)
            except ValueError:
                # Precision mismatch (unexpected — writer pins p=8).
                # Drop this row; the merged sketch still reflects every
                # other input. Better partial than wrong.
                continue
        _phase("merge", (_time.perf_counter() - _t) * 1000.0)

        result_counts: dict[tuple[str, str], int] = {k: int(v.count()) for k, v in merged.items()}

        # Per-field metadata so the caller can render coverage hints
        # without re-walking the filesystem. ``coverage_hours`` is the
        # set union of bundled-covered hours (which contain rows for
        # ALL fields) and per-field-hour-covered hours specific to
        # this field; ``capped_values`` is the count of distinct
        # (field, value) pairs flagged by any input row.
        per_field_meta: dict[str, dict] = {}
        capped_by_field: dict[str, int] = {f: 0 for f in safe_fields}
        for (field, _value), capped in any_capped.items():
            if capped:
                capped_by_field[field] = capped_by_field.get(field, 0) + 1

        for field in safe_fields:
            field_hours = bundled_hours | per_field_hours_by_field.get(field, set())
            per_field_meta[field] = {
                "coverage_hours": len(field_hours),
                "capped_values": capped_by_field.get(field, 0),
            }

        return result_counts, per_field_meta

    def execute_top_n_batch(
        self, fields: list[str], table_name: str, actual_cols: list[str], schema_types: dict[str, str], limit: int = 10
    ) -> tuple[list[tuple], list[str]]:
        """
        Per-field top-N batch over multiple fields. Returns
        (fetchall_results, field_order) where each row is (field, value, count).

        Shape: one ``GROUP BY value ... QUALIFY ROW_NUMBER()`` branch per
        field, stitched together with ``UNION ALL`` and a final
        ``ORDER BY field, c DESC``.

        Why not UNPIVOT: a prior shape projected every field into one CTE and
        ``UNPIVOT``ed it into (field, value) pairs so the source was scanned
        once. That single scan only pays off when the source is on disk (one
        parquet read instead of N). Both callers here pass an **already
        materialized in-memory TEMP TABLE** — the live active-hour
        ``t_active_direct`` (``execute_top_n_rollups``) and the dashboard's
        wide ``t_<hex>`` temp (non-rollup path) — so there is no parquet read
        to amortize. For those, ``UNPIVOT`` is pure overhead: it explodes the
        table to ``rows × N_fields`` intermediate rows before grouping, which
        dominated the live merge on busy active hours (≈2.2s in one
        observed dashboard load). Scanning the in-memory temp once per field
        (DuckDB reads only the one column each branch touches) avoids the
        explosion. The rolled-parquet query in ``execute_top_n_rollups``
        keeps its own UNPIVOT — there the single-scan tradeoff still holds.

        ``ORDER BY field, c DESC`` makes the top-N deterministic: the
        non-rollup dashboard path slices the first ``_PANEL_LIMIT`` rows per
        field in result order without re-sorting, so emitting count-descending
        here is required for that path to show the true top-N. (The rollup
        path re-buckets and re-sorts ``live_res``, so the order is harmless
        there.)
        """
        from backend.core.rollups import _is_safe_ident

        # Fields whose underlying values are stored as FLOAT but represent
        # integer seconds (Fastly's obj.ttl / obj.age). Floating-point jitter
        # at ingest produces near-duplicate keys (3600.027, 3600.028, …) that
        # split GROUP BY into many tiny buckets. Round to integer to collapse
        # them — also drops the "3600.027" display in TopTenTable.
        INT_AGGREGATE_FIELDS = {"ttl", "age"}

        branches: list[str] = []
        field_order: list[str] = []

        for field in fields:
            if not _is_safe_ident(field):
                continue
            sql_col = field
            col_type = schema_types.get(sql_col, "VARCHAR")

            # Normalized VARCHAR value expression for this field. Empty-string
            # folding for VARCHAR cols is done via NULLIF so the
            # ``WHERE value IS NOT NULL`` filter handles both null and
            # empty-string in one place.
            if col_type == "VARCHAR":
                value_expr = f"NULLIF({sql_col}, '')"
            elif field in INT_AGGREGATE_FIELDS:
                value_expr = (
                    f"CASE WHEN {sql_col} IS NULL THEN NULL ELSE CAST(CAST(ROUND({sql_col}) AS INTEGER) AS VARCHAR) END"
                )
            else:
                value_expr = f"CAST({sql_col} AS VARCHAR)"

            # Field name reaches the SQL as a string literal. _is_safe_ident
            # already restricted it to a safe identifier; escape single quotes
            # defensively anyway.
            field_lit = field.replace("'", "''")
            branches.append(
                f"(SELECT '{field_lit}' AS field, value, count(*) AS c "
                f"FROM (SELECT {value_expr} AS value FROM {table_name}) "
                f"WHERE value IS NOT NULL "
                f"GROUP BY value "
                f"QUALIFY ROW_NUMBER() OVER (ORDER BY c DESC) <= {limit})"
            )
            field_order.append(field)

        if not branches:
            return [], []

        q = "\n            UNION ALL\n            ".join(branches) + "\n            ORDER BY field, c DESC"
        return self.execute(q).fetchall(), field_order


# R-1: register the schema + listdir caches so the autouse fixture in
# tests/conftest.py drains them via CacheRegistry.clear_all(). Both
# are hot-path memoes that risk cross-test leak when the same source
# name + temp-table key is reused across tests.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("repositories._base._schema_cols_cache", _schema_cols_cache)
_CacheRegistry.register("repositories._base._listdir_cache", _listdir_cache)
# Per-(service, field) warning-throttle timestamps. Without registration
# a test that triggers an empty-rollup warning leaves entries behind, and
# a later test that expects the warning to fire again is silently throttled.
_CacheRegistry.register("repositories._base._EMPTY_ROLLUP_WARN_TS", _EMPTY_ROLLUP_WARN_TS)
# Same leak class for the missing-hour live-heal warning throttle.
_CacheRegistry.register("repositories._base._MISSING_HOUR_HEAL_WARN_TS", _MISSING_HOUR_HEAL_WARN_TS)
