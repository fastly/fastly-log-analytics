"""Shared helpers used by every repository module.

Import from here instead of redefining in each file:

    from backend.repositories._base import _safe_table, _get_schema, safe_iso, QueryRunner
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import time
from typing import TYPE_CHECKING, Any, Literal

import duckdb

from backend.core import metric_expressions
from backend.core.field_registry import Group, in_group
from backend.core.retry import retry_call
from backend.core.rollups._common import quote_path_list
from backend.models.reader_coverage import (
    HistoryObservation,
    ReaderCoverage,
    RollupPathCoverage,
    ScalarRollupCoverage,
    TopNRollupCoverage,
)
from backend.repositories import rollup_coverage
from backend.repositories.rollup_coverage import (
    MISSING_HOUR_HEAL_CAP as _MISSING_HOUR_HEAL_CAP,
)
from backend.repositories.rollup_coverage import (
    collect_day_prefer_paths,
    collect_hour_paths_with_floor,
    durable_serving_blocked,
    eligible_window,
    plan_hourly_read,
)
from backend.repositories.rollup_coverage import heal_where_clause as _heal_where_clause

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from datetime import datetime

from backend.repositories.ip_spread_rollups import build_ip_spread_sql as _build_ip_spread_sql
from backend.repositories.ip_spread_rollups import fold_sketches as _fold_sketches
from backend.repositories.ip_spread_rollups import plan_ip_spread_paths as _plan_ip_spread_paths
from backend.repositories.ip_spread_rollups import spread_meta as _spread_meta
from backend.repositories.top_n_rollups import SchemaSeeds as _SchemaSeeds
from backend.repositories.top_n_rollups import TopNWindow as _TopNWindow
from backend.repositories.top_n_rollups import build_rolled_sql as _build_rolled_sql
from backend.repositories.top_n_rollups import find_unrolled_hours as _find_unrolled_hours
from backend.repositories.top_n_rollups import fold_rows as _fold_rows
from backend.repositories.top_n_rollups import live_topn_fields as _live_topn_fields
from backend.repositories.top_n_rollups import plan_top_n_paths as _plan_top_n_paths
from backend.repositories.top_n_rollups import truncate_top_n as _truncate_top_n
from backend.utils.log_throttle import log_once_per
from backend.utils.sql_validator import escape_sql_literal

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
_MISSING_HOUR_HEAL_WARN_TS: dict[str, float] = {}

# Clock-skew margin (seconds) for `_create_active_hour_temp_direct`'s buffer
# mtime pruning: a file finalized before `live_start - margin` cannot hold
# rows >= live_start. NOT reused for `_partial_hour_adjusted_live_start`'s
# watermark narrowing (I2, final whole-branch review) — see that method's
# docstring for why subtracting a margin there is unsafe for this writer.
_MTIME_SKEW_MARGIN_S = 300

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
    # METRICS-group catalog entries (requests, hit_rate, p50_latency, ...)
    # all have vcl=None — they are chart-metric-only synthetic concepts,
    # never a real per-row column, so referencing one as a SELECT column
    # against raw/buffer parquet always fails to bind. Derived from the
    # registry (not hand-listed) so a new METRICS field can't silently
    # reintroduce this same live incident: every 30s partial-hour-merge
    # tick errored on every service the moment a "requests" field appeared
    # in FIELDS, because no caller had ever excluded the METRICS group.
    | {f.code for f in in_group(Group.METRICS)}
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

    attached = False
    if sqlite_path and os.path.exists(sqlite_path):
        try:
            escaped = escape_sql_literal(sqlite_path)
            con.execute(f"ATTACH '{escaped}' AS {alias} (TYPE SQLITE, READ_ONLY)")
            attached = True
        except Exception:
            _logger.debug("_attach_sqlite: step failed (ignored)", exc_info=True)
    try:
        yield attached
    finally:
        if attached:
            try:
                con.execute(f"DETACH {alias}")
            except Exception:
                _logger.debug("_attach_sqlite: con.execute failed (ignored)", exc_info=True)


def ensure_ngwaf_bots_materialized(con: duckdb.DuckDBPyConnection, alias: str) -> bool:
    """Ensure ``{alias}.ngwaf_bots`` is queryable on ``con``, materialized
    from the NGWAF bot-cache SQLite file via plain ``sqlite3`` — NOT via
    DuckDB's own SQLite reader (``ATTACH ... TYPE SQLITE`` / ``sqlite_scan``).

    DuckDB's SQLite reader doesn't reliably coordinate with SQLite's own
    WAL/locking protocol; ATTACHing ``ngwaf_bot_cache.db`` directly while
    ``_run_ngwaf_bot_sync`` concurrently writes the same (globally shared)
    file corrupted it in production (2026-07-30) — ``PRAGMA integrity_check``
    failed and every subsequent read/write kept failing until the file was
    rebuilt from the NGWAF API. Reading the (small) cache into memory and
    handing DuckDB an in-memory table sidesteps the cross-engine hazard
    entirely.

    Idempotent per connection: reuses an already-materialized ``{alias}``
    catalog rather than re-reading the cache on every call — same "attach
    once per connection" cost profile the ATTACH-based code this replaces
    had. ``ngwaf_db_path()`` is a single fixed path for the whole
    deployment (never per-service), so there's no "path changed, must
    rebuild" case to handle. Returns ``True`` if ``{alias}.ngwaf_bots`` is
    queryable (empty table counts as queryable), ``False`` if the cache
    file doesn't exist or couldn't be read.

    Uses ``ATTACH ':memory:' AS {alias}`` (a real, distinctly-named
    in-memory DuckDB catalog) rather than ``CREATE SCHEMA {alias}`` in the
    connection's current catalog — a per-service ``con`` from
    ``get_connection()`` has its OWN catalog named after the service, and
    an unqualified ``CREATE SCHEMA {alias}`` there made ``{alias}.ngwaf_bots``
    references ambiguous once other catalogs were attached ("Ambiguous
    reference to catalog or schema", prod 2026-07-30). A separate catalog
    has no such collision.
    """
    existing = con.execute(
        "SELECT 1 FROM duckdb_tables() WHERE table_name = ? AND temporary = true",
        [f"{alias}_ngwaf_bots"],
    ).fetchone()
    if existing:
        return True

    from backend import config as svcconfig

    db_path = svcconfig.ngwaf_sqlite_cache_override()
    rows = []
    if db_path and os.path.exists(db_path):
        import sqlite3

        try:
            sconn = sqlite3.connect(db_path, timeout=5)
            try:
                cur = sconn.execute(
                    "SELECT waf_req_id, bot_name, category, wellknown_bot_name FROM ngwaf_bots WHERE bot_name IS NOT NULL"
                )
                rows = cur.fetchall()
            finally:
                sconn.close()
        except Exception as e:
            _logger.warning("[ngwaf_bots] sqlite cache read failed for %s: %s", alias, e)
            return False
    elif db_path:
        return False
    else:
        try:
            from backend.core.metadata import pg_connection

            pconn = pg_connection.get_pg_readonly_connection()
            try:
                cur = pconn.execute(
                    "SELECT waf_req_id, bot_name, category, wellknown_bot_name FROM ngwaf_bots WHERE bot_name IS NOT NULL"
                )
                rows = [
                    (r["waf_req_id"], r["bot_name"], r["category"], r["wellknown_bot_name"]) for r in cur.fetchall()
                ]
            finally:
                pconn.close()
        except Exception as e:
            _logger.warning("[ngwaf_bots] postgres cache read failed for %s: %s", alias, e)
            return False

    import pyarrow as pa

    tbl = pa.table(
        {
            "waf_req_id": pa.array([r[0] for r in rows], type=pa.string()),
            "bot_name": pa.array([r[1] for r in rows], type=pa.string()),
            "category": pa.array([r[2] for r in rows], type=pa.string()),
            "wellknown_bot_name": pa.array([r[3] for r in rows], type=pa.string()),
        }
    )
    try:
        con.register(f"temp_arrow_{alias}", tbl)
        con.execute(f"CREATE OR REPLACE TEMP TABLE {alias}_ngwaf_bots AS SELECT * FROM temp_arrow_{alias}")
    except Exception as e:
        _logger.warning("[ngwaf_bots] materializing %s_ngwaf_bots failed: %s", alias, e)
        return False
    return True


@contextlib.contextmanager
def attach_ngwaf_cache(con: duckdb.DuckDBPyConnection, schema_cols: list[str], alias: str = "ngwaf_cache"):
    """Materialize the NGWAF bot cache as ``{alias}.ngwaf_bots`` on ``con``
    when the schema warrants it. See :func:`ensure_ngwaf_bots_materialized`.
    """
    if "waf_req_id" not in schema_cols:
        yield False
        return
    yield ensure_ngwaf_bots_materialized(con, alias)


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

    return get_schema(con, src, stats=False)


from backend.utils.date_utils import (  # noqa: E402
    parse_hour_key_strict,
    safe_iso,  # noqa: F401 — re-export
)


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
        or "Cannot open file" in msg
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
    """Unified expression resolving ottfb (us) vs ttfb (s) vs nothing.

    Thin delegate over :func:`backend.core.metric_expressions.origin_latency_us`
    (the canonical catalog). Kept as a named export because origin.py and the
    base-branch tests import this symbol directly.
    """
    expr = metric_expressions.origin_latency_us(actual_cols, absent="NULL")
    # absent="NULL" guarantees a non-None result.
    if expr is None:
        raise RuntimeError("origin_latency_us returned None despite absent='NULL'")
    return expr


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
    observed = getattr(runner, "history", None)
    if not isinstance(observed, HistoryObservation):
        observed = HistoryObservation()
    runner.history = observed
    try:
        from backend import config as svcconfig

        cached = svcconfig.get_status(src["name"])
        if cached:
            observed.extent_source = "status_cache"
            observed.extent_rows = cached.get("local_rows")
            observed.earliest_event_at = cached.get("earliest_log_at")
            observed.latest_event_at = cached.get("latest_log_at")
            return (
                cached.get("local_rows", 0),
                cached.get("earliest_log_at"),
                cached.get("latest_log_at"),
            )
        row = runner.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {orig_table_name}").fetchone()
        if row:
            observed.extent_source = "live_query"
            observed.extent_rows = row[0]
            observed.earliest_event_at, observed.latest_event_at = safe_iso(row[1]), safe_iso(row[2])
            return row[0], safe_iso(row[1]), safe_iso(row[2])
    except Exception:
        try:
            count = runner.execute(f"SELECT count(*) FROM {orig_table_name}").fetchone()[0]
            observed.extent_source, observed.extent_rows = "count_only_query", count
            observed.earliest_event_at, observed.latest_event_at = None, None
            return count, None, None
        except Exception:
            _logger.debug("get_source_extent: step failed (ignored)", exc_info=True)
    observed.extent_source, observed.extent_rows = "unavailable", None
    observed.earliest_event_at, observed.latest_event_at = None, None
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
            _logger.debug("_latest_log_in_window: step failed (ignored)", exc_info=True)
    if end_time:
        try:
            et_dt = parse_iso_utc(end_time)
            if et_dt is not None and latest_dt > et_dt:
                return False
        except Exception:
            _logger.debug("_latest_log_in_window: step failed (ignored)", exc_info=True)
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

    def __init__(self, con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection], src: dict):
        self._con: duckdb.DuckDBPyConnection | None = None
        self._con_factory: Callable[[], duckdb.DuckDBPyConnection] | None = None
        self.con = con  # type: ignore[assignment]
        self.src = src
        self.debug_queries: list[dict] = []
        self.reader_coverage: list[ReaderCoverage] = []
        self.history: HistoryObservation | None = None
        self.actual_cols: set[str] = set()
        # Request-scoped sharing of the direct active-hour temps (see
        # begin_shared_active_hour_temps). None = sharing disabled: every
        # _create_active_hour_temp_direct caller owns + drops its own temp.
        self._shared_active_temps: list[tuple[frozenset[str], Any, Any, str]] | None = None
        self._shared_active_depth: int = 0
        self._shared_active_seed_fields: tuple[str, ...] = ()
        # File count of the most recent direct active-hour read — telemetry
        # only (surfaced as live_active_hour:n_files).
        self._last_active_direct_n_files: int = 0

        from backend.core.iceberg import inject_view_debug

        inject_view_debug(self.debug_queries, src)

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None and self._con_factory is not None:
            self._con = self._con_factory()
        return self._con  # type: ignore[return-value]

    @con.setter
    def con(self, value: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection]) -> None:
        if callable(value) and not hasattr(value, "execute"):
            self._con = None
            self._con_factory = value
        else:
            self._con = value  # type: ignore[assignment]
            self._con_factory = None

    def begin_shared_active_hour_temps(self, seed_fields: Iterable[str] | None = None) -> None:
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
        if self._shared_active_temps is None:
            self._shared_active_temps = []
            self._shared_active_depth = 1
            self._shared_active_seed_fields = tuple(seed_fields) if seed_fields else ()
        else:
            self._shared_active_depth += 1
            if seed_fields:
                existing = list(self._shared_active_seed_fields)
                for f in seed_fields:
                    if f not in existing:
                        existing.append(f)
                self._shared_active_seed_fields = tuple(existing)

    def end_shared_active_hour_temps(self) -> None:
        """Drop every shared active-hour temp and disable sharing.

        Safe to call unconditionally (no-op when sharing was never
        enabled) — callers put it in a ``finally`` so pooled connections
        never accumulate session temps across requests.
        """
        if self._shared_active_depth > 1:
            self._shared_active_depth -= 1
            return
        self._shared_active_depth = 0
        self._shared_active_seed_fields = ()
        temps, self._shared_active_temps = self._shared_active_temps, None
        for _cols, _ws, _we, name in temps or []:
            try:
                self.execute(f'DROP TABLE IF EXISTS "{name}"')
            except Exception:
                if log_once_per("end_shared_active_hour_temps: DROP TABLE failed (ignored)", 60):
                    _logger.debug("end_shared_active_hour_temps: DROP TABLE failed (ignored)", exc_info=True)

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
            if log_once_per("release_active_direct_temp: DROP TABLE failed (ignored)", 60):
                _logger.debug("release_active_direct_temp: DROP TABLE failed (ignored)", exc_info=True)

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
        elapsed_ms = round((time.time() - t0) * 1000, 2)
        compact_sql = _compact_sql_for_debug(q.strip())
        self.debug_queries.append(
            {
                "sql": compact_sql,
                "time_ms": elapsed_ms,
                "engine": "DuckDB",
            }
        )
        try:
            from backend.utils.telemetry import record_telemetry_query

            record_telemetry_query(
                "duckdb",
                compact_sql,
                elapsed_ms,
                service_id=self.src.get("name") or self.src.get("service_id"),
            )
        except Exception:
            _logger.debug("QueryRunner.execute: record_telemetry_query failed", exc_info=True)
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
        if actual_cols == ["timestamp"]:
            actual_cols = []
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
                _logger.debug("get_schema_cols: step failed (ignored)", exc_info=True)
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

    def _run_with_stale_retry(self, run):
        """Run ``run()`` with one stale-view refresh + retry.

        Returns ``(True, result)`` on success. A non-stale error on the first
        attempt re-raises; any failure once the view refresh has been attempted
        (refresh itself or the retried call) yields ``(False, None)``.
        """
        refreshed = False

        def _refresh(_exc: BaseException) -> None:
            nonlocal refreshed
            refreshed = True
            from backend.core import iceberg as db_iceberg

            db_iceberg.update_iceberg_view(self.con, self.src)

        try:
            return True, retry_call(run, retry_on=_is_stale_view_error, attempts=2, before_retry=_refresh)
        except Exception:
            if not refreshed:
                raise
            return False, None

    def execute_with_retry(self, sql: str, params: list | None = None):
        """Execute a query with one stale-view retry.

        Returns the cursor on success, or None on permanent failure after retry.
        Non-stale errors are re-raised immediately.
        """
        _, cursor = self._run_with_stale_retry(lambda: self.execute(sql, params))
        return cursor

    def create_temp_table(self, sql: str, params: list | None = None) -> bool:
        """Execute a CREATE TEMP TABLE statement with one stale-view retry.

        A background commit job may delete a buffer file between the time our
        connection was opened and when we run the query. When that happens,
        re-initialising the Iceberg view and retrying once is sufficient to
        pick up the new snapshot.

        Returns True on success, False on permanent failure after retry.
        Non-stale errors are re-raised immediately.
        """
        ok, _ = self._run_with_stale_retry(lambda: self.execute(sql, params))
        return ok

    def telemetry(self) -> dict:
        """Return the standard debug telemetry dict for response construction."""
        result: dict[str, Any] = {"debug_queries": self.debug_queries, "debug_calls": self.debug_calls}
        if self.reader_coverage:
            result["reader_coverage"] = [observation.model_dump() for observation in self.reader_coverage]
        if self.history is not None:
            result["history"] = self.history.model_dump()
        return result

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
        import uuid as _uuid
        from datetime import timedelta as _timedelta

        from backend import config as svcconfig
        from backend.core.duckdb import _cache_dir

        try:
            cache_dir = _cache_dir(self.src)
        except Exception:
            return None

        buffer_dir = os.path.join(cache_dir, "buffer")
        active_hour_token = live_start.strftime("%Y-%m-%d-%H")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour_token}")

        # Columns the caller requested that actually exist in the schema.
        requested: list[str] = ["timestamp"]
        for f in fields:
            if f in actual_cols and f not in requested:
                requested.append(f)
        requested_set = frozenset(requested)

        # Shared-scope reuse (see begin_shared_active_hour_temps): an
        # earlier temp over the SAME window whose projection covers this
        # caller's columns serves as-is — zero file opens.
        if self._shared_active_temps is not None:
            for cols_have, ws, we, name in self._shared_active_temps:
                if ws == live_start and we == live_end and requested_set <= cols_have:
                    self._last_active_direct_n_files = 0
                    return name
            for cols_have, ws, we, name in self._shared_active_temps:
                if ws <= live_start and we >= live_end and requested_set <= cols_have:
                    sub_name = f"t_active_direct_{_uuid.uuid4().hex}"
                    sub_where = (
                        f"timestamp >= TIMESTAMPTZ '{live_start.isoformat()}' "
                        f"AND timestamp < TIMESTAMPTZ '{live_end.isoformat()}'"
                    )
                    try:
                        self.con.execute(f'CREATE TEMP TABLE {sub_name} AS SELECT * FROM "{name}" WHERE {sub_where}')
                        self._shared_active_temps.append((cols_have, live_start, live_end, sub_name))
                        self._last_active_direct_n_files = 0
                        return sub_name
                    except Exception:
                        _logger.debug("QueryRunner: shared active temp slice failed", exc_info=True)

        projected: list[str] = list(requested)
        if self._shared_active_temps is not None and self._shared_active_seed_fields:
            for f in self._shared_active_seed_fields:
                if f in actual_cols and f not in projected:
                    projected.append(f)
        projected_set = frozenset(projected)

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
        mtime_floor = (live_start - _timedelta(seconds=_MTIME_SKEW_MARGIN_S)).timestamp()

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

        durable_serving = svcconfig.is_durable_serving_mode(self.src)
        buffer_files = [] if durable_serving else _list_parquets(buffer_dir, prune_mtime=True)
        # Committed rows for the active hour live in the per-service
        # DuckLake table (post-v3) — without this branch up to ~55 min of
        # the live hour vanished from dashboards once a commit drained the
        # buffer. DuckLake prunes the scan on file stats, so the timestamp
        # WHERE below keeps this cheap. Tombstoned buffer parquets stay
        # excluded above: their rows are ALREADY in the lake table and
        # reading both double-counts (Trap #26).
        lake_table = None
        try:
            from backend.core.iceberg._ducklake import ducklake_table_name

            cand = ducklake_table_name(self.src)
            row = self.con.execute(
                "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
                [cand],
            ).fetchone()
            if row:
                lake_table = cand
        except Exception:
            lake_table = None
        # Hourly-partition files are already scoped to the active hour —
        # no mtime pruning (compaction may rewrite them with fresh mtimes
        # anyway), and tombstones only ever mark buffer files. Legacy
        # fallback for pre-DuckLake caches: skipped when the lake table
        # exists, since migrated hourly files may be registered there too
        # (reading both would double-count).
        hourly_files = [] if durable_serving or lake_table else _list_parquets(hourly_dir, prune_mtime=False)
        self._last_active_direct_n_files = len(buffer_files) + len(hourly_files)
        if not buffer_files and not hourly_files and not lake_table:
            # Nothing on disk for the active hour. Caller will report
            # empty live_res — semantically correct (no current-hour rows).
            return "__empty__"

        # Escape internal double quotes (audit finding 004).
        cols_sql = ", ".join('"{}"'.format(c.replace('"', '""')) for c in projected)
        where = (
            f"timestamp >= TIMESTAMPTZ '{live_start.isoformat()}' AND timestamp < TIMESTAMPTZ '{live_end.isoformat()}'"
        )

        def _create_sql(union_by_name: str) -> str:
            branches: list[str] = []

            # To get derived fields (c_speed mapped, timestamp_hour, etc.) correctly
            # we must apply the same view derivations to the active-hour raw rows.
            # We select * from the raw sources, union them, wrap with _finalize_view_sql,
            # and finally project just the columns we need.

            if buffer_files:
                paths_sql = quote_path_list(buffer_files)
                branches.append(
                    f"SELECT * FROM read_parquet([{paths_sql}], union_by_name={union_by_name}) WHERE {where}"
                )
            if lake_table:
                lake_ident = 'lake."{}"'.format(lake_table.replace('"', '""'))
                branches.append(f"SELECT * FROM {lake_ident} WHERE {where}")
            if hourly_files:
                paths_sql = quote_path_list(hourly_files)
                branches.append(
                    f"SELECT * FROM read_parquet([{paths_sql}], union_by_name={union_by_name}) WHERE {where}"
                )

            joiner = " UNION ALL BY NAME " if union_by_name == "true" else " UNION ALL "
            # We must use BY NAME across the branches (lake_table vs buffer_files) because
            # their physical column layouts can differ (e.g. lake catalog columns vs raw).
            union_sql = " UNION ALL BY NAME ".join(branches)

            # Probe physical columns present in union_sql so that EXCLUDE clauses
            # in _finalize_view_sql and projection lists only reference columns that
            # actually exist in the underlying raw data (avoiding BinderExceptions on
            # computed/view-only columns like timestamp_hour or dt).
            desc = self.con.execute(f"SELECT * FROM ({union_sql}) LIMIT 0").description or []
            physical_cols = {d[0] for d in desc}

            from backend.core.iceberg.view import _finalize_view_sql

            target_table = "logs"  # QueryRunner active hour direct is only used for logs path right now
            # For exact correctness, we could pass the real dynamic schema fields, but custom fields
            # are real columns in the parquet anyway, so passing an empty set is fine for the EXCLUDE logic.
            finalized = _finalize_view_sql(
                union_sql,
                self.src,
                target_table=target_table,
                dynamic_schema_field_names=set(),
                existing_cols=physical_cols,
            )

            available_cols = physical_cols | {"timestamp_hour", "dt"}
            valid_projected = [c for c in projected if c in available_cols]
            valid_cols_sql = (
                ", ".join('"{}"'.format(c.replace('"', '""')) for c in valid_projected) if valid_projected else "*"
            )

            return f"SELECT {valid_cols_sql} FROM ({finalized})"

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
        except duckdb.InterruptException:
            raise
        except Exception:
            try:
                self.con.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
            except Exception:
                if log_once_per("_create_active_hour_temp_direct: DROP TABLE failed (ignored)", 60):
                    _logger.debug("_create_active_hour_temp_direct: DROP TABLE failed (ignored)", exc_info=True)
            try:
                self.con.execute(f"CREATE TEMP TABLE {temp_name} AS " + _create_sql("true"))
            except duckdb.InterruptException:
                raise
            except Exception:
                # Missing column, unreadable file, etc. Caller falls back.
                try:
                    self.con.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
                except Exception:
                    if log_once_per("_create_active_hour_temp_direct: DROP TABLE failed (ignored)", 60):
                        _logger.debug("_create_active_hour_temp_direct: DROP TABLE failed (ignored)", exc_info=True)
                return None
        if self._shared_active_temps is not None:
            self._shared_active_temps.append((projected_set, live_start, live_end, temp_name))
        return temp_name

    def _partial_hour_adjusted_live_start(self, naive_live_start, active_hour_token: str, window_end):
        """Returns ``(adjusted_live_start, partial_rows, partial_total)`` for
        the active-hour live-scan callers: if a partial-hour rollup exists
        for ``active_hour_token`` AND the caller's own window
        (``naive_live_start``..``window_end``) covers it exactly, narrow the
        live scan to start at its watermark instead of ``naive_live_start``
        and hand back its pre-aggregated
        ``(field, value, count)`` rows plus the real request total (read
        from the writer's synthetic ``__total__`` row — never derive a
        total by summing ``partial_rows``, see C1 in the final whole-branch
        review: that counts every request once PER POPULATED FIELD). If no
        partial rollup exists yet, or the caller's window doesn't cover the
        partial rollup's span, returns the unmodified ``naive_live_start``,
        an empty row list, and a zero total — today's (pre-partial-hour)
        behavior.

        I1 (final whole-branch review): the partial rollup always
        represents the WHOLE ``[hour_start, watermark)`` span — it has no
        notion of a caller's own sub-hour window. Using it is only safe
        when the caller's actual window starts at/before the active hour's
        start AND ends at/after the watermark; otherwise the caller falls
        back to its live-scan-only path (already correct for a custom
        range that doesn't align with the partial rollup's span).

        I2 (final whole-branch review) — investigated, NOT fixed by
        subtracting a margin from the narrowing boundary; see the long
        comment below for why that literal reading of the finding is
        unsafe for this writer, and what's done instead.
        """
        from datetime import UTC, datetime

        from backend.core.rollups.partial_hour import (
            read_partial_hour_all_fields,
            read_partial_hour_total,
            read_partial_hour_watermark,
        )

        watermark = read_partial_hour_watermark(self.src, active_hour_token)
        if watermark <= 0.0:
            return naive_live_start, [], 0

        watermark_dt = datetime.fromtimestamp(watermark, tz=UTC)
        active_hour_dt = parse_hour_key_strict(active_hour_token)

        if naive_live_start > active_hour_dt or window_end < watermark_dt:
            return naive_live_start, [], 0

        # I2 investigation (final whole-branch review): the finding's
        # literal suggestion — narrow to `watermark - skew margin` instead
        # of `watermark` exactly, so the live scan re-covers a "small
        # trailing slice" the partial rollup nominally already includes —
        # is UNSAFE for this writer and was deliberately NOT implemented.
        # merge_partial_hour aggregates a processed file's ENTIRE row set
        # via COUNT(*)/GROUP BY, not gated by each row's event timestamp
        # relative to the file's own mtime; a file can legitimately contain
        # rows spanning several minutes up to its close time (these are
        # ingest-pipeline batch files, not one-row-per-flush). Subtracting
        # even a few minutes from the narrowing boundary would re-scan (and
        # double-count) real rows from whichever file(s) set the current
        # watermark, essentially every time a partial rollup exists — not
        # a rare edge case, since the writer processes a new batch on most
        # 30s ticks. The invariant that DOES hold, and that this function
        # already relies on unchanged from before this review: no row in
        # any PROCESSED file can have an event timestamp after that file's
        # own mtime, so narrowing to the raw (unmodified) watermark_dt can
        # never double-count an already-processed row. The actual gap I2
        # describes — a delivery-lagged row that lands in a file whose
        # mtime is fresh enough to be "new" next tick but isn't processed
        # yet THIS tick — is real but already self-heals within one tick
        # (<=30s at this cadence): the next merge_partial_hour call picks
        # up that file (via COUNT(*), regardless of its rows' individual
        # timestamps) and folds it into both the persisted total and the
        # advanced watermark atomically. That bound matches the feature's
        # own designed latency floor and needs no additional narrowing
        # margin to be correct.
        try:
            partial_rows = read_partial_hour_all_fields(self.src, active_hour_token, con=self.con)
            partial_total = read_partial_hour_total(self.src, active_hour_token, con=self.con)
        except Exception as e:
            _logger.warning(
                "[partial_hour] Failed reading partial hour for %s: %s; falling back to full live scan",
                active_hour_token,
                e,
            )
            return naive_live_start, [], 0

        if not partial_rows and partial_total == 0:
            return naive_live_start, [], 0

        adjusted = max(naive_live_start, watermark_dt)
        return adjusted, partial_rows, partial_total

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
                    if log_once_per("temp_table: DROP TABLE failed (ignored)", 60):
                        _logger.debug("temp_table: DROP TABLE failed (ignored)", exc_info=True)

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
        _coverage_reader: Literal["dashboard.top_n", "dashboard.ua_bots"] = "dashboard.top_n",
    ) -> tuple[list[tuple[str, Any, int]], list[str]]:
        """Compute per-field top-N from rollup parquets + the live active
        hour from the base table. Returns merged (field, value, count)
        tuples truncated to ``per_field_limits.get(field, limit)`` per field.

        per_field_limits lets a caller request a wider top-N for specific
        fields without bloating the others — e.g. ``{"country": 500}`` to
        get up to 500 countries for a choropleth while keeping other
        panels at the default top-10. Both the rollup-parquet branch and
        the live-active-hour / missing-hour-heal branches apply each
        field's own limit to its own QUALIFY cutoff (a per-field CASE in
        the rolled-parquet SQL; a per-field branch limit in
        ``execute_top_n_batch``) — NOT ``max(all_limits)`` applied
        uniformly to every field. The collapsed-max behavior was a real
        bug (fixed 2026-08-03): a single ``{"country": 500}`` override
        was making every other field's QUALIFY <= 500 too, since the SQL
        only had one scalar limit to work with.

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
        from datetime import UTC, datetime

        from backend.core.duckdb import _cache_dir
        from backend.core.rollups import _is_safe_ident, _safe_table_for
        from backend.utils.date_utils import parse_iso_utc

        observed = ReaderCoverage(
            reader=_coverage_reader, maximum_window_days=None, eligible=False, outcome="ineligible"
        )
        self.reader_coverage.append(observed)
        if durable_serving_blocked(self.src):
            observed.eligibility_reason = "durable_unready"
            # The result shape is retained for existing callers, but an
            # empty tuple is explicitly an unavailable rollup result.  Each
            # serving path must then run its DuckLake-backed raw fallback.
            return [], fields

        # Optional phase-log instrumentation. Caller passes a list; we
        # append {"section": "top_n_rollups:<phase>", "time_ms": N} per
        # phase. None = no-op. Negligible overhead.
        def _phase(name: str, ms: float) -> None:
            if _phase_log is not None:
                _phase_log.append({"section": f"top_n_rollups:{name}", "time_ms": round(ms, 2)})

        cache_dir = _cache_dir(self.src)

        # Defense-in-depth: field names land in a SQL IN-list as quoted
        # literals AND the service name lands in the base-table identifier.
        # Both should already be safe (FIELDS + validate_custom_field
        # constrain custom names; service IDs are Fastly-format slugs), but
        # we re-validate here so a future caller can't pierce the boundary.
        safe_fields = [f for f in fields if _is_safe_ident(f)]
        if not safe_fields:
            observed.eligibility_reason = "unsafe_fields"
            return [], fields
        base_table = _safe_table_for(self.src)
        if not base_table:
            observed.eligibility_reason = "unsafe_service"
            # Service name failed the identifier safelist; refuse to query.
            return [], fields

        # Parse bounds
        st_dt = parse_iso_utc(start_time) if start_time else None
        et_dt = parse_iso_utc(end_time) if end_time else None

        observed.window_start = st_dt.isoformat() if st_dt else None
        observed.window_end = et_dt.isoformat() if et_dt else None
        observed.eligible, observed.eligibility_reason = True, "eligible"
        observed.outcome = "unknown"
        observed.top_n_plan = TopNRollupCoverage()
        win = _TopNWindow.build(st_dt, et_dt, datetime.now(UTC))
        active_dt = win.active_dt
        _t_dir_enum = time.perf_counter()
        plan = _plan_top_n_paths(cache_dir, safe_fields, win, observation=observed.top_n_plan)
        day_paths = plan.paths.day_paths
        hour_paths = plan.paths.hour_paths
        bundled_hour_paths = plan.paths.bundled_hour_paths
        bundled_day_paths = plan.paths.bundled_day_paths

        _phase("dir_enum", (time.perf_counter() - _t_dir_enum) * 1000)
        _phase("dir_enum:n_day_files", float(len(day_paths)))
        _phase("dir_enum:n_hour_files", float(len(hour_paths)))
        _phase("dir_enum:n_bundled_hour_files", float(len(bundled_hour_paths)))
        _phase("dir_enum:n_bundled_day_files", float(len(bundled_day_paths)))

        _t_rolled = time.perf_counter()
        rolled_q = _build_rolled_sql(plan.paths, safe_fields, limit, per_field_limits)
        rolled_res: list = []
        if rolled_q is not None:
            try:
                rolled_res = self.execute(rolled_q).fetchall()
                observed.top_n_plan.rolled_read = "used"
            except duckdb.InterruptException:
                raise
            except Exception:
                observed.top_n_plan.rolled_read = "failed"
                rolled_res = []
        _phase("rolled_res", (time.perf_counter() - _t_rolled) * 1000)

        # Callers (dashboard repo) already computed the schema once for the
        # request; the kwargs let them seed both so the live and heal steps
        # skip the duplicate get_schema_cols() / _get_schema() round-trip.
        schema = _SchemaSeeds(actual_cols, schema_types)

        # Live active hour: the rollup tree never holds it.
        _t_live = time.perf_counter()
        live_res, partial_rows = self._top_n_live_active_hour(
            win, safe_fields, base_table, limit, per_field_limits, schema, _phase, observation=observed
        )
        _phase("live_active_hour", (time.perf_counter() - _t_live) * 1000)

        # Missing-hour live heal: closed hours of not-yet-day-compacted days
        # with no rollup coverage are live-queried so the returned top-N stays
        # window-correct through rollup-writer gaps. Diagnosed 2026-07-06: the
        # per-sync recompute only fires when a sync batch ingests rows stamped
        # in an already-closed hour, so on bursty services every closed hour of
        # the current day was silently absent until the nightly deep pass —
        # total_rows (a live COUNT) disagreed with every card's field_total by
        # the same factor. Steady-state cost is ZERO: the hourly rollup_heal
        # cron keeps the bundle tree complete (real bundles for hours with
        # data, empty SENTINEL bundles for verified-zero-row hours), so
        # `missing_hours` is empty. The scan only fires while the writer is
        # behind, bounded by _MISSING_HOUR_HEAL_CAP hours.
        _t_heal = time.perf_counter()
        missing_hours = _find_unrolled_hours(
            plan, win, safe_fields, _MISSING_HOUR_HEAL_CAP, observation=observed.top_n_plan
        )
        heal_res = (
            self._top_n_heal_hours(
                missing_hours, win, safe_fields, base_table, limit, per_field_limits, schema, observation=observed
            )
            if missing_hours
            else []
        )
        _phase("live_missing_hours", (time.perf_counter() - _t_heal) * 1000)
        _phase("live_missing_hours:n_hours", float(len(missing_hours)))

        # Combine rolled and live, bucketed by field (O(rows), see fold_rows).
        _t_merge = time.perf_counter()
        by_field = _fold_rows(rolled_res, live_res, heal_res, partial_rows)

        # Per-field limits override the global default for specific fields
        # (e.g. country at 500 for choropleth).
        top_results, empty_fields = _truncate_top_n(by_field, fields, limit, per_field_limits)
        _service_key = self.src.get("name") or self.src.get("service_id") or "default"
        for field in empty_fields:
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
            if field in _LIVE_TOPN_SKIP_FIELDS:
                continue
            _ts_key = (_service_key, field)
            _now = time.monotonic()
            _last = _EMPTY_ROLLUP_WARN_TS.get(_ts_key)
            if _last is None or _now - _last >= _EMPTY_ROLLUP_WARN_INTERVAL_S:
                _EMPTY_ROLLUP_WARN_TS[_ts_key] = _now
                _logger.debug(
                    "[top_n_rollups] empty result for field=%r (service=%r, window=[%s,%s]) — "
                    "panel will render empty; check rollup backfill coverage if unexpected",
                    field,
                    _service_key,
                    start_time,
                    end_time,
                )
        _phase("merge_sort", (time.perf_counter() - _t_merge) * 1000)

        observed.outcome = "served"
        return top_results, fields

    def _top_n_live_active_hour(
        self,
        win: _TopNWindow,
        safe_fields: list[str],
        base_table: str,
        limit: int,
        per_field_limits: dict[str, int] | None,
        schema: _SchemaSeeds,
        _phase: Callable[[str, float], None],
        *,
        observation: ReaderCoverage | None = None,
    ) -> tuple[list[tuple], list[tuple]]:
        """Top-N rows for the active hour, from the base table: ``(live_rows, partial_rows)``.

        ``partial_rows`` are the already-materialised part of the hour (see
        ``_partial_hour_adjusted_live_start``). Empty when the request window
        does not overlap the active hour. Best-effort: any failure is logged
        and yields whatever was gathered, never an exception.
        """
        st_dt, et_dt, active_dt = win.st_dt, win.et_dt, win.active_dt
        live_res: list[tuple] = []
        partial_rows: list[tuple] = []
        if observation is not None:
            observation.live_top_up = "outside_window"

        # Clamp the live window to the intersection of (active hour) and
        # (requested window). Without this, a partial-hour request like
        # [14:30, 15:30] where active_dt=15:00 would query the FULL active
        # hour [15:00, 16:00) — over-counting rows from [15:30, 16:00) that
        # fall outside the user's window.
        live_start = max(active_dt, st_dt) if st_dt else active_dt
        live_end = min(win.active_dt_end, et_dt) if et_dt else win.active_dt_end
        # We only query the active hour if it overlaps with the requested time window
        if et_dt and et_dt <= active_dt:
            return live_res, partial_rows
        if st_dt and st_dt >= win.active_dt_end:
            return live_res, partial_rows

        try:
            schema.ensure(self.get_schema_cols, lambda: {c["name"]: c["type"] for c in _get_schema(self.con, self.src)})
            actual_cols, schema_types = schema.cols, schema.types

            # To prevent creating a massive UNION, create a temp table for just
            # the live hour. execute_top_n_batch applies per_field_limits per its
            # own per-field branch, so each field fetches only what ITS
            # truncation needs. Fast path: read buffer + active hourly partition
            # directly, skipping the iceberg view (~700ms saved per request on
            # the 2026-06-08 baseline); falls back to the view-based path if the
            # direct read fails. safe_fields (not raw input) feeds the temp so a
            # hostile field name can never reach the SQL builder (audit 004).
            live_topn_fields = _live_topn_fields(safe_fields, _LIVE_TOPN_SKIP_FIELDS, self.src.get("log_fields"))
            live_start, partial_rows, _partial_total = self._partial_hour_adjusted_live_start(
                live_start, win.active_str, live_end
            )
            # Built from the narrowed live_start so the create_filtered_temp_table
            # fallback scans only [narrowed live_start, live_end). Otherwise it
            # re-scans [hour_start, watermark) — the range partial_rows already
            # covers — and both get summed into the same bucket, double-counting.
            live_where = f"timestamp >= '{live_start.isoformat()}' AND timestamp < '{live_end.isoformat()}'"
            if observation is not None:
                observation.live_start, observation.live_end = live_start.isoformat(), live_end.isoformat()
                observation.live_top_up = "unavailable"
            _t_lt = time.perf_counter()
            temp_proj_fields = (
                live_topn_fields + ["waf_req_id"]
                if "waf_req_id" in actual_cols and "waf_req_id" not in live_topn_fields
                else live_topn_fields
            )
            tmp_name = self._create_active_hour_temp_direct(temp_proj_fields, actual_cols, live_start, live_end)
            if tmp_name == "__empty__":
                tmp_name = None
                if observation is not None:
                    observation.live_top_up = "empty"
            elif tmp_name is None:
                tmp_name = self.create_filtered_temp_table(live_topn_fields, actual_cols, base_table, live_where)
                if observation is not None and tmp_name:
                    observation.live_top_up = "view_fallback"
            elif observation is not None:
                observation.live_top_up = "unverified"
            _phase("live_active_hour:temp_create", (time.perf_counter() - _t_lt) * 1000)
            _phase("live_active_hour:n_files", float(self._last_active_direct_n_files))
            if tmp_name:
                try:
                    # Diagnostic row count (in-memory temp, ~free): tells the perf
                    # harness whether a slow live merge is data volume or
                    # per-field batch cost.
                    _n_live = 0
                    try:
                        _n_live = self.execute(f'SELECT COUNT(*) FROM "{tmp_name}"').fetchone()[0]
                        _phase("live_active_hour:n_rows", float(_n_live))
                    except Exception:
                        _n_live = 1
                    # Only columns present in the temp's projection: virtual
                    # fields (waf_sig_ind, edge_score_reason_ind) have rollup
                    # parquets but no live column and would BinderException the
                    # whole UNION ALL. A zero-row hour skips the 50+ grouping
                    # branches (~130ms of planning).
                    if observation is not None and _n_live == 0:
                        observation.live_top_up = "empty"
                    if _n_live > 0:
                        live_fields = [f for f in live_topn_fields if f in actual_cols]
                        if observation is not None and not live_fields:
                            observation.live_top_up = "missing_column"
                        if live_fields:
                            _t_lb = time.perf_counter()
                            live_res, _ = self.execute_top_n_batch(
                                live_fields,
                                tmp_name,
                                actual_cols,
                                schema_types,
                                limit=limit,
                                per_field_limits=per_field_limits,
                            )
                            if observation is not None and observation.live_top_up == "unverified":
                                observation.live_top_up = "used"
                            _phase("live_active_hour:batch", (time.perf_counter() - _t_lb) * 1000)
                finally:
                    # Deferred to scope-exit when the shared active-hour scope
                    # owns this temp (dashboard rollup path).
                    self.release_active_direct_temp(tmp_name)
        except duckdb.InterruptException:
            raise
        except Exception:
            if observation is not None:
                observation.live_top_up = "failed"
            _logger.exception("[top_n_rollups] live active hour query failed")
        return live_res, partial_rows

    def _top_n_heal_hours(
        self,
        missing_hours: list[str],
        win: _TopNWindow,
        safe_fields: list[str],
        base_table: str,
        limit: int,
        per_field_limits: dict[str, int] | None,
        schema: _SchemaSeeds,
        *,
        observation: ReaderCoverage | None = None,
    ) -> list[tuple]:
        """Live top-N rows for closed hours the rollup writer has not covered.

        Best-effort: a failed heal leaves the pre-fallback behavior (rollups +
        active hour only), never a 500.
        """
        heal_res: list[tuple] = []
        if observation is not None:
            observation.closed_hour_heal = "unavailable"
        _now_mono = time.monotonic()
        _svc_key = self.src.get("name") or self.src.get("service_id") or "default"
        _last_heal_warn = _MISSING_HOUR_HEAL_WARN_TS.get(_svc_key)
        if _last_heal_warn is None or _now_mono - _last_heal_warn >= _EMPTY_ROLLUP_WARN_INTERVAL_S:
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
            schema.ensure(self.get_schema_cols, lambda: {c["name"]: c["type"] for c in _get_schema(self.con, self.src)})
            actual_cols, schema_types = schema.cols, schema.types
            heal_topn_fields = _live_topn_fields(safe_fields, _LIVE_TOPN_SKIP_FIELDS, self.src.get("log_fields"))
            heal_where = _heal_where_clause(missing_hours, win.st_dt, win.et_dt)
            heal_tmp = self.create_filtered_temp_table(heal_topn_fields, actual_cols, base_table, heal_where)
            if heal_tmp:
                try:
                    heal_fields = [f for f in heal_topn_fields if f in actual_cols]
                    if heal_fields:
                        heal_res, _ = self.execute_top_n_batch(
                            heal_fields,
                            heal_tmp,
                            actual_cols,
                            schema_types,
                            limit=limit,
                            per_field_limits=per_field_limits,
                        )
                        if observation is not None:
                            observation.closed_hour_heal = "used"
                finally:
                    try:
                        self.execute(f'DROP TABLE IF EXISTS "{heal_tmp}"')
                    except Exception:
                        if log_once_per("execute_top_n_rollups: DROP TABLE failed (ignored)", 60):
                            _logger.debug("execute_top_n_rollups: DROP TABLE failed (ignored)", exc_info=True)
        except duckdb.InterruptException:
            raise
        except Exception:
            if observation is not None:
                observation.closed_hour_heal = "failed"
            _logger.debug("[top_n_rollups] missing-hour live heal failed", exc_info=True)
        return heal_res

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
        from datetime import UTC, timedelta

        from backend.utils.date_utils import parse_iso_utc

        observed = ReaderCoverage(reader="dashboard.time_series", eligible=False, outcome="ineligible")
        self.reader_coverage.append(observed)
        if chart_metric not in self._TS_ROLLUP_METRIC_PARTS:
            observed.eligibility_reason = "unsupported_metric"
            return None
        if interval not in self._TS_ROLLUP_INTERVALS:
            observed.eligibility_reason = "unsupported_interval"
            return None
        if not start_time or not end_time:
            observed.eligibility_reason = "bounds_missing"
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
            observed.eligibility_reason = "invalid_window"
            return None
        if et <= st:
            observed.eligibility_reason = "invalid_window"
            return None
        observed.window_start, observed.window_end = st.isoformat(), et.isoformat()
        if (et - st) > timedelta(days=366):
            observed.eligibility_reason = "window_too_long"
            return None
        observed.eligible, observed.eligibility_reason = True, "eligible"
        observed.scalar_plan = ScalarRollupCoverage()

        # st is UTC (parse_iso_utc guarantees it). For multi-week windows
        # (7d–90d), prefer closed-day bundles (`day_bundled/day=D/time_series.parquet`,
        # or `day_bundled/day=D/overview.parquet` when `chart_metric == "requests"`
        # and `interval in ("1 hour", "1 day")`) for the contiguous closed-day prefix
        # before falling back to hourly bundles for recent days. This prevents
        # 30d windows from disqualifying when >7d hourly `time_series.parquet` files
        # are retired, and cuts 30d parquet file reads from ~720 hourly files to ~30.
        from backend.repositories._rollup_planner import plan_time_series_rollup_window

        wplan = plan_time_series_rollup_window(
            self.src,
            st,
            et,
            can_use_overview=chart_metric == "requests" and interval in ("1 hour", "1 day"),
            coverage_mod=rollup_coverage,
            plan_hourly_read_fn=plan_hourly_read,
            observation=observed.scalar_plan,
        )
        if wplan is None:
            observed.outcome = "no_coverage"
            return None
        observed.outcome = "unknown"
        observed.live_top_up = "outside_window"
        rollup_paths = wplan.rollup_paths
        day_ov_paths = wplan.day_ov_paths
        crosses_active = wplan.crosses_active
        active_hour_str, active_hour_dt = wplan.active_hour_str, wplan.active_hour_dt
        missing_hours = wplan.missing_hours
        heal_where_sql = wplan.heal_where_sql

        parts = self._TS_ROLLUP_METRIC_PARTS[chart_metric]
        # Bucket is TIMESTAMPTZ in the bundle parquets (older notes about
        # "naive TIMESTAMP" referred to a since-removed schema). Use
        # TIMESTAMPTZ literals so the comparison is unambiguous regardless
        # of DuckDB's session timezone — without the explicit offset, a
        # session tz like CDT silently shifts the filter by 5 hours and
        # drops bundles at the window's edges.
        st_tz = st.astimezone(UTC).isoformat()
        st_hour_tz = st.replace(minute=0, second=0, microsecond=0).astimezone(UTC).isoformat()
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
        if day_ov_paths:
            ov_paths_sql = quote_path_list(day_ov_paths)
            hour_ts_expr = (
                "TRY_CAST(CASE WHEN LENGTH(CAST(hour_start AS VARCHAR)) = 13 "
                "THEN CAST(hour_start AS VARCHAR) || ':00:00Z' "
                "ELSE CAST(hour_start AS VARCHAR) END AS TIMESTAMPTZ)"
            )
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', {hour_ts_expr}) AS out_bucket, "
                f"       {parts['num_rollup']} AS num, {parts['den_rollup']} AS den "
                f"FROM read_parquet([{ov_paths_sql}]) "
                f"WHERE {hour_ts_expr} >= TIMESTAMPTZ '{st_hour_tz}' "
                f"  AND {hour_ts_expr} < TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY 1"
            )

        # Missing-hour live heal: a CLOSED hour in [st, et) with no
        # time_series bundle at all is a writer-coverage gap, not proof of
        # zero traffic, so the plan lists it for a live scan of the raw base
        # table. Uses a literal, unparameterized where-clause — sidesteps
        # bound-parameter bookkeeping across UNION ALL branches.
        if missing_hours:
            from backend.core.rollups import _safe_table_for

            heal_base_table = _safe_table_for(self.src)
            observed.closed_hour_heal = "planned" if heal_base_table else "unavailable"
            if heal_base_table:
                select_clauses.append(
                    f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS out_bucket, "
                    f"       {parts['num_live']} AS num, {parts['den_live']} AS den "
                    f"FROM {heal_base_table} "
                    f"WHERE {heal_where_sql} "
                    f"GROUP BY 1"
                )

        direct_live_tmp: str | None = None
        live_needs_params = False
        if crosses_active:
            # Live SQL for the [max(st, active_hour_start), et) slice, using
            # the same metric-derivation logic as the rollup branch so the
            # buckets align exactly.
            live_start = max(st, active_hour_dt)
            # Only "requests" has a compensating partial-rollup merge below
            # (the other metrics' num/den semantics differ per-metric and
            # aren't derivable from partial_rows' plain (field, value, count)
            # shape). Narrowing live_start for those metrics without a
            # compensating clause would silently drop the
            # [original_live_start, watermark) range from BOTH branches —
            # the rollup branch never covers it (still-open hour) and the
            # narrowed live branch would skip it too. So non-"requests"
            # metrics keep scanning their full original live range,
            # unchanged from before partial-hour support existed.
            live_end = et
            if chart_metric == "requests":
                live_start, _partial_rows, partial_total = self._partial_hour_adjusted_live_start(
                    live_start, active_hour_str, live_end
                )
            else:
                partial_total = 0
            live_st_tz = live_start.astimezone(UTC).isoformat()
            live_et_tz = live_end.astimezone(UTC).isoformat()
            observed.live_start, observed.live_end = live_st_tz, live_et_tz

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
            if direct_live_tmp == "__empty__":
                live_source = None
            elif direct_live_tmp is not None:
                live_source, live_where = direct_live_tmp, "1=1"
            else:
                live_source, live_where = table_name, where_clause
                live_needs_params = True

            observed.live_top_up = "empty" if live_source is None else "unverified"
            if live_source is not None:
                select_clauses.append(
                    f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS out_bucket, "
                    f"       {parts['num_live']} AS num, {parts['den_live']} AS den "
                    f"FROM {live_source} "
                    f"WHERE {live_where} "
                    f"  AND timestamp >= TIMESTAMPTZ '{live_st_tz}' "
                    f"  AND timestamp <  TIMESTAMPTZ '{live_et_tz}' "
                    f"GROUP BY 1"
                )

            if partial_total:
                # partial_total is only ever non-zero when chart_metric ==
                # "requests" (see the narrowing gate above) — every other
                # metric's num/den semantics aren't derivable from
                # partial_rows' plain (field, value, count) shape. Read
                # from the writer's synthetic __total__ row (C1, final
                # whole-branch review) — summing partial_rows itself would
                # count each request once per populated field.
                partial_bucket_tz = active_hour_dt.astimezone(UTC).isoformat()
                select_clauses.append(
                    f"SELECT TIMESTAMPTZ '{partial_bucket_tz}' AS out_bucket, "
                    f"       {partial_total} AS num, {partial_total} AS den"
                )

        if not select_clauses:
            observed.outcome = "served"
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

        if not select_clauses:
            observed.outcome = "served"
            return []

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
            if observed.live_top_up == "unverified":
                observed.live_top_up = "view_fallback" if live_needs_params else "used"
            if observed.closed_hour_heal == "planned":
                observed.closed_hour_heal = "used"
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            # Any read failure (stale view, missing column, schema drift
            # in older bundles, …) drops us to the raw path. Logged at
            # debug — the caller will produce a working result anyway.
            _logger.debug("[time_series_rollup] read failed, falling back to raw: %s", e)
            observed.outcome = "read_failed"
            if observed.closed_hour_heal == "planned":
                observed.closed_hour_heal = "failed"
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
        observed.outcome = "served"
        return out

    def try_count_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        table_name: str,
        where_clause: str,
        params: list,
        unfiltered_window: bool = False,
        projected_fields: list[str] | None = None,
        actual_cols: list[str] | set[str] | None = None,
    ) -> int | None:
        """Serve the total requests count from per-hour rollup parquets when eligible.

        Returns the total row count (int), or None to fall back to raw query.
        """
        from datetime import UTC, timedelta

        from backend.utils.date_utils import parse_iso_utc

        observed = ReaderCoverage(reader="dashboard.count", eligible=False, outcome="ineligible")
        self.reader_coverage.append(observed)
        if not unfiltered_window:
            observed.eligibility_reason = "filtered"
            return None
        if not start_time or not end_time:
            observed.eligibility_reason = "bounds_missing"
            return None

        st = parse_iso_utc(start_time)
        et = parse_iso_utc(end_time)
        if st is None or et is None or et <= st:
            observed.eligibility_reason = "invalid_window"
            return None
        observed.window_start, observed.window_end = st.isoformat(), et.isoformat()
        if (et - st) > timedelta(days=366):
            observed.eligibility_reason = "window_too_long"
            return None
        observed.eligible, observed.eligibility_reason = True, "eligible"
        observed.scalar_plan = ScalarRollupCoverage()

        from backend.repositories._rollup_planner import plan_time_series_rollup_window

        wplan = plan_time_series_rollup_window(
            self.src,
            st,
            et,
            can_use_overview=True,
            coverage_mod=rollup_coverage,
            plan_hourly_read_fn=plan_hourly_read,
            observation=observed.scalar_plan,
        )
        if wplan is None:
            observed.outcome = "no_coverage"
            return None
        observed.outcome = "unknown"
        observed.live_top_up = "outside_window"
        rollup_paths = wplan.rollup_paths
        day_ov_paths = wplan.day_ov_paths
        crosses_active = wplan.crosses_active
        active_hour_str, active_hour_dt = wplan.active_hour_str, wplan.active_hour_dt
        missing_hours = wplan.missing_hours
        heal_where_sql = wplan.heal_where_sql

        st_tz = st.astimezone(UTC).isoformat()
        st_hour_tz = st.replace(minute=0, second=0, microsecond=0).astimezone(UTC).isoformat()
        et_tz = et.astimezone(UTC).isoformat()

        select_clauses: list[str] = []
        if rollup_paths:
            paths_sql = quote_path_list(rollup_paths)
            select_clauses.append(
                f"SELECT {metric_expressions.sum_or_zero('requests')} AS num "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE bucket >= TIMESTAMPTZ '{st_tz}' "
                f"  AND bucket < TIMESTAMPTZ '{et_tz}'"
            )
        if day_ov_paths:
            ov_paths_sql = quote_path_list(day_ov_paths)
            hour_ts_expr = (
                "TRY_CAST(CASE WHEN LENGTH(CAST(hour_start AS VARCHAR)) = 13 "
                "THEN CAST(hour_start AS VARCHAR) || ':00:00Z' "
                "ELSE CAST(hour_start AS VARCHAR) END AS TIMESTAMPTZ)"
            )
            select_clauses.append(
                f"SELECT {metric_expressions.sum_or_zero('requests')} AS num "
                f"FROM read_parquet([{ov_paths_sql}]) "
                f"WHERE {hour_ts_expr} >= TIMESTAMPTZ '{st_hour_tz}' "
                f"  AND {hour_ts_expr} < TIMESTAMPTZ '{et_tz}'"
            )

        # Missing-hour live heal: see try_time_series_from_rollup — a CLOSED
        # hour with no time_series bundle is a writer-coverage gap, not zero
        # traffic.
        if missing_hours:
            from backend.core.rollups import _safe_table_for

            heal_base_table = _safe_table_for(self.src)
            observed.closed_hour_heal = "planned" if heal_base_table else "unavailable"
            if heal_base_table:
                select_clauses.append(f"SELECT COUNT(*) AS num FROM {heal_base_table} WHERE {heal_where_sql}")

        direct_live_tmp: str | None = None
        live_needs_params = False
        if crosses_active:
            live_start = max(st, active_hour_dt)
            live_end = et
            live_start, _partial_rows, partial_total = self._partial_hour_adjusted_live_start(
                live_start, active_hour_str, live_end
            )
            live_st_tz = live_start.astimezone(UTC).isoformat()
            live_et_tz = live_end.astimezone(UTC).isoformat()
            observed.live_start, observed.live_end = live_st_tz, live_et_tz

            if unfiltered_window:
                try:
                    direct_live_tmp = self._create_active_hour_temp_direct(
                        projected_fields or [],
                        actual_cols or [],
                        live_start,
                        live_end,
                    )
                except Exception:
                    direct_live_tmp = None
            if direct_live_tmp == "__empty__":
                live_source = None
            elif direct_live_tmp is not None:
                live_source, live_where = direct_live_tmp, "1=1"
            else:
                live_source, live_where = table_name, where_clause
                live_needs_params = True

            observed.live_top_up = "empty" if live_source is None else "unverified"
            if live_source is not None:
                select_clauses.append(
                    f"SELECT COUNT(*) AS num "
                    f"FROM {live_source} "
                    f"WHERE {live_where} "
                    f"  AND timestamp >= TIMESTAMPTZ '{live_st_tz}' "
                    f"  AND timestamp <  TIMESTAMPTZ '{live_et_tz}'"
                )

            if partial_total:
                # Read from the writer's synthetic __total__ row (C1, final
                # whole-branch review) — summing partial_rows itself would
                # count each request once per populated field.
                select_clauses.append(f"SELECT {partial_total} AS num")

        if not select_clauses:
            observed.outcome = "served"
            return 0

        unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
        final_sql = f"SELECT {metric_expressions.sum_or_zero('num')} FROM ({unioned})"

        try:
            result = int(self.execute(final_sql, params if live_needs_params else []).fetchone()[0])
            if observed.live_top_up == "unverified":
                observed.live_top_up = "view_fallback" if live_needs_params else "used"
            observed.outcome = "served"
            if observed.closed_hour_heal == "planned":
                observed.closed_hour_heal = "used"
            return result
        except duckdb.InterruptException:
            raise
        except Exception as e:
            _logger.debug("[count_rollup] read failed, falling back to raw: %s", e)
            observed.outcome = "read_failed"
            if observed.closed_hour_heal == "planned":
                observed.closed_hour_heal = "failed"
            return None
        finally:
            if direct_live_tmp is not None:
                self.release_active_direct_temp(direct_live_tmp)

    # Minimum window-hours below which the rollup read isn't worth the
    # closed-hour enumeration. The slow_urls panel hits raw under 48 h
    # in well under a second on most services, so the rollup path is
    # for the 7 d / 30 d cases that actually hurt.
    _SLOW_URLS_ROLLUP_MIN_HOURS = 24

    def _eligible_rollup_window(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        require_top_asns: list[int] | None = None,
        min_hours: int | None = None,
        observation: ReaderCoverage | None = None,
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
        return eligible_window(
            start_time,
            end_time,
            has_filters=has_filters,
            require_top_asns=require_top_asns,
            min_hours=min_hours if min_hours is not None else self._SLOW_URLS_ROLLUP_MIN_HOURS,
            observation=observation,
        )

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
        if durable_serving_blocked(self.src):
            return None

        from backend.core.rollups import SLOW_URLS_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        # Unlike try_time_series_from_rollup, which falls back on a single
        # missing closed hour (a chart undercount would mislead), the
        # slow_urls panel is a top-N ranking: missing a few hours doesn't
        # change which URLs dominate, so gaps are tolerated down to the
        # shared closed-hour coverage floor. The active hour is skipped
        # entirely — merging live SQL for it would force a raw scan anyway.
        rollup_paths = collect_hour_paths_with_floor(self.src, st, et, SLOW_URLS_BUNDLE_FILENAME)
        if rollup_paths is None:
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[slow_urls_rollup] read failed, falling back to raw: %s", e)
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
        allow_empty_with_done: bool = False,
        observation: RollupPathCoverage | None = None,
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
        return collect_day_prefer_paths(
            self.src,
            st,
            et,
            bundle_filename,
            skip_partial_start=skip_partial_start,
            allow_empty_with_done=allow_empty_with_done,
            observation=observation,
        )

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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[origin_summary_rollup] read failed, falling back to raw: %s", e)
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[origin_pop_rollup] read failed, falling back: %s", e)
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[origin_ip_rollup] read failed, falling back: %s", e)
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[origin_path_rollup] read failed, falling back: %s", e)
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[origin_status_rollup] read failed, falling back: %s", e)
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
        from datetime import UTC, datetime

        from backend.core.rollups import ORIGIN_LATENCY_TS_BUNDLE_FILENAME

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

        # Fail-closed on any missing closed hour (the plan is None when a
        # closed hour had data but no bundle on disk). crosses_active tells us
        # whether to splice in the live tail.
        plan = plan_hourly_read(self.src, st, et, ORIGIN_LATENCY_TS_BUNDLE_FILENAME)
        if plan is None:
            return None
        rollup_paths, crosses_active = plan.paths, plan.crosses_active

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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[origin_latency_ts_rollup] read failed, falling back to raw: %s", e)
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
            return metric_expressions.origin_latency_us(cols, absent=None)
        # ttlb
        if "ottlb" in cols:
            return '"ottlb"'
        return None

    def try_network_quality_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        region_country: str,
    ) -> dict[str, Any] | None:
        from backend.core.rollups._common import (
            NETWORK_QUALITY_ASN_FILENAME,
            NETWORK_QUALITY_COUNTRY_FILENAME,
            NETWORK_QUALITY_POP_FILENAME,
            NETWORK_QUALITY_REGION_FILENAME,
        )

        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=None, min_hours=24
        )
        if win is None:
            return None
        st, et = win

        def _run_dim(
            filename: str, group_col: str, filter_sql: str = "", filter_params: list = None
        ) -> list[dict] | None:
            rollup_paths = self._collect_rollup_paths(st, et, filename)
            if not rollup_paths:
                return None
            from backend.core.rollups._common import quote_path_list

            paths_sql = quote_path_list(rollup_paths)

            sql = (
                f"SELECT dim_val as label, "
                f"       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) / 1000.0 AS rtt_ms, "
                f"       SUM(requests) AS reqs "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE 1=1 {filter_sql} "
                f"GROUP BY dim_val "
                f"ORDER BY reqs DESC "
                f"LIMIT 25"
            )
            rows = self.execute(sql, filter_params or []).fetchall()
            return [
                {"value": str(r[0]), "label": str(r[0]), "rtt_ms": round(float(r[1]), 2), "reqs": int(r[2])}
                for r in rows
                if r[1] is not None
            ]

        try:
            by_country = _run_dim(NETWORK_QUALITY_COUNTRY_FILENAME, "country")
            if by_country is None:
                return None

            by_asn = _run_dim(NETWORK_QUALITY_ASN_FILENAME, "asn")
            if by_asn is None:
                return None

            by_pop = _run_dim(NETWORK_QUALITY_POP_FILENAME, "pop")
            if by_pop is None:
                return None

            by_region = _run_dim(NETWORK_QUALITY_REGION_FILENAME, "region", "AND country = ?", [region_country])
            if by_region is None:
                return None

            return {
                "available": True,
                "by_country": by_country,
                "by_asn": by_asn,
                "by_pop": by_pop,
                "by_region": by_region,
                "region_country": region_country,
                "scatter": [],  # Scatter not supported in rollup; UI will just render empty or we handle it
                "countries": [c["value"] for c in by_country],
                "_approx": True,
            }
        except duckdb.InterruptException:
            raise
        except Exception as e:
            _logger.debug("[network_quality_rollup] read failed: %s", e)
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

        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=top_asns, min_hours=24
        )
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[network_rtt_rollup] read failed, falling back: %s", e)
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

        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=top_asns, min_hours=24
        )
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, NETWORK_SPEED_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        asn_placeholders = ", ".join(["?"] * len(top_asns))
        paths_sql = quote_path_list(rollup_paths)
        from datetime import UTC, datetime

        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        from backend.core.rollups import _safe_table_for

        base_table = _safe_table_for(self.src)

        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            et_iso = et.isoformat()
            sql = (
                f"SELECT asn, c_speed, CAST(SUM(cnt) AS BIGINT) AS cnt FROM ("
                f"  SELECT asn, c_speed, count AS cnt "
                f"  FROM read_parquet([{paths_sql}]) "
                f"  WHERE asn IN ({asn_placeholders}) "
                f"  UNION ALL "
                f"  SELECT asn, c_speed, CAST(COUNT(*) AS BIGINT) AS cnt "
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}' "
                f"    AND asn IN ({asn_placeholders}) AND c_speed IS NOT NULL "
                f"  GROUP BY asn, c_speed"
                f") GROUP BY asn, c_speed "
                f"ORDER BY asn, cnt DESC"
            )
            # Duplicate top_asns because asn_placeholders appears twice
            top_asns = top_asns + top_asns
        else:
            sql = (
                f"SELECT asn, c_speed, CAST(SUM(count) AS BIGINT) AS cnt "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE asn IN ({asn_placeholders}) "
                f"GROUP BY asn, c_speed "
                f"ORDER BY asn, cnt DESC"
            )
        try:
            rows = self.execute(sql, top_asns).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[network_speed_rollup] read failed, falling back: %s", e)
            return None
        return [(int(r[0]), str(r[1]), int(r[2])) for r in rows]

    def try_network_heatmap_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        bucket_seconds: int = 3600,
    ) -> list | None:
        """Serve the /api/network-health ``heatmap`` section (and the
        derived ``leaderboard`` / ``summary`` / ``buckets`` sections)
        from per-hour + per-day network_heatmap parquets when eligible.

        Same eligibility posture as :meth:`try_network_rtt_from_rollup`:
        unfiltered only, window ≥ 24 h, ≥ 50% closed-hour coverage.

        Each parquet row covers exactly one (asn, hour) pair. The reader
        returns one row per (asn, hour) with the hour as the heatmap
        bucket — hourly bucket granularity on 30 d windows (720 buckets
        instead of the 5-min live resolution of 8640), which is still
        meaningful for trend/health analysis.

        BUCKET GRANULARITY: the caller's ``bucket_seconds`` is the FE's
        *requested* resolution. The real caller (network.py) computes it as
        ``max(router_default_300, span // 8640)``, which is 300 s for every
        window from 24 h to 30 d (span // 8640 only exceeds 300 past 30 d).
        So gating on ``== 3600`` made this rollup structurally unreachable
        for every live FE window — every 30 d network-health query fell to
        the multi-second raw scan (the source of the 504s under concurrent
        load). The heatmap is a per-hour TIME SERIES: serving hourly buckets
        when the caller asked for hourly-or-finer is correct, not a
        downgrade of accuracy — the downstream ``bucket_idx`` is assigned
        POSITIONALLY from the distinct row timestamps (network.py's
        ``sorted(all_buckets_set)`` → ``enumerate``), never from
        ``bucket_seconds``, so hourly rows render as a dense 720-column axis
        with no gaps regardless of the requested width. We therefore serve
        whenever ``bucket_seconds <= 3600`` and return ``None`` only for a
        COARSER-than-hourly request (e.g. 2 h / daily buckets), which an
        hourly rollup can't satisfy without re-aggregation this reader
        doesn't do.

        Returns rows shaped as
        ``(asn, bucket_ts, throughput_bps, rtt_med_us, rtt_baseline_us,
           rtt_congestion_us, avg_ploss, jitter_us, error_pct, reqs)``
        matching positions ``r[0]–r[9]`` in the live heatmap loop, or
        ``None`` on any eligibility miss / read error.
        """
        from backend.core.rollups._common import NETWORK_HEATMAP_BUNDLE_FILENAME

        if bucket_seconds > 3600:
            return None

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters, min_hours=24)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, NETWORK_HEATMAP_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        st_iso = st.isoformat()
        et_iso = et.isoformat()
        from datetime import UTC, datetime

        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        from backend.core.rollups import _safe_table_for

        base_table = _safe_table_for(self.src)

        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()

            # The active hour duration in seconds for throughput calculation
            active_duration_secs = max(1, (et - active_hour_start).total_seconds())

            sql = (
                f"SELECT * FROM ("
                f"  SELECT"
                f"    asn,"
                f"    hour_ts                                                             AS bucket_ts,"
                f"    resp_bytes_sum / 3600.0                                            AS throughput_bps,"
                f"    rtt_p50_us                                                         AS rtt_med_us,"
                f"    rtt_min_p50_us                                                     AS rtt_baseline_us,"
                f"    CAST(rtt_p50_us AS BIGINT) - CAST(COALESCE(rtt_min_p50_us, rtt_p50_us) AS BIGINT) AS rtt_congestion_us,"
                f"    ploss_sum / NULLIF(ploss_count, 0)                                AS avg_ploss,"
                f"    rtt_var_p50_us                                                     AS jitter_us,"
                f"    errors * 100.0 / NULLIF(reqs, 0)                                  AS error_pct,"
                f"    CAST(reqs AS BIGINT)                                               AS reqs"
                f"   FROM read_parquet([{paths_sql}])"
                f"   WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"   UNION ALL "
                f"   SELECT"
                f"    asn,"
                f"    TIMESTAMPTZ '{ah_iso}' AS bucket_ts,"
                f"    CAST(SUM(resp_bytes) AS DOUBLE) / {active_duration_secs} AS throughput_bps,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_baseline_us,"  # No min baseline for active hour
                f"    0::BIGINT AS rtt_congestion_us,"
                f"    0::DOUBLE AS avg_ploss,"  # Ploss requires packet info, assume 0 for live
                f"    0::DOUBLE AS jitter_us,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs"
                f"   FROM {base_table} "
                f"   WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'"
                f"     AND asn IS NOT NULL"
                f"   GROUP BY asn"
                f")"
                f" ORDER BY reqs DESC"
            )
        else:
            sql = (
                f"SELECT"
                f"  asn,"
                f"  hour_ts                                                             AS bucket_ts,"
                f"  resp_bytes_sum / 3600.0                                            AS throughput_bps,"
                f"  rtt_p50_us                                                         AS rtt_med_us,"
                f"  rtt_min_p50_us                                                     AS rtt_baseline_us,"
                f"  CAST(rtt_p50_us AS BIGINT) - CAST(COALESCE(rtt_min_p50_us, rtt_p50_us) AS BIGINT) AS rtt_congestion_us,"
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[network_heatmap_rollup] read failed, falling back: %s", e)
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
        bucket_seconds: int = 3600,
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

        if int(bucket_seconds) > 3600:
            return None

        if map_asn != "all":
            return None  # per-ASN map drill-down not supported by geo rollup

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters, min_hours=24)
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
        from datetime import UTC, datetime

        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        from backend.core.rollups import _safe_table_for

        base_table = _safe_table_for(self.src)

        cols = set(self.get_schema_cols())
        if "country" not in cols:
            return None
        has_city = "city" in cols
        has_lat = "lat" in cols and "lon" in cols
        has_metro = "metro" in cols
        city_col = "city" if has_city else "CAST('' AS VARCHAR)"
        lat_col = "CAST(lat AS DOUBLE)" if has_lat else "CAST(NULL AS DOUBLE)"
        lon_col = "CAST(lon AS DOUBLE)" if has_lat else "CAST(NULL AS DOUBLE)"
        metro_col = "metro" if has_metro else "CAST(NULL AS VARCHAR)"
        city_grp = "city" if has_city else "''"
        lat_grp = "lat" if has_lat else "NULL"
        lon_grp = "lon" if has_lat else "NULL"
        metro_grp = "metro" if has_metro else "NULL"

        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            map_sql = (
                f"SELECT * FROM ("
                f"  SELECT"
                f"    country, city, lat, lon, metro, hour_ts AS bucket_ts,"
                f"    rtt_sum / NULLIF(rtt_count, 0) AS rtt_med_us,"
                f"    ploss_sum / NULLIF(ploss_count, 0) AS avg_ploss,"
                f"    errors * 100.0 / NULLIF(reqs, 0) AS error_pct,"
                f"    CAST(reqs AS BIGINT) AS reqs"
                f"  FROM read_parquet([{paths_sql}])"
                f"  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"  UNION ALL "
                f"  SELECT"
                f"    country,"
                f"    {city_col} AS city,"
                f"    {lat_col} AS lat,"
                f"    {lon_col} AS lon,"
                f"    {metro_col} AS metro,"
                f"    TIMESTAMPTZ '{ah_iso}' AS bucket_ts,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,"
                f"    0::DOUBLE AS avg_ploss,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs"
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'"
                f"    AND country IS NOT NULL AND country != ''"
                f"  GROUP BY country, {city_grp}, {lat_grp}, {lon_grp}, {metro_grp}"
                f") ORDER BY bucket_ts, reqs DESC LIMIT 5000"
            )
            metro_sql = (
                f"SELECT country, city, CAST('' AS VARCHAR) AS region, metro, "
                f"  CAST(SUM(rtt_med_us * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS rtt_med_us,"
                f"  CAST(SUM(avg_ploss * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS avg_ploss,"
                f"  CAST(SUM(error_pct * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS error_pct,"
                f"  CAST(SUM(reqs) AS BIGINT) AS total_reqs "
                f"FROM ("
                f"  SELECT country, city, metro,"
                f"    SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0) AS rtt_med_us,"
                f"    SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0) AS avg_ploss,"
                f"    SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0) AS error_pct,"
                f"    CAST(SUM(reqs) AS BIGINT) AS reqs"
                f"  FROM read_parquet([{paths_sql}])"
                f"  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"  GROUP BY country, city, metro"
                f"  UNION ALL "
                f"  SELECT country, {city_col} AS city, {metro_col} AS metro,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,"
                f"    0::DOUBLE AS avg_ploss,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs"
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'"
                f"    AND country IS NOT NULL AND country != ''"
                f"  GROUP BY country, {city_grp}, {metro_grp}"
                f") GROUP BY country, city, metro ORDER BY total_reqs DESC LIMIT 100"
            )
        else:
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[network_geo_rollup] read failed, falling back: %s", e)
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
        from datetime import UTC, datetime

        from backend.core.rollups._common import VERIFIED_BOTS_TS_BUNDLE_FILENAME

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
        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[verified_bots_ts_rollup] read failed, falling back: %s", e)
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[perf_latency_rollup] read failed, falling back: %s", e)
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

    def _try_security_bucket_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        bundle_filename: str,
        log_tag: str,
    ) -> list[dict] | None:
        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, bundle_filename)
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[%s] read failed, falling back: %s", log_tag, e)
            return None
        if not rows:
            return None
        return [{"bucket": r[0], "count": int(r[1] or 0)} for r in rows]

    def try_security_req_size_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list[dict] | None:
        """Serve /api/security/aggregates' ``req_size`` panel from per-hour +
        per-day security_req_size parquets (req_header_bytes histogram buckets).
        """
        from backend.core.rollups._common import SECURITY_REQ_SIZE_BUNDLE_FILENAME

        return self._try_security_bucket_rollup(
            start_time,
            end_time,
            has_filters=has_filters,
            bundle_filename=SECURITY_REQ_SIZE_BUNDLE_FILENAME,
            log_tag="security_req_size_rollup",
        )

    def try_security_conn_reuse_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list[dict] | None:
        """Serve /api/security/aggregates' ``conn_reuse`` panel from per-hour +
        per-day security_conn_reuse parquets (conn_requests reuse buckets).
        """
        from backend.core.rollups._common import SECURITY_CONN_REUSE_BUNDLE_FILENAME

        return self._try_security_bucket_rollup(
            start_time,
            end_time,
            has_filters=has_filters,
            bundle_filename=SECURITY_CONN_REUSE_BUNDLE_FILENAME,
            log_tag="security_conn_reuse_rollup",
        )

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

        observed = ReaderCoverage(reader="dashboard.conn_requests_histogram", minimum_window_hours=2)
        self.reader_coverage.append(observed)
        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, min_hours=2, observation=observed
        )
        if win is None:
            observed.outcome = "ineligible"
            return None
        st, et = win

        attempt = RollupPathCoverage(bundle="security_conn_reuse")
        observed.attempts.append(attempt)
        rollup_paths = self._collect_rollup_paths(st, et, SECURITY_CONN_REUSE_BUNDLE_FILENAME, observation=attempt)
        use_all_fields = rollup_paths is None
        if use_all_fields:
            from backend.core.rollups._common import DAY_BUNDLE_FILENAME

            attempt = RollupPathCoverage(bundle="all_fields")
            observed.attempts.append(attempt)
            rollup_paths = self._collect_rollup_paths(st, et, DAY_BUNDLE_FILENAME, observation=attempt)
            if rollup_paths is None:
                observed.outcome = "no_coverage"
                return None
        if rollup_paths is None:
            raise RuntimeError("rollup_paths is unexpectedly None")

        paths_sql = quote_path_list(rollup_paths)
        if use_all_fields:
            sql = (
                "SELECT value, CAST(SUM(count) AS BIGINT) AS c "
                f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
                "WHERE field = 'conn_requests' GROUP BY value"
            )
        else:
            sql = f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS c FROM read_parquet([{paths_sql}]) GROUP BY bucket"
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[conn_requests_hist_rollup] read failed, falling back: %s", e)
            observed.outcome = "read_failed"
            return None
        if not rows:
            observed.outcome = "empty_result"
            return None

        counts: dict[str, int] = {}
        for bucket, cnt in rows:
            if use_all_fields:
                try:
                    numeric = int(float(bucket))
                except (TypeError, ValueError):
                    continue
                bucket = (
                    "1" if numeric == 1 else "2–5" if 2 <= numeric <= 5 else "6–20" if 6 <= numeric <= 20 else "21+"
                )
            label = self._CONN_REUSE_TO_DASHBOARD_BUCKET.get(bucket, bucket)
            counts[label] = counts.get(label, 0) + int(cnt or 0)

        # Live top-up: [active hour ∩ window). Closed hours are all rollup-
        # served (_collect_rollup_paths stops at the active hour).
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(active_dt, st)
        live_end = min(active_dt + timedelta(hours=1), et)
        observed.live_top_up = "outside_window"
        if live_start < live_end:
            observed.live_start, observed.live_end = live_start.isoformat(), live_end.isoformat()
            observed.live_top_up = "missing_column"
            cols = actual_cols if actual_cols is not None else self.get_schema_cols()
            if "conn_requests" in cols:
                tmp = self._create_active_hour_temp_direct(["conn_requests"], cols, live_start, live_end)
                observed.live_top_up = "unavailable"
                if tmp == "__empty__":
                    observed.live_top_up = "empty"
                elif tmp is not None:
                    try:
                        from backend.repositories._sql import dashboard as _dash_sql

                        live_q = _dash_sql.CONN_REQUESTS_BUCKET.format(
                            requests_metric=CANONICAL_METRICS["requests"],
                            table_name=tmp,
                            where_clause="1=1",
                        )
                        for bucket, cnt in self.execute(live_q).fetchall():
                            counts[bucket] = counts.get(bucket, 0) + int(cnt or 0)
                        observed.live_top_up = "used"
                    except duckdb.Error as e:
                        observed.live_top_up = "failed"
                        _logger.debug("[conn_requests_hist_rollup] live top-up failed (skipped): %s", e)
                    finally:
                        self.release_active_direct_temp(tmp)

        known = [b for b in self._CONN_REQUESTS_BUCKET_ORDER if b in counts]
        # Unknown labels (writer drift) are appended rather than dropped so
        # their counts stay visible instead of silently vanishing.
        extras = [b for b in counts if b not in self._CONN_REQUESTS_BUCKET_ORDER]
        top = [{"value": b, "count": counts[b]} for b in known + extras]
        observed.outcome = "served"
        return {"top": top, "total": sum(counts.values())}

    def try_virtual_field_top_n_from_rollup(
        self,
        virtual_id: str,
        backing_col: str,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        actual_cols: list[str] | set[str] | None = None,
    ) -> dict[str, Any] | None:
        """Serve a CSV-backed virtual field from the all-fields rollups."""
        if has_filters or not start_time or not end_time:
            return None

        from backend.core.rollups._common import DAY_BUNDLE_FILENAME
        from backend.utils.date_utils import parse_iso_utc

        st = parse_iso_utc(start_time)
        et = parse_iso_utc(end_time)
        if st is None or et is None:
            return None
        paths = self._collect_rollup_paths(st, et, DAY_BUNDLE_FILENAME)
        if paths is None:
            return None

        paths_sql = quote_path_list(paths)
        try:
            rows = self.execute(
                "SELECT value, CAST(SUM(count) AS BIGINT) AS c "
                f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
                "WHERE field = ? GROUP BY value ORDER BY c DESC",
                [virtual_id],
            ).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error:
            return None

        counts: dict[str, int] = {}
        for value, count in rows:
            counts[str(value)] = counts.get(str(value), 0) + int(count or 0)

        from datetime import UTC, datetime, timedelta

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(active_dt, st)
        live_end = min(active_dt + timedelta(hours=1), et)
        if live_start < live_end:
            cols = actual_cols if actual_cols is not None else self.get_schema_cols()
            if backing_col in cols:
                tmp = self._create_active_hour_temp_direct([backing_col], cols, live_start, live_end)
                if tmp == "__empty__":
                    return {"top": [], "total": 0}
                if tmp is None:
                    return None
                try:
                    live_rows = self.execute(
                        f"SELECT trim(signal) AS value, COUNT(*) AS c "
                        f"FROM (SELECT unnest(string_split(\"{backing_col}\", ',')) AS signal "
                        f'FROM "{tmp}" WHERE "{backing_col}" IS NOT NULL AND "{backing_col}" != \'\') '
                        "WHERE trim(signal) != '' GROUP BY value"
                    ).fetchall()
                except duckdb.InterruptException:
                    raise
                except duckdb.Error:
                    return None
                finally:
                    self.release_active_direct_temp(tmp)
                for value, count in live_rows:
                    counts[str(value)] = counts.get(str(value), 0) + int(count or 0)

        total = sum(counts.values())
        top = [
            {"value": value, "count": count}
            for value, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
            if value != "__other__"
        ][:10]
        return {"top": top, "total": total}

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

        observed = ReaderCoverage(reader="dashboard.ngwaf_bots", minimum_window_hours=2)
        self.reader_coverage.append(observed)
        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, min_hours=2, observation=observed
        )
        if win is None:
            observed.outcome = "ineligible"
            return None
        st, et = win

        attempt = RollupPathCoverage(bundle="ngwaf_bots")
        observed.attempts.append(attempt)
        rollup_paths = self._collect_rollup_paths(st, et, NGWAF_BOTS_BUNDLE_FILENAME, observation=attempt)
        if rollup_paths is None:
            observed.outcome = "no_coverage"
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT bot_name, category, CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bot_name, category"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[ngwaf_bots_rollup] read failed, falling back: %s", e)
            observed.outcome = "read_failed"
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
        observed.live_top_up = "outside_window"
        if live_start < live_end:
            observed.live_start, observed.live_end = live_start.isoformat(), live_end.isoformat()
            observed.live_top_up = "missing_column"
            cols = self.get_schema_cols()
            if "waf_req_id" in cols:
                tmp = self._create_active_hour_temp_direct(["waf_req_id"], cols, live_start, live_end)
                observed.live_top_up = "unavailable"
                if tmp == "__empty__":
                    observed.live_top_up = "empty"
                elif tmp is not None:
                    try:
                        live_q = (
                            f"SELECT nb.bot_name, nb.category, CAST(COUNT(*) AS BIGINT) AS c "
                            f'FROM "{tmp}" t '
                            f"INNER JOIN temp.ngwaf_top_ngwaf_bots nb USING (waf_req_id) "
                            f"WHERE nb.bot_name IS NOT NULL "
                            f"GROUP BY 1, 2"
                        )
                        for bot_name, category, cnt in self.execute(live_q).fetchall():
                            key = (bot_name, category)
                            counts[key] = counts.get(key, 0) + int(cnt or 0)
                        observed.live_top_up = "used"
                    except duckdb.Error as e:
                        observed.live_top_up = "failed"
                        _logger.debug("[ngwaf_bots_rollup] live top-up failed (skipped): %s", e)
                    finally:
                        self.release_active_direct_temp(tmp)

        ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[: max(0, int(n))]
        observed.outcome = "served"
        return [{"name": bn, "category": cat, "request_count": c} for (bn, cat), c in ranked]

    def try_ngwaf_verified_bots_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list[dict] | None:
        """Serve ``/api/security/aggregates``' ``ngwaf_verified_bots`` panel
        from per-hour/per-day ``ngwaf_bots.parquet`` files + a live active-hour
        top-up over the direct buffer/hourly temp.

        Counts SUM exactly across hours — no ``_approx`` flag. Pre-rebundle
        3-column ``ngwaf_bots.parquet`` files (missing ``wellknown_bot_name``,
        Trap #24) are detected via footer schema inspection and return ``None``
        so the caller falls back to the live temp scan until backfilled.
        """
        from datetime import UTC, datetime, timedelta

        import pyarrow.parquet as pq

        from backend.core.rollups._common import NGWAF_BOTS_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, NGWAF_BOTS_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        try:
            for probe_path in (rollup_paths[0], rollup_paths[-1]):
                if "wellknown_bot_name" not in pq.read_schema(probe_path).names:
                    return None
        except Exception:
            return None

        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT bot_name, wellknown_bot_name, category, CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bot_name, wellknown_bot_name, category"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[ngwaf_verified_bots_rollup] read failed, falling back: %s", e)
            return None

        counts: dict[tuple[str | None, str | None, str | None], int] = {}
        for bot_name, wellknown_bot_name, category, cnt in rows:
            key = (bot_name, wellknown_bot_name, category)
            counts[key] = counts.get(key, 0) + int(cnt or 0)

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(active_dt, st)
        live_end = min(active_dt + timedelta(hours=1), et)
        if live_start < live_end:
            cols = self.get_schema_cols()
            if "waf_req_id" in cols:
                tmp = self._create_active_hour_temp_direct(["waf_req_id"], cols, live_start, live_end)
                if tmp == "__empty__":
                    pass
                elif tmp is not None:
                    try:
                        live_q = (
                            f"SELECT nb.bot_name, nb.wellknown_bot_name, nb.category, "
                            f"       CAST(COUNT(*) AS BIGINT) AS c "
                            f'FROM "{tmp}" t '
                            f"INNER JOIN temp.ngwaf_cache_ngwaf_bots nb USING (waf_req_id) "
                            f"WHERE nb.bot_name IS NOT NULL "
                            f"GROUP BY 1, 2, 3"
                        )
                        for bot_name, wellknown_bot_name, category, cnt in self.execute(live_q).fetchall():
                            key = (bot_name, wellknown_bot_name, category)
                            counts[key] = counts.get(key, 0) + int(cnt or 0)
                    except duckdb.Error as e:
                        _logger.debug("[ngwaf_verified_bots_rollup] live top-up failed (skipped): %s", e)
                    finally:
                        self.release_active_direct_temp(tmp)

        ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
        return [
            {
                "bot_name": bn,
                "wellknown_bot_name": wk,
                "category": cat,
                "request_count": c,
            }
            for (bn, wk, cat), c in ranked
        ]

    def try_ngwaf_verified_bots_ts_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        bucket_seconds: int,
        has_filters: bool,
    ) -> list[tuple] | None:
        """Serve ``/api/security/aggregates``' ``ngwaf_verified_bots_ts`` panel
        from minute-granular ``ngwaf_bots_ts.parquet`` files + a live
        active-hour top-up over the direct buffer/hourly temp.

        Re-bucketing from stored 1-minute granularity is exact whenever
        ``bucket_seconds`` is a whole number of minutes. Counts SUM exactly
        across hours — no ``_approx`` flag. Returns ``[(bucket_ts, bot_name,
        count), ...]`` ordered by ``(bucket_ts, bot_name)``, or ``None`` on
        any eligibility miss / read error.
        """
        from datetime import UTC, datetime, timedelta

        from backend.core.rollups._common import NGWAF_BOTS_TS_BUNDLE_FILENAME

        if has_filters or not start_time or not end_time:
            return None
        try:
            n = int(bucket_seconds)
        except (TypeError, ValueError):
            return None
        if n < 60 or n % 60 != 0:
            return None

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, NGWAF_BOTS_TS_BUNDLE_FILENAME, skip_partial_start=False)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)
        st_iso = st.isoformat()
        et_iso = et.isoformat()
        sql = (
            f"SELECT time_bucket(INTERVAL '{n} seconds', bucket_ts) AS bucket, "
            f"       bot_name, "
            f"       CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}]) "
            f"WHERE bucket_ts >= TIMESTAMPTZ '{st_iso}' AND bucket_ts < TIMESTAMPTZ '{et_iso}' "
            f"GROUP BY 1, 2"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[ngwaf_verified_bots_ts_rollup] read failed, falling back: %s", e)
            return None

        counts: dict[tuple[Any, str], int] = {}
        for bucket, bot_name, cnt in rows:
            key = (bucket, str(bot_name))
            counts[key] = counts.get(key, 0) + int(cnt or 0)

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(active_dt, st)
        live_end = min(active_dt + timedelta(hours=1), et)
        if live_start < live_end:
            cols = self.get_schema_cols()
            if "waf_req_id" in cols:
                tmp = self._create_active_hour_temp_direct(["waf_req_id"], cols, live_start, live_end)
                if tmp == "__empty__":
                    pass
                elif tmp is not None:
                    try:
                        live_q = (
                            f"SELECT time_bucket(INTERVAL '{n} seconds', t.timestamp) AS bucket, "
                            f"       nb.bot_name, "
                            f"       CAST(COUNT(*) AS BIGINT) AS c "
                            f'FROM "{tmp}" t '
                            f"INNER JOIN temp.ngwaf_cache_ngwaf_bots nb USING (waf_req_id) "
                            f"WHERE nb.bot_name IS NOT NULL "
                            f"GROUP BY 1, 2"
                        )
                        for bucket, bot_name, cnt in self.execute(live_q).fetchall():
                            key = (bucket, str(bot_name))
                            counts[key] = counts.get(key, 0) + int(cnt or 0)
                    except duckdb.Error as e:
                        _logger.debug("[ngwaf_verified_bots_ts_rollup] live top-up failed (skipped): %s", e)
                    finally:
                        self.release_active_direct_temp(tmp)

        return [(b, bn, c) for (b, bn), c in sorted(counts.items())]

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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[security_top_ips_rollup] read failed, falling back: %s", e)
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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[security_coverage_rollup] read failed, falling back: %s", e)
            return None
        if row is None or row[0] is None or int(row[0]) == 0:
            return None
        return int(row[0]), int(row[1] or 0)

    def try_security_proxies_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> dict | None:
        """Serve /api/security/aggregates' ``proxies`` section from per-hour +
        per-day security_proxy_quality / security_proxy_flagged parquets.

        Same eligibility posture as :meth:`try_security_req_size_from_rollup`
        (unfiltered, >= 48 h, <= 366 d, >= 50 % closed-hour coverage). EXACT
        across hours: quality counts SUM, and the distinct-IP stats are counted
        over the stored flagged tuples. One difference from the live scan: the
        client list is built from grouped tuples, so a client that tripped the
        flag on several identical requests appears once instead of repeated.

        Returns ``{"stats": {...}, "quality_rows": [(type, count)], "isps":
        [{"asn", "count"}], "clients": [{...}]}`` in the shapes
        ``get_security_proxies`` already consumes, or ``None`` on any
        eligibility miss / read error / empty window.
        """
        from backend.core.rollups._common import (
            SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
            SECURITY_PROXY_QUALITY_BUNDLE_FILENAME,
        )
        from backend.utils.pop_utils import (
            build_pop_lat_case_sql,
            build_pop_lon_case_sql,
        )

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        quality_paths = self._collect_rollup_paths(
            st, et, SECURITY_PROXY_QUALITY_BUNDLE_FILENAME, allow_empty_with_done=True
        )
        flagged_paths = self._collect_rollup_paths(
            st, et, SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME, allow_empty_with_done=True
        )
        if not quality_paths or flagged_paths is None:
            return None

        prefix = (
            (
                f"WITH f AS MATERIALIZED ("
                f"  SELECT ip, asn, pop, rtt_min, tcp_rtt, lat, lon, country, city, SUM(cnt) AS cnt "
                f"  FROM read_parquet([{quote_path_list(flagged_paths)}]) "
                f"  WHERE ip IS NOT NULL AND ip != '' GROUP BY ALL"
                f"), "
                f"pops AS MATERIALIZED ("
                f"  SELECT pop AS p_code, "
                f"         {build_pop_lat_case_sql()} AS pop_lat, "
                f"         {build_pop_lon_case_sql()} AS pop_lon "
                f"  FROM (SELECT DISTINCT pop FROM f)"
                f"), "
                f"geo AS MATERIALIZED ("
                f"  SELECT g.pop AS g_pop, g.lat AS g_lat, g.lon AS g_lon, "
                f"         6371 * 2 * ASIN(SQRT("
                f"           POWER(SIN(RADIANS(g.lat - p.pop_lat) / 2), 2) "
                f"           + COS(RADIANS(p.pop_lat)) * COS(RADIANS(g.lat)) "
                f"           * POWER(SIN(RADIANS(g.lon - p.pop_lon) / 2), 2)"
                f"         )) AS distance_km "
                f"  FROM (SELECT DISTINCT pop, lat, lon FROM f WHERE lat IS NOT NULL AND lon IS NOT NULL) g "
                f"  LEFT JOIN pops p ON g.pop IS NOT DISTINCT FROM p.p_code"
                f"), "
                f"m AS MATERIALIZED ("
                f"  SELECT f.ip, f.asn, f.pop, f.rtt_min, f.tcp_rtt, f.rtt_min / 1000.0 AS rtt_min_ms, "
                f"         f.tcp_rtt / 1000.0 AS tcp_rtt_ms, f.lat AS client_lat, f.lon AS client_lon, "
                f"         p.pop_lat AS pop_lat, p.pop_lon AS pop_lon, "
                f"         geo.distance_km AS distance_km, f.country, f.city, f.cnt "
                f"  FROM f "
                f"  LEFT JOIN pops p ON f.pop IS NOT DISTINCT FROM p.p_code "
                f"  LEFT JOIN geo ON f.pop IS NOT DISTINCT FROM geo.g_pop "
                f"               AND f.lat IS NOT DISTINCT FROM geo.g_lat "
                f"               AND f.lon IS NOT DISTINCT FROM geo.g_lon"
                f") "
            )
            if flagged_paths
            else ""
        )
        dist_flag = "distance_km > (rtt_min_ms * 100.0 + 150.0)"
        tunnel_flag = "tcp_rtt > 0 AND (CAST(rtt_min_ms * 1000.0 AS DOUBLE) / tcp_rtt) < 0.1"
        try:
            quality_rows = self.execute(
                f"SELECT type, CAST(SUM(count) AS BIGINT) AS c "
                f"FROM read_parquet([{quote_path_list(quality_paths)}]) GROUP BY type ORDER BY c DESC"
            ).fetchall()
            if not quality_rows:
                return None
            stats_row = (0, 0, 0)
            isp_rows = []
            client_rows = []
            if flagged_paths:
                combined_rows = self.execute(
                    prefix + f"SELECT 'stats' AS _sec, "
                    f"  COUNT(DISTINCT CASE WHEN {dist_flag} THEN ip WHEN {tunnel_flag} THEN ip ELSE NULL END) AS c1, "
                    f"  CAST({metric_expressions.sum_or_zero(f'CASE WHEN {dist_flag} THEN cnt WHEN {tunnel_flag} THEN cnt ELSE NULL END')} "
                    f"       AS BIGINT) AS c2, "
                    f"  COUNT(DISTINCT CASE WHEN {dist_flag} THEN ip ELSE NULL END) AS c3, "
                    f"  NULL::VARCHAR AS ip, NULL::BIGINT AS asn, NULL::DOUBLE AS rtt_min_ms, "
                    f"  NULL::DOUBLE AS tcp_rtt_ms, NULL::DOUBLE AS distance_km, NULL::VARCHAR AS pop, "
                    f"  NULL::DOUBLE AS client_lat, NULL::DOUBLE AS client_lon, NULL::DOUBLE AS pop_lat, "
                    f"  NULL::DOUBLE AS pop_lon, NULL::BOOLEAN AS impossible_distance, "
                    f"  NULL::VARCHAR AS risk_level, NULL::VARCHAR AS country, NULL::VARCHAR AS city "
                    f"FROM m "
                    f"UNION ALL "
                    f"SELECT * FROM ("
                    f"  SELECT 'isp' AS _sec, NULL::BIGINT, CAST(SUM(cnt) AS BIGINT) AS c2, NULL::BIGINT, "
                    f"    NULL::VARCHAR, asn, NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, "
                    f"    NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::BOOLEAN, "
                    f"    NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR "
                    f"  FROM m "
                    f"  WHERE tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.1 "
                    f"  GROUP BY asn ORDER BY c2 DESC LIMIT 10"
                    f") "
                    f"UNION ALL "
                    f"SELECT * FROM ("
                    f"  SELECT 'client' AS _sec, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, "
                    f"    ip, asn, ROUND(rtt_min_ms, 2), ROUND(tcp_rtt_ms, 2), ROUND(distance_km, 1), pop, "
                    f"    ROUND(client_lat, 4), ROUND(client_lon, 4), CAST(pop_lat AS DOUBLE), CAST(pop_lon AS DOUBLE), "
                    f"    CASE WHEN {dist_flag} THEN true ELSE false END AS impossible_distance, "
                    f"    CASE WHEN {dist_flag} THEN 'High' "
                    f"         WHEN tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.1 THEN 'High' "
                    f"         WHEN tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.2 THEN 'Medium' "
                    f"         ELSE 'Low' END AS risk_level, country, city "
                    f"  FROM m "
                    f"  WHERE rtt_min_ms IS NOT NULL AND (({dist_flag}) OR (tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.2)) "
                    f"  ORDER BY CASE risk_level WHEN 'High' THEN 3 WHEN 'Medium' THEN 2 ELSE 1 END DESC, distance_km DESC "
                    f"  LIMIT 100"
                    f")"
                ).fetchall()
                for r in combined_rows:
                    sec = r[0]
                    if sec == "stats":
                        stats_row = (r[1], r[2], r[3])
                    elif sec == "isp":
                        isp_rows.append((r[5], r[2]))
                    elif sec == "client":
                        client_rows.append(r[4:])
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[security_proxies_rollup] read failed, falling back: %s", e)
            return None

        return {
            "stats": {
                "active_proxies_count": stats_row[0] or 0,
                "total_requests_count": stats_row[1] or 0,
                "distance_mismatches_count": stats_row[2] or 0,
            },
            "quality_rows": [(r[0], r[1]) for r in quality_rows],
            "isps": [{"asn": r[0], "count": r[1]} for r in isp_rows],
            "clients": [
                {
                    "ip": r[0],
                    "asn": r[1],
                    "rtt_min_ms": r[2],
                    "tcp_rtt_ms": r[3],
                    "distance_km": r[4],
                    "pop": r[5],
                    "client_lat": r[6],
                    "client_lon": r[7],
                    "pop_lat": r[8],
                    "pop_lon": r[9],
                    "impossible_distance": r[10],
                    "risk_level": r[11],
                    "country": r[12],
                    "city": r[13],
                }
                for r in client_rows
            ],
        }

    def try_security_bot_efficiency_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list[tuple[str, str, int, int]] | None:
        """Serve /api/security/aggregates' ``bot_efficiency`` section from per-hour
        + per-day security_bot_eff parquets.

        Same eligibility posture as :meth:`try_security_req_size_from_rollup`.
        Site totals and per-UA counts/bytes SUM exactly (per-UA re-capped to the
        live top 2000 by bytes); the per-stem referral counts are a SUM of per-hour
        distinct IPs, so they overcount IPs seen in several hours.

        Returns ``(kind, key, cnt, resp_bytes)`` rows in the shape the live
        ``BOT_EFFICIENCY`` query yields (input to ``summarize_bot_efficiency_rows``),
        or ``None`` on any eligibility miss / read error / missing site row.
        """
        from backend.core.rollups._common import SECURITY_BOT_EFF_BUNDLE_FILENAME, SECURITY_BOT_EFF_UA_TOP_K

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, SECURITY_BOT_EFF_BUNDLE_FILENAME, allow_empty_with_done=True)
        if not rollup_paths:
            return None

        sql = (
            f"WITH g AS ("
            f"  SELECT kind, key, CAST(SUM(cnt) AS BIGINT) AS cnt, CAST(SUM(resp_bytes) AS BIGINT) AS resp_bytes "
            f"  FROM read_parquet([{quote_path_list(rollup_paths)}]) GROUP BY kind, key"
            f"), r AS ("
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY kind ORDER BY resp_bytes DESC, cnt DESC) AS rn FROM g"
            f") "
            f"SELECT kind, key, cnt, resp_bytes FROM r WHERE kind != 'ua' OR rn <= {SECURITY_BOT_EFF_UA_TOP_K}"
        )
        try:
            rows = self.execute(sql).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[security_bot_efficiency_rollup] read failed, falling back: %s", e)
            return None
        if not any(r[0] == "site" for r in rows):
            return None
        return [(str(r[0]), str(r[1] or ""), int(r[2] or 0), int(r[3] or 0)) for r in rows]

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
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[perf_ttl_dist_rollup] read failed, falling back: %s", e)
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
        from datetime import UTC, datetime

        from backend.core.rollups._common import (
            _hour_bundled_root,
            _ip_spread_root,
            _is_safe_ident,
        )
        from backend.utils.date_utils import parse_iso_utc

        if durable_serving_blocked(self.src):
            return {}, {}

        def _phase(name: str, ms: float) -> None:
            if _phase_log is not None:
                _phase_log.append({"section": f"ip_spread_rollup:{name}", "time_ms": round(ms, 2)})

        # Defense in depth: same field-name guard the count rollup
        # uses. Stops any future caller from injecting through the
        # fields parameter into the SQL filter below.
        safe_fields = [f for f in fields if _is_safe_ident(f)]
        if not safe_fields:
            return {}, {}

        win = _TopNWindow.build(
            parse_iso_utc(start_time) if start_time else None,
            parse_iso_utc(end_time) if end_time else None,
            datetime.now(UTC),
        )

        _t = time.perf_counter()
        plan = _plan_ip_spread_paths(_hour_bundled_root(self.src), _ip_spread_root(self.src), safe_fields, win)
        if plan is None:
            return {}, {}
        _phase("enumerate", (time.perf_counter() - _t) * 1000.0)
        # No tree yet (cold pool) or nothing in the window: the caller's signal
        # to fall back to the live count(DISTINCT ip) path.
        if not plan:
            return {}, {}

        _t = time.perf_counter()
        try:
            rows = self.execute(_build_ip_spread_sql(plan, safe_fields)).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            # A read failure here drops the caller to its live fallback
            # — never let an ip_spread bug take down the security tab.
            _logger.debug("[ip_spread_rollup] read failed: %s", e)
            return {}, {}
        _phase("read", (time.perf_counter() - _t) * 1000.0)

        _t = time.perf_counter()
        merged, any_capped = _fold_sketches(rows)
        _phase("merge", (time.perf_counter() - _t) * 1000.0)

        return {k: int(v.count()) for k, v in merged.items()}, _spread_meta(safe_fields, plan, any_capped)

    def execute_top_n_batch(
        self,
        fields: list[str],
        table_name: str,
        actual_cols: list[str],
        schema_types: dict[str, str],
        limit: int = 10,
        per_field_limits: dict[str, int] | None = None,
    ) -> tuple[list[tuple], list[str]]:
        """
        Per-field top-N batch over multiple fields. Returns
        (fetchall_results, field_order) where each row is (field, value, count).

        Shape: one ``GROUP BY value ... QUALIFY ROW_NUMBER()`` branch per
        field, stitched together with ``UNION ALL`` and a final
        ``ORDER BY field, c DESC``.

        ``per_field_limits`` overrides ``limit`` for specific fields (e.g.
        ``{"country": 500}``) — each field's own branch gets its own QUALIFY
        cutoff since this function already builds one branch per field.
        There is no reason to widen every OTHER field's branch to the max
        of all overrides just because one field wants more rows; that
        collapse was the 2026-08-03 dashboard over-fetch bug (a single
        country=500 override was applying QUALIFY <= 500 to all ~20
        fields in the batch).

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
        _pfl = per_field_limits or {}

        # Resolve the ACTUAL column types of the temp we're about to query.
        # schema_types is derived from the durable `logs` view, but the live
        # active-hour temp (_create_active_hour_temp_direct) reads raw buffer
        # parquet and preserves its native types — e.g. `status` as INT32 even
        # when the committed DuckLake schema declares it VARCHAR. Trusting
        # schema_types applied the VARCHAR-only `!= ''` empty guard to an
        # INT32 column, which DuckDB rejects ("Could not convert string '' to
        # INT32"). Prefer the temp's real types; fall back to schema_types.
        actual_types: dict[str, str] = {}
        try:
            for r in self.con.execute(f"DESCRIBE SELECT * FROM {table_name} LIMIT 0").fetchall():
                actual_types[r[0]] = r[1]
        except Exception:
            actual_types = {}

        for field in fields:
            if not _is_safe_ident(field):
                continue
            sql_col = field
            col_type = actual_types.get(sql_col) or schema_types.get(sql_col, "VARCHAR")
            field_limit = int(_pfl.get(field, limit))

            # Escape single quotes defensively on field names
            field_lit = escape_sql_literal(field)

            # Simplify projection and filter predicates to allow direct column
            # scan pushdown inside DuckDB, avoiding intermediate subqueries/views.
            if col_type == "VARCHAR":
                branches.append(
                    f"(SELECT '{field_lit}' AS field, \"{sql_col}\" AS value, count(*) AS c "
                    f"FROM {table_name} "
                    f'WHERE "{sql_col}" IS NOT NULL AND "{sql_col}" != \'\' '
                    f"GROUP BY 2 "
                    f"QUALIFY ROW_NUMBER() OVER (ORDER BY c DESC) <= {field_limit})"
                )
            elif field in INT_AGGREGATE_FIELDS:
                branches.append(
                    f"(SELECT '{field_lit}' AS field, CAST(CAST(ROUND(\"{sql_col}\") AS INTEGER) AS VARCHAR) AS value, count(*) AS c "
                    f"FROM {table_name} "
                    f'WHERE "{sql_col}" IS NOT NULL '
                    f"GROUP BY 2 "
                    f"QUALIFY ROW_NUMBER() OVER (ORDER BY c DESC) <= {field_limit})"
                )
            else:
                branches.append(
                    f"(SELECT '{field_lit}' AS field, CAST(\"{sql_col}\" AS VARCHAR) AS value, count(*) AS c "
                    f"FROM {table_name} "
                    f'WHERE "{sql_col}" IS NOT NULL '
                    f"GROUP BY 2 "
                    f"QUALIFY ROW_NUMBER() OVER (ORDER BY c DESC) <= {field_limit})"
                )
            field_order.append(field)

        if not branches:
            return [], []

        q = "\n            UNION ALL\n            ".join(branches) + "\n            ORDER BY field, c DESC"
        return self.execute(q).fetchall(), field_order

    def try_pop_health_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
    ) -> list[tuple] | None:
        """Serve the /api/network/pop-health panel from parquets."""
        from backend.core.rollups._common import POP_HEALTH_BUNDLE_FILENAME

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters, min_hours=24)
        if win is None:
            return None
        st, et = win

        rollup_paths = self._collect_rollup_paths(st, et, POP_HEALTH_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None

        paths_sql = quote_path_list(rollup_paths)

        from datetime import UTC, datetime

        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        from backend.core.rollups import _safe_table_for

        base_table = _safe_table_for(self.src)

        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            et_iso = et.isoformat()
            query = (
                f"SELECT pop,"
                f"  CAST(SUM(requests) AS BIGINT),"
                f"  CAST(SUM(errors) AS BIGINT),"
                f"  CAST(SUM(cache_hits) AS BIGINT),"
                f"  CAST(SUM(bandwidth_bytes) AS BIGINT),"
                f"  CAST(SUM(p50_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE),"
                f"  CAST(SUM(p95_ttfb_ms * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) "
                f"FROM ("
                f"  SELECT pop, requests, errors, cache_hits, bandwidth_bytes, p50_rtt_us, p95_ttfb_ms "
                f"  FROM read_parquet([{paths_sql}]) "
                f"  WHERE pop IS NOT NULL AND pop != '' "
                f"  UNION ALL "
                f"  SELECT pop, CAST(COUNT(*) AS BIGINT) AS requests, "
                f"         CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS BIGINT) AS errors, "
                f"         CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS cache_hits, "
                f"         CAST(SUM(resp_bytes) AS BIGINT) AS bandwidth_bytes, "
                f"         CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS p50_rtt_us, "
                f"         CAST(approx_quantile(ttfb, 0.95) AS DOUBLE) AS p95_ttfb_ms "
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}' "
                f"    AND pop IS NOT NULL AND pop != '' "
                f"  GROUP BY pop"
                f") "
                f"GROUP BY pop"
            )
        else:
            query = (
                f"SELECT "
                f"  pop, "
                f"  CAST(SUM(requests) AS BIGINT), "
                f"  CAST(SUM(errors) AS BIGINT), "
                f"  CAST(SUM(cache_hits) AS BIGINT), "
                f"  CAST(SUM(bandwidth_bytes) AS BIGINT), "
                f"  CAST(SUM(p50_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE), "
                f"  CAST(SUM(p95_ttfb_ms * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE pop IS NOT NULL AND pop != '' "
                f"GROUP BY pop"
            )
        return self.execute(query).fetchall()


# R-1: register the schema + listdir caches so the autouse fixture in
# tests/conftest.py drains them via CacheRegistry.clear_all(). Both
# are hot-path memoes that risk cross-test leak when the same source
# name + temp-table key is reused across tests.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("repositories._base._schema_cols_cache", _schema_cols_cache)
# Per-(service, field) warning-throttle timestamps. Without registration
# a test that triggers an empty-rollup warning leaves entries behind, and
# a later test that expects the warning to fire again is silently throttled.
_CacheRegistry.register("repositories._base._EMPTY_ROLLUP_WARN_TS", _EMPTY_ROLLUP_WARN_TS)
# Same leak class for the missing-hour live-heal warning throttle.
_CacheRegistry.register("repositories._base._MISSING_HOUR_HEAL_WARN_TS", _MISSING_HOUR_HEAL_WARN_TS)
