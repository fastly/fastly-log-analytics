from __future__ import annotations  # noqa: I001

import contextlib
import logging
import re
import sys as _sys
import time
import types as _types
import uuid as _uuid
from collections.abc import Callable, Iterable
from typing import Any

import duckdb
from cachetools import LRUCache

from backend.core import log_fields as metric_expressions
from backend.core.duckdb import escape_sql_literal  # noqa: F401 — re-export
from backend.core.log_fields import Group, in_group  # noqa: F401 — re-export
from backend.core.retry import retry_call
from backend.core.rollups import recompute as rollup_coverage  # noqa: F401 — re-export
from backend.core.rollups._common import (  # noqa: F401 — re-export
    DAY_BUNDLE_FILENAME,
    NETWORK_GEO_BUNDLE_FILENAME,
    NETWORK_HEATMAP_BUNDLE_FILENAME,
    NETWORK_QUALITY_ASN_FILENAME,
    NETWORK_QUALITY_COUNTRY_FILENAME,
    NETWORK_QUALITY_POP_FILENAME,
    NETWORK_QUALITY_REGION_FILENAME,
    NETWORK_RTT_BUNDLE_FILENAME,
    NETWORK_SPEED_BUNDLE_FILENAME,
    NGWAF_BOTS_BUNDLE_FILENAME,
    NGWAF_BOTS_TS_BUNDLE_FILENAME,
    ORIGIN_IP_BUNDLE_FILENAME,
    ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
    ORIGIN_PATH_BUNDLE_FILENAME,
    ORIGIN_POP_BUNDLE_FILENAME,
    ORIGIN_SUMMARY_BUNDLE_FILENAME,
    PERF_TOP_ASNS_BUNDLE_FILENAME,
    PERF_TOP_URLS_BUNDLE_FILENAME,
    PERF_TTL_DIST_BUNDLE_FILENAME,
    POP_HEALTH_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_UA_TOP_K,
    SECURITY_CONN_REUSE_BUNDLE_FILENAME,
    SECURITY_COV_BUNDLE_FILENAME,
    SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
    SECURITY_PROXY_QUALITY_BUNDLE_FILENAME,
    SECURITY_REQ_SIZE_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_FILENAME,
    SLOW_URLS_BUNDLE_FILENAME,
    VERIFIED_BOTS_TS_BUNDLE_FILENAME,
    _VIRTUAL_FIELD_BACKING,
    _build_virtual_field_copy_query,
    _hour_bundled_root,
    _ip_spread_root,
    _is_safe_ident,
    quote_path_list,
)
from backend.core.rollups.recompute import (  # noqa: F401 — re-export
    MISSING_HOUR_HEAL_CAP as _MISSING_HOUR_HEAL_CAP,
    collect_day_prefer_paths,
    collect_hour_paths_with_floor,
    durable_serving_blocked,
    eligible_window,
    heal_where_clause as _heal_where_clause,
    plan_hourly_read,
    plan_time_series_rollup_window,
)
from backend.models.analytics import (  # noqa: F401 — re-export
    HistoryObservation,
    ReaderCoverage,
    RollupPathCoverage,
    ScalarRollupCoverage,
    TopNRollupCoverage,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_day_key_strict, parse_hour_key_strict, parse_iso_utc, safe_iso  # noqa: F401 — re-export
from backend.utils.telemetry import log_once_per  # noqa: F401 — re-export

_logger = logging.getLogger(__name__)

from backend.repositories import rollup_executor as _rollup_executor  # noqa: E402
from backend.repositories.rollup_executor import (  # noqa: E402, F401
    _EDGE_PREFIXES,
    _EMPTY_ROLLUP_WARN_INTERVAL_S,
    _EMPTY_ROLLUP_WARN_TS,
    _LIVE_TOPN_SKIP_FIELDS,
    _MISSING_HOUR_HEAL_WARN_TS,
    _MTIME_SKEW_MARGIN_S,
    CANONICAL_METRICS,
    IpSpreadPlan,
    RollupExecutorMixin,
    SchemaSeeds,
    TopNPaths,
    TopNPlan,
    TopNRow,
    TopNWindow,
    _active_hour_overlap,
    _build_ip_spread_sql,
    _build_rolled_sql,
    _bundled_day_files,
    _bundled_hour_files,
    _day_covered_by_any_field,
    _day_has_real_rollup,
    _drop_temp_table,
    _find_unrolled_hours,
    _fold_rows,
    _fold_sketches,
    _get_schema,
    _has_real_parquet,
    _live_topn_fields,
    _per_field_files,
    _plan_ip_spread_paths,
    _plan_top_n_paths,
    _quote_ident,
    _SchemaSeeds,
    _should_emit_throttled_warn,
    _spread_meta,
    _TopNWindow,
    _truncate_top_n,
    _us_to_ms,
    build_ip_spread_sql,
    build_rolled_sql,
    find_unrolled_hours,
    fold_rows,
    fold_sketches,
    live_topn_fields,
    plan_ip_spread_paths,
    plan_top_n_paths,
    qualify_limit_expr,
    spread_meta,
    truncate_top_n,
)
from backend.repositories.utils.filters import (  # noqa: E402, F401
    _SAFE_COL_RE,
    FilterBuilder,
    FilterClause,
    SqlFragment,
    _get_utc_date_str,
    _get_utc_hour_str,
    _lookup_ngwaf_waf_req_ids,
    build_geo_select_clause,
    build_where_clause,
    calc_offset,
    filter_spec_attr,
    normalize_filter_key,
)
from backend.repositories.utils.response_cache import (  # noqa: E402, F401
    _CLIENT_TOKEN_COUNTER,
    _MEMO_COUNTER,
    _NEVER_CACHE,
    ResponseMemo,
    SingleFlightGroup,
    _analytics_compute_group,
    _InFlight,
    _registry,
    _registry_lock,
    bucket_time_to_minute,
    cache_get,
    cache_put,
    cache_result,
    client_cache_token,
    coalesce,
    coalesced_cache_result,
    digest_cache_key,
    get_analytics_compute_group,
    serialize_filters_for_key,
)


class SectionTimer:
    __slots__ = ("entries",)

    def __init__(self, entries: list[dict] | None = None) -> None:
        self.entries: list[dict] = entries if entries is not None else []

    def mark(self, name: str, t0: float, **observation: Any) -> None:
        self.entries.append({"section": name, "time_ms": round((time.perf_counter() - t0) * 1000, 2), **observation})

    def call(self, name: str, fn):
        t0 = time.perf_counter()
        try:
            return fn()
        finally:
            self.mark(name, t0)


_PARQUET_LIST_RE = re.compile(r"read_parquet\(\[\s*('[^']+'\s*(?:,\s*'[^']+'\s*)*)\]")
_SCHEMA_COLS_CACHE_MAX_ENTRIES = 64
_schema_cols_cache: LRUCache[tuple[str, str], list[str]] = LRUCache(maxsize=_SCHEMA_COLS_CACHE_MAX_ENTRIES)


def _schema_cols_cache_key(src: dict) -> tuple[str, str] | None:
    sid, fmt = src.get("service_id") or src.get("name"), (src.get("log_fields") or {}).get("format_hash")
    return (sid, fmt) if sid and fmt else None


def _compact_sql_for_debug(sql: str) -> str:
    return _PARQUET_LIST_RE.sub(lambda m: f"read_parquet([{m.group(1).count(chr(39)) // 2} files]", sql)


def record_duckdb_query(sql_str: str, elapsed_ms: float, service_id: str | None = None) -> None:
    try:
        from backend.utils.telemetry import get_queries, record_telemetry_query

        compact = _compact_sql_for_debug(" ".join(sql_str.strip().split()))
        get_queries().append({"sql": compact, "time_ms": elapsed_ms, "engine": "DuckDB"})
        record_telemetry_query("duckdb", compact, elapsed_ms, service_id=service_id)
    except Exception:
        _logger.debug("record_duckdb_query failed", exc_info=True)


def ensure_ngwaf_bots_materialized(con: duckdb.DuckDBPyConnection, alias: str) -> bool:
    if con.execute(
        "SELECT 1 FROM duckdb_tables() WHERE table_name = ? AND temporary = true", [f"{alias}_ngwaf_bots"]
    ).fetchone():
        return True
    _q = "SELECT waf_req_id, bot_name, category, wellknown_bot_name FROM ngwaf_bots WHERE bot_name IS NOT NULL"
    try:
        from backend.core.metadata import base as pg_connection

        with contextlib.closing(pg_connection.get_pg_readonly_connection()) as pconn:
            rows = [
                (r["waf_req_id"], r["bot_name"], r["category"], r["wellknown_bot_name"])
                for r in pconn.execute(_q).fetchall()
            ]
    except Exception as e:
        _logger.warning("[ngwaf_bots] postgres cache read failed for %s: %s", alias, e)
        return False
    import pyarrow as pa

    cols = ("waf_req_id", "bot_name", "category", "wellknown_bot_name")
    tbl = pa.table({c: pa.array([r[i] for r in rows], type=pa.string()) for i, c in enumerate(cols)})
    try:
        con.register(f"temp_arrow_{alias}", tbl)
        con.execute(f"CREATE OR REPLACE TEMP TABLE {alias}_ngwaf_bots AS SELECT * FROM temp_arrow_{alias}")
    except Exception as e:
        _logger.warning("[ngwaf_bots] materializing %s_ngwaf_bots failed: %s", alias, e)
        return False
    return True


@contextlib.contextmanager
def attach_ngwaf_cache(con: duckdb.DuckDBPyConnection, schema_cols: list[str], alias: str = "ngwaf_cache"):
    yield False if "waf_req_id" not in schema_cols else ensure_ngwaf_bots_materialized(con, alias)


def _safe_table(name: str) -> str:
    from backend.core.duckdb import _safe_table_name

    return _safe_table_name(name)


def _is_stale_view_error(e: Exception) -> bool:
    msg = str(e)
    if not any(
        s in msg
        for s in (
            "No files found",
            "Catalog Error: Table with name",
            "does not exist",
            "No such file or directory",
            "Cannot open file",
        )
    ):
        return False
    try:
        import duckdb as _duckdb_mod
    except Exception:
        return True
    return isinstance(e, (_duckdb_mod.IOException, _duckdb_mod.CatalogException))


def optional_col(col: str, actual_cols, default: str = "NULL") -> str:
    return _quote_ident(col) if col in actual_cols else default


VALID_CHART_INTERVALS: frozenset[str] = frozenset({"1 second", "1 minute", "1 hour", "1 day"})


def safe_interval(requested: str, default: str = "1 minute") -> str:
    return requested if requested in VALID_CHART_INTERVALS else default


def time_bucket_select(interval: str, ts_col: str = "timestamp") -> str:
    return f"time_bucket(INTERVAL '{safe_interval(interval)}', {ts_col}) AS bucket"


def fast_utc_bucket_select(interval: str, ts_col: str = "timestamp") -> str:
    """Avoid per-row timezone calendar arithmetic for UTC chart buckets."""
    safe = safe_interval(interval, "1 day")
    parts = safe.split()
    if len(parts) == 2 and parts[0] == "1" and parts[1] in {"minute", "hour", "day", "week", "month", "year"}:
        unit = parts[1]
        return f"date_trunc('{unit}', CAST({ts_col} AS TIMESTAMPTZ) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS bucket"
    return time_bucket_select(safe, ts_col)


def percentile_ms_expr(col: str, p: float = 0.95, filter_expr: str = "", *, approx: bool = False) -> str:
    filter_clause = f" FILTER ({filter_expr})" if filter_expr else ""
    if approx:
        return f"approx_quantile({col}, {p}){filter_clause} / 1000.0"
    return f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {col}){filter_clause} / 1000.0"


def origin_latency_us_expr(actual_cols: set[str] | list[str]) -> str:
    expr = metric_expressions.origin_latency_us(actual_cols, absent="NULL")
    if expr is None:
        raise RuntimeError("origin_latency_us returned None despite absent='NULL'")
    return expr


def empty_schema_response(**extra_fields) -> dict:
    return {"has_data": False, "total": 0, **extra_fields}


def get_source_extent(runner: QueryRunner, src: dict, orig_table_name: str) -> tuple[int, str | None, str | None]:
    observed = getattr(runner, "history", None)
    if not isinstance(observed, HistoryObservation):
        observed = HistoryObservation()
    runner.history = observed
    try:
        from backend import config as svcconfig

        cached = svcconfig.get_status(src["name"])
        if cached:
            observed.extent_source, observed.extent_rows = "status_cache", cached.get("local_rows")
            observed.earliest_event_at, observed.latest_event_at = (
                cached.get("earliest_log_at"),
                cached.get("latest_log_at"),
            )
            return (cached.get("local_rows", 0), cached.get("earliest_log_at"), cached.get("latest_log_at"))
        row = runner.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {orig_table_name}").fetchone()
        if row:
            observed.extent_source, observed.extent_rows = "live_query", row[0]
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


def _latest_log_in_window(latest_log_at: str | None, start_time: str | None, end_time: str | None) -> bool:
    if not latest_log_at:
        return False
    try:
        latest_dt = parse_iso_utc(latest_log_at)
    except Exception:
        return False
    if latest_dt is None:
        return False
    for bound, is_start in ((start_time, True), (end_time, False)):
        if bound:
            try:
                b_dt = parse_iso_utc(bound)
                if b_dt is not None and (latest_dt < b_dt if is_start else latest_dt > b_dt):
                    return False
            except Exception:
                _logger.debug("_latest_log_in_window: step failed (ignored)", exc_info=True)
    return True


def should_self_heal_stale_view(
    *, windowed_count: int, filters, latest_log_at: str | None, start_time: str | None, end_time: str | None
) -> bool:
    if windowed_count != 0 or filters:
        return False
    return _latest_log_in_window(latest_log_at, start_time, end_time)


def _refresh_stale_view(con: duckdb.DuckDBPyConnection, src: dict) -> None:
    from backend.core import storage as db_storage

    db_storage.clear_source_caches(src.get("name", "default"), keep_snapshot_cache=True)
    db_storage.update_storage_view(con, src, force=True)


def force_rebuild_view(con: duckdb.DuckDBPyConnection, src: dict) -> None:
    try:
        _refresh_stale_view(con, src)
    except Exception:
        _logger.debug("[self-heal] force view rebuild failed", exc_info=True)


class QueryRunner(RollupExecutorMixin):
    def __init__(self, con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection], src: dict):
        self._con: duckdb.DuckDBPyConnection | None = None
        self._con_factory: Callable[[], duckdb.DuckDBPyConnection] | None = None
        self.con = con  # type: ignore[assignment]
        self.src = src
        self.debug_queries: list[dict] = []
        self.reader_coverage: list[ReaderCoverage] = []
        self.history: HistoryObservation | None = None
        self.actual_cols: set[str] = set()
        self._shared_active_temps: list[tuple[frozenset[str], Any, Any, str]] | None = None
        self._shared_active_depth: int = 0
        self._shared_active_seed_fields: tuple[str, ...] = ()
        self._last_active_direct_n_files: int = 0
        from backend.core.storage import inject_view_debug

        inject_view_debug(self.debug_queries, src)

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None and self._con_factory is not None:
            self._con = self._con_factory()
        return self._con  # type: ignore[return-value]

    @con.setter
    def con(self, value: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection]) -> None:
        if callable(value) and not hasattr(value, "execute"):
            self._con, self._con_factory = None, value
        else:
            self._con, self._con_factory = value, None  # type: ignore[assignment]

    def begin_shared_active_hour_temps(self, seed_fields: Iterable[str] | None = None) -> None:
        if self._shared_active_temps is None:
            self._shared_active_temps, self._shared_active_depth = [], 1
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
        if self._shared_active_depth > 1:
            self._shared_active_depth -= 1
            return
        self._shared_active_depth, self._shared_active_seed_fields = 0, ()
        temps, self._shared_active_temps = self._shared_active_temps, None
        for _cols, _ws, _we, name in temps or []:
            _drop_temp_table(self, name, "end_shared_active_hour_temps")

    def release_active_direct_temp(self, name: str) -> None:
        if self._shared_active_temps is not None and any(n == name for _c, _s, _e, n in self._shared_active_temps):
            return
        _drop_temp_table(self, name, "release_active_direct_temp")

    @property
    def debug_calls(self) -> list[dict]:
        try:
            from backend.utils.telemetry import get_tracked_calls

            return get_tracked_calls()
        except ImportError:
            return []

    def execute(self, q: str, p: list | None = None):
        t0 = time.time()
        try:
            res = self.con.execute(q, p if p is not None else [])
        except Exception as e:
            if not _is_stale_view_error(e):
                raise
            try:
                _refresh_stale_view(self.con, self.src)
            except Exception:
                raise e
            res = self.con.execute(q, p if p is not None else [])
        elapsed_ms = round((time.time() - t0) * 1000, 2)
        compact_sql = _compact_sql_for_debug(q.strip())
        self.debug_queries.append({"sql": compact_sql, "time_ms": elapsed_ms, "engine": "DuckDB"})
        try:
            from backend.utils.telemetry import record_telemetry_query

            record_telemetry_query(
                "duckdb", compact_sql, elapsed_ms, service_id=self.src.get("name") or self.src.get("service_id")
            )
        except Exception:
            _logger.debug("QueryRunner.execute: record_telemetry_query failed", exc_info=True)
        return res

    def get_schema_cols(self) -> list[str]:
        cache_key = _schema_cols_cache_key(self.src)
        if cache_key is not None and cache_key in _schema_cols_cache:
            cached = _schema_cols_cache[cache_key]
            self.actual_cols = set(cached)
            return cached
        actual_cols = [col["name"] for col in _get_schema(self.con, self.src)]
        if actual_cols == ["timestamp"]:
            actual_cols = []
        if not actual_cols:
            try:
                _refresh_stale_view(self.con, self.src)
                actual_cols = [col["name"] for col in _get_schema(self.con, self.src)]
            except Exception:
                _logger.debug("get_schema_cols: step failed (ignored)", exc_info=True)
        self.actual_cols = set(actual_cols)
        if actual_cols and cache_key is not None:
            _schema_cols_cache[cache_key] = actual_cols
        return actual_cols

    def _run_with_stale_retry(self, run):
        refreshed = False

        def _refresh(_exc: BaseException) -> None:
            nonlocal refreshed
            refreshed = True
            from backend.core import storage as db_storage

            db_storage.update_storage_view(self.con, self.src)

        try:
            return True, retry_call(run, retry_on=_is_stale_view_error, attempts=2, before_retry=_refresh)
        except Exception:
            if not refreshed:
                raise
            return False, None

    def execute_with_retry(self, sql: str, params: list | None = None):
        return self._run_with_stale_retry(lambda: self.execute(sql, params))[1]

    def create_temp_table(self, sql: str, params: list | None = None) -> bool:
        return self._run_with_stale_retry(lambda: self.execute(sql, params))[0]

    def telemetry(self) -> dict:
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
        select_cols = [_quote_ident(c) for c in cols if c in actual_cols]
        if not select_cols:
            return None
        temp_name = f"t_{_uuid.uuid4().hex}"
        sql = (
            f"CREATE TEMP TABLE {temp_name} AS SELECT {', '.join(select_cols)} FROM {source_table} WHERE {where_clause}"
        )
        return temp_name if self.create_temp_table(sql, params) else None

    @contextlib.contextmanager
    def temp_table(
        self,
        cols: list[str],
        actual_cols: list[str] | set[str],
        source_table: str,
        where_clause: str,
        params: list | None = None,
    ):
        name = self.create_filtered_temp_table(cols, actual_cols, source_table, where_clause, params)
        try:
            yield name
        finally:
            _drop_temp_table(self, name, "temp_table")


_CacheRegistry.register("repositories._base._schema_cols_cache", _schema_cols_cache)


class _BaseShimModule(_types.ModuleType):
    """Forwards attribute mutations on ``backend.repositories._base`` to ``rollup_executor``."""

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if hasattr(_rollup_executor, name):
            setattr(_rollup_executor, name, value)

    def __delattr__(self, name: str) -> None:
        super().__delattr__(name)
        if hasattr(_rollup_executor, name):
            with contextlib.suppress(AttributeError):
                delattr(_rollup_executor, name)


_sys.modules[__name__].__class__ = _BaseShimModule
