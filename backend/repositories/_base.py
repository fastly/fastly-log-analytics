from __future__ import annotations  # noqa: I001
import contextlib
import logging
import os
import re
import sys as _sys
import time
import uuid as _uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal
import duckdb
from cachetools import LRUCache
from backend.core import metric_expressions
from backend.core.field_registry import Group, in_group
from backend.core.retry import retry_call
from backend.core.rollups._common import (
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
from backend.models.dashboard import (
    HistoryObservation,
    ReaderCoverage,
    RollupPathCoverage,
    ScalarRollupCoverage,
    TopNRollupCoverage,
)
from backend.repositories import rollup_coverage
from backend.repositories.rollup_coverage import (
    MISSING_HOUR_HEAL_CAP as _MISSING_HOUR_HEAL_CAP,
    collect_day_prefer_paths,
    collect_hour_paths_with_floor,
    durable_serving_blocked,
    eligible_window,
    heal_where_clause as _heal_where_clause,
    plan_hourly_read,
    plan_time_series_rollup_window,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_day_key_strict, parse_hour_key_strict, parse_iso_utc, safe_iso  # noqa: F401 — re-export
from backend.utils.sql_validator import escape_sql_literal
from backend.utils.telemetry import log_once_per

if TYPE_CHECKING:
    pass
_logger = logging.getLogger(__name__)
_backend_mod = _sys.modules.get("backend")
if _backend_mod is not None and hasattr(_backend_mod, "_MODULE_ALIASES"):
    for _alias_key in (
        "backend.repositories.utils.response_cache",
        "backend.repositories.utils.filters",
        "backend.repositories.top_n_rollups",
    ):
        _backend_mod._MODULE_ALIASES.pop(_alias_key, None)

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

_EMPTY_ROLLUP_WARN_TS: dict[tuple[str, str], float] = {}
_EMPTY_ROLLUP_WARN_INTERVAL_S = 300.0
_MISSING_HOUR_HEAL_WARN_TS: dict[str, float] = {}


def _should_emit_throttled_warn(
    table: dict[Any, float], key: Any, interval_s: float = _EMPTY_ROLLUP_WARN_INTERVAL_S, max_entries: int = 256
) -> bool:
    now, last = time.monotonic(), table.get(key)
    if last is not None and now - last < interval_s:
        return False
    if len(table) >= max_entries and key not in table:
        table.clear()
    table[key] = now
    return True


def _active_hour_overlap(st: datetime, et: datetime) -> tuple[datetime, datetime, datetime]:
    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    return active_dt, max(active_dt, st), min(active_dt + timedelta(hours=1), et)


_MTIME_SKEW_MARGIN_S = 300
_LIVE_TOPN_SKIP_FIELDS: frozenset[str] = frozenset(
    {
        "waf_req_id",  # per-request NGWAF id (near-unique; joined elsewhere, never a facet)
        "rid",  # request id (near-unique)
        "prid",  # parent request id (near-unique)
        "_source_file",  # internal FOS provenance column
        "timestamp",  # the time axis, not a top-N facet
        "elapsed",  # raw per-request latency (ms); shown via chart metrics, not a facet
        "ttfb",  # raw per-request time-to-first-byte
        "req_bytes",
        "req_header_bytes",
        "resp_bytes",  # raw per-request byte counts (served via the chart-metric temp)
        "lat",
        "lon",  # raw coordinates — geo facets are city/region/country/metro
        "waf_sig",  # raw col backing the virtual waf_sig_ind card (served via _exploded_top_n)
    }
    | {f.code for f in in_group(Group.METRICS)}
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
    from backend import config as svcconfig

    db_path = svcconfig.ngwaf_sqlite_cache_override()
    _q = "SELECT waf_req_id, bot_name, category, wellknown_bot_name FROM ngwaf_bots WHERE bot_name IS NOT NULL"
    if db_path and os.path.exists(db_path):
        import sqlite3

        try:
            with contextlib.closing(sqlite3.connect(db_path, timeout=5)) as sconn:
                rows = sconn.execute(_q).fetchall()
        except Exception as e:
            _logger.warning("[ngwaf_bots] sqlite cache read failed for %s: %s", alias, e)
            return False
    elif db_path:
        return False
    else:
        try:
            from backend.core.metadata import pg_connection

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


def _get_schema(con: duckdb.DuckDBPyConnection, src: dict) -> list[dict]:
    from backend.core.duckdb import get_schema

    return get_schema(con, src, stats=False)


def _us_to_ms(val: Any, ndigits: int | None = None) -> float | None:
    if val is None:
        return None
    ms = float(val) / 1000.0
    return round(ms, ndigits) if ndigits is not None else ms


def _drop_temp_table(executor: Any, temp_table: str | None, label: str = "temp_cleanup") -> None:
    if not temp_table:
        return
    try:
        executor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
    except Exception:
        if log_once_per(f"{label}: DROP TABLE failed (ignored)", 60):
            _logger.debug("%s: DROP TABLE failed (ignored)", label, exc_info=True)


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


def _quote_ident(col: str) -> str:
    return '"{}"'.format(col.replace('"', '""'))


def optional_col(col: str, actual_cols, default: str = "NULL") -> str:
    return _quote_ident(col) if col in actual_cols else default


VALID_CHART_INTERVALS: frozenset[str] = frozenset({"1 second", "1 minute", "1 hour", "1 day"})


def safe_interval(requested: str, default: str = "1 minute") -> str:
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


def time_bucket_select(interval: str, ts_col: str = "timestamp") -> str:
    return f"time_bucket(INTERVAL '{safe_interval(interval)}', {ts_col}) AS bucket"


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


class QueryRunner:
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

    def _create_active_hour_temp_direct(
        self, fields: list[str], actual_cols: list[str] | set[str], live_start: Any, live_end: Any
    ) -> str | None:
        from backend import config as svcconfig
        from backend.core.duckdb import _cache_dir

        try:
            cache_dir = _cache_dir(self.src)
        except Exception:
            return None
        buffer_dir = os.path.join(cache_dir, "buffer")
        active_hour_token = live_start.strftime("%Y-%m-%d-%H")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour_token}")
        requested: list[str] = ["timestamp"]
        for f in fields:
            if f in actual_cols and f not in requested:
                requested.append(f)
        requested_set = frozenset(requested)
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
        mtime_floor = (live_start - timedelta(seconds=_MTIME_SKEW_MARGIN_S)).timestamp()
        try:
            from backend.core.storage.buffer import _tombstoned_parquet_paths

            tombstoned = _tombstoned_parquet_paths(buffer_dir)
        except Exception:
            tombstoned = set()

        def _list_parquets(d: str, *, prune_mtime: bool) -> list[str]:
            out: list[str] = []
            try:
                with os.scandir(d) as it:
                    for e in it:
                        if not e.name.endswith(".parquet") or e.name.startswith(".tmp_") or e.path in tombstoned:
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
        lake_table = None
        try:
            from backend.core.storage._ducklake import ducklake_table_name

            cand = ducklake_table_name(self.src)
            if self.con.execute(
                "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1", [cand]
            ).fetchone():
                lake_table = cand
        except Exception:
            lake_table = None
        hourly_files = [] if durable_serving or lake_table else _list_parquets(hourly_dir, prune_mtime=False)
        self._last_active_direct_n_files = len(buffer_files) + len(hourly_files)
        if not buffer_files and not hourly_files and not lake_table:
            return "__empty__"
        cols_sql = ", ".join(_quote_ident(c) for c in projected)
        where = (
            f"timestamp >= TIMESTAMPTZ '{live_start.isoformat()}' AND timestamp < TIMESTAMPTZ '{live_end.isoformat()}'"
        )

        def _create_sql(union_by_name: str) -> str:
            branches: list[str] = []
            if buffer_files:
                branches.append(
                    f"SELECT * FROM read_parquet([{quote_path_list(buffer_files)}], union_by_name={union_by_name}) WHERE {where}"
                )
            if lake_table:
                branches.append(f"SELECT * FROM lake.{_quote_ident(lake_table)} WHERE {where}")
            if hourly_files:
                branches.append(
                    f"SELECT * FROM read_parquet([{quote_path_list(hourly_files)}], union_by_name={union_by_name}) WHERE {where}"
                )
            joiner = " UNION ALL BY NAME " if union_by_name == "true" else " UNION ALL "
            union_sql = " UNION ALL BY NAME ".join(branches)
            desc = self.con.execute(f"SELECT * FROM ({union_sql}) LIMIT 0").description or []
            physical_cols = {d[0] for d in desc}
            from backend.core.storage.view import _finalize_view_sql

            target_table = "logs"  # QueryRunner active hour direct is only used for logs path right now
            finalized = _finalize_view_sql(
                union_sql,
                self.src,
                target_table=target_table,
                dynamic_schema_field_names=set(),
                existing_cols=physical_cols,
            )
            available_cols = physical_cols | {"timestamp_hour", "dt"}
            valid_projected = [c for c in projected if c in available_cols]
            valid_cols_sql = ", ".join(_quote_ident(c) for c in valid_projected) if valid_projected else "*"
            return f"SELECT {valid_cols_sql} FROM ({finalized})"

        temp_name = f"t_active_direct_{_uuid.uuid4().hex}"
        try:
            self.con.execute(f"CREATE TEMP TABLE {temp_name} AS " + _create_sql("false"))
        except duckdb.InterruptException:
            raise
        except Exception:
            _drop_temp_table(self.con, temp_name, "_create_active_hour_temp_direct")
            try:
                self.con.execute(f"CREATE TEMP TABLE {temp_name} AS " + _create_sql("true"))
            except duckdb.InterruptException:
                raise
            except Exception:
                _drop_temp_table(self.con, temp_name, "_create_active_hour_temp_direct")
                return None
        if self._shared_active_temps is not None:
            self._shared_active_temps.append((projected_set, live_start, live_end, temp_name))
        return temp_name

    def _partial_hour_adjusted_live_start(self, naive_live_start, active_hour_token: str, window_end):
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
        return max(naive_live_start, watermark_dt), partial_rows, partial_total

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
        exact_window: bool = False,
    ) -> tuple[list[tuple[str, Any, int]], list[str]]:
        from backend.core.duckdb import _cache_dir
        from backend.core.rollups import _safe_table_for

        observed = ReaderCoverage(
            reader=_coverage_reader, maximum_window_days=None, eligible=False, outcome="ineligible"
        )
        self.reader_coverage.append(observed)
        if durable_serving_blocked(self.src):
            observed.eligibility_reason = "durable_unready"
            return [], fields

        def _phase(name: str, ms: float) -> None:
            if _phase_log is not None:
                _phase_log.append({"section": f"top_n_rollups:{name}", "time_ms": round(ms, 2)})

        cache_dir = _cache_dir(self.src)
        safe_fields = [f for f in fields if _is_safe_ident(f)]
        if not safe_fields:
            observed.eligibility_reason = "unsafe_fields"
            return [], fields
        base_table = _safe_table_for(self.src)
        if not base_table:
            observed.eligibility_reason = "unsafe_service"
            return [], fields
        st_dt = parse_iso_utc(start_time) if start_time else None
        et_dt = parse_iso_utc(end_time) if end_time else None
        observed.window_start, observed.window_end = (
            (st_dt.isoformat() if st_dt else None),
            (et_dt.isoformat() if et_dt else None),
        )
        observed.eligible, observed.eligibility_reason, observed.outcome = True, "eligible", "unknown"
        observed.top_n_plan = TopNRollupCoverage()
        win = _TopNWindow.build(st_dt, et_dt, datetime.now(UTC))
        active_dt = win.active_dt
        plan_win = win
        if exact_window:
            closed_start = st_dt.replace(minute=0, second=0, microsecond=0) if st_dt else None
            if st_dt is not None and closed_start is not None and st_dt > closed_start:
                closed_start += timedelta(hours=1)
            closed_end = et_dt.replace(minute=0, second=0, microsecond=0) if et_dt else None
            plan_win = _TopNWindow.build(closed_start, closed_end, active_dt)
        _t_dir_enum = time.perf_counter()
        plan = _plan_top_n_paths(cache_dir, safe_fields, plan_win, observation=observed.top_n_plan)
        day_paths, hour_paths = plan.paths.day_paths, plan.paths.hour_paths
        bundled_hour_paths, bundled_day_paths = plan.paths.bundled_hour_paths, plan.paths.bundled_day_paths
        _phase("dir_enum", (time.perf_counter() - _t_dir_enum) * 1000)
        _phase("dir_enum:n_day_files", float(len(day_paths)))
        _phase("dir_enum:n_hour_files", float(len(hour_paths)))
        _phase("dir_enum:n_bundled_hour_files", float(len(bundled_hour_paths)))
        _phase("dir_enum:n_bundled_day_files", float(len(bundled_day_paths)))
        pre_merge_limit = max(limit * 10, 100) if limit > 0 else limit
        pre_merge_per_field_limits = (
            {k: max(v, pre_merge_limit) for k, v in per_field_limits.items()} if per_field_limits else None
        )
        _t_rolled = time.perf_counter()
        rolled_q = _build_rolled_sql(plan.paths, safe_fields, pre_merge_limit, pre_merge_per_field_limits)
        rolled_res: list = []
        if rolled_q is not None:
            try:
                rolled_res = self.execute(rolled_q).fetchall()
                observed.top_n_plan.rolled_read = "used"
            except duckdb.InterruptException:
                raise
            except Exception:
                observed.top_n_plan.rolled_read = "failed"
                if exact_window:
                    raise
                rolled_res = []
        _phase("rolled_res", (time.perf_counter() - _t_rolled) * 1000)
        schema = _SchemaSeeds(actual_cols, schema_types)
        if exact_window:
            real_fields = [field for field in safe_fields if field not in _VIRTUAL_FIELD_BACKING]
            _t_live = time.perf_counter()
            live_res, partial_rows = self._top_n_live_active_hour(
                win,
                real_fields,
                base_table,
                pre_merge_limit,
                pre_merge_per_field_limits,
                schema,
                _phase,
                observation=observed,
                fail_on_error=True,
            )
            _phase("live_active_hour", (time.perf_counter() - _t_live) * 1000)
            missing_hours = _find_unrolled_hours(
                plan, plan_win, safe_fields, _MISSING_HOUR_HEAL_CAP, observation=observed.top_n_plan
            )
            _t_raw = time.perf_counter()
            raw_res = self._top_n_exact_window_rows(
                win, missing_hours, safe_fields, base_table, pre_merge_limit, pre_merge_per_field_limits, schema
            )
            _phase("raw_boundaries", (time.perf_counter() - _t_raw) * 1000)
            if missing_hours:
                observed.closed_hour_heal = "used"
            top_results, _ = _truncate_top_n(
                _fold_rows(rolled_res, raw_res, live_res, [row for row in partial_rows if row[0] in real_fields]),
                fields,
                limit,
                per_field_limits,
            )
            observed.outcome = "served"
            return top_results, fields
        _t_live = time.perf_counter()
        live_res, partial_rows = self._top_n_live_active_hour(
            win,
            safe_fields,
            base_table,
            pre_merge_limit,
            pre_merge_per_field_limits,
            schema,
            _phase,
            observation=observed,
        )
        _phase("live_active_hour", (time.perf_counter() - _t_live) * 1000)
        _t_heal = time.perf_counter()
        missing_hours = _find_unrolled_hours(
            plan, win, safe_fields, _MISSING_HOUR_HEAL_CAP, observation=observed.top_n_plan
        )
        heal_res = (
            self._top_n_heal_hours(
                missing_hours,
                win,
                safe_fields,
                base_table,
                pre_merge_limit,
                pre_merge_per_field_limits,
                schema,
                observation=observed,
            )
            if missing_hours
            else []
        )
        _phase("live_missing_hours", (time.perf_counter() - _t_heal) * 1000)
        _phase("live_missing_hours:n_hours", float(len(missing_hours)))
        _t_merge = time.perf_counter()
        by_field = _fold_rows(rolled_res, live_res, heal_res, partial_rows)
        top_results, empty_fields = _truncate_top_n(by_field, fields, limit, per_field_limits)
        _service_key = self.src.get("name") or self.src.get("service_id") or "default"
        for field in empty_fields:
            if field in _LIVE_TOPN_SKIP_FIELDS:
                continue
            if _should_emit_throttled_warn(_EMPTY_ROLLUP_WARN_TS, (_service_key, field)):
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

    def _top_n_exact_window_rows(
        self,
        win: _TopNWindow,
        missing_hours: list[str],
        fields: list[str],
        base_table: str,
        limit: int,
        per_field_limits: dict[str, int] | None,
        schema: _SchemaSeeds,
    ) -> list[tuple]:
        raw_hours = set(missing_hours)
        for bound in (win.st_dt, win.et_dt):
            if bound is not None:
                hour = bound.replace(minute=0, second=0, microsecond=0)
                if hour < win.active_dt and bound != hour:
                    raw_hours.add(hour.strftime("%Y-%m-%d-%H"))
        where = _heal_where_clause(sorted(raw_hours), win.st_dt, win.et_dt) if raw_hours else "FALSE"
        virtual_where = where
        if (win.et_dt is None or win.et_dt > win.active_dt) and (win.st_dt is None or win.st_dt < win.active_dt_end):
            virtual_where = _heal_where_clause(sorted(raw_hours | {win.active_str}), win.st_dt, win.et_dt)
        # Dashboard raw reads include the endpoint; hourly aggregates are half-open.
        if win.et_dt is not None:
            where = f"({where}) OR timestamp = TIMESTAMPTZ '{win.et_dt.isoformat()}'"
            virtual_where = f"({virtual_where}) OR timestamp = TIMESTAMPTZ '{win.et_dt.isoformat()}'"
        if where == "FALSE" and virtual_where == "FALSE":
            return []
        schema.ensure(self.get_schema_cols, lambda: {c["name"]: c["type"] for c in _get_schema(self.con, self.src)})
        scoped_table = f"(SELECT * FROM {base_table} WHERE {where})"
        real_fields = [f for f in fields if f in schema.cols]
        rows, _ = self.execute_top_n_batch(
            real_fields, scoped_table, schema.cols, schema.types, limit=limit, per_field_limits=per_field_limits
        )
        for field in fields:
            backing = _VIRTUAL_FIELD_BACKING.get(field)
            if backing is None or backing not in schema.cols:
                continue
            query = _build_virtual_field_copy_query(base_table, field, backing, virtual_where)
            rows.extend(
                self.execute(f"SELECT field, value, SUM(count) AS c FROM ({query}) GROUP BY field, value").fetchall()
            )
        return rows

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
        fail_on_error: bool = False,
    ) -> tuple[list[tuple], list[tuple]]:
        st_dt, et_dt, active_dt = win.st_dt, win.et_dt, win.active_dt
        live_res: list[tuple] = []
        partial_rows: list[tuple] = []
        if observation is not None:
            observation.live_top_up = "outside_window"
        live_start = max(active_dt, st_dt) if st_dt else active_dt
        live_end = min(win.active_dt_end, et_dt) if et_dt else win.active_dt_end
        if (et_dt and et_dt <= active_dt) or (st_dt and st_dt >= win.active_dt_end):
            return live_res, partial_rows
        try:
            schema.ensure(self.get_schema_cols, lambda: {c["name"]: c["type"] for c in _get_schema(self.con, self.src)})
            actual_cols, schema_types = schema.cols, schema.types
            live_topn_fields = _live_topn_fields(safe_fields, _LIVE_TOPN_SKIP_FIELDS, self.src.get("log_fields"))
            live_start, partial_rows, _partial_total = self._partial_hour_adjusted_live_start(
                live_start, win.active_str, live_end
            )
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
                    _n_live = 0
                    try:
                        _n_live = self.execute(f'SELECT COUNT(*) FROM "{tmp_name}"').fetchone()[0]
                        _phase("live_active_hour:n_rows", float(_n_live))
                    except Exception:
                        _n_live = 1
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
                    self.release_active_direct_temp(tmp_name)
        except duckdb.InterruptException:
            raise
        except Exception:
            if observation is not None:
                observation.live_top_up = "failed"
            if fail_on_error:
                raise
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
        heal_res: list[tuple] = []
        if observation is not None:
            observation.closed_hour_heal = "unavailable"
        _svc_key = self.src.get("name") or self.src.get("service_id") or "default"
        if _should_emit_throttled_warn(_MISSING_HOUR_HEAL_WARN_TS, _svc_key):
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
                    _drop_temp_table(self, heal_tmp, "execute_top_n_rollups")
        except duckdb.InterruptException:
            raise
        except Exception:
            if observation is not None:
                observation.closed_hour_heal = "failed"
            _logger.debug("[top_n_rollups] missing-hour live heal failed", exc_info=True)
        return heal_res

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
            "live_cols": ("status"),
        },
        "4xx": {
            "kind": "rate",
            "num_rollup": "SUM(status_4xx)",
            "den_rollup": "SUM(requests)",
            "num_live": "COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499)",
            "den_live": "COUNT(*)",
            "live_cols": ("status"),
        },
        "hit_rate": {
            "kind": "rate",
            "num_rollup": "SUM(hits)",
            "den_rollup": "SUM(requests)",
            "num_live": "COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE'))",
            "den_live": "COUNT(*)",
            "live_cols": ("cache"),
        },
    }
    _TS_ROLLUP_INTERVALS: frozenset[str] = frozenset({"1 minute", "1 hour", "1 day"})
    _HOUR_START_TS_SQL_EXPR: str = (
        "TRY_CAST(CASE WHEN LENGTH(CAST(hour_start AS VARCHAR)) = 13 "
        "THEN CAST(hour_start AS VARCHAR) || ':00:00Z' "
        "ELSE CAST(hour_start AS VARCHAR) END AS TIMESTAMPTZ)"
    )

    def _plan_scalar_rollup_window(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        can_use_overview: bool,
        require_minute_aligned: bool,
        observed: ReaderCoverage,
    ) -> tuple[datetime, datetime, Any, str, str, str, str | None] | None:
        if not start_time or not end_time:
            observed.eligibility_reason = "bounds_missing"
            return None
        st, et = parse_iso_utc(start_time), parse_iso_utc(end_time)
        if st is None or et is None or et <= st:
            observed.eligibility_reason = "invalid_window"
            return None
        observed.window_start, observed.window_end = st.isoformat(), et.isoformat()
        if require_minute_aligned and (st.second or st.microsecond or et.second or et.microsecond):
            observed.eligibility_reason = "unsupported_interval"
            return None
        if (et - st) > timedelta(days=366):
            observed.eligibility_reason = "window_too_long"
            return None
        observed.eligible, observed.eligibility_reason, observed.scalar_plan = True, "eligible", ScalarRollupCoverage()
        wplan = plan_time_series_rollup_window(
            self.src,
            st,
            et,
            can_use_overview=can_use_overview,
            coverage_mod=rollup_coverage,
            plan_hourly_read_fn=plan_hourly_read,
            observation=observed.scalar_plan,
        )
        if wplan is None:
            observed.outcome = "no_coverage"
            return None
        observed.outcome, observed.live_top_up = "unknown", "outside_window"
        heal_base_table: str | None = None
        if wplan.missing_hours:
            from backend.core.rollups import _safe_table_for

            heal_base_table = _safe_table_for(self.src)
            observed.closed_hour_heal = "planned" if heal_base_table else "unavailable"
        st_tz = st.astimezone(UTC).isoformat()
        st_hour_tz = st.replace(minute=0, second=0, microsecond=0).astimezone(UTC).isoformat()
        et_tz = et.astimezone(UTC).isoformat()
        return st, et, wplan, st_tz, st_hour_tz, et_tz, heal_base_table

    def _resolve_scalar_live_source(
        self,
        st: datetime,
        et: datetime,
        wplan: Any,
        table_name: str,
        where_clause: str,
        observed: ReaderCoverage,
        *,
        adjust_partial_hour: bool,
        unfiltered_window: bool,
        get_direct_tmp: Callable[[datetime, datetime], str | None],
    ) -> tuple[str | None, int, str | None, bool]:
        live_start, live_end = max(st, wplan.active_hour_dt), et
        if adjust_partial_hour:
            live_start, _partial_rows, partial_total = self._partial_hour_adjusted_live_start(
                live_start, wplan.active_hour_str, live_end
            )
        else:
            partial_total = 0
        live_st_tz, live_et_tz = live_start.astimezone(UTC).isoformat(), live_end.astimezone(UTC).isoformat()
        observed.live_start, observed.live_end = live_st_tz, live_et_tz
        direct_live_tmp: str | None = None
        live_needs_params = False
        if unfiltered_window:
            try:
                direct_live_tmp = get_direct_tmp(live_start, live_end)
            except Exception:
                direct_live_tmp = None
        if direct_live_tmp == "__empty__":
            live_source, live_where = None, "1=1"
        elif direct_live_tmp is not None:
            live_source, live_where = direct_live_tmp, "1=1"
        else:
            live_source, live_where, live_needs_params = table_name, where_clause, True
        observed.live_top_up = "empty" if live_source is None else "unverified"
        live_from_where = (
            f"FROM {live_source} "
            f"WHERE {live_where} "
            f"  AND timestamp >= TIMESTAMPTZ '{live_st_tz}' "
            f"  AND timestamp <  TIMESTAMPTZ '{live_et_tz}'"
            if live_source is not None
            else None
        )
        return live_from_where, partial_total, direct_live_tmp, live_needs_params

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
        observed = ReaderCoverage(reader="dashboard.time_series", eligible=False, outcome="ineligible")
        self.reader_coverage.append(observed)
        if chart_metric not in self._TS_ROLLUP_METRIC_PARTS:
            observed.eligibility_reason = "unsupported_metric"
            return None
        if interval not in self._TS_ROLLUP_INTERVALS:
            observed.eligibility_reason = "unsupported_interval"
            return None
        planned = self._plan_scalar_rollup_window(
            start_time,
            end_time,
            can_use_overview=chart_metric == "requests" and interval in ("1 hour", "1 day"),
            require_minute_aligned=True,
            observed=observed,
        )
        if planned is None:
            return None
        st, et, wplan, st_tz, st_hour_tz, et_tz, heal_base_table = planned
        parts = self._TS_ROLLUP_METRIC_PARTS[chart_metric]
        select_clauses: list[str] = []
        if wplan.rollup_paths:
            paths_sql = quote_path_list(wplan.rollup_paths)
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', bucket) AS out_bucket, "
                f"       {parts['num_rollup']} AS num, {parts['den_rollup']} AS den "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE bucket >= TIMESTAMPTZ '{st_tz}' "
                f"  AND bucket < TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY 1"
            )
        if wplan.day_ov_paths:
            ov_paths_sql = quote_path_list(wplan.day_ov_paths)
            hour_ts_expr = self._HOUR_START_TS_SQL_EXPR
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', {hour_ts_expr}) AS out_bucket, "
                f"       {parts['num_rollup']} AS num, {parts['den_rollup']} AS den "
                f"FROM read_parquet([{ov_paths_sql}]) "
                f"WHERE {hour_ts_expr} >= TIMESTAMPTZ '{st_hour_tz}' "
                f"  AND {hour_ts_expr} < TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY 1"
            )
        if heal_base_table:
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS out_bucket, "
                f"       {parts['num_live']} AS num, {parts['den_live']} AS den "
                f"FROM {heal_base_table} "
                f"WHERE {wplan.heal_where_sql} "
                f"GROUP BY 1"
            )
        direct_live_tmp: str | None = None
        live_needs_params = False
        if wplan.crosses_active:
            live_cols = parts["live_cols"]

            def _make_ts_direct_tmp(l_start: datetime, l_end: datetime) -> str | None:
                _cols: list[str] = self.get_schema_cols() if live_cols else []
                return (
                    self._create_active_hour_temp_direct(list(live_cols), _cols, l_start, l_end)
                    if all(c in _cols for c in live_cols)
                    else None
                )

            live_from_where, partial_total, direct_live_tmp, live_needs_params = self._resolve_scalar_live_source(
                st,
                et,
                wplan,
                table_name,
                where_clause,
                observed,
                adjust_partial_hour=(chart_metric == "requests"),
                unfiltered_window=unfiltered_window,
                get_direct_tmp=_make_ts_direct_tmp,
            )
            if live_from_where is not None:
                select_clauses.append(
                    f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS out_bucket, "
                    f"       {parts['num_live']} AS num, {parts['den_live']} AS den "
                    f"{live_from_where} "
                    f"GROUP BY 1"
                )
            if partial_total:
                partial_bucket_tz = wplan.active_hour_dt.astimezone(UTC).isoformat()
                select_clauses.append(
                    f"SELECT TIMESTAMPTZ '{partial_bucket_tz}' AS out_bucket, "
                    f"       {partial_total} AS num, {partial_total} AS den"
                )
        if not select_clauses:
            observed.outcome = "served"
            return []
        value_expr = (
            "CAST(SUM(num) AS BIGINT)"
            if parts["kind"] == "count"
            else "ROUND(SUM(num) * 100.0 / NULLIF(SUM(den), 0), 2)"
        )
        unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
        final_sql = (
            f"SELECT out_bucket, {value_expr} AS value "
            f"FROM ({unioned}) "
            f"WHERE out_bucket IS NOT NULL "
            f"GROUP BY out_bucket "
            f"ORDER BY out_bucket"
        )
        rows = self._execute_scalar_rollup_sql(
            final_sql,
            params,
            live_needs_params=live_needs_params,
            direct_live_tmp=direct_live_tmp,
            observed=observed,
            log_tag="time_series_rollup",
        )
        if rows is None:
            return None
        out = [
            {"time": safe_iso(r[0]), "value": float(r[1]) if r[1] is not None else 0.0}
            for r in rows
            if r[0] is not None
        ]
        observed.outcome = "served"
        return out

    def _execute_scalar_rollup_sql(
        self,
        final_sql: str,
        params: list,
        *,
        live_needs_params: bool,
        direct_live_tmp: str | None,
        observed: ReaderCoverage,
        log_tag: str,
        fetchone_int: bool = False,
    ) -> Any:
        try:
            cur = self.execute(final_sql, params if live_needs_params else [])
            res = int(cur.fetchone()[0]) if fetchone_int else cur.fetchall()
            if observed.live_top_up == "unverified":
                observed.live_top_up = "view_fallback" if live_needs_params else "used"
            if observed.closed_hour_heal == "planned":
                observed.closed_hour_heal = "used"
            return res
        except duckdb.InterruptException:
            raise
        except Exception if fetchone_int else duckdb.Error as e:
            _logger.debug("[%s] read failed, falling back to raw: %s", log_tag, e)
            observed.outcome = "read_failed"
            if observed.closed_hour_heal == "planned":
                observed.closed_hour_heal = "failed"
            return None
        finally:
            if direct_live_tmp is not None:
                self.release_active_direct_temp(direct_live_tmp)

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
        observed = ReaderCoverage(reader="dashboard.count", eligible=False, outcome="ineligible")
        self.reader_coverage.append(observed)
        if not unfiltered_window:
            observed.eligibility_reason = "filtered"
            return None
        planned = self._plan_scalar_rollup_window(
            start_time, end_time, can_use_overview=True, require_minute_aligned=True, observed=observed
        )
        if planned is None:
            return None
        st, et, wplan, st_tz, st_hour_tz, et_tz, heal_base_table = planned
        select_clauses: list[str] = []
        if wplan.rollup_paths:
            paths_sql = quote_path_list(wplan.rollup_paths)
            select_clauses.append(
                f"SELECT {metric_expressions.sum_or_zero('requests')} AS num "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE bucket >= TIMESTAMPTZ '{st_tz}' "
                f"  AND bucket < TIMESTAMPTZ '{et_tz}'"
            )
        if wplan.day_ov_paths:
            ov_paths_sql = quote_path_list(wplan.day_ov_paths)
            hour_ts_expr = self._HOUR_START_TS_SQL_EXPR
            select_clauses.append(
                f"SELECT {metric_expressions.sum_or_zero('requests')} AS num "
                f"FROM read_parquet([{ov_paths_sql}]) "
                f"WHERE {hour_ts_expr} >= TIMESTAMPTZ '{st_hour_tz}' "
                f"  AND {hour_ts_expr} < TIMESTAMPTZ '{et_tz}'"
            )
        if heal_base_table:
            select_clauses.append(f"SELECT COUNT(*) AS num FROM {heal_base_table} WHERE {wplan.heal_where_sql}")
        direct_live_tmp: str | None = None
        live_needs_params = False
        if wplan.crosses_active:
            live_from_where, partial_total, direct_live_tmp, live_needs_params = self._resolve_scalar_live_source(
                st,
                et,
                wplan,
                table_name,
                where_clause,
                observed,
                adjust_partial_hour=True,
                unfiltered_window=unfiltered_window,
                get_direct_tmp=lambda l_start, l_end: self._create_active_hour_temp_direct(
                    projected_fields or [], actual_cols or [], l_start, l_end
                ),
            )
            if live_from_where is not None:
                select_clauses.append(f"SELECT COUNT(*) AS num {live_from_where}")
            if partial_total:
                select_clauses.append(f"SELECT {partial_total} AS num")
        if not select_clauses:
            observed.outcome = "served"
            return 0
        unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
        final_sql = f"SELECT {metric_expressions.sum_or_zero('num')} FROM ({unioned})"
        result = self._execute_scalar_rollup_sql(
            final_sql,
            params,
            live_needs_params=live_needs_params,
            direct_live_tmp=direct_live_tmp,
            observed=observed,
            log_tag="count_rollup",
            fetchone_int=True,
        )
        if result is not None:
            observed.outcome = "served"
        return result

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
        return eligible_window(
            start_time,
            end_time,
            has_filters=has_filters,
            require_top_asns=require_top_asns,
            min_hours=min_hours if min_hours is not None else self._SLOW_URLS_ROLLUP_MIN_HOURS,
            observation=observation,
        )

    def try_slow_urls_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, min_requests: int, limit: int
    ) -> dict | None:
        if durable_serving_blocked(self.src):
            return None
        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win
        rollup_paths = collect_hour_paths_with_floor(self.src, st, et, SLOW_URLS_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT url, "
            f"       {self._WEIGHTED_P50_P95_SELECT_SQL}, "
            f"       SUM(p99_us * requests) / NULLIF(SUM(requests), 0) AS p99_us_w "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY url "
            f"HAVING SUM(requests) >= ? "
            f"ORDER BY p95_us_w DESC "
            f"LIMIT ?"
        )
        rows = self._exec_rollup(sql, [min_requests, limit], tag="slow_urls_rollup")
        if rows is None:
            return None
        out_rows = [self._shape_latency_row("url", r, p99_ms=_us_to_ms(r[4])) for r in rows]
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
        return collect_day_prefer_paths(
            self.src,
            st,
            et,
            bundle_filename,
            skip_partial_start=skip_partial_start,
            allow_empty_with_done=allow_empty_with_done,
            observation=observation,
        )

    def _rollup_paths_sql(
        self,
        start_time: str | None,
        end_time: str | None,
        bundle_filename: str,
        *,
        has_filters: bool,
        require_top_asns: list[int] | None = None,
        min_hours: int | None = None,
        skip_partial_start: bool = True,
        allow_empty_with_done: bool = False,
    ) -> str | None:
        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=require_top_asns, min_hours=min_hours
        )
        if win is None:
            return None
        rollup_paths = self._collect_rollup_paths(
            win[0],
            win[1],
            bundle_filename,
            skip_partial_start=skip_partial_start,
            allow_empty_with_done=allow_empty_with_done,
        )
        return quote_path_list(rollup_paths) if rollup_paths is not None else None

    def _exec_rollup(self, sql: str, params: list[Any] | None = None, *, tag: str, fetchone: bool = False) -> Any:
        try:
            cur = self.execute(sql, params) if params is not None else self.execute(sql)
            return cur.fetchone() if fetchone else cur.fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[%s] read failed, falling back to raw: %s", tag, e)
            return None

    def try_origin_summary_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, actual_cols: set[str] | list[str]
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(
            start_time, end_time, ORIGIN_SUMMARY_BUNDLE_FILENAME, has_filters=has_filters
        )
        if paths_sql is None:
            return None
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
        row = self._exec_rollup(sql, tag="origin_summary_rollup", fetchone=True)
        if row is None or row[0] is None or row[0] == 0:
            return None
        cols_set = set(actual_cols)
        if row[4] is None:
            return None
        has_ottlb = "ottlb" in cols_set
        return {
            "has_data": True,
            "total_misses": int(row[1]) if row[1] is not None else None,
            "total_passes": int(row[2]) if row[2] is not None else None,
            "ottfb_p50_ms": _us_to_ms(row[4]),
            "ottfb_p75_ms": _us_to_ms(row[5]),
            "ottfb_p95_ms": _us_to_ms(row[6]),
            "ottfb_p99_ms": _us_to_ms(row[7]),
            "ottlb_p50_ms": _us_to_ms(row[8]) if has_ottlb else None,
            "ottlb_p95_ms": _us_to_ms(row[9]) if has_ottlb else None,
            "cdn_overhead_p50_ms": _us_to_ms(row[10]) if (has_ottlb and "elapsed" in cols_set) else None,
            "origin_error_rate": float(row[11]) if (row[11] is not None and "ost" in cols_set) else None,
            "obytes_p50": float(row[12]) if (row[12] is not None and "obytes" in cols_set) else None,
            "_approx": True,
        }

    _WEIGHTED_P50_P95_SELECT_SQL = (
        "CAST(SUM(requests) AS BIGINT) AS requests, "
        "       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) AS p50_us_w, "
        "       SUM(p95_us * requests) / NULLIF(SUM(requests), 0) AS p95_us_w"
    )

    @staticmethod
    def _shape_latency_row(key_name: str, r: Any, **extra: Any) -> dict[str, Any]:
        return {
            key_name: r[0],
            "requests": int(r[1] or 0),
            "p50_ms": _us_to_ms(r[2]),
            "p95_ms": _us_to_ms(r[3]),
            **extra,
        }

    def try_origin_pop_latency_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, limit: int
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, ORIGIN_POP_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = (
            f"SELECT pop, "
            f"       {self._WEIGHTED_P50_P95_SELECT_SQL} "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY pop "
            f"ORDER BY p95_us_w DESC "
            f"LIMIT ?"
        )
        rows = self._exec_rollup(sql, [limit], tag="origin_pop_rollup")
        if not rows:
            return None
        valid_p95s = sorted(_us_to_ms(r[3]) for r in rows if r[3] is not None)  # type: ignore[type-var]
        median_p95 = valid_p95s[len(valid_p95s) // 2] if valid_p95s else 0
        out_rows = [
            self._shape_latency_row(
                "pop",
                r,
                elevated=(p95_ms := _us_to_ms(r[3])) is not None and median_p95 is not None and p95_ms > median_p95 * 2,
            )
            for r in rows
        ]
        return {
            "has_data": True,
            "requires_group_c": False,
            "median_p95_ms": median_p95,
            "rows": out_rows,
            "_approx": True,
        }

    def try_origin_ip_health_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, limit: int
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, ORIGIN_IP_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = (
            f"SELECT oip, "
            f"       {self._WEIGHTED_P50_P95_SELECT_SQL}, "
            f"       ROUND(SUM(ost_5xx_count) * 100.0 / NULLIF(SUM(ost_total_count), 0), 1) AS error_pct "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY oip "
            f"HAVING SUM(requests) >= 10 "
            f"ORDER BY error_pct DESC "
            f"LIMIT ?"
        )
        rows = self._exec_rollup(sql, [limit], tag="origin_ip_rollup")
        if not rows:
            return None
        out_rows = [
            self._shape_latency_row("oip", r, error_pct=float(r[4]) if r[4] is not None else None) for r in rows
        ]
        return {"has_data": len(out_rows) > 0, "rows": out_rows, "_approx": True}

    def try_origin_path_breakdown_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, ORIGIN_PATH_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = f"SELECT edge,        {self._WEIGHTED_P50_P95_SELECT_SQL} FROM read_parquet([{paths_sql}]) GROUP BY edge"
        rows = self._exec_rollup(sql, tag="origin_path_rollup")
        if not rows:
            return None
        out_rows = [self._shape_latency_row("edge", r) for r in rows]
        return {
            "has_data": len(out_rows) > 0,
            "shielding_detected": any(r[0] is False for r in rows),
            "rows": out_rows,
            "_approx": True,
        }

    def try_origin_status_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, DAY_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
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
        rows = self._exec_rollup(sql, tag="origin_status_rollup")
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
        if split_by_leg or metric not in ("ttfb", "ttlb") or percentile not in ("p50", "p95", "p99"):
            return None
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
        plan = plan_hourly_read(self.src, st, et, ORIGIN_LATENCY_TS_BUNDLE_FILENAME)
        if plan is None:
            return None
        rollup_paths, crosses_active = plan.paths, plan.crosses_active
        cnt_col, pcol = f"{metric}_count", f"{metric}_{percentile}_us"
        st_tz, et_tz = st.astimezone(UTC).isoformat(), et.astimezone(UTC).isoformat()
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
            _active_hour_start, live_start, _ = _active_hour_overlap(st, et)
            live_st_tz, live_et_tz = live_start.astimezone(UTC).isoformat(), et.astimezone(UTC).isoformat()
            lat = self._origin_ts_live_lat_expr(metric, set(self.get_schema_cols()))
            if lat is None:
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
        rows = self._exec_rollup(final_sql, params if crosses_active else [], tag="origin_latency_ts_rollup")
        if rows is None:
            return None
        series = [
            {
                "time": safe_iso(r[0]),
                "miss_count": int(r[2]) if r[2] is not None else 0,
                "value": float(r[1]) if r[1] is not None else None,
            }
            for r in rows
            if r[0] is not None
        ]
        return {"has_data": len(series) > 0, "series": series, "_approx": True}

    @staticmethod
    def _origin_ts_live_lat_expr(metric: str, cols: set[str]) -> str | None:
        if metric == "ttfb":
            return metric_expressions.origin_latency_us(cols, absent=None)
        return '"ottlb"' if "ottlb" in cols else None

    def try_network_quality_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, region_country: str
    ) -> dict[str, Any] | None:
        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=None, min_hours=24
        )
        if win is None:
            return None
        st, et = win

        def _run_dim(filename: str, filter_sql: str = "", filter_params: list = None) -> list[dict] | None:
            rollup_paths = self._collect_rollup_paths(st, et, filename)
            if not rollup_paths:
                return None
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
            by_country = _run_dim(NETWORK_QUALITY_COUNTRY_FILENAME)
            by_asn = _run_dim(NETWORK_QUALITY_ASN_FILENAME) if by_country is not None else None
            by_pop = _run_dim(NETWORK_QUALITY_POP_FILENAME) if by_asn is not None else None
            by_region = (
                _run_dim(NETWORK_QUALITY_REGION_FILENAME, "AND country = ?", [region_country])
                if by_pop is not None
                else None
            )
            if by_country is None or by_asn is None or by_pop is None or by_region is None:
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
        self, start_time: str | None, end_time: str | None, *, top_asns: list[int], has_filters: bool
    ) -> dict[int, dict[str, float | None]] | None:
        paths_sql = self._rollup_paths_sql(
            start_time,
            end_time,
            NETWORK_RTT_BUNDLE_FILENAME,
            has_filters=has_filters,
            require_top_asns=top_asns,
            min_hours=24,
        )
        if paths_sql is None:
            return None
        asn_placeholders = ", ".join(["?"] * len(top_asns))
        sql = (
            f"SELECT asn, "
            f"  SUM(p95_us * rtt_count) / NULLIF(SUM(rtt_count), 0) AS p95_us, "
            f"  SUM(p99_us * rtt_count) / NULLIF(SUM(rtt_count), 0) AS p99_us "
            f"FROM read_parquet([{paths_sql}]) "
            f"WHERE asn IN ({asn_placeholders}) "
            f"GROUP BY asn"
        )
        rows = self._exec_rollup(sql, top_asns, tag="network_rtt_rollup")
        if rows is None:
            return None
        return {
            int(row[0]): {
                "p95_rtt_us": round(float(row[1]), 0) if row[1] is not None else None,
                "p99_rtt_us": round(float(row[2]), 0) if row[2] is not None else None,
            }
            for row in rows
        }

    def _rollup_window_and_paths_with_active_hour(
        self,
        start_time: str | None,
        end_time: str | None,
        bundle_filename: str,
        *,
        has_filters: bool,
        require_top_asns: list[int] | None = None,
        min_hours: int = 24,
    ) -> tuple[datetime, datetime, str, datetime, str | None] | None:
        from backend.core.rollups import _safe_table_for

        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=require_top_asns, min_hours=min_hours
        )
        if win is None:
            return None
        st, et = win
        rollup_paths = self._collect_rollup_paths(st, et, bundle_filename)
        if rollup_paths is None:
            return None
        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        return st, et, quote_path_list(rollup_paths), active_hour_start, _safe_table_for(self.src)

    def try_network_speed_from_rollup(
        self, start_time: str | None, end_time: str | None, *, top_asns: list[int], has_filters: bool
    ) -> list[tuple[int, str, int]] | None:
        resolved = self._rollup_window_and_paths_with_active_hour(
            start_time, end_time, NETWORK_SPEED_BUNDLE_FILENAME, has_filters=has_filters, require_top_asns=top_asns
        )
        if resolved is None:
            return None
        _st, et, paths_sql, active_hour_start, base_table = resolved
        asn_placeholders = ", ".join(["?"] * len(top_asns))
        if et > active_hour_start:
            ah_iso, et_iso = active_hour_start.isoformat(), et.isoformat()
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
            top_asns = top_asns + top_asns
        else:
            sql = (
                f"SELECT asn, c_speed, CAST(SUM(count) AS BIGINT) AS cnt "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE asn IN ({asn_placeholders}) "
                f"GROUP BY asn, c_speed "
                f"ORDER BY asn, cnt DESC"
            )
        rows = self._exec_rollup(sql, top_asns, tag="network_speed_rollup")
        return [(int(r[0]), str(r[1]), int(r[2])) for r in rows] if rows is not None else None

    def try_network_heatmap_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, bucket_seconds: int = 3600
    ) -> list | None:
        if bucket_seconds > 3600:
            return None
        resolved = self._rollup_window_and_paths_with_active_hour(
            start_time, end_time, NETWORK_HEATMAP_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        st, et, paths_sql, active_hour_start, base_table = resolved
        st_iso, et_iso = st.isoformat(), et.isoformat()
        rtt_min_coal = metric_expressions.coalesce("rtt_min_p50_us", "rtt_p50_us")
        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            active_duration_secs = max(1, (et - active_hour_start).total_seconds())
            cols = set(self.get_schema_cols())
            has_rtt_min = "rtt_min" in cols
            rtt_base = (
                "CAST(approx_quantile(rtt_min, 0.5) AS DOUBLE)"
                if has_rtt_min
                else "CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE)"
            )
            rtt_cong = (
                f"CAST(approx_quantile(tcp_rtt, 0.5) AS BIGINT) - CAST({metric_expressions.coalesce('approx_quantile(rtt_min, 0.5)', 'approx_quantile(tcp_rtt, 0.5)')} AS BIGINT)"
                if has_rtt_min
                else "0::BIGINT"
            )
            ploss_expr = "CAST(AVG(ploss) AS DOUBLE)" if "ploss" in cols else "0::DOUBLE"
            jitter_expr = "CAST(approx_quantile(rtt_var, 0.5) AS DOUBLE)" if "rtt_var" in cols else "0::DOUBLE"
            sql = (
                f"SELECT * FROM (  SELECT    asn,    hour_ts                                                             AS bucket_ts,"
                f"    resp_bytes_sum / 3600.0                                            AS throughput_bps,"
                f"    rtt_p50_us                                                         AS rtt_med_us,"
                f"    rtt_min_p50_us                                                     AS rtt_baseline_us,"
                f"    CAST(rtt_p50_us AS BIGINT) - CAST({rtt_min_coal} AS BIGINT) AS rtt_congestion_us,"
                f"    ploss_sum / NULLIF(ploss_count, 0)                                AS avg_ploss,"
                f"    rtt_var_p50_us                                                     AS jitter_us,"
                f"    errors * 100.0 / NULLIF(reqs, 0)                                  AS error_pct,"
                f"    CAST(reqs AS BIGINT)                                               AS reqs"
                f"   FROM read_parquet([{paths_sql}])   WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"   UNION ALL    SELECT    asn,    TIMESTAMPTZ '{ah_iso}' AS bucket_ts,"
                f"    CAST(SUM(resp_bytes) AS DOUBLE) / {active_duration_secs} AS throughput_bps,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,    {rtt_base} AS rtt_baseline_us,"
                f"    {rtt_cong} AS rtt_congestion_us,    {ploss_expr} AS avg_ploss,    {jitter_expr} AS jitter_us,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs   FROM {base_table} "
                f"   WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'     AND asn IS NOT NULL   GROUP BY asn) ORDER BY reqs DESC"
            )
        else:
            sql = (
                f"SELECT  asn,  hour_ts                                                             AS bucket_ts,"
                f"  resp_bytes_sum / 3600.0                                            AS throughput_bps,"
                f"  rtt_p50_us                                                         AS rtt_med_us,"
                f"  rtt_min_p50_us                                                     AS rtt_baseline_us,"
                f"  CAST(rtt_p50_us AS BIGINT) - CAST({rtt_min_coal} AS BIGINT) AS rtt_congestion_us,"
                f"  ploss_sum / NULLIF(ploss_count, 0)                                AS avg_ploss,"
                f"  rtt_var_p50_us                                                     AS jitter_us,"
                f"  errors * 100.0 / NULLIF(reqs, 0)                                  AS error_pct,"
                f"  CAST(reqs AS BIGINT)                                               AS reqs"
                f" FROM read_parquet([{paths_sql}]) WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}' ORDER BY reqs DESC"
            )
        rows = self._exec_rollup(sql, tag="network_heatmap_rollup")
        return list(rows) if rows else None

    def try_network_geo_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        map_asn: str = "all",
        has_filters: bool,
        bucket_seconds: int = 3600,
    ) -> tuple[list, list] | None:
        if int(bucket_seconds) > 3600 or map_asn != "all":
            return None
        resolved = self._rollup_window_and_paths_with_active_hour(
            start_time, end_time, NETWORK_GEO_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        st, et, paths_sql, active_hour_start, base_table = resolved
        st_iso, et_iso = st.isoformat(), et.isoformat()
        cols = set(self.get_schema_cols())
        if "country" not in cols:
            return None
        has_city, has_lat, has_metro = "city" in cols, ("lat" in cols and "lon" in cols), "metro" in cols
        city_col = "city" if has_city else "CAST('' AS VARCHAR)"
        lat_col = "CAST(lat AS DOUBLE)" if has_lat else "CAST(NULL AS DOUBLE)"
        lon_col = "CAST(lon AS DOUBLE)" if has_lat else "CAST(NULL AS DOUBLE)"
        metro_col = "metro" if has_metro else "CAST(NULL AS VARCHAR)"
        city_grp, lat_grp, lon_grp, metro_grp = (
            ("city" if has_city else "''"),
            ("lat" if has_lat else "NULL"),
            ("lon" if has_lat else "NULL"),
            ("metro" if has_metro else "NULL"),
        )
        ploss_live_expr = "CAST(AVG(ploss) AS DOUBLE)" if "ploss" in cols else "0::DOUBLE"
        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            map_sql = (
                f"SELECT * FROM (  SELECT    country, city, lat, lon, metro, hour_ts AS bucket_ts,"
                f"    rtt_sum / NULLIF(rtt_count, 0) AS rtt_med_us,    ploss_sum / NULLIF(ploss_count, 0) AS avg_ploss,"
                f"    errors * 100.0 / NULLIF(reqs, 0) AS error_pct,    CAST(reqs AS BIGINT) AS reqs"
                f"  FROM read_parquet([{paths_sql}])  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"  UNION ALL   SELECT    country,    {city_col} AS city,    {lat_col} AS lat,    {lon_col} AS lon,"
                f"    {metro_col} AS metro,    TIMESTAMPTZ '{ah_iso}' AS bucket_ts,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,    {ploss_live_expr} AS avg_ploss,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'    AND country IS NOT NULL AND country != ''"
                f"  GROUP BY country, {city_grp}, {lat_grp}, {lon_grp}, {metro_grp}) ORDER BY bucket_ts, reqs DESC LIMIT 5000"
            )
            metro_sql = (
                f"SELECT country, city, CAST('' AS VARCHAR) AS region, metro, "
                f"  CAST(SUM(rtt_med_us * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS rtt_med_us,"
                f"  CAST(SUM(avg_ploss * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS avg_ploss,"
                f"  CAST(SUM(error_pct * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS error_pct,"
                f"  CAST(SUM(reqs) AS BIGINT) AS total_reqs FROM (  SELECT country, city, metro,"
                f"    SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0) AS rtt_med_us,    SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0) AS avg_ploss,"
                f"    SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0) AS error_pct,    CAST(SUM(reqs) AS BIGINT) AS reqs"
                f"  FROM read_parquet([{paths_sql}])  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"  GROUP BY country, city, metro  UNION ALL   SELECT country, {city_col} AS city, {metro_col} AS metro,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,    {ploss_live_expr} AS avg_ploss,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'    AND country IS NOT NULL AND country != ''"
                f"  GROUP BY country, {city_grp}, {metro_grp}) GROUP BY country, city, metro ORDER BY total_reqs DESC LIMIT 100"
            )
        else:
            map_sql = (
                f"SELECT  country,  city,  lat,  lon,  metro,"
                f"  hour_ts                                           AS bucket_ts,"
                f"  rtt_sum / NULLIF(rtt_count, 0)                  AS rtt_med_us,"
                f"  ploss_sum / NULLIF(ploss_count, 0)              AS avg_ploss,"
                f"  errors * 100.0 / NULLIF(reqs, 0)                AS error_pct,"
                f"  CAST(reqs AS BIGINT)                             AS reqs"
                f" FROM read_parquet([{paths_sql}]) WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}' ORDER BY hour_ts, reqs DESC LIMIT 5000"
            )
            metro_sql = (
                f"SELECT  country,  city,  CAST('' AS VARCHAR)                                   AS region,  metro,"
                f"  SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0)             AS rtt_med_us,"
                f"  SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0)         AS avg_ploss,"
                f"  SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0)           AS error_pct,"
                f"  CAST(SUM(reqs) AS BIGINT)                             AS total_reqs"
                f" FROM read_parquet([{paths_sql}]) WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f" GROUP BY country, city, metro ORDER BY total_reqs DESC LIMIT 100"
            )
        try:
            map_rows = self.execute(map_sql).fetchall()
            metro_rows = self.execute(metro_sql).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[network_geo_rollup] read failed, falling back: %s", e)
            return None
        return (list(map_rows), list(metro_rows)) if (map_rows or metro_rows) else None

    def _minute_ts_rollup_window_and_paths(
        self,
        start_time: str | None,
        end_time: str | None,
        bucket_seconds: int,
        bundle_filename: str,
        *,
        has_filters: bool,
    ) -> tuple[int, datetime, datetime, str] | None:
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
        rollup_paths = self._collect_rollup_paths(st, et, bundle_filename, skip_partial_start=False)
        return (n, st, et, quote_path_list(rollup_paths)) if rollup_paths is not None else None

    def try_verified_bots_ts_from_rollup(
        self, start_time: str | None, end_time: str | None, *, temp_table: str, bucket_seconds: int, has_filters: bool
    ) -> list[tuple] | None:
        resolved = self._minute_ts_rollup_window_and_paths(
            start_time, end_time, bucket_seconds, VERIFIED_BOTS_TS_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        n, st, et, paths_sql = resolved
        active_iso = datetime.now(UTC).replace(minute=0, second=0, microsecond=0).isoformat()
        st_iso, et_iso = st.isoformat(), et.isoformat()
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
        rows = self._exec_rollup(sql, tag="verified_bots_ts_rollup")
        return [(r[0], str(r[1]), int(r[2])) for r in rows] if rows is not None else None

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
        filename = PERF_TOP_URLS_BUNDLE_FILENAME if dimension == "url" else PERF_TOP_ASNS_BUNDLE_FILENAME
        sort_col = {"avg": "avg_us", "p50": "p50_us_w", "p95": "p95_us_w", "p99": "p99_us_w"}.get(sort_by, "p99_us_w")
        paths_sql = self._rollup_paths_sql(start_time, end_time, filename, has_filters=has_filters)
        if paths_sql is None:
            return None
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
        rows = self._exec_rollup(sql, [min_requests, limit], tag="perf_latency_rollup")
        if rows is None:
            return None
        out_rows = [
            {
                "value": r[0],
                "requests": int(r[1] or 0),
                "avg_ms": _us_to_ms(r[2]),
                "p50_ms": _us_to_ms(r[3]),
                "p95_ms": _us_to_ms(r[4]),
                "p99_ms": _us_to_ms(r[5]),
            }
            for r in rows
        ]
        return {"rows": out_rows, "_approx": True}

    def _try_security_bucket_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, bundle_filename: str, log_tag: str
    ) -> list[dict] | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, bundle_filename, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = (
            f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS c, MIN(min_val) AS mv "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bucket "
            f"ORDER BY mv"
        )
        rows = self._exec_rollup(sql, tag=log_tag)
        return [{"bucket": r[0], "count": int(r[1] or 0)} for r in rows] if rows else None

    def try_security_req_size_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        return self._try_security_bucket_rollup(
            start_time,
            end_time,
            has_filters=has_filters,
            bundle_filename=SECURITY_REQ_SIZE_BUNDLE_FILENAME,
            log_tag="security_req_size_rollup",
        )

    def try_security_conn_reuse_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        return self._try_security_bucket_rollup(
            start_time,
            end_time,
            has_filters=has_filters,
            bundle_filename=SECURITY_CONN_REUSE_BUNDLE_FILENAME,
            log_tag="security_conn_reuse_rollup",
        )

    _CONN_REUSE_TO_DASHBOARD_BUCKET: dict[str, str] = {
        "1 (None)": "1",
        "2-5": "2–5",
        "6-20": "6–20",
        "21-100": "21+",
        ">100": "21+",
    }
    _CONN_REQUESTS_BUCKET_ORDER: tuple[str, ...] = ("1", "2–5", "6–20", "21+")

    def _begin_observed_rollup_window(
        self,
        reader_name: Literal[
            "dashboard.conn_requests_histogram",
            "dashboard.ngwaf_bots",
            "dashboard.count",
            "dashboard.time_series",
            "dashboard.top_n",
            "dashboard.ua_bots",
        ],
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        min_hours: int | None = 2,
    ) -> tuple[ReaderCoverage, tuple[datetime, datetime] | None]:
        observed = ReaderCoverage(reader=reader_name, minimum_window_hours=min_hours)
        self.reader_coverage.append(observed)
        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, min_hours=min_hours, observation=observed
        )
        if win is None:
            observed.outcome = "ineligible"
        return observed, win

    def _collect_observed_rollup_paths(
        self,
        observed: ReaderCoverage,
        st: datetime,
        et: datetime,
        bundle_name: Literal["security_conn_reuse", "all_fields", "ngwaf_bots"],
        filename: str,
    ) -> list[str] | None:
        attempt = RollupPathCoverage(bundle=bundle_name)
        observed.attempts.append(attempt)
        return self._collect_rollup_paths(st, et, filename, observation=attempt)

    def try_conn_requests_hist_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        actual_cols: list[str] | set[str] | None = None,
    ) -> dict | None:
        observed, win = self._begin_observed_rollup_window(
            "dashboard.conn_requests_histogram", start_time, end_time, has_filters=has_filters, min_hours=2
        )
        if win is None:
            return None
        st, et = win
        rollup_paths = self._collect_observed_rollup_paths(
            observed, st, et, "security_conn_reuse", SECURITY_CONN_REUSE_BUNDLE_FILENAME
        )
        use_all_fields = rollup_paths is None
        if use_all_fields:
            rollup_paths = self._collect_observed_rollup_paths(observed, st, et, "all_fields", DAY_BUNDLE_FILENAME)
            if rollup_paths is None:
                observed.outcome = "no_coverage"
                return None
        if rollup_paths is None:
            raise RuntimeError("rollup_paths is unexpectedly None")
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            "SELECT value, CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
            "WHERE field = 'conn_requests' GROUP BY value"
            if use_all_fields
            else f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS c FROM read_parquet([{paths_sql}]) GROUP BY bucket"
        )
        rows = self._exec_rollup(sql, tag="conn_requests_hist_rollup")
        if rows is None:
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
        from backend.repositories import dashboard as _dash_sql

        for bucket, cnt in self._fetch_active_hour_live_rows(
            st,
            et,
            required_col="conn_requests",
            build_sql=lambda tmp: _dash_sql.CONN_REQUESTS_BUCKET.format(
                requests_metric=CANONICAL_METRICS["requests"], table_name=tmp, where_clause="1=1"
            ),
            log_tag="conn_requests_hist_rollup",
            actual_cols=actual_cols,
            observed=observed,
        ):
            counts[bucket] = counts.get(bucket, 0) + int(cnt or 0)
        known = [b for b in self._CONN_REQUESTS_BUCKET_ORDER if b in counts]
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
        if has_filters or not start_time or not end_time:
            return None
        st, et = parse_iso_utc(start_time), parse_iso_utc(end_time)
        if st is None or et is None:
            return None
        paths = self._collect_rollup_paths(st, et, DAY_BUNDLE_FILENAME)
        if paths is None:
            return None
        paths_sql = quote_path_list(paths)
        rows = self._exec_rollup(
            "SELECT value, CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
            "WHERE field = ? GROUP BY value ORDER BY c DESC",
            [virtual_id],
            tag="virtual_field_top_n_rollup",
        )
        if rows is None:
            return None
        counts: dict[str, int] = {}
        for value, count in rows:
            counts[str(value)] = counts.get(str(value), 0) + int(count or 0)
        _active_dt, live_start, live_end = _active_hour_overlap(st, et)
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

    def _fetch_active_hour_live_rows(
        self,
        st: datetime,
        et: datetime,
        *,
        required_col: str,
        build_sql: Callable[[str], str],
        log_tag: str,
        actual_cols: list[str] | set[str] | None = None,
        observed: ReaderCoverage | None = None,
    ) -> list[tuple]:
        _active_dt, live_start, live_end = _active_hour_overlap(st, et)
        if observed is not None:
            observed.live_top_up = "outside_window"
        if live_start >= live_end:
            return []
        if observed is not None:
            observed.live_start, observed.live_end = live_start.isoformat(), live_end.isoformat()
            observed.live_top_up = "missing_column"
        cols = actual_cols if actual_cols is not None else self.get_schema_cols()
        if required_col not in cols:
            return []
        tmp = self._create_active_hour_temp_direct([required_col], cols, live_start, live_end)
        if observed is not None:
            observed.live_top_up = "unavailable"
        if tmp == "__empty__":
            if observed is not None:
                observed.live_top_up = "empty"
            return []
        if tmp is None:
            return []
        try:
            rows = self.execute(build_sql(tmp)).fetchall()
            if observed is not None:
                observed.live_top_up = "used"
            return rows
        except duckdb.Error as e:
            if observed is not None:
                observed.live_top_up = "failed"
            _logger.debug("[%s] live top-up failed (skipped): %s", log_tag, e)
            return []
        finally:
            self.release_active_direct_temp(tmp)

    def _ngwaf_active_hour_live_rows(
        self,
        st: datetime,
        et: datetime,
        *,
        select_cols_sql: str,
        cache_table: str,
        group_by_sql: str,
        log_tag: str,
        observed: ReaderCoverage | None = None,
    ) -> list[tuple]:
        return self._fetch_active_hour_live_rows(
            st,
            et,
            required_col="waf_req_id",
            build_sql=lambda tmp: (
                f"SELECT {select_cols_sql}, CAST(COUNT(*) AS BIGINT) AS c "
                f'FROM "{tmp}" t '
                f"INNER JOIN temp.{cache_table} nb USING (waf_req_id) "
                f"WHERE nb.bot_name IS NOT NULL "
                f"GROUP BY {group_by_sql}"
            ),
            log_tag=log_tag,
            observed=observed,
        )

    def _merge_ngwaf_rollup_and_live(
        self,
        st: datetime,
        et: datetime,
        *,
        sql: str,
        select_cols_sql: str,
        cache_table: str,
        group_by_sql: str,
        log_tag: str,
        observed: ReaderCoverage | None = None,
    ) -> list[tuple] | None:
        rows = self._exec_rollup(sql, tag=log_tag)
        if rows is None:
            if observed is not None:
                observed.outcome = "read_failed"
            return None
        return [
            *rows,
            *self._ngwaf_active_hour_live_rows(
                st,
                et,
                select_cols_sql=select_cols_sql,
                cache_table=cache_table,
                group_by_sql=group_by_sql,
                log_tag=log_tag,
                observed=observed,
            ),
        ]

    @staticmethod
    def _accumulate_row_counts(rows: list[tuple], key_fn: Callable[[tuple], Any]) -> dict[Any, int]:
        counts: dict[Any, int] = {}
        for row in rows:
            key = key_fn(row)
            counts[key] = counts.get(key, 0) + int(row[-1] or 0)
        return counts

    def try_ngwaf_top_bots_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, n: int
    ) -> list[dict] | None:
        observed, win = self._begin_observed_rollup_window(
            "dashboard.ngwaf_bots", start_time, end_time, has_filters=has_filters, min_hours=2
        )
        if win is None:
            return None
        st, et = win
        rollup_paths = self._collect_observed_rollup_paths(observed, st, et, "ngwaf_bots", NGWAF_BOTS_BUNDLE_FILENAME)
        if rollup_paths is None:
            observed.outcome = "no_coverage"
            return None
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT bot_name, category, CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bot_name, category"
        )
        combined = self._merge_ngwaf_rollup_and_live(
            st,
            et,
            sql=sql,
            select_cols_sql="nb.bot_name, nb.category",
            cache_table="ngwaf_top_ngwaf_bots",
            group_by_sql="1, 2",
            log_tag="ngwaf_bots_rollup",
            observed=observed,
        )
        if combined is None:
            return None
        counts = self._accumulate_row_counts(combined, lambda r: (r[0], r[1]))
        ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[: max(0, int(n))]
        observed.outcome = "served"
        return [{"name": bn, "category": cat, "request_count": c} for (bn, cat), c in ranked]

    def try_ngwaf_verified_bots_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        import pyarrow.parquet as pq

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
        combined = self._merge_ngwaf_rollup_and_live(
            st,
            et,
            sql=sql,
            select_cols_sql="nb.bot_name, nb.wellknown_bot_name, nb.category",
            cache_table="ngwaf_cache_ngwaf_bots",
            group_by_sql="1, 2, 3",
            log_tag="ngwaf_verified_bots_rollup",
        )
        if combined is None:
            return None
        counts = self._accumulate_row_counts(combined, lambda r: (r[0], r[1], r[2]))
        ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
        return [
            {"bot_name": bn, "wellknown_bot_name": wk, "category": cat, "request_count": c}
            for (bn, wk, cat), c in ranked
        ]

    def try_ngwaf_verified_bots_ts_from_rollup(
        self, start_time: str | None, end_time: str | None, *, bucket_seconds: int, has_filters: bool
    ) -> list[tuple] | None:
        resolved = self._minute_ts_rollup_window_and_paths(
            start_time, end_time, bucket_seconds, NGWAF_BOTS_TS_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        n, st, et, paths_sql = resolved
        st_iso, et_iso = st.isoformat(), et.isoformat()
        sql = (
            f"SELECT time_bucket(INTERVAL '{n} seconds', bucket_ts) AS bucket, "
            f"       bot_name, "
            f"       CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}]) "
            f"WHERE bucket_ts >= TIMESTAMPTZ '{st_iso}' AND bucket_ts < TIMESTAMPTZ '{et_iso}' "
            f"GROUP BY 1, 2"
        )
        combined = self._merge_ngwaf_rollup_and_live(
            st,
            et,
            sql=sql,
            select_cols_sql=f"time_bucket(INTERVAL '{n} seconds', t.timestamp) AS bucket, nb.bot_name",
            cache_table="ngwaf_cache_ngwaf_bots",
            group_by_sql="1, 2",
            log_tag="ngwaf_verified_bots_ts_rollup",
        )
        if combined is None:
            return None
        counts = self._accumulate_row_counts(combined, lambda r: (r[0], str(r[1])))
        return [(b, bn, c) for (b, bn), c in sorted(counts.items())]

    def try_security_top_ips_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        paths_sql = self._rollup_paths_sql(
            start_time, end_time, SECURITY_TOPIPS_BUNDLE_FILENAME, has_filters=has_filters
        )
        if paths_sql is None:
            return None
        sql = f"SELECT ip, CAST(MAX(max_header) AS BIGINT) AS mh FROM read_parquet([{paths_sql}]) GROUP BY ip ORDER BY mh DESC LIMIT 10"
        rows = self._exec_rollup(sql, tag="security_top_ips_rollup")
        return [{"ip": r[0], "max_header": int(r[1] or 0)} for r in rows] if rows else None

    def try_security_coverage_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> tuple[int, int] | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, SECURITY_COV_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        row = self._exec_rollup(
            f"SELECT SUM(total_rows), SUM(tls_populated) FROM read_parquet([{paths_sql}])",
            tag="security_coverage_rollup",
            fetchone=True,
        )
        if row is None or row[0] is None or int(row[0]) == 0:
            return None
        return int(row[0]), int(row[1] or 0)

    def try_security_proxies_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> dict | None:
        from backend.utils.pop_utils import build_pop_lat_case_sql, build_pop_lon_case_sql

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
            f"WITH f AS MATERIALIZED ("
            f"  SELECT ip, asn, pop, rtt_min, tcp_rtt, lat, lon, country, city, SUM(cnt) AS cnt "
            f"  FROM read_parquet([{quote_path_list(flagged_paths)}]) "
            f"  WHERE ip IS NOT NULL AND ip != '' GROUP BY ALL"
            f"), pops AS MATERIALIZED ("
            f"  SELECT pop AS p_code, {build_pop_lat_case_sql()} AS pop_lat, {build_pop_lon_case_sql()} AS pop_lon "
            f"  FROM (SELECT DISTINCT pop FROM f)"
            f"), geo AS MATERIALIZED ("
            f"  SELECT g.pop AS g_pop, g.lat AS g_lat, g.lon AS g_lon, "
            f"         6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(g.lat - p.pop_lat) / 2), 2) "
            f"           + COS(RADIANS(p.pop_lat)) * COS(RADIANS(g.lat)) * POWER(SIN(RADIANS(g.lon - p.pop_lon) / 2), 2))) AS distance_km "
            f"  FROM (SELECT DISTINCT pop, lat, lon FROM f WHERE lat IS NOT NULL AND lon IS NOT NULL) g "
            f"  LEFT JOIN pops p ON g.pop IS NOT DISTINCT FROM p.p_code"
            f"), m AS MATERIALIZED ("
            f"  SELECT f.ip, f.asn, f.pop, f.rtt_min, f.tcp_rtt, f.rtt_min / 1000.0 AS rtt_min_ms, "
            f"         f.tcp_rtt / 1000.0 AS tcp_rtt_ms, f.lat AS client_lat, f.lon AS client_lon, "
            f"         p.pop_lat AS pop_lat, p.pop_lon AS pop_lon, geo.distance_km AS distance_km, f.country, f.city, f.cnt "
            f"  FROM f LEFT JOIN pops p ON f.pop IS NOT DISTINCT FROM p.p_code "
            f"  LEFT JOIN geo ON f.pop IS NOT DISTINCT FROM geo.g_pop AND f.lat IS NOT DISTINCT FROM geo.g_lat AND f.lon IS NOT DISTINCT FROM geo.g_lon"
            f") "
            if flagged_paths
            else ""
        )
        dist_flag = "distance_km > (rtt_min_ms * 100.0 + 150.0)"
        tunnel_flag = "tcp_rtt > 0 AND (CAST(rtt_min_ms * 1000.0 AS DOUBLE) / tcp_rtt) < 0.1"
        try:
            quality_rows = self.execute(
                f"SELECT type, CAST(SUM(count) AS BIGINT) AS c FROM read_parquet([{quote_path_list(quality_paths)}]) GROUP BY type ORDER BY c DESC"
            ).fetchall()
            if not quality_rows:
                return None
            stats_row = (0, 0, 0)
            isp_rows: list[tuple] = []
            client_rows: list[tuple] = []
            if flagged_paths:
                combined_rows = self.execute(
                    prefix + f"SELECT 'stats' AS _sec, "
                    f"  COUNT(DISTINCT CASE WHEN {dist_flag} THEN ip WHEN {tunnel_flag} THEN ip ELSE NULL END) AS c1, "
                    f"  CAST({metric_expressions.sum_or_zero(f'CASE WHEN {dist_flag} THEN cnt WHEN {tunnel_flag} THEN cnt ELSE NULL END')} AS BIGINT) AS c2, "
                    f"  COUNT(DISTINCT CASE WHEN {dist_flag} THEN ip ELSE NULL END) AS c3, "
                    f"  NULL::VARCHAR AS ip, NULL::BIGINT AS asn, NULL::DOUBLE AS rtt_min_ms, "
                    f"  NULL::DOUBLE AS tcp_rtt_ms, NULL::DOUBLE AS distance_km, NULL::VARCHAR AS pop, "
                    f"  NULL::DOUBLE AS client_lat, NULL::DOUBLE AS client_lon, NULL::DOUBLE AS pop_lat, "
                    f"  NULL::DOUBLE AS pop_lon, NULL::BOOLEAN AS impossible_distance, "
                    f"  NULL::VARCHAR AS risk_level, NULL::VARCHAR AS country, NULL::VARCHAR AS city "
                    f"FROM m UNION ALL SELECT * FROM ("
                    f"  SELECT 'isp' AS _sec, NULL::BIGINT, CAST(SUM(cnt) AS BIGINT) AS c2, NULL::BIGINT, "
                    f"    NULL::VARCHAR, asn, NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, "
                    f"    NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::BOOLEAN, "
                    f"    NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR FROM m "
                    f"  WHERE tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.1 GROUP BY asn ORDER BY c2 DESC LIMIT 10"
                    f") UNION ALL SELECT * FROM ("
                    f"  SELECT 'client' AS _sec, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, "
                    f"    ip, asn, ROUND(rtt_min_ms, 2), ROUND(tcp_rtt_ms, 2), ROUND(distance_km, 1), pop, "
                    f"    ROUND(client_lat, 4), ROUND(client_lon, 4), CAST(pop_lat AS DOUBLE), CAST(pop_lon AS DOUBLE), "
                    f"    CASE WHEN {dist_flag} THEN true ELSE false END AS impossible_distance, "
                    f"    CASE WHEN {dist_flag} THEN 'High' WHEN tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.1 THEN 'High' "
                    f"         WHEN tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.2 THEN 'Medium' ELSE 'Low' END AS risk_level, country, city "
                    f"  FROM m WHERE rtt_min_ms IS NOT NULL AND (({dist_flag}) OR (tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.2)) "
                    f"  ORDER BY CASE risk_level WHEN 'High' THEN 3 WHEN 'Medium' THEN 2 ELSE 1 END DESC, distance_km DESC LIMIT 100"
                    f")"
                ).fetchall()
                for r in combined_rows:
                    if r[0] == "stats":
                        stats_row = (r[1], r[2], r[3])
                    elif r[0] == "isp":
                        isp_rows.append((r[5], r[2]))
                    elif r[0] == "client":
                        client_rows.append(r[4:])
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[security_proxies_rollup] read failed, falling back: %s", e)
            return None
        _client_keys = (
            "ip",
            "asn",
            "rtt_min_ms",
            "tcp_rtt_ms",
            "distance_km",
            "pop",
            "client_lat",
            "client_lon",
            "pop_lat",
            "pop_lon",
            "impossible_distance",
            "risk_level",
            "country",
            "city",
        )
        return {
            "stats": {
                "active_proxies_count": stats_row[0] or 0,
                "total_requests_count": stats_row[1] or 0,
                "distance_mismatches_count": stats_row[2] or 0,
            },
            "quality_rows": [(r[0], r[1]) for r in quality_rows],
            "isps": [{"asn": r[0], "count": r[1]} for r in isp_rows],
            "clients": [dict(zip(_client_keys, r, strict=False)) for r in client_rows],
        }

    def try_security_bot_efficiency_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[tuple[str, str, int, int]] | None:
        paths_sql = self._rollup_paths_sql(
            start_time, end_time, SECURITY_BOT_EFF_BUNDLE_FILENAME, has_filters=has_filters, allow_empty_with_done=True
        )
        if paths_sql is None:
            return None
        sql = (
            f"WITH g AS (SELECT kind, key, CAST(SUM(cnt) AS BIGINT) AS cnt, CAST(SUM(resp_bytes) AS BIGINT) AS resp_bytes "
            f"  FROM read_parquet([{paths_sql}]) GROUP BY kind, key), "
            f"r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY kind ORDER BY resp_bytes DESC, cnt DESC) AS rn FROM g) "
            f"SELECT kind, key, cnt, resp_bytes FROM r WHERE kind != 'ua' OR rn <= {SECURITY_BOT_EFF_UA_TOP_K}"
        )
        rows = self._exec_rollup(sql, tag="security_bot_efficiency_rollup")
        if not rows or not any(r[0] == "site" for r in rows):
            return None
        return [(str(r[0]), str(r[1] or ""), int(r[2] or 0), int(r[3] or 0)) for r in rows]

    def try_perf_ttl_dist_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, PERF_TTL_DIST_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS count FROM read_parquet([{paths_sql}]) GROUP BY bucket ORDER BY MIN(min_ttl)"
        rows = self._exec_rollup(sql, tag="perf_ttl_dist_rollup")
        return [{"bucket": r[0], "count": int(r[1])} for r in rows] if rows is not None else None

    def execute_ip_spread_rollups(
        self, fields: list[str], start_time: str | None, end_time: str | None, *, _phase_log: list[dict] | None = None
    ) -> tuple[dict[tuple[str, str], int], dict[str, dict]]:
        if durable_serving_blocked(self.src):
            return {}, {}

        def _phase(name: str, ms: float) -> None:
            if _phase_log is not None:
                _phase_log.append({"section": f"ip_spread_rollup:{name}", "time_ms": round(ms, 2)})

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
        if not plan:
            return {}, {}
        _t = time.perf_counter()
        try:
            rows = self.execute(_build_ip_spread_sql(plan, safe_fields)).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
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
        INT_AGGREGATE_FIELDS = {"ttl", "age"}
        branches: list[str] = []
        field_order: list[str] = []
        _pfl = per_field_limits or {}
        try:
            actual_types = {
                r[0]: r[1] for r in self.con.execute(f"DESCRIBE SELECT * FROM {table_name} LIMIT 0").fetchall()
            }
        except Exception:
            actual_types = {}
        for field in fields:
            if not _is_safe_ident(field):
                continue
            col_type = actual_types.get(field) or schema_types.get(field, "VARCHAR")
            field_limit = int(_pfl.get(field, limit))
            field_lit = escape_sql_literal(field)
            if col_type == "VARCHAR":
                val_expr, where_pred = f'"{field}"', f'"{field}" IS NOT NULL AND "{field}" != \'\''
            elif field in INT_AGGREGATE_FIELDS:
                val_expr, where_pred = f'CAST(CAST(ROUND("{field}") AS INTEGER) AS VARCHAR)', f'"{field}" IS NOT NULL'
            else:
                val_expr, where_pred = f'CAST("{field}" AS VARCHAR)', f'"{field}" IS NOT NULL'
            branches.append(
                f"(SELECT '{field_lit}' AS field, {val_expr} AS value, count(*) AS c "
                f"FROM {table_name} WHERE {where_pred} GROUP BY 2 "
                f"QUALIFY ROW_NUMBER() OVER (ORDER BY c DESC) <= {field_limit})"
            )
            field_order.append(field)
        if not branches:
            return [], []
        return self.execute(
            "\n            UNION ALL\n            ".join(branches) + "\n            ORDER BY field, c DESC"
        ).fetchall(), field_order

    def try_pop_health_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[tuple] | None:
        resolved = self._rollup_window_and_paths_with_active_hour(
            start_time, end_time, POP_HEALTH_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        st, et, paths_sql, active_hour_start, base_table = resolved
        # Whole-hour bundles omit this leading slice; the merge below only restores the live tail.
        if st != st.replace(minute=0, second=0, microsecond=0):
            return None
        if et > active_hour_start:
            ah_iso, et_iso = active_hour_start.isoformat(), et.isoformat()
            query = (
                f"SELECT pop,  CAST(SUM(requests) AS BIGINT),  CAST(SUM(errors) AS BIGINT),  CAST(SUM(cache_hits) AS BIGINT),"
                f"  CAST(SUM(bandwidth_bytes) AS BIGINT),  CAST(SUM(p50_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE),"
                f"  CAST(SUM(p95_ttfb_ms * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) FROM ("
                f"  SELECT pop, requests, errors, cache_hits, bandwidth_bytes, p50_rtt_us, p95_ttfb_ms "
                f"  FROM read_parquet([{paths_sql}])   WHERE pop IS NOT NULL AND pop != ''   UNION ALL "
                f"  SELECT pop, CAST(COUNT(*) AS BIGINT) AS requests, "
                f"         CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS BIGINT) AS errors, "
                f"         CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS cache_hits, "
                f"         CAST(SUM(resp_bytes) AS BIGINT) AS bandwidth_bytes, "
                f"         CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS p50_rtt_us, "
                f"         CAST(approx_quantile(ttfb, 0.95) * 1000.0 AS DOUBLE) AS p95_ttfb_ms   FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'     AND pop IS NOT NULL AND pop != '' "
                f"  GROUP BY pop) GROUP BY pop"
            )
        else:
            query = (
                f"SELECT   pop,   CAST(SUM(requests) AS BIGINT),   CAST(SUM(errors) AS BIGINT),   CAST(SUM(cache_hits) AS BIGINT), "
                f"  CAST(SUM(bandwidth_bytes) AS BIGINT),   CAST(SUM(p50_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE), "
                f"  CAST(SUM(p95_ttfb_ms * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) "
                f"FROM read_parquet([{paths_sql}]) WHERE pop IS NOT NULL AND pop != '' GROUP BY pop"
            )
        return self.execute(query).fetchall()


_CacheRegistry.register("repositories._base._schema_cols_cache", _schema_cols_cache)
_CacheRegistry.register("repositories._base._EMPTY_ROLLUP_WARN_TS", _EMPTY_ROLLUP_WARN_TS)
_CacheRegistry.register("repositories._base._MISSING_HOUR_HEAL_WARN_TS", _MISSING_HOUR_HEAL_WARN_TS)

from backend.repositories.top_n_rollups import (  # noqa: E402, F401
    _EDGE_PREFIXES,
    IpSpreadPlan,
    SchemaSeeds,
    TopNPaths,
    TopNPlan,
    TopNRow,
    TopNWindow,
    _bundled_day_files,
    _bundled_hour_files,
    _day_covered_by_any_field,
    _day_has_real_rollup,
    _has_real_parquet,
    _per_field_files,
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

_SchemaSeeds = SchemaSeeds
_TopNWindow = TopNWindow
_build_ip_spread_sql = build_ip_spread_sql
_build_rolled_sql = build_rolled_sql
_find_unrolled_hours = find_unrolled_hours
_fold_rows = fold_rows
_fold_sketches = fold_sketches
_live_topn_fields = live_topn_fields
_plan_ip_spread_paths = plan_ip_spread_paths
_plan_top_n_paths = plan_top_n_paths
_spread_meta = spread_meta
_truncate_top_n = truncate_top_n
_sys.modules["backend.repositories.ip_spread_rollups"] = _sys.modules["backend.repositories.top_n_rollups"]
