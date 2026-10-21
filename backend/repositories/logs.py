"""Repository for Raw Logs Explorer."""

from __future__ import annotations

import logging
import time as _time
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend import config as svcconfig
from backend.core.share_db.validation import SESSION_ID_KEYS, mask_ip
from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _compact_sql_for_debug,
    _safe_table,
    empty_schema_response,
)
from backend.repositories.utils.filters import build_where_clause
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc
from backend.utils.telemetry import get_queries, record_telemetry_query

logger = logging.getLogger(__name__)

_SCHEMA_TYPES_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=60.0)
_CacheRegistry.register("repositories.logs._SCHEMA_TYPES_CACHE", _SCHEMA_TYPES_CACHE)

_LOGS_QUERY_MEMO: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=10.0)
_CacheRegistry.register("repositories.logs._LOGS_QUERY_MEMO", _LOGS_QUERY_MEMO)


def _record_log_query(runner: QueryRunner, sql_str: str, elapsed_ms: float, service_id: str | None = None) -> None:
    entry = {"sql": sql_str, "time_ms": elapsed_ms, "engine": "DuckDB"}
    runner.debug_queries.append(entry)
    try:
        get_queries().append(entry)
        record_telemetry_query("duckdb", sql_str, elapsed_ms, service_id=service_id)
    except Exception:
        logger.debug("logs._record_log_query failed", exc_info=True)


def query_logs(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 100,
    offset: int = 0,
    sort_dir: str = "DESC",
    search: str | None = None,
    columns: list[str] | None = None,
    mask_ips: bool = False,
    include_count: bool = True,
) -> dict[str, Any]:
    """Execute a paginated, partition-pruned raw logs query with optional IP masking."""
    timer = SectionTimer()
    section_timings = timer.entries

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t)

    if not actual_cols:
        return empty_schema_response(
            rows=[],
            total_count=0,
            has_more=False,
            limit=limit,
            offset=offset,
            columns=[],
            schema_columns=[],
            missing_fields=[],
            section_timings=section_timings,
            **runner.telemetry(),
        )

    # Fetch column types for schema metadata (cached 60s per table)
    _t = _time.perf_counter()
    cached_schema = _SCHEMA_TYPES_CACHE.get(table_name)
    if cached_schema is not None and len(cached_schema) == len(actual_cols):
        schema_columns: list[dict[str, str]] = list(cached_schema)
    else:
        schema_columns = []
        try:
            t_desc = _time.perf_counter()
            describe_rows = con.execute(f"DESCRIBE {table_name}").fetchall()
            desc_ms = round((_time.perf_counter() - t_desc) * 1000, 2)
            _record_log_query(runner, f"DESCRIBE {table_name}", desc_ms, service_id=source_name)
            schema_columns = [{"name": r[0], "type": str(r[1])} for r in describe_rows]
            _SCHEMA_TYPES_CACHE[table_name] = schema_columns
        except Exception:
            schema_columns = [{"name": c, "type": "VARCHAR"} for c in actual_cols]
    timer.mark("describe_schema", _t)

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(
        start_time,
        end_time,
        filters,
        actual_cols,
        inline_params=False,
        partition_pruning=True,
    )

    # Search filter across common text fields
    conditions = [where_clause]
    query_params = list(params)
    has_search = bool(search and search.strip())
    if has_search and search is not None:
        term = f"%{search.strip()}%"
        search_parts = []
        for search_col in ("url", "client_ip", "status", "pop", "request_method", "fastly_service_id"):
            if search_col in actual_cols:
                search_parts.append(f'CAST("{search_col}" AS VARCHAR) ILIKE ?')
                query_params.append(term)
        if search_parts:
            conditions.append(f"({' OR '.join(search_parts)})")

    eff_where_clause = " AND ".join(conditions)
    timer.mark("build_where_clause", _t)

    has_partition_cols = "dt" in actual_cols or "timestamp_hour" in actual_cols
    memo_key: tuple[Any, ...] | None = None
    if has_partition_cols:
        memo_key = (
            source_name,
            table_name,
            start_time,
            end_time,
            repr(sorted((filters or {}).items())),
            limit,
            offset,
            str(sort_dir).upper(),
            search or "",
            tuple(columns or ()),
            bool(mask_ips),
            bool(include_count),
        )
        cached_resp = _LOGS_QUERY_MEMO.get(memo_key)
        if cached_resp is not None:
            return {
                **cached_resp,
                "section_timings": section_timings,
                **runner.telemetry(),
            }

    # Total count query (skipped when include_count=False, e.g. SSE live stream ticks)
    total_count = 0
    if include_count:
        _t = _time.perf_counter()
        counted = False
        if not filters and not has_search and has_partition_cols:
            try:
                rollup_cnt = runner.try_count_from_rollup(
                    start_time,
                    end_time,
                    table_name,
                    where_clause,
                    params,
                    unfiltered_window=True,
                    actual_cols=actual_cols,
                )
                if rollup_cnt is not None and rollup_cnt > 0:
                    total_count = int(rollup_cnt)
                    counted = True
            except Exception:
                counted = False
            if not counted:
                try:
                    fast_cnt_params, fast_cnt_where = build_where_clause(
                        start_time,
                        end_time,
                        {},
                        actual_cols,
                        inline_params=False,
                        exclude_invalid_ips=False,
                        partition_pruning=True,
                    )
                    fast_cnt_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {fast_cnt_where}"
                    count_res = con.execute(fast_cnt_sql, fast_cnt_params).fetchone()
                    total_count = int(count_res[0]) if count_res else 0
                    count_ms = round((_time.perf_counter() - _t) * 1000, 2)
                    _record_log_query(runner, _compact_sql_for_debug(fast_cnt_sql), count_ms, service_id=source_name)
                    counted = True
                except Exception:
                    counted = False

        if not counted:
            count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {eff_where_clause}"
            try:
                count_res = con.execute(count_sql, query_params).fetchone()
                total_count = int(count_res[0]) if count_res else 0
                count_ms = round((_time.perf_counter() - _t) * 1000, 2)
                _record_log_query(runner, _compact_sql_for_debug(count_sql), count_ms, service_id=source_name)
            except Exception:
                total_count = 0
        timer.mark("count_query", _t)

    # Columns selection
    if columns:
        valid_selected = [c for c in columns if c in actual_cols]
        if "timestamp" in actual_cols and "timestamp" not in valid_selected:
            valid_selected.insert(0, "timestamp")
        selected_cols = valid_selected or actual_cols
    else:
        selected_cols = actual_cols

    col_select_sql = ", ".join(f'"{c}"' for c in selected_cols)
    safe_sort_dir = "ASC" if str(sort_dir).upper() == "ASC" else "DESC"

    order_clause = ""
    if "timestamp" in actual_cols:
        order_clause = f'ORDER BY "timestamp" {safe_sort_dir}'

    data_sql = f"SELECT {col_select_sql} FROM {table_name} WHERE {eff_where_clause} {order_clause} LIMIT ? OFFSET ?"
    data_params = query_params + [limit, offset]

    _t = _time.perf_counter()
    records: list[dict[str, Any]] | None = None
    # Fast-path partition pruning for first-page DESC queries over wide windows:
    # try the most recent 6 hours of partitions first so DuckDB doesn't sort 30 days of Parquet files.
    if safe_sort_dir == "DESC" and offset == 0 and has_partition_cols:
        try:
            st_dt = parse_iso_utc(start_time) if start_time else None
            et_dt = parse_iso_utc(end_time) if end_time else None
            if et_dt is None:
                svc_status = svcconfig.get_status(source_name) or {}
                latest_iso = svc_status.get("latest_log_at")
                et_dt = parse_iso_utc(latest_iso) if latest_iso else datetime.now(UTC)
            if et_dt is not None and (st_dt is None or (et_dt - st_dt) > timedelta(hours=6)):
                recent_st = (et_dt - timedelta(hours=6)).astimezone(UTC)
                recent_conds = [eff_where_clause]
                if "dt" in actual_cols:
                    recent_conds.append(f"dt >= '{recent_st.strftime('%Y-%m-%d')}'")
                if "timestamp_hour" in actual_cols:
                    recent_conds.append(f"timestamp_hour >= '{recent_st.strftime('%Y-%m-%d-%H')}'")
                recent_where = " AND ".join(recent_conds)
                fast_sql = (
                    f"SELECT {col_select_sql} FROM {table_name} WHERE {recent_where} {order_clause} LIMIT ? OFFSET ?"
                )
                t_fast = _time.perf_counter()
                fast_arrow = con.execute(fast_sql, data_params).arrow()
                fast_records = fast_arrow.to_pylist()
                fast_ms = round((_time.perf_counter() - t_fast) * 1000, 2)
                if len(fast_records) >= limit or (include_count and len(fast_records) == total_count):
                    records = fast_records
                    _record_log_query(runner, _compact_sql_for_debug(fast_sql), fast_ms, service_id=source_name)
        except Exception:
            records = None

    if records is None:
        t_data = _time.perf_counter()
        try:
            arrow_table = con.execute(data_sql, data_params).arrow()
            records = arrow_table.to_pylist()
        except Exception:
            cursor = con.execute(data_sql, data_params)
            desc = cursor.description or []
            colnames = [d[0] for d in desc]
            records = [dict(zip(colnames, row)) for row in cursor.fetchall()]
        data_ms = round((_time.perf_counter() - t_data) * 1000, 2)
        _record_log_query(runner, _compact_sql_for_debug(data_sql), data_ms, service_id=source_name)
    timer.mark("data_query", _t)

    if not include_count:
        total_count = len(records)

    # Convert datetimes / timestamps to ISO strings if needed
    _t = _time.perf_counter()
    for row in records:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()

        if mask_ips:
            for ip_key in ("client_ip", "ip", "remote_addr"):
                if ip_key in row and row[ip_key]:
                    row[ip_key] = mask_ip(str(row[ip_key]))
            for sess_key in SESSION_ID_KEYS:
                if sess_key in row and row[sess_key]:
                    row[sess_key] = "[redacted]"
    timer.mark("ip_masking", _t)

    has_more = (offset + len(records)) < total_count

    core_payload = {
        "rows": records,
        "total_count": total_count,
        "has_more": has_more,
        "limit": limit,
        "offset": offset,
        "columns": selected_cols,
        "schema_columns": schema_columns,
        "missing_fields": [],
    }
    if memo_key is not None and total_count > 0:
        _LOGS_QUERY_MEMO[memo_key] = core_payload

    return {
        **core_payload,
        "section_timings": section_timings,
        **runner.telemetry(),
    }
