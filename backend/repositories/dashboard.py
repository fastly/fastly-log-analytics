"""Dashboard repository — pure SQL functions, no HTTP imports."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys as _sys
import time
import uuid
from collections.abc import Callable, Collection
from typing import Any, Literal

import duckdb

from backend.core.sql_validator import escape_sql_literal
from backend.models.analytics import HistoryObservation, ReaderCoverage
from backend.models.common import FiltersDict
from backend.repositories._base import (
    CANONICAL_METRICS,
    QueryRunner,
    SectionTimer,
    _drop_temp_table,
    _get_schema,
    _safe_table,
    force_rebuild_view,
    get_source_extent,
    percentile_ms_expr,
    safe_interval,
    safe_iso,
    should_self_heal_stale_view,
    time_bucket_select,
)
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    bucket_time_to_minute,
    cache_result,
    coalesced_cache_result,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    empty_dashboard_aggregates,
    format_asn_display_label,
    parse_asn_int,
    resolve_asn_names_map,
)
from backend.utils.telemetry import log_once_per

# ── Virtual-field unnest top-N ────────────────────────────────────────────────

VIRTUAL_FIELD_EXPLODED_TOP_N = """
                WITH split_data AS (
                    SELECT trim(signal) AS signal
                    FROM (
                        SELECT unnest(string_split("{backing_col}", ',')) AS signal
                        FROM {table_name}
                        WHERE "{backing_col}" IS NOT NULL AND "{backing_col}" != '' AND {where_clause}
                    )
                    WHERE trim(signal) != ''
                ),
                total_count AS (SELECT {requests_metric} AS tc FROM split_data),
                top_values AS (
                    SELECT signal AS value, {requests_metric} AS c
                    FROM split_data GROUP BY 1 ORDER BY 2 DESC LIMIT 10
                )
                SELECT tv.value, tv.c, tc.tc FROM top_values tv CROSS JOIN total_count tc
            """

# ── conn_requests histogram bucket (en-dash U+2013 preserved for FE match) ────

CONN_REQUESTS_BUCKET = (
    "\n"
    "                SELECT\n"
    "                    CASE\n"
    "                        WHEN \"conn_requests\" = 1 THEN '1'\n"
    "                        WHEN \"conn_requests\" BETWEEN 2 AND 5 THEN '2–5'\n"
    "                        WHEN \"conn_requests\" BETWEEN 6 AND 20 THEN '6–20'\n"
    "                        ELSE '21+'\n"
    "                    END AS bucket,\n"
    "                    {requests_metric} AS c\n"
    "                FROM {table_name}\n"
    '                WHERE "conn_requests" IS NOT NULL AND "conn_requests" > 0 AND {where_clause}\n'
    "                GROUP BY 1\n"
    '                ORDER BY MIN("conn_requests")\n'
    "            "
)

# ── Time series chart ─────────────────────────────────────────────────────────

TIME_SERIES = """
                    SELECT {time_bucket_select},
                           {value_expr} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL{extra_where} AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """

# ── Map data (country aggregate) ──────────────────────────────────────────────

MAP_DATA_BY_COUNTRY = """
                    SELECT "country" AS country, {requests_metric} AS count
                    FROM {table_name}
                    WHERE "country" IS NOT NULL AND {where_clause}
                    GROUP BY 1
                """

# ── Field values: bot UA enumeration ──────────────────────────────────────────

FIELD_VALUES_BOT_UA = """
            SELECT ua, {requests_metric} AS cnt
            FROM {table_name}
            WHERE {where_clause} AND ua IS NOT NULL {ua_filter}
            GROUP BY ua
            ORDER BY cnt DESC
            LIMIT 5000
        """

# ── Field values: virtual CSV-backed signals lookup ───────────────────────────

FIELD_VALUES_VIRTUAL_SIGNALS = """
            SELECT trim(signal) AS value, {requests_metric} AS count
            FROM (
                SELECT unnest(string_split("{backing_col}", ',')) AS signal
                FROM {table_name}
                WHERE {where_clause} AND "{backing_col}" IS NOT NULL AND "{backing_col}" != ''
            )
            WHERE trim(signal) != '' {search_cond}
            GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
        """

# ── Field values: native column lookup ────────────────────────────────────────

FIELD_VALUES_NATIVE_COLUMN = """
            SELECT "{clean_field}" AS value, {requests_metric} AS count
            FROM {table_name}
            WHERE {where_clause} {search_cond}
            GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
        """

SQL: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.dashboard", _sys.modules[__name__])

# ── In-memory caches ──────────────────────────────────────────────────────────
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc

logger = logging.getLogger(__name__)

DASHBOARD_CACHE_TTL: int = int(os.getenv("FLA_DASHBOARD_CACHE_TTL", "30"))  # seconds; 0 disables read+write
_dashboard_cache: BoundedTTLCache = BoundedTTLCache(maxsize=500, ttl_seconds=max(DASHBOARD_CACHE_TTL, 1))


def invalidate_service(service_name: str) -> None:
    """Drop every cached dashboard response keyed to ``service_name``."""
    if not service_name:
        return
    stale = [k for k in list(_dashboard_cache) if k.endswith(f":{service_name}")]
    for k in stale:
        try:
            del _dashboard_cache[k]
        except KeyError:
            pass
    try:
        _dashboard_cache.pop(service_name, None)
    except (KeyError, AttributeError):
        pass


# ── aggregates ────────────────────────────────────────────────────────────────

from backend.core.log_fields import REGISTRY as _FIELD_REGISTRY

_VIRTUAL_BACKING = {"waf_sig_ind": "waf_sig", "edge_score_reason_ind": "edge_score_reason"}
_VIRTUAL_FIELDS = tuple(_VIRTUAL_BACKING)
FIELDS = [f.code for f in _FIELD_REGISTRY if f.code != "_source_file"] + list(_VIRTUAL_FIELDS)

NON_TOP_N_FIELDS = frozenset(
    {
        "timestamp",
        "elapsed",
        "ttfb",
        "req_bytes",
        "req_header_bytes",
        "resp_bytes",
        "lat",
        "lon",
        "rid",
        "prid",
        "waf_req_id",
        "waf_sig",
        "_source_file",
        # Network quality (Group F/G)
        "tcp_rtt",
        "ploss",
        "rtt_min",
        "rtt_var",
        "retrans",
        "bw",
        "delivery_rate",
        "data_segs_out",
        # QUIC/HTTP3 (Group K)
        "q_rtt",
        "q_rtt_var",
        "q_lost",
        "q_cwnd",
        # Origin metrics (Group L)
        "ottfb",
        "ottlb",
        "oconnect_ms",
        "ost",
        "obytes",
        "oip",
        "oretries",
        # IO / other stats
        "io_input_bytes",
        "io_output_bytes",
        # Edge score / threat indicators
        "edge_score",
        "edge_score_l1",
        "edge_score_l2",
        "edge_score_rtt_us",
        "edge_score_exec_us",
    }
)


def _add_bot_columns(actual_cols: Collection[str], columns: list[str], select_cols: list[str]) -> tuple[bool, bool]:
    """Ensure UA + IP (Arcjet) or waf_req_id (NGWAF) columns are in select_cols
    when the caller requested the virtual `_bot_name` / `_ngwaf_bot_name` fields.

    Mutates `select_cols` in place. Returns (wants_bot, wants_ngwaf_bot).
    """
    wants_bot = "_bot_name" in columns
    wants_ngwaf_bot = "_ngwaf_bot_name" in columns
    if wants_bot:
        if "ua" in actual_cols and '"ua"' not in select_cols:
            select_cols.append('"ua"')
        if "ip" in actual_cols and '"ip"' not in select_cols:
            select_cols.append('"ip"')
    if wants_ngwaf_bot and "waf_req_id" in actual_cols and '"waf_req_id"' not in select_cols:
        select_cols.append('"waf_req_id"')
    return wants_bot, wants_ngwaf_bot


def _dashboard_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    chart_interval: str,
    chart_metric: str,
    fields_filter: list[str] | None = None,
    *,
    include_time_series: bool | None = True,
    include_conn_requests: bool | None = True,
    include_map_data: bool | None = True,
    include_top_n: bool | None = True,
) -> str:
    source_name = src["name"]
    _key_payload = json.dumps(
        {
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": serialize_filters_for_key(filters),
            "ci": chart_interval,
            "cm": chart_metric,
            "fields": sorted(fields_filter) if fields_filter is not None else None,
            "its": True if include_time_series is None else include_time_series,
            "icr": True if include_conn_requests is None else include_conn_requests,
            "imd": True if include_map_data is None else include_map_data,
            "itn": True if include_top_n is None else include_top_n,
        },
        separators=(",", ":"),
    )
    return f"{hashlib.sha256(_key_payload.encode()).hexdigest()}:{source_name}"


def _lookup_dashboard_cache(cache_key: str) -> dict | None:
    cached_entry = _dashboard_cache.get(cache_key)
    if cached_entry is not None:
        _, cached_res = cached_entry
        return cache_result(cached_res.copy(), state="hit", ttl_seconds=_dashboard_cache.ttl, prewarmer=False)
    return None


def peek_aggregates(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    chart_interval: str,
    chart_metric: str,
    fields_filter: list[str] | None = None,
    *,
    include_time_series: bool | None = True,
    include_conn_requests: bool | None = True,
    include_map_data: bool | None = True,
    include_top_n: bool | None = True,
) -> dict | None:
    """Return a cached dashboard aggregates response without acquiring a DuckDB connection."""
    if DASHBOARD_CACHE_TTL <= 0:
        return None
    return _lookup_dashboard_cache(
        _dashboard_cache_key(
            src,
            start_time,
            end_time,
            filters,
            chart_interval,
            chart_metric,
            fields_filter,
            include_time_series=include_time_series,
            include_conn_requests=include_conn_requests,
            include_map_data=include_map_data,
            include_top_n=include_top_n,
        )
    )


def get_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    chart_interval: str,
    chart_metric: str,
    fields_filter: list[str] | None = None,
    *,
    include_time_series: bool | None = True,
    include_conn_requests: bool | None = True,
    include_map_data: bool | None = True,
    include_top_n: bool | None = True,
    _query_table: str | None = None,
    _selected_aggregates: dict | None = None,
    _dispatch_reason: str | None = None,
) -> dict:
    # Normalize tri-state (model passes None when the caller doesn't override).
    include_time_series = True if include_time_series is None else include_time_series
    include_conn_requests = True if include_conn_requests is None else include_conn_requests
    include_map_data = True if include_map_data is None else include_map_data
    include_top_n = True if include_top_n is None else include_top_n

    source_name = src["name"]
    table_name = _query_table or _safe_table(source_name)

    lf_config = src.get("log_fields") or {}
    _custom_field_names = [
        cf["name"]
        for cf in lf_config.get("custom_fields", [])
        if cf.get("enabled", True) and cf.get("show_in_dashboard", True)
    ]
    all_fields = FIELDS + _custom_field_names
    if fields_filter is not None:
        fields = [f for f in fields_filter if f in all_fields]
    else:
        fields = all_fields

    cache_key: str | None = None
    now = time.time()
    if DASHBOARD_CACHE_TTL > 0:
        cache_key = _dashboard_cache_key(
            src,
            start_time,
            end_time,
            filters,
            chart_interval,
            chart_metric,
            fields_filter,
            include_time_series=include_time_series,
            include_conn_requests=include_conn_requests,
            include_map_data=include_map_data,
            include_top_n=include_top_n,
        )
        # BoundedTTLCache's ``__contains__`` / ``[]`` already enforce TTL
        # internally, so an entry that reads as present is by definition
        # still fresh — no need for the legacy ``now - cached_at`` check.
        if (hit := _lookup_dashboard_cache(cache_key)) is not None:
            return hit

    def _compute() -> dict:
        if cache_key is not None and (hit := _lookup_dashboard_cache(cache_key)) is not None:
            return hit
        return _compute_dashboard_aggregates(
            con=con,
            src=src,
            start_time=start_time,
            end_time=end_time,
            filters=filters,
            chart_interval=chart_interval,
            chart_metric=chart_metric,
            fields=fields,
            table_name=table_name,
            cache_key=cache_key,
            now=now,
            include_time_series=include_time_series,
            include_conn_requests=include_conn_requests,
            include_map_data=include_map_data,
            include_top_n=include_top_n,
            _selected_aggregates=_selected_aggregates,
            _dispatch_reason=_dispatch_reason,
        )

    if cache_key is not None and _query_table is None and _selected_aggregates is None:
        return coalesced_cache_result(
            f"dash_agg:{cache_key}", _compute, ttl_seconds=_dashboard_cache.ttl, prewarmer=False
        )
    return cache_result(
        _compute(),
        state="miss" if cache_key is not None else "bypass",
        ttl_seconds=_dashboard_cache.ttl if cache_key is not None else None,
        prewarmer=False,
    )


def _raw_extent_reaches_window_start(
    window_start: str | None,
    earliest_event_at: str | None,
) -> bool | None:
    """Compare the requested start with the earliest observed raw event.

    This does not prove continuity or explain why an extent begins later.
    """
    requested_start = parse_iso_utc(window_start)
    earliest = parse_iso_utc(earliest_event_at)
    if requested_start is None or earliest is None:
        return None
    return earliest <= requested_start


def _compute_dashboard_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    chart_interval: str,
    chart_metric: str,
    fields: list[str],
    table_name: str,
    cache_key: str | None,
    now: float,
    *,
    include_time_series: bool,
    include_conn_requests: bool,
    include_map_data: bool,
    include_top_n: bool,
    _selected_aggregates: dict | None,
    _dispatch_reason: str | None,
) -> dict:

    # Per-phase wall-clock timing surfaces in the response under
    # _section_timings so we can attribute the cold dashboard wall
    # without re-running ad-hoc instrumentation. Matches the
    # bootstrap.py pattern. Negligible overhead (perf_counter is ~50ns).
    timer = SectionTimer()
    section_timings = timer.entries
    if _dispatch_reason:
        section_timings.append({"section": f"engine:{_dispatch_reason}", "time_ms": 0.0})

    runner = QueryRunner(con, src)
    interval = "1 minute"

    def _empty_envelope() -> dict[str, Any]:
        return empty_dashboard_aggregates(fields, interval=interval, **runner.telemetry())

    actual_cols = timer.call("get_schema_cols", runner.get_schema_cols)
    if not actual_cols:
        return _empty_envelope()

    params, where_clause = timer.call(
        "build_where_clause",
        lambda: build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True),
    )
    # DuckLake handles partition pruning natively — no manual file enumeration needed.

    # Build temp table with only needed columns
    needed_cols: set[str] = set()
    if "timestamp" in actual_cols:
        needed_cols.add('"timestamp"')
    for field in fields:
        if field in _VIRTUAL_BACKING:
            if (bcol := _VIRTUAL_BACKING[field]) in actual_cols:
                needed_cols.add(f'"{bcol}"')
            continue
        if field in actual_cols:
            needed_cols.add(f'"{field}"')

    for mc in [
        "resp_bytes",
        "elapsed",
        "status",
        "cache",
        "resp_state",
        "req_header_bytes",
        "req_bytes",
        "ttfb",
        "server_region",
        "tls_ciphers_sha",
        "is_ipv6",
        "conn_requests",
    ]:
        if mc in actual_cols:
            needed_cols.add(f'"{mc}"')

    cols_str = ", ".join(needed_cols) if needed_cols else "*"
    from backend.core.rollups import rollups_present
    from backend.core.rollups.rollup_coverage import durable_serving_blocked

    _durable_blocked = durable_serving_blocked(src)
    _rollups_present = rollups_present(src) if not filters else None
    use_rollups = not filters and bool(_rollups_present) and not _durable_blocked
    if not use_rollups:
        refusal: Literal["filtered", "rollups_missing", "durable_unready"] = (
            "filtered" if filters else "rollups_missing" if not _rollups_present else "durable_unready"
        )
        skipped = [ReaderCoverage(reader="dashboard.count", maximum_window_days=None)]
        if include_top_n:
            skipped.append(ReaderCoverage(reader="dashboard.top_n", maximum_window_days=None))
        if include_time_series:
            skipped.append(ReaderCoverage(reader="dashboard.time_series", maximum_window_days=None))
        if include_conn_requests and "conn_requests" in actual_cols:
            skipped.append(ReaderCoverage(reader="dashboard.conn_requests_histogram", maximum_window_days=None))
        for observation in skipped:
            observation.eligible, observation.eligibility_reason, observation.outcome = False, refusal, "ineligible"
            observation.window_start, observation.window_end = (
                safe_iso(parse_iso_utc(start_time)),
                safe_iso(parse_iso_utc(end_time)),
            )
        runner.reader_coverage.extend(skipped)

    temp_table: str | None = None
    orig_table_name = table_name
    orig_where_clause = where_clause
    orig_params = list(params) if params is not None else []

    def _build_query_target() -> tuple[str, str, list, str | None] | None:
        _table_name = orig_table_name
        _where = orig_where_clause
        _params = list(orig_params)
        if use_rollups:
            return _table_name, _where, _params, None
        wide_temp = f"t_{uuid.uuid4().hex}"
        sql = f"CREATE TEMP TABLE {wide_temp} AS SELECT {cols_str} FROM {_table_name} WHERE {orig_where_clause}"
        if not timer.call("wide_temp_create", lambda: runner.create_temp_table(sql, _params)):
            return None
        return wide_temp, "1=1", [], wide_temp

    built = _build_query_target()
    if built is None:
        if _selected_aggregates is not None:
            raise RuntimeError("Pinned DuckLake materialization failed")
        return _empty_envelope()
    table_name, where_clause, params, temp_table = built

    results: dict[str, Any] = {f: {"top": [], "total": 0} for f in fields}
    _self_heal_attempted = False

    if use_rollups:
        runner.begin_shared_active_hour_temps()
    try:
        field_totals: dict[str, int] = {}
        total_rows = 0
        earliest_log_at = None
        latest_log_at = None

        history = HistoryObservation(
            window_start=safe_iso(parse_iso_utc(start_time)),
            window_end=safe_iso(parse_iso_utc(end_time)),
            filters_requested=bool(filters),
        )
        runner.history = history

        def _compute_count(_table_name: str, _where: str, _params: list) -> int:
            if use_rollups:
                try:
                    _count_proj = (
                        [
                            *fields,
                            "country",
                            *[bcol for v, bcol in _VIRTUAL_BACKING.items() if v in fields],
                        ]
                        if include_top_n
                        else [
                            "conn_requests",
                            "status",
                            "cache",
                            "elapsed",
                            "resp_bytes",
                            "req_bytes",
                            "req_header_bytes",
                            "ttfb",
                        ]
                    )
                    rollup_count = runner.try_count_from_rollup(
                        start_time,
                        end_time,
                        _table_name,
                        _where,
                        _params,
                        unfiltered_window=True,
                        projected_fields=_count_proj,
                        actual_cols=actual_cols,
                    )
                    if rollup_count is not None:
                        rollup_count += runner.execute(
                            f"SELECT {CANONICAL_METRICS['requests']} FROM {_table_name} "
                            f"WHERE ({_where}) AND timestamp = ?",
                            [*_params, parse_iso_utc(end_time)],
                        ).fetchone()[0]
                        history.window_rows, history.window_count_source = rollup_count, "rollup"
                        return rollup_count
                except Exception:
                    logger.debug("_compute_count: step failed (ignored)", exc_info=True)

                try:
                    count = runner.execute(
                        f"SELECT {CANONICAL_METRICS['requests']} FROM {_table_name} WHERE {_where}", _params
                    ).fetchone()[0]
                    history.window_rows, history.window_count_source = count, "raw"
                    return count
                except Exception:
                    history.window_rows, history.window_count_source = None, "unknown"
                    return 0
            count_cols: list[str] = [CANONICAL_METRICS["requests"]]
            valid_fields: list[str] = []
            if include_top_n:
                for field in fields:
                    if field in _VIRTUAL_FIELDS:
                        continue
                    if field in actual_cols:
                        count_cols.append(f"count({field})")
                        valid_fields.append(field)
            field_totals.clear()
            if count_cols:
                count_res = runner.execute(
                    f"SELECT {', '.join(count_cols)} FROM {_table_name} WHERE {_where}", _params
                ).fetchone()
                history.window_rows, history.window_count_source = count_res[0], "filtered_temp"
                for i, field in enumerate(valid_fields):
                    field_totals[field] = count_res[i + 1]
                return count_res[0]
            return 0

        count_params, count_where_clause = timer.call(
            "build_where_clause",
            lambda: build_where_clause(
                start_time,
                end_time,
                filters,
                actual_cols,
                inline_params=True,
                exclude_invalid_ips=False,
            ),
        )

        def _count_for_target() -> int:
            return (
                _compute_count(table_name, count_where_clause, count_params)
                if table_name == orig_table_name
                else _compute_count(table_name, where_clause, params)
            )

        total_rows = _count_for_target()

        if _selected_aggregates is not None and (
            sum(point["value"] for point in _selected_aggregates["time_series"]) != total_rows
            or _selected_aggregates["country"]["total"] != field_totals.get("country")
        ):
            raise RuntimeError("Hybrid snapshot counts disagree")

        total_rows_total, earliest_log_at, latest_log_at = timer.call(
            "source_extent", lambda: get_source_extent(runner, src, orig_table_name)
        )
        history.raw_extent_reaches_window_start = _raw_extent_reaches_window_start(
            history.window_start, earliest_log_at
        )

        if (
            _selected_aggregates is None
            and not _self_heal_attempted
            and should_self_heal_stale_view(
                windowed_count=total_rows,
                filters=filters,
                latest_log_at=latest_log_at,
                start_time=start_time,
                end_time=end_time,
            )
        ):
            _self_heal_attempted = True
            timer.call("view_self_heal_rebuild", lambda: force_rebuild_view(runner.con, src))
            if temp_table is not None:
                try:
                    runner.con.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
                except Exception:
                    if log_once_per("get_aggregates: DROP TABLE failed (ignored)", 60):
                        logger.debug("get_aggregates: DROP TABLE failed (ignored)", exc_info=True)
                temp_table = None
            rebuilt = _build_query_target()
            if rebuilt is not None:
                table_name, where_clause, params, temp_table = rebuilt
                total_rows = _count_for_target()

        schema_types = timer.call(
            "schema_types", lambda: {col["name"]: col["type"] for col in _get_schema(runner.con, src)}
        )

        if use_rollups:
            batch_fields = [
                f for f in fields if f not in _VIRTUAL_FIELDS and f in actual_cols and f not in NON_TOP_N_FIELDS
            ]
            batch_fields += [v for v, bcol in _VIRTUAL_BACKING.items() if v in fields and bcol in actual_cols]
        else:
            batch_fields = [
                f for f in fields if f not in _VIRTUAL_FIELDS and f in field_totals and f not in NON_TOP_N_FIELDS
            ]

        all_top_res: list[tuple[str, Any, int]] = []
        field_order: list[str] = []
        _need_topn_scan = include_top_n or (use_rollups and include_map_data and "country" in actual_cols)
        if _selected_aggregates is not None:
            results["country"] = _selected_aggregates["country"]
        elif not _need_topn_scan:
            pass
        elif use_rollups:
            _batch_with_country = batch_fields if "country" in batch_fields else batch_fields + ["country"]
            _per_field_limits = {"country": 500} if include_map_data else {}
            all_top_res, field_order = timer.call(
                "top_n_rollups",
                lambda: runner.execute_top_n_rollups(
                    _batch_with_country,
                    start_time,
                    end_time,
                    limit=10,
                    per_field_limits=_per_field_limits,
                    _phase_log=section_timings,
                    actual_cols=list(actual_cols),
                    schema_types=schema_types,
                    exact_window=start_time is not None and end_time is not None,
                ),
            )
            for f_name, _f_val, f_count in all_top_res:
                field_totals[f_name] = field_totals.get(f_name, 0) + int(f_count)
        else:
            all_top_res, field_order = timer.call(
                "top_n_batch",
                lambda: runner.execute_top_n_batch(batch_fields, table_name, actual_cols, schema_types, limit=10),
            )

        if all_top_res and include_top_n:
            for field in field_order:
                results[field] = {"top": [], "total": field_totals.get(field, 0)}

            asn_list = [
                asn_int
                for f_name, f_val, _ in all_top_res
                if f_name == "asn" and (asn_int := parse_asn_int(f_val)) is not None
            ]
            asn_names = {}
            if asn_list:
                asn_names = timer.call("asn_names_lookup", lambda: resolve_asn_names_map(src["name"], asn_list))

            _PANEL_LIMIT = 10
            _panel_count: dict[str, int] = {}
            for f_name, f_val, f_count in all_top_res:
                if f_val == "__other__" or _panel_count.get(f_name, 0) >= _PANEL_LIMIT:
                    continue
                entry = {"value": f_val, "count": f_count}
                if f_name == "asn" and (asn_int := parse_asn_int(f_val)) is not None:
                    entry["label"] = format_asn_display_label(asn_int, asn_names)

                results[f_name]["top"].append(entry)
                _panel_count[f_name] = _panel_count.get(f_name, 0) + 1

        def _exploded_top_n(virtual_id: str, backing_col: str) -> None:
            if virtual_id not in fields:
                return
            existing = results.get(virtual_id)
            if existing and existing.get("top"):
                return
            if backing_col not in actual_cols:
                results[virtual_id] = {"top": [], "total": 0}
                return
            if use_rollups:
                rolled = runner.try_virtual_field_top_n_from_rollup(
                    virtual_id,
                    backing_col,
                    start_time,
                    end_time,
                    has_filters=bool(filters),
                    actual_cols=actual_cols,
                )
                if rolled is not None:
                    results[virtual_id] = rolled
                    return
            q = SQL.VIRTUAL_FIELD_EXPLODED_TOP_N.format(
                backing_col=backing_col,
                table_name=orig_table_name,
                where_clause=orig_where_clause,
                requests_metric=CANONICAL_METRICS["requests"],
            )
            res = runner.execute(q, orig_params).fetchall()
            if res:
                results[virtual_id] = {
                    "top": [{"value": r[0], "count": r[1]} for r in res],
                    "total": res[0][2],
                }
            else:
                results[virtual_id] = {"top": [], "total": 0}

        if include_top_n:
            for _vid, _bcol in _VIRTUAL_BACKING.items():
                timer.call(f"{_vid}_explode", lambda v=_vid, b=_bcol: _exploded_top_n(v, b))

        t_conn_req_0 = time.perf_counter()
        if include_conn_requests and "conn_requests" in actual_cols:
            hist = (
                runner.try_conn_requests_hist_from_rollup(
                    start_time, end_time, has_filters=bool(filters), actual_cols=actual_cols
                )
                if use_rollups
                else None
            )
            if hist is not None:
                results["conn_requests"] = hist
            else:
                q = SQL.CONN_REQUESTS_BUCKET.format(
                    requests_metric=CANONICAL_METRICS["requests"],
                    table_name=table_name,
                    where_clause=where_clause,
                )
                res = runner.execute(q, params).fetchall()
                results["conn_requests"] = {
                    "top": [{"value": r[0], "count": r[1]} for r in res],
                    "total": sum(r[1] for r in res),
                }
        else:
            results["conn_requests"] = {"top": [], "total": 0}
        timer.mark("conn_requests", t_conn_req_0)

        t_ts_0 = time.perf_counter()
        time_series: list[dict] = []
        chart_metric_out = "requests"
        if _selected_aggregates is not None:
            time_series = _selected_aggregates["time_series"]
            interval = chart_interval
        elif include_time_series and "timestamp" in actual_cols:
            interval = safe_interval(chart_interval, default=interval)
            sql_cache = "cache"
            sql_elapsed = "elapsed"

            rollup_metric_ok = chart_metric in QueryRunner._TS_ROLLUP_METRIC_PARTS
            rollup_col_ok = (
                chart_metric == "requests"
                or (chart_metric in ("5xx", "4xx") and "status" in actual_cols)
                or (chart_metric == "hit_rate" and "cache" in actual_cols)
            )
            _skip_raw_time_series = False
            if use_rollups and rollup_metric_ok and rollup_col_ok:
                t_ts_rollup_0 = time.perf_counter()
                rollup_series = runner.try_time_series_from_rollup(
                    chart_metric=chart_metric,
                    interval=interval,
                    start_time=start_time,
                    end_time=end_time,
                    table_name=table_name,
                    where_clause=where_clause,
                    params=params,
                    unfiltered_window=True,
                )
                timer.mark("time_series:rollup_attempt", t_ts_rollup_0)
                if rollup_series is not None:
                    time_series = rollup_series
                    chart_metric_out = chart_metric
                    _skip_raw_time_series = True

            if not _skip_raw_time_series:
                extra_where = ""
                if chart_metric in ("5xx", "4xx") and "status" in actual_cols:
                    chart_metric_out = chart_metric
                    value_expr = CANONICAL_METRICS[f"{chart_metric}_rate"]
                elif chart_metric == "hit_rate" and ("cache" in actual_cols or "resp_state" in actual_cols):
                    chart_metric_out = "hit_rate"
                    cache_col = '"cache"' if "cache" in actual_cols else '"resp_state"'
                    value_expr = CANONICAL_METRICS["hit_rate"].format(cache_col=cache_col)
                elif chart_metric.endswith("_latency") and ("elapsed" in actual_cols or "elapsed_us" in actual_cols):
                    chart_metric_out = chart_metric
                    percentile = (
                        0.50 if chart_metric.startswith("p50") else (0.99 if chart_metric.startswith("p99") else 0.95)
                    )
                    value_expr = percentile_ms_expr(sql_elapsed, percentile)
                    extra_where = f" AND {sql_elapsed} IS NOT NULL"
                elif chart_metric == "throughput" and "resp_bytes" in actual_cols and "elapsed" in actual_cols:
                    chart_metric_out = "throughput"
                    value_expr = CANONICAL_METRICS["throughput"].format(
                        cache_col=sql_cache,
                        elapsed_col="elapsed",
                        resp_bytes_col="resp_bytes",
                    )
                elif chart_metric == "req_size" and any(c in actual_cols for c in ["req_header_bytes", "req_bytes"]):
                    chart_metric_out = "req_size"
                    value_expr = CANONICAL_METRICS["req_size"].format(
                        header_bytes_col='"req_header_bytes"' if "req_header_bytes" in actual_cols else "0",
                        req_bytes_col="req_bytes" if "req_bytes" in actual_cols else "0",
                    )
                elif chart_metric == "ttfb" and "ttfb" in actual_cols:
                    chart_metric_out = "ttfb"
                    value_expr = CANONICAL_METRICS["ttfb_ms"]
                else:
                    chart_metric_out = "requests"
                    value_expr = CANONICAL_METRICS["requests"]
                ts_q = SQL.TIME_SERIES.format(
                    time_bucket_select=time_bucket_select(interval),
                    value_expr=value_expr,
                    table_name=table_name,
                    extra_where=extra_where,
                    where_clause=where_clause,
                )
                ts_res = runner.execute(ts_q, params).fetchall()
                for r in ts_res:
                    if r[0] is None:
                        continue
                    pt: dict[str, Any] = {"time": safe_iso(r[0]), "value": float(r[1]) if r[1] is not None else 0.0}
                    if len(r) >= 3 and r[2] is not None:
                        pt["category"] = str(r[2])
                    time_series.append(pt)
        timer.mark("time_series", t_ts_0)

        t_map_0 = time.perf_counter()
        map_data: list[dict] = []
        if include_map_data and "country" in actual_cols:
            if use_rollups:
                country_counts: dict[str, int] = {}
                for f_name, f_val, f_count in all_top_res:
                    if f_name == "country" and f_val is not None:
                        country_counts[f_val] = country_counts.get(f_val, 0) + int(f_count)
                map_data = [{"country": k, "count": v} for k, v in country_counts.items()]
            else:
                map_q = SQL.MAP_DATA_BY_COUNTRY.format(
                    requests_metric=CANONICAL_METRICS["requests"],
                    table_name=table_name,
                    where_clause=where_clause,
                )
                map_data = [{"country": r[0], "count": r[1]} for r in runner.execute(map_q, params).fetchall()]
        timer.mark("map_data", t_map_0)

        payload: dict[str, Any] = {
            "data": results,
            "time_series": time_series,
            "map_data": map_data,
            "where_clause": where_clause,
            "interval": interval,
            "metric": chart_metric_out,
            "total_rows": total_rows,
            "total_rows_total": total_rows_total,
            "earliest_log_at": earliest_log_at,
            "latest_log_at": latest_log_at,
            "section_timings": section_timings,
            **runner.telemetry(),
        }
        if DASHBOARD_CACHE_TTL > 0 and cache_key is not None and int(payload.get("total_rows") or 0) > 0:
            _dashboard_cache[cache_key] = (now, payload.copy())
        return payload

    finally:
        runner.end_shared_active_hour_temps()
        _drop_temp_table(runner.con, temp_table, "get_aggregates")


# ── raw ───────────────────────────────────────────────────────────────────────


def get_raw_df(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int,
    columns: list[str],
):
    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        import pandas as pd

        return pd.DataFrame()

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols)

    if columns:
        select_cols = [f'"{c}"' for c in columns if c in actual_cols]
        wants_bot, wants_ngwaf_bot = _add_bot_columns(actual_cols, columns, select_cols)
        select_clause = ", ".join(select_cols)
    else:
        wants_bot = True  # By default, calculate bot data if possible
        wants_ngwaf_bot = True
        select_clause = "*"

    q = f"SELECT {select_clause} FROM {table_name} WHERE {where_clause} ORDER BY timestamp DESC LIMIT {limit}"
    df = runner.execute(q, params).fetchdf()

    if wants_bot or wants_ngwaf_bot:
        from backend.utils.bot_sources import enrich_bot_metadata

        enrich_bot_metadata(df)

    if columns:
        # Keep only requested columns in order
        existing_cols = [c for c in columns if c in df.columns]
        if "_bot_name" in columns and "_bot_name" not in existing_cols:
            df["_bot_name"] = "null"
            existing_cols.append("_bot_name")
        if "_ngwaf_bot_name" in columns and "_ngwaf_bot_name" not in existing_cols:
            df["_ngwaf_bot_name"] = None
            existing_cols.append("_ngwaf_bot_name")
        df = df[existing_cols]

    return df


# ── field_values ──────────────────────────────────────────────────────────────


def get_field_values(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    field: str,
    search: str,
    limit: int,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    runner = QueryRunner(con, src)

    clean_field = "".join(ch for ch in field if ch.isalnum() or ch == "_")
    if not clean_field:
        raise ValueError("Invalid field name")

    table_name = _safe_table(src["name"])

    def _resp(vals: list) -> dict:
        return {"values": vals, "field": field, **runner.telemetry()}

    # The reservoir sample has no time or filter scope.
    if not search and start_time is None and end_time is None and not filters:
        try:
            from backend.core.duckdb import _cache_dir

            cache_path = os.path.join(_cache_dir(src), "top_values.json")
            if os.path.exists(cache_path):
                with open(cache_path) as f:
                    top_values = json.load(f)
                if clean_field in top_values:
                    vals = top_values[clean_field][:limit]
                    if clean_field == "asn":
                        from backend.core.duckdb import enrich_asn_labels

                        enrich_asn_labels(vals, src["name"])
                    return _resp(vals)
        except Exception:
            logger.debug("get_field_values: step failed (ignored)", exc_info=True)

    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        return _resp([])

    # Exclude the field's own filter so picker shows all available values
    filters_excl = {k: v for k, v in filters.items() if k != field}
    params, where_clause = build_where_clause(start_time, end_time, filters_excl, actual_cols)

    if field == "_bot_name":
        # SPECIAL HANDLING FOR VIRTUAL BOT NAME FIELD
        if "ua" not in actual_cols:
            return _resp([])

        try:
            from backend.utils.bot_sources import build_matcher, get_bot_regex_pattern
        except ImportError:
            return _resp([])

        # Optimization: use regex pre-filter from known bot literals
        pattern = get_bot_regex_pattern(200)
        ua_filter = f"AND regexp_matches(ua, '{escape_sql_literal(pattern)}')" if pattern else ""

        # We query unique UAs to keep local bot-matching overhead manageable
        q = SQL.FIELD_VALUES_BOT_UA.format(
            requests_metric=CANONICAL_METRICS["requests"],
            table_name=table_name,
            where_clause=where_clause,
            ua_filter=ua_filter,
        )
        rows = runner.execute(q, params).fetchall()

        match_ua = build_matcher()
        bot_counts: dict[str, dict] = {}
        search_lower = search.lower() if search else ""

        for ua_val, cnt in rows:
            for entry in match_ua(str(ua_val) if ua_val else ""):
                bot_id = entry.get("id", "unknown")
                bot_name = entry.get("name", bot_id.replace("-", " ").title())

                # Apply search filter if provided (matching on ID or Name)
                if search_lower and search_lower not in bot_id.lower() and search_lower not in bot_name.lower():
                    continue

                if bot_id not in bot_counts:
                    bot_counts[bot_id] = {"value": bot_id, "label": bot_name, "count": 0}
                bot_counts[bot_id]["count"] += cnt

        sorted_vals = sorted(bot_counts.values(), key=lambda x: x["count"], reverse=True)
        return _resp(sorted_vals[:limit])

    # Virtual fields that explode a CSV backing column: filter-lookup
    # routes through the same unnest path so click-to-filter on a
    # specific signal / reason works the same as native columns.
    is_signals_individual = field in _VIRTUAL_BACKING
    backing_col = _VIRTUAL_BACKING[field] if is_signals_individual else clean_field
    if backing_col not in actual_cols:
        raise LookupError(f"Field '{field}' not found")

    search_params = list(params)

    if is_signals_individual or clean_field in ("waf_sig", "edge_score_reason"):
        search_cond = ""
        if search:
            search_cond = "AND trim(signal) ILIKE ?"
            search_params.append(f"%{search}%")
        q = SQL.FIELD_VALUES_VIRTUAL_SIGNALS.format(
            requests_metric=CANONICAL_METRICS["requests"],
            backing_col=backing_col,
            table_name=table_name,
            where_clause=where_clause,
            search_cond=search_cond,
            limit=limit,
        )
    else:
        search_cond = ""
        if search:
            extra_matches: list[str] = []
            if clean_field == "country":
                from backend.utils.countries import COUNTRY_MAP

                extra_matches = [c for c, name in COUNTRY_MAP.items() if search.lower() in name.lower()]
            elif clean_field == "asn":
                # ASN-name search: pre-fetch matching ASN ints from per-service
                # SQLite metadata, then inline them as a parameterised IN list.
                # Avoids ATTACH overhead / SQLite-extension dependency in DuckDB.
                from backend.core import metadata as metadata_db

                try:
                    extra_matches = [str(a) for a in metadata_db.asn_ints_for_search(src["name"], f"%{search}%")]
                except Exception:
                    extra_matches = []
            search_params.append(f"%{search}%")
            if extra_matches:
                placeholders = ",".join(["?"] * len(extra_matches))
                search_cond = (
                    f'AND (CAST("{clean_field}" AS VARCHAR) ILIKE ? '
                    f'OR CAST("{clean_field}" AS VARCHAR) IN ({placeholders}))'
                )
                search_params.extend(extra_matches)
            else:
                search_cond = f'AND CAST("{clean_field}" AS VARCHAR) ILIKE ?'

        q = SQL.FIELD_VALUES_NATIVE_COLUMN.format(
            clean_field=clean_field,
            requests_metric=CANONICAL_METRICS["requests"],
            table_name=table_name,
            where_clause=where_clause,
            search_cond=search_cond,
            limit=limit,
        )

    result = runner.execute_with_retry(q, search_params)
    if result is None:
        return _resp([])
    res = result.fetchall()

    vals = [{"value": r[0], "count": r[1]} for r in res]
    if clean_field == "asn" and vals:
        from backend.core.duckdb import enrich_asn_labels

        enrich_asn_labels(vals, src["name"])

    return _resp(vals)


_CacheRegistry.register("dashboard._dashboard_cache", _dashboard_cache)
