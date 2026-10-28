from __future__ import annotations  # noqa: I001

import contextvars
import logging
import threading
import time  # noqa: F401
import uuid
from collections.abc import Callable, Collection  # noqa: F401
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta  # noqa: F401
from typing import Any

import duckdb

from backend.repositories._base import (  # noqa: F401
    QueryRunner,
    _compact_sql_for_debug,
    _safe_table,
    record_duckdb_query,
)
from backend.repositories.insights import registry
from backend.repositories.insights.shaping import (
    TOP_K_INSIGHT_ITEMS as _TOP_K,  # noqa: F401
    build_error_insight_card,
    build_insight_card as _build_insight_card,  # noqa: F401
    build_insights_cache_key as _build_insights_cache_key,
    build_insufficient_baseline_card,
    compute_available_history_hours,
    compute_clamped_insight_windows as _compute_clamped_insight_windows,
    heap_to_sorted_items as _heap_to_sorted_items,  # noqa: F401
    iso_or_str as _iso_or_str,
    make_investigate_url_builder,
    p95_regressed as _p95_regressed,  # noqa: F401
    push_top_k as _push_top_k,  # noqa: F401
    shape_cache_collapse_detail,
    shape_insight_card_from_rows,
    share_exceeded as _share_exceeded,  # noqa: F401
)
from backend.repositories.utils.response_shaping import (
    INSIGHT_BUCKET_INTERVALS,
    build_insights_envelope,
    select_insight_bucket_interval,
)
from . import (
    definitions as _definitions,  # noqa: F401  side-effect import: registers insight defs with the registry
    definitions as _insights_sql,
    rollup_fastpath as _rollup_fastpath,
)
from .definitions import (  # noqa: F401
    COALESCED_ASN_AGGREGATES,
    COALESCED_CITY_AGGREGATES,
    COALESCED_DIM_AGGREGATES,
    COALESCED_FP_AGGREGATES,
    COALESCED_GEO_LATENCY_AGGREGATES,
    COALESCED_IP_EXTENDED_AGGREGATES,
    COALESCED_IP_SECURITY_AGGREGATES,
    COALESCED_SCALAR_EXTENDED_AGGREGATES,
    COALESCED_URL_AGGREGATES,
    COALESCED_URL_EXTENDED_AGGREGATES,
    REPEATED_BOT_UA_REGEX,
)
from .rollup_fastpath import (  # noqa: F401
    _coalesced_asn_aggregates,
    _coalesced_city_aggregates,
    _coalesced_dim_aggregates,
    _coalesced_fp_aggregates,
    _coalesced_geo_latency_aggregates,
    _coalesced_ip_security_aggregates,
    _coalesced_scalar_aggregates,
    _coalesced_url_aggregates,
    _col_or_null,
    _exec_coalesced,
    _execute_on_cursor,
)

logger = logging.getLogger(__name__)
INSIGHTS_CACHE_TTL = 300  # seconds
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache  # noqa: E402

_insights_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=500, ttl_seconds=INSIGHTS_CACHE_TTL)
_insights_cache_lock = threading.Lock()


def check_insights_cache(
    src: dict,
    window_hours: float = 24,
    baseline_hours: float = 168,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
    filters: dict | None = None,
) -> dict | None:
    if INSIGHTS_CACHE_TTL <= 0 or force_refresh:
        return None
    cache_key = _build_insights_cache_key(
        src["name"],
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
        clamp_cache_key=clamp_cache_key,
        filters=filters,
    )
    with _insights_cache_lock:
        entry = _insights_cache.get(cache_key)
    if entry is not None:
        cached = entry[1].copy()
        cached["is_cached"] = True
        return cached
    return None


def get_insights(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    window_hours: float = 24,
    baseline_hours: float = 168,
    service_id: str | None = None,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
    filters: dict | None = None,
    max_workers: int = 8,
) -> dict:
    source_name = src["name"]
    table_name = _safe_table(source_name)
    now, now_s, window_start_s, baseline_start_s = _compute_clamped_insight_windows(
        window_hours, baseline_hours, clamp_start, clamp_end, now=datetime.now(UTC)
    )
    cached = check_insights_cache(
        src,
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
        clamp_cache_key=clamp_cache_key,
        force_refresh=force_refresh,
        filters=filters,
    )
    if cached is not None:
        return cached
    cache_key = _build_insights_cache_key(
        source_name,
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
        clamp_cache_key=clamp_cache_key,
        filters=filters,
    )
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()

    def _insights_envelope(insights_list: list[dict], w_total: int = 0) -> dict[str, Any]:
        return build_insights_envelope(
            insights_list,
            window_start=window_start_s,
            window_end=now_s,
            baseline_start=baseline_start_s,
            baseline_end=window_start_s,
            computed_at=now_s,
            window_hours=window_hours,
            baseline_hours=baseline_hours,
            window_total_requests=w_total,
            **runner.telemetry(),
        )

    empty_resp = _insights_envelope([])
    if not actual_cols:
        try:
            from backend.core import storage as db_storage

            db_storage.update_storage_view(con, src, force=True)
            actual_cols = runner.get_schema_cols()
        except Exception:
            logger.debug("get_insights: runner.get_schema_cols failed (ignored)", exc_info=True)
        if not actual_cols:
            return empty_resp
    scratch_alias = f"insights_scratch_{uuid.uuid4().hex[:12]}"
    runner.con.execute(f"ATTACH ':memory:' AS {scratch_alias} (READ_ONLY FALSE)")
    scratch_attached = True
    temp_table = f"{scratch_alias}.insights_temp_{uuid.uuid4().hex[:12]}"
    needed_cols_set: set[str] = {"timestamp"}
    for d in registry.get_all():
        if all(col in actual_cols for col in d.required_fields):
            needed_cols_set.update(d.required_fields)
    fp_support_col = "ja4" if "ja4" in actual_cols else "ja3"
    needed_cols_set.update({fp_support_col, "region", "resp_bytes"})
    cols_sql = ", ".join(f'"{c}"' for c in sorted(needed_cols_set) if c in actual_cols)
    if not cols_sql:
        cols_sql = "*"
    elif "resp_bytes" not in actual_cols:
        cols_sql += ', 0::BIGINT AS "resp_bytes"'
    extra_filter_sql = ""
    if filters:
        from backend.repositories.utils.filters import build_where_clause

        _, f_where = build_where_clause(None, None, filters, actual_cols, inline_params=True)
        if f_where and f_where.strip():
            cleaned = f_where.strip()
            if cleaned.upper().startswith("WHERE "):
                cleaned = cleaned[6:].strip()
            if cleaned and cleaned != "1=1":
                extra_filter_sql = f" AND ({cleaned})"
    create_q = f"CREATE TABLE {temp_table} AS SELECT {cols_sql} FROM {table_name} WHERE timestamp >= CAST('{baseline_start_s}' AS TIMESTAMPTZ) AND timestamp <= CAST('{now_s}' AS TIMESTAMPTZ){extra_filter_sql}"
    temp_created = runner.create_temp_table(create_q, [])
    if not temp_created:
        temp_table = table_name  # Fallback to the lake view directly
    try:
        w_total = 0
        try:
            row_stats = runner.execute(
                f"SELECT min(timestamp), count(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {temp_table}",
                [window_start_s],
            ).fetchone()
            earliest_ts: Any = row_stats[0] if row_stats else None
            w_total = int(row_stats[1] or 0) if row_stats else 0
            available_history_hours = compute_available_history_hours(now, earliest_ts)
        except Exception:
            available_history_hours = 0.0
            try:
                w_total = runner.execute(
                    f"SELECT count(*) FROM {temp_table} WHERE timestamp >= CAST(? AS TIMESTAMPTZ)", [window_start_s]
                ).fetchone()[0]
            except Exception:
                w_total = 0
        try:
            from backend.core.log_fields import INSIGHT_DEFINITIONS as _defs

            defs_map = {d["id"]: d for d in _defs}
        except Exception:
            defs_map = {}

        def check_baseline(insight_id: str) -> dict | None:
            if available_history_hours < baseline_hours:
                d = defs_map.get(insight_id, {})
                reg = registry.get(insight_id)
                return build_insufficient_baseline_card(
                    insight_id=insight_id,
                    title=str(d.get("title", insight_id.replace("_", " ").title())),
                    description=str(d.get("description", "")),
                    category=str(reg.category) if reg else None,
                    baseline_hours=baseline_hours,
                    available_history_hours=available_history_hours,
                )
            return None

        table_name = temp_table
        make_investigate_url = make_investigate_url_builder(service_id, window_start_s, now_s)
        tasks: list[Callable[[], dict | None]] = []
        _debug_lock = threading.Lock()
        from backend.repositories.utils.filters import build_geo_select_clause

        loc_cols, label_expr, country_sel, region_sel = build_geo_select_clause(actual_cols)
        ua_mobile_sel = (
            "SUM(CASE WHEN \"ua\" ILIKE '%Mobi%' OR \"ua\" ILIKE '%Android%' OR \"ua\" ILIKE '%iPhone%' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)"
            if "ua" in actual_cols
            else "0"
        )
        url_col = '"url"' if "url" in actual_cols else "NULL"
        q_col = '"url"' if "url" in actual_cols else ('"digest"' if "digest" in actual_cols else "'(unknown)'")
        ua_col = '"ua"' if "ua" in actual_cols else "NULL"
        bot_ua_regex = REPEATED_BOT_UA_REGEX
        _coalesced_specs: list[tuple[str, bool, str, tuple, dict[str, Any]]] = [
            (
                "city",
                {"city", "status", "elapsed", "timestamp"}.issubset(actual_cols),
                "_coalesced_city_aggregates",
                (runner, table_name, window_start_s, label_expr, region_sel, country_sel, window_hours, baseline_hours),
                {},
            ),
            (
                "URL",
                {"url", "status", "cache", "elapsed", "timestamp"}.issubset(actual_cols),
                "_coalesced_url_aggregates",
                (runner, table_name, window_start_s),
                {"actual_cols": actual_cols},
            ),
            (
                "IP-security",
                {"ip", "url", "status", "timestamp"}.issubset(actual_cols),
                "_coalesced_ip_security_aggregates",
                (runner, table_name, window_start_s, window_hours, baseline_hours),
                {"actual_cols": actual_cols},
            ),
            (
                "FP",
                {"timestamp", "ip"}.issubset(actual_cols),
                "_coalesced_fp_aggregates",
                (runner, table_name, window_start_s, window_hours, baseline_hours, actual_cols),
                {},
            ),
            *(
                (lbl, cond, fn_name, (runner, table_name, window_start_s, actual_cols), {})
                for lbl, cond, fn_name in (
                    ("dim", "timestamp" in actual_cols, "_coalesced_dim_aggregates"),
                    ("geo-latency", "timestamp" in actual_cols, "_coalesced_geo_latency_aggregates"),
                    ("ASN", {"asn", "timestamp"}.issubset(actual_cols), "_coalesced_asn_aggregates"),
                    (
                        "scalar",
                        "timestamp" in actual_cols
                        and bool({"transport", "cache", "oconnect_ms", "ottfb"}.intersection(actual_cols)),
                        "_coalesced_scalar_aggregates",
                    ),
                )
            ),
        ]

        def _run_coalesced_spec(spec: tuple[str, bool, str, tuple, dict[str, Any]]) -> dict[str, list[tuple]]:
            label, enabled, fn_name, args, kwargs = spec
            if not enabled:
                return {}
            try:
                return globals()[fn_name](*args, shadow_cursor=runner.con.cursor(), debug_lock=_debug_lock, **kwargs)
            except Exception as e:
                logger.warning("[insights] coalesced %s aggregates failed, falling back: %s", label, e)
                return {}

        max_coalesce_workers = max(1, min(max_workers, 4))
        precomputed: dict[str, list[tuple]] = {}
        with ThreadPoolExecutor(
            max_workers=max_coalesce_workers, thread_name_prefix="insights-coalesce"
        ) as _coalesce_pool:
            for fut in [
                _coalesce_pool.submit(contextvars.copy_context().run, _run_coalesced_spec, spec)
                for spec in _coalesced_specs
            ]:
                precomputed.update(fut.result())
        for definition in registry.get_all():
            if not all(col in actual_cols for col in definition.required_fields):
                continue
            if definition.id == "waf_signal_spikes" and not temp_created:
                continue

            def _make_task(d=definition):
                def compute_insight() -> dict | None:
                    fp_col = "ja4" if "ja4" in actual_cols else "ja3"
                    extra_args = {}
                    if d.id == "impossible_distance":
                        from backend.utils.pop_utils import get_pop_lat_lon_map

                        pop_map = get_pop_lat_lon_map()
                        if not pop_map:
                            return None
                        extra_args["pop_values"] = ", ".join(
                            f"('{code}', {float(lat)}::DOUBLE, {float(lon)}::DOUBLE)"
                            for code, (lat, lon) in pop_map.items()
                            if lat is not None and lon is not None
                        )
                        extra_args["edge_filter"] = 'AND t."edge" = true' if "edge" in actual_cols else ""
                    else:
                        r = check_baseline(d.id)
                        if r:
                            return r
                    default_sql = getattr(_insights_sql, d.id.upper(), None)
                    is_default_template = default_sql is None or d.sql_template == default_sql
                    if is_default_template and d.id in precomputed:
                        rows = precomputed[d.id]
                    else:
                        try:
                            sql = d.sql_template.format(
                                table_name=table_name,
                                waf_table=table_name,
                                window_hours=window_hours,
                                baseline_hours=baseline_hours,
                                fp_col=fp_col,
                                loc_cols=loc_cols,
                                label_expr=label_expr,
                                country_sel=country_sel,
                                region_sel=region_sel,
                                ua_mobile_sel=ua_mobile_sel,
                                url_col=url_col,
                                q_col=q_col,
                                ua_col=ua_col,
                                bot_ua_regex=bot_ua_regex,
                                **extra_args,
                            )
                        except KeyError:
                            return None
                        params = [window_start_s] * sql.count("?")
                        rows = _execute_on_cursor(runner, _debug_lock, runner.con.cursor(), sql, params).fetchall()
                    return shape_insight_card_from_rows(
                        d,
                        rows,
                        service_id=src["name"],
                        window_hours=window_hours,
                        baseline_hours=baseline_hours,
                        fp_col=fp_col,
                        actual_cols=actual_cols,
                        mask_ips=mask_ips,
                        investigate_url_builder=make_investigate_url,
                        swallow_row_errors=True,
                    )

                compute_insight._insight_id = d.id  # type: ignore[attr-defined]
                compute_insight._insight_title = d.title  # type: ignore[attr-defined]
                compute_insight._insight_category = str(d.category)  # type: ignore[attr-defined]
                return compute_insight

            tasks.append(_make_task())
        insights_list: list[dict] = []

        def _safe_run(fn: Callable[[], dict | None]) -> dict | None:
            try:
                return fn()
            except Exception as e:
                insight_id = getattr(fn, "_insight_id", "unknown")
                return build_error_insight_card(
                    insight_id=insight_id,
                    title=getattr(fn, "_insight_title", insight_id.replace("_", " ").title()),
                    category=getattr(fn, "_insight_category", None),
                    error_message=str(e),
                )

        if tasks:
            pool_workers = max(1, min(max_workers, 3, len(tasks)))
            ctx_tasks = [(contextvars.copy_context(), t) for t in tasks]
            with ThreadPoolExecutor(max_workers=pool_workers, thread_name_prefix="insights") as pool:
                for res in pool.map(lambda pair: pair[0].run(_safe_run, pair[1]), ctx_tasks):
                    if res:
                        insights_list.append(res)
        payload = _insights_envelope(insights_list, w_total)
        if INSIGHTS_CACHE_TTL > 0:
            with _insights_cache_lock:
                _insights_cache[cache_key] = (now_s, payload)
        return payload
    finally:
        if scratch_attached:
            try:
                runner.con.execute(f"DETACH {scratch_alias}")
            except Exception:
                logger.debug("get_insights: runner.con.execute failed (ignored)", exc_info=True)


_BUCKET_INTERVALS = INSIGHT_BUCKET_INTERVALS


def get_cache_collapse_detail(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    url: str,
    window_hours: float,
    baseline_hours: float,
    *,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
) -> dict:
    source_name = src["name"]
    table_name = _safe_table(source_name)
    _, now_s, window_start_s, baseline_start_s = _compute_clamped_insight_windows(
        window_hours, baseline_hours, clamp_start, clamp_end, now=datetime.now(UTC)
    )
    runner = QueryRunner(con, src)
    rates_sql = f"""
        WITH base AS (
            SELECT cache,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "url" = ? AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
        )
        SELECT
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
            SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_miss,
            SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_pass,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
            SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_miss,
            SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_pass,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base
    """
    params = [window_start_s, window_start_s, url, baseline_start_s, now_s]
    rates_res = runner.execute(rates_sql, params).fetchone() or (0, 0, 0, 0, 0, 0, 0, 0)
    rates = tuple(int(v or 0) for v in rates_res)
    total_hours = window_hours + baseline_hours
    bucket_interval = select_insight_bucket_interval(total_hours)
    timeline_sql = f"""
        SELECT
            time_bucket(INTERVAL '{bucket_interval}', timestamp) AS bucket,
            COUNT(*) AS total_requests,
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) AS real_hits,
            SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) AS misses
        FROM {table_name}
        WHERE "url" = ? AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
        GROUP BY bucket
        ORDER BY bucket ASC
    """
    timeline_rows = runner.execute(timeline_sql, [url, baseline_start_s, now_s]).fetchall()
    misses_sql = f"""
        SELECT timestamp, cache, pop, ip, status
        FROM {table_name}
        WHERE "url" = ? AND starts_with(cache, 'MISS') AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
        ORDER BY timestamp DESC
        LIMIT 50
    """
    miss_rows = runner.execute(misses_sql, [url, baseline_start_s, now_s]).fetchall()
    return shape_cache_collapse_detail(
        url,
        rates,  # type: ignore[arg-type]
        timeline_rows,
        miss_rows,
        mask_ips=mask_ips,
        iso_formatter=_iso_or_str,
        **runner.telemetry(),
    )


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402
from backend.utils.module_shim import install_shim_module  # noqa: E402

_CacheRegistry.register("insights._insights_cache", _insights_cache)
install_shim_module(__name__, (_rollup_fastpath,), extra_module_names=("backend.repositories.insights",))
