"""Fastly Value repository — executive summary metrics from DuckDB logs."""

from __future__ import annotations

import logging
import re
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _safe_table,
    collect_hourly_bundle_paths,
    optional_col,
    quote_path_list,
    safe_interval,
    time_bucket_select,
)
from backend.repositories.utils.filters import build_where_clause
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.date_utils import parse_iso_utc, safe_iso

logger = logging.getLogger(__name__)

_IO_STATS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=60.0)

_IO_TRANSFORM_FIELD = "imgopto"
_IO_RESP_BYTES_FIELD = "imgopto_resp_body_bytes"
_IO_SHIELD_BYTES_FIELD = "imgopto_shield_resp_body_bytes"

_IO_FORMAT_FIELDS = {
    "webp": "imgopto_webp_count",
    "avif": "imgopto_avif_count",
    "jpeg": "imgopto_jpeg_count",
    "png": "imgopto_png_count",
    "gif": "imgopto_gif_count",
    "jpegxl": "imgopto_jpegxl_count",
    "svg": "imgopto_svg_count",
    "mp4": "imgopto_mp4_count",
}
_MODERN_FORMATS = {"webp", "avif", "jpegxl"}
_IO_COST_PER_TRANSFORM = 0.0025


def _fetch_io_stats(
    service_id: str,
    start_time: str | None,
    end_time: str | None,
    timer: SectionTimer,
) -> dict[str, Any] | None:
    """Fetch Image Optimizer metrics from the Fastly Historical Stats API."""
    from backend.config import get_fastly_api_key, get_fastly_logging_service_id
    from backend.core.fastly.client import fastly

    cdn_svc = get_fastly_logging_service_id(service_id)
    api_key = get_fastly_api_key(service_id)
    if not api_key or not cdn_svc:
        return None

    from backend.utils.date_utils import parse_iso_utc

    now = datetime.now(UTC)
    if start_time:
        start_dt = parse_iso_utc(start_time) or (now - timedelta(days=30))
    else:
        start_dt = now - timedelta(days=30)
    if end_time:
        end_dt = parse_iso_utc(end_time) or now
    else:
        end_dt = now

    start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = end_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    from_ts = int(start_dt.timestamp())
    to_ts = int(end_dt.timestamp())
    path = f"/stats/service/{cdn_svc}?by=day&from={from_ts}&to={to_ts}"

    cached = _IO_STATS_CACHE.get(path)
    if cached is not None:
        return cached

    _t = time.perf_counter()
    try:
        payload = fastly("GET", path, token=api_key)
    except Exception:
        logger.debug("IO stats fetch failed for %s", cdn_svc, exc_info=True)
        return None
    timer.mark("io_stats_fetch", _t)

    _IO_STATS_CACHE[path] = payload
    return payload


def _build_io_metrics(payload: dict, timer: SectionTimer) -> dict[str, Any]:
    """Aggregate IO metrics from Fastly stats records."""
    _t = time.perf_counter()

    total_transforms = 0
    total_io_bytes = 0
    total_io_shield_bytes = 0
    total_requests = 0
    daily_transforms: list[dict] = []
    daily_bandwidth: list[dict] = []
    format_counts: dict[str, int] = {fmt: 0 for fmt in _IO_FORMAT_FIELDS}

    for rec in payload.get("data", []):
        transforms = int(rec.get(_IO_TRANSFORM_FIELD, 0) or 0)
        io_bytes = int(rec.get(_IO_RESP_BYTES_FIELD, 0) or 0)
        io_shield_bytes = int(rec.get(_IO_SHIELD_BYTES_FIELD, 0) or 0)
        requests = int(rec.get("requests", 0) or 0)

        total_transforms += transforms
        total_io_bytes += io_bytes
        total_io_shield_bytes += io_shield_bytes
        total_requests += requests

        for fmt, field in _IO_FORMAT_FIELDS.items():
            format_counts[fmt] += int(rec.get(field, 0) or 0)

        ts = rec.get("start_time")
        if ts is not None:
            day_str = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d")
            daily_transforms.append({"time": day_str, "value": transforms})
            if io_bytes:
                daily_bandwidth.append({"time": day_str, "value": io_bytes})

    timer.mark("io_aggregate", _t)

    if total_transforms == 0 and total_io_bytes == 0:
        return {}

    total_format_count = sum(format_counts.values())
    format_distribution: list[dict[str, Any]] = []
    if total_format_count > 0:
        pairs = [(cnt, fmt) for fmt, cnt in format_counts.items() if cnt > 0]
        pairs.sort(reverse=True)
        format_distribution = [
            {"format": fmt, "count": cnt, "pct": round(cnt * 100.0 / total_format_count, 1)} for cnt, fmt in pairs
        ]

    modern_count = sum(format_counts.get(f, 0) for f in _MODERN_FORMATS)
    modern_format_pct = round(modern_count * 100.0 / total_format_count, 1) if total_format_count > 0 else None

    return {
        "io_transforms": total_transforms,
        "io_bandwidth_bytes": total_io_bytes,
        "io_shield_bandwidth_bytes": total_io_shield_bytes,
        "total_requests": total_requests,
        "io_time_series": daily_transforms,
        "io_pct_of_traffic": round(total_transforms * 100.0 / total_requests, 1) if total_requests > 0 else None,
        "io_estimated_cost_usd": round(total_transforms * _IO_COST_PER_TRANSFORM, 2),
        "format_distribution": format_distribution,
        "modern_format_pct": modern_format_pct,
        "io_bandwidth_time_series": daily_bandwidth,
    }


def _fetch_image_traffic_and_opportunities(
    runner: QueryRunner,
    table_name: str,
    where_clause: str,
    params: list[Any] | None,
    actual_cols: set[str],
    timer: SectionTimer,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Single-scan image traffic estimate + optimization opportunities via CTE."""
    if not ({"url", "resp_bytes", "status"} <= actual_cols):
        return {}, []

    _t = time.perf_counter()
    img_filter = (
        "status = 200 AND (url ILIKE '%.jpg%' OR url ILIKE '%.jpeg%' OR url ILIKE '%.png%' OR url ILIKE '%.gif%')"
    )
    sql = (
        f"WITH img AS ("
        f"  SELECT url, resp_bytes FROM {table_name} "
        f"  WHERE {where_clause} AND {img_filter}"
        f") "
        f"SELECT '__total__' AS url, COUNT(*) AS request_count, "
        f"SUM(resp_bytes) AS total_bytes, NULL AS avg_kb "
        f"FROM img "
        f"UNION ALL "
        f"SELECT url, COUNT(*) AS request_count, "
        f"SUM(resp_bytes) AS total_bytes, "
        f"ROUND(AVG(resp_bytes) / 1024, 1) AS avg_kb "
        f"FROM img "
        f"WHERE url NOT ILIKE '%auto=webp%' AND url NOT ILIKE '%format=auto%' "
        f"AND url NOT ILIKE '%format=webp%' AND url NOT ILIKE '%format=avif%' "
        f"GROUP BY url HAVING total_bytes > 1024 * 512 "
        f"ORDER BY total_bytes DESC LIMIT 11"
    )
    rows = runner.execute(sql, params).fetchall()
    timer.mark("image_traffic_and_opportunities", _t)

    estimate: dict[str, Any] = {}
    opps: list[dict[str, Any]] = []
    for r in rows:
        if r[0] == "__total__":
            if r[1]:
                image_requests = int(r[1])
                image_bytes = int(r[2] or 0)
                estimate = {
                    "image_request_count": image_requests,
                    "image_bandwidth_bytes": image_bytes,
                    "estimated_savings_bytes": int(image_bytes * 0.40),
                }
        else:
            opps.append(
                {
                    "url": r[0],
                    "request_count": int(r[1]),
                    "total_bytes": int(r[2]),
                    "avg_kb": float(r[3]) if r[3] is not None else None,
                }
            )

    return estimate, opps


def _build_io_per_request_metrics(
    runner: QueryRunner,
    table_name: str,
    where_clause: str,
    params: list[Any] | None,
    actual_cols: set[str],
    bucket_select: str,
    timer: SectionTimer,
) -> dict[str, Any]:
    """Compute per-request IO metrics from group M fields (io_input_bytes, io_output_bytes, io_input/output_format)."""
    result: dict[str, Any] = {}
    input_col = optional_col("io_input_bytes", actual_cols)
    output_col = optional_col("io_output_bytes", actual_cols)

    _t = time.perf_counter()
    agg_sql = (
        f"SELECT SUM({input_col}) AS total_input, SUM({output_col}) AS total_output "
        f"FROM {table_name} WHERE {where_clause} "
        f"AND {input_col} IS NOT NULL AND {input_col} > 0"
    )
    row = runner.execute(agg_sql, params).fetchone()
    timer.mark("io_per_request_agg", _t)

    if row and row[0] and row[1] and row[1] > 0:
        total_input, total_output = int(row[0]), int(row[1])
        result["io_actual_bandwidth_saved_bytes"] = total_input - total_output
        result["io_actual_compression_ratio"] = round(total_input / total_output, 2)

    has_formats = "io_input_format" in actual_cols and "io_output_format" in actual_cols
    if has_formats:
        ifmt_col = optional_col("io_input_format", actual_cols)
        ofmt_col = optional_col("io_output_format", actual_cols)

        _t = time.perf_counter()
        pairs_sql = (
            f"SELECT {ifmt_col} AS input_fmt, {ofmt_col} AS output_fmt, "
            f"COUNT(*) AS cnt, "
            f"ROUND(AVG(CASE WHEN {output_col} > 0 THEN {input_col}::DOUBLE / {output_col} ELSE NULL END), 2) AS avg_ratio "
            f"FROM {table_name} WHERE {where_clause} "
            f"AND {ifmt_col} != '' AND {ofmt_col} != '' "
            f"GROUP BY {ifmt_col}, {ofmt_col} ORDER BY cnt DESC LIMIT 20"
        )
        pair_rows = runner.execute(pairs_sql, params).fetchall()
        timer.mark("io_format_pairs", _t)

        if pair_rows:
            result["io_format_conversion_pairs"] = [
                {
                    "input_format": r[0],
                    "output_format": r[1],
                    "count": int(r[2]),
                    "avg_ratio": float(r[3]) if r[3] else None,
                }
                for r in pair_rows
            ]

    _t = time.perf_counter()
    ts_sql = (
        f"SELECT {bucket_select}, "
        f"SUM({input_col}) - SUM({output_col}) AS saved "
        f"FROM {table_name} WHERE {where_clause} "
        f"AND {input_col} IS NOT NULL AND {input_col} > 0 "
        f"GROUP BY bucket ORDER BY bucket"
    )
    ts_rows = runner.execute(ts_sql, params).fetchall()
    timer.mark("io_compression_ts", _t)

    if ts_rows:
        result["io_compression_time_series"] = [
            {"time": safe_iso(r[0]), "value": int(r[1]) if r[1] is not None else 0} for r in ts_rows
        ]

    return result


# ── Overview rollup reader ──────────────────────────────────────────────


def _try_overview_from_rollup(
    runner: QueryRunner,
    *,
    start_time: str | None,
    end_time: str | None,
    chart_interval: str,
    include_overview: bool,
    has_resp_bytes: bool,
    has_shield: bool,
    has_waf: bool,
    has_elapsed: bool,
    timer: SectionTimer,
) -> dict[str, Any] | None:
    """Serve the overview+caching sections from pre-rolled hourly parquets.

    Returns ``{"caching": {...}, "overview": {...}}`` when eligible, or
    ``None`` to fall through to the raw scan.
    """
    import os

    from backend.core.rollups import OVERVIEW_BUNDLE_FILENAME, _hour_bundled_root

    if not start_time or not end_time:
        return None
    st = parse_iso_utc(start_time)
    et = parse_iso_utc(end_time)
    if st is None or et is None or et <= st:
        return None
    if (et - st) > timedelta(days=366):
        return None

    bundled_root = _hour_bundled_root(runner.src)
    if not os.path.isdir(bundled_root):
        return None

    collected = collect_hourly_bundle_paths(
        runner.src,
        st,
        et,
        bundled_root,
        OVERVIEW_BUNDLE_FILENAME,
    )
    if collected is None:
        return None
    rollup_paths, crosses_active = collected

    if not rollup_paths and not crosses_active:
        return None

    interval = safe_interval(chart_interval, "1 day")
    _HIT_STATES = "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"

    select_clauses: list[str] = []

    if rollup_paths:
        paths_sql = quote_path_list(rollup_paths)
        st_tz = st.astimezone(UTC).isoformat()
        et_tz = et.astimezone(UTC).isoformat()
        select_clauses.append(
            f"SELECT time_bucket(INTERVAL '{interval}', hour_start) AS bucket, "
            f"  SUM(requests) AS total, SUM(hit_requests) AS hit_cnt, "
            f"  SUM(miss_requests) AS miss_cnt, SUM(pass_requests) AS pass_cnt, "
            f"  SUM(synth_requests) AS synth_cnt, SUM(origin_requests) AS origin_cnt, "
            f"  SUM(bandwidth_saved_bytes) AS bandwidth_saved, "
            f"  SUM(total_bandwidth_bytes) AS total_bytes, "
            f"  SUM(shield_hit_requests) AS shield_hit, "
            f"  SUM(shield_total_requests) AS shield_total, "
            f"  SUM(threats_blocked) AS threats_cnt, "
            f"  SUM(hit_elapsed_sum) AS sum_hit_elapsed, "
            f"  SUM(hit_elapsed_count) AS cnt_hit_elapsed, "
            f"  SUM(miss_elapsed_sum) AS sum_miss_elapsed, "
            f"  SUM(miss_elapsed_count) AS cnt_miss_elapsed "
            f"FROM read_parquet([{paths_sql}]) "
            f"WHERE hour_start >= TIMESTAMPTZ '{st_tz}' "
            f"  AND hour_start < TIMESTAMPTZ '{et_tz}' "
            f"GROUP BY 1"
        )

    if crosses_active:
        active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(st, active_hour_dt)
        live_st_tz = live_start.astimezone(UTC).isoformat()
        live_et_tz = et.astimezone(UTC).isoformat()

        sum_miss_elapsed = "COALESCE(SUM(elapsed) FILTER (WHERE cache = 'MISS'), 0.0)" if has_elapsed else "0.0"
        cnt_miss_elapsed = "COUNT(elapsed) FILTER (WHERE cache = 'MISS')" if has_elapsed else "0"

        table_name = _safe_table(runner.src["name"])
        select_clauses.append(
            f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS bucket, "
            f"  COUNT(*) AS total, "
            f"  COUNT(*) FILTER (WHERE cache IN {_HIT_STATES}) AS hit_cnt, "
            f"  COUNT(*) FILTER (WHERE cache = 'MISS') AS miss_cnt, "
            f"  COUNT(*) FILTER (WHERE cache = 'PASS') AS pass_cnt, "
            f"  COUNT(*) FILTER (WHERE cache = 'SYNTH') AS synth_cnt, "
            f"  COUNT(*) FILTER (WHERE cache IN ('MISS','PASS','SYNTH','ERROR')) AS origin_cnt, "
            f"  COALESCE(SUM(resp_bytes) FILTER (WHERE cache IN {_HIT_STATES}), 0) AS bandwidth_saved, "
            f"  COALESCE(SUM(resp_bytes), 0) AS total_bytes, "
            f"  {'COUNT(*) FILTER (WHERE is_shield = true AND cache IN ' + _HIT_STATES + ')' if has_shield else '0'} AS shield_hit, "
            f"  {'COUNT(*) FILTER (WHERE is_shield = true)' if has_shield else '0'} AS shield_total, "
            f"  {'COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 406)' if has_waf else '0'} AS threats_cnt, "
            f"  {'COALESCE(SUM(elapsed) FILTER (WHERE cache IN ' + _HIT_STATES + '), 0.0)' if has_elapsed else '0.0'} AS sum_hit_elapsed, "
            f"  {'COUNT(elapsed) FILTER (WHERE cache IN ' + _HIT_STATES + ')' if has_elapsed else '0'} AS cnt_hit_elapsed, "
            f"  {sum_miss_elapsed} AS sum_miss_elapsed, "
            f"  {cnt_miss_elapsed} AS cnt_miss_elapsed "
            f"FROM {table_name} "
            f"WHERE timestamp >= TIMESTAMPTZ '{live_st_tz}' "
            f"  AND timestamp < TIMESTAMPTZ '{live_et_tz}'"
        )

    if not select_clauses:
        return None

    unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
    final_sql = (
        f"SELECT bucket, SUM(total) AS total, SUM(hit_cnt) AS hit_cnt, "
        f"  SUM(miss_cnt) AS miss_cnt, SUM(pass_cnt) AS pass_cnt, "
        f"  SUM(synth_cnt) AS synth_cnt, SUM(origin_cnt) AS origin_cnt, "
        f"  SUM(bandwidth_saved) AS bandwidth_saved, SUM(total_bytes) AS total_bytes, "
        f"  SUM(shield_hit) AS shield_hit, SUM(shield_total) AS shield_total, "
        f"  SUM(threats_cnt) AS threats_cnt, "
        f"  SUM(sum_hit_elapsed) AS sum_hit_elapsed, SUM(cnt_hit_elapsed) AS cnt_hit_elapsed, "
        f"  SUM(sum_miss_elapsed) AS sum_miss_elapsed, SUM(cnt_miss_elapsed) AS cnt_miss_elapsed "
        f"FROM ({unioned}) "
        f"WHERE bucket IS NOT NULL "
        f"GROUP BY bucket ORDER BY bucket"
    )

    try:
        rows = runner.execute(final_sql, []).fetchall()
    except duckdb.Error as e:
        logger.debug("[overview_rollup] read failed, falling back to raw: %s", e)
        return None

    col_names = [d[0] for d in runner.con.description]
    buckets = [dict(zip(col_names, r)) for r in rows]

    if not buckets:
        return None

    _sum = lambda key: sum(b.get(key, 0) or 0 for b in buckets)  # noqa: E731
    total_requests = _sum("total")
    total_hits = _sum("hit_cnt")
    total_misses = _sum("miss_cnt")
    total_passes = _sum("pass_cnt")
    total_synths = _sum("synth_cnt")

    shield_eff = None
    if has_shield:
        sh_hit = _sum("shield_hit")
        sh_total = _sum("shield_total")
        shield_eff = round(sh_hit * 100.0 / sh_total, 1) if sh_total > 0 else None

    caching: dict[str, Any] = {
        "origin_offload_pct": round(total_hits * 100.0 / total_requests, 1) if total_requests > 0 else None,
        "bandwidth_saved_bytes": _sum("bandwidth_saved") if has_resp_bytes else None,
        "shield_effectiveness_pct": shield_eff,
        "total_requests": total_requests,
        "hit_requests": total_hits,
        "miss_requests": total_misses,
        "pass_requests": total_passes,
        "synth_requests": total_synths,
    }

    caching["offload_time_series"] = [
        {
            "time": safe_iso(b["bucket"]),
            "value": round(b["hit_cnt"] * 100.0 / b["total"], 1) if b["total"] > 0 else 0,
        }
        for b in buckets
    ]

    state_ts: list[dict] = []
    for b in buckets:
        t_str = safe_iso(b["bucket"])
        other = b["total"] - b["hit_cnt"] - b["miss_cnt"] - b["pass_cnt"]
        state_ts.append({"time": t_str, "value": int(b["hit_cnt"]), "category": "HIT"})
        state_ts.append({"time": t_str, "value": int(b["miss_cnt"]), "category": "MISS"})
        state_ts.append({"time": t_str, "value": int(b["pass_cnt"]), "category": "PASS"})
        if other > 0:
            state_ts.append({"time": t_str, "value": int(other), "category": "OTHER"})
    caching["cache_state_time_series"] = state_ts

    out: dict[str, Any] = {"caching": caching}

    if include_overview:
        avg_hit = None
        avg_miss = None
        if has_elapsed:
            s_h = _sum("sum_hit_elapsed")
            c_h = _sum("cnt_hit_elapsed")
            s_m = _sum("sum_miss_elapsed")
            c_m = _sum("cnt_miss_elapsed")
            avg_hit = s_h / c_h if c_h > 0 else None
            avg_miss = s_m / c_m if c_m > 0 else None
        accel = round(avg_miss / avg_hit, 1) if avg_hit and avg_miss and avg_hit > 0 else None

        out["overview"] = {
            "origin_offload_pct": round(total_hits * 100.0 / total_requests, 1) if total_requests > 0 else None,
            "threats_blocked": _sum("threats_cnt") if has_waf else None,
            "cache_acceleration_factor": accel,
            "total_requests": total_requests,
            "total_bandwidth_bytes": _sum("total_bytes") if has_resp_bytes else None,
            "edge_hit_requests": total_hits,
            "origin_requests": _sum("origin_cnt"),
        }

    return out


def _try_network_from_rollup(
    runner: QueryRunner,
    *,
    start_time: str | None,
    end_time: str | None,
    actual_cols: set[str],
) -> dict[str, Any] | None:
    """Serve the network section aggregates from pre-rolled hourly parquets.

    Returns the ``net`` dict with percentage metrics when eligible, or
    ``None`` to fall through to the raw scan. Does NOT include the
    protocol time series (that still needs a raw scan per-protocol
    per-bucket).
    """
    import os

    from backend.core.rollups import NETWORK_SUMMARY_BUNDLE_FILENAME, _hour_bundled_root

    if not start_time or not end_time:
        return None
    st = parse_iso_utc(start_time)
    et = parse_iso_utc(end_time)
    if st is None or et is None or et <= st:
        return None
    if (et - st) > timedelta(days=366):
        return None

    bundled_root = _hour_bundled_root(runner.src)
    if not os.path.isdir(bundled_root):
        return None

    collected = collect_hourly_bundle_paths(
        runner.src,
        st,
        et,
        bundled_root,
        NETWORK_SUMMARY_BUNDLE_FILENAME,
    )
    if collected is None:
        return None
    rollup_paths, crosses_active = collected

    if not rollup_paths and not crosses_active:
        return None

    has_protocol = "protocol" in actual_cols
    has_is_ssl = "is_ssl" in actual_cols
    has_ipv6 = "ipv6" in actual_cols

    select_clauses: list[str] = []

    if rollup_paths:
        paths_sql = quote_path_list(rollup_paths)
        st_tz = st.astimezone(UTC).isoformat()
        et_tz = et.astimezone(UTC).isoformat()
        select_clauses.append(
            f"SELECT SUM(requests) AS total_requests, "
            f"  SUM(http3_requests) AS http3, "
            f"  SUM(h2_requests) AS h2, "
            f"  SUM(tls_requests) AS tls, "
            f"  SUM(ipv6_requests) AS ipv6 "
            f"FROM read_parquet([{paths_sql}]) "
            f"WHERE hour_start >= TIMESTAMPTZ '{st_tz}' "
            f"  AND hour_start < TIMESTAMPTZ '{et_tz}'"
        )

    if crosses_active:
        active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(st, active_hour_dt)
        live_st_tz = live_start.astimezone(UTC).isoformat()
        live_et_tz = et.astimezone(UTC).isoformat()

        http3_clause = "COUNT(*) FILTER (WHERE protocol = 'HTTP/3')" if has_protocol else "NULL"
        h2_clause = "COUNT(*) FILTER (WHERE protocol = 'HTTP/2')" if has_protocol else "NULL"
        tls_clause = (
            "COUNT(*) FILTER (WHERE is_ssl = true)"
            if "is_ssl" in actual_cols
            else ("COUNT(*) FILTER (WHERE tls IS NOT NULL AND tls != '')" if "tls" in actual_cols else "NULL")
        )
        ipv6_clause = "COUNT(*) FILTER (WHERE ipv6 = true)" if has_ipv6 else "NULL"

        table_name = _safe_table(runner.src["name"])
        select_clauses.append(
            f"SELECT COUNT(*) AS total_requests, "
            f"  {http3_clause} AS http3, "
            f"  {h2_clause} AS h2, "
            f"  {tls_clause} AS tls, "
            f"  {ipv6_clause} AS ipv6 "
            f"FROM {table_name} "
            f"WHERE timestamp >= TIMESTAMPTZ '{live_st_tz}' "
            f"  AND timestamp < TIMESTAMPTZ '{live_et_tz}'"
        )

    combined_sql = (
        "SELECT SUM(total_requests) AS total_requests, "
        "SUM(http3) AS http3, SUM(h2) AS h2, "
        "SUM(tls) AS tls, SUM(ipv6) AS ipv6 "
        f"FROM ({' UNION ALL '.join(select_clauses)})"
    )
    try:
        row = runner.execute(combined_sql).fetchone()
    except duckdb.Error as e:
        logger.debug("[network_rollup] read failed, falling back to raw: %s", e)
        return None

    if not row or not row[0]:
        return None

    total = int(row[0])
    if total == 0:
        return {"total_requests": 0, "http3_pct": None, "h2_pct": None, "tls_pct": None, "ipv6_pct": None}

    def _pct(val: Any) -> float | None:
        if val is None:
            return None
        return round(int(val) * 100.0 / total, 1)

    return {
        "total_requests": total,
        "http3_pct": _pct(row[1]),
        "h2_pct": _pct(row[2]),
        "tls_pct": _pct(row[3]),
        "ipv6_pct": _pct(row[4]),
    }


def get_summary(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    chart_interval: str = "1 day",
    sections: set[str] | None = None,
    service_id: str | None = None,
) -> dict[str, Any]:
    timer = SectionTimer()
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)

    _t = time.perf_counter()
    actual_cols = set(runner.get_schema_cols())
    timer.mark("get_schema_cols", _t)

    if not actual_cols:
        return {
            "section_timings": section_timings,
            **runner.telemetry(),
        }

    _t = time.perf_counter()
    params, where_clause = build_where_clause(
        start_time,
        end_time,
        filters,
        list(actual_cols),
        inline_params=True,
    )
    timer.mark("build_where_clause", _t)

    interval = safe_interval(chart_interval, "1 day")
    bucket_select = time_bucket_select(interval)

    has_cache = "cache" in actual_cols
    has_elapsed = "elapsed" in actual_cols
    has_status = "status" in actual_cols
    has_resp_bytes = "resp_bytes" in actual_cols
    has_is_ssl = "is_ssl" in actual_cols or "tls" in actual_cols
    has_protocol = "protocol" in actual_cols
    has_ipv6 = "ipv6" in actual_cols
    has_waf = "waf" in actual_cols and "waf_resp" in actual_cols
    has_bot = "_bot_name" in actual_cols or "ua" in actual_cols
    has_shield = "is_shield" in actual_cols
    has_h2 = "h2" in actual_cols or "protocol" in actual_cols

    result: dict[str, Any] = {
        "section_timings": section_timings,
        **runner.telemetry(),
    }

    # ── Rollup fast path for overview + caching ────────────────────────
    _overview_done = False
    _caching_done = False
    _HIT_STATES = "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"

    if (_want("caching") or _want("overview")) and has_cache and not filters:
        _t = time.perf_counter()
        rollup_result = _try_overview_from_rollup(
            runner,
            start_time=start_time,
            end_time=end_time,
            chart_interval=chart_interval,
            include_overview=_want("overview"),
            has_resp_bytes=has_resp_bytes,
            has_shield=has_shield,
            has_waf=has_waf,
            has_elapsed=has_elapsed,
            timer=timer,
        )
        if rollup_result is not None:
            if "caching" in rollup_result:
                result["caching"] = rollup_result["caching"]
                _caching_done = True
            if "overview" in rollup_result:
                result["overview"] = rollup_result["overview"]
                _overview_done = True
        timer.mark("overview_rollup_attempt", _t)

    # ── Combined overview + caching (raw scan fallback) ──────────────
    # When both sections are requested, a single GROUP BY query produces
    # per-bucket aggregates for all overview AND caching stats + both
    # time-series. This replaces 4 independent full-table scans with 1.

    if _want("caching") and has_cache and not _caching_done:
        _t = time.perf_counter()
        include_overview = _want("overview")

        combined_parts: list[str] = [
            bucket_select,
            "COUNT(*) AS total",
            f"COUNT(*) FILTER (WHERE cache IN {_HIT_STATES}) AS hit_cnt",
            "COUNT(*) FILTER (WHERE cache = 'MISS') AS miss_cnt",
            "COUNT(*) FILTER (WHERE cache = 'PASS') AS pass_cnt",
            "COUNT(*) FILTER (WHERE cache = 'SYNTH') AS synth_cnt",
        ]
        if has_resp_bytes:
            combined_parts.append(f"SUM(resp_bytes) FILTER (WHERE cache IN {_HIT_STATES}) AS bandwidth_saved")
        if has_shield:
            combined_parts.append(f"COUNT(*) FILTER (WHERE is_shield = true AND cache IN {_HIT_STATES}) AS shield_hit")
            combined_parts.append("COUNT(*) FILTER (WHERE is_shield = true) AS shield_total")
        if include_overview:
            if has_resp_bytes:
                combined_parts.append("SUM(resp_bytes) AS total_bytes")
            combined_parts.append("COUNT(*) FILTER (WHERE cache IN ('MISS', 'PASS', 'SYNTH', 'ERROR')) AS origin_cnt")
            if has_waf:
                combined_parts.append("COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 406) AS threats_cnt")
            if has_elapsed:
                combined_parts.append(f"SUM(elapsed) FILTER (WHERE cache IN {_HIT_STATES}) AS sum_hit_elapsed")
                combined_parts.append(f"COUNT(elapsed) FILTER (WHERE cache IN {_HIT_STATES}) AS cnt_hit_elapsed")
                combined_parts.append("SUM(elapsed) FILTER (WHERE cache = 'MISS') AS sum_miss_elapsed")
                combined_parts.append("COUNT(elapsed) FILTER (WHERE cache = 'MISS') AS cnt_miss_elapsed")

        combined_sql = (
            f"SELECT {', '.join(combined_parts)} FROM {table_name} WHERE {where_clause} GROUP BY bucket ORDER BY bucket"
        )
        rows = runner.execute(combined_sql, params).fetchall()
        col_names = [d[0] for d in runner.con.description]
        buckets = [dict(zip(col_names, r)) for r in rows]
        timer.mark("overview_caching_combined", _t)

        _sum = lambda key: sum(b.get(key, 0) or 0 for b in buckets)  # noqa: E731
        total_requests = _sum("total")
        total_hits = _sum("hit_cnt")
        total_misses = _sum("miss_cnt")
        total_passes = _sum("pass_cnt")
        total_synths = _sum("synth_cnt")

        shield_eff = None
        if has_shield:
            sh_hit = _sum("shield_hit")
            sh_total = _sum("shield_total")
            shield_eff = round(sh_hit * 100.0 / sh_total, 1) if sh_total > 0 else None

        caching: dict[str, Any] = {
            "origin_offload_pct": round(total_hits * 100.0 / total_requests, 1) if total_requests > 0 else None,
            "bandwidth_saved_bytes": _sum("bandwidth_saved") if has_resp_bytes else None,
            "shield_effectiveness_pct": shield_eff,
            "total_requests": total_requests,
            "hit_requests": total_hits,
            "miss_requests": total_misses,
            "pass_requests": total_passes,
            "synth_requests": total_synths,
        }

        caching["offload_time_series"] = [
            {
                "time": safe_iso(b["bucket"]),
                "value": round(b["hit_cnt"] * 100.0 / b["total"], 1) if b["total"] > 0 else 0,
            }
            for b in buckets
        ]

        state_ts: list[dict] = []
        for b in buckets:
            t_str = safe_iso(b["bucket"])
            other = b["total"] - b["hit_cnt"] - b["miss_cnt"] - b["pass_cnt"]
            state_ts.append({"time": t_str, "value": int(b["hit_cnt"]), "category": "HIT"})
            state_ts.append({"time": t_str, "value": int(b["miss_cnt"]), "category": "MISS"})
            state_ts.append({"time": t_str, "value": int(b["pass_cnt"]), "category": "PASS"})
            if other > 0:
                state_ts.append({"time": t_str, "value": int(other), "category": "OTHER"})
        caching["cache_state_time_series"] = state_ts

        result["caching"] = caching

        if include_overview:
            avg_hit = None
            avg_miss = None
            if has_elapsed:
                s_h = _sum("sum_hit_elapsed")
                c_h = _sum("cnt_hit_elapsed")
                s_m = _sum("sum_miss_elapsed")
                c_m = _sum("cnt_miss_elapsed")
                avg_hit = s_h / c_h if c_h > 0 else None
                avg_miss = s_m / c_m if c_m > 0 else None
            accel = round(avg_miss / avg_hit, 1) if avg_hit and avg_miss and avg_hit > 0 else None

            result["overview"] = {
                "origin_offload_pct": round(total_hits * 100.0 / total_requests, 1) if total_requests > 0 else None,
                "threats_blocked": _sum("threats_cnt") if has_waf else None,
                "cache_acceleration_factor": accel,
                "total_requests": total_requests,
                "total_bandwidth_bytes": _sum("total_bytes") if has_resp_bytes else None,
                "edge_hit_requests": total_hits,
                "origin_requests": _sum("origin_cnt"),
            }
            _overview_done = True

    # ── Overview (standalone fallback when caching wasn't co-requested) ──
    if _want("overview") and not _overview_done:
        _t = time.perf_counter()
        parts: list[str] = ["COUNT(*) AS total_requests"]
        if has_resp_bytes:
            parts.append("SUM(resp_bytes) AS total_bandwidth_bytes")
        if has_cache:
            parts.append(f"COUNT(*) FILTER (WHERE cache IN {_HIT_STATES}) AS edge_hit_requests")
            parts.append("COUNT(*) FILTER (WHERE cache IN ('MISS', 'PASS', 'SYNTH', 'ERROR')) AS origin_requests")
        if has_waf:
            parts.append("COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 406) AS threats_blocked")
        if has_elapsed and has_cache:
            parts.append(f"AVG(elapsed) FILTER (WHERE cache IN {_HIT_STATES}) AS avg_hit_elapsed")
            parts.append("AVG(elapsed) FILTER (WHERE cache = 'MISS') AS avg_miss_elapsed")

        sql = f"SELECT {', '.join(parts)} FROM {table_name} WHERE {where_clause}"
        row = runner.execute(sql, params).fetchone()
        timer.mark("overview_query", _t)

        if row:
            cols = [d[0] for d in runner.con.description]
            d = dict(zip(cols, row))
            total = d.get("total_requests", 0) or 0
            hits = d.get("edge_hit_requests", 0) or 0
            offload = round(hits * 100.0 / total, 1) if total > 0 else None
            avg_hit = d.get("avg_hit_elapsed")
            avg_miss = d.get("avg_miss_elapsed")
            accel = round(avg_miss / avg_hit, 1) if avg_hit and avg_miss and avg_hit > 0 else None

            result["overview"] = {
                "origin_offload_pct": offload,
                "threats_blocked": d.get("threats_blocked"),
                "cache_acceleration_factor": accel,
                "total_requests": total,
                "total_bandwidth_bytes": d.get("total_bandwidth_bytes"),
                "edge_hit_requests": hits,
                "origin_requests": d.get("origin_requests"),
            }

    # ── Security ─────────────────────────────────────────────────────────
    if _want("security") and has_waf:
        _t = time.perf_counter()
        sec_parts = [
            "COUNT(*) AS total_requests",
            "COUNT(*) FILTER (WHERE waf = 1) AS waf_inspected",
            "COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 406) AS waf_blocked",
            "COUNT(*) FILTER (WHERE waf = 1 AND waf_resp NOT IN (406) AND waf_sig IS NOT NULL AND waf_sig != '') AS waf_logged",
            "COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 200) AS waf_passed",
        ]
        sql = f"SELECT {', '.join(sec_parts)} FROM {table_name} WHERE {where_clause}"
        row = runner.execute(sql, params).fetchone()
        timer.mark("security_agg_query", _t)

        security: dict[str, Any] = {}
        if row:
            cols = [d[0] for d in runner.con.description]
            security = dict(zip(cols, row))

        # Threat timeline
        _t = time.perf_counter()
        threat_sql = (
            f"SELECT {bucket_select}, "
            f"COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 406) AS blocked, "
            f"COUNT(*) FILTER (WHERE waf = 1 AND waf_resp NOT IN (406) AND waf_sig IS NOT NULL AND waf_sig != '') AS logged "
            f"FROM {table_name} WHERE {where_clause} "
            f"GROUP BY bucket ORDER BY bucket"
        )
        threat_rows = runner.execute(threat_sql, params).fetchall()
        timer.mark("security_threat_ts", _t)
        threat_ts: list[dict] = []
        for r in threat_rows:
            t_str = safe_iso(r[0])
            if r[1]:
                threat_ts.append({"time": t_str, "value": int(r[1]), "category": "blocked"})
            if r[2]:
                threat_ts.append({"time": t_str, "value": int(r[2]), "category": "logged"})
        security["threat_time_series"] = threat_ts

        # Top WAF signals (from waf_sig CSV column if present)
        if "waf_sig" in actual_cols:
            _t = time.perf_counter()
            sig_sql = (
                f"SELECT signal, COUNT(*) AS cnt FROM ("
                f"  SELECT UNNEST(string_split(waf_sig, ',')) AS signal "
                f"  FROM {table_name} WHERE {where_clause} AND waf_sig IS NOT NULL AND waf_sig != ''"
                f") GROUP BY signal ORDER BY cnt DESC LIMIT 10"
            )
            sig_rows = runner.execute(sig_sql, params).fetchall()
            timer.mark("security_top_signals", _t)
            security["top_waf_signals"] = [{"signal": r[0], "count": int(r[1])} for r in sig_rows]

        result["security"] = security

    # ── Bots ─────────────────────────────────────────────────────────────
    if _want("bots") and has_bot:
        _t = time.perf_counter()
        bot_col = "_bot_name" if "_bot_name" in actual_cols else None
        has_ua = "ua" in actual_cols

        has_waf_sig = "waf_sig" in actual_cols
        verified_bots_expr = "COUNT(*) FILTER (WHERE waf_sig ILIKE '%VERIFIED-BOT.%')" if has_waf_sig else "NULL"

        if bot_col:
            bot_sql = (
                f"SELECT COUNT(*) AS total_requests, "
                f"COUNT(*) FILTER (WHERE {bot_col} IS NOT NULL AND {bot_col} != '') AS bot_requests, "
                f"{verified_bots_expr} AS verified_bots "
                f"FROM {table_name} WHERE {where_clause}"
            )
            row = runner.execute(bot_sql, params).fetchone()
            timer.mark("bots_agg_query", _t)

            bots: dict[str, Any] = {
                "total_requests": row[0] if row else 0,
                "bot_requests": row[1] if row else 0,
                "verified_bots": row[2] if row else None,
            }

            _t = time.perf_counter()
            top_sql = (
                f"SELECT {bot_col} AS bot_name, COUNT(*) AS cnt "
                f"FROM {table_name} WHERE {where_clause} "
                f"AND {bot_col} IS NOT NULL AND {bot_col} != '' "
                f"GROUP BY bot_name ORDER BY cnt DESC LIMIT 10"
            )
            top_rows = runner.execute(top_sql, params).fetchall()
            timer.mark("bots_top_query", _t)
            bots["top_bots"] = [{"name": r[0], "count": int(r[1])} for r in top_rows]

            result["bots"] = bots

        elif has_ua:
            from backend.utils.bot_sources import get_bot_regex_pattern, get_ilike_prefilter_literals

            pattern = get_bot_regex_pattern(500)
            if pattern:
                pattern_sql = pattern.replace("'", "''")
                bot_filter = f"regexp_matches(ua, '{pattern_sql}')"
            else:
                bot_filter = "ua LIKE '%bot%' OR ua LIKE '%crawl%' OR ua LIKE '%spider%'"

            bot_sql = (
                f"SELECT COUNT(*) AS total_requests, "
                f"COUNT(*) FILTER (WHERE ua IS NOT NULL AND ({bot_filter})) AS bot_requests, "
                f"{verified_bots_expr} AS verified_bots "
                f"FROM {table_name} WHERE {where_clause}"
            )
            row = runner.execute(bot_sql, params).fetchone()
            timer.mark("bots_agg_query", _t)

            bots_result: dict[str, Any] = {
                "total_requests": row[0] if row else 0,
                "bot_requests": row[1] if row else 0,
                "verified_bots": row[2] if row else None,
                "top_bots": [],
            }

            _t = time.perf_counter()
            top_literals = get_ilike_prefilter_literals()[:50]
            if top_literals:
                extract_pattern = "(?i)(" + "|".join(re.escape(lit) for lit in top_literals) + ")"
                extract_sql = extract_pattern.replace("'", "''")
                top_sql = (
                    f"SELECT bot_name, COUNT(*) AS cnt FROM ("
                    f"  SELECT regexp_extract(ua, '{extract_sql}', 1) AS bot_name "
                    f"  FROM {table_name} WHERE {where_clause} "
                    f"  AND ua IS NOT NULL AND ({bot_filter})"
                    f") WHERE bot_name != '' "
                    f"GROUP BY bot_name ORDER BY cnt DESC LIMIT 10"
                )
                top_rows = runner.execute(top_sql, params).fetchall()
                bots_result["top_bots"] = [{"name": r[0], "count": int(r[1])} for r in top_rows]
            timer.mark("bots_top_query", _t)

            result["bots"] = bots_result

    # ── Performance ──────────────────────────────────────────────────────
    if _want("performance") and has_elapsed:
        _t = time.perf_counter()
        perf_parts = ["COUNT(*) AS total_requests"]
        if has_cache:
            perf_parts.extend(
                [
                    "AVG(elapsed) FILTER (WHERE cache IN ('HIT', 'HIT-STALE', 'HIT-CLUSTER')) AS avg_hit_elapsed",
                    "AVG(elapsed) FILTER (WHERE cache = 'MISS') AS avg_miss_elapsed",
                    "PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY elapsed) AS p99_elapsed",
                ]
            )
        else:
            perf_parts.append("AVG(elapsed) AS avg_elapsed")
            perf_parts.append("PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY elapsed) AS p99_elapsed")

        sql = f"SELECT {', '.join(perf_parts)} FROM {table_name} WHERE {where_clause}"
        row = runner.execute(sql, params).fetchone()
        timer.mark("performance_agg_query", _t)

        perf: dict[str, Any] = {"total_requests": 0}
        if row:
            cols = [d[0] for d in runner.con.description]
            d = dict(zip(cols, row))
            avg_hit = d.get("avg_hit_elapsed")
            avg_miss = d.get("avg_miss_elapsed")
            accel = round(avg_miss / avg_hit, 1) if avg_hit and avg_miss and avg_hit > 0 else None
            perf = {
                "cache_accel_factor": accel,
                "avg_hit_latency_ms": round(avg_hit / 1000.0, 2) if avg_hit else None,
                "avg_miss_latency_ms": round(avg_miss / 1000.0, 2) if avg_miss else None,
                "p99_latency_ms": round(d.get("p99_elapsed", 0) / 1000.0, 2) if d.get("p99_elapsed") else None,
                "total_requests": d.get("total_requests", 0),
            }

        # Latency time series (hit vs miss)
        if has_cache:
            _t = time.perf_counter()
            lat_sql = (
                f"SELECT {bucket_select}, "
                f"AVG(elapsed) FILTER (WHERE cache IN ('HIT', 'HIT-STALE', 'HIT-CLUSTER')) / 1000.0 AS hit_ms, "
                f"AVG(elapsed) FILTER (WHERE cache = 'MISS') / 1000.0 AS miss_ms "
                f"FROM {table_name} WHERE {where_clause} "
                f"GROUP BY bucket ORDER BY bucket"
            )
            lat_rows = runner.execute(lat_sql, params).fetchall()
            timer.mark("performance_latency_ts", _t)
            lat_ts: list[dict] = []
            for r in lat_rows:
                t_str = safe_iso(r[0])
                if r[1] is not None:
                    lat_ts.append({"time": t_str, "value": round(float(r[1]), 2), "category": "hit"})
                if r[2] is not None:
                    lat_ts.append({"time": t_str, "value": round(float(r[2]), 2), "category": "miss"})
            perf["latency_time_series"] = lat_ts

        result["performance"] = perf

    # ── Network ──────────────────────────────────────────────────────────
    if _want("network"):
        _t = time.perf_counter()

        # Try rollup for aggregate stats (unfiltered only)
        net: dict[str, Any] | None = None
        if not filters:
            net = _try_network_from_rollup(
                runner,
                start_time=start_time,
                end_time=end_time,
                actual_cols=actual_cols,
            )
            if net is not None:
                timer.mark("network_rollup", _t)

        # Raw scan fallback for aggregates
        if net is None:
            net_parts = ["COUNT(*) AS total_requests"]
            if has_protocol:
                net_parts.append(
                    "ROUND(COUNT(*) FILTER (WHERE protocol = 'HTTP/3') * 100.0 / NULLIF(COUNT(*), 0), 1) AS http3_pct"
                )
                net_parts.append(
                    "ROUND(COUNT(*) FILTER (WHERE protocol = 'HTTP/2') * 100.0 / NULLIF(COUNT(*), 0), 1) AS h2_pct"
                )
            if "is_ssl" in actual_cols:
                net_parts.append(
                    "ROUND(COUNT(*) FILTER (WHERE is_ssl = true) * 100.0 / NULLIF(COUNT(*), 0), 1) AS tls_pct"
                )
            elif "tls" in actual_cols:
                net_parts.append(
                    "ROUND(COUNT(*) FILTER (WHERE tls IS NOT NULL AND tls != '') * 100.0 / NULLIF(COUNT(*), 0), 1) AS tls_pct"
                )
            if has_ipv6:
                net_parts.append(
                    "ROUND(COUNT(*) FILTER (WHERE ipv6 = true) * 100.0 / NULLIF(COUNT(*), 0), 1) AS ipv6_pct"
                )

            sql = f"SELECT {', '.join(net_parts)} FROM {table_name} WHERE {where_clause}"
            row = runner.execute(sql, params).fetchone()
            timer.mark("network_agg_query", _t)

            net = {"total_requests": 0}
            if row:
                cols = [d[0] for d in runner.con.description]
                d = dict(zip(cols, row))
                net = {
                    "http3_pct": d.get("http3_pct"),
                    "tls_pct": d.get("tls_pct"),
                    "ipv6_pct": d.get("ipv6_pct"),
                    "h2_pct": d.get("h2_pct"),
                    "total_requests": d.get("total_requests", 0),
                }

        # Protocol adoption time series (always raw — per-protocol per-bucket)
        if has_protocol:
            _t = time.perf_counter()
            proto_sql = (
                f"SELECT {bucket_select}, "
                f"CASE "
                f"  WHEN protocol = 'HTTP/3' THEN 'HTTP/3' "
                f"  WHEN protocol = 'HTTP/2' THEN 'HTTP/2' "
                f"  WHEN protocol = 'HTTP/1.1' THEN 'HTTP/1.1' "
                f"  ELSE 'Other' "
                f"END AS proto, "
                f"COUNT(*) AS cnt "
                f"FROM {table_name} WHERE {where_clause} "
                f"GROUP BY bucket, proto ORDER BY bucket, proto"
            )
            proto_rows = runner.execute(proto_sql, params).fetchall()
            timer.mark("network_protocol_ts", _t)
            net["protocol_time_series"] = [
                {"time": safe_iso(r[0]), "value": int(r[2]), "category": r[1]} for r in proto_rows
            ]

        result["network"] = net

    # ── Image Optimizer ─────────────────────────────────────────────────
    if _want("io"):
        io_result: dict[str, Any] = {}
        if service_id:
            payload = _fetch_io_stats(service_id, start_time, end_time, timer)
            if payload:
                io_result = _build_io_metrics(payload, timer)
        has_io_fields = "io_input_bytes" in actual_cols and "io_output_bytes" in actual_cols
        if has_io_fields:
            io_result.update(
                _build_io_per_request_metrics(
                    runner, table_name, where_clause, params, actual_cols, bucket_select, timer
                )
            )
        estimate, opps = _fetch_image_traffic_and_opportunities(
            runner, table_name, where_clause, params, actual_cols, timer
        )
        if opps:
            io_result["optimization_opportunities"] = opps
        if estimate:
            io_result.update(estimate)
        if io_result:
            result["io"] = io_result

    return result
