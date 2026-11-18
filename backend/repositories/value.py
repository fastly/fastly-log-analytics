"""Fastly Value pure-Python shaping and Fastly Historical Stats IO helpers."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.repositories.utils.response_shaping import SectionTimer
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.date_utils import iso_z, safe_iso

logger = logging.getLogger(__name__)

_IO_STATS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=60.0)
CacheRegistry.register("value._IO_STATS_CACHE", _IO_STATS_CACHE)
_FASTLY_REPORTED_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=300.0)
CacheRegistry.register("value._FASTLY_REPORTED_CACHE", _FASTLY_REPORTED_CACHE)
_FASTLY_REPORTED_MAX_SPAN_S = 31 * 86400
# Logs are written on every node that handles a request, so the all-node Stats
# fields match them; edge_only logging matches the edge_* fields instead.
_FASTLY_REPORTED_FIELDS = {
    False: ("requests", ("resp_body_bytes", "resp_header_bytes"), "hits"),
    True: ("edge_requests", ("edge_resp_body_bytes", "edge_resp_header_bytes"), "edge_hit_requests"),
}

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
# https://www.fastly.com/pricing#image-optimizer — USD per 10,000 Image Requests, billed monthly.
_IO_FREE_MONTHLY_REQUESTS = 100_000
_IO_PRICE_TIERS: tuple[tuple[float, float], ...] = (
    (10_000_000, 0.50),
    (50_000_000, 0.15),
    (150_000_000, 0.07),
    (500_000_000, 0.03),
    (1_000_000_000, 0.02),
    (5_000_000_000, 0.015),
    (float("inf"), 0.01),
)


def _io_monthly_cost(requests: int) -> float:
    """Marginal-tier list price for one month of Image Requests."""
    cost = 0.0
    lower = float(_IO_FREE_MONTHLY_REQUESTS)
    for upper, per_10k in _IO_PRICE_TIERS:
        if requests <= lower:
            break
        cost += (min(requests, upper) - lower) / 10_000 * per_10k
        lower = upper
    return cost


def _fetch_io_stats(
    service_id: str,
    start_time: str | None,
    end_time: str | None,
    timer: SectionTimer,
) -> dict[str, Any] | None:
    """Fetch Image Optimizer metrics from the Fastly Historical Stats API."""
    from backend.config import get_fastly_api_key, get_fastly_logging_service_id
    from backend.core.fastly.client import fastly
    from backend.utils.date_utils import parse_iso_utc

    cdn_svc = get_fastly_logging_service_id(service_id)
    api_key = get_fastly_api_key(service_id)
    if not api_key or not cdn_svc:
        return None

    now = datetime.now(UTC)
    start_dt = (parse_iso_utc(start_time) if start_time else None) or (now - timedelta(days=30))
    end_dt = (parse_iso_utc(end_time) if end_time else None) or now

    start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = end_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    path = f"/stats/service/{cdn_svc}?by=day&from={int(start_dt.timestamp())}&to={int(end_dt.timestamp())}"

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


def fastly_reported_from_stats(
    records: list[dict[str, Any]],
    from_s: int,
    to_s: int,
    *,
    edge_only: bool,
    sample_rate: int,
) -> dict[str, Any] | None:
    """Sum hourly Stats buckets inside ``[from_s, to_s)``; the window ends at the last hour Fastly reported."""
    req_field, byte_fields, hit_field = _FASTLY_REPORTED_FIELDS[edge_only]
    requests = bandwidth = hits = 0
    last_start: int | None = None
    for rec in records:
        start = rec.get("start_time")
        if not isinstance(start, (int, float)) or not from_s <= int(start) < to_s:
            continue
        last_start = max(last_start or int(start), int(start))
        requests += int(rec.get(req_field) or 0)
        bandwidth += sum(int(rec.get(f) or 0) for f in byte_fields)
        hits += int(rec.get(hit_field) or 0)
    if last_start is None:
        return None
    return {
        "window_start": iso_z(datetime.fromtimestamp(from_s, tz=UTC)),
        "window_end": iso_z(datetime.fromtimestamp(last_start + 3600, tz=UTC)),
        "requests": requests,
        "bandwidth_bytes": bandwidth,
        "hit_requests": hits,
        "origin_offload_pct": round(hits * 100.0 / requests, 2) if requests else None,
        "sample_rate": sample_rate,
        "edge_only": edge_only,
    }


def fetch_fastly_reported(
    service_id: str,
    start_time: str | None,
    end_time: str | None,
    *,
    now: datetime | None = None,
    coverage_end: datetime | None = None,
) -> dict[str, Any] | None:
    """Fastly Historical Stats totals for the whole closed UTC hours of the range, or None when unavailable."""
    from backend import config
    from backend.core.fastly import client
    from backend.utils.date_utils import parse_iso_utc

    start_dt, end_dt = parse_iso_utc(start_time), parse_iso_utc(end_time)
    if start_dt is None or end_dt is None:
        return None
    now = now or datetime.now(UTC)
    from_s = -(-int(start_dt.timestamp()) // 3600) * 3600
    to_s = min(int(end_dt.timestamp()), int(now.timestamp()))
    if coverage_end is not None:
        to_s = min(to_s, int(coverage_end.timestamp()))
    to_s = to_s // 3600 * 3600
    if to_s <= from_s or to_s - from_s > _FASTLY_REPORTED_MAX_SPAN_S:
        return None

    cdn_svc = config.get_fastly_logging_service_id(service_id)
    api_key = config.get_fastly_api_key(service_id)
    if not api_key or not cdn_svc:
        return None
    cfg = config.load_config(service_id) or {}
    prov = cfg.get("provisioning") or {}
    sample_raw = prov.get("sample_rate") if "sample_rate" in prov else cfg.get("sample_rate")
    edge_only = bool(prov.get("edge_only") if "edge_only" in prov else cfg.get("edge_only", False))
    sample_rate = min(100, max(1, int(100 if sample_raw is None else sample_raw)))

    path = f"/stats/service/{cdn_svc}?by=hour&from={from_s}&to={to_s}"
    payload = _FASTLY_REPORTED_CACHE.get(path)
    if payload is None:
        try:
            payload = client.fastly("GET", path, token=api_key, timeout=8, max_retries=1)
        except Exception:
            logger.debug("Fastly-reported stats fetch failed for %s", cdn_svc, exc_info=True)
            return None
        _FASTLY_REPORTED_CACHE[path] = payload
    records = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        return None
    return fastly_reported_from_stats(records, from_s, to_s, edge_only=edge_only, sample_rate=sample_rate)


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
    monthly_transforms: dict[str, int] = {}

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
            monthly_transforms[day_str[:7]] = monthly_transforms.get(day_str[:7], 0) + transforms
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
        "io_estimated_cost_usd": round(sum(_io_monthly_cost(n) for n in monthly_transforms.values()), 2),
        "format_distribution": format_distribution,
        "modern_format_pct": modern_format_pct,
        "io_bandwidth_time_series": daily_bandwidth,
    }


def _shape_caching_and_overview_buckets(
    buckets: list[dict[str, Any]],
    *,
    include_overview: bool,
    has_resp_bytes: bool,
    has_shield: bool,
    has_waf: bool,
    has_elapsed: bool,
    include_performance: bool = False,
) -> dict[str, Any]:
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

    offload_pct = round(total_hits * 100.0 / total_requests, 1) if total_requests > 0 else None
    state_ts: list[dict] = []
    for b in buckets:
        t_str = safe_iso(b["bucket"])
        other = b["total"] - b["hit_cnt"] - b["miss_cnt"] - b["pass_cnt"]
        state_ts.extend(
            [
                {"time": t_str, "value": int(b["hit_cnt"]), "category": "HIT"},
                {"time": t_str, "value": int(b["miss_cnt"]), "category": "MISS"},
                {"time": t_str, "value": int(b["pass_cnt"]), "category": "PASS"},
            ]
        )
        if other > 0:
            state_ts.append({"time": t_str, "value": int(other), "category": "OTHER"})

    caching: dict[str, Any] = {
        "origin_offload_pct": offload_pct,
        "bandwidth_saved_bytes": _sum("bandwidth_saved") if has_resp_bytes else None,
        "shield_effectiveness_pct": shield_eff,
        "total_requests": total_requests,
        "hit_requests": total_hits,
        "miss_requests": total_misses,
        "pass_requests": total_passes,
        "synth_requests": total_synths,
        "offload_time_series": [
            {
                "time": safe_iso(b["bucket"]),
                "value": round(b["hit_cnt"] * 100.0 / b["total"], 1) if b["total"] > 0 else 0,
            }
            for b in buckets
        ],
        "cache_state_time_series": state_ts,
    }
    out: dict[str, Any] = {"caching": caching}

    avg_hit = avg_miss = accel = None
    if (include_overview or include_performance) and has_elapsed:
        s_h, c_h = _sum("sum_hit_elapsed"), _sum("cnt_hit_elapsed")
        s_m, c_m = _sum("sum_miss_elapsed"), _sum("cnt_miss_elapsed")
        avg_hit = s_h / c_h if c_h > 0 else None
        avg_miss = s_m / c_m if c_m > 0 else None
        accel = round(avg_miss / avg_hit, 1) if avg_hit and avg_miss and avg_hit > 0 else None

    if include_overview:
        out["overview"] = {
            "origin_offload_pct": offload_pct,
            "threats_blocked": _sum("threats_cnt") if has_waf else None,
            "cache_acceleration_factor": accel,
            "total_requests": total_requests,
            "total_bandwidth_bytes": _sum("total_bytes") if has_resp_bytes else None,
            "edge_hit_requests": total_hits,
            "origin_requests": _sum("origin_cnt"),
        }

    if include_performance and has_elapsed:
        lat_ts: list[dict] = []
        for b in buckets:
            t_str = safe_iso(b["bucket"])
            c_h_b = int(b.get("cnt_hit_elapsed") or 0)
            c_m_b = int(b.get("cnt_miss_elapsed") or 0)
            if c_h_b > 0 and b.get("sum_hit_elapsed") is not None:
                lat_ts.append(
                    {
                        "time": t_str,
                        "value": round(float(b["sum_hit_elapsed"]) / c_h_b / 1000.0, 2),
                        "category": "hit",
                    }
                )
            if c_m_b > 0 and b.get("sum_miss_elapsed") is not None:
                lat_ts.append(
                    {
                        "time": t_str,
                        "value": round(float(b["sum_miss_elapsed"]) / c_m_b / 1000.0, 2),
                        "category": "miss",
                    }
                )
        out["performance_from_buckets"] = {
            "cache_accel_factor": accel,
            "avg_hit_latency_ms": round(avg_hit / 1000.0, 2) if avg_hit else None,
            "avg_miss_latency_ms": round(avg_miss / 1000.0, 2) if avg_miss else None,
            "total_requests": total_requests,
            "latency_time_series": lat_ts,
        }
    return out


def _build_proto_ts(buckets: list[dict[str, Any]], suffix: str = "_cnt") -> list[dict[str, Any]]:
    proto_ts: list[dict[str, Any]] = []
    for b in buckets:
        t_str = safe_iso(b["bucket"])
        for col_key, proto_label in (
            (f"h11{suffix}", "HTTP/1.1"),
            (f"h2{suffix}", "HTTP/2"),
            (f"http3{suffix}", "HTTP/3"),
            (f"other_proto{suffix}", "Other"),
        ):
            cnt = int(b.get(col_key) or 0)
            if cnt > 0:
                proto_ts.append({"time": t_str, "value": cnt, "category": proto_label})
    return proto_ts


def _shape_raw_network_buckets(
    buckets: list[dict[str, Any]],
    total_requests: int,
    *,
    has_protocol: bool,
    has_is_ssl: bool,
    has_ipv6: bool,
) -> dict[str, Any]:
    def _pct(key: str) -> float | None:
        if total_requests <= 0:
            return None
        return round(sum(int(b.get(key, 0) or 0) for b in buckets) * 100.0 / total_requests, 1)

    net: dict[str, Any] = {
        "http3_pct": _pct("http3_cnt") if has_protocol else None,
        "h2_pct": _pct("h2_cnt") if has_protocol else None,
        "tls_pct": _pct("tls_cnt") if has_is_ssl else None,
        "ipv6_pct": _pct("ipv6_cnt") if has_ipv6 else None,
        "total_requests": total_requests,
    }
    if has_protocol:
        net["protocol_time_series"] = _build_proto_ts(buckets, "_cnt")
    return net


__all__ = [
    "_IO_FORMAT_FIELDS",
    "_IO_FREE_MONTHLY_REQUESTS",
    "_IO_PRICE_TIERS",
    "_IO_RESP_BYTES_FIELD",
    "_IO_SHIELD_BYTES_FIELD",
    "_IO_STATS_CACHE",
    "_IO_TRANSFORM_FIELD",
    "_MODERN_FORMATS",
    "_build_io_metrics",
    "_build_proto_ts",
    "_fetch_io_stats",
    "_io_monthly_cost",
    "_shape_caching_and_overview_buckets",
    "_shape_raw_network_buckets",
]
