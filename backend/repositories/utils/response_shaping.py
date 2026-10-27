"""Cross-engine response shaping and post-processing helpers.

Shared between DuckDB repositories (``backend/repositories/*``) and ClickHouse
high-scale services (``backend/high_scale/*``) so both analytical engines share
a single source of truth for:
1. ASN label enrichment
2. Time-bucket alignment & zero-filling
3. Latency & percentile normalization
4. Empty-response scaffolding
5. Top-N row formatting & domain breakdown shaping
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.utils.date_utils import ensure_utc, parse_iso_utc

# ── 1. ASN Label Enrichment ──────────────────────────────────────────────────


def parse_asn_int(val: Any) -> int | None:
    """Parse an integer ASN from arbitrary input, returning ``None`` on failure or bool."""
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def resolve_asn_names_map(service_name: str, asns: Iterable[Any]) -> dict[int, str]:
    """Look up ASN organization names from the metadata store for ``asns``."""
    from backend.core import duckdb as _db

    asn_ints = sorted({asn_int for a in asns if (asn_int := parse_asn_int(a)) is not None})
    if not asn_ints:
        return {}
    return _db.get_asn_names(service_name, asn_ints)


def format_asn_display_label(
    asn_val: Any,
    asn_names: Mapping[int, str],
    *,
    non_int_fallback: str | None = None,
) -> str:
    """Format a human-readable ASN display label ``Name (ASN)`` or ``AS{asn}``."""
    from backend.core import duckdb as _db

    asn_int = parse_asn_int(asn_val)
    if asn_int is not None:
        return _db.format_asn_label(asn_int, asn_names.get(asn_int, ""))
    if non_int_fallback is not None:
        return non_int_fallback
    return str(asn_val) if asn_val is not None else ""


def build_asn_label_map(service_name: str, asns: Iterable[Any]) -> dict[int, str]:
    """Return ``{asn_int: formatted_display_label}`` for all valid integer ASNs in ``asns``."""
    from backend.core import duckdb as _db

    asn_ints = [asn_int for a in asns if (asn_int := parse_asn_int(a)) is not None]
    if not asn_ints:
        return {}
    unique_asns = list(dict.fromkeys(asn_ints))
    names = _db.get_asn_names(service_name, unique_asns)
    return {asn: _db.format_asn_label(asn, names.get(asn, "")) for asn in unique_asns}


def enrich_asn_rows(
    rows: list[dict[str, Any]],
    service_name: str,
    *,
    asn_key: str = "asn",
    label_key: str = "label",
    name_lookup: Callable[[str, list[int]], Mapping[int, str]] | None = None,
) -> list[dict[str, Any]]:
    """Enrich a list of row dicts in-place with formatted ASN labels under ``label_key``."""
    if not rows:
        return rows
    if name_lookup is not None:
        asn_ints = [asn_int for r in rows if (asn_int := parse_asn_int(r.get(asn_key))) is not None]
        asn_names: Mapping[int, str] = name_lookup(service_name, asn_ints) if asn_ints else {}
    else:
        asn_names = resolve_asn_names_map(service_name, (r.get(asn_key) for r in rows))
    for row in rows:
        raw_asn = row.get(asn_key)
        asn_int = parse_asn_int(raw_asn)
        if asn_int is not None:
            row[label_key] = format_asn_display_label(asn_int, asn_names)
        elif label_key not in row or not row[label_key]:
            row[label_key] = str(raw_asn) if raw_asn is not None else ""
    return rows


def shape_performance_asn_rows(
    service_name: str,
    rows: Iterable[Any],
) -> list[dict[str, Any]]:
    """Shape and ASN-enrich performance ``top_asns`` rows from either tuple or dict rows."""
    row_list = list(rows)
    if not row_list:
        return []
    raw_asns = [r["value"] if isinstance(r, Mapping) else r[0] for r in row_list]
    asn_names = resolve_asn_names_map(service_name, [int(v) for v in raw_asns if str(v).isdigit()])
    out: list[dict[str, Any]] = []
    for r in row_list:
        if isinstance(r, Mapping):
            raw_val = r["value"]
            asn_val: int | str = int(raw_val) if str(raw_val).isdigit() else raw_val
            requests = int(r["req_count"]) if "req_count" in r else r["requests"]
            avg_v = float_or_zero(r["avg_ms"]) if "avg_ms" in r else r["avg"]
            p50_v = float_or_zero(r["p50_ms"]) if "p50_ms" in r else r["p50"]
            p95_v = float_or_zero(r["p95_ms"]) if "p95_ms" in r else r["p95"]
            p99_v = float_or_zero(r["p99_ms"]) if "p99_ms" in r else r["p99"]
        else:
            raw_val = r[0]
            asn_val = int(raw_val) if str(raw_val).isdigit() else raw_val
            requests, avg_v, p50_v, p95_v, p99_v = r[1], r[2], r[3], r[4], r[5]
        label = format_asn_display_label(asn_val, asn_names) if isinstance(asn_val, int) else str(asn_val)
        out.append(
            {
                "asn": asn_val,
                "label": label,
                "requests": requests,
                "avg": avg_v,
                "p50": p50_v,
                "p95": p95_v,
                "p99": p99_v,
            }
        )
    return out


def finish_security_proxies(
    source_name: str,
    stats: Mapping[str, Any],
    quality_rows: Iterable[Any],
    raw_isps: Iterable[Mapping[str, Any]],
    raw_clients: Iterable[Mapping[str, Any]],
    mask_ips: bool,
) -> dict[str, Any]:
    """Shape the security proxies payload (traffic quality, suspicious ISPs, active clients) across engines."""
    q_list = list(quality_rows)
    isp_list = list(raw_isps)
    client_list = list(raw_clients)

    traffic_quality: list[dict[str, Any]] = []
    if q_list:
        pairs: list[tuple[Any, int]] = []
        for item in q_list:
            if isinstance(item, Mapping):
                pairs.append((item.get("type"), int(item.get("count") or 0)))
            else:
                pairs.append((item[0], int(item[1] or 0)))
        total_count = sum(cnt for _, cnt in pairs)
        if total_count > 0:
            for row_type, row_count in pairs:
                label = str(row_type if row_type is not None else "Unknown")
                value = round((float(row_count) / total_count) * 100, 1)
                traffic_quality.append({"label": label, "value": value})

    asns_to_resolve = {
        asn_int for row in (*isp_list, *client_list) if (asn_int := parse_asn_int(row.get("asn"))) is not None
    }
    from backend.core.duckdb import get_asn_names

    asn_names = get_asn_names(source_name, list(asns_to_resolve))

    def _asn_display(asn_int: int | None) -> str:
        return (asn_names.get(asn_int) if asn_int is not None else None) or (
            f"AS{asn_int}" if asn_int else "Unknown ISP"
        )

    suspicious_isps = [
        {
            "isp": _asn_display(asn_int := parse_asn_int(row.get("asn"))),
            "asn": asn_int,
            "count": int(row.get("count", 0)),
        }
        for row in isp_list
    ]

    active_clients: list[dict[str, Any]] = []
    if mask_ips:
        from backend.core.share_db.validation import mask_ip
    for row in client_list:
        asn_int = parse_asn_int(row.get("asn"))
        client_item = {**row}
        if mask_ips and client_item.get("ip"):
            client_item["ip"] = mask_ip(str(client_item["ip"]))
        client_item["asn_name"] = _asn_display(asn_int)
        client_item.pop("asn", None)
        active_clients.append(client_item)

    return {
        "active_proxies_count": int(stats.get("active_proxies_count", 0)),
        "tunnel_requests_count": int(stats.get("total_requests_count", 0)),
        "distance_mismatches_count": int(stats.get("distance_mismatches_count", 0)),
        "traffic_quality": traffic_quality,
        "suspicious_isps": suspicious_isps,
        "active_clients": active_clients,
    }


def build_insight_asn_context(
    service_id: str,
    rows: Iterable[Any],
    insight_id: str,
    *,
    include_dma: bool | None = None,
) -> dict[str, Any]:
    """Build ``{"asn_names": ..., "dma_map": ...}`` context for ASN/metro insights across DuckDB and ClickHouse."""
    if not any(marker in insight_id for marker in ("asn", "metro")):
        return {}
    row_list = list(rows)
    if not row_list:
        return {}
    from backend.core import duckdb as _db

    asn_vals = [r["asn"] if isinstance(r, Mapping) else r[0] for r in row_list if r]
    context: dict[str, Any] = {
        "asn_names": _db.get_asn_names(service_id, asn_vals),
    }
    want_dma = ("metro" in insight_id) if include_dma is None else include_dma
    if want_dma:
        context["dma_map"] = _db._get_dma_map()
    return context


# ── 2. Time-Bucket Alignment & Zero-Filling ──────────────────────────────────

CHART_INTERVAL_SECONDS: dict[str, int] = {
    "1 second": 1,
    "5 seconds": 5,
    "10 seconds": 10,
    "15 seconds": 15,
    "30 seconds": 30,
    "1 minute": 60,
    "5 minutes": 300,
    "10 minutes": 600,
    "15 minutes": 900,
    "30 minutes": 1800,
    "1 hour": 3600,
    "1 day": 86400,
}

_CHART_INTERVAL_ALIASES: dict[str, str] = {
    "1 second": "1 second",
    "1 seconds": "1 second",
    "1s": "1 second",
    "second": "1 second",
    "seconds": "1 second",
    "5 seconds": "5 seconds",
    "5s": "5 seconds",
    "10 seconds": "10 seconds",
    "10s": "10 seconds",
    "15 seconds": "15 seconds",
    "15s": "15 seconds",
    "30 seconds": "30 seconds",
    "30s": "30 seconds",
    "1 minute": "1 minute",
    "1 minutes": "1 minute",
    "1m": "1 minute",
    "minute": "1 minute",
    "minutes": "1 minute",
    "5 minute": "5 minutes",
    "5 minutes": "5 minutes",
    "5m": "5 minutes",
    "10 minutes": "10 minutes",
    "10m": "10 minutes",
    "15 minutes": "15 minutes",
    "15m": "15 minutes",
    "30 minutes": "30 minutes",
    "30m": "30 minutes",
    "1 hour": "1 hour",
    "1 hours": "1 hour",
    "1h": "1 hour",
    "hour": "1 hour",
    "hours": "1 hour",
    "1 day": "1 day",
    "1 days": "1 day",
    "1d": "1 day",
    "day": "1 day",
    "days": "1 day",
}

CMCD_TS_DEFAULTS: dict[str, dict[str, Any]] = {
    "sessions_ts": {"concurrent_sessions": 0, "rebuffer_session_pct": None, "new_sessions": 0},
    "buffer_health_ts": {"p50_buffer": None, "p95_buffer": None, "starvation_rate": None},
    "bitrate_ts": {"avg_bitrate": None, "utilization_ratio": None},
    "throughput_ts": {"p50": None, "p95": None, "p99": None},
    "startup_ts": {"startup_ratio": None},
}


def normalize_chart_interval(chart_interval: str | None, default: str = "1 minute") -> str:
    """Validate and normalize a dashboard chart interval string."""
    if not chart_interval:
        return default
    ci = chart_interval.strip().lower()
    if ci in _CHART_INTERVAL_ALIASES:
        return _CHART_INTERVAL_ALIASES[ci]
    return ci if ci in CHART_INTERVAL_SECONDS else default


def chart_interval_seconds(chart_interval: str | None, default: int = 60) -> int:
    """Return the duration in seconds for a chart interval string."""
    return CHART_INTERVAL_SECONDS.get(normalize_chart_interval(chart_interval), default)


def _coerce_utc_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_utc(value)
    return parse_iso_utc(value)


def clamp_bucket_seconds(
    bucket_seconds: int,
    start_time: str | datetime | None,
    end_time: str | datetime | None,
    *,
    max_buckets: int = 8640,
) -> int:
    """Floor ``bucket_seconds`` so the time span contains at most ``max_buckets`` buckets."""
    st = _coerce_utc_datetime(start_time)
    et = _coerce_utc_datetime(end_time)
    if st is None or et is None:
        return bucket_seconds
    span_secs = max(1, int((et - st).total_seconds()))
    return max(bucket_seconds, span_secs // max_buckets) or bucket_seconds


def generate_epoch_aligned_buckets(
    start_time: str | datetime | None,
    end_time: str | datetime | None,
    bucket_seconds: int,
    *,
    fmt: str = "%Y-%m-%dT%H:%M:%S",
) -> list[str]:
    """Generate epoch-aligned bucket timestamp strings covering ``[start_time, end_time]``."""
    st = _coerce_utc_datetime(start_time)
    et = _coerce_utc_datetime(end_time)
    if st is None or et is None or bucket_seconds <= 0:
        return []
    first = (int(st.timestamp()) // bucket_seconds) * bucket_seconds
    last = (int(et.timestamp()) // bucket_seconds) * bucket_seconds
    if last < first:
        return []
    return [datetime.fromtimestamp(ts, tz=UTC).strftime(fmt) for ts in range(first, last + 1, bucket_seconds)]


def pad_timeseries_buckets(
    start_time: str | datetime | None,
    end_time: str | datetime | None,
    bucket_seconds: int,
    rows_by_bucket: Mapping[str, Mapping[str, Any]],
    default_vals: Mapping[str, Any],
    *,
    bucket_fmt: str = "%Y-%m-%d %H:%M:%S",
) -> list[dict[str, Any]]:
    """Zero-fill missing time-series buckets between ``start_time`` and ``end_time``.

    Accepts ``start_time`` / ``end_time`` as ISO strings, ``datetime`` objects, or ``None``.
    Falls back to sorted existing buckets when bounds cannot be parsed.
    """
    st = _coerce_utc_datetime(start_time)
    et = _coerce_utc_datetime(end_time)
    if st is None or et is None or bucket_seconds <= 0:
        return sorted([{"bucket": k, **v} for k, v in rows_by_bucket.items()], key=lambda x: x["bucket"])

    st_epoch = int(st.timestamp())
    st_aligned = st_epoch - (st_epoch % bucket_seconds)
    st_dt = datetime.fromtimestamp(st_aligned, tz=UTC)

    et_epoch = int(et.timestamp())
    et_aligned = et_epoch - (et_epoch % bucket_seconds)
    et_dt = datetime.fromtimestamp(et_aligned, tz=UTC)

    padded: list[dict[str, Any]] = []
    curr = st_dt
    step = timedelta(seconds=bucket_seconds)
    while curr <= et_dt:
        bucket_str = curr.strftime(bucket_fmt)
        if bucket_str in rows_by_bucket:
            padded.append({"bucket": bucket_str, **rows_by_bucket[bucket_str]})
        else:
            padded.append({"bucket": bucket_str, **default_vals})
        curr += step
    return padded


INSIGHT_BUCKET_INTERVALS: tuple[tuple[float, str], ...] = (
    (2, "1 minute"),
    (12, "5 minutes"),
    (48, "15 minutes"),
    (168, "1 hour"),
    (720, "4 hours"),
)


def select_insight_bucket_interval(
    total_hours: float,
    *,
    clickhouse: bool = False,
    clickhouse_syntax: bool = False,
) -> str:
    """Select the adaptive time-bucket interval for insights timeline queries."""
    use_ch = clickhouse or clickhouse_syntax
    if use_ch:
        if total_hours <= 2:
            return "INTERVAL 1 MINUTE"
        if total_hours <= 12:
            return "INTERVAL 5 MINUTE"
        if total_hours <= 48:
            return "INTERVAL 15 MINUTE"
        if total_hours <= 168:
            return "INTERVAL 1 HOUR"
        if total_hours <= 720:
            return "INTERVAL 4 HOUR"
        return "INTERVAL 1 DAY"
    return next((iv for max_h, iv in INSIGHT_BUCKET_INTERVALS if total_hours <= max_h), "1 day")


# ── 3. Latency & Percentile Normalization ────────────────────────────────────

WATERFALL_KEYS: tuple[str, ...] = (
    "edge_processing",
    "origin_wait",
    "origin_download",
    "client_download",
)


def opt_float(
    val: Any,
    ndigits: int | None = None,
    *,
    reject_non_finite: bool = False,
) -> float | None:
    """Convert ``val`` to ``float`` (optionally rounded), returning ``None`` on ``None`` or non-finite."""
    if val is None:
        return None
    try:
        f = float(val)
    except (ValueError, TypeError):
        return None
    if reject_non_finite and not math.isfinite(f):
        return None
    return round(f, ndigits) if ndigits is not None else f


def float_or_zero(val: Any, ndigits: int | None = None) -> float:
    """Convert ``val`` to ``float`` (optionally rounded), returning ``0.0`` when ``None`` or non-finite."""
    res = opt_float(val, ndigits=ndigits, reject_non_finite=True)
    return res if res is not None else 0.0


def us_to_ms(val: Any, ndigits: int | None = None) -> float | None:
    """Convert microseconds to milliseconds, returning ``None`` on ``None`` or non-finite values."""
    f = opt_float(val, reject_non_finite=True)
    if f is None:
        return None
    ms = f / 1000.0
    return round(ms, ndigits) if ndigits is not None else ms


def shape_latency_percentiles(
    avg: Any,
    p50: Any = None,
    p95: Any = None,
    p99: Any = None,
    *,
    p75: Any = None,
    include_p75: bool = False,
    ndigits: int | None = None,
    default_zero: bool = False,
) -> dict[str, Any]:
    """Normalize a standard latency percentile dictionary (``avg``, ``p50``, ``p95``, ``p99``) or row mapping."""
    if isinstance(avg, Mapping):
        row = avg
        out_row: dict[str, Any] = {}
        if "req_count" in row:
            out_row["requests"] = int(row["req_count"] or 0)
        elif "requests" in row:
            out_row["requests"] = int(row["requests"] or 0)
        out_row["avg"] = float_or_zero(row.get("avg_ms", row.get("avg")), ndigits)
        out_row["p50"] = float_or_zero(row.get("p50_ms", row.get("p50")), ndigits)
        out_row["p95"] = float_or_zero(row.get("p95_ms", row.get("p95")), ndigits)
        out_row["p99"] = float_or_zero(row.get("p99_ms", row.get("p99")), ndigits)
        if include_p75 or "p75_ms" in row or "p75" in row:
            out_row["p75"] = float_or_zero(row.get("p75_ms", row.get("p75")), ndigits)
        return out_row
    conv = (lambda v: float_or_zero(v, ndigits)) if default_zero else (lambda v: opt_float(v, ndigits))
    out: dict[str, Any] = {
        "avg": conv(avg),
        "p50": conv(p50),
        "p95": conv(p95),
        "p99": conv(p99),
    }
    if include_p75:
        out["p75"] = conv(p75)
    return out


def is_pop_latency_elevated(p95_ms: float | None, median_p95_ms: float | None) -> bool:
    """Return True when a POP's P95 latency exceeds 2× the median P95 across POPs."""
    return p95_ms is not None and median_p95_ms is not None and p95_ms > median_p95_ms * 2


def shape_pop_latency_rows(
    rows: Sequence[Any],
    *,
    include_median_when_empty: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    """Shape POP latency breakdown rows and compute ``median_p95_ms`` + per-POP ``elevated`` flag.

    Supports both tuple rows ``(pop, requests, p50_ms, p95_ms)`` (DuckDB) and pre-extracted
    dicts ``{"pop": ..., "requests": ..., "p50_ms": ..., "p95_ms": ...}`` (ClickHouse).
    """
    if not rows:
        base: dict[str, Any] = {"has_data": False, "requires_group_c": False}
        if include_median_when_empty:
            base["median_p95_ms"] = 0
        base["rows"] = []
        base.update(extra)
        return base

    normalized: list[tuple[Any, Any, Any, Any]] = []
    for r in rows:
        if isinstance(r, Mapping):
            normalized.append((r["pop"], r["requests"], r["p50_ms"], r["p95_ms"]))
        else:
            normalized.append((r[0], r[1], r[2], r[3]))

    valid_p95s = sorted(p95 for _, _, _, p95 in normalized if p95 is not None)
    median_p95 = valid_p95s[len(valid_p95s) // 2] if valid_p95s else 0
    return {
        "has_data": True,
        "requires_group_c": False,
        "median_p95_ms": median_p95,
        "rows": [
            {
                "pop": pop,
                "requests": reqs,
                "p50_ms": p50,
                "p95_ms": p95,
                "elevated": is_pop_latency_elevated(p95, median_p95),
            }
            for pop, reqs, p50, p95 in normalized
        ],
        **extra,
    }


def shape_waterfall_avg(
    edge_processing: Any = 0.0,
    origin_wait: Any = 0.0,
    origin_download: Any = 0.0,
    client_download: Any = 0.0,
) -> dict[str, dict[str, float]]:
    """Shape the performance waterfall average breakdown from a mapping or 4 positional values."""
    if isinstance(edge_processing, Mapping):
        row = edge_processing
        return {"avg": {k: float_or_zero(row.get(k)) for k in WATERFALL_KEYS}}
    return {
        "avg": {
            "edge_processing": float_or_zero(edge_processing),
            "origin_wait": float_or_zero(origin_wait),
            "origin_download": float_or_zero(origin_download),
            "client_download": float_or_zero(client_download),
        }
    }


def compute_network_health_score(
    throughput_bps: float | None,
    rtt_congestion_us: float | None,
    avg_ploss: float | None,
    rtt_jitter_us: float | None,
    error_pct: float | None,
) -> float:
    """Compute the composite 0–100 network health score from packet loss, RTT congestion, jitter, and 5xx rate."""
    _ = throughput_bps
    pkt = min((avg_ploss or 0) / 0.05, 1.0) if avg_ploss is not None else 0
    cong = min((rtt_congestion_us or 0) / 200_000, 1.0) if rtt_congestion_us is not None else 0
    jitter = min((rtt_jitter_us or 0) / 100_000, 1.0) if rtt_jitter_us is not None else 0
    err = min((error_pct or 0) / 10.0, 1.0) if error_pct is not None else 0
    weighted = pkt * 0.40 + cong * 0.30 + jitter * 0.20 + err * 0.10
    return round((1.0 - weighted) * 100, 1)


def avg_bucket_health_score(
    buckets_data: Mapping[str, Mapping[str, Any]],
    keys: Iterable[str],
) -> float | None:
    """Average ``health_score`` over a sequence of bucket keys, or ``None`` if no bucket has a score."""
    scores = [hs for b in keys if b in buckets_data and (hs := buckets_data[b].get("health_score")) is not None]
    return round(sum(scores) / len(scores), 1) if scores else None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometers between two WGS84 coordinates."""
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2.0 * 6371.0 * math.asin(math.sqrt(a))


def enrich_row_with_pop_distance(row: dict[str, Any], *, min_requests: int = 30) -> dict[str, Any]:
    """Enrich an origin shielding route row with POP coordinates, great-circle distance, and light-speed RTT."""
    from backend.utils.pop_utils import get_pop_lat_lon_map

    pops = get_pop_lat_lon_map()
    e_coords = pops.get(str(row.get("edge_pop", "")).upper())
    s_coords = pops.get(str(row.get("shield_pop", "")).upper())
    low_sample = (row.get("requests") or 0) < min_requests
    dist_km = light_rtt_ms = efficiency = None
    anomaly_eligible = False
    if e_coords and s_coords:
        dist = haversine_km(e_coords[0], e_coords[1], s_coords[0], s_coords[1])
        dist_km, light_rtt_ms = round(dist, 1), round(2.0 * dist / 200_000.0 * 1000.0, 2)
        p50 = row.get("p50_ms")
        efficiency = round(p50 / light_rtt_ms, 2) if (light_rtt_ms > 0.5 and p50 is not None) else None
        anomaly_eligible = bool(
            efficiency is not None and efficiency > 3.0 and p50 is not None and p50 - light_rtt_ms >= 20.0
        )
    row.update(
        distance_km=dist_km,
        light_speed_rtt_ms=light_rtt_ms,
        efficiency_ratio=efficiency,
        low_sample=low_sample,
        anomaly_eligible=anomaly_eligible,
        anomaly_static=anomaly_eligible and not low_sample,
        edge_lat=e_coords[0] if e_coords else None,
        edge_lon=e_coords[1] if e_coords else None,
        shield_lat=s_coords[0] if s_coords else None,
        shield_lon=s_coords[1] if s_coords else None,
    )
    return row


# ── 4. Empty-Response Scaffolding ────────────────────────────────────────────

ORIGIN_SUMMARY_KEYS: tuple[str, ...] = (
    "total_misses",
    "total_passes",
    "ottfb_p50_ms",
    "ottfb_p75_ms",
    "ottfb_p95_ms",
    "ottfb_p99_ms",
    "ottlb_p50_ms",
    "ottlb_p95_ms",
    "cdn_overhead_p50_ms",
    "origin_error_rate",
    "obytes_p50",
)

PERF_SECTION_DEFAULTS: dict[str, Any] = {
    "top_urls": [],
    "top_asns": [],
    "ttl_dist": [],
    "scatter": [],
    "waterfall": {},
}

SECURITY_EMPTY_CORE_SECTIONS: tuple[str, ...] = (
    "tls_fingerprints",
    "req_size_dist",
    "ipv6_adoption",
    "proxy_dist",
    "conn_reuse_dist",
)

SECURITY_SECTION_DEFAULTS: tuple[tuple[str, bool], ...] = (
    ("verified_bots_ts", True),
    ("ngwaf_verified_bots", True),
    ("ngwaf_verified_bots_ts", True),
    ("wellknown_bots", True),
    ("bot_efficiency", True),
    ("tls_fingerprints", True),
    ("fingerprint_coverage", False),
    ("req_size_dist", True),
    ("top_ips_header", True),
    ("ipv6_adoption", True),
    ("proxy_dist", True),
    ("conn_reuse_dist", True),
)


def empty_origin_summary(**extra: Any) -> dict[str, Any]:
    """Return an empty origin summary payload with all metric keys set to ``None``."""
    return {"has_data": False, **dict.fromkeys(ORIGIN_SUMMARY_KEYS, None), **extra}


def empty_origin_aggregates(*, include_has_data: bool = True, **extra: Any) -> dict[str, Any]:
    """Return a fresh empty composite origin aggregates payload."""
    base: dict[str, Any] = {}
    if include_has_data:
        base["has_data"] = False
    base.update(
        {
            "summary": {},
            "timeseries": {"has_data": False, "series": []},
            "slow_urls": {"has_data": False, "rows": []},
            "status_codes": {"has_data": False, "rows": []},
            "path_breakdown": {"has_data": False, "shielding_detected": False, "rows": []},
            "pop_latency": {"has_data": False, "requires_group_c": False, "rows": []},
            "ip_health": {"has_data": False, "rows": []},
            **extra,
        }
    )
    return base


def empty_performance_aggregates(
    *,
    sections: set[str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Return a fresh empty performance aggregates payload."""
    out: dict[str, Any] = {
        k: ([] if isinstance(v, list) else {})
        for k, v in PERF_SECTION_DEFAULTS.items()
        if sections is None or k in sections
    }
    out.update(extra)
    return out


def empty_cmcd_aggregates(
    *,
    available: bool = False,
    has_data: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    """Return an empty CMCD aggregates payload."""
    return {"available": available, "has_data": has_data, **extra}


def empty_security_aggregates(**extra: Any) -> dict[str, Any]:
    """Return an empty security aggregates payload when schema columns are unavailable."""
    return {
        **{k: [] for k in SECURITY_EMPTY_CORE_SECTIONS},
        **extra,
    }


def empty_security_proxies(**extra: Any) -> dict[str, Any]:
    """Return an empty security proxies payload."""
    return {
        "active_proxies_count": 0,
        "tunnel_requests_count": 0,
        "distance_mismatches_count": 0,
        "traffic_quality": [],
        "suspicious_isps": [],
        "active_clients": [],
        **extra,
    }


def empty_network_quality(region_country: str = "US", **extra: Any) -> dict[str, Any]:
    """Return an empty network quality payload."""
    return {
        "available": False,
        "by_country": [],
        "by_asn": [],
        "by_region": [],
        "region_country": region_country,
        "by_pop": [],
        "scatter": [],
        "countries": [],
        **extra,
    }


def empty_transport_metrics(
    *,
    has_transport_data: bool = False,
    missing_fields: list[str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Return an empty transport metrics payload."""
    return {
        "has_transport_data": has_transport_data,
        "tcp_rtt_p50_ms": None,
        "tcp_rtt_p90_ms": None,
        "tcp_rtt_p95_ms": None,
        "tcp_rtt_p99_ms": None,
        "retrans_rate_pct": None,
        "packet_loss_pct": None,
        "delivery_rate_mbps": None,
        "bandwidth_bytes": 0,
        "total_requests": 0,
        "protocol_distribution": [],
        "missing_fields": list(missing_fields) if missing_fields is not None else [],
        **extra,
    }


def empty_sessions_response(
    *,
    page: int = 1,
    limit: int = 50,
    has_rtt: bool = False,
    has_ja4: bool = False,
    has_edge: bool = False,
    has_edge_sid: bool = False,
    has_cmcd: bool = False,
    min_reqs_flag: int | None = None,
    min_4xx_pct_flag: float | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Return an empty sessions list response payload."""
    res: dict[str, Any] = {
        "sessions": [],
        "total": 0,
        "page": page,
        "limit": limit,
        "has_rtt": has_rtt,
        "has_ja4": has_ja4,
        "has_edge": has_edge,
        "has_edge_sid": has_edge_sid,
        "has_cmcd": has_cmcd,
    }
    if min_reqs_flag is not None:
        res["min_reqs_flag"] = min_reqs_flag
    if min_4xx_pct_flag is not None:
        res["min_4xx_pct_flag"] = min_4xx_pct_flag
    res.update(extra)
    return res


def empty_dashboard_aggregates(
    fields: Iterable[str],
    *,
    interval: str = "1 minute",
    metric: str = "requests",
    where_clause: str = "1=1",
    **extra: Any,
) -> dict[str, Any]:
    """Return an empty dashboard aggregates response payload."""
    return {
        "data": {f: {"top": [], "total": 0} for f in fields},
        "time_series": [],
        "map_data": [],
        "where_clause": where_clause,
        "interval": interval,
        "metric": metric,
        "total_rows": 0,
        "total_rows_total": 0,
        **extra,
    }


def build_insights_envelope(
    insights: list[dict[str, Any]] | None = None,
    *,
    window_start: str = "",
    window_end: str = "",
    baseline_start: str = "",
    baseline_end: str = "",
    computed_at: str = "",
    window_hours: float = 0.0,
    baseline_hours: float = 0.0,
    window_total_requests: int = 0,
    **extra: Any,
) -> dict[str, Any]:
    """Build the standard insights response envelope across DuckDB and ClickHouse."""
    return {
        "insights": list(insights) if insights is not None else [],
        "window_start": window_start,
        "window_end": window_end,
        "baseline_start": baseline_start,
        "baseline_end": baseline_end,
        "computed_at": computed_at,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "window_total_requests": int(window_total_requests),
        **extra,
    }


def build_query_response_envelope(
    *,
    columns: list[str],
    data: list[dict[str, Any]],
    total_rows: int,
    truncated: bool,
    elapsed_ms: float,
    debug_queries: list[dict[str, Any]],
    debug_calls: list[dict[str, Any]],
    section_timings: list[dict[str, Any]],
    explain_plan: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build the canonical ``/api/query`` response envelope across DuckDB and ClickHouse."""
    resp: dict[str, Any] = {
        "columns": columns,
        "data": data,
        "row_count": len(data),
        "total_rows": total_rows,
        "truncated": truncated,
        "elapsed_ms": int(elapsed_ms),
        "debug_queries": debug_queries,
        "_debug_queries": debug_queries,
        "debug_calls": debug_calls,
        "section_timings": section_timings,
        **extra,
    }
    if explain_plan is not None:
        resp["explain_plan"] = explain_plan
    return resp


# ── 5. Top-N Row Formatting & Domain Breakdown Shaping ───────────────────────


def shape_keyed_rows(
    rows: Sequence[Sequence[Any]],
    keys: Sequence[str],
    **extra: Any,
) -> dict[str, Any]:
    """Zip positional tuple rows with ``keys`` into ``{"has_data": bool(rows), **extra, "rows": [...]}``."""
    return {"has_data": bool(rows), **extra, "rows": [dict(zip(keys, r, strict=False)) for r in rows]}


def compute_share_pct(
    count: int | float,
    total: int | float,
    *,
    ndigits: int | None = 2,
) -> float:
    """Compute ``(count / total) * 100`` safely, returning ``0.0`` when ``total <= 0``."""
    if not total or total <= 0:
        return 0.0
    pct = (float(count) * 100.0) / float(total)
    return round(pct, ndigits) if ndigits is not None else pct


def shape_status_code_rows(
    rows: Sequence[Any],
    *,
    round_pct: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    """Shape origin status code distribution rows from tuples or dicts."""
    if not rows:
        return {"has_data": False, "rows": [], **extra}
    first = rows[0]
    if not isinstance(first, Mapping):
        return shape_keyed_rows(rows, ("status", "count", "pct"), **extra)

    if "total_requests" in first:
        shaped = [
            {
                "status": int(r["value"]),
                "count": int(r["requests"]),
                "pct": compute_share_pct(
                    int(r["requests"]),
                    int(r["total_requests"]),
                    ndigits=2 if round_pct else None,
                ),
            }
            for r in rows
            if int(r.get("total_requests") or 0) > 0
        ]
    else:
        total = sum(int(r["requests"]) for r in rows)
        shaped = [
            {
                "status": int(r["value"]),
                "count": int(r["requests"]),
                "pct": compute_share_pct(int(r["requests"]), total, ndigits=2 if round_pct else None),
            }
            for r in rows
            if total > 0
        ]
    return {"has_data": bool(rows), "rows": shaped, **extra}


def shape_protocol_distribution(
    rows: Iterable[Any],
    *,
    fallback_total: int = 0,
    total_requests: int | None = None,
) -> list[dict[str, Any]]:
    """Shape HTTP protocol distribution rows into ``[{"protocol", "requests", "share_pct"}]``."""
    row_list = list(rows)
    if not row_list:
        return []
    pairs: list[tuple[str, int]] = []
    for r in row_list:
        if isinstance(r, Mapping):
            pairs.append(
                (str(r.get("proto_clean", r.get("protocol", "Other"))), int(r.get("reqs", r.get("requests", 0))))
            )
        else:
            pairs.append((str(r[0]), int(r[1])))
    fb = total_requests if total_requests is not None else fallback_total
    total = sum(cnt for _, cnt in pairs) or fb or 1
    return [
        {
            "protocol": proto,
            "requests": cnt,
            "share_pct": round(cnt * 100.0 / total, 2),
        }
        for proto, cnt in pairs
    ]


def parse_metro_code(metro_raw: Any) -> int | None:
    """Parse a Fastly geo metro/DMA code into an integer, returning ``None`` on invalid input."""
    if metro_raw is None:
        return None
    try:
        return int(float(metro_raw))
    except (ValueError, TypeError):
        return None


def build_asn_speed_mix(
    speed_rows: Iterable[Any],
    *,
    top_k: int = 5,
) -> dict[int, dict[str, float]]:
    """Build per-ASN connection speed share maps (top ``top_k`` speed classes per ASN)."""
    asn_speed_rows: dict[int, list[tuple[str, int]]] = {}
    for r in speed_rows:
        if isinstance(r, Mapping):
            raw_asn, raw_speed, raw_cnt = r.get("asn"), r.get("c_speed"), r.get("cnt")
        else:
            raw_asn, raw_speed, raw_cnt = r[0], r[1], r[2]
        if raw_asn is None:
            continue
        asn_v = int(raw_asn)
        bucket = asn_speed_rows.setdefault(asn_v, [])
        if len(bucket) < top_k:
            bucket.append((str(raw_speed), int(raw_cnt or 0)))
    return {
        asn_v: {cs: round(cnt / total, 3) for cs, cnt in rows}
        for asn_v, rows in asn_speed_rows.items()
        if (total := sum(cnt for _, cnt in rows)) > 0
    }


def deduplicate_metro_leaderboard(
    metro_entries: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Deduplicate metro leaderboard entries by formatted display ``city`` with request-weighted health scores."""
    seen: dict[str, dict[str, Any]] = {}
    for entry in metro_entries:
        key = str(entry["city"])
        if key in seen:
            prev = seen[key]
            prev_reqs = int(prev["total_reqs"])
            new_reqs = int(entry["total_reqs"])
            total = prev_reqs + new_reqs
            if prev.get("health_score") is not None and entry.get("health_score") is not None and total > 0:
                prev["health_score"] = round(
                    (prev["health_score"] * prev_reqs + entry["health_score"] * new_reqs) / total, 1
                )
            prev["total_reqs"] = total
        else:
            seen[key] = dict(entry)
    return sorted(seen.values(), key=lambda x: x["total_reqs"], reverse=True)


def build_network_asn_leaderboard(
    top_asns: Sequence[int],
    asn_labels: Mapping[int, str],
    asn_bucket_data: Mapping[int, Mapping[str, Mapping[str, Any]]],
    all_buckets: Sequence[str],
    bucket_seconds: int,
    all_asns_seen: Mapping[int, int],
    asn_speed_mix: Mapping[int, Mapping[str, float]],
    asn_rtt_pct: Mapping[int, Mapping[str, float | None]],
) -> list[dict[str, Any]]:
    """Build the ASN health leaderboard with 1h/1w trend analysis across DuckDB and ClickHouse."""
    leaderboard: list[dict[str, Any]] = []
    for asn in top_asns:
        buckets_data = asn_bucket_data.get(asn, {})
        if not buckets_data:
            continue
        label = asn_labels.get(asn, f"AS{asn}")
        latest_bucket = max(buckets_data)
        hs_now = buckets_data[latest_bucket].get("health_score")
        hs_1h = hs_now
        hs_1w = hs_now
        if len(all_buckets) > 1:
            n_buckets_1h = max(1, 3600 // bucket_seconds)
            sorted_buckets = sorted(buckets_data)
            early = sorted_buckets[: max(1, len(sorted_buckets) // 4)]
            hs_1w = avg_bucket_health_score(buckets_data, early)
            mid = sorted_buckets[max(0, len(sorted_buckets) - n_buckets_1h) :]
            hs_1h = avg_bucket_health_score(buckets_data, mid)

        trend = "stable"
        if hs_now is not None and hs_1h is not None:
            delta = hs_now - hs_1h
            if delta < -5:
                trend = "degrading"
            elif delta > 5:
                trend = "improving"

        rtt_pct = asn_rtt_pct.get(asn, {})
        leaderboard.append(
            {
                "asn": asn,
                "label": label,
                "health_score_now": hs_now,
                "health_score_1h_ago": hs_1h,
                "health_score_1w_ago": hs_1w,
                "trend": trend,
                "total_reqs": all_asns_seen.get(asn, 0),
                "c_speed_mix": dict(asn_speed_mix.get(asn, {})),
                "p95_rtt_us": rtt_pct.get("p95_rtt_us"),
                "p99_rtt_us": rtt_pct.get("p99_rtt_us"),
            }
        )
    return leaderboard


def compute_network_global_summary(
    leaderboard: Sequence[Mapping[str, Any]],
    all_rtt: Sequence[float] | None = None,
    total_reqs: int = 0,
    map_buckets: Sequence[Mapping[str, Any]] = (),
    cities_list: Sequence[Mapping[str, Any]] = (),
    *,
    rtt_values: Sequence[float] | None = None,
    has_country: bool = True,
    zero_as_int: bool = True,
) -> dict[str, Any]:
    """Compute global network health summary (global score, avg RTT, worst ASN, worst country/city)."""
    from backend.utils.countries import format_city_label

    rtts = rtt_values if rtt_values is not None else (all_rtt or ())
    zero_default: int | float = 0 if zero_as_int else 0.0

    all_hs = [le["health_score_now"] for le in leaderboard if le.get("health_score_now") is not None]
    global_hs = round(sum(all_hs) / len(all_hs), 1) if all_hs else zero_default

    avg_rtt_ms = round(sum(rtts) / len(rtts) / 1000.0, 1) if rtts else zero_default

    worst_asn: dict[str, Any] | None = None
    if leaderboard:
        significant = [le for le in leaderboard if le.get("total_reqs", 0) > total_reqs * 0.01]
        if significant:
            worst = min(
                significant,
                key=lambda le: le["health_score_now"] if le.get("health_score_now") is not None else 100,
            )
            worst_asn = {"label": worst["label"], "score": worst["health_score_now"]}

    worst_country: dict[str, Any] | None = None
    if has_country and map_buckets:
        for b in reversed(map_buckets):
            cities_in_b = b.get("cities")
            cities_seq = cities_in_b if isinstance(cities_in_b, list) else []
            sig_countries = [c for c in cities_seq if isinstance(c, Mapping) and c.get("reqs", 0) >= 1]
            if sig_countries:
                wc = min(
                    sig_countries,
                    key=lambda c: c["health_score"] if c.get("health_score") is not None else 100,
                )
                idx = wc.get("city_idx", -1)
                city_entry = cities_list[idx] if 0 <= idx < len(cities_list) else None
                city_name = city_entry["name"] if city_entry else ""
                label = format_city_label(city_name, wc["country"])
                worst_country = {"label": label, "score": wc["health_score"]}
                break

    return {
        "global_health_score": global_hs,
        "avg_rtt_ms": avg_rtt_ms,
        "total_reqs": total_reqs,
        "worst_asn": worst_asn,
        "worst_country": worst_country,
    }


def build_cache_collapse_breakdown(
    w_hits: int,
    w_miss: int,
    w_pass: int,
    w_total: int,
) -> dict[str, int]:
    """Build the cache disposition breakdown dictionary for the ``cache_collapse`` insight."""
    return {
        "hits": int(w_hits),
        "misses": int(w_miss),
        "passes": int(w_pass),
        "other": max(0, int(w_total) - int(w_hits) - int(w_miss) - int(w_pass)),
    }


def shape_cache_collapse_timeline_point(
    bucket_str: str,
    total_requests: Any,
    real_hits: Any,
    misses: Any,
    baseline_hit_rate: float,
) -> dict[str, Any]:
    """Shape a single timeline point for the ``cache_collapse`` insight."""
    total_i = int(total_requests or 0)
    hits_i = int(real_hits or 0)
    misses_i = int(misses or 0)
    cacheable = hits_i + misses_i
    return {
        "bucket": bucket_str,
        "expected_hits": float(cacheable * baseline_hit_rate),
        "real_hits": hits_i,
        "misses": misses_i,
        "total_requests": total_i,
        "hit_rate": float(hits_i / cacheable) if cacheable > 0 else 0.0,
    }


__all__ = [
    "CHART_INTERVAL_SECONDS",
    "CMCD_TS_DEFAULTS",
    "INSIGHT_BUCKET_INTERVALS",
    "ORIGIN_SUMMARY_KEYS",
    "PERF_SECTION_DEFAULTS",
    "SECURITY_EMPTY_CORE_SECTIONS",
    "SECURITY_SECTION_DEFAULTS",
    "WATERFALL_KEYS",
    "avg_bucket_health_score",
    "build_asn_label_map",
    "build_asn_speed_mix",
    "build_cache_collapse_breakdown",
    "build_insight_asn_context",
    "build_insights_envelope",
    "build_network_asn_leaderboard",
    "build_query_response_envelope",
    "chart_interval_seconds",
    "clamp_bucket_seconds",
    "compute_network_global_summary",
    "compute_network_health_score",
    "compute_share_pct",
    "deduplicate_metro_leaderboard",
    "empty_cmcd_aggregates",
    "empty_dashboard_aggregates",
    "empty_network_quality",
    "empty_origin_aggregates",
    "empty_origin_summary",
    "empty_performance_aggregates",
    "empty_security_aggregates",
    "empty_security_proxies",
    "empty_sessions_response",
    "empty_transport_metrics",
    "enrich_asn_rows",
    "enrich_row_with_pop_distance",
    "finish_security_proxies",
    "float_or_zero",
    "format_asn_display_label",
    "generate_epoch_aligned_buckets",
    "haversine_km",
    "is_pop_latency_elevated",
    "normalize_chart_interval",
    "opt_float",
    "pad_timeseries_buckets",
    "parse_asn_int",
    "parse_metro_code",
    "resolve_asn_names_map",
    "select_insight_bucket_interval",
    "shape_cache_collapse_timeline_point",
    "shape_keyed_rows",
    "shape_latency_percentiles",
    "shape_performance_asn_rows",
    "shape_pop_latency_rows",
    "shape_protocol_distribution",
    "shape_status_code_rows",
    "shape_waterfall_avg",
    "us_to_ms",
]
