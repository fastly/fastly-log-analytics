"""Cross-engine insight post-processing, severity scoring, evidence formatting, and card assembly.

Shared between the DuckDB insights repository (``backend/repositories/insights/repository.py``)
and the High-Scale ClickHouse insights service (``backend/high_scale/insights/``).
"""

from __future__ import annotations

import heapq
import re
import urllib.parse
from collections.abc import Callable, Collection, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.repositories.utils.response_shaping import (
    build_cache_collapse_breakdown,
    build_insight_asn_context,
    shape_cache_collapse_timeline_point,
)
from backend.utils.date_utils import parse_iso_utc

TOP_K_INSIGHT_ITEMS = 15

# ── 1. Shared Token & Regex Constants ────────────────────────────────────────

NEW_PROBES: list[str] = [
    "admin",
    ".env",
    ".git",
    "wp-",
    "phpmyadmin",
    "config",
    "backup",
    "shell",
    "passwd",
    "xmlrpc",
    "actuator",
    "console",
    "cgi-bin",
    ".php",
    ".asp",
    "../../",
    "swagger",
    "api-docs",
    "graphql",
    "debug",
]
COMPRESSIBLE_EXTENSIONS: tuple[str, ...] = (".js", ".css", ".html", ".json", ".svg", ".xml", ".txt")
AUTH_PATHS: tuple[str, ...] = (
    "login",
    "signin",
    "sign-in",
    "log-in",
    "auth",
    "oauth",
    "openid",
    "token",
    "sso",
    "saml",
    "session",
    "account",
    "password",
    "passwd",
    "passwrd",
    "credential",
    "wp-login",
    "xmlrpc",
    "mfa",
    "otp",
    "2fa",
    "verify",
)
BOT_UA_TOKENS: tuple[str, ...] = (
    "googlebot",
    "bingbot",
    "slurp",
    "duckduckbot",
    "baiduspider",
    "yandexbot",
    "applebot",
    "uptimerobot",
    "pingdom",
    "statuscake",
    "datadoghq",
    "newrelic",
    "ahrefsbot",
    "semrushbot",
    "gptbot",
    "claudebot",
    "ccbot",
    "facebookexternalhit",
    "bot",
    "crawler",
    "spider",
    "monitor",
)
PROBE_REGEX = "(?i)" + "|".join(re.escape(path) for path in NEW_PROBES)
AUTH_REGEX = "(?i)" + "|".join(re.escape(path) for path in AUTH_PATHS)
BOT_UA_REGEX = "(?i)" + "|".join(re.escape(token) for token in BOT_UA_TOKENS)
COMPRESSIBLE_REGEX = "(?i)" + "|".join(re.escape(ext) for ext in COMPRESSIBLE_EXTENSIONS)


def fingerprint_column(present: Collection[str]) -> str | None:
    """Return the preferred TLS fingerprint column (``ja4`` then ``ja3``) available in ``present``."""
    for column in ("ja4", "ja3"):
        if column in present:
            return column
    return None


# ── 2. Time Window Clamping, History Hours & Cache Key Construction ──────────


def clamp_insight_window(
    window_hours: float,
    baseline_hours: float,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    *,
    now: datetime | None = None,
) -> tuple[datetime, datetime, datetime]:
    """Return ``(baseline_start, window_start, now)`` as UTC datetimes, honouring analyst clamps."""
    if now is None:
        now = datetime.now(UTC)
    if clamp_end:
        ceiling = parse_iso_utc(clamp_end)
        if ceiling and ceiling < now:
            now = ceiling
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(hours=baseline_hours + window_hours)
    if clamp_start:
        floor = parse_iso_utc(clamp_start)
        if floor:
            baseline_start = max(baseline_start, floor)
            window_start = max(window_start, floor)
    return baseline_start, window_start, now


def compute_clamped_insight_windows(
    window_hours: float,
    baseline_hours: float,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    *,
    now: datetime | None = None,
) -> tuple[datetime, str, str, str]:
    """Return ``(now, now_iso, window_start_iso, baseline_start_iso)`` for standard insights queries."""
    baseline_start, window_start, now_dt = clamp_insight_window(
        window_hours, baseline_hours, clamp_start, clamp_end, now=now
    )
    return now_dt, now_dt.isoformat(), window_start.isoformat(), baseline_start.isoformat()


def compute_available_history_hours(now: datetime, earliest_ts: Any) -> float:
    """Compute hours of available history between ``earliest_ts`` and ``now``."""
    if not earliest_ts:
        return 0.0
    dt = parse_iso_utc(earliest_ts) if isinstance(earliest_ts, str) else earliest_ts
    if isinstance(dt, datetime):
        dt = dt.astimezone(UTC) if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
        return (now - dt).total_seconds() / 3600.0
    return 0.0


def resolve_clamp_cache_key(
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    clamp_cache_key: str | None = None,
) -> str:
    """Normalize analyst time-clamp bounds into a minute-bucketed cache key component."""
    if clamp_cache_key is not None:
        return clamp_cache_key
    if clamp_start or clamp_end:
        from backend.repositories.utils.response_cache import bucket_time_to_minute

        return f"{bucket_time_to_minute(clamp_start) or ''}:{bucket_time_to_minute(clamp_end) or ''}"
    return ""


def build_insights_cache_key(
    source_name: str,
    window_hours: float = 24,
    baseline_hours: float = 168,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    filters: dict[str, Any] | None = None,
) -> str:
    """Build the canonical cache key for ``get_insights``."""
    key_clamp = resolve_clamp_cache_key(clamp_start, clamp_end, clamp_cache_key)
    filter_key_part = ""
    if filters:
        from backend.repositories.utils.filters import filter_spec_attr

        f_items = []
        for k in sorted(filters.keys()):
            spec = filters[k]
            mode = filter_spec_attr(spec, "mode") or "include"
            vals = filter_spec_attr(spec, "values") or []
            if vals:
                f_items.append(f"{k}:{mode}:{','.join(str(v) for v in vals)}")
        if f_items:
            filter_key_part = ":" + "|".join(f_items)
    return f"{source_name}:{window_hours}:{baseline_hours}:{key_clamp}:{int(mask_ips)}{filter_key_part}"


# ── 3. Investigate URL, Severity, Summary & Card Assembly ────────────────────


def iso_or_str(val: Any) -> str:
    """Return ``val.isoformat()`` when supported, otherwise ``str(val)``."""
    return val.isoformat() if hasattr(val, "isoformat") else str(val)


def make_investigate_url_builder(
    service_id: str | None,
    window_start: str | datetime,
    window_end: str | datetime,
) -> Callable[[dict[str, Any] | None], str]:
    """Return a closure that builds ``/dashboard?...`` drill-down URLs for an insight window."""
    start_s = iso_or_str(window_start)
    end_s = iso_or_str(window_end)

    def build(filters: dict[str, Any] | None = None) -> str:
        pairs: list[tuple[str, str]] = ([("service", service_id)] if service_id else []) + [
            ("start", start_s),
            ("end", end_s),
        ]
        pairs.extend((f"filter_{col}", str(val)) for col, val in (filters or {}).items() if val is not None)
        return "/dashboard?" + urllib.parse.urlencode(pairs)

    return build


def evaluate_insight_severity(definition: Any, items: list[dict[str, Any]]) -> str:
    """Evaluate card-level severity from processed ``items`` and optional ``definition.severity_logic``."""
    if not items:
        return "clean"
    severity_logic = getattr(definition, "severity_logic", None)
    if severity_logic is not None:
        return str(severity_logic(items))
    return "critical" if any(item.get("severity") == "critical" for item in items) else "warning"


def format_insight_summary(definition: Any, items: list[dict[str, Any]]) -> str:
    """Format the human-readable card summary line for an insight definition and its items."""
    title = str(getattr(definition, "title", ""))
    insight_id = str(getattr(definition, "id", ""))
    if not items:
        return f"No {title.lower()} detected"
    if insight_id == "error_spikes":
        return f"{len(items)} URLs with elevated server error rates"
    if insight_id == "botnet_grouping":
        return f"{len(items)} fingerprints with suspicious IP spread"
    return f"{len(items)} anomalies detected"


def build_insight_card(
    insight_id: str,
    title: str,
    description: str,
    category: str | None,
    severity: str,
    summary: str,
    items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Construct a standardized insight card dictionary."""
    return {
        "id": insight_id,
        "title": title,
        "description": description,
        "category": category,
        "severity": severity,
        "summary": summary,
        "items": items if items is not None else [],
    }


def build_insufficient_baseline_card(
    insight_id: str,
    title: str,
    description: str,
    category: str | None,
    baseline_hours: float,
    available_history_hours: float,
) -> dict[str, Any]:
    """Construct an ``info`` card when historical data is shorter than ``baseline_hours``."""
    available = max(0.1, round(available_history_hours, 1))
    return build_insight_card(
        insight_id=insight_id,
        title=title,
        description=description,
        category=category,
        severity="info",
        summary=f"Requires {int(baseline_hours)}h of historical data (only {available}h available)",
        items=[],
    )


def build_error_insight_card(
    insight_id: str,
    title: str,
    category: str | None,
    error_message: str,
    description: str = "",
) -> dict[str, Any]:
    """Construct an ``error`` card when an insight query fails."""
    return build_insight_card(
        insight_id=insight_id,
        title=title,
        description=description,
        category=category,
        severity="error",
        summary=f"Query failed: {error_message}",
        items=[],
    )


def process_insight_rows(
    definition: Any,
    rows: Iterable[Any],
    *,
    service_id: str,
    window_hours: float,
    baseline_hours: float,
    fp_col: str,
    actual_cols: Collection[str],
    mask_ips: bool,
    investigate_url_builder: Callable[[dict[str, Any] | None], str],
    swallow_row_errors: bool = False,
) -> list[dict[str, Any]]:
    """Run ``definition.row_processor`` across ``rows`` with ASN/DMA context and drill-down URLs."""
    row_processor = getattr(definition, "row_processor", None)
    row_list = list(rows)
    if not row_list:
        return []
    insight_id = str(getattr(definition, "id", ""))
    context: dict[str, Any] = {
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "fp_col": fp_col,
        "actual_cols": actual_cols,
        "mask_ips": mask_ips,
        **build_insight_asn_context(
            service_id, row_list, insight_id, include_dma="metro" in insight_id and "metro" in actual_cols
        ),
    }
    items: list[dict[str, Any]] = []
    for row in row_list:
        try:
            item = row_processor(row, definition, context) if row_processor else {}
            if "investigate_url" not in item:
                item["investigate_url"] = investigate_url_builder(item.get("meta", {}).get("filters"))
            items.append(item)
        except Exception:
            if swallow_row_errors:
                continue
            raise
    return items


def shape_insight_card_from_rows(
    definition: Any,
    rows: Iterable[Any],
    *,
    service_id: str,
    window_hours: float,
    baseline_hours: float,
    fp_col: str,
    actual_cols: Collection[str],
    mask_ips: bool,
    investigate_url_builder: Callable[[dict[str, Any] | None], str],
    swallow_row_errors: bool = False,
) -> dict[str, Any]:
    """Process raw rows for ``definition`` and return the complete insight card dict."""
    items = (
        process_insight_rows(
            definition,
            rows,
            service_id=service_id,
            window_hours=window_hours,
            baseline_hours=baseline_hours,
            fp_col=fp_col,
            actual_cols=actual_cols,
            mask_ips=mask_ips,
            investigate_url_builder=investigate_url_builder,
            swallow_row_errors=swallow_row_errors,
        )
        if getattr(definition, "row_processor", None) is not None
        else []
    )
    severity = evaluate_insight_severity(definition, items)
    summary = format_insight_summary(definition, items)
    return build_insight_card(
        insight_id=definition.id,
        title=definition.title,
        description=definition.description,
        category=str(definition.category),
        severity=severity,
        summary=summary,
        items=items,
    )


# ── 4. Cache-Collapse Detail Shaping ─────────────────────────────────────────


def compute_cache_collapse_rates(
    w_hits: int,
    w_miss: int,
    w_pass: int,
    w_total: int,
    b_hits: int,
    b_miss: int,
    b_pass: int,
    b_total: int,
) -> tuple[float, float, float, float]:
    """Return ``(w_hit_rate, b_hit_rate, w_pass_rate, b_pass_rate)`` in ``[0.0, 1.0]``."""
    w_cacheable, b_cacheable = w_hits + w_miss, b_hits + b_miss
    w_rate = (w_hits / w_cacheable) if w_cacheable else 0.0
    b_rate = (b_hits / b_cacheable) if b_cacheable else 0.0
    w_pass_rate = (w_pass / w_total) if w_total else 0.0
    b_pass_rate = (b_pass / b_total) if b_total else 0.0
    return w_rate, b_rate, w_pass_rate, b_pass_rate


def shape_cache_collapse_recent_misses(
    miss_rows: Iterable[Any],
    *,
    mask_ips: bool = False,
    iso_formatter: Callable[[Any], str] | None = None,
) -> list[dict[str, Any]]:
    """Normalize recent cache-miss rows (tuple or dict) with optional IP masking."""
    from backend.core.share_db import mask_ip

    fmt = iso_formatter or iso_or_str
    recent_misses: list[dict[str, Any]] = []
    for row in miss_rows:
        if isinstance(row, Mapping):
            ts_raw = row.get("timestamp")
            cache_val = row.get("cache") or "UNKNOWN"
            pop_val = row.get("pop") or None
            ip_val = row.get("ip")
            status_val = row.get("status")
        else:
            ts_raw, cache_val, pop_val, ip_val, status_val = row[0], row[1] or "UNKNOWN", row[2], row[3], row[4]
        recent_misses.append(
            {
                "timestamp": fmt(ts_raw),
                "cache": cache_val,
                "pop": pop_val,
                "ip": mask_ip(ip_val) if mask_ips and ip_val else ip_val,
                "status": int(status_val) if status_val is not None else None,
            }
        )
    return recent_misses


def shape_cache_collapse_detail(
    url: str,
    rates: tuple[int, int, int, int, int, int, int, int],
    timeline_rows: Iterable[Any],
    miss_rows: Iterable[Any],
    *,
    mask_ips: bool = False,
    include_has_data: bool = False,
    iso_formatter: Callable[[Any], str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Assemble the complete cache-collapse drill-down payload across DuckDB and ClickHouse."""
    fmt = iso_formatter or iso_or_str
    w_hits, w_miss, w_pass, w_total, b_hits, b_miss, b_pass, b_total = rates
    w_rate, b_rate, w_pass_rate, b_pass_rate = compute_cache_collapse_rates(
        w_hits, w_miss, w_pass, w_total, b_hits, b_miss, b_pass, b_total
    )
    timeline = [
        shape_cache_collapse_timeline_point(
            fmt(r["bucket"] if isinstance(r, Mapping) else r[0]),
            r["total_requests"] if isinstance(r, Mapping) else r[1],
            r["real_hits"] if isinstance(r, Mapping) else r[2],
            r["misses"] if isinstance(r, Mapping) else r[3],
            b_rate,
        )
        for r in timeline_rows
    ]
    recent_misses = shape_cache_collapse_recent_misses(miss_rows, mask_ips=mask_ips, iso_formatter=fmt)
    payload: dict[str, Any] = {
        "url": url,
        "timeline": timeline,
        "recent_misses": recent_misses,
        "breakdown": build_cache_collapse_breakdown(w_hits, w_miss, w_pass, w_total),
        "baseline_hit_rate": float(b_rate) * 100,
        "window_hit_rate": float(w_rate) * 100,
        "baseline_pass_rate": float(b_pass_rate) * 100,
        "window_pass_rate": float(w_pass_rate) * 100,
        **extra,
    }
    if include_has_data:
        payload["has_data"] = bool(w_total + b_total)
    return payload


# ── 5. Anomaly Threshold & Top-K Helpers ─────────────────────────────────────


def share_exceeded(
    wc: int,
    w_tot: int,
    bc: int,
    b_tot: int,
    min_share: float,
    mult: float = 2.0,
    offset: float = 0.05,
) -> bool:
    """Return True when window share is at least ``min_share`` and exceeds ``mult * baseline_share + offset``."""
    return w_tot > 0 and (wc / w_tot) >= min_share and (b_tot == 0 or (wc / w_tot) >= (bc / b_tot) * mult + offset)


def p95_regressed(
    w_tot: Any,
    b_tot: Any,
    w_p95: Any,
    b_p95: Any,
    min_w: int,
    min_b: int,
    mult: float,
    min_diff: float,
) -> bool:
    """Return True when P95 latency regressed by both ``mult`` factor and ``min_diff`` ms with sufficient volume."""
    return (
        (w_tot or 0) >= min_w
        and (b_tot or 0) >= min_b
        and w_p95 is not None
        and b_p95 is not None
        and w_p95 >= b_p95 * mult
        and (w_p95 - b_p95) >= min_diff
    )


def push_top_k(
    heap: list[tuple[Any, ...]],
    score: Any,
    counter: int,
    item: tuple[Any, ...],
    k: int = TOP_K_INSIGHT_ITEMS,
) -> None:
    """Maintain a bounded min-heap of the top-``k`` highest-scoring items."""
    (heapq.heappush if len(heap) < k else heapq.heappushpop)(heap, (score, counter, item))


def heap_to_sorted_items(heap: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """Extract items from a bounded top-K heap sorted by descending score."""
    return [entry[2] for entry in sorted(heap, key=lambda e: e[0], reverse=True)]


__all__ = [
    "AUTH_PATHS",
    "AUTH_REGEX",
    "BOT_UA_REGEX",
    "BOT_UA_TOKENS",
    "COMPRESSIBLE_EXTENSIONS",
    "COMPRESSIBLE_REGEX",
    "PROBE_REGEX",
    "TOP_K_INSIGHT_ITEMS",
    "build_error_insight_card",
    "build_insight_card",
    "build_insights_cache_key",
    "build_insufficient_baseline_card",
    "clamp_insight_window",
    "compute_available_history_hours",
    "compute_cache_collapse_rates",
    "compute_clamped_insight_windows",
    "evaluate_insight_severity",
    "fingerprint_column",
    "format_insight_summary",
    "heap_to_sorted_items",
    "iso_or_str",
    "make_investigate_url_builder",
    "p95_regressed",
    "process_insight_rows",
    "push_top_k",
    "resolve_clamp_cache_key",
    "shape_cache_collapse_detail",
    "shape_cache_collapse_recent_misses",
    "shape_insight_card_from_rows",
    "share_exceeded",
]
