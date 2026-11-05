"""Response shaping and post-processing helpers.

Shared across analytical services (``backend/high_scale/*``) and repositories
(``backend/repositories/*``) for:
1. ASN label enrichment (``asn.py``)
2. Time-bucket alignment & zero-filling (``time_buckets.py``)
3. Latency & percentile normalization, empty-response scaffolding, and domain shaping (``domains.py``)
"""

from __future__ import annotations

import math as math  # noqa: F401
import re
import time
from collections.abc import Callable, Iterable, Mapping, Sequence  # noqa: F401
from datetime import UTC, datetime, timedelta  # noqa: F401
from typing import TYPE_CHECKING, Any

from backend.core.fos_client import _safe_table_name
from backend.repositories.utils.response_shaping import asn as _asn_mod
from backend.repositories.utils.response_shaping import domains as _domains_mod
from backend.repositories.utils.response_shaping import time_buckets as _time_buckets_mod
from backend.repositories.utils.response_shaping.asn import (
    avg_bucket_health_score,
    build_asn_label_map,
    build_asn_speed_mix,
    build_insight_asn_context,
    build_network_asn_leaderboard,
    enrich_asn_rows,
    finish_security_proxies,
    format_asn_display_label,
    parse_asn_int,
    resolve_asn_names_map,
    shape_performance_asn_rows,
)
from backend.repositories.utils.response_shaping.domains import (
    ORIGIN_SUMMARY_KEYS,
    PERF_SECTION_DEFAULTS,
    PERF_TTL_BUCKETS,
    PERF_TTL_LAST_BUCKET,
    SECURITY_CONN_REUSE_BUCKETS,
    SECURITY_CONN_REUSE_LAST,
    SECURITY_EMPTY_CORE_SECTIONS,
    SECURITY_REQ_HEADER_BUCKETS,
    SECURITY_REQ_HEADER_LAST,
    SECURITY_SECTION_DEFAULTS,
    SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG,
    SESSIONS_DEFAULT_MIN_REQS_FLAG,
    SESSIONS_SORTABLE_COLUMNS,
    WATERFALL_KEYS,
    build_cache_collapse_breakdown,
    build_insights_envelope,
    build_query_response_envelope,
    compute_network_global_summary,
    compute_network_health_score,
    compute_share_pct,
    deduplicate_metro_leaderboard,
    empty_cmcd_aggregates,
    empty_dashboard_aggregates,
    empty_network_quality,
    empty_origin_aggregates,
    empty_origin_summary,
    empty_performance_aggregates,
    empty_security_aggregates,
    empty_security_proxies,
    empty_sessions_response,
    empty_transport_metrics,
    enrich_row_with_pop_distance,
    float_or_zero,
    haversine_km,
    is_pop_latency_elevated,
    normalize_sessions_sort,
    opt_float,
    parse_metro_code,
    shape_cache_collapse_timeline_point,
    shape_keyed_rows,
    shape_latency_percentiles,
    shape_origin_ip_health_response,
    shape_origin_ip_health_row,
    shape_origin_path_breakdown,
    shape_origin_path_row,
    shape_origin_slow_url_row,
    shape_origin_slow_urls_response,
    shape_origin_status_row,
    shape_origin_summary,
    shape_perf_latency_row,
    shape_perf_scatter_row,
    shape_perf_top_url_row,
    shape_perf_ttl_dist_rows,
    shape_pop_latency_rows,
    shape_protocol_distribution,
    shape_security_bucket_rows,
    shape_security_fingerprint_row,
    shape_security_ngwaf_bot_row,
    shape_security_ngwaf_top_bot_row,
    shape_security_threat_intel_row,
    shape_security_top_ip_header_row,
    shape_sessions_envelope,
    shape_status_code_rows,
    shape_waterfall_avg,
    us_to_ms,
)
from backend.repositories.utils.response_shaping.time_buckets import (
    _CHART_INTERVAL_ALIASES,  # noqa: F401
    CHART_INTERVAL_SECONDS,
    CMCD_TS_DEFAULTS,
    INSIGHT_BUCKET_INTERVALS,
    _coerce_utc_datetime,  # noqa: F401
    chart_interval_seconds,
    clamp_bucket_seconds,
    generate_epoch_aligned_buckets,
    normalize_chart_interval,
    pad_timeseries_buckets,
    select_insight_bucket_interval,
)
from backend.utils.date_utils import ensure_utc, parse_iso_utc  # noqa: F401
from backend.utils.module_shim import install_shim_module

_asn_mod.float_or_zero = float_or_zero

if TYPE_CHECKING:
    from backend.repositories.insights.shaping import (
        build_error_insight_card,
        build_insight_card,
        build_insights_cache_key,
        build_insufficient_baseline_card,
        clamp_insight_window,
        compute_available_history_hours,
        compute_cache_collapse_rates,
        compute_clamped_insight_windows,
        evaluate_insight_severity,
        fingerprint_column,
        format_insight_summary,
        make_investigate_url_builder,
        process_insight_rows,
        resolve_clamp_cache_key,
        shape_cache_collapse_detail,
        shape_cache_collapse_recent_misses,
        shape_insight_card_from_rows,
    )

_INSIGHT_SHAPING_EXPORTS = frozenset(
    {
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
        "make_investigate_url_builder",
        "process_insight_rows",
        "resolve_clamp_cache_key",
        "shape_cache_collapse_detail",
        "shape_cache_collapse_recent_misses",
        "shape_insight_card_from_rows",
    }
)


def __getattr__(name: str) -> Any:
    if name in _INSIGHT_SHAPING_EXPORTS:
        from backend.repositories.insights import shaping as _insight_shaping

        return getattr(_insight_shaping, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


_PARQUET_LIST_RE = re.compile(
    r"read_parquet\(\s*\[\s*'([^']+)'(?:\s*,\s*'[^']*')+\s*\]",
    re.IGNORECASE,
)


def _compact_sql_for_debug(sql: str) -> str:
    """Collapse long ``read_parquet([...])`` file lists in debug SQL payloads."""

    def _sub(m: re.Match[str]) -> str:
        first = m.group(1)
        n = m.group(0).count("'") // 2
        return f"read_parquet(['{first}', ... ({n} files)"

    return _PARQUET_LIST_RE.sub(_sub, sql)


class SectionTimer:
    """Collects per-section timing entries (``[{"section": ..., "time_ms": ...}]``)."""

    __slots__ = ("entries",)

    def __init__(self, entries: list[dict[str, Any]] | None = None) -> None:
        self.entries: list[dict[str, Any]] = entries if entries is not None else []

    def mark(self, name: str, t0: float, **observation: Any) -> None:
        self.entries.append({"section": name, "time_ms": round((time.perf_counter() - t0) * 1000, 2), **observation})

    def call(self, name: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        t0 = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            self.mark(name, t0)


def _safe_table(name: str) -> str:
    """Validate and return a safe SQL identifier for table names."""
    return _safe_table_name(name)


_SUBMODULES = (
    _asn_mod,
    _time_buckets_mod,
    _domains_mod,
)

_ShimModule = install_shim_module(__name__, _SUBMODULES, only_existing_attrs=True)


__all__ = [
    "CHART_INTERVAL_SECONDS",
    "CMCD_TS_DEFAULTS",
    "INSIGHT_BUCKET_INTERVALS",
    "ORIGIN_SUMMARY_KEYS",
    "PERF_SECTION_DEFAULTS",
    "PERF_TTL_BUCKETS",
    "PERF_TTL_LAST_BUCKET",
    "SECURITY_CONN_REUSE_BUCKETS",
    "SECURITY_CONN_REUSE_LAST",
    "SECURITY_EMPTY_CORE_SECTIONS",
    "SECURITY_REQ_HEADER_BUCKETS",
    "SECURITY_REQ_HEADER_LAST",
    "SECURITY_SECTION_DEFAULTS",
    "SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG",
    "SESSIONS_DEFAULT_MIN_REQS_FLAG",
    "SESSIONS_SORTABLE_COLUMNS",
    "SectionTimer",
    "WATERFALL_KEYS",
    "_compact_sql_for_debug",
    "_safe_table",
    "avg_bucket_health_score",
    "build_asn_label_map",
    "build_asn_speed_mix",
    "build_cache_collapse_breakdown",
    "build_error_insight_card",
    "build_insight_asn_context",
    "build_insight_card",
    "build_insights_cache_key",
    "build_insights_envelope",
    "build_insufficient_baseline_card",
    "build_network_asn_leaderboard",
    "build_query_response_envelope",
    "chart_interval_seconds",
    "clamp_bucket_seconds",
    "clamp_insight_window",
    "compute_available_history_hours",
    "compute_cache_collapse_rates",
    "compute_clamped_insight_windows",
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
    "evaluate_insight_severity",
    "fingerprint_column",
    "finish_security_proxies",
    "float_or_zero",
    "format_asn_display_label",
    "format_insight_summary",
    "generate_epoch_aligned_buckets",
    "haversine_km",
    "is_pop_latency_elevated",
    "make_investigate_url_builder",
    "normalize_chart_interval",
    "normalize_sessions_sort",
    "opt_float",
    "pad_timeseries_buckets",
    "parse_asn_int",
    "parse_metro_code",
    "process_insight_rows",
    "resolve_asn_names_map",
    "resolve_clamp_cache_key",
    "select_insight_bucket_interval",
    "shape_cache_collapse_detail",
    "shape_cache_collapse_recent_misses",
    "shape_cache_collapse_timeline_point",
    "shape_insight_card_from_rows",
    "shape_keyed_rows",
    "shape_latency_percentiles",
    "shape_origin_ip_health_response",
    "shape_origin_ip_health_row",
    "shape_origin_path_breakdown",
    "shape_origin_path_row",
    "shape_origin_slow_url_row",
    "shape_origin_slow_urls_response",
    "shape_origin_status_row",
    "shape_origin_summary",
    "shape_perf_latency_row",
    "shape_perf_scatter_row",
    "shape_perf_top_url_row",
    "shape_perf_ttl_dist_rows",
    "shape_performance_asn_rows",
    "shape_pop_latency_rows",
    "shape_protocol_distribution",
    "shape_security_bucket_rows",
    "shape_security_fingerprint_row",
    "shape_security_ngwaf_bot_row",
    "shape_security_ngwaf_top_bot_row",
    "shape_security_threat_intel_row",
    "shape_security_top_ip_header_row",
    "shape_sessions_envelope",
    "shape_status_code_rows",
    "shape_waterfall_avg",
    "us_to_ms",
]
