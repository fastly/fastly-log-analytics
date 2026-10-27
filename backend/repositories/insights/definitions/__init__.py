from __future__ import annotations

import re  # noqa: F401
import sys as _sys
import types as _types
from typing import Any

from backend.core.metric_expressions import coalesce  # noqa: F401
from backend.core.share_db import mask_ip  # noqa: F401
from backend.repositories.insights import InsightCategory, InsightDefinition, registry  # noqa: F401
from backend.repositories.insights.definitions import (
    errors_and_cache,
    performance,
    security,
    sql_coalesced,
    traffic,
)
from backend.repositories.insights.definitions.errors_and_cache import (
    CACHE_COLLAPSE,
    CACHE_HIT_CLIFF,
    CACHE_TTL_MISMATCH,
    CACHEABILITY_REGRESSION,
    CITY_ERROR_SPIKES,
    ERROR_SPIKES,
    ORIGIN_ERROR_RATE,
    ORIGIN_IP_FAILURE,
    ORIGIN_RETRIES,
    cache_collapse_processor,
    cache_hit_cliff_processor,
    cache_ttl_mismatch_processor,
    cacheability_regression_processor,
    city_error_spikes_processor,
    error_spikes_processor,
    origin_error_rate_processor,
    origin_ip_failure_processor,
    origin_retries_processor,
)
from backend.repositories.insights.definitions.performance import (
    ASN_METRO_PERFORMANCE,
    CITY_LATENCY_REGRESSIONS,
    HTTP3_FALLBACK,
    IMAGE_OPTIMIZATION_OPPORTUNITIES,
    LATENCY_REGRESSION,
    METRO_DELIVERY_DEGRADATION,
    NETWORK_ASN_HEALTH,
    ORIGIN_LATENCY_SPIKE,
    PAYLOAD_COMPRESSION_REGRESSION,
    POP_LATENCY_REGRESSION,
    REGION_LATENCY,
    SHIELD_PATH_DEGRADATION,
    TAIL_LATENCY,
    TIMEOUT_SPLIT,
    asn_metro_performance_processor,
    city_latency_processor,
    http3_fallback_processor,
    image_optimization_processor,
    latency_regression_processor,
    metro_delivery_degradation_processor,
    network_asn_health_processor,
    origin_latency_spike_processor,
    payload_compression_regression_processor,
    pop_latency_regression_processor,
    region_latency_processor,
    shield_path_degradation_processor,
    tail_latency_processor,
    timeout_split_processor,
)
from backend.repositories.insights.definitions.security import (
    BOTNET_GROUPING,
    CIPHER_SPREAD,
    CONNECTION_ABUSE,
    CONTENT_DISCOVERY,
    CRAWLER_DISPARITY,
    CREDENTIAL_ENUMERATION,
    LOW_AND_SLOW,
    NEW_PROBE_URLS,
    ORPHANED_DEEP_CRAWL,
    PROXY_SURGE,
    REPEATED_PATTERNS,
    REPEATED_PATTERNS_FP,
    REQUEST_SIZE_ANOMALY,
    RESIDENTIAL_FINGERPRINT_DISPERSION,
    SESSION_HARVESTING,
    STALE_BROWSER_VERSION,
    WAF_SIGNAL_SPIKES,
    _process_repeated_patterns,  # noqa: F401
    botnet_grouping_processor,
    cipher_spread_processor,
    connection_abuse_processor,
    content_discovery_processor,
    crawler_disparity_processor,
    credential_enumeration_processor,
    low_and_slow_processor,
    new_probe_urls_processor,
    orphaned_deep_crawl_processor,
    proxy_surge_processor,
    proxy_surge_severity,
    repeated_patterns_fp_processor,
    repeated_patterns_processor,
    repeated_patterns_severity,
    request_size_anomaly_processor,
    residential_fingerprint_dispersion_processor,
    session_harvesting_processor,
    stale_browser_version_processor,
    waf_signal_spikes_processor,
)
from backend.repositories.insights.definitions.sql_coalesced import (
    _AUTH_PATHS,  # noqa: F401
    _BOT_UA_TOKENS,  # noqa: F401
    _COALESCED_URL_CORE_COLS,  # noqa: F401
    _COMPRESSIBLE_EXTS,  # noqa: F401
    AUTH_PATHS_REGEX,
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
    COMPRESSIBLE_REGEX,
    NEW_PROBE_REGEX,
    NEW_PROBES,
    REPEATED_BOT_UA_REGEX,
    _build_ip_max_vs_p95_sql,  # noqa: F401
    _build_ip_spread_sql,  # noqa: F401
    _build_monoculture_sql,  # noqa: F401
    _build_new_dim_traffic_sql,  # noqa: F401
    _build_p95_latency_regression_sql,  # noqa: F401
    _build_repeated_patterns_sql,  # noqa: F401
    _city_filters,  # noqa: F401
    _error_rate_spike_item,  # noqa: F401
    _fmt_asn_label,  # noqa: F401
    _fmt_metro_label,  # noqa: F401
    _ip_max_vs_p95_processor,  # noqa: F401
    _ip_sec_core_cols,  # noqa: F401
    _item,  # noqa: F401
    _mask_ip_if_needed,  # noqa: F401
    _monoculture_processor,  # noqa: F401
    _p95_latency_item,  # noqa: F401
    _reg,  # noqa: F401
    _scale_baseline,  # noqa: F401
    _traffic_share_rates,  # noqa: F401
    _url_rate_regression_processor,  # noqa: F401
    _zero_baseline_count_item,  # noqa: F401
)
from backend.repositories.insights.definitions.traffic import (
    ASN_CONCENTRATION,
    ASN_HOSTING_SHIFT,
    CITY_SURGES,
    CONNECTION_TYPE_MIX,
    IMPOSSIBLE_DISTANCE,
    METHOD_DRIFT,
    NEW_ASN_TRAFFIC,
    NEW_CITY_TRAFFIC,
    NEW_COUNTRY_TRAFFIC,
    REFERER_MONOCULTURE,
    UA_MONOCULTURE,
    asn_concentration_processor,
    asn_hosting_shift_processor,
    city_surges_processor,
    connection_type_mix_processor,
    impossible_distance_processor,
    method_drift_processor,
    new_asn_traffic_processor,
    new_city_traffic_processor,
    new_country_traffic_processor,
    referer_monoculture_processor,
    ua_monoculture_processor,
)
from backend.utils.countries import format_city_label  # noqa: F401

_ORIGINAL_REGISTRATION_ORDER = (
    "error_spikes",
    "botnet_grouping",
    "new_country_traffic",
    "city_surges",
    "city_error_spikes",
    "city_latency_regressions",
    "new_city_traffic",
    "ua_monoculture",
    "new_probe_urls",
    "waf_signal_spikes",
    "proxy_surge",
    "asn_concentration",
    "asn_metro_performance",
    "cache_collapse",
    "cacheability_regression",
    "latency_regression",
    "impossible_distance",
    "tail_latency",
    "cipher_spread",
    "request_size_anomaly",
    "connection_abuse",
    "region_latency",
    "cache_ttl_mismatch",
    "image_optimization_opportunities",
    "origin_latency_spike",
    "origin_error_rate",
    "origin_retries",
    "origin_ip_failure",
    "shield_path_degradation",
    "repeated_patterns",
    "repeated_patterns_fp",
    "low_and_slow",
    "credential_enumeration",
    "network_asn_health",
    "content_discovery",
    "referer_monoculture",
    "method_drift",
    "new_asn_traffic",
    "asn_hosting_shift",
    "metro_delivery_degradation",
    "connection_type_mix",
    "pop_latency_regression",
    "http3_fallback",
    "cache_hit_cliff",
    "payload_compression_regression",
    "session_harvesting",
    "timeout_split",
    "crawler_disparity",
    "stale_browser_version",
    "orphaned_deep_crawl",
    "residential_fingerprint_dispersion",
)

_ordered = {k: registry._definitions[k] for k in _ORIGINAL_REGISTRATION_ORDER if k in registry._definitions}
for _k, _v in registry._definitions.items():
    if _k not in _ordered:
        _ordered[_k] = _v
registry._definitions.clear()
registry._definitions.update(_ordered)


class _ShimModule(_types.ModuleType):
    """Propagate attribute monkeypatches across the definitions package."""

    def __setattr__(self, name: str, value: object) -> None:
        super().__setattr__(name, value)
        if name.startswith("__") and name.endswith("__"):
            return
        for mod in _SUBMODULES:
            if mod is not self and hasattr(mod, name):
                _types.ModuleType.__setattr__(mod, name, value)


_SUBMODULES = (
    _sys.modules[__name__],
    sql_coalesced,
    errors_and_cache,
    traffic,
    performance,
    security,
)
for _m in _SUBMODULES:
    _m.__class__ = _ShimModule

SQL: Any = _sys.modules[__name__]
_sys.modules["backend.repositories._sql.insights"] = _sys.modules[__name__]

__all__ = [
    "NEW_PROBES",
    "NEW_PROBE_REGEX",
    "ERROR_SPIKES",
    "BOTNET_GROUPING",
    "NEW_COUNTRY_TRAFFIC",
    "CITY_SURGES",
    "CITY_ERROR_SPIKES",
    "CITY_LATENCY_REGRESSIONS",
    "NEW_CITY_TRAFFIC",
    "UA_MONOCULTURE",
    "NEW_PROBE_URLS",
    "WAF_SIGNAL_SPIKES",
    "PROXY_SURGE",
    "ASN_CONCENTRATION",
    "ASN_METRO_PERFORMANCE",
    "CACHE_COLLAPSE",
    "CACHEABILITY_REGRESSION",
    "LATENCY_REGRESSION",
    "IMPOSSIBLE_DISTANCE",
    "TAIL_LATENCY",
    "CIPHER_SPREAD",
    "REQUEST_SIZE_ANOMALY",
    "CONNECTION_ABUSE",
    "REGION_LATENCY",
    "CACHE_TTL_MISMATCH",
    "IMAGE_OPTIMIZATION_OPPORTUNITIES",
    "ORIGIN_LATENCY_SPIKE",
    "ORIGIN_ERROR_RATE",
    "ORIGIN_RETRIES",
    "ORIGIN_IP_FAILURE",
    "SHIELD_PATH_DEGRADATION",
    "REPEATED_BOT_UA_REGEX",
    "REPEATED_PATTERNS",
    "REPEATED_PATTERNS_FP",
    "LOW_AND_SLOW",
    "AUTH_PATHS_REGEX",
    "CREDENTIAL_ENUMERATION",
    "NETWORK_ASN_HEALTH",
    "CONTENT_DISCOVERY",
    "REFERER_MONOCULTURE",
    "METHOD_DRIFT",
    "NEW_ASN_TRAFFIC",
    "ASN_HOSTING_SHIFT",
    "METRO_DELIVERY_DEGRADATION",
    "CONNECTION_TYPE_MIX",
    "POP_LATENCY_REGRESSION",
    "HTTP3_FALLBACK",
    "CACHE_HIT_CLIFF",
    "COMPRESSIBLE_REGEX",
    "PAYLOAD_COMPRESSION_REGRESSION",
    "SESSION_HARVESTING",
    "TIMEOUT_SPLIT",
    "CRAWLER_DISPARITY",
    "STALE_BROWSER_VERSION",
    "ORPHANED_DEEP_CRAWL",
    "RESIDENTIAL_FINGERPRINT_DISPERSION",
    "COALESCED_CITY_AGGREGATES",
    "COALESCED_URL_AGGREGATES",
    "COALESCED_URL_EXTENDED_AGGREGATES",
    "COALESCED_IP_SECURITY_AGGREGATES",
    "COALESCED_IP_EXTENDED_AGGREGATES",
    "COALESCED_ASN_AGGREGATES",
    "COALESCED_SCALAR_EXTENDED_AGGREGATES",
    "COALESCED_FP_AGGREGATES",
    "COALESCED_DIM_AGGREGATES",
    "COALESCED_GEO_LATENCY_AGGREGATES",
    "asn_concentration_processor",
    "asn_hosting_shift_processor",
    "asn_metro_performance_processor",
    "botnet_grouping_processor",
    "cache_collapse_processor",
    "cache_hit_cliff_processor",
    "cache_ttl_mismatch_processor",
    "cacheability_regression_processor",
    "cipher_spread_processor",
    "city_error_spikes_processor",
    "city_latency_processor",
    "city_surges_processor",
    "connection_abuse_processor",
    "connection_type_mix_processor",
    "content_discovery_processor",
    "crawler_disparity_processor",
    "credential_enumeration_processor",
    "error_spikes_processor",
    "http3_fallback_processor",
    "image_optimization_processor",
    "impossible_distance_processor",
    "latency_regression_processor",
    "low_and_slow_processor",
    "metro_delivery_degradation_processor",
    "method_drift_processor",
    "network_asn_health_processor",
    "new_asn_traffic_processor",
    "new_city_traffic_processor",
    "new_country_traffic_processor",
    "new_probe_urls_processor",
    "origin_error_rate_processor",
    "origin_ip_failure_processor",
    "origin_latency_spike_processor",
    "origin_retries_processor",
    "orphaned_deep_crawl_processor",
    "payload_compression_regression_processor",
    "pop_latency_regression_processor",
    "proxy_surge_processor",
    "proxy_surge_severity",
    "referer_monoculture_processor",
    "region_latency_processor",
    "repeated_patterns_fp_processor",
    "repeated_patterns_processor",
    "repeated_patterns_severity",
    "request_size_anomaly_processor",
    "residential_fingerprint_dispersion_processor",
    "session_harvesting_processor",
    "shield_path_degradation_processor",
    "stale_browser_version_processor",
    "tail_latency_processor",
    "timeout_split_processor",
    "ua_monoculture_processor",
    "waf_signal_spikes_processor",
]
