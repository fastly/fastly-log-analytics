"""Hourly Top-N + bundled rollups for the dashboard.

Carved out of a single 2,045-line ``rollups.py`` for 10.9 file-size
sweep. Submodules:

- :mod:`._common` — constants, validators, paths, query builders, atomic
  marker IO. Imported by every other sub-module.
- :mod:`.time_series` — per-hour 1-minute time_series.parquet bundles.
- :mod:`.sessions` — per-hour per-(ip, ja4) sessions.parquet bundles.
- :mod:`.hour_bundles` — combine per-field hour parquets into
  ``hour_bundled/hour=H/all_fields.parquet`` + retention sweep + backfill.
- :mod:`.day_bundles` — combine per-field day parquets into
  ``day_bundled/day=D/all_fields.parquet`` + closed-day compactor.
- :mod:`.recompute` — per-tick recompute + one-shot backfill + cleanup +
  the shared per-field COPY driver.
- :mod:`.wellknown_bots` — pre-materialised bot-prefiltered rollup +
  read path.

External surface (preserved verbatim from the pre-split file — every
symbol on the right-hand side of an existing
``from backend.core.rollups import ...`` keeps working):

  Public writers:
    build_time_series_bundles
    build_session_bundles
    bundle_hours
    bundle_days, backfill_day_bundles, compact_closed_days_to_daily
    recompute_touched_hours, backfill_rollups
    cleanup_old_rollups
    recompute_wellknown_bots_rollup, read_wellknown_bots_rollup

  Test-touched / cross-package internals:
    _is_safe_ident, _safe_table_for, _VIRTUAL_FIELD_BACKING
    _rollups_root, _hour_bundled_root, _day_bundled_root
    _build_copy_query, _build_virtual_field_copy_query
    _publish_field_partitions, _get_fields
    TOP_K, DAY_BUNDLE_FILENAME, DAY_BUNDLE_TOP_K,
    TIME_SERIES_BUNDLE_FILENAME, SESSIONS_BUNDLE_FILENAME
    _run_per_field_copy, _cleanup_per_field_after_bundle
"""

from __future__ import annotations

# Re-exports — pull every public symbol up so callers continue to use
# the flat ``backend.core.rollups.X`` path. Order doesn't matter; no
# side-effect imports here.
from ._common import (
    _VIRTUAL_FIELD_BACKING,
    DAY_BUNDLE_FILENAME,
    DAY_BUNDLE_TOP_K,
    NETWORK_SUMMARY_BUNDLE_FILENAME,
    NGWAF_BOTS_BUNDLE_FILENAME,
    NGWAF_BOTS_TS_BUNDLE_FILENAME,
    ORIGIN_IP_BUNDLE_FILENAME,
    ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
    ORIGIN_PATH_BUNDLE_FILENAME,
    ORIGIN_POP_BUNDLE_FILENAME,
    ORIGIN_SUMMARY_BUNDLE_FILENAME,
    OVERVIEW_BUNDLE_FILENAME,
    PERF_TOP_ASNS_BUNDLE_FILENAME,
    PERF_TOP_URLS_BUNDLE_FILENAME,
    PERF_TTL_DIST_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_UA_TOP_K,
    SECURITY_CONN_REUSE_BUNDLE_FILENAME,
    SECURITY_COV_BUNDLE_FILENAME,
    SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
    SECURITY_PROXY_QUALITY_BUNDLE_FILENAME,
    SECURITY_REQ_SIZE_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_FILENAME,
    SESSIONS_BUNDLE_FILENAME,
    SLOW_URLS_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    TOP_K,
    VERIFIED_BOTS_TS_BUNDLE_FILENAME,
    _build_copy_query,
    _build_virtual_field_copy_query,
    _day_bundled_root,
    _get_fields,
    _hour_bundled_root,
    _is_safe_ident,
    _publish_field_partitions,
    _rollups_root,
    _safe_table_for,
    rollups_present,
)
from .day_bundles import (
    backfill_day_bundles,
    bundle_days,
    compact_closed_days_to_daily,
    compact_network_quality_closed_days_to_daily,
    compact_network_rtt_closed_days_to_daily,
    compact_network_speed_closed_days_to_daily,
    compact_ngwaf_bots_closed_days_to_daily,
    compact_ngwaf_bots_ts_closed_days_to_daily,
    compact_origin_dims_closed_days_to_daily,
    compact_origin_latency_ts_closed_days_to_daily,
    compact_origin_summary_closed_days_to_daily,
    compact_overview_closed_days_to_daily,
    compact_perf_dims_closed_days_to_daily,
    compact_perf_latency_closed_days_to_daily,
    compact_pop_health_closed_days_to_daily,
    compact_security_dims_closed_days_to_daily,
    compact_security_proxies_closed_days_to_daily,
    compact_time_series_closed_days_to_daily,
    compact_verified_bots_ts_closed_days_to_daily,
    retire_compacted_hour_bundles,
)
from .hour_bundles import (
    _cleanup_per_field_after_bundle,
    bundle_hours,
)
from .network_health_geo import backfill_network_geo_bundles, build_network_geo_bundles
from .network_health_heatmap import backfill_network_heatmap_bundles, build_network_heatmap_bundles
from .network_quality import backfill_network_quality_bundles, build_network_quality_bundles
from .network_rtt import backfill_network_rtt_bundles, build_network_rtt_bundles
from .network_speed import backfill_network_speed_bundles, build_network_speed_bundles
from .network_summary import backfill_network_summary_bundles, build_network_summary_bundles
from .ngwaf_bots import (
    backfill_ngwaf_bots_bundles,
    backfill_ngwaf_bots_ts_bundles,
    build_ngwaf_bots_bundles,
    build_ngwaf_bots_ts_bundles,
)
from .origin_dims import backfill_origin_dims_bundles, build_origin_dims_bundles
from .origin_latency_ts import backfill_origin_latency_ts_bundles, build_origin_latency_ts_bundles
from .origin_summary import backfill_origin_summary_bundles, build_origin_summary_bundles
from .overview import backfill_overview_bundles, build_overview_bundles
from .perf_dims import backfill_perf_dims_bundles, build_perf_dims_bundles
from .perf_latency import backfill_perf_latency_bundles, build_perf_latency_bundles
from .pop_health import backfill_pop_health_bundles, build_pop_health_bundles
from .recompute import (
    _run_per_field_copy,
    backfill_missing_hour_bundles,
    backfill_missing_hour_ip_spread,
    cleanup_old_rollups,
    recompute_touched_hours,
)
from .rum import backfill_missing_rum_aggregates, recompute_rum_aggregates
from .security_dims import backfill_security_dims_bundles, build_security_dims_bundles
from .security_proxies import backfill_security_proxies_bundles, build_security_proxies_bundles
from .sessions import build_session_bundles
from .slow_urls import backfill_slow_urls_bundles, build_slow_urls_bundles
from .time_series import build_time_series_bundles
from .verified_bots_ts import backfill_verified_bots_ts_bundles, build_verified_bots_ts_bundles
from .wellknown_bots import (
    backfill_wellknown_bots_rollup,
    read_wellknown_bots_rollup,
    recompute_wellknown_bots_rollup,
)

__all__ = [
    # Public writers
    "build_time_series_bundles",
    "build_session_bundles",
    "build_slow_urls_bundles",
    "backfill_slow_urls_bundles",
    "build_origin_summary_bundles",
    "backfill_origin_summary_bundles",
    "build_overview_bundles",
    "backfill_overview_bundles",
    "build_origin_dims_bundles",
    "backfill_origin_dims_bundles",
    "build_origin_latency_ts_bundles",
    "backfill_origin_latency_ts_bundles",
    "build_network_rtt_bundles",
    "backfill_network_rtt_bundles",
    "build_network_heatmap_bundles",
    "backfill_network_heatmap_bundles",
    "build_network_geo_bundles",
    "backfill_network_geo_bundles",
    "build_network_summary_bundles",
    "backfill_network_summary_bundles",
    "build_network_speed_bundles",
    "build_network_quality_bundles",
    "backfill_network_quality_bundles",
    "backfill_network_speed_bundles",
    "build_ngwaf_bots_bundles",
    "backfill_ngwaf_bots_bundles",
    "build_ngwaf_bots_ts_bundles",
    "backfill_ngwaf_bots_ts_bundles",
    "build_verified_bots_ts_bundles",
    "backfill_verified_bots_ts_bundles",
    "build_perf_latency_bundles",
    "backfill_perf_latency_bundles",
    "build_pop_health_bundles",
    "backfill_pop_health_bundles",
    "build_perf_dims_bundles",
    "backfill_perf_dims_bundles",
    "build_security_dims_bundles",
    "backfill_security_dims_bundles",
    "build_security_proxies_bundles",
    "backfill_security_proxies_bundles",
    "bundle_hours",
    "bundle_days",
    "backfill_day_bundles",
    "compact_closed_days_to_daily",
    "compact_overview_closed_days_to_daily",
    "compact_origin_summary_closed_days_to_daily",
    "compact_origin_dims_closed_days_to_daily",
    "compact_origin_latency_ts_closed_days_to_daily",
    "compact_network_rtt_closed_days_to_daily",
    "compact_network_quality_closed_days_to_daily",
    "compact_network_speed_closed_days_to_daily",
    "compact_ngwaf_bots_closed_days_to_daily",
    "compact_ngwaf_bots_ts_closed_days_to_daily",
    "compact_verified_bots_ts_closed_days_to_daily",
    "compact_perf_latency_closed_days_to_daily",
    "compact_pop_health_closed_days_to_daily",
    "compact_perf_dims_closed_days_to_daily",
    "compact_security_dims_closed_days_to_daily",
    "compact_security_proxies_closed_days_to_daily",
    "compact_time_series_closed_days_to_daily",
    "retire_compacted_hour_bundles",
    "recompute_touched_hours",
    "backfill_missing_hour_bundles",
    "backfill_missing_hour_ip_spread",
    "backfill_missing_rum_aggregates",
    "recompute_rum_aggregates",
    "cleanup_old_rollups",
    "recompute_wellknown_bots_rollup",
    "backfill_wellknown_bots_rollup",
    "read_wellknown_bots_rollup",
    # Module-level constants
    "TOP_K",
    "DAY_BUNDLE_FILENAME",
    "DAY_BUNDLE_TOP_K",
    "TIME_SERIES_BUNDLE_FILENAME",
    "SESSIONS_BUNDLE_FILENAME",
    "SLOW_URLS_BUNDLE_FILENAME",
    "NETWORK_SUMMARY_BUNDLE_FILENAME",
    "OVERVIEW_BUNDLE_FILENAME",
    "ORIGIN_SUMMARY_BUNDLE_FILENAME",
    "ORIGIN_POP_BUNDLE_FILENAME",
    "ORIGIN_IP_BUNDLE_FILENAME",
    "ORIGIN_PATH_BUNDLE_FILENAME",
    "ORIGIN_LATENCY_TS_BUNDLE_FILENAME",
    "VERIFIED_BOTS_TS_BUNDLE_FILENAME",
    "PERF_TOP_URLS_BUNDLE_FILENAME",
    "PERF_TOP_ASNS_BUNDLE_FILENAME",
    "PERF_TTL_DIST_BUNDLE_FILENAME",
    "SECURITY_REQ_SIZE_BUNDLE_FILENAME",
    "NGWAF_BOTS_BUNDLE_FILENAME",
    "NGWAF_BOTS_TS_BUNDLE_FILENAME",
    "SECURITY_CONN_REUSE_BUNDLE_FILENAME",
    "SECURITY_TOPIPS_BUNDLE_FILENAME",
    "SECURITY_COV_BUNDLE_FILENAME",
    "SECURITY_PROXY_QUALITY_BUNDLE_FILENAME",
    "SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME",
    "SECURITY_BOT_EFF_BUNDLE_FILENAME",
    "SECURITY_BOT_EFF_UA_TOP_K",
    # Cross-package + test-touched internals
    "_is_safe_ident",
    "_safe_table_for",
    "_VIRTUAL_FIELD_BACKING",
    "_rollups_root",
    "_hour_bundled_root",
    "_day_bundled_root",
    "rollups_present",
    "_build_copy_query",
    "_build_virtual_field_copy_query",
    "_publish_field_partitions",
    "_get_fields",
    "_run_per_field_copy",
    "_cleanup_per_field_after_bundle",
]
