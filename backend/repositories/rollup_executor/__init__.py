"""Rollup query execution mixins for QueryRunner (Top-N, time-series, and scalar rollups)."""
# ruff: noqa: F401

from __future__ import annotations  # noqa: I001

import logging
import os
import sys as _sys
import time
import types as _types
import uuid as _uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

import duckdb

from backend.core import log_fields as metric_expressions
from backend.core.duckdb import escape_sql_literal
from backend.core.log_fields import Group, in_group
from backend.core.rollups import recompute as rollup_coverage
from backend.core.rollups._common import (
    DAY_BUNDLE_FILENAME,
    NETWORK_GEO_BUNDLE_FILENAME,
    NETWORK_HEATMAP_BUNDLE_FILENAME,
    NETWORK_QUALITY_ASN_FILENAME,
    NETWORK_QUALITY_COUNTRY_FILENAME,
    NETWORK_QUALITY_POP_FILENAME,
    NETWORK_QUALITY_REGION_FILENAME,
    NETWORK_RTT_BUNDLE_FILENAME,
    NETWORK_SPEED_BUNDLE_FILENAME,
    NGWAF_BOTS_BUNDLE_FILENAME,
    NGWAF_BOTS_TS_BUNDLE_FILENAME,
    ORIGIN_IP_BUNDLE_FILENAME,
    ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
    ORIGIN_PATH_BUNDLE_FILENAME,
    ORIGIN_POP_BUNDLE_FILENAME,
    ORIGIN_SUMMARY_BUNDLE_FILENAME,
    PERF_TOP_ASNS_BUNDLE_FILENAME,
    PERF_TOP_URLS_BUNDLE_FILENAME,
    PERF_TTL_DIST_BUNDLE_FILENAME,
    POP_HEALTH_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_UA_TOP_K,
    SECURITY_CONN_REUSE_BUNDLE_FILENAME,
    SECURITY_COV_BUNDLE_FILENAME,
    SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
    SECURITY_PROXY_QUALITY_BUNDLE_FILENAME,
    SECURITY_REQ_SIZE_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_FILENAME,
    SLOW_URLS_BUNDLE_FILENAME,
    VERIFIED_BOTS_TS_BUNDLE_FILENAME,
    _VIRTUAL_FIELD_BACKING,
    _build_virtual_field_copy_query,
    _hour_bundled_root,
    _ip_spread_root,
    _is_safe_ident,
    quote_path_list,
)
from backend.core.rollups.recompute import (
    MISSING_HOUR_HEAL_CAP as _MISSING_HOUR_HEAL_CAP,
    collect_day_prefer_paths,
    collect_hour_paths_with_floor,
    durable_serving_blocked,
    eligible_window,
    heal_where_clause as _heal_where_clause,
    plan_hourly_read,
    plan_time_series_rollup_window,
)
from backend.models.analytics import (
    ReaderCoverage,
    RollupPathCoverage,
    ScalarRollupCoverage,
    TopNRollupCoverage,
)
from backend.repositories.top_n_rollups import (  # noqa: F401
    _EDGE_PREFIXES,
    IpSpreadPlan,
    SchemaSeeds,
    TopNPaths,
    TopNPlan,
    TopNRow,
    TopNWindow,
    _bundled_day_files,
    _bundled_hour_files,
    _day_covered_by_any_field,
    _day_has_real_rollup,
    _has_real_parquet,
    _per_field_files,
    build_ip_spread_sql,
    build_rolled_sql,
    find_unrolled_hours,
    fold_rows,
    fold_sketches,
    live_topn_fields,
    plan_ip_spread_paths,
    plan_top_n_paths,
    qualify_limit_expr,
    spread_meta,
    truncate_top_n,
)
from backend.repositories.utils.response_shaping import us_to_ms as _us_to_ms
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_day_key_strict, parse_hour_key_strict, parse_iso_utc, safe_iso  # noqa: F401
from backend.utils.module_shim import install_shim_module
from backend.utils.telemetry import log_once_per

from . import scalars as _scalars_mod
from . import time_series as _time_series_mod
from . import top_n as _top_n_mod
from .scalars import _ScalarsRollupMixin
from .time_series import _TimeSeriesRollupMixin
from .top_n import (
    _EMPTY_ROLLUP_WARN_INTERVAL_S,
    _EMPTY_ROLLUP_WARN_TS,
    _LIVE_TOPN_SKIP_FIELDS,
    _MISSING_HOUR_HEAL_WARN_TS,
    _MTIME_SKEW_MARGIN_S,
    CANONICAL_METRICS,
    _active_hour_overlap,
    _build_ip_spread_sql,
    _build_rolled_sql,
    _drop_temp_table,
    _find_unrolled_hours,
    _fold_rows,
    _fold_sketches,
    _get_schema,
    _live_topn_fields,
    _logger,
    _plan_ip_spread_paths,
    _plan_top_n_paths,
    _quote_ident,
    _SchemaSeeds,
    _should_emit_throttled_warn,
    _spread_meta,
    _TopNRollupMixin,
    _TopNWindow,
    _truncate_top_n,
)


class RollupExecutorMixin(_TopNRollupMixin, _TimeSeriesRollupMixin, _ScalarsRollupMixin):
    """Combined rollup query execution mixin for QueryRunner."""


_SUBMODULES = (
    _top_n_mod,
    _time_series_mod,
    _scalars_mod,
)

_ShimModule = install_shim_module(__name__, _SUBMODULES, only_existing_attrs=True)
