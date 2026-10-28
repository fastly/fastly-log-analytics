from __future__ import annotations

import logging
import sys
import time as _time

from backend import config as svcconfig
from backend.core.duckdb import escape_sql_literal
from backend.core.log_fields import coalesce
from backend.models.analytics import ReaderCoverage
from backend.models.common import FiltersDict
from backend.repositories import dashboard as _dashboard_repo
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _drop_temp_table,
    _safe_table,
    empty_schema_response,
    ensure_ngwaf_bots_materialized,
    safe_iso,
    time_bucket_select,
)
from backend.repositories.security import aggregates as _aggregates_mod
from backend.repositories.security import sql as _sql_mod
from backend.repositories.security import watchdog as _watchdog_mod
from backend.repositories.security.aggregates import (
    _SECTION_DEFAULTS,
    _SECURITY_CACHE_TTL,
    _TEMP_COL_ORDER,
    _build_security_response,
    _memo,
    _response_cache,
    _security_memo_key,
    get_security_aggregates,
    peek_security_aggregates,
)
from backend.repositories.security.sql import (
    BOT_EFFICIENCY,
    CONN_REUSE_DIST,
    FINGERPRINT_COVERAGE_BULK,
    FINGERPRINT_TOP_N,
    GET_SECURITY_PROXIES_COMBINED,
    IPV6_ADOPTION_TS,
    NGWAF_TOP_BOTS_JOIN,
    NGWAF_TOP_BOTS_JOIN_DIRECT,
    NGWAF_VERIFIED_BOTS,
    NGWAF_VERIFIED_BOTS_TS,
    PROXY_TYPE_DIST,
    REQ_HEADER_SIZE_DIST,
    TOP_IPS_BY_MAX_HEADER,
    TOP_UAS_BY_COUNT,
    VERIFIED_BOTS_TS,
    WELLKNOWN_BOTS_UA_IP,
    _DynamicPopSqlTemplate,
    threat_intel_sql,
)
from backend.repositories.security.watchdog import (
    _BOT_UA_CLASSIFY_LIMIT,
    _GENERIC_BOT_RE,
    _RDNS_ENQUEUE_LIMIT,
    _RDNS_LOOKUP_LIMIT,
    _ROLLUP_MIN_WINDOW_SECONDS,
    _empty_proxies_payload,
    _finish_proxies,
    _has_rollup_coverage,
    _ipv6_per_hour_from_rollups,
    _parse_asn_int,
    _proxy_dist_from_rollups,
    _stems_match,
    _top_bots_cache_key,
    _window_eligible_for_rollup,
    classify_ua_counts,
    get_security_proxies,
    get_threat_intel_rows,
    get_top_bots,
    peek_top_bots,
    summarize_bot_efficiency_rows,
    summarize_wellknown_bot_rows,
)
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    cache_result,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    SECURITY_SECTION_DEFAULTS,
    empty_security_aggregates,
    empty_security_proxies,
    finish_security_proxies,
    parse_asn_int,
    shape_security_fingerprint_row,
    shape_security_ngwaf_bot_row,
    shape_security_ngwaf_top_bot_row,
    shape_security_top_ip_header_row,
)
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.date_utils import parse_window
from backend.utils.module_shim import install_shim_module

_logger = logging.getLogger(__name__)
SQL = sys.modules[__name__]
sys.modules.setdefault("backend.repositories._sql.security", sys.modules[__name__])

__all__ = [
    "BOT_EFFICIENCY",
    "BoundedTTLCache",
    "CONN_REUSE_DIST",
    "CacheRegistry",
    "FINGERPRINT_COVERAGE_BULK",
    "FINGERPRINT_TOP_N",
    "FiltersDict",
    "GET_SECURITY_PROXIES_COMBINED",
    "IPV6_ADOPTION_TS",
    "NGWAF_TOP_BOTS_JOIN",
    "NGWAF_TOP_BOTS_JOIN_DIRECT",
    "NGWAF_VERIFIED_BOTS",
    "NGWAF_VERIFIED_BOTS_TS",
    "PROXY_TYPE_DIST",
    "QueryRunner",
    "REQ_HEADER_SIZE_DIST",
    "ReaderCoverage",
    "ResponseMemo",
    "SECURITY_SECTION_DEFAULTS",
    "SQL",
    "SectionTimer",
    "TOP_IPS_BY_MAX_HEADER",
    "TOP_UAS_BY_COUNT",
    "VERIFIED_BOTS_TS",
    "WELLKNOWN_BOTS_UA_IP",
    "_BOT_UA_CLASSIFY_LIMIT",
    "_DynamicPopSqlTemplate",
    "_GENERIC_BOT_RE",
    "_RDNS_ENQUEUE_LIMIT",
    "_RDNS_LOOKUP_LIMIT",
    "_ROLLUP_MIN_WINDOW_SECONDS",
    "_SECTION_DEFAULTS",
    "_SECURITY_CACHE_TTL",
    "_TEMP_COL_ORDER",
    "_build_security_response",
    "_dashboard_repo",
    "_drop_temp_table",
    "_empty_proxies_payload",
    "_finish_proxies",
    "_has_rollup_coverage",
    "_ipv6_per_hour_from_rollups",
    "_logger",
    "_memo",
    "_parse_asn_int",
    "_proxy_dist_from_rollups",
    "_response_cache",
    "_safe_table",
    "_security_memo_key",
    "_stems_match",
    "_time",
    "_top_bots_cache_key",
    "_window_eligible_for_rollup",
    "bucket_time_to_minute",
    "build_where_clause",
    "cache_result",
    "classify_ua_counts",
    "coalesce",
    "digest_cache_key",
    "empty_schema_response",
    "empty_security_aggregates",
    "empty_security_proxies",
    "ensure_ngwaf_bots_materialized",
    "escape_sql_literal",
    "finish_security_proxies",
    "get_security_aggregates",
    "get_security_proxies",
    "get_threat_intel_rows",
    "get_top_bots",
    "parse_asn_int",
    "parse_window",
    "peek_security_aggregates",
    "peek_top_bots",
    "safe_iso",
    "serialize_filters_for_key",
    "shape_security_fingerprint_row",
    "shape_security_ngwaf_bot_row",
    "shape_security_ngwaf_top_bot_row",
    "shape_security_top_ip_header_row",
    "summarize_bot_efficiency_rows",
    "summarize_wellknown_bot_rows",
    "svcconfig",
    "threat_intel_sql",
    "time_bucket_select",
]

_self = sys.modules[__name__]
_SUBMODULES = (_sql_mod, _watchdog_mod, _aggregates_mod)
_SYNC_MODULES = (_self, *_SUBMODULES)
_ShimModule = install_shim_module(__name__, _SUBMODULES, seed_submodules_from_root=True)
