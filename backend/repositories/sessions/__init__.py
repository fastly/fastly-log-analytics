"""Sessions repository — pure SQL functions, no HTTP imports."""

from __future__ import annotations

import logging
import os
import sys as _sys
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
from fastapi import HTTPException

from backend.core.duckdb import escape_sql_literal
from backend.core.log_fields import sum_or_zero
from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, SectionTimer, _safe_table
from backend.repositories.sessions import (
    queries as _queries,
)
from backend.repositories.sessions import (
    rollup_reader as _rollup_reader,
)
from backend.repositories.sessions import (
    scoring as _scoring,
)
from backend.repositories.sessions.queries import (
    SESSIONS_COUNT_WRAPPER,
    SESSIONS_CTE_PIPELINE,
    SESSIONS_PAGE_SELECT,
    _memo,
    _response_cache,
    get_session_detail,
    get_sessions,
)
from backend.repositories.sessions.rollup_reader import (
    _SESSION_ACTIVE_HOUR_COLS,
    _build_active_hour_session_sql,
    _build_asn_label_map,
    _build_direct_active_hour_session_sql,
    _build_rollup_filter_sql,
    _collect_sessions_rollup_paths,
    _get_sessions_from_rollup,
    _parse_rollup_hour_dt,
    _schema_cap_flags,
    _sessions_envelope,
    _shape_session_rows,
)
from backend.repositories.sessions.scoring import (
    _SCORING_COLUMN_TYPES,
    COMPLIANCE_BREAKDOWN,
    HEALTH_SNAPSHOT,
    LATENCY_TIMESERIES,
    SCORE_DISTRIBUTION,
    THRESHOLD_PREVIEW_LABELED,
    THRESHOLD_PREVIEW_UNLABELED,
    TOP_FLAGGED,
    _scoring_table_columns,
    compliance_breakdown_rows,
    describe_table,
    fetch_session_events,
    health_snapshot_row,
    health_snapshot_sql,
    latency_timeseries_rows,
    latency_timeseries_sql,
    logger,
    query_logs,
    reconstruct_labeled_sessions,
    score_distribution_rows,
    scoring_source,
    threshold_preview_counts,
    top_flagged_rows,
)
from backend.repositories.utils.filters import build_where_clause, calc_offset
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG,
    SESSIONS_DEFAULT_MIN_REQS_FLAG,
    SESSIONS_SORTABLE_COLUMNS,
    build_asn_label_map,
    empty_sessions_response,
    shape_sessions_envelope,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.module_shim import install_shim_module

SQL: Any = _sys.modules[__name__]
sql_scoring: Any = _sys.modules[__name__]

_sys.modules.setdefault("backend.repositories._sql.sessions", _sys.modules[__name__])
_sys.modules.setdefault("backend.repositories._sql.session_scoring", _sys.modules[__name__])

install_shim_module(
    __name__,
    (_rollup_reader, _queries, _scoring),
    seed_submodules_from_root=True,
)

__all__ = [
    "COMPLIANCE_BREAKDOWN",
    "FiltersDict",
    "HEALTH_SNAPSHOT",
    "HTTPException",
    "LATENCY_TIMESERIES",
    "QueryRunner",
    "ResponseMemo",
    "SCORE_DISTRIBUTION",
    "SESSIONS_COUNT_WRAPPER",
    "SESSIONS_CTE_PIPELINE",
    "SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG",
    "SESSIONS_DEFAULT_MIN_REQS_FLAG",
    "SESSIONS_PAGE_SELECT",
    "SESSIONS_SORTABLE_COLUMNS",
    "SQL",
    "SectionTimer",
    "THRESHOLD_PREVIEW_LABELED",
    "THRESHOLD_PREVIEW_UNLABELED",
    "TOP_FLAGGED",
    "UTC",
    "_CacheRegistry",
    "_SCORING_COLUMN_TYPES",
    "_SESSION_ACTIVE_HOUR_COLS",
    "_build_active_hour_session_sql",
    "_build_asn_label_map",
    "_build_direct_active_hour_session_sql",
    "_build_rollup_filter_sql",
    "_collect_sessions_rollup_paths",
    "_get_sessions_from_rollup",
    "_memo",
    "_parse_rollup_hour_dt",
    "_response_cache",
    "_safe_table",
    "_schema_cap_flags",
    "_scoring_table_columns",
    "_sessions_envelope",
    "_shape_session_rows",
    "bucket_time_to_minute",
    "build_asn_label_map",
    "build_where_clause",
    "calc_offset",
    "compliance_breakdown_rows",
    "datetime",
    "describe_table",
    "digest_cache_key",
    "duckdb",
    "empty_sessions_response",
    "escape_sql_literal",
    "fetch_session_events",
    "get_session_detail",
    "get_sessions",
    "health_snapshot_row",
    "health_snapshot_sql",
    "latency_timeseries_rows",
    "latency_timeseries_sql",
    "logger",
    "logging",
    "os",
    "query_logs",
    "reconstruct_labeled_sessions",
    "score_distribution_rows",
    "scoring_source",
    "serialize_filters_for_key",
    "shape_sessions_envelope",
    "sql_scoring",
    "sum_or_zero",
    "threshold_preview_counts",
    "time",
    "timedelta",
    "top_flagged_rows",
]
