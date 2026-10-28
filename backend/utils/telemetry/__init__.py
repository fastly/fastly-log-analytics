from __future__ import annotations

import asyncio as asyncio
import contextvars as contextvars
import importlib as _importlib
import itertools as itertools
import json as json
import logging as logging
import os as os
import queue as queue
import re as re
import sys as sys
import tempfile as tempfile
import threading as threading
import time as time
from collections import defaultdict as defaultdict
from collections import deque as deque
from collections.abc import AsyncIterator as AsyncIterator
from collections.abc import Callable as Callable
from collections.abc import Iterator as Iterator
from collections.abc import MutableMapping as MutableMapping
from contextlib import contextmanager as contextmanager
from contextlib import suppress as suppress
from contextvars import ContextVar as ContextVar
from datetime import UTC as UTC
from datetime import datetime as datetime
from pathlib import Path as Path
from types import FrameType as FrameType
from typing import Any as Any

import aiohttp as aiohttp
import structlog as structlog
import yarl as yarl
from aiohttp import web as web
from botocore.auth import S3SigV4Auth as S3SigV4Auth
from botocore.awsrequest import AWSRequest as AWSRequest
from botocore.credentials import Credentials as Credentials
from opentelemetry import trace as trace
from starlette.middleware.base import BaseHTTPMiddleware as BaseHTTPMiddleware
from starlette.requests import Request as Request
from starlette.responses import Response as Response

from backend.core.retry import retry_call as retry_call
from backend.utils.bounded_cache import (
    _MISSING as _MISSING,
)
from backend.utils.bounded_cache import (
    _PREV_CACHE_ENTRIES as _PREV_CACHE_ENTRIES,
)
from backend.utils.bounded_cache import (
    BoundedCache as BoundedCache,
)
from backend.utils.bounded_cache import (
    BoundedTTLCache as BoundedTTLCache,
)
from backend.utils.bounded_cache import (
    CacheRegistry as CacheRegistry,
)
from backend.utils.bounded_cache import (
    _try_clear_cache as _try_clear_cache,
)
from backend.utils.date_utils import (
    _parse_dt as _parse_dt,
)
from backend.utils.date_utils import (
    ensure_utc as ensure_utc,
)
from backend.utils.date_utils import (
    iso_z as iso_z,
)
from backend.utils.date_utils import (
    iso_z_now as iso_z_now,
)
from backend.utils.date_utils import (
    parse_date_window as parse_date_window,
)
from backend.utils.date_utils import (
    parse_day_key_strict as parse_day_key_strict,
)
from backend.utils.date_utils import (
    parse_hour_key as parse_hour_key,
)
from backend.utils.date_utils import (
    parse_hour_key_strict as parse_hour_key_strict,
)
from backend.utils.date_utils import (
    parse_iso_utc as parse_iso_utc,
)
from backend.utils.date_utils import (
    parse_relative_time_window as parse_relative_time_window,
)
from backend.utils.date_utils import (
    parse_window as parse_window,
)
from backend.utils.date_utils import (
    parse_window_str_to_dt as parse_window_str_to_dt,
)
from backend.utils.date_utils import (
    safe_iso as safe_iso,
)
from backend.utils.date_utils import (
    window_to_epoch as window_to_epoch,
)
from backend.utils.hll import (
    _HASH_BITS as _HASH_BITS,
)
from backend.utils.hll import (
    DEFAULT_PRECISION as DEFAULT_PRECISION,
)
from backend.utils.hll import (
    HLL_SKETCH_NAMES as HLL_SKETCH_NAMES,
)
from backend.utils.hll import (
    SERIALIZATION_VERSION as SERIALIZATION_VERSION,
)
from backend.utils.hll import (
    HyperLogLog as HyperLogLog,
)
from backend.utils.hll import (
    _alpha as _alpha,
)
from backend.utils.hll import (
    _hash64 as _hash64,
)
from backend.utils.hll import (
    _leading_zero_count as _leading_zero_count,
)
from backend.utils.hll import (
    get_hll_sketch as get_hll_sketch,
)
from backend.utils.hll import (
    merge_hll_sketches as merge_hll_sketches,
)
from backend.utils.hll import (
    query_hll_distinct as query_hll_distinct,
)
from backend.utils.hll import (
    query_hll_distinct_by_bucket as query_hll_distinct_by_bucket,
)
from backend.utils.hll import (
    query_hll_distinct_filtered as query_hll_distinct_filtered,
)
from backend.utils.http import (
    _DEFAULT_UA as _DEFAULT_UA,
)
from backend.utils.http import (
    http_get_text as http_get_text,
)
from backend.utils.http import (
    make_client as make_client,
)
from backend.utils.module_shim import install_shim_module
from backend.utils.telemetry import (
    log_flusher as _log_flusher,
)
from backend.utils.telemetry import (
    logging_config as _logging_config,
)
from backend.utils.telemetry import (
    metrics as _metrics,
)
from backend.utils.telemetry import (
    proxy_server as _proxy_server,
)
from backend.utils.telemetry import (
    system_stats as _system_stats,
)

if getattr(sys.modules.get(__name__), "_SHIM_INITIALIZED", False):
    for _m in (sys.modules[__name__], _logging_config, _system_stats, _log_flusher, _proxy_server, _metrics):
        _m.__dict__.pop("__shim_rules__", None)
    _importlib.reload(_logging_config)
    _importlib.reload(_system_stats)
    _importlib.reload(_log_flusher)
    _importlib.reload(_proxy_server)
    _importlib.reload(_metrics)

from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _IN_FLIGHT_LOCK as _IN_FLIGHT_LOCK,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _IN_FLIGHT_REQUESTS as _IN_FLIGHT_REQUESTS,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _LOG_BATCH_MAX_INTERVAL_S as _LOG_BATCH_MAX_INTERVAL_S,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _LOG_BATCH_MAX_ROWS as _LOG_BATCH_MAX_ROWS,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _LOG_FLUSHER_LOCK as _LOG_FLUSHER_LOCK,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _LOG_FLUSHER_STOP as _LOG_FLUSHER_STOP,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _LOG_FLUSHER_THREAD as _LOG_FLUSHER_THREAD,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _LOG_QUEUE as _LOG_QUEUE,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    PUBLISH_STATS as PUBLISH_STATS,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    ValkeyPublisher as ValkeyPublisher,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _ensure_log_flusher_running as _ensure_log_flusher_running,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _flush_batch as _flush_batch,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _flush_log_writes_for_tests as _flush_log_writes_for_tests,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _FlushBarrier as _FlushBarrier,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _get_sync_redis as _get_sync_redis,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _log_flusher_loop as _log_flusher_loop,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _submit_log_write as _submit_log_write,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _sync_redis as _sync_redis,
)
from backend.utils.telemetry.log_flusher import (  # noqa: E402
    _sync_redis_lock as _sync_redis_lock,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _LOG_EVERY_S as _LOG_EVERY_S,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _MAX_KEYS as _MAX_KEYS,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _MIN_SALT_LEN as _MIN_SALT_LEN,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _UVICORN_LOGGERS as _UVICORN_LOGGERS,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    ABS_PATH_RE as ABS_PATH_RE,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    CID_SALT_LITERAL_RE as CID_SALT_LITERAL_RE,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    REDACTED as REDACTED,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _add_otel_trace_context as _add_otel_trace_context,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _drop_uvicorn_color_message as _drop_uvicorn_color_message,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _known_salts as _known_salts,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _last as _last,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _log_once_lock as _log_once_lock,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _log_rate_limited as _log_rate_limited,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    _salts_lock as _salts_lock,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    audit_log as audit_log,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    bridge_uvicorn_loggers as bridge_uvicorn_loggers,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    configure_structlog as configure_structlog,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    effective_format as effective_format,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    log_once_per as log_once_per,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    redact_error_text as redact_error_text,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    register_salt as register_salt,
)
from backend.utils.telemetry.logging_config import (  # noqa: E402
    scrub_secrets as scrub_secrets,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _ACTIVE_CONTEXTS as _ACTIVE_CONTEXTS,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _BUFFER_CAP as _BUFFER_CAP,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _CALLS as _CALLS,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _IOTHREAD_QUERIED as _IOTHREAD_QUERIED,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _JSON_CONTENT_TYPES as _JSON_CONTENT_TYPES,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _LATEST_PROCESS_CONTEXT as _LATEST_PROCESS_CONTEXT,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _LATEST_PROCESS_CONTEXT_LOCK as _LATEST_PROCESS_CONTEXT_LOCK,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _PAGE_LOAD_ID as _PAGE_LOAD_ID,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _PROCESS_CONTEXT as _PROCESS_CONTEXT,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _QUERIES as _QUERIES,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _RECORDING_TELEMETRY as _RECORDING_TELEMETRY,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _REQUEST_START_TS as _REQUEST_START_TS,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _SQL_TRUNCATE as _SQL_TRUNCATE,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _SQLITE_QUERIES as _SQLITE_QUERIES,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _STREAMING_CONTENT_TYPES as _STREAMING_CONTENT_TYPES,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    TelemetryResponseBodyMiddleware as TelemetryResponseBodyMiddleware,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _add_span_event as _add_span_event,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _buffer as _buffer,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _content_type as _content_type,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _describe_params as _describe_params,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _dropped as _dropped,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _get_ctx_list as _get_ctx_list,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _is_json_response as _is_json_response,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _is_streaming_content_type as _is_streaming_content_type,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _live_deregister as _live_deregister,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _live_register as _live_register,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _load_service_source as _load_service_source,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _os as _os,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _query_iothread_calls_from_usage_log as _query_iothread_calls_from_usage_log,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _reconstruct as _reconstruct,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _record as _record,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _seq as _seq,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _set_process_context_for_tests as _set_process_context_for_tests,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    _summarize_sql as _summarize_sql,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    buffer_stats as buffer_stats,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    build_cdn_miss_synth_details as build_cdn_miss_synth_details,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    clear as clear,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    clear_queries as clear_queries,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    clear_recent_sqlite_queries as clear_recent_sqlite_queries,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    debug_envelope as debug_envelope,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    flush_usage_log as flush_usage_log,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_page_load_id as get_page_load_id,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_page_telemetry as get_page_telemetry,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_process_context as get_process_context,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_process_context_with_fallback as get_process_context_with_fallback,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_queries as get_queries,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_recent as get_recent,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_recent_sqlite_queries as get_recent_sqlite_queries,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_sqlite_queries as get_sqlite_queries,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_sqlite_stats as get_sqlite_stats,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    get_tracked_calls as get_tracked_calls,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    is_full_miss as is_full_miss,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    logger as logger,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    process_context_scope as process_context_scope,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    record_call as record_call,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    record_cdn_call as record_cdn_call,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    record_sqlite_query as record_sqlite_query,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    record_telemetry_query as record_telemetry_query,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    run_usage_log_cleanup as run_usage_log_cleanup,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    set_page_load_id as set_page_load_id,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    start_call_tracking as start_call_tracking,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    track_query as track_query,
)
from backend.utils.telemetry.metrics import (  # noqa: E402
    tracked_call as tracked_call,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _BOTO3_CALLER_HINT as _BOTO3_CALLER_HINT,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _HOP_BY_HOP as _HOP_BY_HOP,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _LOOP as _LOOP,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _LOOPBACK_HOSTS as _LOOPBACK_HOSTS,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _POOL_PER_HOST as _POOL_PER_HOST,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _PORT as _PORT,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _READY as _READY,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _RETRYABLE_CONNECTION_ERRORS as _RETRYABLE_CONNECTION_ERRORS,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _RUNNER as _RUNNER,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _SERVER_THREAD as _SERVER_THREAD,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _SESSION as _SESSION,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _START_LOCK as _START_LOCK,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _TELEMETRY_HEADERS as _TELEMETRY_HEADERS,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _UPSTREAM_KEEPALIVE_S as _UPSTREAM_KEEPALIVE_S,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _UPSTREAM_TIMEOUT as _UPSTREAM_TIMEOUT,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _UPSTREAM_TIMEOUT_TOTAL_S as _UPSTREAM_TIMEOUT_TOTAL_S,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _build_details as _build_details,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _build_upstream_headers as _build_upstream_headers,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _bust_config_cache as _bust_config_cache,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _cdn_host_for as _cdn_host_for,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _create_session as _create_session,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _handle_request_inner as _handle_request_inner,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _is_target_host_allowed as _is_target_host_allowed,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _load_config_cached as _load_config_cached,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _reset_for_tests as _reset_for_tests,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _run_server as _run_server,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _scheme_host as _scheme_host,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _service_for_target as _service_for_target,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _sign_request as _sign_request,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    _strip_port_for_loopback_check as _strip_port_for_loopback_check,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    handle_healthz as handle_healthz,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    handle_request as handle_request,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    install_boto3_proxy_hook as install_boto3_proxy_hook,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    proxy_endpoint as proxy_endpoint,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    start_proxy_server as start_proxy_server,
)
from backend.utils.telemetry.proxy_server import (  # noqa: E402
    stop_proxy_server as stop_proxy_server,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    _active_count as _active_count,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    _active_requests_logger as _active_requests_logger,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    _ActiveRequestsResetAdapter as _ActiveRequestsResetAdapter,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    _defer_started_at as _defer_started_at,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    _lock as _lock,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    _mkstemp_sibling as _mkstemp_sibling,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    _reset_active_requests_for_tests as _reset_active_requests_for_tests,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    _write_stream as _write_stream,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    active_request_count as active_request_count,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    atomic_temp_path as atomic_temp_path,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    atomic_write_bytes as atomic_write_bytes,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    atomic_write_json as atomic_write_json,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    decrement_active_requests as decrement_active_requests,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    increment_active_requests as increment_active_requests,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    should_defer_cron as should_defer_cron,
)
from backend.utils.telemetry.system_stats import (  # noqa: E402
    yield_to_api as yield_to_api,
)
from backend.utils.time_window import (  # noqa: E402
    _BANDS as _BANDS,
)
from backend.utils.time_window import (  # noqa: E402
    _FIXED_TOKEN_DELTAS as _FIXED_TOKEN_DELTAS,
)
from backend.utils.time_window import (  # noqa: E402
    ANCHOR_QUANTUM_SECONDS as ANCHOR_QUANTUM_SECONDS,
)
from backend.utils.time_window import (  # noqa: E402
    STATIC_DEFAULT as STATIC_DEFAULT,
)
from backend.utils.time_window import (  # noqa: E402
    VALID_RANGE_TOKENS as VALID_RANGE_TOKENS,
)
from backend.utils.time_window import (  # noqa: E402
    _pick_auto_token as _pick_auto_token,
)
from backend.utils.time_window import (  # noqa: E402
    history_hours_from_earliest as history_hours_from_earliest,
)
from backend.utils.time_window import (  # noqa: E402
    invite_clamp_fingerprint as invite_clamp_fingerprint,
)
from backend.utils.time_window import (  # noqa: E402
    is_valid_range_token as is_valid_range_token,
)
from backend.utils.time_window import (  # noqa: E402
    pick_insights_default as pick_insights_default,
)
from backend.utils.time_window import (  # noqa: E402
    quantize_anchor as quantize_anchor,
)
from backend.utils.time_window import (  # noqa: E402
    resolve_window as resolve_window,
)

_SUBMODULES = (_logging_config, _metrics, _proxy_server, _log_flusher, _system_stats)
_self = sys.modules[__name__]

_ShimModule = install_shim_module(
    __name__,
    _SUBMODULES,
    skip_attrs=(
        "logging_config",
        "metrics",
        "system_stats",
        "proxy_server",
        "log_flusher",
        "_logging_config",
        "_metrics",
        "_system_stats",
        "_proxy_server",
        "_log_flusher",
        "_self",
    ),
    delegated_attrs={
        "_LATEST_PROCESS_CONTEXT": _metrics,
        "_dropped": _metrics,
        "_sync_redis": _log_flusher,
        "_LOG_FLUSHER_THREAD": _log_flusher,
        "_IN_FLIGHT_REQUESTS": _proxy_server,
        "_PORT": _proxy_server,
        "_SERVER_THREAD": _proxy_server,
        "_RUNNER": _proxy_server,
        "_LOOP": _proxy_server,
        "_SESSION": _proxy_server,
        "_UPSTREAM_TIMEOUT_TOTAL_S": _proxy_server,
        "_UPSTREAM_TIMEOUT": _proxy_server,
        "_UPSTREAM_KEEPALIVE_S": _proxy_server,
        "_active_count": _system_stats,
    },
)
