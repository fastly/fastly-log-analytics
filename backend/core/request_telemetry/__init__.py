"""RequestTelemetry package — re-exports OTel instruments, query registry, request tracker, and middleware."""

from __future__ import annotations

import sys
import types
from typing import Any

from backend.core.request_telemetry import (
    instruments,
    middleware,
    tracker,
)
from backend.core.request_telemetry import (
    query_registry as _query_registry_mod,
)
from backend.core.request_telemetry.instruments import (  # noqa: F401
    _SERVICE_NAME,
    _TRACER_NAME,
    DEFAULT_MAX_MB,
    LOG_PATH,
    BatchSpanProcessor,
    ConsoleMetricExporter,
    ConsoleSpanExporter,
    MeterProvider,
    PeriodicExportingMetricReader,
    Resource,
    Span,
    Status,
    StatusCode,
    Tracer,
    TracerProvider,
    _db_spans_enabled,
    _end_db_span,
    _init_lock,
    _initialised,
    _max_bytes,
    _otel_enabled,
    _otel_exporter,
    _rotate_if_needed,
    _setup_sdk,
    _start_db_span,
    _thread_wait_histogram,
    _thread_wait_lock,
    _web_vitals_logger,
    _write_lock,
    append_sample,
    asgi_instrumentation_enabled,
    collection_enabled,
    effective_exporter,
    ensure_initialized,
    force_flush,
    get_meter,
    get_tracer,
    metrics,
    rotated_path,
    thread_wait_histogram,
    trace,
)
from backend.core.request_telemetry.middleware import (  # noqa: F401
    _BANDS,
    _FIXED_TOKEN_DELTAS,
    _JSON_CONTENT_TYPES,
    _MIN_SALT_LEN,
    _MISSING,
    _STREAMING_CONTENT_TYPES,
    _UVICORN_LOGGERS,
    ABS_PATH_RE,
    ANCHOR_QUANTUM_SECONDS,
    CID_SALT_LITERAL_RE,
    REDACTED,
    STATIC_DEFAULT,
    VALID_RANGE_TOKENS,
    BoundedCache,
    BoundedTTLCache,
    CacheRegistry,
    TelemetryResponseBodyMiddleware,
    _ActiveRequestsResetAdapter,
    _add_otel_trace_context,
    _content_type,
    _drop_uvicorn_color_message,
    _is_json_response,
    _is_streaming_content_type,
    _known_salts,
    _pick_auto_token,
    _reconstruct,
    _reset_for_tests,
    _salts_lock,
    active_request_count,
    atomic_temp_path,
    atomic_write_bytes,
    atomic_write_json,
    audit_log,
    bridge_uvicorn_loggers,
    configure_structlog,
    decrement_active_requests,
    effective_format,
    history_hours_from_earliest,
    increment_active_requests,
    invite_clamp_fingerprint,
    is_valid_range_token,
    pick_insights_default,
    quantize_anchor,
    redact_error_text,
    register_salt,
    resolve_window,
    scrub_secrets,
    should_defer_cron,
    yield_to_api,
)
from backend.core.request_telemetry.query_registry import (  # noqa: F401
    _ERR_TRUNCATE,
    _HISTORY_CAP,
    _INSTRUMENTATION_PREFIXES,
    _MEMORY_PROBE_THRESHOLD_S,
    _POOL_QUERY_TIMEOUT_S,
    _POOL_QUERY_WATCH_INTERVAL_S,
    _READER_METHODS,
    _REGISTRY_DISABLED,
    _SLOW_QUERY_PERSIST_DISABLED,
    _SLOW_QUERY_PERSIST_THRESHOLD_MS,
    _SQL_TRUNCATE,
    _TERMINAL_METHODS,
    ActiveQuery,
    Attribution,
    CompletedQuery,
    InstrumentedDuckDBConnection,
    QueryRegistry,
    _attribution_payload,
    _CancelledKeyCache,
    _capture_caller,
    _conn_to_query,
    _conn_to_query_lock,
    _deregister,
    _ensure_metrics,
    _InstrumentedRecordReader,
    _InstrumentedResult,
    _metric_active_count,
    _metric_cancelled_total,
    _metric_duration_ms,
    _metric_lock,
    _metric_safe,
    _parse_memory_mb,
    _persist_slow_query,
    _pool_query_watchdog,
    _probe_duckdb_memory,
    _row_for_active,
    _row_for_completed,
    _safe_weakref,
    _scrub_secrets,
    _seq,
    _truncate,
    current_attribution,
    derive_from_process_context,
    duckdb,
    query_registry,
    time,
)
from backend.core.request_telemetry.tracker import (  # noqa: F401
    _CURRENT,
    FAILURE_BACKOFF_BASE_S,
    FAILURE_BACKOFF_MAX_S,
    QUERY_TIMEOUT_S,
    RECONCILE_INTERVAL_S,
    STOP_TIMEOUT_S,
    HighScaleSections,
    RequestMetricsObserver,
    RequestTelemetry,
    _active_query,
    _cancelled,
    _check_cancelled,
    _interrupt_until_finished,
    _logger,
    _query_lock,
    _read_durable_rum_metrics,
    _refresh_lock,
    _Result,
    _Source,
    clickhouse_source,
    compute_sync_status_cached,
    config,
    current_clickhouse_scope,
    get_high_scale_service_registry,
    get_page_load_id,
    interrupt_request_metrics,
    logger,
    observer,
    parse_iso_utc,
    publisher,
    refresh_durable_request_metrics,
)

_SYNC_MODULES = (
    instruments,
    _query_registry_mod,
    tracker,
    middleware,
)

_DELEGATED_ATTRS = {
    "_initialised": instruments,
    "_thread_wait_histogram": instruments,
    "_active_query": tracker,
    "_metric_active_count": _query_registry_mod,
    "_metric_duration_ms": _query_registry_mod,
    "_metric_cancelled_total": _query_registry_mod,
}


class _ShimModule(types.ModuleType):
    """Module proxy that forwards monkeypatch attributes into domain submodules."""

    def __getattribute__(self, name: str) -> Any:
        delegated = _DELEGATED_ATTRS.get(name)
        if delegated is not None:
            return getattr(delegated, name)
        return super().__getattribute__(name)

    def __getattr__(self, name: str) -> Any:
        for mod in _SYNC_MODULES:
            if hasattr(mod, name):
                return getattr(mod, name)
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        for mod in _SYNC_MODULES:
            if hasattr(mod, name):
                setattr(mod, name, value)


_self = sys.modules[__name__]
if not isinstance(_self, _ShimModule):
    _self.__class__ = _ShimModule
