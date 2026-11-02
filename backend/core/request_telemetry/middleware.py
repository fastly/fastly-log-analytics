"""ASGI response telemetry middleware and shared telemetry utility re-exports."""

from __future__ import annotations

import json
import logging
import os as _os
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Any

from starlette.requests import Request
from starlette.responses import Response

import backend.utils.telemetry as _telemetry_pkg
from backend.utils.telemetry import (  # noqa: E402, F401, I001
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

logger = logging.getLogger("backend.utils.telemetry")

_DEBUG_BYTE_MARKERS: tuple[bytes, ...] = (
    b'"_component_observations"',
    b'"component_observations"',
    b'"_debug_queries"',
    b'"_debug_calls"',
    b'"_debug_sqlite"',
    b'"_debug_postgres"',
    b'"debug_queries"',
    b'"debug_calls"',
    b'"debug_sqlite"',
    b'"debug_postgres"',
)
_DEBUG_BYTE_MARKERS_WITH_CACHE: tuple[bytes, ...] = (*_DEBUG_BYTE_MARKERS, b'"_is_cached"')
_MAX_NON_DEBUG_BUFFER_BYTES = 256 * 1024


async def _dispatch_telemetry_response(self: TelemetryResponseBodyMiddleware, request: Request, call_next) -> Response:
    response = await call_next(request)
    try:
        from backend.models.common import _DEBUG_ENVELOPE_KEYS, _debug_responses_enabled
    except Exception:
        return response
    master_enabled = _debug_responses_enabled()
    is_remote = getattr(request.state, "is_remote", False)
    analyst_query_enabled = analyst_api_enabled = False
    if is_remote and master_enabled:
        from backend.utils.remote_access import analyst_debug_visibility

        analyst_query_enabled, analyst_api_enabled = analyst_debug_visibility(request)
    if _is_streaming_content_type(response) or not _is_json_response(response):
        return response
    opt_in = master_enabled and (
        request.headers.get("x-debug-responses") == "1"
        or request.cookies.get("fla.debugResponses") == "1"
        or _os.getenv("DEBUG_RESPONSES_FORCE_INCLUDE") == "1"
    )
    if is_remote and opt_in and getattr(request.state, "_analyst_envelope_complete", False):
        return response

    if not opt_in:
        markers = _DEBUG_BYTE_MARKERS_WITH_CACHE if master_enabled else _DEBUG_BYTE_MARKERS
        cl_header = response.headers.get("content-length")
        if cl_header is not None:
            with suppress(ValueError):
                if int(cl_header) > _MAX_NON_DEBUG_BUFFER_BYTES and not master_enabled:
                    return response
        chunks: list[bytes] = []
        total_bytes = 0
        overflowed = False
        it = response.body_iterator.__aiter__()
        try:
            while True:
                try:
                    chunk = await it.__anext__()
                except StopAsyncIteration:
                    break
                chunks.append(chunk)
                total_bytes += len(chunk)
                if total_bytes > _MAX_NON_DEBUG_BUFFER_BYTES and not any(m in b"".join(chunks) for m in markers):
                    overflowed = True
                    break
        except Exception as e:
            logger.warning("[telemetry-middleware] failed to read response body: %s", e)
            return response
        if overflowed:

            async def _passthrough_stream() -> AsyncIterator[bytes]:
                for c in chunks:
                    yield c
                async for c in it:
                    yield c

            response.body_iterator = _passthrough_stream()
            return response
        body = b"".join(chunks)
        if not body or not any(m in body for m in markers):
            return _reconstruct(response, body)
    else:
        try:
            body = b"".join([chunk async for chunk in response.body_iterator])
        except Exception as e:
            logger.warning("[telemetry-middleware] failed to read response body: %s", e)
            return response
        if not body:
            return _reconstruct(response, body)

    try:
        parsed: Any = json.loads(body)
    except (ValueError, json.JSONDecodeError) as e:
        logger.debug("[telemetry-middleware] body not JSON-parseable: %s", e)
        return _reconstruct(response, body)
    if not isinstance(parsed, dict):
        return _reconstruct(response, body)
    if not opt_in:
        keys_to_strip = (*_DEBUG_ENVELOPE_KEYS, "_is_cached") if master_enabled else _DEBUG_ENVELOPE_KEYS
        stripped = [parsed.pop(k, _MISSING) for k in keys_to_strip]
        if all(v is _MISSING for v in stripped):
            return _reconstruct(response, body)
        try:
            return _reconstruct(response, json.dumps(parsed, default=str).encode("utf-8"))
        except Exception as e:
            logger.warning("[telemetry-middleware] failed to strip telemetry: %s", e)
            return _reconstruct(response, body)
    if "_debug_queries" in parsed:
        return _reconstruct(response, body)
    try:
        inject_queries = analyst_query_enabled if is_remote else True
        inject_api_calls = analyst_api_enabled if is_remote else True
        if inject_queries:
            parsed["_debug_queries"] = _telemetry_pkg.get_queries()
            pg_queries = list(_telemetry_pkg.get_sqlite_queries())
            parsed["_debug_postgres"] = pg_queries
            parsed["_debug_sqlite"] = pg_queries
        if inject_api_calls:
            parsed["_debug_calls"] = _telemetry_pkg.get_tracked_calls()
        if not is_remote or inject_queries or inject_api_calls:
            parsed.setdefault("_is_cached", False)
        return _reconstruct(response, json.dumps(parsed, default=str).encode("utf-8"))
    except Exception as e:
        logger.warning("[telemetry-middleware] failed to inject telemetry: %s", e)
        return _reconstruct(response, body)


TelemetryResponseBodyMiddleware.dispatch = _dispatch_telemetry_response  # type: ignore[method-assign]
