"""Backstop middleware: auto-injects ``_debug_queries`` / ``_debug_calls`` /
``_is_cached`` into JSON responses that don't already carry them.

Most endpoints route through ``models/common.py::BaseResponse.with_telemetry``
and serialise the three telemetry keys themselves. Newly-added endpoints
that return a plain ``dict`` (or that forgot to use ``BaseResponse``) drop
the telemetry on the floor — the frontend's Debug Panel goes blank for
that request and operators have no signal that the endpoint exists.

This middleware backstops that gap: after the route handler runs, if the
response body is a JSON object missing ``_debug_queries``, it parses,
merges, and re-serialises with the contextvar collectors.

Constraints:
  * MUST register INNER to ``GZipMiddleware`` — otherwise the body it
    reads is already gzip-compressed and json.loads explodes. In
    ``main.py`` this means calling ``add_middleware(TelemetryResponseBodyMiddleware)``
    BEFORE the ``add_middleware(GZipMiddleware)`` line. Starlette's
    middleware ordering is reverse-stack: the LAST add_middleware call
    becomes the OUTERMOST.
  * Skips streaming responses (SSE, file downloads, server-sent events).
    A streaming response's body iterator can be consumed exactly once
    and is the entire reason the route opted into streaming — buffering
    it here would defeat the purpose AND introduce a deadlock risk on
    infinite-stream SSE.
  * Skips responses whose body isn't a JSON dict (lists, primitives,
    empty bodies, non-JSON content-types). Top-level lists can't host
    keys without breaking their contract.
  * Skips when the body already has ``_debug_queries`` — never
    double-injects.
  * Gated on ``DEBUG_RESPONSES`` env (same flag as ``BaseResponse``).
    When off, the middleware is a near no-op (still detects skip
    conditions but never touches the body).

Failure modes are silent + non-blocking: a body that won't parse as
JSON, a contextvar read that raises, a re-serialisation that fails —
all collapse to "pass the original response through unchanged". The
backstop is hardening, not a correctness gate; never break a working
endpoint to add telemetry to it.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)


_JSON_CONTENT_TYPES: tuple[str, ...] = ("application/json",)
# Skip these content-type prefixes regardless of anything else — they're
# streaming protocols (or known-binary) and buffering them would either
# deadlock (SSE) or corrupt (binary). Note: detecting streaming via
# ``isinstance(response, StreamingResponse)`` does NOT work here —
# Starlette's BaseHTTPMiddleware wraps every response in a private
# ``_StreamingResponse`` regardless of how the route returned it, so
# the isinstance check is always True. Content-Type is the reliable
# signal.
_STREAMING_CONTENT_TYPES: tuple[str, ...] = (
    "text/event-stream",
    "application/octet-stream",
    "application/x-ndjson",
    "application/jsonl",
)


def _content_type(response: Response) -> str:
    return (response.headers.get("content-type") or response.media_type or "").lower()


def _is_json_response(response: Response) -> bool:
    """True iff the response's Content-Type identifies it as JSON.

    Conservative match on the type prefix only — ``application/json;
    charset=utf-8`` and ``application/json`` both qualify. Anything else
    (text/html, text/event-stream, application/octet-stream, …) is
    passed through.
    """
    media = _content_type(response)
    return any(media.startswith(t) for t in _JSON_CONTENT_TYPES)


def _is_streaming_content_type(response: Response) -> bool:
    media = _content_type(response)
    return any(media.startswith(t) for t in _STREAMING_CONTENT_TYPES)


class TelemetryResponseBodyMiddleware(BaseHTTPMiddleware):
    """Inject telemetry into JSON dict responses that lack it.

    See module docstring for the full contract. Three properties pinned
    by the test suite:

      1. **No double-injection** — a response whose body already has
         ``_debug_queries`` is returned unchanged (byte-identical).
      2. **Plain-dict endpoints gain telemetry** — a route that returns
         ``{"foo": 1}`` becomes ``{"foo": 1, "_debug_queries": [...],
         "_debug_calls": [...], "_is_cached": false}``.
      3. **Streaming responses are never buffered** — SSE / file
         downloads / chunked streams pass through with their body
         iterator intact.
    """

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Bail early on the cheapest signals so the common case
        # (non-JSON, streaming, gated off) pays close to zero overhead.
        try:
            from backend.models.common import _debug_responses_enabled
        except Exception:
            # Circular-import or test-harness setup glitch — never block
            # the request.
            return response

        # 2026-06-10 audit (N-1): never attach telemetry to analyst
        # responses, regardless of DEBUG_RESPONSES. The envelope leaks the
        # Fastly KV store ID via _debug_calls and raw SQL via
        # _debug_queries. Stripping in RemoteAccessMiddleware isn't enough
        # because this middleware sits OUTSIDE it in the dispatch order
        # and would re-inject. Honor the same is_remote flag the strip
        # uses so admin (loopback) keeps the debug panel and analyst gets
        # clean payloads.
        if getattr(request.state, "is_remote", False):
            return response

        if not _debug_responses_enabled():
            return response
        if _is_streaming_content_type(response):
            return response
        if not _is_json_response(response):
            return response

        # Read the full body. BaseHTTPMiddleware wraps the underlying
        # response in a streaming pipe even for non-streaming Responses,
        # so we always consume body_iterator (not response.body).
        try:
            body_chunks: list[bytes] = []
            async for chunk in response.body_iterator:
                body_chunks.append(chunk)
            body = b"".join(body_chunks)
        except Exception as e:
            logger.warning("[telemetry-middleware] failed to read response body: %s", e)
            return response

        # Empty body (e.g. 204 No Content slipped through with a JSON
        # content-type, or an endpoint that returned ``None``) — nothing
        # to inject into, but we still have to reconstruct the response
        # because the body_iterator has been drained.
        if not body:
            return _reconstruct(response, body)

        try:
            parsed: Any = json.loads(body)
        except (ValueError, json.JSONDecodeError) as e:
            # Malformed JSON in an application/json response is a bug
            # in the endpoint, but the middleware is not the right
            # place to surface it — pass the original bytes through
            # so the frontend sees the same broken payload it would
            # have seen without the middleware.
            logger.debug("[telemetry-middleware] body not JSON-parseable: %s", e)
            return _reconstruct(response, body)

        if not isinstance(parsed, dict):
            # Top-level lists / primitives can't host the telemetry
            # keys without breaking the endpoint's published shape.
            return _reconstruct(response, body)

        if "_debug_queries" in parsed:
            # Endpoint already supplied telemetry (BaseResponse or
            # manual injection). Never double-inject.
            return _reconstruct(response, body)

        # Inject from the contextvar collectors. Errors here MUST NOT
        # block the response — telemetry is observability, not data.
        try:
            from backend.utils.telemetry import get_queries, get_tracked_calls

            parsed["_debug_queries"] = get_queries()
            parsed["_debug_calls"] = get_tracked_calls()
            parsed.setdefault("_is_cached", False)
            new_body = json.dumps(parsed, default=str).encode("utf-8")
        except Exception as e:
            logger.warning("[telemetry-middleware] failed to inject telemetry: %s", e)
            return _reconstruct(response, body)

        return _reconstruct(response, new_body)


def _reconstruct(original: Response, body: bytes) -> Response:
    """Build a new ``Response`` from ``body`` with the original's status
    code, media type, and headers — minus ``Content-Length`` which we
    re-derive from the (possibly modified) body length.

    Why a fresh ``Response`` and not mutating the original: Starlette's
    streaming pipe has already started, and the original's headers
    iterator may be exhausted depending on the ASGI server. A fresh
    Response is cheap and guaranteed correct.

    Headers are copied via ``raw_headers`` (not ``headers.items()``) so
    multi-valued headers survive the round-trip. ``headers.items()`` is a
    dict-like view that collapses duplicates to the last value, which
    silently dropped the pending-session Set-Cookie on the share-login
    response (login sets the pending cookie AND deletes the full cookie —
    two Set-Cookie headers, and the dict comprehension kept only the
    delete). Same trap applies to any future endpoint emitting multiple
    Set-Cookie, Link, or Vary values.
    """
    # Drop Content-Length so Starlette recomputes it for the new body.
    # Drop Content-Encoding because we never touch already-encoded bodies
    # (the compress middleware sits outside us), but defending against a
    # future re-ordering is cheap.
    #
    # Content-Type needs careful handling. ``original.media_type`` is None
    # when ``original`` is the ``_StreamingResponse`` that Starlette's
    # BaseHTTPMiddleware wraps every inner response in — and that includes
    # us, because we ARE a BaseHTTPMiddleware. Without media_type, the new
    # ``Response()`` init sets no content-type header, and any outer
    # compression middleware downstream (CompressMiddleware in main.py)
    # sees an untyped response and bails (its
    # ``is_start_message_satisfied`` requires content-type to decide if
    # the body is compressible). 2026-06-09 audit caught this — every
    # /api/* response was uncompressed because the chain dropped the
    # FastAPI-set ``application/json``. Fix: read the actual content-type
    # off raw_headers (which DOES carry it through BaseHTTPMiddleware) and
    # pass it as media_type so the new Response re-emits it.
    drop = (b"content-length", b"content-encoding", b"content-type")
    media_type = original.media_type
    if media_type is None:
        for k, v in original.raw_headers:
            if k.lower() == b"content-type":
                try:
                    media_type = v.decode("ascii")
                except UnicodeDecodeError:
                    pass
                break
    new = Response(content=body, status_code=original.status_code, media_type=media_type)
    new.raw_headers.extend((k, v) for k, v in original.raw_headers if k.lower() not in drop)
    return new
