"""Shared ``EventSourceResponse`` factory for SSE endpoints.

Every SSE route in the app returns the identical
``EventSourceResponse(stream(), ping=5, headers=SSE_PASSTHROUGH_HEADERS)``.
``sse_response()`` centralizes that shape so the ping interval and the
proxy-passthrough headers (Fastly / Caddy / nginx no-buffer hints) live in one
place; the per-endpoint ``stream()`` closure — including its own
cancellation / cleanup ``finally`` block — is passed in unchanged.

This module is in ``backend.utils`` on purpose: it must not import routers
(import-contracts forbid routers↛routers edges), so the helper takes the async
generator and returns the response without reaching back into route code.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import queue
import threading
from collections.abc import AsyncIterable, AsyncIterator, Callable, Generator, Iterator
from typing import Any

from sse_starlette.sse import EventSourceResponse

from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS


class ClientDisconnected(Exception):
    """Raised when the client disconnects during a streaming response."""


class _AbortableQueue(queue.Queue):
    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self.aborted = False

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        if self.aborted:
            if item is None:
                return
            raise ClientDisconnected("Client disconnected during streaming")
        super().put(item, block, timeout)


def _stream_from_worker(worker: Callable[[_AbortableQueue], Any], *, maxsize: int = 10) -> Generator[Any]:
    """Run *worker(q)* in a daemon thread and yield items it puts into the queue until ``None``."""
    q: _AbortableQueue = _AbortableQueue(maxsize=maxsize)
    # Copy the request's context (process_context, _CALLS list) so any
    # record_call() inside the worker thread lands in the same _usage_log batch.
    ctx = contextvars.copy_context()
    thread = threading.Thread(target=lambda: ctx.run(worker, q), daemon=True)
    thread.start()
    try:
        while True:
            chunk = q.get()
            if chunk is None:
                break
            yield chunk
    finally:
        q.aborted = True
        while True:
            try:
                q.get_nowait()
            except queue.Empty:
                break


def stream_sse_from_worker(
    worker: Callable[[_AbortableQueue], Any],
    *,
    ping: int = 5,
    maxsize: int = 0,
) -> EventSourceResponse:
    """Run *worker(q)* in a background daemon thread via ``_stream_from_worker`` and stream JSON SSE frames."""

    def _safe_worker(q: _AbortableQueue) -> None:
        try:
            worker(q)
        finally:
            q.put(None)

    async def _stream() -> AsyncIterator[str]:
        gen = _stream_from_worker(_safe_worker, maxsize=maxsize)
        sentinel = object()
        try:
            while True:
                event = await asyncio.to_thread(next, gen, sentinel)
                if event is sentinel:
                    break
                yield event if isinstance(event, str) else json.dumps(event)
        finally:
            gen.close()

    return sse_response(_stream(), ping=ping)


def sse_response(stream: AsyncIterable[Any] | Iterator[Any], *, ping: int = 5) -> EventSourceResponse:
    """Wrap an SSE ``stream`` in an ``EventSourceResponse``.

    Applies the shared ``ping`` interval (default 5s, matching every existing
    call site) and ``SSE_PASSTHROUGH_HEADERS`` so no caller has to repeat them.
    Event framing / payloads are produced entirely by ``stream`` and are left
    untouched.
    """
    return EventSourceResponse(stream, ping=ping, headers=SSE_PASSTHROUGH_HEADERS)


def stream_iac_reconcile(
    service_id: str,
    resolved_token: str,
    *,
    start_message: str,
    done_event: dict[str, Any] | Callable[[], dict[str, Any]],
    after_done: Callable[[], None] | None = None,
    error_event: Callable[[Exception], dict[str, Any]] | None = None,
    log_label: str = "reconcile_infrastructure",
    logger_obj: Any = None,
) -> EventSourceResponse:
    """Stream ``reconcile_infrastructure`` events via ``run_with_events`` as SSE JSON frames."""
    from backend.provision.declarative.iac_reconciler import reconcile_infrastructure
    from backend.provision.orchestrator import run_with_events

    def _stream() -> Iterator[str]:
        yield json.dumps({"type": "status", "message": start_message})
        try:
            for event in run_with_events(reconcile_infrastructure, service_id, resolved_token):
                yield json.dumps(event)
            done_payload = done_event() if callable(done_event) else done_event
            yield json.dumps(done_payload)
            if after_done is not None:
                after_done()
        except Exception as e:
            if logger_obj is not None:
                logger_obj.exception("%s failed for %s", log_label, service_id)
            err_payload = error_event(e) if error_event is not None else {"type": "error", "message": str(e)}
            yield json.dumps(err_payload)

    return sse_response(_stream())
