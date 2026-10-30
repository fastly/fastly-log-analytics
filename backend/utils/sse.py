"""Server-Sent Events (SSE) streaming helpers and worker-thread queue bridges."""

from __future__ import annotations

import asyncio
import contextvars
import json
import queue
import threading
from collections.abc import AsyncIterable, AsyncIterator, Callable, Generator, Iterator
from typing import Any

from fastapi import Request
from sse_starlette.sse import EventSourceResponse

SSE_PASSTHROUGH_HEADERS: dict[str, str] = {
    "Surrogate-Control": "no-store",
    "Cache-Control": "private, no-store, no-transform",
    "X-Accel-Buffering": "no",
}


async def iter_with_disconnect_ping[T](
    source: AsyncIterable[T],
    request: Request,
    *,
    ping_seconds: float = 15,
) -> AsyncIterator[T]:
    """Yield each payload from ``source`` until the request disconnects."""
    from contextlib import suppress

    aiter = source.__aiter__()
    pending: asyncio.Future[T] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(aiter.__anext__())
            done, _ = await asyncio.wait({pending}, timeout=ping_seconds)
            if not done:
                if await request.is_disconnected():
                    return
                continue
            try:
                payload = pending.result()
            except StopAsyncIteration:
                pending = None
                return
            pending = None
            if await request.is_disconnected():
                return
            yield payload
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            with suppress(asyncio.CancelledError, StopAsyncIteration):
                await pending
        aclose = getattr(aiter, "aclose", None)
        if callable(aclose):
            with suppress(Exception):
                await aclose()


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
    """Wrap an SSE ``stream`` in an ``EventSourceResponse``."""
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
    from backend.provision.declarative.reconciler import reconcile_infrastructure
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
