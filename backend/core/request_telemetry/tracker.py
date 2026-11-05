"""RequestTelemetry facade, High-Scale section tracking, and request metrics observer."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from typing import Any, Literal, TypeVar
from uuid import uuid4

import structlog
from opentelemetry import trace
from opentelemetry.trace import Span

from backend.core.fos_client import _safe_table_name
from backend.core.request_telemetry.instruments import get_tracer
from backend.core.request_telemetry.query_registry import current_attribution
from backend.utils.telemetry import get_page_load_id

logger = logging.getLogger("backend.core.request_telemetry")


class RequestTelemetry:
    """Per-request OTel facade held on `RequestContext.telemetry`."""

    __slots__ = (
        "request_path",
        "request_method",
        "_root_span",
        "_root_ctx_token",
        "_owns_root_span",
        "_t_start",
        "is_cached",
    )

    def __init__(self, request_method: str, request_path: str) -> None:
        self.request_method = request_method
        self.request_path = request_path
        self._root_span: Span | None = None
        self._root_ctx_token: Any = None
        self._owns_root_span: bool = False
        self._t_start: float = 0.0
        self.is_cached: bool = False

    def start_request(self) -> None:
        """Open or attach to the root request span. Idempotent."""
        if self._root_span is not None:
            return
        self._t_start = time.monotonic()
        ambient = trace.get_current_span()
        if ambient is not None and ambient.is_recording():
            self._root_span = ambient
            self._owns_root_span = False
            return
        self._root_span = get_tracer().start_span(
            name=f"{self.request_method} {self.request_path}",
            attributes={"http.method": self.request_method, "http.route": self.request_path},
        )
        self._owns_root_span = True

    def elapsed_ms(self) -> float:
        return (time.monotonic() - self._t_start) * 1000 if self._t_start else 0.0

    def end_request(self, status_code: int | None = None) -> None:
        """Attach final attributes and close the root span if owned by this instance."""
        if self._root_span is None:
            return
        if self._root_span.is_recording():
            if status_code is not None:
                self._root_span.set_attribute("http.status_code", int(status_code))
            self._root_span.set_attribute("app.is_cached", bool(self.is_cached))
            self._root_span.set_attribute("app.total_ms", round((time.monotonic() - self._t_start) * 1000, 2))
            if self._owns_root_span:
                self._root_span.end()
        self._root_span = None
        try:
            if get_page_load_id():
                from backend.core.metadata import usage_log

                usage_log.flush_telemetry_buffer()
        except Exception:
            logger.debug("end_request: flush_telemetry_buffer failed (ignored)", exc_info=True)


# ── High-scale client-side section wall time and ClickHouse query ownership ──

_Result = TypeVar("_Result")
_Source = Literal["fact", "projection", "projected_fact", "mixed", "unknown"]
_CURRENT: ContextVar[dict[str, Any] | None] = ContextVar("high_scale_section", default=None)
_logger = structlog.get_logger(__name__)


def current_clickhouse_scope() -> dict[str, Any]:
    identity = _CURRENT.get()
    if identity is not None:
        return dict(identity)
    attribution = current_attribution.get()
    return {
        "request_id": attribution.request_id if attribution is not None else None,
        "page_load_id": get_page_load_id(),
        "service_id": None,
        "component": None,
        "section": None,
        "section_id": None,
        "parent_section_id": None,
        "source": "unknown",
    }


@contextmanager
def clickhouse_source(source: _Source) -> Iterator[None]:
    token = _CURRENT.set({**current_clickhouse_scope(), "source": source})
    try:
        yield
    finally:
        _CURRENT.reset(token)


class HighScaleSections:
    def __init__(self, service_id: str, component: str) -> None:
        self.service_id = service_id
        self.component = component
        self._lock = threading.Lock()
        self._entries: list[dict[str, Any]] = []
        self._truncated = False

    def submit(
        self,
        pool: Executor,
        section: str,
        fn: Callable[[], _Result],
        *,
        source: _Source = "unknown",
        activity: Literal["compute", "overhead"] = "compute",
    ) -> Future[_Result]:
        from backend.utils.telemetry.metrics import get_queries

        get_queries()
        queued = time.perf_counter()
        return pool.submit(
            copy_context().run, self.run, section, fn, source=source, activity=activity, submitted_at=queued
        )

    def run(
        self,
        section: str,
        fn: Callable[[], _Result],
        *,
        source: _Source = "unknown",
        submitted_at: float | None = None,
        activity: Literal["compute", "overhead"] = "compute",
    ) -> _Result:
        parent = current_clickhouse_scope()
        identity = {
            **parent,
            "service_id": self.service_id,
            "component": self.component,
            "section": f"{self.component}.{section}",
            "section_id": uuid4().hex,
            "parent_section_id": parent["section_id"],
            "source": source,
        }
        token = _CURRENT.set(identity)
        started = time.perf_counter()
        outcome = "error"
        try:
            result = fn()
            outcome = "success"
            return result
        finally:
            elapsed = (time.perf_counter() - started) * 1000
            _CURRENT.reset(token)
            entry = {
                **identity,
                "time_ms": elapsed,
                "executor_dispatch": "thread_pool" if submitted_at is not None else "inline",
                "executor_wait_ms": max(0.0, (started - submitted_at) * 1000) if submitted_at is not None else None,
                "outcome": outcome,
                "activity": activity,
                "timing_kind": "client_section_wall",
                "current_execution": True,
                "server_cpu_ms": None,
            }
            with self._lock:
                if len(self._entries) < 256:
                    self._entries.append(entry)
                else:
                    self._truncated = True
            try:
                _logger.info("high_scale.section", **entry)
            except Exception:
                logger.debug("section logging failed", exc_info=True)

    def run_parallel(
        self,
        tasks: Sequence[tuple[str, Callable[[], _Result]]],
        *,
        source: _Source | Callable[[str], _Source] = "unknown",
        activity: Literal["compute", "overhead"] | Callable[[str], Literal["compute", "overhead"]] = "compute",
        max_workers: int | None = None,
        thread_name_prefix: str = "",
    ) -> dict[str, _Result]:
        if not tasks:
            return {}
        src_fn = source if callable(source) else (lambda _: source)
        act_fn = activity if callable(activity) else (lambda _: activity)
        if len(tasks) == 1:
            key, fn = tasks[0]
            return {key: self.run(key, fn, source=src_fn(key), activity=act_fn(key))}
        workers = min(max_workers, len(tasks)) if max_workers is not None else len(tasks)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=thread_name_prefix) as pool:
            futures = [(key, self.submit(pool, key, fn, source=src_fn(key), activity=act_fn(key))) for key, fn in tasks]
            return {key: fut.result() for key, fut in futures}

    def timings(self) -> list[dict[str, Any]]:
        with self._lock:
            result = [dict(entry) for entry in self._entries]
            if self._truncated:
                result.append(
                    {"section": f"{self.component}.observations_truncated", "time_ms": None, "activity": "unknown"}
                )
            return result


# ── Request Metrics & Observer ───────────────────────────────────────────────


def compute_sync_status_cached(*args: Any, **kwargs: Any) -> Any:
    from backend.cron.scheduler import compute_sync_status_cached as _compute

    return _compute(*args, **kwargs)


def get_high_scale_service_registry(*args: Any, **kwargs: Any) -> Any:
    from backend.high_scale.ingest_controller import get_high_scale_service_registry as _get_registry

    return _get_registry(*args, **kwargs)


QUERY_TIMEOUT_S = 30.0
RECONCILE_INTERVAL_S = 15.0
STOP_TIMEOUT_S = 5.0
FAILURE_BACKOFF_BASE_S = 15.0
FAILURE_BACKOFF_MAX_S = 300.0

_refresh_lock = threading.Lock()
_query_lock = threading.Lock()
_active_query: Any | None = None
_cancelled = threading.Event()


def interrupt_request_metrics() -> None:
    """Interrupt only this observer's dedicated connection, never a reader."""
    with _query_lock:
        if _active_query is not None:
            _cancelled.set()
            _active_query.interrupt()


def _interrupt_until_finished(finished: threading.Event) -> None:
    while not finished.is_set():
        interrupt_request_metrics()
        finished.wait(0.1)


def _check_cancelled(stop_event: threading.Event | None) -> None:
    if _cancelled.is_set() or (stop_event is not None and stop_event.is_set()):
        raise InterruptedError("request metrics observation cancelled")


def refresh_durable_request_metrics(
    source: dict, *, stop_event: threading.Event | None = None, extra_status: dict | None = None
) -> dict | None:
    """No-op after DuckLake removal; ClickHouse services use high-scale watermarks."""
    del stop_event, extra_status
    _safe_table_name(str(source.get("name") or ""))
    return None


class RequestMetricsObserver:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._pass_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._published: dict[str, dict] = {}
        self._failure_backoff_s: dict[str, float] = {}
        self._next_attempt_mono: dict[str, float] = {}

    def reconcile(self) -> None:
        return None

    def start(self) -> None:
        return None

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._stop.set()
            interrupt_request_metrics()
            if self._thread is not None:
                self._thread.join(timeout=STOP_TIMEOUT_S)
                if not self._thread.is_alive():
                    self._thread = None


observer = RequestMetricsObserver()
