"""RequestTelemetry facade, High-Scale section tracking, and durable request metrics observer."""

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

from backend import config
from backend.celery_app import publisher
from backend.core.request_telemetry.instruments import get_tracer
from backend.core.request_telemetry.query_registry import (
    InstrumentedDuckDBConnection,
    current_attribution,
)
from backend.utils.telemetry import get_page_load_id, parse_iso_utc

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


# ── Request Metrics & Observer (folded from backend.core.request_metrics) ───


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


def _read_durable_rum_metrics(con: Any, source: dict) -> dict[str, Any] | None:
    """Read RUM extents from the same durable serving connection as requests."""
    from backend.core.storage._core import ducklake_table_name

    tables: list[str] = []
    for table_name in (ducklake_table_name(source, "client_vitals"), ducklake_table_name(source, "client_errors")):
        try:
            con.execute(f'SELECT 1 FROM lake."{table_name}" LIMIT 0')
            tables.append(table_name)
        except Exception:
            continue
    if not tables:
        return None

    union_sql = " UNION ALL ".join(f'SELECT req_id, cid, timestamp FROM lake."{table}"' for table in tables)
    row = con.execute(
        "SELECT COUNT(DISTINCT COALESCE(NULLIF(req_id, ''), "
        "concat(cid, '_', CAST(epoch(timestamp) AS BIGINT)))), "
        f"MAX(timestamp) FROM ({union_sql}) AS rum_rows"
    ).fetchone()
    return {
        "latest_log_at": row[1].isoformat() if row and row[1] is not None else None,
        "total_rows": int(row[0] or 0) if row else 0,
    }


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
    """Read one current count/min/max, then persist; failures never write zeros."""
    global _active_query
    if not config.is_durable_serving_mode(source):
        raise ValueError("request metrics observation requires durable serving mode")
    if not _refresh_lock.acquire(blocking=False):
        return None
    con = None
    timer = None
    finished = threading.Event()
    try:
        from backend.core.duckdb import _safe_table_name, open_serving_connection
        from backend.core.metadata.usage_log import latest_cron_per_task
        from backend.utils.telemetry import process_context_scope

        service_id = source["name"]
        with process_context_scope("request_metrics_observer"):
            if stop_event is not None and stop_event.is_set():
                return None
            con = open_serving_connection(source, max_wait=5, skip_view_update=False)
            if stop_event is not None and stop_event.is_set():
                return None
            with _query_lock:
                _cancelled.clear()
                _active_query = con
            timer = threading.Timer(QUERY_TIMEOUT_S, _interrupt_until_finished, args=(finished,))
            timer.daemon = True
            timer.start()
            row = (
                InstrumentedDuckDBConnection(con, service_id=service_id)
                .execute(f'SELECT count(*), min(timestamp), max(timestamp) FROM lake."{_safe_table_name(service_id)}"')
                .fetchone()
            )
            if row is None:
                raise RuntimeError("request metrics aggregate returned no result")
            _check_cancelled(stop_event)

            cron = latest_cron_per_task(service_id)
            current = config.get_status(service_id) or {}
            rum = _read_durable_rum_metrics(con, source)
            if rum is None:
                rum = (extra_status or {}).get("rum", current.get("rum"))
            rum = dict(rum or {})
            rum.setdefault("last_sync_at", None)
            rum_cron_times = [
                t
                for k in ("rum_discovery", "rum_sync", "ledger_rum_sweep")
                if isinstance(t := cron.get(k, {}).get("started_at"), str)
            ]
            if rum_cron_times:
                latest_rum_cron = max(
                    rum_cron_times,
                    key=lambda v: dt.timestamp() if (dt := parse_iso_utc(v)) is not None else float("-inf"),
                )
                cur_dt = parse_iso_utc(rum.get("last_sync_at")) if rum.get("last_sync_at") else None
                new_dt = parse_iso_utc(latest_rum_cron)
                if cur_dt is None or (new_dt is not None and new_dt > cur_dt):
                    rum["last_sync_at"] = latest_rum_cron

            rum_rows = rum.get("total_rows") or 0
            earliest = row[1].isoformat() if row[1] is not None else None
            latest = row[2].isoformat() if row[2] is not None else None
            observation = {
                "earliest_log_at": earliest,
                "latest_log_at": latest,
                "local_rows": int(row[0]) + rum_rows,
                "rum": rum,
                "request": {
                    "latest_log_at": latest,
                    "total_rows": int(row[0]),
                    "last_sync_at": cron.get("log_discovery", {}).get("started_at")
                    or cron.get("sync", {}).get("started_at"),
                },
            }
            _check_cancelled(stop_event)
            config.update_status(service_id, {**(extra_status or {}), **observation})
            return {**observation, "rum": rum}
    finally:
        finished.set()
        if timer is not None:
            timer.cancel()
            timer.join()
        with _query_lock:
            _active_query = None
        try:
            if con is not None:
                con.close()
        finally:
            _refresh_lock.release()


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
        if not self._pass_lock.acquire(blocking=False):
            return
        try:
            for cfg in config.list_configs():
                if self._stop.is_set():
                    break
                source = config.config_to_source(cfg)
                service_id = source["name"]
                if (
                    not config.is_durable_serving_mode(source)
                    or get_high_scale_service_registry().resolve(service_id) is not None
                    or time.monotonic() < self._next_attempt_mono.get(service_id, 0.0)
                ):
                    continue
                try:
                    observed = refresh_durable_request_metrics(source, stop_event=self._stop)
                    if observed is None:
                        continue
                    self._failure_backoff_s.pop(service_id, None)
                    self._next_attempt_mono.pop(service_id, None)
                    if observed == self._published.get(service_id):
                        continue
                    snapshot = compute_sync_status_cached(service_id)
                    if snapshot is not None and not self._stop.is_set():
                        publisher.publish(service_id, snapshot)
                        self._published[service_id] = observed
                        logger.info(
                            "request_metrics.published service=%s rows=%s",
                            service_id,
                            observed["request"]["total_rows"],
                        )
                except Exception:
                    delay = min(
                        self._failure_backoff_s.get(service_id, FAILURE_BACKOFF_BASE_S),
                        FAILURE_BACKOFF_MAX_S,
                    )
                    self._failure_backoff_s[service_id] = min(delay * 2, FAILURE_BACKOFF_MAX_S)
                    self._next_attempt_mono[service_id] = time.monotonic() + delay
                    logger.warning("request_metrics.refresh_failed service=%s", service_id, exc_info=True)
        finally:
            self._pass_lock.release()

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    self.reconcile()
                except Exception:
                    logger.warning("request_metrics.reconcile_failed", exc_info=True)
                self._stop.wait(RECONCILE_INTERVAL_S)
        finally:
            from backend.core.metadata.base import release_thread_connection

            release_thread_connection()

    def start(self) -> None:
        if not config.is_durable_serving_mode():
            return
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._published.clear()
            self._failure_backoff_s.clear()
            self._next_attempt_mono.clear()
            self._thread = threading.Thread(target=self._run, name="request-metrics-observer", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._stop.set()
            interrupt_request_metrics()
            if self._thread is not None:
                self._thread.join(timeout=STOP_TIMEOUT_S)
                if self._thread.is_alive():
                    logger.warning("request_metrics.stop_timeout")
                else:
                    self._thread = None


observer = RequestMetricsObserver()
