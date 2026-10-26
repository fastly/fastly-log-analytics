"""RequestTelemetry — OpenTelemetry wrapper, query attribution, High-Scale sections, and request metrics."""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal, TypeVar
from uuid import uuid4

import structlog
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Span, Tracer

from backend import config
from backend.celery_app import publisher
from backend.utils.date_utils import parse_iso_utc
from backend.utils.telemetry import get_page_load_id

logger = logging.getLogger(__name__)


# ── Query Attribution (folded from backend.core.query_attribution) ───────────

_INSTRUMENTATION_PREFIXES: tuple[str, ...] = (
    "backend/core/query_registry",
    "backend/core/query_attribution",
    "backend/core/request_telemetry",
    "backend/core/query_instrumentation",
    "backend/utils/sqlite_profiler",
    "backend/core/duckdb_pool",
    "/sqlite3/",
    "/duckdb/",
)


def _capture_caller(skip_frames: int = 2) -> tuple[str, str]:
    """Return ``(qualname, "<rel-path>:<lineno>")`` of the first non-instrumentation caller frame."""
    try:
        frame: Any = sys._getframe(skip_frames)
    except ValueError:
        return ("<unknown>", "<unknown>")
    while frame is not None:
        path = frame.f_code.co_filename
        if not any(p in path for p in _INSTRUMENTATION_PREFIXES):
            qual = getattr(frame.f_code, "co_qualname", frame.f_code.co_name)
            display_path = path
            for marker in ("backend/", "frontend/"):
                idx = path.rfind(marker)
                if idx != -1:
                    display_path = path[idx:]
                    break
            return (qual, f"{display_path}:{frame.f_lineno}")
        frame = frame.f_back
    return ("<unknown>", "<unknown>")


@dataclass(slots=True)
class Attribution:
    """Structured attribution for a single in-flight or completed query."""

    kind: str  # "analyst" | "admin" | "cron" | "system"
    analyst_id: str | None = None
    analyst_name: str | None = None
    admin_id: str | None = None
    cron_job: str | None = None
    cron_run_id: str | None = None

    caller_qualname: str = "<unknown>"
    caller_file: str = "<unknown>"
    request_path: str | None = None
    request_id: str | None = None
    cancel_key: str | None = None
    pool_slot: str | None = None

    @classmethod
    def analyst(
        cls,
        *,
        analyst_id: str,
        analyst_name: str | None,
        request_path: str | None,
        request_id: str | None,
        cancel_key: str | None = None,
    ) -> Attribution:
        return cls(
            kind="analyst",
            analyst_id=analyst_id,
            analyst_name=analyst_name,
            request_path=request_path,
            request_id=request_id,
            cancel_key=cancel_key,
        )

    @classmethod
    def admin(
        cls,
        *,
        admin_id: str,
        request_path: str | None,
        request_id: str | None,
        cancel_key: str | None = None,
    ) -> Attribution:
        return cls(
            kind="admin",
            admin_id=admin_id,
            request_path=request_path,
            request_id=request_id,
            cancel_key=cancel_key,
        )

    @classmethod
    def cron(cls, *, cron_job: str, cron_run_id: str | None = None) -> Attribution:
        return cls(kind="cron", cron_job=cron_job, cron_run_id=cron_run_id)

    @classmethod
    def system(cls, *, hint: str | None = None) -> Attribution:
        return cls(
            kind="system",
            caller_qualname=hint or f"thread:{threading.current_thread().name}",
            caller_file="<system>",
        )

    def principal_id(self) -> str | None:
        if self.kind == "analyst":
            return self.analyst_id
        if self.kind == "admin":
            return self.admin_id
        if self.kind == "cron":
            return self.cron_run_id or self.cron_job
        return None

    def display_label(self) -> str:
        tail = f" — {self.request_path}" if self.request_path else ""
        if self.kind == "analyst":
            who = self.analyst_name or f"Guest ({self.analyst_id[-4:] if self.analyst_id else '?'})"
            return f"Analyst: {who}{tail}"
        if self.kind == "admin":
            return f"Admin: {self.admin_id or 'admin'}{tail}"
        if self.kind == "cron":
            run = f" (run {self.cron_run_id})" if self.cron_run_id else ""
            return f"Cron: {self.cron_job}{run}"
        return f"System: {self.caller_qualname}"

    def with_caller(self, qualname: str, file_line: str) -> Attribution:
        return replace(self, caller_qualname=qualname, caller_file=file_line)

    def with_pool_slot(self, slot: str | None) -> Attribution:
        if slot is None or self.pool_slot == slot:
            return self
        return replace(self, pool_slot=slot)


current_attribution: ContextVar[Attribution | None] = ContextVar("current_attribution", default=None)


def derive_from_process_context(process_ctx: str | None) -> Attribution | None:
    """Build an Attribution for ``cron:`` / ``startup:`` / ``shutdown:`` process contexts."""
    if not process_ctx:
        return None
    head, _, tail = process_ctx.partition(":")
    if head == "cron":
        return Attribution.cron(cron_job=tail or process_ctx)
    if head in ("startup", "shutdown"):
        return Attribution.system(hint=process_ctx)
    return None


sys.modules["backend.core.query_attribution"] = sys.modules[__name__]


# ── OpenTelemetry SDK & RequestTelemetry ─────────────────────────────────────

_SERVICE_NAME = "fastly-log-analytics"
_TRACER_NAME = "backend.core.request_telemetry"

_init_lock = threading.Lock()
_initialised = False


def _otel_exporter() -> str:
    return os.environ.get("OTEL_EXPORTER", "none").strip().lower()


def effective_exporter() -> str:
    return _otel_exporter()


def _otel_enabled() -> bool:
    if os.environ.get("OTEL_ENABLED", "1") != "1" or os.environ.get("PYTEST_CURRENT_TEST") is not None:
        return False
    return _otel_exporter() != "none"


def _setup_sdk() -> None:
    """Install tracer + meter providers with the configured exporter (idempotent)."""
    global _initialised
    with _init_lock:
        if _initialised:
            return
        if not _otel_enabled():
            _initialised = True
            return

        exporter = _otel_exporter()
        metrics_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "").strip()
        traces_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "").strip()
        if exporter == "otlp":
            missing = [
                name
                for name, endpoint in (
                    ("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", metrics_endpoint),
                    ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", traces_endpoint),
                )
                if not endpoint
            ]
            if missing:
                raise ValueError(f"OTEL_EXPORTER=otlp requires explicit endpoints: {', '.join(missing)}")

        _initialised = True
        resource_attributes = {"service.name": _SERVICE_NAME, "service.instance.id": str(uuid4())}
        cluster = os.environ.get("ELEVATION_CLUSTER_NAME", "").strip()
        namespace = os.environ.get("POD_NAMESPACE", "").strip()
        if cluster:
            resource_attributes["site"] = cluster if cluster.startswith("elevation-") else f"elevation-{cluster}"
        if namespace:
            resource_attributes["namespace"] = namespace
        resource = Resource.create(resource_attributes)

        tracer_provider = TracerProvider(resource=resource)
        meter_readers: list[Any] = []

        if exporter == "console":
            tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
            meter_readers.append(PeriodicExportingMetricReader(ConsoleMetricExporter(), export_interval_millis=60_000))
        elif exporter == "prometheus":
            try:
                from opentelemetry.exporter.prometheus import PrometheusMetricReader

                meter_readers.append(PrometheusMetricReader())
            except ImportError:
                logger.warning("opentelemetry-exporter-prometheus not installed.")
        elif exporter == "otlp":
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_endpoint)))
            meter_readers.append(
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(endpoint=metrics_endpoint), export_interval_millis=15_000
                )
            )
        else:
            logger.warning(
                "OTEL_EXPORTER=%r is not a recognised value; install providers without exporters",
                exporter,
            )

        trace.set_tracer_provider(tracer_provider)
        metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=meter_readers))


def ensure_initialized() -> None:
    _setup_sdk()


def asgi_instrumentation_enabled() -> bool:
    return _otel_enabled()


def get_tracer() -> Tracer:
    _setup_sdk()
    return trace.get_tracer(_TRACER_NAME)


def get_meter() -> metrics.Meter:
    _setup_sdk()
    return metrics.get_meter(_TRACER_NAME)


def force_flush() -> None:
    """Bounded best-effort export without initializing disabled providers."""
    for get_provider in (metrics.get_meter_provider, trace.get_tracer_provider):
        try:
            flush = getattr(get_provider(), "force_flush", None)
            if flush is not None:
                flush(timeout_millis=5000)
        except Exception:
            logger.debug("force_flush: step failed (ignored)", exc_info=True)


_thread_wait_histogram: Any = None
_thread_wait_lock = threading.Lock()


def thread_wait_histogram() -> Any:
    """Histogram measuring `_Pool.acquire` wait time (ms)."""
    global _thread_wait_histogram
    if _thread_wait_histogram is None:
        with _thread_wait_lock:
            if _thread_wait_histogram is None:
                _thread_wait_histogram = get_meter().create_histogram(
                    name="app.thread_wait_ms",
                    description="DuckDB connection-pool acquire wait time",
                    unit="ms",
                )
    return _thread_wait_histogram


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


# ── Opt-in JSONL sink for client Web Vitals samples ──────────────────────────

_web_vitals_logger = logging.getLogger("backend.web_vitals")
LOG_PATH = Path("data") / "system" / "web_vitals.jsonl"
DEFAULT_MAX_MB = 200
_write_lock = threading.Lock()


def collection_enabled() -> bool:
    return os.environ.get("WEB_VITALS_COLLECT", "0").strip().lower() in ("1", "true", "yes", "on")


def rotated_path(path: Path | None = None) -> Path:
    p = path or LOG_PATH
    return p.with_name(p.name + ".1")


def _max_bytes() -> int:
    raw = os.environ.get("WEB_VITALS_MAX_MB")
    if raw is None or not raw.strip():
        mb: float = DEFAULT_MAX_MB
    else:
        try:
            mb = float(raw)
        except ValueError:
            mb = DEFAULT_MAX_MB
    return int(mb * 1024 * 1024) if mb > 0 else 0


def _rotate_if_needed() -> None:
    cap = _max_bytes()
    if cap <= 0:
        return
    try:
        if LOG_PATH.stat().st_size < cap:
            return
    except FileNotFoundError:
        return
    backup = rotated_path()
    try:
        backup.unlink(missing_ok=True)
        LOG_PATH.replace(backup)
    except OSError as exc:  # pragma: no cover - defensive
        _web_vitals_logger.warning("[web_vitals] log rotation failed: %s", exc)


def append_sample(sample: dict[str, Any]) -> None:
    """Append one Web Vitals sample as a single JSON line. Best-effort."""
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(sample, separators=(",", ":")) + "\n"
        with _write_lock:
            _rotate_if_needed()
            with LOG_PATH.open("a", encoding="utf-8") as fh:
                fh.write(line)
    except Exception as exc:  # pragma: no cover - defensive
        _web_vitals_logger.warning("[web_vitals] sample append failed: %s", exc)


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
    from backend.cron_progress import compute_sync_status_cached as _compute

    return _compute(*args, **kwargs)


def get_high_scale_service_registry(*args: Any, **kwargs: Any) -> Any:
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry as _get_registry

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
    from backend.core.iceberg._ducklake import ducklake_table_name

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
        from backend.core.metadata.cron_log import latest_cron_per_task
        from backend.core.query_instrumentation import InstrumentedDuckDBConnection
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

from backend.utils.telemetry import (  # noqa: E402, F401, I001
    _BANDS,
    _FIXED_TOKEN_DELTAS,
    _FOLDED_TELEMETRY_SUBMODULES,
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

_this_module = sys.modules[__name__]
for _alias in ("high_scale_sections", "query_attribution", "request_metrics"):
    sys.modules[f"backend.core.{_alias}"] = _this_module
sys.modules["backend.request_metrics_observer"] = _this_module

_core_pkg = sys.modules.get("backend.core")
if _core_pkg is not None:
    for _alias in ("high_scale_sections", "query_attribution", "request_metrics"):
        setattr(_core_pkg, _alias, _this_module)

_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None:
    setattr(_backend_pkg, "request_metrics_observer", _this_module)
    if hasattr(_backend_pkg, "_MODULE_ALIASES"):
        _backend_pkg._MODULE_ALIASES.update(
            {
                "backend.core.query_attribution": "backend.core.request_telemetry",
                "backend.core.request_metrics": "backend.core.request_telemetry",
                "backend.request_metrics_observer": "backend.core.request_telemetry",
                **{
                    f"backend.utils.{_submod_name}": "backend.utils.telemetry"
                    for _submod_name in _FOLDED_TELEMETRY_SUBMODULES
                },
            }
        )
