"""OpenTelemetry SDK setup, DB spans, thread-wait histogram, and Web Vitals JSONL sink."""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any
from uuid import uuid4

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Span, Tracer
from opentelemetry.trace.status import Status, StatusCode

logger = logging.getLogger("backend.core.request_telemetry")

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
        cluster = os.environ.get("K8S_CLUSTER_NAME", "").strip()
        namespace = os.environ.get("POD_NAMESPACE", "").strip()
        if cluster:
            resource_attributes["site"] = cluster if cluster.startswith("k8s-") else f"k8s-{cluster}"
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


def _db_spans_enabled() -> bool:
    """True only when DB/IO spans can actually reach a backend.

    Spans export solely under the ``console`` and ``otlp`` exporters (the
    ``prometheus`` branch installs no span processor). ``_otel_enabled()`` is
    False by default, under pytest, and when the exporter is ``none``, so this
    short-circuits to near-zero cost on the hot path whenever telemetry is off.
    """
    try:
        return _otel_enabled() and _otel_exporter() in ("console", "otlp")
    except Exception:
        return False


def _start_db_span(kind: str, statement: str, service_id: str | None) -> Span | None:
    """Start a DB/IO span, or return None if telemetry is off or anything fails.

    Never raises: callers wrap query execution, so a telemetry fault must never
    surface as a query error. Calls the idempotent ``_setup_sdk`` so worker
    processes that run queries without an HTTP request still export.
    """
    if not _db_spans_enabled():
        return None
    try:
        _setup_sdk()
        tracer = trace.get_tracer(_TRACER_NAME)
        span = tracer.start_span(f"db.{kind}")
        span.set_attribute("db.system", kind)
        if statement:
            span.set_attribute("db.statement", statement[:2000])
        if service_id:
            span.set_attribute("fla.service_id", service_id)
        return span
    except Exception:
        return None


def _end_db_span(span: Span | None, error: BaseException | None = None) -> None:
    """End a span started by ``_start_db_span`` (idempotent, never raises)."""
    if span is None:
        return
    try:
        if error is not None:
            span.record_exception(error)
            span.set_status(Status(StatusCode.ERROR, str(error)))
        span.end()
    except Exception:
        logger.debug("_end_db_span failed (ignored)", exc_info=True)


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
