"""RequestTelemetry — OpenTelemetry wrapper for per-request observability.

The v2.0 cleanup (Phase 1) consolidates the four fragmented custom telemetry
surfaces onto OpenTelemetry. This module owns the global tracer + meter
configuration and exposes a thin `RequestTelemetry` per-request facade that
holds the root span context.

Design constraints:

- **Lives next to the RequestContext** (ADR-02, Phase 2). `RequestContext`
  carries `RequestTelemetry` in a single attribute; routes never construct
  one directly.
- **Console exporter only.** v2.0 ships with the OTel console exporter (logs
  spans to stderr). Jaeger / Tempo / Honeycomb / etc. are deploy-config
  decisions for post-v2.0 (no SaaS dep per cleanup_plan.md).
- **Additive, not replacing.** Phase 1 emits OTel spans alongside the existing
  `backend.utils.telemetry` ContextVar machinery. The debug-panel renderer
  (Phase 1.5) reads both sources. Old surfaces are deleted incrementally in
  Phase 10 once OTel adoption is verified end-to-end.
- **Thread-wait metric.** Custom OTel histogram instrumented at
  `_Pool.acquire`. Phase 6 (cron isolation) reads its p95 to choose between
  "separate pool" and "separate process."

Module-level state is initialised lazily on first `get_tracer()` /
`get_meter()` call so unit tests that don't exercise FastAPI can import the
module without paying for SDK setup.
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Span, Tracer

_SERVICE_NAME = "fastly-log-analytics"
_TRACER_NAME = "backend.core.request_telemetry"

_init_lock = threading.Lock()
_initialised = False


def _otel_enabled() -> bool:
    """Whether to install the SDK exporters. Off by default in tests."""
    return os.environ.get("OTEL_ENABLED", "1") == "1" and os.environ.get("PYTEST_CURRENT_TEST") is None


def _setup_sdk() -> None:
    """Install console-exporter tracer + meter providers (idempotent).

    Called lazily from get_tracer/get_meter. Skipped under pytest unless
    OTEL_ENABLED=1 is explicitly set, so unit tests don't pay the cost of
    a background batch-span exporter thread.
    """
    global _initialised
    with _init_lock:
        if _initialised:
            return
        _initialised = True

        if not _otel_enabled():
            return

        resource = Resource.create({"service.name": _SERVICE_NAME})

        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(tracer_provider)

        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[
                PeriodicExportingMetricReader(
                    ConsoleMetricExporter(),
                    export_interval_millis=60_000,
                ),
            ],
        )
        metrics.set_meter_provider(meter_provider)


def get_tracer() -> Tracer:
    """Return the project tracer (initialises the SDK on first call)."""
    _setup_sdk()
    return trace.get_tracer(_TRACER_NAME)


def get_meter() -> metrics.Meter:
    """Return the project meter (initialises the SDK on first call)."""
    _setup_sdk()
    return metrics.get_meter(_TRACER_NAME)


# Custom instruments — accessed lazily so the SDK initialises only when
# someone records a sample. Wrapped in functions (not module-level globals)
# so test isolation works.

_thread_wait_histogram: Any = None
_thread_wait_lock = threading.Lock()


def thread_wait_histogram() -> Any:
    """Histogram measuring `_Pool.acquire` wait time (ms).

    Phase 6 reads the p95 of this metric to decide cron isolation strategy:
    p95 > 50ms during cron windows → escalate from separate-pool to
    separate-process. See ADR-03 + cleanup_plan.md §Phase 6.
    """
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
    """Per-request OTel facade.

    One instance per request, held on `RequestContext.telemetry`. Owns the
    root request span (entered in `start_request`, exited in `end_request`)
    and exposes helpers for per-section sub-spans, call attribution, query
    attribution, and cache-state metadata.

    Mirrors the public methods the debug-panel renderer expects so the wire
    shape of `_debug_calls` / `_debug_queries` / `_section_timings` can be
    derived from this object without reaching for the older ContextVar
    machinery in `backend.utils.telemetry`.
    """

    __slots__ = (
        "request_path",
        "request_method",
        "_root_span",
        "_root_ctx_token",
        "_section_timings",
        "_phase_log",
        "_t_start",
        "is_cached",
    )

    def __init__(self, request_method: str, request_path: str) -> None:
        self.request_method = request_method
        self.request_path = request_path
        self._root_span: Span | None = None
        self._root_ctx_token: Any = None
        self._section_timings: list[dict[str, Any]] = []
        self._phase_log: list[dict[str, Any]] = []
        self._t_start: float = 0.0
        self.is_cached: bool = False

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start_request(self) -> None:
        """Open the root request span. Idempotent."""
        if self._root_span is not None:
            return
        self._t_start = time.monotonic()
        tracer = get_tracer()
        self._root_span = tracer.start_span(
            name=f"http.{self.request_method.lower()}",
            attributes={
                "http.method": self.request_method,
                "http.route": self.request_path,
            },
        )

    def end_request(self, status_code: int | None = None) -> None:
        """Close the root request span and attach final attributes."""
        if self._root_span is None:
            return
        if status_code is not None:
            self._root_span.set_attribute("http.status_code", int(status_code))
        self._root_span.set_attribute("app.is_cached", bool(self.is_cached))
        self._root_span.set_attribute("app.total_ms", round((time.monotonic() - self._t_start) * 1000, 2))
        self._root_span.end()
        self._root_span = None

    # ── Section spans ─────────────────────────────────────────────────────

    @contextmanager
    def section(self, name: str, **attrs: Any):
        """Open a child span for a logical section of the request.

        Example:
            with ctx.telemetry.section("dashboard.aggregates"):
                ...
        """
        tracer = get_tracer()
        t0 = time.monotonic()
        with tracer.start_as_current_span(f"section:{name}") as span:
            for k, v in attrs.items():
                span.set_attribute(k, v)
            try:
                yield span
            finally:
                elapsed_ms = round((time.monotonic() - t0) * 1000, 2)
                span.set_attribute("app.section.elapsed_ms", elapsed_ms)
                self._section_timings.append({"section": name, "elapsed_ms": elapsed_ms})

    # ── Call / query attribution (mirrors backend.utils.telemetry API) ────

    def record_call(
        self,
        method: str,
        path: str,
        time_ms: float,
        status: int | str | None = None,
        service: str = "Fastly API",
        details: str | None = None,
        caller: str | None = None,
        bytes_count: int | None = None,
    ) -> None:
        """Emit a span event for an external call. Mirrored to the legacy
        ContextVar API in backend.utils.telemetry until Phase 10."""
        span = self._current_span()
        if span is None:
            return
        attrs: dict[str, Any] = {
            "app.call.method": method,
            "app.call.path": path,
            "app.call.time_ms": float(time_ms),
            "app.call.service": service,
        }
        if status is not None:
            attrs["app.call.status"] = str(status)
        if details:
            attrs["app.call.details"] = details
        if caller:
            attrs["app.call.caller"] = caller
        if bytes_count is not None:
            attrs["app.call.bytes"] = int(bytes_count)
        span.add_event(name="external_call", attributes=attrs)

    def record_query(self, sql: str, time_ms: float, label: str = "query") -> None:
        """Emit a span event for a DuckDB query."""
        span = self._current_span()
        if span is None:
            return
        span.add_event(
            name="db.query",
            attributes={
                "db.statement": sql.strip()[:4000],  # cap on event-attribute size
                "db.elapsed_ms": float(time_ms),
                "db.label": label,
            },
        )

    def record_phase(self, name: str, **attrs: Any) -> None:
        """Append to the phase log (cheaper than a full span, mirrors
        backend.utils.telemetry's _phase_log shape)."""
        entry = {"phase": name, **attrs}
        self._phase_log.append(entry)
        span = self._current_span()
        if span is not None:
            span.add_event(name=f"phase:{name}", attributes={k: str(v) for k, v in attrs.items()})

    # ── Debug-panel render shape ──────────────────────────────────────────

    def section_timings(self) -> list[dict[str, Any]]:
        return list(self._section_timings)

    def phase_log(self) -> list[dict[str, Any]]:
        return list(self._phase_log)

    # ── Internals ─────────────────────────────────────────────────────────

    def _current_span(self) -> Span | None:
        """The span events should attach to. Prefer the active span (from a
        nested section), fall back to the root span."""
        active = trace.get_current_span()
        # NonRecordingSpan is the default when no provider is active (test
        # mode); skip recording in that case.
        if active and active.is_recording():
            return active
        return self._root_span if self._root_span and self._root_span.is_recording() else None
