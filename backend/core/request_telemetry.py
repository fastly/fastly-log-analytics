"""RequestTelemetry — OpenTelemetry wrapper for per-request observability.

The v2.0 cleanup (Phase 1) consolidates the four fragmented custom telemetry
surfaces onto OpenTelemetry. This module owns the global tracer + meter
configuration and exposes a thin `RequestTelemetry` per-request facade that
holds the root span context.

Design constraints:

- **Lives next to the RequestContext** (ADR-02, Phase 2). `RequestContext`
  carries `RequestTelemetry` in a single attribute; routes never construct
  one directly.
- **Exporter is opt-in.** Default is no exporter — the SDK isn't installed,
  spans/metrics record against the global no-op providers, nothing leaves
  the process. Set ``OTEL_EXPORTER=console`` to install ConsoleSpan /
  ConsoleMetric exporters (loud; useful for local dev), or
  ``OTEL_EXPORTER=otlp`` to export over OTLP/HTTP — endpoints come from the
  standard ``OTEL_EXPORTER_OTLP_{METRICS,TRACES}_ENDPOINT`` env vars, both of
  which must be explicit to avoid the SDK's localhost fallback (see
  docker-compose.observability.yml for the local Prometheus + Tempo wiring).
  Console-by-default was the v2.0 ship state and produced a ~1 MB/min
  stdout dump in prod.
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

import json
import logging
import os
import threading
from pathlib import Path

logger = logging.getLogger(__name__)
import time
from typing import Any
from uuid import uuid4

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


def _otel_exporter() -> str:
    """Which exporter to install. Default ``none`` — see module docstring.

    Returns the env var lowercased and stripped so callers don't have to
    normalise. Unknown values fall through to ``_setup_sdk`` which logs and
    treats them as ``none``.
    """
    return os.environ.get("OTEL_EXPORTER", "none").strip().lower()


def effective_exporter() -> str:
    """The OTel exporter mode actually in effect (``none`` / ``console`` / ``otlp``).

    SRE-20: surfaced on the admin health snapshot. With the prod default
    (``none``) no span records, so the slow_queries request_id is app-minted
    (SRE-01) and structlog's trace fields are empty — the operator needs the
    runtime mode, not the ADR's aspirational wiring.
    """
    return _otel_exporter()


def _otel_enabled() -> bool:
    """Whether to install SDK providers + exporters.

    Off when any of:
      - ``OTEL_ENABLED=0`` (master off-switch; preserves the old escape hatch)
      - Running under pytest (``PYTEST_CURRENT_TEST`` set) — keeps the test
        suite cheap; individual tests opt in via the ``with_sdk`` pattern.
      - ``OTEL_EXPORTER=none`` (default) — no point spinning up provider
        machinery when nothing will be exported.
    """
    if os.environ.get("OTEL_ENABLED", "1") != "1":
        return False
    if os.environ.get("PYTEST_CURRENT_TEST") is not None:
        return False
    return _otel_exporter() != "none"


def _setup_sdk() -> None:
    """Install tracer + meter providers with the configured exporter (idempotent).

    Called lazily from get_tracer/get_meter. No-op when ``_otel_enabled()``
    is false, which is the production default (OTEL_EXPORTER unset → none).
    """
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
        # Concurrent backend/replay processes must not overwrite each other's
        # cumulative counters at the OTLP receiver. One identity per SDK, not
        # per query, batch, or tenant; Prometheus maps it to `instance`.
        resource_attributes = {
            "service.name": _SERVICE_NAME,
            "service.instance.id": str(uuid4()),
        }
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
            meter_readers.append(
                PeriodicExportingMetricReader(
                    ConsoleMetricExporter(),
                    export_interval_millis=60_000,
                )
            )

        elif exporter == "prometheus":
            try:
                from opentelemetry.exporter.prometheus import PrometheusMetricReader

                meter_readers.append(PrometheusMetricReader())
            except ImportError:
                import logging as _logging

                _logging.getLogger(__name__).warning("opentelemetry-exporter-prometheus not installed.")
        elif exporter == "otlp":
            # The OTLP/HTTP exporters resolve their targets from the standard
            # OTEL_EXPORTER_OTLP_{METRICS,TRACES}_ENDPOINT env vars. Import is
            # deferred so the base install doesn't require the exporter
            # package unless this mode is selected.
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_endpoint)))
            meter_readers.append(
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(endpoint=metrics_endpoint),
                    export_interval_millis=15_000,
                )
            )
        else:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "OTEL_EXPORTER=%r is not a recognised value; install providers without exporters",
                exporter,
            )

        trace.set_tracer_provider(tracer_provider)
        metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=meter_readers))


def ensure_initialized() -> None:
    """Install the SDK now instead of on first ``get_tracer``/``get_meter`` call.

    ``main.py`` calls this before ``FastAPIInstrumentor.instrument_app`` at
    import time. The OTel API's tracer/meter proxies resolve the *current*
    global provider on every call, so ordering wouldn't matter for a single
    already-migrated route — but a process whose first HTTP request lands on
    a route that never calls ``get_tracer()`` (still most of ``routers/``;
    see ``build_request_context`` migration status) would otherwise never
    trigger ``_setup_sdk``, leaving FastAPIInstrumentor's spans permanently
    no-op for that process.
    """
    _setup_sdk()


def asgi_instrumentation_enabled() -> bool:
    """Whether ``main.py`` should wrap the app in ``FastAPIInstrumentor``.

    Same gate as ``_otel_enabled`` — no point paying the middleware-wrapping
    cost (extra ASGI layers on every request, even as no-ops) when nothing
    will ever be exported.
    """
    return _otel_enabled()


def get_tracer() -> Tracer:
    """Return the project tracer (initialises the SDK on first call)."""
    _setup_sdk()
    return trace.get_tracer(_TRACER_NAME)


def get_meter() -> metrics.Meter:
    """Return the project meter (initialises the SDK on first call)."""
    _setup_sdk()
    return metrics.get_meter(_TRACER_NAME)


def force_flush() -> None:
    """Bounded best-effort export, including short-lived operator commands.

    Never initialize a provider just to flush; disabled/dry-run stays inert.
    """
    for get_provider in (metrics.get_meter_provider, trace.get_tracer_provider):
        try:
            provider = get_provider()
            flush = getattr(provider, "force_flush", None)
            if flush is not None:
                flush(timeout_millis=5000)
        except Exception:
            logger.debug("force_flush: step failed (ignored)", exc_info=True)


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

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start_request(self) -> None:
        """Open the root request span. Idempotent.

        When FastAPIInstrumentor is wrapping the app (see main.py), it has
        already opened a per-request SERVER span by the time any route
        dependency runs — reuse it instead of opening a second one, or a
        span-metrics processor (Tempo's) double-counts every request that
        also goes through build_request_context. Only start our own span
        (and only end it in end_request) when nothing is already active —
        the RequestContext-free legacy routes, and test mode.
        """
        if self._root_span is not None:
            return
        self._t_start = time.monotonic()
        ambient = trace.get_current_span()
        if ambient is not None and ambient.is_recording():
            self._root_span = ambient
            self._owns_root_span = False
            return
        tracer = get_tracer()
        self._root_span = tracer.start_span(
            # OTel HTTP semantic convention: "{METHOD} {route}". The span
            # NAME (not just an attribute) is what a span-metrics processor
            # groups by, so this is what makes per-route RED metrics
            # possible instead of collapsing every request to "http.get"/
            # "http.post". request_path is the route template, not the raw
            # URL — see request_context.py's build_request_context.
            name=f"{self.request_method} {self.request_path}",
            attributes={
                "http.method": self.request_method,
                "http.route": self.request_path,
            },
        )
        self._owns_root_span = True

    def elapsed_ms(self) -> float:
        """Milliseconds since :meth:`start_request` (0.0 before it has run)."""
        return (time.monotonic() - self._t_start) * 1000 if self._t_start else 0.0

    def end_request(self, status_code: int | None = None) -> None:
        """Attach final attributes and close the root span — but only if
        this instance opened it. A borrowed (FastAPIInstrumentor) span
        closes itself when its own ASGI middleware sees the response finish.
        """
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
            from backend.utils.telemetry import get_page_load_id

            if get_page_load_id():
                from backend.core.metadata import usage_log

                usage_log.flush_telemetry_buffer()
        except Exception:
            logger.debug("end_request: flush_telemetry_buffer failed (ignored)", exc_info=True)


# ── Opt-in JSONL sink for client Web Vitals samples ──────────────────────────

_web_vitals_logger = logging.getLogger("backend.web_vitals")

# Single source of truth for the sink location. Relative to the process
# CWD (repo root in dev, ``/app`` in the container) — the same convention
# ``metric_snapshots.py`` uses for ``data/system``. The analyzer script
# mirrors this path (see scripts/analyze_web_vitals.py).
LOG_PATH = Path("data") / "system" / "web_vitals.jsonl"

# Default active-file size cap in MB, overridable via WEB_VITALS_MAX_MB.
DEFAULT_MAX_MB = 200

# Serializes the append (and the rotation it may trigger) so concurrent
# requests can't interleave a partial line or race the rename.
_write_lock = threading.Lock()


def collection_enabled() -> bool:
    """True when ``WEB_VITALS_COLLECT`` is set to a truthy value.

    Default off: collection is opt-in, so neither dev nor prod writes
    anything (and the frontend, which mirrors this flag via
    ``/api/bootstrap``, sends nothing) until it's explicitly enabled.
    """
    return os.environ.get("WEB_VITALS_COLLECT", "0").strip().lower() in ("1", "true", "yes", "on")


def rotated_path(path: Path | None = None) -> Path:
    """The single rotated-backup path for ``path`` (``…jsonl`` -> ``…jsonl.1``)."""
    p = path or LOG_PATH
    return p.with_name(p.name + ".1")


def _max_bytes() -> int:
    """Active-file size cap in bytes from ``WEB_VITALS_MAX_MB``.

    Defaults to ``DEFAULT_MAX_MB``. A value <= 0 (or unparseable) disables
    the cap entirely (unbounded growth — only for deliberate use).
    """
    raw = os.environ.get("WEB_VITALS_MAX_MB")
    if raw is None or raw.strip() == "":
        mb: float = DEFAULT_MAX_MB
    else:
        try:
            mb = float(raw)
        except ValueError:
            mb = DEFAULT_MAX_MB
    return int(mb * 1024 * 1024) if mb > 0 else 0


def _rotate_if_needed() -> None:
    """Rotate the active file to a single ``.1`` backup once it hits the cap.

    Caller must hold ``_write_lock``. Keeps at most one rotated segment so
    the most recent window is always retained (rather than freezing on the
    oldest data, which a plain drop-when-full would do).
    """
    cap = _max_bytes()
    if cap <= 0:
        return
    try:
        size = LOG_PATH.stat().st_size
    except FileNotFoundError:
        return
    if size < cap:
        return
    backup = rotated_path()
    try:
        backup.unlink(missing_ok=True)
        LOG_PATH.replace(backup)  # atomic rename within the same dir
    except OSError as exc:  # pragma: no cover - defensive
        _web_vitals_logger.warning("[web_vitals] log rotation failed: %s", exc)


def append_sample(sample: dict[str, Any]) -> None:
    """Append one Web Vitals sample as a single JSON line. Best-effort.

    Telemetry must never break the request that produced it, so any IO
    error is logged and swallowed rather than propagated.
    """
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(sample, separators=(",", ":")) + "\n"
        with _write_lock:
            _rotate_if_needed()
            with LOG_PATH.open("a", encoding="utf-8") as fh:
                fh.write(line)
    except Exception as exc:  # pragma: no cover - defensive
        _web_vitals_logger.warning("[web_vitals] sample append failed: %s", exc)
