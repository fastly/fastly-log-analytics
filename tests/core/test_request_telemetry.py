"""Tests for `backend.core.request_telemetry`.

Covers:
- Tracer + meter lazy initialisation (no SDK setup under pytest by default)
- RequestTelemetry lifecycle (start_request / end_request / idempotency)
- Section span context manager records timings
- record_call / record_query / record_phase emit events
- Debug-panel shape helpers (section_timings, phase_log)
- thread_wait_histogram instrument is constructed once

These tests run with OTEL_ENABLED=0 (the default under pytest), so the SDK
isn't actually installed — spans are NonRecording, events are no-ops, but
the public API surface still returns the expected shapes. A separate
`with_sdk` fixture exercises a real in-memory exporter.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from backend.core import request_telemetry


@pytest.fixture(autouse=True)
def _reset_module():
    """Reset module-level lazy state so SDK setup is re-attempted per test."""
    request_telemetry._initialised = False
    request_telemetry._thread_wait_histogram = None
    yield
    request_telemetry._initialised = False
    request_telemetry._thread_wait_histogram = None


def test_otel_disabled_under_pytest_by_default(monkeypatch):
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "true")
    assert request_telemetry._otel_enabled() is False


def test_otel_enabled_requires_exporter_to_be_set(monkeypatch):
    """The default OTEL_EXPORTER ('none') keeps the SDK uninstalled even
    when OTEL_ENABLED=1 — the old code spammed prod stdout with the
    ConsoleSpanExporter because exporter installation wasn't gated."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.delenv("OTEL_EXPORTER", raising=False)
    assert request_telemetry._otel_enabled() is False


def test_otel_enabled_when_exporter_is_console(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.setenv("OTEL_EXPORTER", "console")
    assert request_telemetry._otel_enabled() is True


def test_otel_master_switch_off_overrides_exporter(monkeypatch):
    """OTEL_ENABLED=0 wins even if an exporter is configured."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "0")
    monkeypatch.setenv("OTEL_EXPORTER", "console")
    assert request_telemetry._otel_enabled() is False


def test_get_tracer_returns_a_tracer():
    """No SDK installed in test mode → tracer returns NonRecordingSpan
    when spans are started. Public surface still works."""
    tracer = request_telemetry.get_tracer()
    assert tracer is not None


def test_setup_sdk_is_idempotent():
    request_telemetry._setup_sdk()
    request_telemetry._setup_sdk()
    request_telemetry._setup_sdk()
    # No exception, no duplicate provider registration.


def test_request_lifecycle_in_test_mode():
    """In test mode, spans don't record; the public API still returns
    sensible shapes so callers don't have to special-case."""
    ctx = request_telemetry.RequestTelemetry("GET", "/api/dashboard/aggregates")
    ctx.start_request()
    ctx.start_request()  # idempotent
    ctx.end_request(status_code=200)
    ctx.end_request()  # idempotent

    assert ctx.section_timings() == []
    assert ctx.phase_log() == []


def test_section_records_timing_metadata():
    """Even without an active SDK, the section helper appends a timing
    row to the debug-panel shape so the renderer has data to show."""
    ctx = request_telemetry.RequestTelemetry("GET", "/api/dashboard/aggregates")
    ctx.start_request()
    with ctx.section("dashboard.aggregates", expensive="true"):
        pass
    timings = ctx.section_timings()
    assert len(timings) == 1
    assert timings[0]["section"] == "dashboard.aggregates"
    assert timings[0]["elapsed_ms"] >= 0
    ctx.end_request()


def test_record_phase_appends_to_log():
    ctx = request_telemetry.RequestTelemetry("GET", "/api/dashboard/aggregates")
    ctx.start_request()
    ctx.record_phase("warmup", cached_temps=2)
    ctx.record_phase("query", rows=42)
    log = ctx.phase_log()
    assert log == [{"phase": "warmup", "cached_temps": 2}, {"phase": "query", "rows": 42}]


def test_record_call_and_record_query_do_not_raise_when_no_recording():
    ctx = request_telemetry.RequestTelemetry("GET", "/api/x")
    ctx.start_request()
    ctx.record_call("GET", "/v1/services", time_ms=12.3, service="Fastly API", status=200)
    ctx.record_query("SELECT 1", time_ms=0.4)
    ctx.end_request()


def test_thread_wait_histogram_constructed_once():
    """The lazy property should construct the instrument on first call
    and return the same object thereafter."""
    h1 = request_telemetry.thread_wait_histogram()
    h2 = request_telemetry.thread_wait_histogram()
    assert h1 is h2


# ── With a real in-memory SDK exporter ────────────────────────────────────────


def test_section_emits_real_span_when_sdk_enabled(monkeypatch):
    """Install an in-memory exporter and assert the section context
    manager produces a span with the expected name + attribute."""
    # Use a private tracer provider for this test (avoid touching the
    # module-level global SDK state which is wedge-prone in -n auto mode).
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with patch("backend.core.request_telemetry.get_tracer", return_value=provider.get_tracer("test")):
        ctx = request_telemetry.RequestTelemetry("GET", "/api/test")
        ctx.start_request()
        with ctx.section("test.section", custom="x"):
            pass
        ctx.end_request(status_code=200)

    spans = exporter.get_finished_spans()
    names = [s.name for s in spans]
    assert "section:test.section" in names
    section_span = next(s for s in spans if s.name == "section:test.section")
    assert section_span.attributes is not None
    assert section_span.attributes.get("custom") == "x"
    assert "app.section.elapsed_ms" in section_span.attributes
