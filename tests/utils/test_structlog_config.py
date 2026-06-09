"""Tests for `backend.utils.structlog_config`.

Covers:
- `configure_structlog` is idempotent
- The `_add_otel_trace_context` processor injects trace_id/span_id when a
  span is active
- The processor is a no-op when no span is active (does not raise)
- `get_logger` returns a structlog BoundLogger
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
import structlog
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from backend.utils import structlog_config


@pytest.fixture(autouse=True)
def _reset_structlog():
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()


def test_configure_structlog_is_idempotent():
    structlog_config.configure_structlog()
    structlog_config.configure_structlog()
    # No exception. structlog.is_configured() should be True after.
    assert structlog.is_configured()


def test_get_logger_returns_a_bound_logger():
    structlog_config.configure_structlog()
    log = structlog_config.get_logger("test")
    assert log is not None
    # BoundLoggerLazyProxy resolves to a real logger on first method call.
    log.info("test event", value=1)


def test_otel_trace_processor_skips_when_no_active_span():
    """When no recording span is active, the processor leaves the event
    dict unchanged."""
    event_dict: dict = {"event": "test", "key": "value"}
    out = structlog_config._add_otel_trace_context(None, "info", event_dict)
    assert "trace_id" not in out
    assert "span_id" not in out


def test_otel_trace_processor_injects_trace_id_when_span_recording():
    """When a recording span is active, the processor adds 32-hex
    trace_id and 16-hex span_id keys."""
    provider = TracerProvider()
    tracer = provider.get_tracer("test")

    with patch.object(trace, "get_current_span", wraps=trace.get_current_span):
        with tracer.start_as_current_span("test-span"):
            event_dict: dict = {"event": "test"}
            out = structlog_config._add_otel_trace_context(None, "info", event_dict)

    assert "trace_id" in out
    assert "span_id" in out
    assert len(out["trace_id"]) == 32
    assert len(out["span_id"]) == 16
    # Both are hex.
    int(out["trace_id"], 16)
    int(out["span_id"], 16)


def test_configure_structlog_with_json_format(monkeypatch):
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()
    log = structlog_config.get_logger("json-test")
    # Calling .info() on a json-configured logger does not raise.
    log.info("event", k=1)
