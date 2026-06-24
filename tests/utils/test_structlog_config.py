"""Tests for `backend.utils.structlog_config`.

Covers:
- `configure_structlog` is idempotent
- The `_add_otel_trace_context` processor injects trace_id/span_id when a
  span is active
- The processor is a no-op when no span is active (does not raise)
- `get_logger` returns a structlog BoundLogger
"""

from __future__ import annotations

import json
import logging
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
    log = structlog.get_logger("json-test")
    # Calling .info() on a json-configured logger does not raise.
    log.info("event", k=1)


def test_bridge_uvicorn_loggers_repoints_them_at_root(monkeypatch, capsys):
    """uvicorn's loggers must propagate to the structlog root handler with no
    private handlers of their own, so their lines render as JSON (not uvicorn's
    plaintext ``INFO: …``) and carry the shared processor chain.

    Regression guard: without ``bridge_uvicorn_loggers`` the access log stays on
    uvicorn's own handler (propagate=False) and emits plaintext into an
    otherwise-JSON stream — see backend/utils/structlog_config.py.
    """
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()

    # Simulate uvicorn's default LOGGING_CONFIG: a private handler +
    # propagate=False on each uvicorn logger.
    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        lg.handlers = [logging.NullHandler()]
        lg.propagate = False

    structlog_config.bridge_uvicorn_loggers()

    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        assert lg.handlers == [], f"{name} should have no private handlers"
        assert lg.propagate is True, f"{name} should propagate to root"

    # End-to-end: an access-style record now renders through the root structlog
    # handler as JSON, not uvicorn's plaintext formatter.
    logging.getLogger("uvicorn.access").info('127.0.0.1 - "GET /x HTTP/1.1" 200')
    line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == '127.0.0.1 - "GET /x HTTP/1.1" 200'


def test_stdlib_extra_fields_are_rendered(monkeypatch, capsys):
    """Fields passed via stdlib ``logging``'s ``extra={...}`` must survive
    the bridge into structlog and appear in the rendered output.

    Regression guard: without ``structlog.stdlib.ExtraAdder`` on the
    foreign pre-chain these keys are silently dropped, collapsing every
    web_vitals metric to a bare ``web_vitals`` line (see
    backend/routers/web_vitals.py).
    """
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()

    logging.getLogger("backend.web_vitals").info(
        "web_vitals",
        extra={"web_vitals_name": "LCP", "web_vitals_rating": "good"},
    )

    line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "web_vitals"
    assert record["web_vitals_name"] == "LCP"
    assert record["web_vitals_rating"] == "good"


def test_uvicorn_color_message_extra_is_dropped(monkeypatch, capsys):
    """uvicorn attaches ``extra={"color_message": ...}`` (ANSI + ``%``-template)
    to its boot/access records for its own colorizer. ExtraAdder would surface
    it as a noisy ``color_message='...'`` kv pair on every uvicorn line; the
    bridge must strip it.

    Regression guard for the ``Uvicorn running on ... color_message='...'``
    line that showed up in container logs once uvicorn was bridged through
    structlog (see ``_drop_uvicorn_color_message``).
    """
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()

    logging.getLogger("uvicorn.error").info(
        "Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)",
        extra={"color_message": "Uvicorn running on \x1b[1m%s://%s:%d\x1b[0m (Press CTRL+C to quit)"},
    )

    line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)"
    assert "color_message" not in record
