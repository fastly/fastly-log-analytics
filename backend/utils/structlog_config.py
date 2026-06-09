"""structlog configuration for v2.0.

Configures structlog with a custom processor that injects active OpenTelemetry
`trace_id` + `span_id` into every log record. Importing and calling
`configure_structlog()` from `backend.main` activates it process-wide.

Existing `logging.getLogger(__name__).info("...")` calls keep working —
structlog wraps stdlib logging by default. New code should prefer
`structlog.get_logger(__name__).info("event", a=a)` for structured key/value
pairs (more machine-readable than `%s`-formatted strings).

Output format:

- **Dev (TTY):** colored console output (`ConsoleRenderer`) — readable for a
  human running the dev server.
- **Production:** JSON-line output (`JSONRenderer`) — machine-parseable for
  log aggregation. Toggled by `STRUCTLOG_FORMAT=json`.

Both formats include `trace_id` and `span_id` when an OTel span is active,
empty strings otherwise. The trace_id format is the standard 32-hex string
that OTel exporters emit, so log records can be joined to OTel spans in any
downstream tool.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

import structlog
from opentelemetry import trace


def _add_otel_trace_context(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """structlog processor: inject active OTel trace_id + span_id."""
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        ctx = span.get_span_context()
        if ctx.is_valid:
            # OTel emits trace_id as a 128-bit int; the canonical wire
            # representation in logs/exporters is 32-hex zero-padded.
            event_dict["trace_id"] = format(ctx.trace_id, "032x")
            event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def configure_structlog() -> None:
    """Configure structlog process-wide. Idempotent."""
    # Phase 1 doesn't mandate stdlib propagation changes; default level
    # remains whatever the existing logging.basicConfig set up.
    use_json = os.environ.get("STRUCTLOG_FORMAT", "console").lower() == "json"

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_otel_trace_context,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if use_json:
        processors.append(structlog.processors.JSONRenderer())
    else:
        # ConsoleRenderer with sorted keys keeps tests deterministic and
        # the human-readable output predictable.
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=sys.stderr.isatty(),
                sort_keys=True,
            ),
        )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> Any:
    """Convenience re-export so callers don't import both structlog and
    this module."""
    return structlog.get_logger(name) if name else structlog.get_logger()
