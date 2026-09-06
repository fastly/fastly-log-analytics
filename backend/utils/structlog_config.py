"""structlog configuration with stdlib bridge.

Configures structlog AND the stdlib root logger to share a single processor
chain + sink. After `configure_structlog()` runs:

- `structlog.get_logger(__name__).info("event", a=a)` — structured-first
  callers; their kv pairs survive end-to-end.
- `logging.getLogger(__name__).info("msg %s", arg)` — legacy stdlib callers;
  the record is bridged into structlog's `ProcessorFormatter` via
  `foreign_pre_chain`, so it picks up OTel trace_id + the same JSON / console
  rendering with no per-callsite change.

Pre-bridge state used `PrintLoggerFactory(file=sys.stderr)`, which gave
structlog its own sink. Stdlib calls stayed on the basicConfig handler and
never picked up OTel context. The new wiring uses
`structlog.stdlib.LoggerFactory` + `ProcessorFormatter` so both routes
converge on one handler.

Output format (chosen by `STRUCTLOG_FORMAT`, default `console`):

- **`console` (default — what dev AND prod emit today):** human-readable
  output (`ConsoleRenderer`). Prod runs this because it does not set
  `STRUCTLOG_FORMAT`; the only reader of container stdout right now is a human
  via `docker logs`, for whom console beats JSON.
- **`json` (opt-in, currently dormant):** JSON-line output (`JSONRenderer`) —
  machine-parseable for a log aggregator. The renderer is wired and ready, but
  nothing consumes it today (no aggregator is provisioned — see ADR-08 §3).
  Flip `STRUCTLOG_FORMAT=json` the day one is and logs ship as JSON with no
  other change.

Both formats include `trace_id` and `span_id` when an OTel span is active,
empty strings otherwise. The trace_id format is the standard 32-hex string
that OTel exporters emit, so log records can be joined to OTel spans in any
downstream tool.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import MutableMapping
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


def _drop_uvicorn_color_message(
    _logger: Any, _method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """structlog processor: drop uvicorn's internal ``color_message`` extra.

    uvicorn attaches ``extra={"color_message": "...\\x1b[1m%s\\x1b[0m..."}`` to
    its startup/access records so its *own* colorizing formatter can print a
    bold variant of the same message. We bridge those records through
    ``ExtraAdder`` (which surfaces every ``extra`` key), so without this the raw
    ANSI + ``%``-template string leaks onto each uvicorn line as a noisy
    ``color_message='Uvicorn running on \\x1b[1m%s://...'`` kv pair. The rendered
    ``event`` already carries the message, so the key is pure noise here.
    """
    event_dict.pop("color_message", None)
    return event_dict


def effective_format() -> str:
    """The renderer that's actually active: ``"json"`` or ``"console"``.

    SRE-20: surfaced on the admin health snapshot so an incident responder
    knows whether a ``jq '.status==500'`` reach over ``docker logs`` will
    match anything. Prod leaves ``STRUCTLOG_FORMAT`` unset → ``console``,
    despite ADR-08 wiring the JSON path — so the honest runtime answer can't
    be inferred from the ADR alone.
    """
    return "json" if os.environ.get("STRUCTLOG_FORMAT", "console").lower() == "json" else "console"


def configure_structlog() -> None:
    """Configure structlog + bridge stdlib through it. Idempotent.

    Bridging means a single handler on the root logger runs every record —
    structlog-native or stdlib-foreign — through the same processor chain.
    Re-running the function replaces the root handler in place rather than
    duplicating it, so test fixtures and uvicorn reloads stay clean.
    """
    use_json = os.environ.get("STRUCTLOG_FORMAT", "console").lower() == "json"

    # Processors run on EVERY record. structlog-native records hit these via
    # ``structlog.configure(processors=…)`` below; stdlib-foreign records hit
    # them via ``ProcessorFormatter(foreign_pre_chain=…)`` on the handler.
    # The terminal renderer (JSON / Console) is applied uniformly through
    # ``ProcessorFormatter.processor`` so both routes look identical on the
    # wire.
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_otel_trace_context,
        structlog.processors.StackInfoRenderer(),
    ]

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if use_json
        else structlog.dev.ConsoleRenderer(
            colors=sys.stderr.isatty(),
            sort_keys=True,
            # structlog's default exception_formatter renders via
            # rich.traceback with show_locals=True. Every `logger.warning(...,
            # exc_info=True)` call in this codebase (there are many, e.g. the
            # RUM Faro-sync cron) runs through this renderer, and several of
            # those call sites hold live secrets in scope when they raise
            # (FOS access key/secret, Fastly API tokens) — show_locals=True
            # dumps the full contents of every frame's local variables,
            # including those secrets, straight into container logs on any
            # exception. Disabling it here is a single global choke point
            # rather than auditing/redacting every current and future
            # exc_info=True call site individually.
            exception_formatter=structlog.dev.RichTracebackFormatter(
                color_system="truecolor" if sys.stderr.isatty() else None,
                show_locals=False,
            ),
        )
    )

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        # ExtraAdder surfaces the ``extra={...}`` fields stdlib callers pass
        # (e.g. web_vitals' metric name/value/rating in routers/web_vitals.py).
        # Without it those keys are silently dropped from both console + JSON
        # output, collapsing every metric to a bare ``web_vitals`` line.
        # Native structlog callers pass kwargs directly and don't need it, so
        # it lives only on the foreign (stdlib-bridged) pre-chain.
        foreign_pre_chain=[*shared_processors, structlog.stdlib.ExtraAdder(), _drop_uvicorn_color_message],
        processor=renderer,
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    # Replace handlers wholesale so a second configure_structlog() call
    # (tests, uvicorn reload) doesn't double-print every record.
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)


# uvicorn installs its own handlers on these loggers (propagate=False) from
# its default LOGGING_CONFIG, so their records never reach the root handler
# configure_structlog() owns. bridge_uvicorn_loggers() re-points them at root.
_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def bridge_uvicorn_loggers() -> None:
    """Route uvicorn's loggers through the shared structlog root handler.

    Without ``--log-config``, uvicorn's ``configure_logging()`` installs a
    private ``StreamHandler`` + plaintext formatter on ``uvicorn`` /
    ``uvicorn.error`` / ``uvicorn.access`` with ``propagate=False``. Those
    records stop at uvicorn's handlers and never hit the ``ProcessorFormatter``
    on root — so access lines render as plaintext ``INFO: …`` (no JSON, no OTel
    ``trace_id``) even when the rest of the app logs JSON. This clears their
    handlers and re-enables propagation so they ascend to root and render
    through the same chain as every other log line.

    Timing: uvicorn runs ``configure_logging()`` once at server start, *before*
    the app's lifespan startup. This MUST therefore be called from lifespan
    startup (not at import), or uvicorn's later setup overwrites it. The two
    boot lines uvicorn emits before lifespan ("Started server process",
    "Waiting for application startup") still use uvicorn's formatter — they
    fire before any hook can run. Everything post-startup, including all access
    logs, is bridged. Idempotent.
    """
    for name in _UVICORN_LOGGERS:
        logger = logging.getLogger(name)
        # Clear uvicorn's own handlers so the record isn't ALSO emitted there
        # (which would double-log once propagation is on).
        logger.handlers = []
        logger.propagate = True


# Dedicated audit-action logger. Same processor chain as the rest of
# structlog (so OTel trace_id + structured kv pairs survive), but a stable
# logger name (``audit``) that downstream log routing can grep for or split
# into a dedicated stream. Today it routes through the same sink as every
# other log; promoting to a separate file is a one-line change in whichever
# infra layer (loki, vector, fluent-bit) ingests the JSON.
#
# Use for actions an operator may need to reconstruct post-incident:
# query cancellations, share-passcode revocations, manual cron triggers,
# etc. Always pass structured kwargs (``actor``, ``target``, identifying
# ids); never embed the same info in a free-text message.
audit_log = structlog.get_logger("audit")
