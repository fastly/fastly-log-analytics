from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from collections.abc import MutableMapping
from typing import Any

import structlog
from opentelemetry import trace

logger = logging.getLogger("backend.utils.telemetry_proxy")

_MAX_KEYS = 1024
_last: dict[str, float] = {}
_log_once_lock = threading.Lock()


def log_once_per(key: str, interval_s: float = 60.0) -> bool:
    now = time.monotonic()
    with _log_once_lock:
        prev = _last.get(key)
        if prev is not None and now - prev < interval_s:
            return False
        if len(_last) >= _MAX_KEYS:
            cutoff = now - interval_s
            for k in [k for k, t in _last.items() if t < cutoff]:
                del _last[k]
            if len(_last) >= _MAX_KEYS:
                _last.clear()
        _last[key] = now
        return True


_LOG_EVERY_S = 60.0


def _log_rate_limited(key: str, msg: str, *args: Any) -> None:
    if log_once_per(key, _LOG_EVERY_S):
        logger.warning(msg, *args)


def _add_otel_trace_context(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        ctx = span.get_span_context()
        if ctx.is_valid:
            event_dict["trace_id"] = format(ctx.trace_id, "032x")
            event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def _drop_uvicorn_color_message(
    _logger: Any, _method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    event_dict.pop("color_message", None)
    return event_dict


def effective_format() -> str:
    return "json" if os.environ.get("STRUCTLOG_FORMAT", "console").lower() == "json" else "console"


def configure_structlog() -> None:
    use_json = effective_format() == "json"
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
        foreign_pre_chain=[*shared_processors, structlog.stdlib.ExtraAdder(), _drop_uvicorn_color_message],
        processor=renderer,
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)


_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def bridge_uvicorn_loggers() -> None:
    for name in _UVICORN_LOGGERS:
        uv_logger = logging.getLogger(name)
        uv_logger.handlers = []
        uv_logger.propagate = True


audit_log = structlog.get_logger("audit")

ABS_PATH_RE = re.compile(r"(?<![A-Za-z0-9])/(?:srv|var|tmp|data|home|opt|usr|mnt|app)\b(?:/[A-Za-z0-9_./-]*)?")
CID_SALT_LITERAL_RE = re.compile(r"(sha256\(\s*CONCAT\(\s*')[^']*(')", re.IGNORECASE)
REDACTED = "[redacted]"
_MIN_SALT_LEN = 8
_known_salts: set[str] = set()
_salts_lock = threading.Lock()


def register_salt(salt: str | None) -> None:
    if isinstance(salt, str) and len(salt) >= _MIN_SALT_LEN:
        with _salts_lock:
            _known_salts.add(salt)


def scrub_secrets(text: str) -> str:
    with _salts_lock:
        salts = sorted(_known_salts, key=len, reverse=True)
    for salt in salts:
        text = text.replace(salt, REDACTED)
    return CID_SALT_LITERAL_RE.sub(r"\1[redacted-salt]\2", text)


def redact_error_text(msg: object) -> str:
    return ABS_PATH_RE.sub("<path>", scrub_secrets(str(msg)))
