"""Standardized router error envelopes, correlation IDs, and query error mapping."""

from __future__ import annotations

import asyncio
import logging
import secrets
import uuid
from functools import wraps
from logging import Logger
from typing import NoReturn

from fastapi import HTTPException, Request

from backend.utils.pii_masking import (
    _MIN_SALT_LEN,
    ABS_PATH_RE,
    CID_SALT_LITERAL_RE,
    REDACTED,
    _known_salts,
    _salts_lock,
    redact_error_text,
    register_salt,
    scrub_secrets,
)

logger = logging.getLogger(__name__)


def current_request_id(request: Request | None = None) -> str | None:
    """The access-log request id: from ``request.state`` when given, else the per-request attribution contextvar."""
    rid = getattr(getattr(request, "state", None), "request_id", None) if request is not None else None
    if not (isinstance(rid, str) and rid):
        from backend.core.query_attribution import current_attribution

        attr = current_attribution.get()
        rid = getattr(attr, "request_id", None)
    return rid if isinstance(rid, str) and rid else None


def correlation_id(request: Request | None = None) -> str:
    """The request id when one exists, else a fresh id (the caller must log it alongside the failure)."""
    return current_request_id(request) or secrets.token_hex(8)


def bad_request(code: str) -> dict[str, str]:
    """Envelope for a 400 ``{"error": code}``. Pair with
    ``raise HTTPException(status_code=400, detail=bad_request("..."))``."""
    return {"error": code}


def not_found(code: str = "not_found") -> dict[str, str]:
    """Envelope for a 404 ``{"error": code}``. ``code`` defaults to the
    generic ``"not_found"`` for resources that have no domain-specific code."""
    return {"error": code}


def validation_failed(code: str, messages: list[str]) -> dict:
    """Envelope for a 422 ``{"error": code, "messages": [...]}``. Use when a
    request fails domain validation that Pydantic can't express."""
    return {"error": code, "messages": messages}


def make_error(
    code: str,
    message: str | None = None,
    *,
    error_id: str | None = None,
    **extras: object,
) -> dict[str, object]:
    """Build the unified error-envelope detail dict."""
    detail: dict[str, object] = {"error": code}
    if message is not None:
        detail["message"] = message
    if error_id is not None:
        detail["error_id"] = error_id
    detail.update(extras)
    return detail


def raise_internal(
    log: Logger,
    exc: BaseException,
    *,
    code: str = "request_failed",
    status: int = 500,
    message: str | None = None,
    request: Request | None = None,
) -> NoReturn:
    """Log the full exception server-side; raise a generic ``HTTPException``
    that does NOT echo the original exception message to the client."""
    error_id = uuid.uuid4().hex[:8]
    request_id = current_request_id(request)
    log.exception("%s [error_id=%s request_id=%s]", code, error_id, request_id)
    detail: dict[str, object] = {"error": code}
    if message is not None:
        detail["message"] = message
    detail["error_id"] = error_id
    if request_id:
        detail["request_id"] = request_id
    raise HTTPException(status_code=status, detail=detail) from exc


def _handle_query_error(
    e: Exception,
    *,
    logger_obj: Logger,
    code: str,
    status_code: int,
) -> NoReturn:
    if isinstance(e, HTTPException):
        raise e
    if isinstance(e, ValueError):
        raise HTTPException(status_code=400, detail=make_error("bad_request", redact_error_text(e))) from e
    if isinstance(e, LookupError):
        raise HTTPException(status_code=404, detail=make_error("not_found", redact_error_text(e))) from e
    raise_internal(logger_obj, e, code=code, status=status_code)


def query_errors(status_code: int = 400, *, code: str = "unhandled_error", logger_obj: Logger = logger):
    """Decorator that maps unhandled exceptions to the unified error envelope."""

    def decorator(fn):
        if asyncio.iscoroutinefunction(fn):

            @wraps(fn)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    _handle_query_error(e, logger_obj=logger_obj, code=code, status_code=status_code)

            return async_wrapper

        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                _handle_query_error(e, logger_obj=logger_obj, code=code, status_code=status_code)

        return wrapper

    return decorator


__all__ = [
    "ABS_PATH_RE",
    "CID_SALT_LITERAL_RE",
    "REDACTED",
    "_MIN_SALT_LEN",
    "_handle_query_error",
    "_known_salts",
    "_salts_lock",
    "bad_request",
    "correlation_id",
    "current_request_id",
    "make_error",
    "not_found",
    "query_errors",
    "raise_internal",
    "redact_error_text",
    "register_salt",
    "scrub_secrets",
    "validation_failed",
]
