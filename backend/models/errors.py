"""Shared error-response models for OpenAPI documentation.

Every router declares the canonical error envelope under ``responses=`` so the
generated TypeScript client + schemathesis can pattern-match on a typed shape
rather than substring-matching on free-text. The actual ``detail`` payload is
built by the helpers in ``backend.utils.router_utils`` (``bad_request``,
``not_found``, ``validation_failed``, ``raise_internal``).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ErrorDetail(BaseModel):
    """Inner detail object for the canonical error envelope.

    Allows extra fields because a handful of historical 403 sites attach
    context (``service``, ``invite_id``, etc.) alongside the canonical
    ``error`` code. Locking the shape down would force a coordinated
    rewrite of those handlers without changing observable behavior — the
    frontend's ``extractApiError`` already keys on ``detail.error`` and
    ignores extras.
    """

    model_config = ConfigDict(extra="allow")

    error: str
    """Machine-readable code the frontend keys on (e.g. ``"not_found"``)."""
    error_id: str | None = None
    """Server-side correlation id; populated by ``raise_internal`` on 5xx."""
    messages: list[str] | None = None
    """Optional human-readable messages, populated by ``validation_failed``."""


class ErrorEnvelope(BaseModel):
    """Canonical FastAPI HTTPException envelope shape."""

    detail: ErrorDetail


# ── Default responses= mapping ────────────────────────────────────────────────
#
# Apply to every router via ``APIRouter(responses=DEFAULT_ERROR_RESPONSES)``
# so the OpenAPI schema documents every error code the router tree actually
# raises. Per-endpoint ``responses=`` keyword arguments still override these
# entries when an endpoint needs a more specific shape (e.g. the 415 from
# provision.check_fos which uses an HTTP-standard reason phrase).
DEFAULT_ERROR_RESPONSES: dict[int | str, dict] = {
    400: {"model": ErrorEnvelope, "description": "Bad request"},
    401: {"model": ErrorEnvelope, "description": "Unauthenticated"},
    403: {"model": ErrorEnvelope, "description": "Forbidden"},
    404: {"model": ErrorEnvelope, "description": "Not found"},
    422: {"model": ErrorEnvelope, "description": "Validation failed"},
    429: {"model": ErrorEnvelope, "description": "Rate limited"},
    500: {"model": ErrorEnvelope, "description": "Internal error"},
    502: {"model": ErrorEnvelope, "description": "Upstream error"},
    503: {"model": ErrorEnvelope, "description": "Service unavailable"},
}
