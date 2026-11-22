"""Shared FastAPI router instance for the admin subpackage."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from backend.core.request_context import require_admin
from backend.models.common import DEFAULT_ERROR_RESPONSES

router = APIRouter(
    prefix="/api",
    tags=["admin"],
    responses=DEFAULT_ERROR_RESPONSES,
    dependencies=[Depends(require_admin)],
)

__all__ = ["router"]
