"""Shared FastAPI router instance for the admin subpackage."""

from __future__ import annotations

from fastapi import APIRouter

from backend.models.common import DEFAULT_ERROR_RESPONSES

router = APIRouter(prefix="/api", tags=["admin"], responses=DEFAULT_ERROR_RESPONSES)

__all__ = ["router"]
