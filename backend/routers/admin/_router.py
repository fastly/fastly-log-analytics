"""Shared APIRouter instance for the admin package.

Lives in its own module so sub-modules can do
``from backend.routers.admin._router import router`` without triggering
the package-init's sub-module imports (which would be a circular
dependency).
"""

from __future__ import annotations

from fastapi import APIRouter

router: APIRouter = APIRouter(prefix="/api", tags=["admin"])
