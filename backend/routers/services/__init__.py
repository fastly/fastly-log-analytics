from fastapi import APIRouter

from .audit import router as audit_router
from .core import *  # noqa: F403  # Export functions for tests that import from backend.routers.services directly
from .core import router as core_router
from .cron import router as cron_router

# Combine the sub-routers
router = APIRouter()
router.include_router(core_router)
router.include_router(audit_router)
router.include_router(cron_router)
