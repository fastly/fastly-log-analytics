from fastapi import APIRouter

from . import core as audit  # noqa: F401
from . import core as cron  # noqa: F401
from .core import *  # noqa: F403  # Export functions for tests that import from backend.routers.services directly
from .core import audit_router, cron_router
from .core import router as core_router

# Combine the sub-routers
router = APIRouter()
router.include_router(core_router)
router.include_router(audit_router)
router.include_router(cron_router)
