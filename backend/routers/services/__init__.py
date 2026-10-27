"""Services router package."""

from __future__ import annotations

import sys as _sys

from fastapi import APIRouter

_backend_mod = _sys.modules.get("backend")
if _backend_mod is not None and hasattr(_backend_mod, "_MODULE_ALIASES"):
    for _unwound in (
        "backend.routers.services.core",
        "backend.routers.provision",
        "backend.routers.bootstrap",
        "backend.routers.web_vitals",
        "backend.routers.debug",
    ):
        _backend_mod._MODULE_ALIASES.pop(_unwound, None)

from . import core  # noqa: E402
from .core import *  # noqa: E402, F403
from .core import (  # noqa: E402, F401
    _ANALYST_SAFE_SERVICE_FIELDS,
    _CRON_SCHEDULE_TTL,
    _LOGGING_SETTINGS_CACHE_TTL,
    _check_iceberg_type_lock,
    _cmcd_settings_response,
    _committed_duckdb_field_types,
    _cron_schedule_cache,
    _load_modifiable_custom_fields,
    _locked_iceberg_field_names,
    _logging_settings_cache,
    _persist_custom_fields_change,
    _persist_log_fields,
    _record_audit_best_effort,
    _split_custom_field_warnings,
    _sse,
    _store_fos_credentials,
    _trim_for_analyst,
    _validate_and_save_custom_field,
    _validate_log_format_version,
    audit_router,
    core_router,
    cron_router,
)

audit = core
cron = core
_sys.modules["backend.routers.services.audit"] = core
_sys.modules["backend.routers.services.cron"] = core

router = APIRouter()
router.include_router(core_router)
router.include_router(audit_router)
router.include_router(cron_router)
