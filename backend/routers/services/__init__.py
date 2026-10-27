"""Services router package."""
# ruff: noqa: F401, F403, E402, I001

from __future__ import annotations

import sys as _sys
import types as _types

from fastapi import APIRouter

from . import core, crud, custom_fields, storage, vcl, views  # noqa: E402
from .core import *  # noqa: E402, F403
from .core import (  # noqa: E402, F401
    _ANALYST_SAFE_SERVICE_FIELDS,
    _CRON_SCHEDULE_TTL,
    _LOGGING_SETTINGS_CACHE_TTL,
    _ServicesShimModule,
    _check_storage_type_lock,
    _cmcd_settings_response,
    _committed_duckdb_field_types,
    _cron_schedule_cache,
    _db,
    _filter_user_custom_fields,
    _is_system_field,
    _load_modifiable_custom_fields,
    _locked_storage_field_names,
    _logging_settings_cache,
    _persist_custom_fields_change,
    _persist_log_fields,
    _record_audit_best_effort,
    _split_custom_field_warnings,
    _sse,
    _storage,
    _store_fos_credentials,
    _trim_for_analyst,
    _validate_and_save_custom_field,
    _validate_log_format_version,
    audit_router,
    core_router,
    cron_router,
)

router = APIRouter()
router.include_router(core_router)
router.include_router(audit_router)
router.include_router(cron_router)

_PKG_SUBMODULES = (core, crud, vcl, custom_fields, views, storage)


class _ServicesPkgShimModule(_types.ModuleType):
    """Forwards attribute writes/deletes on backend.routers.services to core and all 5 submodules."""

    def __setattr__(self, name: str, value: object) -> None:
        super().__setattr__(name, value)
        for _mod in _PKG_SUBMODULES:
            _types.ModuleType.__setattr__(_mod, name, value)

    def __delattr__(self, name: str) -> None:
        super().__delattr__(name)
        for _mod in _PKG_SUBMODULES:
            if hasattr(_mod, name):
                try:
                    _types.ModuleType.__delattr__(_mod, name)
                except AttributeError:
                    pass


_pkg_mod = _sys.modules.get(__name__)
if _pkg_mod is not None and not isinstance(_pkg_mod, _ServicesPkgShimModule):
    _pkg_mod.__class__ = _ServicesPkgShimModule
