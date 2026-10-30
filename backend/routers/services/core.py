"""Services management router — aggregates crud, vcl, custom_fields, views, and storage submodules."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys as _sys
import time
import types as _types
import urllib.parse
from collections.abc import Iterator
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from backend.core import duckdb as _db
from backend.core import log_fields as lf_module
from backend.core import metadata as metadata_db
from backend.core import storage as _storage
from backend.core.duckdb import ensure_cid_salt
from backend.core.log_fields import _filter_user_custom_fields, _is_system_field
from backend.core.request_context import get_service_id, get_source
from backend.models.common import (
    DEFAULT_ERROR_RESPONSES,
    AuditLogsResponse,
    CmcdSettingsResponse,
    CredentialsUpdateResponse,
    CronRunsResponse,
    CronScheduleResponse,
    CustomFieldCreate,
    CustomFieldResponse,
    CustomFieldsExportFile,
    CustomFieldsImportBody,
    CustomFieldsImportResponse,
    CustomFieldsListResponse,
    CustomFieldUpdate,
    LogFieldsResponse,
    LogFieldsUpdateRequest,
    LogFieldsUpdateResponse,
    LoggingSettingsResponse,
    OkMessageResponse,
    ServiceCredentialsBody,
    ServiceCronSettingsBody,
    ServicesListResponse,
    VclLintRequest,
    VclLintResponse,
)
from backend.repositories import assets as service_stats_repo
from backend.repositories._base import SectionTimer
from backend.repositories.assets import delete_cron_log, get_cron_logs, purge_cron_logs
from backend.utils.auth import require_service_in_scope
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.router_utils import (
    load_service_config,
    make_error,
    mutate_service_config,
    query_errors,
    raise_internal,
    stored_fastly_token,
)
from backend.utils.sse import sse_response

from . import crud, custom_fields, storage, vcl, views
from .crud import (
    _ANALYST_SAFE_SERVICE_FIELDS,
    _CRON_SCHEDULE_TTL,
    _cron_schedule_cache,
    _record_audit_best_effort,
    _sse,
    _store_fos_credentials,
    _trim_for_analyst,
    api_cron_schedule,
    api_ngwaf_sync,
    api_service_clear_time_range,
    api_service_cron_settings,
    api_service_update_credentials,
    api_services_list,
    cron_logs_stream,
)
from .custom_fields import (
    _load_modifiable_custom_fields,
    _persist_custom_fields_change,
    _split_custom_field_warnings,
    _validate_and_save_custom_field,
    api_create_custom_field,
    api_delete_custom_field,
    api_export_custom_fields,
    api_import_custom_fields,
    api_list_custom_fields,
    api_update_custom_field,
    api_validate_custom_vcl,
)
from .storage import (
    _check_storage_type_lock,
    _committed_duckdb_field_types,
    _locked_storage_field_names,
)
from .vcl import (
    _LOGGING_SETTINGS_CACHE_TTL,
    _cmcd_settings_response,
    _logging_settings_cache,
    _persist_log_fields,
    _validate_log_format_version,
    api_service_log_fields_get,
    api_service_log_fields_set,
    api_service_logging_settings,
    api_service_update_logging_settings,
)
from .views import (
    api_audit_logs,
    api_cron_log_delete,
    api_cron_logs,
    api_cron_logs_purge,
    audit_router,
    cron_router,
)

logger = logging.getLogger(__name__)

core_router = APIRouter(prefix="/api", tags=["services"], responses=DEFAULT_ERROR_RESPONSES)
core_router.include_router(crud.router)
core_router.include_router(vcl.router)
core_router.include_router(custom_fields.router)
core_router.include_router(storage.router)

_CORE_ROUTE_ORDER = {
    name: idx
    for idx, name in enumerate(
        (
            "api_services_list",
            "api_service_cron_settings",
            "api_service_clear_time_range",
            "cron_logs_stream",
            "api_cron_schedule",
            "api_service_update_credentials",
            "api_service_logging_settings",
            "api_service_log_fields_get",
            "api_service_log_fields_set",
            "api_service_update_logging_settings",
            "api_ngwaf_sync",
            "api_list_custom_fields",
            "api_create_custom_field",
            "api_update_custom_field",
            "api_delete_custom_field",
            "api_validate_custom_vcl",
            "api_export_custom_fields",
            "api_import_custom_fields",
        )
    )
}
core_router.routes.sort(key=lambda r: _CORE_ROUTE_ORDER.get(getattr(getattr(r, "endpoint", None), "__name__", ""), 999))

router = core_router

_CacheRegistry.register("services.core._cron_schedule_cache", _cron_schedule_cache)
_CacheRegistry.register("services.core._logging_settings_cache", _logging_settings_cache)

_SUBMODULES = (crud, vcl, custom_fields, views, storage)


class _ServicesShimModule(_types.ModuleType):
    """Forwards attribute writes/deletes across all services router submodules."""

    def __setattr__(self, name: str, value: object) -> None:
        super().__setattr__(name, value)
        for _mod in _SUBMODULES:
            _types.ModuleType.__setattr__(_mod, name, value)
        _pkg = _sys.modules.get("backend.routers.services")
        if _pkg is not None and _pkg is not self:
            _types.ModuleType.__setattr__(_pkg, name, value)

    def __delattr__(self, name: str) -> None:
        super().__delattr__(name)
        for _mod in _SUBMODULES:
            if hasattr(_mod, name):
                try:
                    _types.ModuleType.__delattr__(_mod, name)
                except AttributeError:
                    pass
        _pkg = _sys.modules.get("backend.routers.services")
        if _pkg is not None and _pkg is not self and hasattr(_pkg, name):
            try:
                _types.ModuleType.__delattr__(_pkg, name)
            except AttributeError:
                pass


_self_mod = _sys.modules.get(__name__)
if _self_mod is not None and not isinstance(_self_mod, _ServicesShimModule):
    _self_mod.__class__ = _ServicesShimModule

__all__ = [
    "AuditLogsResponse",
    "BoundedTTLCache",
    "CmcdSettingsResponse",
    "CredentialsUpdateResponse",
    "CronRunsResponse",
    "CronScheduleResponse",
    "CustomFieldCreate",
    "CustomFieldResponse",
    "CustomFieldsExportFile",
    "CustomFieldsImportBody",
    "CustomFieldsImportResponse",
    "CustomFieldsListResponse",
    "CustomFieldUpdate",
    "DEFAULT_ERROR_RESPONSES",
    "Depends",
    "HTTPException",
    "Iterator",
    "LogFieldsResponse",
    "LogFieldsUpdateRequest",
    "LogFieldsUpdateResponse",
    "LoggingSettingsResponse",
    "OkMessageResponse",
    "Query",
    "Request",
    "Response",
    "SectionTimer",
    "ServiceCredentialsBody",
    "ServiceCronSettingsBody",
    "ServicesListResponse",
    "StreamingResponse",
    "UTC",
    "VclLintRequest",
    "VclLintResponse",
    "_ANALYST_SAFE_SERVICE_FIELDS",
    "_CRON_SCHEDULE_TTL",
    "_LOGGING_SETTINGS_CACHE_TTL",
    "_check_storage_type_lock",
    "_cmcd_settings_response",
    "_committed_duckdb_field_types",
    "_cron_schedule_cache",
    "_db",
    "_filter_user_custom_fields",
    "_is_system_field",
    "_load_modifiable_custom_fields",
    "_locked_storage_field_names",
    "_logging_settings_cache",
    "_persist_custom_fields_change",
    "_persist_log_fields",
    "_record_audit_best_effort",
    "_split_custom_field_warnings",
    "_sse",
    "_storage",
    "_store_fos_credentials",
    "_trim_for_analyst",
    "_validate_and_save_custom_field",
    "_validate_log_format_version",
    "api_audit_logs",
    "api_create_custom_field",
    "api_cron_log_delete",
    "api_cron_logs",
    "api_cron_logs_purge",
    "api_cron_schedule",
    "api_delete_custom_field",
    "api_export_custom_fields",
    "api_import_custom_fields",
    "api_list_custom_fields",
    "api_ngwaf_sync",
    "api_service_clear_time_range",
    "api_service_cron_settings",
    "api_service_log_fields_get",
    "api_service_log_fields_set",
    "api_service_logging_settings",
    "api_service_update_credentials",
    "api_service_update_logging_settings",
    "api_services_list",
    "api_update_custom_field",
    "api_validate_custom_vcl",
    "asyncio",
    "audit_router",
    "core_router",
    "cron_logs_stream",
    "cron_router",
    "crud",
    "custom_fields",
    "datetime",
    "delete_cron_log",
    "ensure_cid_salt",
    "get_cron_logs",
    "get_service_id",
    "get_source",
    "json",
    "lf_module",
    "load_service_config",
    "logger",
    "make_error",
    "metadata_db",
    "mutate_service_config",
    "os",
    "purge_cron_logs",
    "query_errors",
    "raise_internal",
    "re",
    "require_service_in_scope",
    "router",
    "service_stats_repo",
    "sse_response",
    "storage",
    "stored_fastly_token",
    "time",
    "urllib",
    "vcl",
    "views",
]
