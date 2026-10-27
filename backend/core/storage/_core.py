"""DuckLake / Parquet integration for FOS log analysis — compatibility re-export shim.

The implementation is split across four cohesive domain modules under
``backend/core/storage/``:

- ``catalog.py``: DuckLake catalog attach, secret/extension management, table
  DDL/schemas/migrations, write admission, and shared storage locks/state/helpers.
- ``view.py``: ``logs`` and RUM view stitching, schema evolution, view cache,
  ``execute_with_stale_view_retry``, and snapshot calendar/manifest queries.
- ``buffer.py``: Local Parquet buffer staging, tombstones, quarantine, and
  promotion/commit.
- ``compaction.py``: Local hourly/daily/weekly compaction and DuckLake
  maintenance (optimize, retention, snapshot expiry).

This module re-exports the historical ``backend.core.storage._core`` surface and
keeps attribute mutations synchronized across ``storage``, ``_core``, ``catalog``,
``view``, ``buffer``, and ``compaction`` so monkeypatches on any path reach all
call sites.
"""

from __future__ import annotations

import contextlib
import glob as _glob
import hashlib
import logging
import os
import re
import shutil
import sys
import threading
import time
import types as _types
import urllib.parse
import uuid
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backend import config
from backend.core.duckdb import escape_sql_literal, get_memory_connection, require_safe_ident
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.request_telemetry import InstrumentedDuckDBConnection
from backend.core.retry import retry_call
from backend.core.storage import (
    buffer as _buffer_module,
)
from backend.core.storage import (
    catalog as _catalog_module,
)
from backend.core.storage import (
    compaction as _compaction_module,
)
from backend.core.storage import (
    view as _view_module,
)
from backend.core.storage.buffer import (
    _BUFFER_COMMIT_CHUNK_SIZE,
    _QUARANTINE_SUBDIR,
    _TOMBSTONE_GRACE_SECONDS,
    _TOMBSTONE_SUFFIX,
    _buffer_dir,
    _commit_buffer_impl,
    _commit_one_chunk,
    _is_tombstone_marker,
    _iter_tombstone_markers,
    _quarantine_buffer_file,
    _quarantine_dir,
    _tombstone_marker_path,
    _tombstoned_parquet_paths,
    buffer_backlog_stats,
    buffer_files,
    commit_buffer,
    sweep_tombstoned_buffer_files,
    tombstone_buffer_files,
    write_to_buffer,
)
from backend.core.storage.catalog import (
    _ATTACH_CONFLICT_RETRY_ATTEMPTS,
    _ATTACH_CONFLICT_RETRY_SLEEP_S,
    _ATTACH_LOCK_TIMEOUT_S,
    _C,
    _CATALOG_TYPE_MAP,
    _CDN_URL_ALLOWED_HOST_SUFFIXES,
    _CLOUD_WRITE_LOCK_TIMEOUT_S,
    _DUCKDB_TO_ARROW,
    _DUCKLAKE_TARGET_FILE_SIZE_MB,
    _FIELD_ORDER,
    _ICE,
    _R,
    _RESERVED_FIELD_SLOTS,
    _RUM_BEACON_TABLES,
    _RUM_TAIL_FIELDS,
    CLIENT_ERRORS_ARROW_SCHEMA,
    CLIENT_VITALS_ARROW_SCHEMA,
    DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S,
    RUM_NATURAL_KEYS,
    RUM_TABLE_SCHEMAS,
    DuckLakeAdmissionTimeout,
    _acquire_attach_lock,
    _advisory_key,
    _align_to_schema,
    _apply_target_file_size,
    _arrow_to_duckdb,
    _attach_lock,
    _default_data_path,
    _detach_unlocked,
    _ducklake_attach,
    _ducklake_detach,
    _ducklake_write_connection,
    _effective_field_types,
    _get_service_lock,
    _init_storage_table_locked,
    _is_local_only,
    _lake_columns,
    _lake_ident,
    _load_ducklake_extension,
    _local_lock,
    _locks,
    _locks_guard,
    _probe_lake_usable,
    _prune_empty_dirs,
    _record_stat,
    _reset_for_tests,
    _resolve_catalog_dsn,
    _resolve_data_path,
    _safe_cdn_url,
    _safe_ident,
    _service_locks,
    _service_locks_lock,
    _sleep_releasing_attach_lock,
    _stats,
    _stats_guard,
    _validate_lake_ident,
    _view_cache,
    clear_source_caches,
    configure_duckdb_s3,
    ducklake_current_snapshot_id,
    ducklake_table_name,
    ducklake_write_admission,
    get_admission_stats,
    get_arrow_schema,
    init_storage_table,
    invalidate_service_caches,
)
from backend.core.storage.compaction import (
    _COMPACTION_STATS_CACHE,
    _COMPACTION_STATS_TTL,
    _DAILY_DIR,
    _DAILY_FILE_RE,
    _DAILY_TIER_AGE_DAYS,
    _HOUR_PART_RE,
    _MAX_PARTITION_BYTES,
    _WEEKLY_DIR,
    _WEEKLY_TIER_AGE_DAYS,
    _bin_pack_files,
    _build_merge_select_sql,
    _cleanup_stale_tmp,
    _compact_daily_tier,
    _compact_single_partition,
    _compact_weekly_tier,
    _detect_natural_keys,
    _ducklake_expire_snapshots,
    _ducklake_retention_delete,
    _enforce_disk_watermark,
    _lake_delete_before,
    _optimize_table_impl,
    _rollup_bins,
    _run_cloud_maintenance_impl,
    _run_ducklake_maintenance,
    compact_local_partitions,
    compaction_stats,
    optimize_table,
    run_cloud_maintenance,
)
from backend.core.storage.view import (
    _acquire_ducklake_con,
    _ducklake_session_local,
    _ducklake_table_metadata_cache,
    _ducklake_table_metadata_cache_lock,
    _ducklake_view_token,
    _empty_schema_select,
    _existing_union_cols,
    _finalize_view_sql,
    _get_cached_or_scan_ducklake_metadata,
    _persistent_view_exists,
    _qualified_lake_ident,
    _rebuild_locked,
    _rebuild_signals,
    _rebuild_signals_lock,
    _scan_ducklake_table_metadata,
    _source_variant_fp,
    _try_fast_path_view,
    _update_storage_view_locked,
    _view_target_name,
    ducklake_session,
    ducklake_table_exists,
    execute_with_stale_view_retry,
    get_last_view_stats,
    get_snapshot_calendar,
    get_table_info,
    inject_view_debug,
    is_stale_view_error,
    update_storage_view,
)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_day_key_strict

logger = logging.getLogger(__name__)

_SYNCED_MODULES: list[_types.ModuleType] = []
_SYNC_GUARD = threading.local()
_MODULE_LOCAL_ATTRS = frozenset(
    {
        "__name__",
        "__doc__",
        "__package__",
        "__loader__",
        "__spec__",
        "__path__",
        "__file__",
        "__cached__",
        "__builtins__",
        "__all__",
        "_core",
        "_core_module",
        "_buffer_module",
        "_catalog_module",
        "_compaction_module",
        "_view_module",
        "_ducklake",
        "buffer",
        "catalog",
        "compaction",
        "manifest",
        "rum_schema",
        "sync",
        "view",
        "_SyncedStorageModule",
        "_SYNCED_MODULES",
        "_SYNC_GUARD",
        "_MODULE_LOCAL_ATTRS",
        "register_storage_module",
    }
)


class _SyncedStorageModule(_types.ModuleType):
    """ModuleType subclass that propagates attribute changes across storage submodules."""

    def __setattr__(self, name: str, value: object) -> None:
        object.__setattr__(self, name, value)
        if name.startswith("__") or name in _MODULE_LOCAL_ATTRS:
            return
        if getattr(_SYNC_GUARD, "active", False):
            return
        _SYNC_GUARD.active = True
        try:
            for mod in _SYNCED_MODULES:
                if mod is not self:
                    object.__setattr__(mod, name, value)
        finally:
            _SYNC_GUARD.active = False

    def __delattr__(self, name: str) -> None:
        try:
            object.__delattr__(self, name)
        except AttributeError:
            pass
        if name.startswith("__") or name in _MODULE_LOCAL_ATTRS:
            return
        if getattr(_SYNC_GUARD, "active", False):
            return
        _SYNC_GUARD.active = True
        try:
            for mod in _SYNCED_MODULES:
                if mod is not self and name in mod.__dict__:
                    try:
                        object.__delattr__(mod, name)
                    except AttributeError:
                        pass
        finally:
            _SYNC_GUARD.active = False


def register_storage_module(mod: _types.ModuleType) -> None:
    """Register ``mod`` to share attribute state with all storage modules in-place."""
    if mod not in _SYNCED_MODULES:
        if not isinstance(mod, _SyncedStorageModule):
            mod.__class__ = _SyncedStorageModule
        _SYNCED_MODULES.append(mod)
    for other in list(_SYNCED_MODULES):
        if other is mod:
            continue
        for k, v in list(other.__dict__.items()):
            if k.startswith("__") or k in _MODULE_LOCAL_ATTRS:
                continue
            if k not in mod.__dict__:
                mod.__dict__[k] = v
        for k, v in list(mod.__dict__.items()):
            if k.startswith("__") or k in _MODULE_LOCAL_ATTRS:
                continue
            if k not in other.__dict__:
                other.__dict__[k] = v


for _mod in (
    _catalog_module,
    _buffer_module,
    _view_module,
    _compaction_module,
    sys.modules[__name__],
):
    register_storage_module(_mod)
del _mod

_self_mod = sys.modules[__name__]
for _sub in ("manifest", "_ducklake", "rum_schema", "sync"):
    sys.modules[f"backend.core.storage.{_sub}"] = _self_mod
    setattr(_self_mod, _sub, _self_mod)
del _sub
setattr(_self_mod, "buffer", _buffer_module)
setattr(_self_mod, "catalog", _catalog_module)
setattr(_self_mod, "compaction", _compaction_module)
setattr(_self_mod, "view", _view_module)
del _self_mod

__all__ = [
    "Any",
    "BoundedTTLCache",
    "CLIENT_ERRORS_ARROW_SCHEMA",
    "CLIENT_VITALS_ARROW_SCHEMA",
    "DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S",
    "DuckLakeAdmissionTimeout",
    "InstrumentedDuckDBConnection",
    "Iterator",
    "LOG_FIELD_CATALOG",
    "RUM_NATURAL_KEYS",
    "RUM_TABLE_SCHEMAS",
    "UTC",
    "_ATTACH_CONFLICT_RETRY_ATTEMPTS",
    "_ATTACH_CONFLICT_RETRY_SLEEP_S",
    "_ATTACH_LOCK_TIMEOUT_S",
    "_BUFFER_COMMIT_CHUNK_SIZE",
    "_C",
    "_CATALOG_TYPE_MAP",
    "_CDN_URL_ALLOWED_HOST_SUFFIXES",
    "_CLOUD_WRITE_LOCK_TIMEOUT_S",
    "_COMPACTION_STATS_CACHE",
    "_COMPACTION_STATS_TTL",
    "_CacheRegistry",
    "_DAILY_DIR",
    "_DAILY_FILE_RE",
    "_DAILY_TIER_AGE_DAYS",
    "_DUCKDB_TO_ARROW",
    "_DUCKLAKE_TARGET_FILE_SIZE_MB",
    "_FIELD_ORDER",
    "_HOUR_PART_RE",
    "_ICE",
    "_MAX_PARTITION_BYTES",
    "_QUARANTINE_SUBDIR",
    "_R",
    "_RESERVED_FIELD_SLOTS",
    "_RUM_BEACON_TABLES",
    "_RUM_TAIL_FIELDS",
    "_TOMBSTONE_GRACE_SECONDS",
    "_TOMBSTONE_SUFFIX",
    "_WEEKLY_DIR",
    "_WEEKLY_TIER_AGE_DAYS",
    "_acquire_attach_lock",
    "_acquire_ducklake_con",
    "_advisory_key",
    "_align_to_schema",
    "_apply_target_file_size",
    "_arrow_to_duckdb",
    "_attach_lock",
    "_bin_pack_files",
    "_buffer_dir",
    "_build_merge_select_sql",
    "_cleanup_stale_tmp",
    "_commit_buffer_impl",
    "_commit_one_chunk",
    "_compact_daily_tier",
    "_compact_single_partition",
    "_compact_weekly_tier",
    "_default_data_path",
    "_detach_unlocked",
    "_detect_natural_keys",
    "_ducklake_attach",
    "_ducklake_detach",
    "_ducklake_expire_snapshots",
    "_ducklake_retention_delete",
    "_ducklake_session_local",
    "_ducklake_table_metadata_cache",
    "_ducklake_table_metadata_cache_lock",
    "_ducklake_view_token",
    "_ducklake_write_connection",
    "_effective_field_types",
    "_empty_schema_select",
    "_enforce_disk_watermark",
    "_existing_union_cols",
    "_finalize_view_sql",
    "_get_cached_or_scan_ducklake_metadata",
    "_get_service_lock",
    "_glob",
    "_init_storage_table_locked",
    "_is_local_only",
    "_is_tombstone_marker",
    "_iter_tombstone_markers",
    "_lake_columns",
    "_lake_delete_before",
    "_lake_ident",
    "_load_ducklake_extension",
    "_local_lock",
    "_locks",
    "_locks_guard",
    "_optimize_table_impl",
    "_persistent_view_exists",
    "_probe_lake_usable",
    "_prune_empty_dirs",
    "_qualified_lake_ident",
    "_quarantine_buffer_file",
    "_quarantine_dir",
    "_rebuild_locked",
    "_rebuild_signals",
    "_rebuild_signals_lock",
    "_record_stat",
    "_reset_for_tests",
    "_resolve_catalog_dsn",
    "_resolve_data_path",
    "_rollup_bins",
    "_run_cloud_maintenance_impl",
    "_run_ducklake_maintenance",
    "_safe_cdn_url",
    "_safe_ident",
    "_scan_ducklake_table_metadata",
    "_service_locks",
    "_service_locks_lock",
    "_sleep_releasing_attach_lock",
    "_source_variant_fp",
    "_stats",
    "_stats_guard",
    "_tombstone_marker_path",
    "_tombstoned_parquet_paths",
    "_try_fast_path_view",
    "_update_storage_view_locked",
    "_validate_lake_ident",
    "_view_cache",
    "_view_target_name",
    "buffer_backlog_stats",
    "buffer_files",
    "clear_source_caches",
    "commit_buffer",
    "compact_local_partitions",
    "compaction_stats",
    "config",
    "configure_duckdb_s3",
    "contextlib",
    "contextmanager",
    "datetime",
    "defaultdict",
    "ducklake_current_snapshot_id",
    "ducklake_session",
    "ducklake_table_exists",
    "ducklake_table_name",
    "ducklake_write_admission",
    "escape_sql_literal",
    "execute_with_stale_view_retry",
    "get_admission_stats",
    "get_arrow_schema",
    "get_last_view_stats",
    "get_memory_connection",
    "get_snapshot_calendar",
    "get_table_info",
    "hashlib",
    "init_storage_table",
    "inject_view_debug",
    "invalidate_service_caches",
    "is_stale_view_error",
    "logger",
    "optimize_table",
    "os",
    "pa",
    "parse_day_key_strict",
    "pq",
    "re",
    "register_storage_module",
    "require_safe_ident",
    "retry_call",
    "run_cloud_maintenance",
    "shutil",
    "sweep_tombstoned_buffer_files",
    "sys",
    "threading",
    "time",
    "timedelta",
    "tombstone_buffer_files",
    "update_storage_view",
    "urllib",
    "uuid",
    "write_to_buffer",
]
