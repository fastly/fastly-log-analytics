import concurrent.futures  # noqa: F401
import gzip as gzip
import hashlib as hashlib
import importlib
import itertools as itertools
import json as json
import logging
import math as math
import os as os
import random as random
import re as re
import sys
import tempfile as tempfile
import threading as threading
import time as time
import uuid as uuid
from collections import defaultdict as defaultdict
from collections.abc import Callable as Callable
from datetime import UTC as UTC
from datetime import datetime as datetime
from datetime import timedelta as timedelta
from types import ModuleType
from typing import TYPE_CHECKING, Any

import backend.core as _core_pkg
from backend.celery_app import app
from backend.core import log_fields as fc
from backend.core import metadata as metadata_db
from backend.core import storage
from backend.core.duckdb import (
    _DEFAULT_SOURCE,
    INGEST_CHUNK_SIZE,
    _configure_fos,  # noqa: F401  re-exported for test monkey-patching
    _ensure_source_registered,
    _execute_query_with_retry,
    _get_fos_client,
    _load_httpfs,  # noqa: F401  re-exported for test monkey-patching
    escape_sql_literal,
)
from backend.core.ingest import discovery, parser, writer
from backend.core.ingest.discovery import (
    _DASH_KEY_RE,
    _FASTLY_FNAME_TS_RE,
    _LEDGER_KEY_SLICE,
    _MINUTE_KEY_RE,
    _RECONCILE_LEDGER_EPOCH,
    _RECONCILE_MIN_INTERVAL_S,
    _SLASHED_KEY_RE,
    _STRANDED_DELETE_CAP,
    _V3_MINUTE_KEY_RE,
    LEDGER_CONVERT_BATCH_SIZE,
    LEDGER_MAX_ATTEMPTS,
    LEDGER_RECLAIM_AFTER_S,
    LEDGER_REDISPATCH_AFTER_S,
    LEDGER_RETRY_BASE_S,
    LEDGER_RETRY_JITTER_RATIO,
    LEDGER_RETRY_MAX_S,
    RAW_DELETE_CLAIM_LEASE_S,
    RAW_DELETE_GRACE_S,
    _compute_incremental_start_after,
    _consume_generator,
    _delete_objects_robust,
    _delete_objects_robust_with_failures,
    _discover_prefix_common,
    _download_chunk_to_local,
    _is_permanent_s3_missing,
    _is_permission_error,
    _iter_key_slices,
    _parse_fastly_filename_dt,
    _parse_key_layout_dt,
    _parse_time_bounds,
    _reconcile_last_attempt,
    boto3_client_hot,
    discover_prefix,
    list_fos_files,
)
from backend.core.ingest.parser import (
    _DECODE_EXPRS,
    _EMPTY_VALUE_RE,
    _INGEST_EXCLUDED_GROUPS,
    _INGEST_OUTCOME_COUNTER_KEYS,
    _build_column_select_exprs,
    _capture_corrupt_container,
    _corrupt_gzip_error,
    _failed_object_outcome,
    _finalize_object_outcome,
    _finalize_sync_outcome_counters,
    _mark_object_failed,
    _new_object_outcome,
    _quarantine_convert_corrupt_lines,
    _read_json_auto_sql,
    _scan_invalid_gzip_lines,
    _sum_evidence_counts,
    get_catalog_field_ids,
    get_ingest_columns_sql,
    get_ingest_type_hints,
)
from backend.core.ingest.writer import (
    _celery_ingest_scope,
    _claim_and_resolve_batch_source,
    _cleanup_batch_resources,
    _deterministic_buffer_name,
    _download_and_classify_batch_files,
    _ducklake_replace_source_rows,
    _ducklake_table_exists,
    _export_staging_arrow,
    _finalize_batch_convert,
    _handle_batch_convert_exception,
    _ledger_claim_batch,
    _ledger_mark_committed_batch,
    _ledger_mark_dead_letter_missing,
    _ledger_record_failure,
    _mark_ledger_published,
    _record_ledger_batch_outcomes,
    _record_ledger_object_outcome,
    _record_raw_delete_failure,
    _recover_in_flight,
    _redispatch_pending,
    _run_batch_convert_common,
    _sweep_ledger_common,
    _write_buffer_with_in_flight,
    convert_batch_files,
    convert_batch_objects,
    finalize_committed_raw,
    ingest,
    merge_lake_files,
    sweep_ledger_once,
)
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.metadata import ingest_log as _quarantine_mod
from backend.utils.telemetry import yield_to_api

_core_pkg.quarantine = _quarantine_mod  # type: ignore[attr-defined]
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from backend.core.faro_versions import (
        _ABBREVIATED_PACKUMENT_HEADERS as _ABBREVIATED_PACKUMENT_HEADERS,
    )
    from backend.core.faro_versions import (
        _BUNDLE_MAX_BYTES as _BUNDLE_MAX_BYTES,
    )
    from backend.core.faro_versions import (
        _BUNDLE_MEMBER_NAME as _BUNDLE_MEMBER_NAME,
    )
    from backend.core.faro_versions import (
        _SRI_ALGOS as _SRI_ALGOS,
    )
    from backend.core.faro_versions import (
        _TARBALL_MAX_BYTES as _TARBALL_MAX_BYTES,
    )
    from backend.core.faro_versions import (
        _TARBALL_TIMEOUT as _TARBALL_TIMEOUT,
    )
    from backend.core.faro_versions import (
        _TIMEOUT as _TIMEOUT,
    )
    from backend.core.faro_versions import (
        _TRUSTED_TARBALL_HOST as _TRUSTED_TARBALL_HOST,
    )
    from backend.core.faro_versions import (
        _VERSIONS_CACHE_TTL as _VERSIONS_CACHE_TTL,
    )
    from backend.core.faro_versions import (
        _VERSIONS_ERROR_BACKOFF as _VERSIONS_ERROR_BACKOFF,
    )
    from backend.core.faro_versions import (
        DEFAULT_FARO_VERSION as DEFAULT_FARO_VERSION,
    )
    from backend.core.faro_versions import (
        REGISTRY_URL as REGISTRY_URL,
    )
    from backend.core.faro_versions import (
        _extract_bundle_from_tarball as _extract_bundle_from_tarball,
    )
    from backend.core.faro_versions import (
        _fetch_registry_packument as _fetch_registry_packument,
    )
    from backend.core.faro_versions import (
        _fetch_version_dist as _fetch_version_dist,
    )
    from backend.core.faro_versions import (
        _is_stable_numeric as _is_stable_numeric,
    )
    from backend.core.faro_versions import (
        _tarball_url_from_dist as _tarball_url_from_dist,
    )
    from backend.core.faro_versions import (
        _verify_artifact_integrity as _verify_artifact_integrity,
    )
    from backend.core.faro_versions import (
        _version_sort_key as _version_sort_key,
    )
    from backend.core.faro_versions import (
        _versions_cache as _versions_cache,
    )
    from backend.core.faro_versions import (
        _versions_cache_lock as _versions_cache_lock,
    )
    from backend.core.faro_versions import (
        _VersionsCacheProxy as _VersionsCacheProxy,
    )
    from backend.core.faro_versions import (
        fetch_available_faro_versions as fetch_available_faro_versions,
    )
    from backend.core.faro_versions import (
        fetch_faro_bundle as fetch_faro_bundle,
    )
    from backend.core.faro_versions import (
        get_faro_versions_cached as get_faro_versions_cached,
    )
    from backend.core.faro_versions import (
        reset_versions_cache as reset_versions_cache,
    )
    from backend.core.rum_ingest import (
        _parse_rum_beacon_file as _parse_rum_beacon_file,
    )
    from backend.core.rum_ingest import (
        _parse_rum_line as _parse_rum_line,
    )
    from backend.core.rum_ingest import (
        _quarantine_rum_corrupt_lines as _quarantine_rum_corrupt_lines,
    )
    from backend.core.rum_ingest import (
        _write_rum_arrow_table_batch as _write_rum_arrow_table_batch,
    )
    from backend.core.rum_ingest import (
        convert_batch_rum_files as convert_batch_rum_files,
    )
    from backend.core.rum_ingest import (
        convert_batch_rum_objects as convert_batch_rum_objects,
    )
    from backend.core.rum_ingest import (
        discover_rum_prefix as discover_rum_prefix,
    )
    from backend.core.rum_ingest import (
        extract_metrics_from_faro_payload as extract_metrics_from_faro_payload,
    )
    from backend.core.rum_ingest import (
        ingest_rum_logs as ingest_rum_logs,
    )
    from backend.core.rum_ingest import (
        safe_float as safe_float,
    )
    from backend.core.rum_ingest import (
        safe_int as safe_int,
    )
    from backend.core.rum_ingest import (
        sweep_rum_ledger_once as sweep_rum_ledger_once,
    )


_DELEGATED_INGEST_ATTRS: dict[str, str] = {
    "CDN_SOURCES": "backend.core.faro_versions",
    "DEFAULT_FARO_VERSION": "backend.core.faro_versions",
    "NPM_REGISTRY_URL": "backend.core.faro_versions",
    "_BUNDLE_CACHE": "backend.core.faro_versions",
    "_BUNDLE_CACHE_LOCK": "backend.core.faro_versions",
    "_BUNDLE_CACHE_MAX": "backend.core.faro_versions",
    "_FALLBACK_VERSIONS": "backend.core.faro_versions",
    "_SEMVER_RE": "backend.core.faro_versions",
    "_VERSIONS_CACHE_LOCK": "backend.core.faro_versions",
    "_VERSIONS_CACHE_TTL_S": "backend.core.faro_versions",
    "_parse_semver": "backend.core.faro_versions",
    "_versions_cache": "backend.core.faro_versions",
    "compute_sri_sha384": "backend.core.faro_versions",
    "fetch_available_faro_versions": "backend.core.faro_versions",
    "fetch_faro_bundle": "backend.core.faro_versions",
    "get_faro_versions_cached": "backend.core.faro_versions",
    "reset_versions_cache": "backend.core.faro_versions",
    "_parse_rum_beacon_file": "backend.core.rum_ingest",
    "_parse_rum_line": "backend.core.rum_ingest",
    "_quarantine_rum_corrupt_lines": "backend.core.rum_ingest",
    "_write_rum_arrow_table_batch": "backend.core.rum_ingest",
    "backfill_missing_rum_aggregates": "backend.core.rum_ingest",
    "convert_batch_rum_files": "backend.core.rum_ingest",
    "convert_batch_rum_objects": "backend.core.rum_ingest",
    "discover_rum_prefix": "backend.core.rum_ingest",
    "extract_metrics_from_faro_payload": "backend.core.rum_ingest",
    "finalize_cron_run_if_running": "backend.core.rum_ingest",
    "get_source_for_service": "backend.core.rum_ingest",
    "ingest_rum_logs": "backend.core.rum_ingest",
    "log_cron_run": "backend.core.rum_ingest",
    "recompute_rum_aggregates": "backend.core.rum_ingest",
    "safe_float": "backend.core.rum_ingest",
    "safe_int": "backend.core.rum_ingest",
    "start_cron_run": "backend.core.rum_ingest",
    "sweep_rum_ledger_once": "backend.core.rum_ingest",
}

_SUBMODULES = (discovery, parser, writer)
_SKIP_SHIM_ATTRS = frozenset(
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
        "__class__",
        "_SUBMODULES",
        "_SYNC_MODULES",
        "_SHIM_INITIALIZED",
        "_ShimModule",
        "_IngestModule",
        "_orig_delegated",
    }
)


class _ShimModule(ModuleType):
    """Module proxy keeping attribute patches synced across ingest submodules and delegated modules."""

    _orig_delegated: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        if (mod_name := _DELEGATED_INGEST_ATTRS.get(name)) is not None:
            return getattr(importlib.import_module(mod_name), name)
        raise AttributeError(f"module {self.__name__!r} has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        if (mod_name := _DELEGATED_INGEST_ATTRS.get(name)) is not None:
            mod = importlib.import_module(mod_name)
            if name not in self._orig_delegated:
                self._orig_delegated[name] = getattr(mod, name)
            setattr(mod, name, value)
            if value is self._orig_delegated.get(name):
                self._orig_delegated.pop(name, None)
                for m in _SYNC_MODULES:
                    m.__dict__.pop(name, None)
                return
        super().__setattr__(name, value)
        if not getattr(self, "_SHIM_INITIALIZED", False) or name in _SKIP_SHIM_ATTRS:
            return
        for mod in _SYNC_MODULES:
            if mod is not self and (name in mod.__dict__ or mod is sys.modules.get(__name__)):
                mod.__dict__[name] = value

    def __delattr__(self, name: str) -> None:
        if (mod_name := _DELEGATED_INGEST_ATTRS.get(name)) is not None and name in self._orig_delegated:
            mod = importlib.import_module(mod_name)
            setattr(mod, name, self._orig_delegated.pop(name))
            for m in _SYNC_MODULES:
                m.__dict__.pop(name, None)
            return
        super().__delattr__(name)


_IngestModule = _ShimModule

_self = sys.modules[__name__]
_SYNC_MODULES = (_self, *_SUBMODULES)
for _mod in _SUBMODULES:
    for _k, _v in list(_self.__dict__.items()):
        if _k not in _SKIP_SHIM_ATTRS and not _k.startswith("__"):
            _mod.__dict__.setdefault(_k, _v)
for _mod in _SYNC_MODULES:
    if not isinstance(_mod, _ShimModule):
        _mod.__class__ = _ShimModule
    _mod.__dict__["_SHIM_INITIALIZED"] = True

__all__ = [
    "INGEST_CHUNK_SIZE",
    "LEDGER_CONVERT_BATCH_SIZE",
    "LEDGER_MAX_ATTEMPTS",
    "LEDGER_RECLAIM_AFTER_S",
    "LEDGER_REDISPATCH_AFTER_S",
    "LEDGER_RETRY_BASE_S",
    "LEDGER_RETRY_JITTER_RATIO",
    "LEDGER_RETRY_MAX_S",
    "LOG_FIELD_CATALOG",
    "RAW_DELETE_CLAIM_LEASE_S",
    "RAW_DELETE_GRACE_S",
    "_DASH_KEY_RE",
    "_DECODE_EXPRS",
    "_DEFAULT_SOURCE",
    "_DELEGATED_INGEST_ATTRS",
    "_EMPTY_VALUE_RE",
    "_FASTLY_FNAME_TS_RE",
    "_INGEST_EXCLUDED_GROUPS",
    "_INGEST_OUTCOME_COUNTER_KEYS",
    "_LEDGER_KEY_SLICE",
    "_MINUTE_KEY_RE",
    "_RECONCILE_LEDGER_EPOCH",
    "_RECONCILE_MIN_INTERVAL_S",
    "_SLASHED_KEY_RE",
    "_STRANDED_DELETE_CAP",
    "_V3_MINUTE_KEY_RE",
    "_build_column_select_exprs",
    "_capture_corrupt_container",
    "_celery_ingest_scope",
    "_claim_and_resolve_batch_source",
    "_cleanup_batch_resources",
    "_compute_incremental_start_after",
    "_configure_fos",
    "_consume_generator",
    "_corrupt_gzip_error",
    "_delete_objects_robust",
    "_delete_objects_robust_with_failures",
    "_deterministic_buffer_name",
    "_discover_prefix_common",
    "_download_and_classify_batch_files",
    "_download_chunk_to_local",
    "_ducklake_replace_source_rows",
    "_ducklake_table_exists",
    "_ensure_source_registered",
    "_execute_query_with_retry",
    "_export_staging_arrow",
    "_failed_object_outcome",
    "_finalize_batch_convert",
    "_finalize_object_outcome",
    "_finalize_sync_outcome_counters",
    "_get_fos_client",
    "_handle_batch_convert_exception",
    "_is_permanent_s3_missing",
    "_is_permission_error",
    "_iter_key_slices",
    "_ledger_claim_batch",
    "_ledger_mark_committed_batch",
    "_ledger_mark_dead_letter_missing",
    "_ledger_record_failure",
    "_load_httpfs",
    "_mark_ledger_published",
    "_mark_object_failed",
    "_new_object_outcome",
    "_parse_fastly_filename_dt",
    "_parse_key_layout_dt",
    "_parse_time_bounds",
    "_quarantine_convert_corrupt_lines",
    "_read_json_auto_sql",
    "_reconcile_last_attempt",
    "_record_ledger_batch_outcomes",
    "_record_ledger_object_outcome",
    "_record_raw_delete_failure",
    "_recover_in_flight",
    "_redispatch_pending",
    "_run_batch_convert_common",
    "_scan_invalid_gzip_lines",
    "_sum_evidence_counts",
    "_sweep_ledger_common",
    "_write_buffer_with_in_flight",
    "app",
    "boto3_client_hot",
    "convert_batch_files",
    "convert_batch_objects",
    "discover_prefix",
    "escape_sql_literal",
    "fc",
    "finalize_committed_raw",
    "get_catalog_field_ids",
    "get_ingest_columns_sql",
    "get_ingest_type_hints",
    "ingest",
    "list_fos_files",
    "logger",
    "merge_lake_files",
    "metadata_db",
    "storage",
    "sweep_ledger_once",
    "yield_to_api",
]
