"""Ingest parsing, quarantine outcome helpers, and FOS object deletion re-exports."""

from __future__ import annotations

import logging
import sys

import backend.core as _core_pkg
from backend.core import log_fields as fc
from backend.core import metadata as metadata_db
from backend.core.fos_client import (
    _DEFAULT_SOURCE,
    _delete_objects_robust,
    _delete_objects_robust_with_failures,
    _get_fos_client,
    _is_permission_error,
)
from backend.core.ingest import parser
from backend.core.ingest.parser import (
    _COMPRESSED_STREAM_ERRORS,
    _EMPTY_VALUE_RE,
    _GZIP_MAGIC,
    _INGEST_EXCLUDED_GROUPS,
    _INGEST_OUTCOME_COUNTER_KEYS,
    _ZSTD_MAGIC,
    _capture_corrupt_container,
    _corrupt_gzip_error,
    _detect_compression,
    _failed_object_outcome,
    _finalize_object_outcome,
    _finalize_sync_outcome_counters,
    _mark_object_failed,
    _new_object_outcome,
    _open_compressed_log,
    _scan_invalid_gzip_lines,
    _sum_evidence_counts,
    get_catalog_field_ids,
    get_ingest_type_hints,
)
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.metadata import ingest_log as _quarantine_mod
from backend.utils.module_shim import install_shim_module

_core_pkg.quarantine = _quarantine_mod  # type: ignore[attr-defined]
logger = logging.getLogger(__name__)

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
    "extract_metrics_from_faro_payload": "backend.core.rum_ingest",
    "extract_metrics_from_query_params": "backend.core.rum_ingest",
    "safe_float": "backend.core.rum_ingest",
    "safe_int": "backend.core.rum_ingest",
}

_SUBMODULES = (parser,)
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

_SYNC_MODULES = (sys.modules[__name__], *_SUBMODULES)
_ShimModule = install_shim_module(
    __name__,
    _SUBMODULES,
    skip_attrs=_SKIP_SHIM_ATTRS,
    delegated_attrs=_DELEGATED_INGEST_ATTRS,
    seed_submodules_from_root=True,
    only_existing_attrs=True,
)
_IngestModule = _ShimModule

__all__ = [
    "LOG_FIELD_CATALOG",
    "_COMPRESSED_STREAM_ERRORS",
    "_DEFAULT_SOURCE",
    "_DELEGATED_INGEST_ATTRS",
    "_EMPTY_VALUE_RE",
    "_GZIP_MAGIC",
    "_INGEST_EXCLUDED_GROUPS",
    "_INGEST_OUTCOME_COUNTER_KEYS",
    "_ZSTD_MAGIC",
    "_capture_corrupt_container",
    "_corrupt_gzip_error",
    "_delete_objects_robust",
    "_delete_objects_robust_with_failures",
    "_detect_compression",
    "_failed_object_outcome",
    "_finalize_object_outcome",
    "_finalize_sync_outcome_counters",
    "_get_fos_client",
    "_is_permission_error",
    "_mark_object_failed",
    "_new_object_outcome",
    "_open_compressed_log",
    "_scan_invalid_gzip_lines",
    "_sum_evidence_counts",
    "fc",
    "get_catalog_field_ids",
    "get_ingest_type_hints",
    "logger",
    "metadata_db",
    "parser",
]
