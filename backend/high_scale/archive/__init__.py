"""Typed archive, deployment profile, serving-volume contracts, and verified archive publication for the high-scale plane."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from functools import cached_property
from pathlib import Path
from threading import Event, Lock, RLock
from typing import Any, Literal, Protocol
from urllib.parse import urlparse
from uuid import UUID, uuid4, uuid5

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from cachetools import LRUCache

from backend.core.clickhouse import CLICKHOUSE_HIGH_SCALE_TABLES, get_clickhouse_client
from backend.core.clickhouse import ClickHouseClient as HttpClickHouseClient
from backend.utils.telemetry import atomic_temp_path, atomic_write_json

logger = structlog.get_logger("backend.high_scale.archive")

import types

from backend.high_scale._common import range_value
from backend.high_scale.archive import clickhouse_batch as _clickhouse_batch_mod
from backend.high_scale.archive import jobs as _jobs_mod
from backend.high_scale.archive import manifests as _manifests_mod
from backend.high_scale.archive import topology as _topology_mod
from backend.high_scale.archive.clickhouse_batch import (
    _BATCH_NAMESPACE,
    _COMMON_FACT_COLUMNS,
    _CORE_FACT_KEYS,
    _DOMAIN_COLUMNS,
    _DOMAIN_TABLES,
    _EVENT_NAMESPACE,
    _MAX_CACHED_BATCH_LOCKS,
    _PUBLICATION_COLUMNS,
    _PUBLICATION_TABLE,
    _RUM_ENV_TAIL_COLUMNS,
    _SCHEMA_FILES,
    _TEXT_ALIASES,
    _TTFB_KEYS,
    BaseClickHouseBatchAdapter,
    BatchManifest,
    BatchManifestStore,
    ClickHouseBatchAdapter,
    ClickHouseClient,
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PartialInsert,
    PostgresBatchManifestStore,
    PublicationStatus,
    _batch_uuid,
    _common_values,
    _event_uuid,
    _extract_custom_fields,
    _finite,
    _first_text,
    _optional_asn,
    _position,
    _required_nonnegative_int,
    _required_string,
    _rows_for_domain,
    _rum_environment_values,
    _string_map,
    _text,
    _timestamp,
    install_clickhouse_schema,
    row_matches_serving_domain,
    rum_environment,
    rum_error_position,
)
from backend.high_scale.archive.jobs import (
    _ACTIVE_STATES,
    _TERMINAL_STATES,
    ExportJob,
    ExportManager,
    ExportState,
    HistoricalJob,
    HistoricalJobManager,
    HistoricalJobState,
    ProjectionReplayResult,
    ReplayResult,
    _batch_from_rows,
    _default_export_manager,
    _default_export_manager_lock,
    _default_manager,
    _default_manager_lock,
    _ExportCancelled,
    _hs_pkg,
    _JobRecord,
    _load_manifest_request_events,
    _manifest_domain,
    _manifest_service_id,
    _MutableJob,
    _publish_projection_batches,
    _read_manifest_rows,
    get_export_manager,
    get_historical_job_manager,
    replay_cmcd_projections,
    replay_manifest,
    replay_origin_projections,
    replay_performance_projections,
)
from backend.high_scale.archive.manifests import (
    IDENTITY_FIELDS,
    ArchiveArtifact,
    ArchiveManifest,
    ArchivePublication,
    ArchiveSourceEntry,
    ArchiveSourceObject,
    InMemoryObjectStore,
    ObjectStore,
    PublicationResult,
    PublicationState,
    S3ObjectStore,
    ServingWatermark,
    _artifact_key,
    _checksum,
    _is_not_found,
    _json_bytes,
    _json_default,
    _manifest_from_json,
    _parquet_safe_value,
    _parse_datetime,
    _sha256,
    _sha256_uuid,
    build_batch_id_from_manifest_id,
    build_cmcd_projection_key,
    build_event_id,
    build_request_event_id,
    build_rum_event_id,
    event_digest,
    write_batch_archive_checkpoint,
)
from backend.high_scale.archive.topology import (
    _BASELINE_HOT_DAYS,
    _BASELINE_WARM_DAYS,
    _EMERGENCY_FREE_FRACTION,
    _PROTECTED_RESERVE_FRACTION,
    _PROTECTIVE_FREE_FRACTION,
    _RESERVE_FRACTION,
    _SECONDS_PER_DAY,
    _WARM_COMPACTION_RATIO,
    ARCHIVE_STATES,
    MODE_LEVELS,
    ArchiveState,
    DiskMode,
    DiskSnapshot,
    DiskState,
    HighScaleConfig,
    RetentionRecommendation,
    ServiceProfile,
    StorageProbe,
    StorageProfile,
    TopologyProfile,
    VolumeStats,
    _latest,
    _lock,
    _one_batch_bytes,
    _record,
    _resolve_topology_profile,
    build_topology_profile,
    disk_snapshot_payload,
    evaluate_disk_state,
    from_environment,
    latest_disk_snapshot,
    recommend_retention,
    reset_for_tests,
    sample_disk_state,
    topology_from_environment,
    validate_archive_transition,
)

_SUBMODULES = (_topology_mod, _manifests_mod, _clickhouse_batch_mod, _jobs_mod)

from backend.high_scale.cmcd import project_cmcd  # noqa: E402
from backend.high_scale.origin import build_origin_projection_rows  # noqa: E402
from backend.high_scale.performance import build_performance_projection_rows  # noqa: E402
from backend.high_scale.query_service import QueryPlan, QueryTier  # noqa: E402

for _k, _v in (
    ("project_cmcd", project_cmcd),
    ("build_origin_projection_rows", build_origin_projection_rows),
    ("build_performance_projection_rows", build_performance_projection_rows),
    ("QueryPlan", QueryPlan),
    ("QueryTier", QueryTier),
):
    setattr(_jobs_mod, _k, _v)


class _ShimModule(types.ModuleType):
    def __setattr__(self, name: str, value: object) -> None:
        super().__setattr__(name, value)
        if name.startswith("__") and name.endswith("__"):
            return
        pkg = sys.modules.get("backend.high_scale.archive")
        if pkg is not None and pkg is not self:
            types.ModuleType.__setattr__(pkg, name, value)
        for mod in _SUBMODULES:
            if mod is not self:
                types.ModuleType.__setattr__(mod, name, value)


for _mod in (sys.modules[__name__], *_SUBMODULES):
    _mod.__class__ = _ShimModule

_self_mod = sys.modules[__name__]

__all__ = [
    "_self_mod",
    "hashlib",
    "json",
    "logging",
    "math",
    "os",
    "re",
    "sys",
    "threading",
    "time",
    "uuid",
    "Callable",
    "Iterable",
    "Mapping",
    "Sequence",
    "Future",
    "ThreadPoolExecutor",
    "asdict",
    "dataclass",
    "field",
    "UTC",
    "date",
    "datetime",
    "timedelta",
    "StrEnum",
    "cached_property",
    "Path",
    "Event",
    "Lock",
    "RLock",
    "Any",
    "Literal",
    "Protocol",
    "urlparse",
    "UUID",
    "uuid4",
    "uuid5",
    "pa",
    "pq",
    "structlog",
    "LRUCache",
    "CLICKHOUSE_HIGH_SCALE_TABLES",
    "get_clickhouse_client",
    "HttpClickHouseClient",
    "atomic_temp_path",
    "atomic_write_json",
    "logger",
    "range_value",
    "_SCHEMA_FILES",
    "install_clickhouse_schema",
    "_MAX_CACHED_BATCH_LOCKS",
    "_PUBLICATION_TABLE",
    "_PUBLICATION_COLUMNS",
    "BaseClickHouseBatchAdapter",
    "_TEXT_ALIASES",
    "_TTFB_KEYS",
    "_text",
    "_first_text",
    "_finite",
    "rum_environment",
    "_position",
    "rum_error_position",
    "_BATCH_NAMESPACE",
    "_EVENT_NAMESPACE",
    "_DOMAIN_TABLES",
    "_COMMON_FACT_COLUMNS",
    "_RUM_ENV_TAIL_COLUMNS",
    "_DOMAIN_COLUMNS",
    "PublicationStatus",
    "PartialInsert",
    "HighScaleBatch",
    "BatchManifest",
    "InsertReceipt",
    "BatchManifestStore",
    "ClickHouseClient",
    "ClickHouseBatchAdapter",
    "_batch_uuid",
    "_event_uuid",
    "_required_string",
    "_required_nonnegative_int",
    "_timestamp",
    "_rum_environment_values",
    "_optional_asn",
    "_string_map",
    "_common_values",
    "row_matches_serving_domain",
    "_CORE_FACT_KEYS",
    "_extract_custom_fields",
    "_rows_for_domain",
    "InMemoryBatchManifestStore",
    "PostgresBatchManifestStore",
    "ClickHousePublication",
    "HistoricalJobState",
    "HistoricalJob",
    "_JobRecord",
    "HistoricalJobManager",
    "_ACTIVE_STATES",
    "_TERMINAL_STATES",
    "_default_manager",
    "_default_manager_lock",
    "get_historical_job_manager",
    "ExportState",
    "ExportJob",
    "_MutableJob",
    "ExportManager",
    "_default_export_manager",
    "_default_export_manager_lock",
    "get_export_manager",
    "_ExportCancelled",
    "ReplayResult",
    "ProjectionReplayResult",
    "_manifest_domain",
    "_manifest_service_id",
    "replay_manifest",
    "_load_manifest_request_events",
    "_publish_projection_batches",
    "replay_origin_projections",
    "replay_performance_projections",
    "replay_cmcd_projections",
    "_read_manifest_rows",
    "_batch_from_rows",
    "_hs_pkg",
    "ArchiveSourceObject",
    "ArchiveArtifact",
    "ArchiveSourceEntry",
    "ArchiveManifest",
    "ServingWatermark",
    "ObjectStore",
    "PublicationState",
    "PublicationResult",
    "InMemoryObjectStore",
    "S3ObjectStore",
    "ArchivePublication",
    "_artifact_key",
    "_manifest_from_json",
    "_parse_datetime",
    "_json_bytes",
    "_checksum",
    "_json_default",
    "_is_not_found",
    "_sha256",
    "event_digest",
    "write_batch_archive_checkpoint",
    "_parquet_safe_value",
    "IDENTITY_FIELDS",
    "_sha256_uuid",
    "build_event_id",
    "build_request_event_id",
    "build_rum_event_id",
    "build_cmcd_projection_key",
    "build_batch_id_from_manifest_id",
    "_BASELINE_HOT_DAYS",
    "_BASELINE_WARM_DAYS",
    "_PROTECTED_RESERVE_FRACTION",
    "_WARM_COMPACTION_RATIO",
    "_SECONDS_PER_DAY",
    "ArchiveState",
    "ARCHIVE_STATES",
    "validate_archive_transition",
    "ServiceProfile",
    "StorageProfile",
    "RetentionRecommendation",
    "recommend_retention",
    "TopologyProfile",
    "build_topology_profile",
    "HighScaleConfig",
    "_resolve_topology_profile",
    "from_environment",
    "topology_from_environment",
    "DiskMode",
    "_RESERVE_FRACTION",
    "_PROTECTIVE_FREE_FRACTION",
    "_EMERGENCY_FREE_FRACTION",
    "VolumeStats",
    "DiskState",
    "evaluate_disk_state",
    "MODE_LEVELS",
    "StorageProbe",
    "DiskSnapshot",
    "_lock",
    "_latest",
    "_one_batch_bytes",
    "sample_disk_state",
    "_record",
    "latest_disk_snapshot",
    "disk_snapshot_payload",
    "reset_for_tests",
    "project_cmcd",
    "build_origin_projection_rows",
    "build_performance_projection_rows",
    "QueryPlan",
    "QueryTier",
]
