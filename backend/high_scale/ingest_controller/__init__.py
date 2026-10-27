"""Fenced source-object controller, aggregate batch adapter, assets, and service registry for high-scale."""

from __future__ import annotations

import contextvars
import inspect
import logging
import os
import sys
import tempfile
import types
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from backend.high_scale._common import (
    _BOOLEAN,
    _COLUMNS,
    _DOMAIN_IDENT_RE,
    _LOG_GAMMA,
    _ROUNDED,
    _TIMESERIES_METRICS,
    _UNSUPPORTED,
    GAMMA,
    MAX_PAGE_SIZE,
    RELATIVE_ERROR,
    VISIBLE_PUBLICATION_FENCE_SQL,
    ArchiveManifestCatalog,
    KeysetCursor,
    _b64,
    _bucket,
    _bucket_case,
    _centroid,
    _column,
    _row_timestamp,
    _unb64,
    build_clickhouse_filters,
    build_published_facts_where,
    build_published_projection_where,
    finite_float,
    format_iso_utc,
    format_iso_utc_or_empty,
    inclusive_minute_end,
    optional_int,
    parse_time_window,
    percentiles,
    range_value,
    raw_query_producer,
    request_filter_clauses,
    resolve_asn_display_labels,
    sketch,
    text,
    visible_fact_clauses,
    visible_publication_fence_sql,
)
from backend.high_scale.archive import (
    ArchiveManifest,
    ArchivePublication,
    ArchiveSourceObject,
    ArchiveState,
    BaseClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    InsertReceipt,
    PublicationResult,
    PublicationStatus,
    S3ObjectStore,
    ServingWatermark,
    _batch_uuid,
    build_batch_id_from_manifest_id,
    row_matches_serving_domain,
    write_batch_archive_checkpoint,
)
from backend.high_scale.ingest_controller import coordinator as _coordinator_mod
from backend.high_scale.ingest_controller import service as _service_mod
from backend.high_scale.ingest_controller.coordinator import (
    _RUM_SIBLING_DOMAIN,
    _TERMINAL_SOURCE_STATES,
    HIGH_SCALE_MAX_DEAD_LETTER_RATIO,
    HIGH_SCALE_PAGE_MAX_BYTES,
    INITIAL_SOURCE_CURSOR,
    MAX_SOURCE_PAGE_SIZE,
    RUM_REINSPECTION_INTERVAL,
    RUM_SOURCE_DOMAINS,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    BatchIngestResult,
    HighScaleControlPlane,
    HighScaleIngestController,
    HighScaleWorkerCoordinator,
    PageRun,
    S3SourceObjectLister,
    S3SourceObjectReader,
    SourceControlPlane,
    SourceObjectDescriptor,
    SourceObjectLister,
    SourceObjectMissingError,
    SourceObjectPage,
    SourceObjectReader,
    _client_error_code,
    _enrich_high_scale_ngwaf_bots,
    _owner_fields,
    _source_checksum,
)
from backend.high_scale.ingest_controller.service import (
    _AGGREGATE_TABLES,
    _ASSET_TYPE_CASE,
    _ORIGIN_COLUMNS,
    _SOURCE_FIELD_OVERRIDES,
    _WATERMARK_TABLES,
    DIMENSIONS_BY_DOMAIN,
    AggregateBatchAdapter,
    HighScaleService,
    HighScaleServiceRegistry,
    HighScaleServiceRegistryProtocol,
    ServingClient,
    _bucket_start,
    _datetime_or_none,
    _default_archive_factory,
    _default_control_factory,
    _memo,
    _registry,
    _resolve_watermark,
    _response_cache,
    _string_or_none,
    _watermark,
    assets_aggregates,
    compute_dimension_counts,
    compute_summary_from_dims,
    get_high_scale_service_registry,
    register_high_scale_services,
    register_high_scale_services_from_environment,
)
from backend.models.analytics import AssetsAggregatesResponse, AssetsRequest
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

logger = logging.getLogger("backend.high_scale.ingest_controller")
_SUBMODULES = (_service_mod, _coordinator_mod)

from backend.high_scale.cmcd import project_cmcd  # noqa: E402
from backend.high_scale.network import build_network_projection_rows  # noqa: E402
from backend.high_scale.origin import build_origin_projection_rows  # noqa: E402
from backend.high_scale.performance import build_performance_projection_rows  # noqa: E402
from backend.high_scale.query_service import DecodeResult, QueryClient, decode_source_object  # noqa: E402
from backend.high_scale.security import build_security_projection_rows  # noqa: E402

for _k, _v in (
    ("project_cmcd", project_cmcd),
    ("build_network_projection_rows", build_network_projection_rows),
    ("build_origin_projection_rows", build_origin_projection_rows),
    ("build_performance_projection_rows", build_performance_projection_rows),
    ("DecodeResult", DecodeResult),
    ("QueryClient", QueryClient),
    ("decode_source_object", decode_source_object),
    ("build_security_projection_rows", build_security_projection_rows),
):
    setattr(_service_mod, _k, _v)
    setattr(_coordinator_mod, _k, _v)

import backend.high_scale.recovery as recovery  # noqa: E402
import backend.high_scale.rum as rum  # noqa: E402
from backend.high_scale._common import bucket as bucket  # noqa: E402
from backend.high_scale.archive import (  # noqa: E402
    ProjectionReplayResult,
    _batch_from_rows,
    _load_manifest_request_events,
    replay_cmcd_projections,
    replay_manifest,
    replay_origin_projections,
    replay_performance_projections,
)
from backend.high_scale.postgres_control import (  # noqa: E402
    DeletionController,
    HighScaleDeletionSweeper,
    PostgresDeletionLedger,
)
from backend.high_scale.recovery import (  # noqa: E402
    _DOMAINS,
    _IGNORED_CONTENT,
    _ORIGIN_PROJECTION_TABLES,
    _READ_LIMITS,
    _RUM_FACT_TABLES,
    RecoveryCapacityMeasurement,
    RecoveryCapacityReport,
    RecoveryReport,
    RumRecoveryBatchAdapter,
    RumRecoveryPlan,
    _canonical,
    _content,
    _evaluate_capacity_gate,
    _purge_since,
    _rebuild_request_projections_from_fos,
    _replay_all,
    _sorted,
    _SqlClient,
    evaluate_recovery_capacity,
    execute_rum_recovery,
    plan_rum_recovery,
    purge_cmcd_projections,
    purge_origin_projections,
    purge_rum_facts,
    rebuild_cmcd_projections_from_fos,
    rebuild_origin_projections_from_fos,
    rebuild_performance_projections_from_fos,
    rebuild_rum_facts_from_fos,
    rebuild_security_projections_from_fos,
    rebuild_serving_state_from_fos,
    replay_security_projections,
)
from backend.high_scale.rum import (  # noqa: E402
    _BEACON_IDENTITY,
    _FILTER_COLUMNS,
    _RUM_FILTERS_ADAPTER,
    _filter_clauses,
    _iso,
    _ttfb_ms,
    parse_rum_filters,
    rum_analytics,
    rum_beacon_health,
    rum_events,
    rum_live_events,
)

_WORKER_EXPORTS = frozenset(
    {
        "FairPageSelector",
        "HighScaleWorkerLoop",
        "WorkerPageResult",
        "_csv_env",
        "_MultiServiceWorkerLoop",
        "_weights_from_env",
        "build_worker_from_environment",
        "main",
    }
)


def __getattr__(name: str) -> Any:
    if name == "worker" or name in _WORKER_EXPORTS:
        import backend.high_scale.worker as _worker_mod

        globals()["worker"] = _worker_mod
        for _attr in _WORKER_EXPORTS:
            globals()[_attr] = getattr(_worker_mod, _attr)
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


orchestration = sys.modules[__name__]


class _ShimModule(types.ModuleType):
    def __setattr__(self, name: str, value: object) -> None:
        super().__setattr__(name, value)
        if name.startswith("__") and name.endswith("__"):
            return
        pkg = sys.modules.get("backend.high_scale.ingest_controller")
        if pkg is not None and pkg is not self:
            types.ModuleType.__setattr__(pkg, name, value)
        for mod in _SUBMODULES:
            if mod is not self:
                types.ModuleType.__setattr__(mod, name, value)


for _mod in (sys.modules[__name__], *_SUBMODULES):
    _mod.__class__ = _ShimModule

__all__ = [
    "contextvars",
    "inspect",
    "logging",
    "os",
    "sys",
    "tempfile",
    "Counter",
    "defaultdict",
    "Callable",
    "Iterable",
    "Mapping",
    "Sequence",
    "ThreadPoolExecutor",
    "dataclass",
    "field",
    "replace",
    "UTC",
    "datetime",
    "Path",
    "Any",
    "Protocol",
    "_BOOLEAN",
    "_COLUMNS",
    "_DOMAIN_IDENT_RE",
    "_LOG_GAMMA",
    "_ROUNDED",
    "_TIMESERIES_METRICS",
    "_UNSUPPORTED",
    "GAMMA",
    "MAX_PAGE_SIZE",
    "RELATIVE_ERROR",
    "VISIBLE_PUBLICATION_FENCE_SQL",
    "ArchiveManifestCatalog",
    "KeysetCursor",
    "_b64",
    "_bucket",
    "_bucket_case",
    "_centroid",
    "_column",
    "_row_timestamp",
    "_unb64",
    "build_clickhouse_filters",
    "build_published_facts_where",
    "build_published_projection_where",
    "finite_float",
    "format_iso_utc",
    "format_iso_utc_or_empty",
    "inclusive_minute_end",
    "optional_int",
    "parse_time_window",
    "percentiles",
    "range_value",
    "raw_query_producer",
    "request_filter_clauses",
    "resolve_asn_display_labels",
    "sketch",
    "text",
    "visible_fact_clauses",
    "visible_publication_fence_sql",
    "ArchiveManifest",
    "ArchivePublication",
    "ArchiveSourceObject",
    "ArchiveState",
    "BaseClickHouseBatchAdapter",
    "ClickHousePublication",
    "HighScaleBatch",
    "InsertReceipt",
    "PublicationResult",
    "PublicationStatus",
    "S3ObjectStore",
    "ServingWatermark",
    "_batch_uuid",
    "build_batch_id_from_manifest_id",
    "row_matches_serving_domain",
    "write_batch_archive_checkpoint",
    "HIGH_SCALE_PAGE_MAX_BYTES",
    "HIGH_SCALE_MAX_DEAD_LETTER_RATIO",
    "_RUM_SIBLING_DOMAIN",
    "BatchIngestResult",
    "HighScaleControlPlane",
    "HighScaleIngestController",
    "_enrich_high_scale_ngwaf_bots",
    "MAX_SOURCE_PAGE_SIZE",
    "INITIAL_SOURCE_CURSOR",
    "TERMINAL_SOURCE_CURSOR_PREFIX",
    "RUM_REINSPECTION_INTERVAL",
    "RUM_SOURCE_DOMAINS",
    "SourceObjectMissingError",
    "SourceObjectDescriptor",
    "SourceObjectPage",
    "SourceObjectLister",
    "S3SourceObjectLister",
    "S3SourceObjectReader",
    "_client_error_code",
    "_source_checksum",
    "SourceObjectReader",
    "SourceControlPlane",
    "PageRun",
    "_owner_fields",
    "HighScaleWorkerCoordinator",
    "_TERMINAL_SOURCE_STATES",
    "_resolve_watermark",
    "HighScaleService",
    "HighScaleServiceRegistryProtocol",
    "HighScaleServiceRegistry",
    "_registry",
    "get_high_scale_service_registry",
    "ServingClient",
    "_WATERMARK_TABLES",
    "register_high_scale_services",
    "register_high_scale_services_from_environment",
    "_default_control_factory",
    "_default_archive_factory",
    "_watermark",
    "_datetime_or_none",
    "_string_or_none",
    "DIMENSIONS_BY_DOMAIN",
    "_SOURCE_FIELD_OVERRIDES",
    "compute_dimension_counts",
    "_AGGREGATE_TABLES",
    "_ORIGIN_COLUMNS",
    "_bucket_start",
    "AggregateBatchAdapter",
    "_memo",
    "_response_cache",
    "_ASSET_TYPE_CASE",
    "compute_summary_from_dims",
    "assets_aggregates",
    "AssetsAggregatesResponse",
    "AssetsRequest",
    "ResponseMemo",
    "bucket_time_to_minute",
    "digest_cache_key",
    "serialize_filters_for_key",
    "_CacheRegistry",
    "logger",
    "project_cmcd",
    "build_network_projection_rows",
    "build_origin_projection_rows",
    "build_performance_projection_rows",
    "DecodeResult",
    "QueryClient",
    "decode_source_object",
    "build_security_projection_rows",
    "recovery",
    "rum",
    "bucket",
    "ProjectionReplayResult",
    "_batch_from_rows",
    "_load_manifest_request_events",
    "replay_cmcd_projections",
    "replay_manifest",
    "replay_origin_projections",
    "replay_performance_projections",
    "DeletionController",
    "HighScaleDeletionSweeper",
    "PostgresDeletionLedger",
    "_DOMAINS",
    "_IGNORED_CONTENT",
    "_ORIGIN_PROJECTION_TABLES",
    "_READ_LIMITS",
    "_RUM_FACT_TABLES",
    "RecoveryCapacityMeasurement",
    "RecoveryCapacityReport",
    "RecoveryReport",
    "RumRecoveryBatchAdapter",
    "RumRecoveryPlan",
    "_canonical",
    "_content",
    "_evaluate_capacity_gate",
    "_purge_since",
    "_rebuild_request_projections_from_fos",
    "_replay_all",
    "_sorted",
    "_SqlClient",
    "evaluate_recovery_capacity",
    "execute_rum_recovery",
    "plan_rum_recovery",
    "purge_cmcd_projections",
    "purge_origin_projections",
    "purge_rum_facts",
    "rebuild_cmcd_projections_from_fos",
    "rebuild_origin_projections_from_fos",
    "rebuild_performance_projections_from_fos",
    "rebuild_rum_facts_from_fos",
    "rebuild_security_projections_from_fos",
    "rebuild_serving_state_from_fos",
    "replay_security_projections",
    "_BEACON_IDENTITY",
    "_FILTER_COLUMNS",
    "_RUM_FILTERS_ADAPTER",
    "_filter_clauses",
    "_iso",
    "_ttfb_ms",
    "parse_rum_filters",
    "rum_analytics",
    "rum_beacon_health",
    "rum_events",
    "rum_live_events",
    "_WORKER_EXPORTS",
    "__getattr__",
    "orchestration",
]
