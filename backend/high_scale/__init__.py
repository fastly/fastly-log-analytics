"""Portable high-scale data-plane contracts and pure transformations."""

import sys as _sys

import backend.core.clickhouse as _ch_mod  # noqa: F401

_sys.modules.setdefault("backend.core.clickhouse_client", _ch_mod)
_sys.modules.setdefault("backend.core.clickhouse_manifest", _ch_mod)
_sys.modules.setdefault("backend.core.clickhouse_publication", _ch_mod)

_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    for _real_mod in (
        "backend.high_scale._common",
        "backend.high_scale.worker",
        "backend.high_scale.recovery",
        "backend.high_scale.rum",
        "backend.high_scale.query",
        "backend.high_scale.insights",
        "backend.high_scale.sessions",
        "backend.high_scale.aggregate_query",
        "backend.high_scale.performance",
        "backend.high_scale.cmcd",
        "backend.high_scale.dashboard",
        "backend.high_scale.origin",
        "backend.high_scale.security",
        "backend.high_scale.network",
    ):
        _backend_pkg._MODULE_ALIASES.pop(_real_mod, None)
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.high_scale.archive_publication": "backend.high_scale.archive",
            "backend.high_scale.archive_models": "backend.high_scale.archive",
            "backend.high_scale.archive_writer": "backend.high_scale.archive",
            "backend.high_scale.models": "backend.high_scale.archive",
            "backend.high_scale.disk_sampler": "backend.high_scale.archive",
            "backend.high_scale.retention_watermark": "backend.high_scale.archive",
            "backend.high_scale.publication": "backend.high_scale.archive",
            "backend.high_scale.historical_jobs": "backend.high_scale.archive",
            "backend.high_scale.replay": "backend.high_scale.archive",
            "backend.high_scale.schema": "backend.high_scale.archive",
            "backend.high_scale.decoder": "backend.high_scale.query",
            "backend.high_scale.insight_queries": "backend.high_scale.insights",
            "backend.high_scale.performance_projection": "backend.high_scale.performance",
            "backend.high_scale.origin_projection": "backend.high_scale.origin",
            "backend.high_scale._filters": "backend.high_scale._common",
            "backend.high_scale.rum_recovery": "backend.high_scale.recovery",
            "backend.high_scale.rum_ingest": "backend.high_scale.rum",
            "backend.high_scale.rum_aggregates": "backend.high_scale.rum",
            "backend.high_scale.rum_projection": "backend.high_scale.rum",
            "backend.high_scale.assets": "backend.high_scale.ingest_controller",
            "backend.high_scale.aggregate_batch_adapter": "backend.high_scale.ingest_controller",
            "backend.high_scale.registry_bootstrap": "backend.high_scale.ingest_controller",
            "backend.high_scale.orchestration": "backend.high_scale.ingest_controller",
            "backend.high_scale.source_discovery": "backend.high_scale.ingest_controller",
        }
    )

from backend.high_scale import _common as _common  # noqa: E402
from backend.high_scale import _common as _filters  # noqa: E402
from backend.high_scale import aggregate_query as aggregate_query  # noqa: E402
from backend.high_scale import archive as archive  # noqa: E402
from backend.high_scale import archive as archive_models  # noqa: E402
from backend.high_scale import archive as archive_publication  # noqa: E402
from backend.high_scale import archive as archive_writer  # noqa: E402
from backend.high_scale import archive as disk_sampler  # noqa: E402
from backend.high_scale import archive as historical_jobs  # noqa: E402
from backend.high_scale import archive as models  # noqa: E402
from backend.high_scale import archive as publication  # noqa: E402
from backend.high_scale import archive as retention_watermark  # noqa: E402
from backend.high_scale import archive as schema  # noqa: E402
from backend.high_scale import cmcd as cmcd  # noqa: E402
from backend.high_scale import dashboard as dashboard  # noqa: E402
from backend.high_scale import ingest_controller as aggregate_batch_adapter  # noqa: E402
from backend.high_scale import ingest_controller as assets  # noqa: E402
from backend.high_scale import ingest_controller as ingest_controller  # noqa: E402
from backend.high_scale import ingest_controller as orchestration  # noqa: E402
from backend.high_scale import ingest_controller as registry_bootstrap  # noqa: E402
from backend.high_scale import insights as insight_queries  # noqa: E402
from backend.high_scale import insights as insights  # noqa: E402
from backend.high_scale import network as network  # noqa: E402
from backend.high_scale import origin as origin  # noqa: E402
from backend.high_scale import origin as origin_projection  # noqa: E402
from backend.high_scale import performance as performance  # noqa: E402
from backend.high_scale import performance as performance_projection  # noqa: E402
from backend.high_scale import query as decoder  # noqa: E402
from backend.high_scale import query as query  # noqa: E402
from backend.high_scale import query_service as query_service  # noqa: E402
from backend.high_scale import recovery as recovery  # noqa: E402
from backend.high_scale import recovery as rum_recovery  # noqa: E402
from backend.high_scale import rum as rum  # noqa: E402
from backend.high_scale import security as security  # noqa: E402
from backend.high_scale import sessions as sessions  # noqa: E402
from backend.high_scale.archive import (  # noqa: E402
    ARCHIVE_STATES,
    ArchiveState,
    RetentionRecommendation,
    ServiceProfile,
    StorageProfile,
    install_clickhouse_schema,
    recommend_retention,
    validate_archive_transition,
)
from backend.high_scale.ingest_controller import (  # noqa: E402
    DIMENSIONS_BY_DOMAIN,
    HighScaleService,
    HighScaleServiceRegistry,
    HighScaleServiceRegistryProtocol,
    HighScaleWorkerCoordinator,
    PageRun,
    compute_dimension_counts,
    get_high_scale_service_registry,
)

__all__ = [
    "ARCHIVE_STATES",
    "ArchiveState",
    "DIMENSIONS_BY_DOMAIN",
    "HighScaleService",
    "HighScaleServiceRegistry",
    "HighScaleServiceRegistryProtocol",
    "HighScaleWorkerCoordinator",
    "PageRun",
    "RetentionRecommendation",
    "ServiceProfile",
    "StorageProfile",
    "_common",
    "_filters",
    "aggregate_batch_adapter",
    "aggregate_query",
    "archive",
    "archive_models",
    "archive_publication",
    "archive_writer",
    "assets",
    "cmcd",
    "compute_dimension_counts",
    "dashboard",
    "decoder",
    "disk_sampler",
    "get_high_scale_service_registry",
    "historical_jobs",
    "ingest_controller",
    "insight_queries",
    "insights",
    "install_clickhouse_schema",
    "models",
    "network",
    "orchestration",
    "origin",
    "origin_projection",
    "performance",
    "performance_projection",
    "publication",
    "query",
    "query_service",
    "recommend_retention",
    "recovery",
    "registry_bootstrap",
    "retention_watermark",
    "rum",
    "rum_recovery",
    "schema",
    "security",
    "sessions",
    "validate_archive_transition",
    "worker",
]


def __getattr__(name: str):
    if name == "worker":
        import importlib

        mod = importlib.import_module("backend.high_scale.worker")
        globals()["worker"] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
