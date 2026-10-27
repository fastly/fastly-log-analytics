"""Portable high-scale data-plane contracts and pure transformations."""

from backend.high_scale import _common as _common  # noqa: E402
from backend.high_scale import aggregate_query as aggregate_query  # noqa: E402
from backend.high_scale import archive as archive  # noqa: E402
from backend.high_scale import cmcd as cmcd  # noqa: E402
from backend.high_scale import dashboard as dashboard  # noqa: E402
from backend.high_scale import ingest_controller as ingest_controller  # noqa: E402
from backend.high_scale import insights as insights  # noqa: E402
from backend.high_scale import network as network  # noqa: E402
from backend.high_scale import origin as origin  # noqa: E402
from backend.high_scale import performance as performance  # noqa: E402
from backend.high_scale import query as query  # noqa: E402
from backend.high_scale import query_service as query_service  # noqa: E402
from backend.high_scale import recovery as recovery  # noqa: E402
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
    "aggregate_query",
    "archive",
    "cmcd",
    "compute_dimension_counts",
    "dashboard",
    "get_high_scale_service_registry",
    "ingest_controller",
    "insights",
    "install_clickhouse_schema",
    "network",
    "origin",
    "performance",
    "query",
    "query_service",
    "recommend_retention",
    "recovery",
    "rum",
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
