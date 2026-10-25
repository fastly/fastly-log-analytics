"""Portable high-scale data-plane contracts and pure transformations."""

from backend.high_scale.aggregate_batch_adapter import (
    DIMENSIONS_BY_DOMAIN,
    compute_dimension_counts,
)
from backend.high_scale.models import (
    ARCHIVE_STATES,
    ArchiveState,
    RetentionRecommendation,
    ServiceProfile,
    StorageProfile,
    recommend_retention,
    validate_archive_transition,
)
from backend.high_scale.orchestration import (
    HighScaleWorkerCoordinator,
    PageRun,
)
from backend.high_scale.registry_bootstrap import (
    HighScaleService,
    HighScaleServiceRegistry,
    HighScaleServiceRegistryProtocol,
    get_high_scale_service_registry,
)
from backend.high_scale.schema import install_clickhouse_schema

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
    "compute_dimension_counts",
    "get_high_scale_service_registry",
    "install_clickhouse_schema",
    "recommend_retention",
    "validate_archive_transition",
]
