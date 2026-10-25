"""Portable high-scale data-plane contracts and pure transformations."""

from backend.high_scale import _common as _common  # registers _filters alias
from backend.high_scale import archive_models as disk_sampler
from backend.high_scale import archive_models as models
from backend.high_scale import publication as schema
from backend.high_scale.aggregate_batch_adapter import (
    DIMENSIONS_BY_DOMAIN,
    compute_dimension_counts,
)
from backend.high_scale.archive_models import (
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
from backend.high_scale.publication import install_clickhouse_schema
from backend.high_scale.registry_bootstrap import (
    HighScaleService,
    HighScaleServiceRegistry,
    HighScaleServiceRegistryProtocol,
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
    "compute_dimension_counts",
    "disk_sampler",
    "get_high_scale_service_registry",
    "install_clickhouse_schema",
    "models",
    "recommend_retention",
    "schema",
    "validate_archive_transition",
]
