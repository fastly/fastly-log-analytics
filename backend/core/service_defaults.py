"""Per-service setting defaults applied when a value is absent from a service's config.

Mirrors ``frontend/lib/serviceDefaults.ts`` and the Cloud app (``compute/cloud/src/logging/mod.rs``
``DEFAULT_LOG_PERIOD``, ``logging/rum.rs`` ``DEFAULT_RUM_LOG_PERIOD``, ``admin/model.rs``
``default_retention_days``). Change them together so every screen and both deployments describe
an unconfigured service the same way.
"""

from typing import Literal

DEFAULT_LOG_PERIOD_SECONDS = 10
DEFAULT_RUM_LOG_PERIOD_SECONDS = 300
DEFAULT_LOG_RETENTION_DAYS = 30
DEFAULT_RUM_RETENTION_DAYS = 30
SnapshotIntervalMinutes = Literal[5, 15, 30, 60, 360, 1440]
DEFAULT_SNAPSHOT_INTERVAL_MINUTES: SnapshotIntervalMinutes = 5
VALID_SNAPSHOT_INTERVAL_MINUTES: tuple[int, ...] = (5, 15, 30, 60, 360, 1440)
