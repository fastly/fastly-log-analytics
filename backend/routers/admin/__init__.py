"""Admin router package — ingest, sync status, raw file tree, download."""

from __future__ import annotations

import sys as _sys

from backend.models.admin import SustainedLossAlert  # noqa: F401
from backend.utils.dir_size import _DIR_SIZE_CACHE, _get_dir_size  # noqa: F401

from . import (  # noqa: F401
    compaction,
    log_accounting,
)
from . import health as health  # noqa: F401, I001
from .health import (  # noqa: F401
    ClientDisconnected,
    _AbortableQueue,
    _fetch_file_to_zip,
    _QueueFile,
    _stream_from_worker,
    compute_sync_status_cached,
    router,
)
from .log_accounting import (  # noqa: F401
    LOG_ACCOUNTING_LOSS_THRESHOLD,
    LOG_ACCOUNTING_MIN_RUN,
    compute_log_accounting,
)

# Back-compat submodule aliases for callers/tests patching legacy module paths.
high_scale = health
_helpers = health
_router = health
clickhouse = health
events = health
sync_status = health
admin_queries = health
ingest = compaction
bot_sources = compaction
iceberg = compaction
quarantine = compaction
downloads = compaction
_sys.modules[f"{__name__}.high_scale"] = health
_sys.modules[f"{__name__}._helpers"] = health
_sys.modules[f"{__name__}._router"] = health
_sys.modules[f"{__name__}.clickhouse"] = health
_sys.modules[f"{__name__}.events"] = health
_sys.modules[f"{__name__}.sync_status"] = health
_sys.modules[f"{__name__}.ingest"] = compaction
_sys.modules[f"{__name__}.bot_sources"] = compaction
_sys.modules[f"{__name__}.iceberg"] = compaction
_sys.modules[f"{__name__}.quarantine"] = compaction
_sys.modules[f"{__name__}.downloads"] = compaction
_sys.modules["backend.routers.admin_usage"] = log_accounting
_sys.modules["backend.routers.admin_queries"] = health
_sys.modules["backend.system_metrics_sampler"] = health
if "backend.routers" in _sys.modules:
    setattr(_sys.modules["backend.routers"], "admin_queries", health)


__all__ = [
    "router",
    "compute_sync_status_cached",
    "compute_log_accounting",
    "LOG_ACCOUNTING_LOSS_THRESHOLD",
    "LOG_ACCOUNTING_MIN_RUN",
    "SustainedLossAlert",
    "ClientDisconnected",
    "_QueueFile",
    "_AbortableQueue",
    "_stream_from_worker",
    "_fetch_file_to_zip",
    "_DIR_SIZE_CACHE",
    "_get_dir_size",
]
