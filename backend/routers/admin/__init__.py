"""Admin router package — ingest, sync status, raw file tree, download."""

from __future__ import annotations

import sys as _sys

from backend.models.admin import SustainedLossAlert  # noqa: F401
from backend.utils.dir_size import _DIR_SIZE_CACHE, _get_dir_size  # noqa: F401

from . import compaction as compaction  # noqa: F401
from . import health as health  # noqa: F401, I001
from .compaction import high_scale_router  # noqa: F401
from .health import (  # noqa: F401
    LOG_ACCOUNTING_LOSS_THRESHOLD,
    LOG_ACCOUNTING_MIN_RUN,
    ClientDisconnected,
    _AbortableQueue,
    _fetch_file_to_zip,
    _QueueFile,
    _stream_from_worker,
    compute_log_accounting,
    compute_sync_status_cached,
    router,
)

# Back-compat submodule aliases for callers/tests patching legacy module paths.
log_accounting = health
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
_sys.modules[f"{__name__}.log_accounting"] = health
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
_sys.modules["backend.routers.admin_usage"] = health
_sys.modules["backend.routers.admin_queries"] = health
_sys.modules["backend.routers.high_scale"] = compaction
_sys.modules["backend.system_metrics_sampler"] = health
if "backend.routers" in _sys.modules:
    setattr(_sys.modules["backend.routers"], "admin_queries", health)
    setattr(_sys.modules["backend.routers"], "admin_usage", health)
    setattr(_sys.modules["backend.routers"], "high_scale", compaction)


__all__ = [
    "router",
    "high_scale_router",
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
