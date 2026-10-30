"""Admin router package — ingest, sync status, raw file tree, download."""

from __future__ import annotations

from backend.models.admin import SustainedLossAlert  # noqa: F401
from backend.utils.dir_size import _DIR_SIZE_CACHE, _get_dir_size  # noqa: F401
from backend.utils.sse import (  # noqa: F401
    ClientDisconnected,
    _AbortableQueue,
    _stream_from_worker,
)

from . import _helpers as _helpers  # noqa: E402, F401
from . import _router as _router  # noqa: E402, F401
from . import admin_queries as admin_queries  # noqa: E402, F401
from . import bot_sources as bot_sources  # noqa: E402, F401
from . import clickhouse as clickhouse  # noqa: E402, F401
from . import compaction as compaction  # noqa: E402, F401
from . import downloads as downloads  # noqa: E402, F401
from . import events as events  # noqa: E402, F401
from . import health as health  # noqa: E402, F401
from . import high_scale as high_scale  # noqa: E402, F401
from . import ingest as ingest  # noqa: E402, F401
from . import log_accounting as log_accounting  # noqa: E402, F401
from . import quarantine as quarantine  # noqa: E402, F401
from . import sync_status as sync_status  # noqa: E402, F401
from ._helpers import _fetch_file_to_zip, _QueueFile  # noqa: E402, F401
from ._router import router  # noqa: E402, F401
from .high_scale import high_scale_router  # noqa: E402, F401
from .log_accounting import (  # noqa: E402, F401
    LOG_ACCOUNTING_LOSS_THRESHOLD,
    LOG_ACCOUNTING_MIN_RUN,
    compute_log_accounting,
)
from .sync_status import compute_sync_status_cached, meta_router  # noqa: E402, F401

# Back-compat attribute forwarding for callers/tests accessing symbols via
# health.<attr> or compaction.<attr> from prior module consolidations.
for _src_mod in (_helpers, clickhouse, events, sync_status, admin_queries, log_accounting):
    for _attr_name, _attr_val in vars(_src_mod).items():
        if not _attr_name.startswith("__") and not hasattr(health, _attr_name):
            setattr(health, _attr_name, _attr_val)

for _src_mod in (_helpers, bot_sources, ingest, quarantine, downloads, high_scale):
    for _attr_name, _attr_val in vars(_src_mod).items():
        if not _attr_name.startswith("__") and not hasattr(compaction, _attr_name):
            setattr(compaction, _attr_name, _attr_val)

system_metrics = health


__all__ = [
    "router",
    "high_scale_router",
    "meta_router",
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
