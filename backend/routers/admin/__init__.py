"""Admin router package — ingest, sync status, raw file tree, download."""

from __future__ import annotations

import sys as _sys

from backend.models.admin import SustainedLossAlert  # noqa: F401
from backend.utils.dir_size import _DIR_SIZE_CACHE, _get_dir_size  # noqa: F401
from backend.utils.sse import (  # noqa: F401
    ClientDisconnected,
    _AbortableQueue,
    _stream_from_worker,
)

# Unregister unwound admin submodules from backend._MODULE_ALIASES so standard
# Python import resolution loads their dedicated files under backend/routers/admin/.
_backend_mod = _sys.modules.get("backend")
_global_aliases = getattr(_backend_mod, "_MODULE_ALIASES", None)
if isinstance(_global_aliases, dict):
    for _unwound_submod in (
        "backend.routers.admin._helpers",
        "backend.routers.admin._router",
        "backend.routers.admin.admin_queries",
        "backend.routers.admin.bot_sources",
        "backend.routers.admin.clickhouse",
        "backend.routers.admin.compaction",
        "backend.routers.admin.downloads",
        "backend.routers.admin.events",
        "backend.routers.admin.health",
        "backend.routers.admin.high_scale",
        "backend.routers.admin.ingest",
        "backend.routers.admin.log_accounting",
        "backend.routers.admin.quarantine",
        "backend.routers.admin.sync_status",
    ):
        _global_aliases.pop(_unwound_submod, None)
    _global_aliases["backend.routers.admin_queries"] = "backend.routers.admin.admin_queries"
    _global_aliases["backend.routers.admin_usage"] = "backend.routers.admin.ingest"
    _global_aliases["backend.routers.high_scale"] = "backend.routers.admin.high_scale"
    _global_aliases["backend.routers.admin.system_metrics"] = "backend.routers.admin.health"
    _global_aliases["backend.system_metrics_sampler"] = "backend.routers.admin.health"

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

# Legacy module aliases that remain virtual (no dedicated file at that import path).
system_metrics = health
_sys.modules[f"{__name__}.system_metrics"] = health
_sys.modules["backend.routers.admin_usage"] = ingest
_sys.modules["backend.routers.admin_queries"] = admin_queries
_sys.modules["backend.routers.high_scale"] = high_scale
_sys.modules["backend.system_metrics_sampler"] = health
if "backend.routers" in _sys.modules:
    setattr(_sys.modules["backend.routers"], "admin_queries", admin_queries)
    setattr(_sys.modules["backend.routers"], "admin_usage", ingest)
    setattr(_sys.modules["backend.routers"], "high_scale", high_scale)


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
