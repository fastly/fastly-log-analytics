"""Admin router package — ingest, sync status, raw file tree, download.

Carved out of a single 1,650-line ``admin.py`` for 10.9 file-size sweep.
Submodules attach endpoints to the shared :data:`router` from
:mod:`backend.routers.admin._router`. ``main.py`` includes
``admin.router`` once; importing this package side-effect-registers every
sub-module's endpoints onto it.

External surface (preserved for compat — main.py, bootstrap.py,
cron/jobs/sync.py, and tests/* import these by name):

- ``router``                          — the FastAPI APIRouter
- ``compute_sync_status_cached``      — bootstrap reuses for /api/bootstrap
- ``compute_log_accounting``          — sync.py reuses for the gap-heal cron
- ``LOG_ACCOUNTING_LOSS_THRESHOLD``,
  ``LOG_ACCOUNTING_MIN_RUN``          — shared with scheduler heal trigger
- ``SustainedLossAlert``              — re-exported from models.admin
- ``_QueueFile``, ``_stream_from_worker``,
  ``_fetch_file_to_zip``,
  ``_get_dir_size``, ``ClientDisconnected`` — internal helpers exercised
  directly by tests/routers/test_admin_mutation_endpoints.py
"""

from __future__ import annotations

import sys as _sys

# Re-export SustainedLossAlert so tests can do
# ``from backend.routers.admin import SustainedLossAlert`` (matches the
# pre-split surface where the model was transitively imported at module top).
from backend.models.admin import SustainedLossAlert  # noqa: F401

# Re-exports for the external import surface listed above. These run
# AFTER the side-effect imports so the sub-modules are loaded and the
# helper names are guaranteed to exist on the package.
from backend.utils.dir_size import _DIR_SIZE_CACHE, _get_dir_size  # noqa: F401

# Side-effect imports: each sub-module decorates the shared router.
# ``_helpers`` defines `router`; helpers + dir_size provide module-level
# state the endpoints rely on. Endpoint sub-modules then bind their
# routes onto the shared router instance.
from . import (  # noqa: F401
    _helpers,
    clickhouse,
    compaction,
    downloads,
    events,
    health,
    ingest,
    log_accounting,
    sync_status,
)
from ._helpers import (  # noqa: F401
    ClientDisconnected,
    _AbortableQueue,
    _fetch_file_to_zip,
    _QueueFile,
    _stream_from_worker,
    router,
)
from .log_accounting import (  # noqa: F401
    LOG_ACCOUNTING_LOSS_THRESHOLD,
    LOG_ACCOUNTING_MIN_RUN,
    compute_log_accounting,
)
from .sync_status import compute_sync_status_cached  # noqa: F401

# Back-compat submodule aliases for callers/tests patching legacy module paths.
_router = _helpers
bot_sources = compaction
iceberg = compaction
quarantine = ingest
_sys.modules.setdefault(f"{__name__}._router", _helpers)
_sys.modules.setdefault(f"{__name__}.bot_sources", compaction)
_sys.modules.setdefault(f"{__name__}.iceberg", compaction)
_sys.modules.setdefault(f"{__name__}.quarantine", ingest)
_sys.modules.setdefault("backend.routers.admin_usage", log_accounting)

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
