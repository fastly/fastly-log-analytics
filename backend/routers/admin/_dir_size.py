"""Back-compat shim — the implementation moved to backend/utils/dir_size.py
so non-router callers (cron jobs, sync-status snapshot) don't import through
the routers package. Import from backend.utils.dir_size in new code."""

from backend.utils.dir_size import (  # noqa: F401
    _DIR_SIZE_CACHE,
    _get_dir_size,
    _scan_dir_size,
)
