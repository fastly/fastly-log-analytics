"""Per-path directory-size cache shared by sync-status + health endpoints."""

from __future__ import annotations

import os

_DIR_SIZE_CACHE: dict[str, tuple[float, int]] = {}
_DIR_SIZE_TTL_S = 30.0


def _get_dir_size(path: str) -> int:
    # Cache results per-path with a 30s TTL. The cache walk is O(files-in-tree)
    # and the per-service cache grew from ~300 files to ~19k after the rollups
    # backfill (one parquet per field × hour). At ~700ms per uncached walk,
    # SyncStatusBadge's 15s poll was paying that cost on every refresh; the
    # cache turns it into a single getsize_sum sweep per minute.
    #
    # Files only grow incrementally (ingest + rollup-recompute) so a 30s
    # staleness window means the dashboard's reported disk usage can lag by
    # at most that window. Worth it for the perf vs measuring exact-to-the-
    # millisecond size on a poll endpoint.
    import time as _t

    now = _t.monotonic()
    cached = _DIR_SIZE_CACHE.get(path)
    if cached is not None and (now - cached[0]) < _DIR_SIZE_TTL_S:
        return cached[1]
    total = _scan_dir_size(path)
    _DIR_SIZE_CACHE[path] = (now, total)
    return total


def _scan_dir_size(path: str) -> int:
    total = 0
    if not os.path.exists(path):
        return 0
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_file():
                    total += entry.stat().st_size
                elif entry.is_dir():
                    total += _scan_dir_size(entry.path)
    except Exception:
        pass
    return total
