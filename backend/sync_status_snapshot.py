"""Cached sync-status snapshot, shared by the API and the cron jobs.

Lives at backend top level (like ``sync_status_publisher``) so cron jobs can
build the post-commit SSE snapshot without importing through
``backend.routers`` — the cron -> routers edge breaks import-linter's router
independence contract. ``backend/routers/admin/sync_status.py`` re-exports
this for its historical callers.
"""

from __future__ import annotations

import logging
import os

from backend.utils.dir_size import _get_dir_size

logger = logging.getLogger(__name__)


# Moved out of /admin/ so analysts can also see sync status / time bounds
# for their scoped service. The endpoint returns per-service timestamps and
# row counts — no admin-specific info. Service-scope is still enforced by
# RemoteAccessMiddleware via the x-service-id check on the request.
def compute_sync_status_cached(service_id: str | None) -> dict | None:
    """Return the cached sync-status payload for ``service_id`` without
    grabbing a DuckDB connection.

    Mirrors the ``skip_fos=true`` fast path of /api/sync-status:
    same shape, no DB hop, returns ``None`` when no cached status has
    been persisted yet (caller falls back to the dedicated endpoint).
    Extracted so /api/bootstrap can fold the status into its response
    (perf audit Phase D-2) and the dedicated endpoint can stay
    authoritative for explicit / force / non-cached paths.

    Caller is responsible for analyst-scope enforcement — the dedicated
    endpoint is admin-only via RemoteAccessMiddleware; this helper
    trusts the caller.
    """
    from backend import config as svcconfig
    from backend.core import duckdb as _db

    if not service_id:
        return None
    src = _db.get_source_for_service(service_id)
    if not src:
        return None
    cached_status = svcconfig.get_status(src["name"])
    if not cached_status:
        return None  # fall through to dedicated endpoint

    # Real-time SQLite metadata lookup overlay to bypass stale on-disk config cache
    try:
        import re

        from backend.core import metadata as metadata_db

        summary = metadata_db.get_ingested_files_status_summary(src["name"])
        latest_file_name = summary.get("latest_file_name")
        total_rows = summary.get("total_rows") or 0

        # Celery/ledger mode writes ingest_ledger, never ingested_files — the
        # header's "latest:" froze at cutover until the ledger was consulted
        # too. Take whichever source names the NEWER raw file (both filename
        # formats carry the minute timestamp the regex below parses).
        try:
            from backend.core.metadata.base import get_con as _get_meta_con

            # ORDER BY object_key: the keys embed year=/month=/.../minute= so
            # lexicographic order IS log-time order. Ordering by committed_at
            # instead surfaces whatever the workers finished last — during a
            # backlog drain that's an OLD file, understating freshness.
            lrow = (
                _get_meta_con(service_id)
                .execute(
                    "SELECT object_key FROM ingest_ledger WHERE service_id = ? AND status = 'committed' "
                    "ORDER BY object_key DESC LIMIT 1",
                    (service_id,),
                )
                .fetchone()
            )
            ledger_latest = lrow[0] if lrow else None
            if ledger_latest and (
                not latest_file_name or ledger_latest.split("/")[-1] > latest_file_name.split("/")[-1]
            ):
                latest_file_name = ledger_latest
        except Exception:
            pass

        if latest_file_name:
            fname = latest_file_name.split("/")[-1]
            m = re.search(r"(\d{4}-\d{2}-\d{2})[T-](\d{2}[:.-]\d{2}[:.-]\d{2})", fname)
            if m:
                latest_ingested_file_at = f"{m.group(1)} {m.group(2).replace('-', ':').replace('.', ':')}"
                cached_status["latest_ingested_file_at"] = latest_ingested_file_at
                cached_status["latest_available_file_at"] = latest_ingested_file_at
                cached_status["latest_log_at"] = f"{m.group(1)}T{m.group(2).replace('-', ':').replace('.', ':')}Z"

        if total_rows > 0 and (not cached_status.get("local_rows") or cached_status.get("local_rows") == 0):
            cached_status["local_rows"] = total_rows
    except Exception as e:
        logger.debug("[compute_sync_status_cached] failed to overlay real-time SQLite metadata: %s", e)

    cached_status["access_level"] = src.get("access_level", "read_write")
    cached_status["storage_mode"] = src.get("storage_mode", "cloud")
    cached_status["configured"] = True

    db_path = src.get("duckdb_path") or svcconfig.duckdb_path(service_id)
    db_exists = os.path.exists(db_path)
    db_size = os.path.getsize(db_path) if db_exists else 0
    cache_size = _get_dir_size(_db._cache_dir(src))
    cached_status["duckdb_size_bytes"] = db_size + cache_size
    cached_status["duckdb_exists"] = db_exists

    from backend.cron_progress import get_latest_progress_for_service

    active_run = get_latest_progress_for_service(service_id)
    if active_run:
        cached_status["active_run"] = active_run
        cached_status["busy"] = True

    cfg = svcconfig.load_config(service_id) or {}
    cached_status["ngwaf_workspace_id"] = cfg.get("ngwaf_workspace_id")
    return cached_status
