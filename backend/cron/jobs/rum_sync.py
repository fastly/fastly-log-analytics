"""RUM sync cron job — ingest raw beacon logs from FOS.

Registered in scheduler as cron_rum_sync. Runs periodically (configurable interval).
"""

from __future__ import annotations

import logging

from backend.core.rum_ingest import ingest_rum_logs
from backend.cron.decorators import cron_task

logger = logging.getLogger(__name__)


@cron_task("cron_rum_sync")
def _run_rum_sync(service_id: str, **kwargs) -> None:
    """Sync RUM beacon logs from FOS raw/rum/ into local DuckDB tables.

    Calls ingest_rum_logs generator which handles orphan-row safety internally.
    """
    logger.info(f"RUM sync starting for {service_id}")

    from backend.cron_progress import add_progress, cleanup_progress_and_reap, end_progress, start_progress

    run_id = None
    try:
        for event in ingest_rum_logs(service_id):
            if event[0] == "started":
                run_id = event[1]
                start_progress(run_id, service_id=service_id, task="rum_sync")
                add_progress(run_id, {"type": "status", "message": f"RUM sync starting for {service_id}"})
            elif event[0] == "file_done":
                _, filename, count = event
                msg = f"{filename}: {count} rows"
                logger.debug(f"  {msg}")
                if run_id:
                    add_progress(run_id, {"type": "status", "message": msg})
            elif event[0] == "error":
                _, location, msg = event
                logger.warning(f"  Error in {location}: {msg}")
                if run_id:
                    add_progress(run_id, {"type": "error", "message": f"Error in {location}: {msg}"})
            elif event[0] == "cleanup_done":
                _, cleanup_files, cleanup_bytes = event
                msg = f"RUM cleanup deleted {cleanup_files} files, freed {cleanup_bytes / (1024 * 1024):.2f} MB"
                logger.info(msg)
                if run_id:
                    add_progress(run_id, {"type": "status", "message": msg})
            elif event[0] == "done":
                _, total = event
                msg = f"RUM sync complete: {total} total rows"
                logger.info(msg)
                if run_id:
                    add_progress(run_id, {"type": "status", "message": msg})
                    end_progress(run_id)

    except Exception as e:
        logger.error(f"RUM sync failed: {e}", exc_info=True)
        if run_id:
            add_progress(run_id, {"type": "error", "message": f"RUM sync failed: {e}"})
            end_progress(run_id, {"type": "error", "message": f"RUM sync failed: {e}"})
    finally:
        if run_id:
            cleanup_progress_and_reap()
