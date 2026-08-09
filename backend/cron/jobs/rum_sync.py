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

    try:
        for event in ingest_rum_logs(service_id):
            if event[0] == "file_done":
                _, filename, count = event
                logger.debug(f"  {filename}: {count} rows")
            elif event[0] == "error":
                _, location, msg = event
                logger.warning(f"  Error in {location}: {msg}")
            elif event[0] == "done":
                _, total = event
                logger.info(f"RUM sync complete: {total} total rows")

    except Exception as e:
        logger.error(f"RUM sync failed: {e}", exc_info=True)
