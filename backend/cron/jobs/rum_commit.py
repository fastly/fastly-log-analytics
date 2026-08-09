"""RUM commit cron job — compact local DuckDB tables to Iceberg.

Registered in scheduler as cron_rum_commit. Phase 3 implementation.
"""

from __future__ import annotations

import logging
import time

from backend.core.metadata.cron_log import finalize_cron_run_if_running, log_cron_run, start_cron_run
from backend.cron.decorators import cron_task

logger = logging.getLogger(__name__)


@cron_task("cron_rum_commit")
def _run_rum_commit(service_id: str, **kwargs) -> None:
    """Compact RUM tables from DuckDB cache to Iceberg/FOS (Phase 3 placeholder).

    Full implementation mirrors backend/cron/jobs/commit.py pattern.
    """
    start_time = time.time()
    run_id = start_cron_run(service_id, "rum_commit")

    try:
        logger.info(f"RUM commit starting for {service_id}")

        # Phase 3: Real implementation here
        # - Get iceberg instance for service
        # - Compact client_vitals + client_errors tables
        # - Track metrics (parquet_files_created, etc.)

        duration_s = time.time() - start_time
        log_cron_run(
            service_id,
            "rum_commit",
            duration_s,
            "done",
            rows_ingested=0,
            run_id=run_id,
        )
        logger.info("RUM commit complete")

    except Exception as e:
        logger.error(f"RUM commit failed: {e}", exc_info=True)
        duration_s = time.time() - start_time
        log_cron_run(
            service_id,
            "rum_commit",
            duration_s,
            "error",
            error_message=str(e),
            run_id=run_id,
        )
        raise
    finally:
        finalize_cron_run_if_running(service_id, "rum_commit", run_id)
