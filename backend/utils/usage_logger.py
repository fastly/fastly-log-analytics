"""Usage logger — flush collected FOS/CDN calls to _usage_log for cost analysis."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def flush_usage_log(service_id: str) -> None:
    """Write the current context's tracked FOS/CDN calls to _usage_log.

    Safe to call from cron jobs and API middleware — exits silently on any error
    or when usage logging is disabled globally.
    """
    try:
        from backend import config as svcconfig

        if not svcconfig.is_usage_logging_enabled():
            return

        from backend.utils.telemetry import _CALLS, get_process_context

        # Use the un-augmented view: get_tracked_calls() merges in iothread
        # rows that ALREADY came from usage_log. Round-tripping them through
        # log_usage_calls would duplicate the row.
        calls = _CALLS.get()
        if not calls:
            return

        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return

        source = svcconfig.config_to_source(cfg)
        process_context = get_process_context()

        from backend.core.duckdb import log_usage_calls

        log_usage_calls(source, calls, process_context=process_context)
    except Exception as e:
        logger.warning("[usage_logger] flush failed: %s", e)


def run_usage_log_cleanup(service_id: str) -> None:
    """Purge old rows from _usage_log per the configured retention period."""
    try:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return
        source = svcconfig.config_to_source(cfg)

        from backend.core.duckdb import purge_usage_log

        purge_usage_log(source)
    except Exception as e:
        logger.warning("[usage_logger] cleanup failed: %s", e)
