"""Background prewarmer cron for the default /api/insights selection.

The dashboard's default insights view (window=1h, baseline=168h) costs
~3.5 s per cold call on prod even after session-4's #74 column trim.
INSIGHTS_CACHE_TTL is 300 s, so a steady stream of users would mostly
hit the warm cache — but the cache is process-local and the first user
after any backend restart, cache eviction, or 5-min idle window pays
the full cost.

Running ``get_insights`` every 240 s per service (slightly under the
300 s TTL so the cache never expires) keeps that user-facing p50 at
the warm hit (~100 ms). Implementation mirrors :mod:`backend.cron.jobs.compaction`
exactly: ``@cron_task`` decorator + per-service start_cron_run + sync
DuckDB connection acquisition.

Active-request gate (#84) intentionally NOT applied here — the
prewarmer's whole point is to win during quiet moments; it doesn't
contend with user traffic the way sync/optimize do (no FOS calls,
just a single read-only DuckDB query against the local Iceberg view).
"""

from __future__ import annotations

import logging
import time

from backend.cron.decorators import cron_task
from backend.cron.scheduler import _display_label

logger = logging.getLogger("backend.scheduler")


@cron_task("insights_prewarmer")
def _run_insights_prewarmer(service_id: str) -> None:
    """Warm the insights cache for the default (window=1h, baseline=168h)
    selection so the first user lands on a cache hit instead of paying
    the ~3.5 s cold-path cost."""
    from backend.core.duckdb import get_connection, get_source_for_service, log_cron_run, start_cron_run
    from backend.repositories.insights import get_insights

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "insights_prewarmer")
    except RuntimeError as e:
        logger.info("⏭️  [insights-prewarmer] %s: skipping — %s", service_id, str(e))
        return

    _display = _display_label(src, service_id)

    started = time.time()
    con = None
    try:
        # Read-only + skip_view_update: insights queries against the live
        # Iceberg view; a few seconds of view staleness across cron-tick
        # boundaries is fine here (the next prewarmer tick will resolve
        # newly-bound view tables anyway).
        con = get_connection(source=src, max_wait=5, read_only=True, skip_view_update=True)
        result = get_insights(con, src, window_hours=1.0, baseline_hours=168.0)
        duration = time.time() - started
        was_cache_hit = bool(result.get("_is_cached"))
        log_cron_run(
            src,
            "insights_prewarmer",
            duration,
            "success",
            summary=f"Prewarmed default insights selection ({'cache-hit' if was_cache_hit else 'cache-miss'})",
            run_id=run_id,
        )
        logger.info(
            "✅ [insights-prewarmer] %s: prewarmed in %.2fs (%s)",
            _display,
            duration,
            "cache-hit" if was_cache_hit else "cache-miss",
        )
    except Exception as e:
        duration = time.time() - started
        log_cron_run(
            src,
            "insights_prewarmer",
            duration,
            "error",
            summary=f"Prewarm failed: {e}",
            error_message=str(e),
            run_id=run_id,
        )
        logger.warning("⚠️  [insights-prewarmer] %s: %s", _display, e)
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
