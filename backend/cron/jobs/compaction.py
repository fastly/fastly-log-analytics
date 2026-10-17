"""Local + rollup compaction crons.

* ``_run_local_compact`` — frequent merge of small parquet files in the
  LOCAL CACHE only (does NOT touch FOS). Free in terms of cloud cost, so
  we run it on a 2 min interval.
* ``_run_rollup_hour_heal`` — hourly self-heal that rebuilds hour bundles
  for closed hours the per-sync recompute missed. The per-sync path only
  fires when a sync batch ingests rows STAMPED in an already-closed hour
  (delivery-lag straddling the boundary); on bursty/low-traffic services
  the burst ends mid-hour, nothing straddles, and the closed hour never
  gets a rollup — the top-N reader then silently under-counts every
  window touching that hour until the nightly pass. Hourly cadence caps
  that staleness at ~1 hour.
* ``_run_rollup_compact_daily`` — consolidates per-hour rollup parquet
  into per-day files for closed days, slashing file-open overhead on
  7-day dashboard queries. Also runs the same self-heal with a 30-day
  lookback as a deep pass.
"""

from __future__ import annotations

import logging
import os
import shutil
import time

from backend.cron.decorators import cron_task
from backend.cron.scheduler import (
    _display_label,
)

logger = logging.getLogger("backend.scheduler")


def _ledger_touched_hours(service_id: str, lookback_s: float) -> set[str]:
    """Hours ('YYYY-MM-DD-HH', matching strftime %Y-%m-%d-%H) that received
    ledger commits within ``lookback_s`` — the celery-mode analogue of the
    sync path's ingest-reported touched_hours. Parsed from the raw object
    keys' year=/month=/day=/hour= segments."""
    import re

    from backend.core.metadata.base import get_con

    rows = (
        get_con(service_id)
        .execute(
            "SELECT DISTINCT object_key FROM ingest_ledger WHERE service_id = ? AND committed_at >= ?",
            (service_id, time.time() - lookback_s),
        )
        .fetchall()
    )
    hours: set[str] = set()
    for (key,) in rows:
        m = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})", key)
        if m:
            hours.add(f"{m.group(1)}-{m.group(2)}-{m.group(3)}-{m.group(4)}")
    return hours


@cron_task("local_compact", job_name="local_compact")
def _run_local_compact(service_id: str) -> None:
    """Frequent job: merge small parquet files in the LOCAL CACHE only.

    Does NOT touch FOS — only rewrites files inside cache/<bucket>/data/
    so DuckDB's view-glob picks up fewer files at query time. Free in
    terms of FOS cost (no 30-day-minimum penalty), so we can run it
    aggressively (every 2 min) without billing impact.

    Distinct from ``_run_optimize`` which writes through PyIceberg and
    DOES update FOS.
    """
    from backend.core import local_compaction as _lc
    from backend.core.duckdb import get_source_for_service
    from backend.cron.lifecycle import cron_run
    from backend.utils.active_requests import should_defer_cron

    # Active-request gate (perf #84): cache compaction holds the DuckDB
    # write lock for hundreds of ms — defer when API requests are in flight.
    if should_defer_cron("local_compact", service_id):
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    with cron_run(
        service_id,
        "local_compact",
        source=src,
        skip_log_prefix="local-compact",
        error_summary="local compaction failed",
    ) as run:
        if not run.ok:
            return
        _display = _display_label(src, service_id)
        logger.info("🏎️  \x1b[96m[local-compact]\x1b[0m %s: Local compaction started.", _display)
        run.log_progress({"type": "status", "message": "Scanning local cache partitions..."})

        result = _lc.compact_local_partitions(src)
        errors = result.get("errors") or []
        merged = result.get("files_merged", 0)
        removed = result.get("files_removed", 0)
        partitions = result.get("partitions_compacted", 0)
        summary = (
            f"Compacted {partitions} partition(s): merged {merged} small file(s) into "
            f"{partitions} (removed {removed} originals)"
        )

        # Celery-mode rollup seam: converts write straight to the lake — no
        # cache files land, so the per-sync recompute in jobs/sync.py never
        # fires and every dashboard panel would raw-scan the lake forever
        # (exactly what rollups exist to prevent at scale). Derive the hours
        # that received commits from the ledger and recompute their rollups
        # here — this job is backend-local, so the rollup writers read the
        # pod's DuckDB view without cross-process file-lock contention.
        # recompute_touched_hours excludes the active hour and is idempotent,
        # so the overlapping 15-min lookback just re-heals late arrivals.
        from backend import config as svcconfig

        if svcconfig.is_high_scale_mode(src):
            try:
                rollup_hours = _ledger_touched_hours(service_id, lookback_s=15 * 60)
                if rollup_hours:
                    from backend.core.rollups.recompute import recompute_touched_hours

                    recompute_touched_hours(service_id, src, rollup_hours)
                    summary += f"; rollups recomputed for {len(rollup_hours)} ledger hour(s)"
            except Exception as e:
                errors = list(errors) + [f"high-scale rollup recompute failed: {e}"]
                logger.warning("[local-compact] %s: celery rollup recompute failed: %s", service_id, e)
        if errors:
            err_preview = "\n".join(errors[:3])
            if len(errors) > 3:
                err_preview += f"\n... ({len(errors) - 3} more)"
            summary += f" — {len(errors)} partition error(s)"
            run.warn(summary, error_message=err_preview, files_downloaded=merged)
        else:
            run.success(summary, files_downloaded=merged)
        run.log_progress({"type": "status", "message": summary})
        logger.info("🏁  \x1b[96m[local-compact]\x1b[0m %s: %s in %.2fs", _display, summary, run.elapsed())


@cron_task("rollup_hour_heal", job_name="rollup_hour_heal")
def _run_rollup_hour_heal(
    service_id: str,
    manual: bool = False,
    run_id: int | None = None,
) -> dict:
    """Hourly job: rebuild hour bundles for closed hours the per-sync
    recompute missed.

    The per-sync recompute (``recompute_touched_hours``) only runs when
    sync touches an hour. A closed hour with delivery-lagged rows arriving
    only gets its rollup when a LATER sync batch ingests rows stamped
    inside it. Bursty services (burst ends mid-hour, all rows delivered
    before the boundary) never retrigger — diagnosed 2026-07-06 as top-N
    cards silently missing every closed hour of the current day on the
    low-traffic service. Reuses the idempotent
    ``backfill_missing_hour_bundles`` self-heal. Durable-mode startup coverage
    uses the dashboard's 1-day lookback so the first successful heal can
    enable the fast path without waiting for the daily 30-day deep pass. The
    daily compaction job keeps its 30-day deep pass.

    LOCAL-only writes (rollup parquet under cache/) — no FOS traffic, so
    it is safe under the dev kill switch alongside local_compact /
    rollup_compact.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run
    from backend.core.rollups import backfill_missing_hour_bundles
    from backend.cron.lifecycle import cron_run
    from backend.utils.active_requests import should_defer_cron

    # The heal's view scan + per-field COPY are CPU-bound; defer when API
    # requests are in flight (same politeness gate as local_compact).
    if not manual and should_defer_cron("rollup_hour_heal", service_id):
        logger.info("⏸️  [rollup-heal] %s: deferring due to active user queries", service_id)
        return {
            "status": "deferred",
            "service_id": service_id,
            "summary": "Deferred due to active queries",
        }

    src = get_source_for_service(service_id)
    if src is None:
        return {"status": "error", "service_id": service_id, "summary": "Source not found"}

    # ENOSPC safety check (< 50MB free disk space)
    from backend.core.rollups._common import _hour_bundled_root

    hour_root = _hour_bundled_root(src)
    check_dir = hour_root if os.path.isdir(hour_root) else "."
    try:
        free_bytes = shutil.disk_usage(check_dir).free
        if free_bytes < 50 * 1024 * 1024:
            msg = f"Disk space critically low (< 50MB free: {free_bytes / (1024 * 1024):.1f}MB)"
            logger.warning("⚠️  [rollup-heal] %s: %s", service_id, msg)
            log_cron_run(
                src,
                "rollup_hour_heal",
                0.0,
                "warning",
                summary="Skipped due to low disk space",
                error_message=msg,
                run_id=run_id,
            )
            return {"status": "warning", "service_id": service_id, "summary": msg}
    except Exception as e:
        logger.warning("[rollup-heal] %s: failed to check disk space: %s", service_id, e)

    _display = _display_label(src, service_id)
    outcome: dict = {}
    with cron_run(
        service_id,
        "rollup_hour_heal",
        source=src,
        run_id=run_id,
        error_summary="hour-bundle self-heal failed",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "error_message": str(exc),
            "summary": "hour-bundle self-heal failed",
            "duration_s": run.elapsed(),
            "run_id": run.run_id,
        },
        skip_log_prefix="rollup-heal",
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}
        if run.aborted:
            return dict(run.result or {})

        durable_mode = svcconfig.is_durable_serving_mode(src)
        if durable_mode:
            from backend.core.rollup_readiness import rollup_coverage_ready

            coverage_ready = rollup_coverage_ready(service_id)
        else:
            coverage_ready = True
        lookback_days = 2
        max_missing_hours = 1 if durable_mode and not coverage_ready else None
        heal = backfill_missing_hour_bundles(
            service_id,
            src,
            lookback_days=lookback_days,
            max_missing_hours=max_missing_hours,
        )
        # Durable mode becomes trusted once the dashboard lookback is
        # complete. A failed or timed-out startup catch-up self-heals on the
        # next tick.
        if durable_mode and not coverage_ready and heal.get("coverage_verified", False):
            try:
                from backend.core.rollup_readiness import mark_rollup_coverage_ready

                mark_rollup_coverage_ready(service_id)
            except Exception as e:
                logger.warning("[rollup-heal] %s: could not mark rollup coverage ready: %s", service_id, e)
        summary = (
            f"Healed {heal.get('missing', 0)} missing hour(s): "
            f"{heal.get('rebuilt_fields', 0)} field rollup(s) rebuilt, "
            f"{heal.get('bundled', 0)} hour(s) bundled, "
            f"{heal.get('stamped_empty', 0)} empty hour(s) stamped"
        )
        duration = run.elapsed()
        run.success(summary)
        if heal.get("missing", 0) or heal.get("stamped_empty", 0):
            logger.info("🏁  [rollup-heal] %s: %s in %.2fs", _display, summary, duration)
        outcome = {
            "status": "success",
            "service_id": service_id,
            "missing": heal.get("missing", 0),
            "rebuilt_fields": heal.get("rebuilt_fields", 0),
            "bundled": heal.get("bundled", 0),
            "stamped_empty": heal.get("stamped_empty", 0),
            "coverage_verified": heal.get("coverage_verified", False),
            "duration_s": duration,
            "summary": summary,
            "run_id": run.run_id,
        }
    return run.result if run.result is not None else outcome


# Re-exported from dedicated job module
from backend.cron.jobs.rollup_compact import _run_rollup_compact_daily  # noqa: F401
