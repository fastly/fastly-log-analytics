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
import time
from typing import Any

from backend.cron.decorators import cron_task
from backend.cron.lifecycle import cron_run
from backend.cron.scheduler import (
    JOB_COLORS,
    RESET_COLOR,
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
    from backend.core.duckdb import get_source_for_service
    from backend.core.rollups import backfill_missing_hour_bundles
    from backend.cron.scheduler import cron_run, record_skip_row
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
    from backend.cron.jobs.sync import has_sufficient_disk_space

    hour_root = _hour_bundled_root(src)
    ok_disk, msg = has_sufficient_disk_space(hour_root, "rollup-heal", service_id)
    if not ok_disk:
        record_skip_row(
            src,
            "rollup_hour_heal",
            "Skipped due to low disk space",
            status="warning",
            error_message=msg,
            run_id=run_id,
        )
        return {"status": "warning", "service_id": service_id, "summary": msg}

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
            from backend.core.rollups._common import rollup_coverage_ready

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
        # Ensure per-hour security/bot rollups are also backfilled for any closed
        # hours that already had all_fields/time_series bundles before those writers
        # were registered.
        if not durable_mode:
            try:
                from backend.core.rollups import (
                    backfill_security_proxies_bundles,
                    backfill_wellknown_bots_rollup,
                )

                backfill_wellknown_bots_rollup(service_id, src, lookback_days=lookback_days)
                backfill_security_proxies_bundles(service_id, src)
            except Exception as sec_heal_err:
                logger.warning(
                    "[rollup-heal] %s: security rollup self-heal skipped/failed: %s", service_id, sec_heal_err
                )
        # Also heal any historical RUM aggregate hours (up to 30d) missing
        # 'total_beacons' rows when RUM is enabled on this Standard service.
        try:
            cfg = svcconfig.load_config(service_id) or {}
            if cfg.get("rum", {}).get("enabled") and not durable_mode:
                from contextlib import closing

                from backend.core.duckdb import get_connection, rum_source_for
                from backend.core.iceberg import execute_with_stale_view_retry
                from backend.core.iceberg._ducklake import _ducklake_attach
                from backend.core.rollups.rum import backfill_missing_rum_aggregates

                rum_src = rum_source_for(src)
                with closing(get_connection(rum_src, read_only=False)) as rum_con:
                    try:
                        _ducklake_attach(rum_con, src, read_only=True)
                    except Exception:
                        pass
                    execute_with_stale_view_retry(
                        rum_con,
                        rum_src,
                        lambda c: backfill_missing_rum_aggregates(c, service_id, lookback_days=30),
                        table_name="client_vitals",
                    )
        except Exception as rum_heal_err:
            logger.warning("[rollup-heal] %s: RUM aggregate self-heal skipped/failed: %s", service_id, rum_heal_err)
        # Durable mode becomes trusted once the dashboard lookback is
        # complete. A failed or timed-out startup catch-up self-heals on the
        # next tick.
        if durable_mode and not coverage_ready and heal.get("coverage_verified", False):
            try:
                from backend.core.rollups._common import mark_rollup_coverage_ready

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


def _call_with_lookback(fn: Any, *args: Any, lookback_days: int = 30) -> Any:
    return fn(*args, lookback_days=lookback_days)


@cron_task("rollup_compact_daily", job_name="rollup_compact_daily")
def _run_rollup_compact_daily(
    service_id: str,
    manual: bool = False,
    run_id: int | None = None,
) -> dict:
    from backend.core import rollups
    from backend.core.duckdb import get_source_for_service
    from backend.core.rollups import (
        backfill_day_bundles,
        backfill_missing_hour_bundles,
        backfill_missing_hour_ip_spread,
        compact_closed_days_to_daily,
        retire_compacted_hour_bundles,
    )
    from backend.core.rollups._common import _day_bundled_root
    from backend.core.rollups.recompute import DAY_COMPACTORS, cleanup_old_rollups
    from backend.cron.jobs.sync import has_sufficient_disk_space
    from backend.cron.scheduler import cron_run, record_skip_row
    from backend.utils.active_requests import should_defer_cron

    if not manual and should_defer_cron("rollup_compact", service_id):
        logger.info("⏸️  [rollup-compact] %s: deferring due to active user queries", service_id)
        return {"status": "deferred", "service_id": service_id, "summary": "Deferred due to active queries"}

    src = get_source_for_service(service_id)
    if src is None:
        return {"status": "error", "service_id": service_id, "summary": "Source not found"}

    ok_disk, msg = has_sufficient_disk_space(_day_bundled_root(src), "rollup-compact", service_id)
    if not ok_disk:
        record_skip_row(
            src,
            "rollup_compact_daily",
            "Skipped due to low disk space",
            status="warning",
            error_message=msg,
            run_id=run_id,
        )
        return {"status": "warning", "service_id": service_id, "summary": msg}

    _display = _display_label(src, service_id)
    outcome: dict = {}
    with cron_run(
        service_id,
        "rollup_compact_daily",
        source=src,
        run_id=run_id,
        error_summary="Daily rollup compaction failed",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "summary": str(exc),
            "errors": [str(exc)],
        },
        include_log_output=False,
        progress_task="rollup_compact",
        skip_log_prefix="rollup-compact",
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}
        if run.aborted:
            return dict(run.result) if run.result is not None else outcome
        logger.info("🏎️  [rollup-compact] %s: Daily rollup compaction started.", _display)

        subsystem_errors: list[str] = []
        for label, fn in (
            ("missing_hour_bundles", backfill_missing_hour_bundles),
            ("ip_spread", backfill_missing_hour_ip_spread),
        ):
            try:
                _call_with_lookback(fn, service_id, src, lookback_days=30)
            except Exception as e:
                subsystem_errors.append(f"{label}: {e}")
                logger.warning("[rollup-compact] %s: %s self-heal failed: %s", _display, label, e)

        rebuilt = _call_with_lookback(compact_closed_days_to_daily, service_id, src, lookback_days=30)

        try:
            bundled = _call_with_lookback(backfill_day_bundles, service_id, src, lookback_days=30)
        except Exception as e:
            subsystem_errors.append(f"day_bundles: {e}")
            logger.warning("[rollup-compact] %s: day-bundle backfill failed: %s", _display, e)
            bundled = 0

        for spec in DAY_COMPACTORS:
            try:
                _call_with_lookback(getattr(rollups, spec.compactor), service_id, src, lookback_days=30)
            except Exception as e:
                subsystem_errors.append(f"{spec.label}: {e}")
                logger.warning("[rollup-compact] %s: %s failed: %s", _display, spec.label, e)

        retired_files, retired_bytes = 0, 0
        try:
            retired_files, retired_bytes = _call_with_lookback(
                retire_compacted_hour_bundles, service_id, src, lookback_days=30
            )
        except Exception as e:
            subsystem_errors.append(f"retirement: {e}")
            logger.warning("[rollup-compact] %s: retirement failed: %s", _display, e)

        try:
            retention_months = int(src.get("rollup_retention_months", 12))
            max_age_days = int(src.get("rollups_days", retention_months * 30))
            if max_age_days > 0:
                cleanup_old_rollups(service_id, src, max_age_days=max_age_days, hour_bundle_max_age_days=14)
        except Exception as e:
            logger.warning("[rollup-compact] %s: cleanup pass failed: %s", _display, e)

        summary = (
            f"Rebuilt {rebuilt} (field, day) file(s); bundled {bundled} day(s); "
            f"retired {retired_files} hourly file(s) ({retired_bytes / 1024:.1f}KB)."
        )
        status = "warning" if subsystem_errors else "success"
        err_msg = "; ".join(subsystem_errors) if subsystem_errors else None
        run.record(status, summary=summary, error_message=err_msg)
        outcome = {
            "status": status,
            "service_id": service_id,
            "rebuilt": rebuilt,
            "bundled": bundled,
            "retired_files": retired_files,
            "retired_bytes": retired_bytes,
            "duration_s": round(run.elapsed(), 2),
            "summary": summary,
            "errors": subsystem_errors if subsystem_errors else None,
        }
    return run.result if run.result is not None else outcome


@cron_task("partial_hour_merge", job_name="partial_hour_merge")
def _run_partial_hour_merge(service_id: str) -> None:
    """Fast (30s) tick that folds newly-landed buffer/active-hour-partition files into the pod-local partial-hour rollup."""
    from backend.core.duckdb import get_source_for_service
    from backend.core.field_registry import derived
    from backend.core.rollups.partial_hour import gc_stale_partial_hours, merge_partial_hour
    from backend.cron.lifecycle import cron_run
    from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS
    from backend.repositories.dashboard import FIELDS
    from backend.utils.active_requests import should_defer_cron

    if should_defer_cron("partial_hour_merge", service_id):
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    with cron_run(
        service_id,
        "partial_hour_merge",
        source=src,
        skip_log_prefix="partial-hour",
        error_summary="partial-hour merge failed",
    ) as run:
        if run.skipped:
            return

        logger.info("🕐  [partial-hour] %s: partial-hour merge started.", _display_label(src, service_id))
        derived_fields = {f.code for f in derived()}
        merge_fields = [f for f in FIELDS if f not in _LIVE_TOPN_SKIP_FIELDS and f not in derived_fields]

        stats = merge_partial_hour(service_id, src, merge_fields)
        removed = gc_stale_partial_hours(src)
        summary = f"Merged {stats['new_files']} new file(s) into hour={stats['hour']}; GC'd {removed} stale hour(s)"
        run.success(summary)
        if stats["new_files"]:
            run.log_progress({"type": "status", "message": summary})


@cron_task("cron:expire_snapshots", job_name="expire_snapshots")
def _run_expire_snapshots(service_id: str, *, manual: bool = False) -> dict:
    """Maintenance job: perform cloud maintenance including data deletion, cache cleanup, and snapshot expiry."""
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import get_source_for_service
    from backend.cron.scheduler import dev_mode_no_crons
    from backend.utils.active_requests import should_defer_cron

    if dev_mode_no_crons():
        return {"status": "skipped", "service_id": service_id, "summary": "FLA_DEV_NO_CRONS=1"}
    if not manual and should_defer_cron("expire_snapshots", service_id):
        return {"status": "deferred", "service_id": service_id, "summary": "active requests"}

    src = get_source_for_service(service_id)
    if src is None:
        return {"status": "skipped", "service_id": service_id, "summary": "service not found"}
    if src.get("access_level") == "read_only":
        return {"status": "skipped", "service_id": service_id, "summary": "read-only services cannot run maintenance"}

    svc_id = src.get("service_id", "unknown")
    display_name = _display_label(src, str(svc_id))

    with cron_run(
        service_id,
        "expire_snapshots",
        source=src,
        skip_log_prefix="expire",
        finalize_duration=True,
        finalize_running_backstop=True,
        error_summary="Maintenance raised an uncaught exception",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "error": str(exc),
            "summary": "Maintenance raised an uncaught exception",
        },
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}
        if run.ok:
            logger.info("🏎️  \x1b[90m[expire]\x1b[0m %s: Maintenance job started.", display_name)
            run.log_progress({"type": "status", "message": "Starting snapshot expiry and retention cleanup..."})
            result = db_iceberg.run_cloud_maintenance(src)
            if "error" in result:
                logger.warning(
                    "%s %s: %s", JOB_COLORS["expire"] + "[expire]" + RESET_COLOR, display_name, result["error"]
                )
                run.record(
                    "error",
                    error_message=str(result["error"]),
                    summary="Maintenance failed at catalog load",
                )
                run.result = {
                    **result,
                    "status": "error",
                    "service_id": service_id,
                    "summary": "Maintenance failed at catalog load",
                }
                run.log_progress({"type": "error", "message": str(result["error"])})
            else:
                result = {
                    **result,
                    "retention_deleted_rows": int(
                        result.get("retention_deleted_rows", 0)
                        or sum(
                            int(result.get(key, 0) or 0)
                            for key in ("data_rows_deleted", "rum_log_rows_deleted", "rum_beacon_rows_deleted")
                        )
                    ),
                    "snapshots_expired": int(
                        result.get("snapshots_expired", result.get("snapshots_expired_count", 0)) or 0
                    ),
                    "files_unlinked": int(result.get("files_unlinked", result.get("data_files_cleaned", 0)) or 0),
                }
                summary_parts = []
                sub_errors = []
                for k, v in result.items():
                    if k.endswith("_error"):
                        sub_errors.append(f"{k}={v}")
                    else:
                        summary_parts.append(f"{k}={v}")
                summary = ", ".join(summary_parts) if summary_parts else "no work to do"
                status = "warning" if sub_errors else "success"
                error_message = "; ".join(sub_errors) if sub_errors else None
                files_unlinked = result["files_unlinked"]
                logger.info("🗑️ \x1b[90m[expire]\x1b[0m %s: Maintenance completed. %s", display_name, result)
                run.record(
                    status,
                    error_message=error_message,
                    summary=summary,
                    files_deleted_fos=files_unlinked,
                )
                run.result = {**result, "status": status, "service_id": service_id, "summary": summary}
                event_type = "done" if status == "success" else status
                run.log_progress({"type": event_type, "message": summary})

    logger.info("🏁  \x1b[90m[expire]\x1b[0m %s: Maintenance job finished.", display_name)
    return dict(run.result) if isinstance(run.result, dict) else {}


@cron_task("cron.optimize", job_name="optimize")
def _run_optimize(service_id: str, manual: bool = False) -> dict | None:
    """Daily job: compact small Iceberg data files into target-sized ones."""
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import get_source_for_service
    from backend.cron.decorators import dev_mode_no_crons
    from backend.utils.active_requests import should_defer_cron

    if not manual and dev_mode_no_crons():
        logger.warning("🚫 [scheduler] %s: FLA_DEV_NO_CRONS=1 — optimize refused.", service_id)
        return {"status": "skipped", "service_id": service_id, "summary": "Skipped due to FLA_DEV_NO_CRONS=1"}

    if not manual and should_defer_cron("optimize", service_id):
        logger.info("⏸️  [optimize] %s: deferring due to active user queries", service_id)
        return {"status": "deferred", "service_id": service_id, "summary": "Deferred due to active queries"}

    src = get_source_for_service(service_id)
    if src is None:
        return {"status": "error", "service_id": service_id, "summary": "Source not found"}

    _display = _display_label(src, service_id)
    outcome: dict | None = None
    with cron_run(
        service_id,
        "optimize",
        source=src,
        error_summary="Iceberg optimize failed",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "error": str(exc),
            "files_rewritten": 0,
            "files_added": 0,
            "summary": "Iceberg optimize failed",
        },
        finalize_duration=True,
        skip_log_prefix="optimize",
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}

        logger.info("🏎️  \x1b[92m[optimize]\x1b[0m %s: Optimize job started.", _display)
        run.log_progress({"type": "status", "message": "Scanning Iceberg table for small files to compact..."})

        result = db_iceberg.optimize_table(src, min_files_per_partition=10)
        if "error" in result:
            run.fail("Iceberg optimize failed", error_message=result["error"])
            run.log_progress({"type": "error", "message": result["error"]})
            run.log_progress({"type": "warning", "message": result["error"]})
            outcome = {
                "status": "error",
                "service_id": service_id,
                "error": result["error"],
                "files_rewritten": 0,
                "files_added": 0,
                "summary": "Iceberg optimize failed",
            }
        else:
            summary = f"Rewrote {result.get('files_rewritten', 0)} files into {result.get('files_added', 0)} files"
            partition_errors = result.get("partition_errors") or []
            if partition_errors:
                eligible = result.get("eligible_partitions", 0)
                summary += f" — {len(partition_errors)}/{eligible} partitions failed"
                err_preview = "\n".join(partition_errors[:3])
                if len(partition_errors) > 3:
                    err_preview += f"\n... ({len(partition_errors) - 3} more)"
                status = "error" if result.get("files_added", 0) == 0 else "warning"
            else:
                err_preview = None
                status = "success"
            run.record(
                status,
                summary=summary,
                error_message=err_preview,
                parquet_files_optimized=result.get("files_rewritten", 0),
                parquet_files_created=result.get("files_added", 0),
            )
            event_type = "done" if status == "success" else status
            run.log_progress({"type": event_type, "message": summary})
            logger.info(
                "[scheduler] %s: optimize complete — %s",
                service_id,
                summary,
            )
            outcome = {
                "status": status,
                "service_id": service_id,
                "files_rewritten": result.get("files_rewritten", 0),
                "files_added": result.get("files_added", 0),
                "eligible_partitions": result.get("eligible_partitions", 0),
                "partition_errors": partition_errors,
                "summary": summary,
            }
    if run.result is not None:
        outcome = run.result

    from backend.cron.jobs.sync import publish_sync_status_safe

    publish_sync_status_safe(service_id, "scheduler")

    logger.info("🏁  \x1b[92m[optimize]\x1b[0m %s: Optimize job finished.", _display)
    return outcome
