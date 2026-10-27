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

import importlib
import logging
import sys as _sys
import time
from types import ModuleType
from typing import Any

from backend import config as svcconfig
from backend.core import duckdb as _db  # noqa: F401
from backend.cron.scheduler import (
    _display_label,
    cron_run,
    cron_task,
    record_skip_row,
)

logger = logging.getLogger("backend.scheduler")


def _ledger_touched_hours(service_id: str, lookback_s: float) -> set[str]:
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
    return {
        f"{m.group(1)}-{m.group(2)}-{m.group(3)}-{m.group(4)}"
        for (key,) in rows
        if (m := re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})", key))
    }


@cron_task("local_compact", job_name="local_compact")
def _run_local_compact(service_id: str) -> None:
    from backend.core import storage as _lc  # type: ignore[attr-defined]
    from backend.core.duckdb import get_source_for_service
    from backend.utils.telemetry import should_defer_cron

    if should_defer_cron("local_compact", service_id) or (src := get_source_for_service(service_id)) is None:
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
        merged, removed, partitions = (
            result.get("files_merged", 0),
            result.get("files_removed", 0),
            result.get("partitions_compacted", 0),
        )
        summary = f"Compacted {partitions} partition(s): merged {merged} small file(s) into {partitions} (removed {removed} originals)"
        if svcconfig.is_high_scale_mode(src):
            try:
                if rollup_hours := _ledger_touched_hours(service_id, lookback_s=15 * 60):
                    from backend.core.rollups.recompute import recompute_touched_hours

                    recompute_touched_hours(service_id, src, rollup_hours)
                    summary += f"; rollups recomputed for {len(rollup_hours)} ledger hour(s)"
            except Exception as e:
                errors = list(errors) + [f"high-scale rollup recompute failed: {e}"]
                logger.warning("[local-compact] %s: celery rollup recompute failed: %s", service_id, e)
        if errors:
            err_preview = "\n".join(errors[:3]) + (f"\n... ({len(errors) - 3} more)" if len(errors) > 3 else "")
            summary += f" — {len(errors)} partition error(s)"
            run.warn(summary, error_message=err_preview, files_downloaded=merged)
        else:
            run.success(summary, files_downloaded=merged)
        run.log_progress({"type": "status", "message": summary})
        logger.info("🏁  \x1b[96m[local-compact]\x1b[0m %s: %s in %.2fs", _display, summary, run.elapsed())


@cron_task("rollup_hour_heal", job_name="rollup_hour_heal")
def _run_rollup_hour_heal(service_id: str, manual: bool = False, run_id: int | None = None) -> dict:
    from backend.core.duckdb import get_source_for_service
    from backend.core.rollups import backfill_missing_hour_bundles
    from backend.core.rollups._common import _hour_bundled_root
    from backend.cron.jobs.sync import has_sufficient_disk_space
    from backend.utils.telemetry import should_defer_cron

    if not manual and should_defer_cron("rollup_hour_heal", service_id):
        logger.info("⏸️  [rollup-heal] %s: deferring due to active user queries", service_id)
        return {"status": "deferred", "service_id": service_id, "summary": "Deferred due to active queries"}
    if (src := get_source_for_service(service_id)) is None:
        return {"status": "error", "service_id": service_id, "summary": "Source not found"}
    ok_disk, msg = has_sufficient_disk_space(_hour_bundled_root(src), "rollup-heal", service_id)
    if not ok_disk:
        record_skip_row(
            src, "rollup_hour_heal", "Skipped due to low disk space", status="warning", error_message=msg, run_id=run_id
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
        heal = backfill_missing_hour_bundles(
            service_id,
            src,
            lookback_days=lookback_days,
            max_missing_hours=1 if durable_mode and not coverage_ready else None,
        )
        if not durable_mode:
            try:
                from backend.core.rollups import backfill_security_proxies_bundles, backfill_wellknown_bots_rollup

                backfill_wellknown_bots_rollup(service_id, src, lookback_days=lookback_days)
                backfill_security_proxies_bundles(service_id, src)
            except Exception as sec_heal_err:
                logger.warning(
                    "[rollup-heal] %s: security rollup self-heal skipped/failed: %s", service_id, sec_heal_err
                )
        try:
            cfg = svcconfig.load_config(service_id) or {}
            if cfg.get("rum", {}).get("enabled") and not durable_mode:
                from contextlib import closing

                from backend.core.duckdb import get_connection, rum_source_for
                from backend.core.rollups.recompute import backfill_missing_rum_aggregates
                from backend.core.storage import execute_with_stale_view_retry
                from backend.core.storage._core import _ducklake_attach

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
def _run_rollup_compact_daily(service_id: str, manual: bool = False, run_id: int | None = None) -> dict:
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
    from backend.utils.telemetry import should_defer_cron

    if not manual and should_defer_cron("rollup_compact", service_id):
        logger.info("⏸️  [rollup-compact] %s: deferring due to active user queries", service_id)
        return {"status": "deferred", "service_id": service_id, "summary": "Deferred due to active queries"}
    if (src := get_source_for_service(service_id)) is None:
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
        run.record(status, summary=summary, error_message="; ".join(subsystem_errors) if subsystem_errors else None)
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
    from backend.core.duckdb import get_source_for_service
    from backend.core.log_fields import derived
    from backend.core.rollups.day_bundles import gc_stale_partial_hours, merge_partial_hour
    from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS
    from backend.repositories.dashboard import FIELDS
    from backend.utils.telemetry import should_defer_cron

    if should_defer_cron("partial_hour_merge", service_id) or (src := get_source_for_service(service_id)) is None:
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


_MAINTENANCE_ATTRS = frozenset(
    {
        "_run_metadata_cleanup",
        "_run_expire_snapshots",
        "_run_optimize",
        "schedule_cloud_push",
    }
)


class _CompactionModule(ModuleType):
    def __getattr__(self, name: str) -> Any:
        if name in _MAINTENANCE_ATTRS:
            maint = importlib.import_module("backend.cron.jobs.maintenance")
            return getattr(maint, name)
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        if name in _MAINTENANCE_ATTRS:
            maint = importlib.import_module("backend.cron.jobs.maintenance")
            setattr(maint, name, value)
            return
        super().__setattr__(name, value)

    def __delattr__(self, name: str) -> None:
        if name in _MAINTENANCE_ATTRS:
            maint = importlib.import_module("backend.cron.jobs.maintenance")
            delattr(maint, name)
            return
        super().__delattr__(name)


_sys.modules[__name__].__class__ = _CompactionModule
