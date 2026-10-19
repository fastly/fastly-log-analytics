"""Cron job: rollup_compact_{service_id} — daily consolidation of closed days."""

from __future__ import annotations

import logging
import os
import shutil

from backend.cron.decorators import cron_task
from backend.cron.lifecycle import cron_run

logger = logging.getLogger("backend.scheduler")


def _display_label(src: dict, service_id: str) -> str:
    return src.get("name") or service_id


def _call_with_lookback(fn, *args, lookback_days: int = 30):
    return fn(*args, lookback_days=lookback_days)


@cron_task("rollup_compact_daily", job_name="rollup_compact_daily")
def _run_rollup_compact_daily(
    service_id: str,
    manual: bool = False,
    run_id: int | None = None,
) -> dict:
    from backend.core.duckdb import get_source_for_service, log_cron_run
    from backend.core.rollups import (
        backfill_day_bundles,
        backfill_missing_hour_bundles,
        backfill_missing_hour_ip_spread,
        compact_closed_days_to_daily,
        compact_network_quality_closed_days_to_daily,
        compact_network_rtt_closed_days_to_daily,
        compact_network_speed_closed_days_to_daily,
        compact_ngwaf_bots_closed_days_to_daily,
        compact_origin_dims_closed_days_to_daily,
        compact_origin_latency_ts_closed_days_to_daily,
        compact_origin_summary_closed_days_to_daily,
        compact_overview_closed_days_to_daily,
        compact_perf_dims_closed_days_to_daily,
        compact_perf_latency_closed_days_to_daily,
        compact_pop_health_closed_days_to_daily,
        compact_security_dims_closed_days_to_daily,
        compact_verified_bots_ts_closed_days_to_daily,
        retire_compacted_hour_bundles,
    )
    from backend.utils.active_requests import should_defer_cron

    if not manual and should_defer_cron("rollup_compact", service_id):
        logger.info("⏸️  [rollup-compact] %s: deferring due to active user queries", service_id)
        return {
            "status": "deferred",
            "service_id": service_id,
            "summary": "Deferred due to active queries",
        }

    src = get_source_for_service(service_id)
    if src is None:
        return {"status": "error", "service_id": service_id, "summary": "Source not found"}

    # ENOSPC safety check (< 50MB free disk space)
    from backend.core.rollups._common import _day_bundled_root

    day_root = _day_bundled_root(src)
    check_dir = day_root if os.path.isdir(day_root) else "."
    try:
        free_bytes = shutil.disk_usage(check_dir).free
        if free_bytes < 50 * 1024 * 1024:
            msg = f"Disk space critically low (< 50MB free: {free_bytes / (1024 * 1024):.1f}MB)"
            logger.warning("⚠️  [rollup-compact] %s: %s", service_id, msg)
            log_cron_run(
                src,
                "rollup_compact_daily",
                0.0,
                "warning",
                summary="Skipped due to low disk space",
                error_message=msg,
                run_id=run_id,
            )
            return {"status": "warning", "service_id": service_id, "summary": msg}
    except Exception as e:
        logger.warning("[rollup-compact] %s: failed to check disk space: %s", service_id, e)

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
            return run.result
        logger.info("🏎️  [rollup-compact] %s: Daily rollup compaction started.", _display)

        subsystem_errors: list[str] = []
        # 1. Missing-hour self-heal
        try:
            heal = _call_with_lookback(backfill_missing_hour_bundles, service_id, src, lookback_days=30)
        except Exception as e:
            subsystem_errors.append(f"missing_hour_bundles: {e}")
            logger.warning("[rollup-compact] %s: missing-hour self-heal failed: %s", _display, e)
            heal = {"missing": 0, "bundled": 0}

        # 2. IP-spread self-heal
        try:
            ip_heal = _call_with_lookback(backfill_missing_hour_ip_spread, service_id, src, lookback_days=30)
        except Exception as e:
            subsystem_errors.append(f"ip_spread: {e}")
            logger.warning("[rollup-compact] %s: ip_spread self-heal failed: %s", _display, e)

        # 3. Main per-field daily compaction
        rebuilt = _call_with_lookback(compact_closed_days_to_daily, service_id, src, lookback_days=30)

        # 4. Day bundling
        try:
            bundled = _call_with_lookback(backfill_day_bundles, service_id, src, lookback_days=30)
        except Exception as e:
            subsystem_errors.append(f"day_bundles: {e}")
            logger.warning("[rollup-compact] %s: day-bundle backfill failed: %s", _display, e)
            bundled = 0

        # 5-17. Per-rollup daily compaction. Each subsystem is isolated so one failure is
        # recorded as a warning without stopping the rest; the results are not used.
        daily_compactors = (
            ("origin_summary", compact_origin_summary_closed_days_to_daily),
            ("network_rtt", compact_network_rtt_closed_days_to_daily),
            ("network_speed", compact_network_speed_closed_days_to_daily),
            ("network_quality", compact_network_quality_closed_days_to_daily),
            ("verified_bots_ts", compact_verified_bots_ts_closed_days_to_daily),
            ("perf_latency", compact_perf_latency_closed_days_to_daily),
            ("origin_dims", compact_origin_dims_closed_days_to_daily),
            ("origin_latency_ts", compact_origin_latency_ts_closed_days_to_daily),
            ("security_dims", compact_security_dims_closed_days_to_daily),
            ("perf_dims", compact_perf_dims_closed_days_to_daily),
            ("ngwaf_bots", compact_ngwaf_bots_closed_days_to_daily),
            ("overview", compact_overview_closed_days_to_daily),
            ("pop_health", compact_pop_health_closed_days_to_daily),
        )
        for name, compactor in daily_compactors:
            try:
                _call_with_lookback(compactor, service_id, src, lookback_days=30)
            except Exception as e:
                subsystem_errors.append(f"{name}: {e}")
                logger.warning("[rollup-compact] %s: %s failed: %s", _display, name, e)

        # Retirement pass: unlink constituent hourly bundles once day bundle verified
        retired_files, retired_bytes = 0, 0
        try:
            retired_files, retired_bytes = _call_with_lookback(
                retire_compacted_hour_bundles, service_id, src, lookback_days=30
            )
        except Exception as e:
            subsystem_errors.append(f"retirement: {e}")
            logger.warning("[rollup-compact] %s: retirement failed: %s", _display, e)

        # Retention and historical cleanup pass
        try:
            from backend.core.rollups.recompute import cleanup_old_rollups

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
