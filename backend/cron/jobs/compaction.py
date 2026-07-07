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

from backend.cron.decorators import cron_task
from backend.cron.scheduler import (
    _display_label,
    _extract_log_text,
    _log_and_add_progress,
)

logger = logging.getLogger("backend.scheduler")


@cron_task("local_compact")
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
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.utils.active_requests import should_defer_cron

    # Active-request gate (perf #84): cache compaction holds the DuckDB
    # write lock for hundreds of ms — defer when API requests are in flight.
    if should_defer_cron("local_compact", service_id):
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "local_compact")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[96m[local-compact]\x1b[0m %s: skipping — %s", service_id, str(e))
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="local_compact")
    _display = _display_label(src, service_id)
    logger.info("▶️  \x1b[96m[local-compact]\x1b[0m %s: Local compaction started.", _display)
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="local_compact",
        event={"type": "status", "message": "Scanning local cache partitions..."},
    )

    start_time = time.time()
    try:
        result = _lc.compact_local_partitions(src)
        duration = time.time() - start_time
        errors = result.get("errors") or []
        merged = result.get("files_merged", 0)
        removed = result.get("files_removed", 0)
        partitions = result.get("partitions_compacted", 0)
        summary = (
            f"Compacted {partitions} partition(s): merged {merged} small file(s) into "
            f"{partitions} (removed {removed} originals)"
        )
        if errors:
            err_preview = "\n".join(errors[:3])
            if len(errors) > 3:
                err_preview += f"\n... ({len(errors) - 3} more)"
            status = "warning"
            summary += f" — {len(errors)} partition error(s)"
        else:
            err_preview = None
            status = "success"
        log_cron_run(
            src,
            "local_compact",
            duration,
            status,
            summary=summary,
            error_message=err_preview,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="local_compact",
            event={"type": "status", "message": summary},
        )
        logger.info("⏹️  \x1b[96m[local-compact]\x1b[0m %s: %s in %.2fs", _display, summary, duration)
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "local_compact",
            duration,
            "error",
            error_message=str(e),
            summary="local compaction failed",
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(run_id, service_id, job_name="local_compact", event={"type": "error", "message": str(e)})
        logger.exception("[scheduler] %s: local_compact failed: %s", service_id, e)
    finally:
        end_progress(run_id)


@cron_task("rollup_hour_heal")
def _run_rollup_hour_heal(service_id: str) -> None:
    """Hourly job: rebuild hour bundles for closed hours the per-sync
    recompute missed.

    ``recompute_touched_hours`` excludes the active hour, so a closed hour
    only gets its rollup when a LATER sync batch ingests rows stamped
    inside it. Bursty services (burst ends mid-hour, all rows delivered
    before the boundary) never retrigger — diagnosed 2026-07-06 as top-N
    cards silently missing every closed hour of the current day on the
    low-traffic service. Reuses the idempotent
    ``backfill_missing_hour_bundles`` self-heal with a 1-day lookback:
    one listdir + one GROUP-BY-hour view scan when nothing is missing,
    so the steady-state tick is cheap. The daily compaction job keeps
    its 30-day deep pass.

    LOCAL-only writes (rollup parquet under cache/) — no FOS traffic, so
    it is safe under the dev kill switch alongside local_compact /
    rollup_compact.
    """
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.rollups import backfill_missing_hour_bundles
    from backend.utils.active_requests import should_defer_cron

    # The heal's view scan + per-field COPY are CPU-bound; defer when API
    # requests are in flight (same politeness gate as local_compact).
    if should_defer_cron("rollup_hour_heal", service_id):
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "rollup_hour_heal")
    except RuntimeError as e:
        logger.info("⏭️  [rollup-heal] %s: skipping — %s", service_id, str(e))
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="rollup_hour_heal")
    _display = _display_label(src, service_id)

    start_time = time.time()
    try:
        heal = backfill_missing_hour_bundles(service_id, src, lookback_days=1)
        duration = time.time() - start_time
        summary = (
            f"Healed {heal.get('missing', 0)} missing hour(s): "
            f"{heal.get('rebuilt_fields', 0)} field rollup(s) rebuilt, "
            f"{heal.get('bundled', 0)} hour(s) bundled, "
            f"{heal.get('stamped_empty', 0)} empty hour(s) stamped"
        )
        log_cron_run(
            src,
            "rollup_hour_heal",
            duration,
            "success",
            summary=summary,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        if heal.get("missing", 0) or heal.get("stamped_empty", 0):
            logger.info("⏹️  [rollup-heal] %s: %s in %.2fs", _display, summary, duration)
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "rollup_hour_heal",
            duration,
            "error",
            error_message=str(e),
            summary="hour-bundle self-heal failed",
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(
            run_id, service_id, job_name="rollup_hour_heal", event={"type": "error", "message": str(e)}
        )
        logger.exception("[scheduler] %s: rollup_hour_heal failed: %s", service_id, e)
    finally:
        end_progress(run_id)


@cron_task("rollup_compact_daily")
def _run_rollup_compact_daily(service_id: str) -> None:
    """Daily job: consolidate closed-day per-hour rollup parquet into per-day files.

    Reduces file-open overhead on 7-day dashboard queries from ~1500 files
    to ~30. Reader automatically falls back to per-hour when per-day is
    missing, so this is purely additive.
    """
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.rollups import (
        backfill_day_bundles,
        backfill_missing_hour_bundles,
        backfill_missing_hour_ip_spread,
        compact_closed_days_to_daily,
        compact_network_rtt_closed_days_to_daily,
        compact_network_speed_closed_days_to_daily,
        compact_ngwaf_bots_closed_days_to_daily,
        compact_origin_dims_closed_days_to_daily,
        compact_origin_latency_ts_closed_days_to_daily,
        compact_origin_summary_closed_days_to_daily,
        compact_perf_latency_closed_days_to_daily,
        compact_security_dims_closed_days_to_daily,
        compact_verified_bots_ts_closed_days_to_daily,
    )

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "rollup_compact_daily")
    except RuntimeError as e:
        logger.info("⏭️  [rollup-compact] %s: skipping — %s", service_id, str(e))
        return

    _display = _display_label(src, service_id)
    logger.info("▶️  [rollup-compact] %s: Daily rollup compaction started.", _display)

    start_time = time.time()
    try:
        # Self-heal pass FIRST: any closed hour where the iceberg view
        # has rows but no hour_bundled file exists (ingest's
        # "touched_hours" report under-reported, the recompute fired
        # before the iceberg commit landed, etc.) gets rebuilt here.
        # Surfaced 2026-06-15 as a multi-percent POST undercount on
        # the dashboard method panel — 18 hours over 30 days were
        # silently missing. Best-effort: a failure leaves the
        # compaction pass to run anyway against whatever bundles do
        # exist.
        try:
            heal = backfill_missing_hour_bundles(service_id, src, lookback_days=30)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: missing-hour self-heal failed (compaction continues): %s",
                _display,
                e,
            )
            heal = {"missing": 0, "bundled": 0}

        # IP-spread self-heal: closed hours that have the count bundle
        # but no all_fields_ip.parquet (the common case after #3 ships
        # to a service whose count rollup was already backfilled).
        # Bounded by the same 30-day lookback as the count heal so a
        # one-time backfill can't run away on a long-lived service.
        try:
            ip_heal = backfill_missing_hour_ip_spread(service_id, src, lookback_days=30)
            if ip_heal.get("missing", 0) > 0:
                logger.info(
                    "🔁 [rollup-compact] %s: ip_spread self-heal rebuilt %d/%d hours",
                    _display,
                    ip_heal.get("rebuilt", 0),
                    ip_heal.get("missing", 0),
                )
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: ip_spread self-heal failed (compaction continues): %s",
                _display,
                e,
            )

        rebuilt = compact_closed_days_to_daily(service_id, src)
        # After per-field per-day files are fresh, bundle them across
        # fields so the dashboard reader opens 1 file per day instead
        # of ~40. backfill_day_bundles is idempotent (skips up-to-date
        # bundles via mtime) so running it on every compact tick is
        # cheap when no new per-field days landed. Best-effort —
        # bundle failure degrades to per-field reading, which still
        # works correctly.
        try:
            bundled = backfill_day_bundles(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: day-bundle backfill failed (per-field still serves): %s",
                _display,
                e,
            )
            bundled = 0
        # Origin-summary per-day compaction: takes the 24 per-hour
        # origin_summary.parquet files in hour_bundled and writes a
        # single per-day file in day_bundled/day=YYYY-MM-DD/. The
        # /api/origin/aggregates summary panel reader prefers day-files
        # when present and falls back to per-hour otherwise, so this is
        # purely additive — the file-open overhead drop is the win
        # (3.2 s → ~200 ms on a 30 d window's cold reads on prod cloud
        # disk, per the 2026-06-16 measurement). Idempotent (mtime-
        # gated); best-effort failure leaves the panel on the per-hour
        # path.
        try:
            origin_summary_compacted = compact_origin_summary_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: origin_summary day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            origin_summary_compacted = 0

        # network_rtt per-day compaction: collapses 24 per-hour
        # rtt_percentiles parquets into 1 per-day file so the
        # /api/network-health rtt_percentiles_query rollup reader opens
        # ~30 files on a 30 d window instead of 720. Same mtime-gated
        # idempotent pattern; failure leaves the panel on per-hour.
        try:
            network_rtt_compacted = compact_network_rtt_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: network_rtt day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            network_rtt_compacted = 0

        # network_speed per-day compaction: same shape as network_rtt
        # but the per-day row is a pure GROUP BY (asn, c_speed) SUM.
        try:
            network_speed_compacted = compact_network_speed_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: network_speed day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            network_speed_compacted = 0

        # verified_bots_ts per-day compaction: same shape as network_speed
        # but PRESERVES the minute (bucket_ts) dimension because the panel
        # is a time series (GROUP BY bucket_ts, bot_type).
        try:
            vbts_compacted = compact_verified_bots_ts_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: verified_bots_ts day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            vbts_compacted = 0

        # perf_latency per-day compaction: top_urls + top_asns latency
        # leaderboards (request-weighted percentile merge + re-cap top-K).
        try:
            perf_compacted = compact_perf_latency_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: perf_latency day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            perf_compacted = 0

        # origin_dims per-day compaction: pop / oip / edge latency dimensions
        # (request-weighted percentile merge; oip SUMs exact 5xx/total counts;
        # pop/oip re-cap top-K). Same mtime-gated idempotent pattern; failure
        # leaves those panels on per-hour.
        try:
            origin_dims_compacted = compact_origin_dims_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: origin_dims day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            origin_dims_compacted = 0

        # origin_latency_ts per-day compaction: minute-granular origin-latency
        # percentile time series. Same shape as verified_bots_ts — PRESERVES
        # the minute (bucket_ts) dimension because the panel is a time series.
        # Same mtime-gated idempotent pattern; failure leaves it on per-hour.
        try:
            origin_latency_ts_compacted = compact_origin_latency_ts_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: origin_latency_ts day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            origin_latency_ts_compacted = 0

        # security_dims per-day compaction: req_size / conn_reuse (bucket SUM +
        # MIN-of-MIN), topips (MAX-of-MAX re-cap), cov (one-row SUM). EXACT
        # merges — no _approx. Same mtime-gated idempotent pattern; failure
        # leaves those panels on per-hour.
        try:
            security_dims_compacted = compact_security_dims_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: security_dims day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            security_dims_compacted = 0

        # ngwaf_bots per-day compaction: (bot_name, category) SUM(count) —
        # EXACT merge, no cap. Same mtime-gated idempotent pattern; failure
        # leaves the panel on per-hour files.
        try:
            ngwaf_bots_compacted = compact_ngwaf_bots_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: ngwaf_bots day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            ngwaf_bots_compacted = 0
        duration = time.time() - start_time
        # Pass run_id so log_cron_run UPDATEs the 'running' row that
        # start_cron_run inserted (instead of orphaning it and inserting
        # a fresh terminal row). The same fix applies to the error
        # branch below — without run_id pass-through both branches
        # leave the original 'running' row stuck forever.
        heal_summary = (
            f"; healed {heal['bundled']}/{heal['missing']} missing hour bundle(s)" if heal.get("missing") else ""
        )
        os_summary = f"; compacted {origin_summary_compacted} origin_summary day(s)" if origin_summary_compacted else ""
        nr_summary = f"; compacted {network_rtt_compacted} network_rtt day(s)" if network_rtt_compacted else ""
        ns_summary = f"; compacted {network_speed_compacted} network_speed day(s)" if network_speed_compacted else ""
        vbts_summary = f"; compacted {vbts_compacted} verified_bots_ts day(s)" if vbts_compacted else ""
        perf_summary = f"; compacted {perf_compacted} perf_latency day(s)" if perf_compacted else ""
        od_summary = f"; compacted {origin_dims_compacted} origin_dims day(s)" if origin_dims_compacted else ""
        olts_summary = (
            f"; compacted {origin_latency_ts_compacted} origin_latency_ts day(s)" if origin_latency_ts_compacted else ""
        )
        sd_summary = f"; compacted {security_dims_compacted} security_dims day(s)" if security_dims_compacted else ""
        nb_summary = f"; compacted {ngwaf_bots_compacted} ngwaf_bots day(s)" if ngwaf_bots_compacted else ""
        log_cron_run(
            src,
            "rollup_compact_daily",
            duration,
            "success",
            summary=f"Rebuilt {rebuilt} (field, day) file(s); bundled {bundled} day(s){os_summary}{nr_summary}{ns_summary}{vbts_summary}{perf_summary}{od_summary}{olts_summary}{sd_summary}{nb_summary}{heal_summary}.",
            run_id=run_id,
        )
        logger.info(
            "⏹️  [rollup-compact] %s: Compacted %d (field, day), bundled %d day(s), healed %d/%d hour bundle(s) in %.1fs.",
            _display,
            rebuilt,
            bundled,
            heal.get("bundled", 0),
            heal.get("missing", 0),
            duration,
        )
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "rollup_compact_daily",
            duration,
            "error",
            error_message=str(e),
            run_id=run_id,
        )
        logger.exception("[rollup-compact] %s: Daily rollup compaction failed: %s", _display, e)
