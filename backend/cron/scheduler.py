"""APScheduler lifecycle + cron-job registration.

This is the post-carve home for the ``Scheduler`` class, the
``get_scheduler`` factory, the global singleton, and the small helpers
(logger, color/icon tables, throttle dicts, disk/backlog probes) that
are shared across job modules. (The legacy flat ``backend/scheduler.py``
compat shim was retired 2026-07-06 — import directly from here or from
``backend.cron.jobs.*``.)
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any

# Anchor the logger to the historical ``backend.scheduler`` name so
# log filters in tests (and downstream parsers) keep working after
# the carve. Every job module also imports this logger.
logger = logging.getLogger("backend.scheduler")


# ── Helpers / shared state ────────────────────────────────────────────────────


from backend.cron.decorators import dev_mode_no_crons  # noqa: F401


def partial_hour_merge_enabled() -> bool:
    return os.environ.get("PARTIAL_HOUR_MERGE_ENABLED", "true").lower() in ("1", "true", "yes")


def partial_hour_merge_interval_sec() -> int:
    try:
        val = int(os.environ.get("PARTIAL_HOUR_MERGE_INTERVAL_SEC", "30"))
        return max(10, min(120, val))
    except (ValueError, TypeError):
        return 30


def dev_local_crons_enabled() -> bool:
    return os.environ.get("FLA_DEV_LOCAL_CRONS", "true").lower() in ("1", "true", "yes")


def _resolve_rum_sync_interval(rum_cfg: dict[str, Any], cfg: dict[str, Any], default_request_interval: int) -> int:
    """Resolve the RUM ingest sync/discovery interval in seconds.

    If an operator explicitly configured ``sync_interval_seconds`` in RUM settings,
    respect it for backwards-compatibility.
    Otherwise, derive the sync interval from the RUM log rotation period:
      - If rum_log_period >= 60: max(5, rum_log_period // 2)
      - If rum_log_period < 60: max(5, rum_log_period)
    If no RUM log period is configured, fall back to the request log sync interval.
    """
    if "sync_interval_seconds" in rum_cfg and rum_cfg["sync_interval_seconds"] is not None:
        try:
            return max(5, int(rum_cfg["sync_interval_seconds"]))
        except (ValueError, TypeError):
            pass

    rum_log_period = rum_cfg.get("log_period")
    if rum_log_period is None:
        rum_log_period = cfg.get("rum_log_period")

    if rum_log_period is not None:
        try:
            period_val = int(rum_log_period)
            if period_val >= 60:
                return max(5, period_val // 2)
            return max(5, period_val)
        except (ValueError, TypeError):
            pass

    return max(5, int(default_request_interval))


def _display_label(src: dict, service_id: str) -> str:
    """``"{display_name} ({service_id})"`` when the friendly name differs from
    the service id, otherwise just the service id. Delegates to
    ``get_service_display_label`` in ``backend.cron.jobs._common`` so the
    format lives in one place.
    """
    from backend.cron.jobs._common import get_service_display_label

    return get_service_display_label(service_id, src)


# Per-service throttle for the heavy post-ingest refresh work — specifically
# update_top_values (100k reservoir sample + 24 GROUP BYs that back the filter-
# picker autocomplete cache) and reconcile_fastly_stats (Fastly /stats/aggregate
# call with a 26h window that backfills the Usage Log billing reconciliation).
# At 1s log_period the sync cron fires every 5s; running both phases on every
# tick was the dominant ~16s floor in cron_runs.duration_s. Cheap status fields
# (ingested count, latest file, buffer size, iceberg row counts) still refresh
# every tick so the dashboard header stays current. Filter-picker autocomplete
# degrades to a live query when the cache is missing or a search string is
# typed (see get_field_values), and the Usage Log page reads at hourly grain
# so 60s reconcile lag is invisible.
_HEAVY_REFRESH_INTERVAL_SEC = 60.0
_last_heavy_refresh: dict[str, float] = {}
_last_heavy_refresh_lock = threading.Lock()


def _claim_heavy_refresh(service_id: str) -> bool:
    """Return True iff this caller should run the heavy refresh phases this tick.

    Single-shot claim: the first caller per service per window wins; concurrent
    callers (e.g. a manual sync overlapping a scheduled tick) see False. We
    stamp _last_heavy_refresh on claim so a thread that crashes mid-phase
    can't starve the next tick — the next 60s window simply opens normally.
    """
    now = time.time()
    with _last_heavy_refresh_lock:
        last = _last_heavy_refresh.get(service_id, 0.0)
        if (now - last) >= _HEAVY_REFRESH_INTERVAL_SEC:
            _last_heavy_refresh[service_id] = now
            return True
    return False


def _elapsed_since(start: float) -> str:
    """Format seconds elapsed since *start* (time.time()) as a compact string."""
    s = time.time() - start
    return f"{int(s // 60)}m{int(s % 60):02d}s" if s >= 60 else f"{s:.1f}s"


def _service_has_alerts(service_id: str) -> bool:
    """Return True if the service has at least one alert configured.

    Used to gate the alerts evaluation cron — pointless to fire every tick
    just to log "No alerts configured". On error (e.g. corrupt SQLite),
    defaults to True so we don't silently disable the cron.
    """
    from backend.core import metadata as metadata_db

    try:
        return metadata_db.count_alerts(service_id) > 0
    except Exception:
        return True


# Ensure project root is importable (same as main.py)
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _extract_log_text(run_id: int | None) -> str:
    """Return a plain-text log summary for a cron run from the progress store.

    run_id can be None when start_cron_run failed to register the run; in
    that case the progress store has nothing for it and we return "".
    """
    from backend.cron_progress import get_progress

    if run_id is None:
        return ""
    evs = get_progress(run_id)
    if not evs:
        return ""
    return "\n".join(
        f"[{e.get('type', 'info').upper()}] {e['message']}"
        for e in evs
        if "message" in e and e.get("type") in ("error", "status", "done", "warning")
    )


# ── Telemetry colors / icons ──────────────────────────────────────────────────


JOB_COLORS = {
    "sync": "\x1b[94m",  # Bright Blue
    "commit": "\x1b[95m",  # Bright Magenta
    "metadata_sync": "\x1b[96m",  # Bright Cyan
    "metadata_cleanup": "\x1b[35m",  # Magenta
    "alerts": "\x1b[93m",  # Bright Yellow
    "optimize": "\x1b[92m",  # Bright Green
    "expire": "\x1b[90m",  # Gray
    "ngwaf_sync": "\x1b[36m",  # Cyan
    "usage_log": "\x1b[32m",  # Green
}
RESET_COLOR = "\x1b[0m"

TYPE_ICONS = {
    "error": "❌ ",  # Added trailing space to prevent terminal width collision
    "warning": "⚠️ ",
    "done": "✅ ",
    "status": "ℹ️ ",
    "progress": "⏳ ",
    "sync": "⬇️  ",
    "commit": "💾 ",
    "optimize": "🔨 ",
    "expire": "🗑️ ",
    "metadata_sync": "🔄 ",
    "alerts": "🔔 ",
    "ngwaf_sync": "👾 ",
    "iceberg": "🧊 ",
    "sync_data": "⬇️  ",
    "usage_log": "📊 ",
}


def _log_and_add_progress(
    run_id: int | None, service_id: str, event: dict, job_name: str = "scheduler", service_name: str | None = None
) -> None:
    """Log a cron event and (best-effort) add it to the progress store.

    run_id can be None when start_cron_run failed to register the run; in
    that case add_progress no-ops and only the log message is emitted.
    """
    from backend.cron.jobs._common import get_service_display_label
    from backend.cron_progress import add_progress

    add_progress(run_id, event)
    msg = event.get("message")
    if msg:
        display = get_service_display_label(service_id)

        t = event.get("type", "info")
        # type="status" events are per-phase timing messages (e.g.
        # "1.8s usage_log phase: 43ms"). They power the cron-progress
        # stream that drives the in-app "Recent Cron Activity" view —
        # which is the right place for them. Mirroring every one to
        # stdout floods docker logs with no actionable signal, so the
        # logger emit is skipped for status. info/warning/error still log.
        if t == "status":
            return

        c = JOB_COLORS.get(job_name, "")
        c_end = RESET_COLOR if c else ""

        # If type is just 'info', see if the job_name has a specific icon
        if t == "info" and job_name in TYPE_ICONS:
            icon = TYPE_ICONS[job_name]
        else:
            icon = TYPE_ICONS.get(t, "ℹ️ ")

        display_job_name = job_name
        if display_job_name == "sync":
            display_job_name = "log_discovery"

        prefix = f"{icon}{c}[{display_job_name}]{c_end}"
        # Read the logger from module globals at call time so tests that
        # ``patch("backend.cron.scheduler.logger")`` intercept these calls.
        log = logger
        if t == "error":
            log.error("%s %s: %s", prefix, display, msg)
        elif t == "warning":
            log.warning("%s %s: %s", prefix, display, msg)
        else:
            log.info("%s %s: %s", prefix, display, msg)


# ── Disk + buffer-backlog probes ──────────────────────────────────────────────


# Hard threshold: below this, ingest will refuse to start. A typical
# .gz raw log batch can land 50-200 MB on disk before commit drains it,
# and the iceberg manifest cache adds more. 500 MB is conservative
# enough to leave room for a single in-flight tick to finish safely.
_DISK_FREE_HARD_FLOOR_BYTES = 500 * 1024 * 1024
# Same idea as a percentage, for the (rare) case of a very small disk
# where 500 MB is most of free. Whichever check trips first wins.
_DISK_FREE_HARD_FLOOR_PCT = 0.005  # 0.5 %


def _check_disk_space(cache_dir: str, service_id: str, job_name: str) -> tuple[bool, str]:
    """Probe free space at the cache root before any cloud reads/writes.

    Returns (ok, message). ok=False means abort the job — caller MUST
    log_cron_run(status="error") and return.

    Why: when the cache disk fills, ingest still downloads files (cost!)
    then fails at pq.write — wasting FOS egress. Pre-checking at the
    top of the cron is a cheap circuit-breaker that turns "silent
    cascade of partial writes" into "single explicit error in cron_runs."
    """
    import shutil

    try:
        usage = shutil.disk_usage(cache_dir if os.path.isdir(cache_dir) else ".")
    except OSError as e:
        # Can't even stat the dir → bail with a clear message rather than crashing
        logger.warning("[scheduler] %s: disk-space probe failed for %s: %s", service_id, cache_dir, e)
        return True, ""  # don't block on probe failure — let the job try and fail naturally
    free_pct = usage.free / usage.total if usage.total else 1.0
    if usage.free < _DISK_FREE_HARD_FLOOR_BYTES or free_pct < _DISK_FREE_HARD_FLOOR_PCT:
        free_mb = usage.free // (1024 * 1024)
        total_gb = usage.total / (1024 * 1024 * 1024)
        msg = f"disk almost full: {free_mb} MB free ({free_pct * 100:.1f}% of {total_gb:.1f} GiB)"
        logger.error("💾 \x1b[31m[disk]\x1b[0m %s [%s]: refusing to start — %s", service_id, job_name, msg)
        return False, msg
    return True, ""


# Backlog thresholds. file_count is a static line because any single
# commit cycle that's healthy WILL drain it; >200 leftover files after
# commit means files arrived faster than commit could append them
# OR the commit is failing silently.
_BACKLOG_FILE_COUNT_WARN = 200
# oldest_age scales with the cron cadence: 3x interval = "the last three
# commit cycles haven't touched this file." That's the actionable signal.
_BACKLOG_AGE_MULTIPLIER = 3
# disk pressure proxy. 1 GiB of un-committed parquet means the buffer is
# carrying a non-trivial fraction of free disk on a typical 20-40 GiB cache.
_BACKLOG_BYTES_WARN = 1 * 1024 * 1024 * 1024


def _check_buffer_backlog(src: dict, service_id: str, commit_interval_mins: int) -> str:
    """Inspect the post-commit buffer state and return a suffix string for
    the cron summary line if the backlog crosses any health threshold.

    Returns "" when healthy. Never raises — backlog probing must not fail
    the commit, only annotate it.
    """
    try:
        from backend.core import iceberg as db_iceberg

        stats = db_iceberg.buffer_backlog_stats(src)
    except Exception as e:
        logger.warning("[scheduler] %s: buffer backlog probe failed: %s", service_id, e)
        return ""
    file_count = int(stats.get("file_count", 0) or 0)
    total_bytes = int(stats.get("total_bytes", 0) or 0)
    oldest_age_s = int(stats.get("oldest_age_seconds", 0) or 0)
    if file_count == 0:
        return ""
    max_oldest_age_s = max(60, commit_interval_mins * 60 * _BACKLOG_AGE_MULTIPLIER)
    problems: list[str] = []
    if file_count > _BACKLOG_FILE_COUNT_WARN:
        problems.append(f"{file_count} files")
    if oldest_age_s > max_oldest_age_s:
        problems.append(f"oldest {oldest_age_s // 60}m old")
    if total_bytes > _BACKLOG_BYTES_WARN:
        problems.append(f"{total_bytes // (1024 * 1024)}MB on disk")
    if not problems:
        return ""
    msg = "buffer backlog: " + ", ".join(problems)
    logger.warning(
        "🪣 \x1b[33m[backlog]\x1b[0m %s: %s — commits may be failing silently or ingest is outrunning commit",
        service_id,
        msg,
    )
    return f" ⚠ {msg}"


# ── Scheduler class + global singleton ────────────────────────────────────────


class Scheduler:
    """Thin wrapper around APScheduler's BackgroundScheduler."""

    def __init__(self) -> None:
        from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor
        from apscheduler.schedulers.background import BackgroundScheduler

        executors = {
            "default": APSThreadPoolExecutor(10),
            "streaming": APSThreadPoolExecutor(10),
            "maintenance": APSThreadPoolExecutor(3),
        }
        self._sched = BackgroundScheduler(timezone=UTC, executors=executors)
        # Track per-service job IDs so we can replace them when settings change.
        self._job_ids: dict[str, str] = {}  # job_id -> job_id

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the scheduler and register jobs for all configured services."""
        if dev_mode_no_crons():
            logger.warning(
                "🚫 [scheduler] FLA_DEV_NO_CRONS=1 — skipping all FOS-writing / ingest / outbound crons "
                "(sync/full_sweep/gap_heal/commit/optimize/expire/ngwaf_sync/metadata_cleanup/"
                "bot_data_refresh/rdns/alerts). Registering ONLY local-safe jobs (local_compact, "
                "rollup_compact, insights_prewarmer) that touch the local cache only. "
                "HTTP API calls (including /api/admin/rebuild-local-view) remain functional."
            )
            self._register_dev_local_safe_jobs()
            if self._job_ids:
                self._sched.start()
            logger.info(
                "🟢 [scheduler] Started in dev-local-safe mode (pid: %d). %d local-only job(s) registered.",
                os.getpid(),
                len(self._job_ids),
            )
            return

        from backend.cron.jobs.metadata import _run_metadata_sync

        self._sync_jobs()
        self._sched.start()
        logger.info("🟢 [scheduler] Started (pid: %d). %d job(s) registered.", os.getpid(), len(self._job_ids))

        # Initial metadata sync for analyst (read_only) services only.
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.core.duckdb_pool import (
            _pool_warm_at_boot_count,
            _pool_warm_at_boot_enabled,
            warm_pool_at_startup,
        )

        warm_enabled = _pool_warm_at_boot_enabled()
        warm_count = _pool_warm_at_boot_count() if warm_enabled else 0

        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue

            prov = cfg.get("provisioning", {})
            sync_cfg = prov.get("cron_sync", {})

            # ONLY trigger initial sync if enabled and it's a read-only analyst service
            if cfg.get("access_level") == "read_only" and sync_cfg.get("enabled", True):
                try:
                    # Run in background so we don't block the lifespan startup
                    self._add_job(
                        _run_metadata_sync, args=[service_id], id=f"initial_sync_{service_id}", replace_existing=True
                    )
                except Exception:
                    pass

            # Optional: pre-acquire pool connections so the first request
            # per service finds warm slots. Gated by DUCKDB_POOL_WARM_AT_BOOT
            # (default off) so a slow cold-build can't show up as an
            # operator-visible startup delay until it's explicitly opted
            # into. See _pool_warm_at_boot_enabled for the rationale.
            if warm_enabled:
                src = get_source_for_service(service_id)
                if src and is_configured(src):
                    try:
                        built = warm_pool_at_startup(service_id, src, count=warm_count)
                        logger.info(
                            "🔥 [pool] %s: warm_pool_at_startup built %d/%d idle conns",
                            service_id,
                            built,
                            warm_count,
                        )
                    except Exception as e:
                        logger.warning(
                            "[pool] %s: warm_pool_at_startup raised (continuing startup): %s",
                            service_id,
                            e,
                        )

    def shutdown(self, *, wait: bool = False) -> None:
        """Stop the scheduler gracefully.

        ``wait`` mirrors APScheduler's ``BackgroundScheduler.shutdown``
        kwarg and is forwarded as-is. The lifespan-shutdown bounded-wait
        pattern in ``backend.main._bounded_scheduler_shutdown`` passes
        ``wait=True`` so any in-flight cron job (sync ticks especially —
        up to 4 min wall-clock) gets a chance to land before the
        executor's worker threads die. Default ``wait=False`` for
        fire-and-forget cleanup at call sites that don't run their own
        bounded wait.
        """
        try:
            self._sched.shutdown(wait=wait)
        except Exception:
            pass
        logger.info("[scheduler] Stopped.")

    # ── Job management ────────────────────────────────────────────────────────

    def _register_alerts_evaluation_job(
        self, service_id: str, seconds: int, seen_ids: set[str], enabled: bool = True
    ) -> None:
        """Register (or reschedule) the per-service alerts-evaluation cron
        job. Gated on having at least one alert configured — otherwise the
        cron just fires a "skipped" log every tick. Shared between the
        analyst (read-only) and admin paths in :func:`_sync_jobs`; the only
        per-path difference is the tick interval, so we take it as ``seconds``.
        """
        if not enabled:
            return
        from backend.cron.jobs.metadata import _run_service_alerts_evaluation

        if not _service_has_alerts(service_id):
            return
        alert_job_id = f"alerts_evaluation_{service_id}"
        seen_ids.add(alert_job_id)
        if self._reschedule_or_add_job(
            _run_service_alerts_evaluation,
            "interval",
            seconds=seconds,
            id=alert_job_id,
            args=[service_id],
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        ):
            logger.info(
                "🔔 [scheduler] Registered alerts evaluation job %s (every %ds).",
                alert_job_id,
                seconds,
            )

    def _register_insights_prewarmer_job(self, service_id: str, seen_ids: set[str], prov: dict) -> None:
        """Register (or reschedule) the per-service insights cache prewarmer.
        Runs for both admin and analyst services so users land on a prewarmed cache.
        """
        from backend.cron.jobs.insights_prewarmer import _run_insights_prewarmer

        cron_ip = prov.get("cron_insights_prewarmer", {})
        if not cron_ip.get("enabled", True):
            return

        interval_seconds = int(cron_ip.get("interval_seconds", 240))
        ip_job_id = f"insights_prewarmer_{service_id}"
        seen_ids.add(ip_job_id)
        if self._reschedule_or_add_job(
            _run_insights_prewarmer,
            "interval",
            seconds=interval_seconds,
            jitter=15,
            reschedule_kwargs={"seconds": interval_seconds, "jitter": 15},
            args=[service_id],
            id=ip_job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=360,
        ):
            logger.info(
                "🔥 [scheduler] Registered insights_prewarmer job %s (every %ds).",
                ip_job_id,
                interval_seconds,
            )

    def _register_recycle_job(self, seen_ids: set[str] | None = None) -> None:
        """Register the process-global DuckDB instance recycle job.

        OFF unless ``DUCKDB_RECYCLE_INTERVAL_MIN > 0``. Local-safe (it only
        closes/reopens local DuckDB connections to free the leaking
        ``enable_object_cache`` parquet metadata — no FOS writes, nothing
        outbound), so it is registered from BOTH ``_sync_jobs`` and the
        dev-local-safe path. ``seen_ids`` (when given) is updated so the
        ``_sync_jobs`` stale-sweep doesn't immediately remove it.
        """
        from backend.core.duckdb_recycle import recycle_interval_min
        from backend.cron.jobs.metadata import run_duckdb_recycle

        recycle_interval = recycle_interval_min()
        if recycle_interval <= 0:
            return
        recycle_id = "duckdb_recycle"
        if seen_ids is not None:
            seen_ids.add(recycle_id)
        if recycle_id not in self._job_ids:
            self._add_job(
                run_duckdb_recycle,
                "interval",
                minutes=recycle_interval,
                id=recycle_id,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
                executor="maintenance",
            )
            self._job_ids[recycle_id] = recycle_id
            logger.info(
                "♻️  \x1b[35m[recycle]\x1b[0m Registered DuckDB instance recycle job (every %gm, executor=maintenance).",
                recycle_interval,
            )

    def _register_metric_snapshot_job(self, seen_ids: set[str] | None = None) -> None:
        """Register the minute-cadence operational-vital snapshot job.

        Local-safe (samples local host/process/db vitals, zero FOS writes,
        zero outbound calls), so it is registered from BOTH _sync_jobs and
        _register_dev_local_safe_jobs.
        """
        from backend.cron.jobs.metric_snapshot import _run_metric_snapshot

        snapshot_id = "metric_snapshot"
        if seen_ids is not None:
            seen_ids.add(snapshot_id)
        if snapshot_id not in self._job_ids:
            self._add_job(
                _run_metric_snapshot,
                "interval",
                seconds=60,
                jitter=5,
                id=snapshot_id,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=60,
            )
            self._job_ids[snapshot_id] = snapshot_id
            logger.info("📈 \x1b[34m[metric_snapshot]\x1b[0m Registered metric snapshot job (every 60s).")

    def _sync_local_compaction_jobs(
        self,
        service_id: str,
        cfg: dict,
        seen_ids: set[str] | None = None,
        *,
        dev_local: bool = False,
    ) -> None:
        """Register local-only compaction and rollup jobs for a service."""
        from backend.cron.jobs.compaction import (
            _run_local_compact,
            _run_partial_hour_merge,
            _run_rollup_compact_daily,
            _run_rollup_hour_heal,
        )

        prov = cfg.get("provisioning", {})
        compact_cfg = prov.get("cron_compact", {})
        tag = "(dev-local) " if dev_local else ""

        lc_job_id = f"local_compact_{service_id}"
        if seen_ids is not None:
            seen_ids.add(lc_job_id)
        if lc_job_id not in self._job_ids:
            lc_interval_mins = max(1, int(os.environ.get("LOCAL_COMPACT_INTERVAL_MIN", "2")))
            self._add_job(
                _run_local_compact,
                "interval",
                minutes=lc_interval_mins,
                jitter=10,
                args=[service_id],
                id=lc_job_id,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=60,
            )
            self._job_ids[lc_job_id] = lc_job_id
            logger.info(
                "⚙️  [scheduler] %sRegistered %s%s (every %d min, local-only).",
                tag,
                "" if dev_local else "local_compact job ",
                lc_job_id,
                lc_interval_mins,
            )

        ph_job_id = f"partial_hour_merge_{service_id}"
        if partial_hour_merge_enabled():
            if seen_ids is not None:
                seen_ids.add(ph_job_id)
            if ph_job_id not in self._job_ids:
                ph_interval_sec = partial_hour_merge_interval_sec()
                self._add_job(
                    _run_partial_hour_merge,
                    "interval",
                    seconds=ph_interval_sec,
                    jitter=5,
                    args=[service_id],
                    id=ph_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                )
                self._job_ids[ph_job_id] = ph_job_id
                logger.info(
                    "⚡ [scheduler] %sRegistered %s%s (every %ds, local-only).",
                    tag,
                    "" if dev_local else "partial_hour_merge job ",
                    ph_job_id,
                    ph_interval_sec,
                )

        if compact_cfg.get("enabled", True) and prov.get("access_level") != "read_only":
            rc_job_id = f"rollup_compact_{service_id}"
            if seen_ids is not None:
                seen_ids.add(rc_job_id)
            if rc_job_id not in self._job_ids:
                self._add_job(
                    _run_rollup_compact_daily,
                    "cron",
                    hour=2,
                    minute=0,
                    args=[service_id],
                    id=rc_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=3600,
                )
                self._job_ids[rc_job_id] = rc_job_id
                logger.info(
                    "📦 [scheduler] %sRegistered %s%s (daily 02:00 UTC).",
                    tag,
                    "" if dev_local else "rollup compaction job ",
                    rc_job_id,
                )

            rh_job_id = f"rollup_heal_{service_id}"
            if seen_ids is not None:
                seen_ids.add(rh_job_id)
            if rh_job_id not in self._job_ids:
                self._add_job(
                    _run_rollup_hour_heal,
                    "cron",
                    minute=5,
                    next_run_time=datetime.now(UTC) + timedelta(seconds=5),
                    args=[service_id],
                    id=rh_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=900,
                )
                self._job_ids[rh_job_id] = rh_job_id
                logger.info(
                    "🩹 [scheduler] %sRegistered %s%s (hourly at :05).",
                    tag,
                    "" if dev_local else "rollup hour-heal job ",
                    rh_job_id,
                )

    def _register_dev_local_safe_jobs(self) -> None:
        """Register ONLY the local-only compaction crons (plus the global
        metric-snapshot & recycle jobs if enabled by their own env vars)
        when ``FLA_DEV_NO_CRONS=1`` and ``FLA_DEV_LOCAL_CRONS=1`` (default).

        Mirrors the trigger config of the same jobs in :meth:`_sync_jobs` —
        keep them in sync. Runs once at startup;
        :meth:`reload` stays a no-op under the kill switch so a dev config
        save can't sneak the gated jobs back in.
        """
        if not dev_local_crons_enabled():
            logger.warning("🚫 [scheduler] FLA_DEV_LOCAL_CRONS=0 — no dev-local-safe jobs registered.")
            return

        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

        seen_ids: set[str] = set()
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id", "")
            if not service_id:
                continue
            src = get_source_for_service(service_id)
            if not src or not is_configured(src):
                continue
            if get_high_scale_service_registry().resolve(service_id) is not None:
                logger.info(
                    "⏭️  [scheduler] (dev-local) Skipping legacy local jobs for high-scale service %s.", service_id
                )
                continue

            prov = cfg.get("provisioning", {})
            self._sync_local_compaction_jobs(service_id, cfg, dev_local=True)
            self._register_insights_prewarmer_job(service_id, seen_ids, prov)

        self._register_metric_snapshot_job()
        self._register_recycle_job()

    def _sync_jobs(self) -> None:
        """Read all service configs and add/update scheduled jobs."""
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.cron.jobs.commit import _run_commit
        from backend.cron.jobs.compaction import _run_expire_snapshots, _run_optimize
        from backend.cron.jobs.metadata import (
            _run_bot_data_refresh,
            _run_metadata_cleanup,
            _run_metadata_sync,
            _run_ngwaf_bot_sync,
            _run_rdns_enrichment,
            _run_share_audit_purge,
        )
        from backend.cron.jobs.rum_commit import _run_rum_commit
        from backend.cron.jobs.rum_sync import _run_rum_sync
        from backend.cron.jobs.sync import _run_full_sweep, _run_gap_heal
        from backend.cron.jobs.sync import _run_log_discovery_cron as _run_service_cron

        configs = svcconfig.list_configs()
        seen_ids: set[str] = set()

        for cfg in configs:
            service_id = cfg.get("service_id", "")
            if not service_id:
                continue

            src = get_source_for_service(service_id)
            if not src or not is_configured(src):
                logger.warning("[scheduler] %s: service not fully configured, skipping jobs.", service_id)
                continue

            prov = cfg.get("provisioning", {})
            sync_cfg = prov.get("cron_sync", {})
            if not sync_cfg.get("enabled", True):
                continue

            log_period = int(cfg.get("log_period", 60))
            # Respect an explicitly configured interval; fall back to log_period derivation.
            # interval_mins (set by UI and analyst join flow) takes priority over interval_seconds
            # (written by admin provisioning scripts) so that UI changes are never silently ignored.
            if sync_cfg.get("interval_mins"):
                interval_seconds = max(5, int(sync_cfg["interval_mins"]) * 60)
            elif sync_cfg.get("interval_seconds"):
                interval_seconds = max(5, int(sync_cfg["interval_seconds"]))
            else:
                interval_seconds = max(5, log_period // 2 if log_period >= 60 else log_period)

            commit_interval_mins = max(1, int(sync_cfg.get("commit_interval_mins", 5)))
            is_readonly = cfg.get("access_level") == "read_only"

            # ── Metadata/Data Sync job (Pull-to-Local caching for Analysts) ──
            # Admins (read-write) don't need a separate cron for this; they trigger
            # it on-demand immediately after a successful 'commit' to stay in sync.
            sync_metadata_id = f"sync_metadata_{service_id}"
            if is_readonly:
                meta_cfg = prov.get("cron_metadata_sync", {})
                if meta_cfg.get("enabled", True):
                    meta_interval = int(meta_cfg.get("interval_seconds") or interval_seconds)
                    seen_ids.add(sync_metadata_id)

                    # Start immediately so the dashboard isn't slow/empty
                    if self._reschedule_or_add_job(
                        _run_metadata_sync,
                        "interval",
                        seconds=meta_interval,
                        id=sync_metadata_id,
                        replace_existing=True,
                        start_date=None,
                        args=[service_id],
                        coalesce=True,
                        misfire_grace_time=60,
                    ):
                        logger.info(
                            "[scheduler] Registered metadata sync job %s (every %ds).", sync_metadata_id, meta_interval
                        )

                # ── Alerts evaluation job for analysts ────────────────────────
                # Analysts evaluate alerts against their locally-cached data,
                # so they need this job even though they skip ingest/commit.
                # When the user adds an alert, the alerts router calls
                # scheduler.reload() to register the job; deleting the last
                # alert lets the cleanup loop unregister it on the next sync.
                alerts_cfg = prov.get("cron_alerts", {})
                alerts_enabled = alerts_cfg.get("enabled", True)
                alerts_seconds = int(alerts_cfg.get("interval_seconds") or interval_seconds)
                self._register_alerts_evaluation_job(service_id, alerts_seconds, seen_ids, enabled=alerts_enabled)

                # ── Insights prewarmer job for analysts ───────────────────────
                self._register_insights_prewarmer_job(service_id, seen_ids, prov)

                # Analysts don't ingest or commit — skip the rest.
                continue
            else:
                # If an admin previously had a metadata sync job, ensure we don't track it
                # It will be removed in the cleanup loop below
                pass

            # ── Sync job (ingest raw files from FOS → local buffer) ───────────
            job_id = f"log_discovery_{service_id}"
            seen_ids.add(job_id)

            if job_id in self._job_ids:
                try:
                    job = self._sched.get_job(job_id)
                    if job:
                        job.reschedule("interval", seconds=interval_seconds)
                        logger.info(
                            "[scheduler] Rescheduled log_discovery job %s to every %ds.", job_id, interval_seconds
                        )
                except Exception as e:
                    logger.error("[scheduler] Failed to reschedule sync job %s: %s", job_id, e)
            else:
                # Start immediately so the dashboard isn't slow/empty
                self._add_job(
                    _run_service_cron,
                    "interval",
                    seconds=interval_seconds,
                    jitter=2 if interval_seconds >= 5 else 1,
                    start_date=None,
                    args=[service_id],
                    id=job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                )
                self._job_ids[job_id] = job_id
                logger.info("🔄 [scheduler] Registered log_discovery job %s (every %ds).", job_id, interval_seconds)

            # ── Commit job (flush local buffer → Iceberg snapshot in FOS) ─────
            commit_job_id = f"commit_{service_id}"
            seen_ids.add(commit_job_id)

            if self._reschedule_or_add_job(
                _run_commit,
                "interval",
                minutes=commit_interval_mins,
                jitter=30,
                args=[service_id],
                id=commit_job_id,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=commit_interval_mins * 60,
            ):
                logger.info(
                    "📦 [scheduler] Registered commit job %s (every %dm).",
                    commit_job_id,
                    commit_interval_mins,
                )

            # ── Ledger sweep (celery-mode crash net) ───────────────────────────
            # Reclaims stale ingest_ledger claims, re-dispatches stuck rows, and
            # diffs a lookback FOS LIST. Its own schedule entry (every 15 min):
            # the previous `now.minute % 15 == 0` gate inside the discovery tick
            if svcconfig.is_high_scale_mode(src):
                from backend.cron.jobs.rum_ledger import _run_ledger_sweep

                sweep_cfg = prov.get("cron_ledger_sweep", {})
                if sweep_cfg.get("enabled", True):
                    sweep_interval_mins = max(1, int(sweep_cfg.get("interval_minutes", 15)))
                    sweep_job_id = f"ledger_sweep_{service_id}"
                    seen_ids.add(sweep_job_id)
                    if self._reschedule_or_add_job(
                        _run_ledger_sweep,
                        "interval",
                        minutes=sweep_interval_mins,
                        args=[service_id],
                        id=sweep_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=300,
                    ):
                        logger.info(
                            "🧹 [scheduler] Registered ledger_sweep job %s (every %dm).",
                            sweep_job_id,
                            sweep_interval_mins,
                        )

            # ── RUM ingest jobs (ingest RUM beacons from FOS) ──────────────────
            # Only register if RUM is enabled for this service. Celery mode
            # gets the ledger-based, per-file-fanout jobs (mirroring
            # log_discovery_/ledger_sweep_ above); non-celery mode keeps the
            # original single-job-per-service rum_sync_{id}/rum_commit_{id}
            # pair completely unchanged. The two pipelines must never both
            # be registered for the same service — they'd double-ingest
            # into the same DuckLake client_vitals/client_errors tables via
            # independent dedup registries that don't know about each other.
            rum_cfg = cfg.get("rum")
            rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
            rum_enabled = svcconfig.is_rum_enabled(cfg)
            if rum_enabled and svcconfig.is_high_scale_mode(src):
                from backend.cron.jobs.rum_ledger import _run_rum_discovery_cron, _run_rum_ledger_sweep

                rum_disc_interval_secs = _resolve_rum_sync_interval(rum_cfg, cfg, interval_seconds)
                rum_disc_job_id = f"rum_discovery_{service_id}"
                seen_ids.add(rum_disc_job_id)
                if self._reschedule_or_add_job(
                    _run_rum_discovery_cron,
                    "interval",
                    seconds=rum_disc_interval_secs,
                    args=[service_id],
                    id=rum_disc_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                ):
                    logger.info(
                        "[scheduler] Registered RUM discovery job %s (every %ds).",
                        rum_disc_job_id,
                        rum_disc_interval_secs,
                    )

                rum_sweep_cfg = prov.get("cron_ledger_rum_sweep") or prov.get("cron_rum_ledger_sweep") or {}
                if rum_sweep_cfg.get("enabled", True):
                    rum_sweep_interval_mins = max(1, int(rum_sweep_cfg.get("interval_minutes", 15)))
                    rum_sweep_job_id = f"ledger_rum_sweep_{service_id}"
                    seen_ids.add(rum_sweep_job_id)
                    if self._reschedule_or_add_job(
                        _run_rum_ledger_sweep,
                        "interval",
                        minutes=rum_sweep_interval_mins,
                        args=[service_id],
                        id=rum_sweep_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=300,
                    ):
                        logger.info(
                            "🧹 [scheduler] Registered ledger_rum_sweep job %s (every %dm).",
                            rum_sweep_job_id,
                            rum_sweep_interval_mins,
                        )
            elif rum_enabled:
                rum_sync_interval_secs = _resolve_rum_sync_interval(rum_cfg, cfg, interval_seconds)
                rum_sync_job_id = f"rum_sync_{service_id}"
                seen_ids.add(rum_sync_job_id)

                if self._reschedule_or_add_job(
                    _run_rum_sync,
                    "interval",
                    seconds=rum_sync_interval_secs,
                    args=[service_id],
                    id=rum_sync_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                ):
                    logger.info(
                        "[scheduler] Registered RUM sync job %s (every %ds).", rum_sync_job_id, rum_sync_interval_secs
                    )

                # ── RUM commit job (compact RUM tables) ──────────────────────
                rum_commit_interval_mins = max(1, int(rum_cfg.get("commit_interval_mins", 1)))
                rum_commit_job_id = f"rum_commit_{service_id}"
                seen_ids.add(rum_commit_job_id)

                if self._reschedule_or_add_job(
                    _run_rum_commit,
                    "interval",
                    minutes=rum_commit_interval_mins,
                    args=[service_id],
                    id=rum_commit_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                ):
                    logger.info(
                        "[scheduler] Registered RUM commit job %s (every %dm).",
                        rum_commit_job_id,
                        rum_commit_interval_mins,
                    )

            # ── Alerts evaluation job (Per Service) ───────────────────────────
            # See note above (analyst branch) on the no-alerts gate.
            alerts_cfg = prov.get("cron_alerts", {})
            alerts_enabled = alerts_cfg.get("enabled", True)
            alerts_seconds = int(alerts_cfg.get("interval_seconds") or log_period)
            self._register_alerts_evaluation_job(service_id, alerts_seconds, seen_ids, enabled=alerts_enabled)

            # ── Full-LIST sweep (catches late-arriving files, every 6h) ───────
            full_sweep_cfg = prov.get("cron_full_sweep", {})
            if full_sweep_cfg.get("enabled", True):
                full_job_id = f"full_sync_{service_id}"
                seen_ids.add(full_job_id)
                cron_hours = str(full_sweep_cfg.get("cron_hours", "3,9,15,21"))
                cron_minute = int(full_sweep_cfg.get("cron_minute", 30))
                if self._reschedule_or_add_job(
                    _run_full_sweep,
                    "cron",
                    hour=cron_hours,
                    minute=cron_minute,  # 03:30, 09:30, 15:30, 21:30 UTC
                    args=[service_id],
                    id=full_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=3600,
                ):
                    logger.info(
                        "🔍 [scheduler] Registered full-sweep job %s (hours: %s at :%02d UTC).",
                        full_job_id,
                        cron_hours,
                        cron_minute,
                    )

            # ── Gap-heal evaluator (auto full_sweep on sustained loss) ────────
            # Polls compute_log_accounting every 30 min; when sustained loss
            # is detected (≥2 consecutive completed buckets with ≥5% gap), it
            # invokes _run_full_sweep — throttled adaptively (see
            # ``_gap_heal_throttle_hours``). Requires a logging_service_id
            # since gap math depends on Fastly's /stats/service API. Match
            # the admin endpoint's resolution: fall back to ``service_id``
            # when ``logging_service_id`` isn't set as a distinct field —
            # otherwise the cron silently never registers and a 200k-line
            # burst goes unhealed.
            heal_cfg = prov.get("cron_gap_heal", {})
            has_logging_svc = bool(cfg.get("logging_service_id") or cfg.get("service_id"))
            if heal_cfg.get("enabled", True) and has_logging_svc:
                heal_job_id = f"gap_heal_{service_id}"
                seen_ids.add(heal_job_id)
                interval_mins = int(heal_cfg.get("interval_minutes", 30))
                if self._reschedule_or_add_job(
                    _run_gap_heal,
                    "interval",
                    minutes=interval_mins,
                    args=[service_id],
                    id=heal_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=600,
                ):
                    logger.info(
                        "🩹 [scheduler] Registered gap-heal job %s (every %d min).",
                        heal_job_id,
                        interval_mins,
                    )

            # ── Daily optimize job (Iceberg small-file compaction) ────────────
            compact_cfg = prov.get("cron_compact", {})
            if compact_cfg.get("enabled", True):
                opt_job_id = f"optimize_{service_id}"
                seen_ids.add(opt_job_id)
                if opt_job_id not in self._job_ids:
                    self._add_job(
                        _run_optimize,
                        "cron",
                        hour=4,
                        minute=0,  # 04:00 UTC daily
                        args=[service_id],
                        id=opt_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=3600,
                    )
                    self._job_ids[opt_job_id] = opt_job_id
                    logger.info(
                        "⚙️  [scheduler] Registered optimize job %s (daily 04:00 UTC). Local compact handles ongoing dashboard perf — this is just FOS-side housekeeping.",
                        opt_job_id,
                    )

            # ── Local-only compaction & rollup jobs + insights prewarmer ─────
            self._sync_local_compaction_jobs(service_id, cfg, seen_ids=seen_ids, dev_local=False)
            self._register_insights_prewarmer_job(service_id, seen_ids, prov)

            # ── expire-snapshots job (hourly by default) ───────────────────────
            # Was weekly (Sun 04:00 UTC), which produced a week-long performance
            # sawtooth. metadata.json is read, parsed AND rewritten on EVERY
            # commit, and it grows with the snapshot count — so between runs the
            # per-commit cost climbs continuously. Observed 2026-08-13 on a
            # service committing every 5 min: 1,401 snapshots / 1,987 KB
            # metadata.json, commits taking 50-145s each; expiring dropped it to
            # 391 / 358 KB. It had reached 4,579 snapshots before the 08-09 run.
            #
            # Running hourly holds the table at the ``keep_snapshot_days``
            # steady state instead of letting it drift far above it. NOTE the
            # floor is set by that WINDOW, not by this cadence: at ~288
            # commits/day a 7-day window retains ~2,000 snapshots no matter how
            # often this runs. Shrinking the window trades away time-travel
            # recoverability — the 2026-08 rollback was recovered precisely
            # because old metadata still existed — so it is operator-tunable
            # (``cron_sync.keep_snapshot_days``) and stays at 7 by default.
            if compact_cfg.get("enabled", True) and not dev_mode_no_crons():
                exp_job_id = f"expire_{service_id}"
                seen_ids.add(exp_job_id)
                expire_interval_mins = max(5, int(sync_cfg.get("expire_interval_mins", 60)))
                if exp_job_id not in self._job_ids:
                    self._add_job(
                        _run_expire_snapshots,
                        "interval",
                        minutes=expire_interval_mins,
                        jitter=60,
                        args=[service_id],
                        id=exp_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=3600,
                    )
                    self._job_ids[exp_job_id] = exp_job_id
                    logger.info(
                        "🗑️  [scheduler] Registered expire-snapshots job %s (every %dm).",
                        exp_job_id,
                        expire_interval_mins,
                    )
                else:
                    job = self._sched.get_job(exp_job_id)
                    if job is not None:
                        existing = getattr(getattr(job, "trigger", None), "interval", None)
                        if existing is None or int(existing.total_seconds()) != expire_interval_mins * 60:
                            job.reschedule("interval", minutes=expire_interval_mins)
                            logger.info(
                                "🗑️  [scheduler] Rescheduled expire-snapshots job %s to every %dm.",
                                exp_job_id,
                                expire_interval_mins,
                            )

            # ── NGWAF bot sync job (per-service) ─────────────────────────────
            if svcconfig.get_ngwaf_workspace_id(service_id):
                ngwaf_interval_mins = max(1, int(prov.get("cron_ngwaf", {}).get("interval_mins", 5)))
                ngwaf_job_id = f"ngwaf_sync_{service_id}"
                seen_ids.add(ngwaf_job_id)
                if self._reschedule_or_add_job(
                    _run_ngwaf_bot_sync,
                    "interval",
                    minutes=ngwaf_interval_mins,
                    jitter=15,
                    args=[service_id],
                    id=ngwaf_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                    executor="maintenance",
                ):
                    logger.info(
                        "👾 \x1b[36m[ngwaf_sync]\x1b[0m Registered NGWAF bot sync job %s (every %dm, executor=maintenance).",
                        ngwaf_job_id,
                        ngwaf_interval_mins,
                    )

            # ── Metadata retention cleanup (per service) ──────────────────────
            # Daily 03:15 UTC. Runs before optimize (04:00) and after full_sweep
            # (03:30) so the daily admin cron window stays single-threaded
            # across heavy phases. Trims usage_log + ingested_files
            # + cron_runs per cfg["metadata_retention"]; defaults to 1d for
            # the first two and 7d for cron_runs. See
            # backend.core.metadata_db.cleanup_metadata.
            cleanup_cfg = prov.get("cron_metadata_cleanup", {})
            if cleanup_cfg.get("enabled", True) and not dev_mode_no_crons():
                cleanup_job_id = f"metadata_cleanup_{service_id}"
                seen_ids.add(cleanup_job_id)
                cron_hour = int(cleanup_cfg.get("cron_hour", 3))
                cron_minute = int(cleanup_cfg.get("cron_minute", 15))
                if self._reschedule_or_add_job(
                    _run_metadata_cleanup,
                    "cron",
                    hour=cron_hour,
                    minute=cron_minute,
                    args=[service_id],
                    id=cleanup_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=3600,
                ):
                    logger.info(
                        "🧹 \x1b[35m[metadata_cleanup]\x1b[0m Registered metadata cleanup job %s (daily %02d:%02d UTC).",
                        cleanup_job_id,
                        cron_hour,
                        cron_minute,
                    )

        # ── Bot data refresh job ──────────────────────────────────────────────
        bot_refresh_id = "bot_data_refresh"
        seen_ids.add(bot_refresh_id)
        if bot_refresh_id not in self._job_ids:
            self._add_job(
                _run_bot_data_refresh,
                "cron",
                hour=2,
                minute=0,
                id=bot_refresh_id,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
            self._job_ids[bot_refresh_id] = bot_refresh_id
            logger.info("👾 \x1b[36m[bots]\x1b[0m Registered bot data refresh job (daily 02:00 UTC).")

        # ── rDNS enrichment job ───────────────────────────────────────────────
        rdns_job_id = "rdns_enrichment"
        seen_ids.add(rdns_job_id)
        if rdns_job_id not in self._job_ids:
            self._add_job(
                _run_rdns_enrichment,
                "interval",
                minutes=5,
                jitter=15,
                id=rdns_job_id,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
            )
            self._job_ids[rdns_job_id] = rdns_job_id
            logger.info("🌐 \x1b[34m[rdns]\x1b[0m Registered rDNS enrichment job (every 5m).")

        # ── Remote-share audit log purge ─────────────────────────────────────
        # 03:45 UTC — sits after full_sweep (03:30) and before optimize (04:00)
        # (03:30) so the daily admin cron window stays single-threaded across
        # heavy phases. Retention configurable via the
        # `share_audit_retention_days` share_setting (default 90).
        share_purge_id = "share_audit_purge"
        seen_ids.add(share_purge_id)
        if share_purge_id not in self._job_ids:
            self._add_job(
                _run_share_audit_purge,
                "cron",
                hour=3,
                minute=45,
                id=share_purge_id,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
            self._job_ids[share_purge_id] = share_purge_id
            logger.info(
                "🧹 \x1b[35m[share_audit_purge]\x1b[0m Registered share audit purge job (daily 03:45 UTC, executor=maintenance)."
            )

        # ── Operational-vital snapshots (Trends tab + System Health sparklines) ──
        self._register_metric_snapshot_job(seen_ids)

        # DuckDB instance recycle (object-cache leak guard) — local-safe, so it
        # registers in both this path and the dev-local-safe path.
        self._register_recycle_job(seen_ids)

        # Cleanup: remove jobs from the live APScheduler when their service/config disappears.
        for jid in list(self._job_ids.keys()):
            if jid in seen_ids:
                continue
            try:
                self._sched.remove_job(jid)
            except Exception:
                pass
            logger.info("[scheduler] Removed stale job %s.", jid)
            del self._job_ids[jid]

    def _reschedule_or_add_job(self, func, trigger=None, *, reschedule_kwargs: dict | None = None, **kwargs) -> bool:
        """Reschedule ``kwargs["id"]`` when it is already tracked, else add it.

        Returns True when the job was newly added (so the caller logs its
        "Registered ..." line), False when an existing job was rescheduled.
        A missing/failed reschedule is swallowed: the next ``reload()`` retries.
        By default only the trigger's own timing kwargs are re-applied
        (``seconds``/``minutes``/``hours`` for interval, ``hour``/``minute`` for
        cron); pass ``reschedule_kwargs`` to re-apply a different set. ``jitter``,
        ``misfire_grace_time`` etc. only take effect when the job is first added.
        """
        job_id = kwargs["id"]
        if job_id in self._job_ids:
            if reschedule_kwargs is None:
                keys = ("seconds", "minutes", "hours") if trigger == "interval" else ("hour", "minute")
                reschedule_kwargs = {k: kwargs[k] for k in keys if k in kwargs}
            try:
                job = self._sched.get_job(job_id)
                if job:
                    job.reschedule(trigger, **reschedule_kwargs)
            except Exception:
                pass
            return False
        self._add_job(func, trigger, **kwargs)
        self._job_ids[job_id] = job_id
        return True

    def _add_job(self, func, trigger=None, **kwargs):
        job_id = kwargs.pop("id", None)
        args = kwargs.pop("args", [])

        if "executor" not in kwargs and job_id:
            jid = str(job_id)
            if jid.startswith(("log_discovery_", "rum_discovery_", "rum_sync_", "commit_", "sync_")):
                kwargs["executor"] = "streaming"
            elif jid.startswith(
                (
                    "insights_prewarmer_",
                    "local_compact_",
                    "rollup_compact_",
                    "rollup_heal_",
                    "metric_snapshot",
                    "reconcile_fastly_",
                    "metadata_cleanup",
                    "rdns_enrichment",
                    "rdns_refresh",
                    "bot_data_refresh",
                    "optimize_",
                    "ngwaf_sync_",
                    "share_audit_purge",
                    "duckdb_recycle",
                )
            ):
                kwargs["executor"] = "maintenance"
            else:
                kwargs["executor"] = "default"

        self._sched.add_job(func, trigger, id=job_id, args=args, **kwargs)
        self._job_ids[job_id] = job_id

    def reload(self) -> None:
        """Re-read service configs and update all jobs. Call after adding/removing a service."""
        if dev_mode_no_crons():
            # Don't re-arm jobs that start() refused to register. A
            # downstream caller (e.g. a service-config save) would
            # otherwise sneak crons back in after start() bypassed them.
            logger.warning("🚫 [scheduler] FLA_DEV_NO_CRONS=1 — Scheduler.reload() ignored.")
            return
        self._sync_jobs()

    def get_job(self, job_id: str):
        """Return the APScheduler Job object for a given job ID, or None."""
        return self._sched.get_job(job_id)


# Global scheduler instance for process-wide access
_scheduler: Scheduler | None = None


def get_scheduler() -> Scheduler:
    """Return the global scheduler instance, creating it if necessary."""
    global _scheduler
    if _scheduler is None:
        _scheduler = Scheduler()
    return _scheduler


# R-1: drain the heavy-refresh throttle dict between tests so an
# earlier test's last-tick timestamp doesn't suppress a cron call in
# the next test that uses the same service_id.
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("cron.scheduler._last_heavy_refresh", _last_heavy_refresh)

TASK_MAP = {
    "sync_metadata": "metadata_sync",
    "log_discovery": "log_discovery",
    "rum_sync": "rum_sync",
    "full_sync": "full_sync",
    "gap_heal": "gap_heal",
    "commit": "commit",
    "rum_commit": "rum_commit",
    "optimize": "optimize",
    "local_compact": "local_compact",
    "expire": "expire_snapshots",
    "alerts_evaluation": "alerts",
    "ngwaf_sync": "ngwaf_sync",
    "metadata_cleanup": "metadata_cleanup",
    "insights_prewarmer": "insights_prewarmer",
    "rollup_heal": "rollup_hour_heal",
    "rollup_compact": "rollup_compact_daily",
    "ledger_sweep": "ledger_sweep",
    "partial_hour_merge": "partial_hour_merge",
    "rum_discovery": "rum_discovery",
    "ledger_rum_sweep": "ledger_rum_sweep",
}


def build_cron_schedule_payload(source: dict) -> dict:
    """Return ``{"schedules": [...]}`` for the given ``source``."""
    from backend.core import metadata as metadata_db

    sched = get_scheduler()
    service_id = source["name"]
    last_runs: dict[str, dict] = {}
    try:
        per_task = metadata_db.latest_cron_per_task(service_id)
        for task, info in per_task.items():
            last_runs[task] = {
                "last_run_time": info["started_at"],
                "last_run_status": info["status"],
                "last_run_duration_s": info["duration_s"],
                "last_run_summary": info["summary"],
            }
    except Exception:
        pass
    schedules = []

    # Pod-local APScheduler jobs — the ONLY source of cron jobs.
    for job in sched._sched.get_jobs():
        job_id = getattr(job, "id", "")
        if not job_id.endswith(f"_{service_id}"):
            continue
        if job_id.startswith("initial_sync"):
            continue
        task_name = job_id[: -len(f"_{service_id}")]
        db_task = TASK_MAP.get(task_name)
        if db_task is None:
            continue
        from backend.utils.date_utils import iso_z

        next_run = iso_z(job.next_run_time) if job.next_run_time else None  # type: ignore
        schedules.append({"task": db_task, "next_run_time": next_run, **last_runs.get(db_task, {})})
    existing = {s["task"] for s in schedules}
    for task, info in last_runs.items():
        if task not in existing and task in TASK_MAP.values():
            schedules.append({"task": task, "next_run_time": None, **info})

    try:
        if metadata_db.count_alerts(service_id) == 0:
            alerts_entry = next((s for s in schedules if s["task"] == "alerts"), None)
            if alerts_entry is None:
                schedules.append(
                    {
                        "task": "alerts",
                        "next_run_time": None,
                        "disabled_reason": "no_alerts_configured",
                    }
                )
            elif alerts_entry.get("next_run_time") is None:
                alerts_entry["disabled_reason"] = "no_alerts_configured"
    except Exception:
        pass

    return {"schedules": schedules}
