"""In-process APScheduler for background sync and buffer commit.

A single BackgroundScheduler manages three job types per configured service:

  sync_{id}    — ingests new raw .gz files from FOS at the log_period cadence
  commit_{id}  — commits the local buffer to the shared Iceberg table in FOS
                 at the user-configured commit_interval_mins (default 5 min)
  optimize_{id}— daily Iceberg small-file compaction (03:00 UTC)
  expire_{id}  — weekly snapshot expiry (Sunday 04:00 UTC)

Decoupling ingest from commit lets users dial the freshness/cost tradeoff:
a 1-minute log_period can still commit to Iceberg every 5–30 minutes,
creating far fewer snapshots while keeping dashboards nearly real-time.

Usage (called from main.py lifespan):
    from backend.scheduler import get_scheduler
    scheduler = get_scheduler()
    scheduler.start()
    ...
    scheduler.shutdown()
"""

from __future__ import annotations

import logging

logging.getLogger("pyiceberg.io").setLevel(logging.WARNING)
import os
import sys
import threading
import time
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


import concurrent.futures
from functools import wraps

# Hard upper bound on any single cron invocation. Ingest is already capped at
# max_seconds=240 inside _run_service_cron; this leaves ~60s for the post-ingest
# phases (refresh_config_status, usage-log block, update_cron_duration). If the
# inner thread runs past this, the APScheduler worker thread returns anyway so
# max_instances=1 cannot stay wedged across ticks. The leaked inner thread is
# accepted — Python cannot cleanly kill a thread, but it will eventually unblock
# (SQLite timeouts are 30s) and flush its own usage log on exit.
_CRON_HARD_CAP_S = 300


def _display_name(src: dict, fallback: str) -> str:
    """Return src['service_name'] or src['name'], falling back to ``fallback``.
    Used by every cron-log site that wants the human-friendly name with
    the service id as fallback when the friendly name isn't populated."""
    return src.get("service_name") or src.get("name", fallback)


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


def cron_task(name: str):
    """Wraps a cron handler with telemetry + usage-log flush + a hard watchdog.

    The process_context_scope wrapper resets both the ContextVar and the
    process-global mirror (CAS-style) on exit. Otherwise APScheduler's
    worker threads carry the stale ContextVar into the next job, and the
    fsspec iothread keeps reading the stale global — misattributing every
    subsequent cron's I/O to whichever job ran last.

    Watchdog: runs the wrapped function on a single-worker ThreadPoolExecutor
    bounded by _CRON_HARD_CAP_S. On timeout, the executor is shut down with
    wait=False so this wrapper returns and the APScheduler worker thread is
    freed for the next tick.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(service_id: str, *args, **kwargs):
            def _body():
                from backend.utils.telemetry import process_context_scope, start_call_tracking
                from backend.utils.usage_logger import flush_usage_log

                with process_context_scope(name):
                    start_call_tracking()
                    try:
                        return func(service_id, *args, **kwargs)
                    finally:
                        flush_usage_log(service_id)

            ex = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"cron-{name}-{service_id}")
            shutdown_wait = True
            try:
                fut = ex.submit(_body)
                try:
                    return fut.result(timeout=_CRON_HARD_CAP_S)
                except concurrent.futures.TimeoutError:
                    logger.error(
                        "[scheduler] %s/%s exceeded %ds hard cap — abandoning worker "
                        "thread so APScheduler max_instances=1 doesn't wedge ingestion",
                        name,
                        service_id,
                        _CRON_HARD_CAP_S,
                    )
                    shutdown_wait = False
                    return None
            finally:
                ex.shutdown(wait=shutdown_wait)

        return wrapper

    return decorator


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
    from backend.core import metadata_db

    try:
        return metadata_db.count_alerts(service_id) > 0
    except Exception:
        return True


# Ensure project root is importable (same as main.py)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _extract_log_text(run_id: int) -> str:
    """Return a plain-text log summary for a cron run from the progress store."""
    from backend.cron_progress import get_progress

    evs = get_progress(run_id)
    if not evs:
        return ""
    return "\n".join(
        f"[{e.get('type', 'info').upper()}] {e['message']}"
        for e in evs
        if "message" in e and e.get("type") in ("error", "status", "done", "warning")
    )


class Scheduler:
    """Thin wrapper around APScheduler's BackgroundScheduler."""

    def __init__(self) -> None:
        from apscheduler.schedulers.background import BackgroundScheduler

        self._sched = BackgroundScheduler(timezone=UTC)
        # Track per-service job IDs so we can replace them when settings change.
        self._job_ids: dict[str, str] = {}  # job_id -> job_id

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the scheduler and register jobs for all configured services."""
        self._sync_jobs()
        self._sched.start()
        logger.info("🟢 [scheduler] Started (pid: %d). %d job(s) registered.", os.getpid(), len(self._job_ids))

        # Initial metadata sync for analyst (read_only) services only.
        from backend import config as svcconfig

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
                    self._sched.add_job(
                        _run_metadata_sync, args=[service_id], id=f"initial_sync_{service_id}", replace_existing=True
                    )
                except Exception:
                    pass

    def shutdown(self) -> None:
        """Stop the scheduler gracefully."""
        try:
            self._sched.shutdown(wait=False)
        except Exception:
            pass
        logger.info("[scheduler] Stopped.")

    # ── Job management ────────────────────────────────────────────────────────

    def _sync_jobs(self) -> None:
        """Read all service configs and add/update scheduled jobs."""
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured

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
                seen_ids.add(sync_metadata_id)

                if sync_metadata_id in self._job_ids:
                    try:
                        job = self._sched.get_job(sync_metadata_id)
                        if job:
                            job.reschedule("interval", seconds=interval_seconds)
                    except Exception:
                        pass
                else:
                    # Start immediately so the dashboard isn't slow/empty
                    self._sched.add_job(
                        _run_metadata_sync,
                        "interval",
                        seconds=interval_seconds,
                        id=sync_metadata_id,
                        replace_existing=True,
                        start_date=None,
                        args=[service_id],
                        coalesce=True,
                        misfire_grace_time=60,
                    )
                    self._job_ids[sync_metadata_id] = sync_metadata_id
                    logger.info(
                        "[scheduler] Registered metadata sync job %s (every %ds).", sync_metadata_id, interval_seconds
                    )

                # ── Alerts evaluation job for analysts ────────────────────────
                # Analysts evaluate alerts against their locally-cached data,
                # so they need this job even though they skip ingest/commit.
                # Gated on having at least one alert configured — otherwise the
                # cron just fires a "skipped" log every tick. When the user
                # adds an alert, the alerts router calls scheduler.reload() to
                # register the job; deleting the last alert lets the cleanup
                # loop unregister it on the next sync.
                if _service_has_alerts(service_id):
                    alert_job_id = f"alerts_evaluation_{service_id}"
                    seen_ids.add(alert_job_id)
                    if alert_job_id in self._job_ids:
                        try:
                            job = self._sched.get_job(alert_job_id)
                            if job:
                                job.reschedule("interval", seconds=interval_seconds)
                        except Exception:
                            pass
                    else:
                        self._sched.add_job(
                            _run_service_alerts_evaluation,
                            "interval",
                            seconds=interval_seconds,
                            id=alert_job_id,
                            args=[service_id],
                            max_instances=1,
                            coalesce=True,
                            misfire_grace_time=60,
                        )
                        self._job_ids[alert_job_id] = alert_job_id
                        logger.info(
                            "🔔 [scheduler] Registered alerts evaluation job %s (every %ds).",
                            alert_job_id,
                            interval_seconds,
                        )

                # Analysts don't ingest or commit — skip the rest.
                continue
            else:
                # If an admin previously had a metadata sync job, ensure we don't track it
                # It will be removed in the cleanup loop below
                pass

            # ── Sync job (ingest raw files from FOS → local buffer) ───────────
            job_id = f"sync_{service_id}"
            seen_ids.add(job_id)

            if job_id in self._job_ids:
                try:
                    job = self._sched.get_job(job_id)
                    if job:
                        job.reschedule("interval", seconds=interval_seconds)
                        logger.info("[scheduler] Rescheduled sync job %s to every %ds.", job_id, interval_seconds)
                except Exception as e:
                    logger.error("[scheduler] Failed to reschedule sync job %s: %s", job_id, e)
            else:
                # Start immediately so the dashboard isn't slow/empty
                self._sched.add_job(
                    _run_service_cron,
                    "interval",
                    seconds=interval_seconds,
                    start_date=None,
                    args=[service_id],
                    id=job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                )
                self._job_ids[job_id] = job_id
                logger.info("🔄 [scheduler] Registered sync job %s (every %ds).", job_id, interval_seconds)

            # ── Commit job (flush local buffer → Iceberg snapshot in FOS) ─────
            commit_job_id = f"commit_{service_id}"
            seen_ids.add(commit_job_id)

            if commit_job_id in self._job_ids:
                try:
                    job = self._sched.get_job(commit_job_id)
                    if job:
                        job.reschedule("interval", minutes=commit_interval_mins)
                except Exception:
                    pass
            else:
                self._sched.add_job(
                    _run_commit,
                    "interval",
                    minutes=commit_interval_mins,
                    args=[service_id],
                    id=commit_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                )
                self._job_ids[commit_job_id] = commit_job_id
                logger.info(
                    "📦 [scheduler] Registered commit job %s (every %dm).",
                    commit_job_id,
                    commit_interval_mins,
                )

            # ── Alerts evaluation job (Per Service) ───────────────────────────
            # See note above (analyst branch) on the no-alerts gate.
            if _service_has_alerts(service_id):
                alert_job_id = f"alerts_evaluation_{service_id}"
                seen_ids.add(alert_job_id)
                if alert_job_id in self._job_ids:
                    try:
                        job = self._sched.get_job(alert_job_id)
                        if job:
                            job.reschedule("interval", seconds=log_period)
                    except Exception:
                        pass
                else:
                    self._sched.add_job(
                        _run_service_alerts_evaluation,
                        "interval",
                        seconds=log_period,
                        id=alert_job_id,
                        args=[service_id],
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=60,
                    )
                    self._job_ids[alert_job_id] = alert_job_id
                    logger.info(
                        "🔔 [scheduler] Registered alerts evaluation job %s (every %ds).", alert_job_id, log_period
                    )

            # ── Daily full-LIST sweep (catches late-arriving files) ───────────
            full_sweep_cfg = prov.get("cron_full_sweep", {})
            if full_sweep_cfg.get("enabled", True):
                full_job_id = f"full_sync_{service_id}"
                seen_ids.add(full_job_id)
                if full_job_id not in self._job_ids:
                    self._sched.add_job(
                        _run_full_sweep,
                        "cron",
                        hour=3,
                        minute=30,  # 03:30 UTC — offset from optimize at 03:00 to avoid pile-up
                        args=[service_id],
                        id=full_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=3600,
                    )
                    self._job_ids[full_job_id] = full_job_id
                    logger.info("🔍 [scheduler] Registered full-sweep job %s (daily 03:30 UTC).", full_job_id)

            # ── Gap-heal evaluator (auto full_sweep on sustained loss) ────────
            # Polls compute_log_accounting every 30 min; when sustained loss
            # is detected (≥2 consecutive completed buckets with ≥5% gap), it
            # invokes _run_full_sweep — throttled to one heal per
            # GAP_HEAL_THROTTLE_HOURS to prevent thrashing on unrecoverable
            # Fastly→FOS transport loss. Requires a logging_service_id since
            # gap math depends on Fastly's /stats/service API.
            heal_cfg = prov.get("cron_gap_heal", {})
            has_logging_svc = bool(cfg.get("logging_service_id"))
            if heal_cfg.get("enabled", True) and has_logging_svc:
                heal_job_id = f"gap_heal_{service_id}"
                seen_ids.add(heal_job_id)
                if heal_job_id not in self._job_ids:
                    self._sched.add_job(
                        _run_gap_heal,
                        "interval",
                        minutes=int(heal_cfg.get("interval_minutes", 30)),
                        args=[service_id],
                        id=heal_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=600,
                    )
                    self._job_ids[heal_job_id] = heal_job_id
                    logger.info(
                        "🩹 [scheduler] Registered gap-heal job %s (every %d min).",
                        heal_job_id,
                        int(heal_cfg.get("interval_minutes", 30)),
                    )

            # ── Daily optimize job (Iceberg small-file compaction) ────────────
            compact_cfg = prov.get("cron_compact", {})
            if compact_cfg.get("enabled", True):
                opt_job_id = f"optimize_{service_id}"
                seen_ids.add(opt_job_id)
                if opt_job_id not in self._job_ids:
                    self._sched.add_job(
                        _run_optimize,
                        "cron",
                        hour=3,
                        minute=0,  # 03:00 UTC daily — original low-traffic window
                        args=[service_id],
                        id=opt_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=3600,
                    )
                    self._job_ids[opt_job_id] = opt_job_id
                    logger.info(
                        "⚙️  [scheduler] Registered optimize job %s (daily 03:00 UTC). Local compact handles ongoing dashboard perf — this is just FOS-side housekeeping.",
                        opt_job_id,
                    )

            # ── Local-only compaction every 2 min ─────────────────────────────
            # Runs for ALL services regardless of access_level — admins
            # (read-write) AND analysts (read-only, sharing the FOS bucket
            # with the admin). It only touches the LOCAL cache so it's
            # safe for analyst processes that have no FOS write access.
            # Outside the `compact_cfg.enabled` gate above because that
            # gate is for the FOS-touching optimize cron; this one is
            # always-on so every dashboard (admin or analyst) gets the
            # same fast scans.
            lc_job_id = f"local_compact_{service_id}"
            seen_ids.add(lc_job_id)
            if lc_job_id not in self._job_ids:
                self._sched.add_job(
                    _run_local_compact,
                    "interval",
                    minutes=2,
                    args=[service_id],
                    id=lc_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=120,
                )
                self._job_ids[lc_job_id] = lc_job_id
                logger.info("⚙️  [scheduler] Registered local_compact job %s (every 2 min, local-only).", lc_job_id)

            # ── Weekly expire-snapshots job ───────────────────────────────────
            if compact_cfg.get("enabled", True):
                exp_job_id = f"expire_{service_id}"
                seen_ids.add(exp_job_id)
                if exp_job_id not in self._job_ids:
                    self._sched.add_job(
                        _run_expire_snapshots,
                        "cron",
                        day_of_week="sun",
                        hour=4,
                        minute=0,  # Sunday 04:00 UTC
                        args=[service_id],
                        id=exp_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=3600,
                    )
                    self._job_ids[exp_job_id] = exp_job_id
                    logger.info("🗑️  [scheduler] Registered expire-snapshots job %s (weekly Sun 04:00 UTC).", exp_job_id)

            # ── NGWAF bot sync job (per-service) ─────────────────────────────
            if svcconfig.get_ngwaf_workspace_id(service_id):
                ngwaf_interval_mins = max(1, int(prov.get("cron_ngwaf", {}).get("interval_mins", 5)))
                ngwaf_job_id = f"ngwaf_sync_{service_id}"
                seen_ids.add(ngwaf_job_id)
                if ngwaf_job_id in self._job_ids:
                    try:
                        job = self._sched.get_job(ngwaf_job_id)
                        if job:
                            job.reschedule("interval", minutes=ngwaf_interval_mins)
                    except Exception:
                        pass
                else:
                    self._sched.add_job(
                        _run_ngwaf_bot_sync,
                        "interval",
                        minutes=ngwaf_interval_mins,
                        args=[service_id],
                        id=ngwaf_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=300,
                    )
                    self._job_ids[ngwaf_job_id] = ngwaf_job_id
                    logger.info(
                        "👾 \x1b[36m[ngwaf_sync]\x1b[0m Registered NGWAF bot sync job %s (every %dm).",
                        ngwaf_job_id,
                        ngwaf_interval_mins,
                    )

            # ── Metadata retention cleanup (per service) ──────────────────────
            # Daily 03:15 UTC. Slots between optimize (03:00) and full_sweep
            # (03:30) so the daily admin cron window stays single-threaded
            # across heavy phases. Trims usage_log + ingested_files
            # + cron_runs per cfg["metadata_retention"]; defaults to 1d for
            # the first two and 7d for cron_runs. See
            # backend.core.metadata_db.cleanup_metadata.
            cleanup_job_id = f"metadata_cleanup_{service_id}"
            seen_ids.add(cleanup_job_id)
            if cleanup_job_id not in self._job_ids:
                self._sched.add_job(
                    _run_metadata_cleanup,
                    "cron",
                    hour=3,
                    minute=15,
                    args=[service_id],
                    id=cleanup_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=3600,
                )
                self._job_ids[cleanup_job_id] = cleanup_job_id
                logger.info(
                    "🧹 \x1b[35m[metadata_cleanup]\x1b[0m Registered metadata cleanup job %s (daily 03:15 UTC).",
                    cleanup_job_id,
                )

        # ── Bot data refresh job ──────────────────────────────────────────────
        bot_refresh_id = "bot_data_refresh"
        seen_ids.add(bot_refresh_id)
        if bot_refresh_id not in self._job_ids:
            self._sched.add_job(
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
            self._sched.add_job(
                _run_rdns_enrichment,
                "interval",
                minutes=5,
                id=rdns_job_id,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
            )
            self._job_ids[rdns_job_id] = rdns_job_id
            logger.info("🌐 \x1b[34m[rdns]\x1b[0m Registered rDNS enrichment job (every 5m).")

        # ── Remote-share audit log purge ─────────────────────────────────────
        # 03:45 UTC — sits after per-service optimize (03:00) and full_sweep
        # (03:30) so the daily admin cron window stays single-threaded across
        # heavy phases. Retention configurable via the
        # `share_audit_retention_days` share_setting (default 90).
        share_purge_id = "share_audit_purge"
        seen_ids.add(share_purge_id)
        if share_purge_id not in self._job_ids:
            self._sched.add_job(
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
            logger.info("🧹 \x1b[35m[share_audit_purge]\x1b[0m Registered share audit purge job (daily 03:45 UTC).")

        # Remove jobs for deleted services
        stale = set(self._job_ids) - seen_ids
        for job_id in stale:
            try:
                self._sched.remove_job(job_id)
            except Exception:
                pass
            del self._job_ids[job_id]
            logger.info("[scheduler] Removed stale job %s.", job_id)

    def reload(self) -> None:
        """Re-read service configs and update all jobs. Call after adding/removing a service."""
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
    run_id: int, service_id: str, event: dict, job_name: str = "scheduler", service_name: str | None = None
) -> None:
    from backend.cron_progress import add_progress

    add_progress(run_id, event)
    msg = event.get("message")
    if msg:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id)
        svc_name = cfg.get("name", service_id) if cfg else service_id
        display = f"{svc_name} ({service_id})" if svc_name != service_id else service_id

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

        prefix = f"{icon}{c}[{job_name}]{c_end}"
        if t == "error":
            logger.error("%s %s: %s", prefix, display, msg)
        elif t == "warning":
            logger.warning("%s %s: %s", prefix, display, msg)
        else:
            logger.info("%s %s: %s", prefix, display, msg)


# ── Per-service sync logic ────────────────────────────────────────────────────


def _run_metadata_sync(
    service_id: str, run_id: int | None = None, start_time: str | None = None, end_time: str | None = None
) -> None:
    """Refresh Iceberg table metadata and DuckDB view for read-only services.

    Called for 'Analyst' users who don't ingest raw logs but need to see
    new snapshots committed by Admin users.
    """
    from backend import config as svcconfig
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import (
        get_connection,
        get_source_for_service,
        log_cron_run,
        refresh_config_status,
        start_cron_run,
    )
    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    if run_id is None:
        try:
            run_id = start_cron_run(src, "metadata_sync")
        except RuntimeError as e:
            logger.info("[scheduler] %s: skipping metadata_sync — %s", service_id, str(e))
            return

    cleanup_progress_and_reap()
    try:
        pass
    except Exception:
        pass

    # For manual runs (run_id is not None), we ignore the default limit unless
    # it was explicitly passed in. If a manual run is triggered without
    # start_time, it means "Import All", so we should clear any existing limit.
    is_manual = run_id is not None

    if not start_time and not is_manual:
        prov = cfg.get("provisioning", {})
        tr = prov.get("time_range")
        if tr and tr.get("start"):
            start_time = tr["start"]
            logger.info("[scheduler] %s: Using configured start_time limit: %s", service_id, start_time)

    start_time_exec = time.time()

    def elapsed() -> str:
        return _elapsed_since(start_time_exec)

    start_progress(run_id, service_id=service_id, task="metadata_sync")
    _svc_name = cfg.get("name", service_id) if cfg else service_id
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[96m[metadata_sync]\x1b[0m %s: Metadata sync job started.", _display)
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="metadata_sync",
        event={"type": "status", "message": f"{elapsed()} Starting metadata sync..."},
    )

    try:
        # 1. Refresh Iceberg catalog from cloud
        # In PyIceberg SqlCatalog, load_table() will verify metadata from S3
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="metadata_sync",
            event={"type": "status", "message": f"{elapsed()} Checking cloud for new Iceberg snapshots..."},
        )
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="metadata_sync",
            event={
                "type": "status",
                "message": f"{elapsed()}   ↳ Downloading and parsing the latest catalog metadata (this may take 5-10 seconds)...",
            },
        )
        try:
            db_iceberg.init_iceberg_table(src, create=False)
        except Exception as e:
            # If the table doesn't exist yet, it's not an error we need to log as a failure.
            # This happens for brand new services that haven't committed logs yet.
            err_str = str(e).lower()
            if "not found" in err_str or "does not exist" in err_str or "nosuchtable" in err_str:
                msg = "Iceberg table not found, skipping sync until data is committed."
                _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"message": msg})
                _log_and_add_progress(
                    run_id, service_id, job_name="metadata_sync", event={"type": "status", "message": msg}
                )
                log_cron_run(src, "metadata_sync", time.time() - start_time_exec, "success", summary=msg, run_id=run_id)
                _log_and_add_progress(
                    run_id, service_id, job_name="metadata_sync", event={"type": "done", "message": msg}
                )
                end_progress(run_id)
                return
            raise

        # 2. Sync data files (Pull-to-Local caching)
        msg = "Scanning Iceberg table for new data files..."
        if start_time or end_time:
            msg += f" (Range: {start_time or 'Start'} to {end_time or 'End'})"

            # Save the manually requested range so the DuckDB view can strictly bound to it
            prov = cfg.get("provisioning", {})
            if "time_range" not in prov:
                prov["time_range"] = {}
            if start_time:
                prov["time_range"]["start"] = start_time
            if end_time:
                prov["time_range"]["end"] = end_time
            cfg["provisioning"] = prov
            svcconfig.save_config(service_id, cfg)
            # Update local src reference since we mutated cfg
            src["time_range"] = prov["time_range"]
        elif is_manual:
            # Manual "Sync All": clear any previously pinned range
            prov = cfg.get("provisioning", {})
            if "time_range" in prov:
                del prov["time_range"]
                cfg["provisioning"] = prov
                svcconfig.save_config(service_id, cfg)
                src["time_range"] = None
                logger.info("[scheduler] %s: Manual sync-all, cleared time_range limit.", service_id)

        _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "status", "message": msg})

        def _sync_progress(downloaded: int, total: int, filename: str, rows: int) -> None:
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="metadata_sync",
                event={
                    "type": "status",
                    "message": f"Downloading file {downloaded}/{total}: {filename} ({rows:,} rows)",
                },
            )

        data_res = db_iceberg.sync_data(src, progress_callback=_sync_progress, start_time=start_time, end_time=end_time)
        files_cached = data_res.get("files_downloaded", 0)
        rows_cached = data_res.get("rows_downloaded", 0)

        if files_cached == 0:
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="metadata_sync",
                event={"type": "status", "message": "No new Iceberg files to sync — already up to date."},
            )
        else:
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="metadata_sync",
                event={
                    "type": "status",
                    "message": f"Synced {files_cached} Iceberg data file(s) to local cache, {rows_cached:,} rows.",
                },
            )

        # 3. Update DuckDB view
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="metadata_sync",
            event={"type": "status", "message": "Updating DuckDB views..."},
        )
        con = get_connection(source=src, read_only=False)
        try:
            db_iceberg.update_iceberg_view(con, src)
        finally:
            con.close()

        # 4. Import shared history and views/alerts from Admin
        try:
            from backend.state_sync import import_admin_state

            import_admin_state(service_id)
        except Exception as e:
            _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "warning", "message": e})

        # 5. Refresh cached status (row count, etc)
        refresh_config_status(service_id)

        # ── 6. Invalidate dashboard cache ─────────────────────────────────────
        try:
            from backend.repositories.dashboard import _dashboard_cache

            stale_keys = [k for k in _dashboard_cache if k.endswith(f":{src['name']}")]
            for k in stale_keys:
                del _dashboard_cache[k]
        except Exception:
            pass

        duration = time.time() - start_time_exec
        summary = "Refreshed metadata"
        if files_cached > 0:
            verb = "downloaded" if src.get("access_level") == "read_only" else "synced"
            summary += f" and {verb} {files_cached} new Iceberg data file(s)"

        log_cron_run(
            src,
            "metadata_sync",
            duration,
            "success",
            files_downloaded=files_cached,
            rows_ingested=rows_cached,
            summary=summary,
            run_id=run_id,
        )
        _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "done", "message": summary})

    except Exception as e:
        duration = time.time() - start_time_exec
        log_cron_run(
            src, "metadata_sync", duration, "error", error_message=str(e), summary="Metadata sync failed", run_id=run_id
        )
        _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "error", "message": str(e)})
        logger.exception("[scheduler] %s: Metadata sync failed: %s", service_id, e)
    finally:
        end_progress(run_id)

    if run_id is not None:
        try:
            from backend.core.duckdb import update_cron_duration

            update_cron_duration(src, run_id, time.time() - start_time_exec)
        except Exception:
            pass

    logger.info("⏹️  \x1b[96m[metadata_sync]\x1b[0m %s: Metadata sync job finished.", _display)


@cron_task("cron_sync")
def _run_service_cron(
    service_id: str,
    force: bool = False,
    delete_after: bool | None = None,
    run_id: int | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> None:
    """Ingest new raw .gz files from FOS into the local buffer.

    Does NOT commit to Iceberg — that is handled by the separate commit_{id} job
    so ingest cadence and cloud-freshness can be tuned independently.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, refresh_config_status, start_cron_run
    from backend.core.ingest import ingest

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        logger.warning("[scheduler] %s: config not found, skipping.", service_id)
        return

    src = get_source_for_service(service_id)
    if src is None:
        logger.warning("[scheduler] %s: source not found, skipping.", service_id)
        return

    if src.get("access_level") == "read_only" and not force:
        return

    try:
        pass
    except Exception:
        pass

    prov = cfg.get("provisioning", {})
    sync_cfg = prov.get("cron_sync", {})

    sync_enabled = sync_cfg.get("enabled", True)

    if delete_after is None:
        delete_after = sync_cfg.get("delete_after", True)

    _svc_name = cfg.get("name", service_id) if cfg else service_id
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id

    # ── 1. Ingest ─────────────────────────────────────────────────────────────
    if sync_enabled or force:
        # For manual runs (run_id is not None), we ignore the default limit unless
        # it was explicitly passed in.
        is_manual = run_id is not None

        if not start_time and not is_manual:
            tr = prov.get("time_range")
            if tr and tr.get("start"):
                start_time = tr["start"]
                logger.info("[scheduler] %s: Using configured start_time limit: %s", service_id, start_time)
            # time_range.end is intentionally NOT re-applied here. It is only used for
            # the initial import or an explicit manual backfill. Applying it every cron
            # run would permanently freeze ingestion at the original import end date.
        elif is_manual and not start_time:
            # Manual "Sync All": clear any previously pinned range
            prov = cfg.get("provisioning", {})
            if "time_range" in prov:
                del prov["time_range"]
                cfg["provisioning"] = prov
                svcconfig.save_config(service_id, cfg)
                src["time_range"] = None
                logger.info("[scheduler] %s: Manual sync-all, cleared time_range limit.", service_id)

        try:
            if run_id is None:
                run_id = start_cron_run(src, "sync")
        except RuntimeError as e:
            logger.info("[scheduler] %s: skipping sync — %s", service_id, str(e))
            return

        # Disk pre-check: refuse to start if free space is below the floor.
        # Avoids the "pull from FOS, write fails, repeat next tick" cost loop.
        from backend.core.duckdb import _cache_dir

        ok, disk_msg = _check_disk_space(_cache_dir(src), service_id, "sync")
        if not ok:
            log_cron_run(
                src,
                "sync",
                0.0,
                "error",
                run_id=run_id,
                error_message=disk_msg,
                summary=f"Sync aborted: {disk_msg}",
            )
            return

        from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

        cleanup_progress_and_reap()
        start_progress(run_id, service_id=service_id, task="sync")
        logger.info("▶️  \x1b[94m[sync]\x1b[0m %s: Sync job started.", _display)

        start_time_exec = time.time()

        def elapsed() -> str:
            return _elapsed_since(start_time_exec)

        msg = "Starting sync..."
        if start_time or end_time:
            msg += f" (Range: {start_time or 'Start'} to {end_time or 'End'})"
        _log_and_add_progress(
            run_id, service_id, job_name="sync", event={"type": "status", "message": f"{elapsed()} {msg}"}
        )

        done_event: dict = {}
        processed_files = 0
        inserted_rows = 0
        corrupt_rows = 0

        try:
            for event in ingest(
                source=src,
                delete_after=delete_after,
                max_files=5000,
                max_seconds=240,
                start_time=start_time,
                end_time=end_time,
                incremental_only=not is_manual,
            ):
                _log_and_add_progress(run_id, service_id, job_name="sync", event=event)

                if event.get("type") == "file_done":
                    processed_files = event.get("current", processed_files)
                    inserted_rows = event.get("total_inserted", inserted_rows)
                    corrupt_rows = event.get("total_corrupt", corrupt_rows)
                elif event.get("type") == "done":
                    done_event = event
                elif event.get("type") == "error":
                    summary = "Ingestion failed"
                    if processed_files > 0:
                        summary += f" after processing {processed_files} files ({inserted_rows} rows)"
                    log_text = _extract_log_text(run_id)
                    log_cron_run(
                        src,
                        "sync",
                        time.time() - start_time_exec,
                        "error",
                        run_id=run_id,
                        error_message=event.get("message"),
                        summary=summary,
                        files_downloaded=processed_files,
                        rows_ingested=inserted_rows,
                        corrupt_rows=corrupt_rows,
                        log_output=log_text,
                    )
                    _log_and_add_progress(
                        run_id, service_id, job_name="sync", event={"type": "error", "message": event.get("message")}
                    )
                    break
            else:
                if done_event:
                    log_text = _extract_log_text(run_id)
                    if done_event.get("new_files", 0) == 0:
                        log_cron_run(
                            src,
                            "sync",
                            time.time() - start_time_exec,
                            "success",
                            summary="No new log files found in bucket",
                            run_id=run_id,
                            log_output=log_text,
                        )
                        _log_and_add_progress(
                            run_id,
                            service_id,
                            job_name="sync",
                            event={"type": "done", "message": f"{elapsed()} No new log files found in bucket."},
                        )
                    else:
                        summary = (
                            f"Ingested {done_event.get('new_files', 0)} files, "
                            f"{done_event.get('rows_inserted', 0)} rows."
                        )
                        if done_event.get("corrupt_rows"):
                            summary += f" Skipped {done_event.get('corrupt_rows')} corrupted/invalid lines."
                        if done_event.get("deleted_files"):
                            summary += f" Deleted {done_event.get('deleted_files')} raw files."
                        corrupt_details = done_event.get("corrupt_details", [])
                        corrupt_message = "\n".join(corrupt_details) if corrupt_details else None

                        log_cron_run(
                            src,
                            "sync",
                            time.time() - start_time_exec,
                            "success",
                            files_downloaded=done_event.get("new_files", 0),
                            files_deleted_fos=done_event.get("deleted_files", 0),
                            rows_ingested=done_event.get("rows_inserted", 0),
                            corrupt_rows=done_event.get("corrupt_rows", 0),
                            summary=summary,
                            error_message=corrupt_message,
                            run_id=run_id,
                            log_output=log_text,
                        )

                        # Republish the persistent DuckDB view so dashboard reads pick
                        # up the buffer parquets we just wrote. Dashboard reads use
                        # read_only=True + skip_view_update=True (commit 19dfffc) and
                        # never refresh the view themselves. The only other writer-side
                        # update_iceberg_view caller is metadata_sync, which runs right
                        # after commit_buffer drains the buffer — so without this hop,
                        # the view is always republished buffer-less and dashboard lag
                        # is bounded by commit_interval_mins instead of the sync
                        # cadence. CREATE OR REPLACE VIEW is metadata-only (no cloud
                        # reads), so this is cheap.
                        if done_event.get("rows_inserted", 0) > 0:
                            _t0 = time.time()
                            try:
                                from backend.core import iceberg as _ice
                                from backend.core.duckdb import get_connection as _get_conn

                                con_v = _get_conn(source=src, read_only=False)
                                try:
                                    _ice.update_iceberg_view(con_v, src)
                                finally:
                                    con_v.close()
                            except Exception as _e:
                                logger.warning(
                                    "[scheduler] %s: post-sync view refresh failed: %s",
                                    service_id,
                                    _e,
                                )
                            _log_and_add_progress(
                                run_id,
                                service_id,
                                job_name="sync",
                                event={
                                    "type": "status",
                                    "message": f"{elapsed()} View refresh: {int((time.time() - _t0) * 1000)}ms",
                                },
                            )

                        touched_hours = done_event.get("touched_hours", [])
                        if touched_hours:
                            _t_roll = time.time()
                            try:
                                from backend.core.rollups import recompute_touched_hours

                                recompute_touched_hours(service_id, src, set(touched_hours))
                                _log_and_add_progress(
                                    run_id,
                                    service_id,
                                    job_name="sync",
                                    event={
                                        "type": "status",
                                        "message": f"{elapsed()} Rollups computed: {int((time.time() - _t_roll) * 1000)}ms",
                                    },
                                )
                            except Exception as _re:
                                logger.warning(
                                    "[scheduler] %s: post-sync rollup recompute failed: %s",
                                    service_id,
                                    _re,
                                )

        except Exception as e:
            log_text = _extract_log_text(run_id)
            summary = "Ingestion crashed"
            if processed_files > 0:
                summary += f" after processing {processed_files} files ({inserted_rows} rows)"
                _log_and_add_progress(
                    run_id,
                    service_id,
                    job_name="sync",
                    event={
                        "type": "status",
                        "message": f"Crash occurred. Successfully ingested {processed_files} files so far.",
                    },
                )
            log_cron_run(
                src,
                "sync",
                time.time() - start_time_exec,
                "error",
                files_downloaded=processed_files,
                rows_ingested=inserted_rows,
                corrupt_rows=corrupt_rows,
                error_message=str(e),
                summary=summary,
                run_id=run_id,
                log_output=log_text,
            )
            logger.exception("[scheduler] %s: unexpected ingest error.", service_id)
            _log_and_add_progress(run_id, service_id, job_name="sync", event={"type": "error", "message": str(e)})
        finally:
            end_progress(run_id)

    # ── 2. Refresh cached status ──────────────────────────────────────────────
    # Single 60s window covers both the heavy refresh (top_values cache) and
    # the heavy usage-log phase (reconcile_fastly_stats) — claim once per tick
    # and share the verdict so they don't drift relative to each other.
    do_heavy_refresh = _claim_heavy_refresh(service_id) or bool(force)
    if (sync_enabled or force) and run_id is not None:
        _msg_suffix = "+ filter suggestions" if do_heavy_refresh else "(header only)"
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} Refreshing sync status {_msg_suffix}...",
            },
        )
    _t0 = time.time()
    try:
        refresh_config_status(service_id, include_top_values=do_heavy_refresh)
    except Exception:
        pass
    if run_id is not None:
        _heavy = " (heavy)" if do_heavy_refresh else ""
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} refresh_config_status{_heavy}: {int((time.time() - _t0) * 1000)}ms",
            },
        )

    # ── 3. Invalidate dashboard cache ─────────────────────────────────────────
    _t0 = time.time()
    _invalidated = 0
    try:
        from backend.repositories.dashboard import _dashboard_cache

        src_name = src.get("name", "")
        stale_keys = [k for k in _dashboard_cache if k.endswith(f":{src_name}")]
        _invalidated = len(stale_keys)
        for k in stale_keys:
            del _dashboard_cache[k]
    except Exception:
        pass
    if run_id is not None and _invalidated:
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} dashboard cache invalidate ({_invalidated} keys): {int((time.time() - _t0) * 1000)}ms",
            },
        )

    # ── 4. Usage log bookkeeping ──────────────────────────────────────────────
    # Each ingested raw log file = 1 billable Class A PutObject by Fastly's edge.
    # Synthesise those rows + flush in-process FOS/CDN calls + purge old entries.
    # Idempotent — safe to call after every sync, including after a retry.
    if (sync_enabled or force) and run_id is not None:
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} Updating usage log (Fastly-edge writes, in-process calls, retention purge)...",
            },
        )

    def _usage_log_phase() -> None:
        from backend.core.duckdb import backfill_fastly_edge_writes, reconcile_fastly_stats
        from backend.utils.usage_logger import run_usage_log_cleanup

        try:
            inserted = backfill_fastly_edge_writes(src)
            if inserted:
                if run_id is not None:
                    _log_and_add_progress(
                        run_id,
                        service_id,
                        job_name="usage_log",
                        event={"type": "status", "message": f"Backfilled {inserted} Fastly-edge PUTs to usage log"},
                    )
                else:
                    logger.info("[usage_log] %s: backfilled %d Fastly-edge PUTs", service_id, inserted)
        except Exception as e:
            logger.warning("[usage_log] backfill failed for %s: %s", service_id, e)

        # Pull Fastly /stats/aggregate to reconcile per-hour op counts. Closes
        # the multipart-upload + bookkeeping gap that backfill_fastly_edge_writes
        # cannot observe (it counts 1 op per file; Fastly emits ~3+). Writes one
        # compact row per hour/class gap via SUM(count) aggregation.
        # Window is 26h so the Usage Log page's 24h view always shows fully
        # reconciled data (and survives a small clock-skew buffer). One
        # Fastly API call covers the whole window regardless of hours_back.
        # Gated by do_heavy_refresh so a 1s log_period (5s tick) doesn't fire
        # this every 5s — Usage Log reads at hourly grain so 60s lag is invisible.
        if do_heavy_refresh:
            try:
                written = reconcile_fastly_stats(src, hours_back=26)
                if written:
                    if run_id is not None:
                        _log_and_add_progress(
                            run_id,
                            service_id,
                            job_name="usage_log",
                            event={"type": "status", "message": f"Reconciled {written} hourly Fastly stats gap(s)"},
                        )
                    else:
                        logger.info("[usage_log] %s: reconciled %d hourly stats gap(s)", service_id, written)
            except Exception as e:
                logger.warning("[usage_log] Fastly stats reconciliation failed for %s: %s", service_id, e)

        run_usage_log_cleanup(service_id)

    # Run _usage_log_phase inline. Pre-fix this was wrapped in a NESTED
    # ThreadPoolExecutor — but ``_run_service_cron`` is itself already
    # running inside the ``@cron_task`` executor (one layer up). On the
    # 30s timeout path the old code called ``shutdown(wait=False)``,
    # which abandons the worker thread + everything it pinned (DuckDB
    # connections, aiohttp sessions, Fastly API state). On a 50-service
    # deployment with reconcile_fastly_stats hitting the API in lockstep,
    # the inner timeout fired routinely and each leak orphaned an 8-12MB
    # stack plus whatever Python state was live. Over hours: multi-GB
    # unbounded growth — a confirmed contributor to the recurring host
    # OOM-kills.
    #
    # Running inline drops the leak and matches every other phase in
    # this cron body. If a per-phase timeout is needed in the future,
    # use a cooperative cancel token through the I/O layer rather than
    # abandoning a thread.
    _t0 = time.time()
    try:
        _usage_log_phase()
    except Exception as e:
        logger.warning("[scheduler] %s: usage_log phase failed: %s", service_id, e)
    if run_id is not None:
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} usage_log phase: {int((time.time() - _t0) * 1000)}ms",
            },
        )

    # ── 5. Final duration record ──────────────────────────────────────────────
    if (sync_enabled or force) and run_id is not None:
        try:
            from backend.core.duckdb import update_cron_duration

            # Refresh log_output too — the initial log_cron_run snapshot was
            # taken before phases 1.5-4 (view refresh, refresh_config_status,
            # cache invalidate, usage_log) emitted their per-phase timing events.
            update_cron_duration(
                src,
                run_id,
                time.time() - start_time_exec,
                log_output=_extract_log_text(run_id),
            )
        except Exception as e:
            logger.warning("Failed to update full cron duration: %s", e)

    logger.info("⏹️  \x1b[94m[sync]\x1b[0m %s: Sync job finished.", _display)


@cron_task("full_sync")
def _run_full_sweep(service_id: str) -> None:
    """Daily catch-net: full LIST over raw/ to pick up late-arriving files.

    The minute-cadence sync uses a 4h ``StartAfter`` lookback to bound LIST
    cost. If a Fastly POP backfills logs older than that window (recovery,
    timestamp skew, manual replay), the incremental scan never sees them.
    This sweep lists the entire raw/ prefix once a day and ingests anything
    not already in ``ingested_files``. Logged as task=``full_sync`` so users
    can distinguish catch-net runs from regular sync in the cron history.
    """
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.ingest import ingest

    src = get_source_for_service(service_id)
    if src is None or src.get("access_level") == "read_only":
        return

    try:
        run_id = start_cron_run(src, "full_sync")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[95m[full_sync]\x1b[0m %s: skipping — %s", service_id, e)
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="full_sync")
    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[95m[full_sync]\x1b[0m %s: Daily full-LIST sweep started.", _display)

    start_time_exec = time.time()
    processed_files = 0
    inserted_rows = 0
    corrupt_rows = 0
    done_event: dict = {}

    try:
        for event in ingest(
            source=src,
            delete_after=False,  # catch-net only ingests; regular sync handles deletion
            max_files=20000,
            max_seconds=900,
            incremental_only=False,
        ):
            _log_and_add_progress(run_id, service_id, job_name="full_sync", event=event)
            if event.get("type") == "file_done":
                processed_files = event.get("current", processed_files)
                inserted_rows = event.get("total_inserted", inserted_rows)
                corrupt_rows = event.get("total_corrupt", corrupt_rows)
            elif event.get("type") == "done":
                done_event = event
            elif event.get("type") == "error":
                log_cron_run(
                    src,
                    "full_sync",
                    time.time() - start_time_exec,
                    "error",
                    error_message=event.get("message"),
                    summary="Full-sweep failed",
                    files_downloaded=processed_files,
                    rows_ingested=inserted_rows,
                    corrupt_rows=corrupt_rows,
                    run_id=run_id,
                    log_output=_extract_log_text(run_id),
                )
                end_progress(run_id)
                return

        new_files = done_event.get("new_files", 0)
        rows = done_event.get("rows_inserted", 0)
        summary = (
            "No late-arriving files found"
            if new_files == 0
            else f"Backfilled {new_files} late-arriving file(s), {rows} row(s)"
        )
        log_cron_run(
            src,
            "full_sync",
            time.time() - start_time_exec,
            "success",
            files_downloaded=new_files,
            rows_ingested=rows,
            corrupt_rows=done_event.get("corrupt_rows", 0),
            summary=summary,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(run_id, service_id, job_name="full_sync", event={"type": "done", "message": summary})
    except Exception as e:
        log_cron_run(
            src,
            "full_sync",
            time.time() - start_time_exec,
            "error",
            error_message=str(e),
            summary="Full-sweep crashed",
            files_downloaded=processed_files,
            rows_ingested=inserted_rows,
            corrupt_rows=corrupt_rows,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        logger.exception("[full_sync] %s: unexpected error", service_id)
    finally:
        end_progress(run_id)

    logger.info("⏹️  \x1b[95m[full_sync]\x1b[0m %s: Daily full-LIST sweep finished.", _display)


# Throttle window between gap-heal-triggered full_sweep invocations. The
# detection cron itself runs more often (every 30 min) so we react fast to
# new sustained loss, but the actual heal is bounded to prevent thrashing.
GAP_HEAL_THROTTLE_HOURS = 4


@cron_task("gap_heal")
def _run_gap_heal(service_id: str) -> None:
    """Periodic gap detector that triggers a full_sweep when sustained loss
    is observed between Fastly's authoritative log-line emission counts and
    our ingested rows.

    Sustained loss = ≥LOG_ACCOUNTING_MIN_RUN consecutive completed hourly
    buckets with gap_pct ≥ LOG_ACCOUNTING_LOSS_THRESHOLD. The in-flight
    bucket is excluded (Fastly Stats lags ingest), matching the UI callout.

    Throttled to one heal per GAP_HEAL_THROTTLE_HOURS hours so that a
    persistent gap (e.g. Fastly→FOS transport loss we cannot recover from)
    doesn't thrash the scheduler.
    """
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run

    src = get_source_for_service(service_id)
    if src is None or src.get("access_level") == "read_only":
        return

    try:
        run_id = start_cron_run(src, "gap_heal")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[95m[gap_heal]\x1b[0m %s: skipping — %s", service_id, e)
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="gap_heal")
    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id

    start_time_exec = time.time()
    try:
        from backend.routers.admin import compute_log_accounting

        result = compute_log_accounting(src, hours=24, by="hour")
        sustained = result.get("sustained_loss")
        if sustained is None:
            log_cron_run(
                src,
                "gap_heal",
                time.time() - start_time_exec,
                "success",
                summary="No sustained loss detected",
                run_id=run_id,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="gap_heal",
                event={"type": "done", "message": "No sustained loss detected"},
            )
            return

        # Sustained loss observed — apply throttle to actual heal trigger.
        last_heal = _last_successful_gap_heal_trigger(service_id)
        if last_heal is not None:
            elapsed_hours = (time.time() - last_heal) / 3600.0
            if elapsed_hours < GAP_HEAL_THROTTLE_HOURS:
                msg = (
                    f"Sustained loss detected ({sustained.n_buckets} bucket(s), "
                    f"max gap {sustained.max_gap_pct:.1%}) — throttled, last heal "
                    f"{elapsed_hours:.1f}h ago (< {GAP_HEAL_THROTTLE_HOURS}h)"
                )
                log_cron_run(
                    src,
                    "gap_heal",
                    time.time() - start_time_exec,
                    "success",
                    summary=msg,
                    run_id=run_id,
                    log_output=_extract_log_text(run_id),
                )
                _log_and_add_progress(run_id, service_id, job_name="gap_heal", event={"type": "done", "message": msg})
                return

        msg = (
            f"Sustained loss detected ({sustained.n_buckets} bucket(s) "
            f"from {sustained.started_at}, max gap {sustained.max_gap_pct:.1%}, "
            f"{sustained.total_lost_lines} lost line(s)) — triggering full_sweep"
        )
        logger.warning("🩹 \x1b[33m[gap_heal]\x1b[0m %s: %s", _display, msg)
        _log_and_add_progress(run_id, service_id, job_name="gap_heal", event={"type": "status", "message": msg})
        log_cron_run(
            src,
            "gap_heal",
            time.time() - start_time_exec,
            "success",
            summary=msg,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        # Mark heal trigger BEFORE invoking the sweep so a long-running sweep
        # doesn't itself trip a second gap_heal tick into re-triggering.
        _mark_gap_heal_triggered(service_id)
        _run_full_sweep(service_id)
    except Exception as e:
        log_cron_run(
            src,
            "gap_heal",
            time.time() - start_time_exec,
            "error",
            error_message=str(e),
            summary="Gap-heal evaluation crashed",
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        logger.exception("[gap_heal] %s: unexpected error", service_id)
    finally:
        end_progress(run_id)


# Tracks the wall-clock time of the most recent gap_heal that actually
# triggered a full_sweep. Lives in-process so a service restart clears it
# (acceptable: a restart implies the operator is paying attention; one
# extra sweep at startup is fine). Keyed by service_id.
_GAP_HEAL_LAST_TRIGGER: dict[str, float] = {}


def _last_successful_gap_heal_trigger(service_id: str) -> float | None:
    return _GAP_HEAL_LAST_TRIGGER.get(service_id)


def _mark_gap_heal_triggered(service_id: str) -> None:
    _GAP_HEAL_LAST_TRIGGER[service_id] = time.time()


# Hard threshold: below this, ingest will refuse to start. A typical
# .gz raw log batch can land 50-200 MB on disk before commit drains it,
# and the iceberg manifest cache adds more. 500 MB is conservative
# enough to leave room for a single in-flight tick to finish safely.
_DISK_FREE_HARD_FLOOR_BYTES = 500 * 1024 * 1024
# Same idea as a percentage, for the (rare) case of a very small disk
# where 500 MB is most of free. Whichever check trips first wins.
_DISK_FREE_HARD_FLOOR_PCT = 0.03  # 3 %


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


@cron_task("cron_compact")
def _run_commit(service_id: str, force: bool = False, run_id: int | None = None) -> None:
    """Commit the local buffer to the shared Iceberg table in FOS.

    Runs on its own cadence (commit_interval_mins) — independent of how often
    raw files are ingested. This lets the user control cloud data freshness
    without changing the Fastly logging endpoint period.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    if src.get("access_level") == "read_only" and not force:
        return

    try:
        pass
    except Exception:
        pass

    prov = cfg.get("provisioning", {})
    sync_cfg = prov.get("cron_sync", {})
    if not sync_cfg.get("enabled", True) and not force:
        return

    try:
        if run_id is None:
            run_id = start_cron_run(src, "commit")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[95m[commit]\x1b[0m %s: skipping — %s", service_id, str(e))
        return

    # Disk pre-check: commits write manifest cache + cloud-staged parquet
    # locally before upload. A full disk during commit can corrupt the
    # iceberg state midway, which is much worse than refusing to start.
    from backend.core.duckdb import _cache_dir as _commit_cache_dir

    ok, disk_msg = _check_disk_space(_commit_cache_dir(src), service_id, "commit")
    if not ok:
        log_cron_run(
            src,
            "commit",
            0.0,
            "error",
            run_id=run_id,
            error_message=disk_msg,
            summary=f"Commit aborted: {disk_msg}",
        )
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="commit")
    _svc_name = cfg.get("name", service_id) if cfg else service_id
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[95m[commit]\x1b[0m %s: Commit job started.", _display)
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="commit",
        event={"type": "status", "message": "Committing local buffer to Iceberg snapshot..."},
    )

    start_time = time.time()
    try:
        from backend.core import iceberg as db_iceberg

        def _commit_progress(type, msg):
            _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": type, "message": msg})

        result = db_iceberg.commit_buffer(src, progress_callback=_commit_progress)
        duration = time.time() - start_time
        quarantined = int(result.get("quarantined_files", 0) or 0)
        quarantine_suffix = f" ⚠ quarantined {quarantined} unreadable file(s)" if quarantined else ""
        # Post-commit backlog probe: if anything is still in the buffer after a
        # successful commit, the next commit was racing with a fresh ingest OR
        # the drain is genuinely stuck (catalog perms, schema mismatch, etc.).
        # The threshold scales with commit_interval_mins so "stuck" means
        # "older than what a single commit cycle could reasonably leave behind."
        backlog_suffix = _check_buffer_backlog(
            src, service_id, commit_interval_mins=int(sync_cfg.get("commit_interval_mins", 5))
        )
        if result.get("files_committed", 0) > 0:
            summary = (
                f"Committed {result['files_committed']} buffer file(s) "
                f"({result['rows_committed']} rows) → snapshot {result.get('snapshot_id')}.{quarantine_suffix}{backlog_suffix}"
            )
            log_cron_run(
                src,
                "commit",
                duration,
                "success",
                run_id=run_id,
                rows_ingested=result["rows_committed"],
                summary=summary,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": "done", "message": summary})

            # ── On-demand Sync ──
            # Since we just committed new data to the cloud, trigger a sync
            # immediately so the local cache/Data Lake view is updated.
            try:
                _run_metadata_sync(service_id)
            except Exception as e:
                _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": "warning", "message": e})

            # ── Compact-on-sync ──
            # New parquet files just landed in the local cache. Fire local
            # compaction immediately to merge them rather than waiting up
            # to 2 min for the cron tick. Cheap and keeps the small-file
            # count as low as possible for the next dashboard render.
            # Wrapped in a fresh thread so a slow merge doesn't extend
            # the sync cron's wall-clock and risk the watchdog.
            try:
                import threading as _t

                from backend.core import local_compaction as _lc

                _t.Thread(
                    target=lambda: _lc.compact_local_partitions(src),
                    name=f"local-compact-on-sync:{service_id}",
                    daemon=True,
                ).start()
            except Exception as e:
                logger.warning("[scheduler] %s: post-sync local compaction failed to launch: %s", service_id, e)
        else:
            summary = "No new data to commit" + quarantine_suffix + backlog_suffix
            log_cron_run(
                src,
                "commit",
                duration,
                "success",
                run_id=run_id,
                summary=summary,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": "done", "message": summary})
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "commit",
            duration,
            "error",
            run_id=run_id,
            error_message=str(e),
            summary="Buffer commit failed",
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": "error", "message": str(e)})
        logger.exception("[scheduler] %s: buffer commit failed: %s", service_id, e)
    finally:
        end_progress(run_id)

    if run_id is not None:
        try:
            from backend.core.duckdb import update_cron_duration

            update_cron_duration(src, run_id, time.time() - start_time)
        except Exception:
            pass

    logger.info("⏹️  \x1b[95m[commit]\x1b[0m %s: Commit job finished.", _display)


# ── Iceberg maintenance workers ───────────────────────────────────────────────


@cron_task("local_compact")
def _run_local_compact(service_id: str) -> None:
    """Frequent job: merge small parquet files in the LOCAL CACHE only.

    Does NOT touch FOS — only rewrites files inside cache/<bucket>/data/
    so DuckDB's view-glob picks up fewer files at query time. Free in
    terms of FOS cost (no 30-day-minimum penalty), so we can run it
    aggressively (every 10 min) without billing impact.

    Distinct from ``_run_optimize`` which writes through PyIceberg and
    DOES update FOS.
    """
    import time

    from backend.core import local_compaction as _lc
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run

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
    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
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


@cron_task("optimize_iceberg")
def _run_optimize(service_id: str) -> None:
    """Daily job: compact small Iceberg data files into target-sized ones."""
    import time

    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        pass
    except Exception:
        pass

    try:
        run_id = start_cron_run(src, "optimize")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[92m[optimize]\x1b[0m %s: skipping — %s", service_id, str(e))
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="optimize")
    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[92m[optimize]\x1b[0m %s: Optimize job started.", _display)
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="optimize",
        event={"type": "status", "message": "Scanning Iceberg table for small files to compact..."},
    )

    start_time = time.time()
    try:
        # Pin the cron's threshold to the conservative original (>10 files
        # per partition) so the daily FOS-touching pass stays cheap. The
        # auto-derive heuristic stays available for the admin endpoint
        # (`/admin/optimize-now`) when you want to force aggressive cleanup.
        result = db_iceberg.optimize_table(src, min_files_per_partition=10)
        duration = time.time() - start_time
        if "error" in result:
            log_cron_run(
                src,
                "optimize",
                duration,
                "error",
                error_message=result["error"],
                summary="Iceberg optimize failed",
                run_id=run_id,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(
                run_id, service_id, job_name="optimize", event={"type": "error", "message": result["error"]}
            )
            _log_and_add_progress(
                run_id, service_id, job_name="optimize", event={"type": "warning", "message": result["error"]}
            )
        else:
            summary = f"Rewrote {result.get('files_rewritten', 0)} files into {result.get('files_added', 0)} files"
            partition_errors = result.get("partition_errors") or []
            if partition_errors:
                eligible = result.get("eligible_partitions", 0)
                summary += f" — {len(partition_errors)}/{eligible} partitions failed"
                # First 3 errors give enough signal for triage without exploding log size.
                err_preview = "\n".join(partition_errors[:3])
                if len(partition_errors) > 3:
                    err_preview += f"\n... ({len(partition_errors) - 3} more)"
                status = "error" if result.get("files_added", 0) == 0 else "warning"
            else:
                err_preview = None
                status = "success"
            log_cron_run(
                src,
                "optimize",
                duration,
                status,
                run_id=run_id,
                parquet_files_optimized=result.get("files_rewritten", 0),
                parquet_files_created=result.get("files_added", 0),
                summary=summary,
                error_message=err_preview,
                log_output=_extract_log_text(run_id),
            )
            event_type = "done" if status == "success" else status
            _log_and_add_progress(
                run_id, service_id, job_name="optimize", event={"type": event_type, "message": summary}
            )
            logger.info(
                "[scheduler] %s: optimize complete — %s",
                service_id,
                summary,
            )
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "optimize",
            duration,
            "error",
            error_message=str(e),
            summary="Iceberg optimize failed",
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(run_id, service_id, job_name="optimize", event={"type": "error", "message": str(e)})
        logger.exception("[scheduler] %s: optimize failed: %s", service_id, e)
    finally:
        end_progress(run_id)

    if run_id is not None:
        try:
            from backend.core.duckdb import update_cron_duration

            update_cron_duration(src, run_id, time.time() - start_time)
        except Exception:
            pass

    logger.info("⏹️  \x1b[92m[optimize]\x1b[0m %s: Optimize job finished.", _display)


@cron_task("expire_snapshots")
def _run_expire_snapshots(service_id: str) -> None:
    """Weekly job: perform cloud maintenance including data deletion, cache cleanup, and snapshot expiry."""
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import get_source_for_service

    src = get_source_for_service(service_id)
    if src is None:
        return

    svc_id = src.get("service_id", "unknown")
    svc_name = _display_name(src, svc_id)
    display_name = f"{svc_name} ({svc_id})" if svc_name != svc_id else svc_id
    logger.info("▶️  \x1b[90m[expire]\x1b[0m %s: Maintenance job started.", display_name)

    try:
        pass
    except Exception:
        pass

    try:
        result = db_iceberg.run_cloud_maintenance(src)
        if "error" in result:
            logger.warning("%s %s: %s", JOB_COLORS["expire"] + "[expire]" + RESET_COLOR, display_name, result["error"])
        else:
            logger.info("🗑️ \x1b[90m[expire]\x1b[0m %s: Maintenance completed. %s", display_name, result)
    except Exception as e:
        logger.exception(
            "%s %s: Maintenance failed: %s", JOB_COLORS["expire"] + "[expire]" + RESET_COLOR, display_name, e
        )

    logger.info("⏹️  \x1b[90m[expire]\x1b[0m %s: Maintenance job finished.", display_name)


@cron_task("sync_ngwaf_bots")
def _run_ngwaf_bot_sync(service_id: str) -> None:
    """Fetch NGWAF VERIFIED-BOT records and upsert into the local SQLite cache.

    Skips silently if ngwaf_workspace_id is not configured for the service.
    Resumes from last_timestamp_synced so restarts after a crash don't lose progress.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.utils.ngwaf import fetch_verified_bots_paged
    from backend.utils.ngwaf_bot_cache import cleanup_old_bots, ensure_schema, upsert_bots

    # Make sure the cache file + tables exist before anything else touches it.
    # Otherwise the planner query in oldest_unenriched_timestamp throws on the
    # very first run and the cron exits without ever populating data.
    try:
        ensure_schema()
    except Exception:
        pass

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
    if not workspace_id:
        return  # Not configured — skip silently

    src = get_source_for_service(service_id)
    if src is None:
        return

    api_key = cfg.get("fastly_api_key", "")
    if not api_key:
        logger.warning("[ngwaf_sync] %s: no fastly_api_key configured, skipping.", service_id)
        return

    try:
        run_id = start_cron_run(src, "ngwaf_sync")
    except RuntimeError as e:
        logger.info("[ngwaf_sync] %s: skipping — %s", service_id, e)
        return

    svc_display = cfg.get("name", service_id)
    logger.info("▶️  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job started.", svc_display)

    try:
        pass
    except Exception:
        pass

    prov = cfg.get("provisioning", {})
    retention_days = int(prov.get("cron_ngwaf", {}).get("log_retention_days", 30))
    server_name_filter = cfg.get("server_name") or None

    from backend.utils.bot_sources import build_matcher
    from backend.utils.ngwaf_bot_cache import get_last_timestamp, update_sync_watermark

    matcher = build_matcher()
    # Watermark-only resume path. upsert_bots() advances last_timestamp_synced
    # after every successful page, so steady state reads from local SQLite with
    # zero cloud I/O. On first-ever sync the watermark is NULL — seed it with
    # "now" and skip this cycle so the next one starts cleanly from "now".
    # We don't enrich pre-provisioning log rows (rarely the user's intent) and
    # we don't fall back to a cloud planner that scans every iceberg manifest.
    from_ts = get_last_timestamp(workspace_id)
    if not from_ts:
        now_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        update_sync_watermark(workspace_id, now_ts)
        summary = (
            f"First sync — seeded watermark at {now_ts}. Next cycle will fetch new bot records from this point forward."
        )
        log_cron_run(src, "ngwaf_sync", 0.0, "success", summary=summary, run_id=run_id)
        _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary})
        logger.info("⏹️  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)
        return

    total_records = 0
    start_time = time.time()
    # Budget: page for up to 4 minutes per execution. Each page is committed so
    # a crash or budget cut never loses partially-synced data.
    max_runtime_secs = 240
    budget_exceeded = False

    try:
        for page_records, page_latest_ts, _raw_count in fetch_verified_bots_paged(api_key, workspace_id, from_ts):
            if server_name_filter:
                page_records = [
                    r for r in page_records if not r.get("server_name") or r["server_name"] == server_name_filter
                ]

            enriched: list[dict] = []
            for r in page_records:
                ua = r.get("user_agent")
                wk_matches = matcher(ua) if ua else ()
                wk_match = wk_matches[0] if wk_matches else None
                enriched.append(
                    {
                        **r,
                        "wellknown_bot_id": wk_match.get("id") if wk_match else None,
                        "wellknown_bot_name": wk_match.get("name") if wk_match else None,
                    }
                )

            if enriched or page_latest_ts:
                upsert_bots(enriched, workspace_id, page_latest_ts)
            total_records += len(enriched)

            if time.time() - start_time >= max_runtime_secs:
                budget_exceeded = True
                break

        deleted = cleanup_old_bots(retention_days)
        if budget_exceeded:
            summary = f"Synced {total_records} bot record(s) (budget reached — will continue next run), cleaned {deleted} old row(s)."
        else:
            summary = f"Synced {total_records} bot record(s), cleaned {deleted} old row(s)."
        log_cron_run(src, "ngwaf_sync", time.time() - start_time, "success", summary=summary, run_id=run_id)
        _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary})
    except Exception as e:
        log_cron_run(
            src,
            "ngwaf_sync",
            time.time() - start_time,
            "error",
            error_message=str(e),
            summary="NGWAF sync failed",
            run_id=run_id,
        )
        _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "error", "message": str(e)})
        logger.exception("[ngwaf_sync] %s: sync failed: %s", svc_display, e)

    logger.info("⏹️  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)


def _run_bot_data_refresh() -> None:
    """Fetch and cache all enabled bot sources (nightly 02:00 UTC)."""
    import time

    from backend.utils.bot_sources import refresh_all_sources
    from backend.utils.system_jobs import record_job_run

    logger.info("▶️  \x1b[36m[bots]\x1b[0m Bot data refresh job started.")
    start = time.monotonic()
    try:
        results = refresh_all_sources()
        total = sum(r.get("entry_count", 0) for r in results)
        record_job_run(
            "bot_data_refresh",
            "success",
            time.monotonic() - start,
            f"Updated {len(results)} source(s), {total} total entries",
        )
        logger.info("✅ \x1b[36m[bots]\x1b[0m Refreshed %d source(s), %d total entries", len(results), total)
    except Exception as e:
        record_job_run("bot_data_refresh", "error", time.monotonic() - start, str(e))
        logger.error("[bot_data_refresh] Failed: %s", e)

    logger.info("⏹️  \x1b[36m[bots]\x1b[0m Bot data refresh job finished.")


def _run_rdns_enrichment() -> None:
    """Resolve pending rDNS lookups and discover new IPs (every 5 min)."""
    import time

    from backend.utils.rdns_cache import enrich_batch
    from backend.utils.system_jobs import record_job_run

    logger.info("▶️  \x1b[34m[rdns]\x1b[0m rDNS enrichment job started.")
    start = time.monotonic()
    try:
        summary = enrich_batch()
        record_job_run(
            "rdns_enrichment",
            "success",
            time.monotonic() - start,
            f"resolved={summary['resolved']} errors={summary['errors']} discovered={summary['discovered']}",
        )
    except Exception as e:
        record_job_run("rdns_enrichment", "error", time.monotonic() - start, str(e))
        logger.error("[rdns_enrichment] Failed: %s", e)

    logger.info("⏹️  \x1b[34m[rdns]\x1b[0m rDNS enrichment job finished.")


def _run_share_audit_purge() -> None:
    """Drop remote-share audit rows older than the retention window (daily 03:45 UTC).

    Retention is read from the `share_audit_retention_days` setting, defaulting
    to 90 days. The companion endpoint is `share_db.purge_old_audit_logs`.
    """
    import time

    from backend.core import share_db
    from backend.utils.system_jobs import record_job_run

    logger.info("▶️  \x1b[35m[share_audit_purge]\x1b[0m Share audit purge job started.")
    start = time.monotonic()
    try:
        raw = share_db.get_setting("share_audit_retention_days", "90")
        try:
            retention = max(1, int(raw or "90"))
        except (TypeError, ValueError):
            retention = 90
        deleted = share_db.purge_old_audit_logs(retention_days=retention)
        record_job_run(
            "share_audit_purge",
            "success",
            time.monotonic() - start,
            f"deleted={deleted} retention_days={retention}",
        )
        logger.info(
            "✅ \x1b[35m[share_audit_purge]\x1b[0m Deleted %d row(s) older than %d days.",
            deleted,
            retention,
        )
    except Exception as e:
        record_job_run("share_audit_purge", "error", time.monotonic() - start, str(e))
        logger.error("[share_audit_purge] Failed: %s", e)

    logger.info("⏹️  \x1b[35m[share_audit_purge]\x1b[0m Share audit purge job finished.")


@cron_task("evaluate_alerts")
def _run_service_alerts_evaluation(service_id: str) -> None:
    """Evaluate all enabled alerts for a specific service."""
    import time

    from backend.core.duckdb import get_connection, get_source_for_service, log_cron_run, start_cron_run
    from backend.repositories import alerts as alert_repo

    start = time.monotonic()

    src = get_source_for_service(service_id)
    if not src:
        logger.warning("Could not find source for service_id %s", service_id)
        return

    task_name = "alerts"
    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[93m[alerts]\x1b[0m %s: Alerts evaluation job started.", _display)

    try:
        pass
    except Exception:
        pass

    # Fetch alerts from per-service metadata SQLite (no DuckDB needed).
    alerts = alert_repo.get_alerts(service_id=service_id)
    enabled_alerts = [a for a in alerts if a["enabled"]]
    # DuckDB connection is only needed if we actually have alerts to evaluate.
    con_ro = get_connection(src, read_only=True) if enabled_alerts else None

    if not enabled_alerts:
        from backend.core.duckdb import log_cron_run

        logger.info("🔔 \x1b[93m[alerts]\x1b[0m %s: No alerts configured, skipping.", _display)
        log_cron_run(src, task_name, time.monotonic() - start, "skipped", summary="No alerts configured")
        logger.info("⏹️  \x1b[93m[alerts]\x1b[0m %s: Alerts evaluation job finished.", _display)
        return
    run_id = None
    try:
        run_id = start_cron_run(src, task_name)
    except Exception as e:
        if con_ro is not None:
            con_ro.close()
        logger.debug("[scheduler] Could not start alerts evaluation for %s: %s", service_id, e)
        return

    try:
        s_name = _display_name(src, service_id)
        display_name = f"{s_name} ({service_id})" if s_name != service_id else service_id

        # (alert_id, webhook_url, payload, max_ts) for each alert that should fire
        triggered_items: list[tuple[str, str | None, dict | None, str | None]] = []

        for alert in enabled_alerts:
            try:
                fired, webhook_url, payload, max_ts = alert_repo.evaluate_alert(
                    con_ro, src, alert, display_name=display_name, service_id=service_id
                )
                if fired:
                    triggered_items.append((alert["id"], webhook_url, payload, max_ts))
                    logger.info("🚨  \x1b[93m[alerts]\x1b[0m %s: Alert triggered: %s", display_name, alert["name"])
            except Exception as e:
                logger.error(
                    "%s Failed to evaluate alert %s for %s: %s",
                    JOB_COLORS["alerts"] + "[alerts]" + RESET_COLOR,
                    alert["id"],
                    display_name,
                    e,
                )
    finally:
        if con_ro is not None:
            con_ro.close()

    try:
        # Second pass: write timestamps first, then dispatch webhooks so a crash
        # between the two doesn't cause duplicate notifications on the next run.
        if triggered_items:
            for alert_id, _, _, max_ts in triggered_items:
                alert_repo.update_last_triggered(service_id, alert_id, max_ts)

            # Export updated state before sending webhooks so the quiet-period
            # timestamp is durable even if a webhook call hangs or fails.
            from backend.state_sync import export_admin_state

            export_admin_state(service_id)

            import httpx

            for alert_id, webhook_url, payload, _ in triggered_items:
                if webhook_url and payload:
                    try:
                        httpx.post(webhook_url, json=payload, timeout=5)
                    except Exception as e:
                        logger.error(
                            "%s Failed to send webhook for alert %s: %s",
                            JOB_COLORS["alerts"] + "[alerts]" + RESET_COLOR,
                            alert_id,
                            e,
                        )

        n_eval = len(enabled_alerts)
        n_trig = len(triggered_items)
        summary = (
            f"Evaluated {n_eval} {'alert' if n_eval == 1 else 'alerts'}. "
            f"{n_trig} {'alert' if n_trig == 1 else 'alerts'} triggered."
        )

        log_cron_run(
            src,
            task_name,
            time.monotonic() - start,
            "success",
            summary=summary,
            files_downloaded=n_eval,
            rows_ingested=n_trig,
            run_id=run_id,
        )

    except Exception as e:
        import traceback

        err_msg = traceback.format_exc()
        logger.error(
            "%s Failed during alerts evaluation job for %s: %s\n%s",
            JOB_COLORS["alerts"] + "[alerts]" + RESET_COLOR,
            service_id,
            e,
            err_msg,
        )
        log_cron_run(
            src,
            task_name,
            time.monotonic() - start,
            "error",
            summary=f"Alerts evaluation failed: {e}",
            error_message=err_msg,
            files_downloaded=0,
            rows_ingested=0,
            run_id=run_id,
        )
    finally:
        if run_id is not None:
            try:
                from backend.core.duckdb import update_cron_duration

                update_cron_duration(src, run_id, time.monotonic() - start)
            except Exception:
                pass


@cron_task("metadata_cleanup")
def _run_metadata_cleanup(service_id: str) -> None:
    """Daily: trim usage_log + ingested_files + cron_runs per service retention cfg.

    Retention defaults to 1 day for usage_log/ingested_files, 7 days for
    cron_runs (see ``metadata_db.DEFAULT_METADATA_RETENTION``). Override
    per service via cfg["metadata_retention"]:

        {"metadata_retention": {"usage_log_days": 7, "ingested_files_days": 30,
                                "cron_runs_days": 30}}

    A value of 0 (or negative) disables cleanup for that table — useful for
    a long-retention analyst service that wants the full audit trail.

    VACUUM only runs when something was actually deleted. On a healthy
    daily cadence this means: first run trims everything older than
    retention, subsequent runs are mostly no-ops (only that day's
    just-aged rows to trim), and VACUUM happens cheaply on small deltas.

    Writes a row to the cron_runs audit table on completion so the run
    shows up on the Data Management cron schedule + history grid alongside
    the other tasks. The cron_runs row itself becomes part of the next
    cleanup's trimming target (capped at cron_runs_days retention).
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.metadata_db import cleanup_metadata

    src = get_source_for_service(service_id)
    if src is None:
        return

    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}

    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    color = JOB_COLORS.get("metadata_cleanup", "")
    label = f"{color}[metadata_cleanup]{RESET_COLOR}"
    logger.info("▶️  %s %s: Starting metadata cleanup.", label, _display)

    start_ts = time.time()
    run_id = start_cron_run(src, "metadata_cleanup")
    try:
        result = cleanup_metadata(service_id, retention)
    except Exception as e:
        logger.exception("%s %s: cleanup failed: %s", label, _display, e)
        log_cron_run(
            src,
            "metadata_cleanup",
            time.time() - start_ts,
            "error",
            error_message=str(e),
            summary=f"cleanup failed: {e}",
            run_id=run_id,
        )
        return

    total_deleted = sum(result["deleted"].values())
    summary_parts = [f"{t}={n}" for t, n in result["deleted"].items() if n]
    summary = (
        (
            f"Trimmed {total_deleted:,} rows ({', '.join(summary_parts)}). "
            f"VACUUM={'yes' if result['vacuumed'] else 'skipped (no deletions)'}."
        )
        if total_deleted
        else "No rows older than retention windows."
    )

    if total_deleted:
        logger.info(
            "🧹 %s %s: deleted %d rows (%s) vacuumed=%s in %.2fs",
            label,
            _display,
            total_deleted,
            ", ".join(summary_parts),
            result["vacuumed"],
            result["duration_s"],
        )
    else:
        logger.info("⏹️  %s %s: no rows to trim (took %.2fs)", label, _display, result["duration_s"])

    log_cron_run(
        src,
        "metadata_cleanup",
        time.time() - start_ts,
        "success",
        summary=summary,
        # Repurpose the rows_ingested column for the count of rows trimmed —
        # the schema is shared across all cron tasks, and "rows_ingested" is
        # the closest semantic fit (each task interprets it by context).
        rows_ingested=total_deleted,
        run_id=run_id,
    )
