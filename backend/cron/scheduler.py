"""APScheduler lifecycle + cron-job registration.

This is the post-carve home for the ``Scheduler`` class, the
``get_scheduler`` factory, the global singleton, and the small helpers
(logger, color/icon tables, throttle dicts, disk/backlog probes) that
are shared across job modules. The legacy ``backend/scheduler.py`` is
now a thin shim that re-exports the public surface — see that module's
docstring for the back-compat story.
"""

from __future__ import annotations

import logging

logging.getLogger("pyiceberg.io").setLevel(logging.WARNING)
import os
import sys
import threading
import time
from datetime import UTC

# Anchor the logger to the historical ``backend.scheduler`` name so
# log filters in tests (and downstream parsers) keep working after
# the carve. Every job module also imports this logger.
logger = logging.getLogger("backend.scheduler")


# ── Helpers / shared state ────────────────────────────────────────────────────


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
        # Resolve through the ``backend.scheduler`` shim so tests that
        # ``patch("backend.scheduler.logger")`` continue to intercept these
        # calls. The helper falls back to the module-local logger if the
        # shim is not yet (or no longer) importable, which keeps unit
        # tests for this module isolated from the shim layer.
        from backend.cron.jobs._common import shim_attr

        log = shim_attr("logger", logger)
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


# ── Scheduler class + global singleton ────────────────────────────────────────


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
        from backend.cron.jobs.metadata import _run_metadata_sync

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
        from backend.cron.jobs.commit import _run_commit
        from backend.cron.jobs.compaction import _run_local_compact, _run_rollup_compact_daily
        from backend.cron.jobs.expire import _run_expire_snapshots
        from backend.cron.jobs.metadata import (
            _run_bot_data_refresh,
            _run_metadata_cleanup,
            _run_metadata_sync,
            _run_ngwaf_bot_sync,
            _run_rdns_enrichment,
            _run_service_alerts_evaluation,
            _run_share_audit_purge,
        )
        from backend.cron.jobs.optimize import _run_optimize
        from backend.cron.jobs.sync import _run_full_sweep, _run_gap_heal, _run_service_cron

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

            # ── Daily rollup compaction (per-day parquet from per-hour) ────
            # 02:00 UTC — runs before optimize (03:00) so per-day rollups
            # are ready when the next day's queries start. Only for
            # read-write services that own the rollup data.
            if compact_cfg.get("enabled", True) and prov.get("access_level") != "read_only":
                rc_job_id = f"rollup_compact_{service_id}"
                seen_ids.add(rc_job_id)
                if rc_job_id not in self._job_ids:
                    self._sched.add_job(
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
                        "📦 [scheduler] Registered rollup compaction job %s (daily 02:00 UTC).",
                        rc_job_id,
                    )

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
