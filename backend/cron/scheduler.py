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


def dev_mode_no_crons() -> bool:
    """Kill switch for local-dev runs: never schedule or execute cron work.

    Set ``FLA_DEV_NO_CRONS=1`` in the environment to:
     - skip registering every APScheduler job at startup EXCEPT a small
       allowlist of LOCAL-ONLY jobs (``local_compact``, ``rollup_compact``,
       ``rollup_heal``) that only rewrite the local parquet cache and never
       touch the shared FOS bucket or send anything outbound — see
       :meth:`Scheduler._register_dev_local_safe_jobs`
     - skip ``Scheduler.reload()`` re-registration
     - short-circuit ingest-class job entry points
       (``_run_service_cron``, ``_run_full_sweep``, ``_run_gap_heal``)
       so even an HTTP-triggered manual sync with ``force=True`` bails

    The allowlist exists because the blanket switch otherwise also kills
    local_compact, which keeps dashboard scans fast and is provably
    FOS-free (``optimize``/``commit``/``expire`` write FOS and stay gated).

    HTTP API calls are NOT affected — the user can still trigger
    ``/api/admin/rebuild-local-view`` to refresh the local Iceberg
    view from cloud (metadata-only sync, no raw-file ingestion).

    Why this exists: local dev runs against the same FOS bucket as
    prod, so any unintended ingestion races the prod cron. Per the
    ``dev-no-crons`` operating note, dev is meant to be a pure
    reader. ``provisioning.cron_*.enabled = false`` alone wasn't
    enough — the gap_heal cron triggered ``_run_full_sweep``
    unconditionally, the manual ``/admin/ingest-logs`` endpoint
    passed ``force=True``, and several jobs (local_compact,
    insights_prewarmer, metadata_cleanup) registered with no
    config gate at all. One env var beats N per-task disable flags
    and CAN'T be silently undone by an admin clicking "Save" in
    the cron settings modal.
    """
    return os.environ.get("FLA_DEV_NO_CRONS", "").lower() in ("1", "true", "yes")


def _display_name(src: dict, fallback: str) -> str:
    """Return src['service_name'] or src['name'], falling back to ``fallback``.
    Used by every cron-log site that wants the human-friendly name with
    the service id as fallback when the friendly name isn't populated."""
    return src.get("service_name") or src.get("name", fallback)


def _display_label(src: dict, service_id: str) -> str:
    """``"{display_name} ({service_id})"`` when the friendly name differs from
    the service id, otherwise just the service id. Every cron job that logs
    a start/end line builds this same 2-line `_svc_name = _display_name(...);
    _display = f"{_svc_name} ({sid})" if _svc_name != sid else sid` pair —
    folded here so the format lives in one place.
    """
    name = _display_name(src, service_id)
    return f"{name} ({service_id})" if name != service_id else service_id


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

        display_job_name = job_name
        if display_job_name == "sync":
            display_job_name = "log_discovery"
        elif display_job_name == "commit":
            display_job_name = "log_ingest"

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
        import os

        self.mode = os.environ.get("SCHEDULER_MODE", "inprocess")
        from apscheduler.schedulers.background import BackgroundScheduler

        self._sched = BackgroundScheduler(timezone=UTC)
        # Track per-service job IDs so we can replace them when settings change.
        self._job_ids: dict[str, str] = {}  # job_id -> job_id

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    # Jobs that go to RedBeat (Celery workers) in external mode: the
    # ledger/FOS family that never opens the per-service .duckdb file.
    # EVERYTHING ELSE stays on this process's APScheduler even in external
    # mode — rollup/compaction/alerts/view-refresh jobs read the pod-local
    # DuckDB file and cache, which is single-writer across processes: run
    # from a worker they either fight the backend's readers for the file
    # lock or (metric_snapshot) sample the wrong process's vitals. The
    # allowlist is deliberately tight so a NEW job defaults to backend-local
    # unless explicitly promoted to the worker fleet.
    _REDBEAT_JOB_PREFIXES = (
        "log_discovery_",
        "log_ingest_",
        "ledger_sweep_",
        "full_sync_",
        "gap_heal_",
        "rum_discovery_",
        "ledger_rum_sweep_",
    )

    def _routes_to_redbeat(self, job_id: str) -> bool:
        return self.mode == "external" and str(job_id).startswith(self._REDBEAT_JOB_PREFIXES)

    def start(self) -> None:
        if self.mode == "external":
            logger.info(
                "[scheduler] External mode: ledger/FOS jobs -> RedBeat (workers); "
                "pod-local jobs (rollups/compaction/alerts/snapshots) -> in-process APScheduler."
            )
            self._sync_jobs()
            self._sched.start()
            return
        """Start the scheduler and register jobs for all configured services."""
        if dev_mode_no_crons():
            logger.warning(
                "🚫 [scheduler] FLA_DEV_NO_CRONS=1 — skipping all FOS-writing / ingest / outbound crons "
                "(sync/full_sweep/gap_heal/commit/optimize/expire/ngwaf_sync/metadata_cleanup/"
                "bot_data_refresh/rdns/alerts). Registering ONLY local-safe jobs (local_compact, "
                "rollup_compact) that touch the local cache only. "
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

    def _register_alerts_evaluation_job(self, service_id: str, seconds: int, seen_ids: set[str]) -> None:
        """Register (or reschedule) the per-service alerts-evaluation cron
        job. Gated on having at least one alert configured — otherwise the
        cron just fires a "skipped" log every tick. Shared between the
        analyst (read-only) and admin paths in :func:`_sync_jobs`; the only
        per-path difference is the tick interval, so we take it as ``seconds``.
        """
        from backend.cron.jobs.metadata import _run_service_alerts_evaluation

        if not _service_has_alerts(service_id):
            return
        alert_job_id = f"alerts_evaluation_{service_id}"
        seen_ids.add(alert_job_id)
        if alert_job_id in self._job_ids:
            try:
                job = self._sched.get_job(alert_job_id)
                if job:
                    job.reschedule("interval", seconds=seconds)
            except Exception:
                pass
        else:
            self._add_job(
                _run_service_alerts_evaluation,
                "interval",
                seconds=seconds,
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
                seconds,
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
        from backend.cron.jobs.duckdb_recycle import run_duckdb_recycle

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
            )
            self._job_ids[recycle_id] = recycle_id
            logger.info(
                "♻️  \x1b[35m[recycle]\x1b[0m Registered DuckDB instance recycle job (every %gm).",
                recycle_interval,
            )

    def _register_dev_local_safe_jobs(self) -> None:
        """FLA_DEV_NO_CRONS allowlist: register ONLY the local-only jobs.

        ``local_compact`` + ``rollup_compact`` + ``rollup_heal`` rewrite
        the local parquet cache and never touch the shared FOS bucket or
        send anything outbound, so they're safe to run against a dev
        backend that reads the same FOS bucket as prod. Everything else
        stays gated off (see :func:`dev_mode_no_crons`); the FOS-writing
        jobs (``optimize``/``commit``/``expire``) are deliberately NOT here.

        Mirrors the trigger config of the same three jobs in
        :meth:`_sync_jobs` — keep them in sync. Runs once at startup;
        :meth:`reload` stays a no-op under the kill switch so a dev config
        save can't sneak the gated jobs back in.
        """
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.cron.jobs.compaction import _run_local_compact, _run_rollup_compact_daily, _run_rollup_hour_heal

        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id", "")
            if not service_id:
                continue
            src = get_source_for_service(service_id)
            if not src or not is_configured(src):
                continue

            # local_compact — local-only parquet compaction, always-on
            # (no config/access gate), every 2 min. Matches _sync_jobs.
            lc_job_id = f"local_compact_{service_id}"
            if lc_job_id not in self._job_ids:
                self._add_job(
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
                logger.info("⚙️  [scheduler] (dev-local) Registered %s (every 2 min, local-only).", lc_job_id)

            # rollup_compact — local-only per-day rollup compaction, daily
            # 02:00 UTC, gated on cron_compact.enabled + read-write.
            # Matches _sync_jobs.
            prov = cfg.get("provisioning", {})
            compact_cfg = prov.get("cron_compact", {})
            if compact_cfg.get("enabled", True) and prov.get("access_level") != "read_only":
                rc_job_id = f"rollup_compact_{service_id}"
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
                    logger.info("📦 [scheduler] (dev-local) Registered %s (daily 02:00 UTC).", rc_job_id)

                # rollup_heal — hourly hour-bundle self-heal, local-only
                # rollup writes (see _run_rollup_hour_heal). Matches
                # _sync_jobs.
                rh_job_id = f"rollup_heal_{service_id}"
                if rh_job_id not in self._job_ids:
                    self._add_job(
                        _run_rollup_hour_heal,
                        "cron",
                        minute=5,
                        args=[service_id],
                        id=rh_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=900,
                    )
                    self._job_ids[rh_job_id] = rh_job_id
                    logger.info("🩹 [scheduler] (dev-local) Registered %s (hourly at :05).", rh_job_id)

        # DuckDB instance recycle is local-safe (closes/reopens local conns,
        # no FOS writes) → safe under the dev kill switch. Still OFF unless
        # DUCKDB_RECYCLE_INTERVAL_MIN>0. No seen_ids here (dev path doesn't
        # stale-sweep).
        self._register_recycle_job()

    def _sync_jobs(self) -> None:
        """Read all service configs and add/update scheduled jobs."""
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.cron.jobs.commit import _run_log_ingest as _run_commit
        from backend.cron.jobs.compaction import _run_local_compact, _run_rollup_compact_daily, _run_rollup_hour_heal
        from backend.cron.jobs.expire import _run_expire_snapshots
        from backend.cron.jobs.insights_prewarmer import _run_insights_prewarmer
        from backend.cron.jobs.metadata import (
            _run_bot_data_refresh,
            _run_metadata_cleanup,
            _run_metadata_sync,
            _run_ngwaf_bot_sync,
            _run_rdns_enrichment,
            _run_share_audit_purge,
        )
        from backend.cron.jobs.optimize import _run_optimize
        from backend.cron.jobs.rum_commit import _run_rum_commit
        from backend.cron.jobs.rum_sync import _run_rum_sync
        from backend.cron.jobs.sync import _run_full_sweep, _run_gap_heal
        from backend.cron.jobs.sync import _run_log_discovery_cron as _run_service_cron

        configs = svcconfig.list_configs()
        seen_ids: set[str] = set()

        if self.mode == "external":
            # RedBeat entry.save() is an upsert, so re-registering every
            # redbeat-routed job on each reload is cheap and is what lets an
            # interval change from a config edit take effect. ONLY the
            # redbeat-routed ids are cleared — the pod-local jobs live on the
            # real APScheduler where re-adding an existing id raises, and its
            # reschedule path below handles their interval changes.
            for jid in [j for j in self._job_ids if str(j).startswith(self._REDBEAT_JOB_PREFIXES)]:
                del self._job_ids[jid]

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
                    self._add_job(
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
                # When the user adds an alert, the alerts router calls
                # scheduler.reload() to register the job; deleting the last
                # alert lets the cleanup loop unregister it on the next sync.
                self._register_alerts_evaluation_job(service_id, interval_seconds, seen_ids)

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
            commit_job_id = f"log_ingest_{service_id}"
            seen_ids.add(commit_job_id)

            if commit_job_id in self._job_ids:
                try:
                    job = self._sched.get_job(commit_job_id)
                    if job:
                        job.reschedule("interval", minutes=commit_interval_mins)
                except Exception:
                    pass
            else:
                self._add_job(
                    _run_commit,
                    "interval",
                    minutes=commit_interval_mins,
                    jitter=30,
                    args=[service_id],
                    id=commit_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=commit_interval_mins * 60,
                )
                self._job_ids[commit_job_id] = commit_job_id
                logger.info(
                    "📦 [scheduler] Registered log_ingest job %s (every %dm).",
                    commit_job_id,
                    commit_interval_mins,
                )

            # ── Ledger sweep (celery-mode crash net) ───────────────────────────
            # Reclaims stale ingest_ledger claims, re-dispatches stuck rows, and
            # diffs a lookback FOS LIST. Its own schedule entry (every 15 min):
            # the previous `now.minute % 15 == 0` gate inside the discovery tick
            # fired zero-or-multiple times depending on interval alignment.
            if svcconfig.INGEST_MODE == "celery":
                from backend.cron.jobs.sync import _run_ledger_sweep

                sweep_job_id = f"ledger_sweep_{service_id}"
                seen_ids.add(sweep_job_id)
                if sweep_job_id not in self._job_ids:
                    self._add_job(
                        _run_ledger_sweep,
                        "interval",
                        minutes=15,
                        args=[service_id],
                        id=sweep_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=300,
                    )
                    self._job_ids[sweep_job_id] = sweep_job_id
                    logger.info("🧹 [scheduler] Registered ledger_sweep job %s (every 15m).", sweep_job_id)

            # ── RUM ingest jobs (ingest RUM beacons from FOS) ──────────────────
            # Only register if RUM is enabled for this service. Celery mode
            # gets the ledger-based, per-file-fanout jobs (mirroring
            # log_discovery_/ledger_sweep_ above); non-celery mode keeps the
            # original single-job-per-service rum_sync_{id}/rum_commit_{id}
            # pair completely unchanged. The two pipelines must never both
            # be registered for the same service — they'd double-ingest
            # into the same DuckLake client_vitals/client_errors tables via
            # independent dedup registries that don't know about each other.
            rum_cfg = cfg.get("rum", {})
            rum_enabled = bool(cfg.get("rum_enabled", False) or rum_cfg.get("enabled", False))
            if rum_enabled and svcconfig.INGEST_MODE == "celery":
                from backend.cron.jobs.rum_ledger import _run_rum_discovery_cron, _run_rum_ledger_sweep

                rum_disc_interval_secs = max(5, int(rum_cfg.get("sync_interval_seconds", interval_seconds)))
                rum_disc_job_id = f"rum_discovery_{service_id}"
                seen_ids.add(rum_disc_job_id)
                if rum_disc_job_id not in self._job_ids:
                    self._add_job(
                        _run_rum_discovery_cron,
                        "interval",
                        seconds=rum_disc_interval_secs,
                        args=[service_id],
                        id=rum_disc_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=60,
                    )
                    self._job_ids[rum_disc_job_id] = rum_disc_job_id
                    logger.info(
                        "[scheduler] Registered RUM discovery job %s (every %ds).",
                        rum_disc_job_id,
                        rum_disc_interval_secs,
                    )

                rum_sweep_job_id = f"ledger_rum_sweep_{service_id}"
                seen_ids.add(rum_sweep_job_id)
                if rum_sweep_job_id not in self._job_ids:
                    self._add_job(
                        _run_rum_ledger_sweep,
                        "interval",
                        minutes=15,
                        args=[service_id],
                        id=rum_sweep_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=300,
                    )
                    self._job_ids[rum_sweep_job_id] = rum_sweep_job_id
                    logger.info("🧹 [scheduler] Registered ledger_rum_sweep job %s (every 15m).", rum_sweep_job_id)
            elif rum_enabled:
                rum_sync_interval_secs = max(5, int(rum_cfg.get("sync_interval_seconds", interval_seconds)))
                rum_sync_job_id = f"rum_sync_{service_id}"
                seen_ids.add(rum_sync_job_id)

                if rum_sync_job_id in self._job_ids:
                    try:
                        job = self._sched.get_job(rum_sync_job_id)
                        if job:
                            job.reschedule("interval", seconds=rum_sync_interval_secs)
                    except Exception:
                        pass
                else:
                    self._add_job(
                        _run_rum_sync,
                        "interval",
                        seconds=rum_sync_interval_secs,
                        args=[service_id],
                        id=rum_sync_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=60,
                    )
                    self._job_ids[rum_sync_job_id] = rum_sync_job_id
                    logger.info(
                        "[scheduler] Registered RUM sync job %s (every %ds).", rum_sync_job_id, rum_sync_interval_secs
                    )

                # ── RUM commit job (compact RUM tables) ──────────────────────
                rum_commit_interval_mins = max(1, int(rum_cfg.get("commit_interval_mins", commit_interval_mins)))
                rum_commit_job_id = f"rum_commit_{service_id}"
                seen_ids.add(rum_commit_job_id)

                if rum_commit_job_id in self._job_ids:
                    try:
                        job = self._sched.get_job(rum_commit_job_id)
                        if job:
                            job.reschedule("interval", minutes=rum_commit_interval_mins)
                    except Exception:
                        pass
                else:
                    self._add_job(
                        _run_rum_commit,
                        "interval",
                        minutes=rum_commit_interval_mins,
                        args=[service_id],
                        id=rum_commit_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=60,
                    )
                    self._job_ids[rum_commit_job_id] = rum_commit_job_id
                    logger.info(
                        "[scheduler] Registered RUM commit job %s (every %dm).",
                        rum_commit_job_id,
                        rum_commit_interval_mins,
                    )

            # ── Alerts evaluation job (Per Service) ───────────────────────────
            # See note above (analyst branch) on the no-alerts gate.
            self._register_alerts_evaluation_job(service_id, log_period, seen_ids)

            # ── Daily full-LIST sweep (catches late-arriving files) ───────────
            full_sweep_cfg = prov.get("cron_full_sweep", {})
            if full_sweep_cfg.get("enabled", True):
                full_job_id = f"full_sync_{service_id}"
                seen_ids.add(full_job_id)
                if full_job_id not in self._job_ids:
                    self._add_job(
                        _run_full_sweep,
                        "cron",
                        hour=3,
                        minute=30,  # 03:30 UTC — runs before optimize (04:00)
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
                    self._add_job(
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
                self._add_job(
                    _run_local_compact,
                    "interval",
                    minutes=2,
                    jitter=15,
                    args=[service_id],
                    id=lc_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=120,
                )
                self._job_ids[lc_job_id] = lc_job_id
                logger.info("⚙️  [scheduler] Registered local_compact job %s (every 2 min, local-only).", lc_job_id)

            # ── Insights cache prewarmer (perf #76) ───────────────────────────
            # 240 s cadence — just under the 300 s INSIGHTS_CACHE_TTL so the
            # default (window=1h, baseline=168h) selection never expires
            # between prewarmer ticks. Runs for both admin and analyst
            # services since the insights tab is visible to both.
            ip_job_id = f"insights_prewarmer_{service_id}"
            seen_ids.add(ip_job_id)
            if ip_job_id not in self._job_ids:
                self._add_job(
                    _run_insights_prewarmer,
                    "interval",
                    seconds=240,
                    jitter=15,
                    args=[service_id],
                    id=ip_job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=360,
                )
                self._job_ids[ip_job_id] = ip_job_id
                logger.info(
                    "🔥 [scheduler] Registered insights_prewarmer job %s (every 240s).",
                    ip_job_id,
                )

            # ── Daily rollup compaction (per-day parquet from per-hour) ────
            # 02:00 UTC — runs before optimize (04:00) so per-day rollups
            # are ready when the next day's queries start. Only for
            # read-write services that own the rollup data.
            if compact_cfg.get("enabled", True) and prov.get("access_level") != "read_only":
                rc_job_id = f"rollup_compact_{service_id}"
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
                        "📦 [scheduler] Registered rollup compaction job %s (daily 02:00 UTC).",
                        rc_job_id,
                    )

                # ── Hourly hour-bundle self-heal ───────────────────────────
                # :05 past each hour (Fastly log delivery lags a few
                # minutes, so the just-closed hour's rows have landed by
                # then). Rebuilds bundles for closed hours the per-sync
                # recompute missed — on bursty services nothing straddles
                # the hour boundary, so without this every closed hour of
                # the current day is silently absent from top-N until the
                # 02:00 deep pass. Same gate as rollup_compact (local-only
                # rollup writes, read-write services only).
                rh_job_id = f"rollup_heal_{service_id}"
                seen_ids.add(rh_job_id)
                if rh_job_id not in self._job_ids:
                    self._add_job(
                        _run_rollup_hour_heal,
                        "cron",
                        minute=5,
                        args=[service_id],
                        id=rh_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=900,
                    )
                    self._job_ids[rh_job_id] = rh_job_id
                    logger.info(
                        "🩹 [scheduler] Registered rollup hour-heal job %s (hourly at :05).",
                        rh_job_id,
                    )

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
            if compact_cfg.get("enabled", True):
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
                if ngwaf_job_id in self._job_ids:
                    try:
                        job = self._sched.get_job(ngwaf_job_id)
                        if job:
                            job.reschedule("interval", minutes=ngwaf_interval_mins)
                    except Exception:
                        pass
                else:
                    self._add_job(
                        _run_ngwaf_bot_sync,
                        "interval",
                        minutes=ngwaf_interval_mins,
                        jitter=15,
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
            # Daily 03:15 UTC. Runs before optimize (04:00) and after full_sweep
            # (03:30) so the daily admin cron window stays single-threaded
            # across heavy phases. Trims usage_log + ingested_files
            # + cron_runs per cfg["metadata_retention"]; defaults to 1d for
            # the first two and 7d for cron_runs. See
            # backend.core.metadata_db.cleanup_metadata.
            cleanup_job_id = f"metadata_cleanup_{service_id}"
            seen_ids.add(cleanup_job_id)
            if cleanup_job_id not in self._job_ids:
                self._add_job(
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
            logger.info("🧹 \x1b[35m[share_audit_purge]\x1b[0m Registered share audit purge job (daily 03:45 UTC).")

        # ── Operational-vital snapshots (Trends tab + System Health sparklines) ──
        from backend.cron.jobs.metric_snapshot import _run_metric_snapshot

        snapshot_id = "metric_snapshot"
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

        # DuckDB instance recycle (object-cache leak guard) — local-safe, so it
        # registers in both this path and the dev-local-safe path.
        self._register_recycle_job(seen_ids)

        # Cleanup
        if self.mode == "external":
            # Sweep Redis itself, not just this process's _job_ids: RedBeat
            # entries persist across restarts, so a renamed/removed/relocated
            # job otherwise keeps firing forever (observed as a KeyError storm
            # in the worker after the sync→log_discovery rename). An entry is
            # stale if it isn't a currently-seen id OR isn't redbeat-routed at
            # all (a pod-local job left behind in Redis from before the
            # backend-local/worker split). Leave celery's internal entries
            # (e.g. celery.backend_cleanup) alone.
            from redbeat import RedBeatSchedulerEntry

            from backend.celery_app import app
            from backend.celery_status import redbeat_schedule_entries

            for stale in redbeat_schedule_entries():
                name = stale["name"]
                if name.startswith("celery."):
                    continue
                if name in seen_ids and self._routes_to_redbeat(name):
                    continue
                try:
                    RedBeatSchedulerEntry.from_key(f"redbeat:{name}", app=app).delete()
                    logger.info("[scheduler] Removed stale RedBeat entry %s (task=%s).", name, stale.get("task"))
                except Exception as e:
                    logger.warning("[scheduler] Failed to remove stale RedBeat entry %s: %s", name, e)
                if self._routes_to_redbeat(name):
                    self._job_ids.pop(name, None)

        # Pod-local jobs (all jobs in inprocess mode; the non-redbeat family
        # in external mode) are removed from the live APScheduler when their
        # service/config disappears.
        for jid in list(self._job_ids.keys()):
            if jid in seen_ids or self._routes_to_redbeat(jid):
                continue
            try:
                self._sched.remove_job(jid)
            except Exception:
                pass
            logger.info("[scheduler] Removed stale job %s.", jid)
            del self._job_ids[jid]

    def _add_job(self, func, trigger=None, **kwargs):
        job_id = kwargs.pop("id", None)
        args = kwargs.pop("args", [])

        if self.mode == "external" and not self._routes_to_redbeat(job_id):
            # Pod-local job in external mode: schedule on this process's
            # APScheduler exactly like inprocess mode (see the
            # _REDBEAT_JOB_PREFIXES note above for why the split exists).
            self._sched.add_job(func, trigger, id=job_id, args=args, **kwargs)
            self._job_ids[job_id] = job_id
            return

        if self.mode == "external":
            from celery.schedules import crontab as celery_crontab
            from celery.schedules import schedule as celery_schedule
            from redbeat import RedBeatSchedulerEntry

            from backend.celery_app import app

            celery_task = getattr(func, "celery_task", None)
            if celery_task is None:
                # Scheduling an unregistered name makes beat fire KeyErrors
                # forever with zero work done — refuse loudly instead.
                logger.error(
                    "[scheduler] Cannot schedule %s.%s in external mode: it has no "
                    "registered Celery task (wrap it with @cron_task/@global_job or "
                    "attach .celery_task). Job %s NOT scheduled.",
                    func.__module__,
                    func.__name__,
                    job_id,
                )
                return
            task_name = celery_task.name

            if trigger == "interval":
                secs = kwargs.get("seconds", 0) + kwargs.get("minutes", 0) * 60 + kwargs.get("hours", 0) * 3600
                schedule = celery_schedule(run_every=secs)
            elif trigger == "cron":
                # APScheduler's cron trigger defaults unspecified lower-order
                # fields to their MINIMUM (hour=2 ⇒ minute 0, once daily);
                # celery's crontab defaults minute='*' (hour=2 ⇒ 60 runs/hour).
                # Mirror APScheduler so daily jobs stay daily.
                h = kwargs.get("hour", "*")
                m = kwargs.get("minute", 0 if "hour" in kwargs else "*")
                dow = kwargs.get("day_of_week", "*")
                schedule = celery_crontab(minute=m, hour=h, day_of_week=dow)
            else:
                schedule = celery_schedule(run_every=60)

            # entry.save() is an upsert keyed by name, so re-registering an
            # existing job updates its schedule in place (interval changes
            # from a config edit take effect on the next reload).
            entry = RedBeatSchedulerEntry(job_id, task_name, schedule, args=args, app=app)
            entry.save()
            self._job_ids[job_id] = job_id
        else:
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
        """Return the APScheduler Job object for a given job ID, or None.

        RedBeat-routed jobs have no APScheduler object (their reschedule
        happens via the upsert in ``_add_job``); pod-local jobs resolve
        normally in both modes.
        """
        if self._routes_to_redbeat(job_id):
            return None
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
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("cron.scheduler._last_heavy_refresh", _last_heavy_refresh)
