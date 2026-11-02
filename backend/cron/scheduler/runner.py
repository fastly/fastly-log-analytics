"""APScheduler runner, job registration, and schedule payload builder."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

from backend.cron.scheduler.lifecycle import (
    _resolve_rum_sync_interval,
    _service_has_alerts,
    dev_local_crons_enabled,
    dev_mode_no_crons,
    partial_hour_merge_enabled,
    partial_hour_merge_interval_sec,
)

logger = logging.getLogger("backend.scheduler")


class Scheduler:
    def __init__(self) -> None:
        from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor
        from apscheduler.schedulers.background import BackgroundScheduler

        executors = {
            "default": APSThreadPoolExecutor(10),
            "streaming": APSThreadPoolExecutor(10),
            "maintenance": APSThreadPoolExecutor(3),
        }
        self._sched = BackgroundScheduler(timezone=UTC, executors=executors)
        self._job_ids: dict[str, str] = {}  # job_id -> job_id

    def start(self) -> None:
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
        self._sync_jobs()
        self._sched.start()
        logger.info("🟢 [scheduler] Started (pid: %d). %d job(s) registered.", os.getpid(), len(self._job_ids))
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
        try:
            self._sched.shutdown(wait=wait)
        except Exception:
            pass
        logger.info("[scheduler] Stopped.")

    def _register_once_job(
        self,
        job_id: str,
        func,
        trigger: str,
        seen_ids: set[str] | None = None,
        log_msg: str | None = None,
        *log_args,
        **kwargs,
    ) -> bool:
        if seen_ids is not None:
            seen_ids.add(job_id)
        if job_id in self._job_ids:
            return False
        self._add_job(func, trigger, id=job_id, max_instances=1, coalesce=True, **kwargs)
        self._job_ids[job_id] = job_id
        if log_msg:
            logger.info(log_msg, *log_args)
        return True

    def _register_reschedulable_job(
        self,
        job_id: str,
        func,
        trigger: str,
        seen_ids: set[str] | None = None,
        log_msg: str | None = None,
        *log_args,
        reschedule_kwargs: dict | None = None,
        max_instances: int = 1,
        **kwargs,
    ) -> bool:
        if seen_ids is not None:
            seen_ids.add(job_id)
        add_kw = {"id": job_id, "coalesce": True, **kwargs}
        if max_instances:
            add_kw["max_instances"] = max_instances
        added = self._reschedule_or_add_job(func, trigger, reschedule_kwargs=reschedule_kwargs, **add_kw)
        if added and log_msg:
            logger.info(log_msg, *log_args)
        return added

    def _register_alerts_evaluation_job(
        self, service_id: str, seconds: int, seen_ids: set[str], enabled: bool = True
    ) -> None:
        if not enabled or not _service_has_alerts(service_id):
            return
        from backend.cron.jobs.metadata import _run_service_alerts_evaluation

        alert_job_id = f"alerts_evaluation_{service_id}"
        self._register_reschedulable_job(
            alert_job_id,
            _run_service_alerts_evaluation,
            "interval",
            seen_ids,
            "🔔 [scheduler] Registered alerts evaluation job %s (every %ds).",
            alert_job_id,
            seconds,
            seconds=seconds,
            args=[service_id],
            misfire_grace_time=60,
        )

    def _register_insights_prewarmer_job(self, service_id: str, seen_ids: set[str], prov: dict) -> None:
        from backend.cron.jobs.insights_prewarmer import _run_insights_prewarmer

        cron_ip = prov.get("cron_insights_prewarmer", {})
        if not cron_ip.get("enabled", True):
            return
        interval_seconds = int(cron_ip.get("interval_seconds", 240))
        ip_job_id = f"insights_prewarmer_{service_id}"
        self._register_reschedulable_job(
            ip_job_id,
            _run_insights_prewarmer,
            "interval",
            seen_ids,
            "🔥 [scheduler] Registered insights_prewarmer job %s (every %ds).",
            ip_job_id,
            interval_seconds,
            seconds=interval_seconds,
            jitter=15,
            reschedule_kwargs={"seconds": interval_seconds, "jitter": 15},
            args=[service_id],
            misfire_grace_time=360,
        )

    def _register_recycle_job(self, seen_ids: set[str] | None = None) -> None:
        from backend.cron.jobs.duckdb_recycle import recycle_interval_min
        from backend.cron.jobs.metadata import run_duckdb_recycle

        recycle_interval = recycle_interval_min()
        if recycle_interval <= 0:
            return
        self._register_once_job(
            "duckdb_recycle",
            run_duckdb_recycle,
            "interval",
            seen_ids,
            "♻️  \x1b[35m[recycle]\x1b[0m Registered DuckDB instance recycle job (every %gm, executor=maintenance).",
            recycle_interval,
            minutes=recycle_interval,
            misfire_grace_time=120,
            executor="maintenance",
        )

    def _register_metric_snapshot_job(self, seen_ids: set[str] | None = None) -> None:
        from backend.cron.jobs.metric_snapshots import _run_metric_snapshot

        self._register_once_job(
            "metric_snapshot",
            _run_metric_snapshot,
            "interval",
            seen_ids,
            "📈 \x1b[34m[metric_snapshot]\x1b[0m Registered metric snapshot job (every 60s).",
            seconds=60,
            jitter=5,
            misfire_grace_time=60,
        )

    def _sync_local_compaction_jobs(
        self,
        service_id: str,
        cfg: dict,
        seen_ids: set[str] | None = None,
        *,
        dev_local: bool = False,
    ) -> None:
        from backend.cron.jobs.metadata import (
            _run_local_compact,
            _run_partial_hour_merge,
            _run_rollup_compact_daily,
            _run_rollup_hour_heal,
        )

        prov = cfg.get("provisioning", {})
        compact_cfg = prov.get("cron_compact", {})
        tag = "(dev-local) " if dev_local else ""
        lc_job_id = f"local_compact_{service_id}"
        lc_interval_mins = max(1, int(os.environ.get("LOCAL_COMPACT_INTERVAL_MIN", "2")))
        self._register_once_job(
            lc_job_id,
            _run_local_compact,
            "interval",
            seen_ids,
            "⚙️  [scheduler] %sRegistered %s%s (every %d min, local-only).",
            tag,
            "" if dev_local else "local_compact job ",
            lc_job_id,
            lc_interval_mins,
            minutes=lc_interval_mins,
            jitter=10,
            args=[service_id],
            misfire_grace_time=60,
        )
        if partial_hour_merge_enabled():
            ph_job_id = f"partial_hour_merge_{service_id}"
            ph_interval_sec = partial_hour_merge_interval_sec()
            self._register_once_job(
                ph_job_id,
                _run_partial_hour_merge,
                "interval",
                seen_ids,
                "⚡ [scheduler] %sRegistered %s%s (every %ds, local-only).",
                tag,
                "" if dev_local else "partial_hour_merge job ",
                ph_job_id,
                ph_interval_sec,
                seconds=ph_interval_sec,
                jitter=5,
                args=[service_id],
                misfire_grace_time=60,
            )
        if compact_cfg.get("enabled", True):
            rc_job_id = f"rollup_compact_{service_id}"
            self._register_once_job(
                rc_job_id,
                _run_rollup_compact_daily,
                "cron",
                seen_ids,
                "📦 [scheduler] %sRegistered %s%s (daily 02:00 UTC).",
                tag,
                "" if dev_local else "rollup compaction job ",
                rc_job_id,
                hour=2,
                minute=0,
                args=[service_id],
                misfire_grace_time=3600,
            )
            rh_job_id = f"rollup_heal_{service_id}"
            self._register_once_job(
                rh_job_id,
                _run_rollup_hour_heal,
                "cron",
                seen_ids,
                "🩹 [scheduler] %sRegistered %s%s (hourly at :05).",
                tag,
                "" if dev_local else "rollup hour-heal job ",
                rh_job_id,
                minute=5,
                next_run_time=datetime.now(UTC) + timedelta(seconds=5),
                args=[service_id],
                misfire_grace_time=900,
            )

    def _register_dev_local_safe_jobs(self) -> None:
        if not dev_local_crons_enabled():
            logger.warning("🚫 [scheduler] FLA_DEV_LOCAL_CRONS=0 — no dev-local-safe jobs registered.")
            return
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.high_scale.ingest_controller import get_high_scale_service_registry

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
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.cron.jobs.metadata import (
            _run_bot_data_refresh,
            _run_expire_snapshots,
            _run_metadata_cleanup,
            _run_ngwaf_bot_sync,
            _run_optimize,
            _run_rdns_enrichment,
            _run_share_audit_purge,
        )
        from backend.cron.jobs.sync import _run_commit, _run_full_sweep, _run_gap_heal, _run_rum_commit, _run_rum_sync
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
            if sync_cfg.get("interval_mins"):
                interval_seconds = max(5, int(sync_cfg["interval_mins"]) * 60)
            elif sync_cfg.get("interval_seconds"):
                interval_seconds = max(5, int(sync_cfg["interval_seconds"]))
            else:
                interval_seconds = max(5, log_period // 2 if log_period >= 60 else log_period)
            commit_interval_mins = max(1, int(sync_cfg.get("commit_interval_mins", 5)))
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
                self._add_job(
                    _run_service_cron,
                    "interval",
                    seconds=interval_seconds,
                    jitter=None if interval_seconds <= 15 else 2,
                    start_date=None,
                    args=[service_id],
                    id=job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                )
                self._job_ids[job_id] = job_id
                logger.info("🔄 [scheduler] Registered log_discovery job %s (every %ds).", job_id, interval_seconds)
            commit_job_id = f"commit_{service_id}"
            self._register_reschedulable_job(
                commit_job_id,
                _run_commit,
                "interval",
                seen_ids,
                "📦 [scheduler] Registered commit job %s (every %dm).",
                commit_job_id,
                commit_interval_mins,
                minutes=commit_interval_mins,
                jitter=30,
                args=[service_id],
                misfire_grace_time=commit_interval_mins * 60,
            )
            if svcconfig.is_high_scale_mode(src):
                from backend.cron.jobs.sync import _run_ledger_sweep

                sweep_cfg = prov.get("cron_ledger_sweep", {})
                if sweep_cfg.get("enabled", True):
                    sweep_interval_mins = max(1, int(sweep_cfg.get("interval_minutes", 15)))
                    sweep_job_id = f"ledger_sweep_{service_id}"
                    self._register_reschedulable_job(
                        sweep_job_id,
                        _run_ledger_sweep,
                        "interval",
                        seen_ids,
                        "🧹 [scheduler] Registered ledger_sweep job %s (every %dm).",
                        sweep_job_id,
                        sweep_interval_mins,
                        minutes=sweep_interval_mins,
                        args=[service_id],
                        misfire_grace_time=300,
                    )
            rum_cfg = cfg.get("rum")
            rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
            rum_enabled = svcconfig.is_rum_enabled(cfg)
            if rum_enabled and svcconfig.is_high_scale_mode(src):
                from backend.cron.jobs.sync import _run_rum_discovery_cron, _run_rum_ledger_sweep

                rum_disc_interval_secs = _resolve_rum_sync_interval(rum_cfg, cfg, interval_seconds)
                rum_disc_job_id = f"rum_discovery_{service_id}"
                self._register_reschedulable_job(
                    rum_disc_job_id,
                    _run_rum_discovery_cron,
                    "interval",
                    seen_ids,
                    "[scheduler] Registered RUM discovery job %s (every %ds).",
                    rum_disc_job_id,
                    rum_disc_interval_secs,
                    seconds=rum_disc_interval_secs,
                    args=[service_id],
                    misfire_grace_time=60,
                )
                rum_sweep_cfg = prov.get("cron_ledger_rum_sweep") or prov.get("cron_rum_ledger_sweep") or {}
                if rum_sweep_cfg.get("enabled", True):
                    rum_sweep_interval_mins = max(1, int(rum_sweep_cfg.get("interval_minutes", 15)))
                    rum_sweep_job_id = f"ledger_rum_sweep_{service_id}"
                    self._register_reschedulable_job(
                        rum_sweep_job_id,
                        _run_rum_ledger_sweep,
                        "interval",
                        seen_ids,
                        "🧹 [scheduler] Registered ledger_rum_sweep job %s (every %dm).",
                        rum_sweep_job_id,
                        rum_sweep_interval_mins,
                        minutes=rum_sweep_interval_mins,
                        args=[service_id],
                        misfire_grace_time=300,
                    )
            elif rum_enabled:
                rum_sync_interval_secs = _resolve_rum_sync_interval(rum_cfg, cfg, interval_seconds)
                rum_sync_job_id = f"rum_sync_{service_id}"
                self._register_reschedulable_job(
                    rum_sync_job_id,
                    _run_rum_sync,
                    "interval",
                    seen_ids,
                    "[scheduler] Registered RUM sync job %s (every %ds).",
                    rum_sync_job_id,
                    rum_sync_interval_secs,
                    seconds=rum_sync_interval_secs,
                    args=[service_id],
                    misfire_grace_time=60,
                )
                rum_commit_interval_mins = max(1, int(rum_cfg.get("commit_interval_mins", 1)))
                rum_commit_job_id = f"rum_commit_{service_id}"
                self._register_reschedulable_job(
                    rum_commit_job_id,
                    _run_rum_commit,
                    "interval",
                    seen_ids,
                    "[scheduler] Registered RUM commit job %s (every %dm).",
                    rum_commit_job_id,
                    rum_commit_interval_mins,
                    minutes=rum_commit_interval_mins,
                    args=[service_id],
                    misfire_grace_time=60,
                )
            alerts_cfg = prov.get("cron_alerts", {})
            alerts_enabled = alerts_cfg.get("enabled", True)
            alerts_seconds = int(alerts_cfg.get("interval_seconds") or log_period)
            self._register_alerts_evaluation_job(service_id, alerts_seconds, seen_ids, enabled=alerts_enabled)
            full_sweep_cfg = prov.get("cron_full_sweep", {})
            if full_sweep_cfg.get("enabled", True):
                full_job_id = f"full_sync_{service_id}"
                cron_hours = str(full_sweep_cfg.get("cron_hours", "3,9,15,21"))
                cron_minute = int(full_sweep_cfg.get("cron_minute", 30))
                self._register_reschedulable_job(
                    full_job_id,
                    _run_full_sweep,
                    "cron",
                    seen_ids,
                    "🔍 [scheduler] Registered full-sweep job %s (hours: %s at :%02d UTC).",
                    full_job_id,
                    cron_hours,
                    cron_minute,
                    hour=cron_hours,
                    minute=cron_minute,
                    args=[service_id],
                    misfire_grace_time=3600,
                )
            heal_cfg = prov.get("cron_gap_heal", {})
            has_logging_svc = bool(cfg.get("logging_service_id") or cfg.get("service_id"))
            if heal_cfg.get("enabled", True) and has_logging_svc:
                heal_job_id = f"gap_heal_{service_id}"
                interval_mins = int(heal_cfg.get("interval_minutes", 30))
                self._register_reschedulable_job(
                    heal_job_id,
                    _run_gap_heal,
                    "interval",
                    seen_ids,
                    "🩹 [scheduler] Registered gap-heal job %s (every %d min).",
                    heal_job_id,
                    interval_mins,
                    minutes=interval_mins,
                    args=[service_id],
                    misfire_grace_time=600,
                )
            compact_cfg = prov.get("cron_compact", {})
            if compact_cfg.get("enabled", True):
                opt_job_id = f"optimize_{service_id}"
                self._register_once_job(
                    opt_job_id,
                    _run_optimize,
                    "cron",
                    seen_ids,
                    "⚙️  [scheduler] Registered optimize job %s (daily 04:00 UTC). Local compact handles ongoing dashboard perf — this is just FOS-side housekeeping.",
                    opt_job_id,
                    hour=4,
                    minute=0,
                    args=[service_id],
                    misfire_grace_time=3600,
                )
            self._sync_local_compaction_jobs(service_id, cfg, seen_ids=seen_ids, dev_local=False)
            self._register_insights_prewarmer_job(service_id, seen_ids, prov)
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
            if svcconfig.get_ngwaf_workspace_id(service_id):
                ngwaf_interval_mins = max(1, int(prov.get("cron_ngwaf", {}).get("interval_mins", 5)))
                ngwaf_job_id = f"ngwaf_sync_{service_id}"
                self._register_reschedulable_job(
                    ngwaf_job_id,
                    _run_ngwaf_bot_sync,
                    "interval",
                    seen_ids,
                    "👾 \x1b[36m[ngwaf_sync]\x1b[0m Registered NGWAF bot sync job %s (every %dm, executor=maintenance).",
                    ngwaf_job_id,
                    ngwaf_interval_mins,
                    minutes=ngwaf_interval_mins,
                    jitter=15,
                    args=[service_id],
                    misfire_grace_time=300,
                    executor="maintenance",
                )
            cleanup_cfg = prov.get("cron_metadata_cleanup", {})
            if cleanup_cfg.get("enabled", True) and not dev_mode_no_crons():
                cleanup_job_id = f"metadata_cleanup_{service_id}"
                cron_hour = int(cleanup_cfg.get("cron_hour", 3))
                cron_minute = int(cleanup_cfg.get("cron_minute", 15))
                self._register_reschedulable_job(
                    cleanup_job_id,
                    _run_metadata_cleanup,
                    "cron",
                    seen_ids,
                    "🧹 \x1b[35m[metadata_cleanup]\x1b[0m Registered metadata cleanup job %s (daily %02d:%02d UTC).",
                    cleanup_job_id,
                    cron_hour,
                    cron_minute,
                    hour=cron_hour,
                    minute=cron_minute,
                    args=[service_id],
                    misfire_grace_time=3600,
                )
        self._register_once_job(
            "bot_data_refresh",
            _run_bot_data_refresh,
            "cron",
            seen_ids,
            "👾 \x1b[36m[bots]\x1b[0m Registered bot data refresh job (daily 02:00 UTC).",
            hour=2,
            minute=0,
            misfire_grace_time=3600,
        )
        self._register_once_job(
            "rdns_enrichment",
            _run_rdns_enrichment,
            "interval",
            seen_ids,
            "🌐 \x1b[34m[rdns]\x1b[0m Registered rDNS enrichment job (every 5m).",
            minutes=5,
            jitter=15,
            misfire_grace_time=300,
        )
        self._register_once_job(
            "share_audit_purge",
            _run_share_audit_purge,
            "cron",
            seen_ids,
            "🧹 \x1b[35m[share_audit_purge]\x1b[0m Registered share audit purge job (daily 03:45 UTC, executor=maintenance).",
            hour=3,
            minute=45,
            misfire_grace_time=3600,
        )
        self._register_metric_snapshot_job(seen_ids)
        self._register_recycle_job(seen_ids)
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
        if dev_mode_no_crons():
            logger.warning("🚫 [scheduler] FLA_DEV_NO_CRONS=1 — Scheduler.reload() ignored.")
            return
        self._sync_jobs()

    def get_job(self, job_id: str):
        return self._sched.get_job(job_id)


_scheduler: Scheduler | None = None


def get_scheduler() -> Scheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = Scheduler()
    return _scheduler


TASK_MAP = {
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
    for job in sched._sched.get_jobs():
        job_id = getattr(job, "id", "")
        if not job_id.endswith(f"_{service_id}"):
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


__all__ = [
    "Scheduler",
    "TASK_MAP",
    "_resolve_rum_sync_interval",
    "_scheduler",
    "_service_has_alerts",
    "build_cron_schedule_payload",
    "dev_local_crons_enabled",
    "dev_mode_no_crons",
    "get_scheduler",
    "logger",
    "partial_hour_merge_enabled",
    "partial_hour_merge_interval_sec",
]
