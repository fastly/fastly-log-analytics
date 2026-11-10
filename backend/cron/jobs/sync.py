"""Ingest-class cron jobs.

  * ``_run_log_discovery_cron`` — per-tick discovery of new raw .gz files from FOS
    into the High-Scale control plane.
  * ``_run_full_sweep`` — daily catch-net that LISTs the full raw/request/ prefix
    to pick up late-arriving files outside the incremental window.
  * ``_run_gap_heal`` — periodic gap detector that triggers a full sweep
    when sustained loss is observed between Fastly stats and our ingest.

The gap-heal trigger reads its collaborators (``_run_full_sweep`` and the
throttle-marker helpers) from module globals at call time, so tests patch
them at ``backend.cron.jobs.sync.<name>``.
"""

from __future__ import annotations

import importlib as _importlib
import logging
import sys as _sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from backend import config as svcconfig
from backend.core.fos_client import (
    backfill_fastly_edge_writes,
    get_source_for_service,
    reconcile_fastly_stats,
)
from backend.core.service_defaults import DEFAULT_LOG_PERIOD_SECONDS
from backend.cron.jobs import _common
from backend.cron.jobs._common import (
    _INGEST_ERROR_OUTCOME_KEYS,  # noqa: F401
    _has_ingest_outcome_errors,  # noqa: F401
    _orig_finalize_cron_duration,
    _orig_publish_sync_status_safe,
    _refuse_if_dev_no_crons,
    _resolve_sync_hook,
)
from backend.cron.jobs.rum_ledger import _discover_high_scale_prefix
from backend.cron.scheduler import (
    _check_disk_space,  # noqa: F401
    _claim_heavy_refresh,
    _display_label,
    _elapsed_since,
    _extract_log_text,
    _log_and_add_progress,
    _record_heavy_refresh_cost,
    cron_run,
    cron_task,
    dev_mode_no_crons,  # noqa: F401
)
from backend.repositories.utils.response_cache import invalidate_service_cache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc

if TYPE_CHECKING:
    from backend.cron.jobs._common import (
        apply_time_range_pinning as apply_time_range_pinning,
    )
    from backend.cron.jobs._common import (
        finalize_cron_duration as finalize_cron_duration,
    )
    from backend.cron.jobs._common import (
        get_service_display_label as get_service_display_label,
    )
    from backend.cron.jobs._common import (
        has_sufficient_disk_space as has_sufficient_disk_space,
    )
    from backend.cron.jobs._common import (
        publish_sync_status_safe as publish_sync_status_safe,
    )
    from backend.cron.jobs.rum_ledger import (
        _run_ledger_sweep as _run_ledger_sweep,
    )
    from backend.cron.jobs.rum_ledger import (
        _run_ledger_sweep_impl as _run_ledger_sweep_impl,
    )
    from backend.cron.jobs.rum_ledger import (
        _run_rum_discovery_cron as _run_rum_discovery_cron,
    )
    from backend.cron.jobs.rum_ledger import (
        _run_rum_ledger_sweep as _run_rum_ledger_sweep,
    )
    from backend.cron.jobs.rum_ledger import (
        discover_rum_prefix as discover_rum_prefix,
    )
    from backend.cron.jobs.rum_ledger import (
        sweep_ledger_once as sweep_ledger_once,
    )
    from backend.cron.jobs.rum_ledger import (
        sweep_rum_ledger_once as sweep_rum_ledger_once,
    )
    from backend.cron.jobs.rum_sync import (
        _faro_purge_surrogate_key as _faro_purge_surrogate_key,
    )
    from backend.cron.jobs.rum_sync import (
        _reconcile_faro_bundle as _reconcile_faro_bundle,
    )

logger = logging.getLogger("backend.scheduler")


def discover_prefix(
    service_id: str,
    prefix_subpath: str | None = None,
    start_time: str | None = None,
    run_id: int | None = None,
    originating_task: str = "log_discovery",
) -> int:
    del start_time, run_id, originating_task
    return _discover_high_scale_prefix(service_id, domain="request", prefix_subpath=prefix_subpath)


def _is_batch_due(service_id: str, cfg: dict[str, Any] | None = None) -> bool:
    """Return True when a periodic Fastly log batch is actively expected to land in FOS."""
    status = svcconfig.get_status(service_id)
    if not isinstance(status, dict):
        return False
    req_status = status.get("request")
    latest_str = (
        req_status.get("latest_log_at") if isinstance(req_status, dict) else status.get("latest_log_at")
    ) or status.get("latest_available_file_at")
    latest_dt = parse_iso_utc(latest_str) if isinstance(latest_str, str) else None
    if latest_dt is None:
        return False
    age_s = (datetime.now(UTC) - latest_dt).total_seconds()
    log_period = max(5, int((cfg or {}).get("log_period", DEFAULT_LOG_PERIOD_SECONDS) or DEFAULT_LOG_PERIOD_SECONDS))
    return bool(max(6.0, log_period - 3.0) <= age_s <= max(35.0, log_period * 3.5))


@cron_task("cron.log_discovery", job_name="log_discovery")
def _run_log_discovery_cron(
    service_id: str,
    force: bool = False,
    delete_after: bool | None = None,
    run_id: int | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> None:
    """Discover new raw .gz files from FOS and register them in the high-scale control plane."""
    del delete_after, end_time
    if _refuse_if_dev_no_crons(service_id, "sync"):
        return

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        logger.warning("[scheduler] %s: config not found, skipping.", service_id)
        return
    src = get_source_for_service(service_id)
    if src is None:
        logger.warning("[scheduler] %s: source not found, skipping.", service_id)
        return
    sync_cfg = cfg.get("provisioning", {}).get("cron_sync", {})
    sync_enabled = sync_cfg.get("enabled", True)
    _display = _common.get_service_display_label(service_id, cfg)
    start_time_exec = time.time()
    discovered = 0

    def elapsed() -> str:
        return _elapsed_since(start_time_exec)

    def _progress_status(message: str) -> None:
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="log_discovery",
            event={"type": "status", "message": f"{elapsed()} {message}"},
        )

    # ── 1. Discover ───────────────────────────────────────────────────────────
    if sync_enabled or force:
        is_manual = run_id is not None
        start_time, _ = _common.apply_time_range_pinning(
            service_id, cfg, src, start_time=start_time, is_manual=is_manual, fallback_to_created_at=True
        )

        with cron_run(
            service_id,
            "log_discovery",
            source=src,
            run_id=run_id,
            error_summary="Log discovery failed",
            skip_log_prefix="log_discovery",
            finalize_running_backstop=True,
            backstop_summary="Sync exited without recording a terminal status",
            backstop_error_message="orphaned run auto-finalized (no terminal ingest event)",
        ) as run:
            if not run.ok:
                return
            run_id = run.run_id
            run.include_log_output = False

            from backend.provision.cli import minute_list_prefix

            try:
                adaptive = sync_cfg.get("polling_mode", "adaptive") == "adaptive" and not is_manual
                due_batch = adaptive and _is_batch_due(service_id, cfg)
                poll_started = time.monotonic()
                for poll_index in range(3):
                    if poll_index:
                        if not adaptive or poll_index > 2 or time.monotonic() - poll_started + 1.5 > 20:
                            break
                        time.sleep(1.5)
                    now = datetime.now(UTC)
                    pass_discovered = sum(
                        discover_prefix(
                            service_id,
                            prefix_subpath=minute_list_prefix(now - timedelta(minutes=i)),
                            start_time=start_time,
                            run_id=run_id,
                        )
                        for i in range(4, -1, -1)
                    )
                    discovered += pass_discovered
                    if pass_discovered == 0 and not (poll_index == 0 and due_batch):
                        break
                run.success(
                    f"Discovered {discovered} new file(s); dispatched to ingest workers"
                    if discovered
                    else "No new files",
                    files_downloaded=discovered,
                )
            except Exception as e:
                run.fail("Log discovery failed", error_message=str(e))
                logger.exception("[ledger] %s: log discovery failed: %s", service_id, e)
                return

    # ── 2. Invalidate response caches & publish SSE ───────────────────────────
    from backend.utils.telemetry import should_defer_cron

    do_heavy_refresh = (
        False if (not force and should_defer_cron("log_discovery", service_id)) else _claim_heavy_refresh(service_id)
    ) or bool(force)
    needs_status_publish = bool(discovered > 0 or do_heavy_refresh or force)

    try:
        invalidate_service_cache(src.get("name", ""))
    except Exception:
        pass

    if needs_status_publish:
        publish_fn = _resolve_sync_hook(
            "publish_sync_status_safe", _common.publish_sync_status_safe, _orig_publish_sync_status_safe
        )
        publish_fn(service_id, "scheduler")

    # ── 3. Usage log bookkeeping ──────────────────────────────────────────────
    if (sync_enabled or force) and run_id is not None and do_heavy_refresh:
        _progress_status("Updating usage log (Fastly-edge writes, in-process calls, retention purge)...")

    if do_heavy_refresh:
        _t0 = time.time()
        try:
            from backend.utils.telemetry import run_usage_log_cleanup

            for fn, msg_fn, log_label in (
                (
                    lambda: backfill_fastly_edge_writes(src),
                    lambda n: f"Backfilled {n} Fastly-edge PUTs to usage log",
                    "backfill",
                ),
                (
                    lambda: reconcile_fastly_stats(src, hours_back=26),
                    lambda n: f"Reconciled {n} hourly Fastly stats gap(s)",
                    "Fastly stats reconciliation",
                ),
            ):
                try:
                    if count := fn():
                        if run_id is not None:
                            _log_and_add_progress(
                                run_id,
                                service_id,
                                job_name="usage_log",
                                event={"type": "status", "message": msg_fn(count)},
                            )
                        else:
                            logger.info("[usage_log] %s: %s", service_id, msg_fn(count))
                except Exception as e:
                    logger.warning("[usage_log] %s failed for %s: %s", log_label, service_id, e)
            run_usage_log_cleanup(service_id)
        except Exception as e:
            logger.warning("[scheduler] %s: usage_log phase failed: %s", service_id, e)
        _record_heavy_refresh_cost(service_id, time.time() - _t0)
        if run_id is not None:
            _progress_status(f"usage_log phase: {int((time.time() - _t0) * 1000)}ms")

    # ── 4. Final duration record ──────────────────────────────────────────────
    if sync_enabled or force:
        finalize_fn = _resolve_sync_hook(
            "finalize_cron_duration", _common.finalize_cron_duration, _orig_finalize_cron_duration
        )
        finalize_fn(
            src,
            run_id,
            start_time_exec,
            log_output=_extract_log_text(run_id) if run_id is not None else None,
            silent=False,
        )
    logger.info("⏹️  \x1b[94m[log_discovery]\x1b[0m %s: Log Discovery job finished.", _display)


# ── _run_full_sweep (daily catch-net) ────────────────────────────────────────

_FULL_SWEEP_DEFAULT_MAX_FILES = 20_000
_FULL_SWEEP_DEFAULT_MAX_SECONDS = 900


@cron_task("cron.full_sync", job_name="full_sync")
def _run_full_sweep(
    service_id: str,
    max_files: int = _FULL_SWEEP_DEFAULT_MAX_FILES,
    max_seconds: int = _FULL_SWEEP_DEFAULT_MAX_SECONDS,
    run_id: int | None = None,
    force: bool = False,
) -> None:
    """Daily catch-net: full LIST over raw/request/ to pick up late-arriving files."""
    del max_files, max_seconds
    if _refuse_if_dev_no_crons(service_id, "full_sweep"):
        return

    from backend.utils.telemetry import should_defer_cron

    src = get_source_for_service(service_id)
    if src is None or (not force and should_defer_cron("full_sync", service_id)):
        return

    with cron_run(
        service_id,
        "full_sync",
        source=src,
        run_id=run_id,
        error_summary="Full-sweep crashed",
        skip_log_prefix="full_sync",
        finalize_duration=True,
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        run.include_log_output = False
        try:
            discovered = discover_prefix(service_id, run_id=run_id, originating_task="full_sync")
            run.success(
                f"Full-prefix sweep discovered {discovered} unseen file(s); dispatched to ingest workers"
                if discovered
                else "Full-prefix sweep: no unseen files",
                files_downloaded=discovered,
            )
        except Exception as e:
            run.fail("Full-prefix ledger sweep failed", error_message=str(e))
            logger.exception("[full_sync] %s: ledger sweep failed", service_id)


# ── _run_gap_heal (periodic detector → triggers full_sweep) ──────────────────

GAP_HEAL_THROTTLE_HOURS = 4


@dataclass(frozen=True)
class _GapHealSeverityBand:
    name: str
    min_gap_pct: float
    min_lost_lines: int
    throttle_hours: float
    sweep_max_files: int
    sweep_max_seconds: int


_GAP_HEAL_SEVERITY_BANDS: tuple[_GapHealSeverityBand, ...] = (
    _GapHealSeverityBand("critical", 0.80, 500_000, 0.0, 100_000, 1800),
    _GapHealSeverityBand("severe", 0.50, 100_000, 0.25, 50_000, 1500),
    _GapHealSeverityBand("elevated", 0.10, 10_000, 2.0, _FULL_SWEEP_DEFAULT_MAX_FILES, _FULL_SWEEP_DEFAULT_MAX_SECONDS),
    _GapHealSeverityBand(
        "mild", 0.0, 0, GAP_HEAL_THROTTLE_HOURS, _FULL_SWEEP_DEFAULT_MAX_FILES, _FULL_SWEEP_DEFAULT_MAX_SECONDS
    ),
)


def _gap_heal_severity(max_gap_pct: float, total_lost_lines: int) -> _GapHealSeverityBand:
    """Return the first severity band whose floor either field clears."""
    for band in _GAP_HEAL_SEVERITY_BANDS:
        if max_gap_pct >= band.min_gap_pct or total_lost_lines >= band.min_lost_lines:
            return band
    return _GAP_HEAL_SEVERITY_BANDS[-1]


_GAP_HEAL_LAST_TRIGGER: dict[str, float] = {}


def _last_successful_gap_heal_trigger(service_id: str) -> float | None:
    return _GAP_HEAL_LAST_TRIGGER.get(service_id)


def _mark_gap_heal_triggered(service_id: str) -> None:
    _GAP_HEAL_LAST_TRIGGER[service_id] = time.time()


@cron_task("gap_heal", job_name="gap_heal")
def _run_gap_heal(service_id: str, run_id: int | None = None, force: bool = False) -> None:
    """Periodic gap detector that triggers a full_sweep when sustained loss is observed."""
    if _refuse_if_dev_no_crons(service_id, "gap_heal"):
        return

    from backend.utils.telemetry import should_defer_cron

    src = get_source_for_service(service_id)
    if src is None or (not force and should_defer_cron("gap_heal", service_id)):
        return

    _display = _display_label(src, service_id)
    with cron_run(
        service_id,
        "gap_heal",
        source=src,
        run_id=run_id,
        error_summary="Gap-heal evaluation crashed",
        skip_log_prefix="gap_heal",
        finalize_duration=True,
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        try:
            from fastapi import HTTPException

            from backend.routers.admin import compute_log_accounting

            try:
                result = compute_log_accounting(src, hours=24, by="hour")
            except HTTPException as exc:
                if "fastly_stats_failed" in str(exc.detail):
                    summary = "Fastly stats unavailable for service (skipped)"
                    run.success(summary)
                    _log_and_add_progress(
                        run_id, service_id, job_name="gap_heal", event={"type": "done", "message": summary}
                    )
                    logger.info("⏭️  \x1b[95m[gap_heal]\x1b[0m %s: %s", service_id, summary)
                    return
                raise

            sustained = result.get("sustained_loss")
            if sustained is None:
                run.success("No sustained loss detected")
                _log_and_add_progress(
                    run_id,
                    service_id,
                    job_name="gap_heal",
                    event={"type": "done", "message": "No sustained loss detected"},
                )
                return

            band = _gap_heal_severity(sustained.max_gap_pct, sustained.total_lost_lines)
            last_heal = _last_successful_gap_heal_trigger(service_id)
            if not force and band.throttle_hours > 0 and last_heal is not None:
                elapsed_hours = (time.time() - last_heal) / 3600.0
                if elapsed_hours < band.throttle_hours:
                    msg = (
                        f"Sustained loss detected ({sustained.n_buckets} bucket(s), "
                        f"max gap {sustained.max_gap_pct:.1%}, "
                        f"{sustained.total_lost_lines} lost line(s), severity={band.name}) "
                        f"— throttled, last heal {elapsed_hours:.1f}h ago "
                        f"(< {band.throttle_hours:g}h)"
                    )
                    run.warn(msg)
                    _log_and_add_progress(
                        run_id, service_id, job_name="gap_heal", event={"type": "done", "message": msg}
                    )
                    return

            msg = (
                f"Sustained loss detected ({sustained.n_buckets} bucket(s) "
                f"from {sustained.started_at}, max gap {sustained.max_gap_pct:.1%}, "
                f"{sustained.total_lost_lines} lost line(s), severity={band.name}) — "
                f"triggering full_sweep (max_files={band.sweep_max_files}, "
                f"max_seconds={band.sweep_max_seconds})"
            )
            logger.warning("🩹 \x1b[33m[gap_heal]\x1b[0m %s: %s", _display, msg)
            _log_and_add_progress(run_id, service_id, job_name="gap_heal", event={"type": "status", "message": msg})
            run.warn(msg)
            _mark_gap_heal_triggered(service_id)
            _run_full_sweep(service_id, max_files=band.sweep_max_files, max_seconds=band.sweep_max_seconds, force=force)
        except Exception as e:
            run.fail("Gap-heal evaluation crashed", error_message=str(e))
            logger.exception("[gap_heal] %s: unexpected error", service_id)


_CacheRegistry.register("cron.jobs.sync._GAP_HEAL_LAST_TRIGGER", _GAP_HEAL_LAST_TRIGGER)


_DELEGATED_EXPORTS: dict[str, str] = {
    "apply_time_range_pinning": "backend.cron.jobs._common",
    "finalize_cron_duration": "backend.cron.jobs._common",
    "get_service_display_label": "backend.cron.jobs._common",
    "has_sufficient_disk_space": "backend.cron.jobs._common",
    "publish_sync_status_safe": "backend.cron.jobs._common",
    "_faro_purge_surrogate_key": "backend.cron.jobs.rum_sync",
    "_reconcile_faro_bundle": "backend.cron.jobs.rum_sync",
    "_run_rum_discovery_cron": "backend.cron.jobs.rum_ledger",
    "_run_ledger_sweep_impl": "backend.cron.jobs.rum_ledger",
    "_run_ledger_sweep": "backend.cron.jobs.rum_ledger",
    "_run_rum_ledger_sweep": "backend.cron.jobs.rum_ledger",
    "discover_rum_prefix": "backend.cron.jobs.rum_ledger",
    "sweep_ledger_once": "backend.cron.jobs.rum_ledger",
    "sweep_rum_ledger_once": "backend.cron.jobs.rum_ledger",
}


class _SyncModule(_sys.modules[__name__].__class__):  # type: ignore[misc]
    _orig_delegated: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        if (mod_name := _DELEGATED_EXPORTS.get(name)) is not None:
            return getattr(_importlib.import_module(mod_name), name)
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        if (mod_name := _DELEGATED_EXPORTS.get(name)) is not None:
            mod = _importlib.import_module(mod_name)
            if name not in self._orig_delegated:
                self._orig_delegated[name] = getattr(mod, name)
            setattr(mod, name, value)
            if value is self._orig_delegated.get(name):
                self._orig_delegated.pop(name, None)
            self.__dict__.pop(name, None)
            return
        super().__setattr__(name, value)

    def __delattr__(self, name: str) -> None:
        if (mod_name := _DELEGATED_EXPORTS.get(name)) is not None and name in self._orig_delegated:
            mod = _importlib.import_module(mod_name)
            setattr(mod, name, self._orig_delegated.pop(name))
            self.__dict__.pop(name, None)
            return
        super().__delattr__(name)


_sys.modules[__name__].__class__ = _SyncModule
