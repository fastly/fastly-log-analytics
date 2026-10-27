"""Background prewarmer for Insights evaluations on active services."""

from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime

from backend.cron.scheduler import (
    _display_label,
    cron_run,
    cron_task,
)
from backend.utils.bounded_cache import CacheRegistry

logger = logging.getLogger("backend.scheduler")

_MAX_ANALYST_SHAPES = 8
_INGEST_TASKS = ("log_discovery", "rum_sync")
DEFAULT_PREWARM_IDLE_MINUTES = 30.0
_last_full_warm: dict[str, float] = {}
CacheRegistry.register("insights_prewarmer._last_full_warm", _last_full_warm)
_last_insights_request: dict[str, float] = {}
CacheRegistry.register("insights_prewarmer._last_insights_request", _last_insights_request)


def record_insights_request(service_id: str | None) -> None:
    if service_id:
        _last_insights_request[service_id] = time.monotonic()


def _prewarm_idle_minutes(service_id: str, src: dict | None = None) -> float:
    prov = (src or {}).get("provisioning") if isinstance(src, dict) else None
    if not isinstance(prov, dict):
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id) or {}
        prov = cfg.get("provisioning") if isinstance(cfg.get("provisioning"), dict) else {}
    cron_ip = prov.get("cron_insights_prewarmer") if isinstance(prov, dict) else None
    for candidate in (
        cron_ip.get("idle_minutes") if isinstance(cron_ip, dict) else None,
        os.environ.get("INSIGHTS_PREWARM_IDLE_MINUTES", "").strip() or None,
    ):
        if candidate is not None:
            try:
                return float(candidate)
            except (TypeError, ValueError):
                pass
    return DEFAULT_PREWARM_IDLE_MINUTES


def is_insights_active(service_id: str, src: dict | None = None, *, idle_minutes: float | None = None) -> bool:
    limit_mins = _prewarm_idle_minutes(service_id, src) if idle_minutes is None else float(idle_minutes)
    if limit_mins <= 0:
        return True
    last_req = _last_insights_request.get(service_id)
    if last_req is None and isinstance(src, dict):
        alt_id = src.get("service_id") or src.get("name")
        if isinstance(alt_id, str) and alt_id:
            last_req = _last_insights_request.get(alt_id)
    return last_req is not None and (time.monotonic() - last_req) < (limit_mins * 60.0)


def _running_ingest(service_id: str) -> str | None:
    from backend.cron_progress import active_task_for_service

    return active_task_for_service(service_id, _INGEST_TASKS)


def _cache_cold(service_id: str) -> bool:
    from backend.repositories.insights.repository import INSIGHTS_CACHE_TTL

    last = _last_full_warm.get(service_id)
    return last is None or time.monotonic() - last >= INSIGHTS_CACHE_TTL


def _analyst_prewarm_enabled() -> bool:
    return os.environ.get("INSIGHTS_PREWARM_ANALYST", "1").strip().lower() not in ("0", "false", "no", "off")


def _active_analyst_shapes(service_id: str) -> list[tuple[str | None, str | None, int | None, bool]]:
    from backend.core import share_db
    from backend.utils.date_utils import parse_iso_utc

    now = datetime.now(UTC)
    shapes: set[tuple[str | None, str | None, int | None, bool]] = set()
    for inv in share_db.get_remote_invites():
        if inv.get("revoked") or service_id not in (inv.get("service_ids") or []):
            continue
        if (exp := inv.get("expires_at")) and (pe := parse_iso_utc(exp)) is not None and pe < now:
            continue
        policy = inv.get("pii_policy")
        mask = bool(policy.get("mask_ips")) if isinstance(policy, dict) else False
        shapes.add((inv.get("query_start_time"), inv.get("query_end_time"), inv.get("query_window_hours"), mask))
    ordered = sorted(shapes, key=lambda s: (s[0] or "", s[1] or "", s[2] or 0, s[3]))
    if len(ordered) > _MAX_ANALYST_SHAPES:
        logger.warning(
            "[insights-prewarmer] %s: %d analyst shapes, warming first %d",
            service_id,
            len(ordered),
            _MAX_ANALYST_SHAPES,
        )
        ordered = ordered[:_MAX_ANALYST_SHAPES]
    return ordered


@cron_task("insights_prewarmer", job_name="insights_prewarmer")
def _run_insights_prewarmer(service_id: str) -> None:
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.repositories.insights import get_insights
    from backend.utils.active_requests import should_defer_cron, yield_to_api
    from backend.utils.remote_access import resolve_analyst_insights_clamp
    from backend.utils.time_window import history_hours_from_earliest, pick_insights_default
    from backend.utils.tunnel import get_tunnel_manager

    src = get_source_for_service(service_id)
    if src is None or should_defer_cron("insights_prewarmer", service_id):
        return
    idle_mins = _prewarm_idle_minutes(service_id, src)
    if not is_insights_active(service_id, src, idle_minutes=idle_mins):
        logger.info(
            "[insights-prewarmer] %s: skipping — idle (no /api/insights requests in the last %gm)",
            service_id,
            idle_mins,
        )
        return
    must_run = _cache_cold(service_id)
    if not must_run and (ingest_task := _running_ingest(service_id)) is not None:
        logger.info("[insights-prewarmer] %s: deferring — %s in flight and cache still warm", service_id, ingest_task)
        return
    _display = _display_label(src, service_id)
    con = None
    with cron_run(
        service_id,
        "insights_prewarmer",
        source=src,
        error_summary=lambda exc: f"Prewarm failed: {exc}",
        finalize_duration=True,
        include_log_output=False,
        skip_log_prefix="insights-prewarmer",
    ) as run:
        if not run.ok:
            return
        try:
            cached_status = svcconfig.get_status(src["name"]) or {}
            window_hours, baseline_hours = pick_insights_default(
                history_hours_from_earliest(cached_status.get("earliest_log_at"))
            )
            con = get_connection(source=src, max_wait=5, read_only=True, skip_view_update=True)
            yield_to_api(max_wait_secs=1.0)
            get_insights(
                con,
                src,
                window_hours=window_hours,
                baseline_hours=baseline_hours,
                service_id=service_id,
                force_refresh=True,
                max_workers=1,
            )
            analyst_warmed = 0
            stopped_for: str | None = None
            complete = True
            if _analyst_prewarm_enabled() and get_tunnel_manager().is_sharing_active():
                for qs, qe, qwh, mask in _active_analyst_shapes(service_id):
                    if not must_run and (stopped_for := _running_ingest(service_id)) is not None:
                        logger.info(
                            "[insights-prewarmer] %s: stopping early — %s started mid-tick", service_id, stopped_for
                        )
                        complete = False
                        break
                    if should_defer_cron("insights_prewarmer", service_id):
                        logger.info(
                            "[insights-prewarmer] %s: deferring remaining analyst prewarms due to active API requests",
                            service_id,
                        )
                        complete = False
                        break
                    yield_to_api(max_wait_secs=0.5)
                    try:
                        cs, ce, ck = resolve_analyst_insights_clamp(
                            qs, qe, qwh, baseline_hours=baseline_hours, window_hours=window_hours
                        )
                    except ValueError:
                        continue
                    get_insights(
                        con,
                        src,
                        window_hours=window_hours,
                        baseline_hours=baseline_hours,
                        service_id=service_id,
                        clamp_start=cs,
                        clamp_end=ce,
                        mask_ips=mask,
                        clamp_cache_key=ck,
                        force_refresh=True,
                        max_workers=1,
                    )
                    analyst_warmed += 1
            duration = run.elapsed()
            summary = (
                f"Prewarmed default insights selection ({window_hours:g}h/{baseline_hours:g}h: "
                f"admin + {analyst_warmed} analyst shape{'' if analyst_warmed == 1 else 's'})"
            )
            if stopped_for is not None:
                summary += f"; stopped early: {stopped_for} in flight"
            if complete:
                _last_full_warm[service_id] = time.monotonic()
            run.success(summary)
            run.log_progress({"type": "done", "message": summary})
            logger.info(
                "✅ [insights-prewarmer] %s: prewarmed %gh/%gh in %.2fs (admin + %d analyst)",
                _display,
                window_hours,
                baseline_hours,
                duration,
                analyst_warmed,
            )
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass
