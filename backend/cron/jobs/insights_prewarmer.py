"""Background prewarmer cron for the default /api/insights selection.

The dashboard's default insights view costs seconds-to-tens-of-seconds
per cold call (scales with the baseline scan breadth; the 720 h shape on
a long-history service runs ~20 s). INSIGHTS_CACHE_TTL is 300 s, so a
steady stream of users would mostly hit the warm cache — but the cache
is process-local and the first user after any backend restart, cache
eviction, or 5-min idle window pays the full cost.

Running ``get_insights`` every 240 s per service keeps that user-facing
p50 at the warm hit (~100 ms). Implementation mirrors
:mod:`backend.cron.jobs.compaction`: ``@cron_task`` decorator + per-service
start_cron_run + sync DuckDB connection acquisition.

ADAPTIVE default (load-bearing): the frontend picks the default
window/baseline pair from the service's log history
(``frontend/lib/insights-defaults.ts`` — e.g. ≥30 d of history defaults to
window=1 h / baseline=720 h, NOT the static 1 h/168 h). The /api/insights
cache key folds in both hour values, so this cron derives the same pair via
:mod:`backend.utils.insights_defaults` (the Python mirror of that picker)
from the same ``earliest_log_at`` snapshot the clients read. Warming a
hardcoded static pair on a ≥30 d service is a guaranteed miss for every
default page load — that regression shipped with the adaptive-defaults
feature and is why this derivation exists.

ANALYST coverage: the admin entry alone never warmed the analyst path —
an analyst request keys on its invite's clamp shape + mask_ips, which the
admin warm (unclamped, mask_ips=0) never writes (the perf-audit "insights
broken for analysts" finding). So while sharing is active we also warm one
entry per distinct active-invite clamp shape, using the SAME stable cache
key (``analyst_clamp_cache_key``) a live analyst request looks up.

``force_refresh=True``: every tick RECOMPUTES (and rewrites) the entry
instead of short-circuiting on a hit. cachetools' TTL is measured from
insertion, so a hit would NOT reset it — the entry would still expire at
its 300 s mark, leaving a ~(TTL - interval) window each cycle where a user
pays cold. Forcing the recompute at 240 s < 300 s TTL keeps the entry
continuously warm with margin.

Active-request gate (#84) applied — if active dashboard queries are
running, prewarming defers to avoid competing for DuckDB threads and CPU
with user queries.

Ingest yield: a prewarm tick (~55 s avg on the 4-vCPU VM) running alongside
``log_discovery`` / ``rum_sync`` for the same service measurably stretches
their tick time and so ingest freshness. The signal is the in-process
progress registry (:func:`backend.cron_progress.active_task_for_service`) —
no DB query. If ingest is in flight at tick start the tick defers (INFO log,
no lease, no ``cron_runs`` row — same shape as the active-request gate); if
ingest starts mid-tick the remaining shape recomputes are skipped and the run
records success with "stopped early" in its summary. In high_scale mode
discovery runs on a Celery worker in another process, so this signal never
fires there and the prewarmer behaves as before.

Warmth bound (load-bearing): ``_last_full_warm`` stamps the last tick that
warmed EVERY shape. The ingest yield applies only while that stamp is
younger than ``INSIGHTS_CACHE_TTL``; once the oldest warmed entry has reached
the TTL (or this process has never completed a warm) the tick runs to
completion regardless of ingest. With interval I = 240 s < TTL = 300 s < 2I,
at most one tick in a row can be deferred or cut short, so an entry is cold
for at most 2I - TTL = 180 s (plus the recompute itself, as before) — never
longer than one extra interval.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime

from backend.cron.decorators import cron_task
from backend.cron.lifecycle import cron_run
from backend.cron.scheduler import _display_label
from backend.utils.cache_registry import CacheRegistry

logger = logging.getLogger("backend.scheduler")

# Cap on distinct analyst clamp-shapes warmed per tick — bounds background
# compute (each shape is a full recompute). Prod typically has 1. Truncation
# is logged, never silent.
_MAX_ANALYST_SHAPES = 8

_INGEST_TASKS = ("log_discovery", "rum_sync")

# service_id -> time.monotonic() of the last tick that warmed every shape.
_last_full_warm: dict[str, float] = {}
CacheRegistry.register("insights_prewarmer._last_full_warm", _last_full_warm)


def _running_ingest(service_id: str) -> str | None:
    from backend.cron_progress import active_task_for_service

    return active_task_for_service(service_id, _INGEST_TASKS)


def _cache_cold(service_id: str) -> bool:
    """True when the oldest prewarmed entry has reached the cache TTL (or
    nothing was ever fully warmed here) — the tick must not yield to ingest."""
    from backend.repositories.insights.repository import INSIGHTS_CACHE_TTL

    last = _last_full_warm.get(service_id)
    return last is None or time.monotonic() - last >= INSIGHTS_CACHE_TTL


def _analyst_prewarm_enabled() -> bool:
    """Analyst-shape prewarm is on by default; ``INSIGHTS_PREWARM_ANALYST=0``
    is a belt-and-braces kill switch (re-read each call so tests can flip it)."""
    return os.environ.get("INSIGHTS_PREWARM_ANALYST", "1").strip().lower() not in ("0", "false", "no", "off")


def _active_analyst_shapes(service_id: str) -> list[tuple[str | None, str | None, int | None, bool]]:
    """Distinct ``(query_start_time, query_end_time, query_window_hours,
    mask_ips)`` tuples across the service's active (non-revoked, non-expired)
    analyst invites.

    Each tuple is one clamp shape a live ``/api/insights`` request will look
    up; warming it keeps the analyst cache hot the same way the admin entry is.
    Deduped so N invites sharing a window+masking config cost one warm. Capped
    at ``_MAX_ANALYST_SHAPES`` (logged when it bites).
    """
    from backend.core import share_db
    from backend.utils.date_utils import parse_iso_utc

    now = datetime.now(UTC)
    shapes: set[tuple[str | None, str | None, int | None, bool]] = set()
    for inv in share_db.get_remote_invites():
        if inv.get("revoked"):
            continue
        exp = inv.get("expires_at")
        if exp:
            pe = parse_iso_utc(exp)
            if pe is not None and pe < now:
                continue
        if service_id not in (inv.get("service_ids") or []):
            continue
        policy = inv.get("pii_policy")
        mask = bool(policy.get("mask_ips")) if isinstance(policy, dict) else False
        shapes.add(
            (
                inv.get("query_start_time"),
                inv.get("query_end_time"),
                inv.get("query_window_hours"),
                mask,
            )
        )

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
    """Warm the default insights selection — the pair the adaptive frontend
    picker will request for this service's history, for the admin/unclamped
    entry plus each active analyst clamp shape — so the first user (admin OR
    analyst) lands on a cache hit instead of the cold path."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.repositories.insights import get_insights
    from backend.utils.active_requests import should_defer_cron, yield_to_api
    from backend.utils.insights_defaults import history_hours_from_earliest, pick_insights_default
    from backend.utils.remote_access import resolve_analyst_insights_clamp
    from backend.utils.tunnel import get_tunnel_manager

    src = get_source_for_service(service_id)
    if src is None:
        return

    if should_defer_cron("insights_prewarmer", service_id):
        return

    must_run = _cache_cold(service_id)
    if not must_run:
        ingest_task = _running_ingest(service_id)
        if ingest_task is not None:
            logger.info(
                "[insights-prewarmer] %s: deferring — %s in flight and cache still warm",
                service_id,
                ingest_task,
            )
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
            # Same cached snapshot the clients' picker reads (bootstrap
            # log_extents / /api/log-extents both project svcconfig status,
            # un-clamped for analysts) → same bucket → same cache key.
            cached_status = svcconfig.get_status(src["name"]) or {}
            history_hours = history_hours_from_earliest(cached_status.get("earliest_log_at"))
            window_hours, baseline_hours = pick_insights_default(history_hours)

            # Read-only + skip_view_update: insights queries against the live
            # Iceberg view; a few seconds of view staleness across cron-tick
            # boundaries is fine here (the next prewarmer tick will resolve
            # newly-bound view tables anyway).
            con = get_connection(source=src, max_wait=5, read_only=True, skip_view_update=True)

            # Serial insight queries leave headroom for foreground requests.
            # Never change DuckDB threads/memory_limit here:
            # both are instance-global, so they'd clobber every concurrent query, and
            # changing threads mid-flight deadlocks DuckDB (AGENTS.md Trap #42).

            yield_to_api(max_wait_secs=1.0)

            # 1) Admin / unclamped default selection.
            get_insights(
                con,
                src,
                window_hours=window_hours,
                baseline_hours=baseline_hours,
                service_id=service_id,
                force_refresh=True,
                max_workers=1,
            )

            # 2) Analyst clamp shapes — only meaningful while sharing is live (no
            #    analyst can reach /api/insights otherwise). Warm each active
            #    invite's (window-params, mask_ips) shape under the SAME stable key
            #    a live analyst request derives.
            analyst_warmed = 0
            stopped_for: str | None = None
            complete = True
            if _analyst_prewarm_enabled() and get_tunnel_manager().is_sharing_active():
                for qs, qe, qwh, mask in _active_analyst_shapes(service_id):
                    if not must_run:
                        stopped_for = _running_ingest(service_id)
                        if stopped_for is not None:
                            logger.info(
                                "[insights-prewarmer] %s: stopping early — %s started mid-tick",
                                service_id,
                                stopped_for,
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
                            qs,
                            qe,
                            qwh,
                            baseline_hours=baseline_hours,
                            window_hours=window_hours,
                        )
                    except ValueError:
                        continue  # empty window for this shape — nothing to warm
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
            # The warmed shape is in the summary so cron_runs alone can confirm
            # the prewarm matches what the page requests (the drift this cron
            # once had was invisible in an "admin + N analyst" summary).
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
