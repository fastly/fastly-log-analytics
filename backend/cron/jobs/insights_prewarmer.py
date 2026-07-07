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

Active-request gate (#84) intentionally NOT applied here — the
prewarmer's whole point is to win during quiet moments; it doesn't
contend with user traffic the way sync/optimize do (no FOS calls,
just read-only DuckDB queries against the local Iceberg view).
"""

from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime

from backend.cron.decorators import cron_task
from backend.cron.scheduler import _display_label

logger = logging.getLogger("backend.scheduler")

# Cap on distinct analyst clamp-shapes warmed per tick — bounds background
# compute (each shape is a full recompute). Prod typically has 1. Truncation
# is logged, never silent.
_MAX_ANALYST_SHAPES = 8


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


@cron_task("insights_prewarmer")
def _run_insights_prewarmer(service_id: str) -> None:
    """Warm the default insights selection — the pair the adaptive frontend
    picker will request for this service's history, for the admin/unclamped
    entry plus each active analyst clamp shape — so the first user (admin OR
    analyst) lands on a cache hit instead of the cold path."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service, log_cron_run, start_cron_run
    from backend.repositories.insights import get_insights
    from backend.utils.insights_defaults import history_hours_from_earliest, pick_insights_default
    from backend.utils.remote_access import resolve_analyst_insights_clamp
    from backend.utils.tunnel import get_tunnel_manager

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "insights_prewarmer")
    except RuntimeError as e:
        logger.info("⏭️  [insights-prewarmer] %s: skipping — %s", service_id, str(e))
        return

    _display = _display_label(src, service_id)

    started = time.time()
    con = None
    try:
        # Same cached snapshot the clients' picker reads (bootstrap
        # log_extents / /api/log-extents both project svcconfig status,
        # un-clamped for analysts) → same bucket → same cache key.
        cached_status = svcconfig.get_status(src["name"]) or {}
        window_hours, baseline_hours = pick_insights_default(
            history_hours_from_earliest(cached_status.get("earliest_log_at"))
        )

        # Read-only + skip_view_update: insights queries against the live
        # Iceberg view; a few seconds of view staleness across cron-tick
        # boundaries is fine here (the next prewarmer tick will resolve
        # newly-bound view tables anyway).
        con = get_connection(source=src, max_wait=5, read_only=True, skip_view_update=True)

        # 1) Admin / unclamped default selection.
        get_insights(
            con,
            src,
            window_hours=window_hours,
            baseline_hours=baseline_hours,
            force_refresh=True,
        )

        # 2) Analyst clamp shapes — only meaningful while sharing is live (no
        #    analyst can reach /api/insights otherwise). Warm each active
        #    invite's (window-params, mask_ips) shape under the SAME stable key
        #    a live analyst request derives.
        analyst_warmed = 0
        if _analyst_prewarm_enabled() and get_tunnel_manager().is_sharing_active():
            for qs, qe, qwh, mask in _active_analyst_shapes(service_id):
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
                    clamp_start=cs,
                    clamp_end=ce,
                    mask_ips=mask,
                    clamp_cache_key=ck,
                    force_refresh=True,
                )
                analyst_warmed += 1

        duration = time.time() - started
        # The warmed shape is in the summary so cron_runs alone can confirm
        # the prewarm matches what the page requests (the drift this cron
        # once had was invisible in an "admin + N analyst" summary).
        summary = (
            f"Prewarmed default insights selection ({window_hours:g}h/{baseline_hours:g}h: "
            f"admin + {analyst_warmed} analyst shape{'' if analyst_warmed == 1 else 's'})"
        )
        log_cron_run(
            src,
            "insights_prewarmer",
            duration,
            "success",
            summary=summary,
            run_id=run_id,
        )
        logger.info(
            "✅ [insights-prewarmer] %s: prewarmed %gh/%gh in %.2fs (admin + %d analyst)",
            _display,
            window_hours,
            baseline_hours,
            duration,
            analyst_warmed,
        )
    except Exception as e:
        duration = time.time() - started
        log_cron_run(
            src,
            "insights_prewarmer",
            duration,
            "error",
            summary=f"Prewarm failed: {e}",
            error_message=str(e),
            run_id=run_id,
        )
        logger.warning("⚠️  [insights-prewarmer] %s: %s", _display, e)
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
