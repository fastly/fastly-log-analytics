"""Partial-hour merge cron — the fast (30s) tick that folds newly-landed
buffer/active-hour-partition files into the pod-local partial-hour rollup.

Structurally mirrors ``backend/cron/jobs/compaction.py::_run_local_compact``
(same active-request defer gate, same start_cron_run/log_cron_run lease
contract) but on its own faster cadence instead of piggybacking onto
local_compact's 2 min tick.
"""

from __future__ import annotations

import logging

from backend.cron.decorators import cron_task
from backend.cron.lifecycle import cron_run
from backend.cron.scheduler import _display_label

logger = logging.getLogger("backend.scheduler")


@cron_task("partial_hour_merge", job_name="partial_hour_merge")
def _run_partial_hour_merge(service_id: str) -> None:
    from backend.core.duckdb import get_source_for_service
    from backend.core.field_registry import derived
    from backend.core.rollups.partial_hour import gc_stale_partial_hours, merge_partial_hour
    from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS
    from backend.repositories.dashboard import FIELDS
    from backend.utils.active_requests import should_defer_cron

    if should_defer_cron("partial_hour_merge", service_id):
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    with cron_run(
        service_id,
        "partial_hour_merge",
        source=src,
        skip_log_prefix="partial-hour",
        error_summary="partial-hour merge failed",
    ) as run:
        if run.skipped:
            return

        logger.info("🕐  [partial-hour] %s: partial-hour merge started.", _display_label(src, service_id))
        # Only fields execute_top_n_rollups's own live top-up would bother
        # aggregating (M1, final whole-branch review) — the full FIELDS list
        # includes high-cardinality identifier/raw-measurement columns
        # (_LIVE_TOPN_SKIP_FIELDS) with no top-N panel, so aggregating them
        # every 30s bought nothing any reader consumes.
        #
        # Also exclude EVERY derived field (vcl=None: METRICS, VIRTUAL, or
        # INTERNAL group) outright — this writer has no special handling for
        # any of them (no CSV-unnest builder, no bot-cache join, nothing; it
        # only ever does a plain SELECT <field> against the raw/buffer
        # parquet). _LIVE_TOPN_SKIP_FIELDS alone isn't enough here: it's a
        # hand-maintained deny-list for a DIFFERENT purpose (real per-row
        # columns that aren't useful top-N facets), so a derived field simply
        # never added to it slips straight through as an "actual" field. Two
        # live incidents this way in the same tick: "requests" (METRICS) then
        # "_bot_name" (VIRTUAL, not in the narrow _VIRTUAL_FIELDS tuple) — see
        # AGENTS.md Trap #40.
        derived_fields = {f.code for f in derived()}
        merge_fields = [f for f in FIELDS if f not in _LIVE_TOPN_SKIP_FIELDS and f not in derived_fields]

        stats = merge_partial_hour(service_id, src, merge_fields)
        removed = gc_stale_partial_hours(src)
        # NOTE (C2, final whole-branch review): this tick deliberately does
        # NOT call mark_rollup_coverage_ready. A successful tick — even a
        # real one — only proves the partial-hour writer path is alive, not
        # that this pod's local closed-hour rollup tree reflects a genuine
        # backfill pass; the 6 durable-mode trust gates need the latter.
        # That flag is now only raised by main.py's startup catch-up and by
        # _run_rollup_hour_heal's success path (both call
        # backfill_missing_hour_bundles, the real coverage-establishing
        # function).
        summary = f"Merged {stats['new_files']} new file(s) into hour={stats['hour']}; GC'd {removed} stale hour(s)"
        # run.success() finalizes through log_cron_run first — it releases
        # the job_runs lease the same way a real tick does, which the NEXT
        # tick's start_cron_run depends on to avoid spuriously seeing this
        # one as still "running".
        run.success(summary)
        if stats["new_files"]:
            run.log_progress({"type": "status", "message": summary})
