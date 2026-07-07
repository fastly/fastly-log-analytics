"""Compaction + metadata-retention/storage/cleanup admin endpoints."""

from __future__ import annotations

from fastapi import Depends, Query
from sse_starlette.sse import EventSourceResponse

from backend.deps import get_source
from backend.models.admin import (
    BackfillBundleRollupsResponse,
    CompactionStatsResponse,
    LocalCompactNowResponse,
    MetadataRetentionResponse,
    MetadataStorageResponse,
    OptimizeNowResponse,
)
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS

from ._router import router


@router.post(
    "/admin/optimize-now",
    response_model=OptimizeNowResponse,
    response_model_exclude_unset=True,
)
def optimize_now(
    source: dict = Depends(get_source),
    min_files: int | None = Query(
        default=None, description="Override auto-derived threshold. Pass 1 for max-aggressive cleanup."
    ),
):
    """Trigger an immediate Iceberg table optimize (compaction) pass.
    Bypasses the nightly cron schedule for ad-hoc cleanup. Returns the
    optimize_table result dict (files_rewritten / files_added / etc).
    Writes through to FOS — use ``/admin/local-compact-now`` for the
    free local-only equivalent.
    """
    from backend.core import iceberg as _ice

    return _ice.optimize_table(source, min_files_per_partition=min_files)


@router.post(
    "/admin/backfill-bundle-rollups",
    response_model=BackfillBundleRollupsResponse,
    response_model_exclude_unset=True,
)
def backfill_bundle_rollups(source: dict = Depends(get_source)):
    """One-shot self-heal for the slow_urls + origin_summary per-hour
    bundle rollups AND the origin_summary per-day compaction.

    These rollups land via ``recompute_touched_hours`` after every cron
    tick, but historical hours that already had ``all_fields.parquet``
    BEFORE the rollups shipped never get re-touched by the ingest path.
    This endpoint walks the existing bundle tree and (re-)issues each
    rollup writer for every closed hour where the rollup file is
    missing. Then runs the origin_summary closed-day compactor so the
    summary panel's 30 d cold path opens day files (1 per closed day)
    instead of 24 hour files per day. All steps idempotent — skip
    already-built outputs.

    Must be called in-process via the running backend because DuckDB
    keeps an exclusive lock on the per-service .duckdb file. A separate
    process (docker exec / external script) hits a DBBusyError because
    even ``read_only=True`` is silently overridden to False (see
    ``backend.core.duckdb.get_connection`` docstring).

    Returns the count of bundles written per kind.
    """
    from backend.core.rollups import (
        backfill_network_rtt_bundles,
        backfill_network_speed_bundles,
        backfill_ngwaf_bots_bundles,
        backfill_origin_dims_bundles,
        backfill_origin_latency_ts_bundles,
        backfill_origin_summary_bundles,
        backfill_perf_dims_bundles,
        backfill_perf_latency_bundles,
        backfill_security_dims_bundles,
        backfill_slow_urls_bundles,
        backfill_verified_bots_ts_bundles,
        backfill_wellknown_bots_rollup,
        compact_network_rtt_closed_days_to_daily,
        compact_network_speed_closed_days_to_daily,
        compact_ngwaf_bots_closed_days_to_daily,
        compact_origin_dims_closed_days_to_daily,
        compact_origin_latency_ts_closed_days_to_daily,
        compact_origin_summary_closed_days_to_daily,
        compact_perf_dims_closed_days_to_daily,
        compact_perf_latency_closed_days_to_daily,
        compact_security_dims_closed_days_to_daily,
        compact_verified_bots_ts_closed_days_to_daily,
    )

    sid = source.get("service_id") or source.get("name") or ""
    n_su = backfill_slow_urls_bundles(sid, source)
    n_os = backfill_origin_summary_bundles(sid, source)
    # status_codes reads the existing all_fields.parquet bundle (no dedicated
    # writer / backfill); only origin_dims (pop / oip / edge) gets an explicit
    # backfill + day-compact here.
    n_od = backfill_origin_dims_bundles(sid, source)
    # origin_latency_ts: the minute-granular origin-latency percentile time
    # series feeding the timeseries panel (the last section to roll up — its
    # backfill is what lets the get_aggregates skip-temp guard fire on 30 d
    # unfiltered).
    n_olts = backfill_origin_latency_ts_bundles(sid, source)
    n_nr = backfill_network_rtt_bundles(sid, source)
    n_ns = backfill_network_speed_bundles(sid, source)
    n_vbts = backfill_verified_bots_ts_bundles(sid, source)
    n_perf = backfill_perf_latency_bundles(sid, source)
    # security_dims: req_size / conn_reuse / topips / cov — the all-rows live
    # scans behind /api/security/aggregates' equivalent panels. EXACT.
    n_sd = backfill_security_dims_bundles(sid, source)
    # perf_dims: ttl_dist — the all-rows live scan behind /api/performance
    # /aggregates' ttl_dist histogram panel. EXACT (count SUM + MIN-of-MIN).
    n_pd = backfill_perf_dims_bundles(sid, source)
    # wellknown_bots: the regex-prefiltered (ua, ip, count) rollup behind the
    # security wellknown card. Backfilling historical closed hours lets the
    # reader clear its 50% coverage floor on 7d/30d so the live regex + its
    # all-rows temp drop off the request path (collapsing the catalog temp to
    # the NGWAF-only subset).
    n_wk = backfill_wellknown_bots_rollup(sid, source)
    # ngwaf_bots: the write-time waf_req_id ⨝ ngwaf_bot_cache aggregation
    # behind get_top_bots' ngwaf panel. Backfilling closed hours lets the
    # reader serve the panel without the per-request direct join. Hours
    # whose cache rows were already retention-trimmed aggregate to empty —
    # identical to what the live join returns for them today.
    n_nb = backfill_ngwaf_bots_bundles(sid, source)
    n_os_day = compact_origin_summary_closed_days_to_daily(sid, source)
    n_od_day = compact_origin_dims_closed_days_to_daily(sid, source)
    n_olts_day = compact_origin_latency_ts_closed_days_to_daily(sid, source)
    n_nr_day = compact_network_rtt_closed_days_to_daily(sid, source)
    n_ns_day = compact_network_speed_closed_days_to_daily(sid, source)
    n_vbts_day = compact_verified_bots_ts_closed_days_to_daily(sid, source)
    n_perf_day = compact_perf_latency_closed_days_to_daily(sid, source)
    n_sd_day = compact_security_dims_closed_days_to_daily(sid, source)
    n_pd_day = compact_perf_dims_closed_days_to_daily(sid, source)
    n_nb_day = compact_ngwaf_bots_closed_days_to_daily(sid, source)
    return {
        "slow_urls": n_su,
        "origin_summary": n_os,
        "origin_summary_days": n_os_day,
        "origin_dims": n_od,
        "origin_dims_days": n_od_day,
        "origin_latency_ts": n_olts,
        "origin_latency_ts_days": n_olts_day,
        "network_rtt": n_nr,
        "network_rtt_days": n_nr_day,
        "network_speed": n_ns,
        "network_speed_days": n_ns_day,
        "verified_bots_ts": n_vbts,
        "verified_bots_ts_days": n_vbts_day,
        "perf_latency": n_perf,
        "perf_latency_days": n_perf_day,
        "security_dims": n_sd,
        "security_dims_days": n_sd_day,
        "perf_ttl_dist": n_pd,
        "perf_ttl_dist_days": n_pd_day,
        "ngwaf_bots": n_nb,
        "ngwaf_bots_days": n_nb_day,
        "wellknown_bots": n_wk,
    }


@router.post(
    "/admin/local-compact-now",
    response_model=LocalCompactNowResponse,
    response_model_exclude_unset=True,
)
def local_compact_now(
    source: dict = Depends(get_source),
    min_files: int = Query(
        default=3,
        ge=0,
        description=(
            "Compact partitions with strictly more files than this. "
            "Default 3 = normal cron behaviour. Pass 1 to dedupe the "
            "2-3-file orphan pattern. Pass 0 to force-rewrite every "
            "partition through the dedup pipeline (one-shot historical "
            "cleanup of intra-file dups in single-parquet partitions)."
        ),
    ),
    dry_run: bool = Query(default=False, description="Report what would happen without writing."),
):
    """Trigger an immediate local-only parquet compaction pass.

    Does NOT touch FOS — only rewrites files inside the local cache, so
    no 30-day-minimum billing penalty. Safe to call as often as needed.
    The 2-minute cron does this automatically; this endpoint is for
    ad-hoc cleanup.
    """
    from backend.core import local_compaction as _lc

    return _lc.compact_local_partitions(source, min_files_per_partition=min_files, dry_run=dry_run)


@router.get("/admin/compaction-stats", response_model=CompactionStatsResponse)
def compaction_stats(source: dict = Depends(get_source)) -> CompactionStatsResponse:
    """Snapshot of file-count distribution across local cache partitions.

    Useful for monitoring: rising partitions_above_3 means the local
    compaction cron has stopped keeping up; rising avg_files_per_partition
    correlates with slow dashboard scans.
    """
    from backend.core import local_compaction as _lc

    return CompactionStatsResponse(**_lc.compaction_stats(source))


@router.patch(
    "/admin/metadata-retention",
    response_model=MetadataRetentionResponse,
    response_model_exclude_unset=True,
)
def update_metadata_retention(body: dict, source: dict = Depends(get_source)):
    """Update the per-service ``metadata_retention`` config block.

    Body shape: any subset of ``{usage_log_days, ingested_files_days,
    cron_runs_days}``. Each value is coerced to int; negative / non-numeric
    inputs are clamped to 0 (which disables cleanup for that table per
    cleanup_metadata's semantics). Missing keys preserve their current
    value. Returns the resolved retention (defaults merged with cfg) so the
    UI can confirm what was saved.
    """
    from backend import config as svcconfig
    from backend.core import metadata as _mdb
    from backend.core.metadata import DEFAULT_METADATA_RETENTION
    from backend.utils.router_utils import load_service_config

    service_id = source["name"]
    cfg = load_service_config(service_id)

    from backend.core.metadata import is_ingested_files_dedup_active

    current = dict(cfg.get("metadata_retention") or {})
    for key in ("usage_log_days", "ingested_files_days", "cron_runs_days"):
        if key in body:
            try:
                v = int(body[key])
            except (TypeError, ValueError):
                v = 0
            current[key] = max(0, v)

    # Mirror the cleanup helper's safety override at the write layer:
    # if delete_after=false on this service, refuse to persist a non-zero
    # ingested_files_days. Storing it would mislead the operator into
    # thinking the value will be honored when the cleanup ignores it.
    if not is_ingested_files_dedup_active(service_id) and int(current.get("ingested_files_days") or 0) > 0:
        current["ingested_files_days"] = 0

    cfg["metadata_retention"] = current
    svcconfig.save_config(service_id, cfg)
    try:
        _mdb.record_audit(
            service_id=service_id,
            event_type="metadata_retention_update",
            details=current,
        )
    except Exception:
        pass

    return {"retention": {**DEFAULT_METADATA_RETENTION, **current}}


@router.get(
    "/admin/metadata-storage",
    response_model=MetadataStorageResponse,
    response_model_exclude_unset=True,
)
def metadata_storage(source: dict = Depends(get_source)):
    """Per-table row count + estimated bytes for this service's metadata.db.

    Includes the resolved retention policy (per-service cfg merged with
    defaults). The UI uses this to render the Metadata Storage card on
    the admin page — table sizes, bytes, and a Cleanup-now button.
    """
    from backend import config as svcconfig
    from backend.core.metadata import (
        DEFAULT_METADATA_RETENTION,
        get_metadata_storage_stats,
        is_ingested_files_dedup_active,
    )

    service_id = source["name"]
    stats = get_metadata_storage_stats(service_id)
    cfg = svcconfig.load_config(service_id) or {}
    retention = {**DEFAULT_METADATA_RETENTION, **(cfg.get("metadata_retention") or {})}
    # ingested_files_locked surfaces the safety override: when
    # cron_sync.delete_after=False the ingested_files table is the
    # dedup gate, so the cleanup helper force-disables its trimming
    # regardless of the configured retention. UI uses this to disable
    # the input + show a tooltip explaining the override.
    ingested_files_locked = not is_ingested_files_dedup_active(service_id)
    return {**stats, "retention": retention, "ingested_files_locked": ingested_files_locked}


# response_model intentionally omitted: SSE stream (EventSourceResponse),
# not a JSON body — event shapes are documented in the docstring.
@router.post("/admin/metadata-cleanup")
def metadata_cleanup_now(source: dict = Depends(get_source)):
    """Trigger an immediate metadata cleanup, streaming progress as SSE.

    Equivalent to the daily ``metadata_cleanup`` cron at 03:15 UTC but
    on-demand. The DELETE phase is fast; VACUUM rewrites the whole file
    and on a multi-GB metadata.db can take minutes. Streaming gives the
    operator real-time feedback instead of a 5-minute hang behind a
    spinning button.

    Event shapes (between SSE ``data:`` lines):

        {"type": "status",   "message": str}
        {"type": "progress", "current": int, "total": int, "message": str}
        {"type": "done",     "message": str, "result": {...}}
        {"type": "error",    "message": str}

    Writes a row to ``cron_runs`` with task=``metadata_cleanup`` so the
    manual run shows up on the Data Management schedule + history grid
    alongside the scheduled cron's runs.
    """
    import json as _json
    import queue as _queue
    import threading
    import time as _t

    from backend import config as svcconfig
    from backend.core.duckdb import log_cron_run, start_cron_run
    from backend.core.metadata import cleanup_metadata

    service_id = source["name"]
    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}

    # Bridge cleanup_metadata's on_event callback to the SSE generator via
    # a thread-safe queue. The worker thread runs the cleanup synchronously
    # (DELETE then VACUUM — both block the SQLite writer) and pushes events
    # as they happen; the streaming generator consumes them and yields SSE
    # frames. Sentinel ``None`` marks end-of-stream.
    events: _queue.Queue = _queue.Queue()

    def worker():
        started = _t.time()
        run_id = None
        try:
            run_id = start_cron_run(source, "metadata_cleanup")
            result = cleanup_metadata(service_id, retention, on_event=events.put)
        except Exception as e:
            err = str(e)
            events.put({"type": "error", "message": f"Cleanup failed: {err}"})
            try:
                log_cron_run(
                    source,
                    "metadata_cleanup",
                    _t.time() - started,
                    "error",
                    error_message=err,
                    summary=f"cleanup failed: {err}",
                    run_id=run_id,
                )
            finally:
                events.put(None)
            return

        total_deleted = sum(result["deleted"].values())
        if total_deleted:
            parts = [f"{t}={n}" for t, n in result["deleted"].items() if n]
            summary = (
                f"Trimmed {total_deleted:,} rows ({', '.join(parts)}). "
                f"VACUUM={'yes' if result['vacuumed'] else 'skipped'}."
            )
        else:
            summary = "No rows older than retention windows."
        try:
            log_cron_run(
                source,
                "metadata_cleanup",
                _t.time() - started,
                "success",
                summary=summary,
                rows_ingested=total_deleted,
                run_id=run_id,
            )
        finally:
            events.put({"type": "done", "message": summary, "result": result})
            events.put(None)

    threading.Thread(target=worker, daemon=True, name=f"metadata-cleanup-{service_id}").start()

    def stream():
        while True:
            event = events.get()
            if event is None:
                break
            yield _json.dumps(event)

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)
