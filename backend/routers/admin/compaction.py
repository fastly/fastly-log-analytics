"""Compaction + metadata-retention/storage/cleanup admin endpoints."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.deps import get_source

from ._router import router


@router.post("/admin/optimize-now")
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


@router.post("/admin/local-compact-now")
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


@router.get("/admin/compaction-stats")
def compaction_stats(source: dict = Depends(get_source)):
    """Snapshot of file-count distribution across local cache partitions.

    Useful for monitoring: rising partitions_above_3 means the local
    compaction cron has stopped keeping up; rising avg_files_per_partition
    correlates with slow dashboard scans.
    """
    from backend.core import local_compaction as _lc

    return _lc.compaction_stats(source)


@router.patch("/admin/metadata-retention")
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
    from backend.core import metadata_db as _mdb
    from backend.core.metadata_db import DEFAULT_METADATA_RETENTION

    service_id = source["name"]
    cfg = svcconfig.load_config(service_id)
    if cfg is None:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})

    from backend.core.metadata_db import is_ingested_files_dedup_active

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


@router.get("/admin/metadata-storage")
def metadata_storage(source: dict = Depends(get_source)):
    """Per-table row count + estimated bytes for this service's metadata.db.

    Includes the resolved retention policy (per-service cfg merged with
    defaults). The UI uses this to render the Metadata Storage card on
    the admin page — table sizes, bytes, and a Cleanup-now button.
    """
    from backend import config as svcconfig
    from backend.core.metadata_db import (
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
    from backend.core.metadata_db import cleanup_metadata

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
        run_id = start_cron_run(source, "metadata_cleanup")
        try:
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
        # Pre-pad to defeat any reverse-proxy / browser buffering; SSE
        # clients flush on the first blank-line delimiter.
        yield ":" + " " * 2048 + "\n\n"
        while True:
            event = events.get()
            if event is None:
                break
            yield f"data: {_json.dumps(event)}\n\n"

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
