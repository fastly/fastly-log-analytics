"""Compaction + metadata-retention/storage/cleanup admin endpoints."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

from fastapi import Depends, HTTPException, Query

from backend.deps import get_source, require_admin
from backend.models.admin import (
    BackfillBundleRollupsResponse,
    CompactionStatsResponse,
    ConsolidateRollupsResponse,
    ExpireSnapshotsResponse,
    LocalCompactNowResponse,
    MetadataRetentionResponse,
    MetadataRetentionUpdate,
    MetadataStorageResponse,
    OptimizeNowResponse,
    PartialHourMergeResponse,
    PartialHourStatusResponse,
    RollupCompactResponse,
    RollupHealResponse,
    RollupStatusResponse,
)

from ._router import router

logger = logging.getLogger(__name__)


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
    "/admin/optimize/{service_id}",
    response_model=OptimizeNowResponse,
    response_model_exclude_unset=True,
)
def optimize_service(
    service_id: str,
    source: dict = Depends(get_source),
) -> OptimizeNowResponse:
    """Trigger an immediate cloud lakehouse optimization pass for service_id.
    Executes DuckLake durability flush, small-file merge, and data file compaction.
    Logs execution in cron_runs and returns execution outcome.
    """
    from backend.cron.jobs.optimize import _run_optimize

    sid = service_id or source.get("name") or source.get("service_id", "")
    res = _run_optimize(sid, manual=True)
    if isinstance(res, dict):
        return OptimizeNowResponse(**res)
    return OptimizeNowResponse(files_rewritten=0, files_added=0)


@router.post(
    "/admin/expire-snapshots/{service_id}",
    response_model=ExpireSnapshotsResponse,
    response_model_exclude_unset=True,
)
def expire_snapshots_service(
    service_id: str,
    source: dict = Depends(get_source),
) -> ExpireSnapshotsResponse:
    """Trigger an immediate DuckLake retention and snapshot-expiry pass."""
    if source.get("access_level") == "read_only":
        raise HTTPException(status_code=403, detail="Read-only services cannot run cloud maintenance.")

    from backend.cron.jobs.expire import _run_expire_snapshots

    result = _run_expire_snapshots(service_id, manual=True)
    return ExpireSnapshotsResponse(**result)


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
    import logging

    from backend.core import rollups
    from backend.core.rollups._common import (
        SESSIONS_BUNDLE_FILENAME,
        TIME_SERIES_BUNDLE_FILENAME,
        backfill_missing_bundles,
    )
    from backend.core.rollups.sessions import build_session_bundles
    from backend.core.rollups.spec import ADMIN_BACKFILLS, DAY_COMPACTORS
    from backend.core.rollups.time_series import build_time_series_bundles

    sid = source.get("service_id") or source.get("name") or ""
    n_ts = backfill_missing_bundles(
        sid,
        source,
        bundle_filename=TIME_SERIES_BUNDLE_FILENAME,
        label="time_series",
        builder=build_time_series_bundles,
        logger=logging.getLogger(__name__),
    )
    n_sess = backfill_missing_bundles(
        sid,
        source,
        bundle_filename=SESSIONS_BUNDLE_FILENAME,
        label="sessions",
        builder=build_session_bundles,
        logger=logging.getLogger(__name__),
    )

    # The backfill table lives in backend.core.rollups.spec. status_codes has
    # no entry: it reads the existing all_fields.parquet bundle. Backfilling
    # origin_latency_ts is what lets the get_aggregates skip-temp guard fire on
    # 30 d unfiltered; wellknown_bots lets its reader clear the 50% coverage
    # floor on 7d/30d; ngwaf_bots hours whose cache rows were already
    # retention-trimmed aggregate to empty, same as the live join returns.
    counts: dict[str, int] = {key: getattr(rollups, name)(sid, source) for key, name in ADMIN_BACKFILLS}
    for spec in DAY_COMPACTORS:
        n_days = getattr(rollups, spec.compactor)(sid, source)
        if spec.admin_key:
            counts[spec.admin_key] = n_days

    # RUM aggregates backfill
    n_rum = 0
    try:
        from contextlib import closing

        from backend.core.duckdb import get_connection, rum_source_for
        from backend.core.rollups.rum import recompute_rum_aggregates, table_exists
        from backend.repositories import rum as rum_repo

        rum_src = rum_source_for(source)
        with closing(get_connection(rum_src, read_only=False)) as rum_con:
            recompute_rum_aggregates(rum_con, sid)
            if table_exists(rum_con, "rum_vitals_aggregates"):
                bucket_count = rum_repo.count_aggregate_buckets(rum_con, sid)
                if bucket_count is not None:
                    n_rum = bucket_count or 0
    except Exception as rum_backfill_err:
        logging.getLogger(__name__).warning("RUM aggregates backfill failed: %s", rum_backfill_err, exc_info=True)

    return {**counts, "rum": n_rum}


@router.post(
    "/admin/consolidate-rollups",
    response_model=ConsolidateRollupsResponse,
    response_model_exclude_unset=True,
)
def consolidate_rollups(source: dict = Depends(get_source)) -> ConsolidateRollupsResponse:
    """Consolidate historical fragmented rollup files into daily bundles and prune obsolete files.

    Sweeps hour_ip_spread, compacts closed days into day_bundled files, and prunes
    per-field and obsolete hourly files older than 14 days.
    """
    import os
    import time

    from backend.core.rollups import (
        _rollups_root,
        backfill_day_bundles,
        compact_closed_days_to_daily,
    )
    from backend.core.rollups._common import _ip_spread_root
    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread
    from backend.core.rollups.recompute import cleanup_old_rollups

    sid = source.get("service_id") or source.get("name") or ""
    t0 = time.time()

    def _count_files(root: str) -> int:
        if not os.path.isdir(root):
            return 0
        cnt = 0
        try:
            for _, _, fnames in os.walk(root):
                cnt += len(fnames)
        except OSError:
            pass
        return cnt

    cache_dir = os.path.dirname(_rollups_root(source))
    files_before = _count_files(cache_dir)

    # 1. Enumerate and bundle all hour_ip_spread hours
    ip_spread_root = _ip_spread_root(source)
    hours_to_bundle: set[str] = set()
    if os.path.isdir(ip_spread_root):
        try:
            for fe in os.listdir(ip_spread_root):
                if not fe.startswith("field="):
                    continue
                fpath = os.path.join(ip_spread_root, fe)
                try:
                    for he in os.listdir(fpath):
                        if he.startswith("hour="):
                            hours_to_bundle.add(he[5:])
                except OSError:
                    continue
        except OSError:
            pass

    ip_rebuilt = bundle_hours_ip_spread(sid, source, sorted(hours_to_bundle)) if hours_to_bundle else 0

    # 2. Compact all closed days to daily per-field and then bundle days
    days_compacted = compact_closed_days_to_daily(sid, source)
    days_bundled = backfill_day_bundles(sid, source)

    # 3. Retention & historical hourly prune
    retention_months = int(source.get("rollup_retention_months", 12))
    max_age_days = int(source.get("rollups_days", retention_months * 30))
    cleaned_entries = cleanup_old_rollups(sid, source, max_age_days=max_age_days, hour_bundle_max_age_days=14)

    files_after = _count_files(cache_dir)
    duration_s = round(time.time() - t0, 3)

    return ConsolidateRollupsResponse(
        service_id=sid,
        files_before=files_before,
        files_after=files_after,
        files_deleted=max(0, files_before - files_after),
        hours_ip_bundled=ip_rebuilt,
        days_compacted=days_compacted,
        days_bundled=days_bundled,
        cleaned_entries=cleaned_entries,
        duration_s=duration_s,
    )


@router.post(
    "/admin/rollups/compact",
    response_model=RollupCompactResponse,
    response_model_exclude_unset=True,
)
@router.post(
    "/admin/rollups/compact/{service_id}",
    response_model=RollupCompactResponse,
    response_model_exclude_unset=True,
)
def rollup_compact_now(
    service_id: str | None = None,
    source: dict = Depends(get_source),
) -> RollupCompactResponse:
    """Manually trigger daily rollup compaction for closed days over a 30-day window."""
    sid = service_id or source.get("name") or source.get("service_id")
    from backend.cron.jobs.rollup_compact import _run_rollup_compact_daily

    res = _run_rollup_compact_daily(sid, manual=True)
    return RollupCompactResponse(**res)


@router.post(
    "/admin/rollup-heal",
    response_model=RollupHealResponse,
    response_model_exclude_unset=True,
)
@router.post(
    "/admin/rollup-heal/{service_id}",
    response_model=RollupHealResponse,
    response_model_exclude_unset=True,
)
@router.post(
    "/admin/rollups/heal",
    response_model=RollupHealResponse,
    response_model_exclude_unset=True,
)
@router.post(
    "/admin/rollups/heal/{service_id}",
    response_model=RollupHealResponse,
    response_model_exclude_unset=True,
)
def rollup_heal_now(
    service_id: str | None = None,
    source: dict = Depends(get_source),
) -> RollupHealResponse:
    """Manually trigger hourly rollup healing for missing closed-hour bundles over 48h."""
    sid = service_id or source.get("service_id") or source.get("name") or ""
    from backend.cron.jobs.compaction import _run_rollup_hour_heal

    res = _run_rollup_hour_heal(sid, manual=True)
    return RollupHealResponse(**res)


@router.get(
    "/admin/rollups/status",
    response_model=RollupStatusResponse,
    response_model_exclude_unset=True,
)
@router.get(
    "/admin/rollups/status/{service_id}",
    response_model=RollupStatusResponse,
    response_model_exclude_unset=True,
)
def rollup_status(
    service_id: str | None = None,
    source: dict = Depends(get_source),
) -> RollupStatusResponse:
    """Inspect the status of day and hour rollup bundles."""
    sid = service_id or source.get("service_id") or source.get("name") or ""
    from backend.core.rollups._common import _day_bundled_root, _hour_bundled_root

    day_root = _day_bundled_root(source)
    hour_root = _hour_bundled_root(source)
    days: list[str] = []
    if os.path.isdir(day_root):
        for entry in os.listdir(day_root):
            if entry.startswith("day="):
                days.append(entry[len("day=") :])
            elif entry.startswith("day_bundle_") and entry.endswith(".parquet"):
                cand = entry[len("day_bundle_") : -len(".parquet")]
                if len(cand) == 10 and cand not in days:
                    days.append(cand)
    days.sort()
    hour_cnt = 0
    if os.path.isdir(hour_root):
        for entry in os.listdir(hour_root):
            if entry.startswith("hour="):
                hour_cnt += 1
    return RollupStatusResponse(
        service_id=sid,
        days_bundled=len(days),
        hour_bundles=hour_cnt,
        latest_day_bundle=days[-1] if days else None,
        earliest_day_bundle=days[0] if days else None,
    )


@router.post(
    "/admin/local-compact-now",
    response_model=LocalCompactNowResponse,
    response_model_exclude_unset=True,
)
@router.post(
    "/admin/compact/{service_id}",
    response_model=LocalCompactNowResponse,
    response_model_exclude_unset=True,
)
def local_compact_now(
    service_id: str | None = None,
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
@router.get("/admin/compaction-status", response_model=CompactionStatsResponse)
@router.get("/admin/compaction-status/{service_id}", response_model=CompactionStatsResponse)
def compaction_stats(service_id: str | None = None, source: dict = Depends(get_source)) -> CompactionStatsResponse:
    """Snapshot of file-count distribution across local cache partitions.

    Useful for monitoring: rising partitions_above_3 means the local
    compaction cron has stopped keeping up; rising avg_files_per_partition
    correlates with slow dashboard scans.
    """
    from backend.core import local_compaction as _lc

    return CompactionStatsResponse(**_lc.compaction_stats(source))


@router.post(
    "/admin/partial-hour-merge",
    response_model=PartialHourMergeResponse,
    response_model_exclude_unset=True,
)
@router.post(
    "/admin/partial-hour-merge/{service_id}",
    response_model=PartialHourMergeResponse,
    response_model_exclude_unset=True,
)
def partial_hour_merge_now(
    service_id: str | None = None,
    source: dict = Depends(get_source),
):
    """Trigger an immediate partial-hour merge tick for the active UTC hour.

    Folds newly-arrived buffer parquets into the active hour's partial-hour rollup.
    Local-only operation; zero FOS egress.
    """
    sid = service_id or source.get("service_id", source.get("name", ""))
    from backend.core.field_registry import derived
    from backend.core.rollups.partial_hour import gc_stale_partial_hours, merge_partial_hour
    from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS
    from backend.repositories.dashboard import FIELDS

    derived_fields = {f.code for f in derived()}
    merge_fields = [f for f in FIELDS if f not in _LIVE_TOPN_SKIP_FIELDS and f not in derived_fields]

    stats = merge_partial_hour(sid, source, merge_fields)
    removed = gc_stale_partial_hours(source)
    return {
        "service_id": sid,
        "hour": stats.get("hour"),
        "new_files": stats.get("new_files", 0),
        "duration_ms": stats.get("duration_ms", 0.0),
        "gc_hours_removed": removed,
        "status": "success",
    }


@router.get(
    "/admin/partial-hour-status",
    response_model=PartialHourStatusResponse,
    response_model_exclude_unset=True,
)
@router.get(
    "/admin/partial-hour-status/{service_id}",
    response_model=PartialHourStatusResponse,
    response_model_exclude_unset=True,
)
def partial_hour_status(
    service_id: str | None = None,
    source: dict = Depends(get_source),
):
    """Inspect the status of the current active hour's partial rollup."""
    from backend.core.rollups.partial_hour import (
        _all_fields_path,
        read_partial_hour_total,
        read_partial_hour_watermark,
    )

    sid = service_id or source.get("service_id", source.get("name", ""))
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    watermark = read_partial_hour_watermark(source, active_hour)
    file_exists = os.path.isfile(_all_fields_path(source, active_hour))
    total_rows = read_partial_hour_total(source, active_hour)
    watermark_iso = datetime.fromtimestamp(watermark, tz=UTC).isoformat() if watermark > 0 else None

    return {
        "service_id": sid,
        "hour": active_hour,
        "watermark": watermark,
        "watermark_iso": watermark_iso,
        "file_exists": file_exists,
        "total_rows": total_rows,
    }


@router.patch(
    "/admin/metadata-retention",
    response_model=MetadataRetentionResponse,
    response_model_exclude_unset=True,
)
def update_metadata_retention(body: MetadataRetentionUpdate, source: dict = Depends(get_source)):
    """Update the per-service ``metadata_retention`` config block.

    Body shape: any subset of ``{usage_log_days, ingested_files_days,
    cron_runs_days}``. Each value is coerced to int; negative / non-numeric
    inputs are clamped to 0 (which disables cleanup for that table per
    cleanup_metadata's semantics). Missing keys preserve their current
    value. Returns the resolved retention (defaults merged with cfg) so the
    UI can confirm what was saved.
    """
    from backend.core import metadata as _mdb
    from backend.core.metadata import DEFAULT_METADATA_RETENTION
    from backend.utils.router_utils import load_service_config, mutate_service_config

    service_id = source["name"]
    load_service_config(service_id)

    from backend.core.metadata import is_ingested_files_dedup_active

    raw_body = body.model_dump(exclude_unset=True) if hasattr(body, "model_dump") else dict(body)
    updates: dict[str, int] = {}
    for key in ("usage_log_days", "ingested_files_days", "cron_runs_days"):
        if key in raw_body:
            try:
                v = int(raw_body[key])
            except (TypeError, ValueError):
                v = 0
            updates[key] = max(0, v)

    dedup_active = is_ingested_files_dedup_active(service_id)

    def _apply(cfg: dict) -> None:
        merged = dict(cfg.get("metadata_retention") or {})
        merged.update(updates)
        # Mirror the cleanup helper's safety override at the write layer:
        # if delete_after=false on this service, refuse to persist a non-zero
        # ingested_files_days. Storing it would mislead the operator into
        # thinking the value will be honored when the cleanup ignores it.
        if not dedup_active and int(merged.get("ingested_files_days") or 0) > 0:
            merged["ingested_files_days"] = 0
        cfg["metadata_retention"] = merged

    current = mutate_service_config(service_id, _apply)["metadata_retention"]
    try:
        _mdb.record_audit(
            service_id=service_id,
            event_type="metadata_retention_update",
            details=current,
        )
    except Exception:
        logger.warning("update_metadata_retention: _mdb.record_audit failed (ignored)", exc_info=True)

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
@router.post("/admin/metadata-cleanup/{service_id}")
def metadata_cleanup_now(
    service_id: str | None = None,
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
):
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
    import time as _t

    from backend import config as svcconfig
    from backend.core.duckdb import log_cron_run, start_cron_run
    from backend.core.metadata import cleanup_metadata
    from backend.routers.admin._helpers import stream_sse_from_worker

    service_id = service_id or source.get("name") or source.get("service_id", "")
    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}

    def worker(events):
        started = _t.time()
        run_id = None
        try:
            run_id = start_cron_run(source, "metadata_cleanup")
            result = cleanup_metadata(service_id, retention, on_event=events.put)
        except Exception as e:
            err = str(e)
            events.put({"type": "error", "message": f"Cleanup failed: {err}"})
            log_cron_run(
                source,
                "metadata_cleanup",
                _t.time() - started,
                "error",
                error_message=err,
                summary=f"cleanup failed: {err}",
                run_id=run_id,
            )
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

    return stream_sse_from_worker(worker)
