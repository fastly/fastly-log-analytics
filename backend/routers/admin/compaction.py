"""Compaction, DuckLake storage, rollups, and metadata-retention/storage/cleanup admin endpoints."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import Depends, HTTPException, Query

from backend.core.rollups import day_bundles as _day_bundles
from backend.core.rollups.time_series import build_time_series_bundles
from backend.deps import get_source, require_admin
from backend.models.admin import (
    BackfillBundleRollupsResponse,
    CompactionStatsResponse,
    ConsolidateRollupsResponse,
    ExpireSnapshotsResponse,
    LocalCompactNowResponse,
    MetadataRetentionResponse,
    MetadataRetentionUpdate,
    MetadataRetentionValues,
    MetadataStorageResponse,
    OptimizeNowResponse,
    PartialHourMergeResponse,
    PartialHourStatusResponse,
    RebuildTimeSeriesRollupsRequest,
    RebuildTimeSeriesRollupsResponse,
    ResetLogsRequest,
    RollupCompactResponse,
    RollupHealResponse,
    RollupStatusResponse,
    StorageTableInfoResponse,
    SyncStartResponse,
)
from backend.models.admin_actions import StorageCalendarResponse
from backend.utils.date_utils import parse_day_key_strict, parse_hour_key_strict
from backend.utils.router_utils import (
    make_error,
    query_errors,
    require_json_content_type,
)

from ._helpers import service_id_from_source
from ._router import router

logger = logging.getLogger(__name__)


@router.post(
    "/admin/optimize-now",
    response_model=OptimizeNowResponse,
    response_model_exclude_unset=True,
)
def optimize_now(
    source: dict[str, Any] = Depends(get_source),
    min_files: int | None = Query(
        default=None, description="Override auto-derived threshold. Pass 1 for max-aggressive cleanup."
    ),
) -> OptimizeNowResponse:
    """Trigger an immediate DuckLake table optimize (compaction) pass.
    Bypasses the nightly cron schedule for ad-hoc cleanup. Returns the
    optimize_table result dict (files_rewritten / files_added / etc).
    Writes through to FOS — use ``/admin/local-compact-now`` for the
    free local-only equivalent.
    """
    from backend.core import storage as _storage

    res = _storage.optimize_table(source, min_files_per_partition=min_files)
    if isinstance(res, dict):
        return OptimizeNowResponse(**res)
    return OptimizeNowResponse(files_rewritten=0, files_added=0)


@router.post(
    "/admin/optimize/{service_id}",
    response_model=OptimizeNowResponse,
    response_model_exclude_unset=True,
)
def optimize_service(
    service_id: str,
    source: dict[str, Any] = Depends(get_source),
) -> OptimizeNowResponse:
    """Trigger an immediate cloud lakehouse optimization pass for service_id.
    Executes DuckLake durability flush, small-file merge, and data file compaction.
    Logs execution in cron_runs and returns execution outcome.
    """
    from backend.cron.jobs.maintenance import _run_optimize

    sid = service_id_from_source(source, service_id, prefer_name=True)
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
    source: dict[str, Any] = Depends(get_source),
) -> ExpireSnapshotsResponse:
    """Trigger an immediate DuckLake retention and snapshot-expiry pass."""
    from backend.cron.jobs.maintenance import _run_expire_snapshots

    result = _run_expire_snapshots(service_id, manual=True)
    return ExpireSnapshotsResponse(**result)


@router.post(
    "/admin/backfill-bundle-rollups",
    response_model=BackfillBundleRollupsResponse,
    response_model_exclude_unset=True,
)
def backfill_bundle_rollups(source: dict[str, Any] = Depends(get_source)) -> BackfillBundleRollupsResponse:
    """One-shot self-heal for the slow_urls + origin_summary per-hour
    bundle rollups AND the origin_summary per-day compaction.
    """
    from backend.core import rollups
    from backend.core.rollups._common import (
        SESSIONS_BUNDLE_FILENAME,
        TIME_SERIES_BUNDLE_FILENAME,
        backfill_missing_bundles,
    )
    from backend.core.rollups.recompute import ADMIN_BACKFILLS, DAY_COMPACTORS
    from backend.core.rollups.time_series import build_session_bundles

    sid = service_id_from_source(source)
    n_ts = backfill_missing_bundles(
        sid,
        source,
        bundle_filename=TIME_SERIES_BUNDLE_FILENAME,
        label="time_series",
        builder=build_time_series_bundles,
        logger=logger,
    )
    n_sess = backfill_missing_bundles(
        sid,
        source,
        bundle_filename=SESSIONS_BUNDLE_FILENAME,
        label="sessions",
        builder=build_session_bundles,
        logger=logger,
    )

    counts: dict[str, int] = {key: getattr(rollups, name)(sid, source) for key, name in ADMIN_BACKFILLS}
    for spec in DAY_COMPACTORS:
        n_days = getattr(rollups, spec.compactor)(sid, source)
        if spec.admin_key:
            counts[spec.admin_key] = n_days

    n_rum = 0
    try:
        from contextlib import closing

        from backend.core.duckdb import get_connection, rum_source_for
        from backend.core.rollups.rum import recompute_rum_aggregates, table_exists
        from backend.repositories import rum as rum_repo

        rum_src = rum_source_for(source)
        rum_cm = get_connection(rum_src, read_only=False)
        with rum_cm if hasattr(rum_cm, "__enter__") else closing(rum_cm) as rum_con:
            recompute_rum_aggregates(rum_con, sid)
            if table_exists(rum_con, "rum_vitals_aggregates"):
                bucket_count = rum_repo.count_aggregate_buckets(rum_con, sid)
                if bucket_count is not None:
                    n_rum = bucket_count or 0
    except Exception as rum_backfill_err:
        logger.warning("RUM aggregates backfill failed: %s", rum_backfill_err, exc_info=True)

    return BackfillBundleRollupsResponse(time_series=n_ts, sessions=n_sess, rum=n_rum, **counts)


@router.post(
    "/admin/rebuild-time-series-rollups",
    response_model=RebuildTimeSeriesRollupsResponse,
    response_model_exclude_unset=True,
)
def rebuild_time_series_rollups(
    payload: RebuildTimeSeriesRollupsRequest,
    _admin: None = Depends(require_admin),
    source: dict[str, Any] = Depends(get_source),
) -> RebuildTimeSeriesRollupsResponse:
    """Rebuild selected closed UTC hours and raw-backed, complete daily bundles."""
    if len(set(payload.hours)) != len(payload.hours):
        raise HTTPException(status_code=400, detail=make_error("duplicate_rollup_hours"))
    if len(set(payload.days)) != len(payload.days):
        raise HTTPException(status_code=400, detail=make_error("duplicate_rollup_days"))

    try:
        hour_starts = [parse_hour_key_strict(hour) for hour in payload.hours]
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=make_error("invalid_rollup_hour")) from exc

    try:
        day_starts = [parse_day_key_strict(day) for day in payload.days]
        if any(start.strftime("%Y-%m-%d") != day for day, start in zip(payload.days, day_starts, strict=True)):
            raise ValueError("invalid rollup day")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=make_error("invalid_rollup_day")) from exc

    active_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    if any(hour_start >= active_hour for hour_start in hour_starts):
        raise HTTPException(status_code=400, detail=make_error("rollup_hour_not_closed"))
    if any(day_start >= active_hour.replace(hour=0) for day_start in day_starts):
        raise HTTPException(status_code=400, detail=make_error("rollup_day_not_closed"))

    sid = service_id_from_source(source)
    rebuilt = build_time_series_bundles(sid, source, payload.hours) if payload.hours else 0
    rebuilt_days = 0
    if payload.days:
        try:
            rebuilt_days = _day_bundles.rebuild_time_series_days(sid, source, payload.days)
        except RuntimeError as exc:
            logger.error("time_series_day_rebuild_failed", exc_info=True)
            raise HTTPException(status_code=500, detail=make_error("time_series_day_rebuild_failed")) from exc
    logger.info(
        "time_series_rollup_rebuild service_id=%s requested_hours=%s rebuilt=%d rebuilt_days=%d",
        sid,
        payload.hours,
        rebuilt,
        rebuilt_days,
    )
    result = RebuildTimeSeriesRollupsResponse(
        service_id=sid,
        requested_hours=payload.hours,
        rebuilt=rebuilt,
    )
    if payload.days:
        result.requested_days = payload.days
        result.rebuilt_days = rebuilt_days
    return result


@router.post(
    "/admin/consolidate-rollups",
    response_model=ConsolidateRollupsResponse,
    response_model_exclude_unset=True,
)
def consolidate_rollups(source: dict[str, Any] = Depends(get_source)) -> ConsolidateRollupsResponse:
    """Consolidate historical fragmented rollup files into daily bundles and prune obsolete files."""
    import time

    from backend.core.rollups import (
        _rollups_root,
        backfill_day_bundles,
        compact_closed_days_to_daily,
    )
    from backend.core.rollups._common import _ip_spread_root
    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread
    from backend.core.rollups.recompute import cleanup_old_rollups

    sid = service_id_from_source(source)
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

    days_compacted = compact_closed_days_to_daily(sid, source)
    days_bundled = backfill_day_bundles(sid, source)

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
    source: dict[str, Any] = Depends(get_source),
) -> RollupCompactResponse:
    """Manually trigger daily rollup compaction for closed days over a 30-day window."""
    sid = service_id_from_source(source, service_id, prefer_name=True)
    from backend.cron.jobs.compaction import _run_rollup_compact_daily

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
    source: dict[str, Any] = Depends(get_source),
) -> RollupHealResponse:
    """Manually trigger hourly rollup healing for missing closed-hour bundles over 48h."""
    sid = service_id_from_source(source, service_id)
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
    source: dict[str, Any] = Depends(get_source),
) -> RollupStatusResponse:
    """Inspect the status of day and hour rollup bundles."""
    sid = service_id_from_source(source, service_id)
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
    source: dict[str, Any] = Depends(get_source),
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
) -> LocalCompactNowResponse:
    """Trigger an immediate local-only parquet compaction pass."""
    from backend.core.storage import _core as _lc

    res = _lc.compact_local_partitions(source, min_files_per_partition=min_files, dry_run=dry_run)
    return LocalCompactNowResponse(**res)


@router.get("/admin/compaction-stats", response_model=CompactionStatsResponse)
@router.get("/admin/compaction-status", response_model=CompactionStatsResponse)
@router.get("/admin/compaction-status/{service_id}", response_model=CompactionStatsResponse)
def compaction_stats(
    service_id: str | None = None, source: dict[str, Any] = Depends(get_source)
) -> CompactionStatsResponse:
    """Snapshot of file-count distribution across local cache partitions."""
    from backend.core.storage import _core as _lc

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
    source: dict[str, Any] = Depends(get_source),
) -> PartialHourMergeResponse:
    """Trigger an immediate partial-hour merge tick for the active UTC hour."""
    sid = service_id_from_source(source, service_id)
    from backend.core.log_fields import derived
    from backend.core.rollups.partial_hour import gc_stale_partial_hours, merge_partial_hour
    from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS
    from backend.repositories.dashboard import FIELDS

    derived_fields = {f.code for f in derived()}
    merge_fields = [f for f in FIELDS if f not in _LIVE_TOPN_SKIP_FIELDS and f not in derived_fields]

    stats = merge_partial_hour(sid, source, merge_fields)
    removed = gc_stale_partial_hours(source)
    return PartialHourMergeResponse(
        service_id=sid,
        hour=stats.get("hour"),
        new_files=stats.get("new_files", 0),
        duration_ms=stats.get("duration_ms", 0.0),
        gc_hours_removed=removed,
        status="success",
    )


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
    source: dict[str, Any] = Depends(get_source),
) -> PartialHourStatusResponse:
    """Inspect the status of the current active hour's partial rollup."""
    from backend.core.rollups.partial_hour import (
        _all_fields_path,
        read_partial_hour_total,
        read_partial_hour_watermark,
    )

    sid = service_id_from_source(source, service_id)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    watermark = read_partial_hour_watermark(source, active_hour)
    file_exists = os.path.isfile(_all_fields_path(source, active_hour))
    total_rows = read_partial_hour_total(source, active_hour)
    watermark_iso = datetime.fromtimestamp(watermark, tz=UTC).isoformat() if watermark > 0 else None

    return PartialHourStatusResponse(
        service_id=sid,
        hour=active_hour,
        watermark=watermark,
        watermark_iso=watermark_iso,
        file_exists=file_exists,
        total_rows=total_rows,
    )


@router.patch(
    "/admin/metadata-retention",
    response_model=MetadataRetentionResponse,
    response_model_exclude_unset=True,
)
def update_metadata_retention(
    body: MetadataRetentionUpdate, source: dict[str, Any] = Depends(get_source)
) -> MetadataRetentionResponse:
    """Update the per-service ``metadata_retention`` config block."""
    from backend.core import metadata as _mdb
    from backend.core.metadata import DEFAULT_METADATA_RETENTION, is_ingested_files_dedup_active
    from backend.utils.router_utils import load_service_config, mutate_service_config

    service_id = source["name"]
    load_service_config(service_id)

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

    def _apply(cfg: dict[str, Any]) -> None:
        merged = dict(cfg.get("metadata_retention") or {})
        merged.update(updates)
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

    return MetadataRetentionResponse(retention=MetadataRetentionValues(**{**DEFAULT_METADATA_RETENTION, **current}))


@router.get(
    "/admin/metadata-storage",
    response_model=MetadataStorageResponse,
    response_model_exclude_unset=True,
)
def metadata_storage(source: dict[str, Any] = Depends(get_source)) -> MetadataStorageResponse:
    """Per-table row count + estimated bytes for this service's metadata.db."""
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
    ingested_files_locked = not is_ingested_files_dedup_active(service_id)
    return MetadataStorageResponse(
        **stats,
        retention=MetadataRetentionValues(**retention),
        ingested_files_locked=ingested_files_locked,
    )


@router.post("/admin/metadata-cleanup")
@router.post("/admin/metadata-cleanup/{service_id}")
def metadata_cleanup_now(
    service_id: str | None = None,
    source: dict[str, Any] = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> Any:
    """Trigger an immediate metadata cleanup, streaming progress as SSE."""
    import time as _t

    from backend import config as svcconfig
    from backend.core.duckdb import log_cron_run, start_cron_run
    from backend.core.metadata import cleanup_metadata
    from backend.utils.sse import stream_sse_from_worker

    service_id = service_id_from_source(source, service_id, prefer_name=True)
    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}

    def worker(events: Any) -> None:
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


# ── Storage admin endpoints ──────────────────


@router.get("/admin/storage-info", response_model=StorageTableInfoResponse)
@query_errors(status_code=500)
def storage_info_endpoint(
    table_name: Literal["logs", "client_vitals", "client_errors"] = "logs",
    source: dict[str, Any] = Depends(get_source),
) -> StorageTableInfoResponse:
    """Return DuckLake table metadata: snapshots, data files, size, buffer status."""
    from backend.core import storage as db_storage

    result = db_storage.get_table_info(source, table_name=table_name)
    return StorageTableInfoResponse.with_telemetry(**result)


@router.get("/admin/storage-calendar", response_model=StorageCalendarResponse, response_model_exclude_unset=True)
@query_errors(status_code=500)
def storage_calendar_endpoint(
    table_name: Literal["logs", "client_vitals", "client_errors"] = "logs",
    source: dict[str, Any] = Depends(get_source),
) -> Any:
    """Return per-date data file counts from DuckLake file metadata."""
    from backend.core import storage as db_storage
    from backend.utils.telemetry import debug_envelope

    result = db_storage.get_snapshot_calendar(source, table_name=table_name)
    return debug_envelope(result, gated=True, is_cached=None, inject_calls=True)


@router.post(
    "/admin/commit-storage",
    status_code=202,
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
@router.post(
    "/admin/commit/{service_id}",
    status_code=202,
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def storage_commit_endpoint(
    service_id: str | None = None,
    _admin: None = Depends(require_admin),
    source: dict[str, Any] = Depends(get_source),
) -> SyncStartResponse:
    """Manually flush the local buffer to the DuckLake table."""
    from backend.cron.jobs.sync import _run_commit
    from backend.utils.router_utils import run_admin_cron_trigger

    res = run_admin_cron_trigger(
        source,
        "commit",
        _run_commit,
        target_kwargs={"force": True},
        success_msg="Commit started.",
        in_progress_msg="Commit already running.",
        invalidate_cache=False,
    )
    if isinstance(res, SyncStartResponse):
        return res
    return SyncStartResponse(**res)


@router.post(
    "/admin/rebuild-local-view", status_code=202, response_model=SyncStartResponse, response_model_exclude_unset=True
)
def rebuild_local_view_endpoint(source: dict[str, Any] = Depends(get_source)) -> SyncStartResponse:
    """One-button "fix it" for a stuck or stale local DuckDB view."""
    import threading

    from backend.core import storage as db_storage
    from backend.core.duckdb import refresh_config_status
    from backend.cron.jobs.sync import refresh_view_and_warm_pool
    from backend.repositories.dashboard import invalidate_service

    service_id = source["name"]
    db_storage.clear_source_caches(service_id)

    def _rebuild() -> None:
        try:
            refresh_view_and_warm_pool(source, service_id, log_prefix="rebuild: ")
            refresh_config_status(service_id)
            invalidate_service(service_id)
        except Exception:
            logger.exception("rebuild-local-view failed service_id=%s", service_id)

    threading.Thread(target=_rebuild, name=f"rebuild-local-view:{service_id}", daemon=True).start()
    return SyncStartResponse(ok=True, message="Local view rebuild started.")


def _stream_reset_sse(
    body: ResetLogsRequest,
    source: dict[str, Any],
    reset_fn: Any,
    label: str,
    **extra_kwargs: Any,
) -> Any:
    from backend.utils.sse import stream_sse_from_worker

    service_id = source["name"]
    if body.confirm != service_id:
        raise HTTPException(
            status_code=400,
            detail=make_error("confirm_mismatch", "`confirm` must equal the service id being deleted."),
        )

    def worker(events: Any) -> None:
        from backend.cron.scheduler import get_scheduler

        try:
            for event in reset_fn(
                service_id,
                delete_raw_logs=body.delete_raw_logs,
                reload_scheduler=lambda: get_scheduler().reload(),
                **extra_kwargs,
            ):
                events.put(event)
        except Exception as e:
            logger.exception("reset_%s_endpoint(%s) failed", label.lower(), service_id)
            events.put({"type": "error", "message": f"{label} deletion failed: {e}"})

    return stream_sse_from_worker(worker)


@router.post("/admin/reset-logs", dependencies=[Depends(require_json_content_type)])
def reset_logs_endpoint(body: ResetLogsRequest, source: dict[str, Any] = Depends(get_source)) -> Any:
    """Permanently delete this service's log data (cloud + local) over SSE."""
    from backend.core.duckdb import reset_service_logs

    return _stream_reset_sse(
        body,
        source,
        reset_service_logs,
        "Log",
        preserve_usage_history=body.preserve_usage_history,
    )


@router.post("/admin/reset-rum", dependencies=[Depends(require_json_content_type)])
def reset_rum_endpoint(body: ResetLogsRequest, source: dict[str, Any] = Depends(get_source)) -> Any:
    """Permanently delete this service's RUM telemetry data (cloud + local) over SSE."""
    from backend.core.duckdb import reset_service_rum

    return _stream_reset_sse(body, source, reset_service_rum, "RUM")


# ── High-scale DuckDB emulation SQL helpers (kept in compaction.py per _INLINE_SQL_EXEMPT_ROUTERS) ──

DESCRIBE_TABLE = "DESCRIBE {table}"
EMULATION_PROBE = "SELECT 1 AS duckdb_emulation_probe"
EXTENTS = "SELECT MIN(timestamp), MAX(timestamp) FROM {table}"


def build_partition_filter_clauses(start_iso: str, end_iso: str, col_names: set[str]) -> tuple[list[str], list[str]]:
    """Return Hive partition pruning clauses and bind params for ``dt`` / ``timestamp_hour``."""
    from backend.repositories.utils.filters import _get_utc_date_str, _get_utc_hour_str

    clauses: list[str] = []
    params: list[str] = []
    start_date = _get_utc_date_str(start_iso)
    end_date = _get_utc_date_str(end_iso)
    if "dt" in col_names and len(start_date) == 10 and len(end_date) == 10:
        clauses.extend(["dt >= ?", "dt <= ?"])
        params.extend([start_date, end_date])
    start_hour = _get_utc_hour_str(start_iso)
    end_hour = _get_utc_hour_str(end_iso)
    if "timestamp_hour" in col_names and len(start_hour) == 13 and len(end_hour) == 13:
        clauses.extend(["timestamp_hour >= ?", "timestamp_hour <= ?"])
        params.extend([start_hour, end_hour])
    return clauses, params


def fetch_request_facts_emulation(
    con: Any,
    candidate_tables: list[str],
    req: Any,
    *,
    start: datetime,
    end: datetime,
    cursor_ts: Any,
    service_id: str | None = None,
) -> tuple[list[tuple[Any, ...]], Any, Any]:
    """``(page rows + 1 lookahead, coverage_start, coverage_end)`` from the first candidate table with a schema."""
    from backend.utils.date_utils import iso_z
    from backend.utils.telemetry import track_query

    table = candidate_tables[0]
    col_names: set[str] = set()
    for cand in candidate_tables:
        try:
            with track_query(con, DESCRIBE_TABLE.format(table=cand), [], "high_scale.describe_emulation") as desc_cur:
                col_names = {str(r[0]) for r in desc_cur.fetchall()}
            if col_names:
                table = cand
                break
        except Exception:
            continue

    if "timestamp" not in col_names:
        with track_query(con, EMULATION_PROBE, [], "high_scale.emulation_probe") as probe_cur:
            probe_cur.fetchone()
        return [], None, None

    country_col = (
        "country" if "country" in col_names else ("geo_country_code" if "geo_country_code" in col_names else None)
    )

    select_exprs = [
        "timestamp",
        f"{country_col} AS country" if country_col else "'' AS country",
        "client_ip" if "client_ip" in col_names else "'' AS client_ip",
        "url" if "url" in col_names else "'' AS url",
        "CAST(status AS VARCHAR) AS status" if "status" in col_names else "'' AS status",
        "pop" if "pop" in col_names else "'' AS pop",
        "host" if "host" in col_names else "'' AS host",
    ]
    part_clauses, part_params = build_partition_filter_clauses(iso_z(start), iso_z(end), col_names)
    where_parts = [*part_clauses, "timestamp >= ?", "timestamp <= ?"]
    params: list[object] = [*part_params, start, end]

    if cursor_ts is not None:
        where_parts.append("timestamp > ?")
        params.append(cursor_ts)

    if req.country is not None and str(req.country).strip():
        if country_col:
            where_parts.append(f"{country_col} = ?")
            params.append(str(req.country).strip())
        else:
            return [], None, None

    if req.status is not None and str(req.status).strip():
        if "status" in col_names:
            where_parts.append("CAST(status AS VARCHAR) = ?")
            params.append(str(req.status).strip())
        else:
            return [], None, None

    if req.pop is not None and str(req.pop).strip():
        if "pop" in col_names:
            where_parts.append("pop = ?")
            params.append(str(req.pop).strip())
        else:
            return [], None, None

    if req.domain is not None and str(req.domain).strip():
        dom_val = str(req.domain).strip()
        if "host" in col_names and "url" in col_names:
            where_parts.append("(host = ? OR url ILIKE ?)")
            params.extend([dom_val, f"%{dom_val}%"])
        elif "host" in col_names:
            where_parts.append("host = ?")
            params.append(dom_val)
        elif "url" in col_names:
            where_parts.append("url ILIKE ?")
            params.append(f"%{dom_val}%")
        else:
            return [], None, None

    if req.batch_id is not None and str(req.batch_id).strip():
        b_val = str(req.batch_id).strip()
        if b_val != "duckdb-emulation":
            return [], None, None

    limit_plus_one = int(req.limit) + 1
    request_sql = (
        f"SELECT {', '.join(select_exprs)} "
        f"FROM {table} "
        f"WHERE {' AND '.join(where_parts)} "
        f"ORDER BY timestamp ASC "
        f"LIMIT {limit_plus_one}"
    )
    with track_query(con, request_sql, params, "high_scale.request_facts_emulation") as cur:
        fetched = cur.fetchall()

    cov_start = None
    cov_end = None
    if service_id:
        try:
            from backend import config as svcconfig

            st = svcconfig.get_status(service_id) or {}
            cov_start = st.get("earliest_log_at")
            cov_end = st.get("latest_log_at")
        except Exception:
            cov_start = None
            cov_end = None

    if not cov_start or not cov_end:
        ext_sql = EXTENTS.format(table=table)
        with track_query(con, ext_sql, [], "high_scale.request_facts_extents") as ext_cur:
            ext_row = ext_cur.fetchone()
        cov_start = ext_row[0] if ext_row else None
        cov_end = ext_row[1] if ext_row else None
    return fetched, cov_start, cov_end
