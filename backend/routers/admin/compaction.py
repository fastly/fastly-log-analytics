"""Compaction, Iceberg/DuckLake, bot-sources, and metadata-retention/storage/cleanup admin endpoints."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import Depends, HTTPException, Query

from backend.core.rollups.day_bundles import rebuild_time_series_days
from backend.core.rollups.time_series import build_time_series_bundles
from backend.deps import get_source, require_admin
from backend.models.admin import (
    BackfillBundleRollupsResponse,
    BotSourcesResponse,
    CompactionStatsResponse,
    ConsolidateRollupsResponse,
    ExpireSnapshotsResponse,
    IcebergTableInfoResponse,
    LocalCompactNowResponse,
    MetadataRetentionResponse,
    MetadataRetentionUpdate,
    MetadataStorageResponse,
    OptimizeNowResponse,
    PartialHourMergeResponse,
    PartialHourStatusResponse,
    PopLocationsResponse,
    RebuildTimeSeriesRollupsRequest,
    RebuildTimeSeriesRollupsResponse,
    RefreshPopLocationsRequest,
    ResetLogsRequest,
    RollupCompactResponse,
    RollupHealResponse,
    RollupStatusResponse,
    SyncStartResponse,
)
from backend.models.admin_actions import (
    BotSourceRefreshResponse,
    BotSourcesRefreshResponse,
    IcebergCalendarResponse,
    NgwafSyncResponse,
    RdnsEnrichResponse,
)
from backend.models.admin_status import NgwafStatusResponse, RdnsStatsResponse
from backend.utils.date_utils import parse_day_key_strict, parse_hour_key_strict
from backend.utils.router_utils import (
    bad_request,
    load_service_config,
    make_error,
    not_found,
    query_errors,
    raise_internal,
    require_json_content_type,
    validation_failed,
)

from .health import _fetch_file_to_zip, router, service_id_from_source, stream_zip

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
    from backend.cron.jobs.metadata import _run_optimize

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
    source: dict = Depends(get_source),
) -> ExpireSnapshotsResponse:
    """Trigger an immediate DuckLake retention and snapshot-expiry pass."""
    if source.get("access_level") == "read_only":
        raise HTTPException(status_code=403, detail="Read-only services cannot run cloud maintenance.")

    from backend.cron.jobs.metadata import _run_expire_snapshots

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
    from backend.core import rollups
    from backend.core.rollups._common import (
        SESSIONS_BUNDLE_FILENAME,
        TIME_SERIES_BUNDLE_FILENAME,
        backfill_missing_bundles,
    )
    from backend.core.rollups.recompute import ADMIN_BACKFILLS, DAY_COMPACTORS
    from backend.core.rollups.time_series import build_session_bundles, build_time_series_bundles

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
        rum_cm = get_connection(rum_src, read_only=False)
        with rum_cm if hasattr(rum_cm, "__enter__") else closing(rum_cm) as rum_con:
            recompute_rum_aggregates(rum_con, sid)
            if table_exists(rum_con, "rum_vitals_aggregates"):
                bucket_count = rum_repo.count_aggregate_buckets(rum_con, sid)
                if bucket_count is not None:
                    n_rum = bucket_count or 0
    except Exception as rum_backfill_err:
        logger.warning("RUM aggregates backfill failed: %s", rum_backfill_err, exc_info=True)

    return {"time_series": n_ts, "sessions": n_sess, **counts, "rum": n_rum}


@router.post(
    "/admin/rebuild-time-series-rollups",
    response_model=RebuildTimeSeriesRollupsResponse,
    response_model_exclude_unset=True,
)
def rebuild_time_series_rollups(
    payload: RebuildTimeSeriesRollupsRequest,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
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
            rebuilt_days = rebuild_time_series_days(sid, source, payload.days)
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
def consolidate_rollups(source: dict = Depends(get_source)) -> ConsolidateRollupsResponse:
    """Consolidate historical fragmented rollup files into daily bundles and prune obsolete files.

    Sweeps hour_ip_spread, compacts closed days into day_bundled files, and prunes
    per-field and obsolete hourly files older than 14 days.
    """
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
    sid = service_id_from_source(source, service_id, prefer_name=True)
    from backend.cron.jobs.metadata import _run_rollup_compact_daily

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
    sid = service_id_from_source(source, service_id)
    from backend.cron.jobs.metadata import _run_rollup_hour_heal

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
    from backend.core.iceberg import _core as _lc

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
    from backend.core.iceberg import _core as _lc

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
    sid = service_id_from_source(source, service_id)
    from backend.core.log_fields import derived
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

    sid = service_id_from_source(source, service_id)
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
    from backend.utils.sse import stream_sse_from_worker

    service_id = service_id_from_source(source, service_id, prefer_name=True)
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


# ── Bot-sources admin endpoints (folded from bot_sources.py) ──────────


@router.get("/admin/pop-locations", response_model=PopLocationsResponse)
def get_pop_locations():
    """Return the cached POP locations (code, name, coordinates)."""
    from backend.utils.pop_utils import get_pop_locations

    return PopLocationsResponse.with_telemetry(pops=get_pop_locations())


@router.post("/admin/pop-locations/refresh", response_model=PopLocationsResponse)
def refresh_pop_locations(req: RefreshPopLocationsRequest | None = None, token: str | None = Query(default=None)):
    """Refresh the POP locations cache from the Fastly API."""
    api_key = ""
    if req is not None:
        api_key = req.token.strip()

    if not api_key:
        if token is None:
            raise HTTPException(status_code=422, detail=validation_failed("token_required", ["token is required"]))
        api_key = token.strip()
        if not api_key:
            raise HTTPException(status_code=400, detail=bad_request("api_key_required"))

    from backend.utils.pop_utils import fetch_pop_locations, get_pop_locations

    ok = fetch_pop_locations(api_key)
    if not ok:
        # Custom envelope: 502 still surfaces a user-friendly toast string
        # (frontend extractApiError reads detail.error verbatim) alongside
        # the machine-readable code.
        raise HTTPException(
            status_code=502,
            detail={
                "error": "Failed to fetch POP data from Fastly API. Check your API key.",
                "code": "fastly_pop_fetch_failed",
            },
        )
    return PopLocationsResponse.with_telemetry(pops=get_pop_locations())


@router.post("/admin/ngwaf/sync/{service_id}", response_model=NgwafSyncResponse, response_model_exclude_unset=True)
@query_errors(502, code="ngwaf_sync_failed", logger_obj=logger)
def trigger_ngwaf_sync_endpoint(service_id: str):
    """Trigger manual NGWAF bot sync for a service."""
    from backend import config as svcconfig
    from backend.cron.jobs.metadata import _run_ngwaf_bot_sync
    from backend.utils.ngwaf import get_cache_stats

    load_service_config(service_id)

    workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
    if not workspace_id:
        raise HTTPException(status_code=400, detail=bad_request("NGWAF workspace not configured for this service"))

    _run_ngwaf_bot_sync(service_id)
    return {"ok": True, "service_id": service_id, "workspace_id": workspace_id, "stats": get_cache_stats()}


@router.get("/admin/ngwaf/status", response_model=NgwafStatusResponse, response_model_exclude_unset=True)
def get_ngwaf_status_endpoint(service_id: str | None = Query(None, description="Optional service ID")):
    """Return NGWAF cache statistics and workspace sync watermarks."""
    from backend import config as svcconfig
    from backend.utils.ngwaf import get_cache_stats

    stats = get_cache_stats()
    if service_id:
        workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
        stats["service_workspace_id"] = workspace_id
        stats["service_configured"] = bool(workspace_id)
    return {"ok": True, **stats}


@router.get("/admin/bot-sources", response_model=BotSourcesResponse)
def get_bot_sources_endpoint():
    """Return metadata for all bot sources plus rDNS cache stats."""
    from backend.utils.bot_sources import get_all_sources_meta
    from backend.utils.bot_sources import get_stats as rdns_stats

    return BotSourcesResponse.with_telemetry(sources=get_all_sources_meta(), rdns=rdns_stats())


@router.post(
    "/admin/bot-sources/{source_id}/refresh", response_model=BotSourceRefreshResponse, response_model_exclude_unset=True
)
def refresh_bot_source_endpoint(source_id: str):
    """Fetch and re-cache a single bot source."""
    from backend.utils.bot_sources import fetch_and_cache_source

    try:
        meta = fetch_and_cache_source(source_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=not_found("bot_source_not_found"))
    except Exception as e:
        raise_internal(logger, e, code="bot_source_fetch_failed", status=502)
    return {"ok": True, "source": meta}


@router.post("/admin/bot-sources/refresh", response_model=BotSourcesRefreshResponse, response_model_exclude_unset=True)
@router.post("/admin/bots/refresh", response_model=BotSourcesRefreshResponse, response_model_exclude_unset=True)
def refresh_all_bot_sources_endpoint():
    """Fetch and re-cache all enabled bot sources."""
    from backend.utils.bot_sources import refresh_all_sources

    try:
        results = refresh_all_sources()
        failed = [r for r in results if r.get("failed")]
        succeeded = [r for r in results if not r.get("failed")]
        return {
            "ok": len(failed) == 0,
            "sources": results,
            "updated_count": len(succeeded),
            "failed_count": len(failed),
        }
    except Exception as e:
        raise_internal(logger, e, code="bot_sources_refresh_failed", status=502)


@router.post("/admin/rdns/enrich", response_model=RdnsEnrichResponse, response_model_exclude_unset=True)
def trigger_rdns_enrich_endpoint(
    limit: int | None = Query(None, description="Optional override for batch limit"),
):
    """Trigger manual rDNS enrichment batch."""
    from backend.utils.bot_sources import enrich_batch

    summary = enrich_batch(limit=limit)
    return {"ok": True, "summary": summary}


@router.get("/admin/rdns/stats", response_model=RdnsStatsResponse, response_model_exclude_unset=True)
def get_rdns_stats_endpoint():
    """Return rDNS cache stats."""
    from backend.utils.bot_sources import get_stats as rdns_stats

    return {"ok": True, "stats": rdns_stats()}


# ── Iceberg admin endpoints (folded from iceberg.py) ──────────────────


@router.get("/admin/iceberg-info", response_model=IcebergTableInfoResponse)
@query_errors(status_code=500)
def iceberg_info_endpoint(
    table_name: Literal["logs", "client_vitals", "client_errors"] = "logs",
    source: dict = Depends(get_source),
):
    """Return Iceberg table metadata: snapshots, data files, size, buffer status."""
    from backend.core import iceberg as db_iceberg

    result = db_iceberg.get_table_info(source, table_name=table_name)
    return IcebergTableInfoResponse.with_telemetry(**result)


@router.get("/admin/iceberg-calendar", response_model=IcebergCalendarResponse, response_model_exclude_unset=True)
@query_errors(status_code=500)
def iceberg_calendar_endpoint(
    table_name: Literal["logs", "client_vitals", "client_errors"] = "logs",
    source: dict = Depends(get_source),
):
    """Return per-date data file counts from Iceberg partition metadata."""
    from backend.core import iceberg as db_iceberg
    from backend.utils.telemetry import debug_envelope

    result = db_iceberg.get_snapshot_calendar(source, table_name=table_name)
    return debug_envelope(result, gated=True, is_cached=None, inject_calls=True)


@router.post(
    "/admin/commit-iceberg",
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
def iceberg_commit_endpoint(
    service_id: str | None = None,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    """Manually flush the local buffer to the DuckLake table.

    Returns 202 Accepted — the commit runs asynchronously in a worker
    thread and the response body carries a ``run_id`` the caller can
    poll for completion. 200 (the prior status) misled clients that
    treated it as "done"."""
    from backend.cron.jobs.sync import _run_commit
    from backend.utils.router_utils import run_admin_cron_trigger

    return run_admin_cron_trigger(
        source,
        "commit",
        _run_commit,
        target_kwargs={"force": True},
        success_msg="Commit started.",
        in_progress_msg="Commit already running.",
        readonly_detail=make_error("read_only", "Read-only services cannot run DuckLake commit."),
        invalidate_cache=False,
    )


@router.post(
    "/admin/rebuild-local-view", status_code=202, response_model=SyncStartResponse, response_model_exclude_unset=True
)
def rebuild_local_view_endpoint(source: dict = Depends(get_source)):
    """One-button "fix it" for a stuck or stale local DuckDB view.

    Returns 202 Accepted — the rebuild runs asynchronously in a worker
    thread and the response body carries a ``run_id`` the caller can
    poll for completion.

    Clears the in-memory + on-disk caches that drive view SQL generation,
    then triggers a metadata_sync that re-pulls the catalog from the cloud
    and rebuilds the view. The local raw buffer is NOT touched —
    un-committed data is safe.

    When to use: after manually editing parquet files, after a catalog
    schema-mapping desync, or when "Sync All" already ran and the view
    still looks wrong. This is the nuclear-option version of refresh.
    """
    import threading

    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import start_cron_run
    from backend.cron.jobs.metadata import _run_metadata_sync
    from backend.cron_progress import start_progress

    service_id = source["name"]

    db_iceberg.clear_source_caches(service_id)

    try:
        # Deliberate divergence from router_utils.start_or_resume_cron: this
        # path does NOT resume an in-flight run on RuntimeError — it 503s
        # cron_busy instead. A rebuild is the nuclear "fix it" button, so
        # piggy-backing on an existing run_id would silently no-op the
        # cache-clearing the operator explicitly asked for.
        run_id = start_cron_run(source, "metadata_sync")
    except RuntimeError as e:
        logger.warning("cron_busy service_id=%s: %s", service_id, e)
        raise HTTPException(
            status_code=503,
            detail=make_error("cron_busy", "A sync run is already in progress", busy=True),
        ) from e

    start_progress(run_id, service_id=service_id, task="metadata_sync")
    t = threading.Thread(target=_run_metadata_sync, args=(service_id,), kwargs={"run_id": run_id}, daemon=True)
    t.start()
    return {"ok": True, "message": "Local view rebuild started.", "run_id": run_id}


def _stream_reset_sse(
    body: ResetLogsRequest,
    source: dict,
    reset_fn: Any,
    label: str,
    **extra_kwargs: Any,
):
    from backend.utils.sse import stream_sse_from_worker

    service_id = source["name"]
    if source.get("access_level") != "read_write":
        raise HTTPException(status_code=403, detail=make_error("read_only", "This service is read-only."))
    if body.confirm != service_id:
        raise HTTPException(
            status_code=400,
            detail=make_error("confirm_mismatch", "`confirm` must equal the service id being deleted."),
        )

    def worker(events):
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


# response_model intentionally omitted: SSE progress stream (EventSourceResponse),
# not a JSON body — event shapes are documented in reset_service_logs's docstring.
@router.post("/admin/reset-logs", dependencies=[Depends(require_json_content_type)])
def reset_logs_endpoint(body: ResetLogsRequest, source: dict = Depends(get_source)):
    """Permanently delete this service's log data (cloud + local) over SSE.

    Wipes the cloud Iceberg log table, the local DuckDB file + cache, and
    the SQLite ingestion ledgers, then re-initializes an empty, queryable
    0-state. Preserves the service's configuration: saved views, alerts,
    source registration, audit history, and scoring labels/audit. See
    ``backend.core.reset.reset_service_logs`` for the full sequence.

    Fails closed before any work starts: 403 if this service isn't
    read_write (defense-in-depth — ``RemoteAccessMiddleware`` already blocks
    ``/api/admin/*`` for remote analysts), and 400 if ``confirm`` doesn't
    match the resolved service id.
    """
    from backend.core.duckdb import reset_service_logs

    return _stream_reset_sse(
        body,
        source,
        reset_service_logs,
        "Log",
        preserve_usage_history=body.preserve_usage_history,
    )


@router.post("/admin/reset-rum", dependencies=[Depends(require_json_content_type)])
def reset_rum_endpoint(body: ResetLogsRequest, source: dict = Depends(get_source)):
    """Permanently delete this service's RUM telemetry data (cloud + local) over SSE.

    Wipes the cloud Iceberg RUM tables and the local RUM DuckDB file and cache
    while leaving logs data completely untouched.
    """
    from backend.core.duckdb import reset_service_rum

    return _stream_reset_sse(body, source, reset_service_rum, "RUM")


# ── Ingest, Quarantine & Downloads (folded from backend.routers.admin.ingest) ──

"""Manual ingest trigger + ingest quarantine evidence + download endpoints."""


import logging
import sys
from collections.abc import Iterator

from fastapi import Depends, Query
from fastapi.responses import StreamingResponse

from backend.config import path_within_dir
from backend.core.metadata import ingest_log as _quarantine_store
from backend.core.metadata.ingest_log import (
    count_quarantine_evidence,
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    list_quarantine_evidence,
)
from backend.deps import get_source, require_admin
from backend.models.admin import (
    QuarantineDeleteResponse,
    QuarantineEvidenceItem,
    QuarantineListResponse,
    QuarantineSummary,
    SyncStartResponse,
    TreeResponse,
)
from backend.utils.router_utils import (
    query_errors,
    require_service_id,
    run_admin_cron_trigger,
)

logger = logging.getLogger(__name__)


@router.post(
    "/admin/sync/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def sync_service(
    service_id: str,
    force: bool = Query(default=True, description="Force sync even if throttled or intervals not elapsed"),
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_log_discovery_cron

    return run_admin_cron_trigger(
        source,
        "log_discovery",
        _run_log_discovery_cron,
        target_kwargs={"force": force, "start_time": start_time, "end_time": end_time},
        success_msg="Ingestion started.",
        in_progress_msg="Ingestion already running.",
        readonly_detail="Read-only services cannot run log discovery.",
    )


@router.post(
    "/admin/full-sweep/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def full_sweep_service(
    service_id: str,
    force: bool = Query(default=True, description="Force full sweep even if active requests or throttled"),
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_full_sweep

    return run_admin_cron_trigger(
        source,
        "full_sync",
        _run_full_sweep,
        target_kwargs={"force": force},
        success_msg="Full sweep started.",
        in_progress_msg="Full sweep already running.",
        readonly_detail="Read-only services cannot run full sweep.",
    )


@router.post(
    "/admin/gap-heal/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def gap_heal_service(
    service_id: str,
    force: bool = Query(default=True, description="Force gap heal evaluation even if throttled or active requests"),
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_gap_heal

    return run_admin_cron_trigger(
        source,
        "gap_heal",
        _run_gap_heal,
        target_kwargs={"force": force},
        success_msg="Gap heal evaluation started.",
        in_progress_msg="Gap heal already running.",
        readonly_detail="Read-only services cannot run gap heal.",
    )


@router.post(
    "/admin/ledger/sweep/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def ledger_sweep_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_ledger_sweep

    return run_admin_cron_trigger(
        source,
        "ledger_sweep",
        _run_ledger_sweep,
        success_msg="Ledger sweep started.",
        in_progress_msg="Ledger sweep already running.",
        readonly_detail="Read-only services cannot run ledger sweep.",
        require_high_scale=True,
        high_scale_detail="Ledger sweep is only supported under high-scale mode.",
    )


@router.post(
    "/admin/rum/sync/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def rum_sync_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_rum_sync

    return run_admin_cron_trigger(
        source,
        "rum_sync",
        _run_rum_sync,
        success_msg="RUM sync started.",
        in_progress_msg="RUM sync already running.",
        readonly_detail="Read-only services cannot run RUM sync.",
    )


@router.post(
    "/admin/rum/discovery/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def rum_discovery_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_rum_discovery_cron

    return run_admin_cron_trigger(
        source,
        "rum_discovery",
        _run_rum_discovery_cron,
        success_msg="RUM discovery started.",
        in_progress_msg="RUM discovery already running.",
        readonly_detail="Read-only services cannot run RUM discovery.",
        require_high_scale=True,
        high_scale_detail="RUM discovery is only supported under high-scale mode.",
    )


@router.post(
    "/admin/rum/ledger/sweep/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def rum_ledger_sweep_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend import config as svcconfig
    from backend.cron.jobs.sync import _run_rum_ledger_sweep

    def _precheck() -> None:
        try:
            cfg = load_service_config(service_id)
        except HTTPException:
            raise HTTPException(
                status_code=400,
                detail=make_error("service_not_found", "Service configuration not found."),
            ) from None
        if not svcconfig.is_rum_enabled(cfg):
            raise HTTPException(
                status_code=400,
                detail=make_error("rum_not_enabled", "RUM is not enabled for this service."),
            )

    return run_admin_cron_trigger(
        source,
        "ledger_rum_sweep",
        _run_rum_ledger_sweep,
        success_msg="RUM ledger sweep started.",
        in_progress_msg="RUM ledger sweep already running.",
        readonly_detail=make_error("admin_only", "Read-only services cannot run RUM ledger sweep."),
        require_high_scale=True,
        high_scale_detail=make_error(
            "high_scale_required", "RUM ledger sweep is only supported under high-scale mode."
        ),
        precheck=_precheck,
    )


@router.post("/admin/ingest-logs", response_model=SyncStartResponse, response_model_exclude_unset=True)
def ingest_endpoint(
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    source: dict = Depends(get_source),
) -> dict:
    from backend.cron.jobs.metadata import _run_metadata_sync
    from backend.cron.jobs.sync import _run_log_discovery_cron
    from backend.repositories.dashboard import invalidate_service
    from backend.utils.router_utils import start_or_resume_cron

    src = source
    invalidate_service(src["name"])
    if source.get("access_level") == "read_only":
        return start_or_resume_cron(
            source,
            "metadata_sync",
            _run_metadata_sync,
            target_kwargs={"start_time": start_time, "end_time": end_time},
            success_msg="Metadata sync started.",
            in_progress_msg="Metadata sync already running.",
        )
    return start_or_resume_cron(
        src,
        "log_discovery",
        _run_log_discovery_cron,
        target_kwargs={"force": True, "start_time": start_time, "end_time": end_time},
        success_msg="Ingestion started.",
        in_progress_msg="Ingestion already running.",
    )


# ── Ingest quarantine evidence endpoints (folded from quarantine.py) ──

_MAX_EVIDENCE_DOWNLOAD_BYTES = 50 * 1024 * 1024


def _service_id_from_source(source: dict) -> str:
    return str(source.get("service_id") or source.get("name", ""))


def _require_read_write_source(source: dict) -> None:
    if source.get("access_level", "read_write") != "read_write":
        raise HTTPException(
            status_code=403,
            detail=make_error("admin_only", "This operation requires admin access."),
        )


def _fetch_quarantine_list_payload(
    service_id: str,
    *,
    limit: int,
    offset: int,
    error_category: str | None,
    source_type: str | None,
) -> dict:
    items = list_quarantine_evidence(
        service_id,
        limit=limit,
        offset=offset,
        error_category=error_category,
        source_type=source_type,
    )
    for item in items:
        item["quarantined_at"] = str(item["quarantined_at"])
    total = count_quarantine_evidence(
        service_id,
        error_category=error_category,
        source_type=source_type,
    )
    return {
        "items": [QuarantineEvidenceItem(**item) for item in items],
        "total": total,
    }


@router.get("/admin/quarantine", response_model=QuarantineListResponse)
def list_quarantine(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    error_category: str | None = Query(default=None, min_length=1, max_length=64),
    source_type: str | None = Query(default=None, min_length=1, max_length=32),
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    service_id = _service_id_from_source(source)
    return _fetch_quarantine_list_payload(
        service_id,
        limit=limit,
        offset=offset,
        error_category=error_category,
        source_type=source_type,
    )


@router.get("/admin/ledger/quarantine", response_model=QuarantineListResponse)
def list_ledger_quarantine(
    service_id: str | None = Query(default=None, description="Service ID to inspect quarantine for"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    error_category: str | None = Query(default=None, min_length=1, max_length=64),
    source_type: str | None = Query(default=None, min_length=1, max_length=32),
    _admin: None = Depends(require_admin),
) -> dict:
    from backend.core.duckdb import get_source_for_service

    service_id = require_service_id(service_id)
    src = get_source_for_service(service_id)
    if not src:
        raise HTTPException(
            status_code=404,
            detail=make_error("service_not_found", f"Service {service_id} not found."),
        )
    _require_read_write_source(src)
    return _fetch_quarantine_list_payload(
        service_id,
        limit=limit,
        offset=offset,
        error_category=error_category,
        source_type=source_type,
    )


@router.get("/admin/quarantine/summary", response_model=QuarantineSummary)
def quarantine_summary(
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    return get_quarantine_evidence_summary(_service_id_from_source(source))


@router.get("/admin/quarantine/download/{item_id}")
def download_quarantine_evidence(
    item_id: int,
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> StreamingResponse:
    _require_read_write_source(source)
    service_id = _service_id_from_source(source)
    try:
        result = _quarantine_store.read_evidence(service_id, item_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=500,
            detail=make_error("quarantine_evidence_invalid", "Stored quarantine evidence has an invalid path."),
        ) from exc
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("quarantine_not_found", f"Quarantine item {item_id} not found."),
        )
    item, evidence_file = result
    actual_size = evidence_file.seek(0, 2)
    evidence_file.seek(0)
    if item["byte_length"] > _MAX_EVIDENCE_DOWNLOAD_BYTES or actual_size > _MAX_EVIDENCE_DOWNLOAD_BYTES:
        evidence_file.close()
        raise HTTPException(
            status_code=413,
            detail=make_error(
                "quarantine_evidence_too_large", "Quarantine evidence exceeds the 50 MiB download limit."
            ),
        )

    def _stream() -> Iterator[bytes]:
        try:
            while chunk := evidence_file.read(65536):
                yield chunk
        finally:
            evidence_file.close()

    return StreamingResponse(
        _stream(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="quarantine-item-{item_id}.dat"'},
    )


@router.post("/admin/quarantine/purge", response_model=QuarantineDeleteResponse)
def purge_quarantine_evidence(
    item_id: int | None = Query(default=None, gt=0),
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict[str, int]:
    _require_read_write_source(source)
    service_id = _service_id_from_source(source)
    if item_id is not None and get_quarantine_evidence(service_id, item_id) is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("quarantine_not_found", f"Quarantine item {item_id} not found."),
        )
    return _quarantine_store.purge_evidence(service_id, item_id)


# ── Download endpoints (folded from downloads.py) ─────────────────────────────


@router.get("/admin/raw-tree", response_model=TreeResponse)
def raw_tree_endpoint(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
) -> TreeResponse:
    from backend.core import duckdb as _db_mod

    _db: Any = _db_mod
    result = _db.get_raw_tree_node(source, prefix, root="raw")
    resp: TreeResponse = TreeResponse.with_telemetry(nodes=result.get("children", []))
    return resp


@router.get("/admin/iceberg-tree", response_model=TreeResponse)
def iceberg_tree_endpoint(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
) -> TreeResponse:
    from backend.core import duckdb as _db_mod

    _db: Any = _db_mod
    result = _db.get_raw_tree_node(source, prefix, root="iceberg")
    resp: TreeResponse = TreeResponse.with_telemetry(nodes=result.get("children", []))
    return resp


@router.get("/download-folder")
def download_folder(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
    root: str = Query(default="raw"),
) -> StreamingResponse:
    from backend.core import duckdb as _db

    prefix = prefix.strip("/")
    base_prefix = source.get("prefix", "").strip().rstrip("/")
    if base_prefix:
        target_prefix = f"{base_prefix}/{root}/{prefix}" if prefix else f"{base_prefix}/{root}/"
    else:
        target_prefix = f"{root}/{prefix}" if prefix else f"{root}/"

    if not target_prefix.endswith("/"):
        target_prefix += "/"

    def populate(zf: Any) -> None:
        cdn = source.get("cdn_url", "").rstrip("/")
        fos_client = _db._get_fos_client(source)
        paginator = fos_client.get_paginator("list_objects_v2", caller_hint="download_zip")
        pages = paginator.paginate(Bucket=source["bucket"], Prefix=target_prefix)

        for page in pages:
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith("/"):  # Skip directory markers
                    continue

                top_folder = os.path.basename(prefix) if prefix else root
                rel_path = key[len(target_prefix) :]
                arcname = f"{top_folder}/{rel_path}" if rel_path else os.path.basename(key)

                from backend.routers import admin as _admin_pkg

                _fn = _fetch_file_to_zip
                _orig = getattr(sys.modules.get("backend.routers.admin.health"), "_fetch_file_to_zip", None)
                if _fn is _orig:
                    _fn = getattr(_admin_pkg, "_fetch_file_to_zip", _fn)
                _fn(source, fos_client, cdn, key, arcname, zf, "download_zip")

    safe_name = prefix.replace("/", "_") or root
    headers = {
        "Content-Disposition": f'attachment; filename="{safe_name}.zip"',
    }

    return StreamingResponse(
        stream_zip(f"api:GET /admin/download-zip:{root}", source.get("name", ""), populate),
        media_type="application/zip",
        headers=headers,
    )


@router.get("/download")
@query_errors(status_code=500)
def download_file(
    source: dict[str, Any] = Depends(get_source),
    key: str = Query(default=""),
) -> Any:
    import posixpath
    import urllib.parse

    from fastapi.responses import FileResponse

    from backend.core.duckdb import _cache_dir, _get_fos_client

    if not key:
        raise HTTPException(status_code=400, detail={"error": "Missing key parameter"})

    key = posixpath.normpath(key)

    src_prefix = source.get("prefix", "")
    if src_prefix:
        if not src_prefix.endswith("/"):
            src_prefix += "/"
        if not key.startswith(src_prefix):
            raise HTTPException(status_code=400, detail={"error": "invalid_key"})

    cache_dir = os.path.realpath(_cache_dir(source))
    candidate = os.path.realpath(os.path.join(cache_dir, key))
    if not path_within_dir(cache_dir, candidate):
        raise HTTPException(status_code=400, detail={"error": "invalid_key"})
    local_path = candidate
    if os.path.exists(local_path):
        return FileResponse(local_path, filename=os.path.basename(local_path))

    from backend.utils.telemetry import record_call as _record_call

    cdn = source.get("cdn_url", "").rstrip("/")
    if cdn:
        import time as _time
        import urllib.request

        from backend.utils.telemetry import record_cdn_call as _rcdn

        url = f"{cdn}/{urllib.parse.quote(key)}"
        req = urllib.request.Request(url)
        if source.get("cdn_secret"):
            req.add_header("x-fastly-key", source["cdn_secret"])
        try:
            cdn_resp = urllib.request.urlopen(req, timeout=30)
        except Exception as exc:
            raise_internal(logger, exc, code="cdn_fetch_failed", status=502)

        try:
            content_type = cdn_resp.headers.get("Content-Type") or "application/octet-stream"
            content_length = cdn_resp.headers.get("Content-Length")
            filename = os.path.basename(key) or "download"
        except Exception:
            try:
                cdn_resp.close()
            except Exception:
                logger.debug("download_file: cdn_resp.close failed (ignored)", exc_info=True)
            raise

        def _iter_cdn(chunk_size: int = 65536) -> Iterator[bytes]:
            bytes_read = 0
            t0 = _time.time()
            cdn_headers = cdn_resp.headers
            try:
                while True:
                    chunk = cdn_resp.read(chunk_size)
                    if not chunk:
                        break
                    bytes_read += len(chunk)
                    yield chunk
            finally:
                try:
                    cdn_resp.close()
                except Exception:
                    logger.debug("_iter_cdn: cdn_resp.close failed (ignored)", exc_info=True)
                try:
                    _rcdn(
                        "GET",
                        key,
                        round((_time.time() - t0) * 1000, 2),
                        headers=cdn_headers,
                        bytes_count=bytes_read,
                        caller="api:/download",
                    )
                except Exception:
                    logger.debug("_iter_cdn: _rcdn failed (ignored)", exc_info=True)

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "private, no-store",
        }
        if content_length:
            headers["Content-Length"] = content_length
        return StreamingResponse(_iter_cdn(), media_type=content_type, headers=headers)

    fos_client = _get_fos_client(source)
    import time as _time

    try:
        t0 = _time.time()
        obj = fos_client.get_object(Bucket=source["bucket"], Key=key)
        _record_call(
            "GET_OBJECT",
            f"{source['bucket']}/{key}",
            round((_time.time() - t0) * 1000, 2),
            status="SUCCESS",
            service="FOS",
            details="download stream · Class B",
            caller="api:/download",
        )
    except Exception as exc:
        raise_internal(logger, exc, code="fos_fetch_failed", status=502)

    body = obj["Body"]
    content_type = obj.get("ContentType") or "application/octet-stream"
    content_length = obj.get("ContentLength")
    filename = os.path.basename(key) or "download"

    def _iter_fos(chunk_size: int = 65536) -> Iterator[bytes]:
        try:
            yield from body.iter_chunks(chunk_size)
        finally:
            try:
                body.close()
            except Exception:
                logger.debug("_iter_fos: body.close failed (ignored)", exc_info=True)

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "private, no-store",
    }
    if content_length:
        headers["Content-Length"] = str(content_length)

    return StreamingResponse(_iter_fos(), media_type=content_type, headers=headers)


@router.get("/download-all")
def download_all_files(
    source: dict[str, Any] = Depends(get_source),
    include: str = Query(default="all"),
) -> StreamingResponse:
    from backend.core import duckdb as _db

    src = source
    service_id = require_service_id(src.get("name"))

    def populate(zf: Any) -> None:
        if include == "local":
            db_path = src.get("duckdb_path")
            if not db_path:
                from backend import config as svcconfig

                db_path = svcconfig.duckdb_path(service_id)
            if db_path and os.path.exists(db_path):
                zf.write(db_path, os.path.basename(db_path))

            cache_dir = os.path.realpath(_db._cache_dir(src))
            walk_dir = os.path.realpath(
                os.path.join(cache_dir, src.get("prefix", "").lstrip("/")) if src.get("prefix") else cache_dir
            )
            if not path_within_dir(cache_dir, walk_dir):
                raise ValueError("invalid prefix")

            if os.path.exists(walk_dir):
                for root_dir, _, files in os.walk(walk_dir):
                    for file in files:
                        file_path = os.path.join(root_dir, file)
                        arcname = os.path.relpath(file_path, cache_dir)
                        zf.write(file_path, arcname)
        else:
            cdn = src.get("cdn_url", "").rstrip("/")
            fos_client = _db._get_fos_client(src)
            paginator = fos_client.get_paginator("list_objects_v2", caller_hint="download_all")
            pages = paginator.paginate(Bucket=src["bucket"], Prefix=src.get("prefix", ""))

            for page in pages:
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    key = obj["Key"]
                    from backend.routers import admin as _admin_pkg

                    _fn = _fetch_file_to_zip
                    _orig = getattr(sys.modules.get("backend.routers.admin.health"), "_fetch_file_to_zip", None)
                    if _fn is _orig:
                        _fn = getattr(_admin_pkg, "_fetch_file_to_zip", _fn)
                    _fn(src, fos_client, cdn, key, key, zf, "download_all")

    headers = {
        "Content-Disposition": f'attachment; filename="fastly_logs_{service_id}.zip"',
    }

    return StreamingResponse(
        stream_zip(f"api:GET /download-all:{include}", service_id, populate),
        media_type="application/zip",
        headers=headers,
    )


sys.modules["backend.routers.admin.downloads"] = sys.modules[__name__]

sys.modules["backend.routers.admin.ingest"] = sys.modules[__name__]
sys.modules["backend.routers.admin.quarantine"] = sys.modules[__name__]
sys.modules["backend.routers.admin.downloads"] = sys.modules[__name__]
sys.modules["backend.routers.admin.bot_sources"] = sys.modules[__name__]
sys.modules["backend.routers.admin.iceberg"] = sys.modules[__name__]

# ── High-scale control plane & migration endpoints (folded from high_scale.py) ──

import json
import threading
from uuid import uuid4

from fastapi import APIRouter, Depends

from backend.core import metadata
from backend.core.high_scale_sections import HighScaleSections
from backend.core.request_context import RequestContext, build_request_context
from backend.core.share_db.validation import mask_ip
from backend.deps import require_admin
from backend.high_scale._common import KeysetCursor, raw_query_producer
from backend.high_scale.aggregate_query import (
    DEFAULT_AGGREGATE_DIMENSIONS,
    AggregateRequest,
    query_clickhouse_aggregate,
)
from backend.high_scale.exports import ExportJob, ExportManager, get_export_manager
from backend.high_scale.historical_jobs import HistoricalJobManager, get_historical_job_manager
from backend.high_scale.query_router import QueryIntent, QueryRouter, QueryTier
from backend.high_scale.query_router import QueryRequest as RouterQueryRequest
from backend.high_scale.query_service import query_cmcd_facts, query_request_facts, query_rum_facts
from backend.high_scale.registry_bootstrap import (
    HighScaleService,
    HighScaleServiceRegistryProtocol,
    get_high_scale_service_registry,
)
from backend.models._base import DEFAULT_ERROR_RESPONSES, ErrorEnvelope
from backend.models.high_scale import (
    HighScaleAggregateRequest,
    HighScaleAggregateResponse,
    HighScaleAggregateValue,
    HighScaleExportCancelResponse,
    HighScaleExportRequest,
    HighScaleExportResponse,
    HighScaleHistoricalJobCancelResponse,
    HighScaleHistoricalJobResponse,
    HighScaleQueryPlanResponse,
    HighScaleQueryRequest,
    HighScaleQueryResponse,
    HighScaleRequestFactRequest,
    HighScaleRequestFactResponse,
    HighScaleRumFactResponse,
    QueryResponseMetadataResponse,
    ServingWatermarkResponse,
)
from backend.repositories.utils.filters import _get_utc_date_str, _get_utc_hour_str
from backend.utils.auth import mask_ips_for
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.date_utils import iso_z, parse_iso_utc
from backend.utils.router_utils import query_errors
from backend.utils.telemetry import track_query

high_scale_router = APIRouter(prefix="/api/high-scale", tags=["high-scale"], responses=DEFAULT_ERROR_RESPONSES)

DESCRIBE_TABLE = "DESCRIBE {table}"
EMULATION_PROBE = "SELECT 1 AS duckdb_emulation_probe"
EXTENTS = "SELECT MIN(timestamp), MAX(timestamp) FROM {table}"


def build_partition_filter_clauses(start_iso: str, end_iso: str, col_names: set[str]) -> tuple[list[str], list[str]]:
    """Return Hive partition pruning clauses and bind params for ``dt`` / ``timestamp_hour``."""
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
    con,
    candidate_tables: list[str],
    req: HighScaleRequestFactRequest,
    *,
    start: datetime,
    end: datetime,
    cursor_ts: Any,
    service_id: str | None = None,
) -> tuple[list[tuple], Any, Any]:
    """``(page rows + 1 lookahead, coverage_start, coverage_end)`` from the first candidate table with a schema."""
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


_export_owners: BoundedTTLCache = BoundedTTLCache(maxsize=1024, ttl_seconds=3600.0)
CacheRegistry.register("high_scale._export_owners", _export_owners)
_export_owners_lock = threading.Lock()


def _export_response(service_id: str, job: ExportJob, *, cancelled: bool | None = None):
    payload = {
        "export_id": job.job_id,
        "service_id": service_id,
        "state": job.state.value,
        "rows_written": job.rows_written,
        "bytes_written": job.bytes_written,
        "error": job.error,
    }
    if cancelled is None:
        return HighScaleExportResponse.with_telemetry(**payload)
    return HighScaleExportCancelResponse.with_telemetry(cancelled=cancelled, **payload)


def _clamp_and_parse_window(
    ctx: RequestContext, start_time: str | None, end_time: str | None
) -> tuple[datetime, datetime]:
    start_raw, end_raw = ctx.clamp(start_time, end_time)
    start = parse_iso_utc(start_raw)
    end = parse_iso_utc(end_raw)
    if start is None or end is None:
        raise ValueError("start_time and end_time are required ISO-8601 timestamps")
    return start, end


def _serialize_export_row(row: dict) -> bytes:
    return json.dumps(row, default=str, sort_keys=True, separators=(",", ":")).encode("utf-8")


@high_scale_router.post(
    "/services/{service_id}/exports",
    response_model=HighScaleExportResponse,
    dependencies=[Depends(require_admin)],
)
@query_errors()
def request_export(
    service_id: str,
    req: HighScaleExportRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    manager: ExportManager = Depends(get_export_manager),
):
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True, mismatch_on_scope=True)
    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)
    rows = _fetch_facts_page(service, req.domain, start=start, end=end, limit=req.limit, cursor=req.cursor).rows
    export_id = uuid4().hex
    manager.submit(export_id, rows, _serialize_export_row)
    with _export_owners_lock:
        _export_owners[export_id] = service_id
    metadata.record_audit(
        service_id,
        event_type="high_scale_export",
        details={"action": "requested", "export_id": export_id, "domain": req.domain, "row_limit": req.limit},
    )
    return _export_response(service_id, manager.get(export_id))


@high_scale_router.get(
    "/services/{service_id}/exports/{export_id}",
    response_model=HighScaleExportResponse,
)
@query_errors()
def export_status(
    service_id: str,
    export_id: str,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    manager: ExportManager = Depends(get_export_manager),
):
    _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True, mismatch_on_scope=True)
    with _export_owners_lock:
        owner = _export_owners.get(export_id)
    if owner != service_id:
        raise HTTPException(status_code=404, detail=make_error("export_not_found", "Export not found"))
    try:
        job = manager.get(export_id)
    except KeyError:
        with _export_owners_lock:
            _export_owners.pop(export_id, None)
        raise HTTPException(status_code=404, detail=make_error("export_not_found", "Export not found")) from None
    return _export_response(service_id, job)


@high_scale_router.post(
    "/services/{service_id}/exports/{export_id}/cancel",
    response_model=HighScaleExportCancelResponse,
    dependencies=[Depends(require_admin)],
)
@query_errors()
def cancel_export(
    service_id: str,
    export_id: str,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    manager: ExportManager = Depends(get_export_manager),
):
    _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True, mismatch_on_scope=True)
    with _export_owners_lock:
        owner = _export_owners.get(export_id)
    if owner != service_id:
        raise HTTPException(status_code=404, detail=make_error("export_not_found", "Export not found"))
    try:
        cancelled = manager.cancel(export_id)
        job = manager.get(export_id)
    except KeyError:
        with _export_owners_lock:
            _export_owners.pop(export_id, None)
        raise HTTPException(status_code=404, detail=make_error("export_not_found", "Export not found")) from None
    if cancelled:
        metadata.record_audit(
            service_id,
            event_type="high_scale_export",
            details={"action": "cancelled", "export_id": export_id},
        )
    return _export_response(service_id, job, cancelled=cancelled)


def _normalize_ts_iso(val: object) -> str:
    if isinstance(val, datetime):
        from backend.utils.date_utils import iso_z

        return iso_z(val)
    if val is None:
        return ""
    s = str(val)
    if s.endswith("+00:00"):
        return s[:-6] + "Z"
    return s


def _emulate_standard_request_facts(
    ctx: RequestContext,
    req: HighScaleRequestFactRequest,
    *,
    start: datetime,
    end: datetime,
) -> HighScaleRequestFactResponse:
    import os
    import uuid
    from datetime import UTC

    from backend.core.duckdb import _safe_table_name
    from backend.core.iceberg import execute_with_stale_view_retry

    secret = (os.getenv("HIGH_SCALE_CURSOR_SECRET", "").strip() or "standard-emulation-cursor-secret").encode("utf-8")
    decoded = KeysetCursor.decode(req.cursor, secret) if req.cursor else None
    if decoded and (decoded.service_id != ctx.service_id or decoded.domain != "request"):
        raise ValueError("cursor does not belong to this service and domain")

    candidate_tables = [_safe_table_name(str(ctx.source.get("name") or "default"))]
    if "logs" not in candidate_tables:
        candidate_tables.append("logs")

    def _run_emulation(con):
        return fetch_request_facts_emulation(
            con,
            candidate_tables,
            req,
            start=start,
            end=end,
            cursor_ts=decoded.timestamp if decoded is not None else None,
            service_id=ctx.service_id,
        )

    raw_rows, cov_start, cov_end = execute_with_stale_view_retry(ctx.con, ctx.source, _run_emulation)

    page_tuples = raw_rows[: req.limit]
    rows: list[dict[str, object]] = []
    mask = mask_ips_for(ctx.analyst_session)

    for idx, tup in enumerate(page_tuples):
        ts_val, country_val, ip_val, url_val, status_val, pop_val, host_val = tup
        ts_iso = _normalize_ts_iso(ts_val)
        ev_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{ctx.service_id}|{ts_iso}|{idx}|{url_val or ''}"))
        ip_str = str(ip_val or "")
        if mask and ip_str:
            ip_str = mask_ip(ip_str)
        status_str = str(status_val or "")
        status_out: int | str = int(status_str) if status_str.isdigit() else status_str
        pop_str = str(pop_val or "")
        host_str = str(host_val or "")
        rows.append(
            {
                "event_id": ev_id,
                "timestamp": ts_iso,
                "service_id": ctx.service_id,
                "source_object_key": "ducklake://local",
                "source_object_version": "v1",
                "line_ordinal": idx,
                "batch_id": "duckdb-emulation",
                "country": str(country_val or ""),
                "client_ip": ip_str,
                "url": str(url_val or ""),
                "status": status_out,
                "pop": pop_str,
                "host": host_str,
                "custom_fields": {
                    "status": status_str,
                    "pop": pop_str,
                    "host": host_str,
                },
                "cmcd": {},
            }
        )

    next_cursor: str | None = None
    if len(raw_rows) > req.limit and rows:
        last_row = rows[-1]
        last_ts = parse_iso_utc(str(last_row["timestamp"])) or end
        next_cursor = KeysetCursor(last_ts, str(last_row["event_id"]), ctx.service_id, "request").encode(secret)

    cov_start_dt = parse_iso_utc(_normalize_ts_iso(cov_start)) if cov_start is not None else start
    cov_end_dt = parse_iso_utc(_normalize_ts_iso(cov_end)) if cov_end is not None else end
    now_utc = datetime.now(UTC)
    lag = max(0.0, (now_utc - cov_end_dt).total_seconds()) if cov_end_dt else 0.0
    last_ev = str(rows[-1]["event_id"]) if rows else None

    return HighScaleRequestFactResponse.with_telemetry(
        rows=rows,
        next_cursor=next_cursor,
        metadata=QueryResponseMetadataResponse(
            status="complete",
            exact=True,
            coverage=1.0,
            freshness_lag_seconds=round(lag, 2),
            watermark=ServingWatermarkResponse(
                service_id=ctx.service_id,
                domain="request",
                owner_epoch=1,
                coverage_start=cov_start_dt,
                coverage_end=cov_end_dt,
                last_accepted_cursor=next_cursor,
                last_archived_event_id=last_ev,
                last_visible_event_id=last_ev,
                exact=True,
            ),
            approximation_error=None,
            error=None,
            mode="duckdb_emulation",
        ),
    )


def _execute_request_facts_query(
    ctx: RequestContext,
    req: HighScaleRequestFactRequest,
    registry: HighScaleServiceRegistryProtocol,
    *,
    allow_standard_emulation: bool,
    default_window_if_missing: bool,
) -> HighScaleRequestFactResponse:
    from datetime import UTC, timedelta

    from backend.utils.date_utils import iso_z

    req_start = req.start_time
    req_end = req.end_time
    if default_window_if_missing and (not req_start or not req_end):
        now_dt = datetime.now(UTC)
        if not req_end:
            req_end = iso_z(now_dt)
        if not req_start:
            req_start = iso_z(now_dt - timedelta(hours=24))

    start, end = _clamp_and_parse_window(ctx, req_start, req_end)

    service = registry.resolve(ctx.service_id)
    if service is None:
        if allow_standard_emulation or req.emulate_standard:
            return _emulate_standard_request_facts(ctx, req, start=start, end=end)
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_service_not_configured", "Service is not registered for high-scale queries"),
        )
    if service.service_id != ctx.service_id:
        raise HTTPException(
            status_code=409,
            detail=make_error(
                "high_scale_service_mismatch", "High-scale service binding does not match the requested service"
            ),
        )

    page = query_request_facts(
        service.client,
        service_id=ctx.service_id,
        start=start,
        end=end,
        cursor_secret=service.cursor_secret,
        watermark=service.watermark(),
        limit=req.limit,
        cursor=req.cursor,
        status=req.status,
        domain_filter=req.domain,
        pop=req.pop,
        country=req.country,
        batch_id=req.batch_id,
    )
    rows = [dict(row) for row in page.rows]
    for row in rows:
        cf = row.get("custom_fields")
        if isinstance(cf, dict):
            for k in ("status", "pop", "host"):
                if k not in row and cf.get(k) is not None:
                    row[k] = cf[k]
    _mask_client_ip_rows(rows, ctx)

    meta = page.metadata
    return HighScaleRequestFactResponse.with_telemetry(
        rows=rows,
        next_cursor=page.next_cursor,
        metadata=_build_query_metadata_response(meta, mode="clickhouse"),
    )


@high_scale_router.get(
    "/request-facts",
    response_model=HighScaleRequestFactResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def get_request_facts_root(
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 100,
    cursor: str | None = None,
    status: str | None = None,
    domain: str | None = None,
    pop: str | None = None,
    country: str | None = None,
    batch_id: str | None = None,
    emulate_standard: bool = True,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    req = HighScaleRequestFactRequest(
        start_time=start_time,
        end_time=end_time,
        limit=max(1, min(500, limit)),
        cursor=cursor,
        status=status,
        domain=domain,
        pop=pop,
        country=country,
        batch_id=batch_id,
        emulate_standard=emulate_standard,
    )
    return _execute_request_facts_query(
        ctx,
        req,
        registry,
        allow_standard_emulation=emulate_standard,
        default_window_if_missing=True,
    )


@high_scale_router.post(
    "/request-facts",
    response_model=HighScaleRequestFactResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def post_request_facts_root(
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    return _execute_request_facts_query(
        ctx,
        req,
        registry,
        allow_standard_emulation=True,
        default_window_if_missing=True,
    )


@high_scale_router.post(
    "/services/{service_id}/request-facts",
    response_model=HighScaleRequestFactResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def request_facts(
    service_id: str,
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    if service_id != ctx.service_id:
        raise HTTPException(status_code=403, detail=make_error("service_access_denied", "Service access denied"))
    return _execute_request_facts_query(
        ctx,
        req,
        registry,
        allow_standard_emulation=req.emulate_standard,
        default_window_if_missing=False,
    )


def _build_query_metadata_response(
    metadata,
    *,
    mode: str | None = None,
) -> QueryResponseMetadataResponse:
    kwargs: dict = {
        "status": metadata.status,
        "exact": metadata.exact,
        "coverage": metadata.coverage,
        "freshness_lag_seconds": metadata.freshness_lag_seconds,
        "watermark": ServingWatermarkResponse(
            service_id=metadata.watermark.service_id,
            domain=metadata.watermark.domain,
            owner_epoch=metadata.watermark.owner_epoch,
            coverage_start=metadata.watermark.coverage_start,
            coverage_end=metadata.watermark.coverage_end,
            last_accepted_cursor=metadata.watermark.last_accepted_cursor,
            last_archived_event_id=metadata.watermark.last_archived_event_id,
            last_visible_event_id=metadata.watermark.last_visible_event_id,
            exact=metadata.watermark.exact,
        ),
        "approximation_error": metadata.approximation_error,
        "error": metadata.error,
    }
    if mode is not None:
        kwargs["mode"] = mode
    return QueryResponseMetadataResponse(**kwargs)


def _resolve_scoped_high_scale_service(
    service_id: str,
    ctx: RequestContext,
    registry: HighScaleServiceRegistryProtocol,
    *,
    check_binding: bool = True,
    mismatch_on_scope: bool = False,
) -> HighScaleService:
    if service_id != ctx.service_id:
        if mismatch_on_scope:
            raise HTTPException(
                status_code=409,
                detail=make_error(
                    "high_scale_service_mismatch",
                    "High-scale service binding does not match the requested service",
                ),
            )
        raise HTTPException(status_code=403, detail=make_error("service_access_denied", "Service access denied"))
    service = registry.resolve(ctx.service_id)
    if service is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_service_not_configured", "Service is not registered for high-scale queries"),
        )
    if check_binding and service.service_id != ctx.service_id:
        raise HTTPException(
            status_code=409,
            detail=make_error(
                "high_scale_service_mismatch", "High-scale service binding does not match the requested service"
            ),
        )
    return service


@high_scale_router.post(
    "/services/{service_id}/rum-facts/{domain}",
    response_model=HighScaleRumFactResponse,
)
@query_errors()
def rum_facts(
    service_id: str,
    domain: str,
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    if service_id != ctx.service_id:
        raise HTTPException(status_code=403, detail=make_error("service_access_denied", "Service access denied"))
    if domain not in {"rum_vitals", "rum_errors"}:
        raise HTTPException(status_code=404, detail=make_error("not_found", "RUM domain not found"))
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True)

    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)

    try:
        watermark = service.watermark_for(domain)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_domain_not_configured", "RUM domain is not configured for this service"),
        ) from None
    page = query_rum_facts(
        service.client,
        service_id=ctx.service_id,
        domain=domain,
        start=start,
        end=end,
        cursor_secret=service.cursor_secret,
        watermark=watermark,
        limit=req.limit,
        cursor=req.cursor,
    )
    rows = _mask_client_ip_rows([dict(row) for row in page.rows], ctx)
    return HighScaleRumFactResponse.with_telemetry(
        rows=rows,
        next_cursor=page.next_cursor,
        metadata=_build_query_metadata_response(page.metadata),
    )


@high_scale_router.post(
    "/services/{service_id}/cmcd-facts",
    response_model=HighScaleRequestFactResponse,
)
@query_errors()
def cmcd_facts(
    service_id: str,
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=False)
    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)
    trace = HighScaleSections(ctx.service_id, "cmcd")
    try:
        watermark = trace.run(
            "watermark",
            lambda: service.watermark_for("cmcd"),
            source="projected_fact",
            activity="overhead",
        )
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_domain_not_configured", "CMCD is not configured for this service"),
        ) from None
    page = trace.run(
        "facts",
        lambda: query_cmcd_facts(
            service.client,
            service_id=ctx.service_id,
            start=start,
            end=end,
            cursor_secret=service.cursor_secret,
            watermark=watermark,
            limit=req.limit,
            cursor=req.cursor,
        ),
        source="projected_fact",
    )
    return HighScaleRequestFactResponse.with_telemetry(
        rows=_mask_client_ip_rows([dict(row) for row in page.rows], ctx),
        next_cursor=page.next_cursor,
        metadata=_build_query_metadata_response(page.metadata),
        section_timings=trace.timings(),
    )


@high_scale_router.post(
    "/services/{service_id}/aggregates",
    response_model=HighScaleAggregateResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def aggregates(
    service_id: str,
    req: HighScaleAggregateRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True)

    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)

    try:
        watermark = service.watermark_for(req.domain)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_domain_not_configured", "Domain is not configured for this service"),
        ) from None

    dimension = req.dimension or DEFAULT_AGGREGATE_DIMENSIONS[req.domain]
    response = query_clickhouse_aggregate(
        service.client,
        AggregateRequest(service_id=ctx.service_id, domain=req.domain, start=start, end=end, dimension=dimension),
        watermark=watermark,
    )
    top_values = list(response.top_values)
    if dimension == "client_ip" and mask_ips_for(ctx.analyst_session):
        top_values = [(mask_ip(value), count) for value, count in top_values]
    return HighScaleAggregateResponse.with_telemetry(
        service_id=response.service_id,
        domain=response.domain,
        dimension=dimension,
        request_count=response.request_count,
        top_values=[HighScaleAggregateValue(value=value, count=count) for value, count in top_values],
        coverage=response.coverage,
        freshness_lag_seconds=response.freshness_lag_seconds,
        exact=response.exact,
        approximation_error=response.approximation_error,
    )


def _fetch_facts_page(
    service: HighScaleService,
    domain: str,
    *,
    start: datetime,
    end: datetime,
    limit: int,
    cursor: str | None,
):
    watermark = service.watermark_for(domain)
    if domain == "request":
        return query_request_facts(
            service.client,
            service_id=service.service_id,
            start=start,
            end=end,
            cursor_secret=service.cursor_secret,
            watermark=watermark,
            limit=limit,
            cursor=cursor,
        )
    if domain == "cmcd":
        return query_cmcd_facts(
            service.client,
            service_id=service.service_id,
            start=start,
            end=end,
            cursor_secret=service.cursor_secret,
            watermark=watermark,
            limit=limit,
            cursor=cursor,
        )
    return query_rum_facts(
        service.client,
        service_id=service.service_id,
        domain=domain,
        start=start,
        end=end,
        cursor_secret=service.cursor_secret,
        watermark=watermark,
        limit=limit,
        cursor=cursor,
    )


def _mask_client_ip_rows(rows: list[dict], ctx: RequestContext) -> list[dict]:
    if mask_ips_for(ctx.analyst_session):
        for row in rows:
            value = row.get("client_ip")
            if isinstance(value, str):
                row["client_ip"] = mask_ip(value)
    return rows


@high_scale_router.post(
    "/services/{service_id}/queries",
    response_model=HighScaleQueryResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def submit_query(
    service_id: str,
    req: HighScaleQueryRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    jobs: HistoricalJobManager = Depends(get_historical_job_manager),
):
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True)

    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)

    plan = QueryRouter().plan(
        RouterQueryRequest(
            service_id=ctx.service_id, domain=req.domain, start=start, end=end, intent=QueryIntent.RAW, limit=req.limit
        )
    )
    plan_response = HighScaleQueryPlanResponse(
        tier=plan.tier.value,
        sources=list(plan.sources),
        queued=plan.queued,
        bounded=plan.bounded,
        max_rows=plan.max_rows,
        reason=plan.reason,
    )

    if plan.tier is QueryTier.COLD_JOB:
        if service.manifest_catalog is None or service.archive is None:
            raise HTTPException(
                status_code=404,
                detail=make_error(
                    "high_scale_cold_query_not_configured",
                    "Cold-tier historical queries are not configured for this service",
                ),
            )
        producer = raw_query_producer(
            service.manifest_catalog,
            service.archive,
            service_id=ctx.service_id,
            domain=req.domain,
            start=start,
            end=end,
        )
        job_id = jobs.submit(plan, producer)
        return HighScaleQueryResponse.with_telemetry(
            service_id=ctx.service_id,
            domain=req.domain,
            plan=plan_response,
            rows=None,
            next_cursor=None,
            metadata=None,
            job_id=job_id,
        )

    try:
        page = _fetch_facts_page(service, req.domain, start=start, end=end, limit=req.limit, cursor=req.cursor)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_domain_not_configured", "Domain is not configured for this service"),
        ) from None
    rows = _mask_client_ip_rows([dict(row) for row in page.rows], ctx)
    return HighScaleQueryResponse.with_telemetry(
        service_id=ctx.service_id,
        domain=req.domain,
        plan=plan_response,
        rows=rows,
        next_cursor=page.next_cursor,
        metadata=_build_query_metadata_response(page.metadata),
        job_id=None,
    )


@high_scale_router.get(
    "/services/{service_id}/queries/{job_id}",
    response_model=HighScaleHistoricalJobResponse,
)
@query_errors()
def query_job_status(
    service_id: str,
    job_id: str,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    jobs: HistoricalJobManager = Depends(get_historical_job_manager),
):
    _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=False)
    job = jobs.get(job_id)
    if job is None or job.plan.service_id != ctx.service_id:
        raise HTTPException(status_code=404, detail=make_error("historical_job_not_found", "Historical job not found"))
    rows = _mask_client_ip_rows([dict(row) for row in job.rows], ctx)
    return HighScaleHistoricalJobResponse.with_telemetry(
        job_id=job.job_id,
        state=job.state.value,
        rows=rows,
        row_count=job.row_count,
        output_bytes=job.output_bytes,
        truncated=job.truncated,
        error=job.error,
        created_at=job.created_at,
        expires_at=job.expires_at,
        terminal_at=job.terminal_at,
    )


@high_scale_router.post(
    "/services/{service_id}/queries/{job_id}/cancel",
    response_model=HighScaleHistoricalJobCancelResponse,
    dependencies=[Depends(require_admin)],
)
@query_errors()
def cancel_query_job(
    service_id: str,
    job_id: str,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    jobs: HistoricalJobManager = Depends(get_historical_job_manager),
):
    _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=False)
    job = jobs.get(job_id)
    if job is None or job.plan.service_id != ctx.service_id:
        raise HTTPException(status_code=404, detail=make_error("historical_job_not_found", "Historical job not found"))
    cancelled = jobs.cancel(job_id)
    return HighScaleHistoricalJobCancelResponse.with_telemetry(job_id=job_id, cancelled=cancelled)


router = high_scale_router

_self_comp = sys.modules[__name__]
for _mod_name in (
    "backend.routers.admin.ingest",
    "backend.routers.admin.quarantine",
    "backend.routers.admin.downloads",
    "backend.routers.admin.iceberg",
    "backend.routers.admin.bot_sources",
    "backend.routers.high_scale",
):
    sys.modules[_mod_name] = _self_comp
if "backend.routers" in sys.modules:
    setattr(sys.modules["backend.routers"], "high_scale", _self_comp)
if "backend" in sys.modules and hasattr(sys.modules["backend"], "_MODULE_ALIASES"):
    sys.modules["backend"]._MODULE_ALIASES["backend.routers.high_scale"] = __name__
