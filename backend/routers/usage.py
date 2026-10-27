"""Usage and cost estimation endpoints."""

from __future__ import annotations

import logging
import urllib.parse
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.config import get_fastly_api_key
from backend.core.fastly.client import api_key_or_mock
from backend.core.fastly.utils import FASTLY_LOG_FIELDS
from backend.deps import get_source
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.admin import (
    CurrentStorageResponse,
    PrefillRatesResponse,
    PrefillResponse,
    RumBreakdownPoint,
    UsageBandwidthResponse,
    UsageLogActivityResponse,
    UsageOperationsResponse,
    UsageRumBreakdownResponse,
)
from backend.repositories import assets as repo
from backend.repositories._base import SectionTimer
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import window_to_epoch
from backend.utils.router_utils import query_errors, raise_internal

logger = logging.getLogger(__name__)

usage_router = APIRouter(prefix="/api/usage", tags=["usage"], responses=DEFAULT_ERROR_RESPONSES)
router = usage_router

_FASTLY_STATS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=60.0)
_STORAGE_INSPECTION_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=60.0)
_CacheRegistry.register("routers.usage._FASTLY_STATS_CACHE", _FASTLY_STATS_CACHE)
_CacheRegistry.register("routers.usage._STORAGE_INSPECTION_CACHE", _STORAGE_INSPECTION_CACHE)


def _fastly_api(path: str, api_key: str) -> dict[str, Any]:
    """Thin wrapper delegating to the central Fastly client with TTL caching."""
    from backend.core.fastly.client import fastly

    cached = _FASTLY_STATS_CACHE.get(path)
    if cached is not None:
        return dict(cached)
    response = fastly("GET", path, token=api_key)
    _FASTLY_STATS_CACHE[path] = response
    return dict(response)


def _extract_fos_ops(record: dict[str, Any]) -> tuple[int, int]:
    """Read Class A / Class B counts from a Fastly /stats/aggregate record."""
    from backend.core.fastly.client import StatsRecord

    return StatsRecord.model_validate(record).fos_ops()


def _compute_prefill_rates(source: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Local-config-only prefill payload (FAST path — no Fastly, no DuckDB)."""
    from backend import config as svcconfig

    global_rates = svcconfig.load_usage_logging_config()
    result: dict[str, Any] = {
        "avg_log_file_size_kb": None,
        "estimated_bytes_per_line": None,
        "data_days": 0,
        "log_period_seconds": None,
        "commit_interval_mins": 5,
        "sample_rate": 100,
        "edge_only": False,
        "compaction_enabled": True,
        "delete_after": True,
        "log_retention_days": 90,
        "class_a_rate_per_1k": float(global_rates.get("class_a_rate_per_1k", 0.005)),
        "class_b_rate_per_10k": float(global_rates.get("class_b_rate_per_10k", 0.01)),
        "cdn_egress_rate_per_gb": float(global_rates.get("cdn_egress_rate_per_gb", 0.12)),
        "storage_rate_per_gb_month": float(global_rates.get("storage_rate_per_gb_month", 0.02)),
        "min_billed_days": int(global_rates.get("min_billed_days", 30)),
        "billing_zone": str(global_rates.get("billing_zone", "MBZ200")),
        "rum_beacons_per_day": 0,
    }

    prov: dict[str, Any] = {}
    try:
        cfg = svcconfig.load_config(source["name"])
        if cfg:
            prov = cfg.get("provisioning", {})
            cron_sync = prov.get("cron_sync", {})
            result["sample_rate"] = int(prov.get("sample_rate", 100))
            result["edge_only"] = prov.get("edge_only", True)
            result["compaction_enabled"] = prov.get("cron_compact", {}).get("enabled", True)
            result["delete_after"] = bool(cron_sync.get("delete_after", True))
            result["commit_interval_mins"] = int(cron_sync.get("commit_interval_mins", 5))
            result["log_retention_days"] = int(cfg.get("log_retention_days", 90))
            try:
                from backend.core.log_fields import estimate_log_line_bytes

                lf_cfg = cfg.get("log_fields") or prov.get("log_fields", {})
                if not lf_cfg:
                    lf_cfg = {"groups": ["A", "B", "C", "D"], "field_overrides": {}}
                result["estimated_bytes_per_line"] = estimate_log_line_bytes(lf_cfg)
            except Exception:
                logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)
            try:
                from backend.provision import parse_period

                period_val = cfg.get("log_period", prov.get("log_period", 60))
                period_secs = period_val if isinstance(period_val, int) else parse_period(str(period_val))
                result["log_period_seconds"] = period_secs
            except Exception:
                logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)
    except Exception:
        logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)

    try:
        cached_status = svcconfig.get_status(source["name"])
        if cached_status:
            avg_kb = cached_status.get("avg_log_size_kb")
            if avg_kb:
                result["avg_log_file_size_kb"] = round(float(avg_kb), 2)
            earliest = cached_status.get("earliest_log_at")
            latest = cached_status.get("latest_log_at")
            if earliest and latest:
                try:
                    from backend.utils.date_utils import parse_iso_utc

                    _now = datetime.now(UTC)
                    e_dt = parse_iso_utc(earliest) or _now
                    l_dt = parse_iso_utc(latest) or _now
                    result["data_days"] = max(1, (l_dt - e_dt).days + 1)
                except Exception:
                    logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)
    except Exception:
        logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)

    from backend.core import metadata as metadata_db

    try:
        avg = metadata_db.get_node_count_avg(source["name"])
        if avg:
            result["avg_nodes_per_flush"] = round(float(avg))
    except Exception:
        logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)

    try:
        val = metadata_db.get_rum_beacons_avg_per_day(source["name"])
        result["rum_beacons_per_day"] = round(val) if val is not None else 0
    except Exception as e:
        logger.debug("Failed to calculate rum_beacons_per_day: %s", e)

    return result, prov


@usage_router.get("/prefill/rates", response_model=PrefillRatesResponse)
@query_errors()
def prefill_rates(source: dict[str, Any] = Depends(get_source)) -> PrefillRatesResponse:
    """Instant prefill subset for stat cards."""
    result, _prov = _compute_prefill_rates(source)
    resp: PrefillRatesResponse = PrefillRatesResponse.with_telemetry(**result)
    return resp


@usage_router.get("/prefill", response_model=PrefillResponse)
@query_errors()
async def prefill(source: dict[str, Any] = Depends(get_source)) -> PrefillResponse:
    import asyncio
    import time as _time

    from backend import config as svcconfig
    from backend.config import get_fastly_logging_service_id

    timer = SectionTimer()
    section_timings = timer.entries

    fast_result, prov = await asyncio.to_thread(_compute_prefill_rates, source)
    result: dict[str, Any] = {
        "requests_per_day": None,
        "edge_requests_per_day": None,
        "edge_ratio": None,
        **fast_result,
    }

    api_key = api_key_or_mock(get_fastly_api_key(source["service_id"]))
    if api_key:
        from backend.core.fastly.client import fastly, find_condition, get_active_version

        stats_svc_id = (source.get("cdn_service_id") or get_fastly_logging_service_id() or "").strip()
        logging_svc_id = (source.get("logging_service_id") or get_fastly_logging_service_id() or "").strip()
        svc_id = stats_svc_id
        now = datetime.now(UTC)
        from_ts = int((now - timedelta(days=3)).timestamp())
        to_ts = int(now.timestamp())
        by = "day"

        async def _resolve_endpoint_chain() -> dict[str, Any]:
            updates: dict[str, Any] = {}
            try:
                if not logging_svc_id:
                    return updates
                active_ver = await asyncio.to_thread(get_active_version, logging_svc_id, api_key)
                if not active_ver:
                    return updates
                endpoint_name = prov.get("endpoint_name", "Fastly Object Storage Logs")
                encoded_name = urllib.parse.quote(endpoint_name, safe="")
                current_ep = await asyncio.to_thread(
                    fastly,
                    "GET",
                    f"/service/{logging_svc_id}/version/{active_ver}/logging/s3/{encoded_name}",
                    token=api_key,
                )
                if "period" in current_ep:
                    updates["log_period_seconds"] = int(current_ep["period"])
                cond_name = current_ep.get("response_condition")
                if cond_name == "Log Sampling":
                    import re

                    cond = await asyncio.to_thread(find_condition, cond_name, logging_svc_id, active_ver, api_key)
                    if cond:
                        stmt = cond.get("statement", "")
                        m = re.search(r"randombool\((\d+),", stmt)
                        if m:
                            updates["sample_rate"] = int(m.group(1))
                        if "req.restarts == 0" in stmt:
                            updates["edge_only"] = True
            except Exception:
                logger.debug("_resolve_endpoint_chain: step failed (ignored)", exc_info=True)
            return updates

        async def _fetch_stats() -> dict[str, Any] | None:
            try:
                if svc_id:
                    return await asyncio.to_thread(
                        _fastly_api, f"/stats/service/{svc_id}?by={by}&from={from_ts}&to={to_ts}", api_key
                    )
                return await asyncio.to_thread(
                    _fastly_api, f"/stats/aggregate?by={by}&from={from_ts}&to={to_ts}", api_key
                )
            except Exception:
                return None

        try:
            _t = _time.perf_counter()
            chain_updates, payload = await asyncio.gather(_resolve_endpoint_chain(), _fetch_stats())
            timer.mark("fastly_chain_and_stats", _t)
            result.update(chain_updates)

            daily_reqs: dict[str, int] = {}
            daily_edge: dict[str, int] = {}
            if payload:
                for rec in payload.get("data", []):
                    ts = rec.get("start_time")
                    if ts is None:
                        continue
                    day = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d")
                    daily_reqs[day] = daily_reqs.get(day, 0) + int(rec.get("requests") or 0)
                    daily_edge[day] = daily_edge.get(day, 0) + int(rec.get("edge_requests") or 0)
            today = now.strftime("%Y-%m-%d")
            complete_days = [d for d, v in daily_reqs.items() if v > 0 and d != today]
            avg_days = complete_days if complete_days else [d for d, v in daily_reqs.items() if v > 0]
            if avg_days:
                result["requests_per_day"] = round(sum(daily_reqs[d] for d in avg_days) / len(avg_days))
                result["edge_requests_per_day"] = round(sum(daily_edge[d] for d in avg_days) / len(avg_days))
        except Exception:
            logger.debug("prefill: step failed (ignored)", exc_info=True)

    debug_queries: list[Any] = []
    cached_status = svcconfig.get_status(source["name"]) or {}
    cached_edge_ratio = cached_status.get("edge_ratio")
    if cached_edge_ratio is not None:
        result["edge_ratio"] = cached_edge_ratio
    else:
        try:
            from backend.core.duckdb import get_connection

            def _edge_ratio_blocking() -> tuple[Any, ...]:
                con = get_connection(source=source, max_wait=5, read_only=True, skip_view_update=True)
                try:
                    return repo.get_edge_ratio(con, source)
                finally:
                    con.close()

            _t = _time.perf_counter()
            edge_ratio, debug_queries = await asyncio.to_thread(_edge_ratio_blocking)
            timer.mark("edge_ratio_query", _t)
            if edge_ratio is not None:
                result["edge_ratio"] = edge_ratio
        except Exception:
            logger.debug("prefill: step failed (ignored)", exc_info=True)

    resp: PrefillResponse = PrefillResponse.with_telemetry(
        debug_queries=debug_queries, section_timings=section_timings, **result
    )
    return resp


@usage_router.get("/current-storage", response_model=CurrentStorageResponse)
@query_errors()
def usage_current_storage(
    start: str = Query(default=""),
    end: str = Query(default=""),
    source: dict[str, Any] = Depends(get_source),
) -> CurrentStorageResponse:
    from backend.core.duckdb import _get_fos_client

    src = source
    if not src.get("bucket"):
        raise HTTPException(status_code=400, detail={"error": "Fastly Object Storage bucket not configured."})

    from backend.utils.date_utils import parse_date_window

    now = datetime.now(UTC)
    start_str, end_str = parse_date_window(start, end)

    try:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(src["name"]) or {}
        cron_sync = cfg.get("provisioning", {}).get("cron_sync", {})
        delete_after = cron_sync.get("delete_after", True)
        retention_days = int(cron_sync.get("log_retention_days", 30))
        retention_end_str = now.strftime("%Y-%m-%d %H:%M:%S")
        MIN_BILLING_DAYS = 30
        query_days = MIN_BILLING_DAYS if delete_after else retention_days
        retention_start_str = (now - timedelta(days=query_days)).strftime("%Y-%m-%d %H:%M:%S")

        stats = repo.get_storage_stats(None, src, retention_start_str, retention_end_str)

        total_files = stats["total_files"]
        total_bytes = stats["total_bytes"]
        debug_queries = stats.get("debug_queries", [])

        cache_key = (src.get("name"), src.get("bucket"), src.get("prefix"))
        cached_insp = _STORAGE_INSPECTION_CACHE.get(cache_key)
        if cached_insp is not None:
            storage_bytes, storage_files, quarantine_bytes, rum_bytes = cached_insp
        else:
            storage_bytes = 0
            storage_files = 0
            storage_info_success = False
            cached_status = svcconfig.get_status(src["name"]) or {}
            cached_ib = cached_status.get("storage_bytes")
            cached_if = cached_status.get("storage_files")
            if cached_ib is not None and cached_if is not None:
                storage_bytes = int(cached_ib)
                storage_files = int(cached_if)
                storage_info_success = True
            else:
                try:
                    from backend.core import storage as db_storage

                    storage_info = db_storage.get_table_info(src)
                    if not storage_info.get("error"):
                        storage_bytes = storage_info.get("size_bytes", 0)
                        storage_files = storage_info.get("data_files", 0)
                        storage_info_success = True
                except Exception:
                    logger.debug("usage_current_storage: step failed (ignored)", exc_info=True)

            if not storage_info_success:
                try:
                    s3 = _get_fos_client(src)
                    bucket = src["bucket"]
                    prefix = f"{src.get('prefix', '').strip().rstrip('/')}/" if src.get("prefix") else ""
                    storage_prefix = f"{prefix}ducklake/"
                    paginator = s3.get_paginator("list_objects_v2")
                    for page in paginator.paginate(Bucket=bucket, Prefix=storage_prefix.rstrip("/")):
                        for obj in page.get("Contents", []):
                            storage_bytes += obj.get("Size", 0)
                            storage_files += 1
                except Exception as e:
                    logging.error(f"FOS Scan failed: {e}")

            quarantine_bytes = 0
            try:
                from backend.core.metadata.ingest_log import get_quarantine_storage_total

                quarantine_bytes = get_quarantine_storage_total(src["name"])
            except Exception:
                logger.debug("usage_current_storage: get_quarantine_storage_total failed (ignored)", exc_info=True)

            rum_bytes = 0
            try:
                s3 = _get_fos_client(src)
                bucket = src["bucket"]
                prefix = src.get("prefix", "").strip("/")
                rum_prefix = f"{prefix}/raw/rum/" if prefix else "raw/rum/"
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=rum_prefix):
                    for obj in page.get("Contents", []):
                        rum_bytes += obj.get("Size", 0)
            except Exception:
                logger.debug("usage_current_storage: step failed (ignored)", exc_info=True)

            _STORAGE_INSPECTION_CACHE[cache_key] = (
                storage_bytes,
                storage_files,
                quarantine_bytes,
                rum_bytes,
            )

        regular_log_bytes = total_bytes - rum_bytes

        if delete_after:
            live_bytes = storage_bytes + quarantine_bytes
            live_files = storage_files
            deleted_bytes = total_bytes
        else:
            live_bytes = total_bytes + storage_bytes + quarantine_bytes
            live_files = total_files + storage_files
            deleted_bytes = 0

        billed_bytes = total_bytes + storage_bytes + quarantine_bytes
        billed_gb = billed_bytes / (1024 * 1024 * 1024)
        gb_hours = billed_gb * (MIN_BILLING_DAYS * 24)

        resp: CurrentStorageResponse = CurrentStorageResponse.with_telemetry(
            debug_queries=debug_queries,
            live_bytes=live_bytes,
            live_files=live_files,
            deleted_bytes=deleted_bytes,
            quarantine_bytes=quarantine_bytes,
            total_billed_bytes=billed_bytes,
            total_billed_gb_hours=gb_hours,
            total_files=total_files,
            total_bytes=total_bytes,
            rum_bytes=rum_bytes,
            regular_log_bytes=regular_log_bytes,
            start=start_str,
            end=end_str,
        )
        return resp
    except Exception as e:
        raise_internal(logger, e, code="usage_storage_failed")


@usage_router.get("/operations", response_model=UsageOperationsResponse)
@query_errors()
def usage_operations(
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="day"),
    source: dict[str, Any] = Depends(get_source),
) -> UsageOperationsResponse:
    requested_by = by
    if by not in ("hour", "day"):
        by = "hour"
    clamped_from_sub_hour = requested_by != by

    src = source
    api_key = api_key_or_mock(get_fastly_api_key(src["logging_service_id"]))
    if not api_key:
        raise HTTPException(status_code=403, detail={"error": "Fastly API key is not configured."})

    start_str, end_str, from_ts, to_ts = window_to_epoch(start, end)

    agg: dict[str, dict[str, int]] = {}
    fos_fields_found: set[str] = set()

    def _accumulate(records: list[dict[str, Any]]) -> None:
        for record in records:
            ts = record.get("start_time")
            if ts is None:
                continue
            for k in record:
                if "object_storage" in k:
                    fos_fields_found.add(k)
            sub = record.get("object_storage", {})
            if isinstance(sub, dict):
                for k in sub:
                    if "operations" in k or "class" in k:
                        fos_fields_found.add(f"object_storage.{k}")
            if by == "minute":
                fmt = "%Y-%m-%dT%H:%M"
            elif by == "hour":
                fmt = "%Y-%m-%dT%H:00"
            else:
                fmt = "%Y-%m-%d"
            date_str = datetime.fromtimestamp(ts, tz=UTC).strftime(fmt)
            class_a, class_b = _extract_fos_ops(record)
            if date_str not in agg:
                agg[date_str] = {"class_a": 0, "class_b": 0}
            agg[date_str]["class_a"] += class_a
            agg[date_str]["class_b"] += class_b

    try:
        payload = _fastly_api(f"/stats/aggregate?by={by}&from={from_ts}&to={to_ts}", api_key)
        _accumulate(payload.get("data", []))
    except Exception as e:
        raise_internal(logger, e, code="fastly_stats_aggregate_failed", status=502)

    points: list[dict[str, Any]] = [{"date": d, **v} for d, v in sorted(agg.items())]
    total_a = sum(v["class_a"] for v in agg.values())
    total_b = sum(v["class_b"] for v in agg.values())
    note = (
        "Note: Object Storage operations are reported across your entire Fastly account "
        "and cannot be filtered by service. Estimates may be inflated if other services "
        "use Object Storage."
    )
    if clamped_from_sub_hour:
        note += (
            f" The chart interval you selected ({requested_by}) is finer than the Fastly "
            "Historical Stats API supports for /stats/aggregate — granularity is forced to "
            "hourly minimum (the API is account-wide and prohibitively slow at sub-hour)."
        )
    resp: UsageOperationsResponse = UsageOperationsResponse.with_telemetry(
        data=points,
        total_class_a=total_a,
        total_class_b=total_b,
        granularity=by,
        note=note,
        fos_fields_found=sorted(fos_fields_found),
    )
    return resp


@usage_router.get("/bandwidth", response_model=UsageBandwidthResponse)
@query_errors()
def usage_bandwidth(
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="hour"),
    source: dict[str, Any] = Depends(get_source),
) -> UsageBandwidthResponse:
    src = source
    api_key = api_key_or_mock(get_fastly_api_key(src["logging_service_id"]))
    if not api_key:
        raise HTTPException(status_code=403, detail={"error": "Fastly API key is not configured."})

    if by not in ("hour", "minute", "day"):
        by = "hour"

    start_str, end_str, from_ts, to_ts = window_to_epoch(start, end)

    cdn_svc = src.get("cdn_service_id", "").strip()
    agg: dict[int, dict[str, int]] = {}

    def _merge(payload: dict[str, Any]) -> None:
        for record in payload.get("data", []):
            ts = record.get("start_time")
            if ts is None:
                continue
            if ts not in agg:
                agg[ts] = {"bandwidth_bytes": 0, "requests": 0}
            agg[ts]["bandwidth_bytes"] += int(record.get("bandwidth") or 0)
            agg[ts]["requests"] += int(record.get("requests") or 0)

    if cdn_svc:
        try:
            payload = _fastly_api(f"/stats/service/{cdn_svc}?by={by}&from={from_ts}&to={to_ts}", api_key)
            _merge(payload)
        except Exception as e:
            raise_internal(logger, e, code="fastly_stats_service_failed", status=502)

    fmt = "%Y-%m-%dT%H:00" if by == "hour" else "%Y-%m-%dT%H:%M" if by == "minute" else "%Y-%m-%d"
    points = [{"time": datetime.fromtimestamp(ts, tz=UTC).strftime(fmt), **v} for ts, v in sorted(agg.items())]
    resp: UsageBandwidthResponse = UsageBandwidthResponse.with_telemetry(
        data=points,
        total_bytes=sum(p["bandwidth_bytes"] for p in points),
        total_log_bytes=0,
        granularity=by,
    )
    return resp


@usage_router.get("/log-activity", response_model=UsageLogActivityResponse)
@query_errors()
def usage_log_activity(
    source: dict[str, Any] = Depends(get_source),
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="hour"),
) -> UsageLogActivityResponse:
    if by not in ("second", "minute", "hour", "day"):
        by = "hour"

    from backend.utils.date_utils import parse_date_window

    start_str, end_str = parse_date_window(start, end)

    res = repo.get_log_activity(source, start_str, end_str, by)

    api_key = api_key_or_mock(get_fastly_api_key(source.get("logging_service_id", "")))
    logging_svc = source.get("logging_service_id", "").strip()

    if api_key and logging_svc and by in ("minute", "hour", "day"):
        from backend.utils.date_utils import parse_window_str_to_dt

        start_dt = parse_window_str_to_dt(start_str)
        end_dt = parse_window_str_to_dt(end_str)
        from_ts = int(start_dt.timestamp())
        to_ts = int(end_dt.timestamp())

        try:
            payload = _fastly_api(f"/stats/service/{logging_svc}?by={by}&from={from_ts}&to={to_ts}", api_key)

            fmt = "%Y-%m-%dT%H:00" if by == "hour" else "%Y-%m-%dT%H:%M" if by == "minute" else "%Y-%m-%d"
            stats_lookup: dict[str, int] = {}
            log_records_lookup: dict[str, int] = {}
            for record in payload.get("data", []):
                ts = record.get("start_time")
                if ts is None:
                    continue
                time_key = datetime.fromtimestamp(ts, tz=UTC).strftime(fmt)
                stats_lookup[time_key] = stats_lookup.get(time_key, 0) + int(record.get("requests") or 0)
                log_count = 0
                for fname in FASTLY_LOG_FIELDS:
                    v = record.get(fname)
                    if v:
                        log_count = int(v)
                        break
                if log_count:
                    log_records_lookup[time_key] = log_records_lookup.get(time_key, 0) + log_count

            total_api_requests = 0
            total_log_records = 0
            existing_points = {p["time"]: p for p in res["data"]}
            all_times = sorted(set(existing_points.keys()) | set(stats_lookup.keys()))

            new_data = []
            for t in all_times:
                p = existing_points.get(t, {"time": t, "row_count": 0, "bytes": 0})
                api_reqs = stats_lookup.get(t, 0)
                p["api_requests"] = api_reqs
                total_api_requests += api_reqs
                log_recs = log_records_lookup.get(t, 0)
                p["fastly_log_records"] = log_recs
                total_log_records += log_recs
                new_data.append(p)

            res["data"] = new_data
            res["total_api_requests"] = total_api_requests
            res["total_fastly_log_records"] = total_log_records

        except Exception:
            logger.debug("usage_log_activity: step failed (ignored)", exc_info=True)

    resp: UsageLogActivityResponse = UsageLogActivityResponse.with_telemetry(**res)
    return resp


@usage_router.get("/rum-breakdown", response_model=UsageRumBreakdownResponse)
@query_errors()
def usage_rum_breakdown(
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="day"),
    source: dict[str, Any] = Depends(get_source),
) -> UsageRumBreakdownResponse:
    """Calculate RUM beacon volume and estimated FOS Class A operations cost."""
    from backend.core import metadata as metadata_db

    requested_by = by
    if by not in ("hour", "day"):
        by = "day"
    clamped_from_sub_hour = requested_by != by

    src = source
    service_id = src.get("name") or src.get("service_id")
    if not service_id or not isinstance(service_id, str):
        empty_resp: UsageRumBreakdownResponse = UsageRumBreakdownResponse.with_telemetry(
            data=[],
            total_beacons=0,
            total_estimated_class_a=0,
            total_estimated_cost_usd=0.0,
            class_a_rate_per_1k=0.0,
            average_beacons_per_day=0.0,
            note="Service not found.",
        )
        return empty_resp

    start_str, end_str, from_ts, to_ts = window_to_epoch(start, end)

    from backend import config as svcconfig

    global_rates = svcconfig.load_usage_logging_config()
    class_a_rate = float(global_rates.get("class_a_rate_per_1k", 0.005))

    try:
        start_date = start_str.split("T")[0]
        end_date = end_str.split("T")[0]
        beacons_by_date: dict[str, int] = metadata_db.get_rum_beacons_by_date(service_id, start_date, end_date)
    except Exception:
        beacons_by_date = {}

    data: list[RumBreakdownPoint] = []
    total_beacons = 0
    total_class_a = 0
    total_cost = 0.0

    for date_str in sorted(beacons_by_date.keys()):
        beacon_count = beacons_by_date[date_str]
        class_a_ops = beacon_count
        cost = (class_a_ops / 1000.0) * class_a_rate

        data.append(
            RumBreakdownPoint(
                date=date_str,
                beacon_count=beacon_count,
                estimated_class_a_ops=class_a_ops,
                estimated_cost_usd=round(cost, 6),
            )
        )

        total_beacons += beacon_count
        total_class_a += class_a_ops
        total_cost += cost

    days = (to_ts - from_ts) / 86400.0
    avg_beacons_per_day = total_beacons / days if days > 0 else 0

    note = (
        "RUM beacon volume is tracked separately. Each beacon is one FOS Class A operation. "
        "This estimate assumes current beacon volume; actual costs depend on user traffic. "
        "These operations are also included in the total FOS Class A operations shown elsewhere."
    )
    if clamped_from_sub_hour:
        note += (
            f" The chart interval you selected ({requested_by}) was adjusted to daily granularity "
            "for RUM data (FOS doesn't provide sub-daily beacon breakdown)."
        )

    resp: UsageRumBreakdownResponse = UsageRumBreakdownResponse.with_telemetry(
        data=data,
        total_beacons=total_beacons,
        total_estimated_class_a=total_class_a,
        total_estimated_cost_usd=round(total_cost, 6),
        class_a_rate_per_1k=class_a_rate,
        average_beacons_per_day=round(avg_beacons_per_day, 2),
        note=note,
    )
    return resp
