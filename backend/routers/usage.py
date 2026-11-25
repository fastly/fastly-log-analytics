"""Usage and cost estimation endpoints."""

from __future__ import annotations

import contextvars
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from backend.config import get_fastly_api_key
from backend.core.clickhouse import get_clickhouse_client
from backend.core.fastly.client import FASTLY_LOG_FIELDS, api_key_or_mock
from backend.core.fos_client import _get_fos_client
from backend.core.request_context import get_source
from backend.core.service_defaults import (
    DEFAULT_LOG_PERIOD_SECONDS,
    DEFAULT_SNAPSHOT_INTERVAL_MINUTES,
)
from backend.high_scale._common import visible_publication_fence_sql
from backend.high_scale.ingest_controller import get_high_scale_service_registry
from backend.models.admin import (
    CurrentStorageResponse,
    PlatformComputeUsage,
    PlatformGeographyPoint,
    PlatformGeographyResponse,
    PlatformNgwafUsage,
    PlatformStorageUsage,
    PlatformUsageResponse,
    PrefillRatesResponse,
    PrefillResponse,
    RumBreakdownPoint,
    UsageBandwidthResponse,
    UsageLogActivityResponse,
    UsageOperationsResponse,
    UsageRumBreakdownResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.repositories import assets as repo
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
)
from backend.repositories.utils.response_shaping import SectionTimer
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import window_to_epoch
from backend.utils.router_utils import query_errors, raise_internal

logger = logging.getLogger(__name__)

usage_router = APIRouter(prefix="/api/usage", tags=["usage"], responses=DEFAULT_ERROR_RESPONSES)
router = usage_router

_USAGE_CACHE_CONTROL = "private, max-age=30, stale-while-revalidate=120"


def _set_usage_cache_control(response: Response) -> None:
    response.headers["Cache-Control"] = _USAGE_CACHE_CONTROL


_FASTLY_STATS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=60.0)
_STORAGE_INSPECTION_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=60.0)
_PREFILL_META_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=60.0)
_PREFILL_ENDPOINT_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=60.0)
_EDGE_RATIO_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=60.0)
_EDGE_RATIO_NONE = object()
_RUM_BREAKDOWN_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=60.0)
_USAGE_RESPONSE_MEMO = ResponseMemo(ttl=60.0, maxsize=256, prewarmer=False)
_CacheRegistry.register("routers.usage._FASTLY_STATS_CACHE", _FASTLY_STATS_CACHE)
_CacheRegistry.register("routers.usage._STORAGE_INSPECTION_CACHE", _STORAGE_INSPECTION_CACHE)
_CacheRegistry.register("routers.usage._PREFILL_META_CACHE", _PREFILL_META_CACHE)
_CacheRegistry.register("routers.usage._PREFILL_ENDPOINT_CACHE", _PREFILL_ENDPOINT_CACHE)
_CacheRegistry.register("routers.usage._EDGE_RATIO_CACHE", _EDGE_RATIO_CACHE)
_CacheRegistry.register("routers.usage._RUM_BREAKDOWN_CACHE", _RUM_BREAKDOWN_CACHE)

STORAGE_PLATFORM_METRICS = (
    "object_storage_buckets_count",
    "object_storage_bytes_count",
)
NGWAF_PLATFORM_METRICS = (
    "ngwaf_requests_total_count",
    "ngwaf_requests_allowed_count",
    "ngwaf_requests_blocked_count",
    "ngwaf_requests_challenged_count",
    "ngwaf_requests_logged_count",
    "ngwaf_requests_timeout_count",
    "ngwaf_requests_unknown_count",
)


def _fastly_api(
    entity_id: str | None,
    granularity: str,
    from_epoch: int,
    to_epoch: int,
    metrics: tuple[str, ...],
    api_key: str,
    group_by: Literal["region", "datacenter"] | None = None,
) -> dict[str, Any]:
    """Fetch Platform Metrics through the central client with TTL caching."""
    from backend.core.fastly.client import fastly_platform_metrics

    token_scope = sha256(api_key.encode()).hexdigest()[:16]
    cache_key = (entity_id, granularity, from_epoch // 60, to_epoch // 60, metrics, group_by, token_scope)
    cached = _FASTLY_STATS_CACHE.get(cache_key)
    if cached is not None:
        return dict(cached)
    response = fastly_platform_metrics(
        entity_id=entity_id,
        granularity=granularity,
        from_epoch=from_epoch,
        to_epoch=to_epoch,
        metrics=metrics,
        token=api_key,
        group_by=group_by,
    )
    _FASTLY_STATS_CACHE[cache_key] = response
    return dict(response)


def _platform_storage(records: list[dict[str, Any]], from_epoch: int, to_epoch: int) -> PlatformStorageUsage:
    bytes_by_day = sorted(
        (int(row["start_time"]), int(row["object_storage_bytes_count"]))
        for row in records
        if row.get("start_time") is not None and row.get("object_storage_bytes_count") is not None
    )
    if not bytes_by_day:
        return PlatformStorageUsage(status="unavailable", scope="account", reason="no_data")

    gb_hours = 0.0
    for index, (start, value) in enumerate(bytes_by_day):
        end = bytes_by_day[index + 1][0] if index + 1 < len(bytes_by_day) else to_epoch
        overlap = max(0, min(end, to_epoch) - max(start, from_epoch))
        gb_hours += value / 1_000_000_000 * overlap / 3600

    bucket_rows = sorted(
        (int(row["start_time"]), int(row["object_storage_buckets_count"]))
        for row in records
        if row.get("start_time") is not None and row.get("object_storage_buckets_count") is not None
    )
    observed_at, latest_bytes = bytes_by_day[-1]
    return PlatformStorageUsage(
        status="ok",
        scope="account",
        observed_at=datetime.fromtimestamp(observed_at, UTC).isoformat().replace("+00:00", "Z"),
        bytes=latest_bytes,
        buckets=bucket_rows[-1][1] if bucket_rows else None,
        gb_hours=gb_hours,
    )


def _platform_ngwaf(records: list[dict[str, Any]]) -> PlatformNgwafUsage:
    if not records:
        return PlatformNgwafUsage(status="unavailable", scope="workspace", reason="no_data")

    totals = {
        name.removeprefix("ngwaf_requests_").removesuffix("_count"): sum(
            int(row[name]) for row in records if row.get(name) is not None
        )
        for name in NGWAF_PLATFORM_METRICS
    }
    if not any(row.get(metric) is not None for row in records for metric in NGWAF_PLATFORM_METRICS):
        return PlatformNgwafUsage(status="unavailable", scope="workspace", reason="no_data")
    return PlatformNgwafUsage(
        status="ok",
        scope="workspace",
        total=totals["total"],
        allowed=totals["allowed"],
        blocked=totals["blocked"],
        challenged=totals["challenged"],
        logged=totals["logged"],
        timeout=totals["timeout"],
        unknown=totals["unknown"],
    )


def _extract_fos_ops(record: dict[str, Any]) -> tuple[int, int]:
    """Read Class A / Class B counts from a Fastly Platform Metrics record."""
    from backend.core.fastly.client import StatsRecord

    return StatsRecord.model_validate(record).fos_ops()


_BILLING_ZONE_RATES: dict[str, float] = {
    "MBZ100": 0.08,
    "MBZ200": 0.12,
    "Global": 0.18,
}


def _billing_zone_for_fos_region(region: str | None) -> str:
    r = (region or "").strip().lower()
    if r.startswith(("us-", "uk-", "eu-")):
        return "MBZ100"
    return "MBZ200"


def _compute_prefill_rates(
    source: dict[str, Any], *, include_metadata_averages: bool = True
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Local-config-only prefill payload (FAST path — no Fastly, no ClickHouse)."""
    from backend import config as svcconfig

    global_rates = svcconfig.load_usage_logging_config()
    result: dict[str, Any] = {
        "avg_log_file_size_kb": None,
        "estimated_bytes_per_line": None,
        "data_days": 0,
        "log_period_seconds": None,
        "sample_rate": 100,
        "edge_only": False,
        "delete_after": True,
        "log_retention_days": 30,
        "rum_retention_days": 30,
        "snapshot_interval_minutes": DEFAULT_SNAPSHOT_INTERVAL_MINUTES,
        "class_a_rate_per_1k": float(global_rates.get("class_a_rate_per_1k", 0.0025)),
        "class_b_rate_per_10k": float(global_rates.get("class_b_rate_per_10k", 0.004)),
        "cdn_egress_rate_per_gb": float(global_rates.get("cdn_egress_rate_per_gb", 0.12)),
        "storage_rate_per_gb_month": float(global_rates.get("storage_rate_per_gb_month", 0.02)),
        "min_billed_days": int(global_rates.get("min_billed_days", 30)),
        "billing_zone": str(global_rates.get("billing_zone", "MBZ200")),
        "fos_region": None,
        "rum_beacons_per_day": 0,
    }

    prov: dict[str, Any] = {}
    try:
        cfg = svcconfig.load_config(source["name"])
        if cfg:
            raw_prov = cfg.get("provisioning")
            prov = raw_prov if isinstance(raw_prov, dict) else {}
            raw_cron_sync = prov.get("cron_sync")
            cron_sync = raw_cron_sync if isinstance(raw_cron_sync, dict) else {}
            fos_region = cfg.get("fos_region") or prov.get("fos_region")
            if isinstance(fos_region, str) and fos_region.strip():
                fos_region_str = fos_region.strip()
                result["fos_region"] = fos_region_str
                if result["billing_zone"] in ("", "MBZ200") and abs(result["cdn_egress_rate_per_gb"] - 0.12) < 1e-6:
                    derived_zone = _billing_zone_for_fos_region(fos_region_str)
                    result["billing_zone"] = derived_zone
                    result["cdn_egress_rate_per_gb"] = _BILLING_ZONE_RATES[derived_zone]
            result["sample_rate"] = int(prov.get("sample_rate", 100))
            result["edge_only"] = prov.get("edge_only", True)
            result["delete_after"] = bool(cron_sync.get("delete_after", True))
            result["log_retention_days"] = int(cron_sync.get("data_retention_days", 30))
            result["rum_retention_days"] = int(cron_sync.get("rum_retention_days", result["log_retention_days"]))
            result["snapshot_interval_minutes"] = svcconfig.resolve_snapshot_interval_minutes(cfg)
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

                period_val = cfg.get("log_period", prov.get("log_period", DEFAULT_LOG_PERIOD_SECONDS))
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

    if include_metadata_averages:
        svc_name = str(source.get("name") or "")
        cached_meta = _PREFILL_META_CACHE.get(svc_name) if svc_name else None
        if cached_meta is not None:
            avg_nodes, rum_beacons = cached_meta
            if avg_nodes is not None:
                result["avg_nodes_per_flush"] = avg_nodes
            result["rum_beacons_per_day"] = rum_beacons
        else:
            from backend.core import metadata as metadata_db

            avg_nodes = None
            # Prefill values are estimates: a failed high-scale read leaves that one field unknown (None, never 0)
            # and is not cached, so the calculator keeps its own default for it and the next visit retries.
            hs_read_failed = False
            hs_service = get_high_scale_service_registry().resolve(svc_name) if svc_name else None
            if hs_service is not None:
                # High-scale source objects live in the Postgres ledger; it never writes ``ingested_files``.
                from backend.high_scale.postgres_control import PostgresControlPlane

                try:
                    avg = PostgresControlPlane().request_files_per_flush(svc_name)
                except Exception:
                    logger.warning("prefill: high-scale nodes per flush unavailable for %s", svc_name, exc_info=True)
                    hs_read_failed = True
                else:
                    if avg:
                        avg_nodes = round(avg)
                        result["avg_nodes_per_flush"] = avg_nodes
            else:
                try:
                    avg = metadata_db.get_node_count_avg(source["name"])
                    if avg:
                        avg_nodes = round(float(avg))
                        result["avg_nodes_per_flush"] = avg_nodes
                except Exception:
                    logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)

            rum_beacons = 0
            if hs_service is not None:
                # High-scale beacons live in ClickHouse RUM facts; it never writes ``ingested_files``.
                from backend.high_scale.rum import rum_beacons_by_date

                rum_end = datetime.now(UTC)
                rum_start = rum_end - timedelta(days=int(result["rum_retention_days"]))
                try:
                    per_day = rum_beacons_by_date(hs_service, rum_start.isoformat(), rum_end.isoformat())
                except Exception:
                    logger.warning(
                        "prefill: high-scale RUM beacons per day unavailable for %s", svc_name, exc_info=True
                    )
                    hs_read_failed = True
                    result["rum_beacons_per_day"] = None
                else:
                    rum_beacons = round(sum(per_day.values()) / len(per_day)) if per_day else 0
                    result["rum_beacons_per_day"] = rum_beacons
            else:
                try:
                    val = metadata_db.get_rum_beacons_avg_per_day(source["name"])
                    rum_beacons = round(val) if val is not None else 0
                    result["rum_beacons_per_day"] = rum_beacons
                except Exception as e:
                    logger.debug("Failed to calculate rum_beacons_per_day: %s", e)

            if svc_name and not hs_read_failed:
                _PREFILL_META_CACHE[svc_name] = (avg_nodes, rum_beacons)

    return result, prov


def _compute_prefill_metadata_averages(
    source: dict[str, Any], *, rum_retention_days: int
) -> tuple[int | None, int | None]:
    """Compute (avg_nodes_per_flush, rum_beacons_per_day), running independent high-scale reads concurrently."""
    svc_name = str(source.get("name") or "")
    cached_meta = _PREFILL_META_CACHE.get(svc_name) if svc_name else None
    if cached_meta is not None:
        return cached_meta

    from backend.core import metadata as metadata_db

    hs_service = get_high_scale_service_registry().resolve(svc_name) if svc_name else None
    if hs_service is not None:
        from backend.high_scale.postgres_control import PostgresControlPlane
        from backend.high_scale.rum import rum_beacons_by_date

        rum_end = datetime.now(UTC)
        rum_start = rum_end - timedelta(days=int(rum_retention_days))

        def _read_nodes() -> tuple[int | None, bool]:
            try:
                avg = PostgresControlPlane().request_files_per_flush(svc_name)
            except Exception:
                logger.warning("prefill: high-scale nodes per flush unavailable for %s", svc_name, exc_info=True)
                return None, True
            return (round(avg) if avg else None), False

        def _read_rum() -> tuple[int | None, bool]:
            try:
                per_day = rum_beacons_by_date(hs_service, rum_start.isoformat(), rum_end.isoformat())
            except Exception:
                logger.warning("prefill: high-scale RUM beacons per day unavailable for %s", svc_name, exc_info=True)
                return None, True
            return (round(sum(per_day.values()) / len(per_day)) if per_day else 0), False

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="usage-prefill-meta") as pool:
            fut_nodes = pool.submit(contextvars.copy_context().run, _read_nodes)
            fut_rum = pool.submit(contextvars.copy_context().run, _read_rum)
            avg_nodes, nodes_failed = fut_nodes.result()
            rum_beacons, rum_failed = fut_rum.result()
        if svc_name and not (nodes_failed or rum_failed):
            _PREFILL_META_CACHE[svc_name] = (avg_nodes, rum_beacons)
        return avg_nodes, rum_beacons

    avg_nodes = None
    try:
        avg = metadata_db.get_node_count_avg(source["name"])
        if avg:
            avg_nodes = round(float(avg))
    except Exception:
        logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)

    rum_beacons = 0
    try:
        val = metadata_db.get_rum_beacons_avg_per_day(source["name"])
        rum_beacons = round(val) if val is not None else 0
    except Exception as e:
        logger.debug("Failed to calculate rum_beacons_per_day: %s", e)

    if svc_name:
        _PREFILL_META_CACHE[svc_name] = (avg_nodes, rum_beacons)
    return avg_nodes, rum_beacons


@usage_router.get(
    "/prefill/rates",
    response_model=PrefillRatesResponse,
    dependencies=[Depends(_set_usage_cache_control)],
)
@query_errors()
def prefill_rates(source: dict[str, Any] = Depends(get_source)) -> PrefillRatesResponse:
    """Instant prefill subset for stat cards."""
    result, _prov = _compute_prefill_rates(source, include_metadata_averages=False)
    resp: PrefillRatesResponse = PrefillRatesResponse.with_telemetry(**result)
    return resp


@usage_router.get(
    "/prefill",
    response_model=PrefillResponse,
    dependencies=[Depends(_set_usage_cache_control)],
)
@query_errors()
async def prefill(source: dict[str, Any] = Depends(get_source)) -> PrefillResponse:
    import asyncio
    import time as _time

    from backend import config as svcconfig
    from backend.config import get_fastly_logging_service_id

    timer = SectionTimer()
    section_timings = timer.entries

    fast_result, prov = _compute_prefill_rates(source, include_metadata_averages=False)
    result: dict[str, Any] = {
        "requests_per_day": None,
        "edge_requests_per_day": None,
        "edge_ratio": None,
        **fast_result,
    }

    async def _fetch_metadata() -> tuple[int | None, int | None]:
        return await asyncio.to_thread(
            _compute_prefill_metadata_averages,
            source,
            rum_retention_days=int(result["rum_retention_days"]),
        )

    api_key = api_key_or_mock(get_fastly_api_key(source["service_id"]))
    now = datetime.now(UTC)

    async def _resolve_endpoint_chain() -> dict[str, Any]:
        if not api_key:
            return {}
        from backend.core.fastly.client import find_condition, get_active_version, get_log_endpoint

        logging_svc_id = (
            source.get("logging_service_id") or source.get("service_id") or get_fastly_logging_service_id() or ""
        ).strip()
        token_scope = sha256(api_key.encode()).hexdigest()[:16]
        updates: dict[str, Any] = {}
        try:
            if not logging_svc_id:
                return updates
            endpoint_name = prov.get("endpoint_name", "Fastly Object Storage Logs")
            ep_cache_key = (logging_svc_id, endpoint_name, token_scope)
            cached_ep = _PREFILL_ENDPOINT_CACHE.get(ep_cache_key)
            if cached_ep is not None:
                return dict(cached_ep)
            active_ver = await asyncio.to_thread(get_active_version, logging_svc_id, api_key)
            if not active_ver:
                return updates
            current_ep = await asyncio.to_thread(get_log_endpoint, logging_svc_id, active_ver, endpoint_name, api_key)
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
            _PREFILL_ENDPOINT_CACHE[ep_cache_key] = dict(updates)
        except Exception:
            logger.debug("_resolve_endpoint_chain: step failed (ignored)", exc_info=True)
        return updates

    async def _fetch_stats() -> dict[str, Any] | None:
        if not api_key:
            return None
        svc_id = (
            source.get("logging_service_id") or source.get("service_id") or get_fastly_logging_service_id() or ""
        ).strip()
        to_ts = (int(now.timestamp()) // 300) * 300
        from_ts = to_ts - 3 * 86_400
        by = "day"
        try:
            if svc_id:
                return await asyncio.to_thread(
                    _fastly_api,
                    svc_id,
                    by,
                    from_ts,
                    to_ts,
                    ("requests", "edge_requests"),
                    api_key,
                )
            return await asyncio.to_thread(
                _fastly_api,
                None,
                by,
                from_ts,
                to_ts,
                ("requests", "edge_requests"),
                api_key,
            )
        except Exception:
            return None

    cached_status = svcconfig.get_status(source["name"]) or {}
    cached_edge_ratio = cached_status.get("edge_ratio")

    async def _fetch_edge_ratio() -> float | None:
        if cached_edge_ratio is not None:
            return float(cached_edge_ratio)

        def _edge_ratio_blocking() -> float | None:
            service_id = str(source.get("service_id") or source["name"])
            cached_ratio = _EDGE_RATIO_CACHE.get(service_id)
            if cached_ratio is _EDGE_RATIO_NONE:
                return None
            if cached_ratio is not None:
                return float(cached_ratio)
            hs_service = get_high_scale_service_registry().resolve(service_id)
            client = hs_service.client if hs_service is not None else get_clickhouse_client()
            if client is None:
                return None
            fence = visible_publication_fence_sql("request", client=client, service_id=service_id)
            start_time = now - timedelta(days=7)
            rows = client.execute(
                f"SELECT countIf(lower(custom_fields['edge']) IN ('true', '1')) * 100.0 / nullIf(count(), 0) AS edge_ratio "
                f"FROM request_facts WHERE service_id={{service_id:String}} "
                f"AND event_timestamp >= {{start_time:DateTime64(3, 'UTC')}} AND {fence}",
                {"service_id": service_id, "start_time": start_time},
            )
            if rows and rows[0].get("edge_ratio") is not None:
                val = round(float(rows[0]["edge_ratio"]), 2)
                _EDGE_RATIO_CACHE[service_id] = val
                return val
            _EDGE_RATIO_CACHE[service_id] = _EDGE_RATIO_NONE
            return None

        try:
            return await asyncio.to_thread(_edge_ratio_blocking)
        except Exception:
            logger.debug("prefill: step failed (ignored)", exc_info=True)
            return None

    _t = _time.perf_counter()
    (avg_nodes, rum_beacons), chain_updates, payload, edge_ratio = await asyncio.gather(
        _fetch_metadata(),
        _resolve_endpoint_chain(),
        _fetch_stats(),
        _fetch_edge_ratio(),
    )
    timer.mark("prefill_parallel_io", _t)

    if avg_nodes is not None:
        result["avg_nodes_per_flush"] = avg_nodes
    result["rum_beacons_per_day"] = rum_beacons

    if api_key:
        try:
            result.update(chain_updates)
            daily_reqs: dict[str, int] = {}
            daily_edge: dict[str, int] = {}
            missing_edge_days: set[str] = set()
            if payload:
                for rec in payload.get("data", []):
                    ts = rec.get("start_time")
                    if ts is None:
                        continue
                    day = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d")
                    if rec.get("requests") is not None:
                        daily_reqs[day] = daily_reqs.get(day, 0) + int(rec["requests"])
                    if rec.get("edge_requests") is not None:
                        daily_edge[day] = daily_edge.get(day, 0) + int(rec["edge_requests"])
                    else:
                        missing_edge_days.add(day)
            today = now.strftime("%Y-%m-%d")
            complete_days = [d for d in daily_reqs if d != today]
            avg_days = complete_days if complete_days else list(daily_reqs)
            if avg_days:
                result["requests_per_day"] = round(sum(daily_reqs[d] for d in avg_days) / len(avg_days))
                if all(d in daily_edge and d not in missing_edge_days for d in avg_days):
                    result["edge_requests_per_day"] = round(sum(daily_edge[d] for d in avg_days) / len(avg_days))
        except Exception:
            logger.debug("prefill: step failed (ignored)", exc_info=True)

    if edge_ratio is not None:
        result["edge_ratio"] = edge_ratio

    debug_queries: list[Any] = []
    resp: PrefillResponse = PrefillResponse.with_telemetry(
        debug_queries=debug_queries, section_timings=section_timings, **result
    )
    return resp


def _get_storage_stats(src: dict[str, Any], start_str: str, end_str: str) -> dict[str, Any]:
    repo_fn = getattr(repo, "get_storage_stats", None)
    if callable(repo_fn):
        return repo_fn(None, src, start_str, end_str)
    from backend.core import metadata as metadata_db

    service_id = str(src.get("name") or src.get("service_id") or "")
    if get_high_scale_service_registry().resolve(service_id) is not None:
        # High-scale ingest records each source object in its Postgres ledger; it never writes ``ingested_files``.
        from backend.high_scale.postgres_control import PostgresControlPlane
        from backend.utils.date_utils import parse_window_str_to_dt

        window = PostgresControlPlane().source_storage_window(
            service_id, parse_window_str_to_dt(start_str), parse_window_str_to_dt(end_str)
        )
        return {
            "total_files": window["files"],
            "total_bytes": window["bytes"],
            "rum_bytes": window["rum_bytes"],
            "_debug_queries": [],
            "_debug_calls": [],
        }
    total_files, total_bytes = metadata_db.get_storage_stats_window(src["name"], start_str, end_str)
    return {
        "total_files": total_files,
        "total_bytes": total_bytes,
        "_debug_queries": [],
        "_debug_calls": [],
    }


def _get_log_activity(src: dict[str, Any], start_str: str, end_str: str, by: str) -> dict[str, Any]:
    repo_fn = getattr(repo, "get_log_activity", None)
    if callable(repo_fn):
        return repo_fn(src, start_str, end_str, by)
    from backend.core import metadata as metadata_db

    service_id = str(src.get("name") or src.get("service_id") or "")
    if get_high_scale_service_registry().resolve(service_id) is not None:
        # High-scale ingest records each source object in its Postgres ledger; it never writes ``ingested_files``.
        from backend.high_scale.postgres_control import PostgresControlPlane
        from backend.utils.date_utils import parse_window_str_to_dt

        buckets = PostgresControlPlane().source_activity(
            service_id, parse_window_str_to_dt(start_str), parse_window_str_to_dt(end_str), by
        )
        points = [{"time": t, "row_count": rows, "bytes": size} for t, rows, size in buckets]
        return {
            "data": points,
            "total_rows": sum(p["row_count"] for p in points),
            "total_bytes": sum(p["bytes"] for p in points),
            "granularity": by,
            "_debug_queries": [],
            "_debug_calls": [],
        }
    out = metadata_db.get_log_activity(service_id, start_str, end_str, by)
    return {**out, "_debug_queries": [], "_debug_calls": []}


@usage_router.get(
    "/current-storage",
    response_model=CurrentStorageResponse,
    dependencies=[Depends(_set_usage_cache_control)],
)
@query_errors()
def usage_current_storage(
    start: str = Query(default=""),
    end: str = Query(default=""),
    source: dict[str, Any] = Depends(get_source),
) -> CurrentStorageResponse:
    src = source
    if not src.get("bucket"):
        raise HTTPException(status_code=400, detail={"error": "Fastly Object Storage bucket not configured."})

    from backend.core.storage_billing import StorageBillingInterval, calculate_storage_billing
    from backend.utils.date_utils import iso_z, parse_date_window, parse_window_str_to_dt

    now = datetime.now(UTC)
    observed_at = iso_z(now)
    start_str, end_str = parse_date_window(start, end)
    start_dt = parse_window_str_to_dt(start_str)
    end_dt = parse_window_str_to_dt(end_str)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    try:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(src["name"]) or {}
        cron_sync = cfg.get("provisioning", {}).get("cron_sync", {})
        delete_after = cron_sync.get("delete_after", True)
        retention_days = int(cron_sync.get("data_retention_days", 30))
        service_id = str(src.get("service_id") or src["name"])

        storage_memo_digest = digest_cache_key(
            {
                "ep": "usage_current_storage",
                "name": src.get("name"),
                "bucket": src.get("bucket"),
                "prefix": src.get("prefix"),
                "s": bucket_time_to_minute(start_str),
                "e": bucket_time_to_minute(end_str),
                "delete_after": bool(delete_after),
                "retention_days": retention_days,
            },
            {"service_id": service_id},
        )
        storage_memo_key = f"{service_id}:{storage_memo_digest}"
        cached_storage = _USAGE_RESPONSE_MEMO.get_with_telemetry(storage_memo_key)
        if cached_storage is not None:
            return CurrentStorageResponse.with_telemetry(**cached_storage)

        def _compute_storage() -> dict[str, Any]:
            if (cached_inner := _USAGE_RESPONSE_MEMO.get_with_telemetry(storage_memo_key)) is not None:
                return dict(cached_inner)

            retention_end_str = now.strftime("%Y-%m-%d %H:%M:%S")
            MIN_BILLING_DAYS = 30
            query_days = MIN_BILLING_DAYS if delete_after else retention_days
            retention_start_str = (now - timedelta(days=query_days)).strftime("%Y-%m-%d %H:%M:%S")

            cache_key = (src.get("name"), src.get("bucket"), src.get("prefix"))
            cached_insp = _STORAGE_INSPECTION_CACHE.get(cache_key)

            try:
                high_scale_service = get_high_scale_service_registry().resolve(service_id)
                archive_storage_complete = high_scale_service is None
            except Exception:
                high_scale_service = None
                archive_storage_complete = False
                logger.debug("usage_current_storage: archive applicability failed (ignored)", exc_info=True)

            def _read_quarantine() -> int:
                try:
                    from backend.core.metadata.ingest_log import get_quarantine_storage_total

                    return get_quarantine_storage_total(src["name"])
                except Exception:
                    logger.debug("usage_current_storage: get_quarantine_storage_total failed (ignored)", exc_info=True)
                    return 0

            def _read_archive() -> tuple[int, int, int, int, bool]:
                try:
                    from backend.high_scale.postgres_control import PostgresControlPlane

                    summary = PostgresControlPlane().archive_storage_summary(service_id)
                    return (
                        summary["live_bytes"],
                        summary["live_objects"],
                        summary["expired_30d_bytes"],
                        int(summary.get("live_rum_bytes", 0)),
                        True,
                    )
                except Exception:
                    logger.debug("usage_current_storage: archive summary failed (ignored)", exc_info=True)
                    return 0, 0, 0, 0, False

            def _read_cloud_copy_deleted() -> tuple[int, bool]:
                try:
                    from backend.core.metadata.usage_log import cloud_copy_deleted_bytes_in_billing_window

                    return cloud_copy_deleted_bytes_in_billing_window(src["name"]), True
                except Exception:
                    logger.debug("usage_current_storage: cloud copy deletions failed (ignored)", exc_info=True)
                    return 0, False

            def _read_pg_billing() -> tuple[float, float, float, bool, list[str]]:
                try:
                    from backend.high_scale.postgres_control import PostgresControlPlane

                    return PostgresControlPlane().storage_billing_summary(
                        service_id, start_dt, end_dt, minimum_days=MIN_BILLING_DAYS
                    )
                except Exception:
                    logger.debug("usage_current_storage: storage_billing_summary failed", exc_info=True)
                    return 0.0, 0.0, 0.0, False, ["high-scale:postgres_unavailable"]

            def _read_cloud_copy_intervals() -> tuple[list[dict[str, Any]], bool]:
                try:
                    from backend.core.metadata.fos_usage import cloud_copy_deletion_records_in_window

                    return (
                        cloud_copy_deletion_records_in_window(
                            src["name"], start_dt, end_dt, minimum_days=MIN_BILLING_DAYS
                        ),
                        True,
                    )
                except Exception:
                    logger.debug("usage_current_storage: cloud copy deletion records failed", exc_info=True)
                    return [], False

            with ThreadPoolExecutor(max_workers=6, thread_name_prefix="usage-storage") as pool:
                fut_stats = pool.submit(
                    contextvars.copy_context().run, _get_storage_stats, src, retention_start_str, retention_end_str
                )
                fut_quarantine = (
                    pool.submit(contextvars.copy_context().run, _read_quarantine) if cached_insp is None else None
                )
                fut_archive = (
                    pool.submit(contextvars.copy_context().run, _read_archive)
                    if high_scale_service is not None
                    else None
                )
                fut_cc_deleted = pool.submit(contextvars.copy_context().run, _read_cloud_copy_deleted)
                fut_pg_billing = (
                    pool.submit(contextvars.copy_context().run, _read_pg_billing)
                    if high_scale_service is not None
                    else None
                )
                fut_cc_intervals = (
                    pool.submit(contextvars.copy_context().run, _read_cloud_copy_intervals)
                    if high_scale_service is not None
                    else None
                )

                stats = fut_stats.result()
                quarantine_from_db = fut_quarantine.result() if fut_quarantine is not None else 0
                if fut_archive is not None:
                    (
                        archive_bytes,
                        archive_objects,
                        archive_expired_bytes,
                        archive_rum_bytes,
                        archive_storage_complete,
                    ) = fut_archive.result()
                else:
                    archive_bytes = archive_objects = archive_expired_bytes = archive_rum_bytes = 0
                cloud_copy_deleted_bytes, cloud_copy_deleted_bytes_complete = fut_cc_deleted.result()
                pg_billing_res = fut_pg_billing.result() if fut_pg_billing is not None else None
                cc_intervals_res = fut_cc_intervals.result() if fut_cc_intervals is not None else None

            total_files = stats["total_files"]
            total_bytes = stats["total_bytes"]
            debug_queries = stats.get("debug_queries", [])

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

                quarantine_bytes = quarantine_from_db

                rum_bytes = 0
                rum_from_meta = "rum_bytes" in stats
                if storage_info_success and not rum_from_meta:
                    try:
                        from backend.core.metadata.ingest_log import get_rum_storage_stats_window

                        rum_bytes = get_rum_storage_stats_window(src["name"], retention_start_str, retention_end_str)
                        rum_from_meta = True
                    except Exception:
                        logger.debug("usage_current_storage: metadata rum_bytes failed (ignored)", exc_info=True)

                if not rum_from_meta:
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

            if "rum_bytes" in stats:
                rum_bytes = int(stats["rum_bytes"])
            regular_log_bytes = total_bytes - rum_bytes

            if delete_after:
                live_bytes = storage_bytes + quarantine_bytes + archive_bytes
                live_files = storage_files + archive_objects
                deleted_bytes = total_bytes + archive_expired_bytes + cloud_copy_deleted_bytes
            else:
                live_bytes = total_bytes + storage_bytes + quarantine_bytes + archive_bytes
                live_files = total_files + storage_files + archive_objects
                deleted_bytes = archive_expired_bytes + cloud_copy_deleted_bytes

            billed_bytes = (
                total_bytes
                + storage_bytes
                + quarantine_bytes
                + archive_bytes
                + archive_expired_bytes
                + cloud_copy_deleted_bytes
            )
            billed_gb = billed_bytes / 1_000_000_000
            gb_hours = billed_gb * (MIN_BILLING_DAYS * 24)

            live_billed_gb_hours = 0.0
            deleted_billed_gb_hours = 0.0
            minimum_billed_gb_hours = 0.0
            storage_billing_complete = False
            storage_billing_provenance: list[str] = []
            storage_intervals: list[StorageBillingInterval] = []

            storage_reads_ok = False
            if (
                high_scale_service is not None
                and isinstance(pg_billing_res, tuple)
                and len(pg_billing_res) == 5
                and isinstance(cc_intervals_res, tuple)
                and len(cc_intervals_res) == 2
            ):
                pg_live, pg_deleted, pg_min, pg_complete, pg_provenance = pg_billing_res
                cloud_copy_intervals, cloud_copy_intervals_ok = cc_intervals_res
                pg_unavailable = "high-scale:postgres_unavailable" in pg_provenance

                if end_ms >= start_ms:
                    cc_summary = calculate_storage_billing(cloud_copy_intervals, start_ms, end_ms)
                    live_billed_gb_hours = pg_live + cc_summary.live_gb_hours
                    deleted_billed_gb_hours = pg_deleted + cc_summary.deleted_gb_hours
                    minimum_billed_gb_hours = pg_min + cc_summary.minimum_billed_gb_hours
                    gb_hours = live_billed_gb_hours + deleted_billed_gb_hours + minimum_billed_gb_hours
                    storage_reads_ok = (
                        not pg_unavailable
                        and archive_storage_complete
                        and cloud_copy_deleted_bytes_complete
                        and cloud_copy_intervals_ok
                    )
                    storage_billing_complete = pg_complete and cc_summary.complete and storage_reads_ok
                    storage_billing_provenance = sorted(set(pg_provenance) | set(cc_summary.provenance))
                else:
                    gb_hours = 0.0
                    storage_billing_complete = False
                    storage_billing_provenance = sorted(set(pg_provenance))
            else:
                storage_reads_ok = archive_storage_complete and cloud_copy_deleted_bytes_complete
                storage_billing_complete = storage_reads_ok
                storage_billing_provenance = (
                    ["metadata_ledgers"] if storage_billing_complete else ["metadata_ledgers_incomplete"]
                )

            payload: dict[str, Any] = {
                "debug_queries": debug_queries,
                "live_bytes": live_bytes,
                "live_files": live_files,
                "deleted_bytes": deleted_bytes,
                "deleted_bytes_complete": archive_storage_complete and cloud_copy_deleted_bytes_complete,
                "deleted_bytes_provenance": (
                    "metadata_ledgers"
                    if archive_storage_complete and cloud_copy_deleted_bytes_complete
                    else "metadata_ledgers_incomplete"
                ),
                "quarantine_bytes": quarantine_bytes,
                "total_billed_bytes": billed_bytes,
                "total_billed_gb_hours": gb_hours,
                "storage_billing_complete": storage_billing_complete,
                "storage_billing_provenance": storage_billing_provenance,
                "live_billed_gb_hours": live_billed_gb_hours,
                "deleted_billed_gb_hours": deleted_billed_gb_hours,
                "minimum_billed_gb_hours": minimum_billed_gb_hours,
                "storage_intervals": storage_intervals,
                "observed_at": observed_at,
                "archive_bytes": archive_bytes,
                "archive_objects": archive_objects,
                "archive_expired_bytes": archive_expired_bytes,
                "cloud_copy_deleted_bytes": cloud_copy_deleted_bytes,
                "total_files": total_files,
                "total_bytes": total_bytes,
                "rum_bytes": rum_bytes,
                "regular_log_bytes": regular_log_bytes,
                "processed_request_bytes": archive_bytes - archive_rum_bytes,
                "processed_rum_bytes": archive_rum_bytes,
                "processed_objects": archive_objects,
                "raw_logs_deleted": bool(delete_after),
                "start": start_str,
                "end": end_str,
            }
            if storage_reads_ok:
                _USAGE_RESPONSE_MEMO.put(storage_memo_key, payload)
            return payload

        result_payload = _USAGE_RESPONSE_MEMO.coalesce(f"usage_current_storage:{storage_memo_key}", _compute_storage)
        resp: CurrentStorageResponse = CurrentStorageResponse.with_telemetry(**result_payload)
        return resp
    except Exception as e:
        raise_internal(logger, e, code="usage_storage_failed")


@usage_router.get("/platform", response_model=PlatformUsageResponse)
@query_errors()
def platform_usage(
    start: str = Query(default=""),
    end: str = Query(default=""),
    source: dict[str, Any] = Depends(get_source),
) -> PlatformUsageResponse:
    from backend import config as svcconfig

    _, _, from_epoch, to_epoch = window_to_epoch(start, end)
    service_id = str(source.get("service_id") or "")
    workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
    token = api_key_or_mock(get_fastly_api_key(str(source.get("logging_service_id") or service_id)))

    compute = PlatformComputeUsage(status="not_configured")
    if not token:
        storage = PlatformStorageUsage(status="unavailable", scope="account", reason="missing_token")
        ngwaf = PlatformNgwafUsage(
            status="unavailable" if workspace_id else "not_configured",
            scope="workspace" if workspace_id else None,
            reason="missing_token" if workspace_id else None,
        )
    else:

        def _fetch_storage() -> PlatformStorageUsage:
            try:
                storage_payload = _fastly_api(
                    None,
                    "day",
                    from_epoch,
                    to_epoch,
                    STORAGE_PLATFORM_METRICS,
                    token,
                )
            except Exception:
                return PlatformStorageUsage(status="unavailable", scope="account", reason="request_failed")
            try:
                return _platform_storage(storage_payload.get("data", []), from_epoch, to_epoch)
            except Exception:
                return PlatformStorageUsage(status="unavailable", scope="account", reason="invalid_data")

        def _fetch_ngwaf() -> PlatformNgwafUsage:
            if not workspace_id:
                return PlatformNgwafUsage(status="not_configured")
            try:
                ngwaf_payload = _fastly_api(
                    workspace_id,
                    "day",
                    from_epoch,
                    to_epoch,
                    NGWAF_PLATFORM_METRICS,
                    token,
                )
            except Exception:
                return PlatformNgwafUsage(
                    status="unavailable",
                    scope="workspace",
                    reason="request_failed",
                )
            try:
                return _platform_ngwaf(ngwaf_payload.get("data", []))
            except Exception:
                return PlatformNgwafUsage(
                    status="unavailable",
                    scope="workspace",
                    reason="invalid_data",
                )

        if workspace_id:
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="usage-platform") as pool:
                fut_storage = pool.submit(contextvars.copy_context().run, _fetch_storage)
                fut_ngwaf = pool.submit(contextvars.copy_context().run, _fetch_ngwaf)
                storage = fut_storage.result()
                ngwaf = fut_ngwaf.result()
        else:
            storage = _fetch_storage()
            ngwaf = PlatformNgwafUsage(status="not_configured")

    statuses = (storage.status, ngwaf.status)
    source_status: Literal["fastly", "partial", "unavailable"]
    if "ok" in statuses:
        source_status = "partial" if "unavailable" in statuses else "fastly"
    else:
        source_status = "unavailable"
    return PlatformUsageResponse(
        source=source_status,
        storage=storage,
        compute=compute,
        ngwaf=ngwaf,
    )


@usage_router.get("/geography", response_model=PlatformGeographyResponse)
@query_errors()
def geography(
    start: str = Query(default=""),
    end: str = Query(default=""),
    group_by: Literal["region", "datacenter"] = Query(default="region"),
    source: dict[str, Any] = Depends(get_source),
) -> PlatformGeographyResponse:
    entity_id = str(source.get("cdn_service_id") or "").strip()
    if not entity_id:
        return PlatformGeographyResponse(
            source="unavailable",
            group_by=group_by,
            data=[],
            reason="not_configured",
        )

    token_service_id = str(source.get("logging_service_id") or source.get("service_id") or entity_id).strip()
    token = api_key_or_mock(get_fastly_api_key(token_service_id))
    if not token:
        return PlatformGeographyResponse(
            source="unavailable",
            group_by=group_by,
            data=[],
            reason="missing_token",
        )

    _, _, from_epoch, to_epoch = window_to_epoch(start, end)
    try:
        payload = _fastly_api(
            entity_id,
            "day",
            from_epoch,
            to_epoch,
            ("requests", "bandwidth"),
            token,
            group_by,
        )
        records = payload.get("data")
        if not isinstance(records, list):
            raise ValueError("invalid geography data")
        if not records:
            return PlatformGeographyResponse(
                source="unavailable",
                group_by=group_by,
                data=[],
                reason="no_data",
            )

        groups: dict[str, dict[str, int]] = {}
        for record in records:
            if not isinstance(record, dict):
                raise ValueError("invalid geography row")
            location = record.get(group_by)
            if not isinstance(location, str) or not location.strip():
                raise ValueError("invalid geography group")
            values = groups.setdefault(location, {"requests": 0, "bandwidth_bytes": 0})
            values["requests"] += int(record.get("requests") or 0)
            values["bandwidth_bytes"] += int(record.get("bandwidth") or 0)
    except (TypeError, ValueError):
        return PlatformGeographyResponse(
            source="unavailable",
            group_by=group_by,
            data=[],
            reason="invalid_data",
        )
    except Exception:
        return PlatformGeographyResponse(
            source="unavailable",
            group_by=group_by,
            data=[],
            reason="request_failed",
        )

    total_requests = sum(values["requests"] for values in groups.values())
    total_bandwidth = sum(values["bandwidth_bytes"] for values in groups.values())
    points = [
        PlatformGeographyPoint(
            location=location,
            requests=values["requests"],
            bandwidth_bytes=values["bandwidth_bytes"],
            request_share=values["requests"] / total_requests if total_requests else 0.0,
            bandwidth_share=values["bandwidth_bytes"] / total_bandwidth if total_bandwidth else 0.0,
        )
        for location, values in groups.items()
    ]
    points.sort(key=lambda point: (-point.requests, point.location))
    return PlatformGeographyResponse(
        source="fastly",
        group_by=group_by,
        data=points,
        requests=total_requests,
        bandwidth_bytes=total_bandwidth,
    )


@usage_router.get(
    "/operations",
    response_model=UsageOperationsResponse,
    dependencies=[Depends(_set_usage_cache_control)],
)
@query_errors()
def usage_operations(
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="day"),
    source: dict[str, Any] = Depends(get_source),
) -> UsageOperationsResponse:
    from typing import Literal, cast

    from backend.core.metadata.fos_usage import get_usage_operation_buckets
    from backend.utils.date_utils import parse_date_window

    requested_by = by
    granularity: Literal["hour", "day"] = "day" if by == "day" else "hour"
    clamped_from_sub_hour = requested_by != granularity

    src = source
    service_id = str(src.get("service_id") or src["name"])
    start_str, end_str = parse_date_window(start, end)

    ops_digest = digest_cache_key(
        {
            "ep": "usage_operations",
            "s": bucket_time_to_minute(start_str),
            "e": bucket_time_to_minute(end_str),
            "g": granularity,
            "clamped": clamped_from_sub_hour,
            "req_by": requested_by if clamped_from_sub_hour else granularity,
        },
        {"service_id": service_id},
    )
    ops_memo_key = f"{service_id}:{ops_digest}"
    cached_ops = _USAGE_RESPONSE_MEMO.get_with_telemetry(ops_memo_key)
    if cached_ops is not None:
        return UsageOperationsResponse.with_telemetry(**cached_ops)

    def _compute_ops() -> dict[str, Any]:
        if (cached_inner := _USAGE_RESPONSE_MEMO.get_with_telemetry(ops_memo_key)) is not None:
            return dict(cached_inner)
        try:
            local = get_usage_operation_buckets(
                service_id, start_str, end_str, cast(Literal["hour", "day"], granularity)
            )
        except Exception:
            logger.debug("usage_operations: local usage_log query failed", exc_info=True)
            return {
                "data": [],
                "total_class_a": 0,
                "total_class_b": 0,
                "granularity": granularity,
                "source": "local",
                "complete": False,
                "provenance": [],
                "note": "Local Object Storage operation ledger is unavailable for this service.",
                "fos_fields_found": [],
            }

        note = "Measured from this service's local Object Storage operation ledger."
        if clamped_from_sub_hour:
            note += f" Granularity '{requested_by}' normalized to '{granularity}'."
        payload = {
            "data": local["data"],
            "total_class_a": local["total_class_a"],
            "total_class_b": local["total_class_b"],
            "granularity": granularity,
            "source": "local",
            "complete": True,
            "provenance": ["high-scale:usage_log"],
            "note": note,
            "fos_fields_found": ["usage_log.class_a", "usage_log.class_b"],
        }
        _USAGE_RESPONSE_MEMO.put(ops_memo_key, payload)
        return payload

    ops_payload = _USAGE_RESPONSE_MEMO.coalesce(f"usage_operations:{ops_memo_key}", _compute_ops)
    resp: UsageOperationsResponse = UsageOperationsResponse.with_telemetry(**ops_payload)
    return resp


@usage_router.get(
    "/bandwidth",
    response_model=UsageBandwidthResponse,
    dependencies=[Depends(_set_usage_cache_control)],
)
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
    service_id = str(src.get("service_id") or src.get("name") or "")
    cdn_svc = src.get("cdn_service_id", "").strip()
    token_scope = sha256(str(api_key).encode()).hexdigest()[:16]

    bw_digest = digest_cache_key(
        {
            "ep": "usage_bandwidth",
            "cdn_svc": cdn_svc,
            "s": bucket_time_to_minute(start_str),
            "e": bucket_time_to_minute(end_str),
            "by": by,
            "tok": token_scope,
        },
        {"service_id": service_id},
    )
    bw_memo_key = f"{service_id}:{bw_digest}"
    cached_bw = _USAGE_RESPONSE_MEMO.get_with_telemetry(bw_memo_key)
    if cached_bw is not None:
        return UsageBandwidthResponse.with_telemetry(**cached_bw)

    def _compute_bandwidth() -> dict[str, Any]:
        if (cached_inner := _USAGE_RESPONSE_MEMO.get_with_telemetry(bw_memo_key)) is not None:
            return dict(cached_inner)

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
                payload = _fastly_api(
                    cdn_svc,
                    by,
                    from_ts,
                    to_ts,
                    ("bandwidth", "requests"),
                    api_key,
                )
                _merge(payload)
            except Exception as e:
                raise_internal(logger, e, code="fastly_stats_service_failed", status=502)

        fmt = "%Y-%m-%dT%H:00" if by == "hour" else "%Y-%m-%dT%H:%M" if by == "minute" else "%Y-%m-%d"
        points = [{"time": datetime.fromtimestamp(ts, tz=UTC).strftime(fmt), **v} for ts, v in sorted(agg.items())]
        result_payload = {
            "data": points,
            "total_bytes": sum(v["bandwidth_bytes"] for v in agg.values()),
            "total_log_bytes": 0,
            "granularity": by,
        }
        _USAGE_RESPONSE_MEMO.put(bw_memo_key, result_payload)
        return result_payload

    bw_payload = _USAGE_RESPONSE_MEMO.coalesce(f"usage_bandwidth:{bw_memo_key}", _compute_bandwidth)
    resp: UsageBandwidthResponse = UsageBandwidthResponse.with_telemetry(**bw_payload)
    return resp


@usage_router.get(
    "/log-activity",
    response_model=UsageLogActivityResponse,
    dependencies=[Depends(_set_usage_cache_control)],
)
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

    service_id = str(source.get("service_id") or source.get("name") or "")
    api_key = api_key_or_mock(get_fastly_api_key(source.get("logging_service_id", "")))
    logging_svc = source.get("logging_service_id", "").strip()
    can_fetch_fastly = bool(api_key and logging_svc and by in ("minute", "hour", "day"))
    token_scope = sha256(str(api_key).encode()).hexdigest()[:16] if api_key else ""

    act_digest = digest_cache_key(
        {
            "ep": "usage_log_activity",
            "name": source.get("name"),
            "log_svc": logging_svc,
            "s": bucket_time_to_minute(start_str),
            "e": bucket_time_to_minute(end_str),
            "by": by,
            "tok": token_scope,
        },
        {"service_id": service_id},
    )
    act_memo_key = f"{service_id}:{act_digest}"
    cached_act = _USAGE_RESPONSE_MEMO.get_with_telemetry(act_memo_key)
    if cached_act is not None:
        return UsageLogActivityResponse.with_telemetry(**cached_act)

    def _compute_activity() -> dict[str, Any]:
        if (cached_inner := _USAGE_RESPONSE_MEMO.get_with_telemetry(act_memo_key)) is not None:
            return dict(cached_inner)

        payload: dict[str, Any] | None = None
        if can_fetch_fastly:
            import concurrent.futures
            import contextvars

            from backend.utils.date_utils import parse_window_str_to_dt

            start_dt = parse_window_str_to_dt(start_str)
            end_dt = parse_window_str_to_dt(end_str)
            from_ts = int(start_dt.timestamp())
            to_ts = int(end_dt.timestamp())
            ctx_repo = contextvars.copy_context()
            ctx_api = contextvars.copy_context()
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                fut_repo = pool.submit(ctx_repo.run, _get_log_activity, source, start_str, end_str, by)
                fut_api = pool.submit(
                    ctx_api.run,
                    _fastly_api,
                    logging_svc,
                    by,
                    from_ts,
                    to_ts,
                    ("requests", *FASTLY_LOG_FIELDS),
                    str(api_key),
                )
                res = fut_repo.result()
                try:
                    payload = fut_api.result()
                except Exception:
                    logger.debug("usage_log_activity: step failed (ignored)", exc_info=True)
                    payload = None
        else:
            res = _get_log_activity(source, start_str, end_str, by)

        if payload is not None:
            try:
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

        if res.get("data") or res.get("total_rows") or res.get("total_bytes"):
            _USAGE_RESPONSE_MEMO.put(act_memo_key, res)
        return res

    act_payload = _USAGE_RESPONSE_MEMO.coalesce(f"usage_log_activity:{act_memo_key}", _compute_activity)
    resp: UsageLogActivityResponse = UsageLogActivityResponse.with_telemetry(**act_payload)
    return resp


@usage_router.get(
    "/rum-breakdown",
    response_model=UsageRumBreakdownResponse,
    dependencies=[Depends(_set_usage_cache_control)],
)
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
    class_a_rate = float(global_rates.get("class_a_rate_per_1k", 0.0025))

    hs_service = get_high_scale_service_registry().resolve(service_id)
    rum_digest = digest_cache_key(
        {
            "ep": "usage_rum_breakdown",
            "mode": "high_scale" if hs_service is not None else "legacy",
            "s": bucket_time_to_minute(start_str),
            "e": bucket_time_to_minute(end_str),
            "by": by,
            "req_by": requested_by if clamped_from_sub_hour else by,
            "rate": class_a_rate,
        },
        {"service_id": service_id},
    )
    rum_memo_key = f"{service_id}:{rum_digest}"
    cached_rum = _USAGE_RESPONSE_MEMO.get_with_telemetry(rum_memo_key)
    if cached_rum is not None:
        return UsageRumBreakdownResponse.with_telemetry(**cached_rum)

    def _compute_rum() -> dict[str, Any]:
        if (cached_inner := _USAGE_RESPONSE_MEMO.get_with_telemetry(rum_memo_key)) is not None:
            return dict(cached_inner)

        beacons_by_date: dict[str, int]
        cacheable = True
        if hs_service is not None:
            # High-scale ingest records RUM in ClickHouse facts only; it never writes ``ingested_files``.
            from backend.high_scale.rum import rum_beacons_by_date

            beacons_by_date = rum_beacons_by_date(hs_service, start_str, end_str)
        else:
            try:
                start_date = start_str.split("T")[0]
                end_date = end_str.split("T")[0]
                rum_cache_key = (service_id, start_date, end_date)
                cached_beacons = _RUM_BREAKDOWN_CACHE.get(rum_cache_key)
                if cached_beacons is not None:
                    beacons_by_date = dict(cached_beacons)
                else:
                    beacons_by_date = metadata_db.get_rum_beacons_by_date(service_id, start_date, end_date)
                    if beacons_by_date:
                        _RUM_BREAKDOWN_CACHE[rum_cache_key] = dict(beacons_by_date)
                    else:
                        cacheable = False
            except Exception:
                beacons_by_date = {}
                cacheable = False

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

        payload: dict[str, Any] = {
            "data": data,
            "total_beacons": total_beacons,
            "total_estimated_class_a": total_class_a,
            "total_estimated_cost_usd": round(total_cost, 6),
            "class_a_rate_per_1k": class_a_rate,
            "average_beacons_per_day": round(avg_beacons_per_day, 2),
            "note": note,
        }
        if cacheable:
            _USAGE_RESPONSE_MEMO.put(rum_memo_key, payload)
        return payload

    rum_payload = _USAGE_RESPONSE_MEMO.coalesce(f"usage_rum_breakdown:{rum_memo_key}", _compute_rum)
    resp: UsageRumBreakdownResponse = UsageRumBreakdownResponse.with_telemetry(**rum_payload)
    return resp
