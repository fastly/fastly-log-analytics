"""RUM analytics, beacon health, and live events endpoints."""

from __future__ import annotations

import asyncio
import datetime as _dt_mod
import logging
import os
from types import SimpleNamespace
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from backend import config as svcconfig
from backend.core.duckdb import rum_source_for
from backend.core.request_context import (
    RequestContext,
    _ConnectionHolder,
    build_request_context,
)
from backend.core.storage import execute_with_stale_view_retry
from backend.models.analytics import (
    RumAnalyticsResponse,
    RumBeaconHealthResponse,
    RumLiveEventItem,
)
from backend.repositories import rum as rum_repo
from backend.repositories.utils.response_cache import bucket_time_to_minute, digest_cache_key
from backend.routers.rum.provisioning import _normalize_sec
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.date_utils import iso_z, parse_iso_utc
from backend.utils.router_utils import correlation_id, make_error

logger = logging.getLogger(__name__)

repo = rum_repo

# Short/recent RUM windows (<= this span) are served directly from the
# already-attached read connection via a raw client_vitals/client_errors scan,
# never a synchronous in-request rollup recompute.
_RUM_RAW_WINDOW_MAX_S = int(os.getenv("RUM_RAW_WINDOW_MAX_S", str(6 * 60 * 60)))

_RUM_FILTERS_MESSAGE = (
    "filters must be a JSON object over browser, os, device, country, pathname, city, region, pop or tls"
)

rum_analytics_router = APIRouter()


def _inject_telemetry(res: dict) -> dict:
    from backend.models.common import _debug_responses_enabled
    from backend.utils.telemetry import debug_envelope

    if not _debug_responses_enabled():
        return res
    return debug_envelope(res, gated=False)


def _parse_rum_filter_specs(filters: str | None) -> dict[str, Any]:
    """Parse and validate RUM ``filters`` query parameter into ``FilterSpec`` objects."""
    if not filters:
        return {}
    from backend.high_scale.rum import _FILTER_COLUMNS, parse_rum_filters
    from backend.models.common import FilterSpec
    from backend.repositories.utils.filters import filter_spec_attr, normalize_filter_key

    try:
        raw_dict = parse_rum_filters(filters)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
        ) from exc

    parsed_filters: dict[str, Any] = {}
    unsupported: list[str] = []
    for raw_key, spec in raw_dict.items():
        values = [
            value
            for value in (filter_spec_attr(spec, "values") or [])
            if value is not None and not (isinstance(value, str) and value.strip() == "")
        ]
        column = normalize_filter_key(raw_key)
        if values and column not in _FILTER_COLUMNS:
            unsupported.append(raw_key)
            continue
        if isinstance(spec, dict):
            parsed_filters[raw_key] = FilterSpec(mode=spec.get("mode", "include"), values=spec.get("values", []))
        elif isinstance(spec, FilterSpec):
            parsed_filters[raw_key] = spec
    if unsupported:
        raise HTTPException(status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE))
    return parsed_filters


def _rum_beacon_health_sync(ctx: RequestContext) -> dict[str, Any]:
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import rum_beacon_health as hs_rum_health

        try:
            return hs_rum_health(high_scale_service, source=ctx.source)
        except TypeError:
            return hs_rum_health(high_scale_service)

    service_id = ctx.service_id
    cfg = svcconfig.load_config(service_id) or {}
    if not svcconfig.is_rum_enabled(cfg):
        return {
            "enabled": False,
            "beacon_fire_rate": 0,
            "recent_beacons": 0,
            "last_beacon_time": None,
            "setup_complete": False,
            "message": "RUM not enabled for this service",
        }

    try:
        rum_source = rum_source_for(ctx.source)

        with _ConnectionHolder(rum_source, read_only=True) as rum_con:
            recent_count, last_dt = execute_with_stale_view_retry(
                rum_con,
                rum_source,
                lambda con: rum_repo.get_beacon_health(con, source=rum_source),
            )

        last_beacon_time = None
        if last_dt:
            if isinstance(last_dt, _dt_mod.datetime):
                last_beacon_time = last_dt.isoformat()
            else:
                try:
                    last_beacon_dt = _dt_mod.datetime.fromisoformat(str(last_dt))
                    last_beacon_time = last_beacon_dt.isoformat()
                except (ValueError, TypeError):
                    pass

        has_data = recent_count > 0 or last_dt is not None
        return {
            "enabled": True,
            "beacon_fire_rate": recent_count,
            "recent_beacons": recent_count,
            "last_beacon_time": last_beacon_time,
            "setup_complete": has_data,
            "message": "Script installed and firing" if has_data else "Waiting for beacons...",
        }
    except Exception as e:
        logger.error(f"[rum] Setup check failed for {service_id}: {e}")
        return {
            "enabled": True,
            "beacon_fire_rate": 0,
            "recent_beacons": 0,
            "last_beacon_time": None,
            "setup_complete": False,
            "message": f"Setup check failed: {e}",
        }


@rum_analytics_router.get(
    "/{service_id}/rum/beacon-health", response_model=RumBeaconHealthResponse, response_model_exclude_unset=True
)
async def rum_beacon_health(
    request: Request,
    ctx: RequestContext = Depends(build_request_context),
) -> dict[str, Any]:
    """Check if RUM beacons are arriving (validation endpoint for setup)."""
    return _inject_telemetry(await asyncio.to_thread(_rum_beacon_health_sync, ctx))


@rum_analytics_router.get(
    "/{service_id}/rum/analytics", response_model=RumAnalyticsResponse, response_model_exclude_unset=True
)
async def rum_analytics(
    request: Request,
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    filters: str | None = Query(None),
    ctx: RequestContext = Depends(build_request_context),
    range_token: Annotated[str | None, Query()] = None,
    anchor: Annotated[str | None, Query()] = None,
) -> dict[str, Any]:
    """Retrieve parsed RUM analytics from DuckDB views."""
    window = SimpleNamespace(range_token=range_token, anchor=anchor, start_time=start_time, end_time=end_time)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import parse_rum_filters
        from backend.high_scale.rum import rum_analytics as hs_rum_analytics

        start_time, end_time = ctx.resolve_window(window)
        try:
            parsed_hs_filters = parse_rum_filters(filters)
            hs_result = await asyncio.to_thread(
                hs_rum_analytics, high_scale_service, start_time, end_time, parsed_hs_filters
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
            ) from exc
        return _inject_telemetry(hs_result)

    service_id = ctx.service_id

    start_time, end_time = ctx.resolve_window(window)

    if not start_time and not end_time:
        end_dt = _dt_mod.datetime.now(_dt_mod.UTC)
        start_dt = end_dt - _dt_mod.timedelta(hours=24)
        start_time = iso_z(start_dt)
        end_time = iso_z(end_dt)
    else:
        if start_time:
            parsed_start = parse_iso_utc(start_time)
            if parsed_start:
                start_time = iso_z(parsed_start)
        if end_time:
            parsed_end = parse_iso_utc(end_time)
            if parsed_end:
                end_time = iso_z(parsed_end)

    parsed_since = parse_iso_utc(start_time) if start_time else None
    since: _dt_mod.datetime = (
        parsed_since if parsed_since is not None else (_dt_mod.datetime.now(_dt_mod.UTC) - _dt_mod.timedelta(hours=24))
    )

    parsed_until = parse_iso_utc(end_time) if end_time else None
    until: _dt_mod.datetime = parsed_until if parsed_until is not None else _dt_mod.datetime.now(_dt_mod.UTC)

    parsed_filters = _parse_rum_filter_specs(filters)

    from backend.high_scale.rum import _FILTER_COLUMNS
    from backend.repositories.utils.filters import build_where_clause

    params, where_sql = build_where_clause(
        start_time,
        end_time,
        parsed_filters,
        actual_cols=list(_FILTER_COLUMNS),
        exclude_invalid_ips=False,
    )

    rum_source = rum_source_for(ctx.source)

    try:

        def _get_analytics(con):
            return rum_repo.fetch_rum_analytics(
                con,
                service_id=service_id,
                start_time=start_time,
                end_time=end_time,
                parsed_filters=parsed_filters,
                where_sql=where_sql,
                params=params,
                raw_window_max_s=_RUM_RAW_WINDOW_MAX_S,
                source=rum_source,
            )

        def _run_analytics_query():
            with _ConnectionHolder(rum_source, read_only=True) as rum_con:
                return execute_with_stale_view_retry(rum_con, rum_source, _get_analytics)

        db_res = await asyncio.to_thread(_run_analytics_query)

        if db_res.get("no_data", False):
            return _inject_telemetry(
                {
                    "is_mock": False,
                    "no_data": True,
                    "beacon_count": 0,
                    "pageview_count": 0,
                    "interaction_count": 0,
                    "error_count": 0,
                    "message": "Waiting for real-time RUM user events...",
                    "vitals": {
                        "lcp": {"p75": None, "distribution": None},
                        "cls": {"p75": None, "distribution": None},
                        "inp": {"p75": None, "distribution": None},
                        "fid": {"p75": None, "fcp": None, "ttfb": None},
                    },
                    "worst_pages": [],
                    "errors": [],
                    "trends": {"timestamps": [], "lcp": [], "cls": [], "error_rate": []},
                    "environments": {"browsers": {}, "os": {}, "devices": {}},
                }
            )

        vitals: dict[str, Any] = {
            "lcp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
            "cls": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
            "inp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
            "fid": {"p75": None, "fcp": None, "ttfb": None},
        }

        for metric, p75, total, good, ni, poor in db_res["vitals_rows"]:
            if not metric:
                continue
            key = metric.lower()
            if key in ("lcp", "cls", "inp"):
                if total > 0:
                    good_pct = int(good * 100 / total)
                    poor_pct = int(poor * 100 / total)
                    ni_pct = max(0, 100 - good_pct - poor_pct)
                else:
                    good_pct, ni_pct, poor_pct = 0, 0, 0

                if key == "cls":
                    p75_val = round(p75, 3) if p75 is not None else None
                elif key == "inp":
                    p75_val = int(p75) if p75 is not None else None
                else:
                    p75_val = _normalize_sec(p75)

                vitals[key] = {
                    "p75": p75_val,
                    "distribution": {"good": good_pct, "needs_improvement": ni_pct, "poor": poor_pct},
                }
            elif key == "fid":
                vitals["fid"]["p75"] = round(p75, 1) if p75 is not None else None
            elif key in ("fcp", "ttfb") and p75 is not None:
                vitals["fid"][key] = _normalize_sec(p75)

        worst_pages = [
            {
                "path": path,
                "views": views,
                "avg_load_time": _normalize_sec(avg_load),
                "lcp_p75": _normalize_sec(lcp),
                "cls_p75": round(cls, 3) if cls is not None else None,
                "error_rate": round(err_rate, 2) if err_rate is not None else 0.0,
            }
            for path, views, avg_load, lcp, cls, err_rate in db_res["worst_pages_rows"]
        ]

        errors = [
            {"message": msg, "file": file or "unknown.js", "line": line or 0, "col": col or 0, "count": count}
            for msg, file, line, col, count in db_res["errors_rows"]
        ]

        span_hours = (until - since).total_seconds() / 3600.0
        use_hourly = span_hours <= 48
        trend_buckets: dict[str, dict[str, float | int | None]] = {}

        curr = (
            since.replace(minute=0, second=0, microsecond=0)
            if use_hourly
            else since.replace(hour=0, minute=0, second=0, microsecond=0)
        )
        target = (
            until.replace(minute=0, second=0, microsecond=0)
            if use_hourly
            else until.replace(hour=0, minute=0, second=0, microsecond=0)
        )
        step = _dt_mod.timedelta(hours=1) if use_hourly else _dt_mod.timedelta(days=1)
        fmt = "%Y-%m-%d %H:00" if use_hourly else "%Y-%m-%d"

        while curr <= target:
            bucket_key = curr.strftime(fmt)
            trend_buckets[bucket_key] = {
                "lcp": None,
                "cls": None,
                "error_rate": None,
                "pageviews": 0,
                "interactions": 0,
                "errors": 0,
            }
            curr += step

        for hour_ts, lcp, cls, pvs, ints, errs, error_rate in db_res["trends_rows"]:
            if not hour_ts:
                continue
            if isinstance(hour_ts, str):
                try:
                    dt = _dt_mod.datetime.fromisoformat(hour_ts.replace("Z", "+00:00"))
                except Exception:
                    continue
            else:
                dt = hour_ts
            bucket_key = dt.strftime(fmt)
            if bucket_key in trend_buckets:
                trend_buckets[bucket_key] = {
                    "lcp": _normalize_sec(lcp),
                    "cls": round(cls, 3) if cls is not None else None,
                    "error_rate": round(error_rate, 2) if error_rate is not None else 0.0,
                    "pageviews": int(pvs or 0),
                    "interactions": int(ints or 0),
                    "errors": int(errs or 0),
                }

        sorted_keys = sorted(trend_buckets.keys())
        trend_timestamps: list[str] = []
        trend_lcps: list[float | None] = []
        trend_clss: list[float | None] = []
        trend_error_rates: list[float | None] = []
        trend_pageviews: list[int] = []
        trend_interactions: list[int] = []
        trend_errors: list[int] = []

        for k in sorted_keys:
            try:
                dt_parsed = _dt_mod.datetime.strptime(k, fmt)
                dt_parsed = dt_parsed.replace(tzinfo=_dt_mod.UTC)
                display_ts = dt_parsed.isoformat()
            except Exception:
                display_ts = k

            trend_timestamps.append(display_ts)
            stats = trend_buckets[k]
            trend_lcps.append(stats["lcp"])
            trend_clss.append(stats["cls"])
            trend_error_rates.append(stats["error_rate"])
            trend_pageviews.append(int(stats["pageviews"] or 0))
            trend_interactions.append(int(stats["interactions"] or 0))
            trend_errors.append(int(stats["errors"] or 0))

        return _inject_telemetry(
            {
                "is_mock": False,
                "no_data": False,
                "beacon_count": db_res["total_beacons"],
                "pageview_count": db_res.get("pageviews", 0),
                "interaction_count": db_res.get("interactions", 0),
                "error_count": db_res.get("errors_count", 0),
                "vitals": vitals,
                "worst_pages": worst_pages,
                "errors": errors,
                "trends": {
                    "timestamps": trend_timestamps,
                    "lcp": trend_lcps,
                    "cls": trend_clss,
                    "error_rate": trend_error_rates,
                    "pageviews": trend_pageviews,
                    "interactions": trend_interactions,
                    "errors": trend_errors,
                },
                "environments": {
                    "browsers": db_res["browsers"],
                    "os": db_res["os"],
                    "devices": db_res["devices"],
                },
            }
        )

    except Exception as e:
        error_id = correlation_id(request)
        logger.error(f"[rum] Analytics query failed for {service_id} error_id={error_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=make_error("rum_analytics_failed", "RUM analytics query failed", error_id=error_id),
        )


_live_events_cache: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=15)
CacheRegistry.register("rum._live_events_cache", _live_events_cache)


@rum_analytics_router.get("/{service_id}/rum/live-events", response_model=list[RumLiveEventItem])
async def rum_live_events(
    start_time: str | None = None,
    end_time: str | None = None,
    filters: str | None = None,
    ctx: RequestContext = Depends(build_request_context),
    range_token: Annotated[str | None, Query()] = None,
    anchor: Annotated[str | None, Query()] = None,
) -> list[dict[str, Any]]:
    """Fetch recent live beacons stream to feed frontend ticker."""
    window = SimpleNamespace(range_token=range_token, anchor=anchor, start_time=start_time, end_time=end_time)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import parse_rum_filters
        from backend.high_scale.rum import rum_live_events as hs_rum_live

        start_time, end_time = ctx.resolve_window(window)
        try:
            parsed_hs_filters = parse_rum_filters(filters)
            return await asyncio.to_thread(hs_rum_live, high_scale_service, start_time, end_time, 50, parsed_hs_filters)
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
            ) from exc

    service_id = ctx.service_id

    start_time, end_time = ctx.resolve_window(window)

    if not start_time and not end_time:
        end_dt = _dt_mod.datetime.now(_dt_mod.UTC)
        start_dt = end_dt - _dt_mod.timedelta(hours=24)
        start_time = iso_z(start_dt)
        end_time = iso_z(end_dt)

    parsed_filters = _parse_rum_filter_specs(filters)

    from backend.high_scale.rum import _FILTER_COLUMNS
    from backend.repositories.utils.filters import build_where_clause

    rum_source = rum_source_for(ctx.source)
    rum_db_path = rum_source.get("duckdb_path")
    db_mtime = 0
    if rum_db_path:
        try:
            db_mtime = os.stat(rum_db_path).st_mtime_ns
        except OSError:
            db_mtime = 0

    cache_key = digest_cache_key(
        {
            "svc": service_id,
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": filters or "",
            "m": db_mtime,
        },
        rum_source,
    )
    cached_events = _live_events_cache.get(cache_key)
    if cached_events:
        return list(cached_events)

    try:

        def _get_live_events(con):
            actual_cols = [c for c in rum_repo.get_client_vitals_columns(con) if c in _FILTER_COLUMNS] or list(
                _FILTER_COLUMNS
            )

            s_dt = parse_iso_utc(start_time) if start_time else None
            e_dt = parse_iso_utc(end_time) if end_time else None
            if s_dt and e_dt and (e_dt - s_dt) > _dt_mod.timedelta(hours=2):
                narrow_start = iso_z(e_dt - _dt_mod.timedelta(hours=2))
                narrow_params, narrow_where = build_where_clause(
                    narrow_start,
                    end_time,
                    parsed_filters,
                    actual_cols=actual_cols,
                    partition_pruning=True,
                    exclude_invalid_ips=False,
                )
                narrow_rows = rum_repo.fetch_rum_live_events(
                    con, where_sql=narrow_where, params=narrow_params, source=rum_source
                )
                if len(narrow_rows) >= 50:
                    return narrow_rows

            params, where_sql = build_where_clause(
                start_time,
                end_time,
                parsed_filters,
                actual_cols=actual_cols,
                partition_pruning=True,
                exclude_invalid_ips=False,
            )
            return rum_repo.fetch_rum_live_events(con, where_sql=where_sql, params=params, source=rum_source)

        def _run_live_events_query():
            with _ConnectionHolder(rum_source, read_only=True) as rum_con:
                return execute_with_stale_view_retry(rum_con, rum_source, _get_live_events)

        rows = await asyncio.to_thread(_run_live_events_query)

        events = []
        for (
            etype,
            ts,
            path,
            mname,
            mval,
            mrating,
            browser,
            os_name,
            device,
            cid,
            req_id,
            err_msg,
            city,
            region,
            country,
            pop,
            tls,
            ttfb,
        ) in rows:
            desc = "Page loaded successfully"
            if etype == "error":
                desc = err_msg or "JS Error"
            elif mname:
                metric_name_upper = mname.upper()
                val_str = f": {mval}" if mval is not None else ""
                desc = f"Metric {metric_name_upper}{val_str}"
                if mrating:
                    desc += f" ({mrating.upper()})"

            if ttfb is not None:
                if ttfb < 15:
                    ttfb = int(ttfb * 1000)
                else:
                    ttfb = int(ttfb)

            events.append(
                {
                    "time": ts.isoformat() if isinstance(ts, _dt_mod.datetime) else str(ts),
                    "type": etype,
                    "path": path or "/",
                    "desc": desc,
                    "browser": browser or "Unknown",
                    "os": os_name or "Unknown",
                    "raw_log": {
                        "meta": {
                            "browser": {"name": browser or "Unknown"},
                            "os": {"name": os_name or "Unknown"},
                            "device": {"type": device or "Unknown"},
                            "page": {"url": path or "/"},
                        },
                        "measurements": [
                            {
                                "type": "web-vitals",
                                "values": {mname: mval} if mname else {},
                                "context": {"rating": mrating or ""},
                            }
                        ]
                        if mname
                        else [],
                        "exceptions": [
                            {
                                "type": "Error",
                                "value": err_msg or "JS Error",
                            }
                        ]
                        if etype == "error"
                        else [],
                        "cid": cid,
                        "req_id": req_id,
                        "city": city or "Unknown",
                        "region": region or "Unknown",
                        "country": country or "Unknown",
                        "pop": pop or "Unknown",
                        "tls": tls or "Unknown",
                        "ttfb": ttfb,
                    },
                }
            )

        if events:
            _live_events_cache[cache_key] = events
        return events
    except Exception as e:
        logger.error(f"[rum] Failed to fetch live events for {service_id}: {e}")
        return []
