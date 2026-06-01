"""Usage router — cost estimator prefill, FOS storage, operations, bandwidth, log activity."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.core.fastly.utils import FASTLY_LOG_FIELDS
from backend.deps import get_con, get_source
from backend.models.usage import (
    CurrentStorageResponse,
    PrefillResponse,
    UsageBandwidthResponse,
    UsageLogActivityResponse,
    UsageOperationsResponse,
)
from backend.repositories import usage as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api/usage", tags=["usage"])


def _fastly_api(path: str, api_key: str) -> dict:
    req = urllib.request.Request(
        f"https://api.fastly.com{path}",
        headers={"Fastly-Key": api_key, "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _extract_fos_ops(record: dict) -> tuple[int, int]:
    """Read Class A / Class B counts from a Fastly /stats/aggregate record.

    The aggregate endpoint returns object_storage metrics flattened with an
    `object_storage_` prefix (object_storage_class_a_operations_count, etc.),
    not nested under an `object_storage` dict. We accept either shape.
    """

    def _get(d, key):
        return int(d.get(key) or 0)

    a = _get(record, "object_storage_class_a_operations_count")
    b = _get(record, "object_storage_class_b_operations_count")
    if a or b:
        return a, b

    sub = record.get("object_storage")
    if isinstance(sub, dict):
        return _get(sub, "class_a_operations_count"), _get(sub, "class_b_operations_count")
    return 0, 0


@router.get("/prefill", response_model=PrefillResponse)
@query_errors()
def prefill(source: dict = Depends(get_source)):
    from backend import config as svcconfig
    from backend.config import get_fastly_api_key, get_fastly_logging_service_id

    global_rates = svcconfig.load_usage_logging_config()

    result: dict = {
        "requests_per_day": None,
        "edge_requests_per_day": None,
        "avg_log_file_size_kb": None,
        "estimated_bytes_per_line": None,
        "data_days": 0,
        "log_period_seconds": None,
        "commit_interval_mins": 5,
        "sample_rate": 100,
        "edge_only": False,
        "edge_ratio": None,
        "compaction_enabled": True,
        "delete_after": True,
        "log_retention_days": 90,
        "class_a_rate_per_1k": float(global_rates.get("class_a_rate_per_1k", 0.005)),
        "class_b_rate_per_10k": float(global_rates.get("class_b_rate_per_10k", 0.01)),
        "cdn_egress_rate_per_gb": float(global_rates.get("cdn_egress_rate_per_gb", 0.12)),
        "storage_rate_per_gb_month": float(global_rates.get("storage_rate_per_gb_month", 0.02)),
        "min_billed_days": int(global_rates.get("min_billed_days", 30)),
    }

    cfg = None
    prov = {}
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
                from backend.core import log_fields as lf

                lf_cfg = cfg.get("log_fields") or prov.get("log_fields", {})
                if not lf_cfg:
                    lf_cfg = {"groups": ["A", "B", "C", "D"], "field_overrides": {}}
                result["estimated_bytes_per_line"] = lf.estimate_log_line_bytes(lf_cfg)
            except Exception:
                pass
            try:
                from backend.provision import parse_period

                period_val = cfg.get("log_period", prov.get("log_period", 60))
                period_secs = period_val if isinstance(period_val, int) else parse_period(str(period_val))
                result["log_period_seconds"] = period_secs
            except Exception:
                pass
    except Exception:
        pass

    # Use cached status — fastest source for avg file size and data date range.
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
                    from backend.utils.date_utils import _parse_dt as _pdt

                    _now = datetime.now(UTC)
                    e_dt = _pdt(earliest, _now)
                    l_dt = _pdt(latest, _now)
                    result["data_days"] = max(1, (l_dt - e_dt).days + 1)
                except Exception:
                    pass
    except Exception:
        pass

    # Pull real traffic numbers and live logging config from Fastly Stats API.
    api_key = get_fastly_api_key(source["service_id"])
    if api_key:
        from backend.core.fastly.client import fastly
        from backend.core.fastly.service import find_condition, get_active_version

        # CDN service for bandwidth stats; logging service for S3 endpoint config lookup
        stats_svc_id = (source.get("cdn_service_id") or get_fastly_logging_service_id() or "").strip()
        logging_svc_id = (source.get("logging_service_id") or get_fastly_logging_service_id() or "").strip()
        svc_id = stats_svc_id
        now = datetime.now(UTC)
        from_ts = int((now - timedelta(days=3)).timestamp())
        to_ts = int(now.timestamp())
        by = "day"
        try:
            daily_reqs: dict[str, int] = {}
            daily_edge: dict[str, int] = {}
            if svc_id:
                try:
                    active_ver = get_active_version(logging_svc_id, api_key) if logging_svc_id else None
                    if active_ver:
                        endpoint_name = prov.get("endpoint_name", "Fastly Object Storage Logs")
                        encoded_name = urllib.parse.quote(endpoint_name, safe="")
                        current_ep = fastly(
                            "GET",
                            f"/service/{logging_svc_id}/version/{active_ver}/logging/s3/{encoded_name}",
                            token=api_key,
                        )
                        if "period" in current_ep:
                            result["log_period_seconds"] = int(current_ep["period"])
                        cond_name = current_ep.get("response_condition")
                        if cond_name == "Log Sampling":
                            import re

                            cond = find_condition(cond_name, logging_svc_id, active_ver, api_key)
                            if cond:
                                stmt = cond.get("statement", "")
                                m = re.search(r"randombool\((\d+),", stmt)
                                if m:
                                    result["sample_rate"] = int(m.group(1))
                                if "req.restarts == 0" in stmt:
                                    result["edge_only"] = True
                except Exception:
                    pass
                from backend.utils.telemetry import tracked_call as _tc

                with _tc("GET", f"/stats/service/{svc_id}?by={by}", service="Fastly API"):
                    payload = _fastly_api(f"/stats/service/{svc_id}?by={by}&from={from_ts}&to={to_ts}", api_key)
                for rec in payload.get("data", []):
                    ts = rec.get("start_time")
                    if ts is None:
                        continue
                    day = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d")
                    daily_reqs[day] = daily_reqs.get(day, 0) + int(rec.get("requests") or 0)
                    daily_edge[day] = daily_edge.get(day, 0) + int(rec.get("edge_requests") or 0)
            else:
                from backend.utils.telemetry import tracked_call as _tc

                with _tc("GET", f"/stats/aggregate?by={by}", service="Fastly API"):
                    payload = _fastly_api(f"/stats/aggregate?by={by}&from={from_ts}&to={to_ts}", api_key)
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
            pass

    debug_queries: list = []
    try:
        from backend.core.duckdb import get_connection

        # read_only: get_edge_ratio is a SELECT against the view.
        con = get_connection(source=source, max_wait=5, read_only=True)
        try:
            edge_ratio, debug_queries = repo.get_edge_ratio(con, source)
            if edge_ratio is not None:
                result["edge_ratio"] = edge_ratio

            # Empirical node count analysis for prefill — derive from per-service
            # ingested_files SQLite metadata.
            try:
                from backend.core import metadata_db

                avg = metadata_db.get_node_count_avg(source["name"])
                if avg:
                    result["avg_nodes_per_flush"] = round(float(avg))
            except Exception:
                pass
        finally:
            con.close()
    except Exception:
        pass

    return PrefillResponse.with_telemetry(debug_queries=debug_queries, **result)


@router.get("/current-storage", response_model=CurrentStorageResponse)
@query_errors()
def usage_current_storage(
    start: str = Query(default=""),
    end: str = Query(default=""),
    source: dict = Depends(get_source),
):
    from backend.core.duckdb import _get_fos_client

    src = source
    if not src.get("bucket"):
        raise HTTPException(status_code=400, detail={"error": "Fastly Object Storage bucket not configured."})

    from datetime import UTC, datetime, timedelta

    from backend.utils.date_utils import parse_date_window

    now = datetime.now(UTC)
    start_str, end_str = parse_date_window(start, end)

    try:
        from backend import config as svcconfig
        from backend.core.duckdb import get_connection

        # Config lookup is keyed on service_id; fall back to {} when the
        # config file is missing (e.g. mid-teardown, or when name != service_id
        # for any reason). Defensive: a None cfg here would AttributeError
        # on the subsequent .get() calls and 500 the cost panel.
        cfg = svcconfig.load_config(src["name"]) or {}
        cron_sync = cfg.get("provisioning", {}).get("cron_sync", {})
        delete_after = cron_sync.get("delete_after", True)
        retention_days = int(cron_sync.get("log_retention_days", 30))
        retention_end_str = now.strftime("%Y-%m-%d %H:%M:%S")
        # When delete_after=True, raw files are deleted quickly but billed for Fastly's
        # 30-day minimum. Query exactly that window to capture all currently-billed files.
        MIN_BILLING_DAYS = 30
        query_days = MIN_BILLING_DAYS if delete_after else retention_days
        retention_start_str = (now - timedelta(days=query_days)).strftime("%Y-%m-%d %H:%M:%S")

        # get_storage_stats reads from per-service SQLite metadata_db
        # (file list + sizes), not the Iceberg view — RO + skip-view is safe
        # and avoids contending with ingest's writer lock.
        con = get_connection(source=src, max_wait=5, skip_view_update=True, read_only=True)
        try:
            stats = repo.get_storage_stats(con, src, retention_start_str, retention_end_str)
        finally:
            con.close()

        total_files = stats["total_files"]
        total_bytes = stats["total_bytes"]
        debug_queries = stats.get("debug_queries", [])

        # Prefer the cached metadata path: get_table_info reads manifest
        # summaries (with multi-layer local caching) instead of doing a
        # paginated FOS LIST on iceberg/. The undercount vs LIST is just
        # metadata-file bytes (manifests/snapshots, typically <1% of total),
        # and trading that for ~5s of cloud LIST per panel open is correct
        # per the standing "be a little behind the data rather than read
        # extra from the cloud" rule. The FOS-LIST fallback below still
        # covers the cold-start case where get_table_info errors out.
        iceberg_bytes = 0
        iceberg_files = 0
        try:
            from backend.core import iceberg as db_iceberg

            iceberg_info = db_iceberg.get_table_info(src)
            if not iceberg_info.get("error"):
                iceberg_bytes = iceberg_info.get("size_bytes", 0)
                iceberg_files = iceberg_info.get("data_files", 0)
        except Exception:
            pass

        if iceberg_bytes == 0:
            try:
                s3 = _get_fos_client(src)
                bucket = src["bucket"]
                prefix = f"{src.get('prefix', '').strip().rstrip('/')}/" if src.get("prefix") else ""
                iceberg_prefix = f"{prefix}iceberg/"
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=iceberg_prefix.rstrip("/")):
                    for obj in page.get("Contents", []):
                        iceberg_bytes += obj.get("Size", 0)
                        iceberg_files += 1
            except Exception as e:
                import logging

                logging.error(f"FOS Scan failed: {e}")

        if delete_after:
            live_bytes = iceberg_bytes
            live_files = iceberg_files
            deleted_bytes = total_bytes
            deleted_files = total_files
        else:
            live_bytes = total_bytes + iceberg_bytes
            live_files = total_files + iceberg_files
            deleted_bytes = 0
            deleted_files = 0

        billed_bytes = total_bytes + iceberg_bytes
        # GB-hours using the 720-hour (30-day) basis so the frontend can divide by 720
        # to get GB-months cost. This correctly applies the 30-day minimum billing
        # floor: files deleted in < 30 days are still billed for their full minimum period.
        billed_gb = billed_bytes / (1024 * 1024 * 1024)
        gb_hours = billed_gb * (MIN_BILLING_DAYS * 24)

        return CurrentStorageResponse.with_telemetry(
            debug_queries=debug_queries,
            live_bytes=live_bytes,
            live_files=live_files,
            deleted_bytes=deleted_bytes,
            total_billed_bytes=billed_bytes,
            total_billed_gb_hours=gb_hours,
            total_files=total_files,
            total_bytes=total_bytes,
            start=start_str,
            end=end_str,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": str(e)})


@router.get("/operations", response_model=UsageOperationsResponse)
@query_errors()
def usage_operations(
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="day"),
    source: dict = Depends(get_source),
):
    # /stats/aggregate is account-wide and very slow at sub-hour granularity;
    # clamp to hour minimum. Track whether we clamped so the frontend can
    # surface the discrepancy ("you asked for 1m but the chart is 1h").
    requested_by = by
    if by not in ("hour", "day"):
        by = "hour"
    clamped_from_sub_hour = requested_by != by

    from backend.config import get_fastly_api_key

    src = source
    api_key = get_fastly_api_key(src["logging_service_id"])
    if not api_key:
        raise HTTPException(status_code=403, detail={"error": "Fastly API key is not configured."})

    from datetime import UTC, datetime

    from backend.utils.date_utils import parse_date_window

    start_str, end_str = parse_date_window(start, end)
    from backend.utils.date_utils import parse_window_str_to_dt

    start_dt = parse_window_str_to_dt(start_str)
    end_dt = parse_window_str_to_dt(end_str)

    from_ts = int(start_dt.timestamp())
    to_ts = int(end_dt.timestamp())

    agg: dict[str, dict] = {}
    fos_fields_found: set[str] = set()

    def _accumulate(records: list) -> None:
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

    from backend.utils.telemetry import tracked_call

    try:
        with tracked_call("GET", f"/stats/aggregate?by={by}", service="Fastly API"):
            payload = _fastly_api(f"/stats/aggregate?by={by}&from={from_ts}&to={to_ts}", api_key)
        _accumulate(payload.get("data", []))
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise HTTPException(status_code=502, detail={"error": f"Fastly Stats API {e.code}: {body}"})
    except Exception as e:
        raise HTTPException(status_code=502, detail={"error": str(e)})

    points = [{"date": d, **v} for d, v in sorted(agg.items())]
    total_a = sum(d["class_a"] for d in points)
    total_b = sum(d["class_b"] for d in points)
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
    return UsageOperationsResponse.with_telemetry(
        data=points,
        total_class_a=total_a,
        total_class_b=total_b,
        granularity=by,
        note=note,
        fos_fields_found=sorted(fos_fields_found),
    )


@router.get("/bandwidth", response_model=UsageBandwidthResponse)
@query_errors()
def usage_bandwidth(
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="hour"),
    source: dict = Depends(get_source),
):
    from backend.config import get_fastly_api_key

    src = source
    api_key = get_fastly_api_key(src["logging_service_id"])
    if not api_key:
        raise HTTPException(status_code=403, detail={"error": "Fastly API key is not configured."})

    if by not in ("hour", "minute", "day"):
        by = "hour"

    from datetime import UTC, datetime

    from backend.utils.date_utils import parse_date_window

    start_str, end_str = parse_date_window(start, end)
    from backend.utils.date_utils import parse_window_str_to_dt

    start_dt = parse_window_str_to_dt(start_str)
    end_dt = parse_window_str_to_dt(end_str)

    from_ts = int(start_dt.timestamp())
    to_ts = int(end_dt.timestamp())

    cdn_svc = src.get("cdn_service_id", "").strip()
    agg: dict[int, dict] = {}

    def _merge(payload):
        for record in payload.get("data", []):
            ts = record.get("start_time")
            if ts is None:
                continue
            if ts not in agg:
                agg[ts] = {"bandwidth_bytes": 0, "requests": 0}
            agg[ts]["bandwidth_bytes"] += int(record.get("bandwidth") or 0)
            agg[ts]["requests"] += int(record.get("requests") or 0)

    from backend.utils.telemetry import tracked_call

    if cdn_svc:
        try:
            with tracked_call("GET", f"/stats/service/{cdn_svc}?by={by}", service="Fastly API"):
                payload = _fastly_api(f"/stats/service/{cdn_svc}?by={by}&from={from_ts}&to={to_ts}", api_key)
            _merge(payload)
        except Exception as e:
            raise HTTPException(status_code=502, detail={"error": str(e)})

    fmt = "%Y-%m-%dT%H:00" if by == "hour" else "%Y-%m-%dT%H:%M" if by == "minute" else "%Y-%m-%d"
    points = [{"time": datetime.fromtimestamp(ts, tz=UTC).strftime(fmt), **v} for ts, v in sorted(agg.items())]
    return UsageBandwidthResponse.with_telemetry(
        data=points,
        total_bytes=sum(p["bandwidth_bytes"] for p in points),
        total_log_bytes=0,
        granularity=by,
    )


@router.get("/log-activity", response_model=UsageLogActivityResponse)
@query_errors()
def usage_log_activity(
    source: dict = Depends(get_source),
    con=Depends(get_con),
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="hour"),
):
    if by not in ("second", "minute", "hour", "day"):
        by = "hour"

    from datetime import UTC, datetime

    from backend.config import get_fastly_api_key
    from backend.utils.date_utils import parse_date_window

    now = datetime.now(UTC)
    start_str, end_str = parse_date_window(start, end)

    res = repo.get_log_activity(con, source, start_str, end_str, by)

    # Fetch Fastly API stats for the logging service to compare generated vs processed
    api_key = get_fastly_api_key(source.get("logging_service_id", ""))
    logging_svc = source.get("logging_service_id", "").strip()

    if api_key and logging_svc and by in ("minute", "hour", "day"):
        from backend.utils.date_utils import parse_window_str_to_dt

        start_dt = parse_window_str_to_dt(start_str)
        end_dt = parse_window_str_to_dt(end_str)
        from_ts = int(start_dt.timestamp())
        to_ts = int(end_dt.timestamp())

        try:
            from backend.utils.telemetry import tracked_call

            with tracked_call("GET", f"/stats/service/{logging_svc}?by={by}", service="Fastly API"):
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
                # Pick the first non-zero candidate field for log records;
                # mirrors compute_log_accounting's probe-list strategy.
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
            pass

    return UsageLogActivityResponse.with_telemetry(**res)
