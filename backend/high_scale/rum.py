from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale._projection_utils import range_value
from backend.high_scale._scope import visible_fact_clauses
from backend.high_scale.registry import HighScaleService
from backend.repositories.utils.filters import filter_spec_attr, normalize_filter_key
from backend.utils.date_utils import safe_iso

# Columns present on BOTH rum fact tables; a filter on anything else cannot apply to every beacon kind.
_FILTER_COLUMNS = frozenset({"browser", "os", "device", "country", "pathname", "city", "region", "pop", "tls"})
_BEACON_IDENTITY = """
        coalesce(nullIf(toString(request_event_id), ''),
                 concat(client_id, '_', toString(toInt64(round(toUnixTimestamp64Milli(event_timestamp) / 1000.0)))))
    """


def parse_rum_filters(raw: str | None) -> dict[str, Any]:
    """Decode the ``filters`` query parameter (a JSON object of ``{column: {mode, values}}``)."""
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("filters must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("filters must be a JSON object")
    return payload


def _filter_clauses(filters: Mapping[str, Any] | None) -> tuple[list[str], dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    unsupported: list[str] = []
    for i, (raw_key, spec) in enumerate((filters or {}).items()):
        values = [
            value
            for value in (filter_spec_attr(spec, "values") or [])
            if value is not None and not (isinstance(value, str) and value.strip() == "")
        ]
        if not values:
            continue
        column = normalize_filter_key(raw_key)
        if column not in _FILTER_COLUMNS:
            unsupported.append(raw_key)
            continue
        names: list[str] = []
        for j, value in enumerate(values):
            name = f"flt_{i}_{j}"
            params[name] = str(value)
            names.append(f"{{{name}:String}}")
        operator = "NOT IN" if str(filter_spec_attr(spec, "mode") or "include") == "exclude" else "IN"
        clauses.append(f"{column} {operator} ({', '.join(names)})")
    if unsupported:
        raise ValueError(f"high-scale RUM cannot filter on: {', '.join(sorted(unsupported))}")
    return clauses, params


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        stamp = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return stamp.isoformat().replace("+00:00", "Z")
    return safe_iso(value) or ""


def rum_beacon_health(service: HighScaleService) -> dict[str, Any]:
    try:
        res = service.client.execute(
            "SELECT sum(event_count) as c FROM rum_vitals_aggregates WHERE service_id = {service_id:String} AND dimension = 'metric_name' AND publication_state='visible'",
            {"service_id": service.service_id},
        )
        beacons = res[0].get("c", 0) if res and res[0].get("c") is not None else 0
    except Exception:
        res = service.client.execute(
            "SELECT count() as c FROM rum_vitals_facts WHERE service_id = {service_id:String} AND publication_state='visible'",
            {"service_id": service.service_id},
        )
        beacons = res[0].get("c", 0) if res else 0
    return {"has_data": beacons > 0, "beacons": beacons}


def rum_analytics(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    filters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Serve the standard RUM wire contract from visible, time-filtered facts.

    Counts use beacon identity, not metric-row count. All SQL goes through the
    instrumented client; dependency failures must not become empty chart data.
    """
    now = datetime.now(UTC)
    start = range_value(start_time) if start_time else now - timedelta(hours=24)
    end = range_value(end_time) if end_time else now
    start = start.replace(tzinfo=UTC) if start.tzinfo is None else start.astimezone(UTC)
    end = end.replace(tzinfo=UTC) if end.tzinfo is None else end.astimezone(UTC)
    if end < start:
        raise ValueError("RUM end time precedes start time")
    filter_clauses, filter_params = _filter_clauses(filters)
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}

    def scope(domain: str) -> str:
        return " AND ".join(
            [
                *visible_fact_clauses(domain),
                "event_timestamp >= {start:DateTime64(3, 'UTC')}",
                "event_timestamp <= {end:DateTime64(3, 'UTC')}",
                *filter_clauses,
            ]
        )

    where_vitals = scope("rum_vitals")
    where_errors = scope("rum_errors")
    # Same fallback as the standard raw producer's req_id / cid + epoch-second
    # identity. Do not count separate LCP/CLS rows as separate pageviews.
    identity = _BEACON_IDENTITY
    events = f"""
        WITH events AS (
            SELECT event_timestamp, {identity} AS beacon_id, 'vitals' AS src,
                   metric_name, metric_value, pathname
            FROM rum_vitals_facts WHERE {where_vitals}
            UNION ALL
            SELECT event_timestamp, {identity} AS beacon_id, 'errors' AS src,
                   '' AS metric_name, CAST(NULL AS Nullable(Float64)) AS metric_value, pathname
            FROM rum_error_facts WHERE {where_errors}
        )
    """
    counts = service.client.execute(
        events
        + """
        /* rum_analytics_counts */
        SELECT uniqExact(beacon_id) AS beacons,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name NOT LIKE 'event_%') AS pageviews,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name LIKE 'event_%') AS interactions,
               uniqExactIf(beacon_id, src = 'errors') AS errors
        FROM events
        """,
        params,
    )[0]
    no_data = False
    if not counts["beacons"]:
        # Distinguish an empty selected range from a never-configured service.
        any_vitals = " AND ".join(visible_fact_clauses("rum_vitals"))
        any_errors = " AND ".join(visible_fact_clauses("rum_errors"))
        any_data = service.client.execute(
            f"""
            /* rum_analytics_any_data */
            SELECT count() AS has_data FROM (
                (SELECT event_id FROM rum_vitals_facts WHERE {any_vitals} LIMIT 1)
                UNION ALL
                (SELECT event_id FROM rum_error_facts WHERE {any_errors} LIMIT 1)
            )
            """,
            {"service_id": service.service_id},
        )
        no_data = not any_data[0]["has_data"]

    rows = service.client.execute(
        f"""
        /* rum_analytics_vitals */
        SELECT lower(metric_name) AS metric_name,
               quantileExactInclusiveOrNull(0.75)(metric_value) as p75,
               count() as total,
               countIf(metric_rating = 'good') as good,
               countIf(metric_rating IN ('needs-improvement', 'needs_improvement')) as ni,
               countIf(metric_rating = 'poor') as poor
        FROM rum_vitals_facts
        WHERE {where_vitals}
        GROUP BY metric_name
        """,
        params,
    )
    environment_rows = service.client.execute(
        f"""
        /* rum_analytics_environments */
        SELECT if(browser = '', 'Unknown', browser) AS env_browser,
               if(os = '', 'Unknown', os) AS env_os,
               if(device = '', 'Unknown', device) AS env_device,
               uniqExact({identity}) AS beacons,
               grouping(browser) AS g_browser, grouping(os) AS g_os, grouping(device) AS g_device
        FROM rum_vitals_facts
        WHERE {where_vitals}
        GROUP BY GROUPING SETS ((browser), (os), (device))
        """,
        params,
    )
    browsers: dict[str, int] = {}
    operating_systems: dict[str, int] = {}
    devices: dict[str, int] = {}
    for row in environment_rows:
        if not row["g_browser"]:
            browsers[row["env_browser"]] = int(row["beacons"])
        elif not row["g_os"]:
            operating_systems[row["env_os"]] = int(row["beacons"])
        elif not row["g_device"]:
            devices[row["env_device"]] = int(row["beacons"])

    vitals: dict[str, Any] = {
        "lcp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "cls": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "inp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "fid": {"p75": None, "fcp": None, "ttfb": None},
    }

    def seconds(value: float | None) -> float | None:
        return round(value / 1000.0 if value > 20 else value, 2) if value is not None else None

    for r in rows:
        m = r["metric_name"].lower()
        p75 = r["p75"]
        if m in ("lcp", "cls", "inp"):
            vitals[m]["p75"] = (
                (seconds(p75) if m == "lcp" else (round(p75, 3) if m == "cls" else int(p75)))
                if p75 is not None
                else None
            )
            t = r["total"]
            if t:
                good = int(r["good"] * 100 / t)
                poor = int(r["poor"] * 100 / t)
                vitals[m]["distribution"] = {"good": good, "poor": poor, "needs_improvement": max(0, 100 - good - poor)}
        elif m == "fid":
            vitals["fid"]["p75"] = round(p75, 1) if p75 is not None else None
        elif m in ("fcp", "ttfb"):
            vitals["fid"][m] = seconds(p75)

    hourly = end - start <= timedelta(hours=48)
    # Fixed internal expression only; user values remain typed parameters.
    bucket_sql = "toStartOfHour(event_timestamp, 'UTC')" if hourly else "toStartOfDay(event_timestamp, 'UTC')"
    trend_rows = service.client.execute(
        events
        + f"""
        /* rum_analytics_trends */
        SELECT {bucket_sql} AS bucket,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'lcp') AS lcp,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'cls') AS cls,
               uniqExactIf(beacon_id, src = 'vitals') AS views,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name NOT LIKE 'event_%') AS pageviews,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name LIKE 'event_%') AS interactions,
               uniqExactIf(beacon_id, src = 'errors') AS errors
        FROM events GROUP BY bucket ORDER BY bucket
        """,
        params,
    )
    trend_by_bucket = {}
    for row in trend_rows:
        ts = row["bucket"]
        dt = ts if isinstance(ts, datetime) else range_value(ts)
        dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
        trend_by_bucket[dt] = row

    trends: dict[str, list[Any]] = {
        "timestamps": [],
        "lcp": [],
        "cls": [],
        "error_rate": [],
        "pageviews": [],
        "interactions": [],
        "errors": [],
    }
    curr = start.replace(minute=0, second=0, microsecond=0)
    target = end.replace(minute=0, second=0, microsecond=0)
    step = timedelta(hours=1) if hourly else timedelta(days=1)
    if not hourly:
        curr = curr.replace(hour=0)
        target = target.replace(hour=0)
    while not no_data and curr <= target:
        bucket_row = trend_by_bucket.get(curr)
        trends["timestamps"].append(curr.isoformat())
        trends["lcp"].append(seconds(bucket_row["lcp"]) if bucket_row else None)
        trends["cls"].append(round(bucket_row["cls"], 3) if bucket_row and bucket_row["cls"] is not None else None)
        denominator = bucket_row["views"] + bucket_row["errors"] if bucket_row else 0
        trends["error_rate"].append(
            round(bucket_row["errors"] * 100.0 / denominator, 2) if bucket_row and denominator else None
        )
        for field in ("pageviews", "interactions", "errors"):
            trends[field].append(int(bucket_row[field]) if bucket_row else 0)
        curr += step

    page_rows = service.client.execute(
        events
        + """
        /* rum_analytics_pages */
        SELECT pathname AS path,
               uniqExactIf(beacon_id, src = 'vitals') AS views,
               avgOrNullIf(metric_value, metric_name IN
                   ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')) AS avg_load,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'lcp') AS lcp,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'cls') AS cls,
               uniqExactIf(beacon_id, src = 'errors') * 100.0
                   / nullIf(views + uniqExactIf(beacon_id, src = 'errors'), 0) AS error_rate
        FROM events GROUP BY pathname HAVING views > 0
        ORDER BY error_rate DESC, avg_load DESC, path LIMIT 5
        """,
        params,
    )
    worst_pages = [
        {
            "path": row["path"],
            "views": row["views"],
            "avg_load_time": seconds(row["avg_load"] or 0.0),
            "lcp_p75": seconds(row["lcp"]),
            "cls_p75": round(row["cls"], 3) if row["cls"] is not None else None,
            "error_rate": round(row["error_rate"], 2) if row["error_rate"] is not None else 0.0,
        }
        for row in page_rows
    ]
    error_rows = service.client.execute(
        f"""
        /* rum_analytics_exceptions */
        SELECT error_message, error_file, error_line, error_col, count() AS count
        FROM rum_error_facts WHERE {where_errors}
        GROUP BY error_message, error_file, error_line, error_col
        ORDER BY count DESC, error_message, error_file, error_line, error_col LIMIT 3
        """,
        params,
    )
    result = {
        "is_mock": False,
        "no_data": no_data,
        "beacon_count": int(counts["beacons"]),
        "pageview_count": int(counts["pageviews"]),
        "interaction_count": int(counts["interactions"]),
        "error_count": int(counts["errors"]),
        "vitals": vitals,
        "worst_pages": worst_pages,
        "errors": [
            {
                "message": row["error_message"],
                "file": row["error_file"] or "unknown.js",
                "line": int(row.get("error_line") or 0),
                "col": int(row.get("error_col") or 0),
                "count": row["count"],
            }
            for row in error_rows
        ],
        "trends": trends,
        "environments": {"browsers": browsers, "os": operating_systems, "devices": devices},
    }
    if no_data:
        result["message"] = "Waiting for real-time RUM user events..."
        for metric in ("lcp", "cls", "inp"):
            vitals[metric]["distribution"] = None
    return result


def _ttfb_ms(value: Any) -> int | None:
    """Beacons report TTFB in seconds or milliseconds; the live feed always shows whole milliseconds."""
    if value is None:
        return None
    number = float(value)
    return int(number * 1000) if number < 15 else int(number)


def rum_live_events(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    limit: int,
    filters: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    end_dt = range_value(end_time) if end_time else now
    start_dt = range_value(start_time) if start_time else (end_dt - timedelta(days=1))
    filter_clauses, filter_params = _filter_clauses(filters)
    window = ["event_timestamp >= {start_time:DateTime64(3)}", "event_timestamp <= {end_time:DateTime64(3)}"]

    def scope(domain: str) -> str:
        return " AND ".join([*visible_fact_clauses(domain), *window, *filter_clauses])

    # ClickHouse binds a trailing ORDER BY / LIMIT to the last SELECT only, so the union is wrapped.
    query = f"""
    /* rum_live_events */
    SELECT * FROM (
    SELECT
        event_timestamp AS timestamp,
        'pageview' AS type,
        pathname,
        metric_name,
        metric_value,
        metric_rating,
        browser, os, device,
        client_id AS cid,
        toString(request_event_id) AS req_id,
        '' AS error_message,
        city, region, country, pop, tls, ttfb
    FROM rum_vitals_facts
    WHERE {scope("rum_vitals")}

    UNION ALL

    SELECT
        event_timestamp AS timestamp,
        'error' AS type,
        pathname,
        '' AS metric_name,
        CAST(NULL AS Nullable(Float64)) AS metric_value,
        '' AS metric_rating,
        browser, os, device,
        client_id AS cid,
        toString(request_event_id) AS req_id,
        error_message,
        city, region, country, pop, tls, ttfb
    FROM rum_error_facts
    WHERE {scope("rum_errors")}
    )
    ORDER BY timestamp DESC
    LIMIT {{limit:UInt32}}
    """
    res = service.client.execute(
        query,
        {
            "service_id": service.service_id,
            "start_time": start_dt,
            "end_time": end_dt,
            "limit": limit,
            **filter_params,
        },
    )

    events: list[dict[str, Any]] = []
    for d in res:
        etype = d.get("type", "pageview")
        mname = d.get("metric_name")
        mrating = d.get("metric_rating")
        mval = d.get("metric_value")
        emsg = d.get("error_message")
        path = d.get("pathname") or "/"
        browser = d.get("browser") or "Unknown"
        os_name = d.get("os") or "Unknown"
        meta = {
            "browser": {"name": browser},
            "os": {"name": os_name},
            "device": {"type": d.get("device") or "Unknown"},
            "page": {"url": path},
        }
        context = {
            "cid": d.get("cid"),
            "req_id": d.get("req_id"),
            "city": d.get("city") or "Unknown",
            "region": d.get("region") or "Unknown",
            "country": d.get("country") or "Unknown",
            "pop": d.get("pop") or "Unknown",
            "tls": d.get("tls") or "Unknown",
            "ttfb": _ttfb_ms(d.get("ttfb")),
        }
        if etype == "error":
            desc = emsg or "JS Error"
            raw_log: dict[str, Any] = {
                "meta": meta,
                "measurements": [],
                "exceptions": [{"type": "Error", "value": desc}],
                **context,
            }
        else:
            desc = "Page loaded successfully"
            if mname:
                desc = f"Metric {mname.upper()}: {float(mval)}" if mval is not None else f"Metric {mname.upper()}"
                if mrating:
                    desc += f" ({mrating.upper()})"
            raw_log = {
                "meta": meta,
                "measurements": [
                    {
                        "type": "web-vitals",
                        "values": {mname: mval} if mname else {},
                        "context": {"rating": mrating or ""},
                    }
                ]
                if mname
                else [],
                "exceptions": [],
                **context,
            }
        events.append(
            {
                "time": _iso(d.get("timestamp")),
                "type": etype,
                "path": path,
                "desc": desc,
                "browser": browser,
                "os": os_name,
                "raw_log": raw_log,
            }
        )
    return events
