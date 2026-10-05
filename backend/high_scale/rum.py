from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale.registry import HighScaleService


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


def rum_analytics(service: HighScaleService, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    """Serve the standard RUM wire contract from visible, time-filtered facts.

    Counts use beacon identity, not metric-row count. All SQL goes through the
    instrumented client; dependency failures must not become empty chart data.
    """
    now = datetime.now(UTC)
    start = datetime.fromisoformat(start_time.replace("Z", "+00:00")) if start_time else now - timedelta(hours=24)
    end = datetime.fromisoformat(end_time.replace("Z", "+00:00")) if end_time else now
    start = start.replace(tzinfo=UTC) if start.tzinfo is None else start.astimezone(UTC)
    end = end.replace(tzinfo=UTC) if end.tzinfo is None else end.astimezone(UTC)
    if end < start:
        raise ValueError("RUM end time precedes start time")
    params = {"service_id": service.service_id, "start": start, "end": end}
    where = """
        service_id = {service_id:String} AND publication_state = 'visible'
        AND event_timestamp >= {start:DateTime64(3, 'UTC')}
        AND event_timestamp <= {end:DateTime64(3, 'UTC')}
    """
    # Same fallback as the standard raw producer's req_id / cid + epoch-second
    # identity. Do not count separate LCP/CLS rows as separate pageviews.
    identity = """
        coalesce(nullIf(toString(request_event_id), ''),
                 concat(client_id, '_', toString(toInt64(round(toUnixTimestamp64Milli(event_timestamp) / 1000.0)))))
    """
    events = f"""
        WITH events AS (
            SELECT event_timestamp, {identity} AS beacon_id, 'vitals' AS src,
                   metric_name, metric_value, pathname
            FROM rum_vitals_facts WHERE {where}
            UNION ALL
            SELECT event_timestamp, {identity} AS beacon_id, 'errors' AS src,
                   '' AS metric_name, CAST(NULL AS Nullable(Float64)) AS metric_value, pathname
            FROM rum_error_facts WHERE {where}
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
        any_data = service.client.execute(
            """
            /* rum_analytics_any_data */
            SELECT count() AS has_data FROM (
                (SELECT event_id FROM rum_vitals_facts
                 WHERE service_id={service_id:String} AND publication_state='visible' LIMIT 1)
                UNION ALL
                (SELECT event_id FROM rum_error_facts
                 WHERE service_id={service_id:String} AND publication_state='visible' LIMIT 1)
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
        WHERE {where}
        GROUP BY metric_name
        """,
        params,
    )

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
        dt = ts if isinstance(ts, datetime) else datetime.fromisoformat(ts.replace("Z", "+00:00"))
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
        SELECT error_message, error_file, count() AS count
        FROM rum_error_facts WHERE {where}
        GROUP BY error_message, error_file ORDER BY count DESC, error_message, error_file LIMIT 3
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
                "line": 0,
                "col": 0,
                "count": row["count"],
            }
            for row in error_rows
        ],
        "trends": trends,
        # These dimensions are not present in the native fact schema.
        "environments": {"browsers": {}, "os": {}, "devices": {}},
    }
    if no_data:
        result["message"] = "Waiting for real-time RUM user events..."
        for metric in ("lcp", "cls", "inp"):
            vitals[metric]["distribution"] = None
    return result


def rum_live_events(
    service: HighScaleService, start_time: str | None, end_time: str | None, limit: int
) -> list[dict[str, Any]]:
    query = """
    SELECT
        event_timestamp as timestamp,
        'vitals' as type,
        pathname,
        metric_name,
        metric_value,
        metric_rating,
        client_id as cid,
        request_event_id as req_id,
        country,
        '' as error_message
    FROM rum_vitals_facts
    WHERE service_id = {service_id:String}
      AND publication_state = 'visible'
      AND event_timestamp >= {start_time:DateTime64(3)}
      AND event_timestamp <= {end_time:DateTime64(3)}

    UNION ALL

    SELECT
        event_timestamp as timestamp,
        'error' as type,
        pathname,
        '' as metric_name,
        0.0 as metric_value,
        '' as metric_rating,
        client_id as cid,
        request_event_id as req_id,
        country,
        error_message
    FROM rum_error_facts
    WHERE service_id = {service_id:String}
      AND publication_state = 'visible'
      AND event_timestamp >= {start_time:DateTime64(3)}
      AND event_timestamp <= {end_time:DateTime64(3)}

    ORDER BY timestamp DESC
    LIMIT {limit:UInt32}
    """
    try:
        res = service.client.execute(
            query,
            {
                "service_id": service.service_id,
                "start_time": datetime.fromisoformat(start_time.replace("Z", "+00:00")) if start_time else None,
                "end_time": datetime.fromisoformat(end_time.replace("Z", "+00:00")) if end_time else None,
                "limit": limit,
            },
        )
    except Exception as e:
        import logging

        logging.getLogger("backend").error(f"ERROR in rum_live_events: {e}")
        return []

    events = []
    for d in res:
        ts = d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat") else str(d["timestamp"])
        ts = ts.replace("+00:00", "Z") if "+" in ts else ts + "Z"

        etype = d.get("type", "vitals")
        mname = d.get("metric_name")
        mrating = d.get("metric_rating")
        mval = d.get("metric_value")
        emsg = d.get("error_message")

        if etype == "error":
            desc = emsg or "Unknown JavaScript Error"
            raw_log = {
                "meta": {
                    "browser": {"name": "Unknown"},
                    "os": {"name": "Unknown"},
                    "device": {"type": "Unknown"},
                    "page": {"url": d.get("pathname") or "/"},
                },
                "events": [{"type": "error", "value": emsg}],
                "cid": d.get("cid"),
                "req_id": d.get("req_id"),
                "city": "Unknown",
                "region": "Unknown",
                "country": d.get("country") or "Unknown",
                "pop": "Unknown",
                "tls": "Unknown",
                "ttfb": 0,
            }
        else:
            desc = "Page loaded successfully"
            if mname:
                desc = f"Metric {mname.upper()}: {mval}"
                if mrating:
                    desc += f" ({mrating.upper()})"
            raw_log = {
                "meta": {
                    "browser": {"name": "Unknown"},
                    "os": {"name": "Unknown"},
                    "device": {"type": "Unknown"},
                    "page": {"url": d.get("pathname") or "/"},
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
                "cid": d.get("cid"),
                "req_id": d.get("req_id"),
                "city": "Unknown",
                "region": "Unknown",
                "country": d.get("country") or "Unknown",
                "pop": "Unknown",
                "tls": "Unknown",
                "ttfb": 0,
            }

        events.append(
            {
                "time": ts,
                "type": etype,
                "path": d.get("pathname") or "/",
                "desc": desc,
                "browser": "Unknown",
                "os": "Unknown",
                "raw_log": raw_log,
            }
        )
    return events
