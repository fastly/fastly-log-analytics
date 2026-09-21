from __future__ import annotations

from datetime import datetime
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
    # Check if there is data
    health = rum_beacon_health(service)
    if not health["has_data"]:
        return {"no_data": True}

    start = datetime.fromisoformat(start_time.replace("Z", "+00:00")) if start_time else None
    end = datetime.fromisoformat(end_time.replace("Z", "+00:00")) if end_time else None

    query_base = """
        SELECT metric_name,
               quantile(0.75)(metric_value) as p75,
               count() as total,
               sum(if(metric_rating = 'good', 1, 0)) as good,
               sum(if(metric_rating = 'needs-improvement' OR metric_rating = 'needs_improvement', 1, 0)) as ni,
               sum(if(metric_rating = 'poor', 1, 0)) as poor
        FROM rum_vitals_facts
        WHERE service_id={service_id:String} AND publication_state='visible'
    """
    if start and end:
        query_base += " AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp <= {end:DateTime64(3)}"
    query = query_base + " GROUP BY metric_name"
    try:
        rows = service.client.execute(query, {"service_id": service.service_id, "start": start, "end": end})
    except Exception:
        rows = []

    vitals: dict[str, Any] = {
        "lcp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "cls": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "inp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "fid": {"p75": None, "fcp": None, "ttfb": None},
    }

    total_pageviews = 0
    for r in rows:
        m = r["metric_name"].lower()
        if m in vitals:
            total_pageviews += r["total"]
            vitals[m]["p75"] = r["p75"]
            if m in ("lcp", "cls", "inp"):
                t = r["total"]
                if t > 0:
                    vitals[m]["distribution"]["good"] = int(r["good"] * 100 / t)
                    vitals[m]["distribution"]["poor"] = int(r["poor"] * 100 / t)
                    vitals[m]["distribution"]["needs_improvement"] = max(
                        0, 100 - vitals[m]["distribution"]["good"] - vitals[m]["distribution"]["poor"]
                    )

    error_count = 0
    try:
        err_query = "SELECT sum(error_count) as c FROM rum_error_aggregates WHERE service_id={service_id:String} AND dimension='error_message' AND publication_state='visible'"
        if start and end:
            err_query += " AND bucket_start >= {start:DateTime} AND bucket_start <= {end:DateTime}"
        err_res = service.client.execute(err_query, {"service_id": service.service_id, "start": start, "end": end})
        error_count = err_res[0].get("c", 0) if err_res and err_res[0].get("c") is not None else 0
    except Exception:
        try:
            err_query = "SELECT count() as c FROM rum_error_facts WHERE service_id={service_id:String} AND publication_state='visible'"
            if start and end:
                err_query += " AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp <= {end:DateTime64(3)}"
            err_res = service.client.execute(err_query, {"service_id": service.service_id, "start": start, "end": end})
            error_count = err_res[0].get("c", 0) if err_res else 0
        except Exception:
            pass

    vitals_and_interactions_total = sum(r["total"] for r in rows)
    beacon_count = vitals_and_interactions_total + error_count

    return {
        "is_mock": False,
        "no_data": False,
        "beacon_count": beacon_count,
        "pageview_count": total_pageviews,
        "interaction_count": 0,
        "error_count": error_count,
        "vitals": vitals,
        "worst_pages": [],
        "errors": [],
        "trends": {
            "timestamps": [],
            "lcp": [],
            "cls": [],
            "error_rate": [],
            "pageviews": [],
            "interactions": [],
            "errors": [],
        },
        "environments": {"browsers": {}, "os": {}, "devices": {}},
    }


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
