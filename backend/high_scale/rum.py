from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.high_scale.registry import HighScaleService


def rum_beacon_health(service: HighScaleService) -> dict[str, Any]:
    res = service.client.execute(
        "SELECT sum(event_count) FROM fastly_log_analytics.rum_vitals_aggregates WHERE service_id = {service_id:String}",
        {"service_id": service.service_id},
    )
    beacons = res[0].get("sum(event_count)", 0) if res else 0 or 0
    return {"has_data": beacons > 0, "beacons": beacons}


def rum_analytics(service: HighScaleService, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    # Check if there is data
    health = rum_beacon_health(service)
    if not health["has_data"]:
        return {"no_data": True}

    return {
        "has_data": True,
        "pageviews": 0,
        "interactions": 0,
        "errors": 0,
        "metrics": [],
        "environments": {"browser": [], "os": [], "device": []},
        "worst_pages": [],
        "worst_sessions": [],
    }


def rum_live_events(
    service: HighScaleService, start_time: str | None, end_time: str | None, limit: int
) -> list[dict[str, Any]]:
    query = """
    SELECT
        event_timestamp as timestamp,
        'vitals' as type,
        url as pathname,
        custom_fields['metric_name'] as metric_name,
        toFloat64OrZero(custom_fields['metric_value']) as metric_value,
        custom_fields['metric_rating'] as metric_rating,
        custom_fields['browser'] as browser,
        custom_fields['os'] as os,
        custom_fields['device'] as device,
        custom_fields['cid'] as cid,
        custom_fields['req_id'] as req_id,
        CAST(NULL as String) as error_message,
        custom_fields['city'] as city,
        custom_fields['region'] as region,
        country,
        custom_fields['pop'] as pop,
        custom_fields['tls'] as tls,
        toFloat64OrZero(custom_fields['ttfb']) as ttfb
    FROM fastly_log_analytics.rum_vitals_facts
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
                "start_time": datetime.fromisoformat(start_time) if start_time else None,
                "end_time": datetime.fromisoformat(end_time) if end_time else None,
                "limit": limit,
            },
        )
    except Exception:
        return []

    events = []
    for d in res:
        ts = d["timestamp"].isoformat()
        ts = ts.replace("+00:00", "Z") if "+" in ts else ts + "Z"

        mname = d.get("metric_name")
        mrating = d.get("metric_rating")
        mval = d.get("metric_value")

        desc = "Page loaded successfully"
        if mname:
            desc = f"Metric {mname.upper()}: {mval}"
            if mrating:
                desc += f" ({mrating.upper()})"

        events.append(
            {
                "time": ts,
                "type": "vitals",
                "path": d.get("pathname") or "/",
                "desc": desc,
                "browser": d.get("browser") or "Unknown",
                "os": d.get("os") or "Unknown",
                "raw_log": {
                    "meta": {
                        "browser": {"name": d.get("browser") or "Unknown"},
                        "os": {"name": d.get("os") or "Unknown"},
                        "device": {"type": d.get("device") or "Unknown"},
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
                    "city": d.get("city") or "Unknown",
                    "region": d.get("region") or "Unknown",
                    "country": d.get("country") or "Unknown",
                    "pop": d.get("pop") or "Unknown",
                    "tls": d.get("tls") or "Unknown",
                    "ttfb": d.get("ttfb") or 0,
                },
            }
        )
    return events
