from __future__ import annotations

from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import SessionDetailRequest, SessionsRequest


def sessions_endpoint(
    service: HighScaleService, req: SessionsRequest, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    query = """
    WITH base AS (
        SELECT
            client_ip as ip,
            custom_fields['ja4'] as ja4,
            event_timestamp as ts,
            country,
            custom_fields['asn'] as asn,
            url,
            custom_fields['edge'] as edge,
            custom_fields['edge_sid'] as edge_sid,
            custom_fields['ua'] as ua,
            CAST(custom_fields['status'] AS Int32) as status,
            CAST(custom_fields['resp_bytes'] AS Int64) as resp_bytes,
            CAST(custom_fields['tcp_rtt'] AS Float64) as tcp_rtt
        FROM fastly_log_analytics.request_facts
        WHERE service_id = {service_id:String}
          AND publication_state = 'visible'
          AND event_timestamp >= {start_time:DateTime}
          AND event_timestamp <= {end_time:DateTime}
    ),
    gaps AS (
        SELECT
            *,
            dateDiff('second', lagInFrame(ts) OVER (PARTITION BY ip, ja4 ORDER BY ts), ts) as gap
        FROM base
    ),
    marks AS (
        SELECT
            *,
            if(gap IS NULL OR gap > 1800, 1, 0) as is_new
        FROM gaps
    ),
    sessions_raw AS (
        SELECT
            *,
            sum(is_new) OVER (PARTITION BY ip, ja4 ORDER BY ts
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sid
        FROM marks
    ),
    sessions_agg AS (
        SELECT
            ip,
            ja4,
            min(ts) as session_start,
            max(ts) as session_end,
            count() as req_count,
            min(asn) as asn,
            min(country) as country,
            count(distinct url) as unique_urls,
            min(ua) as ua,
            sum(if(status >= 400 AND status < 500, 1, 0)) as reqs_4xx,
            sum(if(status >= 500, 1, 0)) as reqs_5xx,
            sum(resp_bytes) as total_bytes,
            median(tcp_rtt) / 1000.0 as median_rtt_ms,
            sum(if(edge = '1', 1, 0)) as edge_count,
            sum(if(edge = '0', 1, 0)) as shield_count,
            max(edge_sid) as edge_sid
        FROM sessions_raw
        GROUP BY ip, ja4, sid
    )
    SELECT *
    FROM sessions_agg
    ORDER BY session_start DESC
    LIMIT {limit:UInt32} OFFSET {offset:UInt32}
    """

    res = service.client.execute(
        query,
        {
            "service_id": service.service_id,
            "start_time": start_time,
            "end_time": end_time,
            "limit": req.limit,
            "offset": (req.page - 1) * req.limit,
        },
    )

    sessions = res
    for s in sessions:
        if s.get("session_start"):
            val = s["session_start"].isoformat()
            s["session_start"] = val.replace("+00:00", "Z") if "+" in val else val + "Z"
        if s.get("session_end"):
            val = s["session_end"].isoformat()
            s["session_end"] = val.replace("+00:00", "Z") if "+" in val else val + "Z"

    # For now, fast path total
    total = len(sessions)
    if total == req.limit:
        total = 1000  # Hack to allow pagination

    return {
        "sessions": sessions,
        "total": total,
        "page": req.page,
        "limit": req.limit,
        "has_rtt": True,
        "has_ja4": True,
        "has_edge": True,
        "has_edge_sid": True,
        "has_cmcd": False,
        "min_reqs_flag": req.min_reqs_flag,
        "min_4xx_pct_flag": req.min_4xx_pct_flag,
    }


def sessions_detail(
    service: HighScaleService,
    req: SessionDetailRequest,
    start_time: str | None,
    end_time: str | None,
    ip: str,
    ja4: str | None,
) -> dict[str, Any]:
    query = """
    SELECT
        client_ip as ip,
        custom_fields['ja4'] as ja4,
        event_timestamp as timestamp,
        country,
        url,
        custom_fields['status'] as status,
        custom_fields['method'] as method,
        custom_fields['protocol'] as protocol,
        custom_fields['resp_bytes'] as resp_bytes,
        custom_fields['tcp_rtt'] as tcp_rtt,
        custom_fields['edge'] as edge,
        custom_fields['edge_sid'] as edge_sid,
        custom_fields['ua'] as ua,
        custom_fields['asn'] as asn
    FROM fastly_log_analytics.request_facts
    WHERE service_id = {service_id:String}
      AND publication_state = 'visible'
      AND client_ip = {ip:String}
      AND event_timestamp >= {start_time:DateTime}
      AND event_timestamp <= {end_time:DateTime}
    """

    params = {"service_id": service.service_id, "start_time": start_time, "end_time": end_time, "ip": ip}

    if ja4:
        query += " AND custom_fields['ja4'] = {ja4:String}"
        params["ja4"] = ja4

    query += " ORDER BY timestamp ASC LIMIT 500"

    res = service.client.execute(query, params)
    data = res
    cols = list(data[0].keys()) if data else []
    for d in data:
        if d.get("timestamp"):
            val = d["timestamp"].isoformat()
            d["timestamp"] = val.replace("+00:00", "Z") if "+" in val else val + "Z"

    return {"data": data, "columns": cols}
