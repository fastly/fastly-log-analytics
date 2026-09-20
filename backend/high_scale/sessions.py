from __future__ import annotations

from datetime import datetime
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
            toInt32OrNull(custom_fields['asn']) as asn,
            url,
            custom_fields['edge'] as edge,
            custom_fields['edge_sid'] as edge_sid,
            custom_fields['ua'] as ua,
            toInt32OrZero(custom_fields['status']) as status,
            toInt64OrZero(custom_fields['resp_bytes']) as resp_bytes,
            toFloat64OrZero(custom_fields['tcp_rtt']) as tcp_rtt,
            custom_fields['cmcd_sid'] as cmcd_sid
        FROM fastly_log_analytics.request_facts
        WHERE service_id = {service_id:String}
          AND publication_state = 'visible'
          AND event_timestamp >= {start_time:DateTime64(3)}
          AND event_timestamp <= {end_time:DateTime64(3)}
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
            max(edge_sid) as edge_sid,
            sum(if(cmcd_sid != '', 1, 0)) as streaming_reqs
        FROM sessions_raw
        GROUP BY ip, ja4, sid
    )
    SELECT * FROM (
        SELECT *,
               ((req_count >= {min_reqs_flag:UInt32}) OR (req_count > 0 AND (reqs_4xx * 100.0 / req_count) >= {min_4xx_pct_flag:Float64})) as flagged,
               (streaming_reqs > 0) as is_streaming
        FROM sessions_agg
    ) sub
    WHERE (({flagged_only:UInt8} = 0) OR flagged = 1)
      AND (({streaming_only:UInt8} = 0) OR is_streaming = 1)
    ORDER BY session_start DESC
    LIMIT {limit:UInt32} OFFSET {offset:UInt32}
    """

    res = service.client.execute(
        query,
        {
            "service_id": service.service_id,
            "start_time": datetime.fromisoformat(start_time) if start_time else None,
            "end_time": datetime.fromisoformat(end_time) if end_time else None,
            "limit": req.limit,
            "offset": (req.page - 1) * req.limit,
            "min_reqs_flag": req.min_reqs_flag if req.min_reqs_flag is not None else 1000,
            "min_4xx_pct_flag": req.min_4xx_pct_flag if req.min_4xx_pct_flag is not None else 20.0,
            "flagged_only": 1 if req.flagged_only else 0,
            "streaming_only": 1 if req.streaming_only else 0,
        },
    )

    sessions = res
    for s in sessions:
        if s.get("asn") == "":
            s["asn"] = None
        elif s.get("asn"):
            try:
                s["asn"] = int(s["asn"])
            except ValueError:
                s["asn"] = None

        if s.get("session_start"):
            val = (
                s["session_start"].isoformat()
                if hasattr(s["session_start"], "isoformat")
                else s["session_start"].replace(" ", "T")
            )
            s["session_start"] = val.replace("+00:00", "Z") if "+" in val else val + "Z"
        if s.get("session_end"):
            val = (
                s["session_end"].isoformat()
                if hasattr(s["session_end"], "isoformat")
                else s["session_end"].replace(" ", "T")
            )
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
        toInt32OrZero(custom_fields['status']) as status,
        custom_fields['method'] as method,
        custom_fields['protocol'] as protocol,
        toInt64OrZero(custom_fields['resp_bytes']) as resp_bytes,
        toFloat64OrZero(custom_fields['tcp_rtt']) as tcp_rtt,
        custom_fields['edge'] as edge,
        custom_fields['edge_sid'] as edge_sid,
        custom_fields['ua'] as ua,
        toInt32OrNull(custom_fields['asn']) as asn
    FROM fastly_log_analytics.request_facts
    WHERE service_id = {service_id:String}
      AND publication_state = 'visible'
      AND client_ip = {ip:String}
      AND event_timestamp >= {start_time:DateTime64(3)}
      AND event_timestamp <= {end_time:DateTime64(3)}
    """

    params = {
        "service_id": service.service_id,
        "start_time": datetime.fromisoformat(start_time) if start_time else None,
        "end_time": datetime.fromisoformat(end_time) if end_time else None,
        "ip": ip,
    }

    if ja4:
        query += " AND custom_fields['ja4'] = {ja4:String}"
        params["ja4"] = ja4

    query += " ORDER BY timestamp ASC LIMIT 500"

    res = service.client.execute(query, params)
    data = res
    cols = list(data[0].keys()) if data else []
    for d in data:
        if d.get("timestamp"):
            val = (
                d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat") else d["timestamp"].replace(" ", "T")
            )
            d["timestamp"] = val.replace("+00:00", "Z") if "+" in val else val + "Z"

    return {"data": data, "columns": cols}
