"""High-scale session stitching over ClickHouse ``request_facts``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale._filters import request_filter_clauses
from backend.high_scale._projection_utils import range_value
from backend.high_scale._scope import visible_fact_clauses
from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import SessionDetailRequest, SessionsRequest
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    client_cache_token,
    digest_cache_key,
)
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import safe_iso

_memo = ResponseMemo(ttl=30.0, maxsize=128)
_CacheRegistry.register("high_scale.sessions._response_cache", _memo.cache)

_DEFAULT_MIN_REQS_FLAG = 1000
_DEFAULT_MIN_4XX_PCT_FLAG = 20.0
_SORTABLE = frozenset(
    {
        "session_start",
        "session_end",
        "req_count",
        "edge_count",
        "shield_count",
        "unique_urls",
        "median_rtt_ms",
        "total_bytes",
    }
)
_HELPER_COLUMNS = (
    "sid",
    "total_sessions",
    "any_rtt",
    "any_ja4",
    "any_edge",
    "any_edge_sid",
    "any_cmcd",
    "streaming_reqs",
    "has_rtt_row",
    "has_edge_sid_row",
)
_DETAIL_LIMIT = 500
_FULL_CTE_SORTS = frozenset({"unique_urls", "median_rtt_ms"})

_SESSIONS_FAST_CTE = """
    WITH raw_events AS (
        SELECT
            client_ip AS ip,
            custom_fields['ja4'] AS ja4,
            event_timestamp AS ts,
            custom_fields['edge'] AS edge,
            toUInt8(custom_fields['edge_sid'] != '') AS has_edge_sid_ev,
            toInt32OrZero(custom_fields['status']) AS status,
            toInt64OrZero(custom_fields['resp_bytes']) AS resp_bytes,
            toUInt8(toFloat64OrNull(custom_fields['tcp_rtt']) IS NOT NULL) AS has_rtt_ev,
            cmcd['sid'] AS cmcd_sid
        FROM request_facts
        WHERE {where}
    ),
    base AS (
        SELECT
            ip,
            ja4,
            toStartOfMinute(ts) AS bucket_ts,
            min(ts) AS min_ts,
            max(ts) AS max_ts,
            count() AS req_count,
            sum(if(status >= 400 AND status < 500, 1, 0)) AS reqs_4xx,
            sum(if(status >= 500, 1, 0)) AS reqs_5xx,
            sum(resp_bytes) AS total_bytes,
            sum(if(lower(edge) IN ('1', 'true'), 1, 0)) AS edge_count,
            sum(if(lower(edge) IN ('0', 'false'), 1, 0)) AS shield_count,
            max(has_rtt_ev) AS has_rtt_row,
            max(has_edge_sid_ev) AS has_edge_sid_row,
            sum(if(cmcd_sid != '', 1, 0)) AS streaming_reqs
        FROM raw_events
        GROUP BY ip, ja4, bucket_ts
    ),
    gaps AS (
        SELECT
            *,
            dateDiff('second', lagInFrame(max_ts) OVER (PARTITION BY ip, ja4 ORDER BY min_ts), min_ts) AS gap
        FROM base
    ),
    sessions_raw AS (
        SELECT
            *,
            sum(if(gap IS NULL OR gap > 1800, 1, 0)) OVER (
                PARTITION BY ip, ja4 ORDER BY min_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS sid
        FROM gaps
    ),
    sessions_agg AS (
        SELECT
            ip,
            ja4,
            min(min_ts) AS session_start,
            max(max_ts) AS session_end,
            sum(req_count) AS req_count,
            sum(reqs_4xx) AS reqs_4xx,
            sum(reqs_5xx) AS reqs_5xx,
            sum(total_bytes) AS total_bytes,
            sum(edge_count) AS edge_count,
            sum(shield_count) AS shield_count,
            max(has_rtt_row) AS has_rtt_row,
            max(has_edge_sid_row) AS has_edge_sid_row,
            sum(streaming_reqs) AS streaming_reqs,
            sid
        FROM sessions_raw
        GROUP BY ip, ja4, sid
    ),
    flagged_sessions AS (
        SELECT
            *,
            ((req_count >= {{min_reqs_flag:UInt32}})
                OR (req_count > 0 AND (reqs_4xx * 100.0 / req_count) >= {{min_4xx_pct_flag:Float64}})) AS flagged,
            (streaming_reqs > 0) AS is_streaming
        FROM sessions_agg
    )
"""

_SESSIONS_CTE = """
    WITH raw_events AS (
        SELECT
            client_ip AS ip,
            custom_fields['ja4'] AS ja4,
            event_timestamp AS ts,
            country,
            toInt32OrNull(custom_fields['asn']) AS asn,
            url,
            custom_fields['edge'] AS edge,
            custom_fields['edge_sid'] AS edge_sid,
            custom_fields['ua'] AS ua,
            toInt32OrZero(custom_fields['status']) AS status,
            toInt64OrZero(custom_fields['resp_bytes']) AS resp_bytes,
            toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt,
            cmcd['sid'] AS cmcd_sid
        FROM request_facts
        WHERE {where}
    ),
    base AS (
        SELECT
            ip,
            ja4,
            toStartOfMinute(ts) AS bucket_ts,
            min(ts) AS min_ts,
            max(ts) AS max_ts,
            count() AS req_count,
            min(asn) AS asn,
            min(country) AS country,
            uniqState(url) AS url_state,
            min(ua) AS ua,
            sum(if(status >= 400 AND status < 500, 1, 0)) AS reqs_4xx,
            sum(if(status >= 500, 1, 0)) AS reqs_5xx,
            sum(resp_bytes) AS total_bytes,
            medianState(tcp_rtt) AS rtt_state,
            sum(if(lower(edge) IN ('1', 'true'), 1, 0)) AS edge_count,
            sum(if(lower(edge) IN ('0', 'false'), 1, 0)) AS shield_count,
            max(edge_sid) AS edge_sid,
            sum(if(cmcd_sid != '', 1, 0)) AS streaming_reqs
        FROM raw_events
        GROUP BY ip, ja4, bucket_ts
    ),
    gaps AS (
        SELECT
            *,
            dateDiff('second', lagInFrame(max_ts) OVER (PARTITION BY ip, ja4 ORDER BY min_ts), min_ts) AS gap
        FROM base
    ),
    sessions_raw AS (
        SELECT
            *,
            sum(if(gap IS NULL OR gap > 1800, 1, 0)) OVER (
                PARTITION BY ip, ja4 ORDER BY min_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS sid
        FROM gaps
    ),
    sessions_agg AS (
        SELECT
            ip,
            ja4,
            min(min_ts) AS session_start,
            max(max_ts) AS session_end,
            sum(req_count) AS req_count,
            min(asn) AS asn,
            min(country) AS country,
            uniqMerge(url_state) AS unique_urls,
            min(ua) AS ua,
            sum(reqs_4xx) AS reqs_4xx,
            sum(reqs_5xx) AS reqs_5xx,
            sum(total_bytes) AS total_bytes,
            medianMerge(rtt_state) / 1000.0 AS median_rtt_ms,
            sum(edge_count) AS edge_count,
            sum(shield_count) AS shield_count,
            max(edge_sid) AS edge_sid,
            toUInt8(medianMerge(rtt_state) IS NOT NULL) AS has_rtt_row,
            toUInt8(max(edge_sid) != '') AS has_edge_sid_row,
            sum(streaming_reqs) AS streaming_reqs,
            sid
        FROM sessions_raw
        GROUP BY ip, ja4, sid
    ),
    flagged_sessions AS (
        SELECT
            *,
            ((req_count >= {{min_reqs_flag:UInt32}})
                OR (req_count > 0 AND (reqs_4xx * 100.0 / req_count) >= {{min_4xx_pct_flag:Float64}})) AS flagged,
            (streaming_reqs > 0) AS is_streaming
        FROM sessions_agg
    )
"""
_FILTERED = """
    FROM flagged_sessions
    WHERE (({streaming_only:UInt8} = 0) OR is_streaming = 1)
      AND (({flagged_only:UInt8} = 0) OR flagged = 1)
"""


def _parse_ts(val: str | None) -> datetime | None:
    return range_value(val) if val else None


def _window(start_time: str | None, end_time: str | None, default: timedelta) -> tuple[datetime, datetime]:
    end_dt = _parse_ts(end_time) or datetime.now(UTC)
    return _parse_ts(start_time) or (end_dt - default), end_dt


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        stamp = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return stamp.isoformat().replace("+00:00", "Z")
    return safe_iso(value)


def _asn_label_map(service_id: str, asns: list[int]) -> dict[int, str]:
    from backend.core import duckdb as _db

    names = _db.get_asn_names(service_id, asns)
    return {asn: _db.format_asn_label(asn, names.get(asn, "")) for asn in asns}


def _as_dt(val: Any) -> datetime | None:
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        return _parse_ts(val)
    return None


def _enrich_paginated_sessions(
    service: HighScaleService,
    rows: list[dict[str, Any]],
    where: str,
    params: dict[str, Any],
) -> None:
    tuples_sql_parts: list[str] = []
    enrich_params: dict[str, Any] = dict(params)
    min_start: datetime | None = None
    max_end: datetime | None = None
    unique_ips: list[str] = []
    seen_ips: set[str] = set()

    for idx, row in enumerate(rows):
        row.setdefault("asn", None)
        row.setdefault("country", "")
        row.setdefault("unique_urls", 0)
        row.setdefault("ua", "")
        row.setdefault("median_rtt_ms", None)
        row.setdefault("edge_sid", "")

        s_dt = _as_dt(row.get("session_start"))
        e_dt = _as_dt(row.get("session_end"))
        if s_dt is None or e_dt is None:
            continue
        if min_start is None or s_dt < min_start:
            min_start = s_dt
        if max_end is None or e_dt > max_end:
            max_end = e_dt
        ip_val = str(row.get("ip") or "")
        ja4_val = str(row.get("ja4") or "")
        if ip_val not in seen_ips:
            seen_ips.add(ip_val)
            unique_ips.append(ip_val)
        enrich_params[f"en_ip_{idx}"] = ip_val
        enrich_params[f"en_ja4_{idx}"] = ja4_val
        enrich_params[f"en_s_{idx}"] = s_dt
        enrich_params[f"en_e_{idx}"] = e_dt
        tuples_sql_parts.append(
            f"tuple(toUInt32({idx}), {{en_ip_{idx}:String}}, {{en_ja4_{idx}:String}}, "
            f"{{en_s_{idx}:DateTime64(3)}}, {{en_e_{idx}:DateTime64(3)}})"
        )

    if not tuples_sql_parts or not unique_ips:
        return

    if min_start is not None:
        enrich_params["start_time"] = min_start
    if max_end is not None:
        enrich_params["end_time"] = max_end

    ip_placeholders: list[str] = []
    for i, ip_val in enumerate(unique_ips):
        enrich_params[f"en_uip_{i}"] = ip_val
        ip_placeholders.append(f"{{en_uip_{i}:String}}")

    enrich_sql = (
        "/* sessions:enrich */ "
        "WITH page_keys AS ("
        "    SELECT t.1 AS pk_idx, t.2 AS pk_ip, t.3 AS pk_ja4, t.4 AS pk_start, t.5 AS pk_end "
        f"    FROM (SELECT arrayJoin([{', '.join(tuples_sql_parts)}]) AS t)"
        ") "
        "SELECT "
        "    pk.pk_idx AS row_idx, "
        "    min(toInt32OrNull(custom_fields['asn'])) AS asn, "
        "    min(country) AS country, "
        "    uniq(url) AS unique_urls, "
        "    min(custom_fields['ua']) AS ua, "
        "    median(toFloat64OrNull(custom_fields['tcp_rtt'])) / 1000.0 AS median_rtt_ms, "
        "    max(custom_fields['edge_sid']) AS edge_sid "
        "FROM request_facts "
        "INNER JOIN page_keys AS pk "
        "    ON client_ip = pk.pk_ip AND custom_fields['ja4'] = pk.pk_ja4 "
        f"WHERE {where} "
        f"  AND client_ip IN ({', '.join(ip_placeholders)}) "
        "  AND event_timestamp >= pk.pk_start "
        "  AND event_timestamp <= pk.pk_end "
        "GROUP BY pk.pk_idx"
    )
    enriched_rows = service.client.execute(enrich_sql, enrich_params)
    for en_row in enriched_rows:
        idx = int(en_row["row_idx"])
        if 0 <= idx < len(rows):
            rows[idx]["asn"] = en_row.get("asn")
            rows[idx]["country"] = en_row.get("country") or ""
            rows[idx]["unique_urls"] = int(en_row.get("unique_urls") or 0)
            rows[idx]["ua"] = en_row.get("ua") or ""
            rows[idx]["median_rtt_ms"] = en_row.get("median_rtt_ms")
            rows[idx]["edge_sid"] = en_row.get("edge_sid") or ""


def sessions_endpoint(
    service: HighScaleService, req: SessionsRequest, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    start_dt, end_dt = _window(start_time, end_time, timedelta(days=7))
    min_reqs_flag = req.min_reqs_flag if req.min_reqs_flag is not None else _DEFAULT_MIN_REQS_FLAG
    min_4xx_pct_flag = req.min_4xx_pct_flag if req.min_4xx_pct_flag is not None else _DEFAULT_MIN_4XX_PCT_FLAG
    filter_clauses, filter_params = request_filter_clauses(req.filters)
    where = " AND ".join(
        [
            *visible_fact_clauses("request"),
            "event_timestamp >= {start_time:DateTime64(3)}",
            "event_timestamp <= {end_time:DateTime64(3)}",
            *filter_clauses,
        ]
    )
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "start_time": start_dt,
        "end_time": end_dt,
        "min_reqs_flag": min_reqs_flag,
        "min_4xx_pct_flag": min_4xx_pct_flag,
        "flagged_only": 1 if req.flagged_only else 0,
        "streaming_only": 1 if req.streaming_only else 0,
        **filter_params,
    }
    sort_by = req.sort_by if req.sort_by in _SORTABLE else "session_start"
    sort_dir = "ASC" if str(req.sort_dir).lower() == "asc" else "DESC"
    fast_cte = _SESSIONS_FAST_CTE.format(where=where)
    cte = _SESSIONS_CTE.format(where=where) if sort_by in _FULL_CTE_SORTS else fast_cte
    memo_key = digest_cache_key(
        {
            "cid": client_cache_token(service.client),
            "s": bucket_time_to_minute(start_time) if start_time else start_dt.strftime("%Y-%m-%dT%H:%M"),
            "e": bucket_time_to_minute(end_time) if end_time else end_dt.strftime("%Y-%m-%dT%H:%M"),
            "p": req.page,
            "l": req.limit,
            "sb": sort_by,
            "sd": sort_dir,
            "mr": min_reqs_flag,
            "m4": min_4xx_pct_flag,
            "fo": bool(req.flagged_only),
            "so": bool(req.streaming_only),
            "fc": list(filter_clauses),
            "fp": sorted((k, str(v)) for k, v in filter_params.items()),
        },
        {"service_id": service.service_id},
    )
    cached = _memo.get(memo_key)
    if cached is not None:
        return cached

    rows = [
        dict(r)
        for r in service.client.execute(
            "/* sessions:page */ "
            + cte
            + """
    SELECT *,
           count() OVER () AS total_sessions,
           max(has_rtt_row) OVER () AS any_rtt,
           max(ja4 != '') OVER () AS any_ja4,
           max(edge_count + shield_count > 0) OVER () AS any_edge,
           max(has_edge_sid_row) OVER () AS any_edge_sid,
           max(streaming_reqs > 0) OVER () AS any_cmcd
    FROM flagged_sessions
    WHERE (({streaming_only:UInt8} = 0) OR is_streaming = 1)
      AND (({flagged_only:UInt8} = 0) OR flagged = 1)
    """
            + f"ORDER BY {sort_by} {sort_dir} LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}",
            {**params, "limit": req.limit, "offset": (req.page - 1) * req.limit},
        )
    ]

    if rows and "unique_urls" not in rows[0]:
        _enrich_paginated_sessions(service, rows, where, params)

    if rows:
        total = int(rows[0]["total_sessions"])
    elif req.page > 1:
        counted = service.client.execute(
            "/* sessions:count */ " + fast_cte + "SELECT count() AS total_sessions" + _FILTERED, params
        )
        total = int(counted[0]["total_sessions"]) if counted else 0
    else:
        total = 0

    first = rows[0] if rows else {}
    sessions: list[dict[str, Any]] = []
    for row in rows:
        session = {key: value for key, value in row.items() if key not in _HELPER_COLUMNS}
        session["session_start"] = _iso(session.get("session_start"))
        session["session_end"] = _iso(session.get("session_end"))
        session["flagged"] = bool(session.get("flagged"))
        session["is_streaming"] = bool(session.get("is_streaming"))
        sessions.append(session)

    asns = sorted({int(session["asn"]) for session in sessions if session.get("asn") is not None})
    result = {
        "sessions": sessions,
        "asn_names": _asn_label_map(service.service_id, asns) if asns else {},
        "total": total,
        "page": req.page,
        "limit": req.limit,
        "has_rtt": bool(first.get("any_rtt")),
        "has_ja4": bool(first.get("any_ja4")),
        "has_edge": bool(first.get("any_edge")),
        "has_edge_sid": bool(first.get("any_edge_sid")),
        "has_cmcd": bool(first.get("any_cmcd")),
        "min_reqs_flag": min_reqs_flag,
        "min_4xx_pct_flag": min_4xx_pct_flag,
    }
    _memo.put(memo_key, result)
    return result


def sessions_detail(
    service: HighScaleService,
    req: SessionDetailRequest,
    start_time: str | None,
    end_time: str | None,
    ip: str,
    ja4: str | None,
) -> dict[str, Any]:
    start_dt, end_dt = _window(start_time, end_time, timedelta(days=7))
    clauses = [
        *visible_fact_clauses("request"),
        "client_ip = {ip:String}",
        "event_timestamp >= {start_time:DateTime64(3)}",
        "event_timestamp <= {end_time:DateTime64(3)}",
    ]
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "start_time": start_dt,
        "end_time": end_dt,
        "ip": ip,
    }
    if ja4:
        clauses.append("custom_fields['ja4'] = {ja4:String}")
        params["ja4"] = ja4
    data = service.client.execute(
        """
    /* sessions:detail */
    SELECT
        client_ip AS ip,
        custom_fields['ja4'] AS ja4,
        event_timestamp AS timestamp,
        country,
        url,
        toInt32OrZero(custom_fields['status']) AS status,
        custom_fields['method'] AS method,
        custom_fields['protocol'] AS protocol,
        toInt64OrZero(custom_fields['resp_bytes']) AS resp_bytes,
        toFloat64OrZero(custom_fields['tcp_rtt']) AS tcp_rtt,
        custom_fields['edge'] AS edge,
        custom_fields['edge_sid'] AS edge_sid,
        custom_fields['ua'] AS ua,
        toInt32OrNull(custom_fields['asn']) AS asn,
        nullIf(cmcd['ot'], '') AS cmcd_ot,
        toInt64OrNull(cmcd['br']) AS cmcd_br,
        toInt64OrNull(cmcd['bl']) AS cmcd_bl,
        nullIf(cmcd['sf'], '') AS cmcd_sf,
        nullIf(cmcd['st'], '') AS cmcd_st,
        toInt64OrNull(cmcd['d']) AS cmcd_d,
        toInt64OrNull(cmcd['mtp']) AS cmcd_mtp,
        toInt64OrNull(cmcd['tb']) AS cmcd_tb,
        if(cmcd['bs'] = '', NULL, lower(cmcd['bs']) IN ('true', '1')) AS cmcd_bs,
        if(cmcd['su'] = '', NULL, lower(cmcd['su']) IN ('true', '1')) AS cmcd_su,
        nullIf(cmcd['cid'], '') AS cmcd_cid,
        nullIf(cmcd['sid'], '') AS cmcd_sid,
        toInt32OrNull(cmcd['v']) AS cmcd_v
    FROM request_facts
    WHERE """
        + " AND ".join(clauses)
        + f" ORDER BY timestamp ASC LIMIT {_DETAIL_LIMIT}",
        params,
    )
    for row in data:
        if row.get("timestamp"):
            row["timestamp"] = _iso(row["timestamp"])
    return {"data": data, "columns": list(data[0].keys()) if data else []}
