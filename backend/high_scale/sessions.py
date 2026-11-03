"""High-scale session analytics and detail queries."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from backend.high_scale._common import (
    format_iso_utc_or_empty as _iso,
)
from backend.high_scale._common import (
    parse_time_window,
    range_value,
    request_filter_clauses,
    visible_fact_clauses,
)
from backend.models.analytics import SessionDetailRequest, SessionsRequest
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    client_cache_token,
    digest_cache_key,
)
from backend.repositories.utils.response_shaping import (
    SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG,
    SESSIONS_DEFAULT_MIN_REQS_FLAG,
    SESSIONS_SORTABLE_COLUMNS,
    build_asn_label_map,
    normalize_sessions_sort,
    shape_sessions_envelope,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

_sessions_memo = ResponseMemo(ttl=30.0, maxsize=128)
_CacheRegistry.register("high_scale.sessions._response_cache", _sessions_memo.cache)
_DEFAULT_MIN_REQS_FLAG = SESSIONS_DEFAULT_MIN_REQS_FLAG
_DEFAULT_MIN_4XX_PCT_FLAG = SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG
_SORTABLE = SESSIONS_SORTABLE_COLUMNS
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
_INDEX_FAST_SORTS = frozenset({"req_count", "session_start", "session_end", "ip", "ja4"})
_SESSIONS_INDEX_FAST_CTE = """
    WITH raw_events AS (
        SELECT
            client_ip AS ip,
            custom_fields['ja4'] AS ja4,
            toUInt64(toUnixTimestamp64Milli(event_timestamp)) AS ts_ms,
            custom_fields['edge'] AS edge,
            toUInt8(toFloat64OrNull(custom_fields['tcp_rtt']) IS NOT NULL) AS has_rtt,
            toUInt8(lower(edge) IN ('1', 'true') OR lower(edge) IN ('0', 'false')) AS has_edge,
            toUInt8(custom_fields['edge_sid'] != '') AS has_esid,
            toUInt8(cmcd['sid'] != '') AS has_cmcd
        FROM request_facts
        WHERE {where}
    ),
    pair_sessions AS (
        SELECT
            ip,
            ja4,
            min(ts_ms) AS p_min_ms,
            max(ts_ms) AS p_max_ms,
            count() AS p_reqs,
            max(has_rtt) AS p_rtt,
            max(has_edge) AS p_edge,
            max(has_esid) AS p_esid,
            max(has_cmcd) AS p_cmcd,
            groupArray(ts_ms) AS raw_ts,
            if(intDiv(p_max_ms, 1000) - intDiv(p_min_ms, 1000) > 1800, arraySort(raw_ts), []) AS sorted_ts,
            if(
                intDiv(p_max_ms, 1000) - intDiv(p_min_ms, 1000) <= 1800,
                [toUInt32(1)],
                arrayFilter(
                    i -> i = 1 OR intDiv(sorted_ts[i], 1000) - intDiv(sorted_ts[i - 1], 1000) > 1800,
                    arrayEnumerate(sorted_ts)
                )
            ) AS starts
        FROM raw_events
        GROUP BY ip, ja4
    ),
    sessions_agg AS (
        SELECT
            ip,
            ja4,
            fromUnixTimestamp64Milli(toInt64(if(sess_idx = 0, p_min_ms, sorted_ts[starts[sess_idx]]))) AS session_start,
            fromUnixTimestamp64Milli(toInt64(if(
                sess_idx = 0,
                p_max_ms,
                sorted_ts[if(sess_idx < length(starts), starts[sess_idx + 1] - 1, length(sorted_ts))]
            ))) AS session_end,
            if(
                sess_idx = 0,
                p_reqs,
                toUInt64(if(sess_idx < length(starts), starts[sess_idx + 1] - starts[sess_idx], length(sorted_ts) - starts[sess_idx] + 1))
            ) AS req_count,
            toUInt64(0) AS reqs_4xx,
            toUInt64(0) AS reqs_5xx,
            toUInt64(0) AS total_bytes,
            toUInt64(p_edge) AS edge_count,
            toUInt64(0) AS shield_count,
            p_rtt AS has_rtt_row,
            p_esid AS has_edge_sid_row,
            toUInt64(p_cmcd) AS streaming_reqs,
            if(sess_idx = 0, toUInt32(1), sess_idx) AS sid
        FROM pair_sessions
        ARRAY JOIN if(length(starts) <= 1, [toUInt32(0)], arrayEnumerate(starts)) AS sess_idx
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
_SESSIONS_FAST_CTE = """
    WITH raw_events AS (
        SELECT
            client_ip AS ip,
            custom_fields['ja4'] AS ja4,
            toUInt64(toUnixTimestamp64Milli(event_timestamp)) AS ts_ms,
            custom_fields['edge'] AS edge,
            toInt32OrZero(custom_fields['status']) AS status,
            toUInt64(greatest(toInt64OrZero(custom_fields['resp_bytes']), 0)) AS resp_bytes,
            (bitShiftLeft(toUInt64(toUnixTimestamp64Milli(event_timestamp)), 8)
                + bitShiftLeft(toUInt64(status >= 400 AND status < 500), 6)
                + bitShiftLeft(toUInt64(status >= 500), 5)
                + bitShiftLeft(toUInt64(lower(edge) IN ('1', 'true')), 4)
                + bitShiftLeft(toUInt64(lower(edge) IN ('0', 'false')), 3)
                + bitShiftLeft(toUInt64(toFloat64OrNull(custom_fields['tcp_rtt']) IS NOT NULL), 2)
                + bitShiftLeft(toUInt64(custom_fields['edge_sid'] != ''), 1)
                + toUInt64(cmcd['sid'] != '')) AS packed_ev
        FROM request_facts
        WHERE {where}
    ),
    pair_sessions AS (
        SELECT
            ip,
            ja4,
            min(ts_ms) AS p_min_ms,
            max(ts_ms) AS p_max_ms,
            count() AS p_reqs,
            sum(bitAnd(bitShiftRight(packed_ev, 6), 1)) AS p_4xx,
            sum(bitAnd(bitShiftRight(packed_ev, 5), 1)) AS p_5xx,
            sum(resp_bytes) AS p_bytes,
            sum(bitAnd(bitShiftRight(packed_ev, 4), 1)) AS p_edge,
            sum(bitAnd(bitShiftRight(packed_ev, 3), 1)) AS p_shield,
            max(toUInt8(bitAnd(bitShiftRight(packed_ev, 2), 1))) AS p_rtt,
            max(toUInt8(bitAnd(bitShiftRight(packed_ev, 1), 1))) AS p_esid,
            sum(bitAnd(packed_ev, 1)) AS p_stream,
            groupArray(packed_ev) AS raw_ev,
            groupArray(resp_bytes) AS raw_bytes,
            if(intDiv(p_max_ms, 1000) - intDiv(p_min_ms, 1000) > 1800, arraySort(raw_ev), []) AS sorted_ev,
            if(intDiv(p_max_ms, 1000) - intDiv(p_min_ms, 1000) > 1800, arraySort((b, e) -> e, raw_bytes, raw_ev), []) AS sorted_bytes,
            if(
                intDiv(p_max_ms, 1000) - intDiv(p_min_ms, 1000) <= 1800,
                [toUInt32(1)],
                arrayFilter(
                    i -> i = 1 OR intDiv(bitShiftRight(sorted_ev[i], 8), 1000) - intDiv(bitShiftRight(sorted_ev[i - 1], 8), 1000) > 1800,
                    arrayEnumerate(sorted_ev)
                )
            ) AS starts
        FROM raw_events
        GROUP BY ip, ja4
    ),
    sessions_sliced AS (
        SELECT
            ip,
            ja4,
            p_min_ms,
            p_max_ms,
            p_reqs,
            p_4xx,
            p_5xx,
            p_bytes,
            p_edge,
            p_shield,
            p_rtt,
            p_esid,
            p_stream,
            sess_idx,
            if(
                sess_idx = 0,
                [],
                arraySlice(
                    sorted_ev,
                    starts[sess_idx],
                    if(sess_idx < length(starts), starts[sess_idx + 1] - starts[sess_idx], length(sorted_ev) - starts[sess_idx] + 1)
                )
            ) AS s_ev,
            if(
                sess_idx = 0,
                [],
                arraySlice(
                    sorted_bytes,
                    starts[sess_idx],
                    if(sess_idx < length(starts), starts[sess_idx + 1] - starts[sess_idx], length(sorted_bytes) - starts[sess_idx] + 1)
                )
            ) AS s_bytes
        FROM pair_sessions
        ARRAY JOIN if(length(starts) <= 1, [toUInt32(0)], arrayEnumerate(starts)) AS sess_idx
    ),
    sessions_agg AS (
        SELECT
            ip,
            ja4,
            fromUnixTimestamp64Milli(toInt64(if(sess_idx = 0, p_min_ms, bitShiftRight(s_ev[1], 8)))) AS session_start,
            fromUnixTimestamp64Milli(toInt64(if(sess_idx = 0, p_max_ms, bitShiftRight(s_ev[-1], 8)))) AS session_end,
            if(sess_idx = 0, p_reqs, toUInt64(length(s_ev))) AS req_count,
            if(sess_idx = 0, p_4xx, arraySum(x -> bitAnd(bitShiftRight(x, 6), 1), s_ev)) AS reqs_4xx,
            if(sess_idx = 0, p_5xx, arraySum(x -> bitAnd(bitShiftRight(x, 5), 1), s_ev)) AS reqs_5xx,
            if(sess_idx = 0, p_bytes, arraySum(s_bytes)) AS total_bytes,
            if(sess_idx = 0, p_edge, arraySum(x -> bitAnd(bitShiftRight(x, 4), 1), s_ev)) AS edge_count,
            if(sess_idx = 0, p_shield, arraySum(x -> bitAnd(bitShiftRight(x, 3), 1), s_ev)) AS shield_count,
            if(sess_idx = 0, p_rtt, arrayMax(x -> toUInt8(bitAnd(bitShiftRight(x, 2), 1)), s_ev)) AS has_rtt_row,
            if(sess_idx = 0, p_esid, arrayMax(x -> toUInt8(bitAnd(bitShiftRight(x, 1), 1)), s_ev)) AS has_edge_sid_row,
            if(sess_idx = 0, p_stream, arraySum(x -> bitAnd(x, 1), s_ev)) AS streaming_reqs,
            if(sess_idx = 0, toUInt32(1), sess_idx) AS sid
        FROM sessions_sliced
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
            sum(streaming_reqs) AS streaming_reqs,
            sid
        FROM sessions_raw
        GROUP BY ip, ja4, sid
    ),
    flagged_sessions AS (
        SELECT
            *,
            toUInt8(median_rtt_ms IS NOT NULL) AS has_rtt_row,
            toUInt8(edge_sid != '') AS has_edge_sid_row,
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
_asn_label_map = build_asn_label_map


def _as_dt(val: Any) -> datetime | None:
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        return range_value(val) if val else None
    return None


def _enrich_paginated_sessions(
    service: HighScaleService, rows: list[dict[str, Any]], where: str, params: dict[str, Any]
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
        "    min(toInt32OrNull(custom_fields['asn'])) AS en_asn, "
        "    min(country) AS en_country, "
        "    uniq(url) AS en_unique_urls, "
        "    min(custom_fields['ua']) AS en_ua, "
        "    median(toFloat64OrNull(custom_fields['tcp_rtt'])) / 1000.0 AS en_median_rtt_ms, "
        "    max(custom_fields['edge_sid']) AS en_edge_sid, "
        "    sum(if(toInt32OrZero(custom_fields['status']) >= 400 AND toInt32OrZero(custom_fields['status']) < 500, 1, 0)) AS en_reqs_4xx, "
        "    sum(if(toInt32OrZero(custom_fields['status']) >= 500, 1, 0)) AS en_reqs_5xx, "
        "    sum(greatest(toInt64OrZero(custom_fields['resp_bytes']), 0)) AS en_total_bytes, "
        "    sum(if(lower(custom_fields['edge']) IN ('1', 'true'), 1, 0)) AS en_edge_count, "
        "    sum(if(lower(custom_fields['edge']) IN ('0', 'false'), 1, 0)) AS en_shield_count, "
        "    sum(if(cmcd['sid'] != '', 1, 0)) AS en_streaming_reqs "
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
            rows[idx]["asn"] = en_row.get("en_asn", en_row.get("asn"))
            rows[idx]["country"] = en_row.get("en_country", en_row.get("country")) or ""
            rows[idx]["unique_urls"] = int(en_row.get("en_unique_urls", en_row.get("unique_urls")) or 0)
            rows[idx]["ua"] = en_row.get("en_ua", en_row.get("ua")) or ""
            rows[idx]["median_rtt_ms"] = en_row.get("en_median_rtt_ms", en_row.get("median_rtt_ms"))
            rows[idx]["edge_sid"] = en_row.get("en_edge_sid", en_row.get("edge_sid")) or ""
            if "en_reqs_4xx" in en_row:
                r_cnt = int(rows[idx].get("req_count") or 0)
                r_4xx = int(en_row.get("en_reqs_4xx") or 0)
                r_stream = int(en_row.get("en_streaming_reqs") or 0)
                min_reqs = int(params.get("min_reqs_flag") or _DEFAULT_MIN_REQS_FLAG)
                min_4xx_pct = float(params.get("min_4xx_pct_flag") or _DEFAULT_MIN_4XX_PCT_FLAG)
                rows[idx]["reqs_4xx"] = r_4xx
                rows[idx]["reqs_5xx"] = int(en_row.get("en_reqs_5xx") or 0)
                rows[idx]["total_bytes"] = int(en_row.get("en_total_bytes") or 0)
                rows[idx]["edge_count"] = int(en_row.get("en_edge_count") or 0)
                rows[idx]["shield_count"] = int(en_row.get("en_shield_count") or 0)
                rows[idx]["streaming_reqs"] = r_stream
                rows[idx]["flagged"] = bool(
                    (r_cnt >= min_reqs) or (r_cnt > 0 and (r_4xx * 100.0 / r_cnt) >= min_4xx_pct)
                )
                rows[idx]["is_streaming"] = bool(r_stream > 0)


def sessions_endpoint(
    service: HighScaleService, req: SessionsRequest, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    start_dt, end_dt = parse_time_window(start_time, end_time, timedelta(days=7))
    min_reqs_flag = req.min_reqs_flag if req.min_reqs_flag is not None else _DEFAULT_MIN_REQS_FLAG
    min_4xx_pct_flag = req.min_4xx_pct_flag if req.min_4xx_pct_flag is not None else _DEFAULT_MIN_4XX_PCT_FLAG
    filter_clauses, filter_params = request_filter_clauses(req.filters)
    where = " AND ".join(
        [
            *visible_fact_clauses("request", client=service.client, service_id=service.service_id),
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
    sort_by, sort_dir = normalize_sessions_sort(req.sort_by, str(req.sort_dir))
    use_index_fast = not req.flagged_only and not req.streaming_only and sort_by in _INDEX_FAST_SORTS
    fast_cte = (_SESSIONS_INDEX_FAST_CTE if use_index_fast else _SESSIONS_FAST_CTE).format(where=where)
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
    cached = _sessions_memo.get_with_telemetry(memo_key)
    if cached is not None:
        return {**cached, "sessions": [dict(r) for r in cached.get("sessions", [])]}

    def _compute() -> dict[str, Any]:
        if (cached_inner := _sessions_memo.get_with_telemetry(memo_key)) is not None:
            return cached_inner
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
        result = shape_sessions_envelope(
            sessions,
            total=total,
            page=req.page,
            limit=req.limit,
            has_rtt=bool(first.get("any_rtt")),
            has_ja4=bool(first.get("any_ja4")),
            has_edge=bool(first.get("any_edge")),
            has_edge_sid=bool(first.get("any_edge_sid")),
            has_cmcd=bool(first.get("any_cmcd")),
            min_reqs_flag=min_reqs_flag,
            min_4xx_pct_flag=min_4xx_pct_flag,
            service_id=service.service_id,
            asn_label_resolver=_asn_label_map,
        )
        _sessions_memo.put(memo_key, result)
        return result

    out = _sessions_memo.coalesce(f"hs_sessions:{memo_key}", _compute)
    return {**out, "sessions": [dict(r) for r in out.get("sessions", [])]}


def sessions_detail(
    service: HighScaleService,
    req: SessionDetailRequest,
    start_time: str | None,
    end_time: str | None,
    ip: str,
    ja4: str | None,
) -> dict[str, Any]:
    start_dt, end_dt = parse_time_window(start_time, end_time, timedelta(days=7))
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key: str | None = None
    if is_real_ch and start_time is not None and end_time is not None:
        memo_key = digest_cache_key(
            {
                "ep": "hs_sessions_detail",
                "cid": client_cache_token(service.client),
                "s": start_dt.isoformat(),
                "e": end_dt.isoformat(),
                "ip": ip,
                "ja4": ja4 or "",
            },
            {"name": service.service_id, "service_id": service.service_id},
        )
        if (cached := _sessions_memo.get_with_telemetry(memo_key)) is not None:
            return cached
    clauses = [
        *visible_fact_clauses("request"),
        "client_ip = {ip:String}",
        "event_timestamp >= {start_time:DateTime64(3)}",
        "event_timestamp <= {end_time:DateTime64(3)}",
    ]
    params: dict[str, Any] = {"service_id": service.service_id, "start_time": start_dt, "end_time": end_dt, "ip": ip}
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
    out = {"data": data, "columns": list(data[0].keys()) if data else []}
    if memo_key is not None:
        _sessions_memo.put(memo_key, dict(out))
        return _sessions_memo.result(out, state="miss")
    return out


if (_hs_pkg := sys.modules.get("backend.high_scale")) is not None:
    setattr(_hs_pkg, "sessions", sys.modules[__name__])
