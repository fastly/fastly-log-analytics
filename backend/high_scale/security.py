"""High-scale (ClickHouse) Security reads.

Every section mirrors the standard repository contract in
``backend/repositories/security.py``: same bucket labels, same ``time``/``pct``
series shapes, same row keys. Sections are served from ``request_facts`` (so the
request ``filters`` apply) except the NGWAF bot sections of an unfiltered request,
which read the ``ngwaf_bot`` rows of ``security_minute_dimensions`` (built at
ingest from the NGWAF bot cache, which is keyed by ``waf_req_id`` -- a column
``request_facts`` does not carry). Filtered NGWAF sections fall back to the
``ngwaf_bot*`` keys persisted on each fact row at ingest.

Queries are never swallowed: a ClickHouse failure propagates to the router's
error envelope instead of rendering as an empty card.
"""

from __future__ import annotations

import contextvars
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale._filters import build_clickhouse_filters as _build_clickhouse_filters
from backend.high_scale._projection_utils import range_value as _range_value
from backend.high_scale.registry import HighScaleService
from backend.models.security import (
    SecurityAggregatesResponse,
    SecurityProxiesResponse,
    SecurityTopBotsResponse,
)
from backend.repositories.security import (
    _BOT_UA_CLASSIFY_LIMIT,
    classify_ua_counts,
    summarize_wellknown_bot_rows,
)
from backend.utils.date_utils import safe_iso

_WHERE = "__WHERE__"

_REQ_HEADER_BUCKETS = (
    (256, "0-256B"),
    (512, "256-512B"),
    (768, "512-768B"),
    (1024, "768B-1KB"),
    (1536, "1-1.5KB"),
    (2048, "1.5-2KB"),
    (3072, "2-3KB"),
    (4096, "3-4KB"),
    (6144, "4-6KB"),
    (8192, "6-8KB"),
    (12288, "8-12KB"),
    (16384, "12-16KB"),
    (24576, "16-24KB"),
    (32768, "24-32KB"),
)
_REQ_HEADER_LAST = ">32KB"
_CONN_REUSE_BUCKETS = ((1, "1 (None)"), (5, "2-5"), (20, "6-20"), (100, "21-100"))
_CONN_REUSE_LAST = ">100"

# Same fixed POP coordinates as the standard GET_PROXY_STATS / GET_ACTIVE_PROXY_CLIENTS
# templates, so a service reports identical distance verdicts in either mode.
_POP_COORDS = (("SJC", 37.3382, -121.8863), ("IAD", 38.9531, -77.4565), ("NRT", 35.7720, 140.3929))
_POP_DEFAULT = (38.0, -97.0)

_IS_IPV6 = "lower(custom_fields['is_ipv6']) IN ('1', 'true', 't', 'yes')"
_IS_BOT = "lower(custom_fields['is_bot']) IN ('1', 'true', 't', 'yes')"


def _bucket_case(column: str, buckets: tuple[tuple[int, str], ...], last: str) -> str:
    arms = ", ".join(f"{column} <= {limit}, '{label}'" for limit, label in buckets)
    return f"multiIf({arms}, '{last}')"


def _pop_coord(index: int, default: float) -> str:
    arms = ", ".join(f"pop = '{name}', {(lat, lon)[index]}" for name, lat, lon in _POP_COORDS)
    return f"multiIf({arms}, {default})"


_POP_LAT = _pop_coord(0, _POP_DEFAULT[0])
_POP_LON = _pop_coord(1, _POP_DEFAULT[1])
_DISTANCE_KM = (
    "6371 * 2 * asin(sqrt(pow(sin(radians(lat - pop_lat) / 2), 2) "
    "+ cos(radians(pop_lat)) * cos(radians(lat)) * pow(sin(radians(lon - pop_lon) / 2), 2)))"
)
_IMPOSSIBLE_DISTANCE = f"ifNull({_DISTANCE_KM} > (rtt_min / 1000.0 * 100.0 + 150.0), 0)"
_TUNNEL = "ifNull(tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.1, 0)"


@dataclass(frozen=True)
class _Scope:
    service_id: str
    start: datetime
    end: datetime
    where: str
    params: dict[str, Any]
    filtered: bool


def _scope(service: HighScaleService, req: Any, start_time: str | None, end_time: str | None) -> _Scope:
    end = _range_value(end_time) or datetime.now(UTC)
    start = _range_value(start_time) or (end - timedelta(days=1))
    filter_sql, filter_params = _build_clickhouse_filters(getattr(req, "filters", None) or {})
    where = (
        "service_id={service_id:String} AND publication_state='visible' "
        "AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)} "
        f"AND {filter_sql}"
    )
    return _Scope(
        service_id=service.service_id,
        start=start,
        end=end,
        where=where,
        params={"service_id": service.service_id, "start": start, "end": end, **filter_params},
        filtered=filter_sql != "1=1",
    )


def _run(service: HighScaleService, scope: _Scope, tag: str, sql: str, **extra: Any) -> list[dict[str, Any]]:
    return service.client.execute(
        f"/* hs_security:{tag} */ {sql.replace(_WHERE, scope.where)}",
        {**scope.params, **extra},
    )


def _iso(ts: Any) -> str | None:
    return safe_iso(datetime.fromtimestamp(int(ts), UTC).replace(tzinfo=None))


def _bucket_seconds(req: Any) -> int:
    return max(1, int(getattr(req, "bucket_seconds", 300) or 300))


def _bucket_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"bucket": r["bucket"], "count": int(r["count"])} for r in rows]


def _tls_fingerprints(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "tls_fingerprints",
        """
        SELECT custom_fields['tls_ciphers_sha'] AS fingerprint,
               uniqExact(client_ip) AS ip_count,
               count() AS request_count
        FROM request_facts
        WHERE __WHERE__ AND custom_fields['tls_ciphers_sha'] != ''
        GROUP BY fingerprint ORDER BY request_count DESC LIMIT 20
        """,
    )
    return [
        {"fingerprint": r["fingerprint"], "ip_count": int(r["ip_count"]), "request_count": int(r["request_count"])}
        for r in rows
    ]


def _fingerprint_coverage(service: HighScaleService, scope: _Scope) -> dict[str, float]:
    rows = _run(
        service,
        scope,
        "fingerprint_coverage",
        """
        SELECT count() AS total_rows, countIf(custom_fields['tls_ciphers_sha'] != '') AS populated
        FROM request_facts WHERE __WHERE__
        """,
    )
    total = int(rows[0]["total_rows"]) if rows else 0
    populated = int(rows[0]["populated"]) if rows else 0
    return {"tls_ciphers_sha": populated / total if total > 0 else 0.0}


def _req_size_dist(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    case = _bucket_case("h", _REQ_HEADER_BUCKETS, _REQ_HEADER_LAST)
    return _bucket_rows(
        _run(
            service,
            scope,
            "req_size_dist",
            f"""
            SELECT {case} AS bucket, count() AS count, min(h) AS min_val
            FROM (
                SELECT toUInt64OrNull(custom_fields['req_header_bytes']) AS h
                FROM request_facts WHERE __WHERE__
            )
            WHERE h IS NOT NULL
            GROUP BY bucket ORDER BY min_val
            """,
        )
    )


def _top_ips_header(service: HighScaleService, scope: _Scope, mask_ips: bool) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "top_ips_header",
        """
        SELECT client_ip AS ip, max(h) AS max_header
        FROM (
            SELECT client_ip, toUInt64OrNull(custom_fields['req_header_bytes']) AS h
            FROM request_facts WHERE __WHERE__ AND client_ip != ''
        )
        WHERE h IS NOT NULL
        GROUP BY ip ORDER BY max_header DESC LIMIT 10
        """,
    )
    return [{"ip": _maybe_mask(r["ip"], mask_ips), "max_header": int(r["max_header"])} for r in rows]


def _conn_reuse_dist(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    case = _bucket_case("c", _CONN_REUSE_BUCKETS, _CONN_REUSE_LAST)
    return _bucket_rows(
        _run(
            service,
            scope,
            "conn_reuse_dist",
            f"""
            SELECT {case} AS bucket, count() AS count, min(c) AS min_val
            FROM (
                SELECT toUInt64OrNull(custom_fields['conn_requests']) AS c
                FROM request_facts WHERE __WHERE__
            )
            WHERE c IS NOT NULL AND c > 0
            GROUP BY bucket ORDER BY min_val
            """,
        )
    )


def _ipv6_adoption(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "ipv6_adoption",
        f"""
        SELECT toUnixTimestamp(toStartOfHour(event_timestamp)) AS bucket_ts,
               countIf({_IS_IPV6}) * 100.0 / count() AS pct
        FROM request_facts WHERE __WHERE__
        GROUP BY bucket_ts ORDER BY bucket_ts
        """,
    )
    return [{"time": _iso(r["bucket_ts"]), "pct": float(r["pct"])} for r in rows]


def _proxy_dist(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "proxy_dist",
        """
        SELECT custom_fields['p_type'] AS type, count() AS count
        FROM request_facts WHERE __WHERE__ AND custom_fields['p_type'] != ''
        GROUP BY type ORDER BY count DESC
        """,
    )
    return [{"type": r["type"], "count": int(r["count"])} for r in rows]


def _verified_bots_ts(service: HighScaleService, scope: _Scope, bucket_seconds: int) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "verified_bots_ts",
        f"""
        SELECT intDiv(toUnixTimestamp(event_timestamp), {bucket_seconds}) * {bucket_seconds} AS bucket_ts,
               replaceOne(tag, 'VERIFIED-BOT.', '') AS bot_type,
               count() AS count
        FROM request_facts
        ARRAY JOIN splitByChar(',', custom_fields['waf_sig']) AS tag
        WHERE __WHERE__
          AND positionCaseInsensitive(custom_fields['waf_sig'], 'VERIFIED-BOT.') > 0
          AND startsWith(tag, 'VERIFIED-BOT.')
        GROUP BY bucket_ts, bot_type ORDER BY bucket_ts, bot_type
        """,
    )
    return [{"time": _iso(r["bucket_ts"]), "bot_type": r["bot_type"], "count": int(r["count"])} for r in rows]


# NGWAF bot rows. An unfiltered request reads the per-minute ``ngwaf_bot`` projection;
# a filtered one reads the ``ngwaf_bot*`` keys persisted on request_facts at ingest.
_NGWAF_PROJECTION_WHERE = (
    "service_id={service_id:String} AND publication_state='visible' AND dimension='ngwaf_bot' "
    "AND bucket_start >= toStartOfMinute({start:DateTime64(3)}) AND bucket_start < {end:DateTime64(3)}"
)


def _ngwaf_summary(service: HighScaleService, scope: _Scope, limit: int | None = None) -> list[dict[str, Any]]:
    suffix = f" LIMIT {int(limit)}" if limit is not None else ""
    if scope.filtered:
        sql = f"""
            SELECT custom_fields['ngwaf_bot'] AS bot_name,
                   anyIf(custom_fields['ngwaf_wellknown_bot'], custom_fields['ngwaf_wellknown_bot'] != '') AS wk_name,
                   anyIf(custom_fields['ngwaf_bot_category'], custom_fields['ngwaf_bot_category'] != '') AS bot_cat,
                   count() AS request_count
            FROM request_facts WHERE __WHERE__ AND custom_fields['ngwaf_bot'] != ''
            GROUP BY bot_name ORDER BY request_count DESC{suffix}
            """
    else:
        sql = f"""
            SELECT value AS bot_name,
                   anyIf(wellknown_bot_name, wellknown_bot_name != '') AS wk_name,
                   anyIf(bot_category, bot_category != '') AS bot_cat,
                   sum(requests) AS request_count
            FROM security_minute_dimensions WHERE {_NGWAF_PROJECTION_WHERE}
            GROUP BY bot_name ORDER BY request_count DESC{suffix}
            """
    return _run(service, scope, "ngwaf_bots", sql)


def _ngwaf_verified_bots(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    return [
        {
            "bot_name": r["bot_name"],
            "wellknown_bot_name": r["wk_name"] or None,
            "category": r["bot_cat"] or None,
            "request_count": int(r["request_count"]),
        }
        for r in _ngwaf_summary(service, scope)
    ]


def _ngwaf_verified_bots_ts(service: HighScaleService, scope: _Scope, bucket_seconds: int) -> list[dict[str, Any]]:
    if scope.filtered:
        sql = f"""
            SELECT intDiv(toUnixTimestamp(event_timestamp), {bucket_seconds}) * {bucket_seconds} AS bucket_ts,
                   custom_fields['ngwaf_bot'] AS bot_name, count() AS count
            FROM request_facts WHERE __WHERE__ AND custom_fields['ngwaf_bot'] != ''
            GROUP BY bucket_ts, bot_name ORDER BY bucket_ts, bot_name
            """
    else:
        sql = f"""
            SELECT intDiv(toUnixTimestamp(bucket_start), {bucket_seconds}) * {bucket_seconds} AS bucket_ts,
                   value AS bot_name, sum(requests) AS count
            FROM security_minute_dimensions WHERE {_NGWAF_PROJECTION_WHERE}
            GROUP BY bucket_ts, bot_name ORDER BY bucket_ts, bot_name
            """
    rows = _run(service, scope, "ngwaf_verified_bots_ts", sql)
    return [{"time": _iso(r["bucket_ts"]), "bot_name": r["bot_name"], "count": int(r["count"])} for r in rows]


def _ngwaf_configured(service: HighScaleService) -> bool:
    try:
        from backend import config as svcconfig

        return bool(svcconfig.get_ngwaf_workspace_id(service.service_id))
    except Exception:
        return False


def _wellknown_bots(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    from backend.utils.bot_sources import get_bot_regex_pattern

    pattern = get_bot_regex_pattern(500)
    prefilter = " AND match(custom_fields['ua'], {bot_pattern:String})" if pattern else ""
    rows = _run(
        service,
        scope,
        "wellknown_bots",
        f"""
        SELECT custom_fields['ua'] AS ua, client_ip AS ip, count() AS cnt
        FROM request_facts
        WHERE __WHERE__ AND custom_fields['ua'] != '' AND client_ip != ''{prefilter}
        GROUP BY ua, ip ORDER BY cnt DESC LIMIT 10000
        """,
        **({"bot_pattern": pattern} if pattern else {}),
    )
    return summarize_wellknown_bot_rows([(r["ua"], r["ip"], int(r["cnt"])) for r in rows])


def _maybe_mask(ip: Any, mask_ips: bool) -> Any:
    if not mask_ips or not ip:
        return ip
    from backend.core.share_db.validation import mask_ip

    return mask_ip(str(ip))


def security_aggregates(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    sections: set[str] | None = None,
    mask_ips: bool = False,
) -> SecurityAggregatesResponse:
    scope = _scope(service, req, start_time, end_time)
    bucket_seconds = _bucket_seconds(req)

    def want(name: str) -> bool:
        return sections is None or name in sections

    tasks: list[tuple[str, Callable[[], Any]]] = []
    if want("tls_fingerprints"):
        tasks.append(("tls_fingerprints", lambda: _tls_fingerprints(service, scope)))
    if want("fingerprint_coverage"):
        tasks.append(("fingerprint_coverage", lambda: _fingerprint_coverage(service, scope)))
    if want("req_size_dist"):
        tasks.append(("req_size_dist", lambda: _req_size_dist(service, scope)))
    if want("top_ips_header"):
        tasks.append(("top_ips_header", lambda: _top_ips_header(service, scope, mask_ips)))
    if want("ipv6_adoption"):
        tasks.append(("ipv6_adoption", lambda: _ipv6_adoption(service, scope)))
    if want("proxy_dist"):
        tasks.append(("proxy_dist", lambda: _proxy_dist(service, scope)))
    if want("conn_reuse_dist"):
        tasks.append(("conn_reuse_dist", lambda: _conn_reuse_dist(service, scope)))
    if want("verified_bots_ts"):
        tasks.append(("verified_bots_ts", lambda: _verified_bots_ts(service, scope, bucket_seconds)))
    if want("ngwaf_verified_bots") or want("ngwaf_verified_bots_ts"):
        tasks.append(("ngwaf_configured", lambda: _ngwaf_configured(service)))
        tasks.append(("ngwaf_verified_bots", lambda: _ngwaf_verified_bots(service, scope)))
        tasks.append(("ngwaf_verified_bots_ts", lambda: _ngwaf_verified_bots_ts(service, scope, bucket_seconds)))
    if want("wellknown_bots"):
        tasks.append(("wellknown_bots", lambda: _wellknown_bots(service, scope)))
    if want("top_bots"):
        tasks.append(("_top_bots", lambda: _top_bots(service, scope)))
    if want("proxies"):
        tasks.append(("_proxies", lambda: _proxies(service, scope, mask_ips)))

    res: dict[str, Any] = {}
    if len(tasks) == 1:
        k, fn = tasks[0]
        val = fn()
        if k == "_top_bots":
            res["bots"] = val["bots"]
            res["ngwaf_bots"] = val["ngwaf_bots"]
        elif k == "_proxies":
            res.update(val)
        else:
            res[k] = val
    elif tasks:
        with ThreadPoolExecutor(max_workers=min(8, len(tasks)), thread_name_prefix="hs-sec") as pool:
            futs = [(k, pool.submit(contextvars.copy_context().run, fn)) for k, fn in tasks]
            for k, fut in futs:
                val = fut.result()
                if k == "_top_bots":
                    res["bots"] = val["bots"]
                    res["ngwaf_bots"] = val["ngwaf_bots"]
                elif k == "_proxies":
                    res.update(val)
                else:
                    res[k] = val
    return SecurityAggregatesResponse.with_telemetry(**res)


def _top_bots(service: HighScaleService, scope: _Scope, n: int = 10) -> dict[str, list[dict[str, Any]]]:
    ua_rows = _run(
        service,
        scope,
        "top_bots_ua",
        """
        SELECT custom_fields['ua'] AS ua, count() AS cnt
        FROM request_facts WHERE __WHERE__ AND custom_fields['ua'] != ''
        GROUP BY ua ORDER BY cnt DESC LIMIT {ua_limit:UInt32}
        """,
        ua_limit=_BOT_UA_CLASSIFY_LIMIT,
    )
    bots = classify_ua_counts([(r["ua"], int(r["cnt"])) for r in ua_rows], n)
    ngwaf_bots = [
        {"name": r["bot_name"], "category": r["bot_cat"] or None, "request_count": int(r["request_count"])}
        for r in _ngwaf_summary(service, scope, limit=n)
    ]
    return {"bots": bots, "ngwaf_bots": ngwaf_bots}


def top_bots(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityTopBotsResponse:
    scope = _scope(service, req, start_time, end_time)
    return SecurityTopBotsResponse.with_telemetry(**_top_bots(service, scope))


_PROXY_SOURCE = f"""
    FROM (
        SELECT client_ip AS ip,
               custom_fields['pop'] AS pop,
               country,
               custom_fields['city'] AS city,
               toFloat64OrNull(custom_fields['rtt_min']) AS rtt_min,
               toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt,
               toFloat64OrNull(custom_fields['lat']) AS lat,
               toFloat64OrNull(custom_fields['lon']) AS lon,
               toInt64OrNull(custom_fields['asn']) AS asn,
               {_POP_LAT} AS pop_lat,
               {_POP_LON} AS pop_lon
        FROM request_facts WHERE __WHERE__
    )
"""


def _proxies(service: HighScaleService, scope: _Scope, mask_ips: bool) -> dict[str, Any]:
    def _q_stats():
        return _run(
            service,
            scope,
            "proxy_stats",
            f"""
            SELECT uniqExactIf(ip, {_IMPOSSIBLE_DISTANCE} OR {_TUNNEL}) AS active_proxies_count,
                   countIf({_IMPOSSIBLE_DISTANCE} OR {_TUNNEL}) AS total_requests_count,
                   uniqExactIf(ip, {_IMPOSSIBLE_DISTANCE}) AS distance_mismatches_count
            {_PROXY_SOURCE}
            """,
        )

    def _q_quality():
        return _run(
            service,
            scope,
            "traffic_quality",
            f"""
            SELECT multiIf(
                       rtt_min IS NULL OR tcp_rtt IS NULL, 'Direct Connection',
                       tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.1, 'Active Tunnel / Proxy',
                       tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.7, 'WiFi / Mobile',
                       'Direct Connection') AS type,
                   count() AS count
            {_PROXY_SOURCE}
            GROUP BY type ORDER BY count DESC
            """,
        )

    def _q_isps():
        return _run(
            service,
            scope,
            "suspicious_isps",
            f"""
            SELECT asn, count() AS count
            {_PROXY_SOURCE}
            WHERE {_TUNNEL}
            GROUP BY asn ORDER BY count DESC LIMIT 10
            """,
        )

    def _q_clients():
        return _run(
            service,
            scope,
            "active_clients",
            f"""
            SELECT ip, asn, rtt_min / 1000.0 AS rtt_min_ms, tcp_rtt / 1000.0 AS tcp_rtt_ms,
                   {_DISTANCE_KM} AS distance_km, pop,
                   lat AS client_lat, lon AS client_lon, pop_lat, pop_lon,
                   {_IMPOSSIBLE_DISTANCE} AS impossible_distance,
                   multiIf({_IMPOSSIBLE_DISTANCE}, 'High',
                           {_TUNNEL}, 'High',
                           ifNull(tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.2, 0), 'Medium',
                           'Low') AS risk_level,
                   country, city
            {_PROXY_SOURCE}
            WHERE rtt_min IS NOT NULL
              AND ({_IMPOSSIBLE_DISTANCE} OR ifNull(tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.2, 0))
            ORDER BY multiIf(risk_level = 'High', 0, risk_level = 'Medium', 1, 2), distance_km DESC LIMIT 100
            """,
        )

    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="hs-proxies") as pool:
        f_stats = pool.submit(contextvars.copy_context().run, _q_stats)
        f_qual = pool.submit(contextvars.copy_context().run, _q_quality)
        f_isps = pool.submit(contextvars.copy_context().run, _q_isps)
        f_cli = pool.submit(contextvars.copy_context().run, _q_clients)
        stats = f_stats.result()
        quality_rows = f_qual.result()
        isp_rows = f_isps.result()
        client_rows = f_cli.result()

    stat = stats[0] if stats else {}
    quality_total = sum(int(r["count"]) for r in quality_rows)
    traffic_quality = (
        [{"label": str(r["type"]), "value": round(int(r["count"]) / quality_total * 100, 1)} for r in quality_rows]
        if quality_total > 0
        else []
    )

    from backend.core.duckdb import get_asn_names

    asns = {int(r["asn"]) for r in (*isp_rows, *client_rows) if r.get("asn") is not None}
    asn_names = get_asn_names(scope.service_id, sorted(asns))

    def _asn_name(asn: int | None) -> str:
        return asn_names.get(asn) or (f"AS{asn}" if asn else "Unknown ISP")

    suspicious_isps = []
    for r in isp_rows:
        asn = int(r["asn"]) if r.get("asn") is not None else None
        suspicious_isps.append({"isp": _asn_name(asn), "asn": asn, "count": int(r["count"])})

    active_clients = []
    for r in client_rows:
        asn = int(r["asn"]) if r.get("asn") is not None else None
        active_clients.append(
            {
                "ip": _maybe_mask(r["ip"], mask_ips),
                "rtt_min_ms": round(float(r["rtt_min_ms"]), 2),
                "tcp_rtt_ms": round(float(r["tcp_rtt_ms"]), 2) if r["tcp_rtt_ms"] is not None else None,
                "distance_km": round(float(r["distance_km"]), 1) if r["distance_km"] is not None else None,
                "pop": r["pop"] or None,
                "client_lat": round(float(r["client_lat"]), 4) if r["client_lat"] is not None else None,
                "client_lon": round(float(r["client_lon"]), 4) if r["client_lon"] is not None else None,
                "pop_lat": float(r["pop_lat"]),
                "pop_lon": float(r["pop_lon"]),
                "impossible_distance": bool(int(r["impossible_distance"])),
                "risk_level": r["risk_level"],
                "country": r["country"] or None,
                "city": r["city"] or None,
                "asn_name": _asn_name(asn),
            }
        )

    return {
        "active_proxies_count": int(stat.get("active_proxies_count", 0)),
        "tunnel_requests_count": int(stat.get("total_requests_count", 0)),
        "distance_mismatches_count": int(stat.get("distance_mismatches_count", 0)),
        "traffic_quality": traffic_quality,
        "suspicious_isps": suspicious_isps,
        "active_clients": active_clients,
    }


def get_proxies_data(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    mask_ips: bool = False,
) -> SecurityProxiesResponse:
    scope = _scope(service, req, start_time, end_time)
    return SecurityProxiesResponse.with_telemetry(**_proxies(service, scope, mask_ips))


def get_security_threat_intel(
    service: HighScaleService, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    scope = _scope(service, None, start_time, end_time)
    rows = _run(
        service,
        scope,
        "threat_intel",
        f"""
        SELECT multiIf(custom_fields['ja3'] != '', custom_fields['ja3'], custom_fields['ja4']) AS tls_fingerprint,
               count() AS requests,
               topK(1)(country)[1] AS top_country,
               countIf(custom_fields['waf_logged_rules'] != '' OR custom_fields['waf_blocked_rules'] != '')
                   AS matched_waf_rules_count,
               round(countIf({_IS_BOT}) * 100.0 / count(), 2) AS bot_percentage,
               max(custom_fields['p_type'] != '') AS is_anonymized_proxy
        FROM request_facts
        WHERE __WHERE__ AND tls_fingerprint != ''
        GROUP BY tls_fingerprint ORDER BY requests DESC LIMIT 50
        """,
    )
    return {
        "data": [
            {
                "tls_fingerprint": r["tls_fingerprint"],
                "requests": int(r["requests"]),
                "top_country": r["top_country"] or "Unknown",
                "matched_waf_rules_count": int(r["matched_waf_rules_count"]),
                "bot_percentage": float(r["bot_percentage"] or 0.0),
                "is_anonymized_proxy": bool(int(r["is_anonymized_proxy"])),
            }
            for r in rows
        ]
    }
