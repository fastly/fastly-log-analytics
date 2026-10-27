"""High-scale security, bot verification, NGWAF, proxy & threat-intel analytics, and minute projections."""

from __future__ import annotations

import contextvars
import sys
from collections import defaultdict
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from backend.core.request_telemetry import HighScaleSections, clickhouse_source
from backend.core.share_db import mask_ip
from backend.high_scale._common import (
    _bucket_case,
    parse_time_window,
    visible_publication_fence_sql,
)
from backend.high_scale._common import (
    bucket as _bucket,
)
from backend.high_scale._common import build_clickhouse_filters as _build_clickhouse_filters
from backend.high_scale._common import (
    text as _text,
)
from backend.models.analytics import SecurityAggregatesResponse, SecurityTopBotsResponse
from backend.repositories.security import _BOT_UA_CLASSIFY_LIMIT, classify_ua_counts, summarize_wellknown_bot_rows
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    cache_result,
    client_cache_token,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    SECURITY_CONN_REUSE_BUCKETS,
    SECURITY_CONN_REUSE_LAST,
    SECURITY_REQ_HEADER_BUCKETS,
    SECURITY_REQ_HEADER_LAST,
    finish_security_proxies,
    shape_security_bucket_rows,
    shape_security_fingerprint_row,
    shape_security_ngwaf_bot_row,
    shape_security_ngwaf_top_bot_row,
    shape_security_threat_intel_row,
    shape_security_top_ip_header_row,
)
from backend.repositories.utils.response_shaping import (
    opt_float as _flt,
)
from backend.utils.bounded_cache import CacheRegistry
from backend.utils.date_utils import safe_iso

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

_security_memo = ResponseMemo(ttl=30.0, maxsize=128, prewarmer=False)
CacheRegistry.register("hs_security_aggregates_memo", _security_memo.cache)

_WHERE = "__WHERE__"

_REQ_HEADER_BUCKETS = SECURITY_REQ_HEADER_BUCKETS
_REQ_HEADER_LAST = SECURITY_REQ_HEADER_LAST
_CONN_REUSE_BUCKETS = SECURITY_CONN_REUSE_BUCKETS
_CONN_REUSE_LAST = SECURITY_CONN_REUSE_LAST

from backend.utils.pop_utils import _effective_pop_lat_lon_map

_POP_COORDS = tuple((code, lat, lon) for code, (lat, lon) in sorted(_effective_pop_lat_lon_map().items()))
_POP_DEFAULT = (38.0, -97.0)
_IS_IPV6 = "lower(custom_fields['is_ipv6']) IN ('1', 'true', 't', 'yes')"
_IS_BOT = "lower(custom_fields['is_bot']) IN ('1', 'true', 't', 'yes')"


def _pop_coord(index: int, default: float) -> str:
    arms = ", ".join(f"pop = '{name}', {(lat, lon)[index]}" for name, lat, lon in _POP_COORDS)
    return f"multiIf({arms}, {default})"


_POP_LAT = _pop_coord(0, _POP_DEFAULT[0])
_POP_LON = _pop_coord(1, _POP_DEFAULT[1])
_DISTANCE_KM = (
    "6371 * 2 * asin(sqrt(pow(sin(radians(lat - pop_lat) / 2), 2) "
    "+ cos(radians(pop_lat)) * cos(radians(lat)) * pow(sin(radians(lon - pop_lon) / 2), 2)))"
)
_IMPOSSIBLE_DISTANCE = (
    f"multiIf(rtt_min IS NULL OR lat IS NULL OR lon IS NULL, 0, "
    f"ifNull({_DISTANCE_KM} > (rtt_min / 1000.0 * 100.0 + 150.0), 0))"
)
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
    start, end = parse_time_window(start_time, end_time)
    filter_sql, filter_params = _build_clickhouse_filters(getattr(req, "filters", None) or {})
    where = (
        f"service_id={{service_id:String}} AND {visible_publication_fence_sql('request')} "
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
    source: Literal["fact", "projection"] = (
        "projection" if tag in {"ngwaf_bots", "ngwaf_verified_bots_ts"} and not scope.filtered else "fact"
    )
    with clickhouse_source(source):
        return service.client.execute(
            f"/* hs_security:{tag} */ {sql.replace(_WHERE, scope.where)}", {**scope.params, **extra}
        )


def _sec_iso(ts: Any) -> str | None:
    return safe_iso(datetime.fromtimestamp(int(ts), UTC).replace(tzinfo=None))


def _bucket_seconds(req: Any) -> int:
    return max(1, int(getattr(req, "bucket_seconds", 300) or 300))


def _bucket_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return shape_security_bucket_rows(rows)


def _tls_fingerprints(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "tls_fingerprints",
        "SELECT custom_fields['tls_ciphers_sha'] AS fingerprint, uniqExact(client_ip) AS ip_count, count() AS request_count "
        "FROM request_facts WHERE __WHERE__ AND custom_fields['tls_ciphers_sha'] != '' "
        "GROUP BY fingerprint ORDER BY request_count DESC LIMIT 20",
    )
    return [shape_security_fingerprint_row(r["fingerprint"], r["ip_count"], r["request_count"]) for r in rows]


def _fingerprint_coverage(service: HighScaleService, scope: _Scope) -> dict[str, float]:
    rows = _run(
        service,
        scope,
        "fingerprint_coverage",
        "SELECT count() AS total_rows, countIf(custom_fields['tls_ciphers_sha'] != '') AS populated "
        "FROM request_facts WHERE __WHERE__",
    )
    total = int(rows[0]["total_rows"]) if rows else 0
    return {"tls_ciphers_sha": (int(rows[0]["populated"]) / total) if rows and total > 0 else 0.0}


def _req_size_dist(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    case = _bucket_case("h", _REQ_HEADER_BUCKETS, _REQ_HEADER_LAST)
    return _bucket_rows(
        _run(
            service,
            scope,
            "req_size_dist",
            f"SELECT {case} AS bucket, count() AS count, min(h) AS min_val FROM "
            "(SELECT toUInt64OrNull(custom_fields['req_header_bytes']) AS h FROM request_facts WHERE __WHERE__) "
            "WHERE h IS NOT NULL GROUP BY bucket ORDER BY min_val",
        )
    )


def _top_ips_header(service: HighScaleService, scope: _Scope, mask_ips: bool) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "top_ips_header",
        "SELECT client_ip AS ip, max(h) AS max_header FROM "
        "(SELECT client_ip, toUInt64OrNull(custom_fields['req_header_bytes']) AS h FROM request_facts "
        "WHERE __WHERE__ AND client_ip != '') WHERE h IS NOT NULL GROUP BY ip ORDER BY max_header DESC LIMIT 10",
    )
    return [shape_security_top_ip_header_row(r["ip"], r["max_header"], mask_ips=mask_ips) for r in rows]


def _conn_reuse_dist(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    case = _bucket_case("c", _CONN_REUSE_BUCKETS, _CONN_REUSE_LAST)
    return _bucket_rows(
        _run(
            service,
            scope,
            "conn_reuse_dist",
            f"SELECT {case} AS bucket, count() AS count, min(c) AS min_val FROM "
            "(SELECT toUInt64OrNull(custom_fields['conn_requests']) AS c FROM request_facts WHERE __WHERE__) "
            "WHERE c IS NOT NULL AND c > 0 GROUP BY bucket ORDER BY min_val",
        )
    )


def _ipv6_adoption(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "ipv6_adoption",
        f"SELECT toUnixTimestamp(toStartOfHour(event_timestamp)) AS bucket_ts, countIf({_IS_IPV6}) * 100.0 / count() AS pct "
        "FROM request_facts WHERE __WHERE__ GROUP BY bucket_ts ORDER BY bucket_ts",
    )
    return [{"time": _sec_iso(r["bucket_ts"]), "pct": float(r["pct"])} for r in rows]


def _proxy_dist(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "proxy_dist",
        "SELECT custom_fields['p_type'] AS type, count() AS count FROM request_facts "
        "WHERE __WHERE__ AND custom_fields['p_type'] != '' GROUP BY type ORDER BY count DESC",
    )
    return [{"type": r["type"], "count": int(r["count"])} for r in rows]


def _verified_bots_ts(service: HighScaleService, scope: _Scope, bucket_seconds: int) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "verified_bots_ts",
        f"SELECT intDiv(toUnixTimestamp(event_timestamp), {bucket_seconds}) * {bucket_seconds} AS bucket_ts, "
        "replaceOne(tag, 'VERIFIED-BOT.', '') AS bot_type, count() AS count "
        "FROM request_facts ARRAY JOIN splitByChar(',', custom_fields['waf_sig']) AS tag "
        "WHERE __WHERE__ AND positionCaseInsensitive(custom_fields['waf_sig'], 'VERIFIED-BOT.') > 0 "
        "AND startsWith(tag, 'VERIFIED-BOT.') GROUP BY bucket_ts, bot_type ORDER BY bucket_ts, bot_type",
    )
    return [{"time": _sec_iso(r["bucket_ts"]), "bot_type": r["bot_type"], "count": int(r["count"])} for r in rows]


_NGWAF_PROJECTION_WHERE = (
    f"service_id={{service_id:String}} AND {visible_publication_fence_sql('security_dimensions')} AND dimension='ngwaf_bot' "
    "AND bucket_start >= toStartOfMinute({start:DateTime64(3)}) AND bucket_start < {end:DateTime64(3)}"
)


def _ngwaf_summary(service: HighScaleService, scope: _Scope, limit: int | None = None) -> list[dict[str, Any]]:
    suffix = f" LIMIT {int(limit)}" if limit is not None else ""
    sql = (
        "SELECT custom_fields['ngwaf_bot'] AS bot_name, "
        "anyIf(custom_fields['ngwaf_wellknown_bot'], custom_fields['ngwaf_wellknown_bot'] != '') AS wk_name, "
        "anyIf(custom_fields['ngwaf_bot_category'], custom_fields['ngwaf_bot_category'] != '') AS bot_cat, "
        "count() AS request_count FROM request_facts WHERE __WHERE__ AND custom_fields['ngwaf_bot'] != '' "
        f"GROUP BY bot_name ORDER BY request_count DESC{suffix}"
        if scope.filtered
        else "SELECT value AS bot_name, anyIf(wellknown_bot_name, wellknown_bot_name != '') AS wk_name, "
        "anyIf(bot_category, bot_category != '') AS bot_cat, sum(requests) AS request_count "
        f"FROM security_minute_dimensions WHERE {_NGWAF_PROJECTION_WHERE} GROUP BY bot_name ORDER BY request_count DESC{suffix}"
    )
    return _run(service, scope, "ngwaf_bots", sql)


def _ngwaf_verified_bots(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    return [
        shape_security_ngwaf_bot_row(r["bot_name"], r["wk_name"], r["bot_cat"], r["request_count"])
        for r in _ngwaf_summary(service, scope)
    ]


def _ngwaf_verified_bots_ts(service: HighScaleService, scope: _Scope, bucket_seconds: int) -> list[dict[str, Any]]:
    sql = (
        f"SELECT intDiv(toUnixTimestamp(event_timestamp), {bucket_seconds}) * {bucket_seconds} AS bucket_ts, "
        "custom_fields['ngwaf_bot'] AS bot_name, count() AS count FROM request_facts "
        "WHERE __WHERE__ AND custom_fields['ngwaf_bot'] != '' GROUP BY bucket_ts, bot_name ORDER BY bucket_ts, bot_name"
        if scope.filtered
        else f"SELECT intDiv(toUnixTimestamp(bucket_start), {bucket_seconds}) * {bucket_seconds} AS bucket_ts, "
        f"value AS bot_name, sum(requests) AS count FROM security_minute_dimensions WHERE {_NGWAF_PROJECTION_WHERE} "
        "GROUP BY bucket_ts, bot_name ORDER BY bucket_ts, bot_name"
    )
    return [
        {"time": _sec_iso(r["bucket_ts"]), "bot_name": r["bot_name"], "count": int(r["count"])}
        for r in _run(service, scope, "ngwaf_verified_bots_ts", sql)
    ]


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
        f"SELECT custom_fields['ua'] AS ua, client_ip AS ip, count() AS cnt FROM request_facts "
        f"WHERE __WHERE__ AND custom_fields['ua'] != '' AND client_ip != ''{prefilter} "
        "GROUP BY ua, ip ORDER BY cnt DESC LIMIT 10000",
        **({"bot_pattern": pattern} if pattern else {}),
    )
    return summarize_wellknown_bot_rows([(r["ua"], r["ip"], int(r["cnt"])) for r in rows])


def _bot_efficiency(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    from backend.repositories.security import summarize_bot_efficiency_rows

    rows = _run(
        service,
        scope,
        "bot_efficiency",
        """
        WITH ua_all AS (
            SELECT custom_fields['ua'] AS key, count() AS cnt,
                   ifNull(sum(toInt64OrZero(custom_fields['resp_bytes'])), 0) AS resp_bytes,
                   sum(count()) OVER () AS site_cnt,
                   ifNull(sum(sum(toInt64OrZero(custom_fields['resp_bytes']))) OVER (), 0) AS site_resp_bytes
            FROM request_facts WHERE __WHERE__ GROUP BY key
        ),
        ua_agg AS (
            SELECT 'ua' AS kind, key, cnt, resp_bytes, site_cnt, site_resp_bytes
            FROM ua_all WHERE key != '' ORDER BY resp_bytes DESC, cnt DESC LIMIT 2000
        ),
        ref_extracted AS (
            SELECT client_ip AS ip,
                   if((positionCaseInsensitive(custom_fields['ua'], '-user') > 0
                       OR positionCaseInsensitive(custom_fields['ua'], '-externalfetcher') > 0)
                      AND extract(lower(custom_fields['ua']), '([a-z0-9]+)-(?:user|externalfetcher)') != '',
                      extract(lower(custom_fields['ua']), '([a-z0-9]+)-(?:user|externalfetcher)'),
                      extract(lower(custom_fields['referer']), 'https?://(?:www\\.)?([^/.:;\\s]+)')) AS ref_stem
            FROM request_facts
            WHERE __WHERE__ AND client_ip != ''
              AND ((custom_fields['referer'] != '' AND custom_fields['referer'] != '-')
                   OR positionCaseInsensitive(custom_fields['ua'], '-user') > 0
                   OR positionCaseInsensitive(custom_fields['ua'], '-externalfetcher') > 0)
              AND NOT match(lower(custom_fields['ua']), '(bot|crawler|spider|slurp|archiver|scraper|externalagent|extended|headless)')
        ),
        ref_agg AS (
            SELECT 'ref' AS kind, ref_stem AS key, uniqExact(ip) AS cnt,
                   toInt64(0) AS resp_bytes, toUInt64(0) AS site_cnt, toInt64(0) AS site_resp_bytes
            FROM ref_extracted WHERE ref_stem != '' GROUP BY ref_stem
        )
        SELECT kind, key, cnt, resp_bytes, site_cnt, site_resp_bytes FROM ua_agg
        UNION ALL SELECT kind, key, cnt, resp_bytes, site_cnt, site_resp_bytes FROM ref_agg
        """,
    )
    tagged: list[tuple[str, str, int, int]] = []
    if not any(str(r.get("kind") or "") == "site" for r in rows):
        for r in rows:
            if str(r.get("kind") or "") == "ua" and ("site_cnt" in r or "site_resp_bytes" in r):
                tagged.append(("site", "", int(r.get("site_cnt") or 0), int(r.get("site_resp_bytes") or 0)))
                break
    tagged.extend(
        (str(r["kind"] or ""), str(r["key"] or ""), int(r["cnt"] or 0), int(r["resp_bytes"] or 0)) for r in rows
    )
    return summarize_bot_efficiency_rows(tagged)


def _maybe_mask(ip: Any, mask_ips: bool) -> Any:
    if not mask_ips or not ip:
        return ip

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
    bucket_seconds = _bucket_seconds(req)
    memo_key = digest_cache_key(
        {
            "ep": "hs_security_aggregates",
            "cid": client_cache_token(service.client),
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": serialize_filters_for_key(getattr(req, "filters", None) or {}),
            "b": int(bucket_seconds),
            "sec": sorted(sections) if sections is not None else None,
            "m": bool(mask_ips),
        },
        {"name": service.service_id, "service_id": service.service_id},
    )
    if (cached := _security_memo.get_with_telemetry(memo_key)) is not None:
        return SecurityAggregatesResponse.with_telemetry(**cached)

    def _compute() -> dict[str, Any]:
        if (cached_inner := _security_memo.get(memo_key)) is not None:
            return cached_inner
        scope = _scope(service, req, start_time, end_time)

        def want(name: str) -> bool:
            return sections is None or name in sections

        candidates: tuple[tuple[str, bool, Callable[[], Any]], ...] = (
            ("tls_fingerprints", want("tls_fingerprints"), lambda: _tls_fingerprints(service, scope)),
            ("fingerprint_coverage", want("fingerprint_coverage"), lambda: _fingerprint_coverage(service, scope)),
            ("req_size_dist", want("req_size_dist"), lambda: _req_size_dist(service, scope)),
            ("top_ips_header", want("top_ips_header"), lambda: _top_ips_header(service, scope, mask_ips)),
            ("ipv6_adoption", want("ipv6_adoption"), lambda: _ipv6_adoption(service, scope)),
            ("proxy_dist", want("proxy_dist"), lambda: _proxy_dist(service, scope)),
            ("conn_reuse_dist", want("conn_reuse_dist"), lambda: _conn_reuse_dist(service, scope)),
            ("verified_bots_ts", want("verified_bots_ts"), lambda: _verified_bots_ts(service, scope, bucket_seconds)),
            (
                "ngwaf_configured",
                want("ngwaf_verified_bots") or want("ngwaf_verified_bots_ts"),
                lambda: _ngwaf_configured(service),
            ),
            (
                "ngwaf_verified_bots",
                want("ngwaf_verified_bots") or want("ngwaf_verified_bots_ts"),
                lambda: _ngwaf_verified_bots(service, scope),
            ),
            (
                "ngwaf_verified_bots_ts",
                want("ngwaf_verified_bots") or want("ngwaf_verified_bots_ts"),
                lambda: _ngwaf_verified_bots_ts(service, scope, bucket_seconds),
            ),
            ("wellknown_bots", want("wellknown_bots"), lambda: _wellknown_bots(service, scope)),
            ("bot_efficiency", want("bot_efficiency"), lambda: _bot_efficiency(service, scope)),
            ("_top_bots", want("top_bots"), lambda: _top_bots(service, scope)),
            ("_proxies", want("proxies"), lambda: _proxies(service, scope, mask_ips)),
        )
        tasks = [(k, fn) for k, w, fn in candidates if w]

        trace = HighScaleSections(service.service_id, "security")

        def source_for(key: str) -> Literal["fact", "projection", "mixed", "unknown"]:
            if key == "ngwaf_configured":
                return "unknown"
            if key == "_top_bots" and not scope.filtered:
                return "mixed"
            return "projection" if key.startswith("ngwaf_") and not scope.filtered else "fact"

        res: dict[str, Any] = {}
        for k, val in trace.run_parallel(
            tasks,
            source=source_for,
            activity=lambda k: "overhead" if k == "ngwaf_configured" else "compute",
            max_workers=8,
            thread_name_prefix="hs-sec",
        ).items():
            if k == "_top_bots":
                res.update(bots=val["bots"], ngwaf_bots=val["ngwaf_bots"])
            elif k == "_proxies":
                res.update(val)
            else:
                res[k] = val
        res["section_timings"] = trace.timings()
        _security_memo.put(memo_key, dict(res))
        return res

    return SecurityAggregatesResponse.with_telemetry(**_security_memo.coalesce(f"hs_security_agg:{memo_key}", _compute))


def _top_bots(service: HighScaleService, scope: _Scope, n: int = 10) -> dict[str, list[dict[str, Any]]]:
    ua_rows = _run(
        service,
        scope,
        "top_bots_ua",
        "SELECT custom_fields['ua'] AS ua, count() AS cnt FROM request_facts "
        "WHERE __WHERE__ AND custom_fields['ua'] != '' GROUP BY ua ORDER BY cnt DESC LIMIT {ua_limit:UInt32}",
        ua_limit=_BOT_UA_CLASSIFY_LIMIT,
    )
    return {
        "bots": classify_ua_counts([(r["ua"], int(r["cnt"])) for r in ua_rows], n),
        "ngwaf_bots": [
            shape_security_ngwaf_top_bot_row(r["bot_name"], r["bot_cat"], r["request_count"])
            for r in _ngwaf_summary(service, scope, limit=n)
        ],
    }


def top_bots(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityTopBotsResponse:
    scope = _scope(service, req, start_time, end_time)
    trace = HighScaleSections(service.service_id, "security")
    result = trace.run("top_bots", lambda: _top_bots(service, scope), source="fact" if scope.filtered else "mixed")
    return SecurityTopBotsResponse.with_telemetry(
        **cache_result(result, state="bypass", prewarmer=False), section_timings=trace.timings()
    )


_PROXY_SOURCE = (
    "FROM (SELECT client_ip AS ip, custom_fields['pop'] AS pop, country, custom_fields['city'] AS city, "
    "toFloat64OrNull(custom_fields['rtt_min']) AS rtt_min, toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt, "
    "toFloat64OrNull(custom_fields['lat']) AS lat, toFloat64OrNull(custom_fields['lon']) AS lon, "
    f"toInt64OrNull(custom_fields['asn']) AS asn, {_POP_LAT} AS pop_lat, {_POP_LON} AS pop_lon "
    "FROM request_facts WHERE __WHERE__ AND custom_fields['rtt_min'] != '')"
)


def _proxies(service: HighScaleService, scope: _Scope, mask_ips: bool) -> dict[str, Any]:
    def _q_stats():
        return _run(
            service,
            scope,
            "proxy_stats",
            f"SELECT uniqExactIf(ip, {_IMPOSSIBLE_DISTANCE} OR {_TUNNEL}) AS active_proxies_count, "
            f"countIf({_IMPOSSIBLE_DISTANCE} OR {_TUNNEL}) AS total_requests_count, "
            f"uniqExactIf(ip, {_IMPOSSIBLE_DISTANCE}) AS distance_mismatches_count {_PROXY_SOURCE}",
        )

    def _q_quality():
        return _run(
            service,
            scope,
            "traffic_quality",
            "SELECT multiIf(rtt_min IS NULL OR tcp_rtt IS NULL, 'Direct Connection', "
            "tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.1, 'Active Tunnel / Proxy', "
            "tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.7, 'WiFi / Mobile', 'Direct Connection') AS type, count() AS count "
            "FROM (SELECT toFloat64OrNull(custom_fields['rtt_min']) AS rtt_min, "
            "toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt FROM request_facts WHERE __WHERE__) "
            "GROUP BY type ORDER BY count DESC",
        )

    def _q_isps():
        return _run(
            service,
            scope,
            "suspicious_isps",
            f"SELECT asn, count() AS count {_PROXY_SOURCE} WHERE {_TUNNEL} GROUP BY asn ORDER BY count DESC LIMIT 10",
        )

    def _q_clients():
        return _run(
            service,
            scope,
            "active_clients",
            f"SELECT ip, asn, rtt_min / 1000.0 AS rtt_min_ms, tcp_rtt / 1000.0 AS tcp_rtt_ms, {_DISTANCE_KM} AS distance_km, "
            f"pop, lat AS client_lat, lon AS client_lon, pop_lat, pop_lon, {_IMPOSSIBLE_DISTANCE} AS impossible_distance, "
            f"multiIf({_IMPOSSIBLE_DISTANCE}, 'High', {_TUNNEL}, 'High', "
            "ifNull(tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.2, 0), 'Medium', 'Low') AS risk_level, country, city "
            f"{_PROXY_SOURCE} WHERE rtt_min IS NOT NULL AND ({_IMPOSSIBLE_DISTANCE} OR ifNull(tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.2, 0)) "
            "ORDER BY multiIf(risk_level = 'High', 0, risk_level = 'Medium', 1, 2), distance_km DESC LIMIT 100",
        )

    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="hs-proxies") as pool:
        stats, quality_rows, isp_rows, client_rows = [
            f.result()
            for f in (
                pool.submit(contextvars.copy_context().run, fn) for fn in (_q_stats, _q_quality, _q_isps, _q_clients)
            )
        ]

    stat = stats[0] if stats else {}
    raw_clients = [
        {
            "ip": r["ip"],
            "asn": r.get("asn"),
            "rtt_min_ms": round(float(r["rtt_min_ms"]), 2),
            "tcp_rtt_ms": _flt(r["tcp_rtt_ms"], 2),
            "distance_km": _flt(r["distance_km"], 1),
            "pop": r["pop"] or None,
            "client_lat": _flt(r["client_lat"], 4),
            "client_lon": _flt(r["client_lon"], 4),
            "pop_lat": float(r["pop_lat"]),
            "pop_lon": float(r["pop_lon"]),
            "impossible_distance": bool(int(r["impossible_distance"])),
            "risk_level": r["risk_level"],
            "country": r["country"] or None,
            "city": r["city"] or None,
        }
        for r in client_rows
    ]
    return finish_security_proxies(scope.service_id, stat, quality_rows, isp_rows, raw_clients, mask_ips)


def get_proxies_data(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None, *, mask_ips: bool = False
) -> dict[str, Any]:
    scope = _scope(service, req, start_time, end_time)
    trace = HighScaleSections(service.service_id, "security")
    return {
        **trace.run("proxies", lambda: _proxies(service, scope, mask_ips), source="fact"),
        "_section_timings": trace.timings(),
    }


def get_security_threat_intel(
    service: HighScaleService, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    rows = _run(
        service,
        _scope(service, None, start_time, end_time),
        "threat_intel",
        f"SELECT multiIf(custom_fields['ja3'] != '', custom_fields['ja3'], custom_fields['ja4']) AS tls_fingerprint, "
        f"count() AS requests, topK(1)(country)[1] AS top_country, "
        f"countIf(custom_fields['waf_logged_rules'] != '' OR custom_fields['waf_blocked_rules'] != '') AS matched_waf_rules_count, "
        f"round(countIf({_IS_BOT}) * 100.0 / count(), 2) AS bot_percentage, "
        "max(custom_fields['p_type'] != '') AS is_anonymized_proxy FROM request_facts "
        "WHERE __WHERE__ AND tls_fingerprint != '' GROUP BY tls_fingerprint ORDER BY requests DESC LIMIT 50",
    )
    return {
        "data": [
            shape_security_threat_intel_row(
                r["tls_fingerprint"],
                r["requests"],
                r["top_country"],
                r["matched_waf_rules_count"],
                r["bot_percentage"],
                r["is_anonymized_proxy"],
            )
            for r in rows
        ]
    }


@dataclass(frozen=True)
class SecurityProjection:
    dimension_rows: tuple[dict[str, Any], ...]


@dataclass
class _SecurityAccumulator:
    requests: int = 0
    wellknown_bot_name: str = ""
    bot_category: str = ""
    verified_count: int = 0
    impersonator_count: int = 0
    unverified_count: int = 0


def build_security_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> SecurityProjection:
    dimensions: dict[tuple[datetime, str, str], _SecurityAccumulator] = defaultdict(_SecurityAccumulator)
    for row in rows:
        if (bucket := _bucket(row.get("timestamp"))) is None or not (ngwaf_bot := _text(row.get("_ngwaf_bot_name"))):
            continue
        state = _text(row.get("ngwaf_bot_verification_state", "unverified"))
        acc = dimensions[(bucket, "ngwaf_bot", ngwaf_bot)]
        acc.requests += 1
        acc.verified_count += int(state == "verified")
        acc.impersonator_count += int(state == "impersonator")
        acc.unverified_count += int(state not in ("verified", "impersonator"))
        acc.wellknown_bot_name = acc.wellknown_bot_name or _text(row.get("_ngwaf_wellknown_bot_name"))
        acc.bot_category = acc.bot_category or _text(row.get("_ngwaf_bot_category"))
    return SecurityProjection(
        dimension_rows=tuple(_security_dimension_row(b, d, v, acc) for (b, d, v), acc in sorted(dimensions.items()))
    )


def _security_dimension_row(bucket: datetime, dimension: str, value: str, acc: _SecurityAccumulator) -> dict[str, Any]:
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "requests": acc.requests,
        "wellknown_bot_name": acc.wellknown_bot_name,
        "bot_category": acc.bot_category,
        "verified_count": acc.verified_count,
        "impersonator_count": acc.impersonator_count,
        "unverified_count": acc.unverified_count,
    }


if (_hs_pkg := sys.modules.get("backend.high_scale")) is not None:
    setattr(_hs_pkg, "security", sys.modules[__name__])
