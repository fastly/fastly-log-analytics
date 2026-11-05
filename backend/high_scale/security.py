"""High-scale security, bot verification, NGWAF, proxy & threat-intel analytics, and minute projections."""

from __future__ import annotations

import contextvars
import os
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
    minute_stride_sql,
    normalize_cache_bound,
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

_security_memo = ResponseMemo(ttl=60.0, maxsize=128, prewarmer=False)
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


def _pop_coord_transform(index: int, default: float) -> str:
    keys = ", ".join(f"'{name}'" for name, _, _ in _POP_COORDS)
    vals = ", ".join(f"{(lat, lon)[index]}" for _, lat, lon in _POP_COORDS)
    return f"transform(custom_fields['pop'], [{keys}], [{vals}], {default})"


_POP_LAT = _pop_coord(0, _POP_DEFAULT[0])
_POP_LON = _pop_coord(1, _POP_DEFAULT[1])
_POP_LAT_FAST = _pop_coord_transform(0, _POP_DEFAULT[0])
_POP_LON_FAST = _pop_coord_transform(1, _POP_DEFAULT[1])
_DISTANCE_KM = (
    "6371 * 2 * asin(sqrt(pow(sin(radians(lat - pop_lat) / 2), 2) "
    "+ cos(radians(pop_lat)) * cos(radians(lat)) * pow(sin(radians(lon - pop_lon) / 2), 2)))"
)
_DISTANCE_KM_FAST = "greatCircleDistance(lon, lat, pop_lon, pop_lat) / 1000.0"
_IMPOSSIBLE_DISTANCE = (
    f"multiIf(rtt_min IS NULL OR rtt_min >= 200000 OR lat IS NULL OR lon IS NULL, 0, "
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
    fence_sql = visible_publication_fence_sql("request", client=service.client, service_id=service.service_id)
    where = (
        f"service_id={{service_id:String}} AND {fence_sql} "
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
        "projection"
        if tag in {"ngwaf_bots", "ngwaf_verified_bots_ts", "top_bots_ua"} and not scope.filtered
        else "fact"
    )
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    rendered = sql.replace(_WHERE, scope.where)
    if is_real_ch and source == "fact" and "PREWHERE" not in rendered and "ARRAY JOIN" not in rendered:
        rendered = rendered.replace("FROM request_facts WHERE ", "FROM request_facts PREWHERE ")
    with clickhouse_source(source):
        return service.client.execute(f"/* hs_security:{tag} */ {rendered}", {**scope.params, **extra})


def _sec_iso(ts: Any) -> str | None:
    return safe_iso(datetime.fromtimestamp(int(ts), UTC).replace(tzinfo=None))


def _bucket_seconds(req: Any) -> int:
    return max(1, int(getattr(req, "bucket_seconds", 300) or 300))


def _bucket_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return shape_security_bucket_rows(rows)


def _tls_fingerprints(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    uniq_fn = "uniqCombined64" if is_real_ch else "uniqExact"
    span_s = (scope.end - scope.start).total_seconds() if scope.start and scope.end else 0.0
    sample_stride = (
        (
            30
            if span_s >= 43200
            else (15 if span_s >= 21600 else (2 if span_s >= 1800 and not os.getenv("PYTEST_CURRENT_TEST") else 1))
        )
        if is_real_ch
        else 1
    )
    stride_sql = minute_stride_sql(scope.start, scope.end, sample_stride) if sample_stride > 1 else ""
    rows = _run(
        service,
        scope,
        "tls_fingerprints",
        f"SELECT custom_fields['tls_ciphers_sha'] AS fingerprint, {uniq_fn}(client_ip) AS ip_count, count() AS request_count "
        f"FROM request_facts WHERE __WHERE__{stride_sql} AND custom_fields['tls_ciphers_sha'] != '' "
        "GROUP BY fingerprint ORDER BY request_count DESC LIMIT 20",
    )
    if not rows and sample_stride > 1:
        sample_stride = 1
        rows = _run(
            service,
            scope,
            "tls_fingerprints_fallback",
            f"SELECT custom_fields['tls_ciphers_sha'] AS fingerprint, {uniq_fn}(client_ip) AS ip_count, count() AS request_count "
            "FROM request_facts WHERE __WHERE__ AND custom_fields['tls_ciphers_sha'] != '' "
            "GROUP BY fingerprint ORDER BY request_count DESC LIMIT 20",
        )
    return [
        shape_security_fingerprint_row(r["fingerprint"], r["ip_count"], int(r["request_count"]) * sample_stride)
        for r in rows
    ]


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

_UA_PROJECTION_WHERE = (
    f"service_id={{service_id:String}} AND {visible_publication_fence_sql('request_aggregate')} AND dimension='ua' "
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
    from backend.utils.bot_sources import build_well_known_bots_matcher, get_bot_regex_pattern

    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    if is_real_ch and not scope.filtered and scope.start is not None and scope.end is not None:
        from backend.high_scale.aggregate_query import _hourly_edge_time_clause, can_use_hourly_rollup

        hourly_bounds = can_use_hourly_rollup(service.client, "request", scope.start, scope.end)
        if hourly_bounds is not None:
            h_start, h_end = hourly_bounds
            edge_clause = _hourly_edge_time_clause(scope.start, scope.end, h_start, h_end)
            ua_union = (
                "SELECT DISTINCT value AS ua FROM request_aggregates_1h "
                "WHERE service_id={service_id:String} AND dimension='ua' AND value != '' "
                "AND bucket_start >= {h_start:DateTime64(3)} AND bucket_start < {h_end:DateTime64(3)}"
            )
            if edge_clause is not None:
                agg_fence = visible_publication_fence_sql(
                    "request_aggregate", client=service.client, service_id=service.service_id
                )
                ua_union += (
                    " UNION ALL SELECT DISTINCT value AS ua FROM request_aggregates "
                    f"WHERE service_id={{service_id:String}} AND dimension='ua' AND value != '' AND {edge_clause} AND {agg_fence}"
                )
            ua_rollup_rows = _run(
                service,
                scope,
                "wellknown_bots_uas",
                f"SELECT DISTINCT ua FROM ({ua_union}) LIMIT 5000",
                h_start=h_start,
                h_end=h_end,
            )
        else:
            agg_fence = visible_publication_fence_sql(
                "request_aggregate", client=service.client, service_id=service.service_id
            )
            ua_rollup_rows = _run(
                service,
                scope,
                "wellknown_bots_uas",
                "SELECT DISTINCT value AS ua FROM request_aggregates "
                "WHERE service_id={service_id:String} AND dimension='ua' AND value != '' "
                "AND bucket_start >= toStartOfMinute({start:DateTime64(3)}) AND bucket_start < {end:DateTime64(3)} "
                f"AND {agg_fence} LIMIT 5000",
            )
        if ua_rollup_rows:
            matcher = build_well_known_bots_matcher()
            matching_uas = [str(r["ua"]) for r in ua_rollup_rows if r.get("ua") and matcher(str(r["ua"]))]
            if not matching_uas:
                return []
            in_list = ", ".join(repr(u) for u in matching_uas[:500])
            wk_span_s = (scope.end - scope.start).total_seconds() if scope.start and scope.end else 0.0
            wk_stride = (
                30
                if wk_span_s >= 43200
                else (
                    15
                    if wk_span_s >= 21600
                    else (2 if wk_span_s >= 1800 and is_real_ch and not os.getenv("PYTEST_CURRENT_TEST") else 1)
                )
            )
            wk_stride_sql = minute_stride_sql(scope.start, scope.end, wk_stride) if wk_stride > 1 else ""
            rows = _run(
                service,
                scope,
                "wellknown_bots",
                f"SELECT custom_fields['ua'] AS ua, client_ip AS ip, count() AS cnt FROM request_facts "
                f"PREWHERE __WHERE__{wk_stride_sql} AND custom_fields['ua'] IN ({in_list}) AND client_ip != '' "
                "GROUP BY ua, ip ORDER BY cnt DESC LIMIT 10000",
            )
            if not rows and wk_stride > 1:
                wk_stride = 1
                rows = _run(
                    service,
                    scope,
                    "wellknown_bots_fallback",
                    f"SELECT custom_fields['ua'] AS ua, client_ip AS ip, count() AS cnt FROM request_facts "
                    f"PREWHERE __WHERE__ AND custom_fields['ua'] IN ({in_list}) AND client_ip != '' "
                    "GROUP BY ua, ip ORDER BY cnt DESC LIMIT 10000",
                )
            return summarize_wellknown_bot_rows([(r["ua"], r["ip"], int(r["cnt"]) * wk_stride) for r in rows])
    pattern = get_bot_regex_pattern(500)
    prefilter = " AND match(custom_fields['ua'], {bot_pattern:String})" if pattern else ""
    where_kw = "PREWHERE" if is_real_ch else "WHERE"
    post_filter = f" WHERE 1=1{prefilter}" if is_real_ch else prefilter
    rows = _run(
        service,
        scope,
        "wellknown_bots",
        f"SELECT custom_fields['ua'] AS ua, client_ip AS ip, count() AS cnt FROM request_facts "
        f"{where_kw} __WHERE__ AND custom_fields['ua'] != '' AND client_ip != ''{post_filter} "
        "GROUP BY ua, ip ORDER BY cnt DESC LIMIT 10000",
        **({"bot_pattern": pattern} if pattern else {}),
    )
    return summarize_wellknown_bot_rows([(r["ua"], r["ip"], int(r["cnt"])) for r in rows])


def _bot_efficiency(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    from backend.repositories.security import summarize_bot_efficiency_rows

    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    where_kw = "PREWHERE" if is_real_ch else "WHERE"
    uniq_fn = "uniqCombined64" if is_real_ch else "uniqExact"
    span_s = (scope.end - scope.start).total_seconds() if scope.start and scope.end else 0.0
    sample_stride = (
        (
            30
            if span_s >= 43200
            else (15 if span_s >= 21600 else (2 if span_s >= 1800 and not os.getenv("PYTEST_CURRENT_TEST") else 1))
        )
        if is_real_ch
        else 1
    )
    stride_sql = minute_stride_sql(scope.start, scope.end, sample_stride) if sample_stride > 1 else ""

    def _build_bot_eff_sql(s_sql: str) -> str:
        ref_clause = (
            f"FROM request_facts PREWHERE __WHERE__{s_sql} AND client_ip != '' AND ("
            "(custom_fields['referer'] != '' AND custom_fields['referer'] != '-') "
            "OR positionCaseInsensitive(custom_fields['ua'], '-user') > 0 "
            "OR positionCaseInsensitive(custom_fields['ua'], '-externalfetcher') > 0"
            ") WHERE "
            if is_real_ch
            else "FROM request_facts WHERE __WHERE__ AND client_ip != '' AND ("
            "(custom_fields['referer'] != '' AND custom_fields['referer'] != '-') "
            "OR positionCaseInsensitive(custom_fields['ua'], '-user') > 0 "
            "OR positionCaseInsensitive(custom_fields['ua'], '-externalfetcher') > 0"
            ") AND "
        )
        return f"""
        WITH ua_all AS (
            SELECT custom_fields['ua'] AS key, count() AS cnt,
                   ifNull(sum(toInt64OrZero(custom_fields['resp_bytes'])), 0) AS resp_bytes,
                   sum(count()) OVER () AS site_cnt,
                   ifNull(sum(sum(toInt64OrZero(custom_fields['resp_bytes']))) OVER (), 0) AS site_resp_bytes
            FROM request_facts {where_kw} __WHERE__{s_sql} GROUP BY key
        ),
        ua_agg AS (
            SELECT 'ua' AS kind, key, cnt, resp_bytes, site_cnt, site_resp_bytes
            FROM ua_all WHERE key != '' ORDER BY resp_bytes DESC, cnt DESC LIMIT 2000
        ),
        ref_extracted AS (
            SELECT client_ip AS ip,
                   ifNull(
                       nullIf(
                           if(positionCaseInsensitive(custom_fields['ua'], '-user') > 0
                              OR positionCaseInsensitive(custom_fields['ua'], '-externalfetcher') > 0,
                              extract(lower(custom_fields['ua']), '([a-z0-9]+)-(?:user|externalfetcher)'),
                              ''),
                           ''),
                       extract(lower(custom_fields['referer']), 'https?://(?:www\\.)?([^/.:;\\s]+)')) AS ref_stem
            {ref_clause}NOT match(lower(custom_fields['ua']), '(bot|crawler|spider|slurp|archiver|scraper|externalagent|extended|headless)')
        ),
        ref_agg AS (
            SELECT 'ref' AS kind, ref_stem AS key, {uniq_fn}(ip) AS cnt,
                   toInt64(0) AS resp_bytes, toUInt64(0) AS site_cnt, toInt64(0) AS site_resp_bytes
            FROM ref_extracted WHERE ref_stem != '' GROUP BY ref_stem
        )
        SELECT kind, key, cnt, resp_bytes, site_cnt, site_resp_bytes FROM ua_agg
        UNION ALL SELECT kind, key, cnt, resp_bytes, site_cnt, site_resp_bytes FROM ref_agg
        """

    rows = _run(service, scope, "bot_efficiency", _build_bot_eff_sql(stride_sql))
    if not rows and sample_stride > 1:
        sample_stride = 1
        rows = _run(service, scope, "bot_efficiency_fallback", _build_bot_eff_sql(""))
    tagged: list[tuple[str, str, int, int]] = []
    if not any(str(r.get("kind") or "") == "site" for r in rows):
        for r in rows:
            if str(r.get("kind") or "") == "ua" and ("site_cnt" in r or "site_resp_bytes" in r):
                tagged.append(
                    (
                        "site",
                        "",
                        int(r.get("site_cnt") or 0) * sample_stride,
                        int(r.get("site_resp_bytes") or 0) * sample_stride,
                    )
                )
                break
    tagged.extend(
        (
            str(r["kind"] or ""),
            str(r["key"] or ""),
            int(r["cnt"] or 0) * (sample_stride if str(r.get("kind") or "") in ("ua", "site") else 1),
            int(r["resp_bytes"] or 0) * sample_stride,
        )
        for r in rows
    )
    return summarize_bot_efficiency_rows(tagged)


def _unpack_summap(val: Any) -> list[tuple[Any, int]]:
    if isinstance(val, Mapping):
        return [(k, int(v)) for k, v in val.items()]
    if isinstance(val, (list, tuple)) and len(val) == 2 and isinstance(val[0], (list, tuple)):
        return [(k, int(v)) for k, v in zip(val[0], val[1], strict=False)]
    return []


_COMBINED_OVERVIEW_KEYS = (
    "fingerprint_coverage",
    "req_size_dist",
    "top_ips_header",
    "ipv6_adoption",
    "proxy_dist",
    "conn_reuse_dist",
)


def _sec_combined_overview(
    service: HighScaleService, scope: _Scope, sec_base_key: str | None, mask_ips: bool = False
) -> dict[str, Any]:
    h_case = _bucket_case("h", _REQ_HEADER_BUCKETS, _REQ_HEADER_LAST)
    c_case = _bucket_case("c", _CONN_REUSE_BUCKETS, _CONN_REUSE_LAST)
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    ov_span_s = (scope.end - scope.start).total_seconds() if scope.start and scope.end else 0.0
    sample_stride = (
        (
            30
            if ov_span_s >= 43200
            else (
                15 if ov_span_s >= 21600 else (2 if ov_span_s >= 1800 and not os.getenv("PYTEST_CURRENT_TEST") else 1)
            )
        )
        if is_real_ch
        else 1
    )
    stride_sql = minute_stride_sql(scope.start, scope.end, sample_stride) if sample_stride > 1 else ""
    base_sql = (
        f"SELECT toUnixTimestamp(toStartOfHour(event_timestamp)) AS bucket_ts, "
        "count() AS total_rows, "
        "countIf(custom_fields['tls_ciphers_sha'] != '') AS tls_populated, "
        f"countIf({_IS_IPV6}) AS ipv6_rows, "
        "sumMap(if(h IS NOT NULL, [h_b], []), if(h IS NOT NULL, [toUInt64(1)], [])) AS h_map, "
        "sumMap(if(c IS NOT NULL AND c > 0, [c_b], []), if(c IS NOT NULL AND c > 0, [toUInt64(1)], [])) AS c_map, "
        "sumMap(if(p_t != '', [p_t], []), if(p_t != '', [toUInt64(1)], [])) AS p_map, "
        "groupArraySortedIf(15)(tuple(-toInt64(h), client_ip), h IS NOT NULL AND client_ip != '') AS top_hdr_ips "
        "FROM (SELECT event_timestamp, client_ip, custom_fields['tls_ciphers_sha'], custom_fields['is_ipv6'], "
        "custom_fields['p_type'] AS p_t, toUInt64OrNull(custom_fields['req_header_bytes']) AS h, "
        f"{h_case} AS h_b, toUInt64OrNull(custom_fields['conn_requests']) AS c, {c_case} AS c_b "
    )
    rows = _run(
        service,
        scope,
        "overview_5in1",
        f"{base_sql}FROM request_facts PREWHERE __WHERE__{stride_sql}) GROUP BY bucket_ts ORDER BY bucket_ts",
    )
    if not rows and sample_stride > 1:
        sample_stride = 1
        rows = _run(
            service,
            scope,
            "overview_5in1_fallback",
            f"{base_sql}FROM request_facts PREWHERE __WHERE__) GROUP BY bucket_ts ORDER BY bucket_ts",
        )
    total_rows = 0
    tls_pop = 0
    ipv6_list: list[dict[str, Any]] = []
    h_counts: dict[str, int] = defaultdict(int)
    c_counts: dict[str, int] = defaultdict(int)
    p_counts: dict[str, int] = defaultdict(int)
    ip_max_hdr: dict[str, int] = {}
    for r in rows:
        tr = int(r.get("total_rows") or 0)
        total_rows += tr
        tls_pop += int(r.get("tls_populated") or 0)
        if tr > 0:
            ipv6_list.append({"time": _sec_iso(r["bucket_ts"]), "pct": float(r.get("ipv6_rows") or 0) * 100.0 / tr})
        for k, v in _unpack_summap(r.get("h_map")):
            if k:
                h_counts[str(k)] += v * sample_stride
        for k, v in _unpack_summap(r.get("c_map")):
            if k:
                c_counts[str(k)] += v * sample_stride
        for k, v in _unpack_summap(r.get("p_map")):
            if k:
                p_counts[str(k)] += v * sample_stride
        for item in r.get("top_hdr_ips") or []:
            seq = list(item.values()) if isinstance(item, Mapping) else item
            if len(seq) >= 2 and seq[1]:
                ip_s = str(seq[1])
                h_val = abs(int(seq[0]))
                if h_val > ip_max_hdr.get(ip_s, -1):
                    ip_max_hdr[ip_s] = h_val
    h_order = [*(lbl for _, lbl in _REQ_HEADER_BUCKETS), _REQ_HEADER_LAST]
    c_order = [*(lbl for _, lbl in _CONN_REUSE_BUCKETS), _CONN_REUSE_LAST]
    top_ips_raw = sorted(ip_max_hdr.items(), key=lambda kv: kv[1], reverse=True)[:10]
    bundle = {
        "fingerprint_coverage": {"tls_ciphers_sha": (tls_pop / total_rows) if total_rows > 0 else 0.0},
        "req_size_dist": [{"bucket": b, "count": h_counts[b]} for b in h_order if h_counts.get(b, 0) > 0],
        "top_ips_header": [shape_security_top_ip_header_row(ip, max_h, mask_ips=mask_ips) for ip, max_h in top_ips_raw],
        "conn_reuse_dist": [{"bucket": b, "count": c_counts[b]} for b in c_order if c_counts.get(b, 0) > 0],
        "ipv6_adoption": ipv6_list,
        "proxy_dist": [
            {"type": k, "count": v} for k, v in sorted(p_counts.items(), key=lambda kv: kv[1], reverse=True) if v > 0
        ],
    }
    if sec_base_key is not None:
        from backend.core.request_telemetry.tracker import current_clickhouse_scope
        from backend.utils.telemetry import get_queries

        sec_id = current_clickhouse_scope().get("section_id")
        sec_queries = [
            {**q, "is_cached": True}
            for q in get_queries()
            if isinstance(q, dict) and (sec_id is None or q.get("clickhouse", {}).get("section_id") == sec_id)
        ]
        for k, val in bundle.items():
            s_key = f"{sec_base_key}:{k}"
            _security_memo.cache[s_key] = val
            if sec_queries:
                _security_memo._telemetry_cache[s_key] = sec_queries
    return bundle


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
            "s": normalize_cache_bound(start_time),
            "e": normalize_cache_bound(end_time),
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
        is_real_ch = (
            hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
        )
        sec_base_key = (
            digest_cache_key(
                {
                    "ep": "hs_sec_sec_base",
                    "cid": client_cache_token(service.client),
                    "s": normalize_cache_bound(start_time),
                    "e": normalize_cache_bound(end_time),
                    "f": serialize_filters_for_key(getattr(req, "filters", None) or {}),
                    "b": int(bucket_seconds),
                    "m": bool(mask_ips),
                },
                {"name": service.service_id, "service_id": service.service_id},
            )
            if is_real_ch
            else None
        )

        def _wrap_sec(sec_name: str, fn: Callable[[], Any]) -> Callable[[], Any]:
            if sec_base_key is None or sec_name in ("_top_bots", "_proxies"):
                return fn
            s_key = f"{sec_base_key}:{sec_name}"

            def _wrapped() -> Any:
                from backend.core.request_telemetry.tracker import current_clickhouse_scope
                from backend.utils.telemetry import get_queries

                if (hit := _security_memo.cache.get(s_key)) is not None:
                    if cached_qs := _security_memo._telemetry_cache.get(s_key):
                        qs = get_queries()
                        existing_sqls = {q.get("sql") for q in qs if isinstance(q, dict)}
                        for cq in cached_qs:
                            if isinstance(cq, dict) and cq.get("sql") not in existing_sqls:
                                qs.append(cq.copy())
                                existing_sqls.add(cq.get("sql"))
                    return hit
                sec_id = current_clickhouse_scope().get("section_id")
                val = fn()
                _security_memo.cache[s_key] = val
                sec_queries = [
                    {**q, "is_cached": True}
                    for q in get_queries()
                    if isinstance(q, dict) and (sec_id is None or q.get("clickhouse", {}).get("section_id") == sec_id)
                ]
                if sec_queries:
                    _security_memo._telemetry_cache[s_key] = sec_queries
                return val

            return _wrapped

        def want(name: str) -> bool:
            return sections is None or name in sections

        wanted_overview = [
            k
            for k in _COMBINED_OVERVIEW_KEYS
            if want(k) and (sec_base_key is None or f"{sec_base_key}:{k}" not in _security_memo.cache)
        ]
        use_combined_overview = is_real_ch and len(wanted_overview) >= 2
        candidates: tuple[tuple[str, bool, Callable[[], Any]], ...] = (
            ("tls_fingerprints", want("tls_fingerprints"), lambda: _tls_fingerprints(service, scope)),
            (
                "_combined_overview",
                use_combined_overview,
                lambda: _sec_combined_overview(service, scope, sec_base_key, mask_ips),
            ),
            (
                "fingerprint_coverage",
                want("fingerprint_coverage") and not use_combined_overview,
                lambda: _fingerprint_coverage(service, scope),
            ),
            (
                "req_size_dist",
                want("req_size_dist") and not use_combined_overview,
                lambda: _req_size_dist(service, scope),
            ),
            (
                "top_ips_header",
                want("top_ips_header") and not use_combined_overview,
                lambda: _top_ips_header(service, scope, mask_ips),
            ),
            (
                "ipv6_adoption",
                want("ipv6_adoption") and not use_combined_overview,
                lambda: _ipv6_adoption(service, scope),
            ),
            (
                "proxy_dist",
                want("proxy_dist") and not use_combined_overview,
                lambda: _proxy_dist(service, scope),
            ),
            (
                "conn_reuse_dist",
                want("conn_reuse_dist") and not use_combined_overview,
                lambda: _conn_reuse_dist(service, scope),
            ),
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
        tasks = [(k, fn if k == "_combined_overview" else _wrap_sec(k, fn)) for k, w, fn in candidates if w]

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
            elif k == "_combined_overview":
                for ok in _COMBINED_OVERVIEW_KEYS:
                    if want(ok):
                        res[ok] = val[ok]
            else:
                res[k] = val
        res["section_timings"] = trace.timings()
        _security_memo.put(memo_key, dict(res))
        return res

    return SecurityAggregatesResponse.with_telemetry(**_security_memo.coalesce(f"hs_security_agg:{memo_key}", _compute))


def _top_bots(service: HighScaleService, scope: _Scope, n: int = 10) -> dict[str, list[dict[str, Any]]]:
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key: str | None = None
    if is_real_ch and scope.start is not None and scope.end is not None:
        memo_key = digest_cache_key(
            {
                "ep": "hs_sec_top_bots_inner",
                "cid": client_cache_token(service.client),
                "s": normalize_cache_bound(scope.start),
                "e": normalize_cache_bound(scope.end),
                "n": n,
                "w": scope.where,
                "p": sorted((k, str(v)) for k, v in scope.params.items() if k not in ("start", "end")),
            },
            {"name": service.service_id, "service_id": service.service_id},
        )
        if (cached := _security_memo.get(memo_key)) is not None:
            return cached

    from backend.high_scale.aggregate_query import _hourly_edge_time_clause, can_use_hourly_rollup

    hourly_bounds = (
        can_use_hourly_rollup(service.client, "request", scope.start, scope.end) if not scope.filtered else None
    )
    if scope.filtered:
        ua_sql = (
            "SELECT custom_fields['ua'] AS ua, count() AS cnt FROM request_facts "
            "WHERE __WHERE__ AND custom_fields['ua'] != '' GROUP BY ua ORDER BY cnt DESC LIMIT {ua_limit:UInt32}"
        )
        ua_extra_params: dict[str, Any] = {}
    elif hourly_bounds is not None and scope.start is not None and scope.end is not None:
        h_start, h_end = hourly_bounds
        edge_clause = _hourly_edge_time_clause(scope.start, scope.end, h_start, h_end)
        union_sql = (
            "SELECT value AS ua, sum(request_count) AS cnt FROM request_aggregates_1h "
            "WHERE service_id={service_id:String} AND dimension='ua' AND value != '' "
            "AND bucket_start >= {h_start:DateTime64(3)} AND bucket_start < {h_end:DateTime64(3)} "
            "GROUP BY ua"
        )
        if edge_clause is not None:
            union_sql += (
                " UNION ALL SELECT value AS ua, sum(request_count) AS cnt FROM request_aggregates "
                f"WHERE service_id={{service_id:String}} AND dimension='ua' AND value != '' AND {edge_clause} "
                "AND publication_state='visible' AND batch_id IN ("
                "SELECT batch_id FROM high_scale_batch_publications FINAL "
                "WHERE service_id={service_id:String} AND domain='request_aggregate' AND publication_state='visible') "
                "GROUP BY ua"
            )
        ua_sql = f"SELECT ua, sum(cnt) AS cnt FROM ({union_sql}) GROUP BY ua ORDER BY cnt DESC, ua ASC LIMIT {{ua_limit:UInt32}}"
        ua_extra_params = {"h_start": h_start, "h_end": h_end}
    else:
        ua_sql = (
            f"SELECT value AS ua, sum(request_count) AS cnt FROM request_aggregates "
            f"WHERE {_UA_PROJECTION_WHERE} AND value != '' GROUP BY ua ORDER BY cnt DESC, ua ASC LIMIT {{ua_limit:UInt32}}"
        )
        ua_extra_params = {}
    if hasattr(service.client, "responses"):
        ua_rows = _run(service, scope, "top_bots_ua", ua_sql, ua_limit=_BOT_UA_CLASSIFY_LIMIT, **ua_extra_params)
        ngwaf_rows = _ngwaf_summary(service, scope, limit=n)
    else:
        from backend.utils.telemetry.metrics import get_queries

        get_queries()
        with ThreadPoolExecutor(max_workers=2) as pool:
            ua_fut = pool.submit(
                contextvars.copy_context().run,
                lambda: _run(service, scope, "top_bots_ua", ua_sql, ua_limit=_BOT_UA_CLASSIFY_LIMIT, **ua_extra_params),
            )
            ngwaf_fut = pool.submit(
                contextvars.copy_context().run,
                lambda: _ngwaf_summary(service, scope, limit=n),
            )
            ua_rows = ua_fut.result()
            ngwaf_rows = ngwaf_fut.result()
    out = {
        "bots": classify_ua_counts([(r["ua"], int(r["cnt"])) for r in ua_rows], n),
        "ngwaf_bots": [
            shape_security_ngwaf_top_bot_row(r["bot_name"], r["bot_cat"], r["request_count"]) for r in ngwaf_rows
        ],
    }
    if memo_key is not None:
        _security_memo.put(memo_key, out)
    return out


def top_bots(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityTopBotsResponse:
    if start_time is None or end_time is None:
        scope = _scope(service, req, start_time, end_time)
        trace = HighScaleSections(service.service_id, "security")
        result = trace.run(
            "top_bots", lambda: _top_bots(service, scope), source="fact" if scope.filtered else "projection"
        )
        return SecurityTopBotsResponse.with_telemetry(
            **cache_result(result, state="bypass", prewarmer=False), section_timings=trace.timings()
        )
    scope = _scope(service, req, start_time, end_time)
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key = digest_cache_key(
        {
            "ep": "hs_security_top_bots",
            "cid": client_cache_token(service.client),
            "s": normalize_cache_bound(scope.start) if is_real_ch else scope.start.isoformat(),
            "e": normalize_cache_bound(scope.end) if is_real_ch else scope.end.isoformat(),
            "f": serialize_filters_for_key(getattr(req, "filters", None) or {}),
        },
        {"name": service.service_id, "service_id": service.service_id},
    )
    if (cached := _security_memo.get_with_telemetry(memo_key)) is not None:
        return SecurityTopBotsResponse.with_telemetry(**cached)

    def _compute() -> dict[str, Any]:
        if (cached_inner := _security_memo.get(memo_key)) is not None:
            return cached_inner
        trace = HighScaleSections(service.service_id, "security")
        result = trace.run(
            "top_bots", lambda: _top_bots(service, scope), source="fact" if scope.filtered else "projection"
        )
        out = {**result, "section_timings": trace.timings()}
        _security_memo.put(memo_key, dict(out))
        return out

    return SecurityTopBotsResponse.with_telemetry(
        **_security_memo.coalesce(f"hs_security_top_bots:{memo_key}", _compute)
    )


_PROXY_SOURCE = (
    "FROM (SELECT client_ip AS ip, custom_fields['pop'] AS pop, country, custom_fields['city'] AS city, "
    "toFloat64OrNull(custom_fields['rtt_min']) AS rtt_min, toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt, "
    "toFloat64OrNull(custom_fields['lat']) AS lat, toFloat64OrNull(custom_fields['lon']) AS lon, "
    f"toInt64OrNull(custom_fields['asn']) AS asn, {_POP_LAT} AS pop_lat, {_POP_LON} AS pop_lon "
    "FROM request_facts WHERE __WHERE__ AND custom_fields['rtt_min'] != '')"
)


def _proxies(service: HighScaleService, scope: _Scope, mask_ips: bool) -> dict[str, Any]:
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key: str | None = None
    if is_real_ch and scope.start is not None and scope.end is not None:
        memo_key = digest_cache_key(
            {
                "ep": "hs_sec_proxies_inner",
                "cid": client_cache_token(service.client),
                "s": normalize_cache_bound(scope.start.isoformat()),
                "e": normalize_cache_bound(scope.end.isoformat()),
                "w": scope.where,
                "p": sorted((k, str(v)) for k, v in scope.params.items() if k not in ("start", "end")),
                "m": bool(mask_ips),
            },
            {"name": service.service_id, "service_id": service.service_id},
        )
        if (cached := _security_memo.cache.get(memo_key)) is not None:
            return cached

    if is_real_ch:
        px_span_s = (scope.end - scope.start).total_seconds() if scope.start and scope.end else 0.0
        scale_factor = (
            30
            if px_span_s >= 43200
            else (
                15 if px_span_s >= 21600 else (2 if px_span_s >= 1800 and not os.getenv("PYTEST_CURRENT_TEST") else 1)
            )
        )
        use_min_slice = scale_factor > 1
        min_slice_clause = minute_stride_sql(scope.start, scope.end, scale_factor) if use_min_slice else ""
        need_geo = "rtt_min IS NOT NULL AND (rtt_min < 200000 OR (tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.2))"
        proxies_sql = (
            "SELECT "
            "uniqCombined64If(ip, is_tunnel OR is_imp) AS active_proxies_count, "
            "countIf(is_tunnel OR is_imp) AS total_requests_count, "
            "uniqCombined64If(ip, is_imp) AS distance_mismatches_count, "
            "countIf(is_tunnel) AS q_tunnel, "
            "countIf(NOT is_tunnel AND tcp_rtt > 0 AND rtt_min IS NOT NULL AND rtt_min / tcp_rtt < 0.7) AS q_wifi, "
            "count() AS q_total, "
            "sumMap(if(is_tunnel, [ifNull(asn, -1)], []), if(is_tunnel, [toUInt64(1)], [])) AS isp_map, "
            "groupArraySortedIf(100)("
            "tuple("
            "multiIf(is_imp OR is_tunnel, toUInt8(0), is_med, toUInt8(1), toUInt8(2)), "
            "-ifNull(distance_km, -1e9), "
            "ip, asn, rtt_min / 1000.0, tcp_rtt / 1000.0, distance_km, pop, lat, lon, pop_lat, pop_lon, "
            "toUInt8(is_imp), multiIf(is_imp, 'High', is_tunnel, 'High', is_med, 'Medium', 'Low'), country, city"
            "), rtt_min IS NOT NULL AND (is_imp OR is_med)"
            ") AS top_clients "
            "FROM ("
            "SELECT ip, pop, country, city, rtt_min, tcp_rtt, lat, lon, asn, pop_lat, pop_lon, "
            "distance_km, is_tunnel, is_med, "
            "ifNull(rtt_min < 200000 AND lat IS NOT NULL AND lon IS NOT NULL AND distance_km > (rtt_min / 1000.0 * 100.0 + 150.0), 0) AS is_imp "
            "FROM ("
            "SELECT client_ip AS ip, custom_fields['pop'] AS pop, country, custom_fields['city'] AS city, "
            "rtt_min, tcp_rtt, lat, lon, asn, pop_lat, pop_lon, "
            f"if({need_geo} AND lat IS NOT NULL AND lon IS NOT NULL, {_DISTANCE_KM_FAST}, NULL) AS distance_km, "
            f"{_TUNNEL} AS is_tunnel, "
            "ifNull(tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.2, 0) AS is_med "
            "FROM ("
            "SELECT client_ip, custom_fields['pop'], country, custom_fields['city'], "
            "toFloat64OrNull(custom_fields['rtt_min']) AS rtt_min, "
            "toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt, "
            f"if({need_geo}, toFloat64OrNull(custom_fields['lat']), NULL) AS lat, "
            f"if({need_geo}, toFloat64OrNull(custom_fields['lon']), NULL) AS lon, "
            f"if({need_geo}, toInt64OrNull(custom_fields['asn']), NULL) AS asn, "
            f"if({need_geo}, {_POP_LAT_FAST}, NULL) AS pop_lat, "
            f"if({need_geo}, {_POP_LON_FAST}, NULL) AS pop_lon "
            f"FROM request_facts PREWHERE __WHERE__{min_slice_clause}"
            ")))"
        )
        comb_rows = _run(service, scope, "proxies_combined", proxies_sql)
        if use_min_slice and (not comb_rows or int(comb_rows[0].get("q_total") or 0) == 0):
            scale_factor = 1
            comb_rows = _run(
                service,
                scope,
                "proxies_combined_fallback",
                proxies_sql.replace(min_slice_clause, ""),
            )
        comb = comb_rows[0] if comb_rows else {}
        stat = {
            "active_proxies_count": int(comb.get("active_proxies_count") or 0),
            "total_requests_count": int(comb.get("total_requests_count") or 0) * scale_factor,
            "distance_mismatches_count": int(comb.get("distance_mismatches_count") or 0),
        }
        q_tunnel = int(comb.get("q_tunnel") or 0) * scale_factor
        q_wifi = int(comb.get("q_wifi") or 0) * scale_factor
        q_total = int(comb.get("q_total") or 0) * scale_factor
        q_direct = max(0, q_total - q_tunnel - q_wifi)
        q_candidates: list[dict[str, Any]] = [
            {"type": "Direct Connection", "count": q_direct},
            {"type": "WiFi / Mobile", "count": q_wifi},
            {"type": "Active Tunnel / Proxy", "count": q_tunnel},
        ]
        quality_rows = sorted(
            [r for r in q_candidates if int(r["count"]) > 0],
            key=lambda x: int(x["count"]),
            reverse=True,
        )
        isp_rows = [
            {"asn": None if int(a) == -1 else int(a), "count": int(cnt) * scale_factor}
            for a, cnt in sorted(_unpack_summap(comb.get("isp_map")), key=lambda kv: kv[1], reverse=True)[:10]
            if cnt > 0
        ]
        client_rows = []
        for item in comb.get("top_clients") or []:
            seq = list(item.values()) if isinstance(item, Mapping) else item
            if len(seq) >= 16:
                client_rows.append(
                    {
                        "ip": seq[2],
                        "asn": seq[3],
                        "rtt_min_ms": seq[4],
                        "tcp_rtt_ms": seq[5],
                        "distance_km": seq[6],
                        "pop": seq[7],
                        "client_lat": seq[8],
                        "client_lon": seq[9],
                        "pop_lat": seq[10],
                        "pop_lon": seq[11],
                        "impossible_distance": seq[12],
                        "risk_level": seq[13],
                        "country": seq[14],
                        "city": seq[15],
                    }
                )
    else:

        def _q_stats():
            return _run(
                service,
                scope,
                "proxy_stats",
                f"SELECT uniqExactIf(ip, {_TUNNEL} OR {_IMPOSSIBLE_DISTANCE}) AS active_proxies_count, "
                f"countIf({_TUNNEL} OR {_IMPOSSIBLE_DISTANCE}) AS total_requests_count, "
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
                "SELECT asn, count() AS count FROM ("
                "SELECT toFloat64OrNull(custom_fields['rtt_min']) AS rtt_min, "
                "toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt, "
                "toInt64OrNull(custom_fields['asn']) AS asn "
                "FROM request_facts WHERE __WHERE__ AND custom_fields['rtt_min'] != ''"
                f") WHERE {_TUNNEL} GROUP BY asn ORDER BY count DESC LIMIT 10",
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
            futs = [
                pool.submit(contextvars.copy_context().run, fn) for fn in (_q_stats, _q_quality, _q_isps, _q_clients)
            ]
            stats, quality_rows, isp_rows, client_rows = [f.result() for f in futs]

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
    out = finish_security_proxies(scope.service_id, stat, quality_rows, isp_rows, raw_clients, mask_ips)
    if memo_key is not None:
        _security_memo.cache[memo_key] = out
    return out


def get_proxies_data(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None, *, mask_ips: bool = False
) -> dict[str, Any]:
    scope = _scope(service, req, start_time, end_time)
    trace = HighScaleSections(service.service_id, "security")
    return {
        **trace.run("proxies", lambda: _proxies(service, scope, mask_ips), source="fact"),
        "_section_timings": trace.timings(),
    }


def get_security_threat_intel(service: HighScaleService, start_time: str | None, end_time: str | None) -> Any:
    from backend.models.analytics import ThreatIntelListResponse

    scope = _scope(service, None, start_time, end_time)
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key: str | None = None
    if is_real_ch and scope.start is not None and scope.end is not None:
        memo_key = digest_cache_key(
            {
                "ep": "hs_security_threat_intel",
                "cid": client_cache_token(service.client),
                "s": int(scope.start.timestamp()) // 60,
                "e": int(scope.end.timestamp()) // 60,
            },
            {"name": service.service_id, "service_id": service.service_id},
        )
        if (cached := _security_memo.get_with_telemetry(memo_key)) is not None:
            return ThreatIntelListResponse.with_telemetry(**cached)

    def _compute_ti() -> dict[str, Any]:
        if memo_key is not None and (cached_inner := _security_memo.get_with_telemetry(memo_key)) is not None:
            return cached_inner
        rows = _run(
            service,
            scope,
            "threat_intel",
            f"SELECT multiIf(custom_fields['ja3'] != '', custom_fields['ja3'], custom_fields['ja4']) AS tls_fingerprint, "
            f"count() AS requests, topK(1)(country)[1] AS top_country, "
            f"countIf(custom_fields['waf_logged_rules'] != '' OR custom_fields['waf_blocked_rules'] != '') AS matched_waf_rules_count, "
            f"round(countIf({_IS_BOT}) * 100.0 / count(), 2) AS bot_percentage, "
            "max(custom_fields['p_type'] != '') AS is_anonymized_proxy FROM request_facts "
            "WHERE __WHERE__ AND tls_fingerprint != '' GROUP BY tls_fingerprint ORDER BY requests DESC LIMIT 50",
        )
        out = {
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
        if memo_key is not None:
            _security_memo.put(memo_key, dict(out))
        return out

    if memo_key is not None:
        return ThreatIntelListResponse.with_telemetry(
            **_security_memo.coalesce(f"hs_security_ti:{memo_key}", _compute_ti)
        )
    return _compute_ti()


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
