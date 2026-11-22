"""ClickHouse SQL templates, column mappings, and detection builders for high-scale insights."""

from __future__ import annotations

import re
from dataclasses import dataclass

from backend.high_scale._common import visible_fact_clauses
from backend.repositories.insights.shaping import fingerprint_column as _fingerprint_column

# Logical column name -> ClickHouse expression over request_facts.
COLUMNS: dict[str, str] = {
    "timestamp": "event_timestamp",
    "url": "url",
    "country": "country",
    "ip": "client_ip",
    "status": "toInt32OrNull(custom_fields['status'])",
    "ost": "toInt32OrNull(custom_fields['ost'])",
    "cache": "custom_fields['cache']",
    "elapsed": "toFloat64OrNull(custom_fields['elapsed'])",
    "ottfb": "toFloat64OrNull(custom_fields['ottfb'])",
    "oconnect_ms": "toFloat64OrNull(custom_fields['oconnect_ms'])",
    "oretries": "toInt32OrNull(custom_fields['oretries'])",
    "oip": "custom_fields['oip']",
    "ua": "custom_fields['ua']",
    "referer": "custom_fields['referer']",
    "method": "custom_fields['method']",
    "p_type": "custom_fields['p_type']",
    "c_type": "custom_fields['c_type']",
    "c_speed": "custom_fields['c_speed']",
    "transport": "custom_fields['transport']",
    "pop": "custom_fields['pop']",
    "server_region": "custom_fields['server_region']",
    "ja3": "custom_fields['ja3']",
    "ja4": "custom_fields['ja4']",
    "tls_ciphers_sha": "custom_fields['tls_ciphers_sha']",
    "cookie_session": "custom_fields['cookie_session']",
    "resp_header_content_encoding": "custom_fields['resp_header_content_encoding']",
    "asn": "toInt64OrNull(custom_fields['asn'])",
    "metro": "custom_fields['metro']",
    "city": "custom_fields['city']",
    "region": "custom_fields['region']",
    "delivery_rate": "toFloat64OrNull(custom_fields['delivery_rate'])",
    "resp_bytes": "toInt64OrNull(custom_fields['resp_bytes'])",
    "req_header_bytes": "toInt64OrNull(custom_fields['req_header_bytes'])",
    "conn_requests": "toInt64OrNull(custom_fields['conn_requests'])",
    "sec": "toInt64(toUnixTimestamp(event_timestamp))",
    "rid": "custom_fields['rid']",
    "prid": "custom_fields['prid']",
    "edge": "custom_fields['edge']",
    "ploss": "toFloat64OrNull(custom_fields['ploss'])",
    "rtt_var": "toFloat64OrNull(custom_fields['rtt_var'])",
    "retrans": "toFloat64OrNull(custom_fields['retrans'])",
    "ttl": "toFloat64OrNull(custom_fields['ttl'])",
    "hits": "toFloat64OrNull(custom_fields['hits'])",
    "age": "toFloat64OrNull(custom_fields['age'])",
    "lat": "toFloat64OrNull(custom_fields['lat'])",
    "lon": "toFloat64OrNull(custom_fields['lon'])",
    "tcp_rtt": "toFloat64OrNull(custom_fields['tcp_rtt'])",
    "chrome_version": r"extract(custom_fields['ua'], 'Chrome/([0-9]+)\\.')",
    "signal": (
        "arrayJoin(arrayFilter(s -> s != '' AND s != 'BOT-ANALYSIS', "
        "arrayMap(s -> trim(BOTH ' ' FROM s), splitByChar(',', custom_fields['waf_sig']))))"
    ),
}

_HIT = "startsWith(cache, 'HIT')"
_CACHEABLE = "(startsWith(cache, 'HIT') OR startsWith(cache, 'MISS'))"
_P95 = "quantileTDigestIf(0.95)"


@dataclass(frozen=True)
class Detection:
    """One detection: the base columns it reads, a row-level prefilter, and the SQL after the ``base`` CTE."""

    columns: tuple[str, ...]
    sql: str
    row_filter: str = ""
    window_only: bool = False
    # Must name only real request_facts columns and never the moving time bounds: ClickHouse's
    # query condition cache keys on this exact text, so later evaluations skip non-matching granules.
    prewhere: str = ""
    needs_baseline: bool = True


@dataclass(frozen=True)
class _BaseGroupQuerySpec:
    """Declarative ClickHouse GROUP BY query descriptor over the ``base`` CTE."""

    select_cols: str
    group_by: str
    where: str = ""
    having: str = ""
    order_by: str = ""
    limit: int | None = None
    from_table: str = "base"

    def build_sql(self) -> str:
        where_clause = f" WHERE {self.where}" if self.where else ""
        having_clause = f"\n    HAVING {self.having}" if self.having else ""
        order_clause = f"\n    ORDER BY {self.order_by}" if self.order_by else ""
        limit_clause = f" LIMIT {self.limit}" if self.limit is not None else ""
        return (
            f"\n    SELECT {self.select_cols}\n"
            f"    FROM {self.from_table}{where_clause} GROUP BY {self.group_by}"
            f"{having_clause}{order_clause}{limit_clause}\n    "
        )


def _base_group_sql(
    select_cols: str,
    group_by: str,
    *,
    where: str = "",
    having: str = "",
    order_by: str = "",
    limit: int | None = 15,
    from_table: str = "base",
) -> str:
    return _BaseGroupQuerySpec(select_cols, group_by, where, having, order_by, limit, from_table).build_sql()


def _share_spike(
    column: str,
    share_floor: str,
    *,
    extra_row: str = "",
    min_w_cnt: int = 0,
    multiplier: int = 3,
    delta: str = "0.10",
    limit: int = 10,
    w_first: bool = False,
) -> str:
    """Window traffic share of one value that spiked versus its baseline share."""
    sel_cols = ", ".join(f"g.{c.strip()}" for c in column.split(","))
    totals_cols = "g.w_total, g.b_total" if w_first else "g.b_total, g.w_total"
    cnt_clause = f" AND g.w_cnt >= {min_w_cnt}" if min_w_cnt else ""
    extra_pred = f" AND ({extra_row.removeprefix('WHERE ').strip()})" if extra_row.strip() else ""
    return f""" , grp AS (SELECT {column}, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,
        sum(countIf(is_w)) OVER () AS w_total, sum(countIf(is_b)) OVER () AS b_total FROM base GROUP BY {column})
    SELECT {sel_cols}, g.w_cnt, g.b_cnt, {totals_cols} FROM grp g
    WHERE g.w_total > 0{extra_pred}{cnt_clause} AND g.w_cnt * 1.0 / g.w_total >= {share_floor}
      AND (g.b_total = 0 OR g.w_cnt * 1.0 / g.w_total >= g.b_cnt * 1.0 / nullIf(g.b_total, 0) * {multiplier} + {delta})
    ORDER BY g.w_cnt DESC LIMIT {limit} """


def _p95_regression(
    select_col: str,
    *,
    group_by: str | None = None,
    min_w: int = 20,
    min_b: int = 50,
    ratio: float = 1.5,
    min_delta_ms: int = 100,
    extra_select: str = "",
) -> str:
    grp = group_by or select_col
    key_expr = f"({grp})" if "," in grp else grp
    return _base_group_sql(
        f"{select_col}, {_P95}(elapsed, is_w) / 1000.0 AS w_p95, {_P95}(elapsed, is_b) / 1000.0 AS b_p95, "
        f"countIf(is_w) AS w_total, countIf(is_b) AS b_total{extra_select}",
        grp,
        where=f"{key_expr} IN (SELECT {grp} FROM base WHERE is_w GROUP BY {grp} HAVING count() >= {min_w})",
        having=f"w_total >= {min_w} AND b_total >= {min_b} AND w_p95 >= b_p95 * {ratio} AND w_p95 - b_p95 >= {min_delta_ms}",
        order_by="(w_p95 / nullIf(b_p95, 0)) DESC",
        limit=15,
    )


def _new_traffic(select_col: str, min_w: int, *, where: str = "", group_by: str | None = None) -> str:
    return _base_group_sql(
        f"{select_col}, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt",
        group_by or select_col,
        where=where,
        having=f"w_cnt >= {min_w} AND b_cnt = 0",
        order_by="w_cnt DESC",
        limit=20,
    )


def _error_spike(
    select_col: str,
    *,
    group_by: str | None = None,
    status_min: int = 500,
    min_w: int = 3,
    min_rate: str = "0.05",
    min_b: int = 10,
    multiplier: int = 2,
) -> str:
    grp = group_by or select_col
    key_expr = f"({grp})" if "," in grp else grp
    return _base_group_sql(
        f"{select_col}, countIf(is_w AND status >= {status_min}) * 1.0 / nullIf(countIf(is_w), 0) AS w_rate, "
        f"countIf(is_b AND status >= {status_min}) * 1.0 / nullIf(countIf(is_b), 0) AS b_rate, "
        f"countIf(is_w AND status >= {status_min}) AS w_errors, countIf(is_w) AS w_total, countIf(is_b) AS b_total",
        grp,
        where=(
            f"{key_expr} IN (SELECT {grp} FROM base WHERE is_w "
            f"GROUP BY {grp} HAVING count() >= {min_w} AND countIf(status >= {status_min}) >= 1)"
        ),
        having=f"w_total >= {min_w} AND w_rate >= {min_rate} AND (b_total < {min_b} OR w_rate >= b_rate * {multiplier} + 0.05)",
        order_by="(w_rate - coalesce(b_rate, 0)) DESC",
        limit=15,
    )


def _rate_regression(
    cond: str,
    *,
    denom_cond: str = "",
    having: str = "w_total >= 10 AND b_total >= 50 AND b_rate <= 0.20 AND w_rate >= 0.50 AND w_rate >= b_rate + 0.30",
    order_by: str = "(w_rate - coalesce(b_rate, 0)) DESC",
    from_table: str = "base",
) -> str:
    w_denom = f"is_w AND {denom_cond}" if denom_cond else "is_w"
    b_denom = f"is_b AND {denom_cond}" if denom_cond else "is_b"
    return _base_group_sql(
        f"url, countIf(is_w AND {cond}) * 1.0 / nullIf(countIf({w_denom}), 0) AS w_rate, "
        f"countIf(is_b AND {cond}) * 1.0 / nullIf(countIf({b_denom}), 0) AS b_rate, "
        f"countIf({w_denom}) AS w_total, countIf({b_denom}) AS b_total",
        "url",
        where="url IN (SELECT url FROM base WHERE is_w GROUP BY url HAVING count() >= 5)"
        if from_table == "base"
        else "",
        having=having,
        order_by=order_by,
        limit=15,
        from_table=from_table,
    )


def _fingerprint_spread(column: str, *, with_ratio: bool) -> str:
    """Distinct client IPs per fingerprint in the window versus the baseline's hourly rate."""
    ratio = (
        ", w_ips * 1.0 / greatest(b_ips / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64}, 1) AS ip_ratio"
        if with_ratio
        else ""
    )
    return f""" , win AS (SELECT {column}, uniqExactIf(ip, is_w) AS w_ips, countIf(is_w) AS w_reqs, uniqExactIf(ip, is_b) AS b_ips
        FROM base WHERE {column} != '' AND {column} IN (SELECT {column} FROM base WHERE is_w AND {column} != '' GROUP BY {column} HAVING uniqExact(ip) >= 5)
        GROUP BY {column} HAVING w_ips >= 5)
    SELECT {column}, w_ips, w_reqs, b_ips{ratio} FROM win
    WHERE w_ips > b_ips / greatest({{baseline_hours:Float64}}, 1.0) * {{window_hours:Float64}} * 3
    ORDER BY w_ips * 1.0 / greatest(b_ips / greatest({{baseline_hours:Float64}}, 1.0) * {{window_hours:Float64}}, 1) DESC LIMIT 10 """


def _per_ip_outlier(column: str, window_floor: int, extra_having: str, order: str, aliases: tuple[str, str]) -> str:
    """Per-IP window maximum against that IP's own baseline p95 (request size, connection reuse)."""
    peak, mean = aliases
    return f""" , agg AS (SELECT ip, maxIf({column}, is_w) AS {peak}, avgIf({column}, is_w) AS {mean}, countIf(is_w) AS w_total,
            quantileExactInclusiveIf(0.95)({column}, is_b) AS b_p95, countIf(is_b) AS b_total
        FROM base WHERE ip IN (SELECT ip FROM base WHERE is_w GROUP BY ip HAVING count() >= {window_floor})
        GROUP BY ip HAVING w_total >= {window_floor} AND b_total > 0{extra_having})
    SELECT w.ip, w.{peak}, w.{mean}, w.w_total, w.b_p95 FROM agg w
    WHERE w.w_total >= {window_floor} AND w.{peak} > w.b_p95 * 3{order} """


_CITY_LABEL = "concat(city, ', ', region, ', ', country) AS label, city, region, country"


def _beaconing(entity: str, diversity: str) -> str:
    """Regular-cadence (beaconing) detection per ``entity``: gap statistics over distinct active seconds."""
    return f""" , meta AS (SELECT {entity}, {diversity} AS diversity, count() AS n_events, (max(sec) - min(sec)) AS span_s,
            arraySlice(arrayDifference(arraySort(groupUniqArray(sec))), 2) AS gaps_arr
        FROM base GROUP BY {entity} HAVING n_events >= 13 AND length(gaps_arr) >= 12 AND (n_events * 1.0 / nullIf(span_s, 0)) < 2.0),
    agg AS (SELECT {entity}, diversity, n_events, span_s,
            length(gaps_arr) AS n_gaps, arrayAvg(gaps_arr) AS mean_gap,
            arrayReduce('varSamp', gaps_arr) AS var_gap, arrayReduce('stddevSamp', gaps_arr) AS sd_gap,
            arrayDistinct(gaps_arr) AS u_gaps,
            arrayReduce('argMax', u_gaps, arrayMap(x -> tuple(countEqual(gaps_arr, x), -x), u_gaps)) AS mode_gap,
            arrayAvg(x -> if(abs(x - mode_gap) <= 1, 1.0, 0.0), gaps_arr) AS modal_frac
        FROM meta)
    SELECT a.{entity}, a.n_gaps, a.n_events, round(a.mean_gap, 2) AS avg_interval, round(a.sd_gap, 2) AS stddev_interval,
           round(sqrt(greatest(a.var_gap - 1.0 / 12.0, 0)) / nullIf(a.mean_gap, 0), 3) AS cv_corr,
           round(a.modal_frac, 3) AS modal_frac, a.diversity, a.span_s, a.mode_gap
    FROM agg a
    WHERE a.n_gaps >= 12 AND (a.n_events * 1.0 / nullIf(a.span_s, 0)) < 2.0
      AND ((a.mean_gap >= 5 AND sqrt(greatest(a.var_gap - 1.0 / 12.0, 0)) / nullIf(a.mean_gap, 0) < 0.3 AND a.modal_frac >= 0.6)
        OR (a.mean_gap >= 1 AND a.mean_gap < 5 AND a.modal_frac >= 0.85))
    ORDER BY a.modal_frac DESC, a.n_gaps DESC LIMIT 15 """


def repeated_patterns_fp(fingerprint_column: str) -> Detection:
    """Beaconing keyed on the TLS fingerprint (catches IP-rotating scripts that keep one TLS stack)."""
    if fingerprint_column not in ("ja4", "ja3"):
        raise ValueError("fingerprint column must be ja4 or ja3")
    return Detection(
        ("sec", "ip", "ua", fingerprint_column),
        _beaconing(fingerprint_column, "uniqExact(ip)"),
        row_filter=f"{fingerprint_column} != '' AND NOT (ua != '' AND match(ua, {{bot_ua_regex:String}}))",
        window_only=True,
    )


POP_CODE = re.compile(r"[A-Za-z0-9]{2,8}")


def impossible_distance(
    pop_coordinates: dict[str, tuple[float, float]], fingerprint_column: str, edge_only: bool
) -> Detection:
    """Fingerprints whose client sits farther from the serving POP than the measured RTT physically allows."""
    if fingerprint_column not in ("ja4", "ja3"):
        raise ValueError("fingerprint column must be ja4 or ja3")
    pops = []
    for code, (lat, lon) in pop_coordinates.items():
        if not POP_CODE.fullmatch(code):
            raise ValueError("invalid POP code")
        pops.append(f"('{code}', {float(lat)!r}, {float(lon)!r})")
    edge = " AND lower(edge) IN ('1', 'true')" if edge_only else ""
    columns: tuple[str, ...] = ("ip", "pop", "lat", "lon", "tcp_rtt", "country", "city", fingerprint_column)
    if edge_only:
        columns += ("edge",)
    sql = f""" , pop_coords AS (SELECT t.1 AS pop_code, t.2 AS pop_lat, t.3 AS pop_lon FROM (SELECT arrayJoin([{", ".join(pops)}]) AS t)),
    flagged AS (SELECT {fingerprint_column} AS fp, b.ip AS ip, b.pop AS pop, round(b.lat, 3) AS client_lat, round(b.lon, 3) AS client_lon,
               c.pop_lat AS pop_lat, c.pop_lon AS pop_lon, b.tcp_rtt AS tcp_rtt, b.country AS country, b.city AS city,
               round(2 * 6371 * asin(sqrt(pow(sin(radians(b.lat - c.pop_lat) / 2), 2)
                   + cos(radians(b.lat)) * cos(radians(c.pop_lat)) * pow(sin(radians(b.lon - c.pop_lon) / 2), 2))), 1) AS distance_km,
               round((b.tcp_rtt / 2.0 / 1e6) * 200000 * 2, 1) AS max_km
        FROM base b INNER JOIN pop_coords c ON b.pop = c.pop_code)
    SELECT fp, count() AS hits, max(distance_km - max_km) AS worst_excess_km, max(distance_km) AS max_dist_km,
           min(max_km) AS min_allowed_km, any(pop) AS pop, any(ip) AS sample_ip, any(client_lat), any(client_lon),
           any(pop_lat), any(pop_lon), any(tcp_rtt), any(country), any(city)
    FROM flagged WHERE distance_km > max_km GROUP BY fp HAVING hits >= 2 ORDER BY worst_excess_km DESC LIMIT 15 """
    return Detection(
        columns,
        sql,
        row_filter=f"lat IS NOT NULL AND lon IS NOT NULL AND tcp_rtt IS NOT NULL AND tcp_rtt > 0 AND {fingerprint_column} != ''{edge}",
        window_only=True,
        needs_baseline=False,
    )


_CRAWLER_TOKEN = "'([a-z0-9_.-]*(?:bot|crawler|spider|slurp|archiver|scraper|externalagent|extended|other|preview)[a-z0-9_.-]*|headless[a-z0-9_.-]*|python-[a-z0-9_.-]+|go-http-client|curl|wget|scrapy|aiohttp|httpx|okhttp|axios)'"
_BOT_STEM_SUFFIX = "'(-searchbot|-assistbot|-externalagent|-extended|-user|searchbot|assistbot|externalagent|extended|crawler|spider|slurp|archiver|scraper|preview|other|bot).*$'"
_BOT_STEM = f"if(replaceRegexpOne(raw_tok, {_BOT_STEM_SUFFIX}, '') != '', replaceRegexpOne(raw_tok, {_BOT_STEM_SUFFIX}, ''), raw_tok)"
# crawler_traffic is non-empty exactly when a bot stem has 50+ window requests; deciding that
# from window rows alone avoids re-running crawler_traffic's full-range scan for the guard.
_CRAWLER_IN_WINDOW = (
    f"(SELECT count() FROM (SELECT {_BOT_STEM} AS bot_stem FROM (SELECT extract(lower(ua), {_CRAWLER_TOKEN}) AS raw_tok "
    "FROM base WHERE is_w) WHERE raw_tok != '' AND bot_stem != '' GROUP BY bot_stem HAVING count() >= 50)) > 0"
)


DETECTIONS: dict[str, Detection] = {
    "error_spikes": Detection(("url", "status"), _error_spike("url")),
    "new_country_traffic": Detection(("country",), _new_traffic("country", 3, where="country != ''")),
    "ua_monoculture": Detection(("ua",), _share_spike("ua", "0.25")),
    "new_probe_urls": Detection(
        ("url", "status"),
        _base_group_sql(
            "url, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt, avgIf(if(status >= 400, 1.0, 0.0), is_w) * 100 AS w_error_pct",
            "url",
            where="url != ''",
            having="w_cnt > 0 AND b_cnt = 0",
            order_by="w_cnt DESC",
            limit=25,
        ),
        prewhere="match(url, {probe_regex:String})",
    ),
    "proxy_surge": Detection(
        ("p_type",),
        """ , agg AS (SELECT p_type, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,
            sumIf(countIf(is_w), p_type != '') OVER () AS w_proxy_total,
            sumIf(countIf(is_b), p_type != '') OVER () AS b_proxy_total,
            sum(countIf(is_w)) OVER () AS w_total_all,
            sum(countIf(is_b)) OVER () AS b_total_all FROM base GROUP BY p_type)
    SELECT a.p_type, a.w_cnt, a.b_cnt, a.w_total_all, a.b_total_all FROM agg a
    WHERE a.p_type != '' AND (a.w_proxy_total * 100.0 / nullIf(a.w_total_all, 0)) >= 5
      AND (a.w_proxy_total * 100.0 / nullIf(a.w_total_all, 0)) >= (a.b_proxy_total * 100.0 / nullIf(a.b_total_all, 0)) * 2 + 5 """,
    ),
    "asn_concentration": Detection(("asn",), _share_spike("asn", "0.20", extra_row="WHERE asn IS NOT NULL")),
    "cache_collapse": Detection(
        ("url", "cache"),
        _rate_regression(
            _HIT,
            denom_cond=_CACHEABLE,
            having="w_total >= 5 AND b_total >= 20 AND b_rate >= 0.40 AND w_rate <= b_rate - 0.20 AND w_rate <= b_rate * 0.6",
            order_by="(coalesce(b_rate, 0) - w_rate) DESC",
        ),
    ),
    "cacheability_regression": Detection(("url", "cache"), _rate_regression("startsWith(cache, 'PASS')")),
    "latency_regression": Detection(
        ("url", "elapsed"),
        _p95_regression("url", min_w=5, min_b=20, ratio=2.0, min_delta_ms=200),
        row_filter="elapsed IS NOT NULL",
    ),
    "tail_latency": Detection(
        ("url", "elapsed"),
        _base_group_sql(
            "url, round(quantileTDigest(0.99)(elapsed) / 1000.0, 0) AS p99_ms, round(quantileTDigest(0.50)(elapsed) / 1000.0, 0) AS p50_ms, "
            "round(quantileTDigest(0.99)(elapsed) / nullIf(quantileTDigest(0.50)(elapsed), 0), 1) AS ratio, count() AS total",
            "url",
            having="total >= 20 AND ratio > 5",
            order_by="ratio DESC",
            limit=15,
        ),
        row_filter="elapsed IS NOT NULL",
        window_only=True,
    ),
    "cipher_spread": Detection(("ip", "tls_ciphers_sha"), _fingerprint_spread("tls_ciphers_sha", with_ratio=False)),
    "request_size_anomaly": Detection(
        ("ip", "req_header_bytes"),
        _per_ip_outlier("req_header_bytes", 3, "", " ORDER BY w.max_bytes DESC LIMIT 15", ("max_bytes", "avg_bytes")),
        row_filter="req_header_bytes > 0",
    ),
    "connection_abuse": Detection(
        ("ip", "conn_requests"),
        _per_ip_outlier(
            "conn_requests",
            5,
            " AND max_reqs >= 50",
            " AND w.max_reqs >= 50 ORDER BY w.max_reqs DESC LIMIT 15",
            ("max_reqs", "avg_reqs"),
        ),
        row_filter="conn_requests > 0",
    ),
    "region_latency": Detection(
        ("server_region", "elapsed", "ottfb"),
        _p95_regression(
            "server_region",
            extra_select=", if(countIf(is_w AND ottfb IS NOT NULL) >= 20, round(quantileTDigestIf(0.95)(ottfb, is_w AND ottfb IS NOT NULL) / 1000.0, 0), NULL) AS ottfb_p95",
        ),
        row_filter="elapsed IS NOT NULL AND server_region != ''",
    ),
    "origin_latency_spike": Detection(
        ("ottfb", "url"),
        f""" , overall AS (SELECT {_P95}(ottfb, is_w) / 1000.0 AS w_p95, {_P95}(ottfb, is_b) / 1000.0 AS b_p95 FROM base),
    url_stats AS (SELECT url, count() AS requests, round(quantileTDigest(0.95)(ottfb) / 1000.0, 1) AS p95_ms FROM base WHERE is_w GROUP BY url HAVING requests >= 10)
    SELECT u.url, u.p95_ms, o.w_p95, o.b_p95, u.requests FROM url_stats u CROSS JOIN overall o WHERE o.w_p95 > o.b_p95 * 2 ORDER BY u.p95_ms DESC LIMIT 10 """,
        row_filter="ottfb IS NOT NULL",
    ),
    "origin_error_rate": Detection(
        ("ost",),
        """ , by_status AS (SELECT ost AS status, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,
            sum(countIf(is_w)) OVER () AS w_total, sum(countIf(is_b)) OVER () AS b_total,
            sumIf(countIf(is_w), ost >= 500) OVER () AS w_5xx, sumIf(countIf(is_b), ost >= 500) OVER () AS b_5xx
            FROM base GROUP BY ost)
    SELECT s.status, s.w_cnt, s.w_total, s.b_total, s.w_5xx, s.b_5xx FROM by_status s
    WHERE s.status >= 500 AND s.w_cnt > 0
      AND (s.w_5xx * 100.0 / nullIf(s.w_total, 0)) >= 1.0 AND (s.w_5xx * 100.0 / nullIf(s.w_total, 0)) > (s.b_5xx * 100.0 / nullIf(s.b_total, 0)) * 2 """,
        row_filter="ost IS NOT NULL",
    ),
    "origin_retries": Detection(
        ("url", "oretries"),
        _base_group_sql(
            "url, count() AS requests, round(avg(oretries), 2) AS avg_retries, max(oretries) AS max_retries",
            "url",
            having="requests >= 5",
            order_by="avg_retries DESC",
            limit=10,
        ),
        row_filter="oretries > 0",
        window_only=True,
    ),
    "origin_ip_failure": Detection(
        ("oip", "ost"),
        """ , stats AS (SELECT oip, count() AS requests, round(countIf(ost >= 500) * 100.0 / nullIf(count(), 0), 1) AS error_pct,
            quantileExactInclusive(0.5)(round(countIf(ost >= 500) * 100.0 / nullIf(count(), 0), 1)) OVER () AS median_rate
            FROM base GROUP BY oip HAVING requests >= 10)
    SELECT s.oip, s.requests, s.error_pct, s.median_rate FROM stats s WHERE s.error_pct > s.median_rate * 3 AND s.error_pct > 5 ORDER BY s.error_pct DESC """,
        row_filter="oip != '' AND ost IS NOT NULL",
        window_only=True,
    ),
    "content_discovery": Detection(
        ("ip", "url", "status"),
        _base_group_sql(
            "ip, countIf(is_w AND status = 404) AS w_404, countIf(is_w) AS w_total, uniqExactIf(url, is_w AND status = 404) AS w_distinct_404, countIf(is_b AND status = 404) AS b_404",
            "ip",
            where="ip IN (SELECT ip FROM base WHERE is_w AND status = 404 GROUP BY ip HAVING count() >= 20)",
            having="w_404 >= 20 AND w_404 * 1.0 / nullIf(w_total, 0) >= 0.7 AND w_distinct_404 >= 15",
            order_by="w_404 DESC",
            limit=15,
        ),
        row_filter="ip != '' AND url != ''",
    ),
    "referer_monoculture": Detection(("referer",), _share_spike("referer", "0.20", extra_row="WHERE referer != ''")),
    "method_drift": Detection(
        ("method",),
        _share_spike(
            "method",
            "0.10",
            extra_row="WHERE method != '' AND upper(method) NOT IN ('GET', 'HEAD', 'OPTIONS')",
            min_w_cnt=20,
            multiplier=2,
            delta="0.05",
            w_first=True,
        ),
    ),
    "new_asn_traffic": Detection(("asn",), _new_traffic("asn", 20, where="asn IS NOT NULL")),
    "asn_hosting_shift": Detection(
        ("asn", "p_type"),
        _base_group_sql(
            "asn, countIf(is_w AND p_type = 'hosting') AS w_hosting, countIf(is_w) AS w_total, countIf(is_b AND p_type = 'hosting') AS b_hosting, countIf(is_b) AS b_total",
            "asn",
            where="asn IN (SELECT asn FROM base WHERE is_w GROUP BY asn HAVING count() >= 50)",
            having="w_total >= 50 AND (w_hosting * 1.0 / w_total) >= 0.3 AND (b_total = 0 OR (w_hosting * 1.0 / w_total) >= (b_hosting * 1.0 / nullIf(b_total, 0)) * 3 + 0.10)",
            order_by="w_hosting DESC",
            limit=15,
        ),
        row_filter="asn IS NOT NULL AND p_type != ''",
    ),
    "metro_delivery_degradation": Detection(
        ("metro", "delivery_rate"),
        _base_group_sql(
            "metro, quantileTDigestIf(0.5)(delivery_rate, is_w) AS w_med, quantileTDigestIf(0.5)(delivery_rate, is_b) AS b_med, countIf(is_w) AS w_total, countIf(is_b) AS b_total",
            "metro",
            where="metro IN (SELECT metro FROM base WHERE is_w GROUP BY metro HAVING count() >= 20)",
            having="w_total >= 20 AND b_total >= 50 AND w_med > 0 AND b_med > 0 AND w_med <= b_med * 0.5",
            order_by="(b_med - w_med) DESC",
            limit=15,
        ),
        row_filter="metro != '' AND delivery_rate > 0",
    ),
    "connection_type_mix": Detection(
        ("c_type", "c_speed"),
        _share_spike("c_type, c_speed", "0.15", min_w_cnt=20, multiplier=2, delta="0.05", limit=15, w_first=True),
        row_filter="c_type != ''",
    ),
    "pop_latency_regression": Detection(
        ("pop", "elapsed"), _p95_regression("pop"), row_filter="pop != '' AND elapsed IS NOT NULL"
    ),
    "http3_fallback": Detection(
        ("transport",),
        """ SELECT w_quic, w_total, b_quic, b_total FROM (
        SELECT countIf(is_w AND lower(transport) = 'quic') AS w_quic, countIf(is_w) AS w_total,
               countIf(is_b AND lower(transport) = 'quic') AS b_quic, countIf(is_b) AS b_total FROM base)
    WHERE w_total >= 100 AND b_total >= 100 AND b_quic * 1.0 / nullIf(b_total, 0) >= 0.30
      AND w_quic * 1.0 / nullIf(w_total, 0) <= b_quic * 1.0 / nullIf(b_total, 0) - 0.20 """,
        row_filter="transport != ''",
    ),
    "cache_hit_cliff": Detection(
        ("cache",),
        f""" SELECT w_hits, w_cacheable, b_hits, b_cacheable FROM (
        SELECT countIf(is_w AND {_HIT}) AS w_hits, countIf(is_w AND {_CACHEABLE}) AS w_cacheable,
               countIf(is_b AND {_HIT}) AS b_hits, countIf(is_b AND {_CACHEABLE}) AS b_cacheable FROM base)
    WHERE w_cacheable >= 100 AND b_cacheable >= 200 AND (b_hits * 1.0 / nullIf(b_cacheable, 0)) >= 0.40
      AND (w_hits * 1.0 / nullIf(w_cacheable, 0)) <= (b_hits * 1.0 / nullIf(b_cacheable, 0)) - 0.15 """,
    ),
    "payload_compression_regression": Detection(
        ("url", "status", "resp_bytes", "resp_header_content_encoding"),
        _rate_regression(
            "NOT match(lower(resp_header_content_encoding), 'gzip|br|zstd|deflate')",
        ),
        row_filter="status = 200 AND resp_bytes >= 1024 AND url != '' AND match(url, {compress_regex:String})",
    ),
    "session_harvesting": Detection(
        ("ip", "cookie_session"),
        """ , win AS (SELECT ip, uniqExactIf(cookie_session, is_w) AS w_sessions, countIf(is_w) AS w_reqs, uniqExactIf(cookie_session, is_b) AS b_sessions FROM base WHERE ip IN (SELECT ip FROM base WHERE is_w GROUP BY ip HAVING uniqExact(cookie_session) >= 20) GROUP BY ip HAVING w_sessions >= 20)
    SELECT ip, w_sessions, w_reqs, b_sessions FROM win WHERE w_sessions > b_sessions * 2 + 10 ORDER BY w_sessions DESC LIMIT 15 """,
        row_filter="ip != '' AND cookie_session != ''",
    ),
    "timeout_split": Detection(
        ("oconnect_ms", "ottfb"),
        f""" , prepared AS (SELECT oconnect_ms, greatest(ottfb / 1000.0 - oconnect_ms, 0) AS read_ms, is_w, is_b FROM base),
    agg AS (SELECT {_P95}(oconnect_ms, is_w) AS w_conn, {_P95}(oconnect_ms, is_b) AS b_conn,
               {_P95}(read_ms, is_w) AS w_read, {_P95}(read_ms, is_b) AS b_read, countIf(is_w) AS w_total, countIf(is_b) AS b_total FROM prepared)
    SELECT w_conn, b_conn, w_read, b_read, w_total, b_total FROM agg
    WHERE w_total >= 50 AND b_total >= 100 AND ((w_conn >= b_conn * 2 AND w_conn - b_conn >= 50) OR (w_read >= b_read * 2 AND w_read - b_read >= 100)) """,
        row_filter="oconnect_ms IS NOT NULL AND ottfb IS NOT NULL",
    ),
    "waf_signal_spikes": Detection(
        ("signal",),
        _base_group_sql(
            "signal, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt, w_cnt * 1.0 / greatest(b_cnt * 1.0 / {baseline_hours:Float64} * {window_hours:Float64}, 0.5) AS spike_ratio",
            "signal",
            having="w_cnt >= 3 AND w_cnt > b_cnt * 1.0 / {baseline_hours:Float64} * {window_hours:Float64} * 2 + 2",
            order_by="spike_ratio DESC",
            limit=15,
        ),
        row_filter="custom_fields['waf_sig'] != ''",
    ),
    "low_and_slow": Detection(
        ("ip", "url", "sec"),
        _base_group_sql(
            "ip, count() AS hits, uniqExact(url) AS distinct_paths, (max(sec) - min(sec)) AS span_s, round(count() * 1.0 / nullIf(max(sec) - min(sec), 0), 5) AS rps",
            "ip",
            having="hits >= 5 AND distinct_paths >= 3 AND span_s >= 600 AND count() * 1.0 / nullIf(max(sec) - min(sec), 0) < 0.2",
            order_by="distinct_paths DESC, span_s DESC",
            limit=15,
        ),
        row_filter="ip != '' AND url != ''",
        prewhere="match(url, {probe_regex:String})",
    ),
    "credential_enumeration": Detection(
        ("ip", "url", "status"),
        _base_group_sql(
            "ip, countIf(is_w AND status IN (401, 403)) AS w_denied, countIf(is_w) AS w_attempts, uniqExactIf(url, is_w) AS w_paths, countIf(is_b AND status IN (401, 403)) AS b_denied",
            "ip",
            where="ip IN (SELECT ip FROM base WHERE is_w AND status IN (401, 403) GROUP BY ip HAVING count() >= 20)",
            having="w_denied >= 20 AND w_denied * 1.0 / nullIf(w_attempts, 0) >= 0.5 AND w_denied > b_denied / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 3 + 5",
            order_by="w_denied DESC",
            limit=15,
        ),
        row_filter="ip != '' AND url != '' AND match(url, {auth_regex:String})",
    ),
    "network_asn_health": Detection(
        ("asn", "ploss", "rtt_var", "retrans"),
        f""" , health AS (SELECT asn, avgIf(ploss, is_w) AS w_ploss, avgIf(ploss, is_b) AS b_ploss, {_P95}(rtt_var, is_w) AS w_jitter, {_P95}(rtt_var, is_b) AS b_jitter,
            avgIf(retrans, is_w) AS w_retrans, avgIf(retrans, is_b) AS b_retrans, countIf(is_w) AS w_total, countIf(is_b) AS b_total
        FROM base WHERE asn IN (SELECT asn FROM base WHERE is_w GROUP BY asn HAVING count() >= 50) GROUP BY asn HAVING w_total >= 50 AND b_total >= 100)
    SELECT asn, w_ploss, b_ploss, w_jitter, b_jitter, w_retrans, b_retrans, w_total, b_total FROM health
    WHERE ((coalesce(w_ploss, 0) >= 0.02 AND coalesce(w_ploss, 0) >= coalesce(b_ploss, 0) * 2 + 0.005)
        OR (w_jitter >= b_jitter * 2 AND w_jitter - b_jitter >= 20000) OR (coalesce(w_retrans, 0) >= coalesce(b_retrans, 0) * 2 + 0.5))
    ORDER BY coalesce(w_ploss, 0) DESC, w_jitter DESC LIMIT 15 """,
        row_filter="asn IS NOT NULL",
    ),
    "city_surges": Detection(
        ("city", "region", "country"),
        _base_group_sql(
            f"{_CITY_LABEL}, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt, w_cnt * 1.0 / greatest(b_cnt * 1.0 / greatest({{baseline_hours:Float64}}, 1.0) * {{window_hours:Float64}}, 1.0) AS spike_ratio",
            "city, region, country",
            where="(city, region, country) IN (SELECT city, region, country FROM base WHERE is_w GROUP BY city, region, country HAVING count() >= 20)",
            having="w_cnt >= 20 AND w_cnt > b_cnt / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 3",
            order_by="spike_ratio DESC",
            limit=15,
        ),
        row_filter="city != ''",
    ),
    "city_error_spikes": Detection(
        ("city", "region", "country", "status"),
        _error_spike(
            _CITY_LABEL,
            group_by="city, region, country",
            status_min=400,
            min_w=10,
            min_rate="0.10",
            min_b=50,
            multiplier=3,
        ),
        row_filter="city != ''",
    ),
    "city_latency_regressions": Detection(
        ("city", "region", "country", "elapsed"),
        _p95_regression(_CITY_LABEL, group_by="city, region, country", min_w=10, min_b=50, ratio=3.0, min_delta_ms=500),
        row_filter="city != '' AND elapsed IS NOT NULL",
    ),
    "new_city_traffic": Detection(
        ("city", "region", "country"),
        _new_traffic(_CITY_LABEL, 5, group_by="city, region, country"),
        row_filter="city != ''",
    ),
    "asn_metro_performance": Detection(
        ("asn", "metro", "tcp_rtt", "country"),
        _base_group_sql(
            "asn, metro, quantileTDigestIf(0.50)(tcp_rtt, is_w) / 1000.0 AS w_med, quantileTDigestIf(0.50)(tcp_rtt, is_b) / 1000.0 AS b_med, countIf(is_w) AS w_total, countIf(is_b) AS b_total",
            "asn, metro",
            where="(asn, metro) IN (SELECT asn, metro FROM base WHERE is_w GROUP BY asn, metro HAVING count() >= 20)",
            having="w_total >= 20 AND b_total >= 50 AND w_med >= b_med * 1.5 AND w_med - b_med >= 20",
            order_by="(w_med - b_med) DESC",
            limit=15,
        ),
        row_filter="asn IS NOT NULL AND metro != '' AND tcp_rtt > 0 AND country = 'US'",
    ),
    "cache_ttl_mismatch": Detection(
        ("url", "ttl", "hits", "age"),
        _base_group_sql(
            "url AS label, round(avg(ttl), 0) AS avg_ttl, round(avg(hits), 1) AS avg_hits, round(avg(age), 0) AS avg_age, count() AS sample_count",
            "url",
            having="sample_count >= 10 AND avg(hits) < 2 AND avg(ttl) > 60",
            order_by="avg(ttl) DESC",
            limit=20,
        ),
        row_filter="ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL",
        window_only=True,
    ),
    "image_optimization_opportunities": Detection(
        ("url", "status", "resp_bytes", "ua"),
        _base_group_sql(
            "url, count() AS request_count, sum(resp_bytes) AS total_bytes, round(avg(resp_bytes) / 1024, 1) AS avg_kb, (sum(if(ua ILIKE '%Mobi%' OR ua ILIKE '%Android%' OR ua ILIKE '%iPhone%', 1, 0)) * 1.0 / nullIf(count(), 0)) AS mobile_ratio",
            "url",
            having="total_bytes > 1024 * 512",
            order_by="total_bytes DESC",
            limit=15,
        ),
        row_filter=(
            "status = 200 AND (url ILIKE '%.jpg%' OR url ILIKE '%.jpeg%' OR url ILIKE '%.png%' OR url ILIKE '%.gif%') "
            "AND url NOT ILIKE '%auto=webp%' AND url NOT ILIKE '%format=auto%' AND url NOT ILIKE '%format=webp%' AND url NOT ILIKE '%format=avif%'"
        ),
        window_only=True,
    ),
    "shield_path_degradation": Detection(
        ("rid", "prid", "pop", "ottfb", "edge"),
        """ , edge_logs AS (SELECT rid, pop, ottfb, is_w, is_b FROM base WHERE lower(edge) IN ('1', 'true') AND rid != ''),
    shield_logs AS (SELECT prid, pop, ottfb FROM base WHERE lower(edge) IN ('0', 'false') AND prid != ''),
    joined AS (SELECT e.pop AS edge_pop, if(ifNull(s.prid, '') = '', 'Direct to Origin', s.pop) AS shield_pop,
               (e.ottfb - ifNull(s.ottfb, 0)) / 1000.0 AS transit_ms, e.is_w AS is_w, e.is_b AS is_b
        FROM edge_logs e LEFT JOIN shield_logs s ON s.prid = e.rid)
    SELECT edge_pop, shield_pop, quantileTDigestIf(0.50)(transit_ms, is_w) AS w_p50, quantileTDigestIf(0.50)(transit_ms, is_b) AS b_p50, countIf(is_w) AS w_cnt
    FROM joined GROUP BY edge_pop, shield_pop HAVING w_cnt >= 5 AND w_p50 > b_p50 * 1.5 ORDER BY (w_p50 / nullIf(b_p50, 0)) DESC LIMIT 20 """,
        row_filter="ottfb IS NOT NULL",
    ),
    "repeated_patterns": Detection(
        ("sec", "ip", "ua"),
        _beaconing("ip", "uniqExact(ua)"),
        row_filter="ip != '' AND NOT (ua != '' AND match(ua, {bot_ua_regex:String}))",
        window_only=True,
    ),
    "stale_browser_version": Detection(
        ("ip", "ua", "chrome_version"),
        """ , win AS (SELECT chrome_version, countIf(is_w) AS w_reqs, uniqExactIf(ip, is_w) AS w_ips, countIf(is_b) AS b_reqs FROM base WHERE chrome_version IN (SELECT chrome_version FROM base WHERE is_w GROUP BY chrome_version HAVING count() >= 500) GROUP BY chrome_version HAVING w_reqs >= 500)
    SELECT concat('Chrome ', chrome_version) AS label, w_reqs, w_ips,
        w_reqs * 1.0 / greatest(b_reqs / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64}, 1.0) AS surge_ratio
    FROM win WHERE w_reqs > b_reqs / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 2.5 ORDER BY surge_ratio DESC LIMIT 10 """,
        row_filter=(
            "positionCaseInsensitive(ua, 'chrome') > 0 AND positionCaseInsensitive(ua, 'bot') = 0 "
            "AND positionCaseInsensitive(ua, 'crawler') = 0 AND positionCaseInsensitive(ua, 'spider') = 0 AND chrome_version != '' AND toInt32OrZero(chrome_version) < 131"
        ),
    ),
    "orphaned_deep_crawl": Detection(
        ("ip", "url", "referer"),
        """ , win AS (SELECT ip, countIf(is_w) AS w_reqs, uniqExactIf(url, is_w) AS w_distinct_urls, countIf(is_b) AS b_reqs FROM base WHERE ip IN (SELECT ip FROM base WHERE is_w GROUP BY ip HAVING count() >= 50) GROUP BY ip HAVING w_reqs >= 50 AND w_distinct_urls >= 10)
    SELECT ip AS label, w_reqs, w_distinct_urls, b_reqs, w_reqs * 1.0 / greatest(toFloat64(w_distinct_urls), 1.0) AS requests_per_url,
        w_reqs * 1.0 / greatest(b_reqs / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64}, 1.0) AS surge_ratio
    FROM win ORDER BY w_reqs DESC LIMIT 15 """,
        row_filter="url LIKE '/%/%/%' AND url NOT LIKE '%.%' AND (referer = '' OR referer = '-')",
    ),
    "residential_fingerprint_dispersion": Detection(
        ("ja4", "ip", "asn", "p_type"),
        _base_group_sql(
            "ja4 AS label, uniqExactIf(ip, is_w) AS w_ips, uniqExactIf(asn, is_w) AS w_asns, countIf(is_w) AS w_requests, uniqExactIf(ip, is_b) AS b_ips, w_asns * 1.0 / greatest(toFloat64(uniqExactIf(asn, is_b)), 1.0) AS asn_expansion_ratio",
            "ja4",
            where="ja4 IN (SELECT ja4 FROM base WHERE is_w GROUP BY ja4 HAVING uniqExact(ip) >= 30 AND uniqExact(asn) >= 5)",
            having="w_ips >= 30 AND w_asns >= 5",
            order_by="asn_expansion_ratio DESC",
            limit=10,
        ),
        row_filter="ja4 != '' AND p_type != 'hosting'",
    ),
    "crawler_disparity": Detection(
        ("ua", "referer", "ip", "resp_bytes"),
        f""" , ua_groups AS (SELECT lower(ua) AS l_ua, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,
            sumIf(coalesce(resp_bytes, 0), is_w) AS w_b, sumIf(coalesce(resp_bytes, 0), is_b) AS b_b,
            greatest(sum(sumIf(coalesce(resp_bytes, 0), is_w)) OVER (), 1) AS site_resp_total FROM base WHERE is_w OR lower(ua) IN (SELECT lower(ua) FROM base WHERE is_w GROUP BY lower(ua)) GROUP BY l_ua),
    ua_tagged AS (SELECT w_cnt, b_cnt, w_b, b_b, site_resp_total,
            extract(l_ua, {_CRAWLER_TOKEN}) AS raw_tok,
            extract(l_ua, 'https?://(?:www\\.)?([^/.:;\\s)]+)') AS ua_dom FROM ua_groups),
    ua_classified AS (SELECT w_cnt, b_cnt, w_b, b_b, site_resp_total, raw_tok, ua_dom,
            {_BOT_STEM} AS bot_stem
            FROM ua_tagged WHERE raw_tok != ''),
    crawler_traffic AS (SELECT bot_stem, min(raw_tok) AS raw_tok, max(ua_dom) AS ua_dom, sum(w_cnt) AS w_crawls, sum(b_cnt) AS b_crawls,
               sum(w_b) AS w_bytes, sum(b_b) AS b_bytes, any(site_resp_total) AS site_resp_total FROM ua_classified WHERE bot_stem != '' GROUP BY bot_stem HAVING w_crawls >= 50),
    human_refs AS (SELECT ip, max(is_w) AS is_w, max(is_b) AS is_b,
            if(extract(lower(ua), '([a-z0-9]+)-(?:user|externalfetcher)') != '',
               extract(lower(ua), '([a-z0-9]+)-(?:user|externalfetcher)'),
               extract(lower(referer), 'https?://(?:www\\.)?([^/.:;\\s]+)')) AS ref_stem
        FROM base
        WHERE {_CRAWLER_IN_WINDOW} AND ip != '' AND (referer != '' OR positionCaseInsensitive(ua, '-user') > 0 OR positionCaseInsensitive(ua, '-externalfetcher') > 0)
          AND extract(lower(ua), {_CRAWLER_TOKEN}) = ''
        GROUP BY ip, ref_stem HAVING ref_stem != ''
        UNION ALL SELECT '' AS ip, 0 AS is_w, 0 AS is_b, '' AS ref_stem),
    combined AS (SELECT c.bot_stem AS bot_stem, any(c.raw_tok) AS raw_tok, any(c.w_crawls) AS w_crawls, any(c.b_crawls) AS b_crawls,
            any(c.w_bytes) AS w_bytes, any(c.b_bytes) AS b_bytes, any(c.site_resp_total) AS site_resp_total,
            uniqExactIf(h.ip, toBool(h.is_w) AND h.ref_stem != '' AND (c.bot_stem = h.ref_stem OR (length(replaceRegexpAll(c.bot_stem, '[aeiou]', '')) >= 3 AND replaceRegexpAll(c.bot_stem, '[aeiou]', '') = replaceRegexpAll(h.ref_stem, '[aeiou]', '')) OR (length(c.bot_stem) >= 3 AND length(h.ref_stem) >= 3 AND (position(h.ref_stem, c.bot_stem) > 0 OR position(c.bot_stem, h.ref_stem) > 0)) OR (c.ua_dom != '' AND c.ua_dom = h.ref_stem))) AS w_referrals,
            uniqExactIf(h.ip, toBool(h.is_b) AND h.ref_stem != '' AND (c.bot_stem = h.ref_stem OR (length(replaceRegexpAll(c.bot_stem, '[aeiou]', '')) >= 3 AND replaceRegexpAll(c.bot_stem, '[aeiou]', '') = replaceRegexpAll(h.ref_stem, '[aeiou]', '')) OR (length(c.bot_stem) >= 3 AND length(h.ref_stem) >= 3 AND (position(h.ref_stem, c.bot_stem) > 0 OR position(c.bot_stem, h.ref_stem) > 0)) OR (c.ua_dom != '' AND c.ua_dom = h.ref_stem))) AS b_referrals
        FROM crawler_traffic c CROSS JOIN human_refs h GROUP BY c.bot_stem)
    SELECT c.raw_tok AS label, c.w_crawls, c.w_referrals, c.w_bytes,
        c.w_crawls * 1.0 / greatest(c.w_referrals, 1) AS crawls_per_referral,
        c.b_crawls * 1.0 / greatest(c.b_referrals, 1) AS baseline_crawls_per_referral,
        c.w_bytes * 1.0 / greatest(c.w_referrals, 1) AS bytes_per_referral,
        c.b_bytes * 1.0 / greatest(c.b_referrals, 1) AS baseline_bytes_per_referral,
        round(c.w_bytes * 100.0 / c.site_resp_total, 2) AS bandwidth_share_pct
    FROM combined c
    WHERE c.w_crawls >= 50 AND (c.w_referrals = 0 OR c.w_crawls * 1.0 / greatest(c.w_referrals, 1) >= 100.0 OR c.w_bytes * 1.0 / greatest(c.w_referrals, 1) >= 1048576.0)
    ORDER BY crawls_per_referral DESC, c.w_bytes DESC LIMIT 10 """,
        row_filter="ua != ''",
    ),
}


def botnet_grouping(fingerprint_column: str) -> Detection:
    """Botnet grouping keyed on the JA4 fingerprint when logged, else JA3."""
    if fingerprint_column not in ("ja4", "ja3"):
        raise ValueError("fingerprint column must be ja4 or ja3")
    return Detection(("ip", fingerprint_column), _fingerprint_spread(fingerprint_column, with_ratio=True))


def _base_sql(detection: Detection, insight_id: str) -> str:
    projection = ", ".join(f"{COLUMNS[column]} AS {column}" for column in detection.columns)
    lower = "{window_start:DateTime64(3)}" if detection.window_only else "{baseline_start:DateTime64(3)}"
    row_filter = f" AND ({detection.row_filter})" if detection.row_filter else ""
    prewhere = f" PREWHERE {detection.prewhere}" if detection.prewhere else ""
    scope = " AND ".join(visible_fact_clauses("request"))
    return (
        f"/* insight:{insight_id} */ WITH base AS (SELECT {projection}, "
        "event_timestamp < {window_start:DateTime64(3)} AS is_b, "
        "event_timestamp >= {window_start:DateTime64(3)} AS is_w "
        f"FROM request_facts{prewhere} WHERE {scope} AND event_timestamp >= {lower} "
        f"AND event_timestamp <= {{window_end:DateTime64(3)}}{row_filter}) {detection.sql}"
    )


def _detection_for(insight_id: str, present: set[str]) -> Detection | None:
    """The ClickHouse query for ``insight_id``, or None when this service cannot compute it."""
    fingerprint = _fingerprint_column(present)
    if insight_id == "botnet_grouping":
        return botnet_grouping(fingerprint) if fingerprint else None
    if insight_id == "repeated_patterns_fp":
        return repeated_patterns_fp(fingerprint) if fingerprint else None
    if insight_id == "impossible_distance":
        from backend.utils.pop_utils import get_pop_lat_lon_map

        pops = {
            code: (float(lat), float(lon))
            for code, (lat, lon) in get_pop_lat_lon_map().items()
            if lat is not None and lon is not None and POP_CODE.fullmatch(code)
        }
        if not pops or not fingerprint:
            return None
        return impossible_distance(pops, fingerprint, edge_only="edge" in present)
    return DETECTIONS.get(insight_id)
