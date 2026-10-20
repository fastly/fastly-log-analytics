"""ClickHouse implementations of the standard Insights detections over ``request_facts``.

Every query returns the exact row layout the standard row processors consume (see the
``# row schema`` comment on each processor in ``backend/repositories/insights/definitions.py``),
so cards, thresholds, severities and ``meta`` payloads are produced by the same code on both
data planes. Queries read a ``base`` CTE exposing the DuckDB column names the standard
templates use; only the columns a detection lists are materialised.

Percentiles use t-digest sketches (bounded memory at high-scale volumes); the detections only
compare ratios and large absolute deltas, so the sketch error is far below every threshold.
A missing text field is the empty string (a map lookup), so ``!= ''`` replaces ``IS NOT NULL``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# DuckDB column name -> ClickHouse expression over request_facts.
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

_PROBE_PATHS = (
    "admin",
    ".env",
    ".git",
    "wp-",
    "phpmyadmin",
    "config",
    "backup",
    "shell",
    "passwd",
    "xmlrpc",
    "actuator",
    "console",
    "cgi-bin",
    ".php",
    ".asp",
    "../../",
    "swagger",
    "api-docs",
    "graphql",
    "debug",
)
_COMPRESSIBLE_EXTENSIONS = (".js", ".css", ".html", ".json", ".svg", ".xml", ".txt")
_AUTH_PATHS = (
    "login",
    "signin",
    "sign-in",
    "log-in",
    "auth",
    "oauth",
    "openid",
    "token",
    "sso",
    "saml",
    "session",
    "account",
    "password",
    "passwd",
    "passwrd",
    "credential",
    "wp-login",
    "xmlrpc",
    "mfa",
    "otp",
    "2fa",
    "verify",
)
_BOT_UA_TOKENS = (
    "googlebot",
    "bingbot",
    "slurp",
    "duckduckbot",
    "baiduspider",
    "yandexbot",
    "applebot",
    "uptimerobot",
    "pingdom",
    "statuscake",
    "datadoghq",
    "newrelic",
    "ahrefsbot",
    "semrushbot",
    "gptbot",
    "claudebot",
    "ccbot",
    "facebookexternalhit",
    "bot",
    "crawler",
    "spider",
    "monitor",
)
PROBE_REGEX = "(?i)" + "|".join(re.escape(path) for path in _PROBE_PATHS)
AUTH_REGEX = "(?i)" + "|".join(re.escape(path) for path in _AUTH_PATHS)
BOT_UA_REGEX = "(?i)" + "|".join(re.escape(token) for token in _BOT_UA_TOKENS)
COMPRESSIBLE_REGEX = "(?i)" + "|".join(re.escape(ext) for ext in _COMPRESSIBLE_EXTENSIONS)

_HIT = "startsWith(cache, 'HIT')"
_CACHEABLE = "(startsWith(cache, 'HIT') OR startsWith(cache, 'MISS'))"
_P95 = "quantileTDigestIf(0.95)"


@dataclass(frozen=True)
class Detection:
    """One detection: the base columns it reads, a row-level prefilter, and the SQL after the ``base`` CTE."""

    columns: tuple[str, ...]
    sql: str
    row_filter: str = ""
    # True when the query only reads the detection window, so the baseline is never scanned.
    window_only: bool = False
    # False for the one detection (impossible_distance) the standard path runs without a baseline.
    needs_baseline: bool = True


def _share_spike(column: str, share_floor: str, *, extra_row: str = "") -> str:
    """Window traffic share of one value that spiked versus its baseline share (UA / referer monoculture)."""
    return f"""
    , totals AS (SELECT countIf(is_w) AS w_total, countIf(is_b) AS b_total FROM base),
    grp AS (
        SELECT {column}, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt
        FROM base {extra_row} GROUP BY {column}
    )
    SELECT g.{column}, g.w_cnt, g.b_cnt, t.b_total, t.w_total
    FROM grp g CROSS JOIN totals t
    WHERE t.w_total > 0 AND g.w_cnt * 1.0 / t.w_total >= {share_floor}
      AND (t.b_total = 0 OR g.w_cnt * 1.0 / t.w_total >= g.b_cnt * 1.0 / nullIf(t.b_total, 0) * 3 + 0.10)
    ORDER BY g.w_cnt DESC LIMIT 10
    """


def _fingerprint_spread(column: str, *, with_ratio: bool) -> str:
    """Distinct client IPs per fingerprint in the window versus the baseline's hourly rate."""
    ratio = (
        ", w_ips * 1.0 / greatest(b_ips / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64}, 1) AS ip_ratio"
        if with_ratio
        else ""
    )
    return f"""
    , win AS (
        SELECT {column}, uniqExactIf(ip, is_w) AS w_ips, countIf(is_w) AS w_reqs, uniqExactIf(ip, is_b) AS b_ips
        FROM base WHERE {column} != '' GROUP BY {column} HAVING w_ips >= 5
    )
    SELECT {column}, w_ips, w_reqs, b_ips{ratio}
    FROM win
    WHERE w_ips > b_ips / greatest({{baseline_hours:Float64}}, 1.0) * {{window_hours:Float64}} * 3
    ORDER BY w_ips * 1.0 / greatest(b_ips / greatest({{baseline_hours:Float64}}, 1.0) * {{window_hours:Float64}}, 1) DESC
    LIMIT 10
    """


def _per_ip_outlier(column: str, window_floor: int, extra_having: str, order: str, aliases: tuple[str, str]) -> str:
    """Per-IP window maximum against that IP's own baseline p95 (request size, connection reuse)."""
    peak, mean = aliases
    return f"""
    , win AS (
        SELECT ip, max({column}) AS {peak}, avg({column}) AS {mean}, count() AS w_total
        FROM base WHERE is_w GROUP BY ip HAVING w_total >= {window_floor}{extra_having}
    ),
    base_b AS (
        SELECT ip, quantileExactInclusive(0.95)({column}) AS b_p95
        FROM base WHERE is_b AND ip IN (SELECT ip FROM win) GROUP BY ip
    )
    SELECT w.ip, w.{peak}, w.{mean}, w.w_total, b.b_p95
    FROM win w INNER JOIN base_b b ON w.ip = b.ip
    WHERE w.w_total >= {window_floor} AND w.{peak} > b.b_p95 * 3{order}
    """


_CITY_LABEL = "concat(city, ', ', region, ', ', country) AS label, city, region, country"


def _beaconing(entity: str, diversity: str) -> str:
    """Regular-cadence (beaconing) detection per ``entity``: gap statistics over distinct active seconds."""
    return f"""
    , meta AS (
        SELECT {entity}, {diversity} AS diversity, count() AS n_events, (max(sec) - min(sec)) AS span_s
        FROM base GROUP BY {entity}
        HAVING n_events >= 13 AND (n_events * 1.0 / nullIf(span_s, 0)) < 2.0
    ),
    secs AS (SELECT {entity}, sec FROM base WHERE {entity} IN (SELECT {entity} FROM meta) GROUP BY {entity}, sec),
    gaps AS (
        SELECT {entity},
               sec - lagInFrame(sec) OVER (PARTITION BY {entity} ORDER BY sec
                                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gap,
               row_number() OVER (PARTITION BY {entity} ORDER BY sec) AS position
        FROM secs
    ),
    g AS (SELECT {entity}, gap FROM gaps WHERE position > 1),
    gap_counts AS (SELECT {entity}, gap, count() AS occurrences FROM g GROUP BY {entity}, gap),
    modes AS (SELECT {entity}, argMax(gap, tuple(occurrences, -gap)) AS mode_gap FROM gap_counts GROUP BY {entity}),
    agg AS (
        SELECT g.{entity} AS {entity}, count() AS n_gaps, avg(g.gap) AS mean_gap, varSamp(g.gap) AS var_gap,
               stddevSamp(g.gap) AS sd_gap, any(m.mode_gap) AS mode_gap,
               avg(if(abs(g.gap - m.mode_gap) <= 1, 1.0, 0.0)) AS modal_frac
        FROM g INNER JOIN modes m ON g.{entity} = m.{entity}
        GROUP BY g.{entity} HAVING n_gaps >= 12
    )
    SELECT a.{entity}, a.n_gaps, d.n_events,
           round(a.mean_gap, 2) AS avg_interval,
           round(a.sd_gap, 2) AS stddev_interval,
           round(sqrt(greatest(a.var_gap - 1.0 / 12.0, 0)) / nullIf(a.mean_gap, 0), 3) AS cv_corr,
           round(a.modal_frac, 3) AS modal_frac,
           d.diversity, d.span_s, a.mode_gap
    FROM agg a INNER JOIN meta d ON a.{entity} = d.{entity}
    WHERE a.n_gaps >= 12
      AND (d.n_events * 1.0 / nullIf(d.span_s, 0)) < 2.0
      AND (
            (a.mean_gap >= 5 AND sqrt(greatest(a.var_gap - 1.0 / 12.0, 0)) / nullIf(a.mean_gap, 0) < 0.3
                             AND a.modal_frac >= 0.6)
         OR (a.mean_gap >= 1 AND a.mean_gap < 5 AND a.modal_frac >= 0.85)
          )
    ORDER BY a.modal_frac DESC, a.n_gaps DESC
    LIMIT 15
    """


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
    sql = f"""
    , pop_coords AS (
        SELECT t.1 AS pop_code, t.2 AS pop_lat, t.3 AS pop_lon
        FROM (SELECT arrayJoin([{", ".join(pops)}]) AS t)
    ),
    flagged AS (
        SELECT {fingerprint_column} AS fp, b.ip AS ip, b.pop AS pop, round(b.lat, 3) AS client_lat, round(b.lon, 3) AS client_lon,
               c.pop_lat AS pop_lat, c.pop_lon AS pop_lon, b.tcp_rtt AS tcp_rtt, b.country AS country, b.city AS city,
               round(2 * 6371 * asin(sqrt(pow(sin(radians(b.lat - c.pop_lat) / 2), 2)
                   + cos(radians(b.lat)) * cos(radians(c.pop_lat)) * pow(sin(radians(b.lon - c.pop_lon) / 2), 2))), 1)
                   AS distance_km,
               round((b.tcp_rtt / 2.0 / 1e6) * 200000 * 2, 1) AS max_km
        FROM base b INNER JOIN pop_coords c ON b.pop = c.pop_code
    )
    SELECT fp, count() AS hits, max(distance_km - max_km) AS worst_excess_km, max(distance_km) AS max_dist_km,
           min(max_km) AS min_allowed_km, any(pop) AS pop, any(ip) AS sample_ip, any(client_lat), any(client_lon),
           any(pop_lat), any(pop_lon), any(tcp_rtt), any(country), any(city)
    FROM flagged WHERE distance_km > max_km GROUP BY fp HAVING hits >= 2
    ORDER BY worst_excess_km DESC LIMIT 15
    """
    return Detection(
        columns,
        sql,
        row_filter=f"lat IS NOT NULL AND lon IS NOT NULL AND tcp_rtt IS NOT NULL AND tcp_rtt > 0 AND {fingerprint_column} != ''{edge}",
        window_only=True,
        needs_baseline=False,
    )


DETECTIONS: dict[str, Detection] = {
    "error_spikes": Detection(
        ("url", "status"),
        """
    SELECT url,
        countIf(is_w AND status >= 500) * 1.0 / nullIf(countIf(is_w), 0) AS w_rate,
        countIf(is_b AND status >= 500) * 1.0 / nullIf(countIf(is_b), 0) AS b_rate,
        countIf(is_w AND status >= 500) AS w_errors,
        countIf(is_w) AS w_total,
        countIf(is_b) AS b_total
    FROM base GROUP BY url
    HAVING w_total >= 3 AND w_rate >= 0.05 AND (b_total < 10 OR w_rate >= b_rate * 2 + 0.05)
    ORDER BY (w_rate - coalesce(b_rate, 0)) DESC LIMIT 15
    """,
    ),
    "new_country_traffic": Detection(
        ("country",),
        """
    SELECT country, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt
    FROM base WHERE country != '' GROUP BY country
    HAVING w_cnt >= 3 AND b_cnt = 0
    ORDER BY w_cnt DESC LIMIT 20
    """,
    ),
    "ua_monoculture": Detection(("ua",), _share_spike("ua", "0.25")),
    "new_probe_urls": Detection(
        ("url", "status"),
        """
    SELECT url, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,
        avgIf(if(status >= 400, 1.0, 0.0), is_w) * 100 AS w_error_pct
    FROM base WHERE url != '' AND match(url, {probe_regex:String})
    GROUP BY url HAVING w_cnt > 0 AND b_cnt = 0
    ORDER BY w_cnt DESC LIMIT 25
    """,
    ),
    "proxy_surge": Detection(
        ("p_type",),
        """
    , agg AS (
        SELECT p_type, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt
        FROM base WHERE p_type != '' GROUP BY p_type
    ),
    totals AS (SELECT sum(w_cnt) AS w_proxy_total, sum(b_cnt) AS b_proxy_total FROM agg),
    all_rows AS (SELECT countIf(is_w) AS w_total_all, countIf(is_b) AS b_total_all FROM base)
    SELECT a.p_type, a.w_cnt, a.b_cnt, r.w_total_all, r.b_total_all
    FROM agg a CROSS JOIN totals t CROSS JOIN all_rows r
    WHERE (t.w_proxy_total * 100.0 / nullIf(r.w_total_all, 0)) >= 5
      AND (t.w_proxy_total * 100.0 / nullIf(r.w_total_all, 0))
          >= (t.b_proxy_total * 100.0 / nullIf(r.b_total_all, 0)) * 2 + 5
    """,
    ),
    "asn_concentration": Detection(
        ("asn",),
        """
    , totals AS (SELECT countIf(is_w) AS w_total, countIf(is_b) AS b_total FROM base),
    grp AS (
        SELECT asn, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt
        FROM base WHERE asn IS NOT NULL GROUP BY asn
    )
    SELECT g.asn, g.w_cnt, g.b_cnt, t.b_total, t.w_total
    FROM grp g CROSS JOIN totals t
    WHERE t.w_total > 0 AND g.w_cnt * 1.0 / t.w_total >= 0.20
      AND (t.b_total = 0 OR g.w_cnt * 1.0 / t.w_total >= g.b_cnt * 1.0 / nullIf(t.b_total, 0) * 3 + 0.10)
    ORDER BY g.w_cnt DESC LIMIT 10
    """,
    ),
    "cache_collapse": Detection(
        ("url", "cache"),
        f"""
    SELECT url,
        countIf(is_w AND {_HIT}) * 1.0 / nullIf(countIf(is_w AND {_CACHEABLE}), 0) AS w_rate,
        countIf(is_b AND {_HIT}) * 1.0 / nullIf(countIf(is_b AND {_CACHEABLE}), 0) AS b_rate,
        countIf(is_w AND {_CACHEABLE}) AS w_total,
        countIf(is_b AND {_CACHEABLE}) AS b_total
    FROM base GROUP BY url
    HAVING w_total >= 5 AND b_total >= 20 AND b_rate >= 0.40 AND w_rate <= b_rate - 0.20 AND w_rate <= b_rate * 0.6
    ORDER BY (coalesce(b_rate, 0) - w_rate) DESC LIMIT 15
    """,
    ),
    "cacheability_regression": Detection(
        ("url", "cache"),
        """
    SELECT url,
        countIf(is_w AND startsWith(cache, 'PASS')) * 1.0 / nullIf(countIf(is_w), 0) AS w_rate,
        countIf(is_b AND startsWith(cache, 'PASS')) * 1.0 / nullIf(countIf(is_b), 0) AS b_rate,
        countIf(is_w) AS w_total,
        countIf(is_b) AS b_total
    FROM base GROUP BY url
    HAVING w_total >= 10 AND b_total >= 50 AND b_rate <= 0.20 AND w_rate >= 0.50 AND w_rate >= b_rate + 0.30
    ORDER BY (w_rate - coalesce(b_rate, 0)) DESC LIMIT 15
    """,
    ),
    "latency_regression": Detection(
        ("url", "elapsed"),
        f"""
    SELECT url,
        {_P95}(elapsed, is_w) / 1000.0 AS w_p95,
        {_P95}(elapsed, is_b) / 1000.0 AS b_p95,
        countIf(is_w) AS w_total,
        countIf(is_b) AS b_total
    FROM base GROUP BY url
    HAVING w_total >= 5 AND b_total >= 20 AND w_p95 >= b_p95 * 2.0 AND w_p95 - b_p95 >= 200
    ORDER BY (w_p95 / nullIf(b_p95, 0)) DESC LIMIT 15
    """,
        row_filter="elapsed IS NOT NULL",
    ),
    "tail_latency": Detection(
        ("url", "elapsed"),
        """
    SELECT url,
        round(quantileTDigest(0.99)(elapsed) / 1000.0, 0) AS p99_ms,
        round(quantileTDigest(0.50)(elapsed) / 1000.0, 0) AS p50_ms,
        round(quantileTDigest(0.99)(elapsed) / nullIf(quantileTDigest(0.50)(elapsed), 0), 1) AS ratio,
        count() AS total
    FROM base GROUP BY url HAVING total >= 20 AND ratio > 5
    ORDER BY ratio DESC LIMIT 15
    """,
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
        f"""
    SELECT server_region,
        {_P95}(elapsed, is_w) / 1000.0 AS w_p95,
        {_P95}(elapsed, is_b) / 1000.0 AS b_p95,
        countIf(is_w) AS w_total,
        countIf(is_b) AS b_total,
        if(countIf(is_w AND ottfb IS NOT NULL) >= 20,
           round(quantileTDigestIf(0.95)(ottfb, is_w AND ottfb IS NOT NULL) / 1000.0, 0), NULL) AS ottfb_p95
    FROM base GROUP BY server_region
    HAVING w_total >= 20 AND b_total >= 50 AND w_p95 >= b_p95 * 1.5 AND w_p95 - b_p95 >= 100
    ORDER BY (w_p95 / nullIf(b_p95, 0)) DESC LIMIT 15
    """,
        row_filter="elapsed IS NOT NULL AND server_region != ''",
    ),
    "origin_latency_spike": Detection(
        ("ottfb", "url"),
        f"""
    , overall AS (
        SELECT {_P95}(ottfb, is_w) / 1000.0 AS w_p95, {_P95}(ottfb, is_b) / 1000.0 AS b_p95 FROM base
    ),
    url_stats AS (
        SELECT url, count() AS requests, round(quantileTDigest(0.95)(ottfb) / 1000.0, 1) AS p95_ms
        FROM base WHERE is_w GROUP BY url HAVING requests >= 10
    )
    SELECT u.url, u.p95_ms, o.w_p95, o.b_p95, u.requests
    FROM url_stats u CROSS JOIN overall o
    WHERE o.w_p95 > o.b_p95 * 2
    ORDER BY u.p95_ms DESC LIMIT 10
    """,
        row_filter="ottfb IS NOT NULL",
    ),
    "origin_error_rate": Detection(
        ("ost",),
        """
    , totals AS (
        SELECT countIf(is_w) AS w_total, countIf(is_b) AS b_total,
               countIf(is_w AND ost >= 500) AS w_5xx, countIf(is_b AND ost >= 500) AS b_5xx
        FROM base
    ),
    by_status AS (SELECT ost AS status, count() AS w_cnt FROM base WHERE is_w AND ost >= 500 GROUP BY ost)
    SELECT s.status, s.w_cnt, t.w_total, t.b_total, t.w_5xx, t.b_5xx
    FROM by_status s CROSS JOIN totals t
    WHERE (t.w_5xx * 100.0 / nullIf(t.w_total, 0)) >= 1.0
      AND (t.w_5xx * 100.0 / nullIf(t.w_total, 0)) > (t.b_5xx * 100.0 / nullIf(t.b_total, 0)) * 2
    """,
        row_filter="ost IS NOT NULL",
    ),
    "origin_retries": Detection(
        ("url", "oretries"),
        """
    SELECT url, count() AS requests, round(avg(oretries), 2) AS avg_retries, max(oretries) AS max_retries
    FROM base GROUP BY url HAVING requests >= 5
    ORDER BY avg_retries DESC LIMIT 10
    """,
        row_filter="oretries > 0",
        window_only=True,
    ),
    "origin_ip_failure": Detection(
        ("oip", "ost"),
        """
    , stats AS (
        SELECT oip, count() AS requests, round(countIf(ost >= 500) * 100.0 / nullIf(count(), 0), 1) AS error_pct
        FROM base GROUP BY oip HAVING requests >= 10
    ),
    med AS (SELECT quantileExactInclusive(0.5)(error_pct) AS median_rate FROM stats)
    SELECT s.oip, s.requests, s.error_pct, m.median_rate
    FROM stats s CROSS JOIN med m
    WHERE s.error_pct > m.median_rate * 3 AND s.error_pct > 5
    ORDER BY s.error_pct DESC
    """,
        row_filter="oip != '' AND ost IS NOT NULL",
        window_only=True,
    ),
    "content_discovery": Detection(
        ("ip", "url", "status"),
        """
    SELECT ip,
        countIf(is_w AND status = 404) AS w_404,
        countIf(is_w) AS w_total,
        uniqExactIf(url, is_w AND status = 404) AS w_distinct_404,
        countIf(is_b AND status = 404) AS b_404
    FROM base GROUP BY ip
    HAVING w_404 >= 20 AND w_404 * 1.0 / nullIf(w_total, 0) >= 0.7 AND w_distinct_404 >= 15
    ORDER BY w_404 DESC LIMIT 15
    """,
        row_filter="ip != '' AND url != ''",
    ),
    "referer_monoculture": Detection(("referer",), _share_spike("referer", "0.20", extra_row="WHERE referer != ''")),
    "method_drift": Detection(
        ("method",),
        """
    , totals AS (SELECT countIf(is_w) AS w_total, countIf(is_b) AS b_total FROM base),
    grp AS (
        SELECT method, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt
        FROM base WHERE method != '' AND upper(method) NOT IN ('GET', 'HEAD', 'OPTIONS') GROUP BY method
    )
    SELECT g.method, g.w_cnt, g.b_cnt, t.w_total, t.b_total
    FROM grp g CROSS JOIN totals t
    WHERE t.w_total > 0 AND g.w_cnt >= 20 AND g.w_cnt * 1.0 / t.w_total >= 0.10
      AND (t.b_total = 0 OR g.w_cnt * 1.0 / t.w_total >= g.b_cnt * 1.0 / nullIf(t.b_total, 0) * 2 + 0.05)
    ORDER BY g.w_cnt DESC LIMIT 10
    """,
    ),
    "new_asn_traffic": Detection(
        ("asn",),
        """
    SELECT asn, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt
    FROM base WHERE asn IS NOT NULL GROUP BY asn
    HAVING w_cnt >= 20 AND b_cnt = 0
    ORDER BY w_cnt DESC LIMIT 20
    """,
    ),
    "asn_hosting_shift": Detection(
        ("asn", "p_type"),
        """
    SELECT asn,
        countIf(is_w AND p_type = 'hosting') AS w_hosting,
        countIf(is_w) AS w_total,
        countIf(is_b AND p_type = 'hosting') AS b_hosting,
        countIf(is_b) AS b_total
    FROM base GROUP BY asn
    HAVING w_total >= 50 AND (w_hosting * 1.0 / w_total) >= 0.3
       AND (b_total = 0 OR (w_hosting * 1.0 / w_total) >= (b_hosting * 1.0 / nullIf(b_total, 0)) * 3 + 0.10)
    ORDER BY w_hosting DESC LIMIT 15
    """,
        row_filter="asn IS NOT NULL AND p_type != ''",
    ),
    "metro_delivery_degradation": Detection(
        ("metro", "delivery_rate"),
        """
    SELECT metro,
        quantileTDigestIf(0.5)(delivery_rate, is_w) AS w_med,
        quantileTDigestIf(0.5)(delivery_rate, is_b) AS b_med,
        countIf(is_w) AS w_total,
        countIf(is_b) AS b_total
    FROM base GROUP BY metro
    HAVING w_total >= 20 AND b_total >= 50 AND w_med > 0 AND b_med > 0 AND w_med <= b_med * 0.5
    ORDER BY (b_med - w_med) DESC LIMIT 15
    """,
        row_filter="metro != '' AND delivery_rate > 0",
    ),
    "connection_type_mix": Detection(
        ("c_type", "c_speed"),
        """
    , totals AS (SELECT countIf(is_w) AS w_total, countIf(is_b) AS b_total FROM base),
    grp AS (
        SELECT c_type, c_speed, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt
        FROM base GROUP BY c_type, c_speed
    )
    SELECT g.c_type, g.c_speed, g.w_cnt, g.b_cnt, t.w_total, t.b_total
    FROM grp g CROSS JOIN totals t
    WHERE g.w_cnt >= 20 AND t.w_total > 0 AND g.w_cnt * 1.0 / t.w_total >= 0.15
      AND (t.b_total = 0 OR g.w_cnt * 1.0 / t.w_total >= g.b_cnt * 1.0 / nullIf(t.b_total, 0) * 2 + 0.05)
    ORDER BY g.w_cnt DESC LIMIT 15
    """,
        row_filter="c_type != ''",
    ),
    "pop_latency_regression": Detection(
        ("pop", "elapsed"),
        f"""
    SELECT pop,
        {_P95}(elapsed, is_w) / 1000.0 AS w_p95,
        {_P95}(elapsed, is_b) / 1000.0 AS b_p95,
        countIf(is_w) AS w_total,
        countIf(is_b) AS b_total
    FROM base GROUP BY pop
    HAVING w_total >= 20 AND b_total >= 50 AND w_p95 >= b_p95 * 1.5 AND w_p95 - b_p95 >= 100
    ORDER BY (w_p95 / nullIf(b_p95, 0)) DESC LIMIT 15
    """,
        row_filter="pop != '' AND elapsed IS NOT NULL",
    ),
    "http3_fallback": Detection(
        ("transport",),
        """
    SELECT w_quic, w_total, b_quic, b_total FROM (
        SELECT countIf(is_w AND lower(transport) = 'quic') AS w_quic, countIf(is_w) AS w_total,
               countIf(is_b AND lower(transport) = 'quic') AS b_quic, countIf(is_b) AS b_total
        FROM base
    )
    WHERE w_total >= 100 AND b_total >= 100
      AND b_quic * 1.0 / nullIf(b_total, 0) >= 0.30
      AND w_quic * 1.0 / nullIf(w_total, 0) <= b_quic * 1.0 / nullIf(b_total, 0) - 0.20
    """,
        row_filter="transport != ''",
    ),
    "cache_hit_cliff": Detection(
        ("cache",),
        f"""
    SELECT w_hits, w_cacheable, b_hits, b_cacheable FROM (
        SELECT countIf(is_w AND {_HIT}) AS w_hits, countIf(is_w AND {_CACHEABLE}) AS w_cacheable,
               countIf(is_b AND {_HIT}) AS b_hits, countIf(is_b AND {_CACHEABLE}) AS b_cacheable
        FROM base
    )
    WHERE w_cacheable >= 100 AND b_cacheable >= 200
      AND (b_hits * 1.0 / nullIf(b_cacheable, 0)) >= 0.40
      AND (w_hits * 1.0 / nullIf(w_cacheable, 0)) <= (b_hits * 1.0 / nullIf(b_cacheable, 0)) - 0.15
    """,
    ),
    "payload_compression_regression": Detection(
        ("url", "status", "resp_bytes", "resp_header_content_encoding"),
        """
    SELECT url,
        countIf(is_w AND uncompressed) * 1.0 / nullIf(countIf(is_w), 0) AS w_rate,
        countIf(is_b AND uncompressed) * 1.0 / nullIf(countIf(is_b), 0) AS b_rate,
        countIf(is_w) AS w_total,
        countIf(is_b) AS b_total
    FROM (
        SELECT url, is_w, is_b,
               NOT match(lower(resp_header_content_encoding), 'gzip|br|zstd|deflate') AS uncompressed
        FROM base
    ) GROUP BY url
    HAVING w_total >= 10 AND b_total >= 50 AND b_rate <= 0.20 AND w_rate >= 0.50 AND w_rate >= b_rate + 0.30
    ORDER BY (w_rate - coalesce(b_rate, 0)) DESC LIMIT 15
    """,
        row_filter="status = 200 AND resp_bytes >= 1024 AND url != '' AND match(url, {compress_regex:String})",
    ),
    "session_harvesting": Detection(
        ("ip", "cookie_session"),
        """
    , win AS (
        SELECT ip, uniqExactIf(cookie_session, is_w) AS w_sessions, countIf(is_w) AS w_reqs,
               uniqExactIf(cookie_session, is_b) AS b_sessions
        FROM base GROUP BY ip HAVING w_sessions >= 20
    )
    SELECT ip, w_sessions, w_reqs, b_sessions FROM win
    WHERE w_sessions > b_sessions * 2 + 10
    ORDER BY w_sessions DESC LIMIT 15
    """,
        row_filter="ip != '' AND cookie_session != ''",
    ),
    "timeout_split": Detection(
        ("oconnect_ms", "ottfb"),
        f"""
    , prepared AS (
        SELECT oconnect_ms, greatest(ottfb / 1000.0 - oconnect_ms, 0) AS read_ms, is_w, is_b FROM base
    ),
    agg AS (
        SELECT {_P95}(oconnect_ms, is_w) AS w_conn, {_P95}(oconnect_ms, is_b) AS b_conn,
               {_P95}(read_ms, is_w) AS w_read, {_P95}(read_ms, is_b) AS b_read,
               countIf(is_w) AS w_total, countIf(is_b) AS b_total
        FROM prepared
    )
    SELECT w_conn, b_conn, w_read, b_read, w_total, b_total FROM agg
    WHERE w_total >= 50 AND b_total >= 100
      AND ((w_conn >= b_conn * 2 AND w_conn - b_conn >= 50) OR (w_read >= b_read * 2 AND w_read - b_read >= 100))
    """,
        row_filter="oconnect_ms IS NOT NULL AND ottfb IS NOT NULL",
    ),
    "waf_signal_spikes": Detection(
        ("signal",),
        """
    SELECT signal, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,
        w_cnt * 1.0 / greatest(b_cnt * 1.0 / {baseline_hours:Float64} * {window_hours:Float64}, 0.5) AS spike_ratio
    FROM base GROUP BY signal
    HAVING w_cnt >= 3 AND w_cnt > b_cnt * 1.0 / {baseline_hours:Float64} * {window_hours:Float64} * 2 + 2
    ORDER BY spike_ratio DESC LIMIT 15
    """,
        row_filter="custom_fields['waf_sig'] != ''",
    ),
    "low_and_slow": Detection(
        ("ip", "url", "sec"),
        """
    SELECT ip, count() AS hits, uniqExact(url) AS distinct_paths, (max(sec) - min(sec)) AS span_s,
           round(count() * 1.0 / nullIf(max(sec) - min(sec), 0), 5) AS rps
    FROM base GROUP BY ip
    HAVING hits >= 5 AND distinct_paths >= 3 AND span_s >= 600
       AND count() * 1.0 / nullIf(max(sec) - min(sec), 0) < 0.2
    ORDER BY distinct_paths DESC, span_s DESC LIMIT 15
    """,
        row_filter="ip != '' AND url != '' AND match(url, {probe_regex:String})",
    ),
    "credential_enumeration": Detection(
        ("ip", "url", "status"),
        """
    SELECT ip,
        countIf(is_w AND status IN (401, 403)) AS w_denied,
        countIf(is_w) AS w_attempts,
        uniqExactIf(url, is_w) AS w_paths,
        countIf(is_b AND status IN (401, 403)) AS b_denied
    FROM base GROUP BY ip
    HAVING w_denied >= 20 AND w_denied * 1.0 / nullIf(w_attempts, 0) >= 0.5
       AND w_denied > b_denied / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 3 + 5
    ORDER BY w_denied DESC LIMIT 15
    """,
        row_filter="ip != '' AND url != '' AND match(url, {auth_regex:String})",
    ),
    "network_asn_health": Detection(
        ("asn", "ploss", "rtt_var", "retrans"),
        f"""
    , health AS (
        SELECT asn,
            avgIf(ploss, is_w) AS w_ploss, avgIf(ploss, is_b) AS b_ploss,
            {_P95}(rtt_var, is_w) AS w_jitter, {_P95}(rtt_var, is_b) AS b_jitter,
            avgIf(retrans, is_w) AS w_retrans, avgIf(retrans, is_b) AS b_retrans,
            countIf(is_w) AS w_total, countIf(is_b) AS b_total
        FROM base GROUP BY asn HAVING w_total >= 50 AND b_total >= 100
    )
    SELECT asn, w_ploss, b_ploss, w_jitter, b_jitter, w_retrans, b_retrans, w_total, b_total FROM health
    WHERE (
            (coalesce(w_ploss, 0) >= 0.02 AND coalesce(w_ploss, 0) >= coalesce(b_ploss, 0) * 2 + 0.005)
         OR (w_jitter >= b_jitter * 2 AND w_jitter - b_jitter >= 20000)
         OR (coalesce(w_retrans, 0) >= coalesce(b_retrans, 0) * 2 + 0.5)
       )
    ORDER BY coalesce(w_ploss, 0) DESC, w_jitter DESC LIMIT 15
    """,
        row_filter="asn IS NOT NULL",
    ),
    "city_surges": Detection(
        ("city", "region", "country"),
        f"""
    SELECT {_CITY_LABEL}, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,
        w_cnt * 1.0 / greatest(b_cnt * 1.0 / greatest({{baseline_hours:Float64}}, 1.0) * {{window_hours:Float64}}, 1.0)
            AS spike_ratio
    FROM base GROUP BY city, region, country
    HAVING w_cnt >= 20 AND w_cnt > b_cnt / greatest({{baseline_hours:Float64}}, 1.0) * {{window_hours:Float64}} * 3
    ORDER BY spike_ratio DESC LIMIT 15
    """,
        row_filter="city != ''",
    ),
    "city_error_spikes": Detection(
        ("city", "region", "country", "status"),
        f"""
    SELECT {_CITY_LABEL},
        countIf(is_w AND status >= 400) * 1.0 / nullIf(countIf(is_w), 0) AS w_rate,
        countIf(is_b AND status >= 400) * 1.0 / nullIf(countIf(is_b), 0) AS b_rate,
        countIf(is_w AND status >= 400) AS w_errors, countIf(is_w) AS w_total, countIf(is_b) AS b_total
    FROM base GROUP BY city, region, country
    HAVING w_total >= 10 AND w_rate >= 0.10 AND (b_total < 50 OR w_rate >= b_rate * 3 + 0.05)
    ORDER BY (w_rate - coalesce(b_rate, 0)) DESC LIMIT 15
    """,
        row_filter="city != ''",
    ),
    "city_latency_regressions": Detection(
        ("city", "region", "country", "elapsed"),
        f"""
    SELECT {_CITY_LABEL},
        {_P95}(elapsed, is_w) / 1000.0 AS w_p95, {_P95}(elapsed, is_b) / 1000.0 AS b_p95,
        countIf(is_w) AS w_total, countIf(is_b) AS b_total
    FROM base GROUP BY city, region, country
    HAVING w_total >= 10 AND b_total >= 50 AND w_p95 >= b_p95 * 3.0 AND w_p95 - b_p95 >= 500
    ORDER BY (w_p95 / nullIf(b_p95, 0)) DESC LIMIT 15
    """,
        row_filter="city != '' AND elapsed IS NOT NULL",
    ),
    "new_city_traffic": Detection(
        ("city", "region", "country"),
        f"""
    SELECT {_CITY_LABEL}, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt
    FROM base GROUP BY city, region, country
    HAVING w_cnt >= 5 AND b_cnt = 0
    ORDER BY w_cnt DESC LIMIT 20
    """,
        row_filter="city != ''",
    ),
    "asn_metro_performance": Detection(
        ("asn", "metro", "tcp_rtt", "country"),
        """
    SELECT asn, metro,
        quantileTDigestIf(0.50)(tcp_rtt, is_w) / 1000.0 AS w_med,
        quantileTDigestIf(0.50)(tcp_rtt, is_b) / 1000.0 AS b_med,
        countIf(is_w) AS w_total, countIf(is_b) AS b_total
    FROM base GROUP BY asn, metro
    HAVING w_total >= 20 AND b_total >= 50 AND w_med >= b_med * 1.5 AND w_med - b_med >= 20
    ORDER BY (w_med - b_med) DESC LIMIT 15
    """,
        row_filter="asn IS NOT NULL AND metro != '' AND tcp_rtt > 0 AND country = 'US'",
    ),
    "cache_ttl_mismatch": Detection(
        ("url", "ttl", "hits", "age"),
        """
    SELECT url AS label, round(avg(ttl), 0) AS avg_ttl, round(avg(hits), 1) AS avg_hits, round(avg(age), 0) AS avg_age,
           count() AS sample_count
    FROM base GROUP BY url
    HAVING sample_count >= 10 AND avg(hits) < 2 AND avg(ttl) > 60
    ORDER BY avg(ttl) DESC LIMIT 20
    """,
        row_filter="ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL",
        window_only=True,
    ),
    "image_optimization_opportunities": Detection(
        ("url", "status", "resp_bytes", "ua"),
        """
    SELECT url, count() AS request_count, sum(resp_bytes) AS total_bytes, round(avg(resp_bytes) / 1024, 1) AS avg_kb,
           (sum(if(ua ILIKE '%Mobi%' OR ua ILIKE '%Android%' OR ua ILIKE '%iPhone%', 1, 0)) * 1.0
            / nullIf(count(), 0)) AS mobile_ratio
    FROM base GROUP BY url
    HAVING total_bytes > 1024 * 512
    ORDER BY total_bytes DESC LIMIT 15
    """,
        row_filter=(
            "status = 200 AND (url ILIKE '%.jpg%' OR url ILIKE '%.jpeg%' OR url ILIKE '%.png%' OR url ILIKE '%.gif%') "
            "AND url NOT ILIKE '%auto=webp%' AND url NOT ILIKE '%format=auto%' "
            "AND url NOT ILIKE '%format=webp%' AND url NOT ILIKE '%format=avif%'"
        ),
        window_only=True,
    ),
    "shield_path_degradation": Detection(
        ("rid", "prid", "pop", "ottfb", "edge"),
        """
    , edge_logs AS (SELECT rid, pop, ottfb, is_w, is_b FROM base WHERE lower(edge) IN ('1', 'true')),
    shield_logs AS (SELECT prid, pop, ottfb FROM base WHERE lower(edge) IN ('0', 'false') AND prid != ''),
    joined AS (
        SELECT e.pop AS edge_pop,
               if(ifNull(s.prid, '') = '', 'Direct to Origin', s.pop) AS shield_pop,
               (e.ottfb - ifNull(s.ottfb, 0)) / 1000.0 AS transit_ms,
               e.is_w AS is_w, e.is_b AS is_b
        FROM edge_logs e LEFT JOIN shield_logs s ON s.prid = e.rid
    )
    SELECT edge_pop, shield_pop,
        quantileTDigestIf(0.50)(transit_ms, is_w) AS w_p50,
        quantileTDigestIf(0.50)(transit_ms, is_b) AS b_p50,
        countIf(is_w) AS w_cnt
    FROM joined GROUP BY edge_pop, shield_pop
    HAVING w_cnt >= 5 AND w_p50 > b_p50 * 1.5
    ORDER BY (w_p50 / nullIf(b_p50, 0)) DESC LIMIT 20
    """,
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
        """
    , win AS (
        SELECT chrome_version, countIf(is_w) AS w_reqs, uniqExactIf(ip, is_w) AS w_ips, countIf(is_b) AS b_reqs
        FROM base GROUP BY chrome_version HAVING w_reqs >= 500
    )
    SELECT concat('Chrome ', chrome_version) AS label, w_reqs, w_ips,
        w_reqs * 1.0 / greatest(b_reqs / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64}, 1.0)
            AS surge_ratio
    FROM win
    WHERE w_reqs > b_reqs / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 2.5
    ORDER BY surge_ratio DESC LIMIT 10
    """,
        row_filter=(
            "positionCaseInsensitive(ua, 'chrome') > 0 AND positionCaseInsensitive(ua, 'bot') = 0 "
            "AND positionCaseInsensitive(ua, 'crawler') = 0 AND positionCaseInsensitive(ua, 'spider') = 0 "
            "AND chrome_version != '' AND toInt32OrZero(chrome_version) < 131"
        ),
    ),
    "orphaned_deep_crawl": Detection(
        ("ip", "url", "referer"),
        """
    , win AS (
        SELECT ip, countIf(is_w) AS w_reqs, uniqExactIf(url, is_w) AS w_distinct_urls, countIf(is_b) AS b_reqs
        FROM base GROUP BY ip HAVING w_reqs >= 50 AND w_distinct_urls >= 10
    )
    SELECT ip AS label, w_reqs, w_distinct_urls, b_reqs, w_reqs * 1.0 / greatest(toFloat64(w_distinct_urls), 1.0) AS requests_per_url,
        w_reqs * 1.0 / greatest(b_reqs / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64}, 1.0)
            AS surge_ratio
    FROM win ORDER BY w_reqs DESC LIMIT 15
    """,
        row_filter="url LIKE '/%/%/%' AND url NOT LIKE '%.%' AND (referer = '' OR referer = '-')",
    ),
    "residential_fingerprint_dispersion": Detection(
        ("ja4", "ip", "asn", "p_type"),
        """
    SELECT ja4 AS label, uniqExactIf(ip, is_w) AS w_ips, uniqExactIf(asn, is_w) AS w_asns, countIf(is_w) AS w_requests,
        uniqExactIf(ip, is_b) AS b_ips,
        w_asns * 1.0 / greatest(toFloat64(uniqExactIf(asn, is_b)), 1.0) AS asn_expansion_ratio
    FROM base GROUP BY ja4
    HAVING w_ips >= 30 AND w_asns >= 5
    ORDER BY asn_expansion_ratio DESC LIMIT 10
    """,
        row_filter="ja4 != '' AND p_type != 'hosting'",
    ),
    "crawler_disparity": Detection(
        ("ua", "referer", "ip"),
        """
    , tagged AS (
        SELECT ip, is_w, is_b,
            (position(lower(ua), 'bot') > 0 OR position(lower(ua), 'crawler') > 0 OR position(lower(ua), 'spider') > 0
             OR position(lower(ua), 'ahrefs') > 0 OR position(lower(ua), 'semrush') > 0) AS is_crawler,
            (position(lower(ua), 'bot') = 0 AND position(lower(ua), 'crawler') = 0
             AND position(lower(ua), 'spider') = 0) AS is_human,
            position(lower(ua), 'claude-user') > 0 AS is_claude_user,
            multiIf(
                position(lower(ua), 'claude-searchbot') > 0, 'Anthropic (Claude)',
                position(lower(ua), 'amzn-searchbot') > 0, 'Amazon (Rufus/Alexa)',
                position(lower(ua), 'gptbot') > 0 OR position(lower(ua), 'chatgpt-user') > 0, 'OpenAI',
                position(lower(ua), 'semrushbot') > 0, 'Semrush',
                position(lower(ua), 'ahrefsbot') > 0, 'Ahrefs',
                'Other Crawler') AS crawler_brand,
            multiIf(
                position(lower(referer), 'claude.ai') > 0, 'Anthropic (Claude)',
                position(lower(referer), 'amazon.com') > 0 OR position(lower(referer), 'amazon.co') > 0,
                    'Amazon (Rufus/Alexa)',
                position(lower(referer), 'openai.com') > 0 OR position(lower(referer), 'chatgpt.com') > 0, 'OpenAI',
                position(lower(referer), 'semrush.com') > 0, 'Semrush',
                position(lower(referer), 'ahrefs.com') > 0, 'Ahrefs',
                '') AS ref_brand
        FROM base
    ),
    crawler_traffic AS (
        SELECT crawler_brand AS bot_brand, countIf(is_w) AS w_crawls, countIf(is_b) AS b_crawls
        FROM tagged WHERE is_crawler GROUP BY crawler_brand HAVING w_crawls >= 100
    ),
    referred AS (
        SELECT if(is_claude_user, 'Anthropic (Claude)', if(ref_brand = '', 'Other Crawler', ref_brand)) AS bot_brand,
               uniqExactIf(ip, is_w) AS w_referrals, uniqExactIf(ip, is_b) AS b_referrals
        FROM tagged WHERE is_human
        GROUP BY bot_brand HAVING bot_brand IN (SELECT bot_brand FROM crawler_traffic)
    )
    SELECT c.bot_brand AS label, c.w_crawls, ifNull(r.w_referrals, 0) AS w_referrals,
        c.w_crawls * 1.0 / greatest(ifNull(r.w_referrals, 0), 1) AS crawls_per_referral,
        c.b_crawls * 1.0 / greatest(ifNull(r.b_referrals, 0), 1) AS baseline_crawls_per_referral
    FROM crawler_traffic c LEFT JOIN referred r ON c.bot_brand = r.bot_brand
    WHERE c.w_crawls >= 100
      AND (ifNull(r.bot_brand, '') = '' OR c.w_crawls * 1.0 / greatest(ifNull(r.w_referrals, 0), 1) >= 1000.0)
    ORDER BY crawls_per_referral DESC LIMIT 10
    """,
        row_filter="ua != ''",
    ),
}


def botnet_grouping(fingerprint_column: str) -> Detection:
    """Botnet grouping keyed on the JA4 fingerprint when logged, else JA3."""
    if fingerprint_column not in ("ja4", "ja3"):
        raise ValueError("fingerprint column must be ja4 or ja3")
    return Detection(("ip", fingerprint_column), _fingerprint_spread(fingerprint_column, with_ratio=True))
