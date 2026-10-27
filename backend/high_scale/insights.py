"""High-scale anomaly & insight detections and queries."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

import contextvars
import urllib.parse
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from backend.core.clickhouse_client import ClickHouseError
from backend.core.share_db.validation import mask_ip
from backend.high_scale._common import (
    build_clickhouse_filters,
    visible_fact_clauses,
)
from backend.high_scale._common import format_iso_utc_or_empty as _iso_or_empty
from backend.models.insights import CacheCollapseDetailResponse, InsightsResponse
from backend.repositories._sql.insights import NEW_PROBES
from backend.repositories.insights import InsightDefinition, registry  # noqa: F401  (registers the detections)
from backend.repositories.utils.response_cache import (
    bucket_time_to_minute,
)
from backend.repositories.utils.response_shaping import (
    build_cache_collapse_breakdown,
    build_insight_asn_context,
    build_insights_envelope,
    select_insight_bucket_interval,
    shape_cache_collapse_timeline_point,
)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc

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
PROBE_REGEX = "(?i)" + "|".join(re.escape(path) for path in NEW_PROBES)
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
    window_only: bool = False
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
    return _BaseGroupQuerySpec(
        select_cols=select_cols,
        group_by=group_by,
        where=where,
        having=having,
        order_by=order_by,
        limit=limit,
        from_table=from_table,
    ).build_sql()


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
    totals_cols = "t.w_total, t.b_total" if w_first else "t.b_total, t.w_total"
    cnt_clause = f" AND g.w_cnt >= {min_w_cnt}" if min_w_cnt else ""
    return f"""
    , totals AS (SELECT countIf(is_w) AS w_total, countIf(is_b) AS b_total FROM base),
    grp AS (
        SELECT {column}, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt
        FROM base {extra_row} GROUP BY {column}
    )
    SELECT {sel_cols}, g.w_cnt, g.b_cnt, {totals_cols}
    FROM grp g CROSS JOIN totals t
    WHERE t.w_total > 0{cnt_clause} AND g.w_cnt * 1.0 / t.w_total >= {share_floor}
      AND (t.b_total = 0 OR g.w_cnt * 1.0 / t.w_total >= g.b_cnt * 1.0 / nullIf(t.b_total, 0) * {multiplier} + {delta})
    ORDER BY g.w_cnt DESC LIMIT {limit}
    """


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
    return _base_group_sql(
        f"{select_col},\n"
        f"        {_P95}(elapsed, is_w) / 1000.0 AS w_p95,\n"
        f"        {_P95}(elapsed, is_b) / 1000.0 AS b_p95,\n"
        f"        countIf(is_w) AS w_total,\n"
        f"        countIf(is_b) AS b_total{extra_select}",
        group_by or select_col,
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
    return _base_group_sql(
        f"{select_col},\n"
        f"        countIf(is_w AND status >= {status_min}) * 1.0 / nullIf(countIf(is_w), 0) AS w_rate,\n"
        f"        countIf(is_b AND status >= {status_min}) * 1.0 / nullIf(countIf(is_b), 0) AS b_rate,\n"
        f"        countIf(is_w AND status >= {status_min}) AS w_errors,\n"
        f"        countIf(is_w) AS w_total,\n"
        f"        countIf(is_b) AS b_total",
        group_by or select_col,
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
        f"url,\n"
        f"        countIf(is_w AND {cond}) * 1.0 / nullIf(countIf({w_denom}), 0) AS w_rate,\n"
        f"        countIf(is_b AND {cond}) * 1.0 / nullIf(countIf({b_denom}), 0) AS b_rate,\n"
        f"        countIf({w_denom}) AS w_total,\n"
        f"        countIf({b_denom}) AS b_total",
        "url",
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
    "error_spikes": Detection(("url", "status"), _error_spike("url")),
    "new_country_traffic": Detection(("country",), _new_traffic("country", 3, where="country != ''")),
    "ua_monoculture": Detection(("ua",), _share_spike("ua", "0.25")),
    "new_probe_urls": Detection(
        ("url", "status"),
        _base_group_sql(
            "url, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,\n"
            "        avgIf(if(status >= 400, 1.0, 0.0), is_w) * 100 AS w_error_pct",
            "url",
            where="url != '' AND match(url, {probe_regex:String})",
            having="w_cnt > 0 AND b_cnt = 0",
            order_by="w_cnt DESC",
            limit=25,
        ),
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
    "cacheability_regression": Detection(
        ("url", "cache"),
        _rate_regression("startsWith(cache, 'PASS')"),
    ),
    "latency_regression": Detection(
        ("url", "elapsed"),
        _p95_regression("url", min_w=5, min_b=20, ratio=2.0, min_delta_ms=200),
        row_filter="elapsed IS NOT NULL",
    ),
    "tail_latency": Detection(
        ("url", "elapsed"),
        _base_group_sql(
            "url,\n"
            "        round(quantileTDigest(0.99)(elapsed) / 1000.0, 0) AS p99_ms,\n"
            "        round(quantileTDigest(0.50)(elapsed) / 1000.0, 0) AS p50_ms,\n"
            "        round(quantileTDigest(0.99)(elapsed) / nullIf(quantileTDigest(0.50)(elapsed), 0), 1) AS ratio,\n"
            "        count() AS total",
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
            extra_select=(
                ",\n        if(countIf(is_w AND ottfb IS NOT NULL) >= 20,\n"
                "           round(quantileTDigestIf(0.95)(ottfb, is_w AND ottfb IS NOT NULL) / 1000.0, 0), NULL) AS ottfb_p95"
            ),
        ),
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
        _base_group_sql(
            "ip,\n"
            "        countIf(is_w AND status = 404) AS w_404,\n"
            "        countIf(is_w) AS w_total,\n"
            "        uniqExactIf(url, is_w AND status = 404) AS w_distinct_404,\n"
            "        countIf(is_b AND status = 404) AS b_404",
            "ip",
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
            "asn,\n"
            "        countIf(is_w AND p_type = 'hosting') AS w_hosting,\n"
            "        countIf(is_w) AS w_total,\n"
            "        countIf(is_b AND p_type = 'hosting') AS b_hosting,\n"
            "        countIf(is_b) AS b_total",
            "asn",
            having="w_total >= 50 AND (w_hosting * 1.0 / w_total) >= 0.3\n"
            "       AND (b_total = 0 OR (w_hosting * 1.0 / w_total) >= (b_hosting * 1.0 / nullIf(b_total, 0)) * 3 + 0.10)",
            order_by="w_hosting DESC",
            limit=15,
        ),
        row_filter="asn IS NOT NULL AND p_type != ''",
    ),
    "metro_delivery_degradation": Detection(
        ("metro", "delivery_rate"),
        _base_group_sql(
            "metro,\n"
            "        quantileTDigestIf(0.5)(delivery_rate, is_w) AS w_med,\n"
            "        quantileTDigestIf(0.5)(delivery_rate, is_b) AS b_med,\n"
            "        countIf(is_w) AS w_total,\n"
            "        countIf(is_b) AS b_total",
            "metro",
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
        ("pop", "elapsed"),
        _p95_regression("pop"),
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
        _rate_regression(
            "uncompressed",
            from_table=(
                "(\n"
                "        SELECT url, is_w, is_b,\n"
                "               NOT match(lower(resp_header_content_encoding), 'gzip|br|zstd|deflate') AS uncompressed\n"
                "        FROM base\n"
                "    )"
            ),
        ),
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
        _base_group_sql(
            "signal, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,\n"
            "        w_cnt * 1.0 / greatest(b_cnt * 1.0 / {baseline_hours:Float64} * {window_hours:Float64}, 0.5) AS spike_ratio",
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
            "ip, count() AS hits, uniqExact(url) AS distinct_paths, (max(sec) - min(sec)) AS span_s,\n"
            "           round(count() * 1.0 / nullIf(max(sec) - min(sec), 0), 5) AS rps",
            "ip",
            having="hits >= 5 AND distinct_paths >= 3 AND span_s >= 600\n"
            "       AND count() * 1.0 / nullIf(max(sec) - min(sec), 0) < 0.2",
            order_by="distinct_paths DESC, span_s DESC",
            limit=15,
        ),
        row_filter="ip != '' AND url != '' AND match(url, {probe_regex:String})",
    ),
    "credential_enumeration": Detection(
        ("ip", "url", "status"),
        _base_group_sql(
            "ip,\n"
            "        countIf(is_w AND status IN (401, 403)) AS w_denied,\n"
            "        countIf(is_w) AS w_attempts,\n"
            "        uniqExactIf(url, is_w) AS w_paths,\n"
            "        countIf(is_b AND status IN (401, 403)) AS b_denied",
            "ip",
            having="w_denied >= 20 AND w_denied * 1.0 / nullIf(w_attempts, 0) >= 0.5\n"
            "       AND w_denied > b_denied / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 3 + 5",
            order_by="w_denied DESC",
            limit=15,
        ),
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
        _base_group_sql(
            f"{_CITY_LABEL}, countIf(is_w) AS w_cnt, countIf(is_b) AS b_cnt,\n"
            "        w_cnt * 1.0 / greatest(b_cnt * 1.0 / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64}, 1.0)\n"
            "            AS spike_ratio",
            "city, region, country",
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
            "asn, metro,\n"
            "        quantileTDigestIf(0.50)(tcp_rtt, is_w) / 1000.0 AS w_med,\n"
            "        quantileTDigestIf(0.50)(tcp_rtt, is_b) / 1000.0 AS b_med,\n"
            "        countIf(is_w) AS w_total, countIf(is_b) AS b_total",
            "asn, metro",
            having="w_total >= 20 AND b_total >= 50 AND w_med >= b_med * 1.5 AND w_med - b_med >= 20",
            order_by="(w_med - b_med) DESC",
            limit=15,
        ),
        row_filter="asn IS NOT NULL AND metro != '' AND tcp_rtt > 0 AND country = 'US'",
    ),
    "cache_ttl_mismatch": Detection(
        ("url", "ttl", "hits", "age"),
        _base_group_sql(
            "url AS label, round(avg(ttl), 0) AS avg_ttl, round(avg(hits), 1) AS avg_hits, round(avg(age), 0) AS avg_age,\n"
            "           count() AS sample_count",
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
            "url, count() AS request_count, sum(resp_bytes) AS total_bytes, round(avg(resp_bytes) / 1024, 1) AS avg_kb,\n"
            "           (sum(if(ua ILIKE '%Mobi%' OR ua ILIKE '%Android%' OR ua ILIKE '%iPhone%', 1, 0)) * 1.0\n"
            "            / nullIf(count(), 0)) AS mobile_ratio",
            "url",
            having="total_bytes > 1024 * 512",
            order_by="total_bytes DESC",
            limit=15,
        ),
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
        _base_group_sql(
            "ja4 AS label, uniqExactIf(ip, is_w) AS w_ips, uniqExactIf(asn, is_w) AS w_asns, countIf(is_w) AS w_requests,\n"
            "        uniqExactIf(ip, is_b) AS b_ips,\n"
            "        w_asns * 1.0 / greatest(toFloat64(uniqExactIf(asn, is_b)), 1.0) AS asn_expansion_ratio",
            "ja4",
            having="w_ips >= 30 AND w_asns >= 5",
            order_by="asn_expansion_ratio DESC",
            limit=10,
        ),
        row_filter="ja4 != '' AND p_type != 'hosting'",
    ),
    "crawler_disparity": Detection(
        ("ua", "referer", "ip", "resp_bytes"),
        """
    , tagged AS (
        SELECT
            ip,
            is_w,
            is_b,
            coalesce(resp_bytes, 0) AS r_bytes,
            extract(
                lower(ua),
                '([a-z0-9_.-]*(?:bot|crawler|spider|slurp|archiver|scraper|externalagent|extended|other|preview)[a-z0-9_.-]*|headless[a-z0-9_.-]*|python-[a-z0-9_.-]+|go-http-client|curl|wget|scrapy|aiohttp|httpx|okhttp|axios)'
            ) AS raw_tok,
            extract(lower(ua), 'https?://(?:www\\.)?([^/.:;\\s)]+)') AS ua_dom,
            extract(lower(ua), '([a-z0-9]+)-(?:user|externalfetcher)') AS user_fetcher_stem,
            extract(lower(referer), 'https?://(?:www\\.)?([^/.:;\\s]+)') AS ref_dom
        FROM base
    ),
    classified AS (
        SELECT
            ip,
            is_w,
            is_b,
            r_bytes,
            raw_tok,
            ua_dom,
            (raw_tok != '') AS is_crawler,
            (raw_tok = '') AS is_human,
            if(
                replaceRegexpOne(
                    raw_tok,
                    '(-searchbot|-assistbot|-externalagent|-extended|-user|searchbot|assistbot|externalagent|extended|crawler|spider|slurp|archiver|scraper|preview|other|bot).*$',
                    ''
                ) != '',
                replaceRegexpOne(
                    raw_tok,
                    '(-searchbot|-assistbot|-externalagent|-extended|-user|searchbot|assistbot|externalagent|extended|crawler|spider|slurp|archiver|scraper|preview|other|bot).*$',
                    ''
                ),
                raw_tok
            ) AS bot_stem,
            if(user_fetcher_stem != '', user_fetcher_stem, ref_dom) AS ref_stem
        FROM tagged
    ),
    site_win AS (
        SELECT greatest(sumIf(r_bytes, is_w), 1) AS total_site_bytes FROM classified
    ),
    crawler_traffic AS (
        SELECT
            bot_stem,
            min(raw_tok) AS raw_tok,
            max(ua_dom) AS ua_dom,
            countIf(is_w) AS w_crawls,
            countIf(is_b) AS b_crawls,
            sumIf(r_bytes, is_w) AS w_bytes,
            sumIf(r_bytes, is_b) AS b_bytes
        FROM classified
        WHERE is_crawler AND bot_stem != ''
        GROUP BY bot_stem
        HAVING w_crawls >= 50
    ),
    human_refs AS (
        SELECT ip, is_w, is_b, ref_stem
        FROM classified
        WHERE is_human AND ip != '' AND ref_stem != ''
    ),
    referred AS (
        SELECT
            c.bot_stem AS bot_stem,
            uniqExactIf(h.ip, h.is_w) AS w_referrals,
            uniqExactIf(h.ip, h.is_b) AS b_referrals
        FROM crawler_traffic c
        CROSS JOIN human_refs h
        WHERE c.bot_stem = h.ref_stem
           OR (
               length(replaceRegexpAll(c.bot_stem, '[aeiou]', '')) >= 3
               AND replaceRegexpAll(c.bot_stem, '[aeiou]', '') = replaceRegexpAll(h.ref_stem, '[aeiou]', '')
           )
           OR (
               length(c.bot_stem) >= 3 AND length(h.ref_stem) >= 3
               AND (position(h.ref_stem, c.bot_stem) > 0 OR position(c.bot_stem, h.ref_stem) > 0)
           )
           OR (c.ua_dom != '' AND c.ua_dom = h.ref_stem)
        GROUP BY c.bot_stem
    )
    SELECT c.raw_tok AS label, c.w_crawls, ifNull(r.w_referrals, 0) AS w_referrals, c.w_bytes,
        c.w_crawls * 1.0 / greatest(ifNull(r.w_referrals, 0), 1) AS crawls_per_referral,
        c.b_crawls * 1.0 / greatest(ifNull(r.b_referrals, 0), 1) AS baseline_crawls_per_referral,
        c.w_bytes * 1.0 / greatest(ifNull(r.w_referrals, 0), 1) AS bytes_per_referral,
        c.b_bytes * 1.0 / greatest(ifNull(r.b_referrals, 0), 1) AS baseline_bytes_per_referral,
        round(c.w_bytes * 100.0 / s.total_site_bytes, 2) AS bandwidth_share_pct
    FROM crawler_traffic c CROSS JOIN site_win s LEFT JOIN referred r ON c.bot_stem = r.bot_stem
    WHERE c.w_crawls >= 50
      AND (ifNull(r.w_referrals, 0) = 0 OR c.w_crawls * 1.0 / greatest(ifNull(r.w_referrals, 0), 1) >= 100.0 OR c.w_bytes * 1.0 / greatest(ifNull(r.w_referrals, 0), 1) >= 1048576.0)
    ORDER BY crawls_per_referral DESC, c.w_bytes DESC LIMIT 10
    """,
        row_filter="ua != ''",
    ),
}


def botnet_grouping(fingerprint_column: str) -> Detection:
    """Botnet grouping keyed on the JA4 fingerprint when logged, else JA3."""
    if fingerprint_column not in ("ja4", "ja3"):
        raise ValueError("fingerprint column must be ja4 or ja3")
    return Detection(("ip", fingerprint_column), _fingerprint_spread(fingerprint_column, with_ratio=True))


_MAX_WORKERS = 12
_FIELD_SAMPLE_ROWS = 1000
_BASE_FIELDS = frozenset({"timestamp", "url", "country", "ip"})
_ASN_INSIGHT_MARKERS = ("asn", "metro")
_INSIGHTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=300)
_CacheRegistry.register("high_scale.insights._INSIGHTS_CACHE", _INSIGHTS_CACHE)


def _window(
    window_hours: float, baseline_hours: float, clamp_start: str | None, clamp_end: str | None
) -> tuple[datetime, datetime, datetime]:
    """``(baseline_start, window_start, now)``, honouring the analyst clamp exactly like the standard path."""
    now = datetime.now(UTC)
    if clamp_end:
        ceiling = parse_iso_utc(clamp_end)
        if ceiling and ceiling < now:
            now = ceiling
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(hours=baseline_hours + window_hours)
    if clamp_start:
        floor = parse_iso_utc(clamp_start)
        if floor:
            baseline_start = max(baseline_start, floor)
            window_start = max(window_start, floor)
    return baseline_start, window_start, now


def _base_sql(detection: Detection, insight_id: str) -> str:
    projection = ", ".join(f"{COLUMNS[column]} AS {column}" for column in detection.columns)
    lower = "{window_start:DateTime64(3)}" if detection.window_only else "{baseline_start:DateTime64(3)}"
    row_filter = f" AND ({detection.row_filter})" if detection.row_filter else ""
    scope = " AND ".join(visible_fact_clauses("request"))
    return (
        f"/* insight:{insight_id} */ WITH base AS (SELECT {projection}, "
        "event_timestamp < {window_start:DateTime64(3)} AS is_b, "
        "event_timestamp >= {window_start:DateTime64(3)} AS is_w "
        f"FROM request_facts WHERE {scope} AND event_timestamp >= {lower} "
        f"AND event_timestamp <= {{window_end:DateTime64(3)}}{row_filter}) {detection.sql}"
    )


def _fingerprint_column(present: set[str]) -> str | None:
    for column in ("ja4", "ja3"):
        if column in present:
            return column
    return None


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


def _make_investigate_url(
    service_id: str, window_start: datetime, now: datetime
) -> Callable[[dict[str, Any] | None], str]:
    def build(filters: dict[str, Any] | None = None) -> str:
        pairs: list[tuple[str, str]] = [
            ("service", service_id),
            ("start", window_start.isoformat()),
            ("end", now.isoformat()),
        ]
        pairs.extend((f"filter_{column}", str(value)) for column, value in (filters or {}).items() if value is not None)
        return "/dashboard?" + urllib.parse.urlencode(pairs)

    return build


def _severity(items: list[dict[str, Any]]) -> str:
    if not items:
        return "clean"
    return "critical" if any(item.get("severity") == "critical" for item in items) else "warning"


def _summary(definition: InsightDefinition, items: list[dict[str, Any]]) -> str:
    if not items:
        return f"No {definition.title.lower()} detected"
    if definition.id == "error_spikes":
        return f"{len(items)} URLs with elevated server error rates"
    if definition.id == "botnet_grouping":
        return f"{len(items)} fingerprints with suspicious IP spread"
    return f"{len(items)} anomalies detected"


def _card(definition: InsightDefinition, severity: str, summary: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": definition.id,
        "title": definition.title,
        "description": definition.description,
        "category": str(definition.category),
        "severity": severity,
        "summary": summary,
        "items": items,
    }


def _failure_message(exc: Exception) -> str:
    return str(exc) if isinstance(exc, ClickHouseError) else type(exc).__name__


def _asn_context(service_id: str, rows: list[tuple[Any, ...]], insight_id: str) -> dict[str, Any]:
    if not rows or not any(marker in insight_id for marker in _ASN_INSIGHT_MARKERS):
        return {}
    return build_insight_asn_context(service_id, rows, insight_id, include_dma="metro" in insight_id)


def insights_endpoint(
    service: HighScaleService,
    req: Any,
    clamp_start: str | None,
    clamp_end: str | None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
) -> InsightsResponse:
    window_hours = float(req.window_size_hrs)
    baseline_hours = float(req.baseline_hours)
    req_filters = getattr(req, "filters", None)
    filter_clause, filter_params = build_clickhouse_filters(req_filters) if req_filters else ("", {})
    if clamp_cache_key is not None:
        key_clamp = clamp_cache_key
    elif clamp_start or clamp_end:
        key_clamp = f"{bucket_time_to_minute(clamp_start) or ''}:{bucket_time_to_minute(clamp_end) or ''}"
    else:
        key_clamp = ""
    cache_key = (
        service.service_id,
        id(service.client),
        window_hours,
        baseline_hours,
        key_clamp,
        int(mask_ips),
        filter_clause,
        tuple(sorted((k, str(v)) for k, v in filter_params.items())),
    )
    from backend.high_scale.ingest_controller import HighScaleService

    use_cache = isinstance(service, HighScaleService)
    if use_cache and not force_refresh:
        cached = _INSIGHTS_CACHE.get(cache_key)
        if cached is not None:
            return InsightsResponse.with_telemetry(**cached, is_cached=True, cache_age_seconds=0)
    baseline_start, window_start, now = _window(window_hours, baseline_hours, clamp_start, clamp_end)
    scope = " AND ".join(visible_fact_clauses("request"))
    if filter_clause:
        scope = f"{scope} AND ({filter_clause})"
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "baseline_start": baseline_start,
        "window_start": window_start,
        "window_end": now,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "probe_regex": PROBE_REGEX,
        "compress_regex": COMPRESSIBLE_REGEX,
        "auth_regex": AUTH_REGEX,
        "bot_ua_regex": BOT_UA_REGEX,
        **filter_params,
    }
    in_range = "event_timestamp >= {baseline_start:DateTime64(3)} AND event_timestamp <= {window_end:DateTime64(3)}"
    history_sql = (
        "/* insight:history */ SELECT min(event_timestamp) AS earliest, "
        "countIf(event_timestamp >= {window_start:DateTime64(3)}) AS window_total "
        f"FROM request_facts WHERE {scope} AND {in_range}"
    )
    sample_sql = (
        "/* insight:fields */ SELECT groupUniqArrayArray(mapKeys(custom_fields)) AS fields FROM ("
        f"SELECT custom_fields FROM request_facts WHERE {scope} AND {in_range} "
        f"ORDER BY event_timestamp DESC LIMIT {_FIELD_SAMPLE_ROWS})"
    )
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-insights-meta") as meta_pool:
        fut_history = meta_pool.submit(
            contextvars.copy_context().run, lambda: service.client.execute(history_sql, params)
        )
        fut_sample = meta_pool.submit(
            contextvars.copy_context().run, lambda: service.client.execute(sample_sql, params)
        )
        history = fut_history.result()
        sample = fut_sample.result()
    earliest = history[0].get("earliest") if history else None
    window_total = int(history[0].get("window_total") or 0) if history else 0
    earliest_dt = parse_iso_utc(earliest) if isinstance(earliest, str) else earliest
    if isinstance(earliest_dt, datetime):
        earliest_dt = earliest_dt.astimezone(UTC) if earliest_dt.tzinfo else earliest_dt.replace(tzinfo=UTC)
        history_hours = (now - earliest_dt).total_seconds() / 3600.0
    else:
        history_hours = 0.0
    present = set(_BASE_FIELDS)
    present.update(sample[0].get("fields") or [] if sample else [])
    fingerprint = _fingerprint_column(present) or "ja3"
    make_investigate_url = _make_investigate_url(service.service_id, window_start, now)

    def run(definition: InsightDefinition, detection: Detection) -> dict[str, Any]:
        if detection.needs_baseline and history_hours < baseline_hours:
            available = max(0.1, round(history_hours, 1))
            return _card(
                definition,
                "info",
                f"Requires {int(baseline_hours)}h of historical data (only {available}h available)",
                [],
            )
        try:
            sql = _base_sql(detection, definition.id)
            if filter_clause:
                sql = sql.replace(
                    "AND event_timestamp <= {window_end:DateTime64(3)}",
                    f"AND event_timestamp <= {{window_end:DateTime64(3)}} AND ({filter_clause})",
                )
            rows = [tuple(row.values()) for row in service.client.execute(sql, params)]
            context = {
                "window_hours": window_hours,
                "baseline_hours": baseline_hours,
                "fp_col": fingerprint,
                "actual_cols": sorted(present),
                "mask_ips": mask_ips,
                **_asn_context(service.service_id, rows, definition.id),
            }
            items: list[dict[str, Any]] = []
            for row in rows:
                item = definition.row_processor(row, definition, context) if definition.row_processor else {}
                if "investigate_url" not in item:
                    item["investigate_url"] = make_investigate_url(item.get("meta", {}).get("filters"))
                items.append(item)
        except Exception as exc:
            return _card(definition, "error", f"Query failed: {_failure_message(exc)}", [])
        severity = definition.severity_logic(items) if items and definition.severity_logic else _severity(items)
        return _card(definition, severity, _summary(definition, items), items)

    candidates: list[tuple[InsightDefinition, Detection]] = []
    for definition in registry.get_all():
        detection = _detection_for(definition.id, present)
        if detection is not None and all(column in present for column in definition.required_fields):
            candidates.append((definition, detection))
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="hs-insights") as pool:
        futures = [pool.submit(contextvars.copy_context().run, lambda pair=pair: run(*pair)) for pair in candidates]
        cards = [future.result() for future in futures]
    computed_at = now.isoformat()
    payload = build_insights_envelope(
        cards,
        window_start=window_start.isoformat(),
        window_end=computed_at,
        baseline_start=baseline_start.isoformat(),
        baseline_end=window_start.isoformat(),
        computed_at=computed_at,
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        window_total_requests=window_total,
    )
    if use_cache and not any(c.get("severity") == "error" for c in cards):
        _INSIGHTS_CACHE[cache_key] = payload
    return InsightsResponse.with_telemetry(**payload)


def _bucket_interval(total_hours: float) -> str:
    return select_insight_bucket_interval(total_hours, clickhouse=True)


def cache_collapse_detail_endpoint(
    service: HighScaleService, req: Any, clamp_start: str | None, clamp_end: str | None, mask_ips: bool = False
) -> CacheCollapseDetailResponse:
    """Hit/miss timeline, disposition breakdown and recent misses for one URL, as on the standard path."""
    window_hours = float(req.window_size_hrs)
    baseline_hours = float(req.baseline_hours)
    baseline_start, window_start, now = _window(window_hours, baseline_hours, clamp_start, clamp_end)
    scope = " AND ".join(
        [
            *visible_fact_clauses("request"),
            "url = {url:String}",
            "event_timestamp >= {baseline_start:DateTime64(3)}",
            "event_timestamp <= {window_end:DateTime64(3)}",
        ]
    )
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "url": req.url,
        "baseline_start": baseline_start,
        "window_start": window_start,
        "window_end": now,
    }
    cache = "custom_fields['cache']"
    hit, miss, passed = (f"startsWith({cache}, '{kind}')" for kind in ("HIT", "MISS", "PASS"))
    rates = service.client.execute(
        "/* insight:cache_collapse_rates */ SELECT "
        f"countIf(is_w AND {hit}) AS w_hits, countIf(is_w AND {miss}) AS w_miss, "
        f"countIf(is_w AND {passed}) AS w_pass, countIf(is_w) AS w_total, "
        f"countIf(is_b AND {hit}) AS b_hits, countIf(is_b AND {miss}) AS b_miss, "
        f"countIf(is_b AND {passed}) AS b_pass, countIf(is_b) AS b_total "
        "FROM (SELECT custom_fields, event_timestamp >= {window_start:DateTime64(3)} AS is_w, "
        "event_timestamp < {window_start:DateTime64(3)} AS is_b "
        f"FROM request_facts WHERE {scope})",
        params,
    )[0]
    w_hits, w_miss, w_pass, w_total, b_hits, b_miss, b_pass, b_total = (
        int(rates[key] or 0)
        for key in ("w_hits", "w_miss", "w_pass", "w_total", "b_hits", "b_miss", "b_pass", "b_total")
    )
    w_cacheable, b_cacheable = w_hits + w_miss, b_hits + b_miss
    w_rate = w_hits / w_cacheable if w_cacheable else 0.0
    b_rate = b_hits / b_cacheable if b_cacheable else 0.0
    timeline_rows = service.client.execute(
        "/* insight:cache_collapse_timeline */ SELECT "
        f"toStartOfInterval(event_timestamp, {_bucket_interval(window_hours + baseline_hours)}) AS bucket, "
        f"count() AS total_requests, countIf({hit}) AS real_hits, countIf({miss}) AS misses "
        f"FROM (SELECT event_timestamp, custom_fields FROM request_facts WHERE {scope}) "
        "GROUP BY bucket ORDER BY bucket ASC",
        params,
    )
    timeline = [
        shape_cache_collapse_timeline_point(
            _iso_or_empty(row["bucket"]),
            row["total_requests"],
            row["real_hits"],
            row["misses"],
            b_rate,
        )
        for row in timeline_rows
    ]
    miss_rows = service.client.execute(
        "/* insight:cache_collapse_misses */ SELECT event_timestamp AS timestamp, "
        f"{cache} AS cache, custom_fields['pop'] AS pop, client_ip AS ip, toInt32OrNull(custom_fields['status']) AS status "
        f"FROM request_facts WHERE {scope} AND {miss} ORDER BY event_timestamp DESC LIMIT 50",
        params,
    )
    recent_misses = [
        {
            "timestamp": _iso_or_empty(row["timestamp"]),
            "cache": row.get("cache") or "UNKNOWN",
            "pop": row.get("pop") or None,
            "ip": mask_ip(row["ip"]) if mask_ips and row.get("ip") else row.get("ip"),
            "status": int(row["status"]) if row.get("status") is not None else None,
        }
        for row in miss_rows
    ]
    return CacheCollapseDetailResponse.with_telemetry(
        url=req.url,
        has_data=bool(w_total + b_total),
        timeline=timeline,
        recent_misses=recent_misses,
        breakdown=build_cache_collapse_breakdown(w_hits, w_miss, w_pass, w_total),
        baseline_hit_rate=float(b_rate) * 100,
        window_hit_rate=float(w_rate) * 100,
        baseline_pass_rate=float(b_pass / b_total) * 100 if b_total else 0.0,
        window_pass_rate=float(w_pass / w_total) * 100 if w_total else 0.0,
    )


_self_mod = sys.modules[__name__]
sys.modules["backend.high_scale.insight_queries"] = _self_mod
if (_hs_pkg := sys.modules.get("backend.high_scale")) is not None:
    setattr(_hs_pkg, "insights", _self_mod)
    setattr(_hs_pkg, "insight_queries", _self_mod)
