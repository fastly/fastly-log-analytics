from __future__ import annotations

import re
from typing import Any

from backend.core.metric_expressions import coalesce
from backend.core.share_db import mask_ip
from backend.repositories.insights import InsightCategory, InsightDefinition, registry  # noqa: F401
from backend.utils.countries import format_city_label  # noqa: F401


def _reg(**kwargs: Any) -> None:
    registry.register(InsightDefinition(**kwargs))


NEW_PROBES = [
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
]
NEW_PROBE_REGEX: str = "|".join(re.escape(p) for p in NEW_PROBES)

_BOT_UA_TOKENS = [
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
]
REPEATED_BOT_UA_REGEX: str = "|".join(re.escape(t) for t in _BOT_UA_TOKENS)

_AUTH_PATHS = [
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
]
AUTH_PATHS_REGEX: str = "|".join(re.escape(p) for p in _AUTH_PATHS)

_COMPRESSIBLE_EXTS = [".js", ".css", ".html", ".json", ".svg", ".xml", ".txt"]
COMPRESSIBLE_REGEX: str = "|".join(re.escape(e) for e in _COMPRESSIBLE_EXTS)


def _build_ip_spread_sql(col: str, *, include_ratio: bool = False) -> str:
    ratio_expr = f"""w_ips * 1.0 / GREATEST({coalesce("b_ips", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}}, 1)"""
    extra_sel = f",\n            {ratio_expr} AS ip_ratio" if include_ratio else ""
    order_by = "ip_ratio" if include_ratio else f"({ratio_expr})"
    return f"""
        WITH base AS (
            SELECT "{col}", "ip",
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} WHERE "{col}" IS NOT NULL AND "{col}" != ''
        ),
        agg AS (
            SELECT "{col}",
                COUNT(DISTINCT "ip") FILTER (WHERE is_w) AS w_ips,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT "ip") FILTER (WHERE is_b) AS b_ips
            FROM base GROUP BY "{col}"
            HAVING COUNT(DISTINCT "ip") FILTER (WHERE is_w) >= 5
        )
        SELECT "{col}", w_ips, w_reqs, {coalesce("b_ips", "0")} AS b_ips{extra_sel}
        FROM agg
        WHERE w_ips > {coalesce("b_ips", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}} * 3
        ORDER BY {order_by} DESC LIMIT 10
    """


def _build_new_dim_traffic_sql(col: str, min_w_cnt: int) -> str:
    return f"""
        SELECT "{col}",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
        FROM {{table_name}} WHERE "{col}" IS NOT NULL GROUP BY "{col}"
        HAVING w_cnt >= {min_w_cnt} AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 20
    """


def _build_monoculture_sql(col: str, share_floor: str, where_clause: str = "") -> str:
    and_where = f" AND ({where_clause})" if where_clause else ""
    return f"""
        WITH grp AS (
            SELECT "{col}",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {{table_name}} GROUP BY "{col}"
        ),
        with_totals AS (
            SELECT "{col}", w_cnt, b_cnt,
                CAST({coalesce("SUM(w_cnt) OVER ()", "0")} AS BIGINT) AS w_total,
                CAST({coalesce("SUM(b_cnt) OVER ()", "0")} AS BIGINT) AS b_total
            FROM grp
        )
        SELECT "{col}", w_cnt, b_cnt, b_total, w_total
        FROM with_totals
        WHERE w_total > 0{and_where}
          AND w_cnt * 1.0 / w_total >= {share_floor}
          AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 3 + 0.10)
        ORDER BY w_cnt DESC LIMIT 10
    """


def _build_p95_latency_regression_sql(
    col: str, min_w: int, min_b: int, ratio_mult: str, diff_floor: int, extra_where: str = ""
) -> str:
    where_extra = f"{extra_where} AND " if extra_where else ""
    return f"""
        WITH base AS (
            SELECT "{col}", elapsed,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} WHERE {where_extra}elapsed IS NOT NULL
        )
        SELECT "{col}",
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "{col}"
        HAVING w_total >= {min_w} AND b_total >= {min_b} AND w_p95 >= b_p95 * {ratio_mult} AND w_p95 - b_p95 >= {diff_floor}
        ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
    """


def _build_ip_max_vs_p95_sql(
    col: str, alias_suffix: str, min_w: int, extra_having: str = "", extra_where: str = ""
) -> str:
    max_alias = f"max_{alias_suffix}"
    avg_alias = f"avg_{alias_suffix}"
    return f"""
        WITH base AS (
            SELECT "ip", {col},
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} WHERE {col} > 0
        ),
        win_ips AS (
            SELECT "ip",
                MAX({col}) AS {max_alias},
                AVG({col}) AS {avg_alias},
                COUNT(*) AS w_total
            FROM base WHERE is_w GROUP BY "ip"
            HAVING COUNT(*) >= {min_w}{extra_having}
        ),
        stats AS (
            SELECT w."ip", w.{max_alias}, w.{avg_alias}, w.w_total,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY b.{col}) AS b_p95
            FROM win_ips w JOIN base b ON w."ip" = b."ip" AND b.is_b
            GROUP BY w."ip", w.{max_alias}, w.{avg_alias}, w.w_total
        )
        SELECT "ip", {max_alias}, {avg_alias}, w_total, b_p95 FROM stats
        WHERE w_total >= {min_w} AND {max_alias} > b_p95 * 3{extra_where}
        ORDER BY {max_alias} DESC LIMIT 15
    """


def _build_repeated_patterns_sql(
    *, key_col: str, key_select: str, key_where: str, div_select: str, div_col: str, div_alias: str
) -> str:
    return f"""
    WITH win_rows AS (
        SELECT {key_select} AS {key_col}, epoch("timestamp")::BIGINT AS sec, {div_select} AS {div_col}, {{ua_col}} AS ua_val
        FROM {{table_name}}
        WHERE "timestamp" >= CAST(? AS TIMESTAMPTZ)
          AND {key_where}
    ),
    bot_uas AS (
        SELECT ua_str
        FROM (SELECT DISTINCT CAST(ua_val AS VARCHAR) AS ua_str FROM win_rows WHERE ua_val IS NOT NULL)
        WHERE regexp_matches(ua_str, '{{bot_ua_regex}}', 'i')
    ),
    kept AS (
        SELECT w.{key_col}, w.sec, w.{div_col}
        FROM win_rows w
        LEFT JOIN bot_uas b ON CAST(w.ua_val AS VARCHAR) = b.ua_str
        WHERE b.ua_str IS NULL
    ),
    meta AS (
        SELECT {key_col}, COUNT(DISTINCT {div_col}) AS {div_alias}, COUNT(*) AS n_events,
               (MAX(sec) - MIN(sec)) AS span_s
        FROM kept GROUP BY {key_col}
        HAVING COUNT(*) >= 13
           AND (COUNT(*) * 1.0 / NULLIF(MAX(sec) - MIN(sec), 0)) < 2.0
    ),
    secs AS (
        SELECT k.{key_col}, k.sec FROM kept k JOIN meta d USING({key_col}) GROUP BY k.{key_col}, k.sec
    ),
    gaps AS (
        SELECT {key_col}, sec - LAG(sec) OVER (PARTITION BY {key_col} ORDER BY sec) AS gap FROM secs
    ),
    g AS (SELECT {key_col}, gap FROM gaps WHERE gap IS NOT NULL),
    agg AS (
        SELECT {key_col}, COUNT(*) AS n_gaps, AVG(gap) AS mean_gap, VAR_SAMP(gap) AS var_gap,
               STDDEV_SAMP(gap) AS sd_gap, MODE(gap) AS mode_gap
        FROM g GROUP BY {key_col}
        HAVING COUNT(*) >= 12
    ),
    modal AS (
        SELECT g.{key_col}, AVG(CASE WHEN abs(g.gap - a.mode_gap) <= 1 THEN 1.0 ELSE 0.0 END) AS modal_frac
        FROM g JOIN agg a USING({key_col}) GROUP BY g.{key_col}
    )
    SELECT a.{key_col}, a.n_gaps, d.n_events,
           ROUND(a.mean_gap, 2)  AS avg_interval,
           ROUND(a.sd_gap, 2)    AS stddev_interval,
           ROUND(sqrt(GREATEST(a.var_gap - 1.0/12.0, 0)) / NULLIF(a.mean_gap, 0), 3) AS cv_corr,
           ROUND(m.modal_frac, 3) AS modal_frac,
           d.{div_alias}, d.span_s, a.mode_gap
    FROM agg a JOIN modal m USING({key_col}) JOIN meta d USING({key_col})
    WHERE a.n_gaps >= 12
      AND (d.n_events * 1.0 / NULLIF(d.span_s, 0)) < 2.0
      AND (
            (a.mean_gap >= 5  AND sqrt(GREATEST(a.var_gap - 1.0/12.0,0))/NULLIF(a.mean_gap,0) < 0.3
                              AND m.modal_frac >= 0.6)
         OR (a.mean_gap >= 1  AND a.mean_gap < 5 AND m.modal_frac >= 0.85)
          )
    ORDER BY m.modal_frac DESC, a.n_gaps DESC
    LIMIT 15
"""


COALESCED_CITY_AGGREGATES = """
    WITH base AS (
        SELECT
            "city",
            {region_sel} AS region,
            {country_sel} AS country,
            status,
            elapsed,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
        WHERE "city" IS NOT NULL AND "city" != ''
    )
    SELECT
        {label_expr} AS label, "city", region, country,
        COUNT(*) FILTER (WHERE is_w) AS w_cnt,
        COUNT(*) FILTER (WHERE is_b) AS b_cnt,
        SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_errors_4xx,
        SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_errors_4xx,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95,
        COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL) AS w_lat_total,
        COUNT(*) FILTER (WHERE is_b AND elapsed IS NOT NULL) AS b_lat_total
    FROM base
    GROUP BY "city", region, country
    HAVING COUNT(*) FILTER (WHERE is_w) >= 5
    """

_COALESCED_URL_CORE_COLS = """COUNT(*) FILTER (WHERE is_w) AS w_total,
        COUNT(*) FILTER (WHERE is_b) AS b_total,
        -- error_spikes: 5xx counters
        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_5xx,
        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_5xx,
        -- cache_collapse: cache-hit counters (HIT-only numerator; cacheable
        -- denominator = HIT+MISS, computed from w_miss/b_miss below — PASS is
        -- excluded because it was never eligible to cache)
        SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
        SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
        -- cache_collapse denominator + cacheability_regression: MISS / PASS counters
        SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_miss,
        SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_miss,
        SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_pass,
        SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_pass,
        -- latency_regression: elapsed-only counts + p95s in MILLISECONDS
        COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL) AS w_lat_total,
        COUNT(*) FILTER (WHERE is_b AND elapsed IS NOT NULL) AS b_lat_total,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95,
        -- tail_latency: window-only p99/p50 (rounded to whole ms to match
        -- the legacy template's output exactly)
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY elapsed)
              FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p99,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY elapsed)
              FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p50"""

COALESCED_URL_AGGREGATES = f"""
    WITH base AS (
        SELECT "url", status, cache, elapsed,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} WHERE "url" IS NOT NULL
    )
    SELECT "url",
        -- Common counts
        {_COALESCED_URL_CORE_COLS}
    FROM base GROUP BY "url" HAVING COUNT(*) FILTER (WHERE is_w) >= 3
    """

COALESCED_URL_EXTENDED_AGGREGATES = f"""
    WITH base AS (
        SELECT t."url" AS url, t.status, t.cache, t.elapsed,
            {{resp_bytes_sel}} AS resp_bytes, {{uncompressed_sel}} AS uncompressed,
            {{is_mobile_sel}} AS is_mobile, {{ttl_sel}} AS ttl, {{hits_sel}} AS hits,
            {{age_sel}} AS age, {{oretries_sel}} AS oretries,
            (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} t
        WHERE t."url" IS NOT NULL
    ),
    url_agg AS (
        SELECT url,
            {_COALESCED_URL_CORE_COLS},
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_4xx,
            COUNT(*) FILTER (WHERE is_w AND status = 200 AND resp_bytes >= 1024 AND uncompressed) AS pcr_w_uncomp,
            COUNT(*) FILTER (WHERE is_w AND status = 200 AND resp_bytes >= 1024) AS pcr_w_total,
            COUNT(*) FILTER (WHERE is_b AND status = 200 AND resp_bytes >= 1024 AND uncompressed) AS pcr_b_uncomp,
            COUNT(*) FILTER (WHERE is_b AND status = 200 AND resp_bytes >= 1024) AS pcr_b_total,
            COUNT(*) FILTER (WHERE is_w AND status = 200) AS img_w_cnt,
            SUM(resp_bytes) FILTER (WHERE is_w AND status = 200) AS img_w_bytes,
            AVG(resp_bytes) FILTER (WHERE is_w AND status = 200) AS img_w_avg_bytes,
            SUM(is_mobile) FILTER (WHERE is_w AND status = 200) AS img_w_mobile,
            COUNT(*) FILTER (WHERE is_w AND ttl IS NOT NULL AND ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL) AS ttl_w_cnt,
            AVG(ttl) FILTER (WHERE is_w AND ttl IS NOT NULL AND ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL) AS ttl_w_avg_ttl,
            AVG(hits) FILTER (WHERE is_w AND ttl IS NOT NULL AND ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL) AS ttl_w_avg_hits,
            AVG(age) FILTER (WHERE is_w AND ttl IS NOT NULL AND ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL) AS ttl_w_avg_age,
            COUNT(*) FILTER (WHERE is_w AND oretries > 0) AS oret_w_cnt,
            AVG(oretries) FILTER (WHERE is_w AND oretries > 0) AS oret_w_avg,
            MAX(oretries) FILTER (WHERE is_w AND oretries > 0) AS oret_w_max
        FROM base
        GROUP BY url
        HAVING COUNT(*) FILTER (WHERE is_w) > 0
    ),
    flagged AS (
        SELECT *,
            regexp_matches(url, '{NEW_PROBE_REGEX}', 'i') AS is_probe,
            regexp_matches(url, '{COMPRESSIBLE_REGEX}', 'i') AS is_comp,
            ((url ILIKE '%.jpg%' OR url ILIKE '%.jpeg%' OR url ILIKE '%.png%' OR url ILIKE '%.gif%')
             AND url NOT ILIKE '%auto=webp%' AND url NOT ILIKE '%format=auto%'
             AND url NOT ILIKE '%format=webp%' AND url NOT ILIKE '%format=avif%') AS is_img
        FROM url_agg
    )
    SELECT url,
        w_total, b_total, w_5xx, b_5xx, w_hits, b_hits, w_miss, b_miss,
        w_pass, b_pass, w_lat_total, b_lat_total, w_p95, b_p95, w_p99, w_p50,
        is_probe, w_4xx,
        is_comp,
        CASE WHEN is_comp THEN pcr_w_uncomp ELSE 0 END AS pcr_w_uncomp,
        CASE WHEN is_comp THEN pcr_w_total ELSE 0 END AS pcr_w_total,
        CASE WHEN is_comp THEN pcr_b_uncomp ELSE 0 END AS pcr_b_uncomp,
        CASE WHEN is_comp THEN pcr_b_total ELSE 0 END AS pcr_b_total,
        is_img,
        CASE WHEN is_img THEN img_w_cnt ELSE 0 END AS img_w_cnt,
        CASE WHEN is_img THEN img_w_bytes ELSE NULL END AS img_w_bytes,
        CASE WHEN is_img THEN img_w_avg_bytes ELSE NULL END AS img_w_avg_bytes,
        CASE WHEN is_img THEN img_w_mobile ELSE NULL END AS img_w_mobile,
        ttl_w_cnt, ttl_w_avg_ttl, ttl_w_avg_hits, ttl_w_avg_age,
        oret_w_cnt, oret_w_avg, oret_w_max
    FROM flagged
    WHERE w_total >= 3
        OR (is_probe AND w_total > 0 AND b_total = 0)
        OR (is_img AND img_w_bytes > 524288)
    """


def _ip_sec_core_cols(ug: str = "") -> str:
    return f"""-- low_and_slow: probe paths over the WHOLE span (no window split)
        COUNT(*) FILTER (WHERE {ug}is_sensitive) AS ls_hits,
        COUNT(DISTINCT url) FILTER (WHERE {ug}is_sensitive) AS ls_distinct,
        MIN(sec) FILTER (WHERE {ug}is_sensitive) AS ls_min_sec,
        MAX(sec) FILTER (WHERE {ug}is_sensitive) AS ls_max_sec,
        -- credential_enumeration: auth paths, window/baseline 401/403
        COUNT(*) FILTER (WHERE {ug}is_auth AND is_w AND status IN (401, 403)) AS ce_w_denied,
        COUNT(*) FILTER (WHERE {ug}is_auth AND is_w) AS ce_w_attempts,
        COUNT(DISTINCT url) FILTER (WHERE {ug}is_auth AND is_w) AS ce_w_paths,
        COUNT(*) FILTER (WHERE {ug}is_auth AND is_b AND status IN (401, 403)) AS ce_b_denied,
        -- content_discovery: all URLs, window/baseline 404s
        COUNT(*) FILTER (WHERE {ug}is_w AND status = 404) AS cd_w_404,
        COUNT(*) FILTER (WHERE {ug}is_w) AS cd_w_total,
        COUNT(DISTINCT url) FILTER (WHERE {ug}is_w AND status = 404) AS cd_distinct_404,
        COUNT(*) FILTER (WHERE {ug}is_b AND status = 404) AS cd_b_404"""


COALESCED_IP_SECURITY_AGGREGATES = f"""
    WITH url_flags AS (
        SELECT "url", is_sensitive, is_auth
        FROM (
            SELECT "url",
                regexp_matches("url", '{NEW_PROBE_REGEX}', 'i') AS is_sensitive,
                regexp_matches("url", '{AUTH_PATHS_REGEX}', 'i') AS is_auth
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
        )
        WHERE is_sensitive OR is_auth
    ),
    base AS (
        SELECT t."ip" AS ip, t."url" AS url, t.status, epoch(t."timestamp")::BIGINT AS sec,
            {coalesce("u.is_sensitive", "FALSE")} AS is_sensitive,
            {coalesce("u.is_auth", "FALSE")} AS is_auth,
            (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} t
        LEFT JOIN url_flags u ON t."url" = u."url"
        WHERE t."ip" IS NOT NULL AND t."ip" <> '' AND t."url" IS NOT NULL
    )
    SELECT ip,
        {_ip_sec_core_cols()}
    FROM base
    WHERE is_w OR is_sensitive OR status = 404 OR (is_auth AND status IN (401, 403))
    GROUP BY ip
    HAVING (COUNT(*) FILTER (WHERE is_sensitive) > 0)
        OR (COUNT(*) FILTER (WHERE is_auth AND is_w) > 0)
        OR (COUNT(*) FILTER (WHERE is_w AND status = 404) > 0)
    """

COALESCED_IP_EXTENDED_AGGREGATES = f"""
    WITH url_flags AS (
        SELECT "url", is_sensitive, is_auth, is_deep
        FROM (
            SELECT "url",
                regexp_matches("url", '{NEW_PROBE_REGEX}', 'i') AS is_sensitive,
                regexp_matches("url", '{AUTH_PATHS_REGEX}', 'i') AS is_auth,
                ("url" LIKE '/%/%/%' AND "url" NOT LIKE '%.%') AS is_deep
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
        )
        WHERE is_sensitive OR is_auth OR is_deep
    ),
    base AS (
        SELECT t."ip" AS ip, t."url" AS url, t.status, epoch(t."timestamp")::BIGINT AS sec,
            {{no_referer_sel}} AS no_referer, {{cs_sel}} AS cs,
            {{req_header_bytes_sel}} AS req_header_bytes, {{conn_requests_sel}} AS conn_requests,
            {coalesce("u.is_sensitive", "FALSE")} AS is_sensitive,
            {coalesce("u.is_auth", "FALSE")} AS is_auth,
            {coalesce("u.is_deep", "FALSE")} AS is_deep,
            (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} t
        LEFT JOIN url_flags u ON t."url" = u."url"
        WHERE t."ip" IS NOT NULL AND t."ip" <> ''
    )
    SELECT ip,
        {_ip_sec_core_cols("url IS NOT NULL AND ")},
        -- orphaned_deep_crawl
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_deep AND no_referer AND is_w) AS odc_w_reqs,
        COUNT(DISTINCT url) FILTER (WHERE url IS NOT NULL AND is_deep AND no_referer AND is_w) AS odc_w_urls,
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_deep AND no_referer AND is_b) AS odc_b_reqs,
        -- session_harvesting
        COUNT(DISTINCT cs) FILTER (WHERE is_w AND cs IS NOT NULL AND cs <> '') AS sh_w_sessions,
        COUNT(*) FILTER (WHERE is_w AND cs IS NOT NULL AND cs <> '') AS sh_w_reqs,
        COUNT(DISTINCT cs) FILTER (WHERE is_b AND cs IS NOT NULL AND cs <> '') AS sh_b_sessions,
        -- request_size_anomaly pre-check (min(B) <= P95(B), so if w_max <= b_min * 3 we know w_max <= b_p95 * 3)
        COUNT(*) FILTER (WHERE is_w AND req_header_bytes > 0) AS rsa_w_cnt,
        MAX(req_header_bytes) FILTER (WHERE is_w AND req_header_bytes > 0) AS rsa_w_max,
        MIN(req_header_bytes) FILTER (WHERE is_b AND req_header_bytes > 0) AS rsa_b_min,
        MAX(req_header_bytes) FILTER (WHERE is_b AND req_header_bytes > 0) AS rsa_b_max,
        -- connection_abuse pre-check (same exact min(B) <= P95(B) bound)
        COUNT(*) FILTER (WHERE is_w AND conn_requests > 0) AS ca_w_cnt,
        MAX(conn_requests) FILTER (WHERE is_w AND conn_requests > 0) AS ca_w_max,
        MIN(conn_requests) FILTER (WHERE is_b AND conn_requests > 0) AS ca_b_min,
        MAX(conn_requests) FILTER (WHERE is_b AND conn_requests > 0) AS ca_b_max
    FROM base
    GROUP BY ip
    HAVING (COUNT(*) FILTER (WHERE url IS NOT NULL AND is_sensitive) >= 5)
        OR (COUNT(*) FILTER (WHERE url IS NOT NULL AND is_auth AND is_w AND status IN (401, 403)) >= 20)
        OR (COUNT(*) FILTER (WHERE url IS NOT NULL AND is_w AND status = 404) >= 20)
        OR (COUNT(*) FILTER (WHERE url IS NOT NULL AND is_deep AND no_referer AND is_w) >= 50)
        OR (COUNT(DISTINCT cs) FILTER (WHERE is_w AND cs IS NOT NULL AND cs <> '') >= 20)
        OR (COUNT(*) FILTER (WHERE is_w AND req_header_bytes > 0) >= 3 AND MIN(req_header_bytes) FILTER (WHERE is_b AND req_header_bytes > 0) IS NOT NULL AND MAX(req_header_bytes) FILTER (WHERE is_w AND req_header_bytes > 0) > MIN(req_header_bytes) FILTER (WHERE is_b AND req_header_bytes > 0) * 3)
        OR (COUNT(*) FILTER (WHERE is_w AND conn_requests > 0) >= 5 AND MAX(conn_requests) FILTER (WHERE is_w AND conn_requests > 0) >= 50 AND MIN(conn_requests) FILTER (WHERE is_b AND conn_requests > 0) IS NOT NULL AND MAX(conn_requests) FILTER (WHERE is_w AND conn_requests > 0) > MIN(conn_requests) FILTER (WHERE is_b AND conn_requests > 0) * 3)
    """

COALESCED_ASN_AGGREGATES = """
    WITH base AS (
        SELECT
            "asn" AS asn,
            {ploss_sel} AS ploss,
            {rtt_var_sel} AS rtt_var,
            {retrans_sel} AS retrans,
            {p_type_sel} AS p_type,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        asn,
        COUNT(*) FILTER (WHERE is_w) AS w_cnt,
        COUNT(*) FILTER (WHERE is_b) AS b_cnt,
        AVG(ploss) FILTER (WHERE is_w AND asn IS NOT NULL) AS w_ploss,
        AVG(ploss) FILTER (WHERE is_b AND asn IS NOT NULL) AS b_ploss,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rtt_var) FILTER (WHERE is_w AND asn IS NOT NULL) AS w_jitter,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rtt_var) FILTER (WHERE is_b AND asn IS NOT NULL) AS b_jitter,
        AVG(retrans) FILTER (WHERE is_w AND asn IS NOT NULL) AS w_retrans,
        AVG(retrans) FILTER (WHERE is_b AND asn IS NOT NULL) AS b_retrans,
        COUNT(*) FILTER (WHERE is_w AND asn IS NOT NULL AND p_type = 'hosting') AS w_hosting,
        COUNT(*) FILTER (WHERE is_w AND asn IS NOT NULL AND p_type IS NOT NULL AND p_type != '') AS w_pt_total,
        COUNT(*) FILTER (WHERE is_b AND asn IS NOT NULL AND p_type = 'hosting') AS b_hosting,
        COUNT(*) FILTER (WHERE is_b AND asn IS NOT NULL AND p_type IS NOT NULL AND p_type != '') AS b_pt_total
    FROM base
    GROUP BY asn
    """

COALESCED_SCALAR_EXTENDED_AGGREGATES = f"""
    WITH base AS (
        SELECT
            {{transport_sel}} AS transport,
            {{cache_sel}} AS cache,
            {{oconnect_ms_sel}} AS oconnect_ms,
            {{ottfb_sel}} AS ottfb,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}}
    )
    SELECT
        COUNT(*) FILTER (WHERE is_w AND transport IS NOT NULL AND transport != '' AND lower(transport) = 'quic') AS w_quic,
        COUNT(*) FILTER (WHERE is_w AND transport IS NOT NULL AND transport != '') AS w_tr_total,
        COUNT(*) FILTER (WHERE is_b AND transport IS NOT NULL AND transport != '' AND lower(transport) = 'quic') AS b_quic,
        COUNT(*) FILTER (WHERE is_b AND transport IS NOT NULL AND transport != '') AS b_tr_total,
        {coalesce("SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w)", "0")} AS w_hits,
        {coalesce("SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w)", "0")} AS w_cacheable,
        {coalesce("SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b)", "0")} AS b_hits,
        {coalesce("SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b)", "0")} AS b_cacheable,
        -- timeout_split
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY oconnect_ms) FILTER (WHERE is_w AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_w_conn,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY oconnect_ms) FILTER (WHERE is_b AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_b_conn,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY GREATEST(ottfb / 1000.0 - oconnect_ms, 0)) FILTER (WHERE is_w AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_w_read,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY GREATEST(ottfb / 1000.0 - oconnect_ms, 0)) FILTER (WHERE is_b AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_b_read,
        COUNT(*) FILTER (WHERE is_w AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_w_total,
        COUNT(*) FILTER (WHERE is_b AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_b_total,
        -- origin_latency_spike overall gate
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_w AND ottfb IS NOT NULL) / 1000.0 AS ols_w_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_b AND ottfb IS NOT NULL) / 1000.0 AS ols_b_p95
    FROM base
    """

COALESCED_FP_AGGREGATES = """
    WITH base AS (
        SELECT
            {fp_sel} AS fp,
            {cipher_sel} AS cipher,
            {ip_sel} AS ip,
            {asn_sel} AS asn,
            {is_non_hosting_sel} AS is_non_hosting,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        GROUPING_ID(fp, cipher) AS gid,
        fp,
        cipher,
        -- botnet_grouping (gid = 1, grouped by fp)
        COUNT(DISTINCT ip) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '') AS bg_w_ips,
        COUNT(*) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '') AS bg_w_reqs,
        COUNT(DISTINCT ip) FILTER (WHERE is_b AND fp IS NOT NULL AND fp != '') AS bg_b_ips,
        -- residential_fingerprint_dispersion (gid = 1, grouped by fp, non-hosting)
        COUNT(DISTINCT ip) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_w_ips,
        COUNT(DISTINCT asn) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_w_asns,
        COUNT(*) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_w_reqs,
        COUNT(DISTINCT ip) FILTER (WHERE is_b AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_b_ips,
        COUNT(DISTINCT asn) FILTER (WHERE is_b AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_b_asns,
        -- cipher_spread (gid = 2, grouped by cipher)
        COUNT(DISTINCT ip) FILTER (WHERE is_w AND cipher IS NOT NULL AND cipher != '') AS cs_w_ips,
        COUNT(*) FILTER (WHERE is_w AND cipher IS NOT NULL AND cipher != '') AS cs_w_reqs,
        COUNT(DISTINCT ip) FILTER (WHERE is_b AND cipher IS NOT NULL AND cipher != '') AS cs_b_ips
    FROM base
    GROUP BY GROUPING SETS ((fp), (cipher))
    """

COALESCED_DIM_AGGREGATES = """
    WITH base AS (
        SELECT
            {country_sel} AS country,
            {p_type_sel} AS p_type,
            {method_sel} AS method,
            {c_type_sel} AS c_type,
            {c_speed_sel} AS c_speed,
            {ost_sel} AS ost,
            {oip_sel} AS oip,
            {referer_sel} AS referer,
            {ua_sel} AS ua,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        GROUPING_ID(country, p_type, method, c_type, c_speed, ost, oip, referer, ua) AS gid,
        country, p_type, method, c_type, c_speed, ost, oip, referer, ua,
        COUNT(*) FILTER (WHERE is_w) AS w_cnt,
        COUNT(*) FILTER (WHERE is_b) AS b_cnt,
        COUNT(*) FILTER (WHERE is_w AND ost IS NOT NULL) AS w_ost_nn,
        COUNT(*) FILTER (WHERE is_b AND ost IS NOT NULL) AS b_ost_nn,
        COUNT(*) FILTER (WHERE is_w AND ost >= 500) AS w_ost_5xx,
        COUNT(*) FILTER (WHERE is_b AND ost >= 500) AS b_ost_5xx
    FROM base
    GROUP BY GROUPING SETS (
        (country),
        (p_type),
        (method),
        (c_type, c_speed),
        (ost),
        (oip),
        (referer),
        (ua)
    )
    """

COALESCED_GEO_LATENCY_AGGREGATES = """
    WITH base AS (
        SELECT
            {pop_sel} AS pop,
            {server_region_sel} AS server_region,
            {metro_sel} AS metro,
            {elapsed_sel} AS elapsed,
            {ottfb_sel} AS ottfb,
            {delivery_rate_sel} AS delivery_rate,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        GROUPING_ID(pop, server_region, metro) AS gid,
        pop,
        server_region,
        metro,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95,
        COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL) AS w_lat_total,
        COUNT(*) FILTER (WHERE is_b AND elapsed IS NOT NULL) AS b_lat_total,
        COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL AND ottfb IS NOT NULL) AS w_ottfb_cnt,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_w AND elapsed IS NOT NULL AND ottfb IS NOT NULL) / 1000.0, 0) AS w_ottfb_p95,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_rate) FILTER (WHERE is_w AND delivery_rate > 0) AS w_med_dr,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_rate) FILTER (WHERE is_b AND delivery_rate > 0) AS b_med_dr,
        COUNT(*) FILTER (WHERE is_w AND delivery_rate > 0) AS w_dr_total,
        COUNT(*) FILTER (WHERE is_b AND delivery_rate > 0) AS b_dr_total
    FROM base
    GROUP BY GROUPING SETS ((pop), (server_region), (metro))
    """


def _mask_ip_if_needed(ip: Any, context: dict) -> Any:
    return mask_ip(ip) if (context.get("mask_ips") and ip) else ip


def _fmt_asn_label(asn: Any, context: dict) -> str:
    names_map = context.get("asn_names", {})
    return f"AS{asn} ({names_map[asn]})" if asn in names_map else f"AS{asn}"


def _fmt_metro_label(metro: Any, context: dict) -> str:
    return context.get("dma_map", {}).get(str(metro)) or f"DMA {metro}"


def _scale_baseline(val: Any, context: dict) -> float:
    return float(val) / max(context["baseline_hours"], 1) * context["window_hours"]


def _item(
    label: Any,
    current_val: Any,
    baseline_val: Any,
    unit: str,
    meta: dict[str, Any],
    severity: str,
    *,
    baseline_label: str | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "label": label,
        "current_val": current_val,
        "baseline_val": baseline_val,
        "unit": unit,
        "meta": meta,
        "severity": severity,
    }
    if baseline_label is not None:
        out["baseline_label"] = baseline_label
    return out


def _p95_latency_item(
    label: str, w_p95: Any, b_p95: Any, reqs: Any, filters: dict[str, Any], *, reqs_key: str = "window_requests"
) -> dict[str, Any]:
    w_f, b_f = float(w_p95 or 0), float(b_p95 or 0)
    return _item(
        label,
        w_f,
        b_f,
        "ms (P95)",
        {"regression_ratio": float(w_p95 / b_p95) if b_p95 else 0, reqs_key: reqs, "filters": filters},
        "critical" if w_f >= 5000 else "warning",
    )


def _traffic_share_rates(w_cnt: Any, w_total: Any, b_cnt: Any, b_total: Any) -> tuple[float, float]:
    return (float(w_cnt * 100.0 / w_total) if w_total else 0.0), (float(b_cnt * 100.0 / b_total) if b_total else 0.0)


def _error_rate_spike_item(
    label: str, w_rate: Any, b_rate: Any, w_errors: Any, w_total: Any, unit: str, filters: dict[str, Any]
) -> dict[str, Any]:
    return _item(
        label,
        float(w_rate or 0) * 100,
        float(b_rate or 0) * 100,
        unit,
        {"requests": w_total, "errors": w_errors, "filters": filters},
        "critical" if (w_rate or 0) >= 0.5 else "warning",
    )


def _monoculture_processor(row: tuple, filter_key: str) -> dict:
    val, w_cnt, b_cnt, b_total, w_total = row
    w_rate, b_rate = _traffic_share_rates(w_cnt, w_total, b_cnt, b_total)
    return _item(
        val or "(empty)",
        w_rate,
        b_rate,
        "% of traffic",
        {"requests": w_cnt, "filters": {filter_key: val}},
        "critical" if w_rate >= 50 else "warning",
    )


def _url_rate_regression_processor(row: tuple, unit: str, *, is_critical: bool) -> dict:
    return _item(
        row[0] or "(empty)",
        float(row[1] or 0) * 100,
        float(row[2] or 0) * 100,
        unit,
        {"window_requests": row[3], "baseline_requests": row[4], "filters": {"url": row[0]}},
        "critical" if is_critical else "warning",
    )


def _ip_max_vs_p95_processor(
    row: tuple, context: dict, *, unit: str, max_key: str, avg_key: str, crit_thresh: int
) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    max_val, avg_val, b_p95 = int(row[1] or 0), int(row[2] or 0), int(row[4] or 0)
    return _item(
        ip or "(unknown)",
        max_val,
        b_p95,
        unit,
        {max_key: max_val, avg_key: avg_val, "requests": row[3], "p95_baseline": b_p95, "filters": {"ip": ip}},
        "critical" if (row[1] or 0) > crit_thresh else "warning",
        baseline_label="P95 baseline",
    )


def _city_filters(row: tuple) -> dict:
    return {"city": row[1], "region": row[2] if row[2] else None, "country": row[3]}


def _zero_baseline_count_item(label: str, count: int, meta: dict, warn_threshold: int) -> dict:
    return _item(label, count, 0, "requests", meta, "warning" if count >= warn_threshold else "info")
