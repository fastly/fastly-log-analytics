from __future__ import annotations

from backend.core.metric_expressions import coalesce
from backend.repositories.insights import InsightCategory, InsightDefinition
from backend.repositories.insights.definitions.sql_coalesced import (
    AUTH_PATHS_REGEX,
    NEW_PROBE_REGEX,
    _build_ip_max_vs_p95_sql,
    _build_ip_spread_sql,
    _build_repeated_patterns_sql,
    _ip_max_vs_p95_processor,
    _item,
    _mask_ip_if_needed,
    _reg,
    _scale_baseline,
)

BOTNET_GROUPING = """
        WITH base AS (
            SELECT "{fp_col}", "ip",
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "{fp_col}" IS NOT NULL AND "{fp_col}" != ''
        ),
        agg AS (
            SELECT "{fp_col}",
                COUNT(DISTINCT "ip") FILTER (WHERE is_w) AS w_ips,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT "ip") FILTER (WHERE is_b) AS b_ips
            FROM base
            GROUP BY "{fp_col}"
            HAVING COUNT(DISTINCT "ip") FILTER (WHERE is_w) >= 5
        )
        SELECT "{fp_col}",
            w_ips,
            w_reqs,
            b_ips,
            w_ips * 1.0 / GREATEST(b_ips / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1) AS ip_ratio
        FROM agg
        WHERE w_ips > b_ips / GREATEST({baseline_hours}, 1.0) * {window_hours} * 3
        ORDER BY ip_ratio DESC LIMIT 10
    """

NEW_PROBE_URLS = f"""
        WITH probe_urls AS (
            SELECT "url"
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
            WHERE regexp_matches("url", '{NEW_PROBE_REGEX}', 'i')
        )
        SELECT t."url",
            COUNT(*) FILTER (WHERE t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE t.timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            AVG(CASE WHEN t."status" >= 400 THEN 1.0 ELSE 0.0 END) FILTER (WHERE t.timestamp >= CAST(? AS TIMESTAMPTZ)) * 100 AS w_error_pct
        FROM {{table_name}} t
        JOIN probe_urls p ON t."url" = p."url"
        GROUP BY t."url"
        HAVING w_cnt > 0 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 25
    """

WAF_SIGNAL_SPIKES = f"""
        WITH waf_combos AS (
            SELECT "waf_sig",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {{waf_table}}
            WHERE "waf_sig" IS NOT NULL AND "waf_sig" != ''
            GROUP BY "waf_sig"
        ),
        unrolled AS (
            SELECT trim(unnest(string_split("waf_sig", ','))) AS signal,
                w_cnt,
                b_cnt
            FROM waf_combos
        )
        SELECT signal,
            CAST(SUM(w_cnt) AS BIGINT) AS w_cnt,
            CAST(SUM(b_cnt) AS BIGINT) AS b_cnt,
            SUM(w_cnt) * 1.0 / GREATEST({coalesce("SUM(b_cnt)", "0")} * 1.0 / {{baseline_hours}} * {{window_hours}}, 0.5) AS spike_ratio
        FROM unrolled
        WHERE signal != '' AND signal != 'BOT-ANALYSIS'
        GROUP BY signal
        HAVING SUM(w_cnt) >= 3 AND SUM(w_cnt) > {coalesce("SUM(b_cnt)", "0")} * 1.0 / {{baseline_hours}} * {{window_hours}} * 2 + 2
        ORDER BY spike_ratio DESC LIMIT 15
    """

PROXY_SURGE = f"""
        WITH all_p AS (
            SELECT "p_type",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {{table_name}} WHERE "p_type" IS NOT NULL GROUP BY "p_type"
        ),
        totals AS (
            SELECT
                {coalesce("SUM(w_cnt) FILTER (WHERE \"p_type\" != '')", "0")} AS w_proxy_total,
                {coalesce("SUM(b_cnt) FILTER (WHERE \"p_type\" != '')", "0")} AS b_proxy_total,
                {coalesce("SUM(w_cnt)", "0")} AS w_total_all,
                {coalesce("SUM(b_cnt)", "0")} AS b_total_all
            FROM all_p
        )
        SELECT b."p_type", b.w_cnt, b.b_cnt, t.w_total_all, t.b_total_all
        FROM all_p b, totals t
        WHERE b."p_type" != ''
          AND (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= 5
          AND (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= (t.b_proxy_total * 100.0 / NULLIF(t.b_total_all, 0)) * 2 + 5
    """

CIPHER_SPREAD = _build_ip_spread_sql("tls_ciphers_sha")
REQUEST_SIZE_ANOMALY = _build_ip_max_vs_p95_sql("req_header_bytes", "bytes", 3)
CONNECTION_ABUSE = _build_ip_max_vs_p95_sql(
    "conn_requests", "reqs", 5, " AND MAX(conn_requests) >= 50", " AND max_reqs >= 50"
)

REPEATED_PATTERNS = _build_repeated_patterns_sql(
    key_col="ip",
    key_select='"ip"',
    key_where='"ip" IS NOT NULL AND "ip" <> \'\'',
    div_select="{ua_col}",
    div_col="ua",
    div_alias="distinct_ua",
)
REPEATED_PATTERNS_FP = _build_repeated_patterns_sql(
    key_col="entity",
    key_select=coalesce('"{fp_col}"', "''"),
    key_where='"{fp_col}" IS NOT NULL AND "{fp_col}" != \'\'',
    div_select='"ip"',
    div_col="ip",
    div_alias="distinct_ip",
)

LOW_AND_SLOW = f"""
        WITH base AS (
            SELECT "ip" AS ip, "url" AS url, epoch("timestamp")::BIGINT AS sec
            FROM {{table_name}}
            WHERE "ip" IS NOT NULL AND "ip" <> ''
              AND "url" IS NOT NULL
              AND regexp_matches("url", '{NEW_PROBE_REGEX}', 'i')
        )
        SELECT ip,
            COUNT(*) AS hits,
            COUNT(DISTINCT url) AS distinct_paths,
            (MAX(sec) - MIN(sec)) AS span_s,
            ROUND(COUNT(*) * 1.0 / NULLIF(MAX(sec) - MIN(sec), 0), 5) AS rps
        FROM base
        GROUP BY ip
        HAVING COUNT(*) >= 5
           AND COUNT(DISTINCT url) >= 3
           AND (MAX(sec) - MIN(sec)) >= 600
           AND COUNT(*) * 1.0 / NULLIF(MAX(sec) - MIN(sec), 0) < 0.2
        ORDER BY distinct_paths DESC, span_s DESC
        LIMIT 15
    """

CREDENTIAL_ENUMERATION = f"""
        WITH base AS (
            SELECT "ip" AS ip, "url" AS url, status,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
            WHERE "ip" IS NOT NULL AND "ip" <> ''
              AND "url" IS NOT NULL
              AND regexp_matches("url", '{AUTH_PATHS_REGEX}', 'i')
        )
        SELECT ip,
            COUNT(*) FILTER (WHERE is_w AND status IN (401, 403)) AS w_denied,
            COUNT(*) FILTER (WHERE is_w) AS w_attempts,
            COUNT(DISTINCT url) FILTER (WHERE is_w) AS w_paths,
            COUNT(*) FILTER (WHERE is_b AND status IN (401, 403)) AS b_denied
        FROM base
        GROUP BY ip
        HAVING w_denied >= 20
           AND w_denied * 1.0 / NULLIF(w_attempts, 0) >= 0.5
           AND w_denied > {coalesce("b_denied", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}} * 3 + 5
        ORDER BY w_denied DESC
        LIMIT 15
    """

CONTENT_DISCOVERY = """
        WITH base AS (
            SELECT "ip" AS ip, "url" AS url, status,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "ip" IS NOT NULL AND "ip" <> ''
              AND "url" IS NOT NULL
        )
        SELECT ip,
            COUNT(*) FILTER (WHERE is_w AND status = 404) AS w_404,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(DISTINCT url) FILTER (WHERE is_w AND status = 404) AS w_distinct_404,
            COUNT(*) FILTER (WHERE is_b AND status = 404) AS b_404
        FROM base
        GROUP BY ip
        HAVING COUNT(*) FILTER (WHERE is_w AND status = 404) >= 20
           AND COUNT(*) FILTER (WHERE is_w AND status = 404) * 1.0
               / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) >= 0.7
           AND COUNT(DISTINCT url) FILTER (WHERE is_w AND status = 404) >= 15
        ORDER BY w_404 DESC
        LIMIT 15
    """

SESSION_HARVESTING = f"""
        WITH base AS (
            SELECT "ip" AS ip, "cookie_session" AS cs,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
            WHERE "ip" IS NOT NULL AND "ip" <> '' AND "cookie_session" IS NOT NULL AND "cookie_session" <> ''
        ),
        agg AS (
            SELECT ip,
                COUNT(DISTINCT cs) FILTER (WHERE is_w) AS w_sessions,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT cs) FILTER (WHERE is_b) AS b_sessions
            FROM base
            GROUP BY ip
            HAVING COUNT(DISTINCT cs) FILTER (WHERE is_w) >= 20
        )
        SELECT ip,
            w_sessions,
            w_reqs,
            {coalesce("b_sessions", "0")} AS b_sessions
        FROM agg
        WHERE w_sessions > {coalesce("b_sessions", "0")} * 2 + 10
        ORDER BY w_sessions DESC LIMIT 15
    """

CRAWLER_DISPARITY = rf"""
        WITH ua_agg AS (
            SELECT
                ua,
                COUNT(*) FILTER (WHERE is_w) AS w_cnt,
                COUNT(*) FILTER (WHERE is_b) AS b_cnt,
                CAST({coalesce(f"SUM({coalesce('resp_bytes', '0')}) FILTER (WHERE is_w)", "0")} AS BIGINT) AS w_bytes,
                CAST({coalesce(f"SUM({coalesce('resp_bytes', '0')}) FILTER (WHERE is_b)", "0")} AS BIGINT) AS b_bytes
            FROM (
                SELECT ua, resp_bytes,
                    (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w,
                    (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b
                FROM {{table_name}}
            )
            GROUP BY ua
        ),
        site_win AS (
            SELECT
                GREATEST(CAST({coalesce("SUM(w_bytes)", "0")} AS BIGINT), 1) AS total_site_bytes
            FROM ua_agg
        ),
        ua_extracted AS (
            SELECT
                ua,
                l_ua,
                w_cnt,
                b_cnt,
                w_bytes,
                b_bytes,
                regexp_extract(
                    l_ua,
                    '([a-z0-9_.-]*(bot|crawler|spider|slurp|archiver|scraper|externalagent|extended|other|preview)[a-z0-9_.-]*|headless[a-z0-9_.-]*|python-[a-z0-9_.-]+|go-http-client|curl|wget|scrapy|aiohttp|httpx|okhttp|axios)',
                    1
                ) AS raw_tok,
                regexp_extract(l_ua, 'http[s]*://(www\.)*([^/.:;\s)]+)', 2) AS ua_dom,
                regexp_extract(l_ua, '([a-z0-9]+)-(user|externalfetcher)', 1) AS user_fetcher_stem
            FROM (
                SELECT ua, lower(ua) AS l_ua, w_cnt, b_cnt, w_bytes, b_bytes
                FROM ua_agg
                WHERE ua IS NOT NULL
            )
        ),
        ua_info AS (
            SELECT
                ua,
                w_cnt,
                b_cnt,
                w_bytes,
                b_bytes,
                raw_tok,
                ua_dom,
                user_fetcher_stem,
                (raw_tok != '') AS is_crawler,
                (raw_tok = '') AS is_human,
                NULLIF(
                    regexp_replace(
                        raw_tok,
                        '(-searchbot|-assistbot|-externalagent|-extended|-user|searchbot|assistbot|externalagent|extended|crawler|spider|slurp|archiver|scraper|preview|other|bot).*$',
                        ''
                    ),
                    ''
                ) AS stem
            FROM ua_extracted
        ),
        crawler_traffic AS (
            SELECT
                {coalesce("stem", "raw_tok")} AS bot_stem,
                MIN(raw_tok) FILTER (WHERE w_cnt > 0) AS raw_tok,
                MAX(ua_dom) FILTER (WHERE w_cnt > 0) AS ua_dom,
                CAST({coalesce("SUM(w_cnt)", "0")} AS BIGINT) AS w_crawls,
                CAST({coalesce("SUM(w_bytes)", "0")} AS BIGINT) AS w_bytes,
                CAST({coalesce("SUM(b_cnt)", "0")} AS BIGINT) AS b_crawls,
                CAST({coalesce("SUM(b_bytes)", "0")} AS BIGINT) AS b_bytes
            FROM ua_info
            WHERE is_crawler
            GROUP BY {coalesce("stem", "raw_tok")}
            HAVING SUM(w_cnt) >= 50
        ),
        ref_info AS (
            SELECT
                referer,
                NULLIF(regexp_extract(l_ref, 'http[s]*://(www\.)*([^/.:;\s]+)', 2), '') AS ref_dom
            FROM (
                SELECT referer, lower(referer) AS l_ref
                FROM (
                    SELECT DISTINCT referer
                    FROM {{table_name}}
                    WHERE referer IS NOT NULL AND EXISTS (SELECT 1 FROM crawler_traffic)
                )
            )
        ),
        all_ref_stems AS (
            SELECT DISTINCT ref_stem
            FROM (
                SELECT user_fetcher_stem AS ref_stem FROM ua_info WHERE is_human AND user_fetcher_stem != ''
                UNION
                SELECT ref_dom AS ref_stem FROM ref_info WHERE ref_dom IS NOT NULL AND ref_dom != ''
            )
            WHERE ref_stem IS NOT NULL AND ref_stem != ''
        ),
        stem_matches AS (
            SELECT
                c.bot_stem,
                rs.ref_stem
            FROM crawler_traffic c
            JOIN all_ref_stems rs
              ON c.bot_stem = rs.ref_stem
              OR (
                  length(regexp_replace(c.bot_stem, '[aeiou]', '', 'g')) >= 3
                  AND regexp_replace(c.bot_stem, '[aeiou]', '', 'g') = regexp_replace(rs.ref_stem, '[aeiou]', '', 'g')
              )
              OR (
                  length(c.bot_stem) >= 3 AND length(rs.ref_stem) >= 3
                  AND (contains(rs.ref_stem, c.bot_stem) OR contains(c.bot_stem, rs.ref_stem))
              )
              OR (c.ua_dom != '' AND c.ua_dom = rs.ref_stem)
        ),
        referred_raw AS (
            SELECT
                t.ip,
                (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w,
                (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                {coalesce("NULLIF(u.user_fetcher_stem, '')", "r.ref_dom")} AS ref_stem
            FROM {{table_name}} t
            JOIN ua_info u ON t.ua = u.ua
            LEFT JOIN ref_info r ON t.referer = r.referer
            WHERE u.is_human
              AND t.ip IS NOT NULL
              AND t.timestamp IS NOT NULL
              AND EXISTS (SELECT 1 FROM crawler_traffic)
        ),
        referred_traffic AS (
            SELECT
                sm.bot_stem,
                COUNT(DISTINCT rr.ip) FILTER (WHERE rr.is_w) AS w_referrals,
                COUNT(DISTINCT rr.ip) FILTER (WHERE rr.is_b) AS b_referrals
            FROM referred_raw rr
            JOIN stem_matches sm ON rr.ref_stem = sm.ref_stem
            GROUP BY sm.bot_stem
        )
        SELECT
            c.raw_tok AS label,
            c.w_crawls,
            {coalesce("r.w_referrals", "0")} AS w_referrals,
            c.w_bytes,
            c.w_crawls * 1.0 / GREATEST({coalesce("r.w_referrals", "0")}, 1) AS crawls_per_referral,
            c.b_crawls * 1.0 / GREATEST({coalesce("r.b_referrals", "0")}, 1) AS baseline_crawls_per_referral,
            c.w_bytes * 1.0 / GREATEST({coalesce("r.w_referrals", "0")}, 1) AS bytes_per_referral,
            c.b_bytes * 1.0 / GREATEST({coalesce("r.b_referrals", "0")}, 1) AS baseline_bytes_per_referral,
            ROUND(c.w_bytes * 100.0 / s.total_site_bytes, 2) AS bandwidth_share_pct
        FROM crawler_traffic c
        CROSS JOIN site_win s
        LEFT JOIN referred_traffic r ON c.bot_stem = r.bot_stem
        WHERE c.w_crawls >= 50
          AND (
              r.w_referrals IS NULL
              OR r.w_referrals = 0
              OR c.w_crawls * 1.0 / GREATEST(r.w_referrals, 1) >= 100.0
              OR c.w_bytes * 1.0 / GREATEST(r.w_referrals, 1) >= 1048576.0
          )
        ORDER BY crawls_per_referral DESC, c.w_bytes DESC LIMIT 10
    """

STALE_BROWSER_VERSION = rf"""
        WITH distinct_uas AS (
            SELECT ua, lower(ua) AS l_ua
            FROM (SELECT DISTINCT ua FROM {{table_name}} WHERE ua IS NOT NULL AND contains(lower(ua), 'chrome'))
        ),
        stale_uas AS (
            SELECT
                ua,
                chrome_version
            FROM (
                SELECT
                    ua,
                    regexp_extract(ua, 'Chrome/([0-9]+)\.', 1) AS chrome_version
                FROM distinct_uas
                WHERE contains(l_ua, 'chrome') AND NOT contains(l_ua, 'bot')
                  AND NOT contains(l_ua, 'crawler') AND NOT contains(l_ua, 'spider')
            )
            WHERE chrome_version IS NOT NULL AND chrome_version != ''
              AND CAST(chrome_version AS INTEGER) < 131
        ),
        ua_parsed AS (
            SELECT
                t.ip,
                s.chrome_version,
                (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} t
            JOIN stale_uas s ON t.ua = s.ua
        ),
        version_aggregates AS (
            SELECT
                chrome_version,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT ip) FILTER (WHERE is_w) AS w_ips,
                COUNT(*) FILTER (WHERE is_b) AS b_reqs
            FROM ua_parsed
            GROUP BY chrome_version
            HAVING COUNT(*) FILTER (WHERE is_w) >= 500
        )
        SELECT
            'Chrome ' || chrome_version AS label,
            w_reqs,
            w_ips,
            w_reqs * 1.0 / GREATEST({coalesce("b_reqs", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}}, 1.0) AS surge_ratio
        FROM version_aggregates
        WHERE CAST(chrome_version AS INTEGER) < 131
          AND w_reqs >= 500
          AND w_reqs > {coalesce("b_reqs", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}} * 2.5
        ORDER BY surge_ratio DESC LIMIT 10
    """

ORPHANED_DEEP_CRAWL = f"""
        WITH deep_requests AS (
            SELECT
                ip,
                url,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
            WHERE url LIKE '/%/%/%'
              AND url NOT LIKE '%.%'
              AND (referer IS NULL OR referer = '' OR referer = '-')
        ),
        agg AS (
            SELECT
                ip,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT url) FILTER (WHERE is_w) AS w_distinct_urls,
                COUNT(*) FILTER (WHERE is_b) AS b_reqs
            FROM deep_requests
            GROUP BY ip
            HAVING COUNT(*) FILTER (WHERE is_w) >= 50
               AND COUNT(DISTINCT url) FILTER (WHERE is_w) >= 10
        )
        SELECT
            ip AS label,
            w_reqs,
            w_distinct_urls,
            {coalesce("b_reqs", "0")} AS b_reqs,
            w_reqs * 1.0 / GREATEST(w_distinct_urls, 1.0) AS requests_per_url,
            w_reqs * 1.0 / GREATEST({coalesce("b_reqs", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}}, 1.0) AS surge_ratio
        FROM agg
        ORDER BY w_reqs DESC LIMIT 15
    """

RESIDENTIAL_FINGERPRINT_DISPERSION = f"""
        WITH fingerprint_asn_maps AS (
            SELECT
                ja4,
                ip,
                asn,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
            WHERE ja4 IS NOT NULL AND ja4 != ''
              AND (p_type IS NULL OR p_type != 'hosting')
        ),
        agg AS (
            SELECT
                ja4,
                COUNT(DISTINCT ip) FILTER (WHERE is_w) AS w_ips,
                COUNT(DISTINCT asn) FILTER (WHERE is_w) AS w_asns,
                COUNT(*) FILTER (WHERE is_w) AS w_requests,
                COUNT(DISTINCT ip) FILTER (WHERE is_b) AS b_ips,
                COUNT(DISTINCT asn) FILTER (WHERE is_b) AS b_asns
            FROM fingerprint_asn_maps
            GROUP BY ja4
            HAVING COUNT(DISTINCT ip) FILTER (WHERE is_w) >= 30
               AND COUNT(DISTINCT asn) FILTER (WHERE is_w) >= 5
        )
        SELECT
            ja4 AS label,
            w_ips,
            w_asns,
            w_requests,
            {coalesce("b_ips", "0")} AS b_ips,
            w_asns * 1.0 / GREATEST({coalesce("b_asns", "0")}, 1.0) AS asn_expansion_ratio
        FROM agg
        ORDER BY asn_expansion_ratio DESC LIMIT 10
    """


def botnet_grouping_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    fp_col = context.get("fp_col", "ja4")
    return _item(
        row[0],
        row[1],
        row[3],
        "distinct IPs",
        {"requests": row[2], "ip_ratio": round(float(row[4]), 1), "filters": {fp_col: row[0]}},
        "critical" if row[1] >= 50 else "warning",
    )  # Raw baseline IPS


_reg(
    id="botnet_grouping",
    category=InsightCategory.security,
    title="Botnet Grouping",
    description="TLS fingerprints (JA3/JA4) using far more distinct IPs than their baseline",
    sql_template=BOTNET_GROUPING,
    required_fields=["ip", "timestamp"],
    row_processor=botnet_grouping_processor,
)


def new_probe_urls_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _item(
        row[0],
        row[1],
        None,
        "requests",
        {"error_pct": float(row[3]) if row[3] is not None else 0, "filters": {"url": row[0]}},
        "critical" if row[1] >= 5 else "warning",
    )


_reg(
    id="new_probe_urls",
    category=InsightCategory.security,
    title="New Probe URLs",
    description="Common attack patterns and sensitive paths appearing for the first time",
    sql_template=NEW_PROBE_URLS,
    required_fields=["url", "status", "timestamp"],
    row_processor=new_probe_urls_processor,
)


def waf_signal_spikes_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _item(
        row[0],
        row[1],
        _scale_baseline(row[2], context),
        "hits",
        {"spike_ratio": float(row[3]), "filters": {"waf_sig_ind": row[0]}},
        "critical" if float(row[3]) >= 10 else "warning",
    )


_reg(
    id="waf_signal_spikes",
    category=InsightCategory.security,
    title="WAF Signal Spikes",
    description="Security signals from the Next-Gen WAF showing unusual activity",
    sql_template=WAF_SIGNAL_SPIKES,
    required_fields=["waf_sig", "timestamp"],
    row_processor=waf_signal_spikes_processor,
)


def proxy_surge_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    p_type, w_cnt, _b_cnt, w_total_all, _b_total_all = row
    return _item(
        p_type,
        round(w_cnt * 100.0 / max(w_total_all, 1), 1),
        None,
        "% of traffic",
        {"requests": w_cnt, "filters": {"p_type": p_type}},
        "warning",
    )


def proxy_surge_severity(items: list[dict]) -> str:
    return "warning" if items else "clean"


_reg(
    id="proxy_surge",
    category=InsightCategory.security,
    title="Anonymizing Proxy Surge",
    description="Significant increase in traffic from known VPNs and anonymizing proxies",
    sql_template=PROXY_SURGE,
    required_fields=["p_type", "timestamp"],
    row_processor=proxy_surge_processor,
    severity_logic=proxy_surge_severity,
)


def cipher_spread_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    label = (row[0] or "")[:12] + "…" if row[0] and len(row[0]) > 12 else (row[0] or "(unknown)")
    return _item(
        label,
        row[1],
        round(_scale_baseline(row[3], context), 1),
        "distinct IPs",
        {"requests": row[2], "baseline_total_ips": row[3], "sha": row[0], "filters": {"tls_ciphers_sha": row[0]}},
        "critical" if row[1] >= 50 else "warning",
    )


_reg(
    id="cipher_spread",
    category=InsightCategory.security,
    title="Cipher Fingerprint Clustering",
    description="TLS cipher suites being used by a suspiciously large and spiking number of distinct IPs",
    sql_template=CIPHER_SPREAD,
    required_fields=["tls_ciphers_sha", "ip", "timestamp"],
    row_processor=cipher_spread_processor,
)


def request_size_anomaly_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _ip_max_vs_p95_processor(
        row, context, unit="bytes (max header)", max_key="max_bytes", avg_key="avg_bytes", crit_thresh=64000
    )


_reg(
    id="request_size_anomaly",
    category=InsightCategory.security,
    title="Oversized Request Headers",
    description="IPs sending headers significantly larger than their historical baseline, potential for DoS or exfiltration",
    sql_template=REQUEST_SIZE_ANOMALY,
    required_fields=["ip", "req_header_bytes", "timestamp"],
    row_processor=request_size_anomaly_processor,
)


def connection_abuse_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _ip_max_vs_p95_processor(
        row, context, unit="reqs/conn (max)", max_key="max_conn_reqs", avg_key="avg_conn_reqs", crit_thresh=500
    )


_reg(
    id="connection_abuse",
    category=InsightCategory.security,
    title="Connection Reuse Anomaly",
    description="IPs making an unusually high number of requests per single TCP connection",
    sql_template=CONNECTION_ABUSE,
    required_fields=["ip", "conn_requests", "timestamp"],
    row_processor=connection_abuse_processor,
)


def _process_repeated_patterns(
    row: tuple, *, key_field: str, diversity_key: str, mask_key: bool, context: dict
) -> dict:
    entity = _mask_ip_if_needed(row[0], context) if mask_key else row[0]
    n_gaps, n_events = int(row[1] or 0), int(row[2] or 0)
    mean_interval, stddev = float(row[3] or 0), float(row[4] or 0)
    cv, modal_frac = float(row[5] or 0), float(row[6] or 0)
    diversity_val, span_s = int(row[7] or 0), int(row[8] or 0)
    mode_gap = int(row[9]) if row[9] is not None else None
    rps = round(n_events / span_s, 4) if span_s else 0.0
    score = round(100 * (0.625 * (1 - min(cv, 1.0)) + 0.375 * modal_frac))
    severity = "critical" if (score >= 90 and n_events >= 30) else ("warning" if score >= 70 else "info")
    meta = {
        "score": score,
        "cv": cv,
        "modal_frac": modal_frac,
        "mean_interval_s": mean_interval,
        "stddev_s": stddev,
        "mode_gap_s": mode_gap,
        "n_gaps": n_gaps,
        "n_events": n_events,
        "span_s": span_s,
        "rps": rps,
        diversity_key: diversity_val,
        "filters": {key_field: entity},
    }
    return _item(
        entity or "(unknown)", mean_interval, stddev, "s interval", meta, severity, baseline_label="jitter (σ)"
    )


def repeated_patterns_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _process_repeated_patterns(row, key_field="ip", diversity_key="distinct_ua", mask_key=True, context=context)


def repeated_patterns_severity(items: list[dict]) -> str:
    if not items:
        return "clean"
    if any(i.get("severity") == "critical" for i in items):
        return "critical"
    return "warning" if any(i.get("severity") == "warning" for i in items) else "info"


_reg(
    id="repeated_patterns",
    category=InsightCategory.security,
    title="Scripted Traffic Patterns",
    description="IPs sending requests on a highly regular cadence — automated scrapers, pollers, or cron-scheduled scripts that evade volumetric rate limits",
    sql_template=REPEATED_PATTERNS,
    required_fields=["ip", "timestamp"],
    row_processor=repeated_patterns_processor,
    severity_logic=repeated_patterns_severity,
)


def repeated_patterns_fp_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _process_repeated_patterns(
        row, key_field=context.get("fp_col", "ja4"), diversity_key="distinct_ip", mask_key=False, context=context
    )


_reg(
    id="repeated_patterns_fp",
    category=InsightCategory.security,
    title="Scripted Traffic Patterns (by TLS Fingerprint)",
    description="TLS fingerprints sending requests on a highly regular cadence — catches IP-rotating scrapers, pollers, or cron-scheduled scripts that share a stable TLS stack across many source addresses",
    sql_template=REPEATED_PATTERNS_FP,
    required_fields=["ip", "timestamp"],
    row_processor=repeated_patterns_fp_processor,
    severity_logic=repeated_patterns_severity,
)


def low_and_slow_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    hits, distinct_paths, span_s, rps = int(row[1] or 0), int(row[2] or 0), int(row[3] or 0), float(row[4] or 0)
    return _item(
        ip or "(unknown)",
        distinct_paths,
        None,
        "sensitive paths",
        {"hits": hits, "distinct_paths": distinct_paths, "span_s": span_s, "rps": rps, "filters": {"ip": ip}},
        "critical" if distinct_paths >= 10 else "warning",
    )


_reg(
    id="low_and_slow",
    category=InsightCategory.security,
    title="Low and Slow Scans",
    description="IPs probing many sensitive / vulnerability paths at a deliberately low request rate spread over a long span — designed to evade rate limits",
    sql_template=LOW_AND_SLOW,
    required_fields=["ip", "url"],
    row_processor=low_and_slow_processor,
)


def credential_enumeration_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    w_denied, w_attempts, w_paths, b_denied = int(row[1] or 0), int(row[2] or 0), int(row[3] or 0), int(row[4] or 0)
    fail_rate = round(w_denied * 100.0 / w_attempts, 1) if w_attempts else 0.0
    return _item(
        ip or "(unknown)",
        w_denied,
        b_denied,
        "denied (401/403)",
        {
            "attempts": w_attempts,
            "denied": w_denied,
            "fail_rate_pct": fail_rate,
            "distinct_paths": w_paths,
            "filters": {"ip": ip},
        },
        "critical" if w_denied >= 100 else "warning",
        baseline_label="baseline denied",
    )


_reg(
    id="credential_enumeration",
    category=InsightCategory.security,
    title="Credential Enumeration / Brute Force",
    description="IPs generating a spike of 401/403 responses on authentication paths (login, auth, OAuth, password reset) — credential stuffing or brute force",
    sql_template=CREDENTIAL_ENUMERATION,
    required_fields=["ip", "url", "status"],
    row_processor=credential_enumeration_processor,
)


def content_discovery_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    w_404, w_total, distinct_404, b_404 = int(row[1] or 0), int(row[2] or 0), int(row[3] or 0), int(row[4] or 0)
    ratio = round(w_404 * 100.0 / w_total, 1) if w_total else 0.0
    return _item(
        ip or "(unknown)",
        w_404,
        b_404,
        "404s",
        {
            "requests": w_total,
            "not_found": w_404,
            "not_found_rate_pct": ratio,
            "distinct_404_urls": distinct_404,
            "filters": {"ip": ip},
        },
        "critical" if w_404 >= 100 else "warning",
        baseline_label="baseline 404s",
    )


_reg(
    id="content_discovery",
    category=InsightCategory.security,
    title="Content-Discovery Scanning",
    description="IPs generating a burst of 404s across many distinct URLs — directory / endpoint enumeration probing for hidden or vulnerable paths",
    sql_template=CONTENT_DISCOVERY,
    required_fields=["ip", "url", "status"],
    row_processor=content_discovery_processor,
)


def session_harvesting_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    w_sessions, w_reqs, b_sessions = int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
    return _item(
        ip or "(unknown)",
        w_sessions,
        b_sessions,
        "distinct sessions",
        {
            "requests": w_reqs,
            "distinct_sessions": w_sessions,
            "baseline_distinct_sessions": b_sessions,
            "filters": {"ip": ip},
        },
        "critical" if w_sessions >= 100 else "warning",
        baseline_label="baseline sessions",
    )


_reg(
    id="session_harvesting",
    category=InsightCategory.security,
    title="Session-ID Harvesting",
    description="A single IP presenting a large, spiking number of distinct session cookies vs. baseline — session-token brute forcing, cookie replay, or credential stuffing that mints a fresh session per attempt",
    sql_template=SESSION_HARVESTING,
    required_fields=["ip", "cookie_session"],
    row_processor=session_harvesting_processor,
)


def crawler_disparity_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    from backend.utils.bot_sources import resolve_bot_display_name

    brand = resolve_bot_display_name(row[0], default="Other Bot")
    w_crawls, w_referrals = int(row[1] or 0), int(row[2] or 0)
    if len(row) >= 8:
        w_bytes, c_per_r, b_c_per_r = int(row[3] or 0), float(row[4] or 0), float(row[5] or 0)
        bytes_per_r, b_bytes_per_r = float(row[6] or 0), float(row[7] or 0)
        bw_pct = round(float(row[8] or 0), 2) if len(row) >= 9 else 0.0
    else:
        w_bytes, c_per_r, b_c_per_r = 0, float(row[3] or 0), float(row[4] or 0)
        bytes_per_r = b_bytes_per_r = bw_pct = 0.0
    is_crit = c_per_r >= 5000 or w_bytes >= 500_000_000 or (bw_pct >= 10.0 and w_referrals == 0)
    meta = {
        "crawls": w_crawls,
        "referrals": w_referrals,
        "w_bytes": w_bytes,
        "bandwidth_share_pct": bw_pct,
        "crawls_per_referral": c_per_r,
        "baseline_crawls_per_referral": b_c_per_r,
        "bytes_per_referral": bytes_per_r,
        "baseline_bytes_per_referral": b_bytes_per_r,
    }
    return _item(brand, c_per_r, b_c_per_r, "crawls/referral", meta, "critical" if is_crit else "warning")


_reg(
    id="crawler_disparity",
    category=InsightCategory.security,
    title="Bot & Crawler Referral & Bandwidth Disparity",
    description="Identifies bots, scrapers, and search/AI crawlers consuming high request volume and egress bandwidth while driving negligible human referral traffic back to the platform",
    sql_template=CRAWLER_DISPARITY,
    required_fields=["ua", "referer", "ip", "resp_bytes", "timestamp"],
    row_processor=crawler_disparity_processor,
)


def stale_browser_version_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    label, w_reqs, w_ips, surge = row[0] or "Unknown Chrome", int(row[1] or 0), int(row[2] or 0), float(row[3] or 0)
    return _item(
        label,
        w_reqs,
        w_ips,
        "requests",
        {"requests": w_reqs, "ips": w_ips, "surge_ratio": surge},
        "critical" if surge >= 5.0 else "warning",
        baseline_label="distinct IPs",
    )


_reg(
    id="stale_browser_version",
    category=InsightCategory.security,
    title="Stale Headless Browser",
    description="Detects anomalous clusters of request traffic using stale Chrome major versions, a standard fingerprint for un-updated Puppeteer/Playwright headless bots",
    sql_template=STALE_BROWSER_VERSION,
    required_fields=["ua", "ip", "timestamp"],
    row_processor=stale_browser_version_processor,
)


def orphaned_deep_crawl_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    w_reqs, w_distinct, b_reqs, reqs_per_url, surge = (
        int(row[1] or 0),
        int(row[2] or 0),
        int(row[3] or 0),
        float(row[4] or 0),
        float(row[5] or 0),
    )
    return _item(
        ip or "(unknown)",
        w_reqs,
        b_reqs,
        "requests",
        {
            "requests": w_reqs,
            "distinct_urls": w_distinct,
            "requests_per_url": reqs_per_url,
            "surge_ratio": surge,
            "filters": {"ip": ip},
        },
        "critical" if reqs_per_url >= 20.0 else "warning",
    )


_reg(
    id="orphaned_deep_crawl",
    category=InsightCategory.security,
    title="Orphaned Deep Crawl",
    description="Flags client IPs performing direct, deep scans of nested catalog/database resource routes with empty HTTP referer headers",
    sql_template=ORPHANED_DEEP_CRAWL,
    required_fields=["ip", "url", "referer", "timestamp"],
    row_processor=orphaned_deep_crawl_processor,
)


def residential_fingerprint_dispersion_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ja4, w_ips, w_asns, w_requests, b_ips, asn_expansion = (
        row[0] or "(empty)",
        int(row[1] or 0),
        int(row[2] or 0),
        int(row[3] or 0),
        int(row[4] or 0),
        float(row[5] or 0),
    )
    return _item(
        ja4,
        w_asns,
        b_ips,
        "ASNs",
        {"ja4": ja4, "ips": w_ips, "asns": w_asns, "requests": w_requests, "asn_expansion_ratio": asn_expansion},
        "critical" if asn_expansion >= 3.0 else "warning",
        baseline_label="baseline IPs",
    )


_reg(
    id="residential_fingerprint_dispersion",
    category=InsightCategory.security,
    title="Residential Fingerprint Dispersion",
    description="Identifies identical TLS/TCP client handshakes distributed across multiple residential ASNs, signaling rotated botnet scans evading ASN blocks",
    sql_template=RESIDENTIAL_FINGERPRINT_DISPERSION,
    required_fields=["ja4", "ip", "asn", "p_type", "timestamp"],
    row_processor=residential_fingerprint_dispersion_processor,
)
