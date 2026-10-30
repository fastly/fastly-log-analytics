from __future__ import annotations

from backend.core.metric_expressions import coalesce
from backend.repositories.insights import InsightCategory, InsightDefinition
from backend.repositories.insights.definitions.sql_coalesced import (
    COMPRESSIBLE_REGEX,
    _build_p95_latency_regression_sql,
    _city_filters,
    _fmt_asn_label,
    _fmt_metro_label,
    _item,
    _p95_latency_item,
    _reg,
    _url_rate_regression_processor,
)
from backend.utils.countries import format_city_label

CITY_LATENCY_REGRESSIONS = """
        WITH base AS (
            SELECT {loc_cols}, {label_expr} AS label, elapsed, "city", {region_sel} AS region, {country_sel} AS country,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "city" IS NOT NULL AND "city" != '' AND elapsed IS NOT NULL
        )
        SELECT label, "city", region, country,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY ALL
        HAVING w_total >= 10 AND b_total >= 50 AND w_p95 >= b_p95 * 3.0 AND w_p95 - b_p95 >= 500
        ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
    """

ASN_METRO_PERFORMANCE = """
        WITH base AS (
            SELECT "asn", "metro", tcp_rtt,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "asn" IS NOT NULL AND "metro" IS NOT NULL AND tcp_rtt > 0 AND "country" = 'US'
        )
        SELECT "asn", "metro",
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY tcp_rtt) FILTER (WHERE is_w) / 1000.0 AS w_med,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY tcp_rtt) FILTER (WHERE is_b) / 1000.0 AS b_med,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "asn", "metro"
        HAVING w_total >= 20 AND b_total >= 50 AND w_med >= b_med * 1.5 AND w_med - b_med >= 20
        ORDER BY (w_med - b_med) DESC LIMIT 15
    """

LATENCY_REGRESSION = _build_p95_latency_regression_sql("url", 5, 20, "2.0", 200)

TAIL_LATENCY = """
        SELECT "url",
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "elapsed") / 1000.0, 0) AS p99_ms,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "elapsed") / 1000.0, 0) AS p50_ms,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "elapsed") / NULLIF(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "elapsed"), 0), 1) AS ratio,
            COUNT(*) AS total
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "elapsed" IS NOT NULL
        GROUP BY "url" HAVING COUNT(*) >= 20 AND ratio > 5
        ORDER BY ratio DESC LIMIT 15
    """

REGION_LATENCY = """
        WITH base AS (
            SELECT server_region, elapsed, ottfb,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE elapsed IS NOT NULL AND server_region != ''
        ),
        stats AS (
            SELECT server_region,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                COUNT(*) FILTER (WHERE is_b) AS b_total,
                CASE
                    WHEN COUNT(*) FILTER (WHERE is_w AND ottfb IS NOT NULL) >= 20
                    THEN ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_w AND ottfb IS NOT NULL) / 1000.0, 0)
                    ELSE NULL
                END AS ottfb_p95
            FROM base GROUP BY server_region
            HAVING w_total >= 20 AND b_total >= 50 AND w_p95 >= b_p95 * 1.5 AND w_p95 - b_p95 >= 100
        )
        SELECT server_region, w_p95, b_p95, w_total, b_total, ottfb_p95
        FROM stats
        ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
    """

IMAGE_OPTIMIZATION_OPPORTUNITIES = """
        WITH img_urls AS (
            SELECT "url"
            FROM (SELECT DISTINCT "url" FROM {table_name} WHERE "url" IS NOT NULL)
            WHERE ("url" ILIKE '%.jpg%' OR "url" ILIKE '%.jpeg%' OR "url" ILIKE '%.png%' OR "url" ILIKE '%.gif%')
              AND "url" NOT ILIKE '%auto=webp%' AND "url" NOT ILIKE '%format=auto%' AND "url" NOT ILIKE '%format=webp%' AND "url" NOT ILIKE '%format=avif%'
        )
        SELECT t."url", COUNT(*) as request_count, SUM(t."resp_bytes") as total_bytes,
            ROUND(AVG(t."resp_bytes") / 1024, 1) as avg_kb,
            ({ua_mobile_sel}) AS mobile_ratio
        FROM {table_name} t
        JOIN img_urls i ON t."url" = i."url"
        WHERE t.timestamp >= CAST(? AS TIMESTAMPTZ) AND t."status" = 200
        GROUP BY t."url" HAVING total_bytes > 1024 * 512
        ORDER BY total_bytes DESC LIMIT 15
    """

ORIGIN_LATENCY_SPIKE = """
        WITH base AS (
            SELECT ottfb, {url_col} AS url,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE ottfb IS NOT NULL
        ),
        overall_stats AS (
            SELECT
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_w) / 1000.0 AS w_p95,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_b) / 1000.0 AS b_p95
            FROM base
        ),
        url_stats AS (
            SELECT url, COUNT(*) AS requests, ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) / 1000.0, 1) AS p95_ms
            FROM base WHERE is_w
            GROUP BY url HAVING requests >= 10
        )
        SELECT u.url, u.p95_ms, o.w_p95, o.b_p95, u.requests
        FROM url_stats u, overall_stats o
        WHERE o.w_p95 > o.b_p95 * 2
        ORDER BY u.p95_ms DESC LIMIT 10
    """

SHIELD_PATH_DEGRADATION = f"""
        WITH logs AS (SELECT "rid", "prid", "pop", "ottfb", "edge", timestamp FROM {{table_name}} WHERE ottfb IS NOT NULL),
        edge_logs AS (SELECT rid, pop, ottfb, timestamp FROM logs WHERE edge = true),
        shield_logs AS (SELECT prid, pop, ottfb, timestamp FROM logs WHERE edge = false AND prid IS NOT NULL AND prid != ''),
        joined AS (
            SELECT e.pop AS edge_pop, {coalesce("s.pop", "'Direct to Origin'")} AS shield_pop, (e.ottfb - {coalesce("s.ottfb", "0")}) / 1000.0 AS transit_ms, e.timestamp
            FROM edge_logs e LEFT JOIN shield_logs s ON s.prid = e.rid
        )
        SELECT edge_pop, shield_pop,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY transit_ms) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_p50,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY transit_ms) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_p50,
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt
        FROM joined GROUP BY 1, 2 HAVING w_cnt >= 5 AND w_p50 > b_p50 * 1.5
        ORDER BY (w_p50 / NULLIF(b_p50, 0)) DESC LIMIT 20
    """

NETWORK_ASN_HEALTH = f"""
        WITH base AS (
            SELECT "asn" AS asn, ploss, rtt_var, retrans,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
            WHERE "asn" IS NOT NULL
        ),
        agg AS (
            SELECT asn,
                AVG(ploss) FILTER (WHERE is_w) AS w_ploss,
                AVG(ploss) FILTER (WHERE is_b) AS b_ploss,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rtt_var) FILTER (WHERE is_w) AS w_jitter,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rtt_var) FILTER (WHERE is_b) AS b_jitter,
                AVG(retrans) FILTER (WHERE is_w) AS w_retrans,
                AVG(retrans) FILTER (WHERE is_b) AS b_retrans,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                COUNT(*) FILTER (WHERE is_b) AS b_total
            FROM base
            GROUP BY asn
            HAVING COUNT(*) FILTER (WHERE is_w) >= 50 AND COUNT(*) FILTER (WHERE is_b) >= 100
        )
        SELECT asn,
            w_ploss,
            b_ploss,
            w_jitter,
            b_jitter,
            w_retrans,
            b_retrans,
            w_total,
            b_total
        FROM agg
        WHERE (
                ({coalesce("w_ploss", "0")} >= 0.02 AND {coalesce("w_ploss", "0")} >= {coalesce("b_ploss", "0")} * 2 + 0.005)
             OR (w_jitter >= b_jitter * 2 AND w_jitter - b_jitter >= 20000)
             OR ({coalesce("w_retrans", "0")} >= {coalesce("b_retrans", "0")} * 2 + 0.5)
           )
        ORDER BY {coalesce("w_ploss", "0")} DESC, w_jitter DESC
        LIMIT 15
    """

METRO_DELIVERY_DEGRADATION = """
        WITH base AS (
            SELECT "metro" AS metro, delivery_rate,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "metro" IS NOT NULL AND delivery_rate > 0
        )
        SELECT metro,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_rate) FILTER (WHERE is_w) AS w_med,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_rate) FILTER (WHERE is_b) AS b_med,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY metro
        HAVING w_total >= 20 AND b_total >= 50 AND w_med > 0 AND b_med > 0 AND w_med <= b_med * 0.5
        ORDER BY (b_med - w_med) DESC LIMIT 15
    """

POP_LATENCY_REGRESSION = _build_p95_latency_regression_sql(
    "pop", 20, 50, "1.5", 100, '"pop" IS NOT NULL AND "pop" != \'\''
)

HTTP3_FALLBACK = """
        WITH base AS (
            SELECT lower("transport") AS transport,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "transport" IS NOT NULL AND "transport" != ''
        )
        SELECT
            COUNT(*) FILTER (WHERE is_w AND transport = 'quic') AS w_quic,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b AND transport = 'quic') AS b_quic,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base
        HAVING COUNT(*) FILTER (WHERE is_w) >= 100 AND COUNT(*) FILTER (WHERE is_b) >= 100
            AND COUNT(*) FILTER (WHERE is_b AND transport = 'quic') * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) >= 0.30
            AND COUNT(*) FILTER (WHERE is_w AND transport = 'quic') * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0)
                <= COUNT(*) FILTER (WHERE is_b AND transport = 'quic') * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) - 0.20
    """

PAYLOAD_COMPRESSION_REGRESSION = f"""
        WITH comp_urls AS (
            SELECT "url"
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
            WHERE regexp_matches("url", '{COMPRESSIBLE_REGEX}', 'i')
        ),
        base AS (
            SELECT t."url",
                -- "compressed" = the encoding CONTAINS a known token, so a
                -- multi-value ("gzip, br") or whitespace-padded ("gzip ") header
                -- still counts as compressed; empty / "identity" / "none" are
                -- uncompressed.
                (NOT regexp_matches(lower({coalesce('t."resp_header_content_encoding"', "''")}), 'gzip|br|zstd|deflate')) AS uncompressed,
                (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} t
            JOIN comp_urls c ON t."url" = c."url"
            WHERE t."status" = 200 AND t."resp_bytes" >= 1024
        )
        SELECT "url",
            SUM(CASE WHEN uncompressed THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN uncompressed THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 10 AND b_total >= 50 AND b_rate <= 0.20 AND w_rate >= 0.50 AND w_rate >= b_rate + 0.30
        ORDER BY (w_rate - {coalesce("b_rate", "0")}) DESC LIMIT 15
    """

TIMEOUT_SPLIT = """
        WITH base AS (
            SELECT
                oconnect_ms,
                GREATEST(ottfb / 1000.0 - oconnect_ms, 0) AS read_ms,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE oconnect_ms IS NOT NULL AND ottfb IS NOT NULL
        ),
        agg AS (
            SELECT
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY oconnect_ms) FILTER (WHERE is_w) AS w_conn,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY oconnect_ms) FILTER (WHERE is_b) AS b_conn,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY read_ms) FILTER (WHERE is_w) AS w_read,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY read_ms) FILTER (WHERE is_b) AS b_read,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                COUNT(*) FILTER (WHERE is_b) AS b_total
            FROM base
        )
        SELECT w_conn, b_conn, w_read, b_read, w_total, b_total
        FROM agg
        WHERE w_total >= 50 AND b_total >= 100
            AND ((w_conn >= b_conn * 2 AND w_conn - b_conn >= 50)
                 OR (w_read >= b_read * 2 AND w_read - b_read >= 100))
    """


def city_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _p95_latency_item(
        format_city_label(row[1], row[3], row[2]), row[4], row[5], row[6], _city_filters(row), reqs_key="requests"
    )


_reg(
    id="city_latency_regressions",
    category=InsightCategory.edge,
    title="City Latency Regressions",
    description="Cities experiencing significant increases in P95 latency",
    sql_template=CITY_LATENCY_REGRESSIONS,
    required_fields=["city", "elapsed", "timestamp"],
    row_processor=city_latency_processor,
)


def asn_metro_performance_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_f, b_f = row[0], float(row[2] or 0), float(row[3] or 0)
    return _item(
        f"{_fmt_asn_label(asn, context)} in {_fmt_metro_label(row[1], context)}",
        w_f,
        b_f,
        "ms RTT",
        {
            "regression_ratio": float(row[2] / row[3]) if row[3] else 0,
            "requests": row[4],
            "filters": {"asn": asn, "country": "US"},
        },
        "critical" if w_f >= b_f + 100 else "warning",
    )


_reg(
    id="asn_metro_performance",
    category=InsightCategory.network,
    title="ASN/Metro Performance Regressions",
    description="Specific ISP/Metro combinations showing significantly higher network latency than baseline",
    sql_template=ASN_METRO_PERFORMANCE,
    required_fields=["asn", "metro", "tcp_rtt", "country", "timestamp"],
    row_processor=asn_metro_performance_processor,
)


def latency_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _p95_latency_item(row[0] or "(empty)", row[1], row[2], row[3], {"url": row[0]})


_reg(
    id="latency_regression",
    category=InsightCategory.edge,
    title="Latency Regression",
    description="Endpoints showing significantly slower P95 response times than baseline",
    sql_template=LATENCY_REGRESSION,
    required_fields=["url", "elapsed", "timestamp"],
    row_processor=latency_regression_processor,
)


def tail_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    p99, p50, ratio = float(row[1]), float(row[2]), float(row[3])
    return _item(
        row[0] or "(empty)",
        p99,
        p50,
        "ms (P99 vs P50)",
        {"p99_ms": p99, "p50_ms": p50, "ratio": ratio, "requests": row[4], "filters": {"url": row[0]}},
        "critical" if ratio > 10 else "warning",
        baseline_label="P50",
    )


_reg(
    id="tail_latency",
    category=InsightCategory.edge,
    title="Tail Latency Anomaly",
    description="Endpoints where P99 latency is more than 5× higher than P50, indicating major outliers",
    sql_template=TAIL_LATENCY,
    required_fields=["url", "elapsed", "timestamp"],
    row_processor=tail_latency_processor,
)


def region_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    item = _p95_latency_item(row[0] or "(unknown)", row[1], row[2], row[3], {"server_region": row[0]})
    if row[5] is not None:
        item["meta"]["ottfb_p95_ms"] = float(row[5])
    return item


_reg(
    id="region_latency",
    category=InsightCategory.network,
    title="Regional Latency Degradation",
    description="Geographic regions showing a significant increase in P95 latency compared to baseline",
    sql_template=REGION_LATENCY,
    required_fields=["server_region", "elapsed", "timestamp"],
    row_processor=region_latency_processor,
)


def image_optimization_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    url, req_count, total_bytes, avg_kb, mob_ratio = row
    severity = (
        "critical"
        if total_bytes > 1024 * 1024 * 10 or avg_kb > 1024
        else ("warning" if total_bytes > 1024 * 1024 * 2 or (mob_ratio and mob_ratio > 0.5) else "info")
    )
    return _item(
        url,
        avg_kb,
        None,
        "KB avg",
        {
            "requests": req_count,
            "total_mb": round(total_bytes / (1024 * 1024), 2),
            "mobile_ratio": round(float(mob_ratio), 2) if mob_ratio is not None else 0,
            "filters": {"url": url},
        },
        severity,
    )


_reg(
    id="image_optimization_opportunities",
    category=InsightCategory.edge,
    title="Image Optimization Opportunities",
    description="Large images being served without modern compression (WebP/AVIF), especially to mobile users",
    sql_template=IMAGE_OPTIMIZATION_OPPORTUNITIES,
    required_fields=["url", "resp_bytes", "status", "timestamp", "ua"],
    row_processor=image_optimization_processor,
)


def origin_latency_spike_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    url, p95_ms, _w_p95, b_p95, requests = row
    return _item(
        url or "(unknown)",
        float(p95_ms),
        round(b_p95, 1),
        "ms P95",
        {"requests": requests, "filters": {"url": url}},
        "critical" if float(p95_ms) > b_p95 * 5 else "warning",
    )


_reg(
    id="origin_latency_spike",
    category=InsightCategory.origin,
    title="Origin Latency Spike",
    description="Sudden and significant increase in P95 response time from the origin server",
    sql_template=ORIGIN_LATENCY_SPIKE,
    required_fields=["ottfb", "timestamp"],
    row_processor=origin_latency_spike_processor,
)


def shield_path_degradation_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    edge_pop, shield_pop, cur_p50, base_p50, reqs = row
    ratio = cur_p50 / base_p50 if base_p50 else 0
    return _item(
        f"{edge_pop} → {shield_pop}",
        float(cur_p50),
        float(base_p50),
        "ms P50",
        {"ratio": float(ratio), "requests": reqs},
        "critical" if ratio >= 3.0 else "warning",
    )


_reg(
    id="shield_path_degradation",
    category=InsightCategory.origin,
    title="Shield Path Degradation",
    description="Increased latency on the network path between edge POPs and shield POPs",
    sql_template=SHIELD_PATH_DEGRADATION,
    required_fields=["rid", "prid", "edge", "pop", "ottfb", "timestamp"],
    row_processor=shield_path_degradation_processor,
)


def network_asn_health_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_ploss, b_ploss = row[0], float(row[1] or 0), float(row[2] or 0)
    w_jitter, b_jitter, w_retrans, b_retrans, w_total = (
        float(row[3] or 0),
        float(row[4] or 0),
        float(row[5] or 0),
        float(row[6] or 0),
        int(row[7] or 0),
    )
    meta = {
        "window_packet_loss_pct": round(w_ploss * 100, 2),
        "baseline_packet_loss_pct": round(b_ploss * 100, 2),
        "window_jitter_ms": round(w_jitter / 1000.0, 1),
        "baseline_jitter_ms": round(b_jitter / 1000.0, 1),
        "window_retrans_avg": round(w_retrans, 2),
        "baseline_retrans_avg": round(b_retrans, 2),
        "requests": w_total,
        "asn": asn,
        "filters": {"asn": asn},
    }
    return _item(
        _fmt_asn_label(asn, context),
        round(w_ploss * 100, 2),
        round(b_ploss * 100, 2),
        "% packet loss",
        meta,
        "critical" if w_ploss >= 0.05 else "warning",
    )


_reg(
    id="network_asn_health",
    category=InsightCategory.network,
    title="Network Path (ASN) Health",
    description="ASNs experiencing elevated packet loss, jitter, or TCP retransmissions vs. their baseline",
    sql_template=NETWORK_ASN_HEALTH,
    required_fields=["asn", "ploss", "rtt_var", "retrans"],
    row_processor=network_asn_health_processor,
)


def metro_delivery_degradation_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    metro = row[0]
    w_mbps, b_mbps = round(float(row[1] or 0) * 8 / 1e6, 1), round(float(row[2] or 0) * 8 / 1e6, 1)
    return _item(
        _fmt_metro_label(metro, context),
        w_mbps,
        b_mbps,
        "Mbps (median)",
        {"window_requests": row[3], "window_mbps": w_mbps, "baseline_mbps": b_mbps, "filters": {"metro": metro}},
        "critical" if b_mbps > 0 and w_mbps <= b_mbps * 0.25 else "warning",
    )


_reg(
    id="metro_delivery_degradation",
    category=InsightCategory.network,
    title="Metro Delivery-Rate Degradation",
    description="US metro areas whose median TCP delivery rate (throughput) collapsed vs. baseline — regional last-mile or peering degradation",
    sql_template=METRO_DELIVERY_DEGRADATION,
    required_fields=["metro", "delivery_rate", "timestamp"],
    row_processor=metro_delivery_degradation_processor,
)


def pop_latency_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _p95_latency_item(row[0] or "(unknown)", row[1], row[2], row[3], {"pop": row[0]})


_reg(
    id="pop_latency_regression",
    category=InsightCategory.edge,
    title="PoP Latency Regression",
    description="Individual Fastly PoPs (datacenters) whose P95 edge latency regressed sharply vs. baseline — finer-grained than region or city latency",
    sql_template=POP_LATENCY_REGRESSION,
    required_fields=["pop", "elapsed", "timestamp"],
    row_processor=pop_latency_regression_processor,
)


def http3_fallback_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    w_quic, w_total, b_quic, b_total = row
    w_share, b_share = (
        (round(w_quic * 100.0 / w_total, 1) if w_total else 0.0),
        (round(b_quic * 100.0 / b_total, 1) if b_total else 0.0),
    )
    return _item(
        "HTTP/3 (QUIC) adoption",
        w_share,
        b_share,
        "% QUIC",
        {
            "window_quic": int(w_quic or 0),
            "window_total": int(w_total or 0),
            "share_drop_pts": round(b_share - w_share, 1),
            "filters": {"transport": "tcp"},
        },
        "critical" if (b_share - w_share) >= 40 else "warning",
        baseline_label="baseline QUIC share",
    )


_reg(
    id="http3_fallback",
    category=InsightCategory.network,
    title="HTTP/3 → TCP Fallback Spike",
    description="A service-wide drop in QUIC (HTTP/3) share vs. baseline — clients failing to sustain QUIC and falling back to TCP (middlebox/UDP throttling)",
    sql_template=HTTP3_FALLBACK,
    required_fields=["transport", "timestamp"],
    row_processor=http3_fallback_processor,
)


def payload_compression_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _url_rate_regression_processor(row, "% uncompressed", is_critical=bool((row[1] or 0) >= 0.90))


_reg(
    id="payload_compression_regression",
    category=InsightCategory.edge,
    title="Payload Compression Regression",
    description="Compressible responses (JS/CSS/HTML/JSON/SVG/XML) that flipped from compressed (gzip/br) to served uncompressed vs. baseline — a broken Accept-Encoding path or origin regression inflating egress and TTFB",
    sql_template=PAYLOAD_COMPRESSION_REGRESSION,
    required_fields=["url", "resp_header_content_encoding", "resp_bytes", "status"],
    row_processor=payload_compression_regression_processor,
)


def timeout_split_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    w_conn, b_conn, w_read, b_read = float(row[0] or 0), float(row[1] or 0), float(row[2] or 0), float(row[3] or 0)
    conn_regressed = w_conn >= b_conn * 2 and (w_conn - b_conn) >= 50
    read_regressed = w_read >= b_read * 2 and (w_read - b_read) >= 100
    if conn_regressed and (not read_regressed or (w_conn - b_conn) >= (w_read - b_read)):
        phase, cur, base = "connect", round(w_conn, 1), round(b_conn, 1)
    else:
        phase, cur, base = "read", round(w_read, 1), round(b_read, 1)
    meta = {
        "phase": phase,
        "connect_p95_ms": round(w_conn, 1),
        "baseline_connect_p95_ms": round(b_conn, 1),
        "read_p95_ms": round(w_read, 1),
        "baseline_read_p95_ms": round(b_read, 1),
        "window_requests": int(row[4] or 0),
    }
    return _item(
        f"Origin {phase} P95", cur, base, "ms (P95)", meta, "critical" if base > 0 and cur >= base * 3 else "warning"
    )


_reg(
    id="timeout_split",
    category=InsightCategory.origin,
    title="Origin Connect vs Read Timeout Split",
    description="Splits origin slowness into connect (TCP+TLS handshake) vs read (processing to first byte) phases and flags whichever P95 regressed — slow-connect points at origin/LB saturation, slow-read at app/DB processing",
    sql_template=TIMEOUT_SPLIT,
    required_fields=["oconnect_ms", "ottfb"],
    row_processor=timeout_split_processor,
)
