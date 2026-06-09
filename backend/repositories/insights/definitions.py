from __future__ import annotations

import re

from backend.utils.geo import format_city_label

from .registry import InsightDefinition, registry

# ── 1. Error Spikes ───────────────────────────────────────────────────────


def error_spikes_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    """Process a row from the error_spikes query."""
    # row schema: [url, w_rate, b_rate, w_errors, w_total, b_total]
    return {
        "label": row[0] or "(empty)",
        "current_val": float(row[1] or 0) * 100,
        "baseline_val": float(row[2] or 0) * 100,
        "unit": "% 5xx",
        "meta": {"requests": row[4], "errors": row[3], "filters": {"url": row[0]}},
        "severity": "critical" if (row[1] or 0) >= 0.5 else "warning",
    }


registry.register(
    InsightDefinition(
        id="error_spikes",
        title="Error Spikes",
        description="URLs with abnormally elevated 5xx error rates in the window vs. baseline",
        sql_template="""
        WITH base AS (
            SELECT "url", status,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
        )
        SELECT "url",
            SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_errors,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 3 AND w_rate >= 0.05 AND (b_total < 10 OR w_rate >= b_rate * 2 + 0.05)
        ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15
    """,
        required_fields=["url", "status", "timestamp"],
        row_processor=error_spikes_processor,
    )
)

# ── 2. Botnet Grouping ────────────────────────────────────────────────────


def botnet_grouping_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    """Process a row from the botnet_grouping query."""
    # row schema: [fp, w_ips, w_reqs, b_ips, ip_ratio]
    fp_col = context.get("fp_col", "ja4")
    return {
        "label": row[0],
        "current_val": row[1],
        "baseline_val": row[3],  # Raw baseline IPS
        "unit": "distinct IPs",
        "meta": {"requests": row[2], "ip_ratio": round(float(row[4]), 1), "filters": {fp_col: row[0]}},
        "severity": "critical" if row[1] >= 50 else "warning",
    }


registry.register(
    InsightDefinition(
        id="botnet_grouping",
        title="Botnet Grouping",
        description="TLS fingerprints (JA3/JA4) using far more distinct IPs than their baseline",
        sql_template="""
        WITH base AS (
            SELECT "{fp_col}", "ip",
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "{fp_col}" IS NOT NULL AND "{fp_col}" != ''
        )
        SELECT "{fp_col}",
            COUNT(DISTINCT "ip") FILTER (WHERE is_w) AS w_ips,
            COUNT(*) FILTER (WHERE is_w) AS w_reqs,
            COUNT(DISTINCT "ip") FILTER (WHERE is_b) AS b_ips,
            w_ips * 1.0 / GREATEST(COALESCE(b_ips, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1) AS ip_ratio
        FROM base GROUP BY "{fp_col}"
        HAVING w_ips >= 5 AND w_ips > COALESCE(b_ips, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours} * 3
        ORDER BY ip_ratio DESC LIMIT 10
    """,
        required_fields=["ip", "timestamp"],
        row_processor=botnet_grouping_processor,
    )
)

# ── 4. New Country Traffic ────────────────────────────────────────────────


def new_country_traffic_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: ["country", w_cnt, b_cnt]
    return {
        "label": format_city_label("", row[0]),
        "current_val": row[1],
        "baseline_val": 0,
        "unit": "requests",
        "meta": {"filters": {"country": row[0]}},
        "severity": "warning" if row[1] >= 10 else "info",
    }


registry.register(
    InsightDefinition(
        id="new_country_traffic",
        title="New Country Traffic",
        description="Countries that appeared in the window but had zero requests in the baseline",
        sql_template="""
        SELECT "country",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
        FROM {table_name}
        WHERE "country" IS NOT NULL
        GROUP BY "country"
        HAVING w_cnt >= 3 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 20
    """,
        required_fields=["country", "timestamp"],
        row_processor=new_country_traffic_processor,
    )
)

# ── 5. City Traffic Surges ───────────────────────────────────────────────


def city_surges_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [label, city, region, country, w_cnt, b_cnt, spike_ratio]
    return {
        "label": format_city_label(row[1], row[3], row[2]),
        "current_val": row[4],
        "baseline_val": float(row[5]) / max(context["baseline_hours"], 1) * context["window_hours"],
        "unit": "requests",
        "meta": {
            "spike_ratio": round(float(row[6]), 1),
            "filters": {"city": row[1], "region": row[2] if row[2] else None, "country": row[3]},
        },
        "severity": "info" if float(row[6]) < 10 else "warning",
    }


registry.register(
    InsightDefinition(
        id="city_surges",
        title="City Traffic Surges",
        description="Cities experiencing a significant spike in traffic compared to their baseline",
        sql_template="""
        SELECT {label_expr} AS label, "city", {region_sel}, {country_sel},
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            w_cnt * 1.0 / GREATEST(COALESCE(b_cnt, 0) * 1.0 / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1.0) AS spike_ratio
        FROM {table_name}
        WHERE "city" IS NOT NULL AND "city" != ''
        GROUP BY {loc_cols}, label, "city", {region_sel}, {country_sel}
        HAVING w_cnt >= 20 AND w_cnt > COALESCE(b_cnt, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours} * 3
        ORDER BY spike_ratio DESC LIMIT 15
    """,
        required_fields=["city", "timestamp"],
        row_processor=city_surges_processor,
    )
)

# ── 6. City Error Spikes ─────────────────────────────────────────────────


def city_error_spikes_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [label, city, region, country, w_rate, b_rate, w_errors, w_total, b_total]
    return {
        "label": format_city_label(row[1], row[3], row[2]),
        "current_val": float(row[4] or 0) * 100,
        "baseline_val": float(row[5] or 0) * 100,
        "unit": "% error",
        "meta": {
            "requests": row[7],
            "errors": row[6],
            "filters": {"city": row[1], "region": row[2] if row[2] else None, "country": row[3]},
        },
        "severity": "critical" if (row[4] or 0) >= 0.5 else "warning",
    }


registry.register(
    InsightDefinition(
        id="city_error_spikes",
        title="City Error Spikes",
        description="Cities with abnormally high error rates in the window vs. baseline",
        sql_template="""
        WITH base AS (
            SELECT {loc_cols}, {label_expr} AS label, status, "city", {region_sel} AS region, {country_sel} AS country,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "city" IS NOT NULL AND "city" != ''
        )
        SELECT label, "city", region, country,
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_errors,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY ALL
        HAVING w_total >= 10 AND w_rate >= 0.10 AND (b_total < 50 OR w_rate >= b_rate * 3 + 0.05)
        ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15
    """,
        required_fields=["city", "status", "timestamp"],
        row_processor=city_error_spikes_processor,
    )
)

# ── 7. City Latency Regressions ──────────────────────────────────────────


def city_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [label, city, region, country, w_p95, b_p95, w_total, b_total]
    return {
        "label": format_city_label(row[1], row[3], row[2]),
        "current_val": float(row[4] or 0),
        "baseline_val": float(row[5] or 0),
        "unit": "ms (P95)",
        "meta": {
            "regression_ratio": float(row[4] / row[5]) if row[5] else 0,
            "requests": row[6],
            "filters": {"city": row[1], "region": row[2] if row[2] else None, "country": row[3]},
        },
        "severity": "critical" if float(row[4] or 0) >= 5000 else "warning",
    }


registry.register(
    InsightDefinition(
        id="city_latency_regressions",
        title="City Latency Regressions",
        description="Cities experiencing significant increases in P95 latency",
        sql_template="""
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
    """,
        required_fields=["city", "elapsed", "timestamp"],
        row_processor=city_latency_processor,
    )
)

# ── 8. New City Traffic ──────────────────────────────────────────────────


def new_city_traffic_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [label, city, region, country, w_cnt, b_cnt]
    return {
        "label": format_city_label(row[1], row[3], row[2]),
        "current_val": row[4],
        "baseline_val": 0,
        "unit": "requests",
        "meta": {"filters": {"city": row[1], "region": row[2] if row[2] else None, "country": row[3]}},
        "severity": "info" if row[4] < 50 else "warning",
    }


registry.register(
    InsightDefinition(
        id="new_city_traffic",
        title="New City Traffic",
        description="Cities that recently started sending traffic after a period of zero activity",
        sql_template="""
        SELECT {label_expr} AS label, "city", {region_sel}, {country_sel},
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
        FROM {table_name}
        WHERE "city" IS NOT NULL AND "city" != ''
        GROUP BY {loc_cols}, label, "city", {region_sel}, {country_sel}
        HAVING w_cnt >= 5 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 20
    """,
        required_fields=["city", "timestamp"],
        row_processor=new_city_traffic_processor,
    )
)

# ── 9. User-Agent Monoculture ─────────────────────────────────────────────


def ua_monoculture_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ua, w_cnt, b_cnt, b_total, w_total]
    ua, w_cnt, b_cnt, b_total, w_total = row
    # SQL HAVING clause guarantees w_total > 0, but guard severity too
    # so a future SQL refactor that drops the HAVING can't crash the
    # processor with ZeroDivisionError.
    w_rate = float(w_cnt * 100.0 / w_total) if w_total else 0.0
    return {
        "label": ua or "(empty)",
        "current_val": w_rate,
        "baseline_val": float(b_cnt * 100.0 / b_total) if b_total else 0.0,
        "unit": "% of traffic",
        "meta": {"requests": w_cnt, "filters": {"ua": ua}},
        "severity": "critical" if w_rate >= 50 else "warning",
    }


registry.register(
    InsightDefinition(
        id="ua_monoculture",
        title="User-Agent Monoculture",
        description="User-agents with an unusually high and spiking share of total traffic",
        sql_template="""
        SELECT "ua",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            (SELECT COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS b_total,
            (SELECT COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS w_total
        FROM {table_name} GROUP BY "ua"
        HAVING w_total > 0 AND w_cnt * 1.0 / w_total >= 0.25 AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 3 + 0.10)
        ORDER BY w_cnt DESC LIMIT 10
    """,
        required_fields=["ua", "timestamp"],
        row_processor=ua_monoculture_processor,
    )
)

# ── 10. New Probe URLs ─────────────────────────────────────────────────────


def new_probe_urls_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, w_cnt, b_cnt, w_error_pct]
    return {
        "label": row[0],
        "current_val": row[1],
        "baseline_val": None,
        "unit": "requests",
        "meta": {"error_pct": float(row[3]) if row[3] is not None else 0, "filters": {"url": row[0]}},
        "severity": "critical" if row[1] >= 5 else "warning",
    }


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
# Plain alternation — no inline ``(?i)`` flag because the literal ``?``
# breaks the repository's ``sql.count("?")`` placeholder-counting heuristic.
# Case-insensitivity is supplied via the third ``regexp_matches`` arg below.
NEW_PROBE_REGEX = "|".join(re.escape(p) for p in NEW_PROBES)

registry.register(
    InsightDefinition(
        id="new_probe_urls",
        title="New Probe URLs",
        description="Common attack patterns and sensitive paths appearing for the first time",
        sql_template=f"""
        SELECT "url",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            AVG(CASE WHEN "status" >= 400 THEN 1.0 ELSE 0.0 END) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) * 100 AS w_error_pct
        FROM {{table_name}}
        WHERE "url" IS NOT NULL AND (regexp_matches("url", '{NEW_PROBE_REGEX}', 'i'))
        GROUP BY "url"
        HAVING w_cnt > 0 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 25
    """,
        required_fields=["url", "status", "timestamp"],
        row_processor=new_probe_urls_processor,
    )
)

# ── 11. WAF Signal Spikes ──────────────────────────────────────────────────


def waf_signal_spikes_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [signal, w_cnt, b_cnt, spike_ratio]
    return {
        "label": row[0],
        "current_val": row[1],
        "baseline_val": float(row[2]) / max(context["baseline_hours"], 1) * context["window_hours"],
        "unit": "hits",
        "meta": {"spike_ratio": float(row[3]), "filters": {"waf_sig_ind": row[0]}},
        "severity": "critical" if float(row[3]) >= 10 else "warning",
    }


registry.register(
    InsightDefinition(
        id="waf_signal_spikes",
        title="WAF Signal Spikes",
        description="Security signals from the Next-Gen WAF showing unusual activity",
        sql_template="""
        WITH all_signals AS (
            SELECT timestamp, trim(signal) AS signal
            FROM (SELECT timestamp, unnest(string_split("waf_sig", ',')) AS signal FROM {table_name} WHERE "waf_sig" IS NOT NULL AND "waf_sig" != '')
            WHERE trim(signal) != '' AND trim(signal) != 'BOT-ANALYSIS'
        )
        SELECT signal,
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            w_cnt * 1.0 / GREATEST(COALESCE(b_cnt, 0) * 1.0 / {baseline_hours} * {window_hours}, 0.5) AS spike_ratio
        FROM all_signals GROUP BY signal
        HAVING w_cnt >= 3 AND w_cnt > COALESCE(b_cnt, 0) * 1.0 / {baseline_hours} * {window_hours} * 2 + 2
        ORDER BY spike_ratio DESC LIMIT 15
    """,
        required_fields=["waf_sig", "timestamp"],
        row_processor=waf_signal_spikes_processor,
    )
)

# ── 12. Proxy / VPN Surge ──────────────────────────────────────────────────


def proxy_surge_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [p_type, w_cnt, b_cnt, w_total_all, b_total_all]
    p_type, w_cnt, b_cnt, w_total_all, b_total_all = row
    return {
        "label": p_type,
        "current_val": round(w_cnt * 100.0 / max(w_total_all, 1), 1),
        "baseline_val": None,
        "unit": "% of traffic",
        "meta": {"requests": w_cnt, "filters": {"p_type": p_type}},
        "severity": "warning",
    }


def proxy_surge_severity(items: list[dict]) -> str:
    return "warning" if items else "clean"


registry.register(
    InsightDefinition(
        id="proxy_surge",
        title="Anonymizing Proxy Surge",
        description="Significant increase in traffic from known VPNs and anonymizing proxies",
        sql_template="""
        WITH base AS (
            SELECT "p_type",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {table_name} WHERE "p_type" IS NOT NULL AND "p_type" != '' GROUP BY "p_type"
        ),
        totals AS (
            SELECT
                SUM(w_cnt) AS w_proxy_total,
                SUM(b_cnt) AS b_proxy_total,
                (SELECT COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {table_name} WHERE "p_type" IS NOT NULL) AS w_total_all,
                (SELECT COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) FROM {table_name} WHERE "p_type" IS NOT NULL) AS b_total_all
            FROM base
        )
        SELECT b."p_type", b.w_cnt, b.b_cnt, t.w_total_all, t.b_total_all
        FROM base b, totals t
        WHERE (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= 5
          AND (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= (t.b_proxy_total * 100.0 / NULLIF(t.b_total_all, 0)) * 2 + 5
    """,
        required_fields=["p_type", "timestamp"],
        row_processor=proxy_surge_processor,
        severity_logic=proxy_surge_severity,
    )
)

# ── 13. ASN Concentration ──────────────────────────────────────────────────


def asn_concentration_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [asn, w_cnt, b_cnt, b_total, w_total]
    asn, w_cnt, b_cnt, b_total, w_total = row
    names_map = context.get("asn_names", {})
    asn_label = f"AS{asn}"
    if asn in names_map:
        asn_label += f" ({names_map[asn]})"

    # SQL HAVING clause guarantees w_total > 0, but guard severity too
    # so a future SQL refactor that drops the HAVING can't crash the
    # processor with ZeroDivisionError.
    w_rate = float(w_cnt * 100.0 / w_total) if w_total else 0.0
    return {
        "label": asn_label,
        "current_val": w_rate,
        "baseline_val": float(b_cnt * 100.0 / b_total) if b_total else 0.0,
        "unit": "% of traffic",
        "meta": {"requests": w_cnt, "asn": asn, "filters": {"asn": asn}},
        "severity": "critical" if w_rate >= 50 else "warning",
    }


registry.register(
    InsightDefinition(
        id="asn_concentration",
        title="ASN Concentration",
        description="Traffic spiking from specific Autonomous Systems (ISPs/Data Centers)",
        sql_template="""
        SELECT "asn",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            (SELECT COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS b_total,
            (SELECT COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS w_total
        FROM {table_name} WHERE "asn" IS NOT NULL GROUP BY "asn"
        HAVING w_total > 0 AND w_cnt * 1.0 / w_total >= 0.20 AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 3 + 0.10)
        ORDER BY w_cnt DESC LIMIT 10
    """,
        required_fields=["asn", "timestamp"],
        row_processor=asn_concentration_processor,
    )
)

# ── 14. ASN/Metro Performance Regressions ─────────────────────────────────


def asn_metro_performance_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [asn, metro, w_med, b_med, w_total, b_total]
    asn = row[0]
    metro = str(row[1])
    dma_map = context.get("dma_map", {})
    names_map = context.get("asn_names", {})

    asn_label = f"AS{asn}"
    if asn in names_map:
        asn_label += f" ({names_map[asn]})"

    metro_label = dma_map.get(metro) or f"DMA {metro}"

    return {
        "label": f"{asn_label} in {metro_label}",
        "current_val": float(row[2] or 0),
        "baseline_val": float(row[3] or 0),
        "unit": "ms RTT",
        "meta": {
            "regression_ratio": float(row[2] / row[3]) if row[3] else 0,
            "requests": row[4],
            "filters": {"asn": asn, "country": "US"},
        },
        "severity": "critical" if float(row[2] or 0) >= float(row[3] or 0) + 100 else "warning",
    }


registry.register(
    InsightDefinition(
        id="asn_metro_performance",
        title="ASN/Metro Performance Regressions",
        description="Specific ISP/Metro combinations showing significantly higher network latency than baseline",
        sql_template="""
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
    """,
        required_fields=["asn", "metro", "tcp_rtt", "country", "timestamp"],
        row_processor=asn_metro_performance_processor,
    )
)

# ── 15. Cache Efficiency Collapse ─────────────────────────────────────────


def cache_collapse_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, w_rate, b_rate, w_total, b_total]
    return {
        "label": row[0] or "(empty)",
        "current_val": float(row[1] or 0) * 100,
        "baseline_val": float(row[2] or 0) * 100,
        "unit": "% HIT",
        "meta": {"window_requests": row[3], "baseline_requests": row[4], "filters": {"url": row[0]}},
        "severity": "critical" if (row[1] or 0) <= 0.10 and (row[2] or 0) >= 0.60 else "warning",
    }


registry.register(
    InsightDefinition(
        id="cache_collapse",
        title="Cache Efficiency Collapse",
        description="URLs showing a sudden and drastic drop in cache hit rate vs. their baseline",
        sql_template="""
        WITH base AS (
            SELECT "url", cache,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
        )
        SELECT "url",
            SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 5 AND b_total >= 20 AND b_rate >= 0.40 AND w_rate <= b_rate - 0.20 AND w_rate <= b_rate * 0.6
        ORDER BY (COALESCE(b_rate, 0) - w_rate) DESC LIMIT 15
    """,
        required_fields=["url", "cache", "timestamp"],
        row_processor=cache_collapse_processor,
    )
)

# ── 16. Latency Regression ────────────────────────────────────────────────


def latency_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, w_p95, b_p95, w_total, b_total]
    return {
        "label": row[0] or "(empty)",
        "current_val": float(row[1] or 0),
        "baseline_val": float(row[2] or 0),
        "unit": "ms (P95)",
        "meta": {
            "regression_ratio": float(row[1] / row[2]) if row[2] else 0,
            "window_requests": row[3],
            "filters": {"url": row[0]},
        },
        "severity": "critical" if float(row[1] or 0) >= 5000 else "warning",
    }


registry.register(
    InsightDefinition(
        id="latency_regression",
        title="Latency Regression",
        description="Endpoints showing significantly slower P95 response times than baseline",
        sql_template="""
        WITH base AS (
            SELECT "url", elapsed,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE elapsed IS NOT NULL
        )
        SELECT "url",
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 5 AND b_total >= 20 AND w_p95 >= b_p95 * 2.0 AND w_p95 - b_p95 >= 200
        ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
    """,
        required_fields=["url", "elapsed", "timestamp"],
        row_processor=latency_regression_processor,
    )
)

# ── 17. Impossible Distance / Spoofing ────────────────────────────────────────


def impossible_distance_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [fp, hits, worst_excess_km, max_dist_km, min_allowed_km, pop, sample_ip, client_lat, client_lon, pop_lat, pop_lon, tcp_rtt, country, city]
    return {
        "label": row[0],
        "current_val": round(float(row[3]), 0),
        "baseline_val": round(float(row[4]), 0),
        "baseline_label": "max allowed",
        "unit": "km",
        "meta": {
            "hits": row[1],
            "excess_km": round(float(row[2]), 0),
            "pop": row[5],
            "sample_ip": row[6],
            "client_lat": row[7],
            "client_lon": row[8],
            "pop_lat": row[9],
            "pop_lon": row[10],
            "tcp_rtt": row[11],
            "country": row[12],
            "city": row[13],
            "filters": {context.get("fp_col", "ja3"): row[0]},
        },
        "severity": "critical" if float(row[2]) > 5000 else "warning",
    }


# NOTE: This one needs special hydration for {pop_values} and {edge_filter}
# This will be handled in repository.py by pre-hydrating or by passing them to format
# But wait, InsightDefinition only supports simple placeholders.
# I'll need to handle pop_values in repository.py.

registry.register(
    InsightDefinition(
        id="impossible_distance",
        title="Impossible Distance / Spoofing",
        description="Traffic where the network latency (RTT) is physically too low for the reported client distance",
        sql_template="""
        WITH pop_coords(pop_code, pop_lat, pop_lon) AS (VALUES {pop_values}),
        flagged AS (
            SELECT t."{fp_col}" AS fp, t."ip", t."pop", ROUND(t."lat"::DOUBLE, 3) AS client_lat, ROUND(t."lon"::DOUBLE, 3) AS client_lon, pc.pop_lat, pc.pop_lon, t."tcp_rtt", t."country", t."city",
                ROUND(2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(t."lat"::DOUBLE - pc.pop_lat) / 2), 2) + COS(RADIANS(t."lat"::DOUBLE)) * COS(RADIANS(pc.pop_lat)) * POWER(SIN(RADIANS(t."lon"::DOUBLE - pc.pop_lon) / 2), 2))), 1) AS distance_km,
                ROUND((t."tcp_rtt"::DOUBLE / 2.0 / 1e6) * 200000 * 2, 1) AS max_km
            FROM {table_name} t JOIN pop_coords pc ON t."pop" = pc.pop_code
            WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND t."lat" IS NOT NULL AND t."lon" IS NOT NULL AND t."tcp_rtt" IS NOT NULL AND t."tcp_rtt" > 0 AND t."{fp_col}" IS NOT NULL AND t."{fp_col}" != '' {edge_filter}
        )
        SELECT fp, COUNT(*) AS hits, MAX(distance_km - max_km) AS worst_excess_km, MAX(distance_km) AS max_dist_km, MIN(max_km) AS min_allowed_km, ANY_VALUE(pop) AS pop, ANY_VALUE(ip) AS sample_ip, ANY_VALUE(client_lat), ANY_VALUE(client_lon), ANY_VALUE(pop_lat), ANY_VALUE(pop_lon), ANY_VALUE(tcp_rtt), ANY_VALUE(country), ANY_VALUE(city)
        FROM flagged WHERE distance_km > max_km GROUP BY fp HAVING COUNT(*) >= 2 ORDER BY worst_excess_km DESC LIMIT 15
    """,
        required_fields=["pop", "lat", "lon", "tcp_rtt", "timestamp"],
        row_processor=impossible_distance_processor,
    )
)

# ── 18. Tail Latency Anomaly ──────────────────────────────────────────────────


def tail_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, p99_ms, p50_ms, ratio, total]
    return {
        "label": row[0] or "(empty)",
        "current_val": float(row[1]),
        "baseline_val": float(row[2]),
        "baseline_label": "P50",
        "unit": "ms (P99 vs P50)",
        "meta": {
            "p99_ms": float(row[1]),
            "p50_ms": float(row[2]),
            "ratio": float(row[3]),
            "requests": row[4],
            "filters": {"url": row[0]},
        },
        "severity": "critical" if float(row[3]) > 10 else "warning",
    }


registry.register(
    InsightDefinition(
        id="tail_latency",
        title="Tail Latency Anomaly",
        description="Endpoints where P99 latency is more than 5× higher than P50, indicating major outliers",
        sql_template="""
        SELECT "url",
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "elapsed") / 1000.0, 0) AS p99_ms,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "elapsed") / 1000.0, 0) AS p50_ms,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "elapsed") / NULLIF(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "elapsed"), 0), 1) AS ratio,
            COUNT(*) AS total
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "elapsed" IS NOT NULL
        GROUP BY "url" HAVING COUNT(*) >= 20 AND ratio > 5
        ORDER BY ratio DESC LIMIT 15
    """,
        required_fields=["url", "elapsed", "timestamp"],
        row_processor=tail_latency_processor,
    )
)

# ── 19. Cipher Fingerprint Clustering ─────────────────────────────────────────


def cipher_spread_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [tls_ciphers_sha, w_ips, w_reqs, b_ips]
    return {
        "label": (row[0] or "")[:12] + "…" if row[0] and len(row[0]) > 12 else (row[0] or "(unknown)"),
        "current_val": row[1],
        "baseline_val": round(row[3] / max(context["baseline_hours"], 1) * context["window_hours"], 1),
        "unit": "distinct IPs",
        "meta": {
            "requests": row[2],
            "baseline_total_ips": row[3],
            "sha": row[0],
            "filters": {"tls_ciphers_sha": row[0]},
        },
        "severity": "critical" if row[1] >= 50 else "warning",
    }


registry.register(
    InsightDefinition(
        id="cipher_spread",
        title="Cipher Fingerprint Clustering",
        description="TLS cipher suites being used by a suspiciously large and spiking number of distinct IPs",
        sql_template="""
        WITH base AS (
            SELECT "tls_ciphers_sha", "ip",
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "tls_ciphers_sha" IS NOT NULL AND "tls_ciphers_sha" != ''
        )
        SELECT "tls_ciphers_sha",
            COUNT(DISTINCT "ip") FILTER (WHERE is_w) AS w_ips,
            COUNT(*) FILTER (WHERE is_w) AS w_reqs,
            COUNT(DISTINCT "ip") FILTER (WHERE is_b) AS b_ips
        FROM base GROUP BY "tls_ciphers_sha"
        HAVING w_ips >= 5 AND w_ips > COALESCE(b_ips, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours} * 3
        ORDER BY (w_ips * 1.0 / GREATEST(COALESCE(b_ips, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1)) DESC LIMIT 10
    """,
        required_fields=["tls_ciphers_sha", "ip", "timestamp"],
        row_processor=cipher_spread_processor,
    )
)

# ── 20. Request Size Anomaly ──────────────────────────────────────────────────


def request_size_anomaly_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ip, max_bytes, avg_bytes, w_total, b_p95]
    return {
        "label": row[0] or "(unknown)",
        "current_val": int(row[1] or 0),
        "baseline_val": int(row[4] or 0),
        "baseline_label": "P95 baseline",
        "unit": "bytes (max header)",
        "meta": {
            "max_bytes": int(row[1] or 0),
            "avg_bytes": int(row[2] or 0),
            "requests": row[3],
            "p95_baseline": int(row[4] or 0),
            "filters": {"ip": row[0]},
        },
        "severity": "critical" if (row[1] or 0) > 64000 else "warning",
    }


registry.register(
    InsightDefinition(
        id="request_size_anomaly",
        title="Oversized Request Headers",
        description="IPs sending headers significantly larger than their historical baseline, potential for DoS or exfiltration",
        sql_template="""
        WITH base AS (
            SELECT "ip", req_header_bytes,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE req_header_bytes > 0
        ),
        stats AS (
            SELECT "ip",
                MAX(req_header_bytes) FILTER (WHERE is_w) AS max_bytes,
                AVG(req_header_bytes) FILTER (WHERE is_w) AS avg_bytes,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY req_header_bytes) FILTER (WHERE is_b) AS b_p95
            FROM base GROUP BY "ip"
        )
        SELECT "ip", max_bytes, avg_bytes, w_total, b_p95 FROM stats
        WHERE w_total >= 3 AND max_bytes > b_p95 * 3
        ORDER BY max_bytes DESC LIMIT 15
    """,
        required_fields=["ip", "req_header_bytes", "timestamp"],
        row_processor=request_size_anomaly_processor,
    )
)

# ── 21. Connection Reuse Anomaly ──────────────────────────────────────────────


def connection_abuse_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ip, max_reqs, avg_reqs, w_total, b_p95]
    return {
        "label": row[0] or "(unknown)",
        "current_val": int(row[1] or 0),
        "baseline_val": int(row[4] or 0),
        "baseline_label": "P95 baseline",
        "unit": "reqs/conn (max)",
        "meta": {
            "max_conn_reqs": int(row[1] or 0),
            "avg_conn_reqs": int(row[2] or 0),
            "requests": row[3],
            "p95_baseline": int(row[4] or 0),
            "filters": {"ip": row[0]},
        },
        "severity": "critical" if (row[1] or 0) > 500 else "warning",
    }


registry.register(
    InsightDefinition(
        id="connection_abuse",
        title="Connection Reuse Anomaly",
        description="IPs making an unusually high number of requests per single TCP connection",
        sql_template="""
        WITH base AS (
            SELECT "ip", conn_requests,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE conn_requests > 0
        ),
        stats AS (
            SELECT "ip",
                MAX(conn_requests) FILTER (WHERE is_w) AS max_reqs,
                AVG(conn_requests) FILTER (WHERE is_w) AS avg_reqs,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY conn_requests) FILTER (WHERE is_b) AS b_p95
            FROM base GROUP BY "ip"
        )
        SELECT "ip", max_reqs, avg_reqs, w_total, b_p95 FROM stats
        WHERE w_total >= 5 AND max_reqs > b_p95 * 3 AND max_reqs >= 50
        ORDER BY max_reqs DESC LIMIT 15
    """,
        required_fields=["ip", "conn_requests", "timestamp"],
        row_processor=connection_abuse_processor,
    )
)

# ── 22. Regional Latency Degradation ──────────────────────────────────────────


def region_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [server_region, w_p95, b_p95, w_total, b_total, ottfb_p95]
    item = {
        "label": row[0] or "(unknown)",
        "current_val": float(row[1] or 0),
        "baseline_val": float(row[2] or 0),
        "unit": "ms (P95)",
        "meta": {
            "regression_ratio": float(row[1] / row[2]) if row[2] else 0,
            "window_requests": row[3],
            "filters": {"server_region": row[0]},
        },
        "severity": "critical" if float(row[1] or 0) >= 5000 else "warning",
    }
    if row[5] is not None:
        item["meta"]["ottfb_p95_ms"] = float(row[5])
    return item


registry.register(
    InsightDefinition(
        id="region_latency",
        title="Regional Latency Degradation",
        description="Geographic regions showing a significant increase in P95 latency compared to baseline",
        sql_template="""
        WITH base AS (
            SELECT server_region, elapsed, ottfb,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE elapsed IS NOT NULL AND server_region != ''
        ),
        region_stats AS (
            SELECT server_region,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                COUNT(*) FILTER (WHERE is_b) AS b_total
            FROM base GROUP BY server_region
            HAVING w_total >= 20 AND b_total >= 50 AND w_p95 >= b_p95 * 1.5 AND w_p95 - b_p95 >= 100
        ),
        origin_stats AS (
            SELECT server_region,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) / 1000.0, 0) AS ottfb_p95
            FROM base WHERE is_w AND ottfb IS NOT NULL
            GROUP BY server_region HAVING COUNT(*) >= 20
        )
        SELECT r.server_region, r.w_p95, r.b_p95, r.w_total, r.b_total, o.ottfb_p95
        FROM region_stats r LEFT JOIN origin_stats o ON r.server_region = o.server_region
        ORDER BY (r.w_p95 / NULLIF(r.b_p95, 0)) DESC LIMIT 15
    """,
        required_fields=["server_region", "elapsed", "timestamp"],
        row_processor=region_latency_processor,
    )
)

# ── 23. Cache TTL Inefficiency ─────────────────────────────────────────────────


def cache_ttl_mismatch_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [label, avg_ttl, avg_hits, avg_age, sample_count]
    return {
        "label": str(row[0] or "(unknown)"),
        "current_val": float(row[2]),
        "baseline_val": None,
        "unit": "avg hits",
        "meta": {
            "avg_ttl_s": float(row[1]),
            "avg_hits": float(row[2]),
            "avg_age_s": float(row[3]),
            "samples": row[4],
            "filters": {"url": row[0]},
        },
        "severity": "warning",
    }


registry.register(
    InsightDefinition(
        id="cache_ttl_mismatch",
        title="Cache TTL Inefficiency",
        description="URLs with high TTL but very low hit counts, potentially wasting cache space",
        sql_template="""
        SELECT {q_col} AS label,
            ROUND(AVG("ttl"), 0) AS avg_ttl,
            ROUND(AVG("hits"), 1) AS avg_hits,
            ROUND(AVG("age"), 0) AS avg_age,
            COUNT(*) AS sample_count
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "ttl" IS NOT NULL AND "ttl" > 0 AND "hits" IS NOT NULL AND "age" IS NOT NULL
        GROUP BY {q_col} HAVING sample_count >= 10 AND AVG("hits") < 2 AND AVG("ttl") > 60
        ORDER BY AVG("ttl") DESC LIMIT 20
    """,
        required_fields=["ttl", "hits", "age", "timestamp"],
        row_processor=cache_ttl_mismatch_processor,
    )
)

# ── 24. Image Optimization Opportunities ──────────────────────────────────────


def image_optimization_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, request_count, total_bytes, avg_kb, mobile_ratio]
    url, req_count, total_bytes, avg_kb, mob_ratio = row
    severity = (
        "critical"
        if total_bytes > 1024 * 1024 * 10 or avg_kb > 1024
        else ("warning" if total_bytes > 1024 * 1024 * 2 or (mob_ratio and mob_ratio > 0.5) else "info")
    )
    return {
        "label": url,
        "current_val": avg_kb,
        "baseline_val": None,
        "unit": "KB avg",
        "meta": {
            "requests": req_count,
            "total_mb": round(total_bytes / (1024 * 1024), 2),
            "mobile_ratio": round(float(mob_ratio), 2) if mob_ratio is not None else 0,
            "filters": {"url": url},
        },
        "severity": severity,
    }


registry.register(
    InsightDefinition(
        id="image_optimization_opportunities",
        title="Image Optimization Opportunities",
        description="Large images being served without modern compression (WebP/AVIF), especially to mobile users",
        sql_template="""
        SELECT "url", COUNT(*) as request_count, SUM("resp_bytes") as total_bytes,
            ROUND(AVG("resp_bytes") / 1024, 1) as avg_kb,
            ({ua_mobile_sel}) AS mobile_ratio
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "status" = 200
          AND ("url" ILIKE '%.jpg%' OR "url" ILIKE '%.jpeg%' OR "url" ILIKE '%.png%' OR "url" ILIKE '%.gif%')
          AND "url" NOT ILIKE '%auto=webp%' AND "url" NOT ILIKE '%format=auto%' AND "url" NOT ILIKE '%format=webp%' AND "url" NOT ILIKE '%format=avif%'
        GROUP BY "url" HAVING total_bytes > 1024 * 512
        ORDER BY total_bytes DESC LIMIT 15
    """,
        required_fields=["url", "resp_bytes", "status", "timestamp", "ua"],
        row_processor=image_optimization_processor,
    )
)

# ── 25. Origin Latency Spike ──────────────────────────────────────────────────


def origin_latency_spike_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, p95_ms, w_p95, b_p95, requests]
    url, p95_ms, w_p95, b_p95, requests = row
    return {
        "label": url or "(unknown)",
        "current_val": float(p95_ms),
        "baseline_val": round(b_p95, 1),
        "unit": "ms P95",
        "meta": {"requests": requests, "filters": {"url": url}},
        "severity": "critical" if float(p95_ms) > b_p95 * 5 else "warning",
    }


registry.register(
    InsightDefinition(
        id="origin_latency_spike",
        title="Origin Latency Spike",
        description="Sudden and significant increase in P95 response time from the origin server",
        sql_template="""
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
    """,
        required_fields=["ottfb", "timestamp"],
        row_processor=origin_latency_spike_processor,
    )
)

# ── 26. Origin Error Rate ─────────────────────────────────────────────────────


def origin_error_rate_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [status, w_cnt, w_total, b_total, w_5xx, b_5xx]
    status, w_cnt, w_total, b_total, w_5xx, b_5xx = row
    w_rate = w_5xx * 100.0 / w_total
    return {
        "label": f"HTTP {status}",
        "current_val": round(w_cnt * 100.0 / w_total, 1),
        "baseline_val": None,
        "unit": "%",
        "meta": {"filters": {"ost": status}},
        "severity": "critical" if w_rate > 5 else "warning",
    }


registry.register(
    InsightDefinition(
        id="origin_error_rate",
        title="Origin Error Rate",
        description="Significant increase in 5xx errors returned by the origin server",
        sql_template="""
        WITH base AS (
            SELECT "ost" AS status,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "ost" IS NOT NULL
        ),
        totals AS (
            SELECT
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                COUNT(*) FILTER (WHERE is_b) AS b_total,
                COUNT(*) FILTER (WHERE is_w AND status >= 500) AS w_5xx,
                COUNT(*) FILTER (WHERE is_b AND status >= 500) AS b_5xx
            FROM base
        ),
        by_status AS (
            SELECT status, COUNT(*) FILTER (WHERE is_w) AS w_cnt
            FROM base WHERE is_w AND status >= 500
            GROUP BY status
        )
        SELECT s.status, s.w_cnt, t.w_total, t.b_total, t.w_5xx, t.b_5xx
        FROM by_status s, totals t
        WHERE (t.w_5xx * 100.0 / NULLIF(t.w_total, 0)) >= 1.0
          AND (t.w_5xx * 100.0 / NULLIF(t.w_total, 0)) > (t.b_5xx * 100.0 / NULLIF(t.b_total, 0)) * 2
    """,
        required_fields=["ost", "timestamp"],
        row_processor=origin_error_rate_processor,
    )
)

# ── 27. Origin Retries Elevated ───────────────────────────────────────────────


def origin_retries_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, requests, avg_retries, max_retries]
    return {
        "label": row[0] or "(unknown)",
        "current_val": float(row[2]),
        "baseline_val": None,
        "unit": "avg retries",
        "meta": {"max_retries": int(row[3]), "filters": {"url": row[0]}},
        "severity": "warning",
    }


registry.register(
    InsightDefinition(
        id="origin_retries",
        title="Origin Retries Elevated",
        description="URLs experiencing frequent retries when fetching from origin, indicating backend instability",
        sql_template="""
        SELECT {url_col}, COUNT(*) AS requests, ROUND(AVG("oretries"), 2) AS avg_retries, MAX("oretries") AS max_retries
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "oretries" > 0
        GROUP BY {url_col} HAVING requests >= 5
        ORDER BY avg_retries DESC LIMIT 10
    """,
        required_fields=["oretries", "timestamp"],
        row_processor=origin_retries_processor,
    )
)

# ── 28. Specific Origin IP Failing ────────────────────────────────────────────


def origin_ip_failure_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [oip, requests, error_pct, median_rate]
    return {
        "label": row[0],
        "current_val": float(row[2]),
        "baseline_val": round(float(row[3]), 1),
        "unit": "% errors",
        "meta": {"requests": row[1], "filters": {"oip": row[0]}},
        "severity": "critical" if float(row[2]) > 20 else "warning",
    }


registry.register(
    InsightDefinition(
        id="origin_ip_failure",
        title="Specific Origin IP Failing",
        description="One or more origin IP addresses are returning significantly more errors than their peers",
        sql_template="""
        WITH base AS (
            SELECT "oip", "ost" AS status,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "oip" IS NOT NULL AND "oip" != '' AND "ost" IS NOT NULL
        ),
        stats AS (
            SELECT "oip",
                COUNT(*) AS requests,
                ROUND(COUNT(*) FILTER (WHERE status >= 500) * 100.0 / NULLIF(COUNT(*), 0), 1) AS error_pct
            FROM base WHERE is_w
            GROUP BY "oip" HAVING requests >= 10
        ),
        median_calc AS (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY error_pct) AS median_rate FROM stats
        )
        SELECT s.oip, s.requests, s.error_pct, m.median_rate
        FROM stats s, median_calc m
        WHERE s.error_pct > m.median_rate * 3 AND s.error_pct > 5
        ORDER BY s.error_pct DESC
    """,
        required_fields=["oip", "ost", "timestamp"],
        row_processor=origin_ip_failure_processor,
    )
)

# ── 29. Shield Path Degradation ───────────────────────────────────────────────


def shield_path_degradation_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [edge_pop, shield_pop, w_p50, b_p50, w_cnt]
    edge_pop, shield_pop, cur_p50, base_p50, reqs = row
    ratio = cur_p50 / base_p50 if base_p50 else 0

    # We don't have _enrich_with_distance here easily without imports
    # I'll just skip enrichment for now or add it later

    return {
        "label": f"{edge_pop} → {shield_pop}",
        "current_val": float(cur_p50),
        "baseline_val": float(base_p50),
        "unit": "ms P50",
        "severity": "critical" if ratio >= 3.0 else "warning",
        "meta": {"ratio": float(ratio), "requests": reqs},
    }


registry.register(
    InsightDefinition(
        id="shield_path_degradation",
        title="Shield Path Degradation",
        description="Increased latency on the network path between edge POPs and shield POPs",
        sql_template="""
        WITH logs AS (SELECT "rid", "prid", "pop", "ottfb", "edge", timestamp FROM {table_name} WHERE ottfb IS NOT NULL),
        edge_logs AS (SELECT rid, pop, ottfb, timestamp FROM logs WHERE edge = true),
        shield_logs AS (SELECT prid, pop, ottfb, timestamp FROM logs WHERE edge = false AND prid IS NOT NULL AND prid != ''),
        joined AS (
            SELECT e.pop AS edge_pop, COALESCE(s.pop, 'Direct to Origin') AS shield_pop, (e.ottfb - COALESCE(s.ottfb, 0)) / 1000.0 AS transit_ms, e.timestamp
            FROM edge_logs e LEFT JOIN shield_logs s ON s.prid = e.rid
        )
        SELECT edge_pop, shield_pop,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY transit_ms) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_p50,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY transit_ms) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_p50,
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt
        FROM joined GROUP BY 1, 2 HAVING w_cnt >= 5 AND w_p50 > b_p50 * 1.5
        ORDER BY (w_p50 / NULLIF(b_p50, 0)) DESC LIMIT 20
    """,
        required_fields=["rid", "prid", "edge", "pop", "ottfb", "timestamp"],
        row_processor=shield_path_degradation_processor,
    )
)
