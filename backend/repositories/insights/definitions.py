from __future__ import annotations

import re
from typing import Any

from backend.core.share_db.validation import mask_ip
from backend.repositories._sql import insights as SQL
from backend.utils.geo import format_city_label

from .registry import InsightCategory, InsightDefinition, registry

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
        category=InsightCategory.origin,
        title="Error Spikes",
        description="URLs with abnormally elevated 5xx error rates in the window vs. baseline",
        sql_template=SQL.ERROR_SPIKES,
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
        category=InsightCategory.security,
        title="Botnet Grouping",
        description="TLS fingerprints (JA3/JA4) using far more distinct IPs than their baseline",
        sql_template=SQL.BOTNET_GROUPING,
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
        category=InsightCategory.traffic,
        title="New Country Traffic",
        description="Countries that appeared in the window but had zero requests in the baseline",
        sql_template=SQL.NEW_COUNTRY_TRAFFIC,
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
        category=InsightCategory.traffic,
        title="City Traffic Surges",
        description="Cities experiencing a significant spike in traffic compared to their baseline",
        sql_template=SQL.CITY_SURGES,
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
        category=InsightCategory.origin,
        title="City Error Spikes",
        description="Cities with abnormally high error rates in the window vs. baseline",
        sql_template=SQL.CITY_ERROR_SPIKES,
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
        category=InsightCategory.edge,
        title="City Latency Regressions",
        description="Cities experiencing significant increases in P95 latency",
        sql_template=SQL.CITY_LATENCY_REGRESSIONS,
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
        category=InsightCategory.traffic,
        title="New City Traffic",
        description="Cities that recently started sending traffic after a period of zero activity",
        sql_template=SQL.NEW_CITY_TRAFFIC,
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
        category=InsightCategory.traffic,
        title="User-Agent Monoculture",
        description="User-agents with an unusually high and spiking share of total traffic",
        sql_template=SQL.UA_MONOCULTURE,
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
        category=InsightCategory.security,
        title="New Probe URLs",
        description="Common attack patterns and sensitive paths appearing for the first time",
        sql_template=SQL.NEW_PROBE_URLS,
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
        category=InsightCategory.security,
        title="WAF Signal Spikes",
        description=("Security signals from the Next-Gen WAF showing unusual activity"),
        # Reads from {waf_table} — the insights repo materialises a
        # second TEMP TABLE that pre-unnests "waf_sig" once per request
        # (trimmed, non-empty, excluding BOT-ANALYSIS). When the parent
        # temp table didn't get created or "waf_sig" isn't in schema,
        # {waf_table} substitutes back to the main {table_name} and the
        # inline COALESCE branch below picks up the slack via the legacy
        # all_signals CTE shape.
        sql_template=SQL.WAF_SIGNAL_SPIKES,
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
        category=InsightCategory.security,
        title="Anonymizing Proxy Surge",
        description="Significant increase in traffic from known VPNs and anonymizing proxies",
        sql_template=SQL.PROXY_SURGE,
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
        category=InsightCategory.network,
        title="ASN Concentration",
        description="Traffic spiking from specific Autonomous Systems (ISPs/Data Centers)",
        sql_template=SQL.ASN_CONCENTRATION,
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
        category=InsightCategory.network,
        title="ASN/Metro Performance Regressions",
        description="Specific ISP/Metro combinations showing significantly higher network latency than baseline",
        sql_template=SQL.ASN_METRO_PERFORMANCE,
        required_fields=["asn", "metro", "tcp_rtt", "country", "timestamp"],
        row_processor=asn_metro_performance_processor,
    )
)

# ── 15. Cache Efficiency Collapse ─────────────────────────────────────────


def cache_collapse_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, w_rate, b_rate, w_cacheable, b_cacheable]
    # w_rate / b_rate are HIT/(HIT+MISS) — the cacheable hit ratio (PASS excluded).
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
        category=InsightCategory.edge,
        title="Cache Efficiency Collapse",
        description="URLs whose cacheable hit ratio (HIT/(HIT+MISS)) dropped sharply vs. their baseline",
        sql_template=SQL.CACHE_COLLAPSE,
        required_fields=["url", "cache", "timestamp"],
        row_processor=cache_collapse_processor,
    )
)


# ── 15b. Cacheability Regression ──────────────────────────────────────────


def cacheability_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, w_pass_rate, b_pass_rate, w_total, b_total]
    # w_pass_rate / b_pass_rate are PASS/total — share of requests that bypassed
    # the cache entirely (uncacheable).
    return {
        "label": row[0] or "(empty)",
        "current_val": float(row[1] or 0) * 100,
        "baseline_val": float(row[2] or 0) * 100,
        "unit": "% PASS",
        "meta": {"window_requests": row[3], "baseline_requests": row[4], "filters": {"url": row[0]}},
        "severity": "critical" if (row[1] or 0) >= 0.80 and (row[2] or 0) <= 0.10 else "warning",
    }


registry.register(
    InsightDefinition(
        id="cacheability_regression",
        category=InsightCategory.edge,
        title="Cacheability Regression",
        description="URLs that flipped from cacheable to mostly PASS (uncacheable) vs. their baseline",
        sql_template=SQL.CACHEABILITY_REGRESSION,
        required_fields=["url", "cache", "timestamp"],
        row_processor=cacheability_regression_processor,
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
        category=InsightCategory.edge,
        title="Latency Regression",
        description="Endpoints showing significantly slower P95 response times than baseline",
        sql_template=SQL.LATENCY_REGRESSION,
        required_fields=["url", "elapsed", "timestamp"],
        row_processor=latency_regression_processor,
    )
)

# ── 17. Impossible Distance / Spoofing ────────────────────────────────────────


def impossible_distance_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [fp, hits, worst_excess_km, max_dist_km, min_allowed_km, pop, sample_ip, client_lat, client_lon, pop_lat, pop_lon, tcp_rtt, country, city]
    sample_ip = row[6]
    if context.get("mask_ips") and sample_ip:
        sample_ip = mask_ip(sample_ip)
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
            "sample_ip": sample_ip,
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


# NOTE: impossible_distance needs special hydration for {pop_values} and
# {edge_filter} (InsightDefinition only supports simple placeholders); done in repository.py.

registry.register(
    InsightDefinition(
        id="impossible_distance",
        category=InsightCategory.security,
        title="Impossible Distance / Spoofing",
        description="Traffic where the network latency (RTT) is physically too low for the reported client distance",
        sql_template=SQL.IMPOSSIBLE_DISTANCE,
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
        category=InsightCategory.edge,
        title="Tail Latency Anomaly",
        description="Endpoints where P99 latency is more than 5× higher than P50, indicating major outliers",
        sql_template=SQL.TAIL_LATENCY,
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
        category=InsightCategory.security,
        title="Cipher Fingerprint Clustering",
        description="TLS cipher suites being used by a suspiciously large and spiking number of distinct IPs",
        sql_template=SQL.CIPHER_SPREAD,
        required_fields=["tls_ciphers_sha", "ip", "timestamp"],
        row_processor=cipher_spread_processor,
    )
)

# ── 20. Request Size Anomaly ──────────────────────────────────────────────────


def request_size_anomaly_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ip, max_bytes, avg_bytes, w_total, b_p95]
    # M3: this insight is keyed on the client IP — it appears in the label AND
    # in meta.filters.ip (which also seeds investigate_url). The response
    # middleware masks the filters.ip KEY but not the label or the URL, so an
    # analyst with mask_ips would still read the raw IP. Mask at the source so
    # all three are consistent.
    ip = row[0]
    if context.get("mask_ips") and ip:
        ip = mask_ip(ip)
    return {
        "label": ip or "(unknown)",
        "current_val": int(row[1] or 0),
        "baseline_val": int(row[4] or 0),
        "baseline_label": "P95 baseline",
        "unit": "bytes (max header)",
        "meta": {
            "max_bytes": int(row[1] or 0),
            "avg_bytes": int(row[2] or 0),
            "requests": row[3],
            "p95_baseline": int(row[4] or 0),
            "filters": {"ip": ip},
        },
        "severity": "critical" if (row[1] or 0) > 64000 else "warning",
    }


registry.register(
    InsightDefinition(
        id="request_size_anomaly",
        category=InsightCategory.security,
        title="Oversized Request Headers",
        description="IPs sending headers significantly larger than their historical baseline, potential for DoS or exfiltration",
        sql_template=SQL.REQUEST_SIZE_ANOMALY,
        required_fields=["ip", "req_header_bytes", "timestamp"],
        row_processor=request_size_anomaly_processor,
    )
)

# ── 21. Connection Reuse Anomaly ──────────────────────────────────────────────


def connection_abuse_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ip, max_reqs, avg_reqs, w_total, b_p95]
    # M3: IP-keyed (label + meta.filters.ip + investigate_url) — mask at the
    # source when the analyst policy sets mask_ips. See request_size_anomaly.
    ip = row[0]
    if context.get("mask_ips") and ip:
        ip = mask_ip(ip)
    return {
        "label": ip or "(unknown)",
        "current_val": int(row[1] or 0),
        "baseline_val": int(row[4] or 0),
        "baseline_label": "P95 baseline",
        "unit": "reqs/conn (max)",
        "meta": {
            "max_conn_reqs": int(row[1] or 0),
            "avg_conn_reqs": int(row[2] or 0),
            "requests": row[3],
            "p95_baseline": int(row[4] or 0),
            "filters": {"ip": ip},
        },
        "severity": "critical" if (row[1] or 0) > 500 else "warning",
    }


registry.register(
    InsightDefinition(
        id="connection_abuse",
        category=InsightCategory.security,
        title="Connection Reuse Anomaly",
        description="IPs making an unusually high number of requests per single TCP connection",
        sql_template=SQL.CONNECTION_ABUSE,
        required_fields=["ip", "conn_requests", "timestamp"],
        row_processor=connection_abuse_processor,
    )
)

# ── 22. Regional Latency Degradation ──────────────────────────────────────────


def region_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [server_region, w_p95, b_p95, w_total, b_total, ottfb_p95]
    item: dict[str, Any] = {
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
        category=InsightCategory.network,
        title="Regional Latency Degradation",
        description="Geographic regions showing a significant increase in P95 latency compared to baseline",
        sql_template=SQL.REGION_LATENCY,
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
        category=InsightCategory.edge,
        title="Cache TTL Inefficiency",
        description="URLs with high TTL but very low hit counts, potentially wasting cache space",
        sql_template=SQL.CACHE_TTL_MISMATCH,
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
        category=InsightCategory.edge,
        title="Image Optimization Opportunities",
        description="Large images being served without modern compression (WebP/AVIF), especially to mobile users",
        sql_template=SQL.IMAGE_OPTIMIZATION_OPPORTUNITIES,
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
        category=InsightCategory.origin,
        title="Origin Latency Spike",
        description="Sudden and significant increase in P95 response time from the origin server",
        sql_template=SQL.ORIGIN_LATENCY_SPIKE,
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
        category=InsightCategory.origin,
        title="Origin Error Rate",
        description="Significant increase in 5xx errors returned by the origin server",
        sql_template=SQL.ORIGIN_ERROR_RATE,
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
        category=InsightCategory.origin,
        title="Origin Retries Elevated",
        description="URLs experiencing frequent retries when fetching from origin, indicating backend instability",
        sql_template=SQL.ORIGIN_RETRIES,
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
        category=InsightCategory.origin,
        title="Specific Origin IP Failing",
        description="One or more origin IP addresses are returning significantly more errors than their peers",
        sql_template=SQL.ORIGIN_IP_FAILURE,
        required_fields=["oip", "ost", "timestamp"],
        row_processor=origin_ip_failure_processor,
    )
)

# ── 29. Shield Path Degradation ───────────────────────────────────────────────


def shield_path_degradation_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [edge_pop, shield_pop, w_p50, b_p50, w_cnt]
    edge_pop, shield_pop, cur_p50, base_p50, reqs = row
    ratio = cur_p50 / base_p50 if base_p50 else 0

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
        category=InsightCategory.origin,
        title="Shield Path Degradation",
        description="Increased latency on the network path between edge POPs and shield POPs",
        sql_template=SQL.SHIELD_PATH_DEGRADATION,
        required_fields=["rid", "prid", "edge", "pop", "ottfb", "timestamp"],
        row_processor=shield_path_degradation_processor,
    )
)

# ── 30. Scripted Traffic Patterns ─────────────────────────────────────────────


def repeated_patterns_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ip, n_gaps, n_events, avg_interval, stddev_interval, cv_corr,
    #              modal_frac, distinct_ua, span_s, mode_gap]
    # M3: IP-keyed (label + meta.filters.ip + investigate_url) — mask at the
    # source when the analyst policy sets mask_ips. See connection_abuse.
    ip = row[0]
    if context.get("mask_ips") and ip:
        ip = mask_ip(ip)

    n_gaps = int(row[1] or 0)
    n_events = int(row[2] or 0)
    mean_interval = float(row[3] or 0)
    stddev = float(row[4] or 0)
    cv = float(row[5] or 0)
    modal_frac = float(row[6] or 0)
    distinct_ua = int(row[7] or 0)
    span_s = int(row[8] or 0)
    mode_gap = int(row[9]) if row[9] is not None else None
    rps = round(n_events / span_s, 4) if span_s else 0.0

    # Regularity score 0-100 (higher = more machine-like). The final SQL
    # (plan §5) surfaces only the two robust regularity signals — the
    # Sheppard-corrected CV and the modal-dominance fraction; the optional
    # Bowley term was dropped from the hot-path SQL, so the score renormalizes
    # the CV/modal weights (0.5/0.3 → 0.625/0.375) to span the full 0-100 range.
    score = round(100 * (0.625 * (1 - min(cv, 1.0)) + 0.375 * modal_frac))

    # Soft framing: this is a security *accusation* surfaced to a user, so prefer
    # false negatives. critical only when dense AND near-perfectly regular.
    if score >= 90 and n_events >= 30:
        severity = "critical"
    elif score >= 70:
        severity = "warning"
    else:
        severity = "info"

    return {
        "label": ip or "(unknown)",
        "current_val": mean_interval,
        "baseline_val": stddev,
        "baseline_label": "jitter (σ)",
        "unit": "s interval",
        "meta": {
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
            # Informational only — NOT a gate (D4). High distinct_ua + low CV is
            # a UA-rotating scraper (*more* suspicious), not a reason to suppress.
            "distinct_ua": distinct_ua,
            "filters": {"ip": ip},
        },
        "severity": severity,
    }


def repeated_patterns_severity(items: list[dict]) -> str:
    if not items:
        return "clean"
    if any(i.get("severity") == "critical" for i in items):
        return "critical"
    if any(i.get("severity") == "warning" for i in items):
        return "warning"
    return "info"


registry.register(
    InsightDefinition(
        id="repeated_patterns",
        category=InsightCategory.security,
        title="Scripted Traffic Patterns",
        description=(
            "IPs sending requests on a highly regular cadence — automated scrapers, "
            "pollers, or cron-scheduled scripts that evade volumetric rate limits"
        ),
        sql_template=SQL.REPEATED_PATTERNS,
        required_fields=["ip", "timestamp"],
        row_processor=repeated_patterns_processor,
        severity_logic=repeated_patterns_severity,
    )
)

# ── 30b. Scripted Traffic Patterns (by TLS Fingerprint) ─────────────────────


def repeated_patterns_fp_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [entity, n_gaps, n_events, avg_interval, stddev_interval,
    #              cv_corr, modal_frac, distinct_ip, span_s, mode_gap]
    entity = row[0]
    n_gaps = int(row[1] or 0)
    n_events = int(row[2] or 0)
    mean_interval = float(row[3] or 0)
    stddev = float(row[4] or 0)
    cv = float(row[5] or 0)
    modal_frac = float(row[6] or 0)
    distinct_ip = int(row[7] or 0)
    span_s = int(row[8] or 0)
    mode_gap = int(row[9]) if row[9] is not None else None
    rps = round(n_events / span_s, 4) if span_s else 0.0

    score = round(100 * (0.625 * (1 - min(cv, 1.0)) + 0.375 * modal_frac))

    if score >= 90 and n_events >= 30:
        severity = "critical"
    elif score >= 70:
        severity = "warning"
    else:
        severity = "info"

    fp_col = context.get("fp_col", "ja4")
    return {
        "label": entity or "(unknown)",
        "current_val": mean_interval,
        "baseline_val": stddev,
        "baseline_label": "jitter (σ)",
        "unit": "s interval",
        "meta": {
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
            "distinct_ip": distinct_ip,
            "filters": {fp_col: entity},
        },
        "severity": severity,
    }


registry.register(
    InsightDefinition(
        id="repeated_patterns_fp",
        category=InsightCategory.security,
        title="Scripted Traffic Patterns (by TLS Fingerprint)",
        description=(
            "TLS fingerprints sending requests on a highly regular cadence — catches "
            "IP-rotating scrapers, pollers, or cron-scheduled scripts that share a "
            "stable TLS stack across many source addresses"
        ),
        sql_template=SQL.REPEATED_PATTERNS_FP,
        required_fields=["ip", "timestamp"],
        row_processor=repeated_patterns_fp_processor,
        severity_logic=repeated_patterns_severity,
    )
)

# ── 31. Low-and-Slow Scans ────────────────────────────────────────────────────
# Phase-3: promotes the legacy low_and_slow stub to a computed insight.


def low_and_slow_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ip, hits, distinct_paths, span_s, rps]
    # IP-keyed (label + meta.filters.ip + investigate_url) — mask at the source
    # when the analyst policy sets mask_ips. See request_size_anomaly.
    ip = row[0]
    if context.get("mask_ips") and ip:
        ip = mask_ip(ip)
    hits = int(row[1] or 0)
    distinct_paths = int(row[2] or 0)
    span_s = int(row[3] or 0)
    rps = float(row[4] or 0)
    return {
        "label": ip or "(unknown)",
        "current_val": distinct_paths,
        "baseline_val": None,
        "unit": "sensitive paths",
        "meta": {
            "hits": hits,
            "distinct_paths": distinct_paths,
            "span_s": span_s,
            "rps": rps,
            "filters": {"ip": ip},
        },
        "severity": "critical" if distinct_paths >= 10 else "warning",
    }


registry.register(
    InsightDefinition(
        id="low_and_slow",
        category=InsightCategory.security,
        title="Low and Slow Scans",
        description=(
            "IPs probing many sensitive / vulnerability paths at a deliberately low "
            "request rate spread over a long span — designed to evade rate limits"
        ),
        sql_template=SQL.LOW_AND_SLOW,
        required_fields=["ip", "url"],
        row_processor=low_and_slow_processor,
    )
)

# ── 32. Credential Enumeration / Brute Force ──────────────────────────────────


def credential_enumeration_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ip, w_denied, w_attempts, w_paths, b_denied]
    # IP-keyed — mask at the source when the analyst policy sets mask_ips.
    ip = row[0]
    if context.get("mask_ips") and ip:
        ip = mask_ip(ip)
    w_denied = int(row[1] or 0)
    w_attempts = int(row[2] or 0)
    w_paths = int(row[3] or 0)
    b_denied = int(row[4] or 0)
    fail_rate = round(w_denied * 100.0 / w_attempts, 1) if w_attempts else 0.0
    return {
        "label": ip or "(unknown)",
        "current_val": w_denied,
        "baseline_val": b_denied,
        "baseline_label": "baseline denied",
        "unit": "denied (401/403)",
        "meta": {
            "attempts": w_attempts,
            "denied": w_denied,
            "fail_rate_pct": fail_rate,
            "distinct_paths": w_paths,
            "filters": {"ip": ip},
        },
        "severity": "critical" if w_denied >= 100 else "warning",
    }


registry.register(
    InsightDefinition(
        id="credential_enumeration",
        category=InsightCategory.security,
        title="Credential Enumeration / Brute Force",
        description=(
            "IPs generating a spike of 401/403 responses on authentication paths "
            "(login, auth, OAuth, password reset) — credential stuffing or brute force"
        ),
        sql_template=SQL.CREDENTIAL_ENUMERATION,
        required_fields=["ip", "url", "status"],
        row_processor=credential_enumeration_processor,
    )
)

# ── 33. Network Path (ASN) Health ─────────────────────────────────────────────
# Phase-3: promotes the legacy network_asn_health stub to a computed insight.


def network_asn_health_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [asn, w_ploss, b_ploss, w_jitter, b_jitter, w_retrans, b_retrans, w_total, b_total]
    asn = row[0]
    w_ploss = float(row[1] or 0)
    b_ploss = float(row[2] or 0)
    w_jitter = float(row[3] or 0)
    b_jitter = float(row[4] or 0)
    w_retrans = float(row[5] or 0)
    b_retrans = float(row[6] or 0)
    w_total = int(row[7] or 0)

    names_map = context.get("asn_names", {})
    asn_label = f"AS{asn}"
    if asn in names_map:
        asn_label += f" ({names_map[asn]})"

    return {
        "label": asn_label,
        "current_val": round(w_ploss * 100, 2),
        "baseline_val": round(b_ploss * 100, 2),
        "unit": "% packet loss",
        "meta": {
            "window_packet_loss_pct": round(w_ploss * 100, 2),
            "baseline_packet_loss_pct": round(b_ploss * 100, 2),
            "window_jitter_ms": round(w_jitter / 1000.0, 1),
            "baseline_jitter_ms": round(b_jitter / 1000.0, 1),
            "window_retrans_avg": round(w_retrans, 2),
            "baseline_retrans_avg": round(b_retrans, 2),
            "requests": w_total,
            "asn": asn,
            "filters": {"asn": asn},
        },
        "severity": "critical" if w_ploss >= 0.05 else "warning",
    }


registry.register(
    InsightDefinition(
        id="network_asn_health",
        category=InsightCategory.network,
        title="Network Path (ASN) Health",
        description="ASNs experiencing elevated packet loss, jitter, or TCP retransmissions vs. their baseline",
        sql_template=SQL.NETWORK_ASN_HEALTH,
        required_fields=["asn", "ploss", "rtt_var", "retrans"],
        row_processor=network_asn_health_processor,
    )
)

# ── 34. 404 Content-Discovery Scanning ────────────────────────────────────────
# Track A: per-IP 404 enumeration (directory / endpoint brute-forcing).


def content_discovery_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ip, w_404, w_total, distinct_404, b_404]
    # IP-keyed — mask at the source when the analyst policy sets mask_ips.
    # See request_size_anomaly / credential_enumeration.
    ip = row[0]
    if context.get("mask_ips") and ip:
        ip = mask_ip(ip)
    w_404 = int(row[1] or 0)
    w_total = int(row[2] or 0)
    distinct_404 = int(row[3] or 0)
    b_404 = int(row[4] or 0)
    ratio = round(w_404 * 100.0 / w_total, 1) if w_total else 0.0
    return {
        "label": ip or "(unknown)",
        "current_val": w_404,
        "baseline_val": b_404,
        "baseline_label": "baseline 404s",
        "unit": "404s",
        "meta": {
            "requests": w_total,
            "not_found": w_404,
            "not_found_rate_pct": ratio,
            "distinct_404_urls": distinct_404,
            "filters": {"ip": ip},
        },
        "severity": "critical" if w_404 >= 100 else "warning",
    }


registry.register(
    InsightDefinition(
        id="content_discovery",
        category=InsightCategory.security,
        title="Content-Discovery Scanning",
        description=(
            "IPs generating a burst of 404s across many distinct URLs — directory / "
            "endpoint enumeration probing for hidden or vulnerable paths"
        ),
        sql_template=SQL.CONTENT_DISCOVERY,
        required_fields=["ip", "url", "status"],
        row_processor=content_discovery_processor,
    )
)

# ── 35. Referer Monoculture ───────────────────────────────────────────────────
# Track B: one Referer dominating window traffic (mirrors ua_monoculture).


def referer_monoculture_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [referer, w_cnt, b_cnt, b_total, w_total]
    referer, w_cnt, b_cnt, b_total, w_total = row
    w_rate = float(w_cnt * 100.0 / w_total) if w_total else 0.0
    return {
        "label": referer or "(empty)",
        "current_val": w_rate,
        "baseline_val": float(b_cnt * 100.0 / b_total) if b_total else 0.0,
        "unit": "% of traffic",
        "meta": {"requests": w_cnt, "filters": {"referer": referer}},
        "severity": "critical" if w_rate >= 50 else "warning",
    }


registry.register(
    InsightDefinition(
        id="referer_monoculture",
        category=InsightCategory.traffic,
        title="Referer Monoculture",
        description="A single Referer driving an outsized, spiking share of total traffic — scraping, hotlinking, or a spoofed-referer flood",
        sql_template=SQL.REFERER_MONOCULTURE,
        required_fields=["referer", "timestamp"],
        row_processor=referer_monoculture_processor,
    )
)

# ── 36. HTTP Method Drift ─────────────────────────────────────────────────────
# Track B: a write/verb method surging vs a read-dominated baseline.


def method_drift_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [method, w_cnt, b_cnt, w_total, b_total]
    method, w_cnt, b_cnt, w_total, b_total = row
    w_rate = float(w_cnt * 100.0 / w_total) if w_total else 0.0
    b_rate = float(b_cnt * 100.0 / b_total) if b_total else 0.0
    return {
        "label": method or "(unknown)",
        "current_val": w_rate,
        "baseline_val": b_rate,
        "unit": "% of traffic",
        "meta": {"requests": w_cnt, "method": method, "filters": {"method": method}},
        "severity": "critical" if w_rate >= 25 else "warning",
    }


registry.register(
    InsightDefinition(
        id="method_drift",
        category=InsightCategory.traffic,
        title="HTTP Method Drift",
        description="A write method (POST/PUT/DELETE/…) surging to an outsized share of traffic vs a read-dominated baseline — API abuse or form/credential floods",
        sql_template=SQL.METHOD_DRIFT,
        required_fields=["method", "timestamp"],
        row_processor=method_drift_processor,
    )
)

# ── 37. New ASN Traffic ───────────────────────────────────────────────────────
# Track B: zero-baseline ASN now sending traffic (mirrors new_country/new_city).


def new_asn_traffic_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [asn, w_cnt, b_cnt]
    asn, w_cnt, b_cnt = row
    names_map = context.get("asn_names", {})
    asn_label = f"AS{asn}"
    if asn in names_map:
        asn_label += f" ({names_map[asn]})"
    return {
        "label": asn_label,
        "current_val": w_cnt,
        "baseline_val": 0,
        "unit": "requests",
        "meta": {"requests": w_cnt, "asn": asn, "filters": {"asn": asn}},
        "severity": "warning" if w_cnt >= 100 else "info",
    }


registry.register(
    InsightDefinition(
        id="new_asn_traffic",
        category=InsightCategory.network,
        title="New ASN Traffic",
        description="An ASN (ISP/datacenter) with zero baseline presence now sending meaningful traffic — a new botnet source, proxy pool, or datacenter range",
        sql_template=SQL.NEW_ASN_TRAFFIC,
        required_fields=["asn", "timestamp"],
        row_processor=new_asn_traffic_processor,
    )
)

# ── 37b. ASN Hosting Shift ───────────────────────────────────────────────────
# Track B: per-ASN consumer→datacenter hosting ratio shift.


def asn_hosting_shift_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [asn, w_hosting, w_total, b_hosting, b_total]
    asn, w_hosting, w_total, b_hosting, b_total = row
    w_ratio = round(float(w_hosting * 100.0 / w_total), 1) if w_total else 0.0
    b_ratio = round(float(b_hosting * 100.0 / b_total), 1) if b_total else 0.0
    names_map = context.get("asn_names", {})
    asn_label = f"AS{asn}"
    if asn in names_map:
        asn_label += f" ({names_map[asn]})"
    return {
        "label": asn_label,
        "current_val": w_ratio,
        "baseline_val": b_ratio,
        "unit": "% hosting",
        "meta": {
            "window_hosting": w_hosting,
            "window_total": w_total,
            "asn": asn,
            "filters": {"asn": asn},
        },
        "severity": "critical" if w_ratio >= 60 else "warning",
    }


registry.register(
    InsightDefinition(
        id="asn_hosting_shift",
        category=InsightCategory.network,
        title="ASN Hosting Shift",
        description="An ASN's traffic composition shifting from consumer to datacenter/hosting — bot pools or scrapers spinning up on hosting infrastructure",
        sql_template=SQL.ASN_HOSTING_SHIFT,
        required_fields=["asn", "p_type", "timestamp"],
        row_processor=asn_hosting_shift_processor,
    )
)

# ── 38. Metro Delivery-Rate Degradation ───────────────────────────────────────
# Track B: per-US-metro kernel TCP delivery-rate drop (network last-mile).


def metro_delivery_degradation_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [metro, w_med, b_med, w_total, b_total]
    metro = row[0]
    dma_map = context.get("dma_map", {})
    metro_label = dma_map.get(str(metro)) or f"DMA {metro}"
    # delivery_rate is bytes/sec; present as Mbps (×8 / 1e6).
    w_mbps = round(float(row[1] or 0) * 8 / 1e6, 1)
    b_mbps = round(float(row[2] or 0) * 8 / 1e6, 1)
    return {
        "label": metro_label,
        "current_val": w_mbps,
        "baseline_val": b_mbps,
        "unit": "Mbps (median)",
        "meta": {
            "window_requests": row[3],
            "window_mbps": w_mbps,
            "baseline_mbps": b_mbps,
            "filters": {"metro": metro},
        },
        "severity": "critical" if b_mbps > 0 and w_mbps <= b_mbps * 0.25 else "warning",
    }


registry.register(
    InsightDefinition(
        id="metro_delivery_degradation",
        category=InsightCategory.network,
        title="Metro Delivery-Rate Degradation",
        description="US metro areas whose median TCP delivery rate (throughput) collapsed vs. baseline — regional last-mile or peering degradation",
        sql_template=SQL.METRO_DELIVERY_DEGRADATION,
        required_fields=["metro", "delivery_rate", "timestamp"],
        row_processor=metro_delivery_degradation_processor,
    )
)

# ── 39. Connection-Type Mix Shift ─────────────────────────────────────────────
# Track B: a client connection (type/speed) combo surging in share.


def connection_type_mix_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [c_type, c_speed, w_cnt, b_cnt, w_total, b_total]
    c_type, c_speed, w_cnt, b_cnt, w_total, b_total = row
    w_rate = float(w_cnt * 100.0 / w_total) if w_total else 0.0
    b_rate = float(b_cnt * 100.0 / b_total) if b_total else 0.0
    return {
        "label": f"{c_type or '?'} / {c_speed or '?'}",
        "current_val": w_rate,
        "baseline_val": b_rate,
        "unit": "% of typed traffic",
        "meta": {
            "requests": w_cnt,
            "c_type": c_type,
            "c_speed": c_speed,
            "filters": {"c_type": c_type, "c_speed": c_speed},
        },
        "severity": "warning",
    }


registry.register(
    InsightDefinition(
        id="connection_type_mix",
        category=InsightCategory.network,
        title="Connection-Type Mix Shift",
        description="A client connection type/speed combo (e.g. cellular, datacenter) surging to an outsized share of traffic vs. baseline — a bot pool or routing shift",
        sql_template=SQL.CONNECTION_TYPE_MIX,
        required_fields=["c_type", "c_speed", "timestamp"],
        row_processor=connection_type_mix_processor,
    )
)

# ── 40. PoP Latency Regression ────────────────────────────────────────────────
# Track B: per-Fastly-PoP P95 edge latency regression (finer than region/city).


def pop_latency_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [pop, w_p95, b_p95, w_total, b_total]
    return {
        "label": row[0] or "(unknown)",
        "current_val": float(row[1] or 0),
        "baseline_val": float(row[2] or 0),
        "unit": "ms (P95)",
        "meta": {
            "regression_ratio": float(row[1] / row[2]) if row[2] else 0,
            "window_requests": row[3],
            "filters": {"pop": row[0]},
        },
        "severity": "critical" if float(row[1] or 0) >= 5000 else "warning",
    }


registry.register(
    InsightDefinition(
        id="pop_latency_regression",
        category=InsightCategory.edge,
        title="PoP Latency Regression",
        description="Individual Fastly PoPs (datacenters) whose P95 edge latency regressed sharply vs. baseline — finer-grained than region or city latency",
        sql_template=SQL.POP_LATENCY_REGRESSION,
        required_fields=["pop", "elapsed", "timestamp"],
        row_processor=pop_latency_regression_processor,
    )
)

# ── 41. HTTP/3 → TCP Fallback Spike ───────────────────────────────────────────
# Track B: service-wide QUIC-share drop = clients falling back to TCP.


def http3_fallback_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [w_quic, w_total, b_quic, b_total]
    w_quic, w_total, b_quic, b_total = row
    w_share = round(w_quic * 100.0 / w_total, 1) if w_total else 0.0
    b_share = round(b_quic * 100.0 / b_total, 1) if b_total else 0.0
    return {
        "label": "HTTP/3 (QUIC) adoption",
        "current_val": w_share,
        "baseline_val": b_share,
        "baseline_label": "baseline QUIC share",
        "unit": "% QUIC",
        "meta": {
            "window_quic": int(w_quic or 0),
            "window_total": int(w_total or 0),
            "share_drop_pts": round(b_share - w_share, 1),
            "filters": {"transport": "tcp"},
        },
        "severity": "critical" if (b_share - w_share) >= 40 else "warning",
    }


def http3_fallback_severity(items: list[dict]) -> str:
    if not items:
        return "clean"
    return "critical" if any(i.get("severity") == "critical" for i in items) else "warning"


registry.register(
    InsightDefinition(
        id="http3_fallback",
        category=InsightCategory.network,
        title="HTTP/3 → TCP Fallback Spike",
        description="A service-wide drop in QUIC (HTTP/3) share vs. baseline — clients failing to sustain QUIC and falling back to TCP (middlebox/UDP throttling)",
        sql_template=SQL.HTTP3_FALLBACK,
        required_fields=["transport", "timestamp"],
        row_processor=http3_fallback_processor,
        severity_logic=http3_fallback_severity,
    )
)

# ── 42. Cache HIT-Ratio Cliff ─────────────────────────────────────────────────
# Track B: service-wide edge HIT ratio cliff (headline edge card).


def cache_hit_cliff_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [w_hits, w_cacheable, b_hits, b_cacheable]
    w_hits, w_cacheable, b_hits, b_cacheable = row
    w_rate = round(w_hits * 100.0 / w_cacheable, 1) if w_cacheable else 0.0
    b_rate = round(b_hits * 100.0 / b_cacheable, 1) if b_cacheable else 0.0
    return {
        "label": "Service-wide cache HIT ratio",
        "current_val": w_rate,
        "baseline_val": b_rate,
        "unit": "% HIT",
        "meta": {
            "window_cacheable": int(w_cacheable or 0),
            "baseline_cacheable": int(b_cacheable or 0),
            "drop_pts": round(b_rate - w_rate, 1),
        },
        "severity": "critical" if (b_rate - w_rate) >= 30 else "warning",
    }


def cache_hit_cliff_severity(items: list[dict]) -> str:
    if not items:
        return "clean"
    return "critical" if any(i.get("severity") == "critical" for i in items) else "warning"


registry.register(
    InsightDefinition(
        id="cache_hit_cliff",
        category=InsightCategory.edge,
        title="Cache HIT-Ratio Cliff",
        description="The whole service's edge cache HIT ratio (HIT/(HIT+MISS)) fell off a cliff vs. baseline — a purge storm, TTL change, or origin Cache-Control regression",
        sql_template=SQL.CACHE_HIT_CLIFF,
        required_fields=["cache", "timestamp"],
        row_processor=cache_hit_cliff_processor,
        severity_logic=cache_hit_cliff_severity,
    )
)

# ══════════════════════════════════════════════════════════════════════════════
# Track C — field-gated insights (require the Phase-4 edge fields
# resp_header_content_encoding / cookie_session / oconnect_ms; empty until a
# service re-provisions to emit them AND history accrues).
# ══════════════════════════════════════════════════════════════════════════════

# ── 43. Payload Compression Regression ────────────────────────────────────────


def payload_compression_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [url, w_rate, b_rate, w_total, b_total]  (rates = uncompressed share)
    return {
        "label": row[0] or "(empty)",
        "current_val": float(row[1] or 0) * 100,
        "baseline_val": float(row[2] or 0) * 100,
        "unit": "% uncompressed",
        "meta": {
            "window_requests": row[3],
            "baseline_requests": row[4],
            "filters": {"url": row[0]},
        },
        "severity": "critical" if (row[1] or 0) >= 0.90 else "warning",
    }


registry.register(
    InsightDefinition(
        id="payload_compression_regression",
        category=InsightCategory.edge,
        title="Payload Compression Regression",
        description="Compressible responses (JS/CSS/HTML/JSON/SVG/XML) that flipped from compressed (gzip/br) to served uncompressed vs. baseline — a broken Accept-Encoding path or origin regression inflating egress and TTFB",
        sql_template=SQL.PAYLOAD_COMPRESSION_REGRESSION,
        required_fields=["url", "resp_header_content_encoding", "resp_bytes", "status"],
        row_processor=payload_compression_regression_processor,
    )
)

# ── 44. Session-ID Harvesting / Rotation ──────────────────────────────────────


def session_harvesting_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [ip, w_sessions, w_reqs, b_sessions]
    # IP-keyed — mask at the source when the analyst policy sets mask_ips. The
    # cookie_session hash is only COUNTED upstream (never selected), so no session
    # id can appear in the card; only the client IP needs masking here.
    ip = row[0]
    if context.get("mask_ips") and ip:
        ip = mask_ip(ip)
    w_sessions = int(row[1] or 0)
    w_reqs = int(row[2] or 0)
    b_sessions = int(row[3] or 0)
    return {
        "label": ip or "(unknown)",
        "current_val": w_sessions,
        "baseline_val": b_sessions,
        "baseline_label": "baseline sessions",
        "unit": "distinct sessions",
        "meta": {
            "requests": w_reqs,
            "distinct_sessions": w_sessions,
            "baseline_distinct_sessions": b_sessions,
            "filters": {"ip": ip},
        },
        "severity": "critical" if w_sessions >= 100 else "warning",
    }


registry.register(
    InsightDefinition(
        id="session_harvesting",
        category=InsightCategory.security,
        title="Session-ID Harvesting",
        description="A single IP presenting a large, spiking number of distinct session cookies vs. baseline — session-token brute forcing, cookie replay, or credential stuffing that mints a fresh session per attempt",
        sql_template=SQL.SESSION_HARVESTING,
        required_fields=["ip", "cookie_session"],
        row_processor=session_harvesting_processor,
    )
)

# ── 45. Origin Connect vs Read Timeout Split ──────────────────────────────────


def timeout_split_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    # row schema: [w_conn, b_conn, w_read, b_read, w_total, b_total]  (ms P95)
    w_conn = float(row[0] or 0)
    b_conn = float(row[1] or 0)
    w_read = float(row[2] or 0)
    b_read = float(row[3] or 0)
    conn_regressed = w_conn >= b_conn * 2 and (w_conn - b_conn) >= 50
    read_regressed = w_read >= b_read * 2 and (w_read - b_read) >= 100
    # Surface the dominant phase (larger absolute regression) as the headline.
    if conn_regressed and (not read_regressed or (w_conn - b_conn) >= (w_read - b_read)):
        phase, cur, base = "connect", round(w_conn, 1), round(b_conn, 1)
    else:
        phase, cur, base = "read", round(w_read, 1), round(b_read, 1)
    return {
        "label": f"Origin {phase} P95",
        "current_val": cur,
        "baseline_val": base,
        "unit": "ms (P95)",
        "meta": {
            "phase": phase,
            "connect_p95_ms": round(w_conn, 1),
            "baseline_connect_p95_ms": round(b_conn, 1),
            "read_p95_ms": round(w_read, 1),
            "baseline_read_p95_ms": round(b_read, 1),
            "window_requests": int(row[4] or 0),
        },
        "severity": "critical" if base > 0 and cur >= base * 3 else "warning",
    }


def timeout_split_severity(items: list[dict]) -> str:
    if not items:
        return "clean"
    return "critical" if any(i.get("severity") == "critical" for i in items) else "warning"


registry.register(
    InsightDefinition(
        id="timeout_split",
        category=InsightCategory.origin,
        title="Origin Connect vs Read Timeout Split",
        description="Splits origin slowness into connect (TCP+TLS handshake) vs read (processing to first byte) phases and flags whichever P95 regressed — slow-connect points at origin/LB saturation, slow-read at app/DB processing",
        sql_template=SQL.TIMEOUT_SPLIT,
        required_fields=["oconnect_ms", "ottfb"],
        row_processor=timeout_split_processor,
        severity_logic=timeout_split_severity,
    )
)
