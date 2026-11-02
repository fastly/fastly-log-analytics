from __future__ import annotations

from backend.core.metric_expressions import coalesce
from backend.repositories.insights import InsightCategory, InsightDefinition
from backend.repositories.insights.definitions.sql_coalesced import (
    _build_monoculture_sql,
    _build_new_dim_traffic_sql,
    _city_filters,
    _fmt_asn_label,
    _item,
    _mask_ip_if_needed,
    _monoculture_processor,
    _reg,
    _scale_baseline,
    _traffic_share_rates,
    _zero_baseline_count_item,
)
from backend.utils.countries import format_city_label

NEW_COUNTRY_TRAFFIC = _build_new_dim_traffic_sql("country", 3)

CITY_SURGES = f"""
        SELECT {{label_expr}} AS label, "city", {{region_sel}}, {{country_sel}},
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            w_cnt * 1.0 / GREATEST({coalesce("b_cnt", "0")} * 1.0 / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}}, 1.0) AS spike_ratio
        FROM {{table_name}}
        WHERE "city" IS NOT NULL AND "city" != ''
        GROUP BY {{loc_cols}}, label, "city", {{region_sel}}, {{country_sel}}
        HAVING w_cnt >= 20 AND w_cnt > {coalesce("b_cnt", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}} * 3
        ORDER BY spike_ratio DESC LIMIT 15
    """

NEW_CITY_TRAFFIC = """
        SELECT {label_expr} AS label, "city", {region_sel}, {country_sel},
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
        FROM {table_name}
        WHERE "city" IS NOT NULL AND "city" != ''
        GROUP BY {loc_cols}, label, "city", {region_sel}, {country_sel}
        HAVING w_cnt >= 5 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 20
    """

UA_MONOCULTURE = _build_monoculture_sql("ua", "0.25")
ASN_CONCENTRATION = _build_monoculture_sql("asn", "0.20", '"asn" IS NOT NULL')

IMPOSSIBLE_DISTANCE = """
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
    """

REFERER_MONOCULTURE = _build_monoculture_sql("referer", "0.20", '"referer" IS NOT NULL AND "referer" != \'\'')

METHOD_DRIFT = f"""
        WITH grp AS (
            SELECT "method",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {{table_name}}
            GROUP BY "method"
        ),
        with_totals AS (
            SELECT "method", w_cnt, b_cnt,
                CAST({coalesce("SUM(w_cnt) OVER ()", "0")} AS BIGINT) AS w_total,
                CAST({coalesce("SUM(b_cnt) OVER ()", "0")} AS BIGINT) AS b_total
            FROM grp
        )
        SELECT "method", w_cnt, b_cnt, w_total, b_total
        FROM with_totals
        WHERE "method" IS NOT NULL AND "method" != '' AND upper("method") NOT IN ('GET', 'HEAD', 'OPTIONS')
          AND w_total > 0 AND w_cnt >= 20 AND w_cnt * 1.0 / w_total >= 0.10
          AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 2 + 0.05)
        ORDER BY w_cnt DESC LIMIT 10
    """

NEW_ASN_TRAFFIC = _build_new_dim_traffic_sql("asn", 20)

ASN_HOSTING_SHIFT = """
        SELECT "asn",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "p_type" = 'hosting') AS w_hosting,
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_total,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ) AND "p_type" = 'hosting') AS b_hosting,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_total
        FROM {table_name}
        WHERE "asn" IS NOT NULL AND "p_type" IS NOT NULL AND "p_type" != ''
        GROUP BY "asn"
        HAVING w_total >= 50
            AND w_total > 0
            AND (CAST(w_hosting AS DOUBLE) / w_total) >= 0.3
            AND (b_total = 0 OR (CAST(w_hosting AS DOUBLE) / w_total) >= (CAST(b_hosting AS DOUBLE) / NULLIF(b_total, 0)) * 3 + 0.10)
        ORDER BY w_hosting DESC LIMIT 15
    """

CONNECTION_TYPE_MIX = """
        WITH base AS (
            SELECT "c_type" AS c_type, "c_speed" AS c_speed,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "c_type" IS NOT NULL AND "c_type" != ''
        ),
        grouped AS (
            SELECT c_type, c_speed,
                COUNT(*) FILTER (WHERE is_w) AS w_cnt,
                COUNT(*) FILTER (WHERE is_b) AS b_cnt
            FROM base GROUP BY c_type, c_speed
        ),
        with_totals AS (
            SELECT c_type, c_speed, w_cnt, b_cnt,
                SUM(w_cnt) OVER () AS w_total,
                SUM(b_cnt) OVER () AS b_total
            FROM grouped
        )
        SELECT c_type, c_speed, w_cnt, b_cnt, w_total, b_total
        FROM with_totals
        WHERE w_cnt >= 20 AND w_total > 0
            AND w_cnt * 1.0 / w_total >= 0.15
            AND (b_total = 0
                 OR w_cnt * 1.0 / w_total
                    >= b_cnt * 1.0 / NULLIF(b_total, 0) * 2 + 0.05)
        ORDER BY w_cnt DESC LIMIT 15
    """


def new_country_traffic_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _zero_baseline_count_item(format_city_label("", row[0]), row[1], {"filters": {"country": row[0]}}, 10)


_reg(
    id="new_country_traffic",
    category=InsightCategory.traffic,
    title="New Country Traffic",
    description="Countries that appeared in the window but had zero requests in the baseline",
    sql_template=NEW_COUNTRY_TRAFFIC,
    required_fields=["country", "timestamp"],
    row_processor=new_country_traffic_processor,
)


def city_surges_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _item(
        format_city_label(row[1], row[3], row[2]),
        row[4],
        _scale_baseline(row[5], context),
        "requests",
        {"spike_ratio": round(float(row[6]), 1), "filters": _city_filters(row)},
        "info" if float(row[6]) < 10 else "warning",
    )


_reg(
    id="city_surges",
    category=InsightCategory.traffic,
    title="City Traffic Surges",
    description="Cities experiencing a significant spike in traffic compared to their baseline",
    sql_template=CITY_SURGES,
    required_fields=["city", "timestamp"],
    row_processor=city_surges_processor,
)


def new_city_traffic_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _zero_baseline_count_item(
        format_city_label(row[1], row[3], row[2]), row[4], {"filters": _city_filters(row)}, 50
    )


_reg(
    id="new_city_traffic",
    category=InsightCategory.traffic,
    title="New City Traffic",
    description="Cities that recently started sending traffic after a period of zero activity",
    sql_template=NEW_CITY_TRAFFIC,
    required_fields=["city", "timestamp"],
    row_processor=new_city_traffic_processor,
)


def ua_monoculture_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _monoculture_processor(row, "ua")


_reg(
    id="ua_monoculture",
    category=InsightCategory.traffic,
    title="User-Agent Monoculture",
    description="User-agents with an unusually high and spiking share of total traffic",
    sql_template=UA_MONOCULTURE,
    required_fields=["ua", "timestamp"],
    row_processor=ua_monoculture_processor,
)


def asn_concentration_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_cnt, b_cnt, b_total, w_total = row
    w_rate, b_rate = _traffic_share_rates(w_cnt, w_total, b_cnt, b_total)
    return _item(
        _fmt_asn_label(asn, context),
        w_rate,
        b_rate,
        "% of traffic",
        {"requests": w_cnt, "asn": asn, "filters": {"asn": asn}},
        "critical" if w_rate >= 50 else "warning",
    )


_reg(
    id="asn_concentration",
    category=InsightCategory.network,
    title="ASN Concentration",
    description="Traffic spiking from specific Autonomous Systems (ISPs/Data Centers)",
    sql_template=ASN_CONCENTRATION,
    required_fields=["asn", "timestamp"],
    row_processor=asn_concentration_processor,
)


def impossible_distance_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    sample_ip = _mask_ip_if_needed(row[6], context)
    meta = {
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
    }
    return _item(
        row[0],
        round(float(row[3]), 0),
        round(float(row[4]), 0),
        "km",
        meta,
        "critical" if float(row[2]) > 5000 else "warning",
        baseline_label="max allowed",
    )


_reg(
    id="impossible_distance",
    category=InsightCategory.security,
    title="Impossible Distance / Spoofing",
    description="Traffic where the network latency (RTT) is physically too low for the reported client distance",
    sql_template=IMPOSSIBLE_DISTANCE,
    required_fields=["pop", "lat", "lon", "tcp_rtt", "timestamp"],
    row_processor=impossible_distance_processor,
)


def referer_monoculture_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _monoculture_processor(row, "referer")


_reg(
    id="referer_monoculture",
    category=InsightCategory.traffic,
    title="Referer Monoculture",
    description="A single Referer driving an outsized, spiking share of total traffic — scraping, hotlinking, or a spoofed-referer flood",
    sql_template=REFERER_MONOCULTURE,
    required_fields=["referer", "timestamp"],
    row_processor=referer_monoculture_processor,
)


def method_drift_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    method, w_cnt, b_cnt, w_total, b_total = row
    w_rate, b_rate = _traffic_share_rates(w_cnt, w_total, b_cnt, b_total)
    return _item(
        method or "(unknown)",
        w_rate,
        b_rate,
        "% of traffic",
        {"requests": w_cnt, "method": method, "filters": {"method": method}},
        "critical" if w_rate >= 25 else "warning",
    )


_reg(
    id="method_drift",
    category=InsightCategory.traffic,
    title="HTTP Method Drift",
    description="A write method (POST/PUT/DELETE/…) surging to an outsized share of traffic vs a read-dominated baseline — API abuse or form/credential floods",
    sql_template=METHOD_DRIFT,
    required_fields=["method", "timestamp"],
    row_processor=method_drift_processor,
)


def new_asn_traffic_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_cnt, _b_cnt = row
    return _zero_baseline_count_item(
        label=_fmt_asn_label(asn, context),
        count=w_cnt,
        meta={"requests": w_cnt, "asn": asn, "filters": {"asn": asn}},
        warn_threshold=100,
    )


_reg(
    id="new_asn_traffic",
    category=InsightCategory.network,
    title="New ASN Traffic",
    description="An ASN (ISP/datacenter) with zero baseline presence now sending meaningful traffic — a new botnet source, proxy pool, or datacenter range",
    sql_template=NEW_ASN_TRAFFIC,
    required_fields=["asn", "timestamp"],
    row_processor=new_asn_traffic_processor,
)


def asn_hosting_shift_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_hosting, w_total, b_hosting, b_total = row
    w_ratio = round(float(w_hosting * 100.0 / w_total), 1) if w_total else 0.0
    b_ratio = round(float(b_hosting * 100.0 / b_total), 1) if b_total else 0.0
    return _item(
        _fmt_asn_label(asn, context),
        w_ratio,
        b_ratio,
        "% hosting",
        {"window_hosting": w_hosting, "window_total": w_total, "asn": asn, "filters": {"asn": asn}},
        "critical" if w_ratio >= 60 else "warning",
    )


_reg(
    id="asn_hosting_shift",
    category=InsightCategory.network,
    title="ASN Hosting Shift",
    description="An ASN's traffic composition shifting from consumer to datacenter/hosting — bot pools or scrapers spinning up on hosting infrastructure",
    sql_template=ASN_HOSTING_SHIFT,
    required_fields=["asn", "p_type", "timestamp"],
    row_processor=asn_hosting_shift_processor,
)


def connection_type_mix_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    c_type, c_speed, w_cnt, b_cnt, w_total, b_total = row
    w_rate, b_rate = _traffic_share_rates(w_cnt, w_total, b_cnt, b_total)
    return _item(
        f"{c_type or '?'} / {c_speed or '?'}",
        w_rate,
        b_rate,
        "% of typed traffic",
        {"requests": w_cnt, "c_type": c_type, "c_speed": c_speed, "filters": {"c_type": c_type, "c_speed": c_speed}},
        "warning",
    )


_reg(
    id="connection_type_mix",
    category=InsightCategory.network,
    title="Connection-Type Mix Shift",
    description="A client connection type/speed combo (e.g. cellular, datacenter) surging to an outsized share of traffic vs. baseline — a bot pool or routing shift",
    sql_template=CONNECTION_TYPE_MIX,
    required_fields=["c_type", "c_speed", "timestamp"],
    row_processor=connection_type_mix_processor,
)
