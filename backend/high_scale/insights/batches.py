"""Consolidated multi-detector ClickHouse queries for High-Scale Insights.

Detections that share the same table, time range, and rollup shape or grouping dimension are
executed in a single ClickHouse scan (via ``GROUP BY`` or ``GROUP BY GROUPING SETS``) and then
partitioned into the exact per-detector row tuples expected by ``shape_insight_card_from_rows``.
When only one member of a batch is enabled for a service, the evaluator falls back to that
detection's standalone query in ``sql_queries.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from backend.high_scale._common import visible_fact_clauses

from .rollup import ROLLUP_TABLE, _to_rollup
from .sql_queries import _CACHEABLE, _HIT, _P95, COLUMNS

QuerySource = Literal["fact", "mixed"]


def _int(value: Any) -> int:
    return int(value or 0)


def _float(value: Any) -> float:
    return float(value or 0.0)


def _opt_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _base_cte(
    name: str,
    projections: list[tuple[str, str]],
    *,
    rollup_shape: str | None = None,
    rollup_span: tuple[datetime, datetime] | None = None,
    window_only: bool = False,
    row_filter: str = "",
    filter_clause: str = "",
    weighted: bool = False,
) -> tuple[str, QuerySource]:
    """Build the ``/* insight:batch_<name> */ WITH base AS (...)`` prefix."""
    scope = " AND ".join(visible_fact_clauses("request"))
    if filter_clause:
        scope = f"{scope} AND ({filter_clause})"
    rf = f" AND ({row_filter})" if row_filter else ""
    raw_proj = ", ".join(f"{expr} AS {alias}" for alias, expr in projections)
    if rollup_shape and rollup_span and not filter_clause and not window_only:
        rollup_proj = ", ".join(f"{_to_rollup(expr)} AS {alias}" for alias, expr in projections)
        raw = (
            f"SELECT {raw_proj}, "
            "event_timestamp < {window_start:DateTime64(3)} AS is_b, "
            "event_timestamp >= {window_start:DateTime64(3)} AS is_w, toUInt64(1) AS n "
            f"FROM request_facts WHERE {scope} AND event_timestamp >= {{baseline_start:DateTime64(3)}} "
            f"AND event_timestamp < {{window_end:DateTime64(3)}}{rf} "
            "AND (event_timestamp < {rollup_start:DateTime64(3)} OR event_timestamp >= {rollup_end:DateTime64(3)})"
        )
        rollup = (
            f"SELECT {rollup_proj}, toUInt8(1) AS is_b, toUInt8(0) AS is_w, request_count AS n "
            f"FROM {ROLLUP_TABLE} WHERE {scope} AND shape = '{rollup_shape}' "
            "AND bucket_start >= {rollup_start:DateTime64(3)} AND bucket_start < {rollup_end:DateTime64(3)}"
            f"{_to_rollup(rf)}"
        )
        return f"/* insight:batch_{name} */ WITH base AS ({raw} UNION ALL {rollup})", "mixed"

    lower = "{window_start:DateTime64(3)}" if window_only else "{baseline_start:DateTime64(3)}"
    weight_col = ", toUInt64(1) AS n" if (weighted or rollup_shape) else ""
    raw = (
        f"SELECT {raw_proj}, "
        "event_timestamp < {window_start:DateTime64(3)} AS is_b, "
        f"event_timestamp >= {{window_start:DateTime64(3)}} AS is_w{weight_col} "
        f"FROM request_facts WHERE {scope} AND event_timestamp >= {lower} "
        f"AND event_timestamp < {{window_end:DateTime64(3)}}{rf}"
    )
    return f"/* insight:batch_{name} */ WITH base AS ({raw})", "fact"


def _share_spike_filter(
    rows: list[tuple[Any, int, int]],
    w_total: int,
    b_total: int,
    *,
    share_floor: float,
    min_w_cnt: int = 0,
    multiplier: float = 3.0,
    delta: float = 0.10,
    limit: int = 10,
    w_first: bool = False,
) -> list[tuple[Any, ...]]:
    if w_total <= 0:
        return []
    out: list[tuple[Any, ...]] = []
    for key, w_cnt, b_cnt in sorted(rows, key=lambda r: r[1], reverse=True):
        if w_cnt < min_w_cnt:
            continue
        w_share = w_cnt / w_total
        if w_share < share_floor:
            continue
        if b_total == 0 or w_share >= (b_cnt / b_total) * multiplier + delta:
            totals = (w_total, b_total) if w_first else (b_total, w_total)
            key_tuple = key if isinstance(key, tuple) else (key,)
            out.append((*key_tuple, w_cnt, b_cnt, *totals))
            if len(out) >= limit:
                break
    return out


# ── Batch 1: misc rollup shape (6 detections) ────────────────────────────────


def _build_misc(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    proj: list[tuple[str, str]] = []
    if "cache_hit_cliff" in active:
        proj.append(("cache", COLUMNS["cache"]))
    if "http3_fallback" in active:
        proj.append(("transport", COLUMNS["transport"]))
    if "origin_error_rate" in active:
        proj.append(("ost_str", "custom_fields['ost']"))
    if "proxy_surge" in active:
        proj.append(("p_type", COLUMNS["p_type"]))
    if "method_drift" in active:
        proj.append(("method", COLUMNS["method"]))
    if "connection_type_mix" in active:
        proj.append(("c_type", COLUMNS["c_type"]))
        proj.append(("c_speed", COLUMNS["c_speed"]))

    cte, source = _base_cte(
        "misc", proj, rollup_shape="misc", rollup_span=rollup_span, filter_clause=filter_clause, weighted=True
    )
    sets: list[str] = []
    if "cache_hit_cliff" in active or "http3_fallback" in active:
        sets.append("()")
    if "origin_error_rate" in active:
        sets.append("(ost_str)")
    if "proxy_surge" in active:
        sets.append("(p_type)")
    if "method_drift" in active:
        sets.append("(method)")
    if "connection_type_mix" in active:
        sets.append("(c_type, c_speed)")

    use_sets = len(sets) > 1 or sets != ["()"]
    g_ost = "grouping(ost_str)" if (use_sets and "origin_error_rate" in active) else "toUInt8(1)"
    g_p_type = "grouping(p_type)" if (use_sets and "proxy_surge" in active) else "toUInt8(1)"
    g_method = "grouping(method)" if (use_sets and "method_drift" in active) else "toUInt8(1)"
    g_c_type = "grouping(c_type)" if (use_sets and "connection_type_mix" in active) else "toUInt8(1)"

    sel = [
        f"{g_ost} AS g_ost",
        f"{g_p_type} AS g_p_type",
        f"{g_method} AS g_method",
        f"{g_c_type} AS g_c_type",
        "ost_str" if "origin_error_rate" in active else "'' AS ost_str",
        "p_type" if "proxy_surge" in active else "'' AS p_type",
        "method" if "method_drift" in active else "'' AS method",
        "c_type" if "connection_type_mix" in active else "'' AS c_type",
        "c_speed" if "connection_type_mix" in active else "'' AS c_speed",
        "sumIf(n, is_w) AS w_cnt",
        "sumIf(n, is_b) AS b_cnt",
    ]
    if "cache_hit_cliff" in active:
        sel.extend(
            [
                f"sumIf(n, is_w AND {_HIT}) AS w_hits",
                f"sumIf(n, is_w AND {_CACHEABLE}) AS w_cacheable",
                f"sumIf(n, is_b AND {_HIT}) AS b_hits",
                f"sumIf(n, is_b AND {_CACHEABLE}) AS b_cacheable",
            ]
        )
    else:
        sel.extend(
            [
                "toUInt64(0) AS w_hits",
                "toUInt64(0) AS w_cacheable",
                "toUInt64(0) AS b_hits",
                "toUInt64(0) AS b_cacheable",
            ]
        )
    if "http3_fallback" in active:
        sel.extend(
            [
                "sumIf(n, is_w AND transport != '' AND lower(transport) = 'quic') AS w_quic",
                "sumIf(n, is_w AND transport != '') AS w_transport_total",
                "sumIf(n, is_b AND transport != '' AND lower(transport) = 'quic') AS b_quic",
                "sumIf(n, is_b AND transport != '') AS b_transport_total",
            ]
        )
    else:
        sel.extend(
            [
                "toUInt64(0) AS w_quic",
                "toUInt64(0) AS w_transport_total",
                "toUInt64(0) AS b_quic",
                "toUInt64(0) AS b_transport_total",
            ]
        )

    group_clause = f" GROUP BY GROUPING SETS ({', '.join(sets)})" if use_sets else ""
    return f"{cte} SELECT {', '.join(sel)} FROM base{group_clause}", source


def _extract_misc(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    out: dict[str, list[tuple[Any, ...]]] = {d: [] for d in active}
    ost_rows: list[tuple[int, int, int]] = []
    p_type_rows: list[tuple[str, int, int]] = []
    method_rows: list[tuple[str, int, int]] = []
    conn_rows: list[tuple[tuple[str, str], int, int]] = []

    for r in rows:
        g_ost, g_p, g_m, g_c = (
            _int(r.get("g_ost")),
            _int(r.get("g_p_type")),
            _int(r.get("g_method")),
            _int(r.get("g_c_type")),
        )
        if g_ost == 1 and g_p == 1 and g_m == 1 and g_c == 1:
            if "cache_hit_cliff" in active:
                w_h, w_c, b_h, b_c = (
                    _int(r.get("w_hits")),
                    _int(r.get("w_cacheable")),
                    _int(r.get("b_hits")),
                    _int(r.get("b_cacheable")),
                )
                if w_c >= 100 and b_c >= 200 and (b_h / b_c) >= 0.40 and (w_h / w_c) <= (b_h / b_c) - 0.15:
                    out["cache_hit_cliff"].append((w_h, w_c, b_h, b_c))
            if "http3_fallback" in active:
                w_q, w_t, b_q, b_t = (
                    _int(r.get("w_quic")),
                    _int(r.get("w_transport_total")),
                    _int(r.get("b_quic")),
                    _int(r.get("b_transport_total")),
                )
                if w_t >= 100 and b_t >= 100 and (b_q / b_t) >= 0.30 and (w_q / w_t) <= (b_q / b_t) - 0.20:
                    out["http3_fallback"].append((w_q, w_t, b_q, b_t))
        elif g_ost == 0 and "origin_error_rate" in active:
            code = _parse_int(r.get("ost_str"))
            if code is not None:
                ost_rows.append((code, _int(r.get("w_cnt")), _int(r.get("b_cnt"))))
        elif g_p == 0 and "proxy_surge" in active:
            p_type_rows.append((str(r.get("p_type") or ""), _int(r.get("w_cnt")), _int(r.get("b_cnt"))))
        elif g_m == 0 and "method_drift" in active:
            method_rows.append((str(r.get("method") or ""), _int(r.get("w_cnt")), _int(r.get("b_cnt"))))
        elif g_c == 0 and "connection_type_mix" in active:
            c_type = str(r.get("c_type") or "")
            if c_type:
                conn_rows.append(((c_type, str(r.get("c_speed") or "")), _int(r.get("w_cnt")), _int(r.get("b_cnt"))))

    if "origin_error_rate" in active and ost_rows:
        w_tot = sum(w for _, w, _ in ost_rows)
        b_tot = sum(b for _, _, b in ost_rows)
        w_5xx = sum(w for code, w, _ in ost_rows if code >= 500)
        b_5xx = sum(b for code, _, b in ost_rows if code >= 500)
        if (
            w_tot > 0
            and b_tot > 0
            and (w_5xx * 100.0 / w_tot) >= 1.0
            and (w_5xx * 100.0 / w_tot) > (b_5xx * 100.0 / b_tot) * 2
        ):
            out["origin_error_rate"] = [
                (code, w, w_tot, b_tot, w_5xx, b_5xx) for code, w, _ in ost_rows if code >= 500 and w > 0
            ]

    if "proxy_surge" in active and p_type_rows:
        w_all = sum(w for _, w, _ in p_type_rows)
        b_all = sum(b for _, _, b in p_type_rows)
        w_proxy = sum(w for pt, w, _ in p_type_rows if pt)
        b_proxy = sum(b for pt, _, b in p_type_rows if pt)
        if w_all > 0 and b_all > 0:
            w_pct = w_proxy * 100.0 / w_all
            b_pct = b_proxy * 100.0 / b_all
            if w_pct >= 5.0 and w_pct >= b_pct * 2.0 + 5.0:
                out["proxy_surge"] = [(pt, w, b, w_all, b_all) for pt, w, b in p_type_rows if pt]

    if "method_drift" in active and method_rows:
        w_all = sum(w for _, w, _ in method_rows)
        b_all = sum(b for _, _, b in method_rows)
        eligible = [(m, w, b) for m, w, b in method_rows if m and m.upper() not in ("GET", "HEAD", "OPTIONS")]
        out["method_drift"] = _share_spike_filter(
            eligible, w_all, b_all, share_floor=0.10, min_w_cnt=20, multiplier=2.0, delta=0.05, limit=10, w_first=True
        )

    if "connection_type_mix" in active and conn_rows:
        w_all = sum(w for _, w, _ in conn_rows)
        b_all = sum(b for _, _, b in conn_rows)
        out["connection_type_mix"] = _share_spike_filter(
            conn_rows, w_all, b_all, share_floor=0.15, min_w_cnt=20, multiplier=2.0, delta=0.05, limit=15, w_first=True
        )

    return out


# ── Batch 2: geo rollup shape (4 detections) ─────────────────────────────────


def _build_geo(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    has_country = "new_country_traffic" in active
    city_ids = active & {"city_surges", "city_error_spikes", "new_city_traffic"}
    proj: list[tuple[str, str]] = [("country", COLUMNS["country"])]
    if city_ids:
        proj.extend([("city", COLUMNS["city"]), ("region", COLUMNS["region"])])
    if "city_error_spikes" in active:
        proj.append(("status", COLUMNS["status"]))
    cte, source = _base_cte(
        "geo", proj, rollup_shape="geo", rollup_span=rollup_span, filter_clause=filter_clause, weighted=True
    )
    use_sets = has_country and bool(city_ids)
    g_city = "grouping(city)" if use_sets else ("toUInt8(0)" if city_ids else "toUInt8(1)")
    city_sel = "city, region" if city_ids else "'' AS city, '' AS region"
    err_sel = (
        "sumIf(n, is_w AND status >= 400) AS w_err, sumIf(n, is_b AND status >= 400) AS b_err"
        if "city_error_spikes" in active
        else "toUInt64(0) AS w_err, toUInt64(0) AS b_err"
    )
    group_by = (
        "GROUP BY GROUPING SETS ((country), (city, region, country))"
        if use_sets
        else ("GROUP BY city, region, country" if city_ids else "GROUP BY country")
    )
    having_parts: list[str] = []
    if has_country:
        having_parts.append("(g_city = 1 AND country != '' AND w_cnt >= 3 AND b_cnt = 0)")
    city_conds: list[str] = []
    if "new_city_traffic" in active:
        city_conds.append("(w_cnt >= 5 AND b_cnt = 0)")
    if "city_surges" in active:
        city_conds.append(
            "(w_cnt >= 20 AND w_cnt > b_cnt / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 3)"
        )
    if "city_error_spikes" in active:
        city_conds.append(
            "(w_cnt >= 10 AND w_err * 1.0 / nullIf(w_cnt, 0) >= 0.10 "
            "AND (b_cnt < 50 OR w_err * 1.0 / nullIf(w_cnt, 0) >= b_err * 1.0 / nullIf(b_cnt, 0) * 3 + 0.05))"
        )
    if city_conds:
        having_parts.append(f"(g_city = 0 AND city != '' AND ({' OR '.join(city_conds)}))")
    sql = (
        f"{cte} SELECT {g_city} AS g_city, country, {city_sel}, "
        f"sumIf(n, is_w) AS w_cnt, sumIf(n, is_b) AS b_cnt, {err_sel} "
        f"FROM base {group_by} HAVING {' OR '.join(having_parts)}"
    )
    return sql, source


def _extract_geo(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    out: dict[str, list[tuple[Any, ...]]] = {d: [] for d in active}
    countries: list[tuple[str, int, int]] = []
    new_cities: list[tuple[str, str, str, str, int, int]] = []
    surges: list[tuple[str, str, str, str, int, int, float]] = []
    err_spikes: list[tuple[str, str, str, str, float, float | None, int, int, int]] = []
    b_scale = max(baseline_hours, 1.0)

    for r in rows:
        g_city = _int(r.get("g_city"))
        country = str(r.get("country") or "")
        w_cnt, b_cnt = _int(r.get("w_cnt")), _int(r.get("b_cnt"))
        if g_city == 1:
            if "new_country_traffic" in active and country and w_cnt >= 3 and b_cnt == 0:
                countries.append((country, w_cnt, b_cnt))
            continue
        city, region = str(r.get("city") or ""), str(r.get("region") or "")
        if not city:
            continue
        label = f"{city}, {region}, {country}"
        if "new_city_traffic" in active and w_cnt >= 5 and b_cnt == 0:
            new_cities.append((label, city, region, country, w_cnt, b_cnt))
        if "city_surges" in active and w_cnt >= 20:
            expected = b_cnt / b_scale * window_hours
            if w_cnt > expected * 3:
                surges.append((label, city, region, country, w_cnt, b_cnt, w_cnt / max(expected, 1.0)))
        if "city_error_spikes" in active and w_cnt >= 10:
            w_err, b_err = _int(r.get("w_err")), _int(r.get("b_err"))
            w_rate = w_err / w_cnt
            b_rate = (b_err / b_cnt) if b_cnt > 0 else None
            if w_rate >= 0.10 and (b_cnt < 50 or (b_rate is not None and w_rate >= b_rate * 3 + 0.05)):
                err_spikes.append((label, city, region, country, w_rate, b_rate, w_err, w_cnt, b_cnt))

    if "new_country_traffic" in active:
        out["new_country_traffic"] = sorted(countries, key=lambda x: x[1], reverse=True)[:20]
    if "new_city_traffic" in active:
        out["new_city_traffic"] = sorted(new_cities, key=lambda x: x[4], reverse=True)[:20]
    if "city_surges" in active:
        out["city_surges"] = sorted(surges, key=lambda x: x[6], reverse=True)[:15]
    if "city_error_spikes" in active:
        out["city_error_spikes"] = sorted(err_spikes, key=lambda x: x[4] - (x[5] or 0.0), reverse=True)[:15]
    return out


# ── Batch 3: asn rollup shape (3 detections) ─────────────────────────────────


def _build_asn(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    proj: list[tuple[str, str]] = [("asn", COLUMNS["asn"])]
    if "asn_hosting_shift" in active:
        proj.append(("p_type", COLUMNS["p_type"]))
    cte, source = _base_cte(
        "asn", proj, rollup_shape="asn", rollup_span=rollup_span, filter_clause=filter_clause, weighted=True
    )
    host_cols = (
        "sumIf(n, is_w AND p_type = 'hosting') AS w_hosting, "
        "sumIf(n, is_w AND p_type != '') AS w_p_total, "
        "sumIf(n, is_b AND p_type = 'hosting') AS b_hosting, "
        "sumIf(n, is_b AND p_type != '') AS b_p_total"
        if "asn_hosting_shift" in active
        else "toUInt64(0) AS w_hosting, toUInt64(0) AS w_p_total, toUInt64(0) AS b_hosting, toUInt64(0) AS b_p_total"
    )
    conds: list[str] = []
    if "asn_concentration" in active:
        conds.append(
            "(w_total_all > 0 AND w_cnt * 1.0 / w_total_all >= 0.20 "
            "AND (b_total_all = 0 OR w_cnt * 1.0 / w_total_all >= b_cnt * 1.0 / nullIf(b_total_all, 0) * 3 + 0.10))"
        )
    if "new_asn_traffic" in active:
        conds.append("(w_cnt >= 20 AND b_cnt = 0)")
    if "asn_hosting_shift" in active:
        conds.append(
            "(w_p_total >= 50 AND (w_hosting * 1.0 / w_p_total) >= 0.3 "
            "AND (b_p_total = 0 OR (w_hosting * 1.0 / w_p_total) >= (b_hosting * 1.0 / nullIf(b_p_total, 0)) * 3 + 0.10))"
        )
    sql = (
        f"{cte}, agg AS (SELECT asn, sumIf(n, is_w) AS w_cnt, sumIf(n, is_b) AS b_cnt, "
        f"sum(sumIf(n, is_w)) OVER () AS w_total_all, sum(sumIf(n, is_b)) OVER () AS b_total_all, {host_cols} "
        "FROM base GROUP BY asn) "
        "SELECT asn, w_cnt, b_cnt, w_total_all, b_total_all, w_hosting, w_p_total, b_hosting, b_p_total "
        f"FROM agg WHERE asn IS NOT NULL AND ({' OR '.join(conds)})"
    )
    return sql, source


def _extract_asn(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    out: dict[str, list[tuple[Any, ...]]] = {d: [] for d in active}
    conc: list[tuple[int, int, int, int, int]] = []
    new_asns: list[tuple[int, int, int]] = []
    hosting: list[tuple[int, int, int, int, int]] = []

    for r in rows:
        asn_val = r.get("asn")
        if asn_val is None:
            continue
        asn = int(asn_val)
        w_cnt, b_cnt = _int(r.get("w_cnt")), _int(r.get("b_cnt"))
        w_tot, b_tot = _int(r.get("w_total_all")), _int(r.get("b_total_all"))
        if "asn_concentration" in active and w_tot > 0:
            w_share = w_cnt / w_tot
            if w_share >= 0.20 and (b_tot == 0 or w_share >= (b_cnt / b_tot) * 3 + 0.10):
                conc.append((asn, w_cnt, b_cnt, b_tot, w_tot))
        if "new_asn_traffic" in active and w_cnt >= 20 and b_cnt == 0:
            new_asns.append((asn, w_cnt, b_cnt))
        if "asn_hosting_shift" in active:
            w_h, w_p = _int(r.get("w_hosting")), _int(r.get("w_p_total"))
            b_h, b_p = _int(r.get("b_hosting")), _int(r.get("b_p_total"))
            if w_p >= 50 and (w_h / w_p) >= 0.3 and (b_p == 0 or (w_h / w_p) >= (b_h / b_p) * 3 + 0.10):
                hosting.append((asn, w_h, w_p, b_h, b_p))

    if "asn_concentration" in active:
        out["asn_concentration"] = sorted(conc, key=lambda x: x[1], reverse=True)[:10]
    if "new_asn_traffic" in active:
        out["new_asn_traffic"] = sorted(new_asns, key=lambda x: x[1], reverse=True)[:20]
    if "asn_hosting_shift" in active:
        out["asn_hosting_shift"] = sorted(hosting, key=lambda x: x[1], reverse=True)[:15]
    return out


# ── Batch 4: url_cache rollup shape (2 detections) ───────────────────────────


def _build_url_cache(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    cte, source = _base_cte(
        "url_cache",
        [("url", COLUMNS["url"]), ("cache", COLUMNS["cache"])],
        rollup_shape="url_cache",
        rollup_span=rollup_span,
        filter_clause=filter_clause,
        weighted=True,
    )
    sql = (
        f"{cte} SELECT url, "
        f"sumIf(n, is_w AND {_HIT}) * 1.0 / nullIf(sumIf(n, is_w AND {_CACHEABLE}), 0) AS c_w_rate, "
        f"sumIf(n, is_b AND {_HIT}) * 1.0 / nullIf(sumIf(n, is_b AND {_CACHEABLE}), 0) AS c_b_rate, "
        f"sumIf(n, is_w AND {_CACHEABLE}) AS c_w_total, sumIf(n, is_b AND {_CACHEABLE}) AS c_b_total, "
        "sumIf(n, is_w AND startsWith(cache, 'PASS')) * 1.0 / nullIf(sumIf(n, is_w), 0) AS p_w_rate, "
        "sumIf(n, is_b AND startsWith(cache, 'PASS')) * 1.0 / nullIf(sumIf(n, is_b), 0) AS p_b_rate, "
        "sumIf(n, is_w) AS p_w_total, sumIf(n, is_b) AS p_b_total "
        "FROM base WHERE url IN (SELECT url FROM base WHERE is_w GROUP BY url HAVING sum(n) >= 5) "
        "GROUP BY url HAVING "
        "(c_w_total >= 5 AND c_b_total >= 20 AND c_b_rate >= 0.40 AND c_w_rate <= c_b_rate - 0.20 AND c_w_rate <= c_b_rate * 0.6) "
        "OR (p_w_total >= 10 AND p_b_total >= 50 AND p_b_rate <= 0.20 AND p_w_rate >= 0.50 AND p_w_rate >= p_b_rate + 0.30)"
    )
    return sql, source


def _extract_url_cache(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    collapse: list[tuple[str, float, float, int, int]] = []
    pass_reg: list[tuple[str, float, float, int, int]] = []
    for r in rows:
        url = str(r.get("url") or "")
        cw, cb = _int(r.get("c_w_total")), _int(r.get("c_b_total"))
        cwr, cbr = _opt_float(r.get("c_w_rate")), _opt_float(r.get("c_b_rate"))
        if (
            cw >= 5
            and cb >= 20
            and cbr is not None
            and cwr is not None
            and cbr >= 0.40
            and cwr <= cbr - 0.20
            and cwr <= cbr * 0.6
        ):
            collapse.append((url, cwr, cbr, cw, cb))
        pw, pb = _int(r.get("p_w_total")), _int(r.get("p_b_total"))
        pwr, pbr = _opt_float(r.get("p_w_rate")), _opt_float(r.get("p_b_rate"))
        if (
            pw >= 10
            and pb >= 50
            and pbr is not None
            and pwr is not None
            and pbr <= 0.20
            and pwr >= 0.50
            and pwr >= pbr + 0.30
        ):
            pass_reg.append((url, pwr, pbr, pw, pb))
    return {
        "cache_collapse": sorted(collapse, key=lambda x: x[2] - x[1], reverse=True)[:15],
        "cacheability_regression": sorted(pass_reg, key=lambda x: x[1] - x[2], reverse=True)[:15],
    }


# ── Batch 5: window-only GROUP BY url (4 detections) ─────────────────────────

_IMG_COND = (
    "status = 200 AND (url ILIKE '%.jpg%' OR url ILIKE '%.jpeg%' OR url ILIKE '%.png%' OR url ILIKE '%.gif%') "
    "AND url NOT ILIKE '%auto=webp%' AND url NOT ILIKE '%format=auto%' "
    "AND url NOT ILIKE '%format=webp%' AND url NOT ILIKE '%format=avif%'"
)
_TTL_COND = "ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL"


def _build_window_url(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    proj: list[tuple[str, str]] = [("url", COLUMNS["url"])]
    if "tail_latency" in active:
        proj.append(("elapsed", COLUMNS["elapsed"]))
    if "cache_ttl_mismatch" in active:
        proj.extend([("ttl", COLUMNS["ttl"]), ("hits", COLUMNS["hits"]), ("age", COLUMNS["age"])])
    if "image_optimization_opportunities" in active:
        proj.extend([("status", COLUMNS["status"]), ("resp_bytes", COLUMNS["resp_bytes"]), ("ua", COLUMNS["ua"])])
    if "origin_retries" in active:
        proj.append(("oretries", COLUMNS["oretries"]))

    cte, source = _base_cte("window_url", proj, window_only=True, filter_clause=filter_clause)
    sel = ["url"]
    having: list[str] = []
    if "tail_latency" in active:
        sel.extend(
            [
                "round(quantileTDigestIf(0.99)(elapsed, elapsed IS NOT NULL) / 1000.0, 0) AS tl_p99",
                "round(quantileTDigestIf(0.50)(elapsed, elapsed IS NOT NULL) / 1000.0, 0) AS tl_p50",
                "round(quantileTDigestIf(0.99)(elapsed, elapsed IS NOT NULL) / nullIf(quantileTDigestIf(0.50)(elapsed, elapsed IS NOT NULL), 0), 1) AS tl_ratio",
                "countIf(elapsed IS NOT NULL) AS tl_total",
            ]
        )
        having.append("(tl_total >= 20 AND tl_ratio > 5)")
    if "cache_ttl_mismatch" in active:
        sel.extend(
            [
                f"round(avgIf(ttl, {_TTL_COND}), 0) AS ttl_avg",
                f"round(avgIf(hits, {_TTL_COND}), 1) AS ttl_hits",
                f"round(avgIf(age, {_TTL_COND}), 0) AS ttl_age",
                f"countIf({_TTL_COND}) AS ttl_cnt",
            ]
        )
        having.append("(ttl_cnt >= 10 AND ttl_hits < 2 AND ttl_avg > 60)")
    if "image_optimization_opportunities" in active:
        sel.extend(
            [
                f"countIf({_IMG_COND}) AS img_cnt",
                f"sumIf(resp_bytes, {_IMG_COND}) AS img_bytes",
                f"round(avgIf(resp_bytes, {_IMG_COND}) / 1024, 1) AS img_avg_kb",
                f"(countIf({_IMG_COND} AND (ua ILIKE '%Mobi%' OR ua ILIKE '%Android%' OR ua ILIKE '%iPhone%')) * 1.0 / nullIf(countIf({_IMG_COND}), 0)) AS img_mob",
            ]
        )
        having.append("(img_bytes > 1024 * 512)")
    if "origin_retries" in active:
        sel.extend(
            [
                "countIf(oretries > 0) AS ret_cnt",
                "round(avgIf(oretries, oretries > 0), 2) AS ret_avg",
                "maxIf(oretries, oretries > 0) AS ret_max",
            ]
        )
        having.append("(ret_cnt >= 5)")
    return f"{cte} SELECT {', '.join(sel)} FROM base GROUP BY url HAVING {' OR '.join(having)}", source


def _extract_window_url(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    out: dict[str, list[tuple[Any, ...]]] = {d: [] for d in active}
    tail: list[tuple[str, float, float, float, int]] = []
    ttl: list[tuple[str, float, float, float, int]] = []
    img: list[tuple[str, int, int, float, float]] = []
    ret: list[tuple[str, int, float, int]] = []

    for r in rows:
        url = str(r.get("url") or "")
        if "tail_latency" in active:
            tl_tot = _int(r.get("tl_total"))
            tl_ratio = _opt_float(r.get("tl_ratio"))
            if tl_tot >= 20 and tl_ratio is not None and tl_ratio > 5:
                tail.append((url, _float(r.get("tl_p99")), _float(r.get("tl_p50")), tl_ratio, tl_tot))
        if "cache_ttl_mismatch" in active:
            ttl_cnt = _int(r.get("ttl_cnt"))
            ttl_avg = _opt_float(r.get("ttl_avg"))
            ttl_hits = _opt_float(r.get("ttl_hits"))
            if ttl_cnt >= 10 and ttl_hits is not None and ttl_hits < 2 and ttl_avg is not None and ttl_avg > 60:
                ttl.append((url, ttl_avg, ttl_hits, _float(r.get("ttl_age")), ttl_cnt))
        if "image_optimization_opportunities" in active:
            img_bytes = _int(r.get("img_bytes"))
            if img_bytes > 1024 * 512:
                img.append(
                    (url, _int(r.get("img_cnt")), img_bytes, _float(r.get("img_avg_kb")), _float(r.get("img_mob")))
                )
        if "origin_retries" in active:
            ret_cnt = _int(r.get("ret_cnt"))
            if ret_cnt >= 5:
                ret.append((url, ret_cnt, _float(r.get("ret_avg")), _int(r.get("ret_max"))))

    if "tail_latency" in active:
        out["tail_latency"] = sorted(tail, key=lambda x: x[3], reverse=True)[:15]
    if "cache_ttl_mismatch" in active:
        out["cache_ttl_mismatch"] = sorted(ttl, key=lambda x: x[1], reverse=True)[:20]
    if "image_optimization_opportunities" in active:
        out["image_optimization_opportunities"] = sorted(img, key=lambda x: x[2], reverse=True)[:15]
    if "origin_retries" in active:
        out["origin_retries"] = sorted(ret, key=lambda x: x[2], reverse=True)[:10]
    return out


# ── Batch 6: baseline+window latency regressions by dimension (3 detections) ─


def _build_latency_dim(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    proj: list[tuple[str, str]] = [("elapsed", COLUMNS["elapsed"])]
    sets: list[str] = []
    if "pop_latency_regression" in active:
        proj.append(("pop", COLUMNS["pop"]))
        sets.append("(pop)")
    if "region_latency" in active:
        proj.extend([("server_region", COLUMNS["server_region"]), ("ottfb", COLUMNS["ottfb"])])
        sets.append("(server_region)")
    if "city_latency_regressions" in active:
        proj.extend([("city", COLUMNS["city"]), ("region", COLUMNS["region"]), ("country", COLUMNS["country"])])
        sets.append("(city, region, country)")

    cte, source = _base_cte("latency_dim", proj, row_filter="elapsed IS NOT NULL", filter_clause=filter_clause)
    g_pop = "grouping(pop)" if "pop_latency_regression" in active else "toUInt8(1)"
    g_reg = "grouping(server_region)" if "region_latency" in active else "toUInt8(1)"
    g_city = "grouping(city)" if "city_latency_regressions" in active else "toUInt8(1)"
    ottfb_sel = (
        "if(countIf(is_w AND ottfb IS NOT NULL) >= 20, round(quantileTDigestIf(0.95)(ottfb, is_w AND ottfb IS NOT NULL) / 1000.0, 0), NULL) AS ottfb_p95"
        if "region_latency" in active
        else "CAST(NULL AS Nullable(Float64)) AS ottfb_p95"
    )
    having: list[str] = []
    if "pop_latency_regression" in active:
        having.append(
            "(g_pop = 0 AND pop != '' AND w_total >= 20 AND b_total >= 50 AND w_p95 >= b_p95 * 1.5 AND w_p95 - b_p95 >= 100)"
        )
    if "region_latency" in active:
        having.append(
            "(g_reg = 0 AND server_region != '' AND w_total >= 20 AND b_total >= 50 AND w_p95 >= b_p95 * 1.5 AND w_p95 - b_p95 >= 100)"
        )
    if "city_latency_regressions" in active:
        having.append(
            "(g_city = 0 AND city != '' AND w_total >= 10 AND b_total >= 50 AND w_p95 >= b_p95 * 3.0 AND w_p95 - b_p95 >= 500)"
        )

    pop_col = "pop" if "pop_latency_regression" in active else "'' AS pop"
    reg_col = "server_region" if "region_latency" in active else "'' AS server_region"
    city_cols = (
        "city, region, country" if "city_latency_regressions" in active else "'' AS city, '' AS region, '' AS country"
    )
    sql = (
        f"{cte} SELECT {g_pop} AS g_pop, {g_reg} AS g_reg, {g_city} AS g_city, "
        f"{pop_col}, {reg_col}, {city_cols}, "
        f"{_P95}(elapsed, is_w) / 1000.0 AS w_p95, {_P95}(elapsed, is_b) / 1000.0 AS b_p95, "
        f"countIf(is_w) AS w_total, countIf(is_b) AS b_total, {ottfb_sel} "
        f"FROM base GROUP BY GROUPING SETS ({', '.join(sets)}) HAVING {' OR '.join(having)}"
    )
    return sql, source


def _extract_latency_dim(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    out: dict[str, list[tuple[Any, ...]]] = {d: [] for d in active}
    pops: list[tuple[str, float, float, int, int]] = []
    regions: list[tuple[str, float, float, int, int, float | None]] = []
    cities: list[tuple[str, str, str, str, float, float, int, int]] = []

    for r in rows:
        w_p95, b_p95 = _float(r.get("w_p95")), _float(r.get("b_p95"))
        w_tot, b_tot = _int(r.get("w_total")), _int(r.get("b_total"))
        if _int(r.get("g_pop")) == 0 and "pop_latency_regression" in active:
            pop = str(r.get("pop") or "")
            if pop and w_tot >= 20 and b_tot >= 50 and w_p95 >= b_p95 * 1.5 and w_p95 - b_p95 >= 100:
                pops.append((pop, w_p95, b_p95, w_tot, b_tot))
        elif _int(r.get("g_reg")) == 0 and "region_latency" in active:
            reg = str(r.get("server_region") or "")
            if reg and w_tot >= 20 and b_tot >= 50 and w_p95 >= b_p95 * 1.5 and w_p95 - b_p95 >= 100:
                regions.append((reg, w_p95, b_p95, w_tot, b_tot, _opt_float(r.get("ottfb_p95"))))
        elif _int(r.get("g_city")) == 0 and "city_latency_regressions" in active:
            city, region, country = str(r.get("city") or ""), str(r.get("region") or ""), str(r.get("country") or "")
            if city and w_tot >= 10 and b_tot >= 50 and w_p95 >= b_p95 * 3.0 and w_p95 - b_p95 >= 500:
                cities.append((f"{city}, {region}, {country}", city, region, country, w_p95, b_p95, w_tot, b_tot))

    if "pop_latency_regression" in active:
        out["pop_latency_regression"] = sorted(pops, key=lambda x: x[1] / x[2] if x[2] else 0.0, reverse=True)[:15]
    if "region_latency" in active:
        out["region_latency"] = sorted(regions, key=lambda x: x[1] / x[2] if x[2] else 0.0, reverse=True)[:15]
    if "city_latency_regressions" in active:
        out["city_latency_regressions"] = sorted(cities, key=lambda x: x[4] / x[5] if x[5] else 0.0, reverse=True)[:15]
    return out


# ── Batch 7: baseline+window GROUP BY url performance (2 detections) ─────────

_COMP_ROW = "status = 200 AND resp_bytes >= 1024 AND url != '' AND match(url, {compress_regex:String})"
_UNCOMP = "NOT match(lower(resp_header_content_encoding), 'gzip|br|zstd|deflate')"


def _build_url_perf(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    proj = [
        ("url", COLUMNS["url"]),
        ("elapsed", COLUMNS["elapsed"]),
        ("status", COLUMNS["status"]),
        ("resp_bytes", COLUMNS["resp_bytes"]),
        ("resp_header_content_encoding", COLUMNS["resp_header_content_encoding"]),
    ]
    cte, source = _base_cte("url_perf", proj, filter_clause=filter_clause)
    sql = (
        f"{cte} SELECT url, "
        f"{_P95}(elapsed, is_w AND elapsed IS NOT NULL) / 1000.0 AS l_w_p95, "
        f"{_P95}(elapsed, is_b AND elapsed IS NOT NULL) / 1000.0 AS l_b_p95, "
        "countIf(is_w AND elapsed IS NOT NULL) AS l_w_total, "
        "countIf(is_b AND elapsed IS NOT NULL) AS l_b_total, "
        f"countIf(is_w AND {_COMP_ROW} AND {_UNCOMP}) * 1.0 / nullIf(countIf(is_w AND {_COMP_ROW}), 0) AS c_w_rate, "
        f"countIf(is_b AND {_COMP_ROW} AND {_UNCOMP}) * 1.0 / nullIf(countIf(is_b AND {_COMP_ROW}), 0) AS c_b_rate, "
        f"countIf(is_w AND {_COMP_ROW}) AS c_w_total, "
        f"countIf(is_b AND {_COMP_ROW}) AS c_b_total "
        "FROM base WHERE url IN (SELECT url FROM base WHERE is_w GROUP BY url HAVING count() >= 5) "
        "GROUP BY url HAVING "
        "(l_w_total >= 5 AND l_b_total >= 20 AND l_w_p95 >= l_b_p95 * 2.0 AND l_w_p95 - l_b_p95 >= 200) "
        "OR (c_w_total >= 10 AND c_b_total >= 50 AND c_b_rate <= 0.20 AND c_w_rate >= 0.50 AND c_w_rate >= c_b_rate + 0.30)"
    )
    return sql, source


def _extract_url_perf(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    lat: list[tuple[str, float, float, int, int]] = []
    comp: list[tuple[str, float, float, int, int]] = []
    for r in rows:
        url = str(r.get("url") or "")
        lw, lb = _int(r.get("l_w_total")), _int(r.get("l_b_total"))
        lwp, lbp = _opt_float(r.get("l_w_p95")), _opt_float(r.get("l_b_p95"))
        if lw >= 5 and lb >= 20 and lwp is not None and lbp is not None and lwp >= lbp * 2.0 and lwp - lbp >= 200:
            lat.append((url, lwp, lbp, lw, lb))
        cw, cb = _int(r.get("c_w_total")), _int(r.get("c_b_total"))
        cwr, cbr = _opt_float(r.get("c_w_rate")), _opt_float(r.get("c_b_rate"))
        if (
            cw >= 10
            and cb >= 50
            and cbr is not None
            and cwr is not None
            and cbr <= 0.20
            and cwr >= 0.50
            and cwr >= cbr + 0.30
        ):
            comp.append((url, cwr, cbr, cw, cb))
    return {
        "latency_regression": sorted(lat, key=lambda x: x[1] / x[2] if x[2] else 0.0, reverse=True)[:15],
        "payload_compression_regression": sorted(comp, key=lambda x: x[1] - x[2], reverse=True)[:15],
    }


# ── Batch 8: baseline+window per-IP outlier (2 detections) ───────────────────


def _build_ip_outlier(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    proj = [
        ("ip", COLUMNS["ip"]),
        ("req_header_bytes", COLUMNS["req_header_bytes"]),
        ("conn_requests", COLUMNS["conn_requests"]),
    ]
    cte, source = _base_cte(
        "ip_outlier", proj, row_filter="req_header_bytes > 0 OR conn_requests > 0", filter_clause=filter_clause
    )
    sql = (
        f"{cte} SELECT ip, "
        "maxIf(req_header_bytes, is_w AND req_header_bytes > 0) AS max_bytes, "
        "avgIf(req_header_bytes, is_w AND req_header_bytes > 0) AS avg_bytes, "
        "countIf(is_w AND req_header_bytes > 0) AS rs_w_total, "
        "quantileExactInclusiveIf(0.95)(req_header_bytes, is_b AND req_header_bytes > 0) AS rs_b_p95, "
        "countIf(is_b AND req_header_bytes > 0) AS rs_b_total, "
        "maxIf(conn_requests, is_w AND conn_requests > 0) AS max_reqs, "
        "avgIf(conn_requests, is_w AND conn_requests > 0) AS avg_reqs, "
        "countIf(is_w AND conn_requests > 0) AS ca_w_total, "
        "quantileExactInclusiveIf(0.95)(conn_requests, is_b AND conn_requests > 0) AS ca_b_p95, "
        "countIf(is_b AND conn_requests > 0) AS ca_b_total "
        "FROM base WHERE ip IN ("
        "SELECT ip FROM base WHERE is_w GROUP BY ip HAVING countIf(req_header_bytes > 0) >= 3 OR countIf(conn_requests > 0) >= 5"
        ") GROUP BY ip HAVING "
        "(rs_w_total >= 3 AND rs_b_total > 0 AND max_bytes > rs_b_p95 * 3) "
        "OR (ca_w_total >= 5 AND ca_b_total > 0 AND max_reqs >= 50 AND max_reqs > ca_b_p95 * 3)"
    )
    return sql, source


def _extract_ip_outlier(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    req_size: list[tuple[str, float, float, int, float]] = []
    conn_ab: list[tuple[str, float, float, int, float]] = []
    for r in rows:
        ip = str(r.get("ip") or "")
        rs_w, rs_b = _int(r.get("rs_w_total")), _int(r.get("rs_b_total"))
        max_b, b_p95 = _opt_float(r.get("max_bytes")), _opt_float(r.get("rs_b_p95"))
        if rs_w >= 3 and rs_b > 0 and max_b is not None and b_p95 is not None and max_b > b_p95 * 3:
            req_size.append((ip, max_b, _float(r.get("avg_bytes")), rs_w, b_p95))
        ca_w, ca_b = _int(r.get("ca_w_total")), _int(r.get("ca_b_total"))
        max_r, ca_p95 = _opt_float(r.get("max_reqs")), _opt_float(r.get("ca_b_p95"))
        if ca_w >= 5 and ca_b > 0 and max_r is not None and ca_p95 is not None and max_r >= 50 and max_r > ca_p95 * 3:
            conn_ab.append((ip, max_r, _float(r.get("avg_reqs")), ca_w, ca_p95))
    return {
        "request_size_anomaly": sorted(req_size, key=lambda x: x[1], reverse=True)[:15],
        "connection_abuse": sorted(conn_ab, key=lambda x: x[1], reverse=True)[:15],
    }


# ── Batch 9: baseline+window per-IP abuse (4 detections) ─────────────────────

_DEEP_URL = "url LIKE '/%/%/%' AND url NOT LIKE '%.%' AND (referer = '' OR referer = '-')"


def _build_ip_abuse(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    proj: list[tuple[str, str]] = [("ip", COLUMNS["ip"])]
    if active & {"content_discovery", "credential_enumeration", "orphaned_deep_crawl"}:
        proj.append(("url", COLUMNS["url"]))
    if active & {"content_discovery", "credential_enumeration"}:
        proj.append(("status", COLUMNS["status"]))
    if "session_harvesting" in active:
        proj.append(("cookie_session", COLUMNS["cookie_session"]))
    if "orphaned_deep_crawl" in active:
        proj.append(("referer", COLUMNS["referer"]))

    cte, source = _base_cte("ip_abuse", proj, row_filter="ip != ''", filter_clause=filter_clause)
    sel = ["ip"]
    pre_having: list[str] = []
    having: list[str] = []

    if "content_discovery" in active:
        sel.extend(
            [
                "countIf(is_w AND url != '' AND status = 404) AS cd_w_404",
                "countIf(is_w AND url != '') AS cd_w_total",
                "uniqExactIf(url, is_w AND url != '' AND status = 404) AS cd_w_distinct",
                "countIf(is_b AND url != '' AND status = 404) AS cd_b_404",
            ]
        )
        pre_having.append("countIf(url != '' AND status = 404) >= 20")
        having.append("(cd_w_404 >= 20 AND cd_w_404 * 1.0 / nullIf(cd_w_total, 0) >= 0.7 AND cd_w_distinct >= 15)")
    if "credential_enumeration" in active:
        auth = "url != '' AND match(url, {auth_regex:String})"
        sel.extend(
            [
                f"countIf(is_w AND {auth} AND status IN (401, 403)) AS ce_w_denied",
                f"countIf(is_w AND {auth}) AS ce_w_attempts",
                f"uniqExactIf(url, is_w AND {auth}) AS ce_w_paths",
                f"countIf(is_b AND {auth} AND status IN (401, 403)) AS ce_b_denied",
            ]
        )
        pre_having.append(f"countIf({auth} AND status IN (401, 403)) >= 20")
        having.append(
            "(ce_w_denied >= 20 AND ce_w_denied * 1.0 / nullIf(ce_w_attempts, 0) >= 0.5 "
            "AND ce_w_denied > ce_b_denied / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 3 + 5)"
        )
    if "session_harvesting" in active:
        sel.extend(
            [
                "uniqExactIf(cookie_session, is_w AND cookie_session != '') AS sh_w_sessions",
                "countIf(is_w AND cookie_session != '') AS sh_w_reqs",
                "uniqExactIf(cookie_session, is_b AND cookie_session != '') AS sh_b_sessions",
            ]
        )
        pre_having.append("uniqExactIf(cookie_session, cookie_session != '') >= 20")
        having.append("(sh_w_sessions >= 20 AND sh_w_sessions > sh_b_sessions * 2 + 10)")
    if "orphaned_deep_crawl" in active:
        sel.extend(
            [
                f"countIf(is_w AND {_DEEP_URL}) AS od_w_reqs",
                f"uniqExactIf(url, is_w AND {_DEEP_URL}) AS od_w_distinct",
                f"countIf(is_b AND {_DEEP_URL}) AS od_b_reqs",
            ]
        )
        pre_having.append(f"countIf({_DEEP_URL}) >= 50")
        having.append("(od_w_reqs >= 50 AND od_w_distinct >= 10)")

    sql = (
        f"{cte} SELECT {', '.join(sel)} FROM base "
        f"WHERE ip IN (SELECT ip FROM base WHERE is_w GROUP BY ip HAVING {' OR '.join(pre_having)}) "
        f"GROUP BY ip HAVING {' OR '.join(having)}"
    )
    return sql, source


def _extract_ip_abuse(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    out: dict[str, list[tuple[Any, ...]]] = {d: [] for d in active}
    cd: list[tuple[str, int, int, int, int]] = []
    ce: list[tuple[str, int, int, int, int]] = []
    sh: list[tuple[str, int, int, int]] = []
    od: list[tuple[str, int, int, int, float, float]] = []
    b_scale = max(baseline_hours, 1.0)

    for r in rows:
        ip = str(r.get("ip") or "")
        if "content_discovery" in active:
            w404, wtot, wdist, b404 = (
                _int(r.get("cd_w_404")),
                _int(r.get("cd_w_total")),
                _int(r.get("cd_w_distinct")),
                _int(r.get("cd_b_404")),
            )
            if w404 >= 20 and wtot > 0 and (w404 / wtot) >= 0.7 and wdist >= 15:
                cd.append((ip, w404, wtot, wdist, b404))
        if "credential_enumeration" in active:
            wd, wa, wp, bd = (
                _int(r.get("ce_w_denied")),
                _int(r.get("ce_w_attempts")),
                _int(r.get("ce_w_paths")),
                _int(r.get("ce_b_denied")),
            )
            if wd >= 20 and wa > 0 and (wd / wa) >= 0.5 and wd > (bd / b_scale * window_hours * 3 + 5):
                ce.append((ip, wd, wa, wp, bd))
        if "session_harvesting" in active:
            ws, wr, bs = _int(r.get("sh_w_sessions")), _int(r.get("sh_w_reqs")), _int(r.get("sh_b_sessions"))
            if ws >= 20 and ws > bs * 2 + 10:
                sh.append((ip, ws, wr, bs))
        if "orphaned_deep_crawl" in active:
            wr, wd, br = _int(r.get("od_w_reqs")), _int(r.get("od_w_distinct")), _int(r.get("od_b_reqs"))
            if wr >= 50 and wd >= 10:
                rpu = wr / max(float(wd), 1.0)
                surge = wr / max(br / b_scale * window_hours, 1.0)
                od.append((ip, wr, wd, br, rpu, surge))

    if "content_discovery" in active:
        out["content_discovery"] = sorted(cd, key=lambda x: x[1], reverse=True)[:15]
    if "credential_enumeration" in active:
        out["credential_enumeration"] = sorted(ce, key=lambda x: x[1], reverse=True)[:15]
    if "session_harvesting" in active:
        out["session_harvesting"] = sorted(sh, key=lambda x: x[1], reverse=True)[:15]
    if "orphaned_deep_crawl" in active:
        out["orphaned_deep_crawl"] = sorted(od, key=lambda x: x[1], reverse=True)[:15]
    return out


# ── Batch 10: baseline+window origin perf (2 detections) ─────────────────────


def _build_origin_perf(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    proj = [
        ("ottfb", COLUMNS["ottfb"]),
        ("url", COLUMNS["url"]),
        ("oconnect_ms", COLUMNS["oconnect_ms"]),
    ]
    cte, source = _base_cte("origin_perf", proj, row_filter="ottfb IS NOT NULL", filter_clause=filter_clause)
    read_ms = "greatest(ottfb / 1000.0 - oconnect_ms, 0)"
    has_conn = "oconnect_ms IS NOT NULL"
    sql = (
        f"{cte}, keyed AS (SELECT if(is_w, url, '') AS w_url, ottfb, oconnect_ms, is_w, is_b FROM base) "
        f"SELECT grouping(w_url) AS g_url, w_url, "
        f"{_P95}(ottfb, is_w) / 1000.0 AS w_p95, {_P95}(ottfb, is_b) / 1000.0 AS b_p95, "
        "countIf(is_w) AS w_reqs, round(quantileTDigestIf(0.95)(ottfb, is_w) / 1000.0, 1) AS url_p95_ms, "
        f"{_P95}(oconnect_ms, is_w AND {has_conn}) AS w_conn, {_P95}(oconnect_ms, is_b AND {has_conn}) AS b_conn, "
        f"{_P95}({read_ms}, is_w AND {has_conn}) AS w_read, {_P95}({read_ms}, is_b AND {has_conn}) AS b_read, "
        f"countIf(is_w AND {has_conn}) AS ts_w_total, countIf(is_b AND {has_conn}) AS ts_b_total "
        "FROM keyed GROUP BY GROUPING SETS ((), (w_url)) "
        "HAVING g_url = 1 OR w_reqs >= 10"
    )
    return sql, source


def _extract_origin_perf(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    overall: dict[str, Any] | None = None
    url_rows: list[dict[str, Any]] = []
    for r in rows:
        if _int(r.get("g_url")) == 1:
            overall = r
        else:
            url_rows.append(r)

    spike: list[tuple[str, float, float, float, int]] = []
    ts: list[tuple[float, float, float, float, int, int]] = []
    if overall is not None:
        o_w, o_b = _opt_float(overall.get("w_p95")), _opt_float(overall.get("b_p95"))
        if o_w is not None and o_b is not None and o_w > o_b * 2:
            for r in url_rows:
                reqs = _int(r.get("w_reqs"))
                if reqs >= 10:
                    spike.append((str(r.get("w_url") or ""), _float(r.get("url_p95_ms")), o_w, o_b, reqs))
            spike = sorted(spike, key=lambda x: x[1], reverse=True)[:10]

        tw, tb = _int(overall.get("ts_w_total")), _int(overall.get("ts_b_total"))
        wc, bc = _opt_float(overall.get("w_conn")), _opt_float(overall.get("b_conn"))
        wr, br = _opt_float(overall.get("w_read")), _opt_float(overall.get("b_read"))
        if tw >= 50 and tb >= 100 and wc is not None and bc is not None and wr is not None and br is not None:
            if (wc >= bc * 2 and wc - bc >= 50) or (wr >= br * 2 and wr - br >= 100):
                ts.append((wc, bc, wr, br, tw, tb))

    return {"origin_latency_spike": spike, "timeout_split": ts}


# ── Batch 11: baseline+window network ASN / metro (3 detections) ─────────────


def _build_network_asn(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    proj: list[tuple[str, str]] = []
    sets: list[str] = []
    has_asn = bool(active & {"network_asn_health", "asn_metro_performance"})
    has_metro = bool(active & {"asn_metro_performance", "metro_delivery_degradation"})
    if has_asn:
        proj.append(("asn_str", "custom_fields['asn']"))
    if has_metro:
        proj.append(("metro", COLUMNS["metro"]))
    if "network_asn_health" in active:
        proj.extend([("ploss", COLUMNS["ploss"]), ("rtt_var", COLUMNS["rtt_var"]), ("retrans", COLUMNS["retrans"])])
        sets.append("(asn_str)")
    if "asn_metro_performance" in active:
        proj.extend([("tcp_rtt", COLUMNS["tcp_rtt"]), ("country", COLUMNS["country"])])
        sets.append("(asn_str, metro)")
    if "metro_delivery_degradation" in active:
        proj.append(("delivery_rate", COLUMNS["delivery_rate"]))
        sets.append("(metro)")

    cte, source = _base_cte("network_asn", proj, filter_clause=filter_clause)
    g_asn = "grouping(asn_str)" if has_asn else "toUInt8(1)"
    g_metro = "grouping(metro)" if has_metro else "toUInt8(1)"
    sel = [
        f"{g_asn} AS g_asn",
        f"{g_metro} AS g_metro",
        "asn_str" if has_asn else "'' AS asn_str",
        "metro" if has_metro else "'' AS metro",
    ]
    having: list[str] = []
    if "network_asn_health" in active:
        sel.extend(
            [
                "avgIf(ploss, is_w) AS w_ploss",
                "avgIf(ploss, is_b) AS b_ploss",
                f"{_P95}(rtt_var, is_w) AS w_jitter",
                f"{_P95}(rtt_var, is_b) AS b_jitter",
                "avgIf(retrans, is_w) AS w_retrans",
                "avgIf(retrans, is_b) AS b_retrans",
                "countIf(is_w) AS nah_w_total",
                "countIf(is_b) AS nah_b_total",
            ]
        )
        having.append(
            "(g_asn = 0 AND g_metro = 1 AND toInt64OrNull(asn_str) IS NOT NULL AND nah_w_total >= 50 AND nah_b_total >= 100 "
            "AND ((coalesce(w_ploss, 0) >= 0.02 AND coalesce(w_ploss, 0) >= coalesce(b_ploss, 0) * 2 + 0.005) "
            "OR (w_jitter >= b_jitter * 2 AND w_jitter - b_jitter >= 20000) "
            "OR (coalesce(w_retrans, 0) >= coalesce(b_retrans, 0) * 2 + 0.5)))"
        )
    if "asn_metro_performance" in active:
        amp = "tcp_rtt > 0 AND country = 'US'"
        sel.extend(
            [
                f"quantileTDigestIf(0.50)(tcp_rtt, is_w AND {amp}) / 1000.0 AS amp_w_med",
                f"quantileTDigestIf(0.50)(tcp_rtt, is_b AND {amp}) / 1000.0 AS amp_b_med",
                f"countIf(is_w AND {amp}) AS amp_w_total",
                f"countIf(is_b AND {amp}) AS amp_b_total",
            ]
        )
        having.append(
            "(g_asn = 0 AND g_metro = 0 AND toInt64OrNull(asn_str) IS NOT NULL AND metro != '' "
            "AND amp_w_total >= 20 AND amp_b_total >= 50 AND amp_w_med >= amp_b_med * 1.5 AND amp_w_med - amp_b_med >= 20)"
        )
    if "metro_delivery_degradation" in active:
        sel.extend(
            [
                "quantileTDigestIf(0.5)(delivery_rate, is_w AND delivery_rate > 0) AS mdd_w_med",
                "quantileTDigestIf(0.5)(delivery_rate, is_b AND delivery_rate > 0) AS mdd_b_med",
                "countIf(is_w AND delivery_rate > 0) AS mdd_w_total",
                "countIf(is_b AND delivery_rate > 0) AS mdd_b_total",
            ]
        )
        having.append(
            "(g_asn = 1 AND g_metro = 0 AND metro != '' AND mdd_w_total >= 20 AND mdd_b_total >= 50 "
            "AND mdd_w_med > 0 AND mdd_b_med > 0 AND mdd_w_med <= mdd_b_med * 0.5)"
        )

    sql = f"{cte} SELECT {', '.join(sel)} FROM base GROUP BY GROUPING SETS ({', '.join(sets)}) HAVING {' OR '.join(having)}"
    return sql, source


def _extract_network_asn(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    out: dict[str, list[tuple[Any, ...]]] = {d: [] for d in active}
    nah: list[
        tuple[int, float | None, float | None, float | None, float | None, float | None, float | None, int, int]
    ] = []
    amp: list[tuple[int, str, float, float, int, int]] = []
    mdd: list[tuple[str, float, float, int, int]] = []

    for r in rows:
        g_asn, g_metro = _int(r.get("g_asn")), _int(r.get("g_metro"))
        if g_asn == 0 and g_metro == 1 and "network_asn_health" in active:
            asn = _parse_int(r.get("asn_str"))
            if asn is not None:
                nah.append(
                    (
                        asn,
                        _opt_float(r.get("w_ploss")),
                        _opt_float(r.get("b_ploss")),
                        _opt_float(r.get("w_jitter")),
                        _opt_float(r.get("b_jitter")),
                        _opt_float(r.get("w_retrans")),
                        _opt_float(r.get("b_retrans")),
                        _int(r.get("nah_w_total")),
                        _int(r.get("nah_b_total")),
                    )
                )
        elif g_asn == 0 and g_metro == 0 and "asn_metro_performance" in active:
            asn = _parse_int(r.get("asn_str"))
            metro = str(r.get("metro") or "")
            if asn is not None and metro:
                amp.append(
                    (
                        asn,
                        metro,
                        _float(r.get("amp_w_med")),
                        _float(r.get("amp_b_med")),
                        _int(r.get("amp_w_total")),
                        _int(r.get("amp_b_total")),
                    )
                )
        elif g_asn == 1 and g_metro == 0 and "metro_delivery_degradation" in active:
            metro = str(r.get("metro") or "")
            if metro:
                mdd.append(
                    (
                        metro,
                        _float(r.get("mdd_w_med")),
                        _float(r.get("mdd_b_med")),
                        _int(r.get("mdd_w_total")),
                        _int(r.get("mdd_b_total")),
                    )
                )

    if "network_asn_health" in active:
        out["network_asn_health"] = sorted(nah, key=lambda x: (x[1] or 0.0, x[3] or 0.0), reverse=True)[:15]
    if "asn_metro_performance" in active:
        out["asn_metro_performance"] = sorted(amp, key=lambda x: x[2] - x[3], reverse=True)[:15]
    if "metro_delivery_degradation" in active:
        out["metro_delivery_degradation"] = sorted(mdd, key=lambda x: x[2] - x[1], reverse=True)[:15]
    return out


# ── Batch 12: baseline+window fingerprint spread (3 detections) ──────────────


def _fp_col(present: set[str]) -> str:
    return "ja4" if "ja4" in present else ("ja3" if "ja3" in present else "")


def _build_fingerprint_spread(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    fp = _fp_col(present) or "ja4"
    has_fp = bool(active & {"botnet_grouping", "residential_fingerprint_dispersion"})
    has_cipher = "cipher_spread" in active
    proj: list[tuple[str, str]] = [("ip", COLUMNS["ip"])]
    if has_fp:
        proj.append((fp, COLUMNS[fp]))
    if "residential_fingerprint_dispersion" in active:
        proj.extend([("asn", COLUMNS["asn"]), ("p_type", COLUMNS["p_type"])])
    if has_cipher:
        proj.append(("tls_ciphers_sha", COLUMNS["tls_ciphers_sha"]))

    cte, source = _base_cte("fingerprint_spread", proj, filter_clause=filter_clause)
    use_sets = has_fp and has_cipher
    g_fp = f"grouping({fp})" if use_sets else ("toUInt8(0)" if has_fp else "toUInt8(1)")
    g_cipher = "grouping(tls_ciphers_sha)" if use_sets else ("toUInt8(0)" if has_cipher else "toUInt8(1)")
    group_by = (
        f"GROUP BY GROUPING SETS (({fp}), (tls_ciphers_sha))"
        if use_sets
        else (f"GROUP BY {fp}" if has_fp else "GROUP BY tls_ciphers_sha")
    )
    sel = [
        f"{g_fp} AS g_fp",
        f"{g_cipher} AS g_cipher",
        f"{fp} AS fp_val" if has_fp else "'' AS fp_val",
        "tls_ciphers_sha" if has_cipher else "'' AS tls_ciphers_sha",
        "uniqExactIf(ip, is_w) AS w_ips",
        "countIf(is_w) AS w_reqs",
        "uniqExactIf(ip, is_b) AS b_ips",
    ]
    having: list[str] = []
    if "botnet_grouping" in active:
        having.append(
            f"(g_fp = 0 AND {fp} != '' AND w_ips >= 5 "
            "AND w_ips > b_ips / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 3)"
        )
    if has_cipher:
        having.append(
            "(g_cipher = 0 AND tls_ciphers_sha != '' AND w_ips >= 5 "
            "AND w_ips > b_ips / greatest({baseline_hours:Float64}, 1.0) * {window_hours:Float64} * 3)"
        )
    if "residential_fingerprint_dispersion" in active:
        sel.extend(
            [
                "uniqExactIf(ip, is_w AND p_type != 'hosting') AS res_w_ips",
                "uniqExactIf(asn, is_w AND p_type != 'hosting') AS res_w_asns",
                "countIf(is_w AND p_type != 'hosting') AS res_w_reqs",
                "uniqExactIf(ip, is_b AND p_type != 'hosting') AS res_b_ips",
                "uniqExactIf(asn, is_b AND p_type != 'hosting') AS res_b_asns",
            ]
        )
        having.append(f"(g_fp = 0 AND {fp} != '' AND res_w_ips >= 30 AND res_w_asns >= 5)")

    sql = f"{cte} SELECT {', '.join(sel)} FROM base {group_by} HAVING {' OR '.join(having)}"
    return sql, source


def _extract_fingerprint_spread(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    out: dict[str, list[tuple[Any, ...]]] = {d: [] for d in active}
    botnet: list[tuple[str, int, int, int, float]] = []
    cipher: list[tuple[str, int, int, int, float]] = []
    res: list[tuple[str, int, int, int, int, float]] = []
    b_scale = max(baseline_hours, 1.0)

    for r in rows:
        g_fp, g_c = _int(r.get("g_fp")), _int(r.get("g_cipher"))
        w_ips, w_reqs, b_ips = _int(r.get("w_ips")), _int(r.get("w_reqs")), _int(r.get("b_ips"))
        expected = b_ips / b_scale * window_hours
        ratio = w_ips / max(expected, 1.0)
        if g_fp == 0:
            fp_val = str(r.get("fp_val") or "")
            if "botnet_grouping" in active and fp_val and w_ips >= 5 and w_ips > expected * 3:
                botnet.append((fp_val, w_ips, w_reqs, b_ips, ratio))
            if "residential_fingerprint_dispersion" in active and fp_val:
                rw_ips, rw_asns = _int(r.get("res_w_ips")), _int(r.get("res_w_asns"))
                if rw_ips >= 30 and rw_asns >= 5:
                    rw_reqs, rb_ips, rb_asns = (
                        _int(r.get("res_w_reqs")),
                        _int(r.get("res_b_ips")),
                        _int(r.get("res_b_asns")),
                    )
                    res.append((fp_val, rw_ips, rw_asns, rw_reqs, rb_ips, rw_asns / max(float(rb_asns), 1.0)))
        elif g_c == 0 and "cipher_spread" in active:
            sha = str(r.get("tls_ciphers_sha") or "")
            if sha and w_ips >= 5 and w_ips > expected * 3:
                cipher.append((sha, w_ips, w_reqs, b_ips, ratio))

    if "botnet_grouping" in active:
        out["botnet_grouping"] = sorted(botnet, key=lambda x: x[4], reverse=True)[:10]
    if "cipher_spread" in active:
        out["cipher_spread"] = [
            (s, wi, wr, bi) for s, wi, wr, bi, _ in sorted(cipher, key=lambda x: x[4], reverse=True)[:10]
        ]
    if "residential_fingerprint_dispersion" in active:
        out["residential_fingerprint_dispersion"] = sorted(res, key=lambda x: x[5], reverse=True)[:10]
    return out


# ── Batch 13: window-only beaconing (2 detections) ───────────────────────────


def _build_beaconing(
    active: set[str],
    *,
    present: set[str],
    rollup_span: tuple[datetime, datetime] | None,
    filter_clause: str,
) -> tuple[str, QuerySource]:
    fp = _fp_col(present) or "ja4"
    proj = [("sec", COLUMNS["sec"]), ("ip", COLUMNS["ip"]), ("ua", COLUMNS["ua"]), (fp, COLUMNS[fp])]
    cte, source = _base_cte(
        "beaconing",
        proj,
        window_only=True,
        row_filter="NOT (ua != '' AND match(ua, {bot_ua_regex:String}))",
        filter_clause=filter_clause,
    )
    sql = (
        f"{cte}, meta AS ("
        f"SELECT grouping(ip) AS g_ip, if(grouping(ip) = 0, ip, {fp}) AS entity, "
        "if(grouping(ip) = 0, uniqExact(ua), uniqExact(ip)) AS diversity, "
        "count() AS n_events, (max(sec) - min(sec)) AS span_s, "
        "arraySlice(arrayDifference(arraySort(groupUniqArray(sec))), 2) AS gaps_arr "
        f"FROM base GROUP BY GROUPING SETS ((ip), ({fp})) "
        "HAVING entity != '' AND n_events >= 13 AND length(gaps_arr) >= 12 AND (n_events * 1.0 / nullIf(span_s, 0)) < 2.0), "
        "agg AS (SELECT g_ip, entity, diversity, n_events, span_s, "
        "length(gaps_arr) AS n_gaps, arrayAvg(gaps_arr) AS mean_gap, "
        "arrayReduce('varSamp', gaps_arr) AS var_gap, arrayReduce('stddevSamp', gaps_arr) AS sd_gap, "
        "arrayDistinct(gaps_arr) AS u_gaps, "
        "arrayReduce('argMax', u_gaps, arrayMap(x -> tuple(countEqual(gaps_arr, x), -x), u_gaps)) AS mode_gap, "
        "arrayAvg(x -> if(abs(x - mode_gap) <= 1, 1.0, 0.0), gaps_arr) AS modal_frac FROM meta) "
        "SELECT a.g_ip, a.entity, a.n_gaps, a.n_events, round(a.mean_gap, 2) AS avg_interval, "
        "round(a.sd_gap, 2) AS stddev_interval, "
        "round(sqrt(greatest(a.var_gap - 1.0 / 12.0, 0)) / nullIf(a.mean_gap, 0), 3) AS cv_corr, "
        "round(a.modal_frac, 3) AS modal_frac, a.diversity, a.span_s, a.mode_gap "
        "FROM agg a WHERE a.n_gaps >= 12 AND (a.n_events * 1.0 / nullIf(a.span_s, 0)) < 2.0 "
        "AND ((a.mean_gap >= 5 AND sqrt(greatest(a.var_gap - 1.0 / 12.0, 0)) / nullIf(a.mean_gap, 0) < 0.3 AND a.modal_frac >= 0.6) "
        "OR (a.mean_gap >= 1 AND a.mean_gap < 5 AND a.modal_frac >= 0.85)) "
        "ORDER BY a.modal_frac DESC, a.n_gaps DESC"
    )
    return sql, source


def _extract_beaconing(
    rows: list[dict[str, Any]], active: set[str], *, window_hours: float, baseline_hours: float
) -> dict[str, list[tuple[Any, ...]]]:
    by_ip: list[tuple[Any, ...]] = []
    by_fp: list[tuple[Any, ...]] = []
    for r in rows:
        tup = (
            r.get("entity"),
            r.get("n_gaps"),
            r.get("n_events"),
            r.get("avg_interval"),
            r.get("stddev_interval"),
            r.get("cv_corr"),
            r.get("modal_frac"),
            r.get("diversity"),
            r.get("span_s"),
            r.get("mode_gap"),
        )
        if _int(r.get("g_ip")) == 0:
            if len(by_ip) < 15:
                by_ip.append(tup)
        else:
            if len(by_fp) < 15:
                by_fp.append(tup)
    return {"repeated_patterns": by_ip, "repeated_patterns_fp": by_fp}


# ── Batch registry ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DetectionBatch:
    name: str
    member_ids: tuple[str, ...]
    build_sql: Callable[..., tuple[str, QuerySource]]
    extract_rows: Callable[..., dict[str, list[tuple[Any, ...]]]]
    rollup_shape: str | None = None
    window_only: bool = False


DETECTION_BATCHES: tuple[DetectionBatch, ...] = (
    DetectionBatch(
        name="misc",
        member_ids=(
            "cache_hit_cliff",
            "http3_fallback",
            "origin_error_rate",
            "proxy_surge",
            "method_drift",
            "connection_type_mix",
        ),
        build_sql=_build_misc,
        extract_rows=_extract_misc,
        rollup_shape="misc",
    ),
    DetectionBatch(
        name="geo",
        member_ids=("new_country_traffic", "city_surges", "city_error_spikes", "new_city_traffic"),
        build_sql=_build_geo,
        extract_rows=_extract_geo,
        rollup_shape="geo",
    ),
    DetectionBatch(
        name="asn",
        member_ids=("asn_concentration", "new_asn_traffic", "asn_hosting_shift"),
        build_sql=_build_asn,
        extract_rows=_extract_asn,
        rollup_shape="asn",
    ),
    DetectionBatch(
        name="url_cache",
        member_ids=("cache_collapse", "cacheability_regression"),
        build_sql=_build_url_cache,
        extract_rows=_extract_url_cache,
        rollup_shape="url_cache",
    ),
    DetectionBatch(
        name="window_url",
        member_ids=("tail_latency", "cache_ttl_mismatch", "image_optimization_opportunities", "origin_retries"),
        build_sql=_build_window_url,
        extract_rows=_extract_window_url,
        window_only=True,
    ),
    DetectionBatch(
        name="latency_dim",
        member_ids=("pop_latency_regression", "region_latency", "city_latency_regressions"),
        build_sql=_build_latency_dim,
        extract_rows=_extract_latency_dim,
    ),
    DetectionBatch(
        name="url_perf",
        member_ids=("latency_regression", "payload_compression_regression"),
        build_sql=_build_url_perf,
        extract_rows=_extract_url_perf,
    ),
    DetectionBatch(
        name="ip_outlier",
        member_ids=("request_size_anomaly", "connection_abuse"),
        build_sql=_build_ip_outlier,
        extract_rows=_extract_ip_outlier,
    ),
    DetectionBatch(
        name="ip_abuse",
        member_ids=("content_discovery", "credential_enumeration", "session_harvesting", "orphaned_deep_crawl"),
        build_sql=_build_ip_abuse,
        extract_rows=_extract_ip_abuse,
    ),
    DetectionBatch(
        name="origin_perf",
        member_ids=("origin_latency_spike", "timeout_split"),
        build_sql=_build_origin_perf,
        extract_rows=_extract_origin_perf,
    ),
    DetectionBatch(
        name="network_asn",
        member_ids=("network_asn_health", "asn_metro_performance", "metro_delivery_degradation"),
        build_sql=_build_network_asn,
        extract_rows=_extract_network_asn,
    ),
    DetectionBatch(
        name="fingerprint_spread",
        member_ids=("botnet_grouping", "cipher_spread", "residential_fingerprint_dispersion"),
        build_sql=_build_fingerprint_spread,
        extract_rows=_extract_fingerprint_spread,
    ),
    DetectionBatch(
        name="beaconing",
        member_ids=("repeated_patterns", "repeated_patterns_fp"),
        build_sql=_build_beaconing,
        extract_rows=_extract_beaconing,
        window_only=True,
    ),
)

BATCH_BY_DETECTION: dict[str, DetectionBatch] = {
    member_id: batch for batch in DETECTION_BATCHES for member_id in batch.member_ids
}


def plan_execution(
    eligible: list[tuple[Any, Any]], present: set[str]
) -> tuple[list[tuple[DetectionBatch, list[tuple[Any, Any]]]], list[tuple[Any, Any]]]:
    """Group ``eligible`` ``(definition, detection)`` pairs into multi-member batches + standalones.

    Batches with fewer than two active members fall back to standalone queries. When the service
    uses ``ja3`` instead of ``ja4``, ``residential_fingerprint_dispersion`` (which is ``ja4``-only)
    is left standalone so ``batch_fingerprint_spread`` never mixes ``ja3`` and ``ja4``.
    """
    fp = _fp_col(present)
    by_batch: dict[str, list[tuple[Any, Any]]] = {}
    standalone: list[tuple[Any, Any]] = []
    for pair in eligible:
        definition = pair[0]
        batch = BATCH_BY_DETECTION.get(definition.id)
        if batch is None or (definition.id == "residential_fingerprint_dispersion" and fp != "ja4"):
            standalone.append(pair)
        else:
            by_batch.setdefault(batch.name, []).append(pair)

    batches: list[tuple[DetectionBatch, list[tuple[Any, Any]]]] = []
    for batch in DETECTION_BATCHES:
        members = by_batch.get(batch.name)
        if not members:
            continue
        if len(members) >= 2:
            batches.append((batch, members))
        else:
            standalone.extend(members)
    return batches, standalone
