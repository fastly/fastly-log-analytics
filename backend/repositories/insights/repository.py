from __future__ import annotations  # noqa: I001
import contextvars
import heapq
import logging
import threading
import time
import urllib.parse
import uuid
from collections.abc import Callable, Collection
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any
import duckdb
from backend.repositories._base import QueryRunner, _compact_sql_for_debug, _safe_table, record_duckdb_query
from backend.repositories.insights import registry
from backend.repositories.utils.response_shaping import (
    INSIGHT_BUCKET_INTERVALS,
    build_cache_collapse_breakdown,
    build_insight_asn_context,
    build_insights_envelope,
    select_insight_bucket_interval,
    shape_cache_collapse_timeline_point,
)
from . import (
    definitions as _definitions,  # noqa: F401  side-effect import: registers insight defs with the registry
    definitions as _insights_sql,
)
from .definitions import (
    COALESCED_ASN_AGGREGATES,
    COALESCED_CITY_AGGREGATES,
    COALESCED_DIM_AGGREGATES,
    COALESCED_FP_AGGREGATES,
    COALESCED_GEO_LATENCY_AGGREGATES,
    COALESCED_IP_EXTENDED_AGGREGATES,
    COALESCED_IP_SECURITY_AGGREGATES,
    COALESCED_SCALAR_EXTENDED_AGGREGATES,
    COALESCED_URL_AGGREGATES,
    COALESCED_URL_EXTENDED_AGGREGATES,
    REPEATED_BOT_UA_REGEX,
)

logger = logging.getLogger(__name__)
INSIGHTS_CACHE_TTL = 300  # seconds
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache

_insights_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=500, ttl_seconds=INSIGHTS_CACHE_TTL)
_insights_cache_lock = threading.Lock()


def _execute_on_cursor(
    runner: QueryRunner,
    debug_lock: threading.Lock,
    cur: duckdb.DuckDBPyConnection,
    sql: str,
    params: list | None = None,
):
    t0 = time.time()
    res = cur.execute(sql, params or [])
    elapsed_ms = round((time.time() - t0) * 1000, 2)
    compact_sql = _compact_sql_for_debug(sql)
    record_duckdb_query(compact_sql, elapsed_ms)
    with debug_lock:
        runner.debug_queries.append({"sql": compact_sql, "time_ms": elapsed_ms, "engine": "DuckDB"})
    return res


def _exec_coalesced(
    runner: QueryRunner,
    sql: str,
    params: list,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
    *,
    chunked: bool = False,
    fetch_one: bool = False,
) -> Any:
    cur = (
        _execute_on_cursor(runner, debug_lock, shadow_cursor, sql, params)
        if shadow_cursor is not None and debug_lock is not None
        else runner.execute(sql, params)
    )
    if chunked:
        return (r for batch in iter(lambda: cur.fetchmany(10000) or [], []) for r in batch)
    return cur.fetchone() if fetch_one else cur.fetchall()


_TOP_K = 15


def _col_or_null(col: str, actual_cols: Collection[str], null_type: str = "VARCHAR", prefix: str = "") -> str:
    return f'{prefix}"{col}"' if col in actual_cols else f"NULL::{null_type}"


def _share_exceeded(
    wc: int, w_tot: int, bc: int, b_tot: int, min_share: float, mult: float = 2.0, offset: float = 0.05
) -> bool:
    return w_tot > 0 and (wc / w_tot) >= min_share and (b_tot == 0 or (wc / w_tot) >= (bc / b_tot) * mult + offset)


def _p95_regressed(
    w_tot: Any, b_tot: Any, w_p95: Any, b_p95: Any, min_w: int, min_b: int, mult: float, min_diff: float
) -> bool:
    return (
        (w_tot or 0) >= min_w
        and (b_tot or 0) >= min_b
        and w_p95 is not None
        and b_p95 is not None
        and w_p95 >= b_p95 * mult
        and (w_p95 - b_p95) >= min_diff
    )


def _iso_or_str(val: Any) -> str:
    return val.isoformat() if hasattr(val, "isoformat") else str(val)


def _push_top_k(heap: list[tuple], score: Any, counter: int, item: tuple, k: int = _TOP_K) -> None:
    (heapq.heappush if len(heap) < k else heapq.heappushpop)(heap, (score, counter, item))


def _heap_to_sorted_items(heap: list[tuple]) -> list[tuple]:
    return [entry[2] for entry in sorted(heap, key=lambda e: e[0], reverse=True)]


def _coalesced_city_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    label_expr: str,
    region_sel: str,
    country_sel: str,
    window_hours: float,
    baseline_hours: float,
    *,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    sql = COALESCED_CITY_AGGREGATES.format(
        table_name=table_name, label_expr=label_expr, region_sel=region_sel, country_sel=country_sel
    )
    rows = _exec_coalesced(runner, sql, [window_start_s, window_start_s], shadow_cursor, debug_lock)
    surges, error_spikes, latency, new_city = [], [], [], []
    baseline_scale = max(baseline_hours, 1.0)
    for label, city, region, country, w_cnt, b_cnt, w_err, b_err, w_p95, b_p95, w_lat_total, b_lat_total in rows:
        b_cnt_i, b_err_i = b_cnt or 0, b_err or 0
        if w_cnt >= 20:
            b_normalized = b_cnt_i * 1.0 / baseline_scale * window_hours
            if w_cnt > b_normalized * 3:
                surges.append((label, city, region, country, w_cnt, b_cnt, w_cnt * 1.0 / max(b_normalized, 1.0)))
        if w_cnt >= 10:
            w_rate = (w_err / w_cnt) if w_cnt else 0.0
            b_rate = (b_err_i / b_cnt_i) if b_cnt_i else None
            if w_rate >= 0.10 and (b_cnt_i < 50 or (b_rate is not None and w_rate >= b_rate * 3 + 0.05)):
                error_spikes.append((label, city, region, country, w_rate, b_rate, w_err, w_cnt, b_cnt))
        if _p95_regressed(w_lat_total, b_lat_total, w_p95, b_p95, 10, 50, 3.0, 500):
            latency.append((label, city, region, country, w_p95, b_p95, w_lat_total, b_lat_total))
        if w_cnt >= 5 and b_cnt_i == 0:
            new_city.append((label, city, region, country, w_cnt, b_cnt))
    surges.sort(key=lambda x: -(x[6] or 0))
    error_spikes.sort(key=lambda x: -((x[4] or 0) - (x[5] or 0)))
    latency.sort(key=lambda x: -((x[4] / x[5]) if x[5] else 0))
    new_city.sort(key=lambda x: -(x[4] or 0))
    return {
        "city_surges": surges[:15],
        "city_error_spikes": error_spikes[:15],
        "city_latency_regressions": latency[:15],
        "new_city_traffic": new_city[:20],
    }


def _coalesced_url_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    *,
    actual_cols: Collection[str] | None = None,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    use_extended = actual_cols is not None
    if actual_cols is not None:
        sql = COALESCED_URL_EXTENDED_AGGREGATES.format(
            table_name=table_name,
            resp_bytes_sel=_col_or_null("resp_bytes", actual_cols, "BIGINT", "t."),
            uncompressed_sel="(NOT regexp_matches(lower(COALESCE(t.\"resp_header_content_encoding\", '')), 'gzip|br|zstd|deflate'))"
            if "resp_header_content_encoding" in actual_cols
            else "FALSE",
            is_mobile_sel="CASE WHEN t.\"ua\" ILIKE '%Mobi%' OR t.\"ua\" ILIKE '%Android%' OR t.\"ua\" ILIKE '%iPhone%' THEN 1 ELSE 0 END"
            if "ua" in actual_cols
            else "0",
            ttl_sel=_col_or_null("ttl", actual_cols, "DOUBLE", "t."),
            hits_sel=_col_or_null("hits", actual_cols, "DOUBLE", "t."),
            age_sel=_col_or_null("age", actual_cols, "DOUBLE", "t."),
            oretries_sel=_col_or_null("oretries", actual_cols, "BIGINT", "t."),
        )
    else:
        sql = COALESCED_URL_AGGREGATES.format(table_name=table_name)
    error_spikes_heap: list[Any] = []
    cache_collapse_heap: list[Any] = []
    cacheability_regression_heap: list[Any] = []
    latency_regression_heap: list[Any] = []
    tail_latency_heap: list[Any] = []
    new_probe_urls_heap: list[Any] = []
    payload_comp_heap: list[Any] = []
    image_opt_heap: list[Any] = []
    cache_ttl_heap: list[Any] = []
    origin_retries_heap: list[Any] = []
    ext_cols = actual_cols if use_extended and actual_cols is not None else ()
    has_pcr = {"resp_header_content_encoding", "resp_bytes"}.issubset(ext_cols)
    has_img = {"resp_bytes", "ua"}.issubset(ext_cols)
    has_ttl = {"ttl", "hits", "age"}.issubset(ext_cols)
    has_oret = "oretries" in ext_cols
    for counter, r in enumerate(
        _exec_coalesced(runner, sql, [window_start_s, window_start_s], shadow_cursor, debug_lock, chunked=True), 1
    ):
        url, w_total, b_total, w_5xx, b_5xx, w_hits, b_hits, w_miss, b_miss = r[:9]
        w_pass, b_pass, w_lat_total, b_lat_total, w_p95, b_p95, w_p99, w_p50 = r[9:17]
        ext = r[17:] if use_extended else (None,) * 19
        is_probe, w_4xx, is_comp, pcr_w_uncomp, pcr_w_total, pcr_b_uncomp, pcr_b_total = ext[:7]
        is_img, img_w_cnt, img_w_bytes, img_w_avg_bytes, img_w_mobile = ext[7:12]
        ttl_w_cnt, ttl_w_avg_ttl, ttl_w_avg_hits, ttl_w_avg_age, oret_w_cnt, oret_w_avg, oret_w_max = ext[12:]
        w_total_i, b_total_i = w_total or 0, b_total or 0
        if w_total_i >= 3:
            w_rate_e = (w_5xx or 0) / w_total_i if w_total_i else 0.0
            b_rate_e = ((b_5xx or 0) / b_total_i) if b_total_i else None
            if w_rate_e >= 0.05 and (b_total_i < 10 or (b_rate_e is not None and w_rate_e >= b_rate_e * 2 + 0.05)):
                _push_top_k(
                    error_spikes_heap,
                    (w_rate_e or 0) - (b_rate_e or 0),
                    counter,
                    (url, w_rate_e, b_rate_e, w_5xx, w_total, b_total),
                )
        w_cacheable, b_cacheable = (w_hits or 0) + (w_miss or 0), (b_hits or 0) + (b_miss or 0)
        if w_cacheable >= 5 and b_cacheable >= 20:
            w_rate_c, b_rate_c = (w_hits or 0) / w_cacheable, (b_hits or 0) / b_cacheable
            if b_rate_c >= 0.40 and w_rate_c <= b_rate_c - 0.20 and w_rate_c <= b_rate_c * 0.6:
                _push_top_k(
                    cache_collapse_heap,
                    (b_rate_c or 0) - (w_rate_c or 0),
                    counter,
                    (url, w_rate_c, b_rate_c, w_cacheable, b_cacheable),
                )
        if w_total_i >= 10 and b_total_i >= 50:
            w_pass_rate, b_pass_rate = (w_pass or 0) / w_total_i, (b_pass or 0) / b_total_i
            if b_pass_rate <= 0.20 and w_pass_rate >= 0.50 and w_pass_rate >= b_pass_rate + 0.30:
                _push_top_k(
                    cacheability_regression_heap,
                    (w_pass_rate or 0) - (b_pass_rate or 0),
                    counter,
                    (url, w_pass_rate, b_pass_rate, w_total, b_total),
                )
        if _p95_regressed(w_total_i, b_total_i, w_p95, b_p95, 5, 20, 2.0, 200):
            _push_top_k(
                latency_regression_heap,
                (w_p95 / b_p95) if b_p95 else 0.0,
                counter,
                (url, w_p95, b_p95, w_total, b_total),
            )
        if w_lat_total is not None and w_lat_total >= 20 and w_p99 is not None and w_p50 is not None and w_p50 > 0:
            ratio = round(w_p99 / w_p50, 1)
            if ratio > 5:
                _push_top_k(tail_latency_heap, ratio or 0.0, counter, (url, w_p99, w_p50, ratio, w_lat_total))
        if use_extended:
            if is_probe and w_total_i > 0 and b_total_i == 0:
                _push_top_k(
                    new_probe_urls_heap,
                    float(w_total_i),
                    counter,
                    (url, w_total_i, b_total_i, (w_4xx or 0) * 100.0 / w_total_i),
                    k=25,
                )
            if has_pcr and is_comp and (pcr_w_total or 0) >= 10 and (pcr_b_total or 0) >= 50:
                w_rate_p, b_rate_p = (pcr_w_uncomp or 0) / (pcr_w_total or 1), (pcr_b_uncomp or 0) / (pcr_b_total or 1)
                if b_rate_p <= 0.20 and w_rate_p >= 0.50 and w_rate_p >= b_rate_p + 0.30:
                    _push_top_k(
                        payload_comp_heap,
                        w_rate_p - b_rate_p,
                        counter,
                        (url, w_rate_p, b_rate_p, pcr_w_total, pcr_b_total),
                        k=15,
                    )
            if has_img and is_img and img_w_bytes is not None and img_w_bytes > 524288:
                avg_kb = round((img_w_avg_bytes or 0.0) / 1024.0, 1)
                mobile_ratio = ((img_w_mobile or 0) * 1.0 / img_w_cnt) if img_w_cnt else 0.0
                _push_top_k(
                    image_opt_heap,
                    float(img_w_bytes),
                    counter,
                    (url, img_w_cnt, img_w_bytes, avg_kb, mobile_ratio),
                    k=15,
                )
            if (
                has_ttl
                and (ttl_w_cnt or 0) >= 10
                and ttl_w_avg_hits is not None
                and ttl_w_avg_hits < 2
                and ttl_w_avg_ttl is not None
                and ttl_w_avg_ttl > 60
            ):
                _push_top_k(
                    cache_ttl_heap,
                    float(ttl_w_avg_ttl),
                    counter,
                    (url, round(ttl_w_avg_ttl, 0), round(ttl_w_avg_hits, 1), round(ttl_w_avg_age or 0.0, 0), ttl_w_cnt),
                    k=20,
                )
            if has_oret and (oret_w_cnt or 0) >= 5 and oret_w_avg is not None:
                avg_ret = round(oret_w_avg, 2)
                _push_top_k(origin_retries_heap, float(avg_ret), counter, (url, oret_w_cnt, avg_ret, oret_w_max), k=10)
    res: dict[str, list[tuple]] = {
        "error_spikes": _heap_to_sorted_items(error_spikes_heap),
        "cache_collapse": _heap_to_sorted_items(cache_collapse_heap),
        "cacheability_regression": _heap_to_sorted_items(cacheability_regression_heap),
        "latency_regression": _heap_to_sorted_items(latency_regression_heap),
        "tail_latency": _heap_to_sorted_items(tail_latency_heap),
    }
    if use_extended:
        res["new_probe_urls"] = _heap_to_sorted_items(new_probe_urls_heap)
        for flag, key, hp in (
            (has_pcr, "payload_compression_regression", payload_comp_heap),
            (has_img, "image_optimization_opportunities", image_opt_heap),
            (has_ttl, "cache_ttl_mismatch", cache_ttl_heap),
            (has_oret, "origin_retries", origin_retries_heap),
        ):
            if flag:
                res[key] = _heap_to_sorted_items(hp)
    return res


def _coalesced_ip_security_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    window_hours: float,
    baseline_hours: float,
    *,
    actual_cols: Collection[str] | None = None,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    use_extended = actual_cols is not None
    if actual_cols is not None:
        sql = COALESCED_IP_EXTENDED_AGGREGATES.format(
            table_name=table_name,
            no_referer_sel='(t."referer" IS NULL OR t."referer" = \'\' OR t."referer" = \'-\')'
            if "referer" in actual_cols
            else "FALSE",
            cs_sel=_col_or_null("cookie_session", actual_cols, "VARCHAR", "t."),
            req_header_bytes_sel=_col_or_null("req_header_bytes", actual_cols, "BIGINT", "t."),
            conn_requests_sel=_col_or_null("conn_requests", actual_cols, "BIGINT", "t."),
        )
    else:
        sql = COALESCED_IP_SECURITY_AGGREGATES.format(table_name=table_name)
    low_and_slow_heap: list[Any] = []
    credential_enumeration_heap: list[Any] = []
    content_discovery_heap: list[Any] = []
    orphaned_deep_crawl_heap: list[Any] = []
    session_harvesting_heap: list[Any] = []
    ext_cols = actual_cols if use_extended and actual_cols is not None else ()
    has_odc, has_sh = "referer" in ext_cols, "cookie_session" in ext_cols
    has_rsa, has_ca = "req_header_bytes" in ext_cols, "conn_requests" in ext_cols
    rsa_candidates = ca_candidates = 0
    baseline_scale = max(baseline_hours, 1.0)
    for counter, r in enumerate(
        _exec_coalesced(runner, sql, [window_start_s, window_start_s], shadow_cursor, debug_lock, chunked=True), 1
    ):
        ip, ls_hits, ls_distinct, ls_min_sec, ls_max_sec, ce_w_denied, ce_w_attempts = r[:7]
        ce_w_paths, ce_b_denied, cd_w_404, cd_w_total, cd_distinct_404, cd_b_404 = r[7:13]
        ext = r[13:] if use_extended else (None,) * 14
        odc_w_reqs, odc_w_urls, odc_b_reqs, sh_w_sessions, sh_w_reqs, sh_b_sessions = ext[:6]
        rsa_w_cnt, rsa_w_max, rsa_b_min, _rsa_b_max, ca_w_cnt, ca_w_max, ca_b_min, _ca_b_max = ext[6:]
        ls_hits_i, ls_distinct_i = ls_hits or 0, ls_distinct or 0
        span_s = (ls_max_sec - ls_min_sec) if (ls_min_sec is not None and ls_max_sec is not None) else 0
        if ls_hits_i >= 5 and ls_distinct_i >= 3 and span_s >= 600 and (ls_hits_i / span_s) < 0.2:
            _push_top_k(
                low_and_slow_heap,
                (ls_distinct_i, span_s),
                counter,
                (ip, ls_hits_i, ls_distinct_i, span_s, round(ls_hits_i / span_s, 5)),
            )
        ce_w_denied_i = ce_w_denied or 0
        if ce_w_denied_i >= 20:
            ce_ratio = (ce_w_denied_i / ce_w_attempts) if ce_w_attempts else 0.0
            if ce_ratio >= 0.5 and ce_w_denied_i > ((ce_b_denied or 0) / baseline_scale * window_hours * 3 + 5):
                _push_top_k(
                    credential_enumeration_heap,
                    (ce_w_denied_i),
                    counter,
                    (ip, ce_w_denied_i, ce_w_attempts, ce_w_paths, ce_b_denied),
                )
        cd_w_404_i, cd_distinct_404_i = cd_w_404 or 0, cd_distinct_404 or 0
        if cd_w_404_i >= 20 and cd_distinct_404_i >= 15 and ((cd_w_404_i / cd_w_total) if cd_w_total else 0.0) >= 0.7:
            _push_top_k(
                content_discovery_heap, (cd_w_404_i), counter, (ip, cd_w_404_i, cd_w_total, cd_distinct_404_i, cd_b_404)
            )
        if use_extended:
            if has_odc and odc_w_reqs is not None and odc_w_reqs >= 50 and odc_w_urls is not None and odc_w_urls >= 10:
                b_reqs_i = odc_b_reqs or 0
                _push_top_k(
                    orphaned_deep_crawl_heap,
                    (odc_w_reqs),
                    counter,
                    (
                        ip,
                        odc_w_reqs,
                        odc_w_urls,
                        b_reqs_i,
                        odc_w_reqs * 1.0 / max(odc_w_urls, 1.0),
                        odc_w_reqs * 1.0 / max(b_reqs_i / baseline_scale * window_hours, 1.0),
                    ),
                )
            if has_sh and sh_w_sessions is not None and sh_w_sessions >= 20:
                b_sess_i = sh_b_sessions or 0
                if sh_w_sessions > b_sess_i * 2 + 10:
                    _push_top_k(
                        session_harvesting_heap, (sh_w_sessions), counter, (ip, sh_w_sessions, sh_w_reqs, b_sess_i)
                    )
            if has_rsa and (rsa_w_cnt or 0) >= 3 and rsa_b_min is not None and (rsa_w_max or 0) > rsa_b_min * 3:
                rsa_candidates += 1
            if (
                has_ca
                and (ca_w_cnt or 0) >= 5
                and (ca_w_max or 0) >= 50
                and ca_b_min is not None
                and (ca_w_max or 0) > ca_b_min * 3
            ):
                ca_candidates += 1
    res: dict[str, list[tuple]] = {
        "low_and_slow": _heap_to_sorted_items(low_and_slow_heap),
        "credential_enumeration": _heap_to_sorted_items(credential_enumeration_heap),
        "content_discovery": _heap_to_sorted_items(content_discovery_heap),
    }
    if use_extended:
        if has_odc:
            res["orphaned_deep_crawl"] = _heap_to_sorted_items(orphaned_deep_crawl_heap)
        if has_sh:
            res["session_harvesting"] = _heap_to_sorted_items(session_harvesting_heap)
        if has_rsa and rsa_candidates == 0:
            res["request_size_anomaly"] = []
        if has_ca and ca_candidates == 0:
            res["connection_abuse"] = []
    return res


def _coalesced_asn_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    actual_cols: Collection[str],
    *,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    has_health = {"ploss", "rtt_var", "retrans"}.issubset(actual_cols)
    has_ptype = "p_type" in actual_cols
    sql = COALESCED_ASN_AGGREGATES.format(
        table_name=table_name,
        ploss_sel=_col_or_null("ploss", actual_cols, "DOUBLE"),
        rtt_var_sel=_col_or_null("rtt_var", actual_cols, "DOUBLE"),
        retrans_sel=_col_or_null("retrans", actual_cols, "DOUBLE"),
        p_type_sel=_col_or_null("p_type", actual_cols),
    )
    rows = _exec_coalesced(runner, sql, [window_start_s, window_start_s], shadow_cursor, debug_lock)
    w_total_all = sum(int(r[1] or 0) for r in rows)
    b_total_all = sum(int(r[2] or 0) for r in rows)
    concentration, new_asn, health, hosting_shift = [], [], [], []
    for r in rows:
        asn, w_cnt, b_cnt, w_ploss, b_ploss, w_jitter, b_jitter, w_retrans, b_retrans = r[:9]
        w_hosting, w_pt_total, b_hosting, b_pt_total = r[9:]
        if asn is None:
            continue
        w_cnt_i, b_cnt_i = int(w_cnt or 0), int(b_cnt or 0)
        if _share_exceeded(w_cnt_i, w_total_all, b_cnt_i, b_total_all, 0.20, mult=3.0, offset=0.10):
            concentration.append((asn, w_cnt_i, b_cnt_i, b_total_all, w_total_all))
        if w_cnt_i >= 20 and b_cnt_i == 0:
            new_asn.append((asn, w_cnt_i, b_cnt_i))
        if has_health and w_cnt_i >= 50 and b_cnt_i >= 100:
            w_ploss_f, b_ploss_f = float(w_ploss or 0.0), float(b_ploss or 0.0)
            w_retrans_f, b_retrans_f = float(w_retrans or 0.0), float(b_retrans or 0.0)
            ploss_bad = w_ploss_f >= 0.02 and w_ploss_f >= b_ploss_f * 2 + 0.005
            jitter_bad = (
                w_jitter is not None
                and b_jitter is not None
                and w_jitter >= b_jitter * 2
                and (w_jitter - b_jitter) >= 20000
            )
            retrans_bad = w_retrans_f >= b_retrans_f * 2 + 0.5
            if ploss_bad or jitter_bad or retrans_bad:
                health.append((asn, w_ploss, b_ploss, w_jitter, b_jitter, w_retrans, b_retrans, w_cnt_i, b_cnt_i))
        if has_ptype:
            w_pt_i, b_pt_i, w_host_i, b_host_i = (
                int(w_pt_total or 0),
                int(b_pt_total or 0),
                int(w_hosting or 0),
                int(b_hosting or 0),
            )
            if w_pt_i >= 50 and _share_exceeded(w_host_i, w_pt_i, b_host_i, b_pt_i, 0.30, mult=3.0, offset=0.10):
                hosting_shift.append((asn, w_host_i, w_pt_i, b_host_i, b_pt_i))
    concentration.sort(key=lambda x: -(x[1] or 0))
    new_asn.sort(key=lambda x: -(x[1] or 0))
    health.sort(key=lambda x: (-(x[1] or 0.0), -(x[3] if x[3] is not None else float("-inf"))))
    hosting_shift.sort(key=lambda x: -(x[1] or 0))
    res: dict[str, list[tuple]] = {"asn_concentration": concentration[:10], "new_asn_traffic": new_asn[:20]}
    if has_health:
        res["network_asn_health"] = health[:15]
    if has_ptype:
        res["asn_hosting_shift"] = hosting_shift[:15]
    return res


def _coalesced_scalar_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    actual_cols: Collection[str],
    *,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    has_transport, has_cache = "transport" in actual_cols, "cache" in actual_cols
    has_oconnect, has_ottfb = "oconnect_ms" in actual_cols, "ottfb" in actual_cols
    has_ts = has_oconnect and has_ottfb
    if not (has_transport or has_cache or has_ts or has_ottfb):
        return {}
    sql = COALESCED_SCALAR_EXTENDED_AGGREGATES.format(
        table_name=table_name,
        transport_sel=_col_or_null("transport", actual_cols),
        cache_sel=_col_or_null("cache", actual_cols),
        oconnect_ms_sel=_col_or_null("oconnect_ms", actual_cols, "DOUBLE"),
        ottfb_sel=_col_or_null("ottfb", actual_cols, "DOUBLE"),
    )
    row = _exec_coalesced(runner, sql, [window_start_s, window_start_s], shadow_cursor, debug_lock, fetch_one=True)
    res: dict[str, list[tuple]] = {}
    if not row:
        return {
            k: []
            for flag, k in (
                (has_transport, "http3_fallback"),
                (has_cache, "cache_hit_cliff"),
                (has_ts, "timeout_split"),
                (has_ottfb, "origin_latency_spike"),
            )
            if flag
        }
    w_quic, w_tr_total, b_quic, b_tr_total, w_hits, w_cacheable, b_hits, b_cacheable = (int(v or 0) for v in row[:8])
    ts_w_conn, ts_b_conn, ts_w_read, ts_b_read, ts_w_total_raw, ts_b_total_raw, ols_w_p95, ols_b_p95 = row[8:16]
    ts_w_total, ts_b_total = int(ts_w_total_raw or 0), int(ts_b_total_raw or 0)
    if has_transport:
        res["http3_fallback"] = (
            [(w_quic, w_tr_total, b_quic, b_tr_total)]
            if w_tr_total >= 100
            and b_tr_total >= 100
            and (b_quic / b_tr_total) >= 0.30
            and (w_quic / w_tr_total) <= (b_quic / b_tr_total) - 0.20
            else []
        )
    if has_cache:
        res["cache_hit_cliff"] = (
            [(w_hits, w_cacheable, b_hits, b_cacheable)]
            if w_cacheable >= 100
            and b_cacheable >= 200
            and (b_hits / b_cacheable) >= 0.40
            and (w_hits / w_cacheable) <= (b_hits / b_cacheable) - 0.15
            else []
        )
    if has_ts:
        conn_bad = ts_w_conn is not None and ts_b_conn is not None and ts_w_conn > ts_b_conn * 2 and ts_w_conn >= 50
        read_bad = ts_w_read is not None and ts_b_read is not None and ts_w_read > ts_b_read * 2 and ts_w_read >= 100
        res["timeout_split"] = (
            [(ts_w_conn, ts_b_conn, ts_w_read, ts_b_read, ts_w_total, ts_b_total)]
            if ts_w_total >= 50 and ts_b_total >= 100 and (conn_bad or read_bad)
            else []
        )
    if has_ottfb:
        if ols_w_p95 is None or ols_b_p95 is None or not (ols_w_p95 > ols_b_p95 * 2):
            res["origin_latency_spike"] = []
    return res


def _coalesced_fp_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    window_hours: float,
    baseline_hours: float,
    actual_cols: Collection[str],
    *,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    has_ip = "ip" in actual_cols
    has_fp = has_ip and ("ja4" in actual_cols or "ja3" in actual_cols)
    has_rfd = has_ip and "ja4" in actual_cols and "asn" in actual_cols and "p_type" in actual_cols
    has_cipher = has_ip and "tls_ciphers_sha" in actual_cols
    if not (has_fp or has_rfd or has_cipher):
        return {}
    fp_col = '"ja4"' if "ja4" in actual_cols else _col_or_null("ja3", actual_cols)
    sql = COALESCED_FP_AGGREGATES.format(
        table_name=table_name,
        fp_sel=fp_col if (has_fp or has_rfd) else "NULL::VARCHAR",
        cipher_sel='"tls_ciphers_sha"' if has_cipher else "NULL::VARCHAR",
        ip_sel='"ip"',
        asn_sel=_col_or_null("asn", actual_cols, "BIGINT"),
        is_non_hosting_sel='("p_type" IS NULL OR "p_type" != \'hosting\')' if "p_type" in actual_cols else "true",
    )
    raw_rows = _exec_coalesced(runner, sql, [window_start_s, window_start_s], shadow_cursor, debug_lock)
    baseline_scale, win_h = max(float(baseline_hours), 1.0), float(window_hours)
    botnet, rfd, cipher_list = [], [], []
    for r in raw_rows:
        gid, fp, cipher, bg_w_ips, bg_w_reqs, bg_b_ips = r[:6]
        rfd_w_ips, rfd_w_asns, rfd_w_reqs, rfd_b_ips, rfd_b_asns, cs_w_ips, cs_w_reqs, cs_b_ips = r[6:14]
        gid_i = int(gid or 0)
        if gid_i == 1 and fp is not None and fp != "":
            if has_fp:
                w_ips, w_reqs, b_ips = int(bg_w_ips or 0), int(bg_w_reqs or 0), int(bg_b_ips or 0)
                scaled_b = b_ips / baseline_scale * win_h
                if w_ips >= 5 and w_ips > scaled_b * 3:
                    botnet.append((fp, w_ips, w_reqs, b_ips, w_ips * 1.0 / max(scaled_b, 1.0)))
            if has_rfd:
                rw_ips, rw_asns, rw_reqs, rb_ips, rb_asns = (
                    int(rfd_w_ips or 0),
                    int(rfd_w_asns or 0),
                    int(rfd_w_reqs or 0),
                    int(rfd_b_ips or 0),
                    int(rfd_b_asns or 0),
                )
                if rw_ips >= 30 and rw_asns >= 5:
                    rfd.append((fp, rw_ips, rw_asns, rw_reqs, rb_ips, rw_asns * 1.0 / max(rb_asns, 1.0)))
        elif gid_i == 2 and has_cipher and cipher is not None and cipher != "":
            cw_ips, cw_reqs, cb_ips = int(cs_w_ips or 0), int(cs_w_reqs or 0), int(cs_b_ips or 0)
            scaled_cb = cb_ips / baseline_scale * win_h
            if cw_ips >= 5 and cw_ips > scaled_cb * 3:
                cipher_list.append((cw_ips * 1.0 / max(scaled_cb, 1.0), (cipher, cw_ips, cw_reqs, cb_ips)))
    botnet.sort(key=lambda r: r[4], reverse=True)
    rfd.sort(key=lambda r: r[5], reverse=True)
    cipher_list.sort(key=lambda pair: pair[0], reverse=True)
    res: dict[str, list[tuple]] = {}
    if has_fp:
        res["botnet_grouping"] = botnet[:10]
    if has_rfd:
        res["residential_fingerprint_dispersion"] = rfd[:10]
    if has_cipher:
        res["cipher_spread"] = [item for _, item in cipher_list[:10]]
    return res


def _coalesced_dim_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    actual_cols: Collection[str],
    *,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    has_country, has_ptype, has_method = "country" in actual_cols, "p_type" in actual_cols, "method" in actual_cols
    has_ctype = "c_type" in actual_cols and "c_speed" in actual_cols
    has_ost = "ost" in actual_cols
    has_oip = "oip" in actual_cols and has_ost
    has_referer, has_ua = "referer" in actual_cols, "ua" in actual_cols
    if not (has_country or has_ptype or has_method or has_ctype or has_ost or has_oip or has_referer or has_ua):
        return {}
    sql = COALESCED_DIM_AGGREGATES.format(
        table_name=table_name,
        country_sel=_col_or_null("country", actual_cols),
        p_type_sel=_col_or_null("p_type", actual_cols),
        method_sel=_col_or_null("method", actual_cols),
        c_type_sel='"c_type"' if has_ctype else "NULL::VARCHAR",
        c_speed_sel='"c_speed"' if has_ctype else "NULL::VARCHAR",
        ost_sel=_col_or_null("ost", actual_cols, "INTEGER"),
        oip_sel='"oip"' if has_oip else "NULL::VARCHAR",
        referer_sel=_col_or_null("referer", actual_cols),
        ua_sel=_col_or_null("ua", actual_cols),
    )
    raw_rows = _exec_coalesced(runner, sql, [window_start_s, window_start_s], shadow_cursor, debug_lock)
    country_rows, ptype_rows, method_rows, ctype_rows = [], [], [], []
    ost_rows, oip_rows, referer_rows, ua_rows = [], [], [], []
    for r in raw_rows:
        gid, country, p_type, method, c_type, c_speed, ost, oip, referer, ua = r[:10]
        w_cnt, b_cnt, w_ost_nn, b_ost_nn, w_ost_5xx, b_ost_5xx = (int(v or 0) for v in r[10:16])
        gid_i = int(gid or 0)
        if gid_i == 255:
            country_rows.append((country, w_cnt, b_cnt))
        elif gid_i == 383:
            ptype_rows.append((p_type, w_cnt, b_cnt))
        elif gid_i == 447:
            method_rows.append((method, w_cnt, b_cnt))
        elif gid_i == 463:
            ctype_rows.append((c_type, c_speed, w_cnt, b_cnt))
        elif gid_i == 503:
            ost_rows.append((ost, w_cnt, b_cnt, w_ost_nn, b_ost_nn, w_ost_5xx, b_ost_5xx))
        elif gid_i == 507:
            oip_rows.append((oip, w_ost_nn, w_ost_5xx))
        elif gid_i == 509:
            referer_rows.append((referer, w_cnt, b_cnt))
        elif gid_i == 510:
            ua_rows.append((ua, w_cnt, b_cnt))
    res: dict[str, list[tuple]] = {}
    if has_country:
        nc = sorted(
            [(c, wc, bc) for c, wc, bc in country_rows if c is not None and wc >= 3 and bc == 0],
            key=lambda r: r[1],
            reverse=True,
        )
        res["new_country_traffic"] = nc[:20]
    if has_ptype:
        pt_nn = [(pt, wc, bc) for pt, wc, bc in ptype_rows if pt is not None]
        w_total_all, b_total_all = sum(wc for _, wc, _ in pt_nn), sum(bc for _, _, bc in pt_nn)
        pt_nonempty = [(pt, wc, bc) for pt, wc, bc in pt_nn if pt != ""]
        w_proxy_total, b_proxy_total = sum(wc for _, wc, _ in pt_nonempty), sum(bc for _, _, bc in pt_nonempty)
        if (
            w_total_all > 0
            and b_total_all > 0
            and (w_proxy_total * 100.0 / w_total_all) >= 5.0
            and (w_proxy_total * 100.0 / w_total_all) >= (b_proxy_total * 100.0 / b_total_all) * 2 + 5
        ):
            res["proxy_surge"] = [(pt, wc, bc, w_total_all, b_total_all) for pt, wc, bc in pt_nonempty]
        else:
            res["proxy_surge"] = []
    if has_method:
        m_w_total, m_b_total = sum(wc for _, wc, _ in method_rows), sum(bc for _, _, bc in method_rows)
        md = sorted(
            [
                (m, wc, bc, m_w_total, m_b_total)
                for m, wc, bc in method_rows
                if m is not None
                and m != ""
                and str(m).upper() not in ("GET", "HEAD", "OPTIONS")
                and wc >= 20
                and _share_exceeded(wc, m_w_total, bc, m_b_total, min_share=0.10, mult=2.0, offset=0.05)
            ],
            key=lambda r: r[1],
            reverse=True,
        )
        res["method_drift"] = md[:10]
    if has_ctype:
        ct_valid = [(ct, cs, wc, bc) for ct, cs, wc, bc in ctype_rows if ct is not None and ct != ""]
        ct_w_total, ct_b_total = sum(wc for _, _, wc, _ in ct_valid), sum(bc for _, _, _, bc in ct_valid)
        ct_out = sorted(
            [
                (ct, cs, wc, bc, ct_w_total, ct_b_total)
                for ct, cs, wc, bc in ct_valid
                if wc >= 20 and _share_exceeded(wc, ct_w_total, bc, ct_b_total, min_share=0.15, mult=2.0, offset=0.05)
            ],
            key=lambda r: r[2],
            reverse=True,
        )
        res["connection_type_mix"] = ct_out[:15]
    if has_ost:
        ost_valid = [r for r in ost_rows if r[0] is not None]
        o_w_total, o_b_total = sum(r[3] for r in ost_valid), sum(r[4] for r in ost_valid)
        o_w_5xx, o_b_5xx = sum(r[5] for r in ost_valid), sum(r[6] for r in ost_valid)
        if (
            o_w_total > 0
            and o_b_total > 0
            and (o_w_5xx * 100.0 / o_w_total) >= 1.0
            and (o_w_5xx * 100.0 / o_w_total) > (o_b_5xx * 100.0 / o_b_total) * 2
        ):
            res["origin_error_rate"] = [
                (r[0], r[1], o_w_total, o_b_total, o_w_5xx, o_b_5xx) for r in ost_valid if int(r[0]) >= 500 and r[1] > 0
            ]
        else:
            res["origin_error_rate"] = []
    if has_oip:
        oip_stats = [
            (oip_val, reqs, round(errs_5xx * 100.0 / reqs, 1))
            for oip_val, reqs, errs_5xx in oip_rows
            if oip_val is not None and oip_val != "" and reqs >= 10
        ]
        if oip_stats:
            rates = sorted(r[2] for r in oip_stats)
            n = len(rates)
            pos = 0.5 * (n - 1)
            lo, hi = int(pos), min(int(pos) + 1, n - 1)
            median_rate = rates[lo] + (pos - lo) * (rates[hi] - rates[lo])
            res["origin_ip_failure"] = sorted(
                [
                    (oip_val, reqs, err_pct, median_rate)
                    for oip_val, reqs, err_pct in oip_stats
                    if err_pct > median_rate * 3 and err_pct > 5
                ],
                key=lambda r: r[2],
                reverse=True,
            )
        else:
            res["origin_ip_failure"] = []

    def _monoculture(rows_in: list[tuple], min_share: float, skip_empty: bool) -> list[tuple]:
        w_tot, b_tot = sum(wc for _, wc, _ in rows_in), sum(bc for _, _, bc in rows_in)
        if w_tot <= 0:
            return []
        out = sorted(
            [
                (val, wc, bc, b_tot, w_tot)
                for val, wc, bc in rows_in
                if (not skip_empty or (val is not None and val != ""))
                and _share_exceeded(wc, w_tot, bc, b_tot, min_share=min_share, mult=3.0, offset=0.10)
            ],
            key=lambda r: r[1],
            reverse=True,
        )
        return out[:10]

    if has_referer:
        res["referer_monoculture"] = _monoculture(referer_rows, 0.20, skip_empty=True)
    if has_ua:
        res["ua_monoculture"] = _monoculture(ua_rows, 0.25, skip_empty=False)
    return res


def _coalesced_geo_latency_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    actual_cols: Collection[str],
    *,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    has_elapsed = "elapsed" in actual_cols
    has_pop, has_sreg = "pop" in actual_cols and has_elapsed, "server_region" in actual_cols and has_elapsed
    has_metro_dr = "metro" in actual_cols and "delivery_rate" in actual_cols
    if not (has_pop or has_sreg or has_metro_dr):
        return {}
    has_ottfb = "ottfb" in actual_cols
    sql = COALESCED_GEO_LATENCY_AGGREGATES.format(
        table_name=table_name,
        pop_sel='"pop"' if has_pop else "NULL::VARCHAR",
        server_region_sel='"server_region"' if has_sreg else "NULL::VARCHAR",
        metro_sel='"metro"' if has_metro_dr else "NULL::INTEGER",
        elapsed_sel=_col_or_null("elapsed", actual_cols, "DOUBLE"),
        ottfb_sel=_col_or_null("ottfb", actual_cols, "DOUBLE"),
        delivery_rate_sel='"delivery_rate"' if has_metro_dr else "NULL::DOUBLE",
    )
    raw_rows = _exec_coalesced(runner, sql, [window_start_s, window_start_s], shadow_cursor, debug_lock)
    pop_out: list[tuple[float, tuple]] = []
    sreg_out: list[tuple[float, tuple]] = []
    metro_out: list[tuple[float, tuple]] = []
    for r in raw_rows:
        gid, pop, server_region, metro, w_p95, b_p95 = r[:6]
        w_lat_total, b_lat_total, w_ottfb_cnt, w_ottfb_p95, w_med_dr, b_med_dr, w_dr_total, b_dr_total = r[6:14]
        gid_i = int(gid or 0)
        if gid_i in (3, 5):
            is_pop = gid_i == 3
            dim_val = pop if is_pop else server_region
            if (is_pop and has_pop or not is_pop and has_sreg) and dim_val is not None and dim_val != "":
                wt, bt = int(w_lat_total or 0), int(b_lat_total or 0)
                if _p95_regressed(wt, bt, w_p95, b_p95, 20, 50, 1.5, 100):
                    ratio = (w_p95 / b_p95) if b_p95 else float("inf")
                    if is_pop:
                        pop_out.append((ratio, (pop, w_p95, b_p95, wt, bt)))
                    else:
                        ottfb_val = w_ottfb_p95 if (has_ottfb and int(w_ottfb_cnt or 0) >= 20) else None
                        sreg_out.append((ratio, (server_region, w_p95, b_p95, wt, bt, ottfb_val)))
        elif gid_i == 6 and has_metro_dr and metro is not None:
            wdt, bdt = int(w_dr_total or 0), int(b_dr_total or 0)
            if (
                wdt >= 20
                and bdt >= 50
                and w_med_dr is not None
                and b_med_dr is not None
                and w_med_dr > 0
                and b_med_dr > 0
                and w_med_dr <= b_med_dr * 0.5
            ):
                metro_out.append((b_med_dr - w_med_dr, (metro, w_med_dr, b_med_dr, wdt, bdt)))
    pop_out.sort(key=lambda pair: pair[0], reverse=True)
    sreg_out.sort(key=lambda pair: pair[0], reverse=True)
    metro_out.sort(key=lambda pair: pair[0], reverse=True)
    res: dict[str, list[tuple]] = {}
    if has_pop:
        res["pop_latency_regression"] = [item for _, item in pop_out[:15]]
    if has_sreg:
        res["region_latency"] = [item for _, item in sreg_out[:15]]
    if has_metro_dr:
        res["metro_delivery_degradation"] = [item for _, item in metro_out[:15]]
    return res


def _build_insights_cache_key(
    source_name: str,
    window_hours: float = 24,
    baseline_hours: float = 168,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    filters: dict | None = None,
) -> str:
    from backend.repositories.utils.response_cache import bucket_time_to_minute

    if clamp_cache_key is not None:
        key_clamp = clamp_cache_key
    elif clamp_start or clamp_end:
        key_clamp = f"{bucket_time_to_minute(clamp_start) or ''}:{bucket_time_to_minute(clamp_end) or ''}"
    else:
        key_clamp = ""
    filter_key_part = ""
    if filters:
        from backend.repositories.utils.filters import filter_spec_attr

        f_items = []
        for k in sorted(filters.keys()):
            spec = filters[k]
            mode = filter_spec_attr(spec, "mode") or "include"
            vals = filter_spec_attr(spec, "values") or []
            if vals:
                f_items.append(f"{k}:{mode}:{','.join(str(v) for v in vals)}")
        if f_items:
            filter_key_part = ":" + "|".join(f_items)
    return f"{source_name}:{window_hours}:{baseline_hours}:{key_clamp}:{int(mask_ips)}{filter_key_part}"


def check_insights_cache(
    src: dict,
    window_hours: float = 24,
    baseline_hours: float = 168,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
    filters: dict | None = None,
) -> dict | None:
    if INSIGHTS_CACHE_TTL <= 0 or force_refresh:
        return None
    cache_key = _build_insights_cache_key(
        src["name"],
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
        clamp_cache_key=clamp_cache_key,
        filters=filters,
    )
    with _insights_cache_lock:
        entry = _insights_cache.get(cache_key)
    if entry is not None:
        cached = entry[1].copy()
        cached["is_cached"] = True
        return cached
    return None


def _compute_clamped_insight_windows(
    window_hours: float, baseline_hours: float, clamp_start: str | None = None, clamp_end: str | None = None
) -> tuple[datetime, str, str, str]:
    from backend.utils.date_utils import parse_iso_utc

    now = datetime.now(UTC)
    if clamp_end:
        ce = parse_iso_utc(clamp_end)
        if ce and ce < now:
            now = ce
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(hours=baseline_hours + window_hours)
    if clamp_start:
        cs = parse_iso_utc(clamp_start)
        if cs:
            baseline_start, window_start = max(baseline_start, cs), max(window_start, cs)
    return now, now.isoformat(), window_start.isoformat(), baseline_start.isoformat()


def get_insights(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    window_hours: float = 24,
    baseline_hours: float = 168,
    service_id: str | None = None,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
    filters: dict | None = None,
    max_workers: int = 8,
) -> dict:
    source_name = src["name"]
    table_name = _safe_table(source_name)
    now, now_s, window_start_s, baseline_start_s = _compute_clamped_insight_windows(
        window_hours, baseline_hours, clamp_start, clamp_end
    )
    cached = check_insights_cache(
        src,
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
        clamp_cache_key=clamp_cache_key,
        force_refresh=force_refresh,
        filters=filters,
    )
    if cached is not None:
        return cached
    cache_key = _build_insights_cache_key(
        source_name,
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
        clamp_cache_key=clamp_cache_key,
        filters=filters,
    )
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()

    def _insights_envelope(insights_list: list[dict], w_total: int = 0) -> dict[str, Any]:
        return build_insights_envelope(
            insights_list,
            window_start=window_start_s,
            window_end=now_s,
            baseline_start=baseline_start_s,
            baseline_end=window_start_s,
            computed_at=now_s,
            window_hours=window_hours,
            baseline_hours=baseline_hours,
            window_total_requests=w_total,
            **runner.telemetry(),
        )

    empty_resp = _insights_envelope([])
    if not actual_cols:
        try:
            from backend.core import storage as db_storage

            db_storage.update_storage_view(con, src, force=True)
            actual_cols = runner.get_schema_cols()
        except Exception:
            logger.debug("get_insights: runner.get_schema_cols failed (ignored)", exc_info=True)
        if not actual_cols:
            return empty_resp
    scratch_alias = f"insights_scratch_{uuid.uuid4().hex[:12]}"
    runner.con.execute(f"ATTACH ':memory:' AS {scratch_alias} (READ_ONLY FALSE)")
    scratch_attached = True
    temp_table = f"{scratch_alias}.insights_temp_{uuid.uuid4().hex[:12]}"
    needed_cols_set: set[str] = {"timestamp"}
    for d in registry.get_all():
        if all(col in actual_cols for col in d.required_fields):
            needed_cols_set.update(d.required_fields)
    fp_support_col = "ja4" if "ja4" in actual_cols else "ja3"
    needed_cols_set.update({fp_support_col, "region", "resp_bytes"})
    cols_sql = ", ".join(f'"{c}"' for c in sorted(needed_cols_set) if c in actual_cols)
    if not cols_sql:
        cols_sql = "*"
    elif "resp_bytes" not in actual_cols:
        cols_sql += ', 0::BIGINT AS "resp_bytes"'
    extra_filter_sql = ""
    if filters:
        from backend.repositories.utils.filters import build_where_clause

        _, f_where = build_where_clause(None, None, filters, actual_cols, inline_params=True)
        if f_where and f_where.strip():
            cleaned = f_where.strip()
            if cleaned.upper().startswith("WHERE "):
                cleaned = cleaned[6:].strip()
            if cleaned and cleaned != "1=1":
                extra_filter_sql = f" AND ({cleaned})"
    create_q = f"CREATE TABLE {temp_table} AS SELECT {cols_sql} FROM {table_name} WHERE timestamp >= CAST('{baseline_start_s}' AS TIMESTAMPTZ) AND timestamp <= CAST('{now_s}' AS TIMESTAMPTZ){extra_filter_sql}"
    temp_created = runner.create_temp_table(create_q, [])
    if not temp_created:
        temp_table = table_name  # Fallback to the lake view directly
    try:
        w_total = 0
        try:
            row_stats = runner.execute(
                f"SELECT min(timestamp), count(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {temp_table}",
                [window_start_s],
            ).fetchone()
            earliest_ts: Any = row_stats[0] if row_stats else None
            w_total = int(row_stats[1] or 0) if row_stats else 0
            if earliest_ts:
                if isinstance(earliest_ts, str):
                    from backend.utils.date_utils import parse_iso_utc

                    earliest_ts = parse_iso_utc(earliest_ts) or earliest_ts
                elif hasattr(earliest_ts, "tzinfo"):
                    earliest_ts = (
                        earliest_ts.replace(tzinfo=UTC) if earliest_ts.tzinfo is None else earliest_ts.astimezone(UTC)
                    )
                available_history_hours = (now - earliest_ts).total_seconds() / 3600.0
            else:
                available_history_hours = 0.0
        except Exception:
            available_history_hours = 0.0
            try:
                w_total = runner.execute(
                    f"SELECT count(*) FROM {temp_table} WHERE timestamp >= CAST(? AS TIMESTAMPTZ)", [window_start_s]
                ).fetchone()[0]
            except Exception:
                w_total = 0
        try:
            from backend.core.field_registry import INSIGHT_DEFINITIONS as _defs

            defs_map = {d["id"]: d for d in _defs}
        except Exception:
            defs_map = {}

        def _build_insight_card(
            insight_id: str,
            title: str,
            description: str,
            category: str | None,
            severity: str,
            summary: str,
            items: list[dict] | None = None,
        ) -> dict:
            return {
                "id": insight_id,
                "title": title,
                "description": description,
                "category": category,
                "severity": severity,
                "summary": summary,
                "items": items if items is not None else [],
            }

        def check_baseline(insight_id: str) -> dict | None:
            if available_history_hours < baseline_hours:
                d = defs_map.get(insight_id, {})
                reg = registry.get(insight_id)
                avail = max(0.1, round(available_history_hours, 1))
                return _build_insight_card(
                    insight_id=insight_id,
                    title=str(d.get("title", insight_id.replace("_", " ").title())),
                    description=str(d.get("description", "")),
                    category=str(reg.category) if reg else None,
                    severity="info",
                    summary=f"Requires {int(baseline_hours)}h of historical data (only {avail}h available)",
                )
            return None

        table_name = temp_table

        def make_investigate_url(filters: dict | None = None) -> str:
            p: list[tuple[str, str]] = ([("service", service_id)] if service_id else []) + [
                ("start", window_start_s),
                ("end", now_s),
            ]
            p.extend((f"filter_{col}", str(val)) for col, val in (filters or {}).items() if val is not None)
            return "/dashboard?" + urllib.parse.urlencode(p)

        tasks: list[Callable[[], dict | None]] = []
        _debug_lock = threading.Lock()
        from backend.repositories.utils.filters import build_geo_select_clause

        loc_cols, label_expr, country_sel, region_sel = build_geo_select_clause(actual_cols)
        ua_mobile_sel = (
            "SUM(CASE WHEN \"ua\" ILIKE '%Mobi%' OR \"ua\" ILIKE '%Android%' OR \"ua\" ILIKE '%iPhone%' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)"
            if "ua" in actual_cols
            else "0"
        )
        url_col = '"url"' if "url" in actual_cols else "NULL"
        q_col = '"url"' if "url" in actual_cols else ('"digest"' if "digest" in actual_cols else "'(unknown)'")
        ua_col = '"ua"' if "ua" in actual_cols else "NULL"
        bot_ua_regex = REPEATED_BOT_UA_REGEX
        _coalesced_specs: list[tuple[str, bool, str, tuple, dict[str, Any]]] = [
            (
                "city",
                {"city", "status", "elapsed", "timestamp"}.issubset(actual_cols),
                "_coalesced_city_aggregates",
                (runner, table_name, window_start_s, label_expr, region_sel, country_sel, window_hours, baseline_hours),
                {},
            ),
            (
                "URL",
                {"url", "status", "cache", "elapsed", "timestamp"}.issubset(actual_cols),
                "_coalesced_url_aggregates",
                (runner, table_name, window_start_s),
                {"actual_cols": actual_cols},
            ),
            (
                "IP-security",
                {"ip", "url", "status", "timestamp"}.issubset(actual_cols),
                "_coalesced_ip_security_aggregates",
                (runner, table_name, window_start_s, window_hours, baseline_hours),
                {"actual_cols": actual_cols},
            ),
            (
                "FP",
                {"timestamp", "ip"}.issubset(actual_cols),
                "_coalesced_fp_aggregates",
                (runner, table_name, window_start_s, window_hours, baseline_hours, actual_cols),
                {},
            ),
            *(
                (lbl, cond, fn_name, (runner, table_name, window_start_s, actual_cols), {})
                for lbl, cond, fn_name in (
                    ("dim", "timestamp" in actual_cols, "_coalesced_dim_aggregates"),
                    ("geo-latency", "timestamp" in actual_cols, "_coalesced_geo_latency_aggregates"),
                    ("ASN", {"asn", "timestamp"}.issubset(actual_cols), "_coalesced_asn_aggregates"),
                    (
                        "scalar",
                        "timestamp" in actual_cols
                        and bool({"transport", "cache", "oconnect_ms", "ottfb"}.intersection(actual_cols)),
                        "_coalesced_scalar_aggregates",
                    ),
                )
            ),
        ]

        def _run_coalesced_spec(spec: tuple[str, bool, str, tuple, dict[str, Any]]) -> dict[str, list[tuple]]:
            label, enabled, fn_name, args, kwargs = spec
            if not enabled:
                return {}
            try:
                return globals()[fn_name](*args, shadow_cursor=runner.con.cursor(), debug_lock=_debug_lock, **kwargs)
            except Exception as e:
                logger.warning("[insights] coalesced %s aggregates failed, falling back: %s", label, e)
                return {}

        max_coalesce_workers = max(1, min(max_workers, 4))
        precomputed: dict[str, list[tuple]] = {}
        with ThreadPoolExecutor(
            max_workers=max_coalesce_workers, thread_name_prefix="insights-coalesce"
        ) as _coalesce_pool:
            for fut in [
                _coalesce_pool.submit(contextvars.copy_context().run, _run_coalesced_spec, spec)
                for spec in _coalesced_specs
            ]:
                precomputed.update(fut.result())
        for definition in registry.get_all():
            if not all(col in actual_cols for col in definition.required_fields):
                continue
            if definition.id == "waf_signal_spikes" and not temp_created:
                continue

            def _make_task(d=definition):
                def compute_insight() -> dict | None:
                    fp_col = "ja4" if "ja4" in actual_cols else "ja3"
                    extra_args = {}
                    if d.id == "impossible_distance":
                        from backend.utils.pop_utils import get_pop_lat_lon_map

                        pop_map = get_pop_lat_lon_map()
                        if not pop_map:
                            return None
                        extra_args["pop_values"] = ", ".join(
                            f"('{code}', {float(lat)}::DOUBLE, {float(lon)}::DOUBLE)"
                            for code, (lat, lon) in pop_map.items()
                            if lat is not None and lon is not None
                        )
                        extra_args["edge_filter"] = 'AND t."edge" = true' if "edge" in actual_cols else ""
                    else:
                        r = check_baseline(d.id)
                        if r:
                            return r
                    default_sql = getattr(_insights_sql, d.id.upper(), None)
                    is_default_template = default_sql is None or d.sql_template == default_sql
                    if is_default_template and d.id in precomputed:
                        rows = precomputed[d.id]
                    else:
                        try:
                            sql = d.sql_template.format(
                                table_name=table_name,
                                waf_table=table_name,
                                window_hours=window_hours,
                                baseline_hours=baseline_hours,
                                fp_col=fp_col,
                                loc_cols=loc_cols,
                                label_expr=label_expr,
                                country_sel=country_sel,
                                region_sel=region_sel,
                                ua_mobile_sel=ua_mobile_sel,
                                url_col=url_col,
                                q_col=q_col,
                                ua_col=ua_col,
                                bot_ua_regex=bot_ua_regex,
                                **extra_args,
                            )
                        except KeyError:
                            return None
                        params = [window_start_s] * sql.count("?")
                        rows = _execute_on_cursor(runner, _debug_lock, runner.con.cursor(), sql, params).fetchall()
                    items = []
                    if d.row_processor:
                        context: dict[str, Any] = {
                            "window_hours": window_hours,
                            "baseline_hours": baseline_hours,
                            "fp_col": fp_col,
                            "actual_cols": actual_cols,
                            "mask_ips": mask_ips,
                        }
                        context.update(
                            build_insight_asn_context(src["name"], rows, d.id, include_dma="metro" in actual_cols)
                        )
                        for row in rows:
                            try:
                                item = d.row_processor(row, d, context)
                                if "investigate_url" not in item:
                                    item["investigate_url"] = make_investigate_url(
                                        item.get("meta", {}).get("filters", {})
                                    )
                                items.append(item)
                            except Exception:
                                continue
                    severity = "clean"
                    if items:
                        severity = (
                            d.severity_logic(items)
                            if d.severity_logic
                            else ("critical" if any(i.get("severity") == "critical" for i in items) else "warning")
                        )
                    if items:
                        if d.id == "error_spikes":
                            summary = f"{len(items)} URLs with elevated server error rates"
                        elif d.id == "botnet_grouping":
                            summary = f"{len(items)} fingerprints with suspicious IP spread"
                        else:
                            summary = f"{len(items)} anomalies detected"
                    else:
                        summary = f"No {d.title.lower()} detected"
                    return _build_insight_card(
                        insight_id=d.id,
                        title=d.title,
                        description=d.description,
                        category=str(d.category),
                        severity=severity,
                        summary=summary,
                        items=items,
                    )

                compute_insight._insight_id = d.id  # type: ignore[attr-defined]
                compute_insight._insight_title = d.title  # type: ignore[attr-defined]
                compute_insight._insight_category = str(d.category)  # type: ignore[attr-defined]
                return compute_insight

            tasks.append(_make_task())
        insights_list: list[dict] = []

        def _safe_run(fn: Callable[[], dict | None]) -> dict | None:
            try:
                return fn()
            except Exception as e:
                insight_id = getattr(fn, "_insight_id", "unknown")
                return _build_insight_card(
                    insight_id=insight_id,
                    title=getattr(fn, "_insight_title", insight_id.replace("_", " ").title()),
                    description="",
                    category=getattr(fn, "_insight_category", None),
                    severity="error",
                    summary=f"Query failed: {str(e)}",
                )

        if tasks:
            pool_workers = max(1, min(max_workers, 3, len(tasks)))
            ctx_tasks = [(contextvars.copy_context(), t) for t in tasks]
            with ThreadPoolExecutor(max_workers=pool_workers, thread_name_prefix="insights") as pool:
                for res in pool.map(lambda pair: pair[0].run(_safe_run, pair[1]), ctx_tasks):
                    if res:
                        insights_list.append(res)
        payload = _insights_envelope(insights_list, w_total)
        if INSIGHTS_CACHE_TTL > 0:
            with _insights_cache_lock:
                _insights_cache[cache_key] = (now_s, payload)
        return payload
    finally:
        if scratch_attached:
            try:
                runner.con.execute(f"DETACH {scratch_alias}")
            except Exception:
                logger.debug("get_insights: runner.con.execute failed (ignored)", exc_info=True)


_BUCKET_INTERVALS = INSIGHT_BUCKET_INTERVALS


def get_cache_collapse_detail(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    url: str,
    window_hours: float,
    baseline_hours: float,
    *,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
) -> dict:
    from backend.core.share_db.validation import mask_ip

    source_name = src["name"]
    table_name = _safe_table(source_name)
    _, now_s, window_start_s, baseline_start_s = _compute_clamped_insight_windows(
        window_hours, baseline_hours, clamp_start, clamp_end
    )
    runner = QueryRunner(con, src)
    rates_sql = f"""
        WITH base AS (
            SELECT cache,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "url" = ? AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
        )
        SELECT
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
            SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_miss,
            SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_pass,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
            SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_miss,
            SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_pass,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base
    """
    params = [window_start_s, window_start_s, url, baseline_start_s, now_s]
    rates_res = runner.execute(rates_sql, params).fetchone() or (0, 0, 0, 0, 0, 0, 0, 0)
    w_hits, w_miss, w_pass, w_total, b_hits, b_miss, b_pass, b_total = (int(v or 0) for v in rates_res)
    w_cacheable, b_cacheable = w_hits + w_miss, b_hits + b_miss
    w_rate, b_rate = (w_hits / w_cacheable) if w_cacheable else 0.0, (b_hits / b_cacheable) if b_cacheable else 0.0
    w_pass_rate, b_pass_rate = (w_pass / w_total) if w_total else 0.0, (b_pass / b_total) if b_total else 0.0
    breakdown = build_cache_collapse_breakdown(w_hits, w_miss, w_pass, w_total)
    total_hours = window_hours + baseline_hours
    bucket_interval = select_insight_bucket_interval(total_hours)
    timeline_sql = f"""
        SELECT
            time_bucket(INTERVAL '{bucket_interval}', timestamp) AS bucket,
            COUNT(*) AS total_requests,
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) AS real_hits,
            SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) AS misses
        FROM {table_name}
        WHERE "url" = ? AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
        GROUP BY bucket
        ORDER BY bucket ASC
    """
    timeline_rows = runner.execute(timeline_sql, [url, baseline_start_s, now_s]).fetchall()
    timeline = [shape_cache_collapse_timeline_point(_iso_or_str(r[0]), r[1], r[2], r[3], b_rate) for r in timeline_rows]
    misses_sql = f"""
        SELECT timestamp, cache, pop, ip, status
        FROM {table_name}
        WHERE "url" = ? AND starts_with(cache, 'MISS') AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
        ORDER BY timestamp DESC
        LIMIT 50
    """
    miss_rows = runner.execute(misses_sql, [url, baseline_start_s, now_s]).fetchall()
    recent_misses = [
        {
            "timestamp": _iso_or_str(mr[0]),
            "cache": mr[1] or "UNKNOWN",
            "pop": mr[2],
            "ip": mask_ip(mr[3]) if mask_ips and mr[3] else mr[3],
            "status": int(mr[4]) if mr[4] is not None else None,
        }
        for mr in miss_rows
    ]
    return {
        "url": url,
        "timeline": timeline,
        "recent_misses": recent_misses,
        "breakdown": breakdown,
        "baseline_hit_rate": float(b_rate) * 100,
        "window_hit_rate": float(w_rate) * 100,
        "baseline_pass_rate": float(b_pass_rate) * 100,
        "window_pass_rate": float(w_pass_rate) * 100,
        **runner.telemetry(),
    }


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("insights._insights_cache", _insights_cache)
