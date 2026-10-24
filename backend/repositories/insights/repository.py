"""Insights repository — anomaly detection queries, no HTTP imports."""

from __future__ import annotations

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

from backend.repositories._base import QueryRunner, _compact_sql_for_debug, _safe_table
from backend.repositories._sql import insights as _insights_sql
from backend.repositories._sql.insights import (
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
from backend.repositories._sql.insights import (
    COALESCED_SCALAR_AGGREGATES as COALESCED_SCALAR_AGGREGATES,
)
from backend.utils.telemetry import record_telemetry_query

from .registry import registry

logger = logging.getLogger(__name__)

# ── Caches ────────────────────────────────────────────────────────────────────

INSIGHTS_CACHE_TTL = 300  # seconds
# Bounded + lazy-reaped. Pre-migration this was a plain dict; entries
# were time-bucketed by ``int(time.time() / TTL)`` so each TTL window
# minted distinct keys but old buckets were never removed. Across hours
# of admin use the bucket-count grew linearly. 500 entries × insights
# payload (~100KB) caps this around ~50MB.
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
    """Execute ``sql`` on ``cur`` — a ``runner.con.cursor()`` shadow connection —
    and append its timing to the REQUEST-scoped ``runner.debug_queries`` under
    ``debug_lock``.

    Shared by the pre-per-insight coalesced-aggregate parallel dispatch (city/
    url/ip-security/WAF-unnest, all launched together via a ThreadPoolExecutor
    in ``get_insights``) and the later per-insight ThreadPoolExecutor dispatch.
    Cursors returned by ``con.cursor()`` are separate shadow connections on the
    same parent — thread-safe to call concurrently from different threads,
    unlike calling ``runner.execute`` (which serialises on the single parent
    cursor). ``debug_lock`` is the caller's request-scoped lock, NOT a module-
    global one, so concurrent unrelated requests never contend on each other's
    append.
    """
    t0 = time.time()
    res = cur.execute(sql, params or [])
    elapsed_ms = round((time.time() - t0) * 1000, 2)
    compact_sql = _compact_sql_for_debug(sql)
    record_telemetry_query("duckdb", compact_sql, elapsed_ms)
    with debug_lock:
        runner.debug_queries.append({"sql": compact_sql, "time_ms": elapsed_ms, "engine": "DuckDB"})
    return res


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
    """Run ONE pass over `table_name` to compute every aggregate the four
    city-based insights need, then demux into per-insight result lists
    whose row schemas match each insight's existing row_processor contract.

    The four insights — city_surges, city_error_spikes,
    city_latency_regressions, new_city_traffic — all GROUP BY
    (city, region, country) over the same WHERE clause
    (``"city" IS NOT NULL AND "city" != ''``). Pre-coalesce, they ran as
    four independent SELECTs and re-read the temp table four times. This
    coalesces them into a single SELECT that computes the superset of
    counts/rates/p95s, then applies each insight's HAVING/ORDER/LIMIT in
    Python.

    Returns ``{insight_id: rows}`` where each rows list matches the per-
    insight schema the existing processor expects:

    - city_surges:              [label, city, region, country, w_cnt, b_cnt, spike_ratio]
    - city_error_spikes:        [label, city, region, country, w_rate, b_rate, w_errors, w_total, b_total]
    - city_latency_regressions: [label, city, region, country, w_p95, b_p95, w_total, b_total]
    - new_city_traffic:         [label, city, region, country, w_cnt, b_cnt]
    """
    sql = COALESCED_CITY_AGGREGATES.format(
        table_name=table_name,
        label_expr=label_expr,
        region_sel=region_sel,
        country_sel=country_sel,
    )
    if shadow_cursor is not None and debug_lock is not None:
        rows = _execute_on_cursor(runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s]).fetchall()
    else:
        rows = runner.execute(sql, [window_start_s, window_start_s]).fetchall()

    surges: list[tuple] = []
    error_spikes: list[tuple] = []
    latency: list[tuple] = []
    new_city: list[tuple] = []

    baseline_scale = max(baseline_hours, 1.0)

    for r in rows:
        (
            label,
            city,
            region,
            country,
            w_cnt,
            b_cnt,
            w_err,
            b_err,
            w_p95,
            b_p95,
            w_lat_total,
            b_lat_total,
        ) = r
        b_cnt_i = b_cnt or 0
        b_err_i = b_err or 0

        # city_surges — HAVING w_cnt >= 20 AND w_cnt > b_cnt/baseline_hours*window_hours*3
        if w_cnt >= 20:
            b_normalized = b_cnt_i * 1.0 / baseline_scale * window_hours
            if w_cnt > b_normalized * 3:
                spike_ratio = w_cnt * 1.0 / max(b_normalized, 1.0)
                surges.append((label, city, region, country, w_cnt, b_cnt, spike_ratio))

        # city_error_spikes — w_total/b_total here are total reqs in window/baseline
        # HAVING w_total >= 10 AND w_rate >= 0.10 AND (b_total < 50 OR w_rate >= b_rate*3 + 0.05)
        if w_cnt >= 10:
            w_rate = (w_err / w_cnt) if w_cnt else 0.0
            b_rate = (b_err_i / b_cnt_i) if b_cnt_i else None
            if w_rate >= 0.10 and (b_cnt_i < 50 or (b_rate is not None and w_rate >= b_rate * 3 + 0.05)):
                error_spikes.append((label, city, region, country, w_rate, b_rate, w_err, w_cnt, b_cnt))

        # city_latency_regressions — uses elapsed-only counts (w_lat_total / b_lat_total)
        # HAVING w_total >= 10 AND b_total >= 50 AND w_p95 >= b_p95*3.0 AND w_p95 - b_p95 >= 500
        if (
            w_lat_total >= 10
            and b_lat_total >= 50
            and w_p95 is not None
            and b_p95 is not None
            and w_p95 >= b_p95 * 3.0
            and w_p95 - b_p95 >= 500
        ):
            latency.append((label, city, region, country, w_p95, b_p95, w_lat_total, b_lat_total))

        # new_city_traffic — HAVING w_cnt >= 5 AND b_cnt = 0
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
    """Coalesce URL-keyed insights into ONE pass over ``table_name``.

    When ``actual_cols`` is None, computes the 5 core URL insights
    (error_spikes, cache_collapse, cacheability_regression,
    latency_regression, tail_latency) via COALESCED_URL_AGGREGATES.
    When ``actual_cols`` is provided, uses COALESCED_URL_EXTENDED_AGGREGATES
    to also compute new_probe_urls, payload_compression_regression,
    image_optimization_opportunities, cache_ttl_mismatch, and origin_retries
    in the same single scan.
    """
    use_extended = actual_cols is not None
    if actual_cols is not None:
        sql = COALESCED_URL_EXTENDED_AGGREGATES.format(
            table_name=table_name,
            resp_bytes_sel='t."resp_bytes"' if "resp_bytes" in actual_cols else "NULL::BIGINT",
            uncompressed_sel=(
                "(NOT regexp_matches(lower(COALESCE(t.\"resp_header_content_encoding\", '')), 'gzip|br|zstd|deflate'))"
                if "resp_header_content_encoding" in actual_cols
                else "FALSE"
            ),
            is_mobile_sel=(
                "CASE WHEN t.\"ua\" ILIKE '%Mobi%' OR t.\"ua\" ILIKE '%Android%' OR t.\"ua\" ILIKE '%iPhone%' THEN 1 ELSE 0 END"
                if "ua" in actual_cols
                else "0"
            ),
            ttl_sel='t."ttl"' if "ttl" in actual_cols else "NULL::DOUBLE",
            hits_sel='t."hits"' if "hits" in actual_cols else "NULL::DOUBLE",
            age_sel='t."age"' if "age" in actual_cols else "NULL::DOUBLE",
            oretries_sel='t."oretries"' if "oretries" in actual_cols else "NULL::BIGINT",
        )
    else:
        sql = COALESCED_URL_AGGREGATES.format(table_name=table_name)
    if shadow_cursor is not None and debug_lock is not None:
        result_cursor = _execute_on_cursor(runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s])
    else:
        result_cursor = runner.execute(sql, [window_start_s, window_start_s])

    _TOP_K = 15
    error_spikes_heap: list[tuple] = []
    cache_collapse_heap: list[tuple] = []
    cacheability_regression_heap: list[tuple] = []
    latency_regression_heap: list[tuple] = []
    tail_latency_heap: list[tuple] = []
    new_probe_urls_heap: list[tuple] = []
    payload_comp_heap: list[tuple] = []
    image_opt_heap: list[tuple] = []
    cache_ttl_heap: list[tuple] = []
    origin_retries_heap: list[tuple] = []

    def _push_top_k(heap: list[tuple], score: float, counter: int, item: tuple, k: int = _TOP_K) -> None:
        if len(heap) < k:
            heapq.heappush(heap, (score, counter, item))
        else:
            heapq.heappushpop(heap, (score, counter, item))

    has_pcr = bool(
        use_extended
        and actual_cols is not None
        and "resp_header_content_encoding" in actual_cols
        and "resp_bytes" in actual_cols
    )
    has_img = bool(use_extended and actual_cols is not None and "resp_bytes" in actual_cols and "ua" in actual_cols)
    has_ttl = bool(
        use_extended
        and actual_cols is not None
        and "ttl" in actual_cols
        and "hits" in actual_cols
        and "age" in actual_cols
    )
    has_oret = bool(use_extended and actual_cols is not None and "oretries" in actual_cols)

    counter = 0

    while True:
        rows = result_cursor.fetchmany(10000)
        if not rows:
            break
        for r in rows:
            if use_extended:
                (
                    url,
                    w_total,
                    b_total,
                    w_5xx,
                    b_5xx,
                    w_hits,
                    b_hits,
                    w_miss,
                    b_miss,
                    w_pass,
                    b_pass,
                    w_lat_total,
                    b_lat_total,
                    w_p95,
                    b_p95,
                    w_p99,
                    w_p50,
                    is_probe,
                    w_4xx,
                    is_comp,
                    pcr_w_uncomp,
                    pcr_w_total,
                    pcr_b_uncomp,
                    pcr_b_total,
                    is_img,
                    img_w_cnt,
                    img_w_bytes,
                    img_w_avg_bytes,
                    img_w_mobile,
                    ttl_w_cnt,
                    ttl_w_avg_ttl,
                    ttl_w_avg_hits,
                    ttl_w_avg_age,
                    oret_w_cnt,
                    oret_w_avg,
                    oret_w_max,
                ) = r
            else:
                (
                    url,
                    w_total,
                    b_total,
                    w_5xx,
                    b_5xx,
                    w_hits,
                    b_hits,
                    w_miss,
                    b_miss,
                    w_pass,
                    b_pass,
                    w_lat_total,
                    b_lat_total,
                    w_p95,
                    b_p95,
                    w_p99,
                    w_p50,
                ) = r

            w_total_i = w_total or 0
            b_total_i = b_total or 0
            counter += 1

            # ── error_spikes ──────────────────────────────────────────────────
            if w_total_i >= 3:
                w_rate_e = (w_5xx or 0) / w_total_i if w_total_i else 0.0
                b_rate_e = ((b_5xx or 0) / b_total_i) if b_total_i else None
                if w_rate_e >= 0.05 and (b_total_i < 10 or (b_rate_e is not None and w_rate_e >= b_rate_e * 2 + 0.05)):
                    item = (url, w_rate_e, b_rate_e, w_5xx, w_total, b_total)
                    score = (w_rate_e or 0) - (b_rate_e or 0)
                    _push_top_k(error_spikes_heap, score, counter, item)

            # ── cache_collapse ────────────────────────────────────────────────
            w_cacheable = (w_hits or 0) + (w_miss or 0)
            b_cacheable = (b_hits or 0) + (b_miss or 0)
            if w_cacheable >= 5 and b_cacheable >= 20:
                w_rate_c = (w_hits or 0) / w_cacheable if w_cacheable else 0.0
                b_rate_c = (b_hits or 0) / b_cacheable if b_cacheable else 0.0
                if b_rate_c >= 0.40 and w_rate_c <= b_rate_c - 0.20 and w_rate_c <= b_rate_c * 0.6:
                    item_cc: tuple = (url, w_rate_c, b_rate_c, w_cacheable, b_cacheable)
                    score = (b_rate_c or 0) - (w_rate_c or 0)
                    _push_top_k(cache_collapse_heap, score, counter, item_cc)

            # ── cacheability_regression ───────────────────────────────────────
            if w_total_i >= 10 and b_total_i >= 50:
                w_pass_rate = (w_pass or 0) / w_total_i if w_total_i else 0.0
                b_pass_rate = (b_pass or 0) / b_total_i if b_total_i else 0.0
                if b_pass_rate <= 0.20 and w_pass_rate >= 0.50 and w_pass_rate >= b_pass_rate + 0.30:
                    item_cr: tuple = (url, w_pass_rate, b_pass_rate, w_total, b_total)
                    score = (w_pass_rate or 0) - (b_pass_rate or 0)
                    _push_top_k(cacheability_regression_heap, score, counter, item_cr)

            # ── latency_regression ────────────────────────────────────────────
            if (
                w_total_i >= 5
                and b_total_i >= 20
                and w_p95 is not None
                and b_p95 is not None
                and w_p95 >= b_p95 * 2.0
                and w_p95 - b_p95 >= 200
            ):
                item_lr: tuple = (url, w_p95, b_p95, w_total, b_total)
                score = (w_p95 / b_p95) if b_p95 else 0.0
                _push_top_k(latency_regression_heap, score, counter, item_lr)

            # ── tail_latency (window-only) ────────────────────────────────────
            if w_lat_total is not None and w_lat_total >= 20 and w_p99 is not None and w_p50 is not None and w_p50 > 0:
                ratio = round(w_p99 / w_p50, 1)
                if ratio > 5:
                    item_tl: tuple = (url, w_p99, w_p50, ratio, w_lat_total)
                    _push_top_k(tail_latency_heap, ratio or 0.0, counter, item_tl)

            if use_extended:
                # ── new_probe_urls ────────────────────────────────────────────
                if is_probe and w_total_i > 0 and b_total_i == 0:
                    w_error_pct = (w_4xx or 0) * 100.0 / w_total_i
                    item_npu: tuple = (url, w_total_i, b_total_i, w_error_pct)
                    _push_top_k(new_probe_urls_heap, float(w_total_i), counter, item_npu, k=25)

                # ── payload_compression_regression ────────────────────────────
                if has_pcr and is_comp and (pcr_w_total or 0) >= 10 and (pcr_b_total or 0) >= 50:
                    w_rate_p = (pcr_w_uncomp or 0) / pcr_w_total
                    b_rate_p = (pcr_b_uncomp or 0) / pcr_b_total
                    if b_rate_p <= 0.20 and w_rate_p >= 0.50 and w_rate_p >= b_rate_p + 0.30:
                        item_pcr: tuple = (url, w_rate_p, b_rate_p, pcr_w_total, pcr_b_total)
                        _push_top_k(payload_comp_heap, w_rate_p - b_rate_p, counter, item_pcr, k=15)

                # ── image_optimization_opportunities ──────────────────────────
                if has_img and is_img and (img_w_bytes or 0) > 524288:
                    avg_kb = round((img_w_avg_bytes or 0.0) / 1024.0, 1)
                    mobile_ratio = ((img_w_mobile or 0) * 1.0 / img_w_cnt) if img_w_cnt else 0.0
                    item_img: tuple = (url, img_w_cnt, img_w_bytes, avg_kb, mobile_ratio)
                    _push_top_k(image_opt_heap, float(img_w_bytes), counter, item_img, k=15)

                # ── cache_ttl_mismatch ────────────────────────────────────────
                if (
                    has_ttl
                    and (ttl_w_cnt or 0) >= 10
                    and ttl_w_avg_hits is not None
                    and ttl_w_avg_hits < 2
                    and ttl_w_avg_ttl is not None
                    and ttl_w_avg_ttl > 60
                ):
                    item_ttl: tuple = (
                        url,
                        round(ttl_w_avg_ttl, 0),
                        round(ttl_w_avg_hits, 1),
                        round(ttl_w_avg_age or 0.0, 0),
                        ttl_w_cnt,
                    )
                    _push_top_k(cache_ttl_heap, float(ttl_w_avg_ttl), counter, item_ttl, k=20)

                # ── origin_retries ────────────────────────────────────────────
                if has_oret and (oret_w_cnt or 0) >= 5 and oret_w_avg is not None:
                    avg_ret = round(oret_w_avg, 2)
                    item_oret: tuple = (url, oret_w_cnt, avg_ret, oret_w_max)
                    _push_top_k(origin_retries_heap, float(avg_ret), counter, item_oret, k=10)

    def _heap_to_sorted_items(heap: list[tuple]) -> list[tuple]:
        # heap is (score, counter, item); return items sorted by score desc.
        return [entry[2] for entry in sorted(heap, key=lambda e: e[0], reverse=True)]

    res: dict[str, list[tuple]] = {
        "error_spikes": _heap_to_sorted_items(error_spikes_heap),
        "cache_collapse": _heap_to_sorted_items(cache_collapse_heap),
        "cacheability_regression": _heap_to_sorted_items(cacheability_regression_heap),
        "latency_regression": _heap_to_sorted_items(latency_regression_heap),
        "tail_latency": _heap_to_sorted_items(tail_latency_heap),
    }
    if use_extended:
        res["new_probe_urls"] = _heap_to_sorted_items(new_probe_urls_heap)
        if has_pcr:
            res["payload_compression_regression"] = _heap_to_sorted_items(payload_comp_heap)
        if has_img:
            res["image_optimization_opportunities"] = _heap_to_sorted_items(image_opt_heap)
        if has_ttl:
            res["cache_ttl_mismatch"] = _heap_to_sorted_items(cache_ttl_heap)
        if has_oret:
            res["origin_retries"] = _heap_to_sorted_items(origin_retries_heap)
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
    """Coalesce the IP-keyed security scans into ONE ``GROUP BY ip`` pass
    over ``table_name``.

    When ``actual_cols`` is None, computes low_and_slow, credential_enumeration,
    and content_discovery via COALESCED_IP_SECURITY_AGGREGATES.
    When ``actual_cols`` is provided, uses COALESCED_IP_EXTENDED_AGGREGATES
    to also compute orphaned_deep_crawl, session_harvesting, and exact
    candidate bounds for request_size_anomaly and connection_abuse.
    """
    use_extended = actual_cols is not None
    if actual_cols is not None:
        sql = COALESCED_IP_EXTENDED_AGGREGATES.format(
            table_name=table_name,
            no_referer_sel=(
                '(t."referer" IS NULL OR t."referer" = \'\' OR t."referer" = \'-\')'
                if "referer" in actual_cols
                else "FALSE"
            ),
            cs_sel='t."cookie_session"' if "cookie_session" in actual_cols else "NULL::VARCHAR",
            req_header_bytes_sel='t."req_header_bytes"' if "req_header_bytes" in actual_cols else "NULL::BIGINT",
            conn_requests_sel='t."conn_requests"' if "conn_requests" in actual_cols else "NULL::BIGINT",
        )
    else:
        sql = COALESCED_IP_SECURITY_AGGREGATES.format(table_name=table_name)
    if shadow_cursor is not None and debug_lock is not None:
        result_cursor = _execute_on_cursor(runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s])
    else:
        result_cursor = runner.execute(sql, [window_start_s, window_start_s])

    _TOP_K = 15
    low_and_slow_heap: list[tuple] = []
    credential_enumeration_heap: list[tuple] = []
    content_discovery_heap: list[tuple] = []
    orphaned_deep_crawl_heap: list[tuple] = []
    session_harvesting_heap: list[tuple] = []

    def _push_top_k(heap: list[tuple], sortkey: tuple, counter: int, item: tuple) -> None:
        if len(heap) < _TOP_K:
            heapq.heappush(heap, (sortkey, counter, item))
        else:
            heapq.heappushpop(heap, (sortkey, counter, item))

    has_odc = bool(use_extended and actual_cols is not None and "referer" in actual_cols)
    has_sh = bool(use_extended and actual_cols is not None and "cookie_session" in actual_cols)
    has_rsa = bool(use_extended and actual_cols is not None and "req_header_bytes" in actual_cols)
    has_ca = bool(use_extended and actual_cols is not None and "conn_requests" in actual_cols)
    rsa_candidates = 0
    ca_candidates = 0

    baseline_scale = max(baseline_hours, 1.0)
    counter = 0

    while True:
        rows = result_cursor.fetchmany(10000)
        if not rows:
            break
        for r in rows:
            if use_extended:
                (
                    ip,
                    ls_hits,
                    ls_distinct,
                    ls_min_sec,
                    ls_max_sec,
                    ce_w_denied,
                    ce_w_attempts,
                    ce_w_paths,
                    ce_b_denied,
                    cd_w_404,
                    cd_w_total,
                    cd_distinct_404,
                    cd_b_404,
                    odc_w_reqs,
                    odc_w_urls,
                    odc_b_reqs,
                    sh_w_sessions,
                    sh_w_reqs,
                    sh_b_sessions,
                    rsa_w_cnt,
                    rsa_w_max,
                    rsa_b_min,
                    _rsa_b_max,
                    ca_w_cnt,
                    ca_w_max,
                    ca_b_min,
                    _ca_b_max,
                ) = r
            else:
                (
                    ip,
                    ls_hits,
                    ls_distinct,
                    ls_min_sec,
                    ls_max_sec,
                    ce_w_denied,
                    ce_w_attempts,
                    ce_w_paths,
                    ce_b_denied,
                    cd_w_404,
                    cd_w_total,
                    cd_distinct_404,
                    cd_b_404,
                ) = r
            counter += 1

            # ── low_and_slow ──────────────────────────────────────────────────
            ls_hits_i = ls_hits or 0
            ls_distinct_i = ls_distinct or 0
            span_s = (ls_max_sec - ls_min_sec) if (ls_min_sec is not None and ls_max_sec is not None) else 0
            if ls_hits_i >= 5 and ls_distinct_i >= 3 and span_s >= 600 and (ls_hits_i / span_s) < 0.2:
                rps = round(ls_hits_i / span_s, 5)
                item_ls: tuple = (ip, ls_hits_i, ls_distinct_i, span_s, rps)
                _push_top_k(low_and_slow_heap, (ls_distinct_i, span_s), counter, item_ls)

            # ── credential_enumeration ────────────────────────────────────────
            ce_w_denied_i = ce_w_denied or 0
            if ce_w_denied_i >= 20:
                ce_ratio = (ce_w_denied_i / ce_w_attempts) if ce_w_attempts else 0.0
                b_norm = (ce_b_denied or 0) / baseline_scale * window_hours * 3 + 5
                if ce_ratio >= 0.5 and ce_w_denied_i > b_norm:
                    item_ce: tuple = (ip, ce_w_denied_i, ce_w_attempts, ce_w_paths, ce_b_denied)
                    _push_top_k(credential_enumeration_heap, (ce_w_denied_i,), counter, item_ce)

            # ── content_discovery ─────────────────────────────────────────────
            cd_w_404_i = cd_w_404 or 0
            cd_distinct_404_i = cd_distinct_404 or 0
            if cd_w_404_i >= 20 and cd_distinct_404_i >= 15:
                cd_ratio = (cd_w_404_i / cd_w_total) if cd_w_total else 0.0
                if cd_ratio >= 0.7:
                    item_cd: tuple = (ip, cd_w_404_i, cd_w_total, cd_distinct_404_i, cd_b_404)
                    _push_top_k(content_discovery_heap, (cd_w_404_i,), counter, item_cd)

            if use_extended:
                # ── orphaned_deep_crawl ───────────────────────────────────────
                if has_odc and (odc_w_reqs or 0) >= 50 and (odc_w_urls or 0) >= 10:
                    b_reqs_i = odc_b_reqs or 0
                    reqs_per_url = odc_w_reqs * 1.0 / max(odc_w_urls, 1.0)
                    surge_ratio = odc_w_reqs * 1.0 / max(b_reqs_i / baseline_scale * window_hours, 1.0)
                    item_odc: tuple = (ip, odc_w_reqs, odc_w_urls, b_reqs_i, reqs_per_url, surge_ratio)
                    _push_top_k(orphaned_deep_crawl_heap, (odc_w_reqs,), counter, item_odc)

                # ── session_harvesting ────────────────────────────────────────
                if has_sh and (sh_w_sessions or 0) >= 20:
                    b_sess_i = sh_b_sessions or 0
                    if sh_w_sessions > b_sess_i * 2 + 10:
                        item_sh: tuple = (ip, sh_w_sessions, sh_w_reqs, b_sess_i)
                        _push_top_k(session_harvesting_heap, (sh_w_sessions,), counter, item_sh)

                # ── request_size_anomaly exact lower-bound check ──────────────
                # Since min(B) <= P95(B), max_w > P95(B)*3 requires max_w > min(B)*3.
                if has_rsa and (rsa_w_cnt or 0) >= 3 and rsa_b_min is not None and (rsa_w_max or 0) > rsa_b_min * 3:
                    rsa_candidates += 1

                # ── connection_abuse exact lower-bound check ──────────────────
                if (
                    has_ca
                    and (ca_w_cnt or 0) >= 5
                    and (ca_w_max or 0) >= 50
                    and ca_b_min is not None
                    and (ca_w_max or 0) > ca_b_min * 3
                ):
                    ca_candidates += 1

    def _heap_to_sorted_items(heap: list[tuple]) -> list[tuple]:
        # heap is (sortkey, counter, item); return items sorted by sortkey desc.
        return [entry[2] for entry in sorted(heap, key=lambda e: e[0], reverse=True)]

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
    """Coalesce the 4 ASN-keyed insights (asn_concentration, new_asn_traffic,
    network_asn_health, asn_hosting_shift) into ONE ``GROUP BY asn`` pass over
    ``table_name``.
    """
    has_health = "ploss" in actual_cols and "rtt_var" in actual_cols and "retrans" in actual_cols
    has_ptype = "p_type" in actual_cols
    sql = COALESCED_ASN_AGGREGATES.format(
        table_name=table_name,
        ploss_sel='"ploss"' if "ploss" in actual_cols else "NULL::DOUBLE",
        rtt_var_sel='"rtt_var"' if "rtt_var" in actual_cols else "NULL::DOUBLE",
        retrans_sel='"retrans"' if "retrans" in actual_cols else "NULL::DOUBLE",
        p_type_sel='"p_type"' if "p_type" in actual_cols else "NULL::VARCHAR",
    )
    if shadow_cursor is not None and debug_lock is not None:
        rows = _execute_on_cursor(runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s]).fetchall()
    else:
        rows = runner.execute(sql, [window_start_s, window_start_s]).fetchall()

    w_total_all = sum(int(r[1] or 0) for r in rows)
    b_total_all = sum(int(r[2] or 0) for r in rows)

    concentration: list[tuple] = []
    new_asn: list[tuple] = []
    health: list[tuple] = []
    hosting_shift: list[tuple] = []

    for r in rows:
        (
            asn,
            w_cnt,
            b_cnt,
            w_ploss,
            b_ploss,
            w_jitter,
            b_jitter,
            w_retrans,
            b_retrans,
            w_hosting,
            w_pt_total,
            b_hosting,
            b_pt_total,
        ) = r
        if asn is None:
            continue

        w_cnt_i = int(w_cnt or 0)
        b_cnt_i = int(b_cnt or 0)

        # asn_concentration
        if w_total_all > 0:
            w_share = w_cnt_i / w_total_all
            if w_share >= 0.20 and (b_total_all == 0 or w_share >= (b_cnt_i / b_total_all) * 3 + 0.10):
                concentration.append((asn, w_cnt_i, b_cnt_i, b_total_all, w_total_all))

        # new_asn_traffic
        if w_cnt_i >= 20 and b_cnt_i == 0:
            new_asn.append((asn, w_cnt_i, b_cnt_i))

        # network_asn_health
        if has_health and w_cnt_i >= 50 and b_cnt_i >= 100:
            w_ploss_f = float(w_ploss or 0.0)
            b_ploss_f = float(b_ploss or 0.0)
            w_retrans_f = float(w_retrans or 0.0)
            b_retrans_f = float(b_retrans or 0.0)
            ploss_bad = w_ploss_f >= 0.02 and w_ploss_f >= b_ploss_f * 2 + 0.005
            jitter_bad = (
                w_jitter is not None
                and b_jitter is not None
                and w_jitter >= b_jitter * 2
                and (w_jitter - b_jitter) >= 20000
            )
            retrans_bad = w_retrans_f >= b_retrans_f * 2 + 0.5
            if ploss_bad or jitter_bad or retrans_bad:
                health.append(
                    (
                        asn,
                        w_ploss,
                        b_ploss,
                        w_jitter,
                        b_jitter,
                        w_retrans,
                        b_retrans,
                        w_cnt_i,
                        b_cnt_i,
                    )
                )

        # asn_hosting_shift
        if has_ptype:
            w_pt_i = int(w_pt_total or 0)
            b_pt_i = int(b_pt_total or 0)
            w_host_i = int(w_hosting or 0)
            b_host_i = int(b_hosting or 0)
            if w_pt_i >= 50:
                w_hr = w_host_i / w_pt_i
                if w_hr >= 0.3 and (b_pt_i == 0 or w_hr >= (b_host_i / b_pt_i) * 3 + 0.10):
                    hosting_shift.append((asn, w_host_i, w_pt_i, b_host_i, b_pt_i))

    concentration.sort(key=lambda x: -(x[1] or 0))
    new_asn.sort(key=lambda x: -(x[1] or 0))
    health.sort(
        key=lambda x: (
            -(x[1] or 0.0),
            -(x[3] if x[3] is not None else float("-inf")),
        )
    )
    hosting_shift.sort(key=lambda x: -(x[1] or 0))

    res: dict[str, list[tuple]] = {
        "asn_concentration": concentration[:10],
        "new_asn_traffic": new_asn[:20],
    }
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
    """Coalesce the ungrouped service-wide scalar insights (http3_fallback,
    cache_hit_cliff, timeout_split, and origin_latency_spike overall gate)
    into ONE pass over ``table_name``.
    """
    has_transport = "transport" in actual_cols
    has_cache = "cache" in actual_cols
    has_oconnect = "oconnect_ms" in actual_cols
    has_ottfb = "ottfb" in actual_cols
    has_ts = has_oconnect and has_ottfb
    if not (has_transport or has_cache or has_ts or has_ottfb):
        return {}
    sql = COALESCED_SCALAR_EXTENDED_AGGREGATES.format(
        table_name=table_name,
        transport_sel='"transport"' if has_transport else "NULL::VARCHAR",
        cache_sel='"cache"' if has_cache else "NULL::VARCHAR",
        oconnect_ms_sel='"oconnect_ms"' if has_oconnect else "NULL::DOUBLE",
        ottfb_sel='"ottfb"' if has_ottfb else "NULL::DOUBLE",
    )
    if shadow_cursor is not None and debug_lock is not None:
        row = _execute_on_cursor(runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s]).fetchone()
    else:
        row = runner.execute(sql, [window_start_s, window_start_s]).fetchone()

    res: dict[str, list[tuple]] = {}
    if not row:
        if has_transport:
            res["http3_fallback"] = []
        if has_cache:
            res["cache_hit_cliff"] = []
        if has_ts:
            res["timeout_split"] = []
        if has_ottfb:
            res["origin_latency_spike"] = []
        return res

    (
        w_quic,
        w_tr_total,
        b_quic,
        b_tr_total,
        w_hits,
        w_cacheable,
        b_hits,
        b_cacheable,
        ts_w_conn,
        ts_b_conn,
        ts_w_read,
        ts_b_read,
        ts_w_total,
        ts_b_total,
        ols_w_p95,
        ols_b_p95,
    ) = row
    w_quic = int(w_quic or 0)
    w_tr_total = int(w_tr_total or 0)
    b_quic = int(b_quic or 0)
    b_tr_total = int(b_tr_total or 0)
    w_hits = int(w_hits or 0)
    w_cacheable = int(w_cacheable or 0)
    b_hits = int(b_hits or 0)
    b_cacheable = int(b_cacheable or 0)
    ts_w_total = int(ts_w_total or 0)
    ts_b_total = int(ts_b_total or 0)

    if has_transport:
        if (
            w_tr_total >= 100
            and b_tr_total >= 100
            and (b_quic / b_tr_total) >= 0.30
            and (w_quic / w_tr_total) <= (b_quic / b_tr_total) - 0.20
        ):
            res["http3_fallback"] = [(w_quic, w_tr_total, b_quic, b_tr_total)]
        else:
            res["http3_fallback"] = []

    if has_cache:
        if (
            w_cacheable >= 100
            and b_cacheable >= 200
            and (b_hits / b_cacheable) >= 0.40
            and (w_hits / w_cacheable) <= (b_hits / b_cacheable) - 0.15
        ):
            res["cache_hit_cliff"] = [(w_hits, w_cacheable, b_hits, b_cacheable)]
        else:
            res["cache_hit_cliff"] = []

    if has_ts:
        if (
            ts_w_total >= 50
            and ts_b_total >= 100
            and (
                (ts_w_conn is not None and ts_b_conn is not None and ts_w_conn > ts_b_conn * 2 and ts_w_conn >= 50)
                or (ts_w_read is not None and ts_b_read is not None and ts_w_read > ts_b_read * 2 and ts_w_read >= 100)
            )
        ):
            res["timeout_split"] = [(ts_w_conn, ts_b_conn, ts_w_read, ts_b_read, ts_w_total, ts_b_total)]
        else:
            res["timeout_split"] = []

    if has_ottfb:
        # Gate origin_latency_spike: only run the per-URL query when overall w_p95 > b_p95 * 2
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
    """Coalesce fingerprint & cipher spread insights (botnet_grouping,
    residential_fingerprint_dispersion, cipher_spread) into ONE pass using
    GROUPING SETS ((fp), (cipher)).
    """
    has_ip = "ip" in actual_cols
    has_fp = has_ip and ("ja4" in actual_cols or "ja3" in actual_cols)
    has_rfd = has_ip and "ja4" in actual_cols and "asn" in actual_cols and "p_type" in actual_cols
    has_cipher = has_ip and "tls_ciphers_sha" in actual_cols
    if not (has_fp or has_rfd or has_cipher):
        return {}
    fp_col = '"ja4"' if "ja4" in actual_cols else ('"ja3"' if "ja3" in actual_cols else "NULL::VARCHAR")
    sql = COALESCED_FP_AGGREGATES.format(
        table_name=table_name,
        fp_sel=fp_col if (has_fp or has_rfd) else "NULL::VARCHAR",
        cipher_sel='"tls_ciphers_sha"' if has_cipher else "NULL::VARCHAR",
        ip_sel='"ip"',
        asn_sel='"asn"' if "asn" in actual_cols else "NULL::BIGINT",
        is_non_hosting_sel='("p_type" IS NULL OR "p_type" != \'hosting\')' if "p_type" in actual_cols else "true",
    )
    if shadow_cursor is not None and debug_lock is not None:
        raw_rows = _execute_on_cursor(
            runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s]
        ).fetchall()
    else:
        raw_rows = runner.execute(sql, [window_start_s, window_start_s]).fetchall()

    baseline_scale = max(float(baseline_hours), 1.0)
    win_h = float(window_hours)

    botnet: list[tuple] = []
    rfd: list[tuple] = []
    cipher_list: list[tuple] = []

    for (
        gid,
        fp,
        cipher,
        bg_w_ips,
        bg_w_reqs,
        bg_b_ips,
        rfd_w_ips,
        rfd_w_asns,
        rfd_w_reqs,
        rfd_b_ips,
        rfd_b_asns,
        cs_w_ips,
        cs_w_reqs,
        cs_b_ips,
    ) in raw_rows:
        gid_i = int(gid or 0)
        if gid_i == 1 and fp is not None and fp != "":
            if has_fp:
                w_ips = int(bg_w_ips or 0)
                w_reqs = int(bg_w_reqs or 0)
                b_ips = int(bg_b_ips or 0)
                scaled_b = b_ips / baseline_scale * win_h
                if w_ips >= 5 and w_ips > scaled_b * 3:
                    ip_ratio = w_ips * 1.0 / max(scaled_b, 1.0)
                    botnet.append((fp, w_ips, w_reqs, b_ips, ip_ratio))
            if has_rfd:
                rw_ips = int(rfd_w_ips or 0)
                rw_asns = int(rfd_w_asns or 0)
                rw_reqs = int(rfd_w_reqs or 0)
                rb_ips = int(rfd_b_ips or 0)
                rb_asns = int(rfd_b_asns or 0)
                if rw_ips >= 30 and rw_asns >= 5:
                    asn_exp = rw_asns * 1.0 / max(rb_asns, 1.0)
                    rfd.append((fp, rw_ips, rw_asns, rw_reqs, rb_ips, asn_exp))
        elif gid_i == 2 and has_cipher and cipher is not None and cipher != "":
            cw_ips = int(cs_w_ips or 0)
            cw_reqs = int(cs_w_reqs or 0)
            cb_ips = int(cs_b_ips or 0)
            scaled_cb = cb_ips / baseline_scale * win_h
            if cw_ips >= 5 and cw_ips > scaled_cb * 3:
                c_ratio = cw_ips * 1.0 / max(scaled_cb, 1.0)
                cipher_list.append((c_ratio, (cipher, cw_ips, cw_reqs, cb_ips)))

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
    """Coalesce 8 low-cardinality dimension insights (new_country_traffic,
    proxy_surge, method_drift, connection_type_mix, origin_error_rate,
    origin_ip_failure, referer_monoculture, ua_monoculture) into ONE pass
    using GROUPING SETS.
    """
    has_country = "country" in actual_cols
    has_ptype = "p_type" in actual_cols
    has_method = "method" in actual_cols
    has_ctype = "c_type" in actual_cols and "c_speed" in actual_cols
    has_ost = "ost" in actual_cols
    has_oip = "oip" in actual_cols and has_ost
    has_referer = "referer" in actual_cols
    has_ua = "ua" in actual_cols
    if not (has_country or has_ptype or has_method or has_ctype or has_ost or has_oip or has_referer or has_ua):
        return {}

    sql = COALESCED_DIM_AGGREGATES.format(
        table_name=table_name,
        country_sel='"country"' if has_country else "NULL::VARCHAR",
        p_type_sel='"p_type"' if has_ptype else "NULL::VARCHAR",
        method_sel='"method"' if has_method else "NULL::VARCHAR",
        c_type_sel='"c_type"' if has_ctype else "NULL::VARCHAR",
        c_speed_sel='"c_speed"' if has_ctype else "NULL::VARCHAR",
        ost_sel='"ost"' if has_ost else "NULL::INTEGER",
        oip_sel='"oip"' if has_oip else "NULL::VARCHAR",
        referer_sel='"referer"' if has_referer else "NULL::VARCHAR",
        ua_sel='"ua"' if has_ua else "NULL::VARCHAR",
    )
    if shadow_cursor is not None and debug_lock is not None:
        raw_rows = _execute_on_cursor(
            runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s]
        ).fetchall()
    else:
        raw_rows = runner.execute(sql, [window_start_s, window_start_s]).fetchall()

    # GROUPING_ID(country, p_type, method, c_type, c_speed, ost, oip, referer, ua)
    # Bit positions (8..0):
    # (country)          -> 0b0_1111_1111 = 255
    # (p_type)           -> 0b1_0111_1111 = 383
    # (method)           -> 0b1_1011_1111 = 447
    # (c_type, c_speed)  -> 0b1_1100_1111 = 463
    # (ost)              -> 0b1_1111_0111 = 503
    # (oip)              -> 0b1_1111_1011 = 507
    # (referer)          -> 0b1_1111_1101 = 509
    # (ua)               -> 0b1_1111_1110 = 510
    country_rows: list[tuple] = []
    ptype_rows: list[tuple] = []
    method_rows: list[tuple] = []
    ctype_rows: list[tuple] = []
    ost_rows: list[tuple] = []
    oip_rows: list[tuple] = []
    referer_rows: list[tuple] = []
    ua_rows: list[tuple] = []

    for (
        gid,
        country,
        p_type,
        method,
        c_type,
        c_speed,
        ost,
        oip,
        referer,
        ua,
        w_cnt,
        b_cnt,
        w_ost_nn,
        b_ost_nn,
        w_ost_5xx,
        b_ost_5xx,
    ) in raw_rows:
        gid_i = int(gid or 0)
        wc = int(w_cnt or 0)
        bc = int(b_cnt or 0)
        if gid_i == 255:
            country_rows.append((country, wc, bc))
        elif gid_i == 383:
            ptype_rows.append((p_type, wc, bc))
        elif gid_i == 447:
            method_rows.append((method, wc, bc))
        elif gid_i == 463:
            ctype_rows.append((c_type, c_speed, wc, bc))
        elif gid_i == 503:
            ost_rows.append(
                (ost, wc, bc, int(w_ost_nn or 0), int(b_ost_nn or 0), int(w_ost_5xx or 0), int(b_ost_5xx or 0))
            )
        elif gid_i == 507:
            oip_rows.append((oip, int(w_ost_nn or 0), int(w_ost_5xx or 0)))
        elif gid_i == 509:
            referer_rows.append((referer, wc, bc))
        elif gid_i == 510:
            ua_rows.append((ua, wc, bc))

    res: dict[str, list[tuple]] = {}

    if has_country:
        nc = [(c, wc, bc) for c, wc, bc in country_rows if c is not None and wc >= 3 and bc == 0]
        nc.sort(key=lambda r: r[1], reverse=True)
        res["new_country_traffic"] = nc[:20]

    if has_ptype:
        # PROXY_SURGE filters WHERE p_type IS NOT NULL
        pt_nn = [(pt, wc, bc) for pt, wc, bc in ptype_rows if pt is not None]
        w_total_all = sum(wc for _, wc, _ in pt_nn)
        b_total_all = sum(bc for _, _, bc in pt_nn)
        pt_nonempty = [(pt, wc, bc) for pt, wc, bc in pt_nn if pt != ""]
        w_proxy_total = sum(wc for _, wc, _ in pt_nonempty)
        b_proxy_total = sum(bc for _, _, bc in pt_nonempty)
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
        m_w_total = sum(wc for _, wc, _ in method_rows)
        m_b_total = sum(bc for _, _, bc in method_rows)
        md: list[tuple] = []
        if m_w_total > 0:
            for m, wc, bc in method_rows:
                if m is not None and m != "" and str(m).upper() not in ("GET", "HEAD", "OPTIONS"):
                    if wc >= 20 and (wc * 1.0 / m_w_total) >= 0.10:
                        if m_b_total == 0 or (wc * 1.0 / m_w_total) >= (bc * 1.0 / m_b_total) * 2 + 0.05:
                            md.append((m, wc, bc, m_w_total, m_b_total))
        md.sort(key=lambda r: r[1], reverse=True)
        res["method_drift"] = md[:10]

    if has_ctype:
        ct_valid = [(ct, cs, wc, bc) for ct, cs, wc, bc in ctype_rows if ct is not None and ct != ""]
        ct_w_total = sum(wc for _, _, wc, _ in ct_valid)
        ct_b_total = sum(bc for _, _, _, bc in ct_valid)
        ct_out: list[tuple] = []
        if ct_w_total > 0:
            for ct, cs, wc, bc in ct_valid:
                if wc >= 20 and (wc * 1.0 / ct_w_total) >= 0.15:
                    if ct_b_total == 0 or (wc * 1.0 / ct_w_total) >= (bc * 1.0 / ct_b_total) * 2 + 0.05:
                        ct_out.append((ct, cs, wc, bc, ct_w_total, ct_b_total))
        ct_out.sort(key=lambda r: r[2], reverse=True)
        res["connection_type_mix"] = ct_out[:15]

    if has_ost:
        ost_valid = [r for r in ost_rows if r[0] is not None]
        o_w_total = sum(r[3] for r in ost_valid)
        o_b_total = sum(r[4] for r in ost_valid)
        o_w_5xx = sum(r[5] for r in ost_valid)
        o_b_5xx = sum(r[6] for r in ost_valid)
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
        oip_stats: list[tuple[str, int, float]] = []
        for oip_val, reqs, errs_5xx in oip_rows:
            if oip_val is not None and oip_val != "" and reqs >= 10:
                err_pct = round(errs_5xx * 100.0 / reqs, 1)
                oip_stats.append((oip_val, reqs, err_pct))
        if oip_stats:
            rates = sorted(r[2] for r in oip_stats)
            n = len(rates)
            pos = 0.5 * (n - 1)
            lo = int(pos)
            hi = min(lo + 1, n - 1)
            frac = pos - lo
            median_rate = rates[lo] + frac * (rates[hi] - rates[lo])
            oip_out = [
                (oip_val, reqs, err_pct, median_rate)
                for oip_val, reqs, err_pct in oip_stats
                if err_pct > median_rate * 3 and err_pct > 5
            ]
            oip_out.sort(key=lambda r: r[2], reverse=True)
            res["origin_ip_failure"] = oip_out
        else:
            res["origin_ip_failure"] = []

    if has_referer:
        ref_w_total = sum(wc for _, wc, _ in referer_rows)
        ref_b_total = sum(bc for _, _, bc in referer_rows)
        ref_out: list[tuple] = []
        if ref_w_total > 0:
            for ref, wc, bc in referer_rows:
                if ref is not None and ref != "":
                    if (wc * 1.0 / ref_w_total) >= 0.20:
                        if ref_b_total == 0 or (wc * 1.0 / ref_w_total) >= (bc * 1.0 / ref_b_total) * 3 + 0.10:
                            ref_out.append((ref, wc, bc, ref_b_total, ref_w_total))
        ref_out.sort(key=lambda r: r[1], reverse=True)
        res["referer_monoculture"] = ref_out[:10]

    if has_ua:
        ua_w_total = sum(wc for _, wc, _ in ua_rows)
        ua_b_total = sum(bc for _, _, bc in ua_rows)
        ua_out: list[tuple] = []
        if ua_w_total > 0:
            for ua_val, wc, bc in ua_rows:
                if (wc * 1.0 / ua_w_total) >= 0.25:
                    if ua_b_total == 0 or (wc * 1.0 / ua_w_total) >= (bc * 1.0 / ua_b_total) * 3 + 0.10:
                        ua_out.append((ua_val, wc, bc, ua_b_total, ua_w_total))
        ua_out.sort(key=lambda r: r[1], reverse=True)
        res["ua_monoculture"] = ua_out[:10]

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
    """Coalesce pop_latency_regression, region_latency, and
    metro_delivery_degradation into ONE pass using GROUPING SETS
    ((pop), (server_region), (metro)).
    """
    has_elapsed = "elapsed" in actual_cols
    has_pop = "pop" in actual_cols and has_elapsed
    has_sreg = "server_region" in actual_cols and has_elapsed
    has_metro_dr = "metro" in actual_cols and "delivery_rate" in actual_cols
    if not (has_pop or has_sreg or has_metro_dr):
        return {}
    has_ottfb = "ottfb" in actual_cols
    sql = COALESCED_GEO_LATENCY_AGGREGATES.format(
        table_name=table_name,
        pop_sel='"pop"' if has_pop else "NULL::VARCHAR",
        server_region_sel='"server_region"' if has_sreg else "NULL::VARCHAR",
        metro_sel='"metro"' if has_metro_dr else "NULL::INTEGER",
        elapsed_sel='"elapsed"' if has_elapsed else "NULL::DOUBLE",
        ottfb_sel='"ottfb"' if has_ottfb else "NULL::DOUBLE",
        delivery_rate_sel='"delivery_rate"' if has_metro_dr else "NULL::DOUBLE",
    )
    if shadow_cursor is not None and debug_lock is not None:
        raw_rows = _execute_on_cursor(
            runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s]
        ).fetchall()
    else:
        raw_rows = runner.execute(sql, [window_start_s, window_start_s]).fetchall()

    # GROUPING_ID(pop, server_region, metro)
    # (pop)           -> 0b011 = 3
    # (server_region) -> 0b101 = 5
    # (metro)         -> 0b110 = 6
    pop_out: list[tuple[float, tuple]] = []
    sreg_out: list[tuple[float, tuple]] = []
    metro_out: list[tuple[float, tuple]] = []

    for (
        gid,
        pop,
        server_region,
        metro,
        w_p95,
        b_p95,
        w_lat_total,
        b_lat_total,
        w_ottfb_cnt,
        w_ottfb_p95,
        w_med_dr,
        b_med_dr,
        w_dr_total,
        b_dr_total,
    ) in raw_rows:
        gid_i = int(gid or 0)
        if gid_i == 3 and has_pop and pop is not None and pop != "":
            wt = int(w_lat_total or 0)
            bt = int(b_lat_total or 0)
            if (
                wt >= 20
                and bt >= 50
                and w_p95 is not None
                and b_p95 is not None
                and w_p95 >= b_p95 * 1.5
                and (w_p95 - b_p95) >= 100
            ):
                ratio = (w_p95 / b_p95) if b_p95 else float("inf")
                pop_out.append((ratio, (pop, w_p95, b_p95, wt, bt)))
        elif gid_i == 5 and has_sreg and server_region is not None and server_region != "":
            wt = int(w_lat_total or 0)
            bt = int(b_lat_total or 0)
            if (
                wt >= 20
                and bt >= 50
                and w_p95 is not None
                and b_p95 is not None
                and w_p95 >= b_p95 * 1.5
                and (w_p95 - b_p95) >= 100
            ):
                ottfb_val = w_ottfb_p95 if (has_ottfb and int(w_ottfb_cnt or 0) >= 20) else None
                ratio = (w_p95 / b_p95) if b_p95 else float("inf")
                sreg_out.append((ratio, (server_region, w_p95, b_p95, wt, bt, ottfb_val)))
        elif gid_i == 6 and has_metro_dr and metro is not None:
            wdt = int(w_dr_total or 0)
            bdt = int(b_dr_total or 0)
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
    """Fast pre-connection check for a cached insights payload."""
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
    """Compute the insight cards for ``src`` over the window/baseline ranges.

    M2: ``clamp_start`` / ``clamp_end`` (ISO-8601) bound the scanned range to
    the analyst's allowed window — the router derives them via
    ``get_analyst_time_bounds`` + ``clamp_or_400``. ``None`` for both is the
    admin / prewarmer path (full range).

    ``clamp_cache_key``: the STABLE cache-key fragment for an analyst clamp
    shape (``backend.utils.remote_access.analyst_clamp_cache_key`` — keyed on
    the invite's window params, not the rolling resolved bounds). The router
    passes it so repeated analyst requests reuse one cache entry instead of
    recomputing every call. ``None`` keeps the admin key shape (and, for any
    caller that passes a clamp but no key, falls back to the rolling bounds so
    the clamped/unclamped isolation guarantee is preserved).

    ``force_refresh``: skip the cache READ and always recompute (the write
    still happens). The prewarmer uses this so every tick rewrites the entry
    and resets its TTL — a cache *hit* would not (cachetools' TTL is from
    insertion), leaving a window each cycle where the entry has expired.

    M3: ``mask_ips`` is passed to the row processors via ``context`` so the
    IP-keyed insights (request_size_anomaly, connection_abuse) mask the client
    IP they place in the label / filters / investigate_url. It's also part of
    the cache key so a masked analyst result and an unmasked one never share an
    entry.
    """
    from backend.utils.date_utils import parse_iso_utc

    source_name = src["name"]
    table_name = _safe_table(source_name)

    now = datetime.now(UTC)
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(hours=baseline_hours + window_hours)

    # M2: clamp to the analyst's window. ``clamp_end`` ceilings the anchor
    # (absolute-window invites can end in the past → re-derive the relative
    # boundaries off the clamped anchor); ``clamp_start`` floors the earliest
    # scanned row. None on both = admin / prewarmer = no clamp.
    if clamp_end:
        ce = parse_iso_utc(clamp_end)
        if ce and ce < now:
            now = ce
            window_start = now - timedelta(hours=window_hours)
            baseline_start = now - timedelta(hours=baseline_hours + window_hours)
    if clamp_start:
        cs = parse_iso_utc(clamp_start)
        if cs:
            baseline_start = max(baseline_start, cs)
            window_start = max(window_start, cs)

    now_s = now.isoformat()
    window_start_s = window_start.isoformat()
    baseline_start_s = baseline_start.isoformat()

    # Stable, time-independent cache key. The previous shape folded the raw
    # ``clamp_start``/``clamp_end`` into the key; the admin path (None,None)
    # was stable, but every analyst request stamped a fresh ``now`` into those
    # bounds, so the analyst key never repeated → they recomputed on EVERY
    # call (the "broken for analysts" perf-audit finding). Now the analyst key
    # uses ``clamp_cache_key`` (keyed on the invite's WINDOW PARAMETERS, not
    # the rolling resolved bounds), so repeated analyst requests reuse one
    # entry — mirroring the admin time-independent key + the BoundedTTLCache's
    # own auto-expiry for freshness. The resolved bounds still drive the scan;
    # only the KEY is stabilized, so a cache hit is ≤TTL stale, identical to
    # the admin contract. The prewarmer (every 240 s, TTL 300 s,
    # force_refresh) re-writes the entry under the TTL so a user never pays
    # cold compute.
    #
    # ``key_clamp`` resolution:
    #   - clamp_cache_key set  → stable analyst key (router / prewarmer).
    #   - no key but a clamp   → legacy rolling-bounds key, so any non-router
    #                            caller keeps the clamped/unclamped isolation
    #                            guarantee (a scoped result never reads or
    #                            overwrites the admin entry).
    #   - neither              → "" (the admin / prewarmer unclamped shape).
    # ``mask_ips`` stays in the key so masked and unmasked never share.
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
    if INSIGHTS_CACHE_TTL > 0 and not force_refresh:
        with _insights_cache_lock:
            entry = _insights_cache.get(cache_key)
        if entry is not None:
            cached = entry[1].copy()
            # The response model's field is ``is_cached`` with
            # serialization_alias ``_is_cached`` (see BaseResponse). Stamping
            # ``_is_cached`` here is dropped on validation — Pydantic matches
            # the unaliased field name — so every cache hit serialized as
            # ``"_is_cached": false``. Stamp the field name so the marker
            # survives. (Mirrors the dashboard.py fix.)
            cached["is_cached"] = True
            return cached

    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()

    empty_resp = {
        "insights": [],
        "window_start": window_start_s,
        "window_end": now_s,
        "baseline_start": baseline_start_s,
        "baseline_end": window_start_s,
        "computed_at": now_s,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "window_total_requests": 0,
        **runner.telemetry(),
    }
    if not actual_cols:
        # Empty actual_cols can mean two things: legitimate "no schema yet,
        # service was just provisioned" OR a race where a concurrent
        # commit deleted the buffer file between get_schema_cols's first
        # call and us reading it. The latter silently shipped an empty
        # insights payload that the frontend cached. Force-rebuild the
        # view once and retry — if the schema lookup STILL returns empty,
        # that's the "legitimate no-data" branch and we ship the empty
        # response. (force=True bypasses the catalog-refresh fast path so
        # the retry actually does work.)
        try:
            from backend.core import iceberg as db_iceberg

            db_iceberg.update_iceberg_view(con, src, force=True)
            actual_cols = runner.get_schema_cols()
        except Exception:
            logger.debug("get_insights: runner.get_schema_cols failed (ignored)", exc_info=True)
        if not actual_cols:
            return empty_resp

    # ── Materialize relevant window into a per-request scratch table ──────────
    # This is the single most important optimization: avoid globbing/metadata
    # parsing 30+ times.
    #
    # The materialised window + the unnested WAF stream both live in a
    # per-request ``ATTACH ':memory:' AS scratch_<uuid>`` database so the
    # per-insight tasks below can run in parallel: DuckDB Connection objects
    # are not thread-safe, but ``con.cursor()`` returns shadow connections
    # that ARE safe across threads and DO see the attached in-memory
    # database. The first parallelize attempt (3a003de, reverted 7909bcb)
    # used a regular CREATE TABLE on the default DB to work around the
    # TEMP-table cross-cursor invisibility, which paid disk I/O on every
    # cold call. ATTACH ':memory:' fixes both problems — cross-cursor
    # visibility AND in-memory storage. The scratch is unique per request
    # so two concurrent requests on the same pool connection don't collide.
    scratch_alias = f"insights_scratch_{uuid.uuid4().hex[:12]}"
    runner.con.execute(f"ATTACH ':memory:' AS {scratch_alias} (READ_ONLY FALSE)")
    scratch_attached = True
    temp_table = f"{scratch_alias}.insights_temp_{uuid.uuid4().hex[:12]}"

    # Derive needed_cols from each ELIGIBLE insight's `required_fields` —
    # eligibility is the same all-required-fields-present check the per-insight
    # loop runs below (around line 595). Pre-filtering here drops columns that
    # only an ineligible insight references, shrinking the temp projection from
    # ~35 cols to whatever the current service's schema can actually feed. The
    # safety guarantee we had before still holds: any eligible insight's
    # required_fields are included, so no SQL template can land on a missing
    # column.
    needed_cols_set: set[str] = {"timestamp"}
    for d in registry.get_all():
        if all(col in actual_cols for col in d.required_fields):
            needed_cols_set.update(d.required_fields)
    # Support cols processors read from context but no insight lists in
    # required_fields (ja3/ja4 fingerprint selection in botnet_grouping; geo
    # columns referenced via build_geo_select_clause). When ja4 is present in
    # the service schema, fp_col resolves to ja4 and ja3 is never read by any
    # insight, so skip materializing 30 days of ja3 strings into the scratch
    # table. Filtered by actual_cols in the cols_sql line below so missing
    # columns don't break the projection.
    fp_support_col = "ja4" if "ja4" in actual_cols else "ja3"
    needed_cols_set.update({fp_support_col, "region", "resp_bytes"})
    needed_cols = sorted(needed_cols_set)
    cols_sql = ", ".join(f'"{c}"' for c in needed_cols if c in actual_cols)
    if not cols_sql:
        cols_sql = "*"
    elif "resp_bytes" not in actual_cols:
        cols_sql += ', 0::BIGINT AS "resp_bytes"'

    # CREATE TABLE (not TEMP) inside the scratch :memory: DB — TEMP tables
    # are connection-scoped and invisible to ``con.cursor()`` shadow
    # connections; tables in an ATTACH'd :memory: DB are visible to every
    # cursor on the parent connection.
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
        temp_table = table_name  # Fallback to the iceberg view directly

    # WAF unnest temp creation moved into the parallel coalesced-aggregates
    # dispatch below (``_task_waf``) — it runs concurrently with the city/url/
    # ip-security aggregate scans instead of sequentially before them.

    try:
        # Available history + window total in a single scan over the scratch table
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
                    # DuckDB returns TIMESTAMPTZ in the *server's* local zone, not UTC.
                    # `.replace(tzinfo=UTC)` would re-label the local wall clock as
                    # UTC and shift the instant by the local offset — turning a
                    # 23h-old row into a 29h-old one on MDT, or vice versa. Use
                    # astimezone so the instant is preserved across zones (and
                    # handle the rare naive case by assuming UTC).
                    if earliest_ts.tzinfo is None:
                        earliest_ts = earliest_ts.replace(tzinfo=UTC)
                    else:
                        earliest_ts = earliest_ts.astimezone(UTC)
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

        # Insight definitions — Phase 7 caller migration. The new
        # field_registry re-exports INSIGHT_DEFINITIONS verbatim (same list
        # of dicts) so existing patch contracts and the dict-key access shape
        # below stay valid. Switching the import lets the registry control
        # the source-of-truth flip in step 13 without re-editing this file.
        try:
            from backend.core.field_registry import INSIGHT_DEFINITIONS as _defs

            defs_map = {d["id"]: d for d in _defs}
        except Exception:
            defs_map = {}

        def _def(insight_id: str) -> dict:
            return defs_map.get(insight_id, {})

        def check_baseline(insight_id: str) -> dict | None:
            if available_history_hours < baseline_hours:
                d = _def(insight_id)
                reg = registry.get(insight_id)
                avail = max(0.1, round(available_history_hours, 1))
                return {
                    "id": insight_id,
                    "title": d.get("title", insight_id.replace("_", " ").title()),
                    "description": d.get("description", ""),
                    "category": str(reg.category) if reg else None,
                    "severity": "info",
                    "summary": f"Requires {int(baseline_hours)}h of historical data (only {avail}h available)",
                    "items": [],
                }
            return None

        table_name = temp_table

        def make_investigate_url(filters: dict | None = None) -> str:
            p: list[tuple[str, str]] = []
            if service_id:
                p.append(("service", service_id))
            p.extend([("start", window_start_s), ("end", now_s)])
            for col, val in (filters or {}).items():
                if val is not None:
                    p.append((f"filter_{col}", str(val)))
            return "/dashboard?" + urllib.parse.urlencode(p)

        def _sev(items: list, crit_key: bool = False) -> str:
            if not items:
                return "clean"
            if crit_key and any(i.get("severity") == "critical" for i in items):
                return "critical"
            return "warning"

        tasks: list[Callable[[], dict | None]] = []

        # Per-task lock that guards parallel appends to ``runner.debug_queries``.
        # Every per-insight task below AND the coalesced-aggregates dispatch
        # above run in their own thread with their own DuckDB cursor (cursors
        # are separate shadow connections — thread-safe), but the debug-
        # queries list is the request-scoped one consumed by
        # ``runner.telemetry()`` at the bottom of this function and ultimately
        # serialised into the response's debug panel. Without the lock,
        # concurrent ``list.append`` is technically safe under CPython's GIL
        # but the SQL/time pairing through ``_compact_sql_for_debug`` is not
        # — interleaved appends could swap sql with the wrong elapsed_ms.
        _debug_lock = threading.Lock()

        # ── Registered Dynamic Insights ───────────────────────────────────────────
        from backend.repositories.utils.filters import build_geo_select_clause

        loc_cols, label_expr, country_sel, region_sel = build_geo_select_clause(actual_cols)
        # Bare expression (no leading comma, no trailing alias) so templates can
        # write ``, {ua_mobile_sel} AS mobile_ratio`` consistently. Returning
        # ``, ... AS mobile_ratio`` here produced ``avg_kb, , ... AS mobile_ratio
        # AS mobile_ratio`` — a syntax error around the double comma.
        ua_mobile_sel = "0"
        if "ua" in actual_cols:
            ua_mobile_sel = "SUM(CASE WHEN \"ua\" ILIKE '%Mobi%' OR \"ua\" ILIKE '%Android%' OR \"ua\" ILIKE '%iPhone%' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)"
        url_col = '"url"' if "url" in actual_cols else "NULL"
        q_col = '"url"' if "url" in actual_cols else ('"digest"' if "digest" in actual_cols else "'(unknown)'")
        # repeated_patterns: conditional UA projection (mirrors url_col/q_col) +
        # the static, ``?``-free bot/monitor allowlist regex. The regex is a
        # fixed constant, so inlining it leaves the template's single ``?`` (the
        # window start) intact for the engine's sql.count("?") binder heuristic.
        ua_col = '"ua"' if "ua" in actual_cols else "NULL"
        bot_ua_regex = REPEATED_BOT_UA_REGEX

        # ── Coalesced aggregates + WAF unnest — PARALLEL dispatch ─────────────────
        # These 4 queries (WAF unnest temp create, city/url/ip-security
        # coalesced GROUP BYs) used to run sequentially, one after another,
        # BEFORE the per-insight ThreadPoolExecutor dispatch below. Prod
        # 2026-08-03 measured city=10.4s + url=6.9s + ip=3.2s + waf=2.7s =
        # ~23s of pure serialization — each is an independent read (or, for
        # WAF, a write into a brand-new uniquely-named scratch table) over
        # the SAME materialized ``temp_table``, so there's no data dependency
        # between them and they parallelize cleanly.
        #
        # Dispatched via the identical ``runner.con.cursor()`` shadow-
        # connection pattern the per-insight dispatch below already uses
        # safely: DuckDB releases the GIL during query execution, and cursors
        # on the same parent connection are thread-safe to use concurrently
        # from different threads. DuckDB's thread pool is instance-global
        # (``DUCKDB_THREADS``), so these cursors share it rather than each
        # spawning their own. The WAF task is the only WRITE (a
        # CREATE TABLE into the scratch ``:memory:`` DB) among the three
        # read-only aggregate SELECTs; it writes a brand-new, uniquely-named
        # catalog object that none of the three reads touch, so there's no
        # write-write conflict with the concurrent SELECTs (verified locally:
        # concurrent CREATE TABLE + 3 SELECTs via cursors on one parent
        # connection completes cleanly with correct, independent results).
        #
        # Each task independently gates on its own required columns and
        # swallows its own exception exactly as the sequential code did —
        # falling back to ``{}`` / ``None`` never breaks the other 3 tasks or
        # the page.
        def _task_city() -> dict[str, list[tuple]]:
            # Only fires when ALL 4 city-based insights are eligible (city +
            # status + elapsed + timestamp all in schema). When a service is
            # missing one of those columns the per-insight scans still run
            # for the eligible subset (handled in the per-insight loop below).
            if not (
                "city" in actual_cols
                and "status" in actual_cols
                and "elapsed" in actual_cols
                and "timestamp" in actual_cols
            ):
                return {}
            try:
                cur = runner.con.cursor()
                return _coalesced_city_aggregates(
                    runner,
                    table_name,
                    window_start_s,
                    label_expr,
                    region_sel,
                    country_sel,
                    window_hours,
                    baseline_hours,
                    shadow_cursor=cur,
                    debug_lock=_debug_lock,
                )
            except Exception as e:
                # Fall back transparently to per-insight scans; never break
                # the page on a coalesced-path bug.
                import logging

                logging.getLogger(__name__).warning("[insights] coalesced city aggregates failed, falling back: %s", e)
                return {}

        def _task_url() -> dict[str, list[tuple]]:
            # Fires only when all the columns the CTE touches are present
            # (url, status, cache, elapsed, timestamp).
            if not (
                "url" in actual_cols
                and "status" in actual_cols
                and "cache" in actual_cols
                and "elapsed" in actual_cols
                and "timestamp" in actual_cols
            ):
                return {}
            try:
                cur = runner.con.cursor()
                return _coalesced_url_aggregates(
                    runner,
                    table_name,
                    window_start_s,
                    actual_cols=actual_cols,
                    shadow_cursor=cur,
                    debug_lock=_debug_lock,
                )
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning("[insights] coalesced URL aggregates failed, falling back: %s", e)
                return {}

        def _task_ip() -> dict[str, list[tuple]]:
            # Fires only when all the columns the CTE touches are present
            # (ip, url, status, timestamp).
            if not (
                "ip" in actual_cols and "url" in actual_cols and "status" in actual_cols and "timestamp" in actual_cols
            ):
                return {}
            try:
                cur = runner.con.cursor()
                return _coalesced_ip_security_aggregates(
                    runner,
                    table_name,
                    window_start_s,
                    window_hours,
                    baseline_hours,
                    actual_cols=actual_cols,
                    shadow_cursor=cur,
                    debug_lock=_debug_lock,
                )
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning(
                    "[insights] coalesced IP-security aggregates failed, falling back: %s", e
                )
                return {}

        def _task_asn() -> dict[str, list[tuple]]:
            if not ("asn" in actual_cols and "timestamp" in actual_cols):
                return {}
            try:
                cur = runner.con.cursor()
                return _coalesced_asn_aggregates(
                    runner,
                    table_name,
                    window_start_s,
                    actual_cols,
                    shadow_cursor=cur,
                    debug_lock=_debug_lock,
                )
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning("[insights] coalesced ASN aggregates failed, falling back: %s", e)
                return {}

        def _task_scalar() -> dict[str, list[tuple]]:
            if not (
                "timestamp" in actual_cols
                and (
                    "transport" in actual_cols
                    or "cache" in actual_cols
                    or "oconnect_ms" in actual_cols
                    or "ottfb" in actual_cols
                )
            ):
                return {}
            try:
                cur = runner.con.cursor()
                return _coalesced_scalar_aggregates(
                    runner,
                    table_name,
                    window_start_s,
                    actual_cols,
                    shadow_cursor=cur,
                    debug_lock=_debug_lock,
                )
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning(
                    "[insights] coalesced scalar aggregates failed, falling back: %s", e
                )
                return {}

        def _task_fp() -> dict[str, list[tuple]]:
            if not ("timestamp" in actual_cols and "ip" in actual_cols):
                return {}
            try:
                cur = runner.con.cursor()
                return _coalesced_fp_aggregates(
                    runner,
                    table_name,
                    window_start_s,
                    window_hours,
                    baseline_hours,
                    actual_cols,
                    shadow_cursor=cur,
                    debug_lock=_debug_lock,
                )
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning("[insights] coalesced FP aggregates failed, falling back: %s", e)
                return {}

        def _task_dim() -> dict[str, list[tuple]]:
            if "timestamp" not in actual_cols:
                return {}
            try:
                cur = runner.con.cursor()
                return _coalesced_dim_aggregates(
                    runner,
                    table_name,
                    window_start_s,
                    actual_cols,
                    shadow_cursor=cur,
                    debug_lock=_debug_lock,
                )
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning("[insights] coalesced dim aggregates failed, falling back: %s", e)
                return {}

        def _task_geo_lat() -> dict[str, list[tuple]]:
            if "timestamp" not in actual_cols:
                return {}
            try:
                cur = runner.con.cursor()
                return _coalesced_geo_latency_aggregates(
                    runner,
                    table_name,
                    window_start_s,
                    actual_cols,
                    shadow_cursor=cur,
                    debug_lock=_debug_lock,
                )
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning(
                    "[insights] coalesced geo-latency aggregates failed, falling back: %s", e
                )
                return {}

        max_coalesce_workers = max(1, min(max_workers, 4))
        with ThreadPoolExecutor(
            max_workers=max_coalesce_workers, thread_name_prefix="insights-coalesce"
        ) as _coalesce_pool:
            _fut_city = _coalesce_pool.submit(contextvars.copy_context().run, _task_city)
            _fut_url = _coalesce_pool.submit(contextvars.copy_context().run, _task_url)
            _fut_ip = _coalesce_pool.submit(contextvars.copy_context().run, _task_ip)
            _fut_fp = _coalesce_pool.submit(contextvars.copy_context().run, _task_fp)
            _fut_dim = _coalesce_pool.submit(contextvars.copy_context().run, _task_dim)
            _fut_geo_lat = _coalesce_pool.submit(contextvars.copy_context().run, _task_geo_lat)
            _fut_asn = _coalesce_pool.submit(contextvars.copy_context().run, _task_asn)
            _fut_scalar = _coalesce_pool.submit(contextvars.copy_context().run, _task_scalar)
            city_precomputed = _fut_city.result()
            url_precomputed = _fut_url.result()
            ip_security_precomputed = _fut_ip.result()
            fp_precomputed = _fut_fp.result()
            dim_precomputed = _fut_dim.result()
            geo_lat_precomputed = _fut_geo_lat.result()
            asn_precomputed = _fut_asn.result()
            scalar_precomputed = _fut_scalar.result()

        for definition in registry.get_all():
            # Check if all required fields are present
            if not all(col in actual_cols for col in definition.required_fields):
                continue
            if definition.id == "waf_signal_spikes" and not temp_created:
                continue

            def _make_task(d=definition):
                def compute_insight() -> dict | None:
                    # Hydrate template
                    fp_col = "ja4" if "ja4" in actual_cols else "ja3"

                    # Special hydration for specific insights
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

                    if d.id != "impossible_distance":
                        r = check_baseline(d.id)
                        if r:
                            return r

                    # O2 / Step 2 bypass: insights pull rows from the precomputed
                    # coalesced aggregates instead of issuing their own SELECT.
                    # Row schema is constructed to match each insight's existing
                    # `# row schema: [...]` processor contract.
                    default_sql = getattr(_insights_sql, d.id.upper(), None)
                    is_default_template = default_sql is None or d.sql_template == default_sql
                    if is_default_template and d.id in city_precomputed:
                        rows = city_precomputed[d.id]
                    elif is_default_template and d.id in url_precomputed:
                        rows = url_precomputed[d.id]
                    elif is_default_template and d.id in ip_security_precomputed:
                        rows = ip_security_precomputed[d.id]
                    elif is_default_template and d.id in fp_precomputed:
                        rows = fp_precomputed[d.id]
                    elif is_default_template and d.id in dim_precomputed:
                        rows = dim_precomputed[d.id]
                    elif is_default_template and d.id in geo_lat_precomputed:
                        rows = geo_lat_precomputed[d.id]
                    elif is_default_template and d.id in asn_precomputed:
                        rows = asn_precomputed[d.id]
                    elif is_default_template and d.id in scalar_precomputed:
                        rows = scalar_precomputed[d.id]
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
                            # If hydration fails due to missing keys (e.g. pop_values), skip this insight
                            return None

                        param_count = sql.count("?")
                        params = [window_start_s] * param_count

                        # Per-task cursor: shadow connection on the same
                        # parent ``runner.con``, thread-safe and able to see
                        # the parent's ATTACH ':memory:' scratch tables.
                        # The closure receives the cursor each call so the
                        # outer ThreadPoolExecutor can run tasks in parallel
                        # without the implicit single-cursor serialisation
                        # that ``runner.execute`` enforces.
                        task_cur = runner.con.cursor()
                        rows = _execute_on_cursor(runner, _debug_lock, task_cur, sql, params).fetchall()
                    items = []
                    if d.row_processor:
                        # Build context for processors
                        context = {
                            "window_hours": window_hours,
                            "baseline_hours": baseline_hours,
                            "fp_col": fp_col,
                            "actual_cols": actual_cols,
                            "mask_ips": mask_ips,
                        }

                        # Lazy load maps if needed by processors
                        if any(p in d.id for p in ["asn", "metro"]):
                            from backend.core import duckdb as _db_core

                            context["asn_names"] = _db_core.get_asn_names(src["name"], [r[0] for r in rows if r])
                            if "metro" in actual_cols:
                                context["dma_map"] = _db_core._get_dma_map()

                        for row in rows:
                            try:
                                item = d.row_processor(row, d, context)
                                if "investigate_url" not in item:
                                    filters = item.get("meta", {}).get("filters", {})
                                    item["investigate_url"] = make_investigate_url(filters)
                                items.append(item)
                            except Exception:
                                continue

                    severity = "clean"
                    if items:
                        if d.severity_logic:
                            severity = d.severity_logic(items)
                        else:
                            severity = _sev(items, crit_key=True)

                    summary = ""
                    if items:
                        if d.id == "error_spikes":
                            summary = f"{len(items)} URLs with elevated server error rates"
                        elif d.id == "botnet_grouping":
                            summary = f"{len(items)} fingerprints with suspicious IP spread"
                        else:
                            summary = f"{len(items)} anomalies detected"
                    else:
                        summary = f"No {d.title.lower()} detected"

                    return {
                        "id": d.id,
                        "title": d.title,
                        "description": d.description,
                        "category": str(d.category),
                        "severity": severity,
                        "summary": summary,
                        "items": items,
                    }

                # Tag the closure with the insight id+title so the error-path
                # below can report which insight failed. Without these, every
                # task closes over the same `compute_insight` name and the
                # error path emits duplicate `id="insight"` entries — which
                # React then warns about as duplicate keys.
                compute_insight._insight_id = d.id  # type: ignore[attr-defined]
                compute_insight._insight_title = d.title  # type: ignore[attr-defined]
                compute_insight._insight_category = str(d.category)  # type: ignore[attr-defined]
                return compute_insight

            tasks.append(_make_task())

        insights_list: list[dict] = []

        def _safe_run(fn: Callable[[], dict | None]) -> dict | None:
            """Run one insight task and surface its failure as an error
            card rather than killing the whole batch — same contract the
            serial path had."""
            try:
                return fn()
            except Exception as e:
                insight_id = getattr(fn, "_insight_id", "unknown")
                insight_title = getattr(fn, "_insight_title", insight_id.replace("_", " ").title())
                insight_category = getattr(fn, "_insight_category", None)
                return {
                    "id": insight_id,
                    "title": insight_title,
                    "category": insight_category,
                    "severity": "error",
                    "summary": f"Query failed: {str(e)}",
                    "description": "",
                    "items": [],
                }

        # ThreadPoolExecutor dispatch — DuckDB releases the GIL during
        # query execution, so the remaining per-insight scans overlap on
        # the same database. Capping pool_workers at 3 avoids oversubscribing
        # DuckDB's shared thread pool across too many simultaneous heavy scans.
        if tasks:
            pool_workers = max(1, min(max_workers, 3, len(tasks)))
            ctx_tasks = [(contextvars.copy_context(), t) for t in tasks]
            with ThreadPoolExecutor(max_workers=pool_workers, thread_name_prefix="insights") as pool:
                for res in pool.map(lambda pair: pair[0].run(_safe_run, pair[1]), ctx_tasks):
                    if res:
                        insights_list.append(res)

        payload: dict[str, Any] = {
            "insights": insights_list,
            "window_start": window_start_s,
            "window_end": now_s,
            "baseline_start": baseline_start_s,
            "baseline_end": window_start_s,
            "computed_at": now_s,
            "window_hours": window_hours,
            "baseline_hours": baseline_hours,
            "window_total_requests": w_total,
            **runner.telemetry(),
        }
        if INSIGHTS_CACHE_TTL > 0:
            with _insights_cache_lock:
                _insights_cache[cache_key] = (now_s, payload)
        return payload
    finally:
        # Release the entire scratch :memory: DB in one DETACH so neither
        # the materialised window nor the WAF stream nor the alias itself
        # leak onto the pooled DuckDB connection (audit finding 009 /
        # 2026-06-15). Best-effort: a failed DETACH still releases when
        # the connection is recycled, and the alias includes a fresh UUID
        # per request so the next call can't collide on the same name.
        if scratch_attached:
            try:
                runner.con.execute(f"DETACH {scratch_alias}")
            except Exception:
                logger.debug("get_insights: runner.con.execute failed (ignored)", exc_info=True)


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
    """Compute detailed timeline and eviction points for a specific URL."""
    from backend.core.share_db.validation import mask_ip
    from backend.utils.date_utils import parse_iso_utc

    source_name = src["name"]
    table_name = _safe_table(source_name)

    now = datetime.now(UTC)
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(hours=baseline_hours + window_hours)

    if clamp_end:
        ce = parse_iso_utc(clamp_end)
        if ce and ce < now:
            now = ce
            window_start = now - timedelta(hours=window_hours)
            baseline_start = now - timedelta(hours=baseline_hours + window_hours)
    if clamp_start:
        cs = parse_iso_utc(clamp_start)
        if cs:
            baseline_start = max(baseline_start, cs)
            window_start = max(window_start, cs)

    now_s = now.isoformat()
    window_start_s = window_start.isoformat()
    baseline_start_s = baseline_start.isoformat()

    runner = QueryRunner(con, src)

    # 1. Fetch window/baseline cache dispositions for this URL.
    #    Hit ratio = HIT/(HIT+MISS) (cacheable only, PASS excluded — the
    #    conventional Fastly definition). Pass rate = PASS/total (uncacheable
    #    share). Counts double as the window breakdown shown in the modal.
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

    w_cacheable = w_hits + w_miss
    b_cacheable = b_hits + b_miss
    w_rate = (w_hits / w_cacheable) if w_cacheable else 0.0
    b_rate = (b_hits / b_cacheable) if b_cacheable else 0.0
    w_pass_rate = (w_pass / w_total) if w_total else 0.0
    b_pass_rate = (b_pass / b_total) if b_total else 0.0
    breakdown = {
        "hits": w_hits,
        "misses": w_miss,
        "passes": w_pass,
        "other": max(0, w_total - w_hits - w_miss - w_pass),
    }

    # 2. Determine time-bucket interval based on total lookback duration
    total_hours = window_hours + baseline_hours
    if total_hours <= 2:
        bucket_interval = "1 minute"
    elif total_hours <= 12:
        bucket_interval = "5 minutes"
    elif total_hours <= 48:
        bucket_interval = "15 minutes"
    elif total_hours <= 168:
        bucket_interval = "1 hour"
    elif total_hours <= 720:
        bucket_interval = "4 hours"
    else:
        bucket_interval = "1 day"

    # 3. Query timeline of requests, hits and misses. expected_hits and hit_rate
    #    are computed against the cacheable count (HIT+MISS) so the chart matches
    #    the cacheable hit ratio shown in the stats — PASS traffic doesn't drag
    #    the line down.
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
    timeline_params = [url, baseline_start_s, now_s]
    timeline_rows = runner.execute(timeline_sql, timeline_params).fetchall()

    timeline = []
    for r in timeline_rows:
        bucket_ts = r[0]
        total_requests = int(r[1] or 0)
        real_hits = int(r[2] or 0)
        misses = int(r[3] or 0)
        cacheable = real_hits + misses
        expected_hits = float(cacheable * b_rate)
        hit_rate = float(real_hits / cacheable) if cacheable > 0 else 0.0
        timeline.append(
            {
                "bucket": bucket_ts.isoformat() if hasattr(bucket_ts, "isoformat") else str(bucket_ts),
                "expected_hits": expected_hits,
                "real_hits": real_hits,
                "misses": misses,
                "total_requests": total_requests,
                "hit_rate": hit_rate,
            }
        )

    # 4. Fetch the most recent 50 actual cache MISSes (cacheable requests that
    #    weren't in cache) — the events relevant to a hit-ratio collapse. PASS
    #    is uncacheable, so it's deliberately excluded here; the PASS share is
    #    summarised in `breakdown`/`window_pass_rate` instead.
    misses_sql = f"""
        SELECT
            timestamp,
            cache,
            pop,
            ip,
            status
        FROM {table_name}
        WHERE "url" = ? AND starts_with(cache, 'MISS') AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
        ORDER BY timestamp DESC
        LIMIT 50
    """
    miss_params = [url, baseline_start_s, now_s]
    miss_rows = runner.execute(misses_sql, miss_params).fetchall()

    recent_misses = []
    for mr in miss_rows:
        raw_ip = mr[3]
        masked_ip_val = mask_ip(raw_ip) if mask_ips and raw_ip else raw_ip
        recent_misses.append(
            {
                "timestamp": mr[0].isoformat() if hasattr(mr[0], "isoformat") else str(mr[0]),
                "cache": mr[1] or "UNKNOWN",
                "pop": mr[2],
                "ip": masked_ip_val,
                "status": int(mr[4]) if mr[4] is not None else None,
            }
        )

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


# R-1: register the insights TTL cache so the autouse fixture in
# tests/conftest.py drains it via CacheRegistry.clear_all().
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("insights._insights_cache", _insights_cache)
