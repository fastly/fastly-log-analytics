"""Insights repository — anomaly detection queries, no HTTP imports."""

from __future__ import annotations

import heapq
import threading
import time
import urllib.parse
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend.repositories._base import QueryRunner, _compact_sql_for_debug, _safe_table
from backend.repositories._sql.insights import (
    COALESCED_CITY_AGGREGATES,
    COALESCED_IP_SECURITY_AGGREGATES,
    COALESCED_URL_AGGREGATES,
    REPEATED_BOT_UA_REGEX,
)

from .registry import registry

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
    with debug_lock:
        runner.debug_queries.append({"sql": _compact_sql_for_debug(sql), "time_ms": elapsed_ms})
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
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    """Coalesce 5 URL-keyed insights (error_spikes, cache_collapse,
    cacheability_regression, latency_regression, tail_latency) into ONE
    pass over ``table_name``.

    Each of those insights previously ran its own GROUP BY url
    scan with the same WHERE clause and same baseline/window split
    ((timestamp < window_start) → baseline, (>=) → window). Coalescing
    them mirrors the O2 city-aggregates pattern that demonstrably saved
    ~520 ms on prod by replacing 4 city scans with 1.

    Why not all url-keyed insights: ``origin_latency_spike`` is grouped by
    URL too but its SQL has a different shape — it uses overall_stats
    CTEs to normalize against the entire population's percentile, so
    its per-url aggregates need a second pass. Leaving it on its own
    SQL template avoids cross-contaminating the simpler shared CTE.

    cache_collapse and cacheability_regression are siblings that split the
    cache story: cache_collapse tracks the hit ratio of *cacheable* traffic
    (HIT/(HIT+MISS), PASS excluded), cacheability_regression tracks a surge
    in *uncacheable* traffic (PASS/total). They share the HIT/MISS/PASS
    counters in COALESCED_URL_AGGREGATES.

    Returns ``{insight_id: rows}`` where each rows list matches the
    insight's existing processor row-schema. On any exception the
    caller falls back to the legacy per-insight scans transparently.

    - error_spikes:             [url, w_rate, b_rate, w_errors, w_total, b_total]
    - cache_collapse:           [url, w_rate, b_rate, w_cacheable, b_cacheable]
    - cacheability_regression:  [url, w_pass_rate, b_pass_rate, w_total, b_total]
    - latency_regression:       [url, w_p95, b_p95, w_total, b_total]
    - tail_latency:             [url, p99_ms, p50_ms, ratio, total]
    """
    sql = COALESCED_URL_AGGREGATES.format(table_name=table_name)
    if shadow_cursor is not None and debug_lock is not None:
        result_cursor = _execute_on_cursor(runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s])
    else:
        result_cursor = runner.execute(sql, [window_start_s, window_start_s])

    # Bounded top-K via min-heap on score: each insight only ever holds
    # at most _TOP_K entries in memory regardless of how many unique
    # URLs match the threshold. Pre-refactor these were unbounded lists
    # that grew linearly with cardinality — an attacker generating
    # millions of unique URLs (e.g. random query-string) that hit any
    # of the per-insight gates could OOM the worker before the trailing
    # sort+slice ran. The counter tie-breaker preserves insertion order
    # for items with identical scores (matches the prior implicit-stable
    # sort behaviour).
    _TOP_K = 15
    error_spikes_heap: list[tuple] = []
    cache_collapse_heap: list[tuple] = []
    cacheability_regression_heap: list[tuple] = []
    latency_regression_heap: list[tuple] = []
    tail_latency_heap: list[tuple] = []

    def _push_top_k(heap: list[tuple], score: float, counter: int, item: tuple) -> None:
        if len(heap) < _TOP_K:
            heapq.heappush(heap, (score, counter, item))
        else:
            heapq.heappushpop(heap, (score, counter, item))

    counter = 0

    while True:
        rows = result_cursor.fetchmany(10000)
        if not rows:
            break
        for r in rows:
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
            # Legacy HAVING: w_total >= 3 AND w_rate >= 0.05
            #                AND (b_total < 10 OR w_rate >= b_rate * 2 + 0.05)
            # ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15
            if w_total_i >= 3:
                w_rate_e = (w_5xx or 0) / w_total_i if w_total_i else 0.0
                b_rate_e = ((b_5xx or 0) / b_total_i) if b_total_i else None
                if w_rate_e >= 0.05 and (b_total_i < 10 or (b_rate_e is not None and w_rate_e >= b_rate_e * 2 + 0.05)):
                    item = (url, w_rate_e, b_rate_e, w_5xx, w_total, b_total)
                    score = (w_rate_e or 0) - (b_rate_e or 0)
                    _push_top_k(error_spikes_heap, score, counter, item)

            # ── cache_collapse ────────────────────────────────────────────────
            # Hit ratio = HIT / (HIT + MISS) — the conventional Fastly cache hit
            # ratio. PASS is uncacheable and excluded from both numerator and
            # denominator, so a PASS surge no longer trips this insight (it is
            # surfaced by cacheability_regression below). Gate on the *cacheable*
            # sample size (HIT+MISS), not total requests. Mirror this exactly in
            # the standalone CACHE_COLLAPSE template (parity test depends on it).
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
            # pass_rate = PASS / ALL requests. Fires when a previously-cacheable
            # URL flips to mostly PASS (origin made it uncacheable). Mirror the
            # standalone CACHEABILITY_REGRESSION template.
            if w_total_i >= 10 and b_total_i >= 50:
                w_pass_rate = (w_pass or 0) / w_total_i if w_total_i else 0.0
                b_pass_rate = (b_pass or 0) / b_total_i if b_total_i else 0.0
                if b_pass_rate <= 0.20 and w_pass_rate >= 0.50 and w_pass_rate >= b_pass_rate + 0.30:
                    item_cr: tuple = (url, w_pass_rate, b_pass_rate, w_total, b_total)
                    score = (w_pass_rate or 0) - (b_pass_rate or 0)
                    _push_top_k(cacheability_regression_heap, score, counter, item_cr)

            # ── latency_regression ────────────────────────────────────────────
            # Legacy HAVING: w_total >= 5 AND b_total >= 20 AND w_p95 >= b_p95 * 2.0
            #                AND w_p95 - b_p95 >= 200
            # ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
            #
            # Note: legacy uses w_total/b_total (TOTAL counts) for the >=5/>=20
            # gate, NOT w_lat_total/b_lat_total — preserve that or this insight
            # would surface MORE urls than the legacy implementation.
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
            # Legacy WHERE timestamp >= window_start; HAVING COUNT(*) >= 20 AND
            # ratio > 5. ORDER BY ratio DESC LIMIT 15.
            # ratio = p99 / NULLIF(p50, 0)
            if w_lat_total is not None and w_lat_total >= 20 and w_p99 is not None and w_p50 is not None and w_p50 > 0:
                ratio = round(w_p99 / w_p50, 1)
                if ratio > 5:
                    item_tl: tuple = (url, w_p99, w_p50, ratio, w_lat_total)
                    _push_top_k(tail_latency_heap, ratio or 0.0, counter, item_tl)

    def _heap_to_sorted_items(heap: list[tuple]) -> list[tuple]:
        # heap is (score, counter, item); return items sorted by score desc.
        return [entry[2] for entry in sorted(heap, key=lambda e: e[0], reverse=True)]

    return {
        "error_spikes": _heap_to_sorted_items(error_spikes_heap),
        "cache_collapse": _heap_to_sorted_items(cache_collapse_heap),
        "cacheability_regression": _heap_to_sorted_items(cacheability_regression_heap),
        "latency_regression": _heap_to_sorted_items(latency_regression_heap),
        "tail_latency": _heap_to_sorted_items(tail_latency_heap),
    }


def _coalesced_ip_security_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    window_hours: float,
    baseline_hours: float,
    *,
    shadow_cursor: duckdb.DuckDBPyConnection | None = None,
    debug_lock: threading.Lock | None = None,
) -> dict[str, list[tuple]]:
    """Coalesce the 3 IP-keyed security scans (low_and_slow,
    credential_enumeration, content_discovery) into ONE ``GROUP BY ip`` pass
    over ``table_name``.

    Each of those insights previously ran its own ``GROUP BY ip`` scan of the
    temp. §9.7 of the design plan requires folding same-key insights into a
    shared pre-agg (mirrors ``_coalesced_url_aggregates``) so cold-path latency
    doesn't grow linearly with the number of IP scans. ``COALESCED_IP_SECURITY_
    AGGREGATES`` computes conditional aggregates for all three keyed on ``ip``;
    this demuxes them into each insight's existing processor row-schema, applies
    each insight's HAVING/ORDER/LIMIT in Python, and (via the caller's
    try/except) falls back to the standalone scans on any exception.

    Returns ``{insight_id: rows}`` where each rows list matches the insight's
    existing processor row-schema:

    - low_and_slow:            [ip, hits, distinct_paths, span_s, rps]
    - credential_enumeration:  [ip, w_denied, w_attempts, w_paths, b_denied]
    - content_discovery:       [ip, w_404, w_total, distinct_404, b_404]
    """
    sql = COALESCED_IP_SECURITY_AGGREGATES.format(table_name=table_name)
    if shadow_cursor is not None and debug_lock is not None:
        result_cursor = _execute_on_cursor(runner, debug_lock, shadow_cursor, sql, [window_start_s, window_start_s])
    else:
        result_cursor = runner.execute(sql, [window_start_s, window_start_s])

    # Bounded top-K via min-heap on a per-insight sort key — each insight holds
    # at most _TOP_K entries regardless of IP cardinality, so an attacker
    # spraying millions of distinct IPs (each tripping the outer HAVING with a
    # single probe/auth/404 request) can't OOM the worker. The counter
    # tie-breaker preserves insertion order for equal keys (stable-sort parity
    # with the standalone ORDER BY). Sort keys are tuples so multi-column ORDER
    # BYs (low_and_slow's distinct DESC, span DESC) compare lexicographically.
    _TOP_K = 15
    low_and_slow_heap: list[tuple] = []
    credential_enumeration_heap: list[tuple] = []
    content_discovery_heap: list[tuple] = []

    def _push_top_k(heap: list[tuple], sortkey: tuple, counter: int, item: tuple) -> None:
        if len(heap) < _TOP_K:
            heapq.heappush(heap, (sortkey, counter, item))
        else:
            heapq.heappushpop(heap, (sortkey, counter, item))

    baseline_scale = max(baseline_hours, 1.0)
    counter = 0

    while True:
        rows = result_cursor.fetchmany(10000)
        if not rows:
            break
        for r in rows:
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
            # Standalone HAVING: hits >= 5 AND distinct >= 3 AND span >= 600 AND
            # hits/span < 0.2. ORDER BY distinct DESC, span DESC LIMIT 15.
            ls_hits_i = ls_hits or 0
            ls_distinct_i = ls_distinct or 0
            span_s = (ls_max_sec - ls_min_sec) if (ls_min_sec is not None and ls_max_sec is not None) else 0
            if (
                ls_hits_i >= 5
                and ls_distinct_i >= 3
                and span_s >= 600
                and (ls_hits_i / span_s) < 0.2  # span_s >= 600 here, never zero
            ):
                rps = round(ls_hits_i / span_s, 5)
                item_ls: tuple = (ip, ls_hits_i, ls_distinct_i, span_s, rps)
                _push_top_k(low_and_slow_heap, (ls_distinct_i, span_s), counter, item_ls)

            # ── credential_enumeration ────────────────────────────────────────
            # Standalone HAVING: w_denied >= 20 AND w_denied/w_attempts >= 0.5 AND
            # w_denied > b_denied/baseline_hours*window_hours*3 + 5.
            # ORDER BY w_denied DESC LIMIT 15.
            ce_w_denied_i = ce_w_denied or 0
            if ce_w_denied_i >= 20:
                ce_ratio = (ce_w_denied_i / ce_w_attempts) if ce_w_attempts else 0.0
                b_norm = (ce_b_denied or 0) / baseline_scale * window_hours * 3 + 5
                if ce_ratio >= 0.5 and ce_w_denied_i > b_norm:
                    item_ce: tuple = (ip, ce_w_denied_i, ce_w_attempts, ce_w_paths, ce_b_denied)
                    _push_top_k(credential_enumeration_heap, (ce_w_denied_i,), counter, item_ce)

            # ── content_discovery ─────────────────────────────────────────────
            # Standalone HAVING: w_404 >= 20 AND w_404/w_total >= 0.7 AND
            # distinct_404 >= 15. ORDER BY w_404 DESC LIMIT 15.
            cd_w_404_i = cd_w_404 or 0
            cd_distinct_404_i = cd_distinct_404 or 0
            if cd_w_404_i >= 20 and cd_distinct_404_i >= 15:
                cd_ratio = (cd_w_404_i / cd_w_total) if cd_w_total else 0.0
                if cd_ratio >= 0.7:
                    item_cd: tuple = (ip, cd_w_404_i, cd_w_total, cd_distinct_404_i, cd_b_404)
                    _push_top_k(content_discovery_heap, (cd_w_404_i,), counter, item_cd)

    def _heap_to_sorted_items(heap: list[tuple]) -> list[tuple]:
        # heap is (sortkey, counter, item); return items sorted by sortkey desc.
        return [entry[2] for entry in sorted(heap, key=lambda e: e[0], reverse=True)]

    return {
        "low_and_slow": _heap_to_sorted_items(low_and_slow_heap),
        "credential_enumeration": _heap_to_sorted_items(credential_enumeration_heap),
        "content_discovery": _heap_to_sorted_items(content_discovery_heap),
    }


def get_insights(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    window_hours: float,
    baseline_hours: float,
    *,
    service_id: str | None = None,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
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
    if clamp_cache_key is not None:
        key_clamp = clamp_cache_key
    elif clamp_start or clamp_end:
        key_clamp = f"{clamp_start or ''}:{clamp_end or ''}"
    else:
        key_clamp = ""
    cache_key = f"{source_name}:{window_hours}:{baseline_hours}:{key_clamp}:{int(mask_ips)}"
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
            pass
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
    runner.con.execute(f"ATTACH ':memory:' AS {scratch_alias}")
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
    # columns referenced via build_geo_select_clause). Filtered by actual_cols
    # in the cols_sql line below so missing columns don't break the projection.
    needed_cols_set.update({"ja3", "ja4", "region"})
    needed_cols = sorted(needed_cols_set)
    cols_sql = ", ".join(f'"{c}"' for c in needed_cols if c in actual_cols)
    if not cols_sql:
        cols_sql = "*"

    # CREATE TABLE (not TEMP) inside the scratch :memory: DB — TEMP tables
    # are connection-scoped and invisible to ``con.cursor()`` shadow
    # connections; tables in an ATTACH'd :memory: DB are visible to every
    # cursor on the parent connection.
    create_q = f"CREATE TABLE {temp_table} AS SELECT {cols_sql} FROM {table_name} WHERE timestamp >= CAST('{baseline_start_s}' AS TIMESTAMPTZ) AND timestamp <= CAST('{now_s}' AS TIMESTAMPTZ)"
    temp_created = runner.create_temp_table(create_q, [])
    if not temp_created:
        temp_table = table_name  # Fallback to the iceberg view directly

    # WAF unnest temp creation moved into the parallel coalesced-aggregates
    # dispatch below (``_task_waf``) — it runs concurrently with the city/url/
    # ip-security aggregate scans instead of sequentially before them.

    try:
        # Available history
        try:
            earliest_ts = runner.execute(f"SELECT min(timestamp) FROM {temp_table}").fetchone()[0]
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

        try:
            w_total = runner.execute(
                f"SELECT count(*) FROM {temp_table} WHERE timestamp >= CAST(? AS TIMESTAMPTZ)", [window_start_s]
            ).fetchone()[0]
        except Exception:
            w_total = 0

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
        # from different threads. With ``DUCKDB_POOL_CONN_THREADS=1`` (each
        # pool connection single-threaded — see backend/core/duckdb_pool.py)
        # up to 4 concurrent cursors here exactly saturates the 4-core prod
        # VM with no oversubscription. The WAF task is the only WRITE (a
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
        def _task_waf() -> str | None:
            if not (temp_created and "waf_sig" in actual_cols):
                return None
            waf_temp = f"{scratch_alias}.insights_waf_{uuid.uuid4().hex[:12]}"
            waf_create_q = (
                f"CREATE TABLE {waf_temp} AS "
                f"SELECT timestamp, trim(signal) AS signal "
                f"FROM (SELECT timestamp, unnest(string_split(\"waf_sig\", ',')) AS signal "
                f"      FROM {temp_table} "
                f'      WHERE "waf_sig" IS NOT NULL AND "waf_sig" != \'\') '
                f"WHERE trim(signal) != '' AND trim(signal) != 'BOT-ANALYSIS'"
            )
            try:
                cur = runner.con.cursor()
                _execute_on_cursor(runner, _debug_lock, cur, waf_create_q)
                return waf_temp
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning(
                    "[insights] WAF unnest temp create failed, skipping waf_signal_spikes: %s", e
                )
                return None

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
                    runner, table_name, window_start_s, shadow_cursor=cur, debug_lock=_debug_lock
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
                    shadow_cursor=cur,
                    debug_lock=_debug_lock,
                )
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning(
                    "[insights] coalesced IP-security aggregates failed, falling back: %s", e
                )
                return {}

        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="insights-coalesce") as _coalesce_pool:
            _fut_waf = _coalesce_pool.submit(_task_waf)
            _fut_city = _coalesce_pool.submit(_task_city)
            _fut_url = _coalesce_pool.submit(_task_url)
            _fut_ip = _coalesce_pool.submit(_task_ip)
            waf_table = _fut_waf.result()
            city_precomputed = _fut_city.result()
            url_precomputed = _fut_url.result()
            ip_security_precomputed = _fut_ip.result()

        for definition in registry.get_all():
            # Check if all required fields are present
            if not all(col in actual_cols for col in definition.required_fields):
                continue
            # waf_signal_spikes reads from the pre-unnested insights_waf temp.
            # If we couldn't materialise it (main temp create failed earlier)
            # skip rather than crash: the SQL template references a "signal"
            # column that only exists on the materialised temp.
            if definition.id == "waf_signal_spikes" and waf_table is None:
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
                    if d.id in city_precomputed:
                        rows = city_precomputed[d.id]
                    elif d.id in url_precomputed:
                        rows = url_precomputed[d.id]
                    elif d.id in ip_security_precomputed:
                        rows = ip_security_precomputed[d.id]
                    else:
                        try:
                            sql = d.sql_template.format(
                                table_name=table_name,
                                waf_table=waf_table or table_name,
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
        # query execution, so the per-insight scans (~15-20 SQL-firing
        # definitions after city_precomputed/url_precomputed bypass the
        # other 5-10) overlap on the same database. ``max_workers=4``
        # bounds memory pressure on the shared scratch :memory: tables
        # and leaves headroom for the main-thread Python work that runs
        # around the SQL.
        if tasks:
            max_workers = min(4, len(tasks))
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="insights") as pool:
                for res in pool.map(_safe_run, tasks):
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
                pass


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
