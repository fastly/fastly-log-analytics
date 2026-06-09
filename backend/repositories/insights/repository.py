"""Insights repository — anomaly detection queries, no HTTP imports."""

from __future__ import annotations

import threading
import time
import urllib.parse
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend.repositories._base import QueryRunner, _safe_table

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


def _coalesced_city_aggregates(
    runner: QueryRunner,
    table_name: str,
    window_start_s: str,
    label_expr: str,
    region_sel: str,
    country_sel: str,
    window_hours: float,
    baseline_hours: float,
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
    sql = f"""
    WITH base AS (
        SELECT
            "city",
            {region_sel} AS region,
            {country_sel} AS country,
            {label_expr} AS label,
            status,
            elapsed,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
        WHERE "city" IS NOT NULL AND "city" != ''
    )
    SELECT
        label, "city", region, country,
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
    GROUP BY ALL
    """
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
) -> dict[str, list[tuple]]:
    """Coalesce 4 URL-keyed insights (error_spikes, cache_collapse,
    latency_regression, tail_latency) into ONE pass over ``table_name``.

    Each of those four insights previously ran its own GROUP BY url
    scan with the same WHERE clause and same baseline/window split
    ((timestamp < window_start) → baseline, (>=) → window). Coalescing
    them mirrors the O2 city-aggregates pattern that demonstrably saved
    ~520 ms on prod by replacing 4 city scans with 1.

    Why these 4 and not all 5: ``origin_latency_spike`` is grouped by
    URL too but its SQL has a different shape — it uses overall_stats
    CTEs to normalize against the entire population's percentile, so
    its per-url aggregates need a second pass. Leaving it on its own
    SQL template avoids cross-contaminating the simpler 4-insight CTE.

    Returns ``{insight_id: rows}`` where each rows list matches the
    insight's existing processor row-schema. On any exception the
    caller falls back to the legacy per-insight scans transparently.

    - error_spikes:        [url, w_rate, b_rate, w_errors, w_total, b_total]
    - cache_collapse:      [url, w_rate, b_rate, w_total, b_total]
    - latency_regression:  [url, w_p95, b_p95, w_total, b_total]
    - tail_latency:        [url, p99_ms, p50_ms, ratio, total]
    """
    sql = f"""
    WITH base AS (
        SELECT
            "url",
            status,
            cache,
            elapsed,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
        WHERE "url" IS NOT NULL
    )
    SELECT
        "url",
        -- Common counts
        COUNT(*) FILTER (WHERE is_w) AS w_total,
        COUNT(*) FILTER (WHERE is_b) AS b_total,
        -- error_spikes: 5xx counters
        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_5xx,
        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_5xx,
        -- cache_collapse: cache-hit counters
        SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
        SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
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
              FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p50
    FROM base
    GROUP BY "url"
    HAVING (COUNT(*) FILTER (WHERE is_w) > 0) OR (COUNT(*) FILTER (WHERE is_b) > 0)
    """
    cursor = runner.execute(sql, [window_start_s, window_start_s])

    error_spikes_out: list[tuple] = []
    cache_collapse_out: list[tuple] = []
    latency_regression_out: list[tuple] = []
    tail_latency_out: list[tuple] = []

    while True:
        rows = cursor.fetchmany(10000)
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
                w_lat_total,
                b_lat_total,
                w_p95,
                b_p95,
                w_p99,
                w_p50,
            ) = r

            w_total_i = w_total or 0
            b_total_i = b_total or 0

            # ── error_spikes ──────────────────────────────────────────────────
            # Legacy HAVING: w_total >= 3 AND w_rate >= 0.05
            #                AND (b_total < 10 OR w_rate >= b_rate * 2 + 0.05)
            # ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15
            if w_total_i >= 3:
                w_rate_e = (w_5xx or 0) / w_total_i if w_total_i else 0.0
                b_rate_e = ((b_5xx or 0) / b_total_i) if b_total_i else None
                if w_rate_e >= 0.05 and (b_total_i < 10 or (b_rate_e is not None and w_rate_e >= b_rate_e * 2 + 0.05)):
                    error_spikes_out.append((url, w_rate_e, b_rate_e, w_5xx, w_total, b_total))

            # ── cache_collapse ────────────────────────────────────────────────
            # Legacy HAVING: w_total >= 5 AND b_total >= 20 AND b_rate >= 0.40
            #                AND w_rate <= b_rate - 0.20 AND w_rate <= b_rate * 0.6
            # ORDER BY (b_rate - w_rate) DESC LIMIT 15
            if w_total_i >= 5 and b_total_i >= 20:
                w_rate_c = (w_hits or 0) / w_total_i if w_total_i else 0.0
                b_rate_c = (b_hits or 0) / b_total_i if b_total_i else 0.0
                if b_rate_c >= 0.40 and w_rate_c <= b_rate_c - 0.20 and w_rate_c <= b_rate_c * 0.6:
                    cache_collapse_out.append((url, w_rate_c, b_rate_c, w_total, b_total))

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
                latency_regression_out.append((url, w_p95, b_p95, w_total, b_total))

            # ── tail_latency (window-only) ────────────────────────────────────
            # Legacy WHERE timestamp >= window_start; HAVING COUNT(*) >= 20 AND
            # ratio > 5. ORDER BY ratio DESC LIMIT 15.
            # ratio = p99 / NULLIF(p50, 0)
            if w_lat_total is not None and w_lat_total >= 20 and w_p99 is not None and w_p50 is not None and w_p50 > 0:
                ratio = round(w_p99 / w_p50, 1)
                if ratio > 5:
                    tail_latency_out.append((url, w_p99, w_p50, ratio, w_lat_total))

    error_spikes_out.sort(key=lambda x: -((x[1] or 0) - (x[2] or 0)))
    cache_collapse_out.sort(key=lambda x: -((x[2] or 0) - (x[1] or 0)))
    latency_regression_out.sort(key=lambda x: -((x[1] / x[2]) if x[2] else 0))
    tail_latency_out.sort(key=lambda x: -(x[3] or 0))

    return {
        "error_spikes": error_spikes_out[:15],
        "cache_collapse": cache_collapse_out[:15],
        "latency_regression": latency_regression_out[:15],
        "tail_latency": tail_latency_out[:15],
    }


def get_insights(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    window_hours: float,
    baseline_hours: float,
) -> dict:
    source_name = src["name"]
    table_name = _safe_table(source_name)

    now = datetime.now(UTC)
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(hours=baseline_hours + window_hours)

    now_s = now.isoformat()
    window_start_s = window_start.isoformat()
    baseline_start_s = baseline_start.isoformat()

    cache_bucket = int(time.time() / max(INSIGHTS_CACHE_TTL, 1))
    cache_key = f"{source_name}:{window_hours}:{baseline_hours}:{cache_bucket}"
    if INSIGHTS_CACHE_TTL > 0:
        with _insights_cache_lock:
            entry = _insights_cache.get(cache_key)
        if entry is not None:
            cached = entry[1].copy()
            cached["_is_cached"] = True
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

    # ── Materialize relevant window into temp table ───────────────────────────
    # This is the single most important optimization: avoid globbing/metadata parsing 30+ times.
    temp_table = f"insights_temp_{int(time.time())}"

    # Derive needed_cols from every registered insight's `required_fields` so
    # we never project a temp table that's missing a column some insight's SQL
    # references. (Previously a hard-coded list silently dropped columns like
    # `metro` / `tls_ciphers_sha` / `oretries` / `conn_requests` — the matching
    # insights then 500'd with "Referenced column not found in FROM clause".)
    needed_cols_set: set[str] = {"timestamp"}
    for d in registry.get_all():
        needed_cols_set.update(d.required_fields)
    # Also include support cols that processors read from context but aren't in
    # required_fields (e.g. ja3/ja4 fingerprint selection in botnet_grouping).
    # Geo columns are referenced via build_geo_select_clause when present
    # in the source schema, even though no insight lists `region` directly
    # in its required_fields.
    needed_cols_set.update({"ja3", "ja4", "region"})
    needed_cols = sorted(needed_cols_set)
    cols_sql = ", ".join(f'"{c}"' for c in needed_cols if c in actual_cols)
    if not cols_sql:
        cols_sql = "*"

    create_q = f"CREATE TEMP TABLE {temp_table} AS SELECT {cols_sql} FROM {table_name} WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)"
    if not runner.create_temp_table(create_q, [baseline_start_s, now_s]):
        temp_table = table_name  # Fallback

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

    # Insight definitions
    try:
        from backend.core.log_fields import INSIGHT_DEFINITIONS as _defs

        defs_map = {d["id"]: d for d in _defs}
    except Exception:
        defs_map = {}

    def _def(insight_id: str) -> dict:
        return defs_map.get(insight_id, {})

    def check_baseline(insight_id: str) -> dict | None:
        if available_history_hours < baseline_hours:
            d = _def(insight_id)
            avail = max(0.1, round(available_history_hours, 1))
            return {
                "id": insight_id,
                "title": d.get("title", insight_id.replace("_", " ").title()),
                "description": d.get("description", ""),
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
        p = [("start", window_start_s), ("end", now_s)]
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

    # ── Coalesced city aggregates (O2 bypass) ─────────────────────────────────
    # The 4 city-based insights (city_surges, city_error_spikes,
    # city_latency_regressions, new_city_traffic) each issued their own
    # GROUP BY (city, region, country) scan of the temp table. On prod
    # 2026-06-05 those four scans were 177+205+219+181 = 782 ms of pure
    # duplication — every row read four times to compute counts/rates/p95s
    # that fit naturally in a single SELECT. Run one pass here and reuse
    # the per-(city, region, country) aggregate rows below; each insight
    # task short-circuits via `city_precomputed` instead of issuing its
    # own SELECT.
    #
    # Only fires when ALL 4 are eligible (city + status + elapsed + timestamp
    # all in schema). When a service is missing one of those columns the
    # per-insight scans still run for the eligible subset.
    city_precomputed: dict[str, list[tuple]] = {}
    if "city" in actual_cols and "status" in actual_cols and "elapsed" in actual_cols and "timestamp" in actual_cols:
        try:
            city_precomputed = _coalesced_city_aggregates(
                runner,
                table_name,
                window_start_s,
                label_expr,
                region_sel,
                country_sel,
                window_hours,
                baseline_hours,
            )
        except Exception as e:
            # Fall back transparently to per-insight scans; never break
            # the page on a coalesced-path bug.
            import logging

            logging.getLogger(__name__).warning("[insights] coalesced city aggregates failed, falling back: %s", e)
            city_precomputed = {}

    # ── Coalesced URL aggregates (Step 2 / Option C, 2026-06-06) ─────────────
    # 4 URL-keyed insights (error_spikes, cache_collapse, latency_regression,
    # tail_latency) all GROUP BY url over the same WHERE clause with the same
    # is_w/is_b baseline-vs-window split. Pre-coalesce, each ran its own scan
    # of the temp table; the audit showed they totalled ~400-600 ms. Coalescing
    # them mirrors O2's city pattern (proven ~520 ms save on prod).
    #
    # origin_latency_spike is the 5th url-keyed insight but its SQL has an
    # overall_stats CTE that normalizes against the entire population's p95
    # — different shape, kept on its own template.
    #
    # Fires only when all the columns the CTE touches are present (url,
    # status, cache, elapsed, timestamp). When a service is missing any of
    # them the per-insight scans run normally for whichever subset is
    # eligible. Failure transparently falls back to per-insight scans —
    # never blocks the page.
    url_precomputed: dict[str, list[tuple]] = {}
    if (
        "url" in actual_cols
        and "status" in actual_cols
        and "cache" in actual_cols
        and "elapsed" in actual_cols
        and "timestamp" in actual_cols
    ):
        try:
            url_precomputed = _coalesced_url_aggregates(runner, table_name, window_start_s)
        except Exception as e:
            import logging

            logging.getLogger(__name__).warning("[insights] coalesced URL aggregates failed, falling back: %s", e)
            url_precomputed = {}

    for definition in registry.get_all():
        # Check if all required fields are present
        if not all(col in actual_cols for col in definition.required_fields):
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
                else:
                    try:
                        sql = d.sql_template.format(
                            table_name=table_name,
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
                            **extra_args,
                        )
                    except KeyError:
                        # If hydration fails due to missing keys (e.g. pop_values), skip this insight
                        return None

                    param_count = sql.count("?")
                    params = [window_start_s] * param_count

                    rows = runner.execute(sql, params).fetchall()
                items = []
                if d.row_processor:
                    # Build context for processors
                    context = {
                        "window_hours": window_hours,
                        "baseline_hours": baseline_hours,
                        "fp_col": fp_col,
                        "actual_cols": actual_cols,
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
            return compute_insight

        tasks.append(_make_task())

    insights_list: list[dict] = []
    for fn in tasks:
        try:
            res = fn()
            if res:
                insights_list.append(res)
        except Exception as e:
            insight_id = getattr(fn, "_insight_id", "unknown")
            insight_title = getattr(fn, "_insight_title", insight_id.replace("_", " ").title())
            insights_list.append(
                {
                    "id": insight_id,
                    "title": insight_title,
                    "severity": "error",
                    "summary": f"Query failed: {str(e)}",
                    "description": "",
                    "items": [],
                }
            )

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
