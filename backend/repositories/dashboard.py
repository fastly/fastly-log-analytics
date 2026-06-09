"""Dashboard repository — pure SQL functions, no HTTP imports."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    CANONICAL_METRICS,
    QueryRunner,
    _get_schema,
    _safe_table,
    get_source_extent,
    percentile_ms_expr,
    safe_interval,
    safe_iso,
    time_bucket_select,
)
from backend.repositories.utils.filters import build_where_clause, resolve_col
from backend.repositories.utils.pagination import calc_offset

# ── In-memory caches ──────────────────────────────────────────────────────────
# Bounded + actively-reaped: dashboard responses can be 30-240MB per entry,
# and diverse filter/time-range/interval combinations mint a distinct key
# each. The previous plain-dict version had a 30s TTL but only checked it
# on hit — stale entries were never evicted, so the cache grew unboundedly
# across hours of dashboard use (a primary OOM contributor on the 16GB VM).
# 500 entries × ~30MB = ~15GB worst case; in practice the working set is
# much smaller, but the cap is a hard backstop.
from backend.utils.bounded_cache import BoundedTTLCache

# Dashboard response cache disabled.
#
# Symptom: a transient empty result (sync mid-commit, iceberg view rebuild in
# flight, brief view-rebind race) used to land in this cache and then serve
# "No data available" to every dashboard request with the same key for the
# next 30 seconds — across all tabs, auto-refreshes, and any analyst hitting
# the same window. Observed in prod 2026-06-09: dashboard showed empty for
# every service even though `Latest Log: 7s ago` in the header.
#
# Set to 0 to make both the read gate at `if DASHBOARD_CACHE_TTL > 0:` and
# the write gate inert without removing the surrounding code (easy to revert
# or replace with a less-aggressive policy later — e.g. only cache when
# total_rows > 0, or only cache windows ending more than 5 min in the past).
DASHBOARD_CACHE_TTL = 0  # seconds; 0 disables read+write
_dashboard_cache: BoundedTTLCache = BoundedTTLCache(maxsize=500, ttl_seconds=max(DASHBOARD_CACHE_TTL, 1))


# ── aggregates ────────────────────────────────────────────────────────────────

from backend.core.log_fields import LOG_FIELD_CATALOG

# Virtual fields are catalog ids whose value is computed by exploding a
# real backing column (CSV string) into individual rows via DuckDB's
# unnest(string_split(...)). They live in the FIELDS list so the dashboard
# top-N machinery picks them up, but the cross-cutting loops below skip
# them in batch-stats / column-need passes (their backing column is what
# actually goes into the temp table).
_VIRTUAL_FIELDS = ("waf_sig_ind", "edge_score_reason_ind")
FIELDS = [f["id"] for f in LOG_FIELD_CATALOG if f["id"] != "_source_file"] + list(_VIRTUAL_FIELDS)


def _add_bot_columns(actual_cols: set[str], columns: list[str], select_cols: list[str]) -> tuple[bool, bool]:
    """Ensure UA + IP (Arcjet) or waf_req_id (NGWAF) columns are in select_cols
    when the caller requested the virtual `_bot_name` / `_ngwaf_bot_name` fields.

    Mutates `select_cols` in place. Returns (wants_bot, wants_ngwaf_bot).
    """
    wants_bot = "_bot_name" in columns
    wants_ngwaf_bot = "_ngwaf_bot_name" in columns
    if wants_bot:
        if "ua" in actual_cols and '"ua"' not in select_cols:
            select_cols.append('"ua"')
        if "ip" in actual_cols and '"ip"' not in select_cols:
            select_cols.append('"ip"')
    if wants_ngwaf_bot and "waf_req_id" in actual_cols and '"waf_req_id"' not in select_cols:
        select_cols.append('"waf_req_id"')
    return wants_bot, wants_ngwaf_bot


def get_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    chart_interval: str,
    chart_metric: str,
) -> dict:
    source_name = src["name"]
    table_name = _safe_table(source_name)

    lf_config = src.get("log_fields") or {}
    _custom_field_names = [
        cf["name"]
        for cf in lf_config.get("custom_fields", [])
        if cf.get("enabled", True) and cf.get("show_in_dashboard", True)
    ]
    fields = FIELDS + _custom_field_names

    _key_payload = json.dumps(
        {
            "s": start_time,
            "e": end_time,
            "f": {k: (v.mode, sorted(str(x) for x in v.values)) for k, v in sorted(filters.items())},
            "ci": chart_interval,
            "cm": chart_metric,
        },
        separators=(",", ":"),
    )
    cache_key = hashlib.sha256(f"{_key_payload}:{source_name}".encode()).hexdigest()
    now = time.time()
    if DASHBOARD_CACHE_TTL > 0:
        # BoundedTTLCache's ``__contains__`` / ``[]`` already enforce TTL
        # internally, so an entry that reads as present is by definition
        # still fresh — no need for the legacy ``now - cached_at`` check.
        cached_entry = _dashboard_cache.get(cache_key)
        if cached_entry is not None:
            cached_at, cached_res = cached_entry
            cached_res = cached_res.copy()
            # Pydantic field name is ``is_cached``; the response model renames
            # it to ``_is_cached`` on serialization via serialization_alias
            # (mirrors the section_timings pattern below at line 654). Passing
            # ``_is_cached`` here gets dropped because Pydantic only matches
            # the unaliased name — the cached response was silently returning
            # ``"_is_cached": false`` in JSON, masking every cache hit.
            cached_res["is_cached"] = True
            return cached_res

    # Per-phase wall-clock timing surfaces in the response under
    # _section_timings so we can attribute the cold dashboard wall
    # without re-running ad-hoc instrumentation. Matches the
    # bootstrap.py pattern. Negligible overhead (perf_counter is ~50ns).
    section_timings: list[dict] = []

    def _timed(name: str, fn):
        t0 = time.perf_counter()
        try:
            return fn()
        finally:
            section_timings.append({"section": name, "time_ms": round((time.perf_counter() - t0) * 1000, 2)})

    runner = QueryRunner(con, src)
    interval = "1 minute"

    actual_cols = _timed("get_schema_cols", runner.get_schema_cols)
    if not actual_cols:
        empty = {f: {"top": [], "total": 0} for f in fields}
        return {
            "data": empty,
            "time_series": [],
            "map_data": [],
            "where_clause": "1=1",
            "interval": interval,
            "metric": "requests",
            "total_rows": 0,
            "total_rows_total": 0,
            **runner.telemetry(),
        }

    params, where_clause = _timed(
        "build_where_clause",
        lambda: build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True),
    )
    # Iceberg handles partition pruning natively via hidden partitioning — no manual file enumeration needed.

    # Build temp table with only needed columns
    needed_cols: set[str] = set()
    if "timestamp" in actual_cols:
        needed_cols.add('"timestamp"')
    for field in fields:
        if field in _VIRTUAL_FIELDS:
            # Virtual fields are exploded from a backing column further
            # down; make sure the backing column is in the temp table.
            if field == "waf_sig_ind" and "waf_sig" in actual_cols:
                needed_cols.add('"waf_sig"')
            elif field == "edge_score_reason_ind" and "edge_score_reason" in actual_cols:
                needed_cols.add('"edge_score_reason"')
            continue
        if field in actual_cols:
            needed_cols.add(f'"{field}"')

    for mc in [
        "resp_bytes",
        "elapsed",
        "status",
        "cache",
        "status",
        "resp_state",
        "req_header_bytes",
        "req_bytes",
        "ttfb",
        "server_region",
        "tls_ciphers_sha",
        "is_ipv6",
        "conn_requests",
    ]:
        if mc in actual_cols:
            needed_cols.add(f'"{mc}"')

    cols_str = ", ".join(needed_cols) if needed_cols else "*"
    # Only take the rollup fast-path when no filters AND a populated
    # rollups tree actually exists on disk. Without the existence check
    # the dashboard routed unfiltered queries to execute_top_n_rollups
    # on services where the initial backfill hadn't completed (or in
    # tests with no rollups built), producing an empty top-N — the field
    # totals stayed at their zero-initialisers since the populate loop
    # is gated on a non-empty top-N. Witnessed in
    # test_get_aggregates_with_data 2026-06-04: 60 mock logs seeded,
    # field_totals["url"] computed correctly via Q2, but results["url"]
    # ["total"] stuck at 0 because no rollup row arrived to trigger the
    # populate path. The temp-table fallback always populates totals.
    from backend.core.duckdb import _cache_dir as _cache_dir_for_rollups

    rollup_dir = os.path.join(_cache_dir_for_rollups(src), "rollups", "hour")
    use_rollups = not filters and os.path.isdir(rollup_dir)
    # Note on freshness when use_rollups=True: the per-field top-N IS
    # current. execute_top_n_rollups (backend/repositories/_base.py:432)
    # excludes the active hour from its rollup-file enumeration AND
    # runs a separate execute_top_n_batch query on the live base table
    # for the active hour, then merges the two via a combined dict
    # before truncating to top-N. So the current hour's contribution is
    # not lost — it joins the merge from the live side. The narrow
    # live_temp built below is for OTHER queries (time_series, signal
    # unnests, conn_requests histogram) that don't go through the
    # rollup path.

    # `temp_table` ends up holding the per-request materialization (if
    # any) so the `finally` cleanup at the bottom of the function can
    # DROP it regardless of which branch built it.
    temp_table: str | None = None
    if use_rollups:
        table_name = _safe_table(source_name)
        # Plan item 14 — live-hour TEMP TABLE on the rollup path.
        # Without this, the rollup branch fires FOUR separate parquet
        # scans for the window-scan sub-queries that the rollups don't
        # cover: total_rows COUNT, the two signal-unnest queries
        # (waf_sig + edge_score_reason), conn_requests bucket, and the
        # time_series chart. Each is independent on the base table.
        # Materializing the filtered window once amortizes the parquet
        # scan + manifest read across all of them. `execute_top_n_rollups`
        # below reads from disk directly and is unaffected.
        #
        # NARROW projection: on the rollup path the per-field top-N
        # comes from execute_top_n_rollups (reads rollup parquet
        # directly), so the live TEMP TABLE only needs the columns
        # consumed by the four window-scan branches: waf_sig +
        # edge_score_reason for signal unnest, conn_requests for the
        # connection-reuse histogram, timestamp for time_series, plus
        # the chart_metric helper cols. A WIDE projection (matching
        # cols_str) made TEMP TABLE materialization itself the
        # bottleneck (~1.4s on a populated 24h window) and erased the
        # savings. The narrow set keeps materialization under ~400ms.
        narrow: list[str] = []
        for c in (
            "waf_sig",
            "edge_score_reason",
            "conn_requests",
            "timestamp",
            "cache",
            "elapsed",
            "status",
            "resp_bytes",
            "req_header_bytes",
            "req_bytes",
            "ttfb",
            "resp_state",
            # `country` is consumed by the map_data fallback below
            # (line ~564). The rollup derives map_data from all_top_res
            # when country is in the top-N field set AND has rows for
            # the window, but if either condition fails it falls back
            # to a `SELECT "country" ... FROM table_name` against the
            # narrow temp. Without `country` here, that fallback raises
            # BinderException and the dashboard renders empty.
            "country",
        ):
            if c in actual_cols:
                narrow.append(f'"{c}"')
        narrow_cols_str = ", ".join(narrow) if narrow else "*"
        live_temp = f"t_live_hour_{uuid.uuid4().hex}"
        sql = f"CREATE TEMP TABLE {live_temp} AS SELECT {narrow_cols_str} FROM {table_name} WHERE {where_clause}"
        if _timed("live_temp_create", lambda: runner.create_temp_table(sql, params)):
            table_name = live_temp
            where_clause = "1=1"
            params = []
            temp_table = live_temp
        # If the live-hour TEMP TABLE creation fails (e.g. stale view),
        # fall back transparently to per-query base-table scans. Slower
        # but functionally correct.
    else:
        # Use TEMP TABLE instead of TEMP VIEW to materialize the filtered results in memory.
        # This prevents DuckDB from re-scanning the underlying files for every branch of the UNION ALL.
        temp_table = f"t_{uuid.uuid4().hex}"
        sql = f"CREATE TEMP TABLE {temp_table} AS SELECT {cols_str} FROM {table_name} WHERE {where_clause}"
        if not _timed("wide_temp_create", lambda: runner.create_temp_table(sql, params)):
            empty = {f: {"top": [], "total": 0} for f in fields}
            return {
                "data": empty,
                "time_series": [],
                "map_data": [],
                "where_clause": "1=1",
                "interval": interval,
                "metric": "requests",
                "total_rows": 0,
                "total_rows_total": 0,
                **runner.telemetry(),
            }
        # All subsequent queries use the temp table
        table_name = temp_table
        where_clause = "1=1"
        params = []

    results: dict[str, Any] = {f: {"top": [], "total": 0} for f in fields}

    try:
        field_totals: dict[str, int] = {}
        total_rows = 0
        earliest_log_at = None
        latest_log_at = None

        if use_rollups:
            # When the rollup fast-path is active, skip the wide per-column
            # COUNT entirely. Two reasons it dominated wall time before:
            #   1. 72 count(col) calls in one statement force DuckDB to
            #      touch every column for every row in the window — ~1s on
            #      prod's 24h × 3M-row view (witnessed 2026-06-04: Q2 was
            #      1063ms of a 3194ms dashboard).
            #   2. The output of all 72 counts is reconstructible from the
            #      rollup query's (field, value, count) rows: SUM by field
            #      across the result IS field_totals[field] for any field
            #      the user displays. We pay for it once via the rollup
            #      read instead of twice.
            #
            # Caveat: TOP_K per (field, hour) caps the rollup to the 500
            # most-frequent values per hour. For high-cardinality fields
            # (timestamp at per-second granularity, or unique-per-request
            # ids) the SUM under-counts vs the true non-null count. In
            # practice the dashboard shows top-10 with their percentages;
            # mild under-counting of the denominator is acceptable for
            # the perf win. If we ever need exact per-field totals here,
            # add a `__total__` aggregate row to each rollup parquet.
            try:
                total_rows = runner.execute(
                    f"SELECT {CANONICAL_METRICS['requests']} FROM {table_name} WHERE {where_clause}", params
                ).fetchone()[0]
            except Exception:
                total_rows = 0
        else:
            # Non-rollups path keeps the wide COUNT — we have the
            # filtered temp table loaded; one combined scan is cheaper
            # than re-counting per field downstream.
            count_cols: list[str] = [CANONICAL_METRICS["requests"]]
            valid_fields: list[str] = []
            for field in fields:
                if field in _VIRTUAL_FIELDS:
                    continue
                if field in actual_cols:
                    count_cols.append(f"count({resolve_col(field, actual_cols)})")
                    valid_fields.append(field)
            if count_cols:
                count_res = runner.execute(
                    f"SELECT {', '.join(count_cols)} FROM {table_name} WHERE {where_clause}", params
                ).fetchone()
                total_rows = count_res[0]
                for i, field in enumerate(valid_fields):
                    field_totals[field] = count_res[i + 1]

        orig_table_name = _safe_table(source_name)
        total_rows_total, earliest_log_at, latest_log_at = _timed(
            "source_extent", lambda: get_source_extent(runner, src, orig_table_name)
        )

        schema_types = _timed("schema_types", lambda: {col["name"]: col["type"] for col in _get_schema(con, src)})

        # When use_rollups=True, field_totals is empty here — populate it
        # below from the rollup query results. Use the full eligible field
        # list (anything non-virtual + in schema) as batch_fields; the
        # rollup helper silently skips fields it has no data for.
        if use_rollups:
            batch_fields = [f for f in fields if f not in _VIRTUAL_FIELDS and f in actual_cols]
        else:
            batch_fields = [f for f in fields if f not in _VIRTUAL_FIELDS and f in field_totals]
        if use_rollups:
            # Bump country's per-field limit to 500 so the map_data path
            # below can use the same call's results — eliminates the
            # second execute_top_n_rollups invocation that was costing
            # ~200-250ms per request (one full active-hour temp + rollup
            # parquet scan duplicated for one low-cardinality field).
            # Other fields stay at limit=10. Make sure country is in the
            # field list — it normally is via FIELDS, but the explicit
            # add guards a future change to FIELDS.
            _batch_with_country = batch_fields if "country" in batch_fields else batch_fields + ["country"]
            all_top_res, field_order = _timed(
                "top_n_rollups",
                lambda: runner.execute_top_n_rollups(
                    _batch_with_country,
                    start_time,
                    end_time,
                    limit=10,
                    per_field_limits={"country": 500},
                    _phase_log=section_timings,
                ),
            )
            # Derive field_totals from the rollup result (cheap Python sum).
            # Each row is (field, value, count); per-field sum = total of
            # values covered by the top-K rollup for that field.
            # NOTE: country now has up to 500 entries; that inflates
            # field_totals[country] but the panel only shows top-10 so
            # the user-visible total is unchanged after the slice below.
            for f_name, _f_val, f_count in all_top_res:
                field_totals[f_name] = field_totals.get(f_name, 0) + int(f_count)
        else:
            all_top_res, field_order = _timed(
                "top_n_batch",
                lambda: runner.execute_top_n_batch(batch_fields, table_name, actual_cols, schema_types, limit=10),
            )

        if all_top_res:
            # Group results back by field
            for field in field_order:
                results[field] = {"top": [], "total": field_totals.get(field, 0)}

            # Prepare to resolve ASN names if 'asn' is present
            asn_list = []
            for f_name, f_val, f_count in all_top_res:
                if f_name == "asn" and f_val is not None and str(f_val).isdigit():
                    asn_list.append(int(f_val))

            asn_names = {}
            if asn_list:
                from backend.core import duckdb as _db

                asn_names = _timed("asn_names_lookup", lambda: _db.get_asn_names(src["name"], asn_list))

            # Per-panel cap at 10. execute_top_n_rollups may return more
            # than 10 for fields with per_field_limits (e.g. country=500
            # for the choropleth); the panel UI only renders 10, so cap
            # the append here. Other fields stay at <=10 naturally.
            _PANEL_LIMIT = 10
            _panel_count: dict[str, int] = {}
            for f_name, f_val, f_count in all_top_res:
                if _panel_count.get(f_name, 0) >= _PANEL_LIMIT:
                    continue
                entry = {"value": f_val, "count": f_count}
                if f_name == "asn" and f_val is not None and str(f_val).isdigit():
                    from backend.core import duckdb as _db

                    asn_int = int(f_val)
                    entry["label"] = _db.format_asn_label(asn_int, asn_names.get(asn_int, ""))

                results[f_name]["top"].append(entry)
                _panel_count[f_name] = _panel_count.get(f_name, 0) + 1

        # Virtual fields: explode comma-separated CSV columns into individual
        # rows via unnest(string_split(...)). Generalized helper handles both
        # waf_sig_ind (backed by waf_sig) and edge_score_reason_ind (backed
        # by edge_score_reason) — same pattern, different backing columns.
        def _exploded_top_n(virtual_id: str, backing_col: str) -> None:
            if virtual_id not in fields:
                return
            if backing_col not in actual_cols:
                results[virtual_id] = {"top": [], "total": 0}
                return
            q = f"""
                WITH split_data AS (
                    SELECT trim(signal) AS signal
                    FROM (
                        SELECT unnest(string_split("{backing_col}", ',')) AS signal
                        FROM {table_name}
                        WHERE "{backing_col}" IS NOT NULL AND "{backing_col}" != '' AND {where_clause}
                    )
                    WHERE trim(signal) != ''
                ),
                total_count AS (SELECT {CANONICAL_METRICS["requests"]} AS tc FROM split_data),
                top_values AS (
                    SELECT signal AS value, {CANONICAL_METRICS["requests"]} AS c
                    FROM split_data GROUP BY 1 ORDER BY 2 DESC LIMIT 10
                )
                SELECT tv.value, tv.c, tc.tc FROM top_values tv CROSS JOIN total_count tc
            """
            res = runner.execute(q).fetchall()
            if res:
                results[virtual_id] = {
                    "top": [{"value": r[0], "count": r[1]} for r in res],
                    "total": res[0][2],
                }
            else:
                results[virtual_id] = {"top": [], "total": 0}

        _timed("waf_sig_ind_explode", lambda: _exploded_top_n("waf_sig_ind", "waf_sig"))
        _timed("edge_score_reason_ind_explode", lambda: _exploded_top_n("edge_score_reason_ind", "edge_score_reason"))

        # Special handling for conn_requests (bucketed histogram)
        t_conn_req_0 = time.perf_counter()
        if "conn_requests" in actual_cols:
            q = f"""
                SELECT
                    CASE
                        WHEN "conn_requests" = 1 THEN '1'
                        WHEN "conn_requests" BETWEEN 2 AND 5 THEN '2–5'
                        WHEN "conn_requests" BETWEEN 6 AND 20 THEN '6–20'
                        ELSE '21+'
                    END AS bucket,
                    {CANONICAL_METRICS["requests"]} AS c
                FROM {table_name}
                WHERE "conn_requests" IS NOT NULL AND "conn_requests" > 0 AND {where_clause}
                GROUP BY 1
                ORDER BY MIN("conn_requests")
            """
            res = runner.execute(q).fetchall()
            total_conn = sum(r[1] for r in res)
            results["conn_requests"] = {
                "top": [{"value": r[0], "count": r[1]} for r in res],
                "total": total_conn,
            }
        else:
            results["conn_requests"] = {"top": [], "total": 0}
        section_timings.append(
            {"section": "conn_requests", "time_ms": round((time.perf_counter() - t_conn_req_0) * 1000, 2)}
        )

        # Time series
        t_ts_0 = time.perf_counter()
        time_series: list[dict] = []
        chart_metric_out = "requests"
        if "timestamp" in actual_cols:
            interval = safe_interval(chart_interval, default=interval)

            sql_cache = resolve_col("cache", actual_cols)
            sql_elapsed = resolve_col("elapsed", actual_cols)

            # Time-series rollup fast path. Serves the chart from per-hour
            # 1-minute pre-aggregated parquets when the metric + interval are
            # rollup-supported and no row-level filters are active. The
            # `use_rollups` gate already encodes "no filters" — reusing it
            # keeps the two paths consistent. Falls back transparently to the
            # raw branches below when the reader returns None.
            rollup_metric_ok = chart_metric in QueryRunner._TS_ROLLUP_METRIC_SQL
            rollup_col_ok = (
                chart_metric == "requests"
                or (chart_metric in ("5xx", "4xx") and "status" in actual_cols)
                or (chart_metric == "hit_rate" and "cache" in actual_cols)
            )
            if use_rollups and rollup_metric_ok and rollup_col_ok:
                t_ts_rollup_0 = time.perf_counter()
                rollup_series = runner.try_time_series_from_rollup(
                    chart_metric=chart_metric,
                    interval=interval,
                    start_time=start_time,
                    end_time=end_time,
                    table_name=table_name,
                    where_clause=where_clause,
                    params=params,
                )
                section_timings.append(
                    {
                        "section": "time_series:rollup_attempt",
                        "time_ms": round((time.perf_counter() - t_ts_rollup_0) * 1000, 2),
                    }
                )
                if rollup_series is not None:
                    time_series = rollup_series
                    chart_metric_out = chart_metric
                    # Skip the raw chart branches below — the rollup served it.
                    # All other aggregations (top-N, signal unnest, etc.) still
                    # run on the temp table; only the chart is short-circuited.
                    _skip_raw_time_series = True
                else:
                    _skip_raw_time_series = False
            else:
                _skip_raw_time_series = False

            if _skip_raw_time_series:
                pass
            elif chart_metric == "5xx" and "status" in actual_cols:
                chart_metric_out = "5xx"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["5xx_rate"]} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "4xx" and "status" in actual_cols:
                chart_metric_out = "4xx"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["4xx_rate"]} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "hit_rate" and ("cache" in actual_cols or "resp_state" in actual_cols):
                chart_metric_out = "hit_rate"
                # Fallback to resp_state if cache is missing
                cache_col = '"cache"' if "cache" in actual_cols else '"resp_state"'
                hit_rate_expr = CANONICAL_METRICS["hit_rate"].format(cache_col=cache_col)
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {hit_rate_expr} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric.endswith("_latency") and ("elapsed" in actual_cols or "elapsed_us" in actual_cols):
                chart_metric_out = chart_metric
                percentile = 0.95
                if chart_metric.startswith("p50"):
                    percentile = 0.50
                elif chart_metric.startswith("p99"):
                    percentile = 0.99
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {percentile_ms_expr(sql_elapsed, percentile)} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL AND {sql_elapsed} IS NOT NULL AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "throughput" and "resp_bytes" in actual_cols and "elapsed" in actual_cols:
                chart_metric_out = "throughput"
                sql_resp_bytes = resolve_col("resp_bytes", actual_cols)
                # Note: elapsed and elapsed_us both map to the same field in DuckDB (µs)
                sql_elapsed_val = resolve_col("elapsed", actual_cols)
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["throughput"].format(cache_col=sql_cache, elapsed_col=sql_elapsed_val, resp_bytes_col=sql_resp_bytes)} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "req_size" and any(c in actual_cols for c in ["req_header_bytes", "req_bytes"]):
                chart_metric_out = "req_size"
                header_col = '"req_header_bytes"' if "req_header_bytes" in actual_cols else "0"
                body_col = resolve_col("req_bytes", actual_cols) if "req_bytes" in actual_cols else "0"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["req_size"].format(header_bytes_col=header_col, req_bytes_col=body_col)} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "ttfb" and "ttfb" in actual_cols:
                chart_metric_out = "ttfb"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["ttfb_ms"]} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """
            else:
                chart_metric_out = "requests"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["requests"]} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """

            if not _skip_raw_time_series:
                ts_res = runner.execute(ts_q, params).fetchall()
                for r in ts_res:
                    if r[0] is None:
                        continue
                    pt: dict[str, Any] = {"time": safe_iso(r[0]), "value": float(r[1]) if r[1] is not None else 0.0}
                    if len(r) >= 3 and r[2] is not None:
                        pt["category"] = str(r[2])
                    time_series.append(pt)
        section_timings.append({"section": "time_series", "time_ms": round((time.perf_counter() - t_ts_0) * 1000, 2)})

        # Map data
        t_map_0 = time.perf_counter()
        map_data: list[dict] = []
        if "country" in actual_cols:
            if use_rollups:
                # Derive map_data directly from all_top_res. The batch call
                # above passed per_field_limits={"country": 500} so the
                # rollup+live merge already produced up to 500 country
                # entries — no need for a second execute_top_n_rollups
                # call. Saves ~200-250ms per request (one full active-hour
                # temp + rollup parquet scan for one low-cardinality field).
                country_counts: dict[str, int] = {}
                for f_name, f_val, f_count in all_top_res:
                    if f_name == "country" and f_val is not None:
                        country_counts[f_val] = country_counts.get(f_val, 0) + int(f_count)
                map_data = [{"country": k, "count": v} for k, v in country_counts.items()]
            else:
                # Non-rollup path runs over the full filtered temp table.
                map_q = f"""
                    SELECT "country" AS country, {CANONICAL_METRICS["requests"]} AS count
                    FROM {table_name}
                    WHERE "country" IS NOT NULL AND {where_clause}
                    GROUP BY 1
                """
                map_data = [{"country": r[0], "count": r[1]} for r in runner.execute(map_q, params).fetchall()]
        section_timings.append({"section": "map_data", "time_ms": round((time.perf_counter() - t_map_0) * 1000, 2)})

        payload: dict[str, Any] = {
            "data": results,
            "time_series": time_series,
            "map_data": map_data,
            "where_clause": where_clause,
            "interval": interval,
            "metric": chart_metric_out,
            "total_rows": total_rows,
            "total_rows_total": total_rows_total,
            "earliest_log_at": earliest_log_at,
            "latest_log_at": latest_log_at,
            # Pydantic field name is `section_timings`; the response model
            # renames it to `_section_timings` on serialization via
            # serialization_alias. Passing `_section_timings` here gets
            # dropped because Pydantic only matches the unaliased name.
            "section_timings": section_timings,
            **runner.telemetry(),
        }
        if DASHBOARD_CACHE_TTL > 0:
            _dashboard_cache[cache_key] = (now, payload)
        return payload

    finally:
        # Covers both the non-rollup TEMP TABLE and the rollup-path
        # live-hour TEMP TABLE (item 14). When TEMP TABLE creation
        # failed and `temp_table` is None, this is a no-op.
        if temp_table is not None:
            try:
                con.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except Exception:
                pass


# ── raw ───────────────────────────────────────────────────────────────────────


def get_raw(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    page: int,
    limit: int,
    sort_col: str | None,
    sort_dir: str,
    columns: list[str],
) -> dict:
    runner = QueryRunner(con, src)

    table_name = _safe_table(src["name"])
    offset = calc_offset(page, limit)

    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        return {
            "columns": [],
            "data": [],
            "total_rows": 0,
            "total_rows_total": 0,
            "page": page,
            "limit": limit,
            **runner.telemetry(),
        }

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols)

    order_clause = ""
    if sort_col and sort_col in actual_cols:
        order_clause = f'ORDER BY "{sort_col}" {sort_dir}'
    elif "timestamp" in actual_cols:
        order_clause = f"ORDER BY timestamp {sort_dir}"

    if columns:
        select_cols = [f'"{c}"' for c in columns if c in actual_cols]
        if sort_col and sort_col in actual_cols and f'"{sort_col}"' not in select_cols:
            select_cols.append(f'"{sort_col}"')
        if "timestamp" in actual_cols and '"timestamp"' not in select_cols:
            select_cols.append('"timestamp"')

        wants_bot, wants_ngwaf_bot = _add_bot_columns(actual_cols, columns, select_cols)

        select_clause = ", ".join(select_cols)
    else:
        wants_bot = True  # By default, calculate bot data if possible
        wants_ngwaf_bot = True
        select_clause = "*"

    q = f"SELECT {select_clause} FROM {table_name} WHERE {where_clause} {order_clause} LIMIT {limit} OFFSET {offset}"

    result = runner.execute_with_retry(q, params)
    if result is None:
        return {
            "columns": columns or [],
            "data": [],
            "total_rows": 0,
            "total_rows_total": 0,
            "page": page,
            "limit": limit,
            **runner.telemetry(),
        }
    df = result.fetchdf()

    if wants_bot or wants_ngwaf_bot:
        from backend.utils.bot_sources import enrich_bot_metadata

        enrich_bot_metadata(df)

    total_rows = len(df)
    total_rows_total = 0
    earliest_log_at = None
    latest_log_at = None

    col_names = list(df.columns)

    if wants_bot and "_bot_name" not in col_names and "_bot_name" in columns:
        df["_bot_name"] = "null"
        col_names.append("_bot_name")

    if wants_ngwaf_bot and "_ngwaf_bot_name" not in col_names and "_ngwaf_bot_name" in (columns or []):
        df["_ngwaf_bot_name"] = None
        col_names.append("_ngwaf_bot_name")

    records = json.loads(df.to_json(orient="records", date_format="iso"))

    if columns:
        filtered_records = []
        for r in records:
            filtered_records.append({c: r.get(c, "null") for c in columns})
        records = filtered_records
        col_names = columns

    # Total-rows + extent come from get_source_extent which itself
    # prefers the cached config status (populated by the sync cron) and
    # only falls back to a live aggregate when the cache is missing.
    # The previous inline COUNT/min/max scanned the whole Iceberg
    # manifest on every dashboard mount — get_source_extent caches the
    # warm path and skips the scan entirely in steady state.
    try:
        total_rows_total, earliest_log_at, latest_log_at = get_source_extent(runner, src, table_name)
    except Exception:
        pass

    return {
        "columns": col_names,
        "data": records,
        "total_rows": total_rows,
        "total_rows_total": total_rows_total,
        "page": page,
        "limit": limit,
        "earliest_log_at": earliest_log_at,
        "latest_log_at": latest_log_at,
        **runner.telemetry(),
    }


def get_raw_df(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int,
    columns: list[str],
):
    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        import pandas as pd

        return pd.DataFrame()

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols)

    if columns:
        select_cols = [f'"{c}"' for c in columns if c in actual_cols]
        wants_bot, wants_ngwaf_bot = _add_bot_columns(actual_cols, columns, select_cols)
        select_clause = ", ".join(select_cols)
    else:
        wants_bot = True  # By default, calculate bot data if possible
        wants_ngwaf_bot = True
        select_clause = "*"

    q = f"SELECT {select_clause} FROM {table_name} WHERE {where_clause} ORDER BY timestamp DESC LIMIT {limit}"
    df = runner.execute(q, params).fetchdf()

    if wants_bot or wants_ngwaf_bot:
        from backend.utils.bot_sources import enrich_bot_metadata

        enrich_bot_metadata(df)

    if columns:
        # Keep only requested columns in order
        existing_cols = [c for c in columns if c in df.columns]
        if "_bot_name" in columns and "_bot_name" not in existing_cols:
            df["_bot_name"] = "null"
            existing_cols.append("_bot_name")
        if "_ngwaf_bot_name" in columns and "_ngwaf_bot_name" not in existing_cols:
            df["_ngwaf_bot_name"] = None
            existing_cols.append("_ngwaf_bot_name")
        df = df[existing_cols]

    return df


# ── field_values ──────────────────────────────────────────────────────────────


def get_field_values(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    field: str,
    search: str,
    limit: int,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    runner = QueryRunner(con, src)

    clean_field = "".join(ch for ch in field if ch.isalnum() or ch == "_")
    if not clean_field:
        raise ValueError("Invalid field name")

    table_name = _safe_table(src["name"])

    # Try top-values cache first (no-search path only)
    if not search:
        try:
            from backend.core.duckdb import _cache_dir

            cache_path = os.path.join(_cache_dir(src), "top_values.json")
            if os.path.exists(cache_path):
                with open(cache_path) as f:
                    top_values = json.load(f)
                if clean_field in top_values:
                    vals = top_values[clean_field][:limit]
                    if clean_field == "asn":
                        from backend.core.duckdb import enrich_asn_labels

                        enrich_asn_labels(vals, src["name"])
                    return {"values": vals, "field": field, **runner.telemetry()}
        except Exception:
            pass

    # Verify table exists
    try:
        exists = (
            runner.execute(
                f"SELECT {CANONICAL_METRICS['requests']} FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
            > 0
        )
        if not exists:
            return {"values": [], "field": field, **runner.telemetry()}
    except Exception:
        return {"values": [], "field": field, **runner.telemetry()}

    actual_cols = runner.get_schema_cols()

    # Exclude the field's own filter so picker shows all available values
    filters_excl = {k: v for k, v in filters.items() if k != field}
    params, where_clause = build_where_clause(start_time, end_time, filters_excl, actual_cols)

    if field == "_bot_name":
        # SPECIAL HANDLING FOR VIRTUAL BOT NAME FIELD
        if "ua" not in actual_cols:
            return {"values": [], "field": field, **runner.telemetry()}

        try:
            from backend.utils.bot_sources import build_matcher, get_bot_regex_pattern
        except ImportError:
            return {"values": [], "field": field, **runner.telemetry()}

        # Optimization: use regex pre-filter from known bot literals
        pattern = get_bot_regex_pattern(200)
        ua_filter = ""
        if pattern:
            pattern_sql = pattern.replace("'", "''")
            ua_filter = f"AND regexp_matches(ua, '{pattern_sql}')"

        # We query unique UAs to keep local bot-matching overhead manageable
        q = f"""
            SELECT ua, {CANONICAL_METRICS["requests"]} AS cnt
            FROM {table_name}
            WHERE {where_clause} AND ua IS NOT NULL {ua_filter}
            GROUP BY ua
            ORDER BY cnt DESC
            LIMIT 5000
        """
        rows = runner.execute(q, params).fetchall()

        match_ua = build_matcher()
        bot_counts: dict[str, dict] = {}
        search_lower = search.lower() if search else ""

        for ua_val, cnt in rows:
            for entry in match_ua(str(ua_val) if ua_val else ""):
                bot_id = entry.get("id", "unknown")
                bot_name = entry.get("name", bot_id.replace("-", " ").title())

                # Apply search filter if provided (matching on ID or Name)
                if search_lower and search_lower not in bot_id.lower() and search_lower not in bot_name.lower():
                    continue

                if bot_id not in bot_counts:
                    bot_counts[bot_id] = {
                        "value": bot_id,
                        "label": bot_name,
                        "count": 0,
                    }
                bot_counts[bot_id]["count"] += cnt

        sorted_vals = sorted(bot_counts.values(), key=lambda x: x["count"], reverse=True)
        return {"values": sorted_vals[:limit], "field": field, **runner.telemetry()}

    # Virtual fields that explode a CSV backing column: filter-lookup
    # routes through the same unnest path so click-to-filter on a
    # specific signal / reason works the same as native columns.
    _VIRTUAL_BACKING = {
        "waf_sig_ind": "waf_sig",
        "edge_score_reason_ind": "edge_score_reason",
    }
    is_signals_individual = field in _VIRTUAL_BACKING
    backing_col = _VIRTUAL_BACKING[field] if is_signals_individual else clean_field
    if backing_col not in actual_cols:
        raise LookupError(f"Field '{field}' not found")

    search_params = list(params)

    if is_signals_individual or clean_field in ("waf_sig", "edge_score_reason"):
        search_cond = ""
        if search:
            search_cond = "AND trim(signal) ILIKE ?"
            search_params.append(f"%{search}%")
        q = f"""
            SELECT trim(signal) AS value, {CANONICAL_METRICS["requests"]} AS count
            FROM (
                SELECT unnest(string_split("{backing_col}", ',')) AS signal
                FROM {table_name}
                WHERE {where_clause} AND "{backing_col}" IS NOT NULL AND "{backing_col}" != ''
            )
            WHERE trim(signal) != '' {search_cond}
            GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
        """
    else:
        search_cond = ""
        if search:
            if clean_field == "country":
                from backend.utils.countries import COUNTRY_MAP

                codes = [c for c, name in COUNTRY_MAP.items() if search.lower() in name.lower()]
                if codes:
                    placeholders = ",".join(["?"] * len(codes))
                    search_cond = (
                        f'AND (CAST("{clean_field}" AS VARCHAR) ILIKE ? '
                        f'OR CAST("{clean_field}" AS VARCHAR) IN ({placeholders}))'
                    )
                    search_params.append(f"%{search}%")
                    search_params.extend(codes)
                else:
                    search_cond = f'AND CAST("{clean_field}" AS VARCHAR) ILIKE ?'
                    search_params.append(f"%{search}%")
            elif clean_field == "asn":
                # ASN-name search: pre-fetch matching ASN ints from per-service
                # SQLite metadata, then inline them as a parameterised IN list.
                # Avoids ATTACH overhead / SQLite-extension dependency in DuckDB.
                from backend.core import metadata_db

                try:
                    matching_asns = metadata_db.asn_ints_for_search(src["name"], f"%{search}%")
                except Exception:
                    matching_asns = []
                if matching_asns:
                    in_placeholders = ",".join(["?"] * len(matching_asns))
                    search_cond = (
                        f'AND (CAST("{clean_field}" AS VARCHAR) ILIKE ? '
                        f'OR CAST("{clean_field}" AS VARCHAR) IN ({in_placeholders}))'
                    )
                    search_params.append(f"%{search}%")
                    search_params.extend([str(a) for a in matching_asns])
                else:
                    search_cond = f'AND CAST("{clean_field}" AS VARCHAR) ILIKE ?'
                    search_params.append(f"%{search}%")
            else:
                search_cond = f'AND CAST("{clean_field}" AS VARCHAR) ILIKE ?'
                search_params.append(f"%{search}%")

        q = f"""
            SELECT "{clean_field}" AS value, {CANONICAL_METRICS["requests"]} AS count
            FROM {table_name}
            WHERE {where_clause} {search_cond}
            GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
        """

    result = runner.execute_with_retry(q, search_params)
    if result is None:
        return {"values": [], "field": field, **runner.telemetry()}
    res = result.fetchall()

    vals = [{"value": r[0], "count": r[1]} for r in res]
    if clean_field == "asn" and vals:
        from backend.core.duckdb import enrich_asn_labels

        enrich_asn_labels(vals, src["name"])

    return {"values": vals, "field": field, **runner.telemetry()}
