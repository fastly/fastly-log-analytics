"""Dashboard repository — pure SQL functions, no HTTP imports."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from collections.abc import Collection
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    CANONICAL_METRICS,
    QueryRunner,
    SectionTimer,
    _get_schema,
    _safe_table,
    force_rebuild_view,
    get_source_extent,
    percentile_ms_expr,
    safe_interval,
    safe_iso,
    should_self_heal_stale_view,
    time_bucket_select,
)
from backend.repositories._sql import dashboard as SQL
from backend.repositories.utils.filters import build_where_clause

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


def invalidate_service(service_name: str) -> None:
    """Drop every cached dashboard response keyed to ``service_name``.

    Public surface for cron / admin callers that need to invalidate after
    an ingest tick or a config change — keeps them out of the private
    ``_dashboard_cache`` deque so its internal shape can change without
    breaking three out-of-package callers.
    """
    if not service_name:
        return
    stale = [k for k in list(_dashboard_cache) if k.endswith(f":{service_name}")]
    for k in stale:
        try:
            del _dashboard_cache[k]
        except KeyError:
            pass
    # Also drop a direct-keyed entry (the admin ingest path uses .pop(name)).
    try:
        _dashboard_cache.pop(service_name, None)
    except (KeyError, AttributeError):
        pass


# ── aggregates ────────────────────────────────────────────────────────────────

# Phase 7 caller migration: read field codes from the frozen-dataclass
# REGISTRY instead of LOG_FIELD_CATALOG. REGISTRY is derived from the
# catalog at import time and preserves wire-order, so FIELDS comes out
# byte-identical (Rust scorer parity invariant).
from backend.core.field_registry import REGISTRY as _FIELD_REGISTRY

# Virtual fields are catalog ids whose value is computed by exploding a
# real backing column (CSV string) into individual rows via DuckDB's
# unnest(string_split(...)). They live in the FIELDS list so the dashboard
# top-N machinery picks them up, but the cross-cutting loops below skip
# them in batch-stats / column-need passes (their backing column is what
# actually goes into the temp table).
_VIRTUAL_FIELDS = ("waf_sig_ind", "edge_score_reason_ind")
FIELDS = [f.code for f in _FIELD_REGISTRY if f.code != "_source_file"] + list(_VIRTUAL_FIELDS)


def _add_bot_columns(actual_cols: Collection[str], columns: list[str], select_cols: list[str]) -> tuple[bool, bool]:
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
    fields_filter: list[str] | None = None,
    *,
    include_time_series: bool | None = True,
    include_conn_requests: bool | None = True,
    include_map_data: bool | None = True,
    include_top_n: bool | None = True,
) -> dict:
    # Normalize tri-state (model passes None when the caller doesn't override).
    include_time_series = True if include_time_series is None else include_time_series
    include_conn_requests = True if include_conn_requests is None else include_conn_requests
    include_map_data = True if include_map_data is None else include_map_data
    include_top_n = True if include_top_n is None else include_top_n

    source_name = src["name"]
    table_name = _safe_table(source_name)

    lf_config = src.get("log_fields") or {}
    _custom_field_names = [
        cf["name"]
        for cf in lf_config.get("custom_fields", [])
        if cf.get("enabled", True) and cf.get("show_in_dashboard", True)
    ]
    all_fields = FIELDS + _custom_field_names
    if fields_filter is not None:
        fields = [f for f in fields_filter if f in all_fields]
    else:
        fields = all_fields

    # Cache is hard-disabled today (DASHBOARD_CACHE_TTL = 0). Gate the
    # key-build + read together so the SHA-256 over the filter payload
    # doesn't run on every request just to be discarded — saves a small
    # per-request cost while keeping the legacy rollback hatch intact.
    cache_key: str | None = None
    now = time.time()
    if DASHBOARD_CACHE_TTL > 0:
        _key_payload = json.dumps(
            {
                "s": start_time,
                "e": end_time,
                "f": {k: (v.mode, sorted(str(x) for x in v.values)) for k, v in sorted(filters.items())},
                "ci": chart_interval,
                "cm": chart_metric,
                "fields": sorted(fields_filter) if fields_filter is not None else None,
                "its": include_time_series,
                "icr": include_conn_requests,
                "imd": include_map_data,
                "itn": include_top_n,
            },
            separators=(",", ":"),
        )
        cache_key = hashlib.sha256(f"{_key_payload}:{source_name}".encode()).hexdigest()
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
    timer = SectionTimer()
    section_timings = timer.entries

    runner = QueryRunner(con, src)
    interval = "1 minute"

    actual_cols = timer.call("get_schema_cols", runner.get_schema_cols)
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

    params, where_clause = timer.call(
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
    # Freshness contract on the rollup path: execute_top_n_rollups
    # (backend/repositories/_base.py) is window-correct.
    #   - Fully-contained UTC days: served from the per-day compacted rollup.
    #   - Closed hours of uncompacted days: served from hour bundles /
    #     per-field hour rollups; hours with NO rollup coverage (writer
    #     gap — e.g. bursty services whose closed hours the per-sync
    #     recompute never touched) are live-queried by the missing-hour
    #     heal, capped at _MISSING_HOUR_HEAL_CAP hours.
    #   - Boundary hours of already-compacted days (window cuts into a
    #     day whose per-hour files were deleted by day compaction): a
    #     known, bounded read gap — deliberately NOT live-queried so a
    #     mid-day window edge doesn't pay a per-request day scan.
    #   - Active hour: live-queried, intersected with the window.
    # All sources are merged before truncation. The rollup path builds NO
    # per-request temp: the remaining non-top-N sections are rollup-served
    # too (time_series / conn_requests histogram / signal unnests), and
    # each one's rollup-miss fallback queries the base table directly.

    # `temp_table` ends up holding the per-request materialization (if
    # any) so the `finally` cleanup at the bottom of the function can
    # DROP it regardless of which branch built it.
    temp_table: str | None = None
    # Stash the originals so fallback paths (e.g. the runtime CSV
    # explode for virtual fields when the rollup is missing rows) can
    # query the base table directly even on the non-rollup path, where
    # the wide temp rewrites table_name/where_clause/params.
    orig_table_name = _safe_table(source_name)
    orig_where_clause = where_clause
    orig_params = list(params) if params is not None else []

    # The per-request materialization (wide filtered temp, non-rollup path
    # only — the rollup path builds no temp) is factored into a closure so
    # the view-lag self-heal can rebuild it a second time after forcing a
    # fresh view. It returns the (table_name, where_clause, params,
    # temp_table) the downstream queries read from, or None when the wide
    # temp build fails (the caller returns the empty shape).
    def _build_query_target() -> tuple[str, str, list, str | None] | None:
        _table_name = _safe_table(source_name)
        _where = orig_where_clause
        _params = list(orig_params)
        if use_rollups:
            # Rollup path: NO per-request materialization. Every window-scan
            # section is rollup-served — top-N/map via execute_top_n_rollups,
            # the chart via try_time_series_from_rollup, the conn_requests
            # histogram via try_conn_requests_hist_from_rollup, the signal
            # unnests via the virtual-field rollups — and each section's
            # rollup-miss fallback runs its ONE query against the base table
            # directly. With at most one consumer per scan there is nothing
            # for a shared temp to amortize: the 2026-07-06 trace measured
            # the old eager narrow temp at 391ms for ~12ms of temp reads
            # (its chart consumer had been served by the rollup). total_rows
            # runs as a single COUNT against the base table — the shape the
            # /charts include-nothing path always used.
            return _table_name, _where, _params, None
        # Non-rollup path. Use TEMP TABLE instead of TEMP VIEW to
        # materialize the filtered results in memory. This prevents DuckDB
        # from re-scanning the underlying files for every branch of the
        # UNION ALL.
        wide_temp = f"t_{uuid.uuid4().hex}"
        sql = f"CREATE TEMP TABLE {wide_temp} AS SELECT {cols_str} FROM {_table_name} WHERE {orig_where_clause}"
        if not timer.call("wide_temp_create", lambda: runner.create_temp_table(sql, _params)):
            return None
        # All subsequent queries use the temp table.
        return wide_temp, "1=1", [], wide_temp

    built = _build_query_target()
    if built is None:
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
    table_name, where_clause, params, temp_table = built

    results: dict[str, Any] = {f: {"top": [], "total": 0} for f in fields}

    # View-lag self-heal fires AT MOST once per call (no retry storm). See
    # the guarded block after get_source_extent below.
    _self_heal_attempted = False

    if use_rollups:
        # Share ONE direct active-hour temp across this request's three
        # live-slice consumers (top-N merge, conn_requests top-up, chart
        # slice) — each would otherwise re-open every buffer + active-hour
        # parquet. Dropped in the finally below.
        runner.begin_shared_active_hour_temps()
    try:
        field_totals: dict[str, int] = {}
        total_rows = 0
        earliest_log_at = None
        latest_log_at = None

        # The window COUNT (+ per-field totals on the non-rollup path) is
        # factored into a closure so the view-lag self-heal can re-issue it
        # against the rebuilt view + temp table. ``field_totals`` is mutated
        # in place; the closure returns ``total_rows``.
        def _compute_count(_table_name: str, _where: str, _params: list) -> int:
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
                    return runner.execute(
                        f"SELECT {CANONICAL_METRICS['requests']} FROM {_table_name} WHERE {_where}", _params
                    ).fetchone()[0]
                except Exception:
                    return 0
            # Non-rollups path keeps the wide COUNT — we have the
            # filtered temp table loaded; one combined scan is cheaper
            # than re-counting per field downstream. Skip the per-field
            # COUNT cols when include_top_n=False (their only consumer is
            # the field_totals dict used by the top-N panels); the bare
            # requests count is still needed for total_rows.
            count_cols: list[str] = [CANONICAL_METRICS["requests"]]
            valid_fields: list[str] = []
            if include_top_n:
                for field in fields:
                    if field in _VIRTUAL_FIELDS:
                        continue
                    if field in actual_cols:
                        count_cols.append(f"count({field})")
                        valid_fields.append(field)
            field_totals.clear()
            if count_cols:
                count_res = runner.execute(
                    f"SELECT {', '.join(count_cols)} FROM {_table_name} WHERE {_where}", _params
                ).fetchone()
                for i, field in enumerate(valid_fields):
                    field_totals[field] = count_res[i + 1]
                return count_res[0]
            return 0

        total_rows = _compute_count(table_name, where_clause, params)

        total_rows_total, earliest_log_at, latest_log_at = timer.call(
            "source_extent", lambda: get_source_extent(runner, src, orig_table_name)
        )

        # ── View-lag self-heal ────────────────────────────────────────────
        # The status cache (latest_log_at, from a direct parquet+buffer
        # read at ingest) updates immediately, but the live Iceberg VIEW
        # the queries above read from is built from CACHED view SQL that
        # lags until a metadata_sync/commit cron tick rebuilds it. With a
        # freshly-buffered log the status cache shows data while the view
        # returns 0 rows for every window — the frontend then loops on
        # "Preparing your data" (and on dev, which runs no crons, it never
        # resolves).
        #
        # Tight trigger (see should_self_heal_stale_view): empty window +
        # no filters + the all-time-latest log is inside [start, end]. If
        # the window contains the latest log but the view is empty, the
        # view MUST be stale. A legitimately-empty low-traffic window won't
        # contain the all-time latest log, so this won't fire there.
        # Rebuild the view ONCE (local boolean — no retry storm), drop the
        # stale temp table, re-materialize from the fresh view, and re-run
        # the count so the downstream per-field / time-series / map queries
        # below read live data.
        if not _self_heal_attempted and should_self_heal_stale_view(
            windowed_count=total_rows,
            filters=filters,
            latest_log_at=latest_log_at,
            start_time=start_time,
            end_time=end_time,
        ):
            _self_heal_attempted = True
            timer.call("view_self_heal_rebuild", lambda: force_rebuild_view(con, src))
            # The temp table (if any) was built from the stale view — drop
            # it and rebuild from the now-fresh view.
            if temp_table is not None:
                try:
                    con.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
                except Exception:
                    pass
                temp_table = None
            rebuilt = _build_query_target()
            if rebuilt is not None:
                table_name, where_clause, params, temp_table = rebuilt
                total_rows = _compute_count(table_name, where_clause, params)

        schema_types = timer.call("schema_types", lambda: {col["name"]: col["type"] for col in _get_schema(con, src)})

        # When use_rollups=True, field_totals is empty here — populate it
        # below from the rollup query results. Use the full eligible field
        # list (anything non-virtual + in schema) as batch_fields; the
        # rollup helper silently skips fields it has no data for.
        #
        # Virtual fields (waf_sig_ind, edge_score_reason_ind) now also
        # have their own rollup entries (rollups/hour/field=waf_sig_ind/...
        # — see _build_virtual_field_copy_query in core/rollups.py).
        # Include them when their BACKING column is in actual_cols so
        # the rollup reader picks them up via the same path as regular
        # fields. Saves the runtime-unnest cost in _exploded_top_n
        # (was ~1.2s + ~0.7s on prod 30d for the two CSV fields).
        from backend.core.rollups import _VIRTUAL_FIELD_BACKING as _VFB

        def _virtuals_with_backing(in_set) -> list[str]:
            return [v for v in _VIRTUAL_FIELDS if v in fields and _VFB.get(v) in in_set]

        if use_rollups:
            batch_fields = [f for f in fields if f not in _VIRTUAL_FIELDS and f in actual_cols]
            # Virtual fields go through the rollup reader too — they
            # have dedicated per-hour entries on disk and the reader
            # silently skips fields with no data, so a service that
            # hasn't backfilled yet just falls through to the runtime
            # explode below.
            batch_fields += _virtuals_with_backing(actual_cols)
        else:
            # Non-rollup path uses execute_top_n_batch which COUNT(...)s
            # the field as a real column. Virtual fields aren't real
            # columns, so they'd raise a BinderException — keep them on
            # the existing runtime-explode path (_exploded_top_n
            # below).
            batch_fields = [f for f in fields if f not in _VIRTUAL_FIELDS and f in field_totals]

        # all_top_res is the merged (field, value, count) result of the
        # batch top-N scan; it also feeds the rollup-path map_data
        # derivation downstream. Initialize to empty so the gated-off
        # case still has a defined shape.
        all_top_res: list[tuple[str, Any, int]] = []
        field_order: list[str] = []
        # The selector may turn off top-N coverage entirely (sections=['core']
        # on /aggregates). When it's off AND map_data isn't pulling country
        # from the same scan, skip the batch call + population loop —
        # they're the dominant rollup-path cost. The non-rollup path still
        # falls through to map_data via SQL.MAP_DATA_BY_COUNTRY.
        _need_topn_scan = include_top_n or (use_rollups and include_map_data and "country" in actual_cols)
        if not _need_topn_scan:
            pass
        elif use_rollups:
            # Bump country's per-field limit to 500 so the map_data path
            # below can use the same call's results — eliminates the
            # second execute_top_n_rollups invocation that was costing
            # ~200-250ms per request (one full active-hour temp + rollup
            # parquet scan duplicated for one low-cardinality field).
            # Other fields stay at limit=10. Make sure country is in the
            # field list — it normally is via FIELDS, but the explicit
            # add guards a future change to FIELDS.
            #
            # When the caller (e.g. /charts) declared include_map_data
            # false, the choropleth never renders so the widened limit
            # is pure waste — analyst body_bytes on /dashboard 24h was
            # 38 KB vs 12 KB for the same call without the widening
            # (the 500-row country payload is the dominant slice).
            # Drop the widening in that case; the country panel still
            # gets limit=10 like every other field.
            _batch_with_country = batch_fields if "country" in batch_fields else batch_fields + ["country"]
            _per_field_limits = {"country": 500} if include_map_data else {}
            all_top_res, field_order = timer.call(
                "top_n_rollups",
                lambda: runner.execute_top_n_rollups(
                    _batch_with_country,
                    start_time,
                    end_time,
                    limit=10,
                    per_field_limits=_per_field_limits,
                    _phase_log=section_timings,
                    # Seed the live-active-hour branch's schema lookups so
                    # it skips a redundant get_schema_cols() + _get_schema()
                    # round-trip — these were already computed above.
                    actual_cols=list(actual_cols),
                    schema_types=schema_types,
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
            all_top_res, field_order = timer.call(
                "top_n_batch",
                lambda: runner.execute_top_n_batch(batch_fields, table_name, actual_cols, schema_types, limit=10),
            )

        if all_top_res and include_top_n:
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

                asn_names = timer.call("asn_names_lookup", lambda: _db.get_asn_names(src["name"], asn_list))

            # Per-panel cap at 10. execute_top_n_rollups may return more
            # than 10 for fields with per_field_limits (e.g. country=500
            # for the choropleth); the panel UI only renders 10, so cap
            # the append here. Other fields stay at <=10 naturally.
            #
            # __other__ filter: the per-day bundle (rollups/day_bundled)
            # truncates to top-DAY_BUNDLE_TOP_K per (field, day) and
            # emits an aggregated ``__other__`` synthetic row carrying
            # the tail count. Skip it for the displayed top-N panel
            # (its `value` is the literal sentinel, not a real value to
            # render) but DO count it in field_totals so the panel's
            # "Total" stays correct.
            _PANEL_LIMIT = 10
            _panel_count: dict[str, int] = {}
            for f_name, f_val, f_count in all_top_res:
                if f_val == "__other__":
                    continue
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
        #
        # Fast path: if the rollup already populated results[virtual_id]
        # via the top_n_rollups call above (the rollup writer now
        # pre-aggregates virtual fields, see
        # core/rollups._build_virtual_field_copy_query), skip the
        # runtime unnest entirely. The runtime fallback only fires
        # when the rollup is empty for this virtual field (cold start,
        # writer behind, etc.).
        def _exploded_top_n(virtual_id: str, backing_col: str) -> None:
            if virtual_id not in fields:
                return
            existing = results.get(virtual_id)
            if existing and existing.get("top"):
                # Rollup already produced rows — keep them, no runtime scan.
                return
            if backing_col not in actual_cols:
                results[virtual_id] = {"top": [], "total": 0}
                return
            # Query the BASE table, not the temp: the temp's narrow
            # projection no longer carries waf_sig / edge_score_reason
            # (the virtual-field rollup serves them on the hot path).
            # Direct base-table scan keeps this fallback functional
            # when the rollup is missing rows — paid only on the rare
            # cold-rollup path.
            q = SQL.VIRTUAL_FIELD_EXPLODED_TOP_N.format(
                backing_col=backing_col,
                table_name=orig_table_name,
                where_clause=orig_where_clause,
                requests_metric=CANONICAL_METRICS["requests"],
            )
            res = runner.execute(q, orig_params).fetchall()
            if res:
                results[virtual_id] = {
                    "top": [{"value": r[0], "count": r[1]} for r in res],
                    "total": res[0][2],
                }
            else:
                results[virtual_id] = {"top": [], "total": 0}

        if include_top_n:
            timer.call("waf_sig_ind_explode", lambda: _exploded_top_n("waf_sig_ind", "waf_sig"))
            timer.call(
                "edge_score_reason_ind_explode", lambda: _exploded_top_n("edge_score_reason_ind", "edge_score_reason")
            )

        # Special handling for conn_requests (bucketed histogram).
        # Skipped entirely when the caller (e.g. /charts) doesn't render it.
        # Rollup fast path: served from the security_conn_reuse per-hour/
        # per-day parquets (same backing column + bucket edges as this
        # panel — see try_conn_requests_hist_from_rollup) + a live active-
        # hour top-up. Falls back to the raw bucket scan on any miss; on
        # the rollup path table_name is the BASE table, so the fallback
        # needs the real where_clause + params (not the temp's "1=1").
        t_conn_req_0 = time.perf_counter()
        if include_conn_requests and "conn_requests" in actual_cols:
            hist = None
            if use_rollups:
                hist = runner.try_conn_requests_hist_from_rollup(
                    start_time,
                    end_time,
                    has_filters=bool(filters),
                    actual_cols=actual_cols,
                )
            if hist is not None:
                results["conn_requests"] = hist
            else:
                q = SQL.CONN_REQUESTS_BUCKET.format(
                    requests_metric=CANONICAL_METRICS["requests"],
                    table_name=table_name,
                    where_clause=where_clause,
                )
                res = runner.execute(q, params).fetchall()
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

        # Time series. Skipped entirely when the caller (e.g. /charts)
        # doesn't render the chart.
        t_ts_0 = time.perf_counter()
        time_series: list[dict] = []
        chart_metric_out = "requests"
        if include_time_series and "timestamp" in actual_cols:
            interval = safe_interval(chart_interval, default=interval)

            sql_cache = "cache"
            sql_elapsed = "elapsed"

            # Time-series rollup fast path. Serves the chart from per-hour
            # 1-minute pre-aggregated parquets when the metric + interval are
            # rollup-supported and no row-level filters are active. The
            # `use_rollups` gate already encodes "no filters" — reusing it
            # keeps the two paths consistent. Falls back transparently to the
            # raw branches below when the reader returns None.
            rollup_metric_ok = chart_metric in QueryRunner._TS_ROLLUP_METRIC_PARTS
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
                    # This branch is inside `if use_rollups` (no row filters),
                    # which is what licenses the reader's direct active-hour
                    # read to substitute its own timestamp clamp.
                    unfiltered_window=True,
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
                ts_q = SQL.TIME_SERIES.format(
                    time_bucket_select=time_bucket_select(interval),
                    value_expr=CANONICAL_METRICS["5xx_rate"],
                    table_name=table_name,
                    extra_where="",
                    where_clause=where_clause,
                )
            elif chart_metric == "4xx" and "status" in actual_cols:
                chart_metric_out = "4xx"
                ts_q = SQL.TIME_SERIES.format(
                    time_bucket_select=time_bucket_select(interval),
                    value_expr=CANONICAL_METRICS["4xx_rate"],
                    table_name=table_name,
                    extra_where="",
                    where_clause=where_clause,
                )
            elif chart_metric == "hit_rate" and ("cache" in actual_cols or "resp_state" in actual_cols):
                chart_metric_out = "hit_rate"
                # Fallback to resp_state if cache is missing
                cache_col = '"cache"' if "cache" in actual_cols else '"resp_state"'
                hit_rate_expr = CANONICAL_METRICS["hit_rate"].format(cache_col=cache_col)
                ts_q = SQL.TIME_SERIES.format(
                    time_bucket_select=time_bucket_select(interval),
                    value_expr=hit_rate_expr,
                    table_name=table_name,
                    extra_where="",
                    where_clause=where_clause,
                )
            elif chart_metric.endswith("_latency") and ("elapsed" in actual_cols or "elapsed_us" in actual_cols):
                chart_metric_out = chart_metric
                percentile = 0.95
                if chart_metric.startswith("p50"):
                    percentile = 0.50
                elif chart_metric.startswith("p99"):
                    percentile = 0.99
                ts_q = SQL.TIME_SERIES.format(
                    time_bucket_select=time_bucket_select(interval),
                    value_expr=percentile_ms_expr(sql_elapsed, percentile),
                    table_name=table_name,
                    extra_where=f" AND {sql_elapsed} IS NOT NULL",
                    where_clause=where_clause,
                )
            elif chart_metric == "throughput" and "resp_bytes" in actual_cols and "elapsed" in actual_cols:
                chart_metric_out = "throughput"
                sql_resp_bytes = "resp_bytes"
                # Note: elapsed and elapsed_us both map to the same field in DuckDB (µs)
                sql_elapsed_val = "elapsed"
                ts_q = SQL.TIME_SERIES.format(
                    time_bucket_select=time_bucket_select(interval),
                    value_expr=CANONICAL_METRICS["throughput"].format(
                        cache_col=sql_cache,
                        elapsed_col=sql_elapsed_val,
                        resp_bytes_col=sql_resp_bytes,
                    ),
                    table_name=table_name,
                    extra_where="",
                    where_clause=where_clause,
                )
            elif chart_metric == "req_size" and any(c in actual_cols for c in ["req_header_bytes", "req_bytes"]):
                chart_metric_out = "req_size"
                header_col = '"req_header_bytes"' if "req_header_bytes" in actual_cols else "0"
                body_col = "req_bytes" if "req_bytes" in actual_cols else "0"
                ts_q = SQL.TIME_SERIES.format(
                    time_bucket_select=time_bucket_select(interval),
                    value_expr=CANONICAL_METRICS["req_size"].format(
                        header_bytes_col=header_col,
                        req_bytes_col=body_col,
                    ),
                    table_name=table_name,
                    extra_where="",
                    where_clause=where_clause,
                )
            elif chart_metric == "ttfb" and "ttfb" in actual_cols:
                chart_metric_out = "ttfb"
                ts_q = SQL.TIME_SERIES.format(
                    time_bucket_select=time_bucket_select(interval),
                    value_expr=CANONICAL_METRICS["ttfb_ms"],
                    table_name=table_name,
                    extra_where="",
                    where_clause=where_clause,
                )
            else:
                chart_metric_out = "requests"
                ts_q = SQL.TIME_SERIES.format(
                    time_bucket_select=time_bucket_select(interval),
                    value_expr=CANONICAL_METRICS["requests"],
                    table_name=table_name,
                    extra_where="",
                    where_clause=where_clause,
                )

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

        # Map data. Skipped entirely when the caller (e.g. /charts)
        # doesn't render the world map.
        t_map_0 = time.perf_counter()
        map_data: list[dict] = []
        if include_map_data and "country" in actual_cols:
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
                map_q = SQL.MAP_DATA_BY_COUNTRY.format(
                    requests_metric=CANONICAL_METRICS["requests"],
                    table_name=table_name,
                    where_clause=where_clause,
                )
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
        # Rollup path: drop the request-scoped shared active-hour temps
        # (no-op when the scope was never enabled — filters present).
        runner.end_shared_active_hour_temps()
        # Non-rollup path: drop the wide filtered TEMP TABLE. When TEMP
        # TABLE creation failed and `temp_table` is None, this is a no-op.
        if temp_table is not None:
            try:
                con.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
            except Exception:
                pass


# ── raw ───────────────────────────────────────────────────────────────────────


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
        q = SQL.FIELD_VALUES_BOT_UA.format(
            requests_metric=CANONICAL_METRICS["requests"],
            table_name=table_name,
            where_clause=where_clause,
            ua_filter=ua_filter,
        )
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
        q = SQL.FIELD_VALUES_VIRTUAL_SIGNALS.format(
            requests_metric=CANONICAL_METRICS["requests"],
            backing_col=backing_col,
            table_name=table_name,
            where_clause=where_clause,
            search_cond=search_cond,
            limit=limit,
        )
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
                from backend.core import metadata as metadata_db

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

        q = SQL.FIELD_VALUES_NATIVE_COLUMN.format(
            clean_field=clean_field,
            requests_metric=CANONICAL_METRICS["requests"],
            table_name=table_name,
            where_clause=where_clause,
            search_cond=search_cond,
            limit=limit,
        )

    result = runner.execute_with_retry(q, search_params)
    if result is None:
        return {"values": [], "field": field, **runner.telemetry()}
    res = result.fetchall()

    vals = [{"value": r[0], "count": r[1]} for r in res]
    if clean_field == "asn" and vals:
        from backend.core.duckdb import enrich_asn_labels

        enrich_asn_labels(vals, src["name"])

    return {"values": vals, "field": field, **runner.telemetry()}


# A-3 (CacheRegistry): register the dashboard response cache so the
# test harness drains it via CacheRegistry.clear_all(). Same leak
# pattern as the iceberg caches the R-1 work uncovered.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("dashboard._dashboard_cache", _dashboard_cache)
