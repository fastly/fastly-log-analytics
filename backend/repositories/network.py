"""Network repository — health heatmap, world map, quality metrics."""

from __future__ import annotations

from typing import Any

import duckdb

from backend.core import duckdb as _db
from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, SectionTimer, _safe_table
from backend.repositories._sql import network as SQL
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    bucket_time_to_minute,
    cache_get,
    cache_put,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.geo import format_city_label

# ── Response memo cache ───────────────────────────────────────────────────────
# /api/network-health does a per-request TEMP TABLE build (19 cols, multi-second
# on 30d windows) followed by 6+ aggregate scans. Re-renders triggered by
# mapAsn toggle / filter tweak / refetch tick re-do the entire pipeline even
# when (src, start_time, end_time, filters, bucket_seconds, top_n, map_asn) is
# unchanged. Same standing rule as origin's response cache: "a little behind
# the data" beats "redo the cloud read" — 30 s is well below ingest cadence.
_RESPONSE_CACHE_TTL = 30.0
_RESPONSE_CACHE_MAXSIZE = 128
_response_cache: BoundedTTLCache = BoundedTTLCache(maxsize=_RESPONSE_CACHE_MAXSIZE, ttl_seconds=_RESPONSE_CACHE_TTL)


def _response_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    metric: str,
    bucket_seconds: int,
    top_n: int,
    map_asn: str,
    sections: set[str] | None,
    mask_ips: bool,
    range_token: str | None = None,
    quantized_anchor: str | None = None,
    invite_clamp_fingerprint: str | None = None,
) -> str:
    # Key field order is load-bearing: it is serialized as-is, so changing it
    # would invalidate every live key. digest_cache_key keeps the bytes stable.
    #
    # Two key shapes, selected by ``range_token``:
    #
    # (1) STABLE relative-range path (``range_token`` present) — the network 30d
    #     analyst-cliff fix. The router has RESOLVED the scan window from
    #     (range_token, quantized_anchor) server-side and clamped it to the
    #     invite ceiling; the resolved+clamped bounds drive the SCAN, but the KEY
    #     is built from the relative token + the quantized anchor instead of the
    #     rolling resolved bounds. Because the token + quantized anchor are
    #     server-reproducible and stable within the anchor quantum, an analyst
    #     loading across rolling minutes now lands on the SAME key and HITS the
    #     memo instead of recomputing the ~26s 30d pipeline.
    #
    #     SECURITY — the key MUST partition by every authorization axis so two
    #     callers with different authorization never alias onto one entry:
    #       * ``src`` (digest → src["name"]) — tenant isolation.
    #       * ``mi`` (mask_ips) — masked vs unmasked analyst.
    #       * ``icf`` (invite_clamp_fingerprint) — open vs date-restricted vs
    #         admin (None) never share. The resolved bounds are clamped to the
    #         invite ceiling before the scan, so two invites with DIFFERENT
    #         ceilings scanning the SAME token would otherwise alias and one
    #         could serve rows past the other's ceiling. Keying on the invite-
    #         clamp shape keeps each ceiling's results in its own partition.
    #       * ``rt`` (range_token) + ``qa`` (quantized_anchor) — two different
    #         ranges (or two different anchor quanta) never alias onto one entry.
    #     The server does NOT trust FE-supplied absolute bounds on this path
    #     (the router resolves them), so a crafted body can't poison a
    #     token+anchor entry with an arbitrary scanned window.
    #
    # (2) Legacy anchor-faithful path (no ``range_token``) — explicit user range
    #     selection / deep-links, unchanged below.
    #
    # SECURITY — anchor-faithful + tenant/PII partitioned (security-rbac review):
    #   * ``s``/``e`` are the minute-bucketed RESOLVED clamp bounds. We key on
    #     the absolute anchor (NOT a span/window-param projection) on purpose:
    #     the clamp window IS the analyst authorization boundary
    #     (``TimeBounds.clamp`` honors caller-supplied start/end, so an
    #     open-invite analyst's resolved window == the request's own bounds).
    #     A window-param key (insights' ``analyst_clamp_cache_key``) would alias
    #     two differently-ANCHORED windows of the same span onto one entry — for
    #     the default OPEN invite that collapses to "||" and aliases EVERY
    #     window the analyst views, serving a different window than was scanned
    #     (incl. rows outside a date-restricted invite's ceiling). Keeping the
    #     resolved bounds keeps a cache hit ≤TTL stale of the SAME window —
    #     never a cross-window crossing. This is the explicit divergence from
    #     the insights stable-key fix: insights never honors a caller absolute
    #     window, network does, so network must stay anchor-faithful.
    #   * ``src`` (via digest_cache_key → src["name"]) partitions by service so
    #     a request can never read another tenant's entry.
    #   * ``mi`` (mask_ips) partitions masked vs unmasked. network-health is
    #     IP-free today, so this is belt-and-braces for uniformity + future-proof.
    #   * ``sec`` (sorted sections tuple) makes the three live FE shapes
    #     (core / map / shielding) each cache distinctly, instead of the cache
    #     only being reachable for the dead ``sections is None`` shape.
    if range_token is not None:
        stable_payload = {
            # ``k`` namespaces this as the stable shape so a stable key can
            # never collide with a legacy (s/e-bearing) key for the same params.
            "k": "rel",
            "rt": range_token,
            "qa": quantized_anchor,
            "icf": invite_clamp_fingerprint,
            "f": serialize_filters_for_key(filters),
            "metric": metric,
            "bs": bucket_seconds,
            "tn": top_n,
            "ma": map_asn,
            "sec": sorted(sections) if sections is not None else None,
            "mi": int(mask_ips),
        }
        return digest_cache_key(stable_payload, src)
    payload = {
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        "f": serialize_filters_for_key(filters),
        "metric": metric,
        "bs": bucket_seconds,
        "tn": top_n,
        "ma": map_asn,
        "sec": sorted(sections) if sections is not None else None,
        "mi": int(mask_ips),
    }
    return digest_cache_key(payload, src)


def _has_signal(payload: dict[str, Any]) -> bool:
    """True when a get_health payload carries real data worth caching.

    SECURITY (stale-empty poison guard — the dashboard-cache poisoning bug
    class, see backend/repositories/dashboard.py:43-56): a transient view-lag /
    mid-commit empty must NOT be cached for the TTL and then served to every
    request on the same key. The ``available: False`` early-returns already
    precede the write, so this guards the ``available: True`` BUT zero-signal
    case: no heatmap rows, no map buckets, no leaderboard, zero total. A
    populated later request then recomputes + caches instead of reading the
    cached blank. Checks only the keys that are present (a section-scoped
    payload won't carry the others)."""
    summary = payload.get("summary")
    if isinstance(summary, dict) and summary.get("total_reqs"):
        return True
    return any(
        payload.get(k) for k in ("heatmap", "map_buckets", "leaderboard", "metro_leaderboard", "cities", "buckets")
    )


def _avg_hs(buckets_data: dict, keys: list[str]) -> float | None:
    """Average health_score over a set of bucket keys, or None if no data."""
    scores = [
        buckets_data[b].get("health_score")
        for b in keys
        if b in buckets_data and buckets_data[b].get("health_score") is not None
    ]
    return round(sum(scores) / len(scores), 1) if scores else None


def _health_score(
    throughput_bps: float | None,
    rtt_congestion_us: float | None,
    avg_ploss: float | None,
    rtt_jitter_us: float | None,
    error_pct: float | None,
) -> float:
    pkt = min((avg_ploss or 0) / 0.05, 1.0) if avg_ploss is not None else 0
    cong = min((rtt_congestion_us or 0) / 200_000, 1.0) if rtt_congestion_us is not None else 0
    jitter = min((rtt_jitter_us or 0) / 100_000, 1.0) if rtt_jitter_us is not None else 0
    err = min((error_pct or 0) / 10.0, 1.0) if error_pct is not None else 0
    weighted = pkt * 0.40 + cong * 0.30 + jitter * 0.20 + err * 0.10
    return round((1.0 - weighted) * 100, 1)


def get_health(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    metric: str = "health_score",
    bucket_seconds: int = 300,
    top_n: int = 30,
    map_asn: str = "all",
    sections: set[str] | None = None,
    mask_ips: bool = False,
    force_refresh: bool = False,
    range_token: str | None = None,
    quantized_anchor: str | None = None,
    invite_clamp_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Return ASN × time heatmap, world map buckets, metro leaderboard, and ASN leaderboard.

    ``mask_ips`` partitions the response cache (masked vs unmasked) — see
    ``_response_cache_key``. network-health is IP-free today so it's
    belt-and-braces, but it keeps the key shape uniform with insights and
    future-proof. ``force_refresh`` skips the cache READ (the write still
    happens) so the prewarmer rewrites the entry every tick and resets the TTL
    (cachetools' TTL is insertion-based — a hit would not reset it).

    ``range_token`` / ``quantized_anchor`` / ``invite_clamp_fingerprint`` select
    the STABLE relative-range cache-key shape (the network 30d analyst-cliff
    fix). The router resolves the scan window from (token, anchor) and clamps it
    to the invite ceiling, then passes ``start_time``/``end_time`` (which still
    drive the SCAN) PLUS these three so the KEY is stable across rolling
    minutes. When ``range_token`` is None the legacy anchor-faithful key (on the
    resolved bounds) is used — unchanged for explicit user ranges / deep-links.
    SECURITY: the bounds drive the scan; the token/anchor only stabilize the key.
    """
    import time as _time

    def _want(name: str) -> bool:
        return sections is None or name in sections

    # heatmap_rows feeds heatmap + leaderboard + summary (via global_hs +
    # worst_asn). map_rows feeds cities + map_buckets + summary (via
    # worst_country). buckets is derived from heatmap_rows. metro_rows
    # feeds only metro_leaderboard.
    _want_heatmap_query = _want("heatmap") or _want("leaderboard") or _want("summary") or _want("buckets")
    _want_map_query = _want("cities") or _want("map_buckets") or _want("summary")
    _want_metro_query = _want("metro_leaderboard")
    _want_leaderboard = _want("leaderboard")

    # Per-phase wall-clock timings surface in the response under
    # _section_timings so the perf harness can attribute /api/network-health
    # without ad-hoc instrumentation. Mirrors dashboard.py.
    timer = SectionTimer()
    section_timings = timer.entries

    # Short-TTL response memo (30 s). Cuts the mapAsn toggle / filter
    # tweak / refetch tick cost from the full ~13 s 30d pipeline to ~50 µs.
    # The key now folds the sorted ``sections`` tuple + ``mask_ips`` so each
    # of the three live FE shapes (core / map / shielding) caches distinctly
    # — previously the memo was reachable ONLY for ``sections is None``, which
    # NO live request sends (the page always passes a section selector), so the
    # cache was dead on the request path. Each section-scoped entry is keyed on
    # its own (sections, resolved-bounds, mask_ips) so a smaller selection
    # never reads a larger one's payload. Cache key excludes section_timings +
    # debug envelope so per-request telemetry stays request-scoped.
    cache_key = _response_cache_key(
        src,
        start_time,
        end_time,
        filters,
        metric,
        bucket_seconds,
        top_n,
        map_asn,
        sections,
        mask_ips,
        range_token=range_token,
        quantized_anchor=quantized_anchor,
        invite_clamp_fingerprint=invite_clamp_fingerprint,
    )
    if not force_refresh:
        cached = cache_get(_response_cache, cache_key)
        if cached is not None:
            runner = QueryRunner(con, src)
            return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])

    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = set(runner.get_schema_cols())
    timer.mark("get_schema_cols", _t)

    if not {"tcp_rtt", "asn"}.issubset(actual_cols):
        return {
            "available": False,
            "reason": "Enable Groups F and G (Network Quality) in your log field configuration.",
            **runner.telemetry(),
        }

    has_ploss = "ploss" in actual_cols
    has_rtt_min = "rtt_min" in actual_cols
    has_rtt_var = "rtt_var" in actual_cols
    has_country = "country" in actual_cols
    has_lat = "lat" in actual_cols and "lon" in actual_cols
    has_c_speed = "c_speed" in actual_cols
    has_metro = "metro" in actual_cols

    # If filtering by a specific ASN for the map, remove the asn column filter so it doesn't conflict
    effective_filters = dict(filters)
    if map_asn != "all" and "asn" in effective_filters:
        del effective_filters["asn"]

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(
        start_time, end_time, effective_filters, list(actual_cols), inline_params=True
    )
    timer.mark("build_where_clause", _t)

    # Floor the heatmap/map bucket width so a 5-second bucket on a
    # 30-day window doesn't synthesise 518k buckets the UI immediately
    # downsamples anyway. 8640 is the cap on emitted rows per series
    # (24h × 360 ticks/h ≈ the chart's max meaningful resolution).
    # max(span / 8640) gives ~10s on 24h, ~70s on 7d, ~300s on 30d —
    # passthrough for any caller already supplying a sane bucket.
    # Computed BEFORE the rollup hoist (readers don't need it, but
    # bucket_seconds is used in the payload below regardless of path).
    try:
        from backend.utils.date_utils import parse_iso_utc as _parse_iso_utc

        if start_time and end_time:
            _st0 = _parse_iso_utc(start_time)
            _et0 = _parse_iso_utc(end_time)
            if _st0 is not None and _et0 is not None:
                span_secs = max(1, int((_et0 - _st0).total_seconds()))
                bucket_seconds = max(bucket_seconds, span_secs // 8640)
    except Exception:
        pass

    bucket_ms = bucket_seconds * 1000

    ploss_expr = "AVG(ploss)" if has_ploss else "NULL"
    rtt_min_expr = "APPROX_QUANTILE(rtt_min, 0.5)" if has_rtt_min else "NULL"
    rtt_var_expr = "APPROX_QUANTILE(rtt_var, 0.5)" if has_rtt_var else "NULL"
    congestion_expr = "APPROX_QUANTILE(COALESCE(tcp_rtt, 0) - COALESCE(rtt_min, 0), 0.5)" if has_rtt_min else "NULL"

    # ── Rollup fast path ────────────────────────────────────────────────────
    # Try the per-hour heatmap + geo rollup readers BEFORE building the temp
    # table. When all requested scan-bound sections hit, the 2–4 s
    # create_filtered_temp_table is skipped entirely. The posture mirrors
    # backend/repositories/origin.py's skip-temp guard.
    #
    # SECTIONS COVERED:
    #   "heatmap"  → runners.try_network_heatmap_from_rollup  (feeds heatmap,
    #                leaderboard, summary, buckets — all derived from heatmap_rows)
    #   "map_geo"  → runner.try_network_geo_from_rollup  (feeds map_buckets,
    #                cities, metro_leaderboard)
    #
    # The RTT-percentile and speed-distribution sections have their OWN rollup
    # readers already (try_network_rtt_from_rollup / try_network_speed_from_rollup)
    # and do not contribute to the _net_missed decision.
    _net_rolled: dict[str, Any] = {}
    _net_missed: set[str] = set()

    def _hoist_net(key: str, want: bool, thunk: Any) -> None:
        if not want:
            return
        _t0 = _time.perf_counter()
        result = thunk()
        timer.mark(f"{key}_rollup", _t0)
        if result is not None:
            _net_rolled[key] = result
        else:
            _net_missed.add(key)

    _hoist_net(
        "heatmap",
        _want_heatmap_query,
        lambda: runner.try_network_heatmap_from_rollup(start_time, end_time, has_filters=bool(filters)),
    )
    _hoist_net(
        "map_geo",
        _want_map_query or _want_metro_query,
        lambda: runner.try_network_geo_from_rollup(start_time, end_time, map_asn=map_asn, has_filters=bool(filters)),
    )

    if not _net_missed:
        # All requested scan-bound sections hit rollup → skip temp table.
        section_timings.append({"section": "network:temp_skipped", "time_ms": 0.0})
        heatmap_rows: list[Any] = _net_rolled.get("heatmap") or []
        _geo = _net_rolled.get("map_geo")
        map_rows: list[Any] = _geo[0] if _geo else []
        metro_rows: list[Any] = _geo[1] if _geo else []
        countries: list[str] = sorted({str(r[0]) for r in map_rows if r[0]}) if has_country else []
        temp_table = None  # no temp table — guards the finally DROP below
        t = None
        w = "1=1"
        p: list[Any] = []
    else:
        # ── Temp table path ───────────────────────────────────────────────
        # Drop ``dt`` and ``resp_state`` from the temp projection — neither is
        # read by any downstream SQL template in backend/repositories/_sql/network.py
        # (verified via grep). Materialising them on every 30d window was 5-15%
        # of the temp-table create cost.
        all_net_cols = [
            "timestamp",
            "asn",
            "country",
            "city",
            "region",
            "lat",
            "lon",
            "metro",
            "tcp_rtt",
            "rtt_min",
            "rtt_var",
            "ploss",
            "status",
            "cache",
            "elapsed",
            "resp_bytes",
            "c_speed",
        ]
        _t = _time.perf_counter()
        temp_table = runner.create_filtered_temp_table(
            all_net_cols, list(actual_cols), table_name, where_clause, params
        )
        timer.mark("temp_table_create", _t)
        if temp_table is None:
            return {
                "available": False,
                "reason": "Data temporarily unavailable — view refresh failed. Retry in a moment.",
                **runner.telemetry(),
            }

        # All further queries hit the temp table
        t = temp_table
        w = "1=1"
        p = []

    try:
        # ── Queries against the temp table ────────────────────────────────
        # These blocks run only on the temp-table path (temp_table is not
        # None). On the rollup fast path countries/heatmap_rows/map_rows/
        # metro_rows are already set above and must NOT be overwritten.

        # ── Countries list ─────────────────────────────────────────────────
        if temp_table is not None:
            countries = []
            if has_country:
                rows = runner.execute(
                    f"SELECT DISTINCT country FROM {t} WHERE {w}"
                    f" AND country IS NOT NULL AND country != '' ORDER BY country",
                    p,
                ).fetchall()
                countries = [r[0] for r in rows]

        # ── Heatmap (ASN × bucket) ─────────────────────────────────────────
        if temp_table is not None:
            heatmap_rows = []
            if _want_heatmap_query:
                heatmap_sql = SQL.HEATMAP_BY_ASN_BUCKET.format(
                    bucket_ms=bucket_ms,
                    rtt_min_expr=rtt_min_expr,
                    congestion_expr=congestion_expr,
                    ploss_expr=ploss_expr,
                    rtt_var_expr=rtt_var_expr,
                    table=t,
                    where=w,
                    row_limit=top_n * 200,
                )
                _t = _time.perf_counter()
                heatmap_rows = runner.execute(heatmap_sql, p).fetchall()
                timer.mark("heatmap_query", _t)

        # ── Map (country × bucket) ─────────────────────────────────────────
        if temp_table is not None:
            map_rows = []
            if _want_map_query and has_country:
                lat_col = "lat" if has_lat else "NULL"
                lon_col = "lon" if has_lat else "NULL"
                metro_col = "metro" if has_metro else "NULL"
                city_col = "city" if "city" in actual_cols else "''"
                # Qualified-for-JOIN variants. The 2-pass CTE's ON clause
                # references the same columns by name on both sides, so
                # bare ``city`` / ``lat`` etc. are ambiguous to DuckDB's
                # binder. Prefix with the temp-table name when the column
                # really exists; keep the NULL / '' literal otherwise.
                join_city_col = f"{t}.city" if "city" in actual_cols else "''"
                join_lat_col = f"{t}.lat" if has_lat else "NULL"
                join_lon_col = f"{t}.lon" if has_lat else "NULL"
                join_metro_col = f"{t}.metro" if has_metro else "NULL"

                map_where = w
                map_params = list(p)
                if map_asn != "all":
                    map_where += " AND asn = ?"
                    map_params.append(int(map_asn))

                # Cap to top 5000 (country, city, bucket) cells by request
                # volume — the map UI renders dots, and the long tail beyond a
                # few thousand points is invisible. Without the cap the
                # response body grew to 5.8MB on busy windows, dominating
                # /network cold-load wall time via transfer + JSON parse.
                # Re-sorted by (bucket, reqs DESC) after the cap to preserve
                # the downstream chronological ordering the map expects.
                map_sql = SQL.MAP_BY_COUNTRY_BUCKET.format(
                    city_col=city_col,
                    lat_col=lat_col,
                    lon_col=lon_col,
                    metro_col=metro_col,
                    join_city_col=join_city_col,
                    join_lat_col=join_lat_col,
                    join_lon_col=join_lon_col,
                    join_metro_col=join_metro_col,
                    bucket_ms=bucket_ms,
                    ploss_expr=ploss_expr,
                    table=t,
                    where=map_where,
                )
                _t = _time.perf_counter()
                # {where} appears twice in the 2-pass CTE shape (CTE WHERE +
                # outer WHERE), so the asn filter placeholder must be bound
                # twice. ``map_params`` is at most one element (the asn int)
                # when ``map_asn != "all"``, empty otherwise.
                map_rows = runner.execute(map_sql, map_params + map_params).fetchall()
                timer.mark("map_query", _t)

        # ── Metro leaderboard ──────────────────────────────────────────────
        if temp_table is not None:
            metro_rows = []
            if _want_metro_query and has_country:
                metro_col_m = "metro" if has_metro else "NULL"
                city_col = "city" if "city" in actual_cols else "''"
                region_col = "region" if "region" in actual_cols else "''"
                # Qualified-for-JOIN variants — same disambiguation pattern
                # as map_query. The 2-pass CTE re-aliases these names on the
                # top_cells side, so the JOIN ON needs table-qualified refs.
                join_metro_col = f"{t}.metro" if has_metro else "NULL"
                join_city_col = f"{t}.city" if "city" in actual_cols else "''"
                join_region_col = f"{t}.region" if "region" in actual_cols else "''"
                metro_sql = SQL.METRO_LEADERBOARD.format(
                    city_col=city_col,
                    region_col=region_col,
                    metro_col=metro_col_m,
                    join_city_col=join_city_col,
                    join_region_col=join_region_col,
                    join_metro_col=join_metro_col,
                    ploss_expr=ploss_expr,
                    table=t,
                    where=w,
                )
                _t = _time.perf_counter()
                metro_rows = runner.execute(metro_sql, p).fetchall()
                timer.mark("metro_query", _t)

        # ── Derive top ASNs ────────────────────────────────────────────────
        all_asns_seen: dict[int, int] = {}
        all_buckets_set: set[str] = set()
        for r in heatmap_rows:
            asn = int(r[0])
            bucket = r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1])
            reqs = int(r[9])
            all_asns_seen[asn] = all_asns_seen.get(asn, 0) + reqs
            all_buckets_set.add(bucket)

        # Selector callers that ask for map_buckets/cities WITHOUT heatmap skip
        # the heatmap query, but the map-bucket assembly still needs the
        # sorted bucket axis for positional bucket_idx; pull bucket times
        # from map_rows in that case so the per-bucket map cells survive.
        if not heatmap_rows and map_rows:
            for r in map_rows:
                bucket = r[5].isoformat() if hasattr(r[5], "isoformat") else str(r[5])
                all_buckets_set.add(bucket)

        all_buckets = sorted(all_buckets_set)
        bucket_idx = {b: i for i, b in enumerate(all_buckets)}
        top_asns = sorted(all_asns_seen, key=lambda a: all_asns_seen[a], reverse=True)[:top_n]
        top_asn_set = set(top_asns)

        # ── Speed distribution (bulk, one query) ──────────────────────────
        # Try the per-hour network_speed rollup first for unfiltered
        # windows >= 48 h. Exact integer SUM across hours (no
        # approximation), so the rollup result is byte-identical to the
        # live SQL — just faster (~50 ms vs ~2.9 s on prod 30 d).
        asn_speed_mix: dict[int, dict[str, float]] = {}
        if _want_leaderboard and has_c_speed and top_asns:
            _t = _time.perf_counter()
            rolled_speed = runner.try_network_speed_from_rollup(
                start_time,
                end_time,
                top_asns=top_asns,
                has_filters=bool(filters),
            )
            if rolled_speed is not None:
                speed_rows = rolled_speed
                timer.mark("speed_distribution_query_rollup", _t)
            elif t is not None:
                placeholders = ",".join(["?"] * len(top_asns))
                _t = _time.perf_counter()
                speed_rows = runner.execute(
                    SQL.SPEED_DISTRIBUTION_BY_ASN.format(
                        table=t,
                        where=w,
                        placeholders=placeholders,
                    ),
                    p + top_asns,
                ).fetchall()
                timer.mark("speed_distribution_query", _t)
            else:
                speed_rows = []  # skip-temp path + speed rollup missed → no fallback
            asn_speed_rows: dict[int, list[tuple]] = {}
            for r in speed_rows:
                asn_v = int(r[0])
                if asn_v not in asn_speed_rows:
                    asn_speed_rows[asn_v] = []
                if len(asn_speed_rows[asn_v]) < 5:
                    asn_speed_rows[asn_v].append((r[1], r[2]))
            for asn_v, rows in asn_speed_rows.items():
                total = sum(cnt for _, cnt in rows)
                if total > 0:
                    asn_speed_mix[asn_v] = {cs: round(cnt / total, 3) for cs, cnt in rows}

        asn_names_map = _db.get_asn_names(src["name"], top_asns)

        # ── Build heatmap entries ──────────────────────────────────────────
        asn_bucket_data: dict[int, dict[str, dict]] = {}
        for r in heatmap_rows:
            asn = int(r[0])
            if asn not in top_asn_set:
                continue
            bucket = r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1])
            tp = float(r[2]) if r[2] is not None else None
            rtt = float(r[3]) if r[3] is not None else None
            rtt_base = float(r[4]) if r[4] is not None else None
            rtt_cong = float(r[5]) if r[5] is not None else None
            pkt = float(r[6]) if r[6] is not None else None
            jitter = float(r[7]) if r[7] is not None else None
            err = float(r[8]) if r[8] is not None else None
            reqs = int(r[9])
            hs = _health_score(tp, rtt_cong, pkt, jitter, err)
            asn_bucket_data.setdefault(asn, {})[bucket] = {
                "bucket_idx": bucket_idx[bucket],
                "bucket": bucket,
                "throughput_bps": round(tp, 0) if tp is not None else None,
                "rtt_med_us": round(rtt, 0) if rtt is not None else None,
                "rtt_baseline_us": round(rtt_base, 0) if rtt_base is not None else None,
                "rtt_congestion_us": round(rtt_cong, 0) if rtt_cong is not None else None,
                "avg_ploss": round(pkt, 5) if pkt is not None else None,
                "rtt_jitter_us": round(jitter, 0) if jitter is not None else None,
                "error_pct": round(err, 2) if err is not None else None,
                "health_score": hs,
                "reqs": reqs,
            }

        heatmap = []
        for asn in top_asns:
            name = asn_names_map.get(asn) or f"AS{asn}"
            label = _db.format_asn_label(asn, name)
            bucket_list = list(asn_bucket_data.get(asn, {}).values())
            heatmap.append(
                {
                    "asn": asn,
                    "label": label,
                    "total_reqs": all_asns_seen.get(asn, 0),
                    "buckets": bucket_list,
                }
            )

        # ── World map buckets ──────────────────────────────────────────────
        # City names ("San Jose", "Los Angeles") repeat heavily across the
        # 288 buckets in a 30d window — typically 50+ cities per bucket,
        # often the same ~200 unique cities. Interning the names into a
        # top-level ``cities`` array (referenced by ``city_idx``) already
        # cut the payload ~90%. Now hoist ``lat``/``lon`` into the same
        # interned record so cells stop repeating coordinates that depend
        # only on the city. SQL GROUP BY at backend/repositories/_sql/
        # network.py:92 includes ``(city, lat, lon)``, so the same city
        # name can appear with distinct centroids (duplicate MaxMind
        # entries); key the intern map by ``(name, lat, lon)`` so each
        # geocenter still gets its own entry — no positional collapse.
        map_buckets: list[dict] = []
        cities_list: list[dict[str, Any]] = []
        cities_index: dict[tuple[str, float | None, float | None], int] = {}

        def _intern_city(name: str, lat: float | None, lon: float | None) -> int:
            key = (name, lat, lon)
            idx = cities_index.get(key)
            if idx is None:
                idx = len(cities_list)
                cities_list.append({"name": name, "lat": lat, "lon": lon})
                cities_index[key] = idx
            return idx

        if map_rows:
            dma_map = _db._get_dma_map() if has_metro else {}
            map_by_bucket: dict[str, list[dict]] = {}
            for r in map_rows:
                ctry = r[0]
                city = r[1] or ""
                lat = float(r[2]) if r[2] is not None else None
                lon = float(r[3]) if r[3] is not None else None
                metro_raw = r[4]
                bucket = r[5].isoformat() if hasattr(r[5], "isoformat") else str(r[5])
                rtt = float(r[6]) if r[6] is not None else None
                pkt = float(r[7]) if r[7] is not None else None
                err = float(r[8]) if r[8] is not None else None
                reqs = int(r[9])
                hs = _health_score(None, None, pkt, None, err)

                metro_code: int | None = None
                if metro_raw is not None:
                    try:
                        metro_code = int(float(metro_raw))
                    except (ValueError, TypeError):
                        pass

                display_city = city.title() if city else ""
                if metro_code is not None and str(metro_code) in dma_map:
                    display_city = dma_map[str(metro_code)]

                map_by_bucket.setdefault(bucket, []).append(
                    {
                        "country": ctry,
                        "city_idx": _intern_city(display_city, lat, lon),
                        "metro_code": metro_code,
                        "rtt_med_us": rtt,
                        "avg_ploss": pkt,
                        "error_pct": round(err, 2) if err is not None else None,
                        "health_score": hs,
                        "reqs": reqs,
                    }
                )

            for i, bucket in enumerate(all_buckets):
                map_buckets.append(
                    {
                        "bucket_idx": i,
                        "bucket": bucket,
                        "cities": map_by_bucket.get(bucket, []),
                    }
                )

        # ── Metro leaderboard ──────────────────────────────────────────────
        metro_leaderboard: list[dict] = []
        if metro_rows:
            dma_map = _db._get_dma_map() if has_metro else {}
            for r in metro_rows:
                ctry = r[0]
                city = r[1] or ""
                region = r[2] or ""
                metro_raw = r[3]
                pkt = float(r[5]) if r[5] is not None else None
                err = float(r[6]) if r[6] is not None else None
                hs = _health_score(None, None, pkt, None, err)

                metro_code = None
                if metro_raw is not None:
                    try:
                        metro_code = int(float(metro_raw))
                    except (ValueError, TypeError):
                        pass

                if metro_code is not None and str(metro_code) in dma_map:
                    city_name = dma_map[str(metro_code)]
                    display = format_city_label(city_name, ctry, region)
                else:
                    display = format_city_label(city, ctry, region)

                metro_leaderboard.append(
                    {
                        "country": ctry,
                        "region": region,
                        "raw_city": city,
                        "city": display,
                        "total_reqs": int(r[7]),
                        "health_score": hs,
                    }
                )

            # Deduplicate by display name (same city can appear with and without metro code)
            seen: dict[str, dict] = {}
            for entry in metro_leaderboard:
                key = entry["city"]
                if key in seen:
                    prev = seen[key]
                    prev_reqs = prev["total_reqs"]
                    new_reqs = entry["total_reqs"]
                    total = prev_reqs + new_reqs
                    if prev["health_score"] is not None and entry["health_score"] is not None:
                        prev["health_score"] = round(
                            (prev["health_score"] * prev_reqs + entry["health_score"] * new_reqs) / total, 1
                        )
                    prev["total_reqs"] = total
                else:
                    seen[key] = dict(entry)
            metro_leaderboard = sorted(seen.values(), key=lambda x: x["total_reqs"], reverse=True)

        # ── P95/P99 RTT per ASN (bulk) ─────────────────────────────────────
        # Try the per-hour network_rtt rollup first for unfiltered windows
        # >= 48 h — the rollup serves in ~50 ms on prod 30 d vs the live
        # bulk query's ~5.2 s. The rollup gates on `not filters` so the
        # filtered path always falls through to the live SQL below.
        asn_rtt_pct: dict[int, dict[str, float | None]] = {}
        if _want_leaderboard and top_asns:
            _t = _time.perf_counter()
            rolled = runner.try_network_rtt_from_rollup(
                start_time,
                end_time,
                top_asns=top_asns,
                has_filters=bool(filters),
            )
            if rolled is not None:
                asn_rtt_pct = rolled
                timer.mark("rtt_percentiles_query_rollup", _t)
            elif t is not None:
                placeholders = ",".join(["?"] * len(top_asns))
                _t = _time.perf_counter()
                pct_rows = runner.execute(
                    SQL.RTT_PERCENTILES_BY_ASN.format(
                        table=t,
                        where=w,
                        placeholders=placeholders,
                    ),
                    p + top_asns,
                ).fetchall()
                timer.mark("rtt_percentiles_query", _t)
                for row in pct_rows:
                    asn_v = int(row[0])
                    asn_rtt_pct[asn_v] = {
                        "p95_rtt_us": round(float(row[1]), 0) if row[1] is not None else None,
                        "p99_rtt_us": round(float(row[2]), 0) if row[2] is not None else None,
                    }
            # else: skip-temp path + RTT rollup missed → leave asn_rtt_pct empty

        # ── ASN leaderboard ────────────────────────────────────────────────
        leaderboard: list[dict] = []
        for asn in top_asns:
            name = asn_names_map.get(asn) or f"AS{asn}"
            label = _db.format_asn_label(asn, name)
            buckets_data = asn_bucket_data.get(asn, {})
            if not buckets_data:
                continue

            latest_bucket = max(buckets_data)
            latest = buckets_data[latest_bucket]
            hs_now = latest.get("health_score")

            hs_1h = hs_now
            hs_1w = hs_now
            if len(all_buckets) > 1:
                n_buckets_1h = max(1, 3600 // bucket_seconds)
                sorted_buckets = sorted(buckets_data)

                early = sorted_buckets[: max(1, len(sorted_buckets) // 4)]
                hs_1w = _avg_hs(buckets_data, early)
                mid = sorted_buckets[max(0, len(sorted_buckets) - n_buckets_1h) :]
                hs_1h = _avg_hs(buckets_data, mid)

            trend = "stable"
            if hs_now is not None and hs_1h is not None:
                delta = hs_now - hs_1h
                if delta < -5:
                    trend = "degrading"
                elif delta > 5:
                    trend = "improving"

            rtt_pct = asn_rtt_pct.get(asn, {})
            leaderboard.append(
                {
                    "asn": asn,
                    "label": label,
                    "health_score_now": hs_now,
                    "health_score_1h_ago": hs_1h,
                    "health_score_1w_ago": hs_1w,
                    "trend": trend,
                    "total_reqs": all_asns_seen.get(asn, 0),
                    "c_speed_mix": asn_speed_mix.get(asn, {}),
                    "p95_rtt_us": rtt_pct.get("p95_rtt_us"),
                    "p99_rtt_us": rtt_pct.get("p99_rtt_us"),
                }
            )

        # ── Global summary ─────────────────────────────────────────────────
        all_hs = [le["health_score_now"] for le in leaderboard if le["health_score_now"] is not None]
        global_hs = round(sum(all_hs) / len(all_hs), 1) if all_hs else 0

        all_rtt: list[float] = []
        total_reqs = 0
        for r in heatmap_rows:
            if r[3] is not None:
                all_rtt.append(float(r[3]))
            total_reqs += int(r[9])

        avg_rtt_ms = round(sum(all_rtt) / len(all_rtt) / 1000.0, 1) if all_rtt else 0

        worst_asn = None
        if leaderboard:
            significant = [le for le in leaderboard if le["total_reqs"] > total_reqs * 0.01]
            if significant:
                worst = min(
                    significant, key=lambda le: le["health_score_now"] if le["health_score_now"] is not None else 100
                )
                worst_asn = {"label": worst["label"], "score": worst["health_score_now"]}

        worst_country = None
        if has_country and map_buckets:
            latest_cities = map_buckets[-1]["cities"]
            # M-4: the prior ``reqs > 10`` floor frequently left worst_country
            # blank on low-traffic 24h windows, rendering "Worst Region: --"
            # alongside a populated Worst ASN. Drop to 1 so the panel
            # surfaces something whenever the data has any city signal at
            # all — operators reading "--" assumed the page was broken.
            sig_countries = [c for c in latest_cities if c["reqs"] >= 1]
            if sig_countries:
                wc = min(sig_countries, key=lambda c: c["health_score"] if c["health_score"] is not None else 100)
                # Resolve the interned city name for the worst-country label.
                idx = wc.get("city_idx", -1)
                city_entry = cities_list[idx] if 0 <= idx < len(cities_list) else None
                city_name = city_entry["name"] if city_entry else ""
                label = format_city_label(city_name, wc["country"])
                worst_country = {"label": label, "score": wc["health_score"]}

        payload: dict[str, Any] = {
            "available": True,
            "metric": metric,
            "bucket_seconds": bucket_seconds,
            "countries": countries,
            "has_metro": has_metro,
            "section_timings": section_timings,
            **runner.telemetry(),
        }
        if _want("buckets"):
            payload["buckets"] = all_buckets
        if _want("heatmap"):
            payload["heatmap"] = heatmap
        if _want("map_buckets"):
            payload["map_buckets"] = map_buckets
        if _want("cities"):
            payload["cities"] = cities_list
        if _want("leaderboard"):
            payload["leaderboard"] = leaderboard
        if _want("metro_leaderboard"):
            payload["metro_leaderboard"] = metro_leaderboard
        if _want("summary"):
            payload["summary"] = {
                "global_health_score": global_hs,
                "avg_rtt_ms": avg_rtt_ms,
                "total_reqs": total_reqs,
                "worst_asn": worst_asn,
                "worst_country": worst_country,
            }
        # Write for EVERY section-set (the key is sections-partitioned), not
        # only ``sections is None`` — that's what makes the live FE shapes
        # cacheable. SECURITY: only cache a result that carries real signal so
        # a transient view-lag / mid-commit empty is never frozen for the TTL
        # and served to every request on this key (the dashboard-cache
        # poisoning bug class). ``available: False`` results return earlier and
        # never reach here; this guards the available-but-zero-rows case.
        if _has_signal(payload):
            # network additionally strips section_timings (per-request paint
            # telemetry) from the stored copy.
            cache_put(_response_cache, cache_key, payload, strip=("section_timings",))
        return payload

    finally:
        if temp_table is not None:
            try:
                runner.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
            except Exception:
                pass


def get_quality(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    region_country: str = "US",
) -> dict[str, Any]:
    """Return TCP RTT metrics aggregated by country, ASN, region, PoP, and a scatter sample."""
    import time as _time

    timer = SectionTimer()
    section_timings = timer.entries

    table_name = _safe_table(src["name"])

    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = set(runner.get_schema_cols())
    timer.mark("get_schema_cols", _t)

    if not actual_cols or "tcp_rtt" not in actual_cols:
        return {
            "available": False,
            "by_country": [],
            "by_asn": [],
            "by_region": [],
            "region_country": region_country,
            "by_pop": [],
            "scatter": [],
            "countries": [],
            **runner.telemetry(),
        }

    params, where_clause = build_where_clause(start_time, end_time, filters, list(actual_cols), inline_params=True)
    rtt_filter = f"{where_clause} AND tcp_rtt IS NOT NULL AND tcp_rtt > 0"

    try:
        runner.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except duckdb.CatalogException:
        return {
            "available": False,
            "by_country": [],
            "by_asn": [],
            "by_region": [],
            "region_country": region_country,
            "by_pop": [],
            "scatter": [],
            "countries": [],
            **runner.telemetry(),
        }

    def run_bar(group_col: str, extra_where: str = "", extra_params: list | None = None) -> list[dict]:
        sql = SQL.QUALITY_BAR_BY_GROUP.format(
            group_col=group_col,
            table=table_name,
            rtt_filter=rtt_filter,
            extra_where=extra_where,
        )
        _t = _time.perf_counter()
        rows = runner.execute(sql, params + (extra_params or [])).fetchall()
        timer.mark(f"quality_bar:{group_col}", _t)
        return [
            {"value": str(r[0]), "label": str(r[0]), "rtt_ms": round(float(r[1]), 2), "reqs": int(r[2])} for r in rows
        ]

    countries_sql = SQL.QUALITY_COUNTRIES_DISTINCT.format(
        table=table_name,
        where_clause=where_clause,
    )
    _t = _time.perf_counter()
    countries = [r[0] for r in runner.execute(countries_sql, params).fetchall()]
    timer.mark("countries_distinct", _t)

    by_country = run_bar("country")
    by_asn = run_bar("asn") if "asn" in actual_cols else []
    if by_asn:
        # Mirror the ASN leaderboard / dashboard: display "Name (7922)" instead
        # of the bare number, keeping `value` as the click-to-filter key.
        try:
            _db.enrich_asn_labels(by_asn, src["name"])
        except Exception:
            pass
    by_region = (
        run_bar("region", extra_where=" AND country = ?", extra_params=[region_country])
        if "region" in actual_cols
        else []
    )
    # by_pop rows stay as the bare PoP code (value == label); the frontend
    # renders the city/region/country via the shared <PopLabel> component
    # (fed by bootstrap's pop_geo map). See frontend/lib/pop.ts.
    by_pop = run_bar("pop") if "pop" in actual_cols else []

    scatter: list[dict] = []
    if "ttfb" in actual_cols:
        scatter_sql = SQL.QUALITY_SCATTER.format(
            table=table_name,
            rtt_filter=rtt_filter,
        )
        _t = _time.perf_counter()
        scatter = [
            {"rtt_ms": round(float(r[0]), 2), "ttfb_ms": round(float(r[1]), 2), "cache": str(r[2])}
            for r in runner.execute(scatter_sql, params).fetchall()
        ]
        timer.mark("scatter_query", _t)

    return {
        "available": True,
        "by_country": by_country,
        "by_asn": by_asn,
        "by_region": by_region,
        "region_country": region_country,
        "by_pop": by_pop,
        "scatter": scatter,
        "countries": countries,
        "section_timings": section_timings,
        **runner.telemetry(),
    }


# A-3 (CacheRegistry): register the network response cache (also
# pointed at by R-1's tests/conftest.py follow-on).
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("network._response_cache", _response_cache)
