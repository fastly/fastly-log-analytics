"""Shared primitives for the rollups package.

Constants, ident validators, path helpers, atomic marker IO, COPY query
builders, and the virtual-field backing map — everything every other
sub-module needs.
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from backend.core.sql_validator import escape_sql_literal, is_safe_ident
from backend.utils.telemetry import parse_hour_key

logger = logging.getLogger(__name__)

# How many top values per (field, hour) we persist. Dashboards render
# 10-25 at a time; 500 gives generous headroom for filter overlays and
# the long-tail "Other" rollup.
TOP_K = 500

# Per-(field, hour, value) IP sample cap for the ip_spread rollup
# writer. Each row in ip_spread.parquet stores a HyperLogLog sketch
# built from at most this many distinct IPs (DuckDB's
# array_slice(array_agg(DISTINCT ip), 1, K) at write time). Sketches
# built from samples larger than this would inflate parquet bundle
# bytes linearly; the HLL itself is constant-size regardless of input
# cardinality (see backend/utils/hll.py), but the intermediate Python
# transfer scales with the array length so we cap it here.
#
# For (field, value) groups where the true distinct count exceeds the
# cap, the per-row ``sample_capped`` flag is set; the reader can then
# raise an admin warning or label the FE display as approximate. In
# practice this only fires on very common values (e.g. a ja3 used by
# the dominant browser family) — the long-tail fingerprints that
# matter most to security have distinct IP counts well under the cap.
IP_SAMPLE_CAP = 5000


# Filename for the per-(field, hour) IP-spread rollup. Stored in a
# parallel directory tree (``rollups/hour_ip_spread`` instead of
# ``rollups/hour``) so the existing count-rollup readers stay
# untouched and so a bundle writer can enumerate the two trees
# independently. Mirrors the per-day shape under
# ``rollups/day_ip_spread``.
IP_SPREAD_DIR_NAME = "hour_ip_spread"

# Sibling filename in hour_bundled / day_bundled for the bundled
# IP-spread parquet (one file per bundle dir, containing rows for
# every field). Lives alongside ``all_fields.parquet`` so the bundle
# reader can pick them up in one directory walk.
IP_SPREAD_BUNDLE_FILENAME = "all_fields_ip.parquet"

# SQL identifier safelist. Field names land verbatim inside ``"..."``
# quoted identifiers and inside SELECT projections; service names land
# in the table identifier ``logs_<name>``. Both come from cfg / DuckDB
# schema and are PROBABLY already validated upstream — but a single
# stray double-quote or backtick in either would break the query in a
# way that's both a correctness bug and a privilege boundary (the
# fields are derived from admin-controlled custom_field entries).
# Defense in depth: this module reject anything not matching the
# pattern with a logged warning.
_is_safe_ident = is_safe_ident


def _safe_table_for(source: dict[str, Any]) -> str | None:
    """Return the DuckDB view name for this service, or ``None`` if no slug.

    Slugifies the same way the dashboard's view-builder does
    (``backend.core.duckdb._safe_table_name``: non-alphanumerics to ``_``,
    lowercased, ``logs_`` prefix) so the rollup COPY/SELECT targets the
    same view name the dashboard creates. Reads ``name`` first (matching
    ``update_storage_view``) and falls back to ``service_id`` for callers that
    pass a raw on-disk config — both cases pass through the slugifier
    identically.
    """
    slug = source.get("name") or source.get("service_id")
    if not slug:
        logger.warning("[rollups] no service_id/name in source dict; skipping rollup")
        return None
    from backend.core.duckdb import _safe_table_name

    return _safe_table_name(slug)


def _get_fields(src: dict[str, Any]) -> list[str]:
    """Return the dashboard fields eligible for rollup.

    Custom-field names are validated against ``_SAFE_IDENT_RE`` — anything
    failing the check is skipped with a warning rather than fed into SQL.

    Includes virtual fields (waf_sig_ind, edge_score_reason_ind) — those
    used to be excluded because they require unnesting a CSV column, but
    we now have a dedicated SQL builder (``_build_virtual_field_copy_query``)
    that does the unnest at write time so the dashboard reader doesn't
    have to rescan + unnest the raw window at query time.

    Excludes every OTHER derived field (``vcl=None``: METRICS group like
    requests/hit_rate/p95_latency, plus any VIRTUAL-group field with no
    dedicated backing, plus INTERNAL) — those have no real per-row column
    to SELECT. Checked via the registry's own ``is_derived``, not a hand-
    maintained list: a hand list is exactly how this broke live (a VIRTUAL-
    group field, ``_bot_name``, was never in the narrow ``_VIRTUAL_FIELDS``
    tuple this used to filter against, so it slipped through as an "actual"
    field). See AGENTS.md Trap #40.
    """
    from backend.core.log_fields import try_get
    from backend.repositories.dashboard import _VIRTUAL_FIELDS, FIELDS

    def _is_unbacked_derived(code: str) -> bool:
        if code in _VIRTUAL_FIELDS:
            return False
        spec = try_get(code)
        return spec is not None and spec.is_derived

    lf_config = src.get("log_fields") or {}
    custom_field_names: list[str] = []
    for cf in lf_config.get("custom_fields", []):
        if not cf.get("enabled", True) or not cf.get("show_in_dashboard", True):
            continue
        name = cf.get("name") or ""
        if not _is_safe_ident(name):
            logger.warning("[rollups] skipping custom field with unsafe name: %r", name)
            continue
        custom_field_names.append(name)
    actual_fields = [
        f for f in FIELDS if f not in _VIRTUAL_FIELDS and not _is_unbacked_derived(f) and _is_safe_ident(f)
    ]
    virtual_fields = [f for f in _VIRTUAL_FIELDS if f in _VIRTUAL_FIELD_BACKING and _is_safe_ident(f)]
    return actual_fields + virtual_fields + custom_field_names


def _rollups_root(source: dict[str, Any]) -> str:
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "hour")


def _day_rollups_root(source: dict[str, Any]) -> str:
    """Per-day compacted rollups directory.

    Companion to `_rollups_root` (which holds per-hour rollups). Populated
    by `compact_closed_days_to_daily` — each (field, closed-day) becomes
    a single parquet file aggregating its 24 source hour parquets. The
    reader (`execute_top_n_rollups`) prefers per-day files for closed
    days and falls back to per-hour for the active trailing window.
    Item 17 / RC-9.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "day")


def _ip_spread_root(source: dict[str, Any]) -> str:
    """Per-(field, hour) IP-spread rollup root.

    Parallel tree to :func:`_rollups_root` so the existing count-rollup
    enumeration in the reader keeps working untouched. Per-row layout
    inside each ``field=X/hour=Y/`` directory:

      field           VARCHAR
      hour            VARCHAR
      value           VARCHAR
      ip_sketch       BLOB     -- HyperLogLog (256 buckets, 258 bytes)
      ip_count_observed INT    -- distinct count from the IP sample
      sample_capped   BOOLEAN  -- True when the sample hit IP_SAMPLE_CAP

    Built by :func:`backend.core.rollups.recompute._run_ip_spread_per_field`
    alongside the regular count rollup so both files for a given hour
    publish atomically under the per-service view lock.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", IP_SPREAD_DIR_NAME)


def _build_ip_spread_select_query(table_ident: str, field: str, where_sql: str) -> str:
    """Return SQL producing ``(field, hour, value, ip_sample,
    ip_count_observed, request_count)`` rows for one field.

    ``ip_sample`` is a LIST(VARCHAR) capped at :data:`IP_SAMPLE_CAP`
    distinct IPs per (field, hour, value). The writer iterates these
    rows in Python and feeds each ``ip_sample`` into a
    :class:`backend.utils.hll.HyperLogLog`, then writes the sketch
    bytes + the observed count + the cap-flag back out as parquet.

    Rows are filtered to ``ip IS NOT NULL`` because the HLL has no
    sensible "missing" value to hash (would all collide on the same
    bucket and distort the estimate). Top-K filtering matches the
    count rollup's ROW_NUMBER OVER (PARTITION BY hour) shape so the
    two parquets describe the SAME set of (field, hour, value) tuples
    in the same rank order — the reader joins by (field, value) without
    needing to coordinate cut-offs between the two writers.

    Inputs must already be validated by :func:`_is_safe_ident` and
    :func:`_safe_table_for`; this function does NO escaping (mirrors
    :func:`_build_copy_query`'s contract).
    """
    return f"""
        SELECT field, hour, value, ip_sample, ip_count_observed, request_count
        FROM (
            SELECT
                '{field}' AS field,
                strftime(timestamp, '%Y-%m-%d-%H') AS hour,
                CAST("{field}" AS VARCHAR) AS value,
                array_slice(array_agg(DISTINCT "ip"), 1, {IP_SAMPLE_CAP}) AS ip_sample,
                COUNT(DISTINCT "ip") AS ip_count_observed,
                COUNT(*) AS request_count,
                ROW_NUMBER() OVER (
                    PARTITION BY strftime(timestamp, '%Y-%m-%d-%H')
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM {table_ident}
            WHERE {where_sql}
              AND "ip" IS NOT NULL
              AND "{field}" IS NOT NULL
              AND NULLIF(CAST("{field}" AS VARCHAR), '') IS NOT NULL
            GROUP BY 1, 2, 3
        ) WHERE rn <= {TOP_K}
    """


def _build_copy_query(table_ident: str, field: str, where_sql: str) -> str:
    """Return the COPY ... TO <tmp> PARTITION_BY (field, hour) SQL for one field.

    Inputs must already be validated — this function does NO escaping.
    Callers (recompute_touched_hours / backfill_rollups) gate via
    ``_is_safe_ident`` and ``_safe_table_for``.
    """
    if field in ("age", "ttl"):
        val_expr = f'CAST(CAST(ROUND("{field}") AS INTEGER) AS VARCHAR)'
    else:
        val_expr = f'CAST("{field}" AS VARCHAR)'

    return f"""
        SELECT field, hour, value, count FROM (
            SELECT
                '{field}' AS field,
                strftime(timestamp, '%Y-%m-%d-%H') AS hour,
                {val_expr} AS value,
                COUNT(*) AS count,
                ROW_NUMBER() OVER (
                    PARTITION BY strftime(timestamp, '%Y-%m-%d-%H')
                    ORDER BY COUNT(*) DESC, {val_expr} ASC
                ) AS rn
            FROM {table_ident}
            WHERE {where_sql}
              AND "{field}" IS NOT NULL
              AND NULLIF({val_expr}, '') IS NOT NULL
            GROUP BY 1, 2, 3
        ) WHERE rn <= {TOP_K}
    """


# Virtual fields are dashboard panels whose values come from
# unnesting a comma-separated CSV column at query time
# (``backend.repositories.dashboard._VIRTUAL_FIELDS``). Pre-aggregating
# them into the rollup tree eliminates the runtime-unnest cost that
# dominates dashboard 30d (per the perf audit: waf_sig_ind_explode
# ~1.2 s + edge_score_reason_ind_explode ~0.7 s on prod 30d).
#
# Map: <virtual_field_name> → <backing_column_name>.
# Mirrors the call sites in dashboard.py:_exploded_top_n.
_VIRTUAL_FIELD_BACKING: dict[str, str] = {
    "waf_sig_ind": "waf_sig",
    "edge_score_reason_ind": "edge_score_reason",
}


def _build_virtual_field_copy_query(table_ident: str, virtual_field: str, backing_col: str, where_sql: str) -> str:
    """COPY SQL for a virtual (unnest-based) field rollup.

    Same output shape as :func:`_build_copy_query` (field/hour/value/count)
    so the per-field rollup tree, hour bundling, day bundling, and
    reader path all work unchanged. The only difference is the inner
    SELECT does the CSV unnest before grouping.

    Same input-validation contract: callers gate via ``_is_safe_ident``
    on both the virtual field name and the backing column name.
    """
    return f"""
        SELECT field, hour, value, count FROM (
            SELECT
                '{virtual_field}' AS field,
                hour,
                value,
                count,
                ROW_NUMBER() OVER (
                    PARTITION BY hour
                    ORDER BY count DESC
                ) AS rn
            FROM (
                SELECT
                    strftime(timestamp, '%Y-%m-%d-%H') AS hour,
                    trim(signal) AS value,
                    COUNT(*) AS count
                FROM (
                    SELECT timestamp, unnest(string_split("{backing_col}", ',')) AS signal
                    FROM {table_ident}
                    WHERE {where_sql}
                      AND "{backing_col}" IS NOT NULL
                      AND "{backing_col}" != ''
                )
                WHERE trim(signal) != ''
                GROUP BY 1, 2
            )
        ) WHERE rn <= {TOP_K}
    """


def _hour_bundled_root(source: dict[str, Any]) -> str:
    """Return the per-hour bundled rollup root.

    Layout: cache/<svc>/rollups/hour_bundled/hour=YYYY-MM-DD-HH/all_fields.parquet
    Each bundle contains rows for ALL fields for that hour with the same
    (field, value, count) schema as the per-field hour parquets. Reading
    one bundle replaces opening ~40+ per-field files for that hour.

    The same hour directory also holds ``time_series.parquet`` — see
    :func:`build_time_series_bundles` for the schema.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "hour_bundled")


def _day_bundled_root(source: dict[str, Any]) -> str:
    """Return the per-day bundled rollup root.

    Layout: cache/<svc>/rollups/day_bundled/day=YYYY-MM-DD/all_fields.parquet
    Each bundle contains rows for ALL fields for that day with the same
    (field, value, count) schema as the per-field day parquets. Reading
    one bundle replaces opening ~40 per-field files for that day; on a
    30-day window this cuts file opens from ~1,200 to ~30. Per the perf
    audit, ``top_n_rollups:rolled_res`` was the dominant cost
    (4 s on prod 30d) entirely because of per-file open overhead on
    the per-field-day tree.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "day_bundled")


def _tier_has_parquet(root: str) -> bool:
    """True if ``root`` holds at least one ``.parquet`` in any partition dir.

    Cheap, early-exit scan: walks partition dirs only until the first parquet
    is found. An empty root (or partition dirs with no parquet left behind by
    cleanup) counts as absent.
    """
    try:
        with os.scandir(root) as partitions:
            for part in partitions:
                if not part.is_dir():
                    if part.name.endswith(".parquet"):
                        return True
                    continue
                try:
                    with os.scandir(part.path) as entries:
                        for e in entries:
                            if e.is_file() and e.name.endswith(".parquet"):
                                return True
                            # Per-field hour tier nests one more level:
                            # rollups/hour/field=X/hour=H/*.parquet
                            if e.is_dir():
                                try:
                                    with os.scandir(e.path) as sub:
                                        for s in sub:
                                            if s.is_file() and s.name.endswith(".parquet"):
                                                return True
                                except OSError:
                                    continue
                except OSError:
                    continue
    except OSError:
        return False
    return False


def rollups_present(source: dict[str, Any]) -> bool:
    """True if ANY reader-usable rollup tier holds data for this service.

    The dashboard rollup fast-path reader (``execute_top_n_rollups``) serves
    from, in preference order, ``day_bundled`` → ``hour_bundled`` → per-field
    ``day`` → per-field ``hour``. Day compaction deletes the per-field ``hour``
    tree once a day closes, so probing only that tier (the historical bug)
    reported "no rollups" for a fully-usable service and forced every
    unfiltered dashboard request onto the slow wide-temp base scan. This checks
    every tier the reader can actually consume.
    """
    return (
        _tier_has_parquet(_day_bundled_root(source))
        or _tier_has_parquet(_hour_bundled_root(source))
        or _tier_has_parquet(_day_rollups_root(source))
        or _tier_has_parquet(_rollups_root(source))
    )


# Filename for the per-day bundled rollup (same as the per-hour
# bundled). Kept identical so future tooling can treat the two trees
# uniformly when needed.
DAY_BUNDLE_FILENAME = "all_fields.parquet"

# Per-(field, day) row cap inside the bundled-day parquet. The
# dashboard top-N panel renders 10 values; 500 gives generous headroom
# for the global top-10 to be visible across a 30-day window without
# losing steady medium-volume values to per-day truncation. Anything
# beyond rank 500 in a single day is aggregated into a single synthetic
# ``__other__`` row so field totals stay correct.
DAY_BUNDLE_TOP_K = 500


# Filename for the per-hour 1-minute time-series rollup. Kept as a constant
# so the writer + reader can never drift on the name.
TIME_SERIES_BUNDLE_FILENAME = "time_series.parquet"

# Filename for the per-hour per-(ip, ja4) sessions rollup. Stored
# alongside time_series.parquet so the same reader can enumerate both
# in one directory walk.
SESSIONS_BUNDLE_FILENAME = "sessions.parquet"

# Filename for the per-hour top-N URLs rollup feeding the /origin
# slow_urls panel. Per-(url, hour) row stores requests + exact within-
# hour p50/p95/p99 latency so the reader can request-weight-average
# them across a multi-day window without re-scanning raw rows. K=100
# URLs per hour (panel limit is 20–50; this gives headroom for re-
# ranking across hours). Stored alongside time_series.parquet so the
# enumerator can pick both up in one walk.
SLOW_URLS_BUNDLE_FILENAME = "slow_urls.parquet"
SLOW_URLS_BUNDLE_TOP_K = 100
SLOW_URLS_BUNDLE_MIN_REQUESTS_PER_HOUR = 5

# Filename for the per-hour origin-summary rollup feeding /api/origin
# /aggregates' ``summary`` panel. One row per closed hour with the
# aggregate counts + per-hour percentiles + percentile counts. The
# reader request-weight-averages across hours, same posture as
# slow_urls — biased relative to the true cross-hour percentile but
# preserves the headline numbers users actually read off the card.
ORIGIN_SUMMARY_BUNDLE_FILENAME = "origin_summary.parquet"

# Filename for the per-hour per-ASN TCP-RTT percentile rollup feeding
# /api/network-health's rtt_percentiles_query (5.2 s on prod 30d).
# Schema: (asn, requests, rtt_count, p95_us, p99_us). Top-K=100
# ASNs per hour ranked by requests with HAVING >= 5/hour. Same
# request-weighted-average posture as slow_urls — ranking-stable
# across hours.
NETWORK_RTT_BUNDLE_FILENAME = "network_rtt.parquet"
NETWORK_RTT_BUNDLE_TOP_K = 100
NETWORK_RTT_BUNDLE_MIN_REQUESTS_PER_HOUR = 5

NETWORK_QUALITY_COUNTRY_FILENAME = "network_quality_country.parquet"
NETWORK_QUALITY_ASN_FILENAME = "network_quality_asn.parquet"
NETWORK_QUALITY_REGION_FILENAME = "network_quality_region.parquet"
NETWORK_QUALITY_POP_FILENAME = "network_quality_pop.parquet"

# Filename for the per-hour per-ASN client-speed (c_speed) distribution
# rollup feeding /api/network-health's speed_distribution_query (2.9 s
# on prod 30 d). Schema: (asn, c_speed, count). Same top-K=100 ASNs
# per hour cap as network_rtt. Math is EXACT across hours (SUM of
# integer counts) — no _approx flag needed.
NETWORK_SPEED_BUNDLE_FILENAME = "network_speed.parquet"
NETWORK_SPEED_BUNDLE_TOP_K = 100
NETWORK_SPEED_BUNDLE_MIN_REQUESTS_PER_HOUR = 5

# Filename for the per-hour MINUTE-granular verified-bot time-series rollup
# feeding /api/security/aggregates' verified_bots_ts panel (~1.2 s on prod
# 30 d). Schema: (bucket_ts TIMESTAMPTZ, bot_type VARCHAR, count BIGINT).
# Unlike network_speed there is NO top-K cap — bot_type is a small fixed
# vocabulary. Math is EXACT across hours (SUM of integer counts) for any
# caller bucket_seconds that's a multiple of 60; the reader re-buckets from
# the stored minute granularity. Unlike the leaderboard rollups, the day
# compactor PRESERVES the minute dimension so a time series can still be
# produced over a multi-day window.
VERIFIED_BOTS_TS_BUNDLE_FILENAME = "verified_bots_ts.parquet"

# Filenames for the per-hour top-N url/asn latency rollups feeding
# /api/performance/aggregates' top_urls (~1.45 s) + top_asns (~0.77 s) panels.
# Same per-dimension-percentiles-over-window shape as slow_urls/network_rtt
# (no time bucket): per-(value, hour) requests + elapsed sum/count + exact
# within-hour p50/p95/p99, top-K by p99. Reader request-weight-averages the
# percentiles across hours (biased → _approx) and re-ranks by the caller's
# sort_by. K=100; per-hour floors 5 (urls) / 10 (asns) match the live HAVING.
PERF_TOP_URLS_BUNDLE_FILENAME = "perf_top_urls.parquet"
PERF_TOP_ASNS_BUNDLE_FILENAME = "perf_top_asns.parquet"
PERF_LATENCY_BUNDLE_TOP_K = 100
PERF_URLS_MIN_REQUESTS_PER_HOUR = 5
PERF_ASNS_MIN_REQUESTS_PER_HOUR = 10

# Filenames for the three per-hour origin-dimension percentile rollups
# feeding /api/origin/aggregates' pop_latency / ip_health / path_breakdown
# panels. All three are the same per-dimension-percentiles-over-window shape
# as slow_urls (no time bucket): per-(key, hour) requests + lat_us sum/count +
# exact within-hour p50/p95. The reader request-weight-averages the
# percentiles across hours (biased → _approx).
#
#  - origin_pop.parquet   key=pop  — top-K by requests (pop cardinality is
#    small; top-100 keeps every pop).
#  - origin_ip.parquet    key=oip  — carries ost_5xx_count + ost_total_count
#    so error_pct is EXACT across hours (SUM(5xx)/SUM(total)); per-hour floor
#    HAVING COUNT(*) >= 5, top-K by requests. The reader re-applies the
#    window-level HAVING SUM(requests) >= 10 + final ORDER BY error_pct DESC.
#  - origin_path.parquet  key=edge (bool) — NO top-K, NO HAVING (only 2 rows
#    per hour).
ORIGIN_POP_BUNDLE_FILENAME = "origin_pop.parquet"
ORIGIN_IP_BUNDLE_FILENAME = "origin_ip.parquet"
ORIGIN_PATH_BUNDLE_FILENAME = "origin_path.parquet"
POP_HEALTH_BUNDLE_FILENAME = "pop_health.parquet"
ORIGIN_DIMS_BUNDLE_TOP_K = 100
# Per-hour minimum-request floor for the oip bundle (mirrors the slow_urls
# noise cut; the live IP_HEALTH panel applies a window-level HAVING >= 10
# which the reader re-applies, this is the per-hour pre-cut).
ORIGIN_IP_MIN_REQUESTS_PER_HOUR = 5

# Filename for the per-hour MINUTE-granular origin-latency-percentile
# time-series rollup feeding /api/origin/aggregates' ``timeseries`` panel.
# A NEW hybrid shape: time-series (like verified_bots_ts) + percentiles (like
# slow_urls). Each closed hour pre-aggregates to per-minute rows carrying BOTH
# latency bases (ttfb + ttlb) so the reader can serve either metric:
#
#   bucket_ts    TIMESTAMPTZ  -- minute-truncated UTC instant
#   ttfb_count   BIGINT       -- COUNT(*) FILTER (ttfb_lat IS NOT NULL)
#   ttfb_p50_us  DOUBLE       -- MEDIAN(ttfb_lat) within (minute)
#   ttfb_p95_us  DOUBLE       -- APPROX_QUANTILE(ttfb_lat, 0.95)
#   ttfb_p99_us  DOUBLE       -- APPROX_QUANTILE(ttfb_lat, 0.99)
#   ttlb_count   BIGINT       -- COUNT(*) FILTER (ttlb_lat IS NOT NULL)
#   ttlb_p50_us / ttlb_p95_us / ttlb_p99_us  DOUBLE
#
# The reader re-buckets the minute grain to any whole-minute width and
# request-weight-averages the percentiles across minutes (biased → _approx);
# counts SUM exact. The day compactor PRESERVES the minute dimension (it is a
# time series, not a leaderboard) — same posture as verified_bots_ts.
ORIGIN_LATENCY_TS_BUNDLE_FILENAME = "origin_latency_ts.parquet"

# Filenames for the four per-hour security-dimension rollups feeding
# /api/security/aggregates' req_size / conn_reuse / topips / coverage panels
# (each an all-rows live scan today). ALL FOUR are EXACT (counts / MAX) — no
# percentile approximation, so none of the readers carry an ``_approx`` flag.
# Each is a closed-hours-only reader (NO active-hour merge, matching slow_urls).
#
#  - security_req_size.parquet  — req_header_bytes histogram bucket (matching
#    REQ_HEADER_SIZE_DIST's CASE) + count + MIN(req_header_bytes). Schema:
#    (bucket VARCHAR, count BIGINT, min_val BIGINT).
#  - security_conn_reuse.parquet — conn_requests reuse bucket (matching
#    CONN_REUSE_DIST's CASE) + count + MIN(conn_requests). Schema:
#    (bucket VARCHAR, count BIGINT, min_val BIGINT).
#  - security_topips.parquet     — top-500 client IPs by MAX(req_header_bytes)
#    per hour (matching TOP_IPS_BY_MAX_HEADER, capped wider than the panel's
#    top-10 so cross-hour re-ranking by MAX-of-MAX stays correct). Schema:
#    (ip VARCHAR, max_header BIGINT).
#  - security_cov.parquet        — one row per hour with the TLS-fingerprint
#    coverage counts (matching FINGERPRINT_COVERAGE_BULK over tls_ciphers_sha).
#    Schema: (total_rows BIGINT, tls_populated BIGINT).
SECURITY_REQ_SIZE_BUNDLE_FILENAME = "security_req_size.parquet"
SECURITY_CONN_REUSE_BUNDLE_FILENAME = "security_conn_reuse.parquet"
SECURITY_TOPIPS_BUNDLE_FILENAME = "security_topips.parquet"
SECURITY_COV_BUNDLE_FILENAME = "security_cov.parquet"
# Per-hour cap on the topips bundle. The panel renders top-10, but the
# cross-hour reader re-ranks by MAX-of-MAX so a wider per-hour keep avoids
# dropping an IP whose window-max lands outside any single hour's top-10.
SECURITY_TOPIPS_BUNDLE_TOP_K = 500

# Per-hour rollups feeding /api/security/aggregates' ``proxies`` + ``bot_efficiency``
# sections, so the catalog temp is skipped entirely when both are rollup-served.
#  - security_proxy_quality.parquet — (type VARCHAR, count BIGINT): the
#    GET_SECURITY_PROXIES_COMBINED traffic-quality classification over ALL rows.
#  - security_proxy_flagged.parquet — one row per distinct flagged client tuple
#    (rtt_min present AND ratio < 0.2 or a distance mismatch); a small slice of
#    traffic. Schema: (ip, asn, pop, rtt_min, tcp_rtt, lat, lon, country, city,
#    cnt). Distance is NOT stored: the reader recomputes it from (pop, lat, lon)
#    so a POP-coordinate update applies to old hours too.
#  - security_bot_eff.parquet — (kind, key, cnt, resp_bytes) in the exact row
#    shape BOT_EFFICIENCY returns ('site' totals, top-K 'ua' by bytes, 'ref'
#    distinct-IP count per referral stem).
SECURITY_PROXY_QUALITY_BUNDLE_FILENAME = "security_proxy_quality.parquet"
SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME = "security_proxy_flagged.parquet"
SECURITY_BOT_EFF_BUNDLE_FILENAME = "security_bot_eff.parquet"
# Matches BOT_EFFICIENCY's ``LIMIT 2000`` on the per-UA aggregate.
SECURITY_BOT_EFF_UA_TOP_K = 2000

# Filename for the per-hour NGWAF-bots rollup feeding get_top_bots'
# ``ngwaf_bots`` panel and /api/security/aggregates' ``ngwaf_verified_bots``
# panel. One row per (bot_name, wellknown_bot_name, category) per closed hour
# — the hour's waf_req_ids joined against the ngwaf_bots cache at WRITE
# time, so the reader SUMs tiny parquets instead of joining the raw window
# per request. EXACT SUM across hours; no per-hour cap (bot cardinality is
# tens, not thousands). Schema: (bot_name VARCHAR, wellknown_bot_name VARCHAR,
# category VARCHAR, count BIGINT).
#
# Freshness note: the bot cache syncs every ~5 min, so an hour built right
# at close can miss attributions for the final few minutes — bounded, tiny,
# and one-sided (the panel is a top-10 by count over multi-hour windows).
# Retention note: the cache trims old rows by synced_at; a rollup built
# while the rows were live PRESERVES those counts after the cache forgets
# them, so old windows read BETTER from the rollup than from a live join.
NGWAF_BOTS_BUNDLE_FILENAME = "ngwaf_bots.parquet"

# Filename for the per-hour minute-granularity NGWAF-bots time-series rollup
# feeding /api/security/aggregates' ``ngwaf_verified_bots_ts`` panel. Stored
# at 1-minute resolution so the reader can re-bucket to any caller-requested
# ``bucket_seconds`` (matching ``verified_bots_ts.parquet``). Schema:
# (bucket_ts TIMESTAMPTZ, bot_name VARCHAR, count BIGINT). EXACT SUM across
# hours; no ``_approx`` flag.
NGWAF_BOTS_TS_BUNDLE_FILENAME = "ngwaf_bots_ts.parquet"

# Filename for the per-hour performance-dimension TTL-distribution rollup
# feeding /api/performance/aggregates' ``ttl_dist`` histogram panel (an
# all-rows live scan today). One row per closed hour PER ttl bucket, matching
# the live histogram CASE (``ttl <= 0 / <= 10 / ... / > 1y``). Schema:
# (bucket VARCHAR, count BIGINT, min_ttl BIGINT). The math is EXACT across
# hours — ``count`` SUMs and ``min_ttl`` is MIN-of-MIN (composing the live
# query's ``ORDER BY min_ttl``), so the reader carries NO ``_approx`` flag.
# Closed-hours-only reader (NO active-hour merge, matching slow_urls).
PERF_TTL_DIST_BUNDLE_FILENAME = "perf_ttl_dist.parquet"

# Filename for the per-hour per-ASN heatmap rollup feeding /api/network-health's
# ``heatmap`` + ``leaderboard`` + ``summary`` + ``buckets`` sections (all of
# which the 2–4 s ``create_filtered_temp_table`` on 30 d windows blocks).
# Schema per (asn, hour): (asn, hour_ts, reqs, errors, resp_bytes_sum,
# rtt_p50_us, rtt_min_p50_us, rtt_var_p50_us, rtt_count, ploss_sum,
# ploss_count). Top-K=200 ASNs per hour ranked by reqs with HAVING >= 3.
# The reader groups by (asn, hour_ts) — each row becomes ONE heatmap bucket
# at hourly granularity, which is still meaningful for 30 d windows.
NETWORK_HEATMAP_BUNDLE_FILENAME = "network_heatmap.parquet"
NETWORK_HEATMAP_BUNDLE_TOP_K = 200
NETWORK_HEATMAP_BUNDLE_MIN_REQUESTS_PER_HOUR = 3

# Filename for the per-hour per-(country, city, lat, lon, metro) geo rollup
# feeding /api/network-health's ``map_buckets`` + ``cities`` + ``metro_leaderboard``
# sections. Schema per (geocell, hour): (country, city, lat, lon, metro,
# hour_ts, reqs, errors, ploss_sum, ploss_count, rtt_sum, rtt_count).
# No top-K cap (geo cardinality is bounded by distinct geocells, not open-ended
# like ASNs). The reader serves both map_rows (per geocell, per hour_ts bucket)
# and metro_rows (aggregated across all hours, no time dimension).
NETWORK_GEO_BUNDLE_FILENAME = "network_geo.parquet"

# Filename for the per-hour overview rollup feeding /api/value/summary's
# combined overview + caching sections. One row per closed hour with
# SUM-aggregatable counts covering requests, cache states, bandwidth,
# shield, WAF, and elapsed-time sums/counts. The reader re-buckets via
# time_bucket(interval, hour_start) and computes derived metrics
# (offload_pct, accel_factor) from the window-wide SUMs.
OVERVIEW_BUNDLE_FILENAME = "overview.parquet"

NETWORK_SUMMARY_BUNDLE_FILENAME = "network_summary.parquet"


def unlink_quiet(path: str) -> None:
    """Remove ``path``, ignoring any OSError (missing file, permissions, ...).

    The cleanup-after-failure idiom for temp parquet files: the caller is
    already on an error path and must not mask it with a second failure.
    """
    with contextlib.suppress(OSError):
        os.remove(path)


def wavg(value_col: str, weight_col: str) -> str:
    """SQL for the weight-averaged ``value_col`` across rows (hours/files).

    Percentile rollups cannot be merged exactly (DuckDB has no sketch
    combine), so both the day compactors and the readers carry a row count
    alongside each percentile and request-weight-average it.
    """
    return f"SUM({value_col} * {weight_col}) / NULLIF(SUM({weight_col}), 0)"


def parquet_zstd_target(path: str) -> str:
    """The ``TO '<path>' (FORMAT PARQUET, COMPRESSION ZSTD)`` tail of a rollup COPY."""
    return f"TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)"


def quote_path_list(paths: Iterable[str]) -> str:
    """Render ``paths`` as a comma-separated list of single-quoted,
    SQL-escaped string literals for a DuckDB ``read_parquet([...])`` /
    ``COPY`` source list. Embedded single quotes are doubled per SQL.

    Equivalent to the ``", ".join("'" + escape_sql_literal(p) + "'" ...)``
    idiom previously inlined across the hour/day bundle writers.
    """
    return ", ".join("'" + escape_sql_literal(p) + "'" for p in paths)


def parse_hour_token(h: str) -> datetime | None:
    """Parse a rollup hour partition token (``"YYYY-MM-DD-HH"``) to a
    tz-aware UTC datetime, or ``None`` if the string doesn't match."""
    return parse_hour_key(h)


# ── Per-service in-process rollup readiness flag ─────────────────────────────

_ready_lock = threading.Lock()
_ready: set[str] = set()


def rollup_coverage_ready(service_id: str) -> bool:
    with _ready_lock:
        return service_id in _ready


def mark_rollup_coverage_ready(service_id: str) -> None:
    with _ready_lock:
        _ready.add(service_id)


def reset_rollup_coverage_ready(service_id: str | None = None) -> None:
    """Test-only: clear one service's flag, or every service's when omitted."""
    with _ready_lock:
        if service_id is None:
            _ready.clear()
        else:
            _ready.discard(service_id)


from backend.utils.module_shim import install_shim_module  # noqa: E402

from . import bundle_builder as _bundle_builder  # noqa: E402
from .bundle_builder import (  # noqa: E402, F401
    _SHARED_ROLLUP_SESSION,
    MultiBundleSpec,
    _bucket_min_copy_sql,
    _closed_day_window,
    _publish_field_partitions,
    _SharedRollupSession,
    _sum_count_copy_sql,
    acquire_rollup_connection_and_cols,
    backfill_missing_bundles,
    backfill_multi_spec_bundles,
    build_multi_spec_bundles,
    build_per_hour_bundles,
    cleanup_per_field_after_bundle,
    collect_per_field_partition_parquets,
    compact_closed_days,
    describe_columns,
    publish_temp_parquet,
    shared_rollup_session,
)

install_shim_module(__name__, (_bundle_builder,), bidirectional_seed=True)
