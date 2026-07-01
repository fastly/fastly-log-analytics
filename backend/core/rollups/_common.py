"""Shared primitives for the rollups package.

Constants, ident validators, path helpers, atomic marker IO, COPY query
builders, and the virtual-field backing map — everything every other
sub-module needs.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

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
IP_SPREAD_DAY_DIR_NAME = "day_ip_spread"

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
_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_ident(name: str) -> bool:
    return bool(name) and bool(_SAFE_IDENT_RE.match(name))


def _safe_table_for(source: dict) -> str | None:
    """Return the DuckDB view name for this service, or ``None`` if no slug.

    Slugifies the same way the dashboard's view-builder does
    (``backend.core.duckdb._safe_table_name``: non-alphanumerics to ``_``,
    lowercased, ``logs_`` prefix) so the rollup COPY/SELECT targets the
    same view name the dashboard creates. Reads ``service_id`` first (the
    canonical slug in normalized source dicts) and falls back to ``name``
    for callers that pass a raw on-disk config — both cases pass through
    the slugifier identically.
    """
    raw = source.get("service_id") or source.get("name") or ""
    if not raw:
        logger.warning("[rollups] no service_id/name in source dict; skipping rollup")
        return None
    from backend.core.duckdb import _safe_table_name

    return _safe_table_name(raw)


def _get_fields(src: dict) -> list[str]:
    """Return the dashboard fields eligible for rollup.

    Custom-field names are validated against ``_SAFE_IDENT_RE`` — anything
    failing the check is skipped with a warning rather than fed into SQL.

    Includes virtual fields (waf_sig_ind, edge_score_reason_ind) — those
    used to be excluded because they require unnesting a CSV column, but
    we now have a dedicated SQL builder (``_build_virtual_field_copy_query``)
    that does the unnest at write time so the dashboard reader doesn't
    have to rescan + unnest the raw window at query time.
    """
    from backend.repositories.dashboard import _VIRTUAL_FIELDS, FIELDS

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
    actual_fields = [f for f in FIELDS if f not in _VIRTUAL_FIELDS and _is_safe_ident(f)]
    virtual_fields = [f for f in _VIRTUAL_FIELDS if f in _VIRTUAL_FIELD_BACKING and _is_safe_ident(f)]
    return actual_fields + virtual_fields + custom_field_names


def _rollups_root(source: dict) -> str:
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "hour")


def _day_rollups_root(source: dict) -> str:
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


def _markers_path(source: dict) -> str:
    """JSON file tracking which fields have been backfilled.

    Replaces the prior single ``.backfill_done`` marker which couldn't
    distinguish "fully backfilled" from "backfilled before a new custom
    field was added". Shape: ``{"field": "ISO timestamp", ...}``.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "backfill_markers.json")


def _load_markers(source: dict) -> dict[str, str]:
    path = _markers_path(source)
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("[rollups] could not read markers at %s: %s", path, e)
        return {}


def _save_markers(source: dict, markers: dict[str, str]) -> None:
    path = _markers_path(source)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Atomic write so a crash mid-write doesn't truncate the file.
    tmp_path = f"{path}.tmp.{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp_path, "w") as f:
            json.dump(markers, f)
        os.replace(tmp_path, path)
    except OSError as e:
        logger.warning("[rollups] could not write markers to %s: %s", path, e)
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def _publish_field_partitions(tmp_field_dir: str, dst_root: str, field: str) -> int:
    """Move per-hour parquet files from a temp PARTITION_BY tree into the
    canonical ``rollups/hour/field=X/hour=Y/`` layout.

    The publish order is RENAME-then-UNLINK to close the race window where
    a concurrent dashboard read could observe an empty hour directory.
    Worst case after this change: a dashboard read briefly sees BOTH the
    new and old parquet for the same hour and double-counts that hour
    until the unlink lands — which is bounded and self-corrects on the
    next refresh. Pre-fix, the dashboard could observe ZERO files for the
    hour (undercount), which was indistinguishable from a real traffic dip.

    Caller MUST hold the per-service iceberg lock around the whole call.
    Returns the number of hour-dirs published.
    """
    field_dir = os.path.join(tmp_field_dir, f"field={field}")
    if not os.path.isdir(field_dir):
        return 0

    published = 0
    for hour_dirname in os.listdir(field_dir):
        if not hour_dirname.startswith("hour="):
            continue
        src_hour_dir = os.path.join(field_dir, hour_dirname)
        dst_hour_dir = os.path.join(dst_root, f"field={field}", hour_dirname)
        os.makedirs(dst_hour_dir, exist_ok=True)

        # 1. Rename new files into place first (overcounting window OK).
        new_names: set[str] = set()
        for fname in os.listdir(src_hour_dir):
            if not fname.endswith(".parquet"):
                continue
            new_name = f"compacted_{uuid.uuid4().hex[:12]}.parquet"
            os.rename(os.path.join(src_hour_dir, fname), os.path.join(dst_hour_dir, new_name))
            new_names.add(new_name)

        # 2. Now unlink any pre-existing files that we didn't just write.
        if new_names:
            for existing in os.listdir(dst_hour_dir):
                if existing.endswith(".parquet") and existing not in new_names:
                    try:
                        os.remove(os.path.join(dst_hour_dir, existing))
                    except OSError as e:
                        logger.warning("[rollups] could not unlink stale %s: %s", existing, e)
            published += 1

    return published


def _ip_spread_root(source: dict) -> str:
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
    publish atomically under the per-service iceberg lock.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", IP_SPREAD_DIR_NAME)


def _ip_spread_day_root(source: dict) -> str:
    """Per-(field, day) IP-spread rollup root.

    Day-compacted counterpart to :func:`_ip_spread_root`. Produced by
    the daily compaction cron the same way the count-side
    ``rollups/day`` tree is — one parquet per (field, closed-day),
    consumed by the reader's closed-day fast path."""
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", IP_SPREAD_DAY_DIR_NAME)


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
            GROUP BY 1, 2, 3
        ) WHERE rn <= {TOP_K}
    """


def _build_copy_query(table_ident: str, field: str, where_sql: str) -> str:
    """Return the COPY ... TO <tmp> PARTITION_BY (field, hour) SQL for one field.

    Inputs must already be validated — this function does NO escaping.
    Callers (recompute_touched_hours / backfill_rollups) gate via
    ``_is_safe_ident`` and ``_safe_table_for``.
    """
    return f"""
        SELECT field, hour, value, count FROM (
            SELECT
                '{field}' AS field,
                strftime(timestamp, '%Y-%m-%d-%H') AS hour,
                CAST("{field}" AS VARCHAR) AS value,
                COUNT(*) AS count,
                ROW_NUMBER() OVER (
                    PARTITION BY strftime(timestamp, '%Y-%m-%d-%H')
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM {table_ident}
            WHERE {where_sql}
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


def _hour_bundled_root(source: dict) -> str:
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


def _day_bundled_root(source: dict) -> str:
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


# Filename for the per-day bundled rollup (same as the per-hour
# bundled). Kept identical so future tooling can treat the two trees
# uniformly when needed.
DAY_BUNDLE_FILENAME = "all_fields.parquet"

# Per-(field, day) row cap inside the bundled-day parquet. The
# dashboard top-N panel renders 10 values; 100 gives generous headroom
# for the global top-10 to be visible in at least one day across a
# 30-day window. Anything beyond rank 100 in a single day is
# aggregated into a single synthetic ``__other__`` row so
# field totals stay correct.
DAY_BUNDLE_TOP_K = 100


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

# Filename for the per-hour performance-dimension TTL-distribution rollup
# feeding /api/performance/aggregates' ``ttl_dist`` histogram panel (an
# all-rows live scan today). One row per closed hour PER ttl bucket, matching
# the live histogram CASE (``ttl <= 0 / <= 10 / ... / > 1y``). Schema:
# (bucket VARCHAR, count BIGINT, min_ttl BIGINT). The math is EXACT across
# hours — ``count`` SUMs and ``min_ttl`` is MIN-of-MIN (composing the live
# query's ``ORDER BY min_ttl``), so the reader carries NO ``_approx`` flag.
# Closed-hours-only reader (NO active-hour merge, matching slow_urls).
PERF_TTL_DIST_BUNDLE_FILENAME = "perf_ttl_dist.parquet"


def _time_series_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", TIME_SERIES_BUNDLE_FILENAME)


def _sessions_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", SESSIONS_BUNDLE_FILENAME)


def _slow_urls_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", SLOW_URLS_BUNDLE_FILENAME)


def _origin_summary_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", ORIGIN_SUMMARY_BUNDLE_FILENAME)


def _verified_bots_ts_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", VERIFIED_BOTS_TS_BUNDLE_FILENAME)


def _perf_top_urls_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", PERF_TOP_URLS_BUNDLE_FILENAME)


def _perf_top_asns_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", PERF_TOP_ASNS_BUNDLE_FILENAME)


def _origin_pop_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", ORIGIN_POP_BUNDLE_FILENAME)


def _origin_ip_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", ORIGIN_IP_BUNDLE_FILENAME)


def _origin_path_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", ORIGIN_PATH_BUNDLE_FILENAME)


def _origin_latency_ts_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", ORIGIN_LATENCY_TS_BUNDLE_FILENAME)


def _security_req_size_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", SECURITY_REQ_SIZE_BUNDLE_FILENAME)


def _security_conn_reuse_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", SECURITY_CONN_REUSE_BUNDLE_FILENAME)


def _security_topips_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", SECURITY_TOPIPS_BUNDLE_FILENAME)


def _security_cov_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", SECURITY_COV_BUNDLE_FILENAME)


def _perf_ttl_dist_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", PERF_TTL_DIST_BUNDLE_FILENAME)


def quote_path_list(paths: Iterable[str]) -> str:
    """Render ``paths`` as a comma-separated list of single-quoted,
    SQL-escaped string literals for a DuckDB ``read_parquet([...])`` /
    ``COPY`` source list. Embedded single quotes are doubled per SQL.

    Equivalent to the ``", ".join("'" + p.replace("'", "''") + "'" ...)``
    idiom previously inlined across the hour/day bundle writers.
    """
    return ", ".join("'" + p.replace("'", "''") + "'" for p in paths)


def parse_hour_token(h: str) -> datetime | None:
    """Parse a rollup hour partition token (``"YYYY-MM-DD-HH"``) to a
    tz-aware UTC datetime, or ``None`` if the string doesn't match."""
    try:
        return datetime.strptime(h, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    except ValueError:
        return None


def discover_closed_hours(source: dict) -> set[str]:
    """Return every ``"YYYY-MM-DD-HH"`` partition that exists under
    ``_rollups_root(source)`` and is strictly before the active hour.

    Skips field directories that don't begin with ``"field="``; tolerates
    missing roots and unreadable sub-directories by treating them as
    empty (the rollups jobs already handle the "no data yet" case).
    """
    hour_root = _rollups_root(source)
    if not os.path.isdir(hour_root):
        return set()

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    all_hours: set[str] = set()
    try:
        for field_entry in os.listdir(hour_root):
            if not field_entry.startswith("field="):
                continue
            field_dir = os.path.join(hour_root, field_entry)
            try:
                for hour_entry in os.listdir(field_dir):
                    if not hour_entry.startswith("hour="):
                        continue
                    hour = hour_entry[len("hour=") :]
                    if hour >= active_hour:
                        continue
                    all_hours.add(hour)
            except OSError:
                continue
    except OSError:
        return set()
    return all_hours


def backfill_missing_bundles(
    service_id: str,
    source: dict,
    *,
    bundle_filename: str,
    label: str,
    builder: Callable[[str, dict, list[str]], int],
    logger: logging.Logger = logger,
) -> int:
    """Self-heal driver shared by the per-feature hour-bundle backfillers.

    Walks every closed hour under the hour-bundled root that already has an
    ``all_fields.parquet`` (the signal the hour was touched by the recompute
    pipeline at least once) but is missing ``bundle_filename``, and hands
    those hours to ``builder`` to (re-)materialize. Skipping hours without
    the per-field rollup avoids writing bundles for ingest gaps.

    Idempotent — ``builder`` itself skips the active hour and atomic-renames
    its output. ``label`` is the feature name used in the info log; pass each
    caller's module ``logger`` so the log record keeps its origin module.
    Returns the number of bundles ``builder`` reports writing.
    """
    bundled_root = _hour_bundled_root(source)
    if not os.path.isdir(bundled_root):
        return 0

    hours_to_build: list[str] = []
    try:
        for entry in sorted(os.listdir(bundled_root)):
            if not entry.startswith("hour="):
                continue
            hour = entry[len("hour=") :]
            if parse_hour_token(hour) is None:
                continue
            if not os.path.isfile(os.path.join(bundled_root, entry, "all_fields.parquet")):
                continue
            if os.path.isfile(os.path.join(bundled_root, entry, bundle_filename)):
                continue
            hours_to_build.append(hour)
    except OSError:
        return 0

    if not hours_to_build:
        return 0

    logger.info(
        "[rollups] %s: backfilling %s for %d closed hour(s)",
        service_id,
        label,
        len(hours_to_build),
    )
    return builder(service_id, source, hours_to_build)


def compact_closed_days(
    service_id: str,
    source: dict,
    *,
    jobs: list[tuple[str, str, Callable[[str, str], str]]],
    logger: logging.Logger = logger,
) -> int:
    """Consolidate closed-day per-hour bundle parquets into per-day files.

    Shared driver for the per-feature day compactors. ``jobs`` is a list of
    ``(bundle_filename, tmp_prefix, build_copy_sql)`` tuples. For each job the
    driver:

      - walks ``hour_bundled/hour=YYYY-MM-DD-HH/<bundle_filename>``, grouping
        files by their ``YYYY-MM-DD`` prefix and skipping the active day (still
        being written);
      - for each closed day, skips when the per-day file is already newer than
        every constituent hour file (mtime gate);
      - otherwise writes ``build_copy_sql(paths_sql, tmp_file)`` to a temp file
        and atomically renames it into place under the per-service iceberg
        lock, cleaning up the temp file on any failure.

    ``build_copy_sql`` receives the SQL-escaped ``read_parquet([...])`` source
    list and the temp target path and returns the full ``COPY (...) TO ...``
    statement — the per-feature aggregation (and any top-K re-cap) lives there,
    byte-for-byte as before. A single in-memory DuckDB connection is shared
    across all jobs and days (matches the 2026-06-06 incident posture: parquet
    inputs only, no per-service catalog state). Returns the total number of
    per-day files rebuilt across all jobs.
    """
    import duckdb

    from backend.core.iceberg.view import _get_service_lock

    hour_root = _hour_bundled_root(source)
    day_root = _day_bundled_root(source)
    if not os.path.isdir(hour_root):
        return 0

    active_day = datetime.now(UTC).strftime("%Y-%m-%d")
    lock_key = source.get("name", "default")

    rebuilt = 0
    con = duckdb.connect(":memory:")
    try:
        for bundle_filename, tmp_prefix, build_copy_sql in jobs:
            hours_by_day: dict[str, list[str]] = {}
            try:
                for hour_entry in os.listdir(hour_root):
                    if not hour_entry.startswith("hour="):
                        continue
                    hour_tok = hour_entry[len("hour=") :]
                    if len(hour_tok) < 13:
                        continue
                    day = hour_tok[:10]
                    if day == active_day:
                        continue
                    p = os.path.join(hour_root, hour_entry, bundle_filename)
                    if os.path.isfile(p):
                        hours_by_day.setdefault(day, []).append(p)
            except OSError:
                continue

            for day in sorted(hours_by_day):
                input_paths = sorted(hours_by_day[day])
                if not input_paths:
                    continue
                day_dir = os.path.join(day_root, f"day={day}")
                day_file = os.path.join(day_dir, bundle_filename)
                try:
                    day_mtime = os.path.getmtime(day_file)
                    max_hour_mtime = max(os.path.getmtime(p) for p in input_paths)
                    if day_mtime >= max_hour_mtime:
                        continue
                except OSError:
                    pass  # day file missing → rebuild

                os.makedirs(day_dir, exist_ok=True)
                tmp_file = os.path.join(day_dir, f"{tmp_prefix}{uuid.uuid4().hex[:12]}.parquet")
                paths_sql = quote_path_list(input_paths)
                copy_sql = build_copy_sql(paths_sql, tmp_file)
                try:
                    con.execute(copy_sql)
                except duckdb.Error as e:
                    logger.warning(
                        "[rollups] %s: %s day-compact COPY failed for day=%s: %s",
                        service_id,
                        bundle_filename,
                        day,
                        e,
                    )
                    try:
                        os.remove(tmp_file)
                    except OSError:
                        pass
                    continue

                with _get_service_lock(lock_key):
                    try:
                        os.replace(tmp_file, day_file)
                        rebuilt += 1
                    except OSError as e:
                        logger.warning(
                            "[rollups] %s: %s day rename to %s failed: %s",
                            service_id,
                            bundle_filename,
                            day_file,
                            e,
                        )
                        try:
                            os.remove(tmp_file)
                        except OSError:
                            pass
    finally:
        con.close()

    return rebuilt


def build_per_hour_bundles(
    service_id: str,
    source: dict,
    hours: list[str],
    *,
    bundle_filename: str,
    tmp_prefix: str,
    label: str,
    eligibility: Callable[[set[str], str], object | None],
    build_copy_sql: Callable[[object, str, str, str, str], str],
    describe_label: str | None = None,
    logger: logging.Logger = logger,
) -> int:
    """Shared per-hour bundle writer — the writer-side mirror of
    :func:`compact_closed_days`.

    Owns the scaffold every per-feature ``build_*_bundles`` writer repeats:
    drop the active hour + malformed tokens, resolve the service view, open a
    read-only connection, describe its columns, then for each closed hour
    write a temp parquet via ``build_copy_sql`` and atomically rename it into
    ``hour=H/<bundle_filename>`` under the per-service iceberg lock (cleaning
    up the temp file on any failure).

    Per-feature logic lives in two callbacks:

      - ``eligibility(cols, table_ident)`` returns ``None`` to skip this
        service entirely (a required column is absent) — emitting any
        feature-specific skip log itself — or an opaque context object that is
        passed straight through to ``build_copy_sql``.
      - ``build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path)``
        returns the full ``COPY (...) TO '<tmp_path>' (...)`` statement,
        byte-for-byte as the inline writers built it.

    ``label`` names the feature in the COPY/publish warnings; ``describe_label``
    overrides it for the ``describe_columns`` log label only (defaults to
    ``label``) so a writer that drives this once per dimension can keep a
    single un-suffixed describe label. Pass each caller's module ``logger`` so
    log records keep their origin module. Returns the number of bundles
    written this call.
    """
    if not hours:
        return 0

    import duckdb

    from backend.core.duckdb import get_connection
    from backend.core.iceberg.view import _get_service_lock

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    target_hours: list[str] = []
    for h in hours:
        if h == active_hour:
            continue
        if parse_hour_token(h) is None:
            logger.warning("[rollups] skipping malformed hour token: %r", h)
            continue
        target_hours.append(h)
    if not target_hours:
        return 0

    table_ident = _safe_table_for(source)
    if not table_ident:
        return 0

    bundled_root = _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")

    con = get_connection(source=source, read_only=True)
    try:
        cols = describe_columns(
            con,
            source,
            table_ident,
            logger=logger,
            log_label=f"cannot describe {describe_label or label} bundle",
        )
        if cols is None:
            return 0

        ctx = eligibility(cols, table_ident)
        if ctx is None:
            return 0

        rebuilt = 0
        for hour in target_hours:
            hour_dt = datetime.strptime(hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
            start_iso = hour_dt.isoformat()
            end_iso = (hour_dt + timedelta(hours=1)).isoformat()

            bundle_dir = os.path.join(bundled_root, f"hour={hour}")
            os.makedirs(bundle_dir, exist_ok=True)
            bundle_path = os.path.join(bundle_dir, bundle_filename)

            tmp_path = os.path.join(bundle_dir, f"{tmp_prefix}{uuid.uuid4().hex[:12]}.parquet")
            query = build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path)
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning(
                    "[rollups] %s: %s COPY failed for hour=%s: %s",
                    service_id,
                    label,
                    hour,
                    e,
                )
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                continue

            try:
                with _get_service_lock(lock_key):
                    os.replace(tmp_path, bundle_path)
                rebuilt += 1
            except OSError as e:
                logger.warning(
                    "[rollups] %s: could not publish %s for hour=%s: %s",
                    service_id,
                    label,
                    hour,
                    e,
                )
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        return rebuilt
    finally:
        con.close()


def describe_columns(
    con: duckdb.DuckDBPyConnection,
    source: dict,
    table_ident: str,
    *,
    logger: logging.Logger | None = None,
    log_label: str = "",
) -> set[str] | None:
    """Run ``DESCRIBE <table_ident>`` against ``con`` with the standard
    stale-view-retry hop, returning the set of column names. Returns
    ``None`` and (optionally) warns through ``logger`` if DuckDB raises —
    callers treat that as "view not ready, skip this round".
    """
    from backend.core.iceberg import execute_with_stale_view_retry

    try:
        rows = execute_with_stale_view_retry(
            con,
            source,
            lambda c: c.execute(f"DESCRIBE {table_ident}").fetchall(),
        )
    except Exception as e:  # noqa: BLE001 — DuckDB raises typed errors but iceberg may wrap them
        if logger is not None:
            service_id = source.get("name", "default")
            label = f"{log_label}: " if log_label else ""
            logger.warning("[rollups] %s: %s%s: %s", service_id, label, table_ident, e)
        return None
    return {row[0] for row in rows}
