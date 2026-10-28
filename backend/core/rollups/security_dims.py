"""Per-hour security-dimension rollups feeding /api/security/aggregates'
``req_size`` + ``conn_reuse`` + ``topips`` + ``coverage`` panels.

All four panels are all-rows live scans of the filtered TEMP table today
(``REQ_HEADER_SIZE_DIST`` / ``CONN_REUSE_DIST`` / ``TOP_IPS_BY_MAX_HEADER`` /
``FINGERPRINT_COVERAGE_BULK`` in ``_sql/security.py``). Pre-aggregating each
closed hour to its bucket counts (or top-K IPs, or one coverage row) removes
the all-rows scan from the catalog temp, so the temp can shrink to nothing.

Unlike the percentile rollups (slow_urls / origin_dims) the math here is EXACT
across hours — counts SUM, ``min_val`` is MIN-of-MIN, and ``max_header`` is
MAX-of-MAX. The readers carry NO ``_approx`` flag.

One module emits FOUR files per closed hour (mirrors how :mod:`.origin_dims`
emits origin_pop + origin_ip + origin_path from one module — they differ only
in the per-hour cut + eligibility gate):

  ``rollups/hour_bundled/hour=H/security_req_size.parquet``   (req_header_bytes)
  ``rollups/hour_bundled/hour=H/security_conn_reuse.parquet`` (conn_requests)
  ``rollups/hour_bundled/hour=H/security_topips.parquet``     (ip, top-500)
  ``rollups/hour_bundled/hour=H/security_cov.parquet``        (1 row)

Schemas:

security_req_size.parquet (REQ_HEADER_SIZE_DIST, ``_sql/security.py``):
  bucket   VARCHAR  -- the REQ_HEADER_SIZE_DIST CASE label (byte-for-byte)
  count    BIGINT   -- COUNT(*) for this bucket in this hour
  min_val  BIGINT   -- MIN(req_header_bytes) for cross-hour bucket ordering

security_conn_reuse.parquet (CONN_REUSE_DIST, ``_sql/security.py``):
  bucket   VARCHAR  -- the CONN_REUSE_DIST CASE label (byte-for-byte)
  count    BIGINT
  min_val  BIGINT   -- MIN(conn_requests)

security_topips.parquet (TOP_IPS_BY_MAX_HEADER, ``_sql/security.py``):
  ip          VARCHAR
  max_header  BIGINT  -- MAX(req_header_bytes) for this ip in this hour
top-K=500 per hour by max_header DESC; the reader re-caps to the panel's
top-10 by MAX-of-MAX across hours (NOT SUM — see compact_*_closed_days_to_daily).

security_cov.parquet (FINGERPRINT_COVERAGE_BULK over tls_ciphers_sha):
  total_rows     BIGINT  -- COUNT(*) in this hour
  tls_populated  BIGINT  -- COUNT(*) FILTER (tls_ciphers_sha NOT NULL AND != '')
one row per closed hour; the reader SUMs both across the window.

Each bundle gates on its required column (req_header_bytes / conn_requests /
ip+req_header_bytes / tls_ciphers_sha); a service missing the column skips that
bundle but still writes the others. The window-predicate + ``table_ident``
substitution match origin_dims (writers must NOT import ``repositories``;
import-linter enforces this — the CASE/WHERE bodies are replicated verbatim).

Active-hour skip + atomic tmp+rename + per-service view lock — same
convention as :mod:`.origin_dims`.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

from backend.core.metric_expressions import coalesce, sum_or_zero
from backend.core.sql_validator import escape_sql_literal
from backend.utils.module_shim import install_shim_module
from backend.utils.pop_utils import build_pop_haversine_case_sql

from ._common import (
    NGWAF_BOTS_BUNDLE_FILENAME,
    NGWAF_BOTS_TS_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_UA_TOP_K,
    SECURITY_CONN_REUSE_BUNDLE_FILENAME,
    SECURITY_COV_BUNDLE_FILENAME,
    SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
    SECURITY_PROXY_QUALITY_BUNDLE_FILENAME,
    SECURITY_REQ_SIZE_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_TOP_K,
    VERIFIED_BOTS_TS_BUNDLE_FILENAME,
    MultiBundleSpec,
    _day_bundled_root,
    _hour_bundled_root,
    backfill_missing_bundles,
    backfill_multi_spec_bundles,
    build_multi_spec_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
    unlink_quiet,
)

logger = logging.getLogger(__name__)


def _build_req_size_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    # REQ_HEADER_SIZE_DIST: histogram of req_header_bytes over the fixed size
    # buckets. CASE replicated byte-for-byte from _sql/security.py so the
    # reader's SUM(count) per bucket equals the live scan's count per bucket.
    return (
        f"COPY ("
        f"  SELECT "
        f"    CASE "
        f"        WHEN req_header_bytes <= 256 THEN '0-256B' "
        f"        WHEN req_header_bytes <= 512 THEN '256-512B' "
        f"        WHEN req_header_bytes <= 768 THEN '512-768B' "
        f"        WHEN req_header_bytes <= 1024 THEN '768B-1KB' "
        f"        WHEN req_header_bytes <= 1536 THEN '1-1.5KB' "
        f"        WHEN req_header_bytes <= 2048 THEN '1.5-2KB' "
        f"        WHEN req_header_bytes <= 3072 THEN '2-3KB' "
        f"        WHEN req_header_bytes <= 4096 THEN '3-4KB' "
        f"        WHEN req_header_bytes <= 6144 THEN '4-6KB' "
        f"        WHEN req_header_bytes <= 8192 THEN '6-8KB' "
        f"        WHEN req_header_bytes <= 12288 THEN '8-12KB' "
        f"        WHEN req_header_bytes <= 16384 THEN '12-16KB' "
        f"        WHEN req_header_bytes <= 24576 THEN '16-24KB' "
        f"        WHEN req_header_bytes <= 32768 THEN '24-32KB' "
        f"        ELSE '>32KB' "
        f"    END AS bucket, "
        f"    CAST(COUNT(*) AS BIGINT) AS count, "
        f"    CAST(MIN(req_header_bytes) AS BIGINT) AS min_val "
        f"  FROM {table_ident} "
        f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
        f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
        f"    AND req_header_bytes IS NOT NULL "
        f"  GROUP BY 1"
        f") {parquet_zstd_target(tmp_path)}"
    )


def _build_conn_reuse_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    # CONN_REUSE_DIST: per-connection request-count distribution. CASE + the
    # ``conn_requests > 0`` floor replicated byte-for-byte from _sql/security.py.
    return (
        f"COPY ("
        f"  SELECT "
        f"    CASE "
        f"        WHEN conn_requests = 1 THEN '1 (None)' "
        f"        WHEN conn_requests <= 5 THEN '2-5' "
        f"        WHEN conn_requests <= 20 THEN '6-20' "
        f"        WHEN conn_requests <= 100 THEN '21-100' "
        f"        ELSE '>100' "
        f"    END AS bucket, "
        f"    CAST(COUNT(*) AS BIGINT) AS count, "
        f"    CAST(MIN(conn_requests) AS BIGINT) AS min_val "
        f"  FROM {table_ident} "
        f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
        f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
        f"    AND conn_requests IS NOT NULL AND conn_requests > 0 "
        f"  GROUP BY 1"
        f") {parquet_zstd_target(tmp_path)}"
    )


def _build_topips_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    # TOP_IPS_BY_MAX_HEADER: per-ip MAX(req_header_bytes). Live LIMIT is 10;
    # the rollup keeps top-500/hour (SECURITY_TOPIPS_BUNDLE_TOP_K) so the
    # cross-hour MAX-of-MAX re-rank in the reader can't drop an ip whose
    # window-max lands outside any single hour's top-10.
    return (
        f"COPY ("
        f"  SELECT ip, CAST(MAX(req_header_bytes) AS BIGINT) AS max_header "
        f"  FROM {table_ident} "
        f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
        f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
        f"    AND ip IS NOT NULL AND req_header_bytes IS NOT NULL "
        f"  GROUP BY 1 "
        f"  ORDER BY 2 DESC "
        f"  LIMIT {SECURITY_TOPIPS_BUNDLE_TOP_K}"
        f") {parquet_zstd_target(tmp_path)}"
    )


def _build_cov_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    # FINGERPRINT_COVERAGE_BULK over the tls_ciphers_sha safelist: total rows +
    # populated count (NOT NULL AND != '' — the empty-string filter is load-
    # bearing per the FINGERPRINT_TOP_N docstring). One row per closed hour.
    return (
        f"COPY ("
        f"  SELECT "
        f"    CAST(COUNT(*) AS BIGINT) AS total_rows, "
        f"    CAST(COUNT(*) FILTER (WHERE tls_ciphers_sha IS NOT NULL AND tls_ciphers_sha != '') AS BIGINT) "
        f"      AS tls_populated "
        f"  FROM {table_ident} "
        f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
        f"    AND timestamp <  TIMESTAMPTZ '{end_iso}'"
        f") {parquet_zstd_target(tmp_path)}"
    )


# (label, output filename, required column(s), build_copy_sql). One COPY each.
# The required column gates which bundle a service can produce — e.g. a service
# with no ``conn_requests`` skips conn_reuse but still writes req_size etc.
_SECURITY_DIMS: tuple[MultiBundleSpec, ...] = (
    ("req_size", SECURITY_REQ_SIZE_BUNDLE_FILENAME, ("req_header_bytes",), _build_req_size_copy_sql),
    ("conn_reuse", SECURITY_CONN_REUSE_BUNDLE_FILENAME, ("conn_requests",), _build_conn_reuse_copy_sql),
    ("topips", SECURITY_TOPIPS_BUNDLE_FILENAME, ("ip", "req_header_bytes"), _build_topips_copy_sql),
    ("cov", SECURITY_COV_BUNDLE_FILENAME, ("tls_ciphers_sha",), _build_cov_copy_sql),
)


def build_security_dims_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write the security_req_size + security_conn_reuse + security_topips +
    security_cov rollups for each closed hour in ``hours``."""
    return build_multi_spec_bundles(
        service_id,
        source,
        hours,
        specs=_SECURITY_DIMS,
        group_label="security_dims",
        tmp_prefix=".tmp_sd_",
        logger=logger,
        driver=build_per_hour_bundles,
    )


def backfill_security_dims_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build the security_dims rollups for every closed hour
    that has ``all_fields.parquet`` but is missing a security_dims file."""
    return backfill_multi_spec_bundles(
        service_id,
        source,
        specs=_SECURITY_DIMS,
        group_label="security_dims",
        tmp_prefix=".tmp_sd_",
        logger=logger,
        driver=build_per_hour_bundles,
    )


def _window(start_iso: str, end_iso: str) -> str:
    return f"timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}'"


def _build_quality_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    return (
        f"COPY ("
        f"  SELECT "
        f"    CASE "
        f"      WHEN rtt_min IS NULL OR tcp_rtt IS NULL THEN 'Direct Connection' "
        f"      WHEN tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.1 THEN 'Active Tunnel / Proxy' "
        f"      WHEN tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.7 THEN 'WiFi / Mobile' "
        f"      ELSE 'Direct Connection' "
        f"    END AS type, "
        f"    CAST(COUNT(*) AS BIGINT) AS count "
        f"  FROM {table_ident} "
        f"  WHERE {_window(start_iso, end_iso)} "
        f"  GROUP BY 1"
        f") {parquet_zstd_target(tmp_path)}"
    )


def _build_flagged_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    cols = ctx if isinstance(ctx, (set, frozenset)) else set()
    country = "country" if "country" in cols else "CAST(NULL AS VARCHAR)"
    city = "city" if "city" in cols else "CAST(NULL AS VARCHAR)"
    window = _window(start_iso, end_iso)
    return (
        f"COPY ("
        f"  WITH geo AS ("
        f"    SELECT pop AS g_pop, lat AS g_lat, lon AS g_lon, {build_pop_haversine_case_sql()} AS distance_km "
        f"    FROM (SELECT DISTINCT pop, lat, lon FROM {table_ident} "
        f"          WHERE {window} AND lat IS NOT NULL AND lon IS NOT NULL)"
        f"  ), "
        f"  m AS ("
        f"    SELECT ip, asn, pop, rtt_min, tcp_rtt, lat, lon, {country} AS country, {city} AS city, "
        f"           rtt_min / 1000.0 AS rtt_min_ms, geo.distance_km AS distance_km "
        f"    FROM {table_ident} "
        f"    LEFT JOIN geo ON pop IS NOT DISTINCT FROM geo.g_pop "
        f"                 AND lat IS NOT DISTINCT FROM geo.g_lat "
        f"                 AND lon IS NOT DISTINCT FROM geo.g_lon "
        f"    WHERE {window} AND rtt_min IS NOT NULL"
        f"  ) "
        f"  SELECT ip, CAST(asn AS BIGINT) AS asn, pop, CAST(rtt_min AS DOUBLE) AS rtt_min, "
        f"         CAST(tcp_rtt AS DOUBLE) AS tcp_rtt, CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon, "
        f"         country, city, CAST(COUNT(*) AS BIGINT) AS cnt "
        f"  FROM m "
        f"  WHERE (distance_km > (rtt_min_ms * 100.0 + 150.0)) "
        f"     OR (tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.2) "
        f"  GROUP BY ALL"
        f") {parquet_zstd_target(tmp_path)}"
    )


def _build_bot_eff_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    cols = ctx if isinstance(ctx, (set, frozenset)) else set()
    resp_bytes = coalesce("t.resp_bytes", "0") if "resp_bytes" in cols else "0"
    referer = "t.referer" if "referer" in cols else "CAST(NULL AS VARCHAR)"
    select_referer = "referer" if "referer" in cols else "CAST(NULL AS VARCHAR) AS referer"
    select_bytes = "resp_bytes" if "resp_bytes" in cols else "CAST(0 AS BIGINT) AS resp_bytes"
    sum_resp_bytes = sum_or_zero(resp_bytes)
    empty = "''"
    ua_low = f"lower({coalesce('t.ua', empty)})"
    ref_low = f"lower({coalesce(referer, empty)})"
    ref_stem = coalesce(
        f"NULLIF(regexp_extract({ua_low}, '([a-z0-9]+)-(user|externalfetcher)', 1), '')",
        f"NULLIF(regexp_extract({ref_low}, 'http[s]*://(www\\.)*([^/.:;\\s]+)', 2), '')",
    )
    return (
        f"COPY ("
        f"  WITH t AS ("
        f"    SELECT ua, ip, {select_referer}, {select_bytes} FROM {table_ident} "
        f"    WHERE {_window(start_iso, end_iso)}"
        f"  ), "
        f"  site_totals AS ("
        f"    SELECT 'site' AS kind, '' AS key, COUNT(*) AS cnt, "
        f"           CAST({sum_resp_bytes} AS BIGINT) AS resp_bytes FROM t"
        f"  ), "
        f"  ua_agg AS ("
        f"    SELECT 'ua' AS kind, t.ua AS key, COUNT(*) AS cnt, "
        f"           CAST({sum_resp_bytes} AS BIGINT) AS resp_bytes "
        f"    FROM t WHERE t.ua IS NOT NULL AND t.ua != '' GROUP BY t.ua "
        f"    ORDER BY resp_bytes DESC, cnt DESC LIMIT {SECURITY_BOT_EFF_UA_TOP_K}"
        f"  ), "
        f"  ref_extracted AS ("
        f"    SELECT t.ip, {ref_stem} AS ref_stem "
        f"    FROM t WHERE t.ip IS NOT NULL AND t.ip != '' "
        f"      AND (({referer} IS NOT NULL AND {referer} != '' AND {referer} != '-') "
        f"           OR {ua_low} LIKE '%-user%' "
        f"           OR {ua_low} LIKE '%-externalfetcher%') "
        f"      AND NOT regexp_matches({ua_low}, "
        f"          '(bot|crawler|spider|slurp|archiver|scraper|externalagent|extended|headless)')"
        f"  ), "
        f"  ref_agg AS ("
        f"    SELECT 'ref' AS kind, ref_stem AS key, COUNT(DISTINCT ip) AS cnt, CAST(0 AS BIGINT) AS resp_bytes "
        f"    FROM ref_extracted WHERE ref_stem IS NOT NULL AND ref_stem != '' GROUP BY ref_stem"
        f"  ) "
        f"  SELECT kind, key, CAST(cnt AS BIGINT) AS cnt, resp_bytes FROM site_totals "
        f"  UNION ALL SELECT kind, key, CAST(cnt AS BIGINT), resp_bytes FROM ua_agg "
        f"  UNION ALL SELECT kind, key, CAST(cnt AS BIGINT), resp_bytes FROM ref_agg"
        f") {parquet_zstd_target(tmp_path)}"
    )


_SECURITY_PROXIES: tuple[MultiBundleSpec, ...] = (
    ("proxy_quality", SECURITY_PROXY_QUALITY_BUNDLE_FILENAME, ("rtt_min", "tcp_rtt"), _build_quality_copy_sql),
    (
        "proxy_flagged",
        SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
        ("ip", "pop", "rtt_min", "tcp_rtt", "lat", "lon", "asn"),
        _build_flagged_copy_sql,
    ),
    ("bot_eff", SECURITY_BOT_EFF_BUNDLE_FILENAME, ("ua", "ip"), _build_bot_eff_copy_sql),
)


def _columns_ctx(cols: set[str], table_ident: str) -> frozenset[str]:
    return frozenset(cols)


def build_security_proxies_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write the security_proxy_quality + security_proxy_flagged + security_bot_eff
    rollups for each closed hour in ``hours``."""
    return build_multi_spec_bundles(
        service_id,
        source,
        hours,
        specs=_SECURITY_PROXIES,
        group_label="security_proxies",
        tmp_prefix=".tmp_sp_",
        logger=logger,
        context_fn=_columns_ctx,
        driver=build_per_hour_bundles,
    )


def backfill_security_proxies_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build the security_proxies rollups for every closed hour
    that has ``all_fields.parquet`` but is missing one of the three files."""
    return backfill_multi_spec_bundles(
        service_id,
        source,
        specs=_SECURITY_PROXIES,
        group_label="security_proxies",
        tmp_prefix=".tmp_sp_",
        logger=logger,
        context_fn=_columns_ctx,
        driver=build_per_hour_bundles,
    )


def build_verified_bots_ts_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour verified_bots_ts rollup for each closed hour."""

    def eligibility(cols, table_ident):
        if "waf_sig" not in cols:
            return None
        return True

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
        return (
            f"COPY ("
            f"  SELECT CAST(date_trunc('minute', timestamp) AS TIMESTAMPTZ) AS bucket_ts, "
            f"         replace(tag, 'VERIFIED-BOT.', '') AS bot_type, "
            f"         CAST(COUNT(*) AS BIGINT) AS count "
            f"  FROM ("
            f"    SELECT timestamp, unnest(string_split(waf_sig, ',')) AS tag "
            f"    FROM {table_ident} "
            f"    WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"      AND timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"      AND waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%'"
            f"  ) sub "
            f"  WHERE tag LIKE 'VERIFIED-BOT.%' "
            f"  GROUP BY 1, 2"
            f") {parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=VERIFIED_BOTS_TS_BUNDLE_FILENAME,
        tmp_prefix=".tmp_vbts_",
        label="verified_bots_ts",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_verified_bots_ts_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build verified_bots_ts.parquet for every closed hour
    that has all_fields.parquet but no verified_bots_ts.parquet yet."""
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=VERIFIED_BOTS_TS_BUNDLE_FILENAME,
        label="verified_bots_ts",
        builder=build_verified_bots_ts_bundles,
        logger=logger,
    )


# ---------------------------------------------------------------------------
# Per-hour NGWAF-bots rollups (formerly backend.core.rollups.ngwaf_bots)
# ---------------------------------------------------------------------------

# Empty-but-valid relation: same shape as the VALUES literal below, zero rows.
# Keeps the JOIN a no-op (not a skip) when the cache exists but has no
# bot-tagged rows yet — the caller still writes the load-bearing empty
# parquet that marks a bot-quiet hour as covered.
_EMPTY_NGWAF_VALUES_SQL = (
    "(SELECT NULL::VARCHAR AS waf_req_id, NULL::VARCHAR AS bot_name, "
    "NULL::VARCHAR AS wellknown_bot_name, NULL::VARCHAR AS category WHERE FALSE) AS nb"
)


def _read_ngwaf_bots_as_values_sql(db_path: str | None = None) -> str | None:
    """Fetch the ``ngwaf_bots`` cache table via Postgres (or test SQLite file)
    and render it as a DuckDB ``VALUES`` literal aliased
    ``nb(waf_req_id, bot_name, wellknown_bot_name, category)``.
    """
    rows = []
    if db_path and os.path.exists(db_path):
        import sqlite3

        try:
            con = sqlite3.connect(db_path, timeout=5)
            try:
                col_names = {r[1] for r in con.execute("PRAGMA table_info(ngwaf_bots)").fetchall()}
                wk_expr = "wellknown_bot_name" if "wellknown_bot_name" in col_names else "NULL"
                rows = con.execute(
                    f"SELECT waf_req_id, bot_name, {wk_expr}, category FROM ngwaf_bots WHERE bot_name IS NOT NULL"
                ).fetchall()
            finally:
                con.close()
        except Exception as e:
            logger.warning("[rollups] failed reading ngwaf_bots test cache at %s: %s", db_path, e)
            return None
    else:
        from backend.core.metadata import base as pg_connection

        try:
            pconn = pg_connection.get_pg_readonly_connection()
            try:
                cur = pconn.execute(
                    "SELECT waf_req_id, bot_name, wellknown_bot_name, category "
                    "FROM ngwaf_bots WHERE bot_name IS NOT NULL"
                )
                rows = [
                    (r["waf_req_id"], r["bot_name"], r["wellknown_bot_name"], r["category"]) for r in cur.fetchall()
                ]
            finally:
                pconn.close()
        except Exception as e:
            logger.warning("[rollups] failed reading ngwaf_bots cache from postgres: %s", e)
            return None

    if not rows:
        return _EMPTY_NGWAF_VALUES_SQL

    def esc(v: object) -> str:
        return "NULL" if v is None else "'" + escape_sql_literal(str(v)) + "'"

    values = ", ".join(f"({esc(r[0])}, {esc(r[1])}, {esc(r[2])}, {esc(r[3])})" for r in rows)
    return f"(VALUES {values}) AS nb(waf_req_id, bot_name, wellknown_bot_name, category)"


def _purge_stale_ngwaf_bots_parquets(source: dict) -> int:
    """Unlink pre-wellknown_bot_name ``ngwaf_bots.parquet`` files (Trap #24).

    Earlier versions wrote ``(bot_name, category, count)`` without
    ``wellknown_bot_name``. Both ``backfill_missing_bundles`` and
    ``compact_closed_days`` skip hours/days whose output file already exists,
    so stale 3-column parquets would block rebundling and break the
    4-column reader / day compactor. Inspecting parquet footer schemas is
    sub-ms per file.
    """
    import pyarrow.parquet as pq

    purged = 0
    for root, prefix in ((_hour_bundled_root(source), "hour="), (_day_bundled_root(source), "day=")):
        if not os.path.isdir(root):
            continue
        try:
            entries = sorted(os.listdir(root))
        except OSError:
            continue
        for entry in entries:
            if not entry.startswith(prefix):
                continue
            p = os.path.join(root, entry, NGWAF_BOTS_BUNDLE_FILENAME)
            if not os.path.isfile(p):
                continue
            try:
                names = set(pq.read_schema(p).names)
            except Exception:
                names = set()
            if "wellknown_bot_name" not in names:
                unlink_quiet(p)
                unlink_quiet(f"{p}.done")
                purged += 1
    return purged


def _ngwaf_bots_eligibility(cols: set[str], *, require_timestamp: bool = False) -> dict[str, str] | None:
    if "waf_req_id" not in cols or (require_timestamp and "timestamp" not in cols):
        return None
    from backend import config as svcconfig

    db_path = svcconfig.ngwaf_sqlite_cache_override()
    if db_path and not os.path.exists(db_path):
        return None
    values_sql = _read_ngwaf_bots_as_values_sql(db_path)
    if values_sql is None:
        return None
    return {"ngwaf_values_sql": values_sql}


def build_ngwaf_bots_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour ngwaf_bots rollup for each closed hour."""

    def eligibility(cols, table_ident):
        return _ngwaf_bots_eligibility(cols)

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
        return (
            f"COPY ("
            f"  SELECT CAST(nb.bot_name AS VARCHAR) AS bot_name, "
            f"         CAST(nb.wellknown_bot_name AS VARCHAR) AS wellknown_bot_name, "
            f"         CAST(nb.category AS VARCHAR) AS category, "
            f"         CAST(COUNT(*) AS BIGINT) AS count "
            f"  FROM {table_ident} t "
            f"  INNER JOIN {ctx['ngwaf_values_sql']} "
            f"          ON t.waf_req_id = nb.waf_req_id "
            f"  WHERE t.timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"    AND t.timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"    AND t.waf_req_id IS NOT NULL "
            f"    AND nb.bot_name IS NOT NULL "
            f"  GROUP BY 1, 2, 3"
            f") {parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=NGWAF_BOTS_BUNDLE_FILENAME,
        tmp_prefix=".tmp_nb_",
        label="ngwaf_bots",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_ngwaf_bots_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build ngwaf_bots.parquet for every closed hour that
    has all_fields.parquet (or time_series.parquet) but no 4-column
    ngwaf_bots.parquet yet.
    """
    _purge_stale_ngwaf_bots_parquets(source)
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NGWAF_BOTS_BUNDLE_FILENAME,
        label="ngwaf_bots",
        builder=build_ngwaf_bots_bundles,
        logger=logger,
    )


def build_ngwaf_bots_ts_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour minute-bucketed ngwaf_bots_ts rollup for each closed hour."""

    def eligibility(cols, table_ident):
        return _ngwaf_bots_eligibility(cols, require_timestamp=True)

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
        return (
            f"COPY ("
            f"  SELECT CAST(date_trunc('minute', t.timestamp) AS TIMESTAMPTZ) AS bucket_ts, "
            f"         CAST(nb.bot_name AS VARCHAR) AS bot_name, "
            f"         CAST(COUNT(*) AS BIGINT) AS count "
            f"  FROM {table_ident} t "
            f"  INNER JOIN {ctx['ngwaf_values_sql']} "
            f"          ON t.waf_req_id = nb.waf_req_id "
            f"  WHERE t.timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"    AND t.timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"    AND t.waf_req_id IS NOT NULL "
            f"    AND nb.bot_name IS NOT NULL "
            f"  GROUP BY 1, 2"
            f") {parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=NGWAF_BOTS_TS_BUNDLE_FILENAME,
        tmp_prefix=".tmp_nbts_",
        label="ngwaf_bots_ts",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_ngwaf_bots_ts_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build ngwaf_bots_ts.parquet for every closed hour that
    has all_fields.parquet (or time_series.parquet) but no ngwaf_bots_ts.parquet yet.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NGWAF_BOTS_TS_BUNDLE_FILENAME,
        label="ngwaf_bots_ts",
        builder=build_ngwaf_bots_ts_bundles,
        logger=logger,
    )


def _parse_iso_to_hour(iso: str) -> datetime | None:
    """Parse an ISO-ish timestamp string into a UTC datetime truncated
    to the hour."""
    if not iso:
        return None
    try:
        s = iso.rstrip("Z")
        dt = datetime.fromisoformat(s).replace(tzinfo=UTC)
    except ValueError:
        return None
    return dt.replace(minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Re-exports from security_bots for backwards compatibility
# ---------------------------------------------------------------------------

from . import security_bots  # noqa: E402
from .security_bots import (  # noqa: E402, F401
    _API_PREFIX,
    _extract_bot_record,
    _get_conn,
    _get_readonly_conn,
    _get_relative_time_range,
    _wellknown_bots_root,
    backfill_wellknown_bots_rollup,
    cleanup_old_bots,
    fetch_verified_bots_paged,
    get_cache_stats,
    get_last_timestamp,
    oldest_unenriched_timestamp,
    read_wellknown_bots_rollup,
    recompute_wellknown_bots_rollup,
    update_sync_watermark,
    upsert_bots,
)

install_shim_module(__name__, (security_bots,))
