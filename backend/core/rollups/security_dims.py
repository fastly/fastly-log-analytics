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

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.origin_dims`.
"""

from __future__ import annotations

import logging

from ._common import (
    SECURITY_CONN_REUSE_BUNDLE_FILENAME,
    SECURITY_COV_BUNDLE_FILENAME,
    SECURITY_REQ_SIZE_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_TOP_K,
    MultiBundleSpec,
    backfill_multi_spec_bundles,
    build_multi_spec_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
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
