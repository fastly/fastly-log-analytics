"""Per-hour NGWAF-bots rollup feeding ``get_top_bots``' ``ngwaf_bots`` panel.

The live panel joins the window's raw ``waf_req_id``s against the SQLite
``ngwaf_bot_cache`` per request (``NGWAF_TOP_BOTS_JOIN_DIRECT`` — ~115ms on
prod 24h, 2026-07-07). This writer does that join ONCE per closed hour and
stores the aggregated ``(bot_name, category, count)`` rows, so the reader
SUMs a handful of tiny parquets instead.

Math across hours is EXACT (pure SUM of integer counts) — no ``_approx``
flag. No per-hour top-K cap: bot-name cardinality is tens per hour.

Freshness / retention posture (also documented on
``NGWAF_BOTS_BUNDLE_FILENAME``):

- The bot cache syncs every ~5 min, so an hour rolled up right at close can
  miss attributions for its final minutes. Bounded and one-sided; the panel
  is a top-10 by count over multi-hour windows.
- The cache trims old rows by ``synced_at``. A rollup written while the rows
  were live PRESERVES those counts, so old windows read BETTER from the
  rollup than from a live join against the since-trimmed cache.

The join reads the cache by fetching it via plain ``sqlite3`` and inlining
the rows as a DuckDB ``VALUES`` literal (see ``_read_ngwaf_bots_as_values_sql``)
rather than letting DuckDB's ``sqlite_scan`` open the live file directly.
``sqlite_scan`` doesn't reliably coordinate with SQLite's own WAL/locking
protocol; reading ``ngwaf_bot_cache.db`` through it while
``_run_ngwaf_bot_sync`` concurrently writes the same (globally shared) file
corrupted it in production (2026-07-30) — ``PRAGMA integrity_check`` failed
and every subsequent read/write against the file kept failing until it was
rebuilt from the NGWAF API. No ATTACH either — the shared driver owns the
connection lifecycle and an ATTACH would leak into it.

Output: ``rollups/hour_bundled/hour=H/ngwaf_bots.parquet``. Schema:

  bot_name   VARCHAR
  category   VARCHAR
  count      BIGINT

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.network_speed`. A zero-bot closed hour writes an EMPTY
parquet, which is load-bearing: it marks the hour as covered-and-empty so
the reader's coverage floor doesn't force a live fallback forever on
bot-quiet services.
"""

from __future__ import annotations

import logging
import os

from ._common import (
    NGWAF_BOTS_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)

# Empty-but-valid relation: same shape as the VALUES literal below, zero rows.
# Keeps the JOIN a no-op (not a skip) when the cache exists but has no
# bot-tagged rows yet — the caller still writes the load-bearing empty
# parquet that marks a bot-quiet hour as covered.
_EMPTY_NGWAF_VALUES_SQL = (
    "(SELECT NULL::VARCHAR AS waf_req_id, NULL::VARCHAR AS bot_name, NULL::VARCHAR AS category WHERE FALSE) AS nb"
)


def _read_ngwaf_bots_as_values_sql(db_path: str) -> str | None:
    """Fetch the ``ngwaf_bots`` cache table via plain ``sqlite3`` and render
    it as a DuckDB ``VALUES`` literal aliased ``nb(waf_req_id, bot_name,
    category)``, instead of handing DuckDB the live file path via
    ``sqlite_scan`` (see module docstring for why). The cache is small
    (bot-name cardinality is tens per hour, trimmed by retention), so
    fetching it whole and inlining it is cheap — done once per
    ``build_ngwaf_bots_bundles`` call, reused across every closed hour.

    Returns ``None`` on a read failure (mirrors the prior "file missing"
    skip); returns :data:`_EMPTY_NGWAF_VALUES_SQL` for a valid-but-empty
    cache so callers still write the load-bearing empty-hour parquet.
    """
    import sqlite3

    try:
        con = sqlite3.connect(db_path, timeout=5)
        try:
            rows = con.execute(
                "SELECT waf_req_id, bot_name, category FROM ngwaf_bots WHERE bot_name IS NOT NULL"
            ).fetchall()
        finally:
            con.close()
    except sqlite3.Error as e:
        logger.warning("[rollups] failed reading ngwaf_bots cache at %s: %s", db_path, e)
        return None

    if not rows:
        return _EMPTY_NGWAF_VALUES_SQL

    def esc(v: object) -> str:
        return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"

    values = ", ".join(f"({esc(r[0])}, {esc(r[1])}, {esc(r[2])})" for r in rows)
    return f"(VALUES {values}) AS nb(waf_req_id, bot_name, category)"


def build_ngwaf_bots_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour ngwaf_bots rollup for each closed hour.

    Skips:
      - The active UTC hour (still being written)
      - Services whose schema lacks ``waf_req_id``
      - Services without an ngwaf_bot_cache SQLite file on disk

    Idempotent — atomic tmp+rename under the per-service iceberg lock.
    Returns the number of bundles written this call.
    """

    def eligibility(cols, table_ident):
        if "waf_req_id" not in cols:
            return None
        from backend import config as svcconfig

        db_path = svcconfig.ngwaf_db_path()
        if not db_path or not os.path.exists(db_path):
            # No NGWAF cache on this deployment — the live join would find
            # nothing either; skip quietly.
            return None
        values_sql = _read_ngwaf_bots_as_values_sql(db_path)
        if values_sql is None:
            return None
        return {"ngwaf_values_sql": values_sql}

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
        return (
            f"COPY ("
            f"  SELECT CAST(nb.bot_name AS VARCHAR) AS bot_name, "
            f"         CAST(nb.category AS VARCHAR) AS category, "
            f"         CAST(COUNT(*) AS BIGINT) AS count "
            f"  FROM {table_ident} t "
            f"  INNER JOIN {ctx['ngwaf_values_sql']} "
            f"          ON t.waf_req_id = nb.waf_req_id "
            f"  WHERE t.timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"    AND t.timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"    AND t.waf_req_id IS NOT NULL "
            f"    AND nb.bot_name IS NOT NULL "
            f"  GROUP BY 1, 2"
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
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
    has all_fields.parquet but no ngwaf_bots.parquet yet.

    Mirrors :func:`backend.core.rollups.network_speed.backfill_network_speed_bundles`.
    Idempotent — skips already-built hours. Returns the number written.

    Note for historical hours: waf_req_ids whose cache rows were already
    trimmed by retention aggregate to nothing — identical to what the live
    join would return today, so the rollup is never WORSE than live.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NGWAF_BOTS_BUNDLE_FILENAME,
        label="ngwaf_bots",
        builder=build_ngwaf_bots_bundles,
        logger=logger,
    )
