"""Per-hour NGWAF-bots rollups feeding ``get_top_bots``' ``ngwaf_bots`` panel
and ``/api/security/aggregates``' ``ngwaf_verified_bots`` /
``ngwaf_verified_bots_ts`` panels.

The live panels join the window's raw ``waf_req_id``s against the
``ngwaf_bots`` cache per request. These writers do that join ONCE per closed
hour:

- ``ngwaf_bots.parquet`` stores aggregated
  ``(bot_name, wellknown_bot_name, category, count)`` rows so both
  ``get_top_bots`` and ``ngwaf_verified_bots`` SUM tiny parquets instead.
- ``ngwaf_bots_ts.parquet`` stores minute-bucketed
  ``(bucket_ts, bot_name, count)`` rows so ``ngwaf_verified_bots_ts`` can
  re-bucket to any caller-requested ``bucket_seconds`` without building the
  window's catalog temp table.

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

The join reads the cache by fetching it via PostgreSQL (or plain ``sqlite3``
when a test override is active via ``svcconfig.ngwaf_sqlite_cache_override()``)
and inlining the rows as a DuckDB ``VALUES`` literal (see
``_read_ngwaf_bots_as_values_sql``) rather than letting DuckDB's
``sqlite_scan`` open a live file directly.

Outputs:
  ``rollups/hour_bundled/hour=H/ngwaf_bots.parquet``:
    bot_name            VARCHAR
    wellknown_bot_name  VARCHAR
    category            VARCHAR
    count               BIGINT

  ``rollups/hour_bundled/hour=H/ngwaf_bots_ts.parquet``:
    bucket_ts           TIMESTAMPTZ
    bot_name            VARCHAR
    count               BIGINT

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.network_speed`. A zero-bot closed hour writes an EMPTY
parquet, which is load-bearing: it marks the hour as covered-and-empty so
the reader's coverage floor doesn't force a live fallback forever on
bot-quiet services.
"""

from __future__ import annotations

import logging
import os

from backend.utils.sql_validator import escape_sql_literal

from ._common import (
    NGWAF_BOTS_BUNDLE_FILENAME,
    NGWAF_BOTS_TS_BUNDLE_FILENAME,
    _day_bundled_root,
    _hour_bundled_root,
    backfill_missing_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
    unlink_quiet,
)

logger = logging.getLogger(__name__)

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
        from backend.core.metadata import pg_connection

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


def build_ngwaf_bots_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour ngwaf_bots rollup for each closed hour.

    Skips:
      - The active UTC hour (still being written)
      - Services whose schema lacks ``waf_req_id``

    Idempotent — atomic tmp+rename under the per-service iceberg lock.
    Returns the number of bundles written this call.
    """

    def eligibility(cols, table_ident):
        if "waf_req_id" not in cols:
            return None
        from backend import config as svcconfig

        db_path = svcconfig.ngwaf_sqlite_cache_override()
        if db_path and not os.path.exists(db_path):
            return None
        values_sql = _read_ngwaf_bots_as_values_sql(db_path)
        if values_sql is None:
            return None
        return {"ngwaf_values_sql": values_sql}

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

    Purges pre-wellknown_bot_name 3-column hour/day files first (Trap #24)
    so closed hours and days are rebuilt with the current schema.
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
    """Write a per-hour minute-bucketed ngwaf_bots_ts rollup for each closed hour.

    Stored at 1-minute resolution (``date_trunc('minute', t.timestamp)``) so
    the reader can re-bucket to any caller-requested ``bucket_seconds`` >= 60s.
    """

    def eligibility(cols, table_ident):
        if "waf_req_id" not in cols or "timestamp" not in cols:
            return None
        from backend import config as svcconfig

        db_path = svcconfig.ngwaf_sqlite_cache_override()
        if db_path and not os.path.exists(db_path):
            return None
        values_sql = _read_ngwaf_bots_as_values_sql(db_path)
        if values_sql is None:
            return None
        return {"ngwaf_values_sql": values_sql}

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
