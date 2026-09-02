"""One-time adoption of legacy pyiceberg-era parquet into DuckLake.

``adopt_iceberg_to_ducklake`` registers the local parquet files the old
pyiceberg pipeline left behind (the ``cache/{bucket}/data/`` hive mirror,
or ``cache/{bucket}/iceberg/data/`` for local-warehouse sources) into the
per-service DuckLake table, so pre-v3 history stays queryable through the
``lake``-backed logs view.

Idempotent: ``ducklake_add_data_files`` DUPLICATES rows when a path is
re-added (verified against ducklake d318a545), so files already present in
``ducklake_list_files`` are skipped. Validation compares the lake row-count
delta against the adopted files' own row counts and raises on mismatch.
"""

from __future__ import annotations

import glob
import logging
import os

from backend.utils.sql_validator import escape_sql_literal

logger = logging.getLogger(__name__)

# Adopt in bounded batches so one bad file fails a small batch, not the run.
_ADOPT_BATCH_SIZE = 200


def _legacy_data_dirs(src: dict, cache_dir: str) -> list[str]:
    """Candidate directories holding the old pyiceberg-era parquet."""
    dirs = [os.path.join(cache_dir, "data")]
    # Local-only warehouses committed under cache/iceberg/ (see _warehouse_uri).
    dirs.append(os.path.join(cache_dir, "iceberg", "data"))
    return [d for d in dirs if os.path.isdir(d)]


def adopt_iceberg_to_ducklake(service_id: str) -> dict:
    """Register the legacy local parquet files into the per-service DuckLake table.

    Returns a summary dict: ``adopted_files``, ``skipped_files``,
    ``rows_adopted``. Raises ``ValueError`` on an unknown service or a
    row-count validation mismatch.
    """
    from backend.core.duckdb import _cache_dir, get_connection, get_source_for_service
    from backend.core.iceberg._ducklake import _ducklake_add_data_files, _ducklake_attach, ducklake_table_name

    src = get_source_for_service(service_id)
    if src is None:
        raise ValueError(f"unknown service: {service_id}")

    cache_dir = _cache_dir(src)
    data_files: list[str] = []
    for d in _legacy_data_dirs(src, cache_dir):
        data_files.extend(glob.glob(os.path.join(d, "**", "*.parquet"), recursive=True))
    data_files = sorted(set(os.path.abspath(p) for p in data_files if os.path.isfile(p)))
    if not data_files:
        logger.info("[ducklake] %s: no legacy parquet files to adopt", service_id)
        return {"adopted_files": 0, "skipped_files": 0, "rows_adopted": 0}

    con = get_connection(src)
    try:
        # Re-attach read-write (get_connection attaches read-only for the pool).
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        if not _ducklake_attach(con, src, read_only=False):
            raise RuntimeError(f"failed to attach DuckLake read-write for {service_id}")

        table = ducklake_table_name(src)
        table_ident = 'lake."{}"'.format(table.replace('"', '""'))

        # Create the per-service table from the FIRST parquet's schema
        # (LIMIT 0 — schema only, no rows).
        first_lit = escape_sql_literal(data_files[0])
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_ident} AS SELECT * FROM read_parquet('{first_lit}') LIMIT 0")

        # Skip files DuckLake already tracks — re-adding duplicates rows.
        already = {
            os.path.abspath(r[0])
            for r in con.execute(
                f"SELECT data_file FROM ducklake_list_files('lake', '{escape_sql_literal(table)}')"
            ).fetchall()
        }
        to_adopt = [p for p in data_files if p not in already]
        skipped = len(data_files) - len(to_adopt)
        if not to_adopt:
            logger.info("[ducklake] %s: all %d legacy files already adopted", service_id, len(data_files))
            return {"adopted_files": 0, "skipped_files": skipped, "rows_adopted": 0}

        pre_row = con.execute(f"SELECT count(*) FROM {table_ident}").fetchone()
        pre_count = int(pre_row[0]) if pre_row else 0

        adopted = 0
        expected_rows = 0
        for i in range(0, len(to_adopt), _ADOPT_BATCH_SIZE):
            batch = to_adopt[i : i + _ADOPT_BATCH_SIZE]
            paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in batch)
            batch_rows_res = con.execute(
                f"SELECT count(*) FROM read_parquet([{paths_sql}], union_by_name=true)"
            ).fetchone()
            batch_rows = int(batch_rows_res[0]) if batch_rows_res else 0
            _ducklake_add_data_files(con, batch, alias="lake", table=table)
            adopted += len(batch)
            expected_rows += batch_rows

        post_row = con.execute(f"SELECT count(*) FROM {table_ident}").fetchone()
        post_count = int(post_row[0]) if post_row else 0

        delta = post_count - pre_count
        if delta != expected_rows:
            raise ValueError(
                f"Migration validation failed for {service_id}: lake count delta {delta} "
                f"!= adopted files' row count {expected_rows} (pre={pre_count}, post={post_count})"
            )

        logger.info(
            "[ducklake] %s: adopted %d legacy parquet files (%d rows, %d already present)",
            service_id,
            adopted,
            expected_rows,
            skipped,
        )
        return {"adopted_files": adopted, "skipped_files": skipped, "rows_adopted": expected_rows}
    finally:
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        _ducklake_attach(con, src, read_only=True)
        con.close()
