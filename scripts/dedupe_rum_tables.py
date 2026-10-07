#!/usr/bin/env python3
"""Deduplicate client_vitals and client_errors rows in DuckLake.

De-duplicates RUM beacon rows by their natural key:
  client_vitals: (timestamp, metric_name, pathname, cid, req_id)
  client_errors: (timestamp, error_message, error_file, error_line, pathname, cid, req_id)

Usage:
  # Dry-run (report counts without modifying data):
  uv run python scripts/dedupe_rum_tables.py --service-id <SERVICE_ID> --dry-run

  # Apply de-duplication:
  uv run python scripts/dedupe_rum_tables.py --service-id <SERVICE_ID> --apply
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend import config as svcconfig
from backend.core.iceberg import clear_source_caches
from backend.core.iceberg._ducklake import ducklake_table_name
from backend.core.iceberg.buffer import _ducklake_write_connection, _lake_columns

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dedupe_rum")

NATURAL_KEYS = {
    "client_vitals": ("timestamp", "metric_name", "pathname", "cid", "req_id"),
    "client_errors": ("timestamp", "error_message", "error_file", "error_line", "pathname", "cid", "req_id"),
}


@contextmanager
def _default_write_connection(src: dict):
    with _ducklake_write_connection(src) as con:
        yield con


def dedupe_service_rum_tables(
    src: dict,
    dry_run: bool = True,
    con_factory: Callable[[dict], Any] | None = None,
) -> dict[str, dict[str, int]]:
    """Inspect and optionally de-duplicate client_vitals and client_errors in DuckLake.

    Returns a report per table:
      {
        "table_name": {
            "total_before": int,
            "unique_rows": int,
            "duplicates": int,
            "deleted": int,
            "total_after": int,
        }
      }
    """
    service_id = src.get("service_id") or src.get("name", "unknown")
    report: dict[str, dict[str, int]] = {}

    @contextmanager
    def _get_con():
        if con_factory is not None:
            c = con_factory(src)
            try:
                yield c
            finally:
                try:
                    c.execute("DETACH lake")
                except Exception:
                    pass
                c.close()
        else:
            with _default_write_connection(src) as c:
                yield c

    with _get_con() as con:
        for table_type, natural_cols in NATURAL_KEYS.items():
            tbl = ducklake_table_name(src, table_name=table_type)
            cols = _lake_columns(con, tbl)
            if not cols:
                continue

            # Verify that all natural key columns exist
            valid_keys = [c for c in natural_cols if c in cols]
            if not valid_keys:
                valid_keys = list(cols)

            group_cols_expr = ", ".join(valid_keys)

            total_before = con.execute(f'SELECT count(*) FROM lake."{tbl}"').fetchone()[0]
            unique_rows = con.execute(
                f'SELECT count(*) FROM (SELECT MIN(rowid) FROM lake."{tbl}" GROUP BY {group_cols_expr})'
            ).fetchone()[0]
            duplicates = max(0, total_before - unique_rows)

            deleted = 0
            total_after = total_before

            if not dry_run and duplicates > 0:
                logger.info(
                    "[%s] %s: Deleting %d duplicate rows (keeping %d unique rows)...",
                    service_id,
                    table_type,
                    duplicates,
                    unique_rows,
                )
                con.execute(
                    f'DELETE FROM lake."{tbl}" WHERE rowid NOT IN ('
                    f'SELECT MIN(rowid) FROM lake."{tbl}" GROUP BY {group_cols_expr}'
                    f")"
                )
                total_after = con.execute(f'SELECT count(*) FROM lake."{tbl}"').fetchone()[0]
                deleted = total_before - total_after
                logger.info(
                    "[%s] %s: Successfully removed %d duplicate rows. Rows remaining: %d",
                    service_id,
                    table_type,
                    deleted,
                    total_after,
                )
            else:
                logger.info(
                    "[%s] %s: total=%d, unique=%d, duplicates=%d (%s)",
                    service_id,
                    table_type,
                    total_before,
                    unique_rows,
                    duplicates,
                    "DRY RUN - no rows deleted" if dry_run else "no duplicates",
                )

            report[table_type] = {
                "total_before": total_before,
                "unique_rows": unique_rows,
                "duplicates": duplicates,
                "deleted": deleted,
                "total_after": total_after,
            }

    if not dry_run and any(r.get("deleted", 0) > 0 for r in report.values()):
        clear_source_caches(src.get("name", "default"))

    return report


def main():
    parser = argparse.ArgumentParser(description="Deduplicate RUM tables in DuckLake")
    parser.add_argument("--service-id", help="Fastly service ID to deduplicate (default: all configured services)")
    parser.add_argument("--apply", action="store_true", help="Apply deletions (default is dry-run)")
    parser.add_argument("--dry-run", action="store_true", default=None, help="Dry run only")

    args = parser.parse_args()
    dry_run = not args.apply if args.dry_run is None else args.dry_run

    services: list[str] = []
    if args.service_id:
        services = [args.service_id]
    else:
        configs_dir = os.path.join(os.path.dirname(__file__), "..", "configs")
        if os.path.exists(configs_dir):
            services = [f[:-5] for f in os.listdir(configs_dir) if f.endswith(".json")]

    if not services:
        logger.error("No service IDs found.")
        sys.exit(1)

    logger.info("Starting RUM table deduplication (dry_run=%s) for services: %s", dry_run, services)

    grand_total_before = 0
    grand_total_after = 0
    grand_duplicates = 0
    grand_deleted = 0

    for sid in services:
        cfg = svcconfig.load_config(sid)
        if not cfg:
            continue
        src = svcconfig.config_to_source(cfg)
        try:
            report = dedupe_service_rum_tables(src, dry_run=dry_run)
            for t, data in report.items():
                grand_total_before += data["total_before"]
                grand_total_after += data["total_after"]
                grand_duplicates += data["duplicates"]
                grand_deleted += data["deleted"]
        except Exception as e:
            logger.error("[%s] Error during deduplication: %s", sid, e, exc_info=True)

    mode_label = "DRY RUN" if dry_run else "APPLIED"
    logger.info(
        "Finished RUM deduplication (%s): total_before=%d, duplicates_found=%d, rows_deleted=%d, total_after=%d",
        mode_label,
        grand_total_before,
        grand_duplicates,
        grand_deleted,
        grand_total_after,
    )


if __name__ == "__main__":
    main()
