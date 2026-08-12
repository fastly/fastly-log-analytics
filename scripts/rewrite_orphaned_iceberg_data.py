#!/usr/bin/env python
"""Rewrite Iceberg data orphaned by a metadata rollback into the live table.

Companion to ``recover_orphaned_iceberg_metadata.py``. Use this when the
orphaned branch and the live branch have DIVERGED FIELD IDS, which makes
``add_files`` impossible: Iceberg binds columns by field id, and a rollback
that re-evolved the custom fields in a different order gives the same column a
different id on each branch. The 2026-08 SE-demo incident had 27 such columns
(``cmcd_*``, ``edge_*``, ``io_*``) — identical names and types, different ids.

Strategy: read the orphaned parquet by NAME, project it onto the live table's
schema, and append. Field ids stop mattering because we write fresh files.
Purely additive — no snapshot is dereferenced, so data unique to the live
branch survives (which a pointer repoint would have destroyed).

Resumability: work is grouped per calendar day and a day is skipped when the
live table already holds files for it. So a partial run resumes cleanly, and a
completed day is never appended twice. Days present on BOTH branches are
skipped unless ``--allow-overlap-day`` is passed, because appending there
could double-count rows.

DRY RUN BY DEFAULT.

    # pilot one day, verify, then do the rest
    python scripts/rewrite_orphaned_iceberg_data.py --service <id> --only-day 2026-07-10 --apply
    python scripts/rewrite_orphaned_iceberg_data.py --service <id> --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _partition_of(path: str) -> str | None:
    for part in path.split("/"):
        if part.startswith("timestamp_hour="):
            return part.split("=", 1)[1]
    return None


def _day_of(path: str) -> str:
    part = _partition_of(path)
    if not part:
        return "(unpartitioned)"
    bits = part.split("-")
    return "-".join(bits[:3]) if len(bits) >= 3 else part


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", required=True)
    ap.add_argument("--table", default="logs")
    ap.add_argument("--namespace", default="default")
    ap.add_argument("--only-day", default="", help="process just this YYYY-MM-DD")
    ap.add_argument("--max-days", type=int, default=0, help="stop after N days (0 = all)")
    ap.add_argument("--allow-overlap-day", action="store_true", help="also process days already present live")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    import duckdb

    from backend.core.duckdb import _configure_fos, _get_fos_client, get_source_for_service
    from backend.core.iceberg import _core as _core_mod
    from backend.core.iceberg._core import (
        _list_metadata_json_keys,
        _metadata_search_prefixes,
        _newest_metadata_key,
        init_iceberg_table,
        metadata_version,
    )
    from scripts.recover_orphaned_iceberg_metadata import _referenced_files, _static_table

    source = get_source_for_service(args.service)
    if not source:
        print(f"ERROR: no source for {args.service}", file=sys.stderr)
        return 2
    if source.get("access_level") == "read_only":
        print("ERROR: read_only source — refusing", file=sys.stderr)
        return 2

    s3 = _get_fos_client(source)
    keys: list[str] = []
    for prefix in _metadata_search_prefixes(source, args.namespace, args.table):
        keys = _list_metadata_json_keys(s3, source["bucket"], prefix)
        if keys:
            break
    if not keys:
        print("ERROR: no metadata.json found", file=sys.stderr)
        return 2
    newest_loc = f"s3://{source['bucket']}/{_newest_metadata_key(keys)}"

    live = init_iceberg_table(source, create=False, table_name=args.table)
    cur_files = _referenced_files(live)
    aband_files = _referenced_files(_static_table(source, newest_loc))
    orphan = aband_files - cur_files

    live_days = {_day_of(p) for p in cur_files}
    by_day: dict[str, list[str]] = defaultdict(list)
    for p in orphan:
        by_day[_day_of(p)].append(p)

    print("=" * 78)
    print(f"service : {args.service}")
    print(f"live    : {len(cur_files)} files, newest metadata v{metadata_version(newest_loc)}")
    print(f"orphaned: {len(orphan)} files across {len(by_day)} days")

    days = sorted(by_day)
    if args.only_day:
        days = [d for d in days if d == args.only_day]
        if not days:
            print(f"ERROR: no orphaned files for day {args.only_day}", file=sys.stderr)
            return 2

    todo, skipped = [], []
    for d in days:
        if d in live_days and not args.allow_overlap_day:
            skipped.append(d)
        else:
            todo.append(d)
    if args.max_days:
        todo = todo[: args.max_days]

    print(f"\nto process: {len(todo)} days ({sum(len(by_day[d]) for d in todo)} files)")
    for d in todo:
        print(f"    {d}  {len(by_day[d]):>4} files")
    if skipped:
        print(f"\nSKIPPED (already present on the live branch — would risk double-counting): {skipped}")
        print("    pass --allow-overlap-day only after checking for row overlap.")

    if not args.apply:
        print("\nDRY RUN — nothing written.")
        return 0

    target = live.schema().as_arrow()
    col_sql = ", ".join(f'"{f.name}"' for f in target)

    con = duckdb.connect()
    _configure_fos(con, source)

    total_rows = 0
    failures: list[tuple[str, str]] = []
    for d in todo:
        paths = sorted(by_day[d])
        try:
            # union_by_name reconciles by COLUMN NAME across files. Required:
            # the orphaned range spans schema evolution, so files inside a
            # single day disagree on column set/order and a plain glob read
            # fails with "schema mismatch in glob". Columns absent from a given
            # file come back NULL, which is the correct semantic for a column
            # added partway through the range.
            res = con.execute(
                f"SELECT {col_sql} FROM read_parquet($paths, union_by_name=true)",
                {"paths": paths},
            )
            # NOT .arrow() — that yields a RecordBatchReader (no num_rows) on
            # current DuckDB. to_arrow_table is the modern name;
            # fetch_arrow_table is its deprecated alias on older builds.
            arrow = res.to_arrow_table() if hasattr(res, "to_arrow_table") else res.fetch_arrow_table()
            arrow = arrow.cast(target)
            n = arrow.num_rows
            if n == 0:
                print(f"  {d}: 0 rows — skipping")
                continue
            live.append(arrow)
            total_rows += n
            print(f"  {d}: appended {n:>7} rows from {len(paths)} files (v{metadata_version(live.metadata_location)})")
            del arrow
        except Exception as e:
            failures.append((d, str(e)[:400]))
            print(f"  {d}: FAILED {str(e)[:400]}")

    try:
        _core_mod._set_cached_table(source, _core_mod._table_identifier(source, table_name=args.table), live)
        _core_mod._write_metadata_pointer(source, live.metadata_location, table=live)
        with _core_mod._pointer_cache_lock:
            _core_mod._pointer_cache.clear()
        print(f"\npointer updated -> {live.metadata_location}")
    except Exception as e:
        print(f"\nWARNING: pointer/cache update failed: {e}", file=sys.stderr)

    print(f"\nappended {total_rows} rows over {len(todo) - len(failures)} days; {len(failures)} failure(s)")
    for d, err in failures:
        print(f"  {d}: {err}")
    print(f"live table now: {len(_referenced_files(live))} files")
    print("\nNext: POST /api/admin/rebuild-local-view, then verify per-day counts.")
    return 0 if not failures else 4


if __name__ == "__main__":
    raise SystemExit(main())
