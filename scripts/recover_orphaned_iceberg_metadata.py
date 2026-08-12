#!/usr/bin/env python
"""Recover data orphaned by an Iceberg metadata rollback.

Background — 2026-08 SE-demo incident. ``_read_metadata_pointer``'s discovery
fallback used an unpaginated ``list_objects_v2``. A response caps at 1000 keys
and pyiceberg zero-pads the metadata version prefix, so the first page holds
the OLDEST versions; ``sorted(...)[-1]`` resolved v952 while v13723 was
current. The table then committed forward from that stale base, and every data
file referenced only by the abandoned branch became unreachable. The parquet
was never deleted — only dereferenced.

The read path is fixed (paginate, order by version, refuse to regress), but a
table already living on the stale branch stays there: its pointer legitimately
advances from the low base. This script reattaches the abandoned data.

Two strategies:

``add-files`` (DEFAULT, additive, lossless)
    Adds the data files referenced by the abandoned branch but not by the
    current table, via ``Table.add_files``. Nothing is ever dereferenced, so
    data unique to the CURRENT branch survives. Safe to abort part-way — each
    batch is its own commit and ``check_duplicate_files`` makes a re-run
    idempotent.

``repoint`` (moves the pointer)
    Rewrites ``metadata_location.txt`` to the newest metadata version. Only
    correct when the current branch holds NOTHING unique — otherwise it trades
    one gap for another. The script refuses unless ``--force`` is passed.

DRY RUN BY DEFAULT. Nothing is written without ``--apply``.

    uv run python scripts/recover_orphaned_iceberg_metadata.py --service <id>
    uv run python scripts/recover_orphaned_iceberg_metadata.py --service <id> --apply

Inside the prod container the repo root must be importable:

    docker exec -e PYTHONPATH=/app app-backend-1 \
        python scripts/recover_orphaned_iceberg_metadata.py --service <id>
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict

# Running as `python scripts/foo.py` puts scripts/ on sys.path, not the repo
# root, so `import backend` fails. Fix it here rather than making every caller
# remember PYTHONPATH.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

BATCH = 100


def _partition_of(path: str) -> str | None:
    for part in path.split("/"):
        if part.startswith("timestamp_hour="):
            return part.split("=", 1)[1]
    return None


def _day_of(partition: str | None) -> str | None:
    if not partition:
        return None
    bits = partition.split("-")
    return "-".join(bits[:3]) if len(bits) >= 3 else partition


def _by_day(paths) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for p in paths:
        d = _day_of(_partition_of(p))
        out[d or "(unpartitioned)"] += 1
    return dict(out)


def _print_by_day(paths, label: str) -> None:
    counts = _by_day(paths)
    print(f"\n{label}: {len(paths)} data files across {len(counts)} days")
    for day in sorted(counts):
        print(f"    {day}  {counts[day]:>5} files")


def _io_props(source: dict) -> dict:
    """S3/FileIO half of _get_catalog's props, for a catalog-free StaticTable."""
    endpoint = source.get("endpoint") or f"{source.get('region', 'us-east-1')}.object.fastlystorage.app"
    return {
        "s3.endpoint": f"https://{endpoint}",
        "s3.access-key-id": source.get("access_key_id", ""),
        "s3.secret-access-key": source.get("secret_access_key", ""),
        "s3.path-style-access": "true",
        "s3.region": source.get("region", "us-east-1"),
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    }


def _referenced_files(table) -> set[str]:
    if table.current_snapshot() is None:
        return set()
    return {t.file.file_path for t in table.scan().plan_files()}


def _static_table(source: dict, metadata_loc: str):
    """Read a metadata.json + its manifests WITHOUT touching the catalog.

    Deliberately not ``register_table``: this runs against a damaged table and
    a probe registration would mutate the same SQLite catalog we're repairing.
    """
    from pyiceberg.table import StaticTable

    from backend.core.iceberg._core import _PENDING_FS_SOURCE

    _PENDING_FS_SOURCE.set(source)
    return StaticTable.from_metadata(metadata_loc, _io_props(source))


def _snapshot_rows(table) -> str:
    snap = table.current_snapshot()
    if snap is None:
        return "no snapshot"
    return snap.summary.get("total-records", "?") if snap.summary else "?"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", required=True, help="logging service id")
    ap.add_argument("--table", default="logs")
    ap.add_argument("--namespace", default="default")
    ap.add_argument("--strategy", choices=("add-files", "repoint"), default="add-files")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--force", action="store_true", help="allow repoint even when it would lose data")
    args = ap.parse_args()

    from backend.core.duckdb import _get_fos_client, get_source_for_service
    from backend.core.iceberg._core import (
        _list_metadata_json_keys,
        _metadata_search_prefixes,
        _newest_metadata_key,
        _read_metadata_pointer,
        init_iceberg_table,
        metadata_version,
    )

    source = get_source_for_service(args.service)
    if not source:
        print(f"ERROR: no source for service {args.service}", file=sys.stderr)
        return 2
    if source.get("access_level") == "read_only":
        print("ERROR: source is read_only — refusing", file=sys.stderr)
        return 2

    identifier = (args.namespace, args.table)
    bucket = source["bucket"]
    s3 = _get_fos_client(source)

    current_loc = _read_metadata_pointer(source, identifier)
    print("=" * 78)
    print(f"service        : {args.service}")
    print(f"table          : {args.namespace}.{args.table}")
    print(f"strategy       : {args.strategy}{'  (APPLY)' if args.apply else '  (dry run)'}")
    print(f"CURRENT POINTER: {current_loc}  (v{metadata_version(current_loc or '')})")

    keys: list[str] = []
    for prefix in _metadata_search_prefixes(source, args.namespace, args.table):
        keys = _list_metadata_json_keys(s3, bucket, prefix)
        if keys:
            break
    if not keys:
        print("ERROR: no metadata.json objects found", file=sys.stderr)
        return 2

    newest_key = _newest_metadata_key(keys)
    newest_loc = f"s3://{bucket}/{newest_key}"
    print(f"NEWEST ON DISK : {newest_loc}  (v{metadata_version(newest_key)})")
    print(f"metadata objects scanned: {len(keys)}")

    if current_loc and metadata_version(newest_key) <= metadata_version(current_loc):
        print("\nNothing to do — the pointer already references the newest metadata.")
        return 0

    print(f"\nABANDONED RANGE: v{metadata_version(current_loc or '')} → v{metadata_version(newest_key)}")
    print("\nresolving data files on both branches (reads manifests)...")

    live = init_iceberg_table(source, create=False, table_name=args.table)
    cur_files = _referenced_files(live)
    abandoned = _static_table(source, newest_loc)
    aband_files = _referenced_files(abandoned)

    recoverable = aband_files - cur_files
    unique_to_current = cur_files - aband_files

    print(f"\ncurrent branch   : {len(cur_files):>5} data files, {_snapshot_rows(live)} rows")
    print(f"abandoned branch : {len(aband_files):>5} data files, {_snapshot_rows(abandoned)} rows")
    _print_by_day(recoverable, "RECOVERABLE (on abandoned branch only)")
    _print_by_day(unique_to_current, "UNIQUE TO CURRENT (a repoint would lose these)")

    if args.strategy == "repoint":
        if unique_to_current and not args.force:
            print(
                f"\nREFUSING to repoint: {len(unique_to_current)} data files exist only on the\n"
                "current branch and would be dereferenced. Use --strategy add-files (additive,\n"
                "loses nothing), or pass --force if you accept the loss."
            )
            return 3
        if not args.apply:
            print("\nDRY RUN — nothing written.")
            return 0
        from backend.core.iceberg._core import _pointer_cache, _pointer_cache_lock, _write_metadata_pointer

        backup = f"{args.service}.pointer-backup.json"
        with open(backup, "w") as fh:
            json.dump({"service": args.service, "pointer": current_loc}, fh, indent=2)
        print(f"\nwrote pointer backup -> {backup}")
        _write_metadata_pointer(source, newest_loc)
        with _pointer_cache_lock:
            _pointer_cache.clear()
        print(f"pointer updated -> {newest_loc}")
        return 0

    # ── add-files ────────────────────────────────────────────────────────────
    if not recoverable:
        print("\nNothing to add — every abandoned file is already referenced.")
        return 0

    if not args.apply:
        print(
            f"\nDRY RUN — would add {len(recoverable)} data files to the live table in "
            f"{(len(recoverable) + BATCH - 1) // BATCH} batches of up to {BATCH}."
        )
        print("Nothing was written. Re-run with --apply.")
        return 0

    ordered = sorted(recoverable)
    print(f"\nadding {len(ordered)} files in batches of {BATCH}...")
    added = 0
    failures: list[tuple[str, str]] = []
    for i in range(0, len(ordered), BATCH):
        batch = ordered[i : i + BATCH]
        try:
            live.add_files(batch, check_duplicate_files=True)
            added += len(batch)
            print(f"  [{added}/{len(ordered)}] committed (v{metadata_version(live.metadata_location)})")
        except Exception as e:
            failures.append((f"batch@{i}", str(e)[:300]))
            print(f"  [batch@{i}] FAILED: {str(e)[:300]}")

    # Same post-commit sequence the buffer commit path uses, so readers in this
    # and other processes pick the new snapshot up.
    from backend.core.iceberg import _core as _core_mod

    try:
        _core_mod._set_cached_table(source, _core_mod._table_identifier(source, table_name=args.table), live)
        _core_mod._write_metadata_pointer(source, live.metadata_location, table=live)
        with _core_mod._pointer_cache_lock:
            _core_mod._pointer_cache.clear()
        print(f"\npointer updated -> {live.metadata_location}")
    except Exception as e:
        print(f"\nWARNING: post-commit cache/pointer update failed: {e}", file=sys.stderr)

    print(f"\nadded {added}/{len(ordered)} files; {len(failures)} batch failure(s)")
    for name, err in failures:
        print(f"  {name}: {err}")
    print(f"live table now: {len(_referenced_files(live))} data files, {_snapshot_rows(live)} rows")
    print("\nNext: POST /api/admin/rebuild-local-view, then verify row counts per day.")
    return 0 if not failures else 4


if __name__ == "__main__":
    raise SystemExit(main())
