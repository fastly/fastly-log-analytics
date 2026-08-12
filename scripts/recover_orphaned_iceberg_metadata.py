#!/usr/bin/env python
"""Recover data orphaned by an Iceberg metadata rollback.

Background — 2026-08 SE-demo incident. ``_read_metadata_pointer``'s discovery
fallback used an unpaginated ``list_objects_v2``. A response caps at 1000 keys
and pyiceberg zero-pads the metadata version prefix, so the first page holds
the OLDEST versions; ``sorted(...)[-1]`` resolved v952 while v8999 was current.
The table then committed forward from that stale base, and every data file
referenced only by the abandoned v953…v8999 range became unreachable. The
parquet was never deleted — only dereferenced.

The read path is fixed (paginate + order by version + refuse to regress), but a
table already living on the stale branch stays there: its pointer legitimately
advances from the low base. This script reattaches the abandoned branch.

DRY RUN BY DEFAULT. Nothing is written without ``--apply``.

    uv run python scripts/recover_orphaned_iceberg_metadata.py --service <id>
    uv run python scripts/recover_orphaned_iceberg_metadata.py --service <id> --apply

What ``--apply`` does, and how to undo it: it rewrites the
``metadata_location.txt`` pointer object (and the local SQLite catalog row) to
the newest metadata version found in ``metadata/``. It creates NO new snapshot
and deletes nothing, so the undo is to write the previous pointer value back —
printed as PREVIOUS POINTER before any change, and saved to
``<service>.pointer-backup.txt``.

Data on the CURRENT branch but not the recovered one is reported as
"would become unreachable". Reattaching is only safe once that list is empty or
you have accepted it — re-ingest or ``add_files`` those partitions afterwards.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f}{unit}" if unit != "B" else f"{n}B"
        n /= 1024.0
    return f"{n:.1f}PB"


def _partition_of(path: str) -> str | None:
    for part in path.split("/"):
        if part.startswith("timestamp_hour="):
            return part.split("=", 1)[1]
    return None


def _day_of(partition: str | None) -> str | None:
    # partitions look like 2026-07-20-12
    if not partition:
        return None
    bits = partition.split("-")
    return "-".join(bits[:3]) if len(bits) >= 3 else partition


def _data_files_for_metadata(source: dict, metadata_loc: str) -> set[str]:
    """Return the data-file paths referenced by a metadata.json's current snapshot.

    Uses ``StaticTable``, which reads the metadata + manifests directly and
    never touches the catalog. Deliberately NOT ``register_table`` — this runs
    against a damaged table, and a probe registration under a scratch
    identifier would mutate the same SQLite catalog we are trying to repair.
    Read-only by construction.
    """
    from pyiceberg.table import StaticTable

    from backend.core.iceberg._core import _PENDING_FS_SOURCE

    # Mirrors the S3/FileIO half of _get_catalog's props. Kept local rather
    # than refactoring a helper out of that hot commit path.
    endpoint = source.get("endpoint") or f"{source.get('region', 'us-east-1')}.object.fastlystorage.app"
    props = {
        "s3.endpoint": f"https://{endpoint}",
        "s3.access-key-id": source.get("access_key_id", ""),
        "s3.secret-access-key": source.get("secret_access_key", ""),
        "s3.path-style-access": "true",
        "s3.region": source.get("region", "us-east-1"),
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    }
    # The FosS3FileSystem subclass resolves credentials off this ContextVar.
    _PENDING_FS_SOURCE.set(source)

    table = StaticTable.from_metadata(metadata_loc, props)
    out: set[str] = set()
    if table.current_snapshot() is None:
        return out
    for task in table.scan().plan_files():
        out.add(task.file.file_path)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", required=True, help="logging service id")
    ap.add_argument("--table", default="logs")
    ap.add_argument("--namespace", default="default")
    ap.add_argument("--apply", action="store_true", help="write the pointer (default: dry run)")
    args = ap.parse_args()

    from backend.core.duckdb import _get_fos_client, get_source_for_service
    from backend.core.iceberg._core import (
        _list_metadata_json_keys,
        _metadata_search_prefixes,
        _newest_metadata_key,
        _read_metadata_pointer,
        _write_metadata_pointer,
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

    current = _read_metadata_pointer(source, identifier)
    print("=" * 78)
    print(f"service        : {args.service}")
    print(f"table          : {args.namespace}.{args.table}")
    print(f"CURRENT POINTER: {current}  (v{metadata_version(current or '')})")

    keys: list[str] = []
    for prefix in _metadata_search_prefixes(source, args.namespace, args.table):
        keys = _list_metadata_json_keys(s3, bucket, prefix)
        if keys:
            break
    if not keys:
        print("ERROR: no metadata.json objects found", file=sys.stderr)
        return 2

    newest_key = _newest_metadata_key(keys)
    newest = f"s3://{bucket}/{newest_key}"
    print(f"NEWEST ON DISK : {newest}  (v{metadata_version(newest_key)})")
    print(f"metadata objects scanned: {len(keys)}")

    if current and metadata_version(newest_key) <= metadata_version(current):
        print("\nNothing to do — the pointer already references the newest metadata.")
        return 0

    print(f"\nORPHANED RANGE : v{metadata_version(current or '')} → v{metadata_version(newest_key)}")

    print("\nresolving data files on both branches (this reads manifests)...")
    cur_files = _data_files_for_metadata(source, current) if current else set()
    new_files = _data_files_for_metadata(source, newest)

    recovered = new_files - cur_files
    lost = cur_files - new_files

    def summarize(paths: set[str], label: str) -> None:
        by_day: dict[str, int] = defaultdict(int)
        for p in paths:
            d = _day_of(_partition_of(p))
            if d:
                by_day[d] += 1
        print(f"\n{label}: {len(paths)} data files across {len(by_day)} days")
        for day in sorted(by_day):
            print(f"    {day}  {by_day[day]:>5} files")

    print(f"\ncurrent branch : {len(cur_files)} data files")
    print(f"recovered branch: {len(new_files)} data files")
    summarize(recovered, "WOULD RECOVER")
    summarize(lost, "WOULD BECOME UNREACHABLE")

    if lost:
        print(
            "\n!! The current branch holds data the recovered branch does not.\n"
            "!! Reattaching alone would trade one gap for another. Re-ingest or\n"
            "!! add_files those partitions after reattaching."
        )

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply to move the pointer.")
        return 0

    backup = f"{args.service}.pointer-backup.txt"
    with open(backup, "w") as fh:
        fh.write(json.dumps({"service": args.service, "identifier": list(identifier), "pointer": current}, indent=2))
    print(f"\nwrote pointer backup -> {backup}")

    _write_metadata_pointer(source, newest)
    print(f"pointer updated -> {newest}")

    from backend.core.iceberg._core import _pointer_cache, _pointer_cache_lock

    with _pointer_cache_lock:
        _pointer_cache.clear()
    print("pointer cache cleared")
    print("\nNext: POST /api/admin/rebuild-local-view to rebind the DuckDB view, then verify row counts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
