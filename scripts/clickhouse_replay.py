"""Explicit bounded snapshot export/replay. Dry-run never mutates storage."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from backend import config
from backend.core.clickhouse_client import close_clickhouse_client
from backend.core.clickhouse_export import FosArtifacts, export_snapshot
from backend.core.clickhouse_manifest import PgManifest
from backend.core.clickhouse_publication import full_rebuild, replay_unpublished
from backend.core.clickhouse_rows import MAX_BATCH_ROWS, MAX_DATASET_ROWS, utc
from backend.core.metadata import pg_connection
from backend.core.request_telemetry import force_flush
from backend.utils.usage_logger import flush_usage_log


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--service-id", required=True)
    result.add_argument("--limit", type=int, default=100, help="Maximum artifacts to replay, 1..1000")
    result.add_argument("--apply", action="store_true", help="Authorize immutable FOS writes / manifest changes")
    mode = result.add_mutually_exclusive_group(required=True)
    mode.add_argument("--export", action="store_true", help="Export a new pinned source snapshot")
    mode.add_argument("--rebuild", metavar="DATASET_ID", help="New generation from ALL retained artifacts")
    mode.add_argument("--resume", metavar="GENERATION", help="Retry unpublished members of this generation")
    result.add_argument("--start")
    result.add_argument("--end")
    result.add_argument("--max-rows", type=int, default=MAX_DATASET_ROWS)
    result.add_argument("--batch-rows", type=int, default=MAX_BATCH_ROWS)
    result.add_argument("--ttl-hours", type=float, default=24)
    return result


def run(args) -> dict:
    if (
        not 1 <= args.limit <= 1000
        or not 1 <= args.max_rows <= MAX_DATASET_ROWS
        or not 1 <= args.batch_rows <= MAX_BATCH_ROWS
    ):
        raise ValueError("invalid bounded limit")
    if not 0 < args.ttl_hours <= 24:
        raise ValueError("invalid TTL")
    if args.export:
        if not args.start or not args.end or utc(args.start) > utc(args.end):
            raise ValueError("export requires ordered inclusive --start and --end")
    elif args.start or args.end:
        raise ValueError("coverage belongs to the sealed dataset, not replay arguments")
    # Dry-run deliberately avoids both source access and Postgres initialization.
    # No usage-log flush, implicit schema setup, empty-target creation or FOS PUT.
    if not args.apply:
        return {
            "dry_run": True,
            "operation": "export" if args.export else "rebuild" if args.rebuild else "resume",
            "limit": args.limit,
            "max_rows": args.max_rows,
            "batch_rows": args.batch_rows,
        }
    if args.export:
        dataset = export_snapshot(
            args.service_id,
            start=utc(args.start),
            end=utc(args.end),
            max_rows=args.max_rows,
            batch_rows=args.batch_rows,
            ttl_hours=args.ttl_hours,
        )
        return {
            "dataset_id": dataset.dataset_id,
            "rows": dataset.expected_rows,
            "source_snapshot": dataset.source_snapshot,
            "expires_at": dataset.expires_at.isoformat(),
        }
    cfg = config.load_config(args.service_id)
    if not cfg or cfg.get("access_level", "read_write") != "read_write":
        raise ValueError("configured admin service required")
    objects = FosArtifacts(config.config_to_source(cfg))
    store = PgManifest()
    if args.rebuild:
        result = full_rebuild(args.service_id, args.rebuild, loader=objects.load, limit=args.limit, store=store)
    else:
        result = replay_unpublished(
            args.service_id, generation=args.resume, loader=objects.load, limit=args.limit, store=store
        )
    return asdict(result)


def main() -> int:
    args = parser().parse_args()
    try:
        result = run(args)
        sys.stdout.write(json.dumps(result) + "\n")
        return 0
    except Exception as exc:
        # Driver/SDK errors can embed DSNs, paths and credentials.
        sys.stderr.write(f"ClickHouse prototype command failed ({type(exc).__name__})\n")
        return 1
    finally:
        force_flush()
        close_clickhouse_client()
        if args.apply:
            flush_usage_log(args.service_id)
            if pg_connection.is_postgres():
                pg_connection.get_pg_pool().close()


if __name__ == "__main__":
    raise SystemExit(main())
