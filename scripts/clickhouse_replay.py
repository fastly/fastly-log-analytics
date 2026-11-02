"""Explicit bounded snapshot export and ClickHouse replay CLI."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from dataclasses import asdict

import backend.core.metadata.base as pg_connection
from backend import config
from backend import config as app_config
from backend.core.clickhouse import (
    MAX_BATCH_ROWS,
    MAX_DATASET_ROWS,
    ClickHouseClient,
    FosArtifacts,
    PgManifest,
    close_clickhouse_client,
    full_rebuild,
    replay_unpublished,
    utc,
)
from backend.core.request_telemetry import force_flush
from backend.utils.telemetry import flush_usage_log


def _restore_standalone_cli_modules() -> None:
    scripts_pkg = sys.modules.get("scripts")
    if scripts_pkg is None:
        return
    aliases = getattr(scripts_pkg, "_MODULE_ALIASES", None)
    if isinstance(aliases, dict):
        aliases.pop("scripts.backfill_high_scale", None)
    backfill_mod = importlib.import_module("scripts.backfill_high_scale")
    setattr(scripts_pkg, "backfill_high_scale", backfill_mod)

    alias_loader = getattr(scripts_pkg, "_AliasLoader", None)
    if alias_loader is not None and not getattr(alias_loader, "_standalone_hooked", False):
        orig_exec = alias_loader.exec_module

        def _exec_module(module) -> None:
            orig_exec(module)
            sys.modules["scripts.backfill_high_scale"] = backfill_mod
            setattr(scripts_pkg, "backfill_high_scale", backfill_mod)

        alias_loader.exec_module = staticmethod(_exec_module)
        alias_loader._standalone_hooked = True


_restore_standalone_cli_modules()

from scripts.backfill_high_scale import (  # noqa: E402
    AggregateBatchAdapter,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    PostgresBatchManifestStore,
    PostgresControlPlane,
    RecoveryCapacityMeasurement,
    backfill_main,
    get_high_scale_service_registry,
    main_origin,
    main_performance,
    main_security,
    parse_capacity_json,
    purge_cmcd_projections,
    purge_origin_projections,
    purge_rum_facts,
    rebuild_cmcd_projections_from_fos,
    rebuild_origin_projections_from_fos,
    rebuild_performance_projections_from_fos,
    rebuild_rum_facts_from_fos,
    rebuild_security_projections_from_fos,
    register_high_scale_services_from_environment,
    run_backfill_cli,
)


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
        if args.export:
            operation = "export"
        elif args.rebuild:
            operation = "rebuild"
        else:
            operation = "resume"
        return {
            "dry_run": True,
            "operation": operation,
            "limit": args.limit,
            "max_rows": args.max_rows,
            "batch_rows": args.batch_rows,
        }
    if args.export:
        raise RuntimeError("Snapshot export is not available in this CLI mode")
    cfg = config.load_config(args.service_id)
    if not cfg:
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


def replay_main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
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


main = replay_main

__all__ = [
    "AggregateBatchAdapter",
    "ClickHouseBatchAdapter",
    "ClickHouseClient",
    "ClickHousePublication",
    "PostgresBatchManifestStore",
    "PostgresControlPlane",
    "RecoveryCapacityMeasurement",
    "app_config",
    "backfill_main",
    "get_high_scale_service_registry",
    "main",
    "main_origin",
    "main_performance",
    "main_security",
    "parse_capacity_json",
    "parser",
    "purge_cmcd_projections",
    "purge_origin_projections",
    "purge_rum_facts",
    "rebuild_cmcd_projections_from_fos",
    "rebuild_origin_projections_from_fos",
    "rebuild_performance_projections_from_fos",
    "rebuild_rum_facts_from_fos",
    "rebuild_security_projections_from_fos",
    "register_high_scale_services_from_environment",
    "replay_main",
    "run",
    "run_backfill_cli",
]


if __name__ == "__main__":
    raise SystemExit(main())
