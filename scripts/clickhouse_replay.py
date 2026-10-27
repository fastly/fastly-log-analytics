"""Explicit bounded snapshot export/replay, high-scale projection backfill, and terminal-manifest RUM repair."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import time
import uuid
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from backend.core.clickhouse_client import ClickHouseClient, close_clickhouse_client
from backend.core.clickhouse_manifest import PgManifest
from backend.core.clickhouse_publication import (
    MAX_BATCH_ROWS,
    MAX_DATASET_ROWS,
    FosArtifacts,
    full_rebuild,
    replay_unpublished,
    utc,
)
from backend.high_scale.aggregate_batch_adapter import AggregateBatchAdapter
from backend.high_scale.publication import ClickHouseBatchAdapter, ClickHousePublication, PostgresBatchManifestStore
from backend.high_scale.recovery import (
    RecoveryCapacityMeasurement,
    purge_cmcd_projections,
    purge_origin_projections,
    purge_rum_facts,
    rebuild_cmcd_projections_from_fos,
    rebuild_origin_projections_from_fos,
    rebuild_performance_projections_from_fos,
    rebuild_rum_facts_from_fos,
    rebuild_security_projections_from_fos,
)
from backend.high_scale.registry_bootstrap import (
    get_high_scale_service_registry,
    register_high_scale_services_from_environment,
)

from backend import config
from backend import config as app_config
from backend.core.metadata import pg_connection
from backend.core.request_telemetry import force_flush
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.utils.telemetry import flush_usage_log


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
        raise RuntimeError("Snapshot export is not available in this CLI mode")
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


# ── Terminal-Manifest RUM Repair (formerly scripts/recover_high_scale_rum.py) ───────────────────


def recover(manifest_prefix: str, *, apply: bool, approve_digest: str | None) -> dict[str, Any]:
    started = time.perf_counter()
    import psycopg
    from backend.high_scale.publication import _DOMAIN_COLUMNS, _DOMAIN_TABLES
    from backend.high_scale.registry_bootstrap import _default_archive_factory
    from backend.high_scale.rum_recovery import RumRecoveryBatchAdapter, execute_rum_recovery, plan_rum_recovery
    from psycopg.rows import dict_row

    from backend.high_scale.postgres_control import _archive_from_row

    if not re.fullmatch(r"[0-9a-f]{8,64}", manifest_prefix):
        raise ValueError("manifest prefix must be 8-64 lowercase hexadecimal characters")
    if apply and not re.fullmatch(r"[0-9a-f]{64}", approve_digest or ""):
        raise ValueError("apply requires reviewed plan digest")
    settings = config.load_clickhouse_config()
    if settings is None:
        raise ValueError("configured ClickHouse required")
    client = ClickHouseClient(replace(settings, query_timeout_s=min(settings.query_timeout_s, 5)))
    limits = {
        "max_rows_to_read": "100000",
        "read_overflow_mode": "throw",
        "max_result_rows": "1001",
        "result_overflow_mode": "throw",
    }
    try:
        with psycopg.connect(os.environ["METADATA_DSN"], row_factory=dict_row) as connection:
            connection.execute("SET LOCAL statement_timeout='15s'")
            connection.execute("SET LOCAL lock_timeout='2s'")
            if not apply:
                connection.execute("SET TRANSACTION READ ONLY")
            records = connection.execute(
                "SELECT * FROM high_scale_archive_manifests WHERE manifest_id LIKE %s LIMIT 2",
                (manifest_prefix + "%",),
            ).fetchall()
            if len(records) != 1:
                raise ValueError("manifest prefix must identify exactly one archive")
            item = _archive_from_row(records[0])
            manifest = item.manifest
            if manifest.domain not in {"rum_vitals", "rum_errors"} or item.state not in {
                "manifest_committed",
                "deletion_eligible",
                "source_deleted",
            }:
                raise ValueError("manifest is not a terminal committed RUM archive")
            if manifest.artifact.row_count > 1000 or manifest.artifact.size_bytes > 32 * 1024 * 1024:
                raise ValueError("manifest exceeds bounded repair limits")
            if apply:
                lock = int.from_bytes(
                    hashlib.sha256(("rum-repair:" + manifest.service_id).encode()).digest()[:8], "big", signed=True
                )
                held = connection.execute("SELECT pg_try_advisory_xact_lock(%s) AS held", (lock,)).fetchone()
                if held is None or not held["held"]:
                    raise ValueError("another scoped RUM repair is active")
            suffix = " FOR SHARE" if apply else ""
            owner = connection.execute(
                "SELECT owner_epoch,current_owner FROM high_scale_ownership WHERE service_id=%s" + suffix,
                (manifest.service_id,),
            ).fetchone()
            if owner is None or owner["current_owner"] != "high_scale" or owner["owner_epoch"] != item.owner_epoch:
                raise ValueError("archive owner epoch is not current")
            ledger = connection.execute(
                "SELECT object_key,status,archive_manifest_id,owner_epoch FROM high_scale_source_objects "
                "WHERE service_id=%s AND object_key=ANY(%s)" + suffix,
                (manifest.service_id, [source.key for source in manifest.sources]),
            ).fetchall()
            if len(ledger) != len(manifest.sources) or any(
                row["status"] not in {"acknowledged", "source_deleted"}
                or row["archive_manifest_id"] != manifest.manifest_id
                or row["owner_epoch"] != item.owner_epoch
                for row in ledger
            ):
                raise ValueError("source ledger is not terminal for this exact archive/owner")
            archive = _default_archive_factory(manifest.service_id)
            if archive is None:
                raise ValueError("verified archive reader required")

            def read_facts(domain, keys):
                placeholders = ",".join("{key" + str(i) + ":String}" for i in range(len(keys)))
                rows = client.execute(
                    f"SELECT {','.join(_DOMAIN_COLUMNS[domain])} FROM {_DOMAIN_TABLES[domain]} "
                    "WHERE service_id={service_id:String} AND source_object_key IN (" + placeholders + ") LIMIT 1001",
                    {"service_id": manifest.service_id, **{f"key{i}": key for i, key in enumerate(keys)}},
                    settings=limits,
                )
                if len(rows) > 1000:
                    raise ValueError("existing fact result exceeds bound")
                receipts = {}
                for row in rows:
                    batch_id = str(row["batch_id"])
                    if batch_id not in receipts:
                        receipt = client.execute(
                            "SELECT publication_state,expected_rows,visible_rows FROM high_scale_batch_publications FINAL "
                            "WHERE service_id={service_id:String} AND domain={domain:String} "
                            "AND generation={generation:String} AND batch_id={batch_id:String} LIMIT 2",
                            {
                                "service_id": manifest.service_id,
                                "domain": domain,
                                "generation": str(item.owner_epoch),
                                "batch_id": batch_id,
                            },
                            settings=limits,
                        )
                        receipts[batch_id] = (
                            len(receipt) == 1
                            and receipt[0]["publication_state"] == "visible"
                            and receipt[0]["expected_rows"] == receipt[0]["visible_rows"]
                        )
                    row["repair_visible"] = receipts[batch_id] and row["publication_state"] == "visible"
                return rows

            def make_plan():
                return plan_rum_recovery(manifest, archive, owner_epoch=item.owner_epoch, read_facts=read_facts)

            # Constructing these adapters performs no writes. Dry-run never calls publish.
            publication = ClickHousePublication(
                PostgresBatchManifestStore(PostgresControlPlane()), RumRecoveryBatchAdapter(client)
            )
            report = execute_rum_recovery(make_plan, publication, apply=apply, approve_digest=approve_digest)
            return {**report, "read_only": not apply, "duration_ms": round((time.perf_counter() - started) * 1000, 2)}
    finally:
        client.close()


def recover_rum_main(argv: list[str] | None = None) -> int:
    rum_parser = argparse.ArgumentParser(description=__doc__)
    rum_parser.add_argument("--manifest-prefix", required=True)
    rum_parser.add_argument("--apply", action="store_true")
    rum_parser.add_argument("--approve-plan")
    args = rum_parser.parse_args(argv)
    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            report = recover(args.manifest_prefix, apply=args.apply, approve_digest=args.approve_plan)
    except Exception as exc:
        print(json.dumps({"status": "blocked", "error_type": type(exc).__name__}))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


# ── High-Scale Projection Backfill (formerly scripts/backfill_high_scale.py) ────────────────────


def parse_capacity_json(parser_obj: argparse.ArgumentParser, path: Path | None) -> RecoveryCapacityMeasurement | None:
    if path is None:
        return None
    try:
        capacity = RecoveryCapacityMeasurement(**json.loads(path.read_text()))
        capacity.validate()
        return capacity
    except (OSError, TypeError, ValueError) as exc:
        parser_obj.error(f"--capacity-json is invalid: {exc}")
        return None


def run_backfill_cli(
    rebuild_fn: Callable[..., Any],
    *,
    description: str | None = None,
    default_days: int = 30,
    with_capacity_json: bool = True,
    configure_parser: Callable[[argparse.ArgumentParser], None] | None = None,
    validate_args: Callable[[argparse.ArgumentParser, argparse.Namespace], None] | None = None,
    select_adapter: Callable[[argparse.Namespace, ModuleType], type] | None = None,
    caller_module: ModuleType | None = None,
    argv: list[str] | None = None,
) -> int:
    mod = caller_module or sys.modules[__name__]
    backfill_parser = argparse.ArgumentParser(description=description)
    backfill_parser.add_argument("--service-id", required=True)
    backfill_parser.add_argument("--days", type=int, default=default_days)
    backfill_parser.add_argument("--rebuild-id")
    if with_capacity_json:
        backfill_parser.add_argument(
            "--capacity-json",
            type=Path,
            help="measured RecoveryCapacityMeasurement fields; exit 3 when the rebuild completes but the claim is invalid",
        )
    if configure_parser is not None:
        configure_parser(backfill_parser)

    args = backfill_parser.parse_args(argv)
    if args.days <= 0:
        backfill_parser.error("--days must be positive")
    if validate_args is not None:
        validate_args(backfill_parser, args)

    capacity = (
        parse_capacity_json(backfill_parser, getattr(args, "capacity_json", None)) if with_capacity_json else None
    )

    cfg = getattr(mod, "app_config", app_config)
    ch_cls = getattr(mod, "ClickHouseClient", ClickHouseClient)
    get_registry = getattr(mod, "get_high_scale_service_registry", get_high_scale_service_registry)
    register_env = getattr(
        mod, "register_high_scale_services_from_environment", register_high_scale_services_from_environment
    )
    control_cls = getattr(mod, "PostgresControlPlane", PostgresControlPlane)
    manifest_store_cls = getattr(mod, "PostgresBatchManifestStore", PostgresBatchManifestStore)
    pub_cls = getattr(mod, "ClickHousePublication", ClickHousePublication)
    default_adapter_cls = getattr(mod, "AggregateBatchAdapter", AggregateBatchAdapter)

    settings = cfg.load_clickhouse_config()
    if settings is None:
        raise RuntimeError("ClickHouse configuration is required")
    client = ch_cls(settings)
    try:
        registry = get_registry()
        register_env(registry, client=client)
        service = registry.resolve(args.service_id)
        if service is None or service.manifest_catalog is None or service.archive is None:
            raise RuntimeError("service is not registered with its archive dependencies")
        control = control_cls()
        adapter_cls = select_adapter(args, mod) if select_adapter is not None else default_adapter_cls
        publication = pub_cls(
            manifest_store_cls(control),
            adapter_cls(cast(ClickHouseClient, service.client)),
        )
        report = rebuild_fn(args=args, service=service, publication=publication, capacity=capacity, module=mod)
        print(json.dumps(asdict(report), sort_keys=True))
        if not report.full_rebuild_complete:
            return 1
        return 3 if getattr(report, "recovery_claim_valid", None) is False else 0
    finally:
        client.close()


def _select(projection: str):
    """``(adapter, purge, rebuild)`` for ``projection``; looked up at call time so callers can substitute them."""
    if projection == "cmcd":
        return ClickHouseBatchAdapter, purge_cmcd_projections, rebuild_cmcd_projections_from_fos
    if projection in ("rum_vitals", "rum_errors"):

        def purge(client, **kwargs):
            return purge_rum_facts(client, domain=projection, **kwargs)

        def rebuild(*args, **kwargs):
            return rebuild_rum_facts_from_fos(*args, domain=projection, **kwargs)

        return ClickHouseBatchAdapter, purge, rebuild
    return AggregateBatchAdapter, purge_origin_projections, rebuild_origin_projections_from_fos


def _configure_origin_parser(parser_obj: argparse.ArgumentParser) -> None:
    parser_obj.add_argument(
        "--projection",
        choices=("origin", "cmcd", "rum_vitals", "rum_errors"),
        default="origin",
        help="which projection (origin, cmcd: request archive) or RUM fact table to rebuild from its archive",
    )
    parser_obj.add_argument(
        "--replace-existing",
        action="store_true",
        help="delete this service's existing rows of the selected projection in the window first "
        "(required when rows already exist, otherwise every request is counted twice)",
    )


def _validate_origin_args(parser_obj: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.replace_existing and args.rebuild_id:
        parser_obj.error("--replace-existing cannot resume a rebuild: the purge would delete rows the resumed id skips")


def main_origin(argv: list[str] | None = None) -> int:
    def _run(*, args, service, publication, capacity, module):
        _, purge, rebuild = _select(args.projection)
        if args.replace_existing:
            purge(
                service.client,
                service_id=args.service_id,
                since=datetime.now(UTC) - timedelta(days=args.days),
            )
        return rebuild(
            service.manifest_catalog,
            service.archive,
            publication,
            service_id=args.service_id,
            window=timedelta(days=args.days),
            rebuild_id=args.rebuild_id,
            capacity=capacity,
        )

    return run_backfill_cli(
        _run,
        configure_parser=_configure_origin_parser,
        validate_args=_validate_origin_args,
        select_adapter=lambda args, _mod: _select(args.projection)[0],
        caller_module=sys.modules[__name__],
        argv=argv,
    )


def _utc_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--until must include a UTC offset, for example 2026-10-20T12:00:00Z")
    return parsed.astimezone(UTC)


def main_performance(argv: list[str] | None = None) -> int:
    pinned_rebuild_id: dict[str, str] = {}

    def _validate(_parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
        rebuild_id = args.rebuild_id or uuid.uuid4().hex
        pinned_rebuild_id["id"] = rebuild_id
        print(f"rebuild_id={rebuild_id}", file=sys.stderr, flush=True)

    return run_backfill_cli(
        lambda *, args, service, publication, **_kw: rebuild_performance_projections_from_fos(
            service.manifest_catalog,
            service.archive,
            publication,
            service_id=args.service_id,
            now=args.until,
            window=timedelta(days=args.days),
            rebuild_id=pinned_rebuild_id["id"],
        ),
        with_capacity_json=False,
        configure_parser=lambda p: p.add_argument(
            "--until", type=_utc_instant, required=True, help="worker deploy time (ISO-8601 with offset)"
        ),
        validate_args=_validate,
        caller_module=sys.modules[__name__],
        argv=argv,
    )


def main_security(argv: list[str] | None = None) -> int:
    return run_backfill_cli(
        lambda *, args, service, publication, capacity, **_kw: rebuild_security_projections_from_fos(
            service.manifest_catalog,
            service.archive,
            publication,
            service_id=args.service_id,
            window=timedelta(days=args.days),
            rebuild_id=args.rebuild_id,
            capacity=capacity,
        ),
        description="Rebuild bounded Security projections from immutable high-scale archives.",
        caller_module=sys.modules[__name__],
        argv=argv,
    )


def backfill_main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    probe = argparse.ArgumentParser(add_help=False)
    probe.add_argument(
        "--projection",
        choices=("origin", "cmcd", "rum_vitals", "rum_errors", "performance", "security"),
        default=None,
    )
    known, remaining = probe.parse_known_args(raw_argv)

    if known.projection == "performance" or (
        known.projection is None and any(arg == "--until" or arg.startswith("--until=") for arg in remaining)
    ):
        return main_performance(remaining)
    if known.projection == "security":
        return main_security(remaining)

    projection = known.projection or "origin"
    return main_origin(["--projection", projection, *remaining])


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    prog = Path(sys.argv[0]).name if sys.argv else ""
    spec_name = globals().get("__spec__").name if globals().get("__spec__") else ""
    if (
        spec_name == "scripts.recover_high_scale_rum"
        or "recover_high_scale_rum" in prog
        or any(
            a in ("--manifest-prefix", "--approve-plan") or a.startswith(("--manifest-prefix=", "--approve-plan="))
            for a in raw_argv
        )
    ):
        return recover_rum_main(argv)
    if (
        spec_name == "scripts.backfill_high_scale"
        or "backfill" in prog
        or any(
            a in ("--projection", "--days", "--rebuild-id", "--replace-existing", "--until", "--capacity-json")
            or a.startswith(("--projection=", "--days=", "--rebuild-id=", "--until=", "--capacity-json="))
            for a in raw_argv
        )
        or (argv is not None and not any(a in ("--export", "--rebuild", "--resume") for a in raw_argv))
    ):
        return backfill_main(argv)
    return replay_main(argv)


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
    "recover",
    "recover_rum_main",
    "register_high_scale_services_from_environment",
    "replay_main",
    "run",
    "run_backfill_cli",
]


if __name__ == "__main__":
    raise SystemExit(main())
