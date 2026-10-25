"""Unified CLI entrypoint and shared runner for rebuilding high-scale projections and fact tables from immutable archives."""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from backend import config as app_config
from backend.core.clickhouse_client import ClickHouseClient
from backend.high_scale.aggregate_batch_adapter import AggregateBatchAdapter
from backend.high_scale.postgres_control import PostgresControlPlane
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


def parse_capacity_json(parser: argparse.ArgumentParser, path: Path | None) -> RecoveryCapacityMeasurement | None:
    if path is None:
        return None
    try:
        capacity = RecoveryCapacityMeasurement(**json.loads(path.read_text()))
        capacity.validate()
        return capacity
    except (OSError, TypeError, ValueError) as exc:
        parser.error(f"--capacity-json is invalid: {exc}")
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
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--service-id", required=True)
    parser.add_argument("--days", type=int, default=default_days)
    parser.add_argument("--rebuild-id")
    if with_capacity_json:
        parser.add_argument(
            "--capacity-json",
            type=Path,
            help="measured RecoveryCapacityMeasurement fields; exit 3 when the rebuild completes but the claim is invalid",
        )
    if configure_parser is not None:
        configure_parser(parser)

    args = parser.parse_args(argv)
    if args.days <= 0:
        parser.error("--days must be positive")
    if validate_args is not None:
        validate_args(parser, args)

    capacity = parse_capacity_json(parser, getattr(args, "capacity_json", None)) if with_capacity_json else None

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


def _configure_origin_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--projection",
        choices=("origin", "cmcd", "rum_vitals", "rum_errors"),
        default="origin",
        help="which projection (origin, cmcd: request archive) or RUM fact table to rebuild from its archive",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="delete this service's existing rows of the selected projection in the window first "
        "(required when rows already exist, otherwise every request is counted twice)",
    )


def _validate_origin_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.replace_existing and args.rebuild_id:
        parser.error("--replace-existing cannot resume a rebuild: the purge would delete rows the resumed id skips")


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


def main(argv: list[str] | None = None) -> int:
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


__all__ = [
    "AggregateBatchAdapter",
    "ClickHouseBatchAdapter",
    "ClickHouseClient",
    "ClickHousePublication",
    "PostgresBatchManifestStore",
    "PostgresControlPlane",
    "RecoveryCapacityMeasurement",
    "app_config",
    "get_high_scale_service_registry",
    "main",
    "main_origin",
    "main_performance",
    "main_security",
    "parse_capacity_json",
    "purge_cmcd_projections",
    "purge_origin_projections",
    "purge_rum_facts",
    "rebuild_cmcd_projections_from_fos",
    "rebuild_origin_projections_from_fos",
    "rebuild_performance_projections_from_fos",
    "rebuild_rum_facts_from_fos",
    "rebuild_security_projections_from_fos",
    "register_high_scale_services_from_environment",
    "run_backfill_cli",
]


if __name__ == "__main__":
    raise SystemExit(main())
