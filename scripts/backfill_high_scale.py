"""Unified CLI entrypoint and shared runner for rebuilding high-scale projections and fact tables from immutable archives."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from backend import config as app_config
from backend.core.clickhouse_client import ClickHouseClient
from backend.high_scale.aggregate_batch_adapter import AggregateBatchAdapter
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.publication import ClickHouseBatchAdapter, ClickHousePublication, PostgresBatchManifestStore
from backend.high_scale.recovery_capacity import RecoveryCapacityMeasurement
from backend.high_scale.registry import get_high_scale_service_registry
from backend.high_scale.registry_bootstrap import register_high_scale_services_from_environment


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


def main(argv: list[str] | None = None) -> int:
    from scripts import (
        backfill_high_scale_origin,
        backfill_high_scale_performance,
        backfill_high_scale_security,
    )

    raw_argv = list(sys.argv[1:] if argv is None else argv)
    probe = argparse.ArgumentParser(add_help=False)
    probe.add_argument(
        "--projection",
        choices=("origin", "cmcd", "rum_vitals", "rum_errors", "performance", "security"),
        default="origin",
    )
    known, remaining = probe.parse_known_args(raw_argv)

    if known.projection == "performance":
        return backfill_high_scale_performance.main(remaining)
    if known.projection == "security":
        return backfill_high_scale_security.main(remaining)

    return backfill_high_scale_origin.main(["--projection", known.projection, *remaining])


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
    "parse_capacity_json",
    "register_high_scale_services_from_environment",
    "run_backfill_cli",
]


if __name__ == "__main__":
    raise SystemExit(main())
