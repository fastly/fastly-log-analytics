"""Rebuild bounded high-scale projections and fact columns (Origin, CMCD, RUM) from immutable archives."""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta

from backend.high_scale.recovery import (
    purge_cmcd_projections,
    purge_origin_projections,
    purge_rum_facts,
    rebuild_cmcd_projections_from_fos,
    rebuild_origin_projections_from_fos,
    rebuild_rum_facts_from_fos,
)
from scripts.backfill_high_scale import (
    AggregateBatchAdapter,
    ClickHouseBatchAdapter,
    ClickHouseClient,
    ClickHousePublication,
    PostgresBatchManifestStore,
    PostgresControlPlane,
    app_config,
    get_high_scale_service_registry,
    register_high_scale_services_from_environment,
    run_backfill_cli,
)


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


def _configure_parser(parser: argparse.ArgumentParser) -> None:
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


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.replace_existing and args.rebuild_id:
        parser.error("--replace-existing cannot resume a rebuild: the purge would delete rows the resumed id skips")


def main(argv: list[str] | None = None) -> int:
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
        configure_parser=_configure_parser,
        validate_args=_validate_args,
        select_adapter=lambda args, _mod: _select(args.projection)[0],
        caller_module=sys.modules[__name__],
        argv=argv,
    )


__all__ = [
    "AggregateBatchAdapter",
    "ClickHouseBatchAdapter",
    "ClickHouseClient",
    "ClickHousePublication",
    "PostgresBatchManifestStore",
    "PostgresControlPlane",
    "app_config",
    "get_high_scale_service_registry",
    "main",
    "register_high_scale_services_from_environment",
]


if __name__ == "__main__":
    raise SystemExit(main())
