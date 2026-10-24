"""Rebuild bounded Security projections from immutable high-scale archives."""

from __future__ import annotations

import sys
from datetime import timedelta

from backend.high_scale.recovery import rebuild_security_projections_from_fos
from scripts.backfill_high_scale import (
    AggregateBatchAdapter,
    ClickHouseClient,
    ClickHousePublication,
    PostgresBatchManifestStore,
    PostgresControlPlane,
    app_config,
    get_high_scale_service_registry,
    register_high_scale_services_from_environment,
    run_backfill_cli,
)


def main(argv: list[str] | None = None) -> int:
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


__all__ = [
    "AggregateBatchAdapter",
    "ClickHouseClient",
    "ClickHousePublication",
    "PostgresBatchManifestStore",
    "PostgresControlPlane",
    "app_config",
    "get_high_scale_service_registry",
    "main",
    "rebuild_security_projections_from_fos",
    "register_high_scale_services_from_environment",
]


if __name__ == "__main__":
    raise SystemExit(main())
