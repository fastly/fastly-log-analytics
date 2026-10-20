"""Rebuild bounded Origin projections from immutable high-scale archives."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from backend import config as app_config
from backend.core.clickhouse_client import ClickHouseClient
from backend.high_scale.aggregate_batch_adapter import AggregateBatchAdapter
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.publication import ClickHousePublication, PostgresBatchManifestStore
from backend.high_scale.recovery import purge_origin_projections, rebuild_origin_projections_from_fos
from backend.high_scale.recovery_capacity import RecoveryCapacityMeasurement
from backend.high_scale.registry import get_high_scale_service_registry
from backend.high_scale.registry_bootstrap import register_high_scale_services_from_environment


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-id", required=True)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--rebuild-id")
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="delete this service's existing Origin projection rows in the window first "
        "(required when rows already exist, otherwise every request is counted twice)",
    )
    parser.add_argument(
        "--capacity-json",
        type=Path,
        help="measured RecoveryCapacityMeasurement fields; exit 3 when the rebuild completes but the claim is invalid",
    )
    args = parser.parse_args()
    if args.days <= 0:
        parser.error("--days must be positive")
    if args.replace_existing and args.rebuild_id:
        parser.error("--replace-existing cannot resume a rebuild: the purge would delete rows the resumed id skips")
    capacity = None
    if args.capacity_json is not None:
        try:
            capacity = RecoveryCapacityMeasurement(**json.loads(args.capacity_json.read_text()))
            capacity.validate()
        except (OSError, TypeError, ValueError) as exc:
            parser.error(f"--capacity-json is invalid: {exc}")

    settings = app_config.load_clickhouse_config()
    if settings is None:
        raise RuntimeError("ClickHouse configuration is required")
    client = ClickHouseClient(settings)
    try:
        registry = get_high_scale_service_registry()
        register_high_scale_services_from_environment(registry, client=client)
        service = registry.resolve(args.service_id)
        if service is None or service.manifest_catalog is None or service.archive is None:
            raise RuntimeError("service is not registered with its archive dependencies")
        control = PostgresControlPlane()
        publication = ClickHousePublication(
            PostgresBatchManifestStore(control),
            AggregateBatchAdapter(cast(ClickHouseClient, service.client)),
        )
        if args.replace_existing:
            purge_origin_projections(
                service.client,
                service_id=args.service_id,
                since=datetime.now(UTC) - timedelta(days=args.days),
            )
        report = rebuild_origin_projections_from_fos(
            service.manifest_catalog,
            service.archive,
            publication,
            service_id=args.service_id,
            window=timedelta(days=args.days),
            rebuild_id=args.rebuild_id,
            capacity=capacity,
        )
        print(json.dumps(asdict(report), sort_keys=True))
        if not report.full_rebuild_complete:
            return 1
        return 3 if report.recovery_claim_valid is False else 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
