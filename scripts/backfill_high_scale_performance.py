"""Backfill the Performance waterfall and scatter projections from immutable high-scale archives.

Run once after deploying the projection-writing worker. ``--until`` must be the moment
that worker started publishing: later manifests already wrote these projections live,
and replaying them would double-count the waterfall sums. Re-run with the same
``--rebuild-id`` to resume an interrupted attempt idempotently.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import cast

from backend import config as app_config
from backend.core.clickhouse_client import ClickHouseClient
from backend.high_scale.aggregate_batch_adapter import AggregateBatchAdapter
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.publication import ClickHousePublication, PostgresBatchManifestStore
from backend.high_scale.recovery import rebuild_performance_projections_from_fos
from backend.high_scale.registry import get_high_scale_service_registry
from backend.high_scale.registry_bootstrap import register_high_scale_services_from_environment


def _utc_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--until must include a UTC offset, for example 2026-10-20T12:00:00Z")
    return parsed.astimezone(UTC)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-id", required=True)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--until", type=_utc_instant, required=True, help="worker deploy time (ISO-8601 with offset)")
    parser.add_argument("--rebuild-id")
    args = parser.parse_args()
    if args.days <= 0:
        parser.error("--days must be positive")
    # Pin the id before any work: batch ids derive from it, so a killed run can only be resumed
    # idempotently (instead of double-counting what it already published) if the id was printed.
    rebuild_id = args.rebuild_id or uuid.uuid4().hex
    print(f"rebuild_id={rebuild_id}", file=sys.stderr, flush=True)

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
        report = rebuild_performance_projections_from_fos(
            service.manifest_catalog,
            service.archive,
            publication,
            service_id=args.service_id,
            now=args.until,
            window=timedelta(days=args.days),
            rebuild_id=rebuild_id,
        )
        print(json.dumps(asdict(report), sort_keys=True))
        return 0 if report.full_rebuild_complete else 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
