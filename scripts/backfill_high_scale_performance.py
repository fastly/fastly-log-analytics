"""Backfill the Performance waterfall and scatter projections from immutable high-scale archives.

Run once after deploying the projection-writing worker. ``--until`` must be the moment
that worker started publishing: later manifests already wrote these projections live,
and replaying them would double-count the waterfall sums. Re-run with the same
``--rebuild-id`` to resume an interrupted attempt idempotently.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import UTC, datetime, timedelta

from backend.high_scale.recovery import rebuild_performance_projections_from_fos
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


def _utc_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--until must include a UTC offset, for example 2026-10-20T12:00:00Z")
    return parsed.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
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


__all__ = [
    "AggregateBatchAdapter",
    "ClickHouseClient",
    "ClickHousePublication",
    "PostgresBatchManifestStore",
    "PostgresControlPlane",
    "app_config",
    "get_high_scale_service_registry",
    "main",
    "rebuild_performance_projections_from_fos",
    "register_high_scale_services_from_environment",
]


if __name__ == "__main__":
    raise SystemExit(main())
