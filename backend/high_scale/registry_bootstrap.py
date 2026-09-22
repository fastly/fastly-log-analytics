"""Build explicit high-scale query bindings from the serving target."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any, Protocol

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.raw_query import ArchiveManifestCatalog
from backend.high_scale.registry import HighScaleService, HighScaleServiceRegistry

logger = logging.getLogger(__name__)


class ServingClient(Protocol):
    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...


_WATERMARK_TABLES = {
    "request": "request_facts",
    "rum_vitals": "rum_vitals_facts",
    "rum_errors": "rum_error_facts",
    "cmcd": "cmcd_projection_facts",
}


def register_high_scale_services(
    registry: HighScaleServiceRegistry,
    *,
    client: ServingClient,
    service_ids: Iterable[str],
    cursor_secret: bytes,
    owner_epoch: int = 1,
    manifest_catalog: ArchiveManifestCatalog | None = None,
    archive_for: Callable[[str], ArchivePublication | None] | None = None,
) -> None:
    if not cursor_secret:
        raise ValueError("high-scale cursor secret is required")
    if owner_epoch < 0:
        raise ValueError("high-scale owner epoch must be non-negative")

    for service_id in service_ids:
        if not service_id:
            raise ValueError("high-scale service id is required")
        watermarks = {
            domain: _watermark(client, service_id, domain, owner_epoch=owner_epoch) for domain in _WATERMARK_TABLES
        }
        # Cold-tier raw queries require BOTH a manifest catalog and an
        # archive reader for this specific service; a service missing
        # either gets neither, so the /queries endpoint reports it as
        # explicitly unconfigured rather than half-wiring a capability that
        # can never actually serve a cold-tier plan.
        archive = archive_for(service_id) if archive_for is not None else None
        registry.register(
            HighScaleService(
                service_id=service_id,
                client=client,
                cursor_secret=cursor_secret,
                request_watermark=watermarks["request"],
                rum_watermarks={
                    "rum_vitals": watermarks["rum_vitals"],
                    "rum_errors": watermarks["rum_errors"],
                },
                cmcd_watermark=watermarks["cmcd"],
                manifest_catalog=manifest_catalog if archive is not None else None,
                archive=archive,
            )
        )


def register_high_scale_services_from_environment(
    registry: HighScaleServiceRegistry,
    *,
    client: ServingClient | None,
    control_factory: Callable[[], ArchiveManifestCatalog] | None = None,
    archive_factory: Callable[[str], ArchivePublication | None] | None = None,
) -> tuple[str, ...]:
    enabled = os.getenv("HIGH_SCALE_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return ()
    if client is None:
        raise RuntimeError("high-scale mode requires a ClickHouse client")
    service_ids = tuple(
        service_id.strip() for service_id in os.getenv("HIGH_SCALE_SERVICE_IDS", "").split(",") if service_id.strip()
    )
    if not service_ids:
        from backend import config as app_config

        service_ids = tuple(
            cfg["service_id"] for cfg in app_config.list_configs() if isinstance(cfg, dict) and cfg.get("service_id")
        )
    if not service_ids:
        raise ValueError("HIGH_SCALE_SERVICE_IDS must contain at least one service id (none found in configs/)")
    cursor_secret = os.getenv("HIGH_SCALE_CURSOR_SECRET", "").encode("utf-8")

    # Cold-tier wiring is best-effort: the control plane is the same
    # Postgres dependency the isolated worker already hard-requires when
    # HIGH_SCALE_ENABLED=1 (see worker.py's build_worker_from_environment),
    # so a failure here is as fatal as it already is for the worker. A
    # per-service archive/FOS lookup failure is NOT fatal — it degrades
    # that one service to cold-tier-unconfigured (a 404, never a crash of
    # every other high-scale endpoint) via register_high_scale_services'
    # archive_for contract.
    manifest_catalog = (control_factory or _default_control_factory)()

    def _archive_for(service_id: str) -> ArchivePublication | None:
        try:
            return (archive_factory or _default_archive_factory)(service_id)
        except Exception:
            logger.warning("high-scale cold-tier archive wiring failed for %s", service_id, exc_info=True)
            return None

    register_high_scale_services(
        registry,
        client=client,
        service_ids=service_ids,
        cursor_secret=cursor_secret,
        owner_epoch=int(os.getenv("HIGH_SCALE_OWNER_EPOCH", "1")),
        manifest_catalog=manifest_catalog,
        archive_for=_archive_for,
    )
    return service_ids


def _default_control_factory() -> ArchiveManifestCatalog:
    from backend.high_scale.postgres_control import PostgresControlPlane

    return PostgresControlPlane()


def _default_archive_factory(service_id: str) -> ArchivePublication | None:
    from backend.core.duckdb import _get_fos_client, get_source_for_service
    from backend.high_scale.archive_publication import S3ObjectStore

    source = get_source_for_service(service_id)
    if source is None:
        return None
    bucket = source.get("bucket")
    if not isinstance(bucket, str) or not bucket:
        return None
    fos_client = _get_fos_client(source)
    archive_prefix = os.getenv("HIGH_SCALE_ARCHIVE_PREFIX", "high-scale/archive")
    return ArchivePublication(S3ObjectStore(fos_client, bucket=bucket, prefix=archive_prefix))


def _watermark(
    client: ServingClient,
    service_id: str,
    domain: str,
    *,
    owner_epoch: int,
) -> ServingWatermark:
    table = _WATERMARK_TABLES[domain]
    event_id_column = "projection_key" if domain == "cmcd" else "event_id"
    rows = client.execute(
        f"SELECT min(event_timestamp) AS coverage_start, "
        f"max(event_timestamp) AS coverage_end, "
        f"argMax(toString({event_id_column}), event_timestamp) "
        "AS last_visible_event_id "
        f"FROM {table} "
        "WHERE service_id={service_id:String} AND publication_state='visible'",
        {"service_id": service_id},
    )
    row = rows[0] if rows else {}
    return ServingWatermark(
        service_id=service_id,
        domain=domain,
        owner_epoch=owner_epoch,
        coverage_start=_datetime_or_none(row.get("coverage_start")),
        coverage_end=_datetime_or_none(row.get("coverage_end")),
        last_accepted_cursor=None,
        last_archived_event_id=None,
        last_visible_event_id=_string_or_none(row.get("last_visible_event_id")),
        exact=True,
    )


def _datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        normalized = value.replace(" ", "T", 1)
        if not normalized.endswith(("Z", "+00:00")):
            normalized += "+00:00"
        try:
            return datetime.fromisoformat(normalized).astimezone(UTC)
        except ValueError:
            return None
    return None


def _string_or_none(value: Any) -> str | None:
    return None if value is None else str(value)
