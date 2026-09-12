"""Build explicit high-scale query bindings from the serving target."""

from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any, Protocol

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry import HighScaleService, HighScaleServiceRegistry


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
            )
        )


def register_high_scale_services_from_environment(
    registry: HighScaleServiceRegistry,
    *,
    client: ServingClient | None,
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
        raise ValueError("HIGH_SCALE_SERVICE_IDS must contain at least one service id")
    cursor_secret = os.getenv("HIGH_SCALE_CURSOR_SECRET", "").encode("utf-8")
    register_high_scale_services(
        registry,
        client=client,
        service_ids=service_ids,
        cursor_secret=cursor_secret,
        owner_epoch=int(os.getenv("HIGH_SCALE_OWNER_EPOCH", "1")),
    )
    return service_ids


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
