"""Derived high-scale projections whose provenance remains request-owned."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from backend.high_scale.schema import build_cmcd_projection_key


def project_cmcd(request_event: Mapping[str, Any]) -> dict[str, Any] | None:
    raw_fields = request_event.get("cmcd")
    if raw_fields is None:
        return None
    if not isinstance(raw_fields, Mapping):
        raise ValueError("CMCD fields must be a mapping")
    request_event_id = request_event.get("event_id")
    if not isinstance(request_event_id, str) or not request_event_id:
        raise ValueError("CMCD projection requires request_event_id")
    service_id = request_event.get("service_id")
    if not isinstance(service_id, str) or not service_id:
        raise ValueError("CMCD projection requires service_id")
    projection = {
        "projection_key": build_cmcd_projection_key({"request_event_id": request_event_id}),
        "request_event_id": request_event_id,
        "service_id": service_id,
        "timestamp": request_event.get("timestamp"),
        "cmcd_version": request_event.get("cmcd_version"),
        "sid": request_event.get("sid"),
        "country": _country(request_event.get("country")),
        "asn": _asn(request_event.get("asn")),
        "fields": dict(raw_fields),
    }
    return projection


def _country(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _asn(value: object) -> int | None:
    """A positive AS number, or None: the analytics group by ASN, never by a malformed label."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip()) or None
    return None
