"""Canonical event identity and stable serialization for high-scale records."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

IDENTITY_FIELDS = (
    "service_id",
    "domain",
    "source_object_key",
    "source_object_version",
    "line_ordinal",
    "transform_version",
)


def _identity_payload(row: Mapping[str, Any], *, domain: str | None = None) -> tuple[Any, ...]:
    values = dict(row)
    if domain is not None:
        values["domain"] = domain
    missing = [field for field in IDENTITY_FIELDS if field not in values]
    if missing:
        raise ValueError(f"event identity missing fields: {', '.join(missing)}")
    if not isinstance(values["line_ordinal"], int) or values["line_ordinal"] < 0:
        raise ValueError("line_ordinal must be a non-negative integer")
    if not all(
        isinstance(values[field], str) and values[field] for field in IDENTITY_FIELDS if field != "line_ordinal"
    ):
        raise ValueError("event identity string fields must be non-empty strings")
    return tuple(values[field] for field in IDENTITY_FIELDS)


def build_event_id(row: Mapping[str, Any], *, domain: str | None = None) -> str:
    payload = _identity_payload(row, domain=domain)
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def build_request_event_id(row: Mapping[str, Any]) -> str:
    return build_event_id(row, domain="request")


def build_rum_event_id(row: Mapping[str, Any], *, rum_kind: str) -> str:
    if rum_kind not in {"vitals", "errors"}:
        raise ValueError("rum_kind must be 'vitals' or 'errors'")
    return build_event_id(row, domain=f"rum:{rum_kind}")


def build_cmcd_projection_key(row: Mapping[str, Any]) -> str:
    request_event_id = row.get("request_event_id")
    if not isinstance(request_event_id, str) or not request_event_id:
        raise ValueError("CMCD projection requires request_event_id")
    return hashlib.sha256(json.dumps(("cmcd", request_event_id), separators=(",", ":")).encode()).hexdigest()
