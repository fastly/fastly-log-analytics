"""Canonical event identity and stable serialization for high-scale records."""

from __future__ import annotations

import hashlib
import uuid
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

    parts = []
    for field in IDENTITY_FIELDS:
        val = str(values[field])
        parts.append(f"{len(val)}:{val}")
    payload = "|".join(parts).encode("utf-8")
    hash_bytes = hashlib.sha256(payload).digest()[:16]
    return str(uuid.UUID(bytes=hash_bytes))


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
    payload = f"4:cmcd|{len(request_event_id)}:{request_event_id}".encode()
    hash_bytes = hashlib.sha256(payload).digest()[:16]
    return str(uuid.UUID(bytes=hash_bytes))


def build_batch_id_from_manifest_id(manifest_id: str) -> str:
    if not isinstance(manifest_id, str) or not manifest_id:
        raise ValueError("manifest_id must be a non-empty string")
    payload = f"8:manifest|{len(manifest_id)}:{manifest_id}".encode()
    hash_bytes = hashlib.sha256(payload).digest()[:16]
    return str(uuid.UUID(bytes=hash_bytes))
