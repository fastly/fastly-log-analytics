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


def build_rum_event_id(row: Mapping[str, Any], *, rum_kind: str, sub_ordinal: int | None = None) -> str:
    if rum_kind not in {"vitals", "errors"}:
        raise ValueError("rum_kind must be 'vitals' or 'errors'")
    resolved_sub_ordinal = sub_ordinal if sub_ordinal is not None else row.get("sub_ordinal")
    domain_tag = (
        f"rum:{rum_kind}:{resolved_sub_ordinal}"
        if isinstance(resolved_sub_ordinal, int) and resolved_sub_ordinal > 0
        else f"rum:{rum_kind}"
    )
    return build_event_id(row, domain=domain_tag)


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


import logging
import re
from pathlib import Path

from backend.core.clickhouse_client import get_clickhouse_client

_SCHEMA_FILES = [
    "publication_schema.sql",
    "request_schema.sql",
    "rum_schema.sql",
    "cmcd_schema.sql",
    "rum_aggregate_schema.sql",
    "cmcd_aggregate_schema.sql",
    "origin_schema.sql",
    "network_schema.sql",
    "security_schema.sql",
    "performance_schema.sql",
]


def install_clickhouse_schema() -> None:
    """Install the idempotent ClickHouse schema required by high-scale workers."""
    client = get_clickhouse_client()
    if not client:
        return

    sql_root = Path(__file__).with_name("sql")

    for filename in _SCHEMA_FILES:
        path = sql_root / filename
        if not path.exists():
            continue

        content = path.read_text("utf-8")

        # Replace Replicated engines with local prototype ones
        content = re.sub(
            r"ReplicatedReplacingMergeTree\((.*?)\)\s*\('/clickhouse/[^']+',\s*'[^']+'\)",
            r"ReplacingMergeTree(\1)",
            content,
        )
        content = re.sub(r"ReplicatedMergeTree\('/clickhouse/[^']+',\s*'[^']+'\)", "MergeTree()", content)
        content = re.sub(
            r"ReplicatedReplacingMergeTree\(([^)]+)\)\n\s*\('[^']+',\s*'[^']+'\)", r"ReplacingMergeTree(\1)", content
        )

        for stmt in content.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    client.execute(stmt)
                except Exception as e:
                    logging.getLogger(__name__).warning("Failed to install schema %s: %s", filename, e)
