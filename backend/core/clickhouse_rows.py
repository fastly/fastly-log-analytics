"""Version-one canonical payload shared by export, replay and verification."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

MAX_DATASET_ROWS = 1_000_000
MAX_BATCH_ROWS = 10_000
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_ROW_BYTES = 64 * 1024
PAYLOAD_COLUMNS = ("source_identity", "row_ordinal", "timestamp", "country", "ip", "url", "conn_requests")


def utc(value: str | datetime) -> datetime:
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def canonical_row(row: dict[str, Any]) -> tuple:
    ordinal = row["row_ordinal"]
    if type(ordinal) is not int or not 0 <= ordinal < MAX_DATASET_ROWS:
        raise ValueError("invalid row ordinal")
    timestamp = utc(row["timestamp"])
    if not datetime(1970, 1, 1, tzinfo=UTC) <= timestamp < datetime(2262, 1, 1, tzinfo=UTC):
        raise ValueError("timestamp outside prototype range")
    values = [row["source_identity"], ordinal, timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")]
    if not isinstance(values[0], str):
        raise ValueError("invalid source identity")
    for name in ("country", "ip", "url"):
        value = row[name]
        if value is not None and not isinstance(value, str):
            raise ValueError("invalid nullable dimension")
        values.append(value)
    conn = row["conn_requests"]
    if conn is not None and (type(conn) is not int or not -(2**63) <= conn < 2**63):
        raise ValueError("invalid conn_requests")
    result = (*values, conn)
    if len(canonical_bytes(result)) > MAX_ROW_BYTES:
        raise ValueError("oversized row")
    return result


def canonical_bytes(row: tuple) -> bytes:
    return (json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest_rows(rows) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(canonical_bytes(row))
    return digest.hexdigest()


def build_batch_id(service_id: str, source_identity: str, content_hash: str) -> str:
    return hashlib.sha256(canonical_bytes((service_id, source_identity, content_hash))).hexdigest()
