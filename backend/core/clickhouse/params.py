"""ClickHouse parameter binding, type formatting, canonicalization, and payload validation."""

from __future__ import annotations

import hashlib
import math
from datetime import UTC, date, datetime
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import Any
from uuid import UUID

import orjson

from backend.core.clickhouse.constants import (
    _DDL_PASSWORD,
    _PARAM_ESCAPES,
    _PARAM_NAME,
    _QUERY_SETTINGS,
    MAX_BATCH_ROWS,
    MAX_DATASET_ROWS,
    MAX_ROW_BYTES,
    PAYLOAD_COLUMNS,
    Artifact,
)
from backend.utils.telemetry import ensure_utc, parse_iso_utc


def _redact_ddl_secrets(sql: str) -> str:
    """Debug telemetry keeps SQL text; user DDL carries the password, which must never be recorded."""
    return _DDL_PASSWORD.sub(r"\1'[REDACTED]'", sql)


def _date_value(value: datetime | date) -> str:
    if isinstance(value, datetime):
        # Naive datetimes are interpreted as UTC, matching the fact timestamp.
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return value.isoformat()


def _json_value(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return _date_value(value)
    if isinstance(value, (UUID, IPv4Address, IPv6Address, Decimal)):
        return str(value)
    raise ValueError("unsupported ClickHouse insert value type")


def _parameter(value: Any) -> str:
    """HTTP param_* values use ClickHouse's escaped-text scalar representation."""
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (datetime, date)):
        return _date_value(value)
    if isinstance(value, (int, float, Decimal)):
        if not math.isfinite(value):
            raise ValueError("ClickHouse parameters must be finite")
        return str(value)
    if isinstance(value, (str, UUID, IPv4Address, IPv6Address)):
        return str(value).translate(_PARAM_ESCAPES)
    raise ValueError("unsupported ClickHouse parameter type; use typed scalar parameters")


def utc(value: str | datetime) -> datetime:
    if isinstance(value, str):
        parsed = parse_iso_utc(value)
        if parsed is None:
            raise ValueError(f"Invalid ISO format string: {value!r}")
        return parsed
    return ensure_utc(value)


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
    for v in row:
        if isinstance(v, float) and not math.isfinite(v):
            raise ValueError("Out of range float values are not JSON compliant")
    return orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE)


def digest_rows(rows: Any) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(canonical_bytes(row))
    return digest.hexdigest()


def build_batch_id(service_id: str, source_identity: str, content_hash: str) -> str:
    return hashlib.sha256(canonical_bytes((service_id, source_identity, content_hash))).hexdigest()


def _validate_payload(artifact: Artifact, rows: tuple) -> None:
    if len(rows) != artifact.row_count or len(rows) > MAX_BATCH_ROWS:
        raise ValueError("artifact payload row count mismatch")
    for index, row in enumerate(rows):
        if (
            canonical_row(dict(zip(PAYLOAD_COLUMNS, row, strict=True))) != row
            or row[1] != artifact.ordinal_start + index
        ):
            raise ValueError("artifact payload ordinals or types mismatch")
    if digest_rows(rows) != artifact.canonical_digest:
        raise ValueError("artifact payload digest mismatch")


__all__ = [
    "_DDL_PASSWORD",
    "_PARAM_ESCAPES",
    "_PARAM_NAME",
    "_QUERY_SETTINGS",
    "_date_value",
    "_json_value",
    "_parameter",
    "_redact_ddl_secrets",
    "_validate_payload",
    "build_batch_id",
    "canonical_bytes",
    "canonical_row",
    "digest_rows",
    "utc",
]
