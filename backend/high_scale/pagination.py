"""Signed keyset pagination contracts for high-scale raw queries."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

MAX_PAGE_SIZE = 500


@dataclass(frozen=True)
class KeysetCursor:
    timestamp: datetime
    event_id: str
    service_id: str
    domain: str

    def encode(self, secret: bytes) -> str:
        if not secret:
            raise ValueError("cursor signing secret is required")
        payload = json.dumps(
            {
                "timestamp": self.timestamp.astimezone(UTC).isoformat(),
                "event_id": self.event_id,
                "service_id": self.service_id,
                "domain": self.domain,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        signature = hmac.new(secret, payload, hashlib.sha256).digest()
        return f"{_b64(payload)}.{_b64(signature)}"

    @classmethod
    def decode(cls, token: str, secret: bytes) -> KeysetCursor:
        if not secret:
            raise ValueError("cursor signing secret is required")
        try:
            payload_token, signature_token = token.split(".", 1)
            payload = _unb64(payload_token)
            signature = _unb64(signature_token)
            expected = hmac.new(secret, payload, hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError("cursor signature mismatch")
            raw = json.loads(payload)
            timestamp = datetime.fromisoformat(raw["timestamp"]).astimezone(UTC)
            if not all(isinstance(raw[key], str) and raw[key] for key in ("event_id", "service_id", "domain")):
                raise ValueError("cursor fields are invalid")
            return cls(timestamp, raw["event_id"], raw["service_id"], raw["domain"])
        except (ValueError, KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid cursor") from exc


@dataclass(frozen=True)
class KeysetPage:
    rows: tuple[Mapping[str, Any], ...]
    next_cursor: str | None


def paginate_rows(
    rows: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]],
    *,
    limit: int,
    secret: bytes,
    service_id: str,
    domain: str,
    cursor: str | None = None,
) -> KeysetPage:
    if limit <= 0 or limit > MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
    decoded_cursor = KeysetCursor.decode(cursor, secret) if cursor else None
    if decoded_cursor and (decoded_cursor.service_id != service_id or decoded_cursor.domain != domain):
        raise ValueError("cursor does not belong to this service and domain")

    ordered = sorted(rows, key=lambda row: (_timestamp(row), str(row["event_id"])))
    if decoded_cursor:
        ordered = [
            row
            for row in ordered
            if (_timestamp(row), str(row["event_id"])) > (decoded_cursor.timestamp, decoded_cursor.event_id)
        ]
    page_rows = ordered[:limit]
    next_cursor = None
    if len(ordered) > limit:
        last = page_rows[-1]
        next_cursor = KeysetCursor(
            _timestamp(last),
            str(last["event_id"]),
            service_id,
            domain,
        ).encode(secret)
    return KeysetPage(tuple(page_rows), next_cursor)


def _timestamp(row: Mapping[str, Any]) -> datetime:
    value = row.get("timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value).astimezone(UTC)
    raise ValueError("row timestamp is required")


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
