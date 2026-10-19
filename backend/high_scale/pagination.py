"""Signed keyset pagination contracts for high-scale raw queries."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime

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


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
