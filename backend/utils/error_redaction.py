"""Redaction for error text that can reach an analyst-visible response body."""

from __future__ import annotations

import re
import secrets
import threading

from fastapi import Request

# NB: ``app`` is the prod container WORKDIR (backend/Dockerfile -> /app), so the real data root is
# /app/data/services/<sid>/... Only deploy-relevant Unix roots preceded by a non-alphanumeric char are
# stripped, so URL paths like https://example.com/var/foo are left alone.
ABS_PATH_RE = re.compile(r"(?<![A-Za-z0-9])/(?:srv|var|tmp|data|home|opt|usr|mnt|app)\b(?:/[A-Za-z0-9_./-]*)?")

# Fallback for a literal salt in SQL text when the value is not in the known-salt set.
CID_SALT_LITERAL_RE = re.compile(r"(sha256\(\s*CONCAT\(\s*')[^']*(')", re.IGNORECASE)

REDACTED = "[redacted]"
_MIN_SALT_LEN = 8
_known_salts: set[str] = set()
_salts_lock = threading.Lock()


def register_salt(salt: str | None) -> None:
    """Remember a cid salt so its literal value is scrubbed from any later text, however it is embedded."""
    if isinstance(salt, str) and len(salt) >= _MIN_SALT_LEN:
        with _salts_lock:
            _known_salts.add(salt)


def scrub_secrets(text: str) -> str:
    with _salts_lock:
        salts = sorted(_known_salts, key=len, reverse=True)
    for salt in salts:
        text = text.replace(salt, REDACTED)
    return CID_SALT_LITERAL_RE.sub(r"\1[redacted-salt]\2", text)


def redact_error_text(msg: object) -> str:
    """Strip absolute server paths and cid-salt material from text destined for an analyst."""
    return ABS_PATH_RE.sub("<path>", scrub_secrets(str(msg)))


def current_request_id(request: Request | None = None) -> str | None:
    """The access-log request id: from ``request.state`` when given, else the per-request attribution contextvar."""
    rid = getattr(getattr(request, "state", None), "request_id", None) if request is not None else None
    if not (isinstance(rid, str) and rid):
        from backend.core.query_attribution import current_attribution

        attr = current_attribution.get()
        rid = getattr(attr, "request_id", None)
    return rid if isinstance(rid, str) and rid else None


def correlation_id(request: Request | None = None) -> str:
    """The request id when one exists, else a fresh id (the caller must log it alongside the failure)."""
    return current_request_id(request) or secrets.token_hex(8)
