"""Shared redaction helpers for text that may be exposed to analysts."""

from __future__ import annotations

import re
import threading

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
    """Remember cid salts so their literal values are scrubbed from later text."""
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
    """Strip absolute server paths and cid-salt material from analyst-visible text."""
    return ABS_PATH_RE.sub("<path>", scrub_secrets(str(msg)))
