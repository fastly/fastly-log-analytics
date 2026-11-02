"""PII masking, secret scrubbing, and outbound debug request obfuscation."""

from __future__ import annotations

from backend.utils.auth import mask_ips_for
from backend.utils.telemetry import (
    _MIN_SALT_LEN,
    ABS_PATH_RE,
    CID_SALT_LITERAL_RE,
    REDACTED,
    _known_salts,
    _salts_lock,
    redact_error_text,
    register_salt,
    scrub_secrets,
)

_SENSITIVE_HEADERS = frozenset({"fastly-key", "authorization", "x-api-key", "x-api-token"})


def _obfuscate_header(name: str, value: str) -> str:
    if name.lower() in _SENSITIVE_HEADERS:
        return f"***{value[-4:]}" if len(value) >= 4 else "***"
    return value


def format_debug_request(
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    query: dict[str, str] | None = None,
) -> str:
    """Return a human-readable representation of an outbound request for debug output.

    Sensitive header values (API keys, auth tokens) are obfuscated.
    """
    lines = ["--- Request ---", f"{method} {url}"]
    if headers:
        for k, v in headers.items():
            lines.append(f"  {k}: {_obfuscate_header(k, v)}")
    if query:
        parts = [f"{k}={_obfuscate_header(k, v)}" for k, v in query.items()]
        lines.append(f"  QueryString: {'&'.join(parts)}")
    return "\n".join(lines)


__all__ = [
    "ABS_PATH_RE",
    "CID_SALT_LITERAL_RE",
    "REDACTED",
    "_MIN_SALT_LEN",
    "_SENSITIVE_HEADERS",
    "_known_salts",
    "_obfuscate_header",
    "_salts_lock",
    "format_debug_request",
    "mask_ips_for",
    "redact_error_text",
    "register_salt",
    "scrub_secrets",
]
