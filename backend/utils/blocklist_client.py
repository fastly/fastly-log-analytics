"""Client for the operator's content-security blocklist HTTP function.

Contract (configured in Admin → Overall Settings → Content Security):

- ``POST <url>`` with JSON ``{"conditions": [{"category": ..., "value": ...}]}``
  blocks requests matching ALL conditions (AND). Always an array, even for
  one condition, so receivers handle a single shape.
- ``DELETE <url>`` with the same JSON body reverts the block.

An optional operator-configured header (e.g. ``Authorization: Bearer …``)
is sent with both calls. It is a secret: never logged, never returned to the
browser.

Any 2xx is success. Redirects are not followed — the admin configured this
exact URL, and following a 3xx would let the function bounce the call (and
its body) somewhere else.
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urlsplit

import httpx

logger = logging.getLogger(__name__)

_TIMEOUT_S = 10.0

# Test seam: tests swap in an ``httpx.MockTransport``.
_TRANSPORT: httpx.BaseTransport | None = None


# RFC 9110 field-name token.
_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
# Headers the client must own: framing, routing and the JSON body.
_RESERVED_HEADERS = frozenset(
    {"host", "content-length", "content-type", "transfer-encoding", "connection", "upgrade", "te", "trailer"}
)
# Any control character except horizontal tab — CR/LF would inject headers.
_CONTROL_RE = re.compile(r"[\x00-\x08\x0a-\x1f\x7f]")


class BlocklistCallError(Exception):
    """The blocklist function was unreachable or returned a non-2xx status."""


def validate_blocklist_url(url: str) -> str:
    """Return the trimmed URL, or raise ValueError if it isn't an absolute http(s) URL."""
    url = url.strip()
    if not url:
        return ""
    if len(url) > 2048:
        raise ValueError("URL must be at most 2048 characters")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("URL must be an absolute http:// or https:// URL")
    if parts.username or parts.password:
        raise ValueError("URL must not embed credentials")
    return url


def parse_auth_header(raw: str) -> tuple[str, str]:
    """Split ``"Name: value"`` into a validated (name, value) pair, or raise ValueError.

    Error messages never echo the value — it is a credential.
    """
    if len(raw) > 4096:
        raise ValueError("Header must be at most 4096 characters")
    if _CONTROL_RE.search(raw):
        raise ValueError("Header must be a single line")
    name, sep, value = raw.partition(":")
    name, value = name.strip(), value.strip()
    if not sep or not name or not value:
        raise ValueError('Header must look like "Name: value", e.g. "Authorization: Bearer <token>"')
    if not _HEADER_NAME_RE.match(name):
        raise ValueError("Header name contains invalid characters")
    if name.lower() in _RESERVED_HEADERS:
        raise ValueError(f"The {name} header is set automatically and cannot be overridden")
    return name, value


def call_blocklist(url: str, method: str, conditions: list[dict], auth_header: str | None = None) -> int:
    """Send one block (``POST``) or unblock (``DELETE``) call; return the HTTP status.

    Raises :class:`BlocklistCallError` on transport failure or a non-2xx reply.
    Only the host is logged — the path or query may carry an operator token.
    """
    host = urlsplit(url).hostname or "?"
    try:
        with httpx.Client(timeout=_TIMEOUT_S, follow_redirects=False, transport=_TRANSPORT) as client:
            headers = dict([parse_auth_header(auth_header)]) if auth_header else None
            resp = client.request(method, url, json={"conditions": conditions}, headers=headers)
    except httpx.HTTPError as exc:
        logger.warning("blocklist %s to %s failed: %s", method, host, type(exc).__name__)
        raise BlocklistCallError(f"Could not reach the blocklist endpoint ({type(exc).__name__})") from exc
    if not 200 <= resp.status_code < 300:
        logger.warning("blocklist %s to %s returned HTTP %d", method, host, resp.status_code)
        raise BlocklistCallError(f"Blocklist endpoint returned HTTP {resp.status_code}")
    return resp.status_code
