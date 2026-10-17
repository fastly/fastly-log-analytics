"""Client for the Content Security blocker (the Compute function that enforces policies).

The backend publishes a service's whole policy set with ``PUT <base>/v2/services/<id>/policy-set``
(see ``backend/core/content_security_publish.py``); ``GET`` reads it back and ``DELETE`` removes it.

An optional operator-configured header (e.g. ``Authorization: Bearer …``) is sent with every call.
It is a secret: never logged, never returned to the browser.

Any 2xx is success. Redirects are not followed: the admin configured this exact URL, and
following a 3xx would let the function bounce the call (and its body) somewhere else.
A 429/5xx is retried a couple of times, because the blocker's KV store limits writes to one per
second per key and publishes follow each other closely (several quick edits in a row).
"""

from __future__ import annotations

import logging
import re
import time
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

logger = logging.getLogger(__name__)

_TIMEOUT_S = 10.0

# Statuses worth a retry: rate limited, or a transient failure at the blocker.
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_ATTEMPTS = 3
_RETRY_DELAY_S = 1.2
_REASON_MAX = 200

# Test seam: tests replace this so retries do not sleep.
_sleep = time.sleep

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


def policy_set_url(url: str, service_id: str) -> str:
    """The v2 policy-set endpoint for ``service_id`` on the configured blocker.

    The configured URL is the blocker's base URL (a trailing ``/block`` from an old setting is
    tolerated). Any query string is dropped: the credential is the auth header.
    """
    parts = urlsplit(url)
    path = parts.path.rstrip("/")
    if path.endswith("/block"):
        path = path[: -len("/block")]
    return urlunsplit(
        (parts.scheme, parts.netloc, f"{path}/v2/services/{quote(service_id, safe='')}/policy-set", "", "")
    )


def _reason(resp: httpx.Response) -> str:
    """The blocker's own explanation (its JSON ``error`` / ``detail``), short and single-line.

    The blocker is the operator's own function and never returns credentials; control
    characters are still replaced so the text is safe to show and to log.
    """
    try:
        data = resp.json()
    except ValueError:
        return ""
    if not isinstance(data, dict):
        return ""
    text = " - ".join(str(data[k]) for k in ("error", "detail") if isinstance(data.get(k), str) and data[k])
    text = _CONTROL_RE.sub(" ", text).strip()[:_REASON_MAX]
    return f": {text}" if text else ""


def call_policy_set(
    url: str, service_id: str, method: str, body: dict | None = None, auth_header: str | None = None
) -> dict:
    """``PUT`` (publish), ``GET`` (read back) or ``DELETE`` the service's policy set; return the JSON reply.

    Raises :class:`BlocklistCallError` on transport failure or a non-2xx reply (after retrying a
    429/5xx). The error carries the blocker's reason when it gave one; only the host is logged.
    """
    target = policy_set_url(url, service_id)
    host = urlsplit(url).hostname or "?"
    headers = dict([parse_auth_header(auth_header)]) if auth_header else None
    resp: httpx.Response | None = None
    for attempt in range(_ATTEMPTS):
        try:
            with httpx.Client(timeout=_TIMEOUT_S, follow_redirects=False, transport=_TRANSPORT) as client:
                resp = client.request(method, target, json=body if body is not None else None, headers=headers)
        except httpx.HTTPError as exc:
            logger.warning("policy-set %s to %s failed: %s", method, host, type(exc).__name__)
            raise BlocklistCallError(f"Could not reach the blocklist endpoint ({type(exc).__name__})") from exc
        if resp.status_code not in _RETRY_STATUSES or attempt == _ATTEMPTS - 1:
            break
        logger.info("policy-set %s to %s returned HTTP %d; retrying", method, host, resp.status_code)
        _sleep(_RETRY_DELAY_S * (attempt + 1))
    assert resp is not None
    if not 200 <= resp.status_code < 300:
        logger.warning("policy-set %s to %s returned HTTP %d", method, host, resp.status_code)
        raise BlocklistCallError(f"Blocklist endpoint returned HTTP {resp.status_code}{_reason(resp)}")
    try:
        data = resp.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}
