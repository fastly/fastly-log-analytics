"""Shared HTTP client helpers for external calls."""

from __future__ import annotations

from typing import Any

import httpx

_DEFAULT_UA = "fastly-log-analysis/1.0"


def http_get_text(url: str, *, headers: dict[str, str] | None = None, timeout: float = 30.0) -> str:
    """Fetch a URL over HTTP(S) and return its decoded text body."""
    hdrs = {"User-Agent": _DEFAULT_UA}
    if headers:
        hdrs.update(headers)
    resp = httpx.get(url, headers=hdrs, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp.content.decode()


def make_client(
    timeout: float | httpx.Timeout | None = 5.0,
    *,
    verify: Any = None,
    transport: httpx.BaseTransport | None = None,
    **kwargs: Any,
) -> httpx.Client:
    """Create a configured ``httpx.Client`` with optional TLS verification and custom transport."""
    if verify is not None:
        kwargs["verify"] = verify
    if transport is not None:
        kwargs["transport"] = transport
    return httpx.Client(timeout=timeout, **kwargs)


__all__ = [
    "_DEFAULT_UA",
    "http_get_text",
    "make_client",
]
