"""Shared HTTP helper for non-Fastly external fetches.

Fastly API traffic must route through :func:`backend.core.fastly.client.fastly`
(which pins ``api.fastly.com`` + ``Fastly-Key`` auth, retries, and telemetry).
This helper covers the *other* case — fetching arbitrary external URLs (bot
source registries, published CIDR feeds) where that client doesn't apply — so
app modules don't hand-roll a low-level stdlib HTTP client (banned by
``scripts/check_no_raw_urllib.sh``).
"""

from __future__ import annotations

from typing import Any

import httpx

_DEFAULT_UA = "fastly-log-analysis/1.0"


def http_get_text(url: str, *, headers: dict[str, str] | None = None, timeout: float = 30.0) -> str:
    """GET ``url`` and return the response body decoded as UTF-8 text.

    Follows redirects and raises on a 4xx/5xx status or a network error,
    matching the previous stdlib URL-open semantics (which raise on a bad
    status and follow redirects by default). ``.decode()`` is explicit UTF-8
    to match the old ``resp.read().decode()`` rather than httpx's
    header-driven charset guessing.
    """
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
    """Build the shared sync ``httpx.Client`` used by outbound HTTP call sites.

    One construction point so TLS verification (httpx's default certifi bundle
    unless ``verify`` overrides it) and the default timeout (httpx's 5 s, matching
    the pre-factory call sites) are uniform.
    ``transport`` is the injectable test seam. Callers own the lifecycle
    (use as a context manager). Never put credentials in the client defaults
    that would be logged; pass auth headers per request or via ``headers=``.
    """
    if verify is not None:
        kwargs["verify"] = verify
    if transport is not None:
        kwargs["transport"] = transport
    return httpx.Client(timeout=timeout, **kwargs)
