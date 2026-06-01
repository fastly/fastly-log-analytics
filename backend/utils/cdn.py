"""CDN URL construction for FOS-backed Fastly CDN services."""

from __future__ import annotations

import urllib.parse
import urllib.request


def build_cdn_url(cdn_base: str, key: str, secret: str = "") -> str:
    """Return a CDN URL for *key*, merging *secret* as a query param when provided.

    Correctly preserves any existing query params already on *cdn_base*.
    Uses urllib.parse throughout so special characters in *key* are always encoded.
    """
    # Parse the base FIRST so any existing query/fragment lives on
    # ``base_parts``, not on the concatenated path. The previous version
    # did ``f"{cdn_base}/{key}"`` then urlparse, which merged the key
    # into an existing ``?foo=bar`` query value when the base already
    # carried one.
    base_parts = urllib.parse.urlparse(cdn_base)
    base_path = base_parts.path.rstrip("/")
    safe_key = urllib.parse.quote(key, safe="/=")
    full_path = f"{base_path}/{safe_key}"

    query = urllib.parse.parse_qs(base_parts.query)
    if secret:
        query["key"] = [secret]

    return urllib.parse.urlunparse(base_parts._replace(path=full_path, query=urllib.parse.urlencode(query, doseq=True)))


def cdn_request(
    cdn_base: str,
    key: str,
    secret: str = "",
    use_header_auth: bool = False,
) -> urllib.request.Request:
    """Return a ``urllib.request.Request`` for *key* via CDN with auth applied.

    Args:
        cdn_base: CDN base URL from ``source["cdn_url"]``.
        key: FOS object key (no leading slash).
        secret: CDN auth secret from ``source["cdn_secret"]``.
        use_header_auth: When True, send the secret as an ``x-fastly-key`` request
            header instead of a ``?key=`` query parameter.  The VCL accepts both.
    """
    if use_header_auth and secret:
        req = urllib.request.Request(build_cdn_url(cdn_base, key))
        req.add_header("x-fastly-key", secret)
    else:
        req = urllib.request.Request(build_cdn_url(cdn_base, key, secret))
    return req
