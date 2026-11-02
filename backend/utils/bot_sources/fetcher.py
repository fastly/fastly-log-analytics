"""Remote feed fetching and atomic cache writer for bot sources."""

from __future__ import annotations

import ipaddress
import json
import logging
import socket
import urllib.parse

import httpx

from backend.utils.date_utils import iso_z_now
from backend.utils.http import _DEFAULT_UA
from backend.utils.telemetry import atomic_write_json

from .registry import (
    _CACHE_DIR,  # noqa: F401
    BOT_SOURCES,
    _cache_path,
    _compiled_ip_feeds,
    _compiled_ip_feeds_lock,
    _matcher_cache,
    _matcher_lock,
    _source_cache,
    _source_cache_lock,
    _version_cache,
    _version_cache_lock,
)

logger = logging.getLogger(__name__)

_MAX_FEED_BYTES = 10 * 1024 * 1024  # 10 MiB
_MAX_REDIRECTS = 5
_REDIRECT_STATUSES = {301, 302, 303, 307, 308}


def _is_disallowed_ip(ip_str: str) -> bool:
    ip = ipaddress.ip_address(ip_str)
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        elif ip.sixtofour is not None:
            ip = ip.sixtofour
        elif ip.teredo is not None:
            server_ip, client_ip = ip.teredo
            return _is_disallowed_ip(str(server_ip)) or _is_disallowed_ip(str(client_ip))
    return (
        not ip.is_global
        or ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_unspecified
        or ip.is_reserved
    )


def _validate_feed_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme.lower() != "https":
        raise ValueError(f"Disallowed bot feed URL scheme {parsed.scheme!r}; only 'https' is permitted")
    if parsed.username or parsed.password:
        raise ValueError("Bot feed URL must not contain credentials")
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("Bot feed URL is missing a hostname")
    if "%" in hostname or "%" in (parsed.netloc or ""):
        raise ValueError("Bot feed URL hostname must not contain zone identifiers or percent-encoding")
    port = parsed.port or 443

    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        if _is_disallowed_ip(hostname):
            raise ValueError(f"Bot feed URL points to disallowed IP address {hostname}")

    try:
        addr_infos = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ValueError(f"Failed to resolve bot feed hostname {hostname!r}: {exc}") from exc

    if not addr_infos:
        raise ValueError(f"No addresses resolved for bot feed hostname {hostname!r}")

    for _family, _type, _proto, _canonname, sockaddr in addr_infos:
        resolved_ip = str(sockaddr[0])
        if "%" in resolved_ip:
            resolved_ip = resolved_ip.split("%", 1)[0]
        if _is_disallowed_ip(resolved_ip):
            raise ValueError(f"Bot feed hostname {hostname!r} resolved to disallowed IP address {resolved_ip}")

    return url


def http_get_text(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
    max_bytes: int = _MAX_FEED_BYTES,
) -> str:
    """Fetch an HTTPS bot feed URL with SSRF validation on initial and redirect hops and bounded body read."""
    hdrs = {"User-Agent": _DEFAULT_UA}
    if headers:
        hdrs.update(headers)

    current_url = _validate_feed_url(url)
    with httpx.Client(timeout=timeout, follow_redirects=False, trust_env=False) as client:
        for _hop in range(_MAX_REDIRECTS + 1):
            with client.stream("GET", current_url, headers=hdrs) as resp:
                network_stream = resp.extensions.get("network_stream") if hasattr(resp, "extensions") else None
                if network_stream is not None and hasattr(network_stream, "get_extra_info"):
                    server_addr = network_stream.get_extra_info("server_addr")
                    if isinstance(server_addr, tuple) and server_addr:
                        peer_ip = str(server_addr[0]).split("%", 1)[0]
                        if _is_disallowed_ip(peer_ip):
                            raise ValueError(f"Bot feed connection peer resolved to disallowed IP address {peer_ip}")
                if resp.status_code in _REDIRECT_STATUSES:
                    location = resp.headers.get("location")
                    if not location:
                        raise ValueError(f"Redirect response {resp.status_code} missing Location header")
                    next_url = urllib.parse.urljoin(current_url, location)
                    current_url = _validate_feed_url(next_url)
                    continue

                resp.raise_for_status()
                content_length = resp.headers.get("content-length")
                if content_length is not None:
                    try:
                        parsed_len = int(content_length)
                    except ValueError:
                        parsed_len = 0
                    if parsed_len > max_bytes:
                        raise ValueError(
                            f"Bot feed response Content-Length ({parsed_len}) exceeds max_bytes ({max_bytes})"
                        )
                chunks: list[bytes] = []
                total = 0
                for chunk in resp.iter_bytes():
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError(f"Bot feed response body exceeded max_bytes ({max_bytes})")
                    chunks.append(chunk)
                raw_bytes = b"".join(chunks)
                encoding = resp.encoding or "utf-8"
                return raw_bytes.decode(encoding, errors="replace")

    raise ValueError(f"Too many redirects (>{_MAX_REDIRECTS}) fetching bot feed {url!r}")


def fetch_external_cidrs(source_config: dict) -> list[str]:
    """Fetch CIDR ranges from an external URL with an optional selector."""
    url = source_config.get("url")
    selector = source_config.get("selector")
    if not url:
        return []

    try:
        content = http_get_text(url, timeout=15)

        try:
            data = json.loads(content)

            # Supported: $.prefixes[*][\"ipv6Prefix\",\"ipv4Prefix\"] (Common for Googlebot)
            # We use a flexible check for the string since escaping can vary
            if selector and "prefixes" in selector and ("ipv4Prefix" in selector or "ipv6Prefix" in selector):
                prefixes = data.get("prefixes", [])
                res = []
                for p in prefixes:
                    if "ipv4Prefix" in p:
                        res.append(p["ipv4Prefix"])
                    if "ipv6Prefix" in p:
                        res.append(p["ipv6Prefix"])
                return res

            # Generic fallback for list of strings or top-level list
            if isinstance(data, list):
                return [str(x) for x in data if isinstance(x, (str, int))]
            if isinstance(data, dict) and "prefixes" in data and isinstance(data["prefixes"], list):
                return [str(x) for x in data["prefixes"] if isinstance(x, str)]
            if isinstance(data, dict) and "prefixes" in data and isinstance(data["prefixes"], dict):
                # Another common format
                return [str(x) for x in data["prefixes"].values() if isinstance(x, str)]

            return []
        except json.JSONDecodeError:
            # Fallback for plain-text IP lists (e.g. Cloudflare, Facebook)
            return [
                line.strip()
                for line in content.splitlines()
                if line.strip()
                and not line.startswith(("#", "//"))
                and ("/" in line or "." in line or ":" in line)  # basic IP/CIDR heuristic
            ]

    except Exception as e:
        logger.warning("[bot_sources] Failed to fetch external CIDRs from %s: %s", url, e)
        return []


def _atomic_write_cache(source_id: str, envelope: dict) -> None:
    """Atomically write the source cache envelope to disk using a temp file."""
    atomic_write_json(_cache_path(source_id), envelope, indent=None, separators=(",", ":"), fsync=True)


def fetch_and_cache_source(source_id: str) -> dict:
    """Fetch the source URL, write the cache file, return updated metadata."""
    src = next((s for s in BOT_SOURCES if s["id"] == source_id), None)
    if src is None:
        raise ValueError(f"Unknown bot source: {source_id!r}")

    raw_content = http_get_text(src["url"], timeout=30)

    if source_id == "well-known-bots":
        raw = json.loads(raw_content)
        # well-known-bots top-level is a list; other sources may differ
        entries = raw if isinstance(raw, list) else raw.get("bots", raw.get("entries", []))

        # Normalize Arcjet format to expected {"domains": [], "cidrs": []} format
        for entry in entries:
            v_list = entry.get("verification")
            if not isinstance(v_list, list):
                continue

            domains = []
            cidrs = []

            for v in v_list:
                v_type = v.get("type")
                if v_type == "dns":
                    for mask in v.get("masks", []):
                        domain = mask.split("*")[-1].lstrip(".").lstrip("@.")
                        if domain:
                            domains.append(domain)
                elif v_type == "cidr":
                    # Static prefixes
                    for p in v.get("prefixes", []):
                        cidrs.append(p)
                    # Dynamic sources
                    for s in v.get("sources", []):
                        cidrs.extend(fetch_external_cidrs(s))

            entry["verification"] = {"domains": list(set(domains)), "cidrs": list(set(cidrs))}
    else:
        # IP/threat feed: Tor, IPsum, Cloud range lists
        cidrs = []
        try:
            data = json.loads(raw_content)
            # AWS / GCP format: check for lists under prefixes key
            if isinstance(data, dict) and "prefixes" in data and isinstance(data["prefixes"], list):
                for p in data["prefixes"]:
                    if "ip_prefix" in p:
                        cidrs.append(p["ip_prefix"])
                    elif "ipv6_prefix" in p:
                        cidrs.append(p["ipv6_prefix"])
                    elif "ipv4Prefix" in p:
                        cidrs.append(p["ipv4Prefix"])
                    elif "ipv6Prefix" in p:
                        cidrs.append(p["ipv6Prefix"])
            elif isinstance(data, list):
                cidrs = [str(x) for x in data if isinstance(x, str)]
        except json.JSONDecodeError:
            # Plain-text format (Tor bulk exit node list, IPsum list)
            for line in raw_content.splitlines():
                line = line.strip()
                if not line or line.startswith(("#", "//")):
                    continue
                # For IPsum, format is "IP score" (e.g. "1.2.3.4 5"), take IP
                parts = line.split()
                if parts:
                    ip = parts[0]
                    if "/" in ip or "." in ip or ":" in ip:
                        cidrs.append(ip)

        # Represent as a single standard entry
        entries = [
            {
                "id": source_id,
                "name": src["name"],
                "pattern": {
                    "accepted": []  # No UA patterns since matched by IP
                },
                "verification": {"domains": [], "cidrs": list(set(cidrs))},
            }
        ]

    entry_count = len(cidrs) if src.get("type") == "ip" else len(entries)
    envelope = {
        "last_updated": iso_z_now(),
        "entry_count": entry_count,
        "entries": entries,
    }
    _atomic_write_cache(source_id, envelope)

    # Invalidate all in-process caches so next call rebuilds
    with _matcher_lock:
        _matcher_cache.clear()
    with _source_cache_lock:
        _source_cache.pop(source_id, None)
    with _compiled_ip_feeds_lock:
        _compiled_ip_feeds.clear()
    with _version_cache_lock:
        _version_cache.clear()

    logger.info("👾 \x1b[36m[bots]\x1b[0m Cached %d entries for %s", entry_count, source_id)
    return {
        "id": source_id,
        "name": src["name"],
        "last_updated": envelope["last_updated"],
        "entry_count": entry_count,
    }


def refresh_all_sources() -> list[dict]:
    """Fetch and cache all enabled sources. Returns updated metadata list."""
    results = []
    for src in BOT_SOURCES:
        if src["enabled"]:
            try:
                results.append(fetch_and_cache_source(src["id"]))
            except Exception as e:
                logger.error("[bot_sources] Failed to refresh %s: %s", src["id"], e)
                results.append(
                    {
                        "id": src["id"],
                        "name": src.get("name", src["id"]),
                        "error": str(e),
                        "entry_count": 0,
                        "failed": True,
                    }
                )
    return results


fetch_bot_source = fetch_and_cache_source
sync_all_bot_sources = refresh_all_sources
