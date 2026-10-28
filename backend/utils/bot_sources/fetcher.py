"""Remote feed fetching and atomic cache writer for bot sources."""

from __future__ import annotations

import json
import logging

from backend.utils.date_utils import iso_z_now
from backend.utils.http import http_get_text
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

        # Represent as a single standard entry for 100% backward compatibility
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
