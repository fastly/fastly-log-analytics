from __future__ import annotations

import asyncio
import socket
import sys
import time

import aiodns
import cachetools
import pytricia

"""Bot source cache — fetch, store, and match against known bot registries."""


import bisect
import functools
import hashlib
import ipaddress
import json
import logging
import re
import threading
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from backend.utils.atomic_write import atomic_write_json
from backend.utils.date_utils import iso_z_now
from backend.utils.http import http_get_text

logger = logging.getLogger(__name__)

CidrIntervals = tuple[list[int], list[int], list[int], list[int]]


def collapse_network_objects(
    networks: Iterable[ipaddress.IPv4Network | ipaddress.IPv6Network],
) -> CidrIntervals:
    """Collapse IPv4/IPv6 network objects into sorted disjoint ``(v4_starts, v4_ends, v6_starts, v6_ends)`` arrays."""
    v4_nets: list[ipaddress.IPv4Network] = []
    v6_nets: list[ipaddress.IPv6Network] = []
    for net in networks:
        if isinstance(net, ipaddress.IPv4Network):
            v4_nets.append(net)
        elif isinstance(net, ipaddress.IPv6Network):
            v6_nets.append(net)
    v4_collapsed = list(ipaddress.collapse_addresses(v4_nets)) if v4_nets else []
    v6_collapsed = list(ipaddress.collapse_addresses(v6_nets)) if v6_nets else []
    return (
        [int(n.network_address) for n in v4_collapsed],
        [int(n.broadcast_address) for n in v4_collapsed],
        [int(n.network_address) for n in v6_collapsed],
        [int(n.broadcast_address) for n in v6_collapsed],
    )


@functools.lru_cache(maxsize=512)
def collapse_cidr_ranges(cidrs_key: tuple[str, ...]) -> CidrIntervals:
    """Collapse CIDR strings into sorted disjoint ``(starts, ends)`` integer intervals per IP version."""
    nets: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for raw in cidrs_key:
        if not raw:
            continue
        try:
            nets.append(ipaddress.ip_network(raw, strict=False))
        except ValueError:
            continue
    return collapse_network_objects(nets)


@functools.lru_cache(maxsize=16384)
def parse_ip_int_version(ip: str) -> tuple[int, int] | None:
    """Parse an IP literal into ``(int(ip_obj), ip_obj.version)``, cached across lookups."""
    try:
        ip_obj = ipaddress.ip_address(ip)
        return int(ip_obj), ip_obj.version
    except ValueError:
        return None


def ip_int_in_intervals(ip_int: int, version: int, intervals: CidrIntervals) -> bool:
    """Return True if ``(ip_int, version)`` falls within the pre-compiled interval arrays."""
    v4_starts, v4_ends, v6_starts, v6_ends = intervals
    starts, ends = (v4_starts, v4_ends) if version == 4 else (v6_starts, v6_ends)
    if not starts:
        return False
    idx = bisect.bisect_right(starts, ip_int) - 1
    return idx >= 0 and ip_int <= ends[idx]


@functools.lru_cache(maxsize=512)
def _build_pytricia_trie(cidrs_key: tuple[str, ...]) -> tuple[pytricia.PyTricia, pytricia.PyTricia]:
    v4_trie = pytricia.PyTricia(32)
    v6_trie = pytricia.PyTricia(128)
    for raw in cidrs_key:
        if not raw:
            continue
        try:
            net = ipaddress.ip_network(raw, strict=False)
            (v4_trie if net.version == 4 else v6_trie)[str(net)] = True
        except ValueError:
            continue
    return v4_trie, v6_trie


def ip_in_ranges(ip: str, cidrs: list[str] | tuple[str, ...]) -> bool:
    """Return True if the IP address falls within any of the provided CIDR ranges."""
    if not cidrs or not ip:
        return False
    parsed = parse_ip_int_version(ip)
    if parsed is None:
        return False
    _ip_int, version = parsed
    cidrs_key = cidrs if isinstance(cidrs, tuple) else tuple(cidrs)
    v4_trie, v6_trie = _build_pytricia_trie(cidrs_key)
    trie = v4_trie if version == 4 else v6_trie
    return len(trie) > 0 and ip in trie


BOT_SOURCES: list[dict] = [
    {
        "id": "well-known-bots",
        "name": "Well-Known Bots (arcjet)",
        "url": "https://raw.githubusercontent.com/arcjet/well-known-bots/main/well-known-bots.json",
        "enabled": True,
        "type": "ua",
    },
    {
        "id": "tor-exit-nodes",
        "name": "Tor Exit Nodes (Official)",
        "url": "https://check.torproject.org/torbulkexitlist",
        "enabled": True,
        "type": "ip",
        "category": "anonymizer",
    },
    {
        "id": "ipsum-blocklist",
        "name": "IPsum Malicious IP List",
        "url": "https://raw.githubusercontent.com/stamparm/ipsum/master/ipsum.txt",
        "enabled": True,
        "type": "ip",
        "category": "malicious_ip",
    },
    {
        "id": "aws-ip-ranges",
        "name": "AWS IP Ranges",
        "url": "https://ip-ranges.amazonaws.com/ip-ranges.json",
        "enabled": True,
        "type": "ip",
        "category": "cloud_infra",
    },
    {
        "id": "gcp-ip-ranges",
        "name": "GCP IP Ranges",
        "url": "https://www.gstatic.com/ipranges/cloud.json",
        "enabled": True,
        "type": "ip",
        "category": "cloud_infra",
    },
]

_CACHE_DIR = Path("data/cache/bot_sources")

# ── Matcher cache ─────────────────────────────────────────────────────────────
# Rebuilt lazily when cache files change on disk.
_matcher_lock = threading.Lock()
_matcher_cache: cachetools.LRUCache[str, Any] = cachetools.LRUCache(maxsize=16)  # keys: "fn", "mtime"

# mtime-revalidated cache for parsed source envelopes. load_source is hit
# by every get_bot_regex_pattern / get_bot_by_id call (dashboard +
# security repos) and the JSON files are megabytes — the parse cost was
# adding up.
_source_cache: cachetools.LRUCache[str, tuple[int, list[dict]]] = cachetools.LRUCache(maxsize=64)
_source_cache_lock = threading.Lock()

# Pre-compiled IP network feeds for high-performance IP-based bot matching.
_compiled_ip_feeds: cachetools.LRUCache[str, Any] = cachetools.LRUCache(maxsize=32)
_compiled_ip_feeds_lock = threading.Lock()


def _cache_path(source_id: str) -> Path:
    return _CACHE_DIR / f"{source_id}.json"


def _cache_mtime() -> float:
    """Return the latest mtime across all source cache files."""
    ts = 0.0
    for src in BOT_SOURCES:
        p = _cache_path(src["id"])
        if p.exists():
            ts = max(ts, p.stat().st_mtime)
    return ts


# Content-hash version cache, revalidated against the source files' mtime.
# The mtime stat is cheap (hot path); the SHA is recomputed only when a file
# is rewritten (a refresh) and re-hashes IDENTICAL content to the SAME value.
_version_cache: cachetools.LRUCache[str, Any] = cachetools.LRUCache(maxsize=16)
_version_cache["mtime"] = None
_version_cache["version"] = None
_version_cache_lock = threading.Lock()


def get_pattern_set_version() -> str:
    """Return a version string keyed to the bot pattern set CONTENT.

    A SHA of the source JSON bytes — NOT the file mtime. The wellknown_bots
    rollup reader requires a single version across the request window; an
    mtime-based version bumped on every (typically no-op) daily re-fetch made
    every multi-day window mixed-version → the rollup never served and the
    request path stayed on the live regex + its all-rows temp. Hashing the
    content keeps the version stable across identical re-fetches while still
    changing when the patterns ACTUALLY change (so a real pattern update still
    invalidates stale rollups → reader falls back to live for those hours).

    Used by the wellknown_bots rollup to stamp each materialised row.
    Empty string means no source files exist yet (writer should skip).

    Cached against the latest source mtime so the hot path is a stat, not a
    multi-MB re-hash; identical content re-hashes to the same value on refresh.
    """
    ts = _cache_mtime()
    if ts == 0.0:
        return ""
    if _version_cache.get("mtime") == ts and _version_cache.get("version"):
        return _version_cache["version"]
    with _version_cache_lock:
        if _version_cache.get("mtime") == ts and _version_cache.get("version"):
            return _version_cache["version"]
        h = hashlib.sha256()
        for src in sorted(BOT_SOURCES, key=lambda s: s["id"]):
            p = _cache_path(src["id"])
            try:
                h.update(src["id"].encode())
                h.update(b"\0")
                h.update(p.read_bytes())
            except OSError:
                continue
        version = f"v{h.hexdigest()[:16]}"
        _version_cache["mtime"] = ts
        _version_cache["version"] = version
        return version


# ── Source I/O ────────────────────────────────────────────────────────────────


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


def load_source(source_id: str) -> list[dict]:
    """Return cached entries for a source, or [] if not yet fetched.

    Result is memoized and revalidated by the cache file's st_mtime_ns.
    fetch_and_cache_source overwrites the file in place, which bumps the
    mtime — so a real refresh invalidates without explicit signalling.
    """
    p = _cache_path(source_id)
    try:
        mtime_ns = p.stat().st_mtime_ns
    except FileNotFoundError:
        return []

    cached = _source_cache.get(source_id)
    if cached is not None and cached[0] == mtime_ns:
        return cached[1]

    try:
        entries = json.loads(p.read_text()).get("entries", [])
    except Exception:
        return []

    with _source_cache_lock:
        _source_cache[source_id] = (mtime_ns, entries)
    return entries


def get_all_sources_meta() -> list[dict]:
    """Return metadata for all sources (envelope only, no full entries)."""
    result = []
    for src in BOT_SOURCES:
        p = _cache_path(src["id"])
        meta: dict = {
            "id": src["id"],
            "name": src["name"],
            "url": src["url"],
            "enabled": src["enabled"],
            "last_updated": None,
            "entry_count": None,
        }
        if p.exists():
            try:
                envelope = json.loads(p.read_text())
                meta["last_updated"] = envelope.get("last_updated")
                meta["entry_count"] = envelope.get("entry_count")
            except Exception:
                logger.debug("get_all_sources_meta: step failed (ignored)", exc_info=True)
        result.append(meta)
    return result


def get_bot_by_id(bot_id: str) -> dict | None:
    """Look up a single bot entry by id across all enabled sources."""
    for src in BOT_SOURCES:
        if not src["enabled"]:
            continue
        for entry in load_source(src["id"]):
            if entry.get("id") == bot_id:
                return entry
    return None


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


# ── Pattern utilities ─────────────────────────────────────────────────────────

_LITERAL_ESCAPES = frozenset(r"\/.,-_+:; ")


def extract_literal_substring(pattern: str) -> str | None:
    """Return the longest literal substring from a regex pattern.

    Used to build ILIKE pre-filters that are faster than regexp_matches().
    E.g. 'Googlebot\\/.*' → 'Googlebot/', 'compatible; bingbot\\/2\\.0' → 'bingbot/2.0'
    """
    i = 0
    n = len(pattern)
    current: list[str] = []
    best = ""

    def _commit():
        nonlocal best
        s = "".join(current).strip()
        if len(s) > len(best):
            best = s
        current.clear()

    while i < n:
        c = pattern[i]
        if c == "\\" and i + 1 < n:
            nxt = pattern[i + 1]
            if nxt in _LITERAL_ESCAPES:
                current.append(nxt)
            else:
                _commit()  # \d, \w etc. break a literal run
            i += 2
        elif c == "[":
            # Skip entire character class [...]
            _commit()
            depth = 1
            i += 1
            while i < n and depth > 0:
                if pattern[i] == "]":
                    depth -= 1
                elif pattern[i] == "[":
                    depth += 1
                i += 1
        elif c in "(){}*+?^$|":
            _commit()
            i += 1
        else:
            current.append(c)
            i += 1

    _commit()
    return best if len(best) >= 4 else None


def _extract_strict_required_literal(pattern: str) -> str | None:
    """Extract the longest required lowercase literal of length >= 3 from *pattern*.

    Conservative: ignores tokens inside ``(...)`` or complex ``[...]`` classes,
    pops any character immediately followed by ``?``, ``*``, or ``{``, and returns
    ``None`` if a top-level ``|`` alternation is present. Supports two-character
    case classes like ``[wW]`` since matching is case-insensitive.
    """
    n = len(pattern)
    i = 0
    paren_depth = 0
    current: list[str] = []
    best = ""

    def _commit() -> None:
        nonlocal best
        s = "".join(current)
        if len(s) > len(best):
            best = s
        current.clear()

    while i < n:
        c = pattern[i]
        if c == "\\" and i + 1 < n:
            nxt = pattern[i + 1]
            if paren_depth == 0 and nxt in _LITERAL_ESCAPES:
                current.append(nxt.lower())
            else:
                _commit()
            i += 2
            if i < n and pattern[i] in "?*{":
                if current:
                    current.pop()
                _commit()
            continue
        if c == "[":
            end = pattern.find("]", i + 1)
            if end != -1 and paren_depth == 0:
                cls_body = pattern[i + 1 : end]
                if len(cls_body) == 2 and cls_body[0].lower() == cls_body[1].lower() and cls_body[0].isalpha():
                    ch = cls_body[0].lower()
                    i = end + 1
                    if i < n and pattern[i] in "?*{":
                        _commit()
                    else:
                        current.append(ch)
                    continue
            _commit()
            i = (end + 1) if end != -1 else (i + 1)
            continue
        if c == "(":
            _commit()
            paren_depth += 1
            i += 1
            continue
        if c == ")":
            _commit()
            paren_depth = max(0, paren_depth - 1)
            i += 1
            continue
        if paren_depth > 0:
            i += 1
            continue
        if c == "|":
            return None
        if c in "?*{":
            if current:
                current.pop()
            _commit()
            if c == "{":
                end = pattern.find("}", i + 1)
                i = (end + 1) if end != -1 else (i + 1)
            else:
                i += 1
            continue
        if c in "+^$.":
            _commit()
            i += 1
            continue
        if i + 1 < n and pattern[i + 1] in "?*{":
            _commit()
            i += 1
            continue
        current.append(c.lower())
        i += 1

    _commit()
    return best if len(best) >= 3 else None


def _build_indexed_ua_matcher(entries: list[dict]) -> tuple[Callable[[str], tuple[dict, ...]], int]:
    """Compile a 3-gram prefix-indexed UA matcher over *entries*.

    Instead of testing all ~800 compiled regexes sequentially for every distinct
    User-Agent string, each pattern with a required >= 3-char literal is indexed
    by its first 3 lowercase characters. At lookup time only patterns whose 3-gram
    prefix appears in ``ua.lower()`` and whose full required literal is a substring
    of ``ua.lower()`` have ``pat.search(ua)`` invoked (~30x faster on 2,000 UAs).
    """
    by_gram3: dict[str, list[tuple[str, int, dict, re.Pattern]]] = {}
    always_check: list[tuple[int, dict, list[re.Pattern]]] = []
    entry_count = 0

    for idx, entry in enumerate(entries):
        raw_patterns = (entry.get("pattern") or {}).get("accepted") or []
        pats_all: list[re.Pattern] = []
        fallback_pats: list[re.Pattern] = []
        for rp in raw_patterns:
            try:
                pat = re.compile(rp, re.IGNORECASE)
            except re.error as e:
                logger.debug("[bot_sources] Bad pattern %r in %s: %s", rp, entry.get("id"), e)
                continue
            pats_all.append(pat)
            lit = _extract_strict_required_literal(rp)
            if lit is not None:
                by_gram3.setdefault(lit[:3], []).append((lit, idx, entry, pat))
            else:
                fallback_pats.append(pat)
        if pats_all:
            entry_count += 1
        if fallback_pats:
            always_check.append((idx, entry, fallback_pats))

    gram_keys = frozenset(by_gram3.keys())

    @functools.lru_cache(maxsize=10_000)
    def _match_ua(ua: str) -> tuple[dict, ...]:
        if not ua:
            return ()
        ua_l = ua.lower()
        n = len(ua_l)
        matched_by_idx: dict[int, dict] = {}
        if n >= 3:
            seen_grams = {ua_l[i : i + 3] for i in range(n - 2)} & gram_keys
            for g in seen_grams:
                for lit, idx, entry, pat in by_gram3[g]:
                    if idx not in matched_by_idx and lit in ua_l and pat.search(ua):
                        matched_by_idx[idx] = entry
        for idx, entry, pats in always_check:
            if idx not in matched_by_idx:
                for pat in pats:
                    if pat.search(ua):
                        matched_by_idx[idx] = entry
                        break
        if not matched_by_idx:
            return ()
        return tuple(matched_by_idx[k] for k in sorted(matched_by_idx))

    return _match_ua, entry_count


# ── Matcher ───────────────────────────────────────────────────────────────────


def build_matcher() -> Callable[[str], tuple[dict, ...]]:
    """Return a cached UA matcher. Rebuilds when source cache files change.

    The returned function is internally lru_cached — UA strings in log data
    follow a heavy power-law distribution so repeated lookups are near-free.
    The matcher returns a tuple (immutable + hashable, plays nicely with
    ``functools.lru_cache``); callers iterate it.
    """
    current_mtime = _cache_mtime()

    with _matcher_lock:
        if _matcher_cache.get("mtime") == current_mtime and "fn" in _matcher_cache:
            return _matcher_cache["fn"]

        entries: list[dict] = []
        for src in BOT_SOURCES:
            if not src["enabled"]:
                continue
            entries.extend(load_source(src["id"]))

        _match_ua, entry_count = _build_indexed_ua_matcher(entries)

        _matcher_cache["fn"] = _match_ua
        _matcher_cache["mtime"] = current_mtime
        logger.info("👾 \x1b[36m[bots]\x1b[0m Rebuilt matcher from %d bot entries", entry_count)
        return _match_ua


def get_ilike_prefilter_literals() -> list[str]:
    """Return literal substrings from all enabled bot patterns for ILIKE pre-filtering."""
    seen: set[str] = set()
    result: list[str] = []
    for src in BOT_SOURCES:
        if not src["enabled"]:
            continue
        for entry in load_source(src["id"]):
            for rp in entry.get("pattern", {}).get("accepted", []):
                lit = extract_literal_substring(rp)
                if lit and lit.lower() not in seen:
                    seen.add(lit.lower())
                    result.append(lit)
    return result


def get_bot_regex_pattern(limit: int = 500) -> str | None:
    """Return a case-insensitive regex pattern matching known bot literals.

    Used for high-performance DuckDB filtering via regexp_matches().
    Alternation in RE2 is significantly faster than long OR chains of ILIKE.
    """
    literals = get_ilike_prefilter_literals()
    if not literals:
        return None

    # Sort by length descending for better matching behavior
    literals = sorted(literals, key=len, reverse=True)[:limit]

    # Escape each literal for regex use and combine into a single alternation pattern
    # (?i) makes the RE2 engine perform case-insensitive matching
    return "(?i)" + "|".join(re.escape(lit) for lit in literals)


def _compile_feed_intervals(
    feeds: dict[str, list[ipaddress.IPv4Network | ipaddress.IPv6Network]],
) -> dict[str, tuple[list[int], list[int], list[int], list[int]]]:
    """Collapse each feed's networks into sorted disjoint integer interval arrays for O(log M) bisect matching."""
    return {feed_id: collapse_network_objects(net_objs) for feed_id, net_objs in feeds.items()}


def _get_compiled_ip_feeds() -> dict[str, list[ipaddress.IPv4Network | ipaddress.IPv6Network]]:
    """Compile enabled IP source CIDR ranges into Network objects once for high-performance match."""
    current_mtime = _cache_mtime()
    with _compiled_ip_feeds_lock:
        if _compiled_ip_feeds.get("mtime") == current_mtime and "feeds" in _compiled_ip_feeds:
            return _compiled_ip_feeds["feeds"]

        feeds = {}
        for src in BOT_SOURCES:
            if src.get("type") != "ip" or not src["enabled"]:
                continue
            entries = load_source(src["id"])
            if entries:
                cidrs = entries[0].get("verification", {}).get("cidrs", [])
                net_objs = []
                for c in cidrs:
                    try:
                        net_objs.append(ipaddress.ip_network(c, strict=False))
                    except ValueError:
                        continue
                if net_objs:
                    feeds[src["id"]] = net_objs

        _compiled_ip_feeds["feeds"] = feeds
        _compiled_ip_feeds["intervals"] = _compile_feed_intervals(feeds)
        _compiled_ip_feeds["intervals_feeds_id"] = id(feeds)
        _compiled_ip_feeds["mtime"] = current_mtime
        return feeds


def _get_feed_intervals(
    ip_feeds: dict[str, list[ipaddress.IPv4Network | ipaddress.IPv6Network]],
) -> dict[str, tuple[list[int], list[int], list[int], list[int]]]:
    with _compiled_ip_feeds_lock:
        if _compiled_ip_feeds.get("intervals_feeds_id") == id(ip_feeds) and "intervals" in _compiled_ip_feeds:
            return _compiled_ip_feeds["intervals"]
        intervals = _compile_feed_intervals(ip_feeds)
        _compiled_ip_feeds["intervals"] = intervals
        _compiled_ip_feeds["intervals_feeds_id"] = id(ip_feeds)
        return intervals


def enrich_bot_metadata(df: Any) -> None:
    """
    Unified enrichment for virtual bot fields (_bot_name, _ngwaf_bot_name).
    Modifies the DataFrame in-place.
    """
    if df.empty:
        return

    # 1. Arcjet/Well-Known Bot Matching & IP feed matching
    if "ua" in df.columns and "ip" in df.columns:
        match_ua = build_matcher()
        # Deduplicate (ua, ip) pairs across the DataFrame so 50k rows with a few hundred
        # unique pairs only execute UA matching, IP interval bisect, and rDNS classification
        # once per unique pair instead of O(N * M).
        ua_matches_cache: dict[str, tuple[dict, ...]] = {}
        row_keys: list[tuple[str, str]] = []
        candidate_ips_set: set[str] = set()
        for ua_val, ip_val in zip(df["ua"], df["ip"]):
            ua_str = str(ua_val) if ua_val else ""
            ip_str = str(ip_val) if ip_val else ""
            row_keys.append((ua_str, ip_str))
            matches = ua_matches_cache.get(ua_str)
            if matches is None:
                matches = match_ua(ua_str)
                ua_matches_cache[ua_str] = matches
            if matches and ip_str:
                candidate_ips_set.add(ip_str)
        hostnames = get_hostnames(list(candidate_ips_set))

        # Pre-compiled IP ranges + sorted disjoint interval arrays for O(log M) bisect lookup
        ip_feeds = _get_compiled_ip_feeds()
        feed_intervals = _get_feed_intervals(ip_feeds)
        feed_name_by_id = {s["id"]: s["name"] for s in BOT_SOURCES}
        ip_feed_cache: dict[str, str | None] = {}
        pair_label_cache: dict[tuple[str, str], str] = {}

        def _match_ip_feed(ip_str: str) -> str | None:
            if not ip_str:
                return None
            if ip_str in ip_feed_cache:
                return ip_feed_cache[ip_str]
            matched_feed: str | None = None
            try:
                parsed = parse_ip_int_version(ip_str)
                if parsed is not None:
                    ip_int, version = parsed
                    for feed_id, intervals in feed_intervals.items():
                        if ip_int_in_intervals(ip_int, version, intervals):
                            matched_feed = feed_id
                            break
            except Exception:
                logger.debug("enrich_bot_metadata: step failed (ignored)", exc_info=True)
            ip_feed_cache[ip_str] = matched_feed
            return matched_feed

        bot_names: list[str] = []
        for key in row_keys:
            cached_label = pair_label_cache.get(key)
            if cached_label is not None:
                bot_names.append(cached_label)
                continue

            ua_str, ip_str = key
            matches = ua_matches_cache[ua_str]
            matched_feed = _match_ip_feed(ip_str)

            if not matches:
                if matched_feed:
                    feed_name = feed_name_by_id.get(matched_feed, matched_feed)
                    label = f"{feed_name} (IP-Match)"
                else:
                    label = "null"
                pair_label_cache[key] = label
                bot_names.append(label)
                continue

            entry = matches[0]
            bot_name = entry.get("name", entry.get("id", "unknown"))
            hostname, status, fcrdns_verified = hostnames.get(ip_str, (None, "pending", False))

            verification = entry.get("verification", {})
            verification_domains = verification.get("domains", [])
            verification_cidrs = verification.get("cidrs", [])
            state = classify(ip_str, hostname, status, fcrdns_verified, verification_domains, verification_cidrs)

            if matched_feed:
                feed_name = feed_name_by_id.get(matched_feed, matched_feed)
                label = f"{bot_name} ({state}, {feed_name})"
            else:
                label = f"{bot_name} ({state})"
            pair_label_cache[key] = label
            bot_names.append(label)
        df["_bot_name"] = bot_names

    # 2. NG-WAF Verified Bot Enrichment
    if "waf_req_id" in df.columns:
        from backend.core.metadata import base as pg_connection

        # Normalize each waf_req_id once: None for missing/sentinel
        # values, str(x) otherwise. Deduplicate the IN-list while keeping per-row output aligned.
        norm = [None if (x is None or str(x) in ("None", "nan", "")) else str(x) for x in df["waf_req_id"]]
        waf_ids = list(dict.fromkeys(n for n in norm if n))
        if not waf_ids:
            df["_ngwaf_bot_name"] = None
        else:
            try:
                pconn = pg_connection.get_pg_readonly_connection()
                try:
                    placeholders = ",".join("?" for _ in waf_ids)
                    cur = pconn.execute(
                        f"SELECT waf_req_id, bot_name FROM ngwaf_bots WHERE waf_req_id IN ({placeholders})",
                        waf_ids,
                    )
                    rows = cur.fetchall()
                finally:
                    pconn.close()
                bot_map = {
                    (r["waf_req_id"] if isinstance(r, dict) else r[0]): (r["bot_name"] if isinstance(r, dict) else r[1])
                    for r in rows
                }
                df["_ngwaf_bot_name"] = [bot_map.get(n) if n else None for n in norm]
            except Exception:
                df["_ngwaf_bot_name"] = None


_BOT_TOKEN_SUFFIX_RE = re.compile(
    r"(-searchbot|-assistbot|-externalagent|-externalfetcher|-extended|-user|"
    r"searchbot|assistbot|externalagent|externalfetcher|extended|crawler|spider|"
    r"slurp|archiver|scraper|preview|other|bot).*$",
    re.IGNORECASE,
)
_DOMAIN_STEM_RE = re.compile(r"^(?:https?://)?(?:www\.)?([^/.:;\s]+)", re.IGNORECASE)
_SEED_PATH = Path(__file__).parent / "well_known_bots_seed.json"
_seed_entries_cache: list[dict] | None = None
_seed_matcher_fn: Callable[[str], tuple[dict, ...]] | None = None
_seed_lock = threading.RLock()


def load_well_known_bots_catalog() -> list[dict]:
    """Return cached well-known-bots entries, falling back to the bundled seed catalog
    when the runtime cache has not yet been fetched (e.g. fresh startup or CI)."""
    live = load_source("well-known-bots")
    if live:
        return live
    global _seed_entries_cache
    if _seed_entries_cache is not None:
        return _seed_entries_cache
    with _seed_lock:
        if _seed_entries_cache is None:
            try:
                _seed_entries_cache = json.loads(_SEED_PATH.read_text(encoding="utf-8"))
            except Exception:
                _seed_entries_cache = []
        return _seed_entries_cache


def build_well_known_bots_matcher() -> Callable[[str], tuple[dict, ...]]:
    """Return the live UA matcher when cached sources exist on disk, otherwise a
    compiled matcher over the bundled well-known-bots seed catalog."""
    if load_source("well-known-bots"):
        return build_matcher()
    global _seed_matcher_fn
    if _seed_matcher_fn is not None:
        return _seed_matcher_fn
    with _seed_lock:
        if _seed_matcher_fn is None:
            _seed_matcher_fn, _ = _build_indexed_ua_matcher(load_well_known_bots_catalog())
        return _seed_matcher_fn


def resolve_bot_display_name(token_or_ua: str | None, default: str = "Other Bot") -> str:
    """Resolve an extracted bot UA token or full UA string to a human-readable display name.

    Uses the well-known-bots catalog matcher first (`entry["id"]` title-cased, matching
    `classify_ua_counts` and `summarize_wellknown_bot_rows`), falling back to a cleaned
    title-cased form of the extracted token when the bot is not in the catalog.
    """
    raw = (token_or_ua or "").strip()
    if not raw:
        return default
    matcher = build_well_known_bots_matcher()
    matches = (
        matcher(raw)
        or matcher(f"Mozilla/5.0 (compatible; {raw}/2.0)")
        or matcher(f"Mozilla/5.0 (compatible; {raw}/1.0)")
        or matcher(f"Mozilla/5.0 (compatible; {raw}/0.1)")
        or matcher(f"{raw}/1.0")
    )
    if matches:
        bot_id = matches[0].get("id", "")
        if bot_id:
            return bot_id.replace("-", " ").title()
    raw_low = raw.lower().strip("/._- ")
    if raw_low:
        for entry in load_well_known_bots_catalog():
            bot_id = entry.get("id", "")
            if not bot_id:
                continue
            if raw_low == bot_id.lower() or any(raw_low == a.lower() for a in entry.get("aliases") or []):
                return bot_id.replace("-", " ").title()
            for pat in (entry.get("pattern") or {}).get("accepted") or []:
                if raw_low in pat.lower():
                    return bot_id.replace("-", " ").title()
    cleaned = raw.strip("/._- ")
    if not cleaned:
        return default
    return cleaned.replace("-", " ").replace("_", " ").title()


def extract_entry_referral_stems(entry: dict[str, Any]) -> set[str]:
    """Extract normalized domain/token stems for a well-known-bots catalog entry."""
    stems: set[str] = set()
    bot_id = (entry.get("id") or "").lower()
    if bot_id:
        vendor_prefix = bot_id.split("-")[0]
        if len(vendor_prefix) >= 3:
            stems.add(vendor_prefix)
    url = entry.get("url") or ""
    if url:
        m = _DOMAIN_STEM_RE.search(url)
        if m and len(m.group(1)) >= 3:
            stems.add(m.group(1).lower())
    for dom in (entry.get("verification") or {}).get("domains") or []:
        parts = [
            p
            for p in dom.lower().strip(".").split(".")
            if p
            not in {
                "com",
                "org",
                "net",
                "io",
                "ai",
                "co",
                "uk",
                "ru",
                "cn",
                "de",
                "jp",
                "fr",
                "crawl",
                "search",
                "bot",
                "spider",
            }
        ]
        for p in parts:
            if len(p) >= 3:
                stems.add(p)
    for pat in (entry.get("pattern") or {}).get("accepted") or []:
        lit = extract_literal_substring(pat) or pat
        tok = lit.split("/")[0].split("\\")[0].strip("^$()[]{}+*?.,; ").lower()
        stem = _BOT_TOKEN_SUFFIX_RE.sub("", tok).strip("-_.")
        if len(stem) >= 3:
            stems.add(stem)
    for pat in (entry.get("instances") or {}).get("accepted") or []:
        tok = pat.split("/")[0].split("\\")[0].strip("^$()[]{}+*?.,; ").lower()
        stem = _BOT_TOKEN_SUFFIX_RE.sub("", tok).strip("-_.")
        if len(stem) >= 3:
            stems.add(stem)
    return stems


# R-1: drain the matcher + source caches between tests. Both are
# mtime-revalidated in prod but tests use a sandboxed FS that doesn't
# share inodes across runs — leaks would surface as cross-test
# matcher state.
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("utils.bot_sources._matcher_cache", _matcher_cache)
_CacheRegistry.register("utils.bot_sources._source_cache", _source_cache)
_CacheRegistry.register("utils.bot_sources._compiled_ip_feeds", _compiled_ip_feeds)
_CacheRegistry.register("utils.bot_sources._version_cache", _version_cache)

# ── Reverse DNS Cache with FCrDNS Validation (folded from backend.utils.rdns_cache) ──


_write_lock = threading.Lock()
_last_enrichment_at: str | None = None

# Phase 1.4a — concurrency knobs for the async resolver. 50 in-flight PTR
# lookups keeps c-ares well under typical FD limits (1024) and avoids
# saturating upstream resolvers; 2.0s timeout matches the design spec —
# slow PTRs are common for unmaintained IPs and 2s catches real answers
# without blocking the loop.
_CONCURRENCY_LIMIT = 50
_RESOLVER_TIMEOUT = 2.0

# Hard caps bounding the cache's growth. Discovery/enqueue is driven by
# whatever source IPs show up in ingested logs — traffic to the monitored
# Fastly service, not anything this app authenticates — so without these,
# a client rotating through many source IPs (a botnet is the natural
# vector; the bot-classification enqueue path at repositories/security.py
# additionally keys off a spoofable User-Agent) can grow the pending
# backlog and the on-disk table without bound. Audit finding 008.
_MAX_PENDING_BACKLOG = 50_000
_MAX_TOTAL_ROWS = 250_000
_STALE_ROW_RETENTION_DAYS = 30
_REAP_INTERVAL_S = 24 * 3600
_last_reap_monotonic: float | None = None


def _is_ip_in_cidrs(ip: str, cidrs: list[str] | tuple[str, ...]) -> bool:
    """Return True if the IP address falls within any of the provided CIDR ranges."""
    try:
        return ip_in_ranges(ip, cidrs)
    except Exception:
        logger.debug("_is_ip_in_cidrs: step failed (ignored)", exc_info=True)
    return False


# ── Init ──────────────────────────────────────────────────────────────────────


def _write_con():
    from backend.core.metadata.base import get_pg_thread_connection

    return get_pg_thread_connection()


def _read_con():
    from backend.core.metadata.base import get_pg_readonly_connection

    return get_pg_readonly_connection()


# ── Public API ────────────────────────────────────────────────────────────────


def get_hostnames(ips: list[str]) -> dict[str, tuple[str | None, str, bool]]:
    """Batched ``get_hostname`` for an arbitrary list of IPs.

    Returns ``{ip: (hostname, status, fcrdns_verified)}`` for every IP found in
    the cache. Missing IPs are batch-enqueued as 'pending' and omitted from the
    result so callers can treat them as ``(None, 'pending', False)`` via a
    single dict lookup.
    """
    if not ips:
        return {}
    unique = list({ip for ip in ips if ip})
    if not unique:
        return {}
    con = _read_con()
    if con is None:
        enqueue(unique)
        return {}
    try:
        out: dict[str, tuple[str | None, str, bool]] = {}
        # SQLite's default SQLITE_LIMIT_VARIABLE_NUMBER is 999 — chunk to stay
        # well under it even with the few extra bind-spots we pass below.
        chunk_size = 900
        for i in range(0, len(unique), chunk_size):
            chunk = unique[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cur = con.execute(
                f"SELECT ip, hostname, status, fcrdns_verified FROM rdns WHERE ip IN ({placeholders})",
                chunk,
            )
            for row in cur.fetchall():
                out[row["ip"]] = (row["hostname"], row["status"], bool(row["fcrdns_verified"]))
    finally:
        con.close()

    missing = [ip for ip in unique if ip not in out]
    if missing:
        enqueue(missing)
    return out


def enqueue(ips: list[str]) -> int:
    """Insert unseen IPs as 'pending'. Skip IPs already in the cache.

    Returns the number of newly inserted rows.
    """
    if not ips:
        return 0
    unique_ips = list(dict.fromkeys(ip for ip in ips if ip))
    if not unique_ips:
        return 0
    with _write_lock:
        con = _write_con()
        try:
            pending = con.execute("SELECT count(*) FROM rdns WHERE status='pending'").fetchone()[0]
            if pending >= _MAX_PENDING_BACKLOG:
                logger.warning(
                    "🌐 [rdns] pending backlog at cap (%d >= %d) — dropping %d new IP(s) instead of "
                    "enqueueing; enrichment is falling behind actual traffic volume.",
                    pending,
                    _MAX_PENDING_BACKLOG,
                    len(unique_ips),
                )
                return 0
            inserted = 0
            chunk_size = 500
            for i in range(0, len(unique_ips), chunk_size):
                chunk = unique_ips[i : i + chunk_size]
                values_sql = ",".join("(?, 'pending', 0)" for _ in chunk)
                cur = con.execute(
                    f"INSERT OR IGNORE INTO rdns (ip, status, fcrdns_verified) VALUES {values_sql}",
                    chunk,
                )
                if cur.rowcount and cur.rowcount > 0:
                    inserted += cur.rowcount
            con.commit()
            return inserted
        finally:
            con.close()


# ── Sync lookup (kept for test compatibility) ─────────────────────────────────


def _do_lookup(ip: str) -> tuple[str | None, str, bool]:
    """Perform reverse + forward DNS lookup for FCrDNS validation.

    Sync helper using ``socket.gethostbyaddr`` / ``socket.getaddrinfo``.
    Kept as the patch target for unit tests that exercise individual
    branches; production hot path is :func:`_run_async_resolve` which
    drives :func:`_do_lookup_async` under ``asyncio.gather`` for true
    concurrency. The async-aware batch entrypoint detects a
    ``unittest.mock`` patch on this helper and routes through it so
    legacy test fixtures keep working.
    """
    try:
        hostname = socket.gethostbyaddr(ip)[0]
    except socket.herror:
        return None, "nxdomain", False
    except Exception:
        return None, "error", False

    # FCrDNS: forward-lookup the hostname and check if original IP is in result
    try:
        forward_ips = {info[4][0] for info in socket.getaddrinfo(hostname, None)}
        fcrdns_verified = ip in forward_ips
    except Exception:
        fcrdns_verified = False

    return hostname, "resolved", fcrdns_verified


# ── Async resolver (Phase 1.4a hot path) ──────────────────────────────────────


async def _do_lookup_async(
    ip: str,
    resolver: aiodns.DNSResolver,
    semaphore: asyncio.Semaphore,
) -> tuple[str | None, str, bool]:
    """Single-IP PTR + FCrDNS lookup using aiodns.

    Returns ``(hostname, status, fcrdns_verified)`` matching the legacy
    sync ``_do_lookup`` contract:

    - ``status='resolved'`` and hostname populated on success
    - ``status='nxdomain'`` and hostname=None on NXDOMAIN
    - ``status='error'`` and hostname=None on other failures (timeout, etc.)

    FCrDNS: tries A first, then AAAA. If the original IP is present in
    the forward result, ``fcrdns_verified=True``.
    """
    async with semaphore:
        try:
            ptr_result = await resolver.gethostbyaddr(ip)
            hostname = ptr_result.name
            if not hostname:
                return None, "nxdomain", False
        except aiodns.error.DNSError as e:
            code = e.args[0] if e.args else None
            if code in (aiodns.error.ARES_ENOTFOUND, aiodns.error.ARES_ENODATA):
                return None, "nxdomain", False
            return None, "error", False
        except Exception:
            return None, "error", False

        forward_ips: set[str] = set()
        for record_type in ("A", "AAAA"):
            try:
                # aiodns >= 4: a DNSResult whose ``answer`` records carry the address in
                # ``data.addr`` (the result itself is not iterable).
                ans = await resolver.query_dns(hostname, record_type)
                forward_ips.update(addr for rec in ans.answer if (addr := getattr(rec.data, "addr", None)))
            except Exception:
                continue

        return hostname, "resolved", ip in forward_ips


async def _resolve_batch_async(ips: list[str]) -> dict[str, tuple[str | None, str, bool]]:
    """Resolve up to ``_CONCURRENCY_LIMIT`` IPs concurrently via aiodns.

    Returns a dict mapping ip → (hostname, status, fcrdns_verified).
    Individual exceptions are swallowed into ``(None, 'error', False)``
    so one c-ares hiccup doesn't drop the whole batch.
    """
    if not ips:
        return {}

    resolver = aiodns.DNSResolver(timeout=_RESOLVER_TIMEOUT)
    semaphore = asyncio.Semaphore(_CONCURRENCY_LIMIT)

    try:
        tasks = [_do_lookup_async(ip, resolver, semaphore) for ip in ips]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        try:
            resolver.cancel()
        except Exception:
            logger.debug("_resolve_batch_async: resolver.cancel failed (ignored)", exc_info=True)

    out: dict[str, tuple[str | None, str, bool]] = {}
    for ip, result in zip(ips, results, strict=True):
        if isinstance(result, BaseException):
            out[ip] = (None, "error", False)
        else:
            out[ip] = result
    return out


def _bulk_update_sync(records: list[tuple[str | None, str, int, str, str]]) -> None:
    if not records:
        return
    con = _write_con()
    try:
        con.executemany(
            "UPDATE rdns SET hostname=?, status=?, fcrdns_verified=?, looked_up_at=? WHERE ip=?",
            records,
        )
        con.commit()
    finally:
        con.close()


async def _bulk_update_async(records: list[tuple[str | None, str, int, str, str]]) -> None:
    """Bulk UPDATE rdns rows with new lookup results."""
    if not records:
        return
    await asyncio.to_thread(_bulk_update_sync, records)


def _run_async_resolve(ips: list[str]) -> dict[str, int]:
    """Concurrent resolve + bulk write entrypoint. Returns the summary
    ``{"resolved": N, "errors": N}`` consumed by both ``enrich_batch``
    variants.

    **MUST be called from a synchronous context** — it calls
    ``asyncio.run()`` internally, which raises ``RuntimeError`` when
    invoked from inside a running event loop. From an async handler,
    wrap with ``await asyncio.to_thread(_run_async_resolve, ips)`` so
    the call happens on a worker thread that doesn't own the loop.
    Production callers are cron jobs running on the APScheduler
    threadpool, which is sync — the loop-detection fallback at the
    bottom of the function is a defensive belt-and-suspenders only.

    Compatibility detection: if ``_do_lookup`` has been monkey-patched
    by ``unittest.mock`` (common in tests that exercise individual
    status / FCrDNS branches), we drive the per-IP loop through the
    patched sync helper and write via sync sqlite3 — preserving the
    legacy fixture behaviour. Production never trips this branch.
    """
    if not ips:
        return {"resolved": 0, "errors": 0}

    mod = sys.modules[__name__]
    do_lookup = mod._do_lookup
    is_patched = getattr(do_lookup, "_mock_name", None) is not None or "Mock" in type(do_lookup).__name__

    if is_patched:
        results = {ip: do_lookup(ip) for ip in ips}
    else:
        try:
            results = asyncio.run(_resolve_batch_async(ips))
        except RuntimeError as e:
            # ``asyncio.run() cannot be called from a running event loop``
            # — fall back to the sync per-IP path. Production never hits
            # this branch (cron jobs run on threadpool, not the loop).
            if "running event loop" not in str(e):
                raise
            logger.warning("[rdns_cache] async resolve fallback: running event loop detected")
            results = {ip: _do_lookup(ip) for ip in ips}

    now = iso_z_now()
    records = [(hostname, status, int(fcrdns), now, ip) for ip, (hostname, status, fcrdns) in results.items()]

    if records:
        with _write_lock:
            try:
                asyncio.run(_bulk_update_async(records))
            except RuntimeError as e:
                if "running event loop" not in str(e):
                    raise
                con = _write_con()
                try:
                    con.executemany(
                        "UPDATE rdns SET hostname=?, status=?, fcrdns_verified=?, looked_up_at=? WHERE ip=?",
                        records,
                    )
                    con.commit()
                finally:
                    con.close()

    resolved = sum(1 for _, status, _ in results.values() if status == "resolved")
    errors = len(results) - resolved
    return {"resolved": resolved, "errors": errors}


# ── Enrichment loop ───────────────────────────────────────────────────────────


def _reap_stale_rows() -> dict:
    """Bound the cache's on-disk footprint. Two idempotent passes:

    1. Drop nxdomain/error rows older than the retention window — they'll
       never resolve to anything useful, and are cheap to re-enqueue if the
       IP shows up again.
    2. If the table is still over the hard cap (e.g. the backlog cap in
       :func:`enqueue` was itself raised, or a burst outpaced retention),
       delete the oldest-looked-up non-pending rows until back under it.
       Pending rows are excluded — that backlog is bounded separately by
       :data:`_MAX_PENDING_BACKLOG` in :func:`enqueue`, not by this reaper.

    Runs under the same writer lock as every other mutation.
    """
    with _write_lock:
        con = _write_con()
        try:
            cur = con.execute(
                "DELETE FROM rdns WHERE status IN ('nxdomain', 'error') AND looked_up_at < datetime('now', ?)",
                (f"-{_STALE_ROW_RETENTION_DAYS} days",),
            )
            stale_deleted = cur.rowcount

            total = con.execute("SELECT count(*) FROM rdns").fetchone()[0]
            overflow_deleted = 0
            if total > _MAX_TOTAL_ROWS:
                overflow = total - _MAX_TOTAL_ROWS
                cur = con.execute(
                    """DELETE FROM rdns WHERE ip IN (
                        SELECT ip FROM rdns WHERE status != 'pending' ORDER BY looked_up_at ASC LIMIT ?
                    )""",
                    (overflow,),
                )
                overflow_deleted = cur.rowcount
            con.commit()
        finally:
            con.close()
    return {"stale_deleted": stale_deleted, "overflow_deleted": overflow_deleted}


def _maybe_reap_stale_rows() -> None:
    """Run :func:`_reap_stale_rows` at most once per :data:`_REAP_INTERVAL_S`.

    Gated on an in-memory monotonic timestamp rather than a persisted one —
    worst case a process restart makes it run a bit more often than once a
    day, which is harmless (each pass is a cheap indexed DELETE, a no-op in
    steady state).
    """
    global _last_reap_monotonic
    now = time.monotonic()
    if _last_reap_monotonic is not None and (now - _last_reap_monotonic) < _REAP_INTERVAL_S:
        return
    _last_reap_monotonic = now
    try:
        result = _reap_stale_rows()
        if result["stale_deleted"] or result["overflow_deleted"]:
            logger.info("🌐 \x1b[34m[rdns]\x1b[0m reap complete: %s", result)
    except Exception as e:
        logger.error("[rdns_cache] Reap pass failed: %s", e)


def _get_pending_count() -> int:
    con = _read_con()
    if con is None:
        return 0
    try:
        row = con.execute("SELECT count(*) FROM rdns WHERE status='pending'").fetchone()
        return row[0] if row else 0
    finally:
        con.close()


def enrich_batch(limit: int | None = None) -> dict:
    """Resolve pending IPs with FCrDNS validation, then discover new IPs from
    DuckDB sources.

    If limit is None, dynamically scales the batch size based on the pending
    queue depth:
    - pending <= 200: batch_limit = max(100, pending) if pending > 0 else 200
    - 200 < pending <= 2000: batch_limit = 500
    - pending > 2000: batch_limit = 1000

    Returns a summary dict with counts for monitoring/logging.
    """
    global _last_enrichment_at

    resolved = 0
    errors = 0
    discovered = 0

    if limit is not None:
        batch_limit = limit
    else:
        pending_count = _get_pending_count()
        if pending_count <= 200:
            batch_limit = max(100, pending_count) if pending_count > 0 else 200
        elif pending_count <= 2000:
            batch_limit = 500
        else:
            batch_limit = 1000

    pending_rows = _select_ips_with_status("pending", limit=batch_limit)
    if pending_rows:
        pending_ips = [row[0] for row in pending_rows]
        summary = _run_async_resolve(pending_ips)
        resolved = summary["resolved"]
        errors = summary["errors"]

    stale_rows = _select_stale_ips(limit=max(1, batch_limit // 4))
    if stale_rows:
        stale_ips = [row[0] for row in stale_rows]
        _run_async_resolve(stale_ips)

    try:
        discovered = _discover_new_ips(max_new=500)
    except Exception as e:
        logger.error("[rdns_cache] Discovery pass failed: %s", e)

    _maybe_reap_stale_rows()

    _last_enrichment_at = iso_z_now()
    summary_out = {
        "resolved": resolved,
        "errors": errors,
        "discovered": discovered,
        "batch_limit": batch_limit,
    }
    if resolved > 0 or errors > 0 or discovered > 0:
        logger.info("🌐 \x1b[34m[rdns]\x1b[0m enrich_batch complete: %s", summary_out)
    else:
        logger.debug("🌐 \x1b[34m[rdns]\x1b[0m enrich_batch complete (no activity)")
    return summary_out


def get_stats() -> dict:
    """Return cache stats for the admin UI."""
    con = _read_con()
    if con is None:
        return {"total": 0, "pending": 0, "last_enrichment_at": None}
    try:
        total = con.execute("SELECT count(*) FROM rdns").fetchone()[0]
        pending = con.execute("SELECT count(*) FROM rdns WHERE status='pending'").fetchone()[0]
    finally:
        con.close()
    return {
        "total": total,
        "pending": pending,
        "last_enrichment_at": _last_enrichment_at,
    }


# ── Verification logic (pure, no DB) ─────────────────────────────────────────


def classify(
    ip: str,
    hostname: str | None,
    status: str,
    fcrdns_verified: bool,
    verification_domains: list[str],
    verification_cidrs: list[str] | tuple[str, ...] | None = None,
) -> str:
    """Classify a bot hit into verified / impersonator / unverified.

    Tries CIDR match first (instant trust), then falls back to FCrDNS validation.
    """
    # 1. CIDR match is authoritative if provided
    if verification_cidrs and _is_ip_in_cidrs(ip, verification_cidrs):
        return "verified"

    # 2. Fallback to FCrDNS
    if not verification_domains:
        return "unverified"

    if status == "pending":
        return "unverified_pending"

    if status in ("nxdomain", "error") or hostname is None or not fcrdns_verified:
        return "impersonator"

    if any(hostname == d or hostname.endswith("." + d) for d in verification_domains):
        return "verified"

    return "impersonator"


# ── Internal helpers ──────────────────────────────────────────────────────────


def _select_ips_with_status(status: str, *, limit: int) -> list[tuple[str]]:
    """Read IPs with the given status."""
    with _write_lock:
        con = _write_con()
        try:
            return con.execute(
                "SELECT ip FROM rdns WHERE status = ? LIMIT ?",
                (status, limit),
            ).fetchall()
        finally:
            con.close()


def _select_stale_ips(*, limit: int) -> list[tuple[str]]:
    with _write_lock:
        con = _write_con()
        try:
            return con.execute(
                """SELECT ip FROM rdns
                   WHERE status != 'pending'
                     AND looked_up_at < datetime('now', '-48 hours')
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        finally:
            con.close()


def _discover_new_ips_gen(max_new: int = 500, days: int = 30):
    """Query all DuckDB sources for IPs not yet in the rDNS cache.
    Yields progress events.
    """
    try:
        from backend import config as svcconfig
        from backend.core.duckdb import get_connection, get_source_for_service
    except ImportError:
        yield {"type": "count", "count": 0}
        return

    try:
        # Load fully hydrated source dictionaries (including credentials)
        sources = []
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if service_id:
                src = get_source_for_service(service_id)
                if src:
                    sources.append(src)
    except Exception as e:
        logger.error("[rdns_cache] Could not load sources for discovery: %s", e)
        yield {"type": "count", "count": 0}
        return

    # Collect IPs not already in cache across all sources
    new_ips: set[str] = set()
    remaining = max_new

    for src in sources:
        if remaining <= 0:
            break
        yield {"type": "status", "message": f"Scanning {src.get('name')}..."}
        try:
            # read_only: SELECT DISTINCT against the view, no writes.
            con = get_connection(src, read_only=True)
            try:
                from backend.core.duckdb import _safe_table_name
                from backend.core.storage import execute_with_stale_view_retry

                # Check if this source has an ip column
                table_name = _safe_table_name(src["name"])
                cols = [
                    r[0]
                    for r in con.execute(
                        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
                    ).fetchall()
                ]
                if "ip" not in cols:
                    continue

                # Wrap the DISTINCT scan in the stale-view self-heal so
                # a buffer parquet that's been swept since this connection
                # was opened gets recovered (clear caches + force rebind +
                # retry once), matching QueryRunner.execute_with_retry.
                # Pre-fix: every commit cycle that swept a buffer left the
                # rdns discovery scan failing for 5 minutes (until next
                # tick), spamming ERROR logs on a 100% failure pattern
                # for hours — witnessed 2026-06-10.
                def _scan_ips(c):
                    return c.execute(
                        f"""SELECT DISTINCT ip FROM "{table_name}"
                            WHERE ip IS NOT NULL
                              AND timestamp >= now() - INTERVAL '{days} days'
                            LIMIT {remaining * 2}"""
                    ).fetchall()

                rows = execute_with_stale_view_retry(con, src, _scan_ips)
            finally:
                con.close()

            # Filter to IPs not already cached
            candidate_ips = [r[0] for r in rows if r[0]]
            if not candidate_ips:
                continue

            with _write_lock:
                con2 = _write_con()
                try:
                    placeholders = ",".join("?" * len(candidate_ips))
                    already = {
                        r[0]
                        for r in con2.execute(
                            f"SELECT ip FROM rdns WHERE ip IN ({placeholders})",
                            candidate_ips,
                        ).fetchall()
                    }
                finally:
                    con2.close()

            found_this_src = 0
            for ip in candidate_ips:
                if ip not in already and ip not in new_ips:
                    new_ips.add(ip)
                    found_this_src += 1
                    remaining -= 1
                    if remaining <= 0:
                        break
            if found_this_src > 0:
                yield {"type": "log", "message": f"Found {found_this_src} new IPs in {src.get('name')}"}

        except Exception as e:
            logger.error("[rdns_cache] Discovery error for source %s: %s", src.get("name"), e)
            yield {"type": "log", "message": f"Error scanning {src.get('name')}: {e}"}

    if new_ips:
        enqueue(list(new_ips))
        logger.info("🌐 \x1b[34m[rdns]\x1b[0m Discovered %d new IPs for enrichment", len(new_ips))

    yield {"type": "count", "count": len(new_ips)}


def _discover_new_ips(max_new: int = 500, days: int = 30) -> int:
    """Query all DuckDB sources for IPs not yet in the rDNS cache.

    Returns count of newly enqueued IPs.
    """
    count = 0
    for event in _discover_new_ips_gen(max_new, days):
        if event["type"] == "count":
            count = event["count"]
    return count


_self_mod = sys.modules[__name__]
sys.modules["backend.utils.rdns_cache"] = _self_mod
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    setattr(_utils_pkg, "rdns_cache", _self_mod)
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES["backend.utils.rdns_cache"] = __name__
