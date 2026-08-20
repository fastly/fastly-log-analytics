"""Bot source cache — fetch, store, and match against known bot registries."""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import re
import threading
import urllib.request
from collections.abc import Callable
from pathlib import Path

from backend.utils.date_utils import iso_z_now

logger = logging.getLogger(__name__)

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
_matcher_cache: dict = {}  # keys: "fn", "mtime"

# mtime-revalidated cache for parsed source envelopes. load_source is hit
# by every get_bot_regex_pattern / get_bot_by_id call (dashboard +
# security repos) and the JSON files are megabytes — the parse cost was
# adding up.
_source_cache: dict[str, tuple[int, list[dict]]] = {}
_source_cache_lock = threading.Lock()


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
_version_cache: dict = {"mtime": None, "version": None}
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
        req = urllib.request.Request(url, headers={"User-Agent": "fastly-log-analysis/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode()

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


def fetch_and_cache_source(source_id: str) -> dict:
    """Fetch the source URL, write the cache file, return updated metadata."""
    src = next((s for s in BOT_SOURCES if s["id"] == source_id), None)
    if src is None:
        raise ValueError(f"Unknown bot source: {source_id!r}")

    req = urllib.request.Request(src["url"], headers={"User-Agent": "fastly-log-analysis/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw_content = resp.read().decode()

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

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    entry_count = len(cidrs) if src.get("type") == "ip" else len(entries)
    envelope = {
        "last_updated": iso_z_now(),
        "entry_count": entry_count,
        "entries": entries,
    }
    _cache_path(source_id).write_text(json.dumps(envelope, separators=(",", ":")))

    # Invalidate the in-process matcher so next call rebuilds
    with _matcher_lock:
        _matcher_cache.clear()

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
                pass
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
    return results


# ── Pattern utilities ─────────────────────────────────────────────────────────


def extract_literal_substring(pattern: str) -> str | None:
    """Return the longest literal substring from a regex pattern.

    Used to build ILIKE pre-filters that are faster than regexp_matches().
    E.g. 'Googlebot\\/.*' → 'Googlebot/', 'compatible; bingbot\\/2\\.0' → 'bingbot/2.0'
    """
    _LITERAL_ESCAPES = set(r"\/.,-_ ")
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

        # Load and compile all patterns
        compiled: list[tuple[dict, list[re.Pattern]]] = []
        for src in BOT_SOURCES:
            if not src["enabled"]:
                continue
            for entry in load_source(src["id"]):
                raw_patterns = entry.get("pattern", {}).get("accepted", [])
                pats: list[re.Pattern] = []
                for rp in raw_patterns:
                    try:
                        pats.append(re.compile(rp, re.IGNORECASE))
                    except re.error as e:
                        logger.debug("[bot_sources] Bad pattern %r in %s: %s", rp, entry.get("id"), e)
                if pats:
                    compiled.append((entry, pats))

        # Snapshot compiled into the closure so the lru_cache is self-contained
        _compiled = compiled

        @functools.lru_cache(maxsize=10_000)
        def _match_ua(ua: str) -> tuple[dict, ...]:
            matches = []
            for entry, pats in _compiled:
                for pat in pats:
                    if pat.search(ua):
                        matches.append(entry)
                        break
            return tuple(matches)

        _matcher_cache["fn"] = _match_ua
        _matcher_cache["mtime"] = current_mtime
        logger.info("👾 \x1b[36m[bots]\x1b[0m Rebuilt matcher from %d bot entries", len(_compiled))
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


import ipaddress
from typing import Any

_compiled_ip_feeds: dict = {}
_compiled_ip_feeds_lock = threading.Lock()


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
        _compiled_ip_feeds["mtime"] = current_mtime
        return feeds


def enrich_bot_metadata(df: Any) -> None:
    """
    Unified enrichment for virtual bot fields (_bot_name, _ngwaf_bot_name).
    Modifies the DataFrame in-place.
    """
    if df.empty:
        return

    # 1. Arcjet/Well-Known Bot Matching & IP feed matching
    if "ua" in df.columns and "ip" in df.columns:
        from backend.utils.bot_sources import build_matcher
        from backend.utils.rdns_cache import classify, get_hostnames

        match_ua = build_matcher()
        # Match UAs first so we know exactly which IPs need hostname resolution
        # — then batch-resolve them in one SQLite read instead of opening a
        # fresh connection per row.
        row_matches: list[tuple[str, tuple[dict, ...]]] = []
        candidate_ips: list[str] = []
        for ua_val, ip_val in zip(df["ua"], df["ip"]):
            matches = match_ua(str(ua_val) if ua_val else "")
            ip_str = str(ip_val) if ip_val else ""
            row_matches.append((ip_str, matches))
            if matches and ip_str:
                candidate_ips.append(ip_str)
        hostnames = get_hostnames(candidate_ips)

        # Get pre-compiled IP ranges
        ip_feeds = _get_compiled_ip_feeds()

        bot_names = []
        for ip_str, matches in row_matches:
            matched_feed = None
            if ip_str:
                try:
                    ip_obj = ipaddress.ip_address(ip_str)
                    for feed_id, net_objs in ip_feeds.items():
                        for net in net_objs:
                            if ip_obj in net:
                                matched_feed = feed_id
                                break
                        if matched_feed:
                            break
                except Exception:
                    pass

            if not matches:
                if matched_feed:
                    feed_name = next((s["name"] for s in BOT_SOURCES if s["id"] == matched_feed), matched_feed)
                    bot_names.append(f"{feed_name} (IP-Match)")
                else:
                    bot_names.append("null")
                continue

            entry = matches[0]
            bot_name = entry.get("name", entry.get("id", "unknown"))
            hostname, status, fcrdns_verified = hostnames.get(ip_str, (None, "pending", False))

            verification = entry.get("verification", {})
            verification_domains = verification.get("domains", [])
            verification_cidrs = verification.get("cidrs", [])
            state = classify(ip_str, hostname, status, fcrdns_verified, verification_domains, verification_cidrs)

            if matched_feed:
                feed_name = next((s["name"] for s in BOT_SOURCES if s["id"] == matched_feed), matched_feed)
                bot_names.append(f"{bot_name} ({state}, {feed_name})")
            else:
                bot_names.append(f"{bot_name} ({state})")
        df["_bot_name"] = bot_names

    # 2. NG-WAF Verified Bot Enrichment
    if "waf_req_id" in df.columns:
        import os
        import sqlite3

        from backend import config as svcconfig

        ngwaf_db = svcconfig.ngwaf_db_path()
        if not os.path.exists(ngwaf_db):
            df["_ngwaf_bot_name"] = None
        else:
            # Normalize each waf_req_id once: None for missing/sentinel
            # values, str(x) otherwise. Both the IN-list and the per-row
            # output then read from this single pass.
            norm = [None if (x is None or str(x) in ("None", "nan", "")) else str(x) for x in df["waf_req_id"]]
            waf_ids = [n for n in norm if n]
            if not waf_ids:
                df["_ngwaf_bot_name"] = None
            else:
                try:
                    placeholders = ",".join("?" * len(waf_ids))
                    # mode=ro: pure SELECT, no schema or row writes — opening
                    # read-only skips SQLite's shared-lock acquisition entirely
                    # and lets concurrent NGWAF sync writers proceed unimpeded.
                    sq = sqlite3.connect(f"file:{ngwaf_db}?mode=ro", uri=True)
                    rows = sq.execute(
                        f"SELECT waf_req_id, bot_name FROM ngwaf_bots WHERE waf_req_id IN ({placeholders})",
                        waf_ids,
                    ).fetchall()
                    sq.close()
                    bot_map = {r[0]: r[1] for r in rows}
                    df["_ngwaf_bot_name"] = [bot_map.get(n) if n else None for n in norm]
                except Exception:
                    df["_ngwaf_bot_name"] = None


# R-1: drain the matcher + source caches between tests. Both are
# mtime-revalidated in prod but tests use a sandboxed FS that doesn't
# share inodes across runs — leaks would surface as cross-test
# matcher state.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("utils.bot_sources._matcher_cache", _matcher_cache)
_CacheRegistry.register("utils.bot_sources._source_cache", _source_cache)
_CacheRegistry.register("utils.bot_sources._compiled_ip_feeds", _compiled_ip_feeds)
