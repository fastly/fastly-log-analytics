"""Bot source cache — fetch, store, and match against known bot registries."""

from __future__ import annotations

import bisect
import functools
import hashlib
import ipaddress
import json
import logging
import os
import re
import tempfile
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from backend.utils.date_utils import iso_z_now
from backend.utils.http import http_get_text

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

# Pre-compiled IP network feeds for high-performance IP-based bot matching.
_compiled_ip_feeds: dict = {}
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
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    target = _cache_path(source_id)
    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(_CACHE_DIR),
    )
    payload = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
    try:
        with os.fdopen(tmp_fd, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, target)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


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


def _compile_feed_intervals(
    feeds: dict[str, list[ipaddress.IPv4Network | ipaddress.IPv6Network]],
) -> dict[str, tuple[list[int], list[int], list[int], list[int]]]:
    """Collapse each feed's networks into sorted disjoint integer interval arrays for O(log M) bisect matching."""
    intervals: dict[str, tuple[list[int], list[int], list[int], list[int]]] = {}
    for feed_id, net_objs in feeds.items():
        v4_nets = [n for n in net_objs if isinstance(n, ipaddress.IPv4Network)]
        v6_nets = [n for n in net_objs if isinstance(n, ipaddress.IPv6Network)]
        v4_collapsed = list(ipaddress.collapse_addresses(v4_nets)) if v4_nets else []
        v6_collapsed = list(ipaddress.collapse_addresses(v6_nets)) if v6_nets else []
        intervals[feed_id] = (
            [int(n.network_address) for n in v4_collapsed],
            [int(n.broadcast_address) for n in v4_collapsed],
            [int(n.network_address) for n in v6_collapsed],
            [int(n.broadcast_address) for n in v6_collapsed],
        )
    return intervals


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
        from backend.utils.bot_sources import build_matcher
        from backend.utils.rdns_cache import classify, get_hostnames

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
                ip_obj = ipaddress.ip_address(ip_str)
                ip_int = int(ip_obj)
                is_v4 = ip_obj.version == 4
                for feed_id, (v4_starts, v4_ends, v6_starts, v6_ends) in feed_intervals.items():
                    starts = v4_starts if is_v4 else v6_starts
                    ends = v4_ends if is_v4 else v6_ends
                    if not starts:
                        continue
                    idx = bisect.bisect_right(starts, ip_int) - 1
                    if idx >= 0 and ip_int <= ends[idx]:
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
        from backend.core.metadata import pg_connection

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
            compiled: list[tuple[dict, list[re.Pattern]]] = []
            for entry in load_well_known_bots_catalog():
                raw_patterns = (entry.get("pattern") or {}).get("accepted") or []
                pats: list[re.Pattern] = []
                for rp in raw_patterns:
                    try:
                        pats.append(re.compile(rp, re.IGNORECASE))
                    except re.error:
                        continue
                if pats:
                    compiled.append((entry, pats))
            _compiled = compiled

            @functools.lru_cache(maxsize=10_000)
            def _match_seed_ua(ua: str) -> tuple[dict, ...]:
                matches = []
                for entry, pats in _compiled:
                    for pat in pats:
                        if pat.search(ua):
                            matches.append(entry)
                            break
                return tuple(matches)

            _seed_matcher_fn = _match_seed_ua
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
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("utils.bot_sources._matcher_cache", _matcher_cache)
_CacheRegistry.register("utils.bot_sources._source_cache", _source_cache)
_CacheRegistry.register("utils.bot_sources._compiled_ip_feeds", _compiled_ip_feeds)
_CacheRegistry.register("utils.bot_sources._version_cache", _version_cache)
