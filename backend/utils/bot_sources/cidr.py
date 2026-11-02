"""CIDR interval helpers and pytricia IP matching for bot sources."""

from __future__ import annotations

import bisect
import functools
import ipaddress
import logging
from collections.abc import Iterable

import pytricia

from .registry import (
    BOT_SOURCES,
    _cache_mtime,
    _compiled_ip_feeds,
    _compiled_ip_feeds_lock,
    load_source,
)

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


def _is_ip_in_cidrs(ip: str, cidrs: list[str] | tuple[str, ...]) -> bool:
    """Return True if the IP address falls within any of the provided CIDR ranges."""
    try:
        return ip_in_ranges(ip, cidrs)
    except Exception:
        logger.debug("_is_ip_in_cidrs: step failed (ignored)", exc_info=True)
    return False


def _compile_feed_intervals(
    feeds: dict[str, list[ipaddress.IPv4Network | ipaddress.IPv6Network]],
) -> dict[str, tuple[list[int], list[int], list[int], list[int]]]:
    """Collapse each feed's networks into sorted disjoint integer interval arrays for O(log M) bisect matching."""
    return {feed_id: collapse_network_objects(net_objs) for feed_id, net_objs in feeds.items()}


def _compile_feed_tries(
    feeds: dict[str, list[ipaddress.IPv4Network | ipaddress.IPv6Network]],
) -> dict[str, tuple[pytricia.PyTricia, pytricia.PyTricia]]:
    """Compile each feed's networks into ``(v4_trie, v6_trie)`` pytricia tries."""
    tries: dict[str, tuple[pytricia.PyTricia, pytricia.PyTricia]] = {}
    for feed_id, net_objs in feeds.items():
        v4_trie = pytricia.PyTricia(32)
        v6_trie = pytricia.PyTricia(128)
        for net in net_objs:
            (v4_trie if net.version == 4 else v6_trie)[str(net)] = True
        tries[feed_id] = (v4_trie, v6_trie)
    return tries


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
        _compiled_ip_feeds["tries"] = _compile_feed_tries(feeds)
        _compiled_ip_feeds["tries_feeds_id"] = id(feeds)
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


def _get_feed_tries(
    ip_feeds: dict[str, list[ipaddress.IPv4Network | ipaddress.IPv6Network]],
) -> dict[str, tuple[pytricia.PyTricia, pytricia.PyTricia]]:
    with _compiled_ip_feeds_lock:
        if _compiled_ip_feeds.get("tries_feeds_id") == id(ip_feeds) and "tries" in _compiled_ip_feeds:
            return _compiled_ip_feeds["tries"]
        tries = _compile_feed_tries(ip_feeds)
        _compiled_ip_feeds["tries"] = tries
        _compiled_ip_feeds["tries_feeds_id"] = id(ip_feeds)
        return tries


def match_ip_to_bot_source(ip: str) -> str | None:
    """Return the first matching IP-based bot source ID for ``ip`` using pytricia tries, or None."""
    if not ip:
        return None
    ip_feeds = _get_compiled_ip_feeds()
    feed_tries = _get_feed_tries(ip_feeds)
    is_v6 = ":" in ip
    try:
        for feed_id, (v4_trie, v6_trie) in feed_tries.items():
            trie = v6_trie if is_v6 else v4_trie
            if len(trie) > 0 and ip in trie:
                return feed_id
    except (ValueError, KeyError):
        return None
    return None
