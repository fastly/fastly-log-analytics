"""Shared CIDR interval collapsing and O(log M) binary-search IP range lookup helpers."""

from __future__ import annotations

import bisect
import functools
import ipaddress
from collections.abc import Iterable

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


def ip_in_ranges(ip: str, cidrs: list[str] | tuple[str, ...]) -> bool:
    """Return True if the IP address falls within any of the provided CIDR ranges."""
    if not cidrs or not ip:
        return False
    parsed = parse_ip_int_version(ip)
    if parsed is None:
        return False
    ip_int, version = parsed
    cidrs_key = cidrs if isinstance(cidrs, tuple) else tuple(cidrs)
    intervals = collapse_cidr_ranges(cidrs_key)
    return ip_int_in_intervals(ip_int, version, intervals)
