"""Outbound webhook URL validation and connection-time address checks."""

from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit

_BLOCKED_HOSTNAMES = frozenset(
    {
        "localhost",
        "localhost.localdomain",
        "metadata.google.internal",
        "metadata",
        "ip6-localhost",
        "ip6-loopback",
    }
)
_NAT64_WELL_KNOWN = ipaddress.IPv6Network("64:ff9b::/96")
_NAT64_LOCAL_USE = ipaddress.IPv6Network("64:ff9b:1::/48")
_IPV4_TRANSLATED = ipaddress.IPv6Network("::ffff:0:0:0/96")
_IPV4_COMPATIBLE = ipaddress.IPv6Network("::/96")
_IPV6_UNSPECIFIED = ipaddress.IPv6Address("::")
_IPV6_LOOPBACK = ipaddress.IPv6Address("::1")
_CGNAT_NET = ipaddress.IPv4Network("100.64.0.0/10")


def _is_restricted_hostname(host: str) -> bool:
    return (
        host in _BLOCKED_HOSTNAMES
        or host.endswith(".localhost")
        or host.endswith(".internal")
        or host.endswith(".local")
    )


def _is_restricted_v4(addr: ipaddress.IPv4Address) -> bool:
    return (
        addr in _CGNAT_NET
        or not addr.is_global
        or addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_reserved
        or addr.is_unspecified
    )


def _extract_embedded_ipv4(addr: ipaddress.IPv6Address) -> list[ipaddress.IPv4Address]:
    embedded: list[ipaddress.IPv4Address] = []
    if addr.ipv4_mapped is not None:
        embedded.append(addr.ipv4_mapped)
    if addr in _IPV4_TRANSLATED:
        embedded.append(ipaddress.IPv4Address(addr.packed[12:16]))
    if addr in _IPV4_COMPATIBLE and addr not in (_IPV6_UNSPECIFIED, _IPV6_LOOPBACK):
        embedded.append(ipaddress.IPv4Address(addr.packed[12:16]))
    if addr.sixtofour is not None:
        embedded.append(addr.sixtofour)
    if addr.teredo is not None:
        embedded.extend(addr.teredo)
    return embedded


def _is_restricted_ip(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(addr, ipaddress.IPv4Address):
        return _is_restricted_v4(addr)
    if addr in _NAT64_WELL_KNOWN or addr in _NAT64_LOCAL_USE:
        return True
    if any(_is_restricted_v4(v4) for v4 in _extract_embedded_ipv4(addr)):
        return True
    return (
        not addr.is_global
        or addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_reserved
        or addr.is_unspecified
    )


def restricted_webhook_host(host: str) -> bool:
    cleaned = (host or "").strip().lower().rstrip(".")
    if not cleaned:
        return True
    if _is_restricted_hostname(cleaned):
        return True
    bare = cleaned.split("%", 1)[0]
    try:
        address = ipaddress.ip_address(bare)
    except ValueError:
        return False
    return _is_restricted_ip(address)


def validate_webhook_url_syntax(url: str) -> str:
    """Validate webhook URL syntax and reject restricted hostnames/IPs (SSRF protection).

    Raises ``ValueError`` with a human-readable message when invalid.
    """
    cleaned = (url or "").strip()
    if not cleaned:
        raise ValueError("Webhook URL must not be empty.")
    parsed = urlsplit(cleaned)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("Webhook URL must use http or https scheme with a valid hostname.")
    host = parsed.hostname.lower().rstrip(".")
    if _is_restricted_hostname(host):
        raise ValueError(f"Webhook target hostname '{host}' is not allowed (SSRF protection).")
    bare = host.split("%", 1)[0]
    try:
        ip = ipaddress.ip_address(bare)
    except ValueError:
        ip = None

    if ip is not None and _is_restricted_ip(ip):
        raise ValueError(f"Webhook target IP '{host}' is in a restricted range (SSRF protection).")
    return cleaned


@dataclass(frozen=True)
class PinnedWebhookDestination:
    url: str
    host: str
    sni_hostname: str | None


def validate_webhook_destination(url: str) -> PinnedWebhookDestination:
    """Resolve a webhook hostname immediately before delivery and reject restricted addresses."""
    parsed = urlsplit((url or "").strip())
    host = parsed.hostname
    if parsed.scheme not in ("http", "https") or not host:
        raise ValueError("invalid_webhook_url")
    if restricted_webhook_host(host):
        raise ValueError("restricted_webhook_host")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ValueError("unresolvable_webhook_host") from exc
    validated: list[str] = []
    for _family, _kind, _proto, _canonical, sockaddr in addresses:
        address = str(sockaddr[0]).split("%", 1)[0]
        if restricted_webhook_host(address):
            raise ValueError("restricted_webhook_address")
        if address not in validated:
            validated.append(address)
    if not validated:
        raise ValueError("unresolvable_webhook_host")
    pinned = validated[0]
    literal = f"[{pinned}]" if ":" in pinned else pinned
    parsed_port = f":{parsed.port}" if parsed.port is not None else ""
    pinned_url = parsed._replace(netloc=f"{literal}{parsed_port}").geturl()
    host_header = host + parsed_port
    return PinnedWebhookDestination(
        url=pinned_url,
        host=host_header,
        sni_hostname=host if parsed.scheme == "https" else None,
    )
