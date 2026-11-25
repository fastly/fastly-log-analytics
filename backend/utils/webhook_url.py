"""Outbound webhook URL validation and connection-time address checks."""

from __future__ import annotations

import ipaddress
import re
import socket
import unicodedata
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
_NON_CANONICAL_IPV4_LABEL_RE = re.compile(r"^(?:0[xX][0-9a-fA-F]+|[0-9]+)$")


def _has_disallowed_url_chars(raw: str) -> bool:
    return "\\" in raw or any(ord(c) < 0x20 or ord(c) == 0x7F for c in raw)


def _normalize_host(raw_host: str | None) -> str | None:
    cleaned = (raw_host or "").strip()
    if not cleaned or any(ord(c) < 0x20 or ord(c) == 0x7F or c.isspace() for c in cleaned):
        return None
    nfkc = unicodedata.normalize("NFKC", cleaned)
    if not nfkc or any(ord(c) < 0x20 or ord(c) == 0x7F or c.isspace() for c in nfkc):
        return None
    if ":" in nfkc:
        try:
            ascii_v6 = nfkc.encode("ascii").decode("ascii").lower().rstrip(".")
        except UnicodeError:
            return None
        return ascii_v6 or None
    stripped = nfkc.rstrip(".")
    if not stripped:
        return None
    try:
        ascii_host = stripped.encode("idna").decode("ascii").lower().rstrip(".")
    except UnicodeError:
        return None
    if not ascii_host or any(ord(c) < 0x20 or ord(c) == 0x7F or c.isspace() for c in ascii_host):
        return None
    return ascii_host


def _is_non_canonical_ipv4_literal(host: str) -> bool:
    labels = host.split(".")
    return bool(labels) and all(_NON_CANONICAL_IPV4_LABEL_RE.fullmatch(label) for label in labels)


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
    packed = addr.packed
    if addr.ipv4_mapped is not None:
        embedded.append(addr.ipv4_mapped)
    if addr in _IPV4_TRANSLATED:
        embedded.append(ipaddress.IPv4Address(packed[12:16]))
    if addr in _IPV4_COMPATIBLE and addr not in (_IPV6_UNSPECIFIED, _IPV6_LOOPBACK):
        embedded.append(ipaddress.IPv4Address(packed[12:16]))
    if (packed[8] & 0xFC) == 0 and packed[9:12] == b"\x00\x5e\xfe":
        embedded.append(ipaddress.IPv4Address(packed[12:16]))
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
    normalized = _normalize_host(host)
    if normalized is None:
        return True
    if _is_restricted_hostname(normalized):
        return True
    bare = normalized.split("%", 1)[0]
    try:
        address = ipaddress.ip_address(bare)
    except ValueError:
        return _is_non_canonical_ipv4_literal(bare)
    return _is_restricted_ip(address)


def validate_webhook_url_syntax(url: str) -> str:
    """Validate webhook URL syntax and reject restricted hostnames/IPs (SSRF protection).

    Raises ``ValueError`` with a human-readable message when invalid.
    """
    if not isinstance(url, str):
        raise ValueError("Webhook URL must not be empty.")
    if _has_disallowed_url_chars(url):
        raise ValueError("Webhook URL contains disallowed characters.")
    cleaned = url.strip()
    if not cleaned:
        raise ValueError("Webhook URL must not be empty.")
    try:
        parsed = urlsplit(cleaned)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Webhook URL must use http or https scheme with a valid hostname and port.") from exc
    if port is not None and not (1 <= port <= 65535):
        raise ValueError("Webhook URL port must be between 1 and 65535.")
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("Webhook URL must use http or https scheme with a valid hostname.")
    host = _normalize_host(parsed.hostname)
    if host is None:
        raise ValueError("Webhook URL has an invalid hostname.")
    if _is_restricted_hostname(host):
        raise ValueError(f"Webhook target hostname '{host}' is not allowed (SSRF protection).")
    bare = host.split("%", 1)[0]
    try:
        ip = ipaddress.ip_address(bare)
    except ValueError:
        ip = None

    if ip is not None:
        if _is_restricted_ip(ip):
            raise ValueError(f"Webhook target IP '{host}' is in a restricted range (SSRF protection).")
    elif _is_non_canonical_ipv4_literal(bare):
        raise ValueError(f"Webhook target hostname '{host}' uses a disallowed IPv4 notation (SSRF protection).")
    return cleaned


@dataclass(frozen=True)
class PinnedWebhookDestination:
    url: str
    host: str
    sni_hostname: str | None


def validate_webhook_destination(url: str) -> PinnedWebhookDestination:
    """Resolve a webhook hostname immediately before delivery and reject restricted addresses."""
    if not isinstance(url, str) or _has_disallowed_url_chars(url):
        raise ValueError("invalid_webhook_url")
    cleaned = url.strip()
    try:
        parsed = urlsplit(cleaned)
        raw_port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid_webhook_url") from exc
    if raw_port is not None and not (1 <= raw_port <= 65535):
        raise ValueError("invalid_webhook_url")
    raw_host = parsed.hostname
    if parsed.scheme not in ("http", "https") or not raw_host:
        raise ValueError("invalid_webhook_url")
    host = _normalize_host(raw_host)
    if host is None or restricted_webhook_host(host):
        raise ValueError("restricted_webhook_host")
    port = raw_port or (443 if parsed.scheme == "https" else 80)
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
    parsed_port = f":{raw_port}" if raw_port is not None else ""
    pinned_url = parsed._replace(netloc=f"{literal}{parsed_port}").geturl()
    host_literal = f"[{host}]" if ":" in host else host
    host_header = host_literal + parsed_port
    return PinnedWebhookDestination(
        url=pinned_url,
        host=host_header,
        sni_hostname=host if parsed.scheme == "https" else None,
    )
