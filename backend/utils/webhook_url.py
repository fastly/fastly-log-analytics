"""Outbound webhook URL validation and connection-time address checks."""

from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit


def restricted_webhook_host(host: str) -> bool:
    host = host.lower().rstrip(".")
    if host in {
        "localhost",
        "localhost.localdomain",
        "metadata.google.internal",
        "metadata",
        "ip6-localhost",
        "ip6-loopback",
    }:
        return True
    if host.endswith(".internal") or host.endswith(".local"):
        return True
    try:
        addresses = [ipaddress.ip_address(host)]
    except ValueError:
        return False
    return any(
        not address.is_global
        or address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
        for address in addresses
    )


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
