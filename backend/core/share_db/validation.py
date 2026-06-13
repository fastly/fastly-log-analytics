"""Generic input validators + PII masking helpers used by the share flow.

Self-contained — no DB access — so other layers (routers, middleware) can
import this module without dragging in the connection pool.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Any

# Conservative ASCII-leaning name regex. Refuses HTML special chars
# (<, >, &, ", '), NULL bytes, and control characters. Allows international
# letters, digits, spaces, periods, commas, apostrophes, hyphens.
_NAME_RE = re.compile(r"^[\w .,'\-]{1,80}$", re.UNICODE)
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


class InvalidNameError(ValueError):
    pass


class InvalidEmailError(ValueError):
    pass


class InvalidPiiPolicyError(ValueError):
    pass


def validate_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        raise InvalidNameError("name is required")
    # Reject HTML metacharacters that have no business in a person's name.
    # Straight apostrophes are KEPT so Irish/Italian/Polynesian names work
    # (O'Brien, D'Angelo, Le'aupepe). React + the backend never interpolate
    # these into raw HTML attributes; they go through proper escaping.
    if "<" in name or ">" in name or "&" in name or '"' in name:
        raise InvalidNameError("name contains disallowed characters (HTML special characters not permitted)")
    if "\x00" in name or any(ord(c) < 32 for c in name):
        raise InvalidNameError("name contains control characters")
    if not _NAME_RE.match(name):
        raise InvalidNameError(
            "name must be 1-80 characters; letters, digits, spaces, periods, commas, apostrophes, hyphens only"
        )
    return name


def validate_email(email: str) -> str:
    email = (email or "").strip().lower()
    if not _EMAIL_RE.match(email):
        raise InvalidEmailError("email is not in a valid format")
    return email


def validate_pii_policy(policy: dict | None) -> dict:
    """Coerce + validate the PII policy dict.

    Today's only known key is ``mask_ips: bool``. Unknown keys are dropped
    with a debug log (forward-compatibility: new fields are added here, never
    rejected silently).
    """
    if policy is None:
        return {"mask_ips": False}
    if not isinstance(policy, dict):
        raise InvalidPiiPolicyError("pii_policy must be an object")
    out: dict[str, Any] = {"mask_ips": bool(policy.get("mask_ips", False))}
    # Reserved future keys — accept now so old clients don't break later.
    for k in ("mask_user_agent", "mask_geo"):
        if k in policy:
            out[k] = bool(policy[k])
    if "redact_fields" in policy:
        rf = policy["redact_fields"]
        if not isinstance(rf, list) or not all(isinstance(x, str) for x in rf):
            raise InvalidPiiPolicyError("redact_fields must be a list of strings")
        out["redact_fields"] = rf
    return out


def parse_ip_whitelist(s: str | None) -> list[str]:
    """Parse a comma-separated list of IPs/CIDRs; validates each entry.

    Returns the list of normalized entries. Raises ``ValueError`` on any
    malformed entry.
    """
    if not s or not s.strip():
        return []
    out: list[str] = []
    for raw in s.split(","):
        item = raw.strip()
        if not item:
            continue
        try:
            if "/" in item:
                net = ipaddress.ip_network(item, strict=False)
                out.append(str(net))
            else:
                ip = ipaddress.ip_address(item)
                out.append(str(ip))
        except ValueError as exc:
            raise ValueError(f"invalid IP/CIDR entry {item!r}: {exc}") from exc
    return out


def ip_in_whitelist(ip: str, whitelist_csv: str | None) -> bool:
    """True iff ``ip`` is permitted by the comma-separated whitelist.

    Empty / None whitelist allows all (existing call sites encode "no
    restriction" as NULL on the invite row).
    """
    if not whitelist_csv:
        return True
    try:
        client = ipaddress.ip_address(ip)
    except ValueError:
        return False
    for raw in whitelist_csv.split(","):
        item = raw.strip()
        if not item:
            continue
        try:
            if "/" in item:
                net = ipaddress.ip_network(item, strict=False)
                if client in net:
                    return True
            else:
                if client == ipaddress.ip_address(item):
                    return True
        except ValueError:
            continue
    return False


def mask_ip(ip: str) -> str:
    """Mask the final octet of IPv4, last 80 bits of IPv6.

    Used by the middleware when ``session.pii_policy.mask_ips`` is True.
    """
    try:
        addr = ipaddress.ip_address(ip)
    except (ValueError, TypeError):
        return ip
    if isinstance(addr, ipaddress.IPv4Address):
        parts = str(addr).split(".")
        return ".".join(parts[:3] + ["xxx"])
    # IPv6: keep first 48 bits, zero the rest.
    packed = bytearray(addr.packed)
    for i in range(6, 16):
        packed[i] = 0
    return str(ipaddress.IPv6Address(bytes(packed)))


def apply_pii_policy(obj, policy: dict):
    """Walk a JSON-serialisable object, masking by policy.

    Today: ``mask_ips`` masks anything that string-parses as an IP in fields
    named ``ip``, ``ip_address``, ``client_ip``, ``remote_addr``.
    """
    if not policy or not policy.get("mask_ips"):
        return obj
    masked_keys = {"ip", "ip_address", "client_ip", "remote_addr"}

    def _walk(node, parent_key=None):
        if isinstance(node, dict):
            return {
                k: (mask_ip(v) if isinstance(v, str) and k in masked_keys else _walk(v, parent_key=k))
                for k, v in node.items()
            }
        if isinstance(node, list):
            # Array fields inherit the parent dict key for masking — e.g.
            # ``{"client_ip": ["1.2.3.4", "5.6.7.8"]}`` must mask each string
            # the same way the scalar form would. Without threading the
            # parent key through, list-of-string IP fields slipped past the
            # masker entirely.
            return [
                (mask_ip(x) if isinstance(x, str) and parent_key in masked_keys else _walk(x, parent_key=parent_key))
                for x in node
            ]
        return node

    return _walk(obj)
