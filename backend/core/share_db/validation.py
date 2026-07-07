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

# Single source of truth for the IP-family field names. Used here as the
# response-side ``masked_keys`` and imported by
# ``backend.utils.remote_access`` as the analyst filter-lock set, so the two
# PII boundaries (mask-in-response and forbid-as-filter) can never drift.
# This module is dependency-free (stdlib only) precisely so security-critical
# callers can import it without dragging in the connection pool.
IP_FAMILY_KEYS = frozenset({"ip", "client_ip", "ip_address", "remote_addr"})

# Client-identifier columns that are NOT IP-shaped but are still per-client PII
# and must be redacted for a ``mask_ips`` analyst (same policy switch as IPs).
# ``cookie_session`` is a SHA-256 of the client's session cookie captured at the
# edge (Phase-4 Track C) — a stable pseudonymous session identifier that lets you
# correlate a single user's requests, so it is masked on the raw ``/logs`` and
# ``/query`` surfaces exactly like ``ip``. mask_ip() would fail-closed to
# "[redacted]" on a hash anyway; we redact explicitly so the intent is clear and
# doesn't depend on the value shape.
SESSION_ID_KEYS = frozenset({"cookie_session"})

# Non-word strip used to canonicalize an analyst-supplied field name to the real
# column it resolves to. MUST match the column resolution used by the query
# layer — ``_SAFE_COL_RE`` in ``backend/repositories/utils/filters.py`` and the
# ``clean_field`` alnum/underscore filter in the field-values path — so a
# junk-suffixed field ("ip.", "cookie_session ") is masked the same as its bare
# form. Duplicated (not imported) to keep this module dependency-free per the
# note above. Regression: tests/.../test_pii_policy* + security_regression.
_NONWORD_RE = re.compile(r"[^\w]")


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

    The only enforced control is ``mask_ips: bool``.

    ``mask_user_agent`` / ``mask_geo`` / ``redact_fields`` are NOT enforced
    anywhere. Rather than accept-and-store them — which let an operator enable
    "mask user agent" and believe PII was hidden when it wasn't — we reject any
    attempt to turn them on. Turning them OFF (or an empty ``redact_fields``)
    is a harmless no-op and accepted. Move each key into the enforced set here
    once its masking actually ships.
    """
    if policy is None:
        return {"mask_ips": False}
    if not isinstance(policy, dict):
        raise InvalidPiiPolicyError("pii_policy must be an object")
    out: dict[str, Any] = {"mask_ips": bool(policy.get("mask_ips", False))}
    for k in ("mask_user_agent", "mask_geo"):
        if policy.get(k):
            raise InvalidPiiPolicyError(f"pii_policy.{k} is not supported yet — it would not be enforced. Remove it.")
    rf = policy.get("redact_fields")
    if rf:
        # Validate the shape first so a malformed value gets the precise error,
        # then reject: redact_fields is accepted-but-ignored today, which is
        # exactly the silent no-op this guard exists to prevent.
        if not isinstance(rf, list) or not all(isinstance(x, str) for x in rf):
            raise InvalidPiiPolicyError("redact_fields must be a list of strings")
        raise InvalidPiiPolicyError(
            "pii_policy.redact_fields is not supported yet — it would not be enforced. Remove it."
        )
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
    # Fail closed: a value that doesn't cleanly parse as an IP (an XFF list
    # like "1.2.3.4, 5.6.7.8", a malformed octet, trailing whitespace, etc.)
    # must NOT be returned verbatim — that would leak the very PII this
    # control exists to mask. Empty / missing values stay empty (nothing to
    # leak, and "[redacted]" for a blank field is just noise).
    if not ip:
        return ip
    # Idempotency: a value already in masked IPv4 form ("a.b.c.xxx") is a
    # no-op. The /api/query path masks by value shape in the repo layer, and
    # the analyst-response middleware then runs the key-name masker over the
    # same body — without this guard a column literally named ``ip`` would be
    # double-masked from "1.2.3.xxx" to "[redacted]" (no leak, but uglier and
    # inconsistent with the analytics endpoints). The masked IPv6 form is a
    # valid address so it re-masks to itself already; only IPv4 needs the
    # short-circuit. ".xxx" is not a valid IP tail so this can't excuse a real
    # address.
    if ip.endswith(".xxx"):
        return ip
    try:
        addr = ipaddress.ip_address(ip)
    except (ValueError, TypeError):
        return "[redacted]"
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
    masked_keys = IP_FAMILY_KEYS

    def _walk(node, parent_key=None, field_ctx=None):
        if isinstance(node, dict):
            out = {}
            # A field-values response names its dimension column in a *sibling*
            # ``field`` key (``{"field": "cookie_session", "values":
            # [{"value": <hash>}]}``) rather than as the parent key the way the
            # dashboard top-N panels do (``data["cookie_session"]["top"][i]
            # ["value"]``). Resolve the owning-field context from that sibling so
            # the generic ``value``-cell masking below fires on the field-values
            # surface too — for BOTH IP fields (``field=ip`` enumerates raw
            # distinct IPs otherwise) and session fields. Falls back to the
            # parent-key-threaded ``field_ctx`` when there's no sibling ``field``
            # (the top-N shape), so that path is unchanged.
            declared_field = node.get("field")
            # Canonicalize the echoed field to the real column the query
            # resolved (field-values strips to alnum/underscore). WITHOUT this,
            # a junk-suffixed field ("ip.", "cookie_session ") echoes back
            # verbatim, fails the exact-match test below, and the value-cell
            # masking is skipped — leaking raw IPs / session hashes (adversarial
            # audit 2026-07-06). Defense-in-depth behind the analyst PII lock,
            # which now also rejects PII field-values enumeration at the boundary.
            canon_field = _NONWORD_RE.sub("", declared_field).lower() if isinstance(declared_field, str) else None
            eff_ctx = (
                canon_field
                if canon_field is not None and (canon_field in SESSION_ID_KEYS or canon_field in masked_keys)
                else field_ctx
            )
            for k, v in node.items():
                # Session-identifier columns (e.g. cookie_session) are per-client
                # PII but not IP-shaped — redact wholesale rather than IP-mask.
                if isinstance(v, str) and k in SESSION_ID_KEYS:
                    out[k] = "[redacted]" if v else v
                    continue
                if isinstance(v, str) and k in masked_keys:
                    out[k] = mask_ip(v)
                    continue
                # Dashboard top-N panels carry the dimension value under the
                # generic key ``value`` (``data["ip"]["top"][i]["value"]`` —
                # backend/repositories/dashboard.py:608/655), NOT under ``ip``,
                # so key-name masking misses them. When the owning field IS an IP
                # field — threaded via the parent key (top-N) OR resolved from a
                # sibling ``field`` key (field-values, via ``eff_ctx``) — value-
                # shape mask the cell so neither the Top IPs card nor a
                # ``field=ip`` field-values picker leaks raw client IPs to a
                # mask_ips analyst, while url/ua panels stay verbatim.
                if k == "value" and isinstance(v, str) and eff_ctx in masked_keys:
                    out[k] = _mask_ip_scalar(v)
                    continue
                # Same generic ``value`` cell, but the owning field is a session
                # id (top-N threads it via the parent key; field-values via the
                # sibling ``field`` key resolved into ``eff_ctx`` above). A hash
                # has no reliable value shape, so redact wholesale by field —
                # the analyst can alias the OUTPUT column but not the field this
                # top-N / picker is grouped by.
                if k == "value" and isinstance(v, str) and eff_ctx in SESSION_ID_KEYS:
                    out[k] = "[redacted]" if v else v
                    continue
                # Carry the owning-field context (IP or session) down through
                # intermediate keys (e.g. "top", "values") so the nested
                # ``value`` cell still sees it.
                next_ctx = k if (k in masked_keys or k in SESSION_ID_KEYS) else eff_ctx
                out[k] = _walk(v, parent_key=k, field_ctx=next_ctx)
            return out
        if isinstance(node, list):
            # Array fields inherit the parent dict key for masking — e.g.
            # ``{"client_ip": ["1.2.3.4", "5.6.7.8"]}`` must mask each string
            # the same way the scalar form would. Without threading the
            # parent key through, list-of-string IP fields slipped past the
            # masker entirely. ``field_ctx`` is carried so top-N rows nested in
            # a list under an IP field still mask their ``value`` cell.
            return [
                (
                    ("[redacted]" if x else x)
                    if isinstance(x, str) and parent_key in SESSION_ID_KEYS
                    else mask_ip(x)
                    if isinstance(x, str) and parent_key in masked_keys
                    else _walk(x, parent_key=parent_key, field_ctx=field_ctx)
                )
                for x in node
            ]
        return node

    return _walk(obj)


# Cheap pre-filter: only a string made entirely of hex digits, dots and
# colons can possibly be a bare IPv4/IPv6 literal. Rejects URLs, user-agents,
# country codes, JA4 fingerprints, hashes, etc. before the more expensive
# ``ipaddress`` parse — keeps the per-cell cost negligible on the large
# free-form result sets ``/api/query`` can return (up to MAX_QUERY_ROWS rows).
_MAYBE_IP_RE = re.compile(r"^[0-9A-Fa-f:.]+$")


def _mask_ip_scalar(value: str) -> str:
    """Mask ``value`` iff it is *shaped* like an IP (single address or XFF list).

    Unlike :func:`mask_ip` (which redacts whole fields and fails closed on
    non-IPs), this only touches cells that genuinely parse as an IP and leaves
    everything else verbatim — the right behavior when masking by value across
    columns whose names we don't control.

      * ``"1.2.3.4"`` / ``"2001:db8::1"`` → masked.
      * ``"1.2.3.4, 5.6.7.8"`` (XFF list, every element an IP) → each masked.
      * any string with a non-IP element → returned unchanged.
    """
    if _MAYBE_IP_RE.match(value):
        try:
            ipaddress.ip_address(value)
        except ValueError:
            pass
        else:
            return mask_ip(value)
    # XFF-style list: mask only when EVERY comma-separated element is an IP.
    # mask_ip's own fail-closed branch can't help here (the whole string isn't
    # an IP), so we split and verify before masking — a list with any non-IP
    # token is left untouched rather than partially masked.
    if "," in value:
        parts = [p.strip() for p in value.split(",")]
        if len(parts) > 1 and all(parts):
            masked: list[str] = []
            for p in parts:
                try:
                    ipaddress.ip_address(p)
                except ValueError:
                    return value
                masked.append(mask_ip(p))
            return ", ".join(masked)
    return value


def mask_ip_values(obj):
    """Recursively mask every IP-shaped string value in ``obj``, by VALUE.

    Value-shape masking for the free-form ``/api/query`` surface. There the
    analyst names the output columns, so the key-name masker
    (:func:`apply_pii_policy`) is trivially bypassed by aliasing
    (``SELECT ip AS addr``). This walker masks any cell that parses as an IP
    regardless of its column name; non-IP strings pass through untouched.

    NOTE: output-side masking on a free-form SQL surface is inherently
    incomplete — an analyst can still defeat it with string manipulation
    (``SELECT 'x' || ip``). It closes the trivial alias bypass; the fully
    robust control would mask at the data source. See the H1 time-window
    rebind in ``backend/repositories/query.py`` for where source-side masking
    would hook in if we tighten this further.
    """
    if isinstance(obj, dict):
        return {k: mask_ip_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [mask_ip_values(x) for x in obj]
    if isinstance(obj, str):
        return _mask_ip_scalar(obj)
    return obj
