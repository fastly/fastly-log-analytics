"""CSPRNG opaque resource-naming helpers (SEC-22 / M5).

Generates DNS- and S3-valid random identifiers with 96 bits of entropy
(``secrets.token_hex(12)``, 24 lowercase hex chars) so system-generated
FOS buckets, CDN proxy domains, session-scorer Compute domains, and
Terraform defaults never embed ``service_id`` or predictable install names.
"""

from __future__ import annotations

import re
import secrets

OPAQUE_PREFIX = "fla"
OPAQUE_ENTROPY_BYTES = 12  # 96 bits → 24 lowercase hex chars
MAX_COLLISION_ATTEMPTS = 5
_MAX_DNS_LABEL_LEN = 63
_HEX_LEN = OPAQUE_ENTROPY_BYTES * 2


def generate_opaque_label(prefix: str = OPAQUE_PREFIX) -> str:
    """Return a DNS- and S3-valid opaque label ``<prefix>-<24-hex>`` (<= 63 chars)."""
    clean_prefix = re.sub(r"[^a-z0-9-]", "-", (prefix or OPAQUE_PREFIX).lower())
    clean_prefix = re.sub(r"-+", "-", clean_prefix).strip("-")
    if not clean_prefix or not clean_prefix[0].isalnum():
        clean_prefix = OPAQUE_PREFIX
    max_prefix_len = _MAX_DNS_LABEL_LEN - 1 - _HEX_LEN
    clean_prefix = clean_prefix[:max_prefix_len].rstrip("-") or OPAQUE_PREFIX
    return f"{clean_prefix}-{secrets.token_hex(OPAQUE_ENTROPY_BYTES)}"


def generate_opaque_bucket_name() -> str:
    """Return an S3- and DNS-compliant opaque FOS bucket name (28 chars)."""
    return generate_opaque_label(OPAQUE_PREFIX)


def generate_opaque_cdn_prefix() -> str:
    """Return an independent opaque ``.global.ssl.fastly.net`` subdomain prefix."""
    return generate_opaque_label(OPAQUE_PREFIX)


def generate_opaque_scorer_domain() -> str:
    """Return an opaque ``.edgecompute.app`` domain for a new session scorer."""
    return f"{generate_opaque_label(OPAQUE_PREFIX)}.edgecompute.app"


def is_domain_collision_error(exc: BaseException) -> bool:
    """Return True if *exc* looks like a Fastly domain-already-taken / 409 error."""
    msg = str(exc).lower()
    return (
        "http 409" in msg
        or "status 409" in msg
        or "409 conflict" in msg
        or "already taken" in msg
        or "already exists" in msg
        or "already owned" in msg
        or "domain is already" in msg
        or "duplicate record" in msg
    )


def is_bucket_collision_error(exc: BaseException) -> bool:
    """Return True if *exc* indicates an S3/FOS bucket name collision."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
        http_status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou", "409", "403", "Forbidden", "AccessDenied"):
            return True
        if http_status in (409, 403):
            return True
    msg = str(exc).lower()
    return (
        "bucketalreadyexists" in msg
        or "bucketalreadyownedbyyou" in msg
        or "http 409" in msg
        or "409 conflict" in msg
        or "already exists" in msg
    )
