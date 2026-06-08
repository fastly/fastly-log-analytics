"""URL → canonical route ID normalization.

Per research doc §5.3: collapse high-cardinality path segments (numeric ids,
UUIDs, slugs that look like keys) into ``*`` placeholders so the transition
matrix doesn't blow up to one row per unique URL. Query strings are
discarded entirely (the scorer keys on path topology, not query
parameters).

The Rust port under ``compute/scorer/`` must produce identical output for
the same URL — these are the lookup keys for the matrix.

Categories: every normalized route also gets a top-level category tag
derived from its first path segment, used by Layer 2's category-level
backoff (§4.2). Categories are intentionally coarse: ``product``, ``cart``,
``account``, ``api``, etc. Add/edit the prefix → category map below as new
sections of the site appear.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final
from urllib.parse import urlsplit

# A segment is "id-like" — and therefore gets collapsed to '*' — if it matches
# any of these. Order matters only when patterns overlap; current set is
# mutually exclusive.
_NUMERIC_ID = re.compile(r"^\d+$")
_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
# 24+ hex chars (common for content hashes / Mongo ObjectId variants).
_HEX_HASH = re.compile(r"^[0-9a-fA-F]{24,}$")
# Common ID prefixes: SKU-12345, ORD-XYZ-789, ASIN-B07X, etc. — leading
# ALL-CAPS-WITH-DASHES followed by alphanumerics.
_PREFIXED_ID = re.compile(r"^[A-Z]{2,5}[-_][A-Za-z0-9_-]+$")
# Long alphanumeric tokens that almost certainly aren't navigation (e.g.
# session-id-looking strings in path). Keep this conservative — false-
# positives here collapse real route names.
_LONG_OPAQUE = re.compile(r"^[A-Za-z0-9_-]{20,}$")

_ID_PATTERNS = (_NUMERIC_ID, _UUID, _HEX_HASH, _PREFIXED_ID, _LONG_OPAQUE)

# First-path-segment → category. Anything not listed → "other".
# Tuned to be additive: new buckets only widen the L2 backoff coverage,
# they never reclassify existing routes.
_CATEGORY_MAP: Final[dict[str, str]] = {
    "": "home",
    "api": "api",
    "graphql": "api",
    "products": "product",
    "product": "product",
    "items": "product",
    "p": "product",
    "categories": "browse",
    "category": "browse",
    "search": "browse",
    "browse": "browse",
    "cart": "cart",
    "basket": "cart",
    "checkout": "checkout",
    "pay": "checkout",
    "order": "checkout",
    "orders": "checkout",
    "account": "account",
    "user": "account",
    "users": "account",
    "profile": "account",
    "settings": "account",
    "auth": "auth",
    "login": "auth",
    "signin": "auth",
    "signup": "auth",
    "register": "auth",
    "logout": "auth",
    "admin": "admin",
    "static": "asset",
    "assets": "asset",
    "blog": "content",
    "news": "content",
    "about": "content",
    "help": "content",
    "support": "content",
    "privacy": "content",
    "terms": "content",
    "faq": "content",
}


@dataclass(frozen=True)
class Route:
    """Canonical normalized route plus its category tag.

    ``path`` is the lookup key for the transition matrix.
    ``category`` is the L2 category-backoff key (§4.2)."""

    path: str
    category: str


def _strip_query(url: str) -> str:
    """Return just the path component of a URL. Handles both relative
    (``/foo/bar?x=1``) and absolute (``https://h/foo/bar?x=1``) inputs."""
    parts = urlsplit(url)
    return parts.path or "/"


def _looks_like_id(segment: str) -> bool:
    if not segment:
        return False
    for pat in _ID_PATTERNS:
        if pat.match(segment):
            return True
    return False


def _category_for(first_segment: str) -> str:
    return _CATEGORY_MAP.get(first_segment.lower(), "other")


def normalize(url: str) -> Route:
    """Convert a raw URL into a canonical (route, category) pair.

    Examples (doc §5.3):
        /                                  → Route('/',                 'home')
        /items/10243                       → Route('/items/*',          'product')
        /users/drew/profile                → Route('/users/*/profile',  'account')
        /api/v2/orders/00000abc-...        → Route('/api/v2/orders/*',  'api')
        /search?q=red+shoes&page=2         → Route('/search',           'browse')
    """
    path = _strip_query(url)
    # Treat the root specially — there's no segment to inspect, and the
    # category is unambiguously 'home'.
    if path in ("", "/"):
        return Route(path="/", category="home")

    # Split, normalize each segment, rejoin. Empty strings between
    # consecutive '/' or at the leading position drop out cleanly via the
    # filter; we re-prepend the leading '/' below.
    raw_segments = [s for s in path.split("/") if s != ""]
    if not raw_segments:
        return Route(path="/", category="home")

    normalized: list[str] = []
    for seg in raw_segments:
        normalized.append("*" if _looks_like_id(seg) else seg.lower())

    canonical = "/" + "/".join(normalized)
    # Category from the FIRST segment of the original (lowercased) — never
    # from a "*" placeholder, since that would obliterate the signal.
    first = raw_segments[0].lower()
    category = _category_for(first)
    return Route(path=canonical, category=category)
