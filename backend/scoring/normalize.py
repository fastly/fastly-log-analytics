"""URL → canonical route ID normalization.

Per research doc §5.3: collapse high-cardinality path segments (numeric ids,
UUIDs, slugs that look like keys) into ``*`` placeholders so the transition
matrix doesn't blow up to one row per unique URL. Query strings are
discarded entirely (the scorer keys on path topology, not query
parameters).

The Rust port under ``compute/scorer/`` must produce identical output for
the same URL — these are the lookup keys for the matrix.

**ASCII-only cross-language parity contract (EC-02).** The train/score parity
guarantee holds for the ASCII subset only. This module lowercases with
``str.lower()`` and collapses ids with ``\\d`` (both full-Unicode), while the
Rust scorer uses ``to_ascii_lowercase`` / ``is_ascii_digit``; on NON-ASCII input
the two therefore diverge (``/CAF%C3%89`` → ``/café`` here vs ``/cafÉ`` at the
edge; Arabic-Indic digits collapse here but not there). This is a deliberate,
documented limit — the worst case is the edge treating an i18n route as novel
(an L2 accuracy loss, never a category-evasion bypass), and porting Unicode
folding into the Wasm would pull in unicode crates. The divergence classes are
pinned by ``tests/scoring/test_normalize_runtime_parity.py``; raw C0 control
chars (``\\t \\n \\r``) are NOT divergent — ``urlsplit`` strips them here and the
Rust ``normalize`` strips them too.

Categories: every normalized route also gets a top-level category tag
derived from its first path segment, used by Layer 2's category-level
backoff (§4.2). Categories are intentionally coarse: ``product``, ``cart``,
``account``, ``api``, etc. Add/edit the prefix → category map below as new
sections of the site appear.
"""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass
from typing import Final
from urllib.parse import unquote, urlsplit

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
    while url.startswith("//"):
        url = url[1:]
    # Do NOT replace %3F with ? before splitting — %3F in a URL path is a
    # literal path character per RFC 3986, not a query delimiter. Decoding
    # it before urlsplit lets an attacker hide path-traversal payloads
    # behind ``%3F`` (audit finding 012): the scorer would categorize
    # `/search%3F/../../etc/passwd` as a benign `/search` browse, while
    # the downstream backend processes the whole traversal.
    try:
        parts = urlsplit(url)
        return parts.path or "/"
    except ValueError:
        return "/"


def _looks_like_id(segment: str) -> bool:
    if not segment:
        return False
    for pat in _ID_PATTERNS:
        if pat.match(segment):
            return True
    return False


def _category_for(first_segment: str) -> str:
    return _CATEGORY_MAP.get(first_segment.lower(), "other")


def unquote_except_slash(s: str) -> str:
    """Decode all percent-encoded sequences in the string EXCEPT for encoded slashes
    (%2f / %2F). This ensures that encoded directory traversals (like %2e%2e)
    can be resolved by normpath, while encoded slashes are preserved as data."""
    # Split by %2f and %2F case-insensitively
    parts = re.split(r"(%2f|%2F)", s)
    # parts will be like [chunk, "%2f", chunk, "%2F", ...]
    # We only unquote chunks, leaving the delimiters intact
    decoded_parts = []
    for i, p in enumerate(parts):
        if i % 2 == 0:
            decoded_parts.append(unquote(p))
        else:
            decoded_parts.append(p)
    return "".join(decoded_parts)


def _unquote_until_stable(s: str, max_iter: int = 4) -> str:
    """Iteratively :func:`unquote_except_slash` until the string stops changing.

    Finding 011 (2026-06-15): a single unquote pass only peels one
    encoding layer, so a doubly-encoded traversal like
    ``/admin/%252e%252e/items`` survived the pre-normpath decode as
    ``/admin/%2e%2e/items`` — :func:`posixpath.normpath` saw it as a
    normal segment (no ``..``) and didn't resolve the traversal. The
    delayed per-segment unquote at the end of :func:`normalize` then
    decoded ``%2e%2e`` to ``..`` AFTER normpath, leaving the traversal
    embedded in the canonical path / leaking the wrong category tag.

    Iterating until fixed-point unwinds multi-level encoding *before*
    normpath sees the path, so attacker payloads converge to the
    actual literal characters and any ``..`` segments collapse
    normally. The ``%2F`` preservation contract from finding 014 is
    untouched — each iteration runs the same except-slash decoder.

    ``max_iter`` is a paranoid cap on pathological inputs (the encoder
    has a finite alphabet, so any well-formed input reaches a
    fixed-point in at most a handful of passes; the cap just keeps
    the loop bounded if someone feeds in a non-converging sequence).
    """
    for _ in range(max_iter):
        decoded = unquote_except_slash(s)
        if decoded == s:
            return decoded
        s = decoded
    return s


def normalize(url: str) -> Route:
    """Convert a raw URL into a canonical (route, category) pair.

    Examples (doc §5.3):
        /                                  → Route('/',                 'home')
        /items/10243                       → Route('/items/*',          'product')
        /users/drew/profile                → Route('/users/*/profile',  'account')
        /api/v2/orders/00000abc-...        → Route('/api/v2/orders/*',  'api')
        /search?q=red+shoes&page=2         → Route('/search',           'browse')
    """
    # 013/014/011: iteratively unquote everything EXCEPT encoded slashes
    # before normalization so that
    #   - encoded traversals (``%2e%2e``) AND multi-level-encoded variants
    #     (``%252e%252e``) are resolved to ``..`` before posixpath.normpath
    #     runs — finding 011 closed the prior single-unquote bypass.
    #   - encoded slashes (``%2F``) still cannot act as structural path
    #     separators — the per-iteration helper preserves them as data.
    path = posixpath.normpath(_unquote_until_stable(_strip_query(url)))
    # Treat the root specially — there's no segment to inspect, and the
    # category is unambiguously 'home'.
    if path in ("", "/"):
        return Route(path="/", category="home")

    # Split, normalize each segment, rejoin. Empty strings between
    # consecutive '/' or at the leading position drop out cleanly via the
    # filter; we re-prepend the leading '/' below.
    # 014: unquote individual segments after splitting by '/' to prevent
    # encoded slashes (%2F) from being treated as directory separators during
    # posixpath.normpath.
    raw_segments = [unquote(s) for s in path.split("/") if s != ""]
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
