"""URL → canonical route ID normalization and Layer 2 (route transition) scoring.

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

import math
import posixpath
import re
import sys as _sys
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
        if pat.fullmatch(segment):
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


# ── Layer 2 tuning constants (research doc §4.2) ──────────────────────────────

# Laplace (additive) smoothing factor. Larger = more conservative (every
# unseen transition gets more probability mass). 0.5 per the doc.
L2_LAPLACE_ALPHA: Final[float] = 0.5

# Skip-gram discount: a high-probability anchor→current transition counts
# slightly less than a direct prev→current high-probability transition.
L2_SKIPGRAM_BETA: Final[float] = 0.7


# Maps TransScore in [0, 1] → contribution in [0, 100]. We use a log-shaped
# transform so the score climbs sharply as transition probability drops
# below 1e-3 (the "almost certainly never happens in human traffic" floor).
# At P = 1.0 → 0. At P = 1e-3 → ~50. At P = 1e-6 → ~100.
def _l2_score_from_trans_prob(p: float) -> int:
    """Map a TransScore probability to a Layer 2 anomaly score [0, 100]."""
    if p >= 1.0:
        return 0
    if p <= 1e-12:
        return 100
    # -log10(p) maps p=1e-3 → 3, p=1e-6 → 6. Scale by 100/6 so p=1e-6 ≈ 100.
    raw = -math.log10(p) * (100.0 / 6.0)
    if raw < 0:
        return 0
    if raw > 100:
        return 100
    return int(round(raw))


# ── Layer 2: Route Transition Shield ──────────────────────────────────────────


def _transition_prob(matrix: dict, prev_route: str, current_route: str, vocab_size: int) -> float:
    """Laplace-smoothed P(current | prev) lookup from a serialized matrix.

    Matrix shape (matches what backend.scoring.matrix emits):
        {
          "counts": {prev_route: {current_route: count, ...}},
          "row_totals": {prev_route: total_out_count},
          "vocab_size": int,
          ...
        }

    Returns the smoothed conditional probability. Always > 0 (Laplace
    floor). An unseen prev route in a POPULATED matrix is penalized to the
    probability floor (→ max L2 score) rather than getting the lenient
    uniform prior — see the fail-closed note below."""
    counts = matrix.get("counts", {})
    row_totals = matrix.get("row_totals", {})
    # Fail closed on an unseen prev route. In a populated matrix a prev
    # route that was never a transition SOURCE isn't "neutral" — it's a
    # transition the model has never observed, which is the exact shape of
    # evasion (prepend an untracked route to dodge the shield). The old
    # uniform Laplace prior (1/vocab) mapped to a sub-threshold score, so
    # unseen-prev sequences slipped past enforcement. Return the floor so it
    # maps to the max L2 score. Only fires when the matrix HAS data; a truly
    # empty matrix still falls through to the uniform prior below (L2 self-disables).
    #
    # EC-04: "unseen" is decided on the row_total VALUE (<= 0), not mere key
    # presence — a prev route PRESENT in row_totals with total 0 (a curr-only
    # route: seen only as a destination, never a source) is still unobserved as a
    # transition source. This mirrors the Rust scorer (scorer.rs::transition_prob
    # gates on ``row_total(id) > 0``), so the train-side reference and the edge
    # agree on a key-present-zero prev. Unreachable from prod matrices (the
    # trainer always +1's a source's row_total), so this is behaviour-preserving.
    if row_totals and row_totals.get(prev_route, 0) <= 0:
        return 1e-12
    prev_row = counts.get(prev_route, {})
    numerator = prev_row.get(current_route, 0) + L2_LAPLACE_ALPHA
    denominator = row_totals.get(prev_route, 0) + L2_LAPLACE_ALPHA * vocab_size
    if denominator <= 0:
        # Truly empty matrix — return the uniform prior.
        return 1.0 / max(vocab_size, 1)
    return numerator / denominator


def score_layer2(
    matrix: dict | None,
    prev_route: Route | None,
    prev_anchor_route: Route | None,
    current_route: Route,
) -> tuple[int, list[str], float]:
    """Apply the L2 rules. Returns (score_contribution, reasons, trans_prob).

    - ``matrix`` is the serialized transition matrix; None → L2 disabled
      (matrix not yet trained).
    - ``prev_route`` is the immediate previous route this session visited
      (None on first request — L2 returns 0 with no reasons).
    - ``prev_anchor_route`` is the most-recent ANCHOR route (skipping
      auxiliary pages like /about-us) — used for the skip-gram lookback.
      Pass None to disable skip-gram; pass the same as prev_route to
      collapse skip-gram down to "look one step back" semantics.
    - ``current_route`` is the request being scored.
    """
    if matrix is None or prev_route is None:
        return 0, [], 1.0

    vocab_size = int(matrix.get("vocab_size", 0))
    if vocab_size <= 0:
        return 0, [], 1.0

    direct_p = _transition_prob(matrix, prev_route.path, current_route.path, vocab_size)
    if (
        prev_anchor_route is not None
        and prev_anchor_route.path != prev_route.path
        and prev_anchor_route.path in matrix.get("counts", {})
    ):
        anchor_p = _transition_prob(matrix, prev_anchor_route.path, current_route.path, vocab_size) * L2_SKIPGRAM_BETA
        trans_prob = max(direct_p, anchor_p)
    else:
        trans_prob = direct_p

    score = _l2_score_from_trans_prob(trans_prob)
    reasons = ["low-transition-prob"] if score >= 50 else []
    return score, reasons, trans_prob


_sys.modules.setdefault("backend.scoring.normalize", _sys.modules[__name__])
_sys.modules.setdefault("backend.scoring.scorer", _sys.modules[__name__])
