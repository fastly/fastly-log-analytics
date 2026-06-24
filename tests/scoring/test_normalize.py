"""Tests for backend.scoring.normalize — URL → canonical (route, category).

Each ID-pattern collapse case is parametrized so adding new patterns later
is one-line additive. Category-mapping tests are split out so renames /
additions don't churn the unrelated route-collapse cases."""

from __future__ import annotations

import pytest

from backend.scoring.normalize import normalize

# ── Route normalization ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "url,expected_path",
    [
        # Doc §5.3 examples we CAN catch heuristically (id-like segments).
        ("/items/10243", "/items/*"),
        ("/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000", "/api/v2/orders/*"),
        # Trivial paths.
        ("/", "/"),
        ("", "/"),
        ("/home", "/home"),
        # Query strings always stripped.
        ("/search?q=red+shoes", "/search"),
        ("/search?q=x&page=2", "/search"),
        ("/items/42?ref=email", "/items/*"),
        # Numeric ids in middle and at end.
        ("/blog/12345", "/blog/*"),
        ("/orders/789/items/42", "/orders/*/items/*"),
        # UUID v4.
        ("/sessions/123e4567-e89b-12d3-a456-426614174000", "/sessions/*"),
        # 24-char hex hash (Mongo ObjectId).
        ("/jobs/64bc89ff1a2b3c4d5e6f7081", "/jobs/*"),
        # Prefixed ID like SKU-12345.
        ("/inventory/SKU-12345", "/inventory/*"),
        ("/orders/ORD-789-ABC", "/orders/*"),
        # Long opaque token (>= 20 chars alphanumeric).
        (
            "/oauth/callback/abcdef0123456789xyzwAA",
            "/oauth/callback/*",
        ),
        # Absolute URL.
        (
            "https://www.example.com/api/v1/users/777?token=abc",
            "/api/v1/users/*",
        ),
        # Trailing slash preserved (or rather: NOT preserved — split drops empty segs).
        ("/products/", "/products"),
        # Mixed case in non-id segments → lowercased for stable matrix keys.
        ("/Products/Foo", "/products/foo"),
    ],
)
def test_normalize_paths(url, expected_path):
    assert normalize(url).path == expected_path


def test_normalize_handles_double_slashes():
    """Multiple consecutive slashes collapse to a single boundary."""
    assert normalize("/foo//bar").path == "/foo/bar"


def test_normalize_does_not_collapse_short_alphanumeric():
    """Real route names that are short alphanumeric strings (e.g. /faq, /cart)
    must NOT be collapsed to '*'."""
    assert normalize("/faq").path == "/faq"
    assert normalize("/cart").path == "/cart"
    assert normalize("/api/v2").path == "/api/v2"  # 'v2' is too short for LONG_OPAQUE


def test_normalize_does_not_collapse_words_with_dashes():
    """Hyphenated route slugs ('/about-us', '/privacy-policy') stay intact —
    they're not key-like even though they have dashes."""
    assert normalize("/about-us").path == "/about-us"
    assert normalize("/privacy-policy").path == "/privacy-policy"


def test_normalize_known_limitation_word_like_user_ids():
    """KNOWN LIMITATION: ``/users/drew/profile`` (doc §5.3 example) is NOT
    collapsed because 'drew' is alphanumeric+short and indistinguishable
    from a real route name without per-site context. Per-service route
    templates are tracked under Phase B as a follow-up; for now the
    matrix carries one row per real user, and Laplace smoothing absorbs
    the cardinality."""
    r = normalize("/users/drew/profile")
    assert r.path == "/users/drew/profile"  # documents current behavior
    assert r.category == "account"  # but the category lookup still works


# ── Category mapping ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "url,expected_category",
    [
        ("/", "home"),
        ("/products/42", "product"),
        ("/items/99", "product"),
        ("/cart", "cart"),
        ("/checkout/step-1", "checkout"),
        ("/orders/789", "checkout"),
        ("/account/settings", "account"),
        ("/users/drew", "account"),
        ("/api/v2/orders/123", "api"),
        ("/graphql", "api"),
        ("/auth/login", "auth"),
        ("/login", "auth"),
        ("/admin/dashboard", "admin"),
        ("/blog/launch-post", "content"),
        ("/about-us", "other"),  # Not in the prefix map; bucketed as 'other'.
        ("/totally-unknown-section/foo", "other"),
    ],
)
def test_category_mapping(url, expected_category):
    assert normalize(url).category == expected_category


def test_category_derived_from_original_first_segment_not_star():
    """If the first segment were collapsed to '*' we'd lose category signal.
    Verify the category lookup happens before the collapse."""
    # A URL whose FIRST segment looks numeric would collapse to /*/foo,
    # and we have to decide what category that gets. Currently we lookup
    # against the original first segment text, so '12345' → 'other'.
    r = normalize("/12345/foo")
    assert r.path == "/*/foo"
    assert r.category == "other"


# ── Route dataclass invariants ───────────────────────────────────────────────


def test_route_is_hashable_for_use_as_dict_key():
    r1 = normalize("/items/42")
    r2 = normalize("/items/99")
    assert r1 == r2  # Same canonical path + category
    # Frozen dataclass → hashable → usable as matrix key.
    {r1: 1, r2: 2}  # noqa: B015  — just verifying no exception


def test_route_immutable():
    r = normalize("/foo")
    with pytest.raises(Exception):
        r.path = "/bar"  # type: ignore[misc]


def test_normalize_canonicalizes_percent_encoding_and_dot_segments():
    """Verify that percent-encoded characters and dot segments are canonicalized."""
    assert normalize("/%61pi/foo").path == "/api/foo"
    assert normalize("/%61pi/foo").category == "api"

    assert normalize("/./api/foo").path == "/api/foo"
    assert normalize("/./api/foo").category == "api"

    assert normalize("/api/../auth/login").path == "/auth/login"
    assert normalize("/api/../auth/login").category == "auth"


def test_normalize_encoded_dot_segments_do_not_traverse():
    """Unquote before normpath to evaluate percent-encoded traversals
    like ``%2e%2e`` and prevent category bypasses."""
    r = normalize("/admin/%2e%2e/items/foo")
    assert r.path == "/items/foo"
    assert r.category == "product"
    r = normalize("/admin/%2e%2e/%2e%2e/etc/passwd")
    assert r.path == "/etc/passwd"
    assert r.category == "other"


def test_normalize_double_slash_path_is_not_authority():
    """Regression for audit finding 018: ``urlsplit('//foo/bar')`` parses
    ``foo`` as a network location and returns ``/bar`` as the path, which
    let an attacker drop the first segment by prefixing the URL with a
    double-slash. _strip_query now flattens leading double-slashes first."""
    assert normalize("//admin/secret").path.startswith("/admin")
    assert normalize("//admin/secret").category == "admin"
    # Triple+ slashes get flattened too.
    assert normalize("///admin/secret").path.startswith("/admin")


def test_normalize_finding_012_encoded_query_does_not_truncate():
    """Verify that encoded query delimiters (%3F) are NOT treated as query
    separators before normalization — finding 012 demonstrated that the
    prior pre-split %3F → ? replacement let an attacker hide path-traversal
    payloads (e.g. ``/search%3F/../../etc/passwd``) behind a benign-looking
    prefix. The path now keeps the encoded character literally so downstream
    scoring sees the whole payload (unquoted at the per-segment unquote pass
    inside normalize)."""
    # Encoded ? becomes a literal ? in the segment after unquote (the
    # full string ends up as a single first-segment, hence the 'other'
    # category fallback).
    assert normalize("/search%3fq=red+shoes&page=2").path == "/search?q=red+shoes&page=2"
    assert normalize("/search%3Fq=red+shoes&page=2").category == "other"


def test_normalize_finding_014_encoded_slash_traversal_bypass():
    """Verify that encoded slashes (%2F) do not act as structural separators,
    and thus do not allow path-traversal bypasses (Finding 014)."""
    r = normalize("/auth/login%2F..%2F..%2Fproduct")
    assert r.path == "/auth/login/../../product"
    assert r.category == "auth"


def test_normalize_urlsplit_value_error_handling():
    """Verify that malformed URLs causing ValueError in urlsplit are gracefully
    handled and fallback to '/' (Finding 008-val)."""
    assert normalize("http://[example.com").path == "/"


def test_normalize_finding_011_double_encoded_traversal_resolves():
    """Finding 011 (2026-06-15): a single ``unquote_except_slash`` pass only
    peeled one encoding layer, so a doubly-encoded traversal like
    ``/admin/%252e%252e/items`` was decoded to ``/admin/%2e%2e/items``
    before posixpath.normpath ran — normpath saw it as a normal segment
    and didn't resolve. The delayed per-segment unquote at the end of
    normalize then decoded ``%2e%2e`` to ``..`` AFTER normpath, leaving
    the traversal embedded in the canonical path and tagging the route
    with the wrong category (``admin`` instead of the resolved target's
    category).

    Iterating until fixed-point unwinds the multi-level encoding *before*
    normpath, so ``/admin/%252e%252e/items`` correctly resolves to
    ``/items`` with category ``product``. The 014 ``%2F``-stays-as-data
    contract is preserved (each iteration runs the same except-slash
    decoder)."""
    r = normalize("/admin/%252e%252e/items")
    assert r.path == "/items", f"double-encoded traversal must resolve; got {r.path!r}"
    assert r.category == "product", (
        f"category must reflect the resolved target, not the pre-traversal path; got {r.category!r}"
    )

    # Triple-encoded variant — paranoid coverage that the loop converges
    # past two layers as well.
    r3 = normalize("/admin/%25252e%25252e/items")
    assert r3.path == "/items"
    assert r3.category == "product"

    # The 014 ``%2F``-as-data contract still holds — encoded slashes
    # decode but don't drive traversal resolution.
    r014 = normalize("/auth/login%2F..%2F..%2Fproduct")
    assert r014.path == "/auth/login/../../product"
    assert r014.category == "auth"


def test_normalize_finding_010_encoded_fragment_does_not_truncate():
    """Finding 010: ``_strip_query`` used to convert ``%23`` to ``#`` before
    ``urlsplit`` so an attacker could write ``/search%23/../../api/admin`` and
    have the scorer categorize it as a benign ``/search`` browse — the
    downstream origin still sees the encoded ``%23`` as a literal character
    and processes the full traversal. After the fix, the encoded fragment
    delimiter is preserved through ``urlsplit``, normpath collapses the
    traversal, and the final categorisation is whatever the resolved path
    is (``api`` in this case)."""
    r = normalize("/search%23/../../api/admin")
    assert r.path == "/api/admin", (
        f"encoded #-fragment must not truncate the path; normpath must collapse the traversal — got {r.path!r}"
    )
    assert r.category == "api"
