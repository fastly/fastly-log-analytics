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
    """Regression for audit finding 017: an early unconditional unquote()
    let a caller smuggle ``..`` via ``%2e%2e`` and escape the route. With
    unquote applied per-segment AFTER normpath, ``%2e%2e`` survives as a
    literal segment name and the route stays anchored to its real prefix."""
    r = normalize("/admin/%2e%2e/items/foo")
    # path stays under /admin (no traversal); the original encoded segment
    # is decoded in place, not collapsed away
    assert r.path.startswith("/admin/")
    assert r.category == "admin"
    r = normalize("/admin/%2e%2e/%2e%2e/etc/passwd")
    assert r.path.startswith("/admin/")
    assert r.category == "admin"


def test_normalize_double_slash_path_is_not_authority():
    """Regression for audit finding 018: ``urlsplit('//foo/bar')`` parses
    ``foo`` as a network location and returns ``/bar`` as the path, which
    let an attacker drop the first segment by prefixing the URL with a
    double-slash. _strip_query now flattens leading double-slashes first."""
    assert normalize("//admin/secret").path.startswith("/admin")
    assert normalize("//admin/secret").category == "admin"
    # Triple+ slashes get flattened too.
    assert normalize("///admin/secret").path.startswith("/admin")
