from __future__ import annotations

"""Tests for backend.scoring.normalize — URL → canonical (route, category).

Each ID-pattern collapse case is parametrized so adding new patterns later
is one-line additive. Category-mapping tests are split out so renames /
additions don't churn the unrelated route-collapse cases."""

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


# ── Merged from test_normalize_runtime_parity.py ──────────────────────────────────────────────────────

"""Runtime Py↔Rust normalize differential parity (toolchain-gated).

Complements :mod:`tests.scoring.test_normalize_parity`, which compares the two
GOLDEN *tables* statically (no toolchain) but never executes the Rust normalizer
on arbitrary input. This test compiles ``compute/scorer/src/normalize.rs``
natively (``rustc``; the module is pure-``std``) and runs BOTH normalizers over a
shared corpus, asserting byte-identical ``(path, category)`` where they MUST
agree and PINNING the known, documented divergence classes where they don't.

Why this exists (Edge/Compute pre-release audit): the matrix is TRAINED with the
Python normalizer and SCORED with the Rust one, so any URL that normalizes
differently is a silent train/score *key* mismatch. A runtime differential over
a fuzz corpus surfaced four divergence classes the table-only test cannot catch:

  1. Unicode lowercasing — Python ``str.lower()`` (full Unicode) vs Rust
     ``to_ascii_lowercase()`` (ASCII only). ``/CAF%C3%89`` → ``/café`` (Py) vs
     ``/cafÉ`` (Rust). DOCUMENTED divergence under the ASCII-only normalization
     contract (EC-02; see both normalizers' module docstrings).
  2. Unicode decimal digits — Python ``\\d`` matches Unicode Nd, Rust
     ``is_ascii_digit`` does not. ``/items/%D9%A1%D9%A2%D9%A3`` → ``/items/*``
     (Py) vs ``/items/١٢٣`` (Rust). Same ASCII-only contract.
  3. ``scheme:opaque`` (``mailto:``/``tel:``) — Python ``urlsplit`` drops the
     scheme, Rust keeps it. ``mailto:foo`` → ``/foo`` (Py) vs ``/mailto:foo``
     (Rust). Unreachable at the edge (request targets start with ``/``).

RESOLVED (EC-02): raw C0 control chars (``\\t \\n \\r``) used to be the only
*category* flip — Python ``urlsplit`` stripped them, Rust kept them
(``/adm\\tin`` → ``admin`` Py vs ``other`` Rust). The Rust ``normalize`` now
strips them too, so the two AGREE; ``test_raw_control_chars_now_agree`` pins the
fix (it trips if the strip is removed on either side).

The agreement block enforces parity on the ASCII-printable, non-control,
non-``scheme:`` subset (where the two genuinely must agree); a NEW pure-ASCII
divergence there would be a real regression. The divergence block pins the
remaining DOCUMENTED non-ASCII / scheme:opaque classes: if a one-sided change
lands (a real fix OR a brand-new divergence), the pin trips and forces both
normalizers + the parity doc to be updated together.

Skipped when ``rustc`` is unavailable so the default toolchain-free suite stays
green; runs locally and in the Scorer CI job where Rust is present.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

_RUSTC = shutil.which("rustc")
_NORMALIZE_RS = Path(__file__).resolve().parents[2] / "compute" / "scorer" / "src" / "normalize.rs"

# A thin wrapper that pulls in the REAL edge normalizer (pure std; #[cfg(test)]
# is excluded without --test). I/O is HEX-framed so URLs/outputs containing
# control chars (\t \n \r) or non-ASCII bytes survive the line protocol intact —
# a raw newline-delimited protocol silently mangles exactly the inputs this test
# needs to probe. One hex line in per url → one "hex(path)\thex(category)" out.
_WRAPPER_SRC = """
#[path = "{rs}"]
mod normalize;
use std::io::{{self, BufRead, Write}};
fn unhex(s: &str) -> Vec<u8> {{
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    let mut i = 0;
    while i + 1 < b.len() {{
        let hi = (b[i] as char).to_digit(16).unwrap();
        let lo = (b[i + 1] as char).to_digit(16).unwrap();
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }}
    out
}}
fn hex(b: &[u8]) -> String {{
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {{
        s.push_str(&format!("{{:02x}}", x));
    }}
    s
}}
fn main() {{
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    for line in stdin.lock().lines() {{
        let line = match line {{ Ok(l) => l, Err(_) => break }};
        let url_bytes = unhex(line.trim());
        let url = String::from_utf8_lossy(&url_bytes);
        let r = normalize::normalize(&url);
        writeln!(out, "{{}}\\t{{}}", hex(r.path.as_bytes()), hex(r.category.as_bytes())).unwrap();
    }}
}}
"""

# ── Agreement corpus: ASCII-printable, no raw C0 control bytes, no scheme:opaque.
# This is the subset where the two normalizers MUST produce identical output.
# Picks hit the id-collapse boundaries, traversal/encoding, %2F-as-data, and the
# embedded-scheme (finding-023) protection — the surface most likely to drift.
_AGREEMENT_URLS = [
    "/",
    "/home",
    "/Products/Foo",
    "/items/10243",
    "/items/42?ref=email",
    "/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000",
    "/api/v2/orders/00000ABC-1234-5678-9ABC-DEADBEEF0000",  # uppercase-hex uuid
    "/jobs/64bc89ff1a2b3c4d5e6f7081",  # 24-hex hash
    "/inventory/SKU-12345",
    "/orders/ORD-789-ABC",
    "/oauth/callback/abcdef0123456789xyzwAA",  # long-opaque >= 20
    "/api/v2",  # too short for long-opaque
    "/about-us",
    "/privacy-policy",
    "/static/../admin",
    "/a/./b/../c",
    "/a/b/%2e%2e%2f%2e%2e%2fc",
    "/admin/%2e%2e/items/foo",
    "/admin/%252e%252e/items",  # double-encoded traversal
    "/auth/login%2F..%2F..%2Fproduct",  # %2F-as-data (finding 014)
    "/items/%2e%2e%2fsecret",
    "/a%2Fb",
    "/%61dmin",
    "/foo//bar",
    "//api/x",
    "///a//b",
    "/admin/delete/http://x/",  # embedded-scheme finding-023
    "/api/v2/orders/file://x/",
    "https://www.example.com/api/v1/users/777?token=abc",
    "ftp://h/a/b",
    "/search?q=red+shoes&page=2",
    "/AB-",  # prefixed-id boundary: empty suffix → not an id
    "/ABCDEF-123",  # 6-char prefix → not prefixed-id
    "/A1-bc",  # digit in prefix → not prefixed-id
    "/AB_CD-EF",  # first separator is '_'
    "/users/drew/profile",  # known v1 limitation (no auto-collapse)
    "/checkout/step-1",
    "/blog/12345",
    "/orders/789/items/42",
    "/cart",
    "/faq",
    "/zzz-unknown",
    "/foo%00bar",  # NUL is not a C0 strip char for urlsplit; both keep it
    "/a b/c",  # space inside a segment (not leading)
]

# ── Divergence corpus: (url, why) — pinned as CURRENTLY divergent. If a fix
# lands (Rust aligned to Python) OR a new divergence appears, the pin trips.
_KNOWN_DIVERGENT = [
    ("/CAF%C3%89", "unicode-lowercase: É lowercased by Python only"),
    ("/%C3%89/%C3%89", "unicode-lowercase"),
    ("/items/%D9%A1%D9%A2%D9%A3", "unicode-digit: Arabic-Indic ١٢٣ collapse Py-only"),
    ("/%D9%A1", "unicode-digit"),
    ("mailto:foo", "scheme:opaque stripped by Python urlsplit only"),
    ("tel:+1234", "scheme:opaque"),
]

# ── Control-char corpus: raw C0 (\t \n \r). EC-02 made BOTH sides strip these
# (Python via urlsplit, Rust in normalize), so they now AGREE. Kept as a pin so
# removing the strip on either side re-opens the category flip and trips
# test_raw_control_chars_now_agree.
_CONTROL_CHARS = [
    "/adm\tin",
    "/admin\t",
    "/admin\n",
    "/admin\r",
    "/che\tckout",
]


def _run_rust(urls: list[str], tmp_path: Path) -> list[tuple[str, str]]:
    """Compile normalize.rs natively and return [(path, category)] per url."""
    assert _NORMALIZE_RS.is_file(), f"normalizer not found: {_NORMALIZE_RS}"
    src = tmp_path / "rust_norm.rs"
    # Escape backslashes for the Rust string literal path (Windows-safe; on
    # posix the path has none, but be defensive).
    src.write_text(_WRAPPER_SRC.format(rs=str(_NORMALIZE_RS).replace("\\", "\\\\")))
    binary = tmp_path / "rust_norm"
    compile_proc = subprocess.run(
        [_RUSTC, "--edition", "2021", "-O", str(src), "-o", str(binary)],
        capture_output=True,
        text=True,
    )
    if compile_proc.returncode != 0:
        pytest.skip(f"rustc could not build normalize.rs wrapper:\n{compile_proc.stderr}")
    # Hex-frame each url (one per line); decode hex(path)\thex(category) back out.
    stdin = "\n".join(u.encode("utf-8", "surrogatepass").hex() for u in urls) + "\n"
    run = subprocess.run([str(binary)], input=stdin, capture_output=True, text=True)
    assert run.returncode == 0, f"rust normalizer crashed: {run.stderr}"
    out = [ln for ln in run.stdout.split("\n") if ln]
    assert len(out) == len(urls), f"expected {len(urls)} rows, got {len(out)}"
    rows: list[tuple[str, str]] = []
    for line in out:
        hpath, _, hcat = line.partition("\t")
        path = bytes.fromhex(hpath).decode("utf-8", "replace")
        cat = bytes.fromhex(hcat).decode("utf-8", "replace")
        rows.append((path, cat))
    return rows


@pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")
def test_python_rust_agree_on_ascii_subset(tmp_path):
    """On the ASCII-printable, non-control, non-scheme:opaque subset the two
    normalizers MUST be byte-identical (train key == score key). A failure here
    is a real, security-relevant divergence (the edge would key a route the
    trainer keyed differently) — NOT an expected i18n/control-char case."""
    rust = _run_rust(_AGREEMENT_URLS, tmp_path)
    mismatches = []
    for url, (rp, rc) in zip(_AGREEMENT_URLS, rust):
        pr = normalize(url)
        if (pr.path, pr.category) != (rp, rc):
            mismatches.append((url, (pr.path, pr.category), (rp, rc)))
    assert not mismatches, (
        "Py↔Rust normalize divergence on the ASCII subset (each row is "
        "url, python(path,cat), rust(path,cat)):\n" + "\n".join(f"  {m}" for m in mismatches)
    )


@pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")
def test_known_divergence_classes_still_diverge(tmp_path):
    """Pin the documented non-ASCII / scheme:opaque divergences as CURRENTLY
    real. If a one-sided change aligns Rust to Python (a fix) or introduces a
    new shape, this trips — update both normalizers + test_normalize_parity.py
    GOLDEN + the parity doc together (never re-baseline one side)."""
    urls = [u for u, _why in _KNOWN_DIVERGENT]
    rust = _run_rust(urls, tmp_path)
    still_same = []
    for (url, why), (rp, rc) in zip(_KNOWN_DIVERGENT, rust):
        pr = normalize(url)
        if (pr.path, pr.category) == (rp, rc):
            still_same.append((url, why, (pr.path, pr.category)))
    assert not still_same, (
        "A known Py↔Rust divergence has CLOSED (Rust now matches Python). "
        "Good — but the pin is stale: remove these rows here and reflect the "
        "fix in test_normalize_parity.py + the parity doc:\n" + "\n".join(f"  {s}" for s in still_same)
    )


@pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")
def test_raw_control_chars_now_agree(tmp_path):
    """EC-02: raw \\t/\\n/\\r are stripped on BOTH sides (Python urlsplit; Rust
    normalize), so the previous category flip (admin → other at the edge) is
    closed. Pin the AGREEMENT — both produce the control-char-free (path,
    category) — so removing the strip on either side trips this test."""
    rust = _run_rust(_CONTROL_CHARS, tmp_path)
    expected = {
        "/adm\tin": ("/admin", "admin"),
        "/admin\t": ("/admin", "admin"),
        "/admin\n": ("/admin", "admin"),
        "/admin\r": ("/admin", "admin"),
        "/che\tckout": ("/checkout", "checkout"),
    }
    for url, (rp, rc) in zip(_CONTROL_CHARS, rust):
        pr = normalize(url)
        want_path, want_cat = expected[url]
        # Python strips the control char (urlsplit).
        assert (pr.path, pr.category) == (want_path, want_cat), (
            f"{url!r}: Python normalize drifted → {(pr.path, pr.category)!r}"
        )
        # Rust now strips it too → byte-identical to Python (no more flip).
        assert (rp, rc) == (want_path, want_cat), (
            f"{url!r}: Rust no longer strips the control char (got {(rp, rc)!r}); the EC-02 fix regressed."
        )


# ── Merged from tests/scoring/test_normalize_parity.py ──

import re

from backend.scoring.normalize import _CATEGORY_MAP

_RUST_NORMALIZE = _NORMALIZE_RS

GOLDEN: list[tuple[str, str, str, str, str]] = [
    ("/", "/", "home", "/", "home"),
    ("/items/10243", "/items/*", "product", "/items/*", "product"),
    (
        "/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000",
        "/api/v2/orders/*",
        "api",
        "/api/v2/orders/*",
        "api",
    ),
    ("/%61dmin", "/admin", "admin", "/admin", "admin"),
    ("/static/../admin", "/admin", "admin", "/admin", "admin"),
    ("/admin/%2e%2e/items/foo", "/items/foo", "product", "/items/foo", "product"),
    ("/search?q=red+shoes&page=2", "/search", "browse", "/search", "browse"),
    ("/auth/login%2F..%2F..%2Fproduct", "/auth/login/../../product", "auth", "/auth/login/../../product", "auth"),
    ("/items/%2e%2e%2fsecret", "/items/../secret", "product", "/items/../secret", "product"),
    ("/a/b/%2e%2e%2f%2e%2e%2fc", "/a/b/../../c", "other", "/a/b/../../c", "other"),
]

KNOWN_DIVERGENCES: set[str] = set()

CATEGORY_GOLDEN: list[tuple[str, str]] = [
    ("/", "home"),
    ("/api/status", "api"),
    ("/products/widget", "product"),
    ("/search", "browse"),
    ("/cart", "cart"),
    ("/checkout/step-1", "checkout"),
    ("/account/settings", "account"),
    ("/login", "auth"),
    ("/admin/users", "admin"),
    ("/static/app.js", "asset"),
    ("/blog/post", "content"),
    ("/zzz-unknown", "other"),
]


@pytest.mark.parametrize("url,py_path,py_cat,_rust_path,_rust_cat", GOLDEN)
def test_python_normalize_matches_golden(url, py_path, py_cat, _rust_path, _rust_cat):
    """Pin the Python normalizer's output for every golden URL."""
    r = normalize(url)
    assert r.path == py_path, f"{url!r}: path drifted {r.path!r} != {py_path!r}"
    assert r.category == py_cat, f"{url!r}: category drifted {r.category!r} != {py_cat!r}"


@pytest.mark.parametrize("url,category", CATEGORY_GOLDEN)
def test_python_category_golden(url, category):
    """Pin the Python category bucket for one URL per CATEGORY_MAP value."""
    assert normalize(url).category == category, f"{url!r}: category drifted {normalize(url).category!r} != {category!r}"


def test_divergence_classification_is_honest():
    """The AGREE/DIVERGE split must match reality."""
    for url, py_path, py_cat, rust_path, rust_cat in GOLDEN:
        same = (py_path, py_cat) == (rust_path, rust_cat)
        if url in KNOWN_DIVERGENCES:
            assert not same, (
                f"{url!r} is marked divergent but Python==Rust expected — gap closed? update KNOWN_DIVERGENCES."
            )
        else:
            assert same, (
                f"{url!r} is treated as parity but expected values differ — new divergence? add it to KNOWN_DIVERGENCES."
            )


def test_finding_014_no_category_evasion():
    """Finding 014: an encoded slash (`%2F`) must not let a traversal relabel a route's category."""
    evasion_targets = {
        "/auth/login%2F..%2F..%2Fproduct": "product",
        "/items/%2e%2e%2fsecret": "other",
    }
    for url, evaded_cat in evasion_targets.items():
        cat = normalize(url).category
        assert cat != evaded_cat, f"{url!r}: category-evasion not prevented (got {cat!r})"
    assert ".." in normalize("/auth/login%2F..%2F..%2Fproduct").path


def _read_rust_source() -> str:
    assert _RUST_NORMALIZE.is_file(), f"Rust normalizer not found at {_RUST_NORMALIZE}"
    return _RUST_NORMALIZE.read_text(encoding="utf-8")


def _extract_slice_body(src: str, const_name: str) -> str:
    m = re.search(rf"const\s+{re.escape(const_name)}\s*:[^=]*=\s*&\[", src)
    assert m, f"could not locate `const {const_name}` slice in {_RUST_NORMALIZE.name}"
    start = m.end()
    end = src.index("]", start)
    return src[start:end]


def _parse_string_tuples(block: str, arity: int) -> list[tuple[str, ...]]:
    block = re.sub(r"//[^\n]*", "", block)
    raw = re.findall(r'"((?:[^"\\]|\\.)*)"', block)
    literals = [s.replace('\\"', '"').replace("\\\\", "\\") for s in raw]
    assert literals, f"no string literals parsed from slice body: {block!r}"
    assert len(literals) % arity == 0, (
        f"parsed {len(literals)} literals, not a multiple of arity {arity} — "
        "the Rust table layout drifted from what the parser expects"
    )
    return [tuple(literals[i : i + arity]) for i in range(0, len(literals), arity)]


def test_rust_category_map_matches_python():
    rust_pairs = _parse_string_tuples(_extract_slice_body(_read_rust_source(), "CATEGORY_MAP"), 2)
    py_pairs = list(_CATEGORY_MAP.items())
    if rust_pairs != py_pairs:
        only_rust = set(rust_pairs) - set(py_pairs)
        only_py = set(py_pairs) - set(rust_pairs)
        raise AssertionError(
            "CATEGORY_MAP desynced between normalize.rs and normalize.py.\n"
            f"  only in Rust: {sorted(only_rust)}\n"
            f"  only in Python: {sorted(only_py)}\n"
            + ("  (same entries, different order)" if not only_rust and not only_py else "")
        )


def test_rust_parity_golden_matches_python_golden():
    rust_golden = _parse_string_tuples(_extract_slice_body(_read_rust_source(), "PARITY_GOLDEN"), 3)
    py_rust_view = [(url, rust_path, rust_cat) for (url, _pp, _pc, rust_path, rust_cat) in GOLDEN]
    assert rust_golden == py_rust_view, (
        "PARITY_GOLDEN (normalize.rs) and GOLDEN (test_normalize.py) diverged.\n"
        f"  Rust:   {rust_golden}\n"
        f"  Python: {py_rust_view}"
    )


def test_rust_category_golden_matches_python():
    rust_cat_golden = _parse_string_tuples(_extract_slice_body(_read_rust_source(), "CATEGORY_GOLDEN"), 2)
    py_cat_golden = [tuple(row) for row in CATEGORY_GOLDEN]
    assert rust_cat_golden == py_cat_golden, (
        "CATEGORY_GOLDEN diverged between normalize.rs and test_normalize.py.\n"
        f"  Rust:   {rust_cat_golden}\n"
        f"  Python: {py_cat_golden}"
    )
