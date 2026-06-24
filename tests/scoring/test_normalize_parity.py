"""Cross-language parity contract for route normalization.

PINS the current behaviour of BOTH normalizers:
  * ``backend/scoring/normalize.py``        (trains the L2 matrix — this file)
  * ``compute/scorer/src/normalize.rs``     (scores at the edge — its in-file
    ``cross_lang_parity_*`` tests are the counterpart)

The same GOLDEN URL list lives in both; each side asserts its own language's
expected output, so a change to either normalizer trips a test.

SCOPE (EC-02): this is the toolchain-free TABLE compare — the GOLDEN list is the
ASCII-printable subset where the two normalizers are guaranteed byte-identical,
and every row here does produce identical (path, category) on both sides. It is
NOT a claim that the normalizers agree on ALL input: under the documented
ASCII-only normalization contract (see both ``normalize`` module docstrings) they
DO diverge on non-ASCII (Unicode lowercase/digits) and on ``scheme:opaque`` — those
classes are exercised + pinned by ``tests/scoring/test_normalize_runtime_parity.py``
(which compiles + runs the real Rust normalizer). Since the matrix is TRAINED with
Python and SCORED with Rust, any GOLDEN row that flips is a silent train/score key
mismatch.

Encoded-slash (``%2F``) URLs used to diverge — Python kept ``%2F`` as data
(audit finding 014, anti category-evasion) while Rust decoded it and popped
``..``. The Rust normalizer was ported to the same "%2F-as-data" model, so
finding 014 is now enforced at the edge as well as in the trainer, and the gap
is closed. ``KNOWN_DIVERGENCES`` is intentionally empty *for this ASCII GOLDEN
table* (no row here diverges); the genuine non-ASCII divergence classes live in
the runtime-parity test above, not here. A future one-sided edit that
re-introduces a divergence in a GOLDEN row trips
``test_divergence_classification_is_honest``.

If you change either normalizer and a row flips, update BOTH this file's GOLDEN
table and the matching ``PARITY_GOLDEN`` in ``compute/scorer/src/normalize.rs``
— never re-baseline just one side. The cross-file tests at the bottom of this
module assert the two tables stay byte-identical, so a one-sided edit fails CI.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from backend.scoring.normalize import _CATEGORY_MAP, normalize

# The Rust normalizer whose golden tables must stay byte-identical with the
# Python ones below. The cross-file tests at the bottom of this module parse
# its source so a one-sided edit to either side trips CI *without* needing a
# Rust toolchain — the cargo `#[test]`s (run by the Scorer CI job) catch logic
# drift; this catches table drift even when cargo never runs.
_RUST_NORMALIZE = Path(__file__).resolve().parents[2] / "compute" / "scorer" / "src" / "normalize.rs"

# (url, py_path, py_cat, rust_path, rust_cat) — measured 2026-06-17 from both
# normalizers. Keep byte-identical with the Rust ``cross_lang_parity_*`` test.
GOLDEN: list[tuple[str, str, str, str, str]] = [
    # ── AGREE (real parity — train key == score key) ──
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
    # %2E%2E followed by a LITERAL slash resolves identically on both sides.
    ("/admin/%2e%2e/items/foo", "/items/foo", "product", "/items/foo", "product"),
    ("/search?q=red+shoes&page=2", "/search", "browse", "/search", "browse"),
    # ── Encoded %2F — finding 014 now enforced on BOTH sides (was the gap) ──
    # `%2F` stays data: the slash never separates, so the literal `..` survives
    # inside the segment and the FIRST real segment keeps the category (no
    # auth→product / product→other evasion at train OR score time).
    ("/auth/login%2F..%2F..%2Fproduct", "/auth/login/../../product", "auth", "/auth/login/../../product", "auth"),
    ("/items/%2e%2e%2fsecret", "/items/../secret", "product", "/items/../secret", "product"),
    ("/a/b/%2e%2e%2f%2e%2e%2fc", "/a/b/../../c", "other", "/a/b/../../c", "other"),
]

# No known divergences remain — the encoded-slash gap was closed by porting the
# "%2F-as-data" model into the Rust normalizer. Kept (empty) so a future
# one-sided edit that re-introduces a divergence trips the honesty check below.
KNOWN_DIVERGENCES: set[str] = set()


# One representative URL per category bucket — pins that EVERY value in
# _CATEGORY_MAP (including the otherwise-uncovered "asset") stays reachable and
# is mirrored identically by the Rust CATEGORY_MAP. Without this a one-sided
# edit to a less-tested bucket would ship silently. Keep byte-identical with
# CATEGORY_GOLDEN in compute/scorer/src/normalize.rs. Asserts category only.
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
    """The AGREE/DIVERGE split must match reality: a row's Python expected
    equals its Rust expected IFF the URL is NOT in KNOWN_DIVERGENCES. Catches a
    golden row mislabelled (e.g. a new DIVERGE row left out of the set, or a gap
    that quietly closed)."""
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
    """Finding 014: an encoded slash (`%2F`) must not let a traversal relabel a
    route's category. For each encoded-slash golden URL, the category must come
    from the FIRST real segment — never the post-traversal target the attacker
    was aiming for. Guards that the closed gap stays closed on the train side."""
    evasion_targets = {
        "/auth/login%2F..%2F..%2Fproduct": "product",  # attacker wanted product
        "/items/%2e%2e%2fsecret": "other",  # attacker wanted to escape product
    }
    for url, evaded_cat in evasion_targets.items():
        cat = normalize(url).category
        assert cat != evaded_cat, f"{url!r}: category-evasion not prevented (got {cat!r})"
    # And both still carry the literal `..` (proof the %2F never separated).
    assert ".." in normalize("/auth/login%2F..%2F..%2Fproduct").path


# ── Cross-file source parity (toolchain-free) ───────────────────────────────
#
# PARITY-01/02: the Rust scorer's `#[test]`s only run under `cargo test` (now
# wired into the Scorer CI job + the pre-push hook). To also guard the
# Python↔Rust *table* contract even when no Rust toolchain is present — and to
# fail with a clear diff the moment someone edits one side's CATEGORY_MAP /
# GOLDEN list without the other — these tests string-parse `normalize.rs` and
# assert byte-equality with the Python tables above. The matrix is TRAINED with
# Python and SCORED with Rust, so a silent table desync is a train/score key
# mismatch (and, for the category map, a category-evasion surface).


def _read_rust_source() -> str:
    assert _RUST_NORMALIZE.is_file(), f"Rust normalizer not found at {_RUST_NORMALIZE}"
    return _RUST_NORMALIZE.read_text(encoding="utf-8")


def _extract_slice_body(src: str, const_name: str) -> str:
    """Return the text between ``= &[`` and its closing ``]`` for a top-level
    ``const NAME: ... = &[ ... ];`` slice. The current tables contain no ``[``
    or ``]`` inside their string literals, so bracket matching is unambiguous."""
    m = re.search(rf"const\s+{re.escape(const_name)}\s*:[^=]*=\s*&\[", src)
    assert m, f"could not locate `const {const_name}` slice in {_RUST_NORMALIZE.name}"
    start = m.end()
    end = src.index("]", start)
    return src[start:end]


def _parse_string_tuples(block: str, arity: int) -> list[tuple[str, ...]]:
    """Parse a Rust slice body of fixed-arity string tuples into Python tuples.

    Strips ``//`` line comments first (so commentary inside the table can't
    inject phantom literals), then collects every ``"..."`` literal in order and
    chunks by ``arity``. Multi-line tuples parse the same as single-line ones
    because grouping is purely positional. None of these tables use escaped
    quotes, but ``\\"`` / ``\\\\`` are unescaped defensively."""
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
    """`CATEGORY_MAP` in normalize.rs must equal `_CATEGORY_MAP` (key, value)
    pairs, in order. Catches a one-sided edit to either map — the documented
    "edit in lockstep" contract, now enforced instead of trusted."""
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
    """`PARITY_GOLDEN` in normalize.rs must equal the Rust columns of the Python
    `GOLDEN` table, row-for-row. The two were declared "byte-identical"; this
    proves it so a row added to one side can't silently miss the other."""
    rust_golden = _parse_string_tuples(_extract_slice_body(_read_rust_source(), "PARITY_GOLDEN"), 3)
    py_rust_view = [(url, rust_path, rust_cat) for (url, _pp, _pc, rust_path, rust_cat) in GOLDEN]
    assert rust_golden == py_rust_view, (
        "PARITY_GOLDEN (normalize.rs) and GOLDEN (test_normalize_parity.py) diverged.\n"
        f"  Rust:   {rust_golden}\n"
        f"  Python: {py_rust_view}"
    )


def test_rust_category_golden_matches_python():
    """`CATEGORY_GOLDEN` in normalize.rs must equal the Python `CATEGORY_GOLDEN`
    — one representative URL per category bucket, kept identical on both sides."""
    rust_cat_golden = _parse_string_tuples(_extract_slice_body(_read_rust_source(), "CATEGORY_GOLDEN"), 2)
    py_cat_golden = [tuple(row) for row in CATEGORY_GOLDEN]
    assert rust_cat_golden == py_cat_golden, (
        "CATEGORY_GOLDEN diverged between normalize.rs and test_normalize_parity.py.\n"
        f"  Rust:   {rust_cat_golden}\n"
        f"  Python: {py_cat_golden}"
    )
