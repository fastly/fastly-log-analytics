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

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from backend.scoring.normalize import normalize

_RUSTC = shutil.which("rustc")
_NORMALIZE_RS = Path(__file__).resolve().parents[2] / "compute" / "scorer" / "src" / "normalize.rs"

pytestmark = pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")

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
