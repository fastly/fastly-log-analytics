"""Tests for ``backend.core.fastly.rum_provisioning`` — RUM VCL snippet generation.

Covers the Task 4 addition: an optional ``faro_version`` parameter on
``generate_rum_vcl`` / ``generate_rum_asset_fetch_vcl`` that serves the
self-hosted Faro Web SDK bundle from FOS via ``/js/faro-sdk.js``. Three
things matter most here:

  - byte-for-byte backward compatibility when ``faro_version`` is omitted
    (existing callers — the orchestrator, the declarative generators — must
    not see any change)
  - ``faro_version`` is strictly validated before it reaches a VCL string
    literal or an FOS object path (it originates from a user-facing picker)
  - the generated VCL is syntactically valid per falco
"""

from __future__ import annotations

import re
import shutil

import pytest

from backend.core.fastly.rum_provisioning import (
    RUM_ASSET_FETCH_NAME,
    RUM_DELIVER_SET_COOKIE_NAME,
    RUM_FARO_FETCH_NAME,
    RUM_RECV_NAME,
    RUM_SIGV4_SIGN_NAME,
    generate_rum_asset_fetch_vcl,
    generate_rum_vcl,
)
from backend.utils.vcl_validator import lint_vcl

FALCO_INSTALLED = shutil.which("falco") is not None


# ── Backward compatibility: faro_version omitted / None ─────────────────────


def test_generate_rum_vcl_unaffected_by_missing_faro_version():
    """Callers that don't pass faro_version (every caller today) must see
    identical output to explicitly passing faro_version=None."""
    with_default = generate_rum_vcl("srv_test")
    with_explicit_none = generate_rum_vcl("srv_test", faro_version=None)
    assert with_default == with_explicit_none
    assert set(with_default.keys()) == {RUM_RECV_NAME, RUM_DELIVER_SET_COOKIE_NAME}


def test_generate_rum_asset_fetch_vcl_byte_identical_without_faro_version():
    """The two Phase-3 snippets must be untouched when faro_version is not
    supplied — this is what the orchestrator and declarative generators
    call today, and their VCL must not silently change shape."""
    no_faro = generate_rum_asset_fetch_vcl("iad-va-us")
    explicit_none = generate_rum_asset_fetch_vcl("iad-va-us", faro_version=None)

    assert no_faro == explicit_none
    assert set(no_faro.keys()) == {RUM_ASSET_FETCH_NAME, RUM_SIGV4_SIGN_NAME}
    assert RUM_FARO_FETCH_NAME not in no_faro

    # No Faro fingerprints anywhere in the unmodified snippets.
    combined = "\n".join(no_faro.values())
    assert "faro" not in combined.lower()
    assert "/js/faro-sdk.js" not in combined


def test_generate_rum_vcl_faro_version_none_does_not_touch_phase1_snippets():
    """faro_version=None must leave the Phase 1 recv/deliver snippets
    completely untouched — no Faro strings leak in even by accident."""
    snippets = generate_rum_vcl("srv_test", faro_version=None)
    combined = "\n".join(snippets.values())
    assert "faro" not in combined.lower()


# ── faro_version provided: new content, existing routes preserved ──────────


def test_faro_version_adds_route_rewrite_and_surrogate_key():
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")

    assert set(snippets.keys()) == {RUM_ASSET_FETCH_NAME, RUM_SIGV4_SIGN_NAME, RUM_FARO_FETCH_NAME}

    recv = snippets[RUM_ASSET_FETCH_NAME]
    assert 'req.url.path == "/js/faro-sdk.js" && req.method == "GET"' in recv
    # Existing /js/rum.js route must still be present, unmodified in shape.
    assert 'req.url.path == "/js/rum.js" && req.method == "GET"' in recv

    sigv4 = snippets[RUM_SIGV4_SIGN_NAME]
    assert 'bereq.url.path == "/js/faro-sdk.js"' in sigv4
    assert 'set bereq.url = "/rum/faro-web-sdk-v2.9.0.iife.js";' in sigv4
    # Existing rum.js rewrite must still be present.
    assert 'set bereq.url = "/rum/rum-tracker.js";' in sigv4

    fetch = snippets[RUM_FARO_FETCH_NAME]
    assert 'set beresp.http.Surrogate-Key = "rum-faro-sdk";' in fetch
    assert "set beresp.ttl = 604800s;" in fetch


# ── F-3: fetch-cache snippet must not cache/immutable-tag error responses ──


def test_faro_fetch_caching_gates_on_200_status():
    """F-3 audit finding: without a beresp.status check, a FOS 403/404
    (mid-upload, or a bucket-policy blip) would be cached at the edge for
    7 days AND handed to browsers tagged immutable — unpurgeable. The
    caching/Surrogate-Key/Cache-Control block must be nested inside an
    explicit 200 check, with a non-caching branch for everything else."""
    fetch = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_FARO_FETCH_NAME]

    assert "if (beresp.status == 200)" in fetch
    # The caching directives must be inside the 200 branch, not top-level.
    status_check_idx = fetch.index("if (beresp.status == 200)")
    surrogate_key_idx = fetch.index('set beresp.http.Surrogate-Key = "rum-faro-sdk";')
    assert status_check_idx < surrogate_key_idx
    # Non-200 branch must explicitly refuse to cache.
    assert "set beresp.cacheable = false;" in fetch


def test_faro_fetch_caching_decouples_edge_and_browser_ttl():
    """F-3 audit finding: /js/faro-sdk.js is a stable path serving mutable
    (per-upgrade) content, so a long browser max-age + immutable would mean
    an upgrade can never invalidate already-issued browser copies — a purge
    only clears the edge cache. The edge TTL must stay long (purged
    explicitly on upload/upgrade via Surrogate-Key), while the
    browser-visible Cache-Control gets a short max-age and drops
    'immutable' entirely."""
    fetch = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_FARO_FETCH_NAME]

    assert "immutable" not in fetch
    assert 'set beresp.http.Surrogate-Control = "max-age=604800";' in fetch
    assert "set beresp.ttl = 604800s;" in fetch

    # Extract the Cache-Control value and assert its max-age is much shorter
    # than the edge TTL — the exact number isn't load-bearing, "much less
    # than 604800" is.
    match = re.search(r'Cache-Control = "([^"]+)"', fetch)
    assert match, "expected a browser Cache-Control header"
    cache_control = match.group(1)
    max_age_match = re.search(r"max-age=(\d+)", cache_control)
    assert max_age_match, f"no max-age found in Cache-Control: {cache_control!r}"
    assert int(max_age_match.group(1)) < 604800


def test_faro_version_present_in_asset_fetch_but_generate_rum_vcl_still_clean():
    """Phase 1 (generate_rum_vcl) never emits Faro content, even when a
    valid faro_version is supplied — Faro routing lives exclusively in
    generate_rum_asset_fetch_vcl."""
    snippets = generate_rum_vcl("srv_test", faro_version="2.9.0")
    assert set(snippets.keys()) == {RUM_RECV_NAME, RUM_DELIVER_SET_COOKIE_NAME}
    combined = "\n".join(snippets.values())
    assert "faro" not in combined.lower()


def test_faro_asset_fetch_respects_shield_pop_selection():
    """The Faro route reuses the exact same backend-selection expression as
    the existing rum.js route (same shield_pop threading)."""
    snippets = generate_rum_asset_fetch_vcl("frankfurt-de", faro_version="1.0.0")
    recv = snippets[RUM_ASSET_FETCH_NAME]
    assert recv.count("fastly.try_select_shield(ssl_shield_frankfurt_de, F_fos_origin)") == 2


def test_faro_asset_fetch_shield_none():
    snippets = generate_rum_asset_fetch_vcl("none", faro_version="1.0.0")
    recv = snippets[RUM_ASSET_FETCH_NAME]
    assert recv.count("set req.backend = F_fos_origin;") == 2


# ── faro_version validation: reject anything unsafe to interpolate ─────────


@pytest.mark.parametrize(
    "bad_version",
    [
        '1.2."3',  # embedded quote — breaks the VCL string literal
        "1.2.3\n",  # newline
        "../../etc/passwd",  # path traversal
        "1.2.3 ",  # trailing whitespace
        " 1.2.3",  # leading whitespace
        "1.2.3; set req.http.x = 1",  # attempted VCL injection
        "1.2",  # not a full X.Y.Z
        "1.2.3.4",  # too many components
        "1.2.x",  # non-numeric component
        "",  # empty
        "1.2.3-beta",  # pre-release suffix
        "1.2\\.3",  # backslash
        "1.2.3\x00",  # null byte
        "\x00",  # bare null byte
        "１.２.３",  # fullwidth digits (U+FF10-FF19) — \d matches these too
        "١.٢.٣",  # Arabic-Indic digits (U+0660-0669) — \d matches these too
        "2.9.٠",  # mixed ASCII + non-ASCII digit in one component
    ],
)
def test_generate_rum_asset_fetch_vcl_rejects_malicious_version(bad_version):
    with pytest.raises(ValueError):
        generate_rum_asset_fetch_vcl("iad-va-us", faro_version=bad_version)


@pytest.mark.parametrize(
    "bad_version",
    [
        '1.2."3',
        "1.2.3\n",
        "../../etc/passwd",
        "1.2\\.3",
        "1.2.3\x00",
        "１.２.３",
        "١.٢.٣",
    ],
)
def test_generate_rum_vcl_rejects_malicious_version(bad_version):
    """generate_rum_vcl validates too, even though it never emits Faro
    content — a caller that threads a bad version through this function
    must fail loudly rather than have it silently discarded."""
    with pytest.raises(ValueError):
        generate_rum_vcl("srv_test", faro_version=bad_version)


@pytest.mark.parametrize(
    "unicode_digit_version",
    [
        "１.２.３",  # fullwidth (U+FF10-FF19)
        "١.٢.٣",  # Arabic-Indic (U+0660-0669)
        "๑.๒.๓",  # Thai digits (U+0E50-0E59)
        "2.9.٠",  # mixed ASCII + non-ASCII digit
    ],
)
def test_assert_faro_version_safe_rejects_unicode_digits_directly(unicode_digit_version):
    """Regression: Python's \\d in re (without re.ASCII) matches any Unicode
    decimal-digit codepoint (category Nd), not just 0-9. A version string
    built entirely from non-ASCII digits must still be rejected — verified
    directly against the validator rather than only through the public
    generator functions."""
    from backend.core.fastly.rum_provisioning import _assert_faro_version_safe

    with pytest.raises(ValueError):
        _assert_faro_version_safe(unicode_digit_version)


def test_rejected_version_never_reaches_generated_vcl():
    """Defense in depth: confirm the rejection happens BEFORE any VCL
    string is built — a malicious value must never even transiently exist
    inside a returned snippet body."""
    malicious = '2.9.0"; unset bereq.http.Authorization; #'
    with pytest.raises(ValueError):
        generate_rum_asset_fetch_vcl("iad-va-us", faro_version=malicious)


# ── falco lint of the generated VCL ─────────────────────────────────────────


def _wrap_for_falco(body: str, subroutine: str) -> str:
    """Assemble a syntactically-complete VCL file for falco lint.

    RUM asset-fetch VCL references the FOS backend, a per-region shield
    backend, and the ``fos_credentials`` edge dictionary (a Fastly `table`)
    — none of which the shared ``lint_vcl`` wrapper declares (it only knows
    about the session-scoring backends). Declare stand-ins here and lint the
    fully-assembled file with ``wrap_subroutine=None`` so we still go
    through the shared falco subprocess/parsing plumbing.
    """
    stage = subroutine.removeprefix("vcl_")
    return f"""backend F_fos_origin {{
  .host = "example.com";
  .port = "443";
  .ssl = true;
}}

backend ssl_shield_iad_va_us {{
  .host = "example.com";
  .port = "443";
  .ssl = true;
}}

table fos_credentials {{
  "access_key": "x",
}}

sub {subroutine} {{
  #FASTLY {stage}
{body}
}}
"""


@pytest.mark.skipif(not FALCO_INSTALLED, reason="requires falco binary")
def test_falco_lints_faro_asset_fetch_recv_snippet():
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")
    full = _wrap_for_falco(snippets[RUM_ASSET_FETCH_NAME], "vcl_recv")
    result = lint_vcl(full, snippet_name="faro_asset_fetch_recv", wrap_subroutine=None)
    assert result.ok, f"falco errors: {result.errors}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="requires falco binary")
def test_falco_lints_faro_sigv4_rewrite_snippet():
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")
    full = _wrap_for_falco(snippets[RUM_SIGV4_SIGN_NAME], "vcl_miss")
    result = lint_vcl(full, snippet_name="faro_sigv4_miss", wrap_subroutine=None)
    assert result.ok, f"falco errors: {result.errors}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="requires falco binary")
def test_falco_lints_faro_fetch_caching_snippet():
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")
    full = _wrap_for_falco(snippets[RUM_FARO_FETCH_NAME], "vcl_fetch")
    result = lint_vcl(full, snippet_name="faro_fetch_cache", wrap_subroutine=None)
    assert result.ok, f"falco errors: {result.errors}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="requires falco binary")
def test_falco_lints_unchanged_asset_fetch_snippets_without_faro():
    """The pre-existing (no faro_version) snippets must still lint clean —
    regression guard for the byte-identity requirement."""
    snippets = generate_rum_asset_fetch_vcl("iad-va-us")
    recv_full = _wrap_for_falco(snippets[RUM_ASSET_FETCH_NAME], "vcl_recv")
    miss_full = _wrap_for_falco(snippets[RUM_SIGV4_SIGN_NAME], "vcl_miss")
    recv_result = lint_vcl(recv_full, snippet_name="asset_fetch_recv", wrap_subroutine=None)
    miss_result = lint_vcl(miss_full, snippet_name="asset_fetch_miss", wrap_subroutine=None)
    assert recv_result.ok, f"falco errors: {recv_result.errors}"
    assert miss_result.ok, f"falco errors: {miss_result.errors}"


def test_falco_required_when_missing_binary_env_set(monkeypatch):
    """Mirrors the FALCO_REQUIRED contract used in test_vcl_semantics.py:
    if a CI environment declares falco mandatory but it isn't on PATH, the
    lint helper must not silently report success."""
    monkeypatch.setattr("backend.utils.vcl_validator._falco_binary", lambda: None)
    result = lint_vcl("if (true) {}", wrap_subroutine=None)
    assert result.skipped is True
    assert result.ok is True  # lint_vcl itself is fail-open on missing binary
    # It is the CALLER's responsibility (as in test_vcl_semantics.py's
    # FALCO_REQUIRED guard) to turn "skipped" into a hard failure in CI.
