"""Tests for backend.utils.vcl_validator — input policy + falco wrapper.

Falco is invoked as a subprocess when available. Tests don't require it
to be installed; the validator's degraded path (skip with warning) is
exercised by patching ``shutil.which`` to return None.
"""

from __future__ import annotations

import shutil
from unittest.mock import patch

import pytest

from backend.utils.vcl_validator import (
    MAX_REGEX_BYTES,
    RegexValidationError,
    lint_vcl,
    validate_recv_exclusion_regex_with_lint,
    validate_url_exclusion_regex,
)

# ── validate_url_exclusion_regex ────────────────────────────────────────────


def test_empty_is_valid_and_means_default():
    """Empty / whitespace-only inputs return "" — the caller's signal to
    fall back to the built-in default."""
    assert validate_url_exclusion_regex("") == ""
    assert validate_url_exclusion_regex("   \t  ") == ""


def test_valid_regex_round_trips():
    assert validate_url_exclusion_regex(r"\.(css|js|png)$") == r"\.(css|js|png)$"
    assert validate_url_exclusion_regex(r"^/healthz") == r"^/healthz"


def test_non_string_rejected():
    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex(b"bytes")  # type: ignore[arg-type]
    assert exc.value.reason == "type"

    with pytest.raises(RegexValidationError):
        validate_url_exclusion_regex(42)  # type: ignore[arg-type]


def test_too_long_rejected():
    # A regex that just barely exceeds the cap.
    too_long = "a" * (MAX_REGEX_BYTES + 1)
    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex(too_long)
    assert exc.value.reason == "too_long"


def test_quote_rejected():
    """A double-quote in the user input would break out of the VCL
    string literal that wraps the regex — non-negotiable reject."""
    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex(r'foo"bar')
    assert exc.value.reason == "disallowed_char"


def test_control_chars_rejected():
    """NULs and control characters can't appear in a VCL string literal
    and would smuggle structure if our escaper ever missed one."""
    for bad in ("\x00", "\x01", "\x07", "\x1b"):
        with pytest.raises(RegexValidationError) as exc:
            validate_url_exclusion_regex(f"foo{bad}bar")
        assert exc.value.reason == "disallowed_char"


def test_invalid_regex_rejected():
    """Unclosed groups / brackets fail at re.compile time so the
    operator gets an error before the VCL ever ships."""
    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex(r"(unclosed")
    assert exc.value.reason == "invalid_regex"

    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex(r"[unbalanced")
    assert exc.value.reason == "invalid_regex"


def test_strips_trailing_whitespace():
    """Operators often copy-paste with stray trailing newlines."""
    assert validate_url_exclusion_regex("\\.css$\n") == r"\.css$"
    assert validate_url_exclusion_regex("  \\.css$  ") == r"\.css$"


# ── lint_vcl ────────────────────────────────────────────────────────────────


def _falco_available() -> bool:
    return shutil.which("falco") is not None


def test_lint_vcl_skips_cleanly_when_falco_missing():
    """When the binary isn't on PATH, the lint returns ``skipped=True``
    and ``ok=True``. The caller decides whether to treat skipped as a
    fail (production should)."""
    with patch("backend.utils.vcl_validator.shutil.which", return_value=None):
        result = lint_vcl("ignore me", snippet_name="test")
    assert result.skipped is True
    assert result.ok is True
    assert "falco" in result.skipped_reason.lower()


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_lint_vcl_accepts_recv_snippet_default():
    """The full default recv snippet (with the asset-ext regex) must
    pass falco when wrapped in vcl_recv with the backends declared."""
    from backend.provision.session_scoring_vcl import recv_snippet

    body = recv_snippet("test_svc", "test_secret_abc")
    result = lint_vcl(body, snippet_name="recv_default")
    assert result.ok, f"expected default recv snippet to pass falco; errors={result.errors}"
    assert not result.skipped


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_lint_vcl_accepts_custom_regex():
    """A trimmed custom regex must also pass falco."""
    from backend.provision.session_scoring_vcl import recv_snippet

    body = recv_snippet("test_svc", "test_secret_abc", exclude_url_regex=r"\.(css|js|png)$")
    result = lint_vcl(body)
    assert result.ok, f"custom regex must pass falco; errors={result.errors}"


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_lint_vcl_rejects_unbalanced_regex_in_assembled_snippet():
    """A regex that opens a character class without closing it produces
    invalid VCL after string interpolation; falco catches it. This is
    the safety net for inputs that snuck past Python's re.compile (e.g.
    in some edge case where the parser accepts something Fastly's
    engine rejects)."""
    # Bypass validate_url_exclusion_regex (which would reject this) by
    # building the snippet directly with raw injection.
    snippet = '# defective snippet\nif (std.tolower(req.url) !~ "unclosed[") {\n  return(pass);\n}'
    result = lint_vcl(snippet)
    assert not result.ok, "falco must flag unbalanced regex in assembled VCL"
    assert any("regex" in e.lower() for e in result.errors), (
        f"expected a regex-related error message, got: {result.errors}"
    )


# ── validate_recv_exclusion_regex_with_lint ─────────────────────────────────


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_end_to_end_validation_accepts_valid():
    from backend.provision.session_scoring_vcl import recv_snippet

    def builder(cleaned: str) -> str:
        return recv_snippet("svc", "secret_abc", exclude_url_regex=cleaned or None)

    cleaned, lint = validate_recv_exclusion_regex_with_lint(
        r"\.(jpg|png)$", build_full_snippet=builder, require_falco=True
    )
    assert cleaned == r"\.(jpg|png)$"
    assert lint.ok


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_end_to_end_validation_rejects_input_policy_first():
    """Input policy violation (quote char) trips before falco even runs."""
    from backend.provision.session_scoring_vcl import recv_snippet

    def builder(cleaned: str) -> str:
        return recv_snippet("svc", "secret_abc", exclude_url_regex=cleaned or None)

    with pytest.raises(RegexValidationError) as exc:
        validate_recv_exclusion_regex_with_lint(r'foo"bar', build_full_snippet=builder, require_falco=True)
    assert exc.value.reason == "disallowed_char"


def test_end_to_end_validation_require_falco_raises_when_missing():
    """When require_falco=True and the binary's missing, validation
    raises rather than silently passing."""

    def builder(_: str) -> str:
        return "irrelevant"

    with patch("backend.utils.vcl_validator.shutil.which", return_value=None):
        with pytest.raises(RegexValidationError) as exc:
            validate_recv_exclusion_regex_with_lint(r"\.css$", build_full_snippet=builder, require_falco=True)
    assert exc.value.reason == "falco_unavailable"


def test_end_to_end_validation_require_false_passes_when_missing():
    """With require_falco=False, missing binary is OK — used for tests
    and local dev where the operator doesn't have falco installed."""

    def builder(_: str) -> str:
        return "irrelevant"

    with patch("backend.utils.vcl_validator.shutil.which", return_value=None):
        cleaned, lint = validate_recv_exclusion_regex_with_lint(
            r"\.css$", build_full_snippet=builder, require_falco=False
        )
    assert cleaned == r"\.css$"
    assert lint.skipped


# ── Recv snippet override path ──────────────────────────────────────────────


def test_recv_snippet_default_used_when_override_none():
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ASSET_EXT_REGEX,
        recv_snippet,
        resolve_exclude_url_regex,
    )

    assert resolve_exclude_url_regex(None) == DEFAULT_ASSET_EXT_REGEX
    assert resolve_exclude_url_regex("") == DEFAULT_ASSET_EXT_REGEX
    assert resolve_exclude_url_regex("   ") == DEFAULT_ASSET_EXT_REGEX

    body = recv_snippet("svc", "sec")
    assert DEFAULT_ASSET_EXT_REGEX in body


def test_recv_snippet_uses_override_when_supplied():
    from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX, recv_snippet

    custom = r"\.(css)$"
    body = recv_snippet("svc", "sec", exclude_url_regex=custom)
    assert custom in body
    assert DEFAULT_ASSET_EXT_REGEX not in body
