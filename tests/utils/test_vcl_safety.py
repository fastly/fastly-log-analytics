"""Tests for the unified VCL safety validator (B16 consolidation).

Each of the four APIs must accept valid input and reject the
injection / forbidden-char cases that the three pre-consolidation layers
(``vcl_utils`` macro check, ``session_scoring_vcl`` forbidden-char regex,
``vcl_validator`` disallowed-char class) each rejected — the unified
policy must be at least as strict as the strictest legacy layer.
"""

from __future__ import annotations

import pytest

from backend.utils.vcl_safety import (
    LintResult,
    VclSafetyError,
    validate_expression,
    validate_log_format_macro,
    validate_quoted_literal,
)


class TestValidateQuotedLiteral:
    def test_accepts_plain_value(self) -> None:
        assert validate_quoted_literal("hello-world_123") == "hello-world_123"

    def test_accepts_empty(self) -> None:
        assert validate_quoted_literal("") == ""

    @pytest.mark.parametrize(
        "bad",
        [
            'has"quote',  # would terminate the literal
            "back\\slash",  # escape injection
            "tab\tinside",  # C0 control (0x09) — stricter than old vcl_validator
            "newline\ninside",
            "null\x00byte",
            "del\x7fchar",
        ],
    )
    def test_rejects_forbidden_chars_strict(self, bad: str) -> None:
        with pytest.raises(VclSafetyError):
            validate_quoted_literal(bad)

    def test_allow_backslash_permits_regex_escapes(self) -> None:
        # regex payloads legitimately contain backslash escapes
        assert validate_quoted_literal(r"\.js$", allow_backslash=True) == r"\.js$"

    def test_allow_backslash_still_rejects_quote_and_tab(self) -> None:
        with pytest.raises(VclSafetyError):
            validate_quoted_literal('re"gex', allow_backslash=True)
        with pytest.raises(VclSafetyError):
            validate_quoted_literal("re\tgex", allow_backslash=True)

    def test_is_valueerror_subclass(self) -> None:
        # legacy `except ValueError` handlers must still catch it
        with pytest.raises(ValueError):
            validate_quoted_literal('"')


class TestValidateExpression:
    def test_accepts_plain_expression(self) -> None:
        assert validate_expression("req.http.Host") == "req.http.Host"

    @pytest.mark.parametrize("bad", ["a; b", "a /* c */ b", "a // c"])
    def test_rejects_statement_and_comment_tokens(self, bad: str) -> None:
        with pytest.raises(VclSafetyError):
            validate_expression(bad)


class TestValidateLogFormatMacro:
    def test_accepts_plain_macro(self) -> None:
        assert validate_log_format_macro("req.http.Host") == "req.http.Host"

    def test_accepts_escaped_heredoc_braces(self) -> None:
        # legitimate strftime heredoc uses escaped braces
        assert validate_log_format_macro(r'strftime(\{"%Y"\}, now)') == r'strftime(\{"%Y"\}, now)'

    @pytest.mark.parametrize(
        "bad",
        [
            "a; b",  # statement terminator
            "a /* c */",  # block comment
            "a // c",  # line comment
            "close}brace",  # unescaped } closes vcl_log
            "open{brace",  # unescaped { opens a subroutine
        ],
    )
    def test_rejects_injection(self, bad: str) -> None:
        with pytest.raises(VclSafetyError):
            validate_log_format_macro(bad)

    def test_stable_error_message(self) -> None:
        # lint_log_format surfaces this message to operators — keep it stable
        with pytest.raises(VclSafetyError) as exc:
            validate_log_format_macro("bad}")
        assert "VCL macro contains invalid characters" in str(exc.value)


class TestLintDocument:
    def test_skips_gracefully_when_falco_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import backend.utils.vcl_safety as vs

        monkeypatch.setattr(vs.shutil, "which", lambda _name: None)
        res = vs.lint_document("sub vcl_recv { }")
        assert isinstance(res, LintResult)
        assert res.ok is True
        assert res.skipped is True
        assert "falco" in res.skipped_reason.lower()
