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


# ── Merged from test_path_safety.py ──

import os

from backend.utils.path_safety import path_within_dir


def test_path_within_dir_basic(tmp_path):
    base_dir = str(tmp_path)

    # 1. Path inside base directory
    candidate = str(tmp_path / "subdir" / "file.txt")
    assert path_within_dir(base_dir, candidate) is True

    # 2. Path equals base directory
    assert path_within_dir(base_dir, base_dir) is True

    # 3. Path outside base directory via relative traversal
    candidate_escape = str(tmp_path / ".." / "other_dir")
    assert path_within_dir(base_dir, candidate_escape) is False


def test_path_within_dir_value_error_fallback(monkeypatch):
    # Mock os.path.commonpath to raise ValueError to test the exception block coverage
    def mock_commonpath(paths):
        raise ValueError("Simulated drive mismatch or empty paths")

    monkeypatch.setattr(os.path, "commonpath", mock_commonpath)
    assert path_within_dir("/tmp/base", "/tmp/base/candidate") is False


# ── Merged from test_sql_literal_escaping_sites.py ──

import pytest

from backend.core.rollups._common import quote_path_list
from backend.utils.sql_validator import escape_sql_literal


def test_quote_path_list_doubles_embedded_quotes() -> None:
    assert quote_path_list(["/a/o'brien.parquet", "/b.parquet"]) == "'/a/o''brien.parquet', '/b.parquet'"


def test_escape_sql_literal_rejects_non_str() -> None:
    with pytest.raises(TypeError):
        escape_sql_literal(5)  # type: ignore[arg-type]


def test_no_hand_rolled_quote_doubling_left_in_backend() -> None:
    import pathlib

    needle = 'replace("\'", "\'\'")'
    root = pathlib.Path(__file__).resolve().parents[2] / "backend"
    offenders = [
        str(p.relative_to(root))
        for p in root.rglob("*.py")
        if needle in p.read_text(encoding="utf-8") and p.name != "sql_validator.py"
    ]
    assert offenders == []
