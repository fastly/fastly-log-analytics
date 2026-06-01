"""Tests for backend.utils.vcl_utils.

Two functions:
- ``log_format_to_vcl_log`` — converts a Fastly log format template into the
  equivalent VCL log statement body. Pure, easy to unit-test.
- ``lint_log_format`` — JSON shape + (optional) Falco VCL validation. We
  exercise the JSON-shape path without falco; the falco path is covered by
  ``tests/core/test_vcl_semantics.py``.
"""

from __future__ import annotations

from unittest.mock import patch

from backend.utils.vcl_utils import lint_log_format, log_format_to_vcl_log

# ── log_format_to_vcl_log ────────────────────────────────────────────────────


def test_pure_literal_without_heredoc_terminator_wraps_once():
    """A literal that does NOT contain the heredoc-closing sequence "} wraps
    cleanly into a single ``{"..."}`` heredoc."""
    out = log_format_to_vcl_log("plain text no braces")
    assert out == '{"plain text no braces"}'


def test_macro_extracted_verbatim():
    out = log_format_to_vcl_log('{"ts": %{time.start}V}')
    # Literal "{"ts": " + macro + literal "}"
    assert "time.start" in out
    assert " + " in out


def test_multiple_macros_concatenated_with_plus():
    fmt = '{"ip":"%{client.ip}V","ua":"%{req.http.User-Agent}V"}'
    out = log_format_to_vcl_log(fmt)
    assert "client.ip" in out
    assert "req.http.User-Agent" in out
    # Must use + concatenation (Falco-safe)
    assert " + " in out


def test_fastly_escaped_braces_unescaped_first():
    fmt = '\\{"raw":"x"\\}'
    out = log_format_to_vcl_log(fmt)
    # The escaped braces should be unwrapped before being placed in the heredoc
    assert "\\{" not in out
    assert "\\}" not in out


def test_internal_quote_brace_in_literal_split_safely():
    """If a literal contains the heredoc-closing sequence "}, the impl splits
    it into two heredocs to avoid a raw VCL syntax error."""
    fmt = '{"a":"b"}{"c":"d"}'
    out = log_format_to_vcl_log(fmt)
    # The split happens at "} → "} + {" — verify the result still doesn't
    # contain a top-level closing heredoc that would terminate prematurely
    assert '"} + {"' in out


# ── lint_log_format (JSON path) ──────────────────────────────────────────────


def test_lint_rejects_empty_format():
    ok, msg = lint_log_format("")
    assert ok is False
    assert "empty" in msg.lower()


def test_lint_rejects_format_with_newline():
    ok, msg = lint_log_format('{"timestamp":%{time.start}V}\n')
    assert ok is False
    assert "newline" in msg.lower()


def test_lint_rejects_non_object_root():
    # Bare string ≠ JSON object
    ok, msg = lint_log_format('"just a string"')
    assert ok is False


def test_lint_requires_timestamp_field():
    ok, msg = lint_log_format('{"ip":"%{client.ip}V"}')
    assert ok is False
    assert "timestamp" in msg.lower()


def test_lint_passes_valid_json_when_no_falco():
    """Without falco on PATH, valid JSON returns ok=True with a hint message."""
    fmt = '{"timestamp":"%{time.start}V","ip":"%{client.ip}V"}'
    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, msg = lint_log_format(fmt)
    assert ok is True
    assert "falco" in msg.lower() or "valid" in msg.lower()


def test_lint_handles_fastly_escaped_braces_in_json():
    """Templates often include \\{ \\} for Fastly's own escaping; the linter
    must unwrap those before JSON parsing."""
    fmt = '\\{"timestamp":"%{time.start}V"\\}'
    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, _ = lint_log_format(fmt)
    assert ok is True
