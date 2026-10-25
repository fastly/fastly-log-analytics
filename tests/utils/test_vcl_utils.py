"""Tests for VCL utilities, safety validation, falco linting, and CMCD capture ordering."""

from __future__ import annotations

import os
import pathlib
import shutil
from unittest.mock import patch

import pytest

from backend.config import path_within_dir
from backend.core.rollups._common import quote_path_list
from backend.provision.cmcd_fields import get_cmcd_fields
from backend.provision.fastly_api import generate_capture_vcl
from backend.utils.sql_validator import escape_sql_literal
from backend.utils.vcl_safety import (
    LintResult,
    VclSafetyError,
    validate_log_format_macro,
    validate_quoted_literal,
)
from backend.utils.vcl_utils import lint_log_format, log_format_to_vcl_log
from backend.utils.vcl_validator import (
    MAX_REGEX_BYTES,
    RegexValidationError,
    lint_vcl,
    validate_recv_exclusion_regex_with_lint,
    validate_url_exclusion_regex,
)

# ── log_format_to_vcl_log ────────────────────────────────────────────────────


def test_pure_literal_without_heredoc_terminator_wraps_once():
    """A literal that does NOT contain the heredoc-closing sequence "} wraps
    cleanly into a single ``{"..."}`` heredoc."""
    out = log_format_to_vcl_log("plain text no braces")
    assert out == '{"plain text no braces"}'


def test_macro_extracted_verbatim():
    out = log_format_to_vcl_log('{"ts": %{time.start}V}')
    assert "time.start" in out
    assert " + " in out


def test_multiple_macros_concatenated_with_plus():
    fmt = '{"ip":"%{client.ip}V","ua":"%{req.http.User-Agent}V"}'
    out = log_format_to_vcl_log(fmt)
    assert "client.ip" in out
    assert "req.http.User-Agent" in out
    assert " + " in out


def test_fastly_escaped_braces_unescaped_first():
    fmt = '\\{"raw":"x"\\}'
    out = log_format_to_vcl_log(fmt)
    assert "\\{" not in out
    assert "\\}" not in out


def test_internal_quote_brace_in_literal_split_safely():
    """If a literal contains the heredoc-closing sequence "}, the impl splits
    it into two heredocs to avoid a raw VCL syntax error."""
    fmt = '{"a":"b"}{"c":"d"}'
    out = log_format_to_vcl_log(fmt)
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


def test_lint_fails_closed_when_falco_errors_only_on_stderr():
    """Regression for F011 (audit run 7ba15352): when ``_run_falco_lint``
    returns non-zero with the error text on stderr and an informational
    banner on stdout, lint_log_format must report the failure."""
    fmt = '{"timestamp":"%{time.start}V"}'
    with (
        patch("backend.utils.vcl_utils.shutil.which", return_value="/usr/bin/falco"),
        patch(
            "backend.utils.vcl_utils._run_falco_lint",
            return_value=(1, "falco 0.4.2 linting…", "[ERROR] vcl_log syntax invalid at line 9"),
        ),
    ):
        ok, msg = lint_log_format(fmt)

    assert ok is False, f"lint_log_format must fail-closed when stderr carries the error; got ok={ok!r}, msg={msg!r}"
    assert "[ERROR]" in msg, f"reported message should include the stderr error line: {msg!r}"


def test_lint_fails_closed_on_nonzero_exit_without_recognized_error():
    """Regression for finding 006 (audit run 80e9f210): non-zero exit must fail closed."""
    fmt = '{"timestamp":"%{time.start}V"}'
    with (
        patch("backend.utils.vcl_utils.shutil.which", return_value="/usr/bin/falco"),
        patch(
            "backend.utils.vcl_utils._run_falco_lint",
            return_value=(2, "falco: unexpected token near 'log'", ""),
        ),
    ):
        ok, msg = lint_log_format(fmt)

    assert ok is False, f"non-zero falco exit must fail closed; got ok={ok!r}, msg={msg!r}"


# ── Merged from test_vcl_safety.py ───────────────────────────────────────────


class TestValidateQuotedLiteral:
    def test_accepts_plain_value(self) -> None:
        assert validate_quoted_literal("hello-world_123") == "hello-world_123"

    def test_accepts_empty(self) -> None:
        assert validate_quoted_literal("") == ""

    @pytest.mark.parametrize(
        "bad",
        [
            'has"quote',
            "back\\slash",
            "tab\tinside",
            "newline\ninside",
            "null\x00byte",
            "del\x7fchar",
        ],
    )
    def test_rejects_forbidden_chars_strict(self, bad: str) -> None:
        with pytest.raises(VclSafetyError):
            validate_quoted_literal(bad)

    def test_allow_backslash_permits_regex_escapes(self) -> None:
        assert validate_quoted_literal(r"\.js$", allow_backslash=True) == r"\.js$"

    def test_allow_backslash_still_rejects_quote_and_tab(self) -> None:
        with pytest.raises(VclSafetyError):
            validate_quoted_literal('re"gex', allow_backslash=True)
        with pytest.raises(VclSafetyError):
            validate_quoted_literal("re\tgex", allow_backslash=True)

    def test_is_valueerror_subclass(self) -> None:
        with pytest.raises(ValueError):
            validate_quoted_literal('"')


class TestValidateLogFormatMacro:
    def test_accepts_plain_macro(self) -> None:
        assert validate_log_format_macro("req.http.Host") == "req.http.Host"

    def test_accepts_escaped_heredoc_braces(self) -> None:
        assert validate_log_format_macro(r'strftime(\{"%Y"\}, now)') == r'strftime(\{"%Y"\}, now)'

    @pytest.mark.parametrize(
        "bad",
        [
            "a; b",
            "a /* c */",
            "a // c",
            "close}brace",
            "open{brace",
        ],
    )
    def test_rejects_injection(self, bad: str) -> None:
        with pytest.raises(VclSafetyError):
            validate_log_format_macro(bad)

    def test_stable_error_message(self) -> None:
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


def test_path_within_dir_basic(tmp_path):
    base_dir = str(tmp_path)

    candidate = str(tmp_path / "subdir" / "file.txt")
    assert path_within_dir(base_dir, candidate) is True

    assert path_within_dir(base_dir, base_dir) is True

    candidate_escape = str(tmp_path / ".." / "other_dir")
    assert path_within_dir(base_dir, candidate_escape) is False


def test_path_within_dir_value_error_fallback(monkeypatch):
    def mock_commonpath(paths):
        raise ValueError("Simulated drive mismatch or empty paths")

    monkeypatch.setattr(os.path, "commonpath", mock_commonpath)
    assert path_within_dir("/tmp/base", "/tmp/base/candidate") is False


def test_quote_path_list_doubles_embedded_quotes() -> None:
    assert quote_path_list(["/a/o'brien.parquet", "/b.parquet"]) == "'/a/o''brien.parquet', '/b.parquet'"


def test_escape_sql_literal_rejects_non_str() -> None:
    with pytest.raises(TypeError):
        escape_sql_literal(5)  # type: ignore[arg-type]


def test_no_hand_rolled_quote_doubling_left_in_backend() -> None:
    needle = 'replace("\'", "\'\'")'
    root = pathlib.Path(__file__).resolve().parents[2] / "backend"
    offenders = [
        str(p.relative_to(root))
        for p in root.rglob("*.py")
        if needle in p.read_text(encoding="utf-8") and p.name != "sql_validator.py"
    ]
    assert offenders == []


# ── Merged from test_vcl_validator.py ────────────────────────────────────────


def test_empty_is_valid_and_means_default():
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
    too_long = "a" * (MAX_REGEX_BYTES + 1)
    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex(too_long)
    assert exc.value.reason == "too_long"


def test_quote_rejected():
    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex(r'foo"bar')
    assert exc.value.reason == "disallowed_char"


def test_control_chars_rejected():
    for bad in ("\x00", "\x01", "\x07", "\x1b"):
        with pytest.raises(RegexValidationError) as exc:
            validate_url_exclusion_regex(f"foo{bad}bar")
        assert exc.value.reason == "disallowed_char"


def test_invalid_regex_rejected():
    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex(r"(unclosed")
    assert exc.value.reason == "invalid_regex"

    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex(r"[unbalanced")
    assert exc.value.reason == "invalid_regex"


def test_strips_trailing_whitespace():
    assert validate_url_exclusion_regex("\\.css$\n") == r"\.css$"
    assert validate_url_exclusion_regex("  \\.css$  ") == r"\.css$"


def _falco_available() -> bool:
    return shutil.which("falco") is not None


def test_lint_vcl_skips_cleanly_when_falco_missing():
    with patch("backend.utils.vcl_validator.shutil.which", return_value=None):
        result = lint_vcl("ignore me", snippet_name="test")
    assert result.skipped is True
    assert result.ok is True
    assert "falco" in result.skipped_reason.lower()


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_lint_vcl_accepts_recv_snippet_default():
    from backend.provision.session_scoring_vcl import recv_snippet

    body = recv_snippet("test_svc")
    result = lint_vcl(body, snippet_name="recv_default")
    assert result.ok, f"expected default recv snippet to pass falco; errors={result.errors}"
    assert not result.skipped


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_lint_vcl_accepts_custom_regex():
    from backend.provision.session_scoring_vcl import recv_snippet

    body = recv_snippet("test_svc", exclude_url_regex=r"\.(css|js|png)$")
    result = lint_vcl(body)
    assert result.ok, f"custom regex must pass falco; errors={result.errors}"


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_lint_vcl_rejects_unbalanced_regex_in_assembled_snippet():
    snippet = '# defective snippet\nif (std.tolower(req.url) !~ "unclosed[") {\n  return(pass);\n}'
    result = lint_vcl(snippet)
    assert not result.ok, "falco must flag unbalanced regex in assembled VCL"
    assert any("regex" in e.lower() for e in result.errors), (
        f"expected a regex-related error message, got: {result.errors}"
    )


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_end_to_end_validation_accepts_valid():
    from backend.provision.session_scoring_vcl import recv_snippet

    def builder(cleaned: str) -> str:
        return recv_snippet("svc", exclude_url_regex=cleaned or None)

    cleaned, lint = validate_recv_exclusion_regex_with_lint(
        r"\.(jpg|png)$", build_full_snippet=builder, require_falco=True
    )
    assert cleaned == r"\.(jpg|png)$"
    assert lint.ok


@pytest.mark.skipif(not _falco_available(), reason="requires falco binary")
def test_end_to_end_validation_rejects_input_policy_first():
    from backend.provision.session_scoring_vcl import recv_snippet

    def builder(cleaned: str) -> str:
        return recv_snippet("svc", exclude_url_regex=cleaned or None)

    with pytest.raises(RegexValidationError) as exc:
        validate_recv_exclusion_regex_with_lint(r'foo"bar', build_full_snippet=builder, require_falco=True)
    assert exc.value.reason == "disallowed_char"


def test_end_to_end_validation_require_falco_raises_when_missing():
    def builder(_: str) -> str:
        return "irrelevant"

    with patch("backend.utils.vcl_validator.shutil.which", return_value=None):
        with pytest.raises(RegexValidationError) as exc:
            validate_recv_exclusion_regex_with_lint(r"\.css$", build_full_snippet=builder, require_falco=True)
    assert exc.value.reason == "falco_unavailable"


def test_end_to_end_validation_require_false_passes_when_missing():
    def builder(_: str) -> str:
        return "irrelevant"

    with patch("backend.utils.vcl_validator.shutil.which", return_value=None):
        cleaned, lint = validate_recv_exclusion_regex_with_lint(
            r"\.css$", build_full_snippet=builder, require_falco=False
        )
    assert cleaned == r"\.css$"
    assert lint.skipped


def test_recv_snippet_default_used_when_override_none():
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ASSET_EXT_REGEX,
        recv_snippet,
        resolve_exclude_url_regex,
    )

    assert resolve_exclude_url_regex(None) == DEFAULT_ASSET_EXT_REGEX
    assert resolve_exclude_url_regex("") == DEFAULT_ASSET_EXT_REGEX
    assert resolve_exclude_url_regex("   ") == DEFAULT_ASSET_EXT_REGEX

    body = recv_snippet("svc")
    assert DEFAULT_ASSET_EXT_REGEX in body


def test_recv_snippet_uses_override_when_supplied():
    from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX, recv_snippet

    custom = r"\.(css)$"
    body = recv_snippet("svc", exclude_url_regex=custom)
    assert custom in body
    assert DEFAULT_ASSET_EXT_REGEX not in body


# ── Merged from test_lint_log_format_degraded_mode.py ────────────────────────

FALCO_INSTALLED = shutil.which("falco") is not None
FALCO_REQUIRED = os.environ.get("FALCO_REQUIRED") == "1"

VALID = '{"timestamp":"%{time.start}V","ip":"%{client.ip}V"}'

JSON_REJECTS = [
    pytest.param("", id="empty"),
    pytest.param('{"timestamp":"x"}\n', id="newline"),
    pytest.param('"not_an_object"', id="root_not_object"),
    pytest.param('{"ip":"%{client.ip}V"}', id="missing_timestamp"),
]

VCL_SYNTAX_BROKEN = '{"timestamp":"%{this_is_not_valid_vcl}V"}'


def test_valid_format_accepted_by_json_path():
    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, _ = lint_log_format(VALID)
    assert ok is True


@pytest.mark.parametrize("fmt", JSON_REJECTS)
def test_json_path_rejects(fmt):
    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, msg = lint_log_format(fmt)
    assert ok is False, (
        f"JSON path should reject {fmt!r} but accepted with msg={msg!r}. "
        f"If this rejection moved to the falco-only path, update the test "
        f"deliberately."
    )


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco not on PATH")
def test_valid_format_accepted_by_falco_path():
    ok, _ = lint_log_format(VALID)
    assert ok is True


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco not on PATH")
@pytest.mark.parametrize("fmt", JSON_REJECTS)
def test_falco_path_also_rejects_what_json_rejects(fmt):
    ok, msg = lint_log_format(fmt)
    assert ok is False, (
        f"asymmetry: falco path ACCEPTED {fmt!r} but JSON path rejects it "
        f"(msg={msg!r}). The two paths disagree on the 'definitely broken' "
        f"set — degraded-mode users would be more strict than CI."
    )


def test_json_path_misses_invalid_vcl_macro_contents():
    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, _ = lint_log_format(VCL_SYNTAX_BROKEN)
    assert ok is True, (
        "JSON-only path used to accept syntactically-invalid VCL macros. "
        "If this changed (great!), update the test to pin the new contract."
    )


# ── Merged from test_cmcd_capture_ordering.py ────────────────────────────────

_LF = {
    "groups": ["A", "B", "C"],
    "custom_fields": get_cmcd_fields(True),
    "field_overrides": {},
}


def _recv() -> str:
    snippets = generate_capture_vcl(_LF, cmcd_enabled=True, cmcd_mode="query_string", cmcd_version=1)
    return snippets["recv"]


def test_extraction_precedes_capture_for_every_cmcd_field():
    recv = _recv()
    for cf in get_cmcd_fields(True):
        name = cf["name"]
        key = name[len("cmcd_") :]
        extract = recv.find(f"set req.http.x-cmcd:{key} ")
        capture = recv.find(f"set req.http.x-fos-edge-data:{name} ")
        assert extract != -1, f"no extraction emitted for {name}"
        assert capture != -1, f"no capture emitted for {name}"
        assert extract < capture, (
            f"{name}: extraction at {extract} must come BEFORE capture at {capture} — "
            "capture reads x-cmcd:* and would copy an empty string"
        )


def test_cmcd_extraction_present_only_when_enabled():
    off = generate_capture_vcl(_LF, cmcd_enabled=False)["recv"]
    assert "set req.http.x-cmcd:sid = " not in off
    assert "set req.http.x-cmcd:sid = " in _recv()


def test_url_filter_stays_after_the_subfield_reads():
    recv = _recv()
    last_read = recv.rfind('subfield(var.cmcd, "rtp"')
    strip = recv.find('querystring.filter(req.url, "CMCD")')
    assert last_read != -1 and strip != -1
    assert last_read < strip, "URL filter ran before the CMCD subfields were read"


def test_declarative_generator_orders_extraction_first():
    from backend.provision.declarative.generators import generate_consolidated_snippet
    from backend.provision.declarative.state import CmcdConfig, FeatureState, LogFieldsConfig

    state = FeatureState(
        service_id="svcCMCDORDER",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="fos.example.com",
        logging_enabled=True,
        cmcd=CmcdConfig(enabled=True, mode="query_string", version=1),
        log_fields=LogFieldsConfig(groups=["A", "B", "C"], custom_fields=get_cmcd_fields(True)),
    )

    recv = generate_consolidated_snippet(state, "vcl_recv")
    extract = recv.find("set req.http.x-cmcd:sid ")
    capture = recv.find("set req.http.x-fos-edge-data:cmcd_sid ")
    if extract == -1 or capture == -1:
        return
    assert extract < capture, "declarative generator emitted capture before extraction"
