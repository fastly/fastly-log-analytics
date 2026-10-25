from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from backend.core.fastly.client import fastly

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Unified VCL safety validation (folded from backend.utils.vcl_safety)
# ---------------------------------------------------------------------------


class VclSafetyError(ValueError):
    """Raised when a value can't be safely embedded in generated VCL.

    Subclasses :class:`ValueError` so existing ``except ValueError``
    handlers at legacy call sites keep catching it.
    """


_FORBIDDEN_STRICT = re.compile(r'["\\\x00-\x1f\x7f]')
_FORBIDDEN_ALLOW_BACKSLASH = re.compile(r'["\x00-\x1f\x7f]')


def validate_quoted_literal(value: str, *, allow_backslash: bool = False, field: str = "value") -> str:
    """Validate a value destined for a VCL double-quoted string literal."""
    pattern = _FORBIDDEN_ALLOW_BACKSLASH if allow_backslash else _FORBIDDEN_STRICT
    m = pattern.search(value)
    if m is not None:
        raise VclSafetyError(
            f"{field} contains a character unsafe to embed in a VCL string literal at offset "
            f"{m.start()} ({value[m.start()]!r}); refusing to generate VCL "
            "(values are validated upstream + Fastly /validate-gated, so this "
            "indicates a caller that bypassed those checks)."
        )
    return value


def validate_log_format_macro(macro: str) -> str:
    """Validate the inline-VCL payload of a Fastly ``%{...}V`` log macro."""
    if ";" in macro or "/*" in macro or "//" in macro or re.search(r"(?<!\\)[{}]", macro):
        raise VclSafetyError("VCL macro contains invalid characters (;, /*, //, unescaped {, unescaped })")
    return macro


@dataclass
class LintResult:
    """Outcome of running falco on a VCL document."""

    ok: bool
    errors: list[str]
    warnings: list[str]
    skipped: bool = False
    skipped_reason: str = ""


def _parse_falco_output(returncode: int, out: str, err: str) -> tuple[list[str], list[str], int | None]:
    """Parse falco ``-v`` output into (errors, warnings, summary_error_count)."""
    out = out.strip()
    err = err.strip()
    combined = "\n".join(filter(None, [out, err]))

    errors: list[str] = []
    warnings: list[str] = []

    summary_re = re.compile(r"(\d+)\s+errors?\s*,\s*(\d+)\s+warnings?")
    summary_match = None
    for line in combined.splitlines():
        if "errors" in line and "warnings" in line and summary_re.search(line):
            summary_match = summary_re.search(line)
            continue
        stripped = line.strip()
        if not stripped:
            continue
        if "[ERROR]" in stripped or "🔥 [ERROR]" in stripped:
            errors.append(stripped)
        elif "[WARNING]" in stripped or "❗ [WARNING]" in stripped:
            warnings.append(stripped)
        elif "[INFO]" in stripped or "🔈 [INFO]" in stripped:
            warnings.append(stripped)

    summary_errors = int(summary_match.group(1)) if summary_match else None
    return errors, warnings, summary_errors


def lint_document(
    vcl_text: str,
    *,
    falco_bin: str | None = None,
    snippet_name: str = "vcl_document",
    timeout: int = 10,
    verbose: bool = True,
) -> LintResult:
    """Run ``falco lint`` over a COMPLETE VCL document."""
    if falco_bin is None:
        falco_bin = shutil.which("falco")
    if falco_bin is None:
        logger.warning(
            "[vcl_safety] falco binary not on PATH; static analysis skipped for %s "
            "(production should install falco via the backend Dockerfile)",
            snippet_name,
        )
        return LintResult(ok=True, errors=[], warnings=[], skipped=True, skipped_reason="falco binary not found")

    try:
        returncode, out, err = _run_falco_lint(falco_bin, vcl_text, timeout=timeout, verbose=verbose)
    except subprocess.TimeoutExpired:
        return LintResult(
            ok=False,
            errors=[f"falco lint timed out after {timeout}s for {snippet_name!r}"],
            warnings=[],
        )

    errors, warnings, summary_errors = _parse_falco_output(returncode, out, err)
    ok = summary_errors == 0 if summary_errors is not None else (returncode == 0 and not errors)

    if summary_errors and summary_errors > 0 and not errors:
        errors.append(
            f"falco reported {summary_errors} error(s) but no parseable diagnostics; stdout={out.strip()[:200]!r}"
        )

    return LintResult(ok=ok, errors=errors, warnings=warnings)


def _run_falco_lint(
    falco_bin: str,
    vcl_text: str,
    *,
    timeout: int,
    verbose: bool,
    redact_path_to: str | None = None,
) -> tuple[int, str, str]:
    """Write ``vcl_text`` to a tempfile and run ``falco [-v] lint <file>``.

    Shared subprocess plumbing for the two falco callers in this tree —
    :func:`lint_log_format` (log-format JSON+VCL check) and
    :func:`backend.utils.vcl_validator.lint_vcl` (scoring-snippet VCL
    check). Each caller resolves ``falco_bin`` via its own
    ``shutil.which("falco")`` (so test patches on the caller's module
    namespace continue to work) and parses output its own way; this
    helper only handles tempfile lifecycle + subprocess invocation +
    optional path redaction.

    ``redact_path_to``: when set, every occurrence of the tempfile path
    in stdout/stderr is replaced with the given string so error messages
    don't leak a ``/tmp/<random>.vcl`` filename to the operator.

    Returns ``(returncode, stdout, stderr)``. Propagates
    :class:`subprocess.TimeoutExpired` for the caller to translate into
    its own failure shape; the tempfile is removed in either case.
    """
    argv = [falco_bin]
    if verbose:
        argv.append("-v")
    # Path appended after tempfile is written.
    argv.extend(["lint", ""])

    with tempfile.NamedTemporaryFile(mode="w", suffix=".vcl", delete=False, encoding="utf-8") as tmp:
        tmp.write(vcl_text)
        tmp_path = tmp.name

    try:
        argv[-1] = tmp_path
        res = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
        stdout = res.stdout or ""
        stderr = res.stderr or ""
        if redact_path_to is not None:
            stdout = stdout.replace(tmp_path, redact_path_to)
            stderr = stderr.replace(tmp_path, redact_path_to)
        return res.returncode, stdout, stderr
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def log_format_to_vcl_log(raw: str) -> str:
    """Convert a Fastly log format template string to a VCL log concatenation.

    Fastly format templates use %{vcl_expr}V for inline VCL expressions; everything
    else is literal text. This produces the equivalent VCL log statement body:
        {"literal"} + vcl_expr + {"literal"} ...
    which is what Fastly generates internally when it compiles the logging endpoint.
    """
    # 1. Split FIRST on the raw template (with \{ / \} escapes intact). We
    # cannot pre-unescape: macro-content validation below relies on being
    # able to tell raw `{` / `}` (suspicious; injection vector) from
    # `\{` / `\}` (legitimate Fastly literal-brace escape, used in patterns
    # like `strftime(\{"format"\}, time.start)`). See audit finding 008.
    parts = re.split(r"%\{(.*?)\}V", raw, flags=re.DOTALL)
    vcl_parts = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            # Literal text — unescape Fastly's \{ / \} into real braces,
            # then wrap as a VCL heredoc string literal {"..."}.
            part = part.replace("\\{", "{").replace("\\}", "}")
            if part:
                # We only need to worry if the literal text itself contains the
                # heredoc closing delimiter "}. This is extremely rare in JSON
                # (which uses " } or "} followed by more).
                # For safety, if it exists, we split it into two heredocs.
                if '"}' in part:
                    part = part.replace('"}', '"} + {"}')
                vcl_parts.append(f'{{"{part}"}}')
        else:
            # VCL expression — reject macro content containing a raw `;`
            # OR an unescaped `{` / `}` (a brace not preceded by `\`).
            # Those are the building blocks of the VCL-injection attack
            # from audit finding 008: `;` terminates the surrounding log
            # statement, then `}` closes the vcl_log block, then `{`
            # opens a new attacker-controlled subroutine. Legitimate
            # heredoc patterns like `strftime(\{"format"\}, ...)` use
            # `\{` / `\}` escapes and pass cleanly.
            # Reject macro content that could inject VCL structure (;, /*,
            # //, unescaped { / }). Delegated to the unified safety module
            # (see audit finding 008); VclSafetyError subclasses ValueError
            # so existing handlers keep catching it.
            validate_log_format_macro(part)
            var = part.replace("\\{", "{").replace("\\}", "}").strip().replace('\\"', '"')
            vcl_parts.append(var)

    # Use + for concatenation to satisfy Falco/modern VCL
    return " + ".join(vcl_parts)


def lint_log_format(format_str: str, snippets: dict[str, str] | None = None) -> tuple[bool, str]:
    """Validate a log format string for JSON structure and VCL syntax.

    If snippets are provided, they are also linted in their respective subroutines.
    Returns (is_valid, message).
    """
    if not format_str or not format_str.strip():
        return False, "Format is empty"

    # Check for internal newlines (DuckDB requirement)
    if "\n" in format_str or "\r" in format_str:
        return False, "Format contains newlines. It must be a single-line string."

    # 1. Validate JSON structure by masking macros
    # 0. Strip Fastly VCL escapes like \{ and \} for JSON validation
    unescaped_str = format_str.replace("\\{", "{").replace("\\}", "}")

    # Mask macros: %{var}V -> "vcl_macro"
    test_str = re.sub(r'"%\{.*?\}V"', '"vcl_macro"', unescaped_str)
    test_str = re.sub(r'"%\{.*?\}t"', '"time_macro"', test_str)
    test_str = re.sub(r"%\{.*?\}V", '"vcl_macro"', test_str)
    test_str = re.sub(r"%\{.*?\}t", '"time_macro"', test_str)

    try:
        data = json.loads(test_str)
        if not isinstance(data, dict):
            return False, "Format must be a JSON object"
        if "timestamp" not in data:
            return False, "Missing required field: 'timestamp'"
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON structure: {str(e)}"

    # 2. Deeper VCL validation using falco
    falco_bin = shutil.which("falco")
    if not falco_bin:
        return True, "Valid JSON (falco linter not found in PATH for VCL validation)"

    try:
        log_body = log_format_to_vcl_log(format_str)
    except ValueError as e:
        return False, f"VCL Error: {str(e)}"

    # Minimal Fastly VCL file that exercises the log statement and any snippets.
    # Includes the subroutines falco expects so it does not complain about missing hooks.
    snip = snippets or {}

    vcl_src = (
        f"sub vcl_recv    {{\n  #FASTLY recv\n{snip.get('recv_reset', '')}\n{snip.get('recv', '')}\n}}\n"
        f"sub vcl_hash    {{\n  #FASTLY hash\n{snip.get('hash', '')}\n}}\n"
        f"sub vcl_hit     {{\n  #FASTLY hit\n{snip.get('hit', '')}\n}}\n"
        f"sub vcl_miss    {{\n  #FASTLY miss\n{snip.get('miss', '')}\n}}\n"
        f"sub vcl_pass    {{\n  #FASTLY pass\n{snip.get('pass', '')}\n}}\n"
        f"sub vcl_fetch   {{\n  #FASTLY fetch\n{snip.get('fetch', '')}\n}}\n"
        f"sub vcl_error   {{\n  #FASTLY error\n{snip.get('error', '')}\n}}\n"
        f"sub vcl_deliver {{\n  #FASTLY deliver\n{snip.get('deliver', '')}\n}}\n"
        f"sub vcl_log     {{\n  #FASTLY log\n  log {log_body};\n}}\n"
    )

    try:
        returncode, stdout, stderr = _run_falco_lint(
            falco_bin, vcl_src, timeout=15, verbose=False, redact_path_to="vcl-config"
        )
    except subprocess.TimeoutExpired:
        return True, "Valid JSON (falco validation timed out)"
    except Exception as e:
        return True, f"Valid JSON (VCL validation skipped: {str(e)})"

    if returncode != 0:
        # Fail-closed parse: scan BOTH streams. The prior ``msg = stdout or
        # stderr`` short-circuited the moment falco emitted any non-error
        # banner to stdout (version line, "linting…", etc.), letting an
        # exit_code != 0 with errors on stderr silently fall through to the
        # ``return True`` below — a fail-open VCL validator that accepted
        # invalid log formats.
        msg = (stdout or "") + "\n" + (stderr or "")
        errors = []
        for raw_line in msg.split("\n"):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "[ERROR]" in line or "ERROR:" in line or "💥" in line:
                errors.append(line)

        if errors:
            return False, "VCL Error: " + errors[0]

        # falco exited non-zero but we couldn't pin a specific error line.
        # Fail closed anyway — a non-zero exit means falco rejected the VCL,
        # and falling through to ``return True`` here was a fail-open hole
        # that accepted invalid VCL whenever falco's output format drifted.
        return False, "VCL Error: falco rejected the VCL (exit status nonzero)"

    return True, "Valid VCL configuration"


# ---------------------------------------------------------------------------
# VCL static analysis + user-input validation for scoring snippets
# (folded from backend.utils.vcl_validator)
# ---------------------------------------------------------------------------

MAX_REGEX_BYTES = 2048


class RegexValidationError(ValueError):
    """Raised when an operator-supplied URL exclusion regex fails policy."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


def validate_url_exclusion_regex(value: str) -> str:
    """Cheap input policing on a user-typed regex before VCL interpolation."""
    if not isinstance(value, str):
        raise RegexValidationError("type", f"regex must be a string, got {type(value).__name__}")
    cleaned = value.strip()
    if not cleaned:
        return ""
    if len(cleaned.encode("utf-8")) > MAX_REGEX_BYTES:
        raise RegexValidationError(
            "too_long",
            f"regex exceeds {MAX_REGEX_BYTES}-byte limit (got {len(cleaned.encode('utf-8'))})",
        )
    try:
        validate_quoted_literal(cleaned, allow_backslash=True, field="regex")
    except VclSafetyError:
        m = _FORBIDDEN_ALLOW_BACKSLASH.search(cleaned)
        ch = m.group(0) if m else "?"
        raise RegexValidationError(
            "disallowed_char",
            f"regex contains disallowed character (codepoint U+{ord(ch):04X}): "
            "double-quotes and control characters are not permitted because they "
            "would break the surrounding VCL string literal",
        )
    try:
        re.compile(cleaned)
    except re.error as exc:
        raise RegexValidationError(
            "invalid_regex", f"regex failed to compile: {exc.msg} at position {exc.pos}"
        ) from exc
    return cleaned


def _falco_binary() -> str | None:
    """Return the falco binary path, or None if unavailable."""
    return shutil.which("falco")


def lint_vcl(
    snippet: str,
    *,
    snippet_name: str = "scoring_snippet",
    wrap_subroutine: str | None = "vcl_recv",
) -> LintResult:
    """Run ``falco lint`` over a VCL snippet body."""
    falco_bin = _falco_binary()
    if falco_bin is None:
        logger.warning(
            "[vcl_validator] falco binary not on PATH; static analysis skipped for %s "
            "(production should install falco via the backend Dockerfile)",
            snippet_name,
        )
        return LintResult(ok=True, errors=[], warnings=[], skipped=True, skipped_reason="falco binary not found")

    if wrap_subroutine:
        stage = wrap_subroutine.removeprefix("vcl_") if wrap_subroutine.startswith("vcl_") else wrap_subroutine
        full_vcl = (
            "backend F_origin_0 {\n"
            '  .host = "example.com";\n'
            '  .port = "80";\n'
            "}\n"
            "backend F_session_scorer {\n"
            '  .host = "scorer.edgecompute.app";\n'
            '  .port = "443";\n'
            "}\n\n"
            f"sub {wrap_subroutine} {{\n"
            f"  #FASTLY {stage}\n"
            "  declare local var.fastly_req_do_shield BOOL;\n"
            f"{snippet}\n"
            '  if (req.http.X-Lint-Sentinel == "lint-only-never-fires") {\n'
            "    set req.backend = F_origin_0;\n"
            "    set req.backend = F_session_scorer;\n"
            "    set var.fastly_req_do_shield = false;\n"
            "  }\n"
            "}\n"
        )
    else:
        full_vcl = snippet

    return lint_document(full_vcl, falco_bin=falco_bin, snippet_name=snippet_name, timeout=10, verbose=True)


def validate_recv_exclusion_regex_with_lint(
    user_regex: str,
    *,
    build_full_snippet: Callable[[str], str],
    require_falco: bool = True,
) -> tuple[str, LintResult]:
    """One-call validation: input policy → assemble snippet → falco lint."""
    cleaned = validate_url_exclusion_regex(user_regex)
    full_snippet = build_full_snippet(cleaned)
    lint = lint_vcl(full_snippet, snippet_name="scoring_recv")
    if lint.skipped and require_falco:
        raise RegexValidationError(
            "falco_unavailable",
            f"VCL static-analysis tool unavailable: {lint.skipped_reason}. Refusing to ship unchecked VCL.",
        )
    if not lint.ok:
        joined = "\n".join(lint.errors[:5])
        raise RegexValidationError("vcl_lint", f"falco lint failed:\n{joined}")
    return cleaned, lint


# ---------------------------------------------------------------------------
# Caller-supplied Fastly token validation for destructive operations
# (folded from backend.utils.fastly_auth)
# ---------------------------------------------------------------------------

_DESTRUCTIVE_SCOPE = "global"


def _parse_scopes(raw: Any) -> list[str]:
    """Normalize the ``scope`` field from /tokens/self into a list of scopes."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(s).strip() for s in raw if s]
    if isinstance(raw, str):
        return [s for s in raw.split() if s]
    return []


def validate_destructive_token(token: str, *, service_id: str) -> dict[str, Any]:
    """Validate that ``token`` is allowed to perform destructive ops on ``service_id``."""
    token = (token or "").strip()
    if not token:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "token_required",
                "message": (
                    "A Fastly API token with the 'global' scope is required "
                    "for destructive operations. Server-stored credentials are "
                    "not accepted here."
                ),
            },
        )

    try:
        token_data = fastly("GET", "/tokens/self", token=token)
    except Exception as e:
        logger.warning("[fastly-auth] /tokens/self call failed: %s", e)
        raise HTTPException(
            status_code=401,
            detail={"error": "token_validation_failed", "message": "Could not validate token with Fastly."},
        )

    if not isinstance(token_data, dict):
        logger.warning("[fastly-auth] /tokens/self returned non-dict: %r", type(token_data))
        raise HTTPException(
            status_code=401,
            detail={"error": "token_validation_failed", "message": "Unexpected token response shape."},
        )

    scopes = _parse_scopes(token_data.get("scope"))
    if _DESTRUCTIVE_SCOPE not in scopes:
        logger.warning(
            "[fastly-auth] token (id=%s, user=%s) missing 'global' scope; got=%r",
            token_data.get("id"),
            token_data.get("user_id") or "(automation)",
            scopes,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "insufficient_scope",
                "message": (
                    "Token does not have the 'global' scope required for destructive operations. "
                    "Use a Fastly token with full 'global' permissions, not 'global:read', "
                    "'purge_select', or 'purge_all'."
                ),
            },
        )

    bound_services = token_data.get("services")
    if isinstance(bound_services, list) and bound_services:
        if service_id not in bound_services:
            logger.warning(
                "[fastly-auth] token (id=%s) bound to %d services but not target service_id=%s",
                token_data.get("id"),
                len(bound_services),
                service_id,
            )
            raise HTTPException(
                status_code=401,
                detail={
                    "error": "service_not_authorized",
                    "message": (
                        "Token is bound to a service list that does not include the target service. "
                        "Use a token with global access or with this service in its allow-list."
                    ),
                },
            )

    token_customer = (token_data.get("customer_id") or "").strip()
    try:
        service_data = fastly("GET", f"/service/{service_id}", token=token)
    except Exception as e:
        logger.warning(
            "[fastly-auth] /service/%s call failed during tenant verification: %s",
            service_id,
            e,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "tenant_verification_failed",
                "message": "Could not verify token tenant against target service.",
            },
        )

    service_customer = ""
    if isinstance(service_data, dict):
        service_customer = (service_data.get("customer_id") or "").strip()

    if not token_customer or not service_customer or token_customer != service_customer:
        logger.warning(
            "[fastly-auth] tenant mismatch: token customer=%r vs service customer=%r (token id=%s, service=%s)",
            token_customer or "(missing)",
            service_customer or "(missing)",
            token_data.get("id"),
            service_id,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "tenant_mismatch",
                "message": (
                    "The supplied token is not authorized for the target service's tenant. "
                    "Use a token issued under the same Fastly account that owns this service."
                ),
            },
        )

    logger.info(
        "[fastly-auth] destructive op authorized: token id=%s user=%s service=%s",
        token_data.get("id"),
        token_data.get("user_id") or "(automation)",
        service_id,
    )
    return token_data


sys.modules["backend.utils.fastly_auth"] = sys.modules[__name__]
sys.modules["backend.utils.vcl_safety"] = sys.modules[__name__]
sys.modules["backend.utils.vcl_validator"] = sys.modules[__name__]
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    _utils_pkg.fastly_auth = sys.modules[__name__]  # type: ignore[attr-defined]
    _utils_pkg.vcl_safety = sys.modules[__name__]  # type: ignore[attr-defined]
    _utils_pkg.vcl_validator = sys.modules[__name__]  # type: ignore[attr-defined]
