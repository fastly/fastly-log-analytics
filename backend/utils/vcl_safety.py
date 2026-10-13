"""Unified VCL safety validation — the single source of truth for "what is
safe to emit into generated VCL".

Historically three layers each decided this independently and drifted:

  * ``backend.utils.vcl_utils.log_format_to_vcl_log`` — macro-injection
    checks on log-format ``%{...}V`` expressions.
  * ``backend.provision.session_scoring_vcl`` — a private forbidden-char
    regex pair guarding values f-string-substituted into VCL string
    literals.
  * ``backend.utils.vcl_validator`` — another disallowed-char class plus
    the falco full-document linter.

This module collapses those into four distinct, explicit APIs so the
rules can't drift across call sites:

  1. :func:`validate_quoted_literal` — a value destined for a VCL
     double-quoted ``"..."`` string literal.
  2. :func:`validate_expression` — a bare VCL expression (no surrounding
     quotes) that must not smuggle statement/comment structure.
  3. :func:`validate_log_format_macro` — the ``%{...}V`` inline-VCL
     payload of a Fastly log-format template.
  4. :func:`lint_document` — full-document static analysis via ``falco``.

The character policy is the UNION of the strictest rules from every
legacy layer (see :data:`_FORBIDDEN_STRICT` / :data:`_FORBIDDEN_ALLOW_BACKSLASH`).
Making it stricter than any single legacy layer is intentional and
permitted — these guards are belt-and-suspenders at the generation
boundary; real input is validated upstream and Fastly ``/validate``-gated
before activate.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class VclSafetyError(ValueError):
    """Raised when a value can't be safely embedded in generated VCL.

    Subclasses :class:`ValueError` so existing ``except ValueError``
    handlers at legacy call sites keep catching it.
    """


# ── Character policy ─────────────────────────────────────────────────────────
#
# STRICT bans the double-quote (would terminate the literal), the backslash
# (could introduce an escape the VCL engine interprets), C0 control chars
# (0x00-0x1f, includes tab/newline) and DEL (0x7f).
#
# ALLOW_BACKSLASH is identical but permits the backslash, for values that
# are themselves regexes (``\.`` etc.). Note it still bans the TAB char
# (0x09), which is stricter than the pre-consolidation vcl_validator class
# — intentional per the module docstring.
_FORBIDDEN_STRICT = re.compile(r'["\\\x00-\x1f\x7f]')
_FORBIDDEN_ALLOW_BACKSLASH = re.compile(r'["\x00-\x1f\x7f]')


def validate_quoted_literal(value: str, *, allow_backslash: bool = False, field: str = "value") -> str:
    """Validate a value destined for a VCL double-quoted string literal.

    Returns ``value`` unchanged on success. Raises :class:`VclSafetyError`
    if it contains a character that would break out of (or inject into)
    the surrounding ``"..."`` literal.

    ``allow_backslash`` permits the backslash for regex payloads that
    legitimately contain escapes like ``\\.``.
    """
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


def validate_expression(expr: str, *, field: str = "expression") -> str:
    """Validate a bare (unquoted) VCL expression fragment.

    Rejects the structural tokens that would let a value break out of the
    expression context into statement/comment structure: the statement
    terminator ``;`` and the comment openers ``/*`` and ``//``.

    Returns ``expr`` unchanged on success.
    """
    if ";" in expr or "/*" in expr or "//" in expr:
        raise VclSafetyError(f"{field} contains invalid characters (;, /*, //)")
    return expr


def validate_log_format_macro(macro: str) -> str:
    """Validate the inline-VCL payload of a Fastly ``%{...}V`` log macro.

    This is :func:`validate_expression` plus a ban on UNESCAPED braces
    (a ``{`` / ``}`` not preceded by a backslash). Those are the building
    blocks of the VCL-injection attack from audit finding 008: ``;``
    terminates the surrounding log statement, ``}`` closes the ``vcl_log``
    block, then ``{`` opens an attacker-controlled subroutine. Legitimate
    heredoc patterns like ``strftime(\\{"%Y"\\}, ...)`` use ``\\{`` /
    ``\\}`` escapes and pass cleanly.

    Returns ``macro`` unchanged on success; raises :class:`VclSafetyError`
    otherwise. The message is kept stable for the ``lint_log_format``
    caller that surfaces it to operators.
    """
    if ";" in macro or "/*" in macro or "//" in macro or re.search(r"(?<!\\)[{}]", macro):
        raise VclSafetyError("VCL macro contains invalid characters (;, /*, //, unescaped {, unescaped })")
    return macro


# ── Full-document static analysis (falco) ────────────────────────────────────


@dataclass
class LintResult:
    """Outcome of running falco on a VCL document."""

    ok: bool
    errors: list[str]
    warnings: list[str]
    # True if falco couldn't run (binary missing). Caller decides whether
    # that's a hard fail; production should treat skipped == fail.
    skipped: bool = False
    skipped_reason: str = ""


def _parse_falco_output(returncode: int, out: str, err: str) -> tuple[list[str], list[str], int | None]:
    """Parse falco ``-v`` output into (errors, warnings, summary_error_count).

    Shared by :func:`lint_document` and the scoring-snippet wrapper in
    :mod:`backend.utils.vcl_validator`.
    """
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
    """Run ``falco lint`` over a COMPLETE VCL document.

    ``vcl_text`` must already be a syntactically-complete VCL file
    (subroutines declared, backends present) — this function does not
    wrap it. Snippet-body callers that need subroutine wrapping use the
    higher-level helpers in :mod:`backend.utils.vcl_validator`.

    ``falco_bin``: pass a pre-resolved binary path (so the caller's own
    ``shutil.which`` mock governs the skip branch); when ``None`` this
    resolves it via ``shutil.which`` in this module's namespace.
    """
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
        from backend.utils.vcl_utils import _run_falco_lint

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
