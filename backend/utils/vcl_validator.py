"""VCL static analysis + user-input validation for scoring snippets.

Anything that ends up interpolated into a VCL snippet that ships to
Fastly is funneled through here so a malformed input can't break the
service version's compile step (which would silently leave the prior
version active and the admin staring at "nothing changed" with no
error).

The validator runs in three layers from cheap → expensive:

  1. ``validate_url_exclusion_regex``: cheap input policing on a regex
     the operator typed — length cap, no quotes (would break the VCL
     string literal), no control chars, must compile under Python's
     ``re`` engine. Catches the majority of bad input in microseconds.

  2. ``lint_vcl``: runs the local ``falco`` binary (Fastly VCL static
     analyzer, github.com/ysugimoto/falco) over the full assembled
     snippet. Catches structural issues — unmatched braces, wrong
     argument types to built-ins, etc. Falco is optional: when the
     binary isn't on PATH (some dev environments don't have it), we
     log a WARNING and skip the static analysis. Production images
     install it via the backend Dockerfile so the path is exercised.

  3. (implicit, runs at deploy time) Fastly's own VCL compiler when
     ``activate_version`` runs against the cloned version. Anything
     that slipped past layers 1+2 fails here with a 422 from the
     Fastly API and the activate step rolls back.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass

from backend.utils.vcl_utils import _run_falco_lint

logger = logging.getLogger(__name__)


# ── Input validation ────────────────────────────────────────────────────────

MAX_REGEX_BYTES = 2048
_DISALLOWED_CHARS_RE = re.compile(r"[\x00-\x08\x0a-\x1f\x7f\"]")


class RegexValidationError(ValueError):
    """Raised when an operator-supplied URL exclusion regex fails policy.

    ``reason`` is a short machine-readable code so the API caller can
    map it to a UI hint; ``message`` is the human-readable explanation.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


def validate_url_exclusion_regex(value: str) -> str:
    """Cheap input policing on a user-typed regex before VCL interpolation.

    Returns the cleaned-up value (stripped of trailing whitespace) on
    success. Raises ``RegexValidationError`` on any policy violation.

    Empty / whitespace-only input is valid and signals "fall back to the
    default" — the caller is expected to substitute the default regex
    when this returns "".
    """
    if not isinstance(value, str):
        raise RegexValidationError("type", f"regex must be a string, got {type(value).__name__}")
    cleaned = value.strip()
    if not cleaned:
        return ""  # "" → caller falls back to default
    if len(cleaned.encode("utf-8")) > MAX_REGEX_BYTES:
        raise RegexValidationError(
            "too_long",
            f"regex exceeds {MAX_REGEX_BYTES}-byte limit (got {len(cleaned.encode('utf-8'))})",
        )
    bad = _DISALLOWED_CHARS_RE.search(cleaned)
    if bad:
        ch = bad.group(0)
        # Don't echo the byte verbatim — could be a control char.
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


# ── Falco static analysis ───────────────────────────────────────────────────


@dataclass
class LintResult:
    """Outcome of running falco on a VCL snippet."""

    ok: bool
    errors: list[str]
    warnings: list[str]
    # True if falco couldn't run (binary missing). Caller decides whether
    # that's a hard fail; production should treat skipped == fail.
    skipped: bool = False
    skipped_reason: str = ""


def _falco_binary() -> str | None:
    """Return the falco binary path, or None if unavailable."""
    return shutil.which("falco")


def lint_vcl(
    snippet: str,
    *,
    snippet_name: str = "scoring_snippet",
    wrap_subroutine: str | None = "vcl_recv",
) -> LintResult:
    """Run ``falco lint`` over a VCL snippet body.

    Falco's parser expects a complete VCL file — snippet bodies on their
    own (a sequence of statements, not wrapped in a subroutine) are
    syntactically invalid as a standalone file even though Fastly accepts
    them as snippet content. To match Fastly's behaviour we wrap the
    snippet in ``sub <wrap_subroutine> { ... }`` plus a minimal backend
    declaration before linting; that's what Fastly does internally when
    it inlines snippets into the main VCL.

    Pass ``wrap_subroutine=None`` for content that's already a full file
    (rarely the case for our snippets). The default ``vcl_recv`` matches
    every snippet generator in ``session_scoring_vcl.py``.

    Falco needs a file path, not stdin — write to a tempfile and invoke
    ``falco lint`` against it. ``snippet_name`` shows up in error
    messages so multi-snippet pipelines can identify which body the
    error came from.
    """
    falco_bin = _falco_binary()
    if falco_bin is None:
        logger.warning(
            "[vcl_validator] falco binary not on PATH; static analysis skipped for %s "
            "(production should install falco via the backend Dockerfile)",
            snippet_name,
        )
        return LintResult(ok=True, errors=[], warnings=[], skipped=True, skipped_reason="falco binary not found")

    # Compose a syntactically-complete VCL file by wrapping the snippet
    # in the same subroutine Fastly inlines it into. We pre-declare the
    # backends and the ``var.fastly_req_do_shield`` magic variable that
    # the scoring snippets reference so falco's symbol resolver doesn't
    # flag them as undefined (Fastly's main VCL declares both — falco's
    # standalone lint mode doesn't know that without seeing them).
    #
    # Two extra bits of boilerplate to keep falco -v output focused on
    # the OPERATOR'S snippet rather than wrapper noise:
    #   1. ``#FASTLY <stage>`` macro inside the sub — without it, falco
    #      emits a "missing Fastly boilerplate comment" warning on every
    #      lint run regardless of snippet content.
    #   2. Sentinel-guarded "uses" of the declared backends + variable
    #      AFTER the snippet body — this dead branch (`X-Lint-Sentinel ==
    #      "0"` never matches in practice) satisfies falco's "unused/
    #      declaration" + "unused/variable" rules without affecting any
    #      real request flow. Without these, every lint surfaces 3
    #      pre-baked warnings that drown out anything the operator
    #      actually changed.
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

    # ``-v`` emits per-warning [WARNING] / [INFO] lines (not just the
    # rolled-up "N warnings" summary). Without it, the parser below
    # sees zero diagnostic lines AND zero errors and reports the
    # snippet as clean — masking real warnings the operator should
    # see (catalog/regex/etc.). The wrapper above is engineered to
    # lint cleanly on its own, so any warning that surfaces here is
    # from the operator's snippet body.
    try:
        returncode, out, err = _run_falco_lint(falco_bin, full_vcl, timeout=10, verbose=True)
    except subprocess.TimeoutExpired:
        return LintResult(
            ok=False,
            errors=[f"falco lint timed out after 10s for snippet {snippet_name!r}"],
            warnings=[],
        )

    # Falco exits non-zero when there are ANY diagnostics (errors or
    # warnings), so the exit code alone isn't a reliable
    # "did the snippet pass?" signal. The authoritative source is the
    # summary line falco emits at the end:
    #     "🔥 N errors, ❗ M warnings, 🔈 K recommendations."
    # We parse the N to decide pass/fail; lines tagged [ERROR] are
    # surfaced as errors, [WARNING] / [INFO] go in warnings.
    out = out.strip()
    err = err.strip()
    combined = "\n".join(filter(None, [out, err]))

    errors: list[str] = []
    warnings: list[str] = []

    # Pre-parse: find the summary line if present.
    summary_re = re.compile(r"(\d+)\s+errors?\s*,\s*(\d+)\s+warnings?")
    summary_match = None
    for line in combined.splitlines():
        if "errors" in line and "warnings" in line and summary_re.search(line):
            summary_match = summary_re.search(line)
            # Don't classify the summary line itself as an error/warning.
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
    ok = summary_errors == 0 if summary_errors is not None else (returncode == 0 and not errors)

    # If falco reported a non-zero error count but no parseable
    # [ERROR] line, surface a generic error so the operator isn't
    # left guessing.
    if summary_errors and summary_errors > 0 and not errors:
        errors.append(f"falco reported {summary_errors} error(s) but no parseable diagnostics; stdout={out[:200]!r}")

    return LintResult(ok=ok, errors=errors, warnings=warnings)


# ── Convenience: validate a recv-snippet exclusion regex end-to-end ─────────


def validate_recv_exclusion_regex_with_lint(
    user_regex: str,
    *,
    build_full_snippet: Callable[[str], str],
    require_falco: bool = True,
) -> tuple[str, LintResult]:
    """One-call validation: input policy → assemble snippet → falco lint.

    ``user_regex`` is what the operator typed. ``build_full_snippet`` is
    a callable that takes the cleaned regex string and returns the
    fully-assembled recv-snippet VCL — we don't know the surrounding
    context (logging service ID, request secret, etc.) here, so the
    caller closes over those.

    ``require_falco``: when True (default), a missing falco binary
    raises ``RegexValidationError``. Production must keep this True so
    a broken Dockerfile doesn't silently downgrade the security
    posture. Tests can pass False to exercise the regex-only path.

    Returns ``(cleaned_regex, lint_result)`` on success. The cleaned
    regex is what gets persisted to svc_cfg; the lint_result.warnings
    is surfaced to the operator alongside the success message.

    Raises ``RegexValidationError`` on any layer's failure.
    """
    cleaned = validate_url_exclusion_regex(user_regex)
    # build_full_snippet must accept the cleaned regex (which may be
    # "" — meaning "use default"). The caller's closure decides how to
    # interpret an empty value.
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
