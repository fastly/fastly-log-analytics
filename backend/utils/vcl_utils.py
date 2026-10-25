import json
import os
import re
import shutil
import subprocess
import tempfile

from backend.utils.vcl_safety import validate_log_format_macro


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
# Caller-supplied Fastly token validation for destructive operations
# (folded from backend.utils.fastly_auth)
# ---------------------------------------------------------------------------

import logging  # noqa: E402
import sys  # noqa: E402
from typing import Any  # noqa: E402

from fastapi import HTTPException  # noqa: E402

from backend.core.fastly.client import fastly  # noqa: E402

logger = logging.getLogger(__name__)

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
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    _utils_pkg.fastly_auth = sys.modules[__name__]  # type: ignore[attr-defined]
