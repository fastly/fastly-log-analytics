"""VCL log format generation, safety validation, falco linting, and Fastly token auth."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import structlog
from fastapi import HTTPException

from backend.core.catalog import (
    _NUMERIC_FIELD_MAX_LEN,
    FASTLY_LOG_LINE_DELIVER_MAX,
    LOG_FIELD_CATALOG,
    resolve_enabled_fields,
)
from backend.core.fastly.client import fastly

logger = structlog.get_logger(__name__)


def generate_log_format(log_fields_config: dict) -> str:
    """Build a compact single-line JSON VCL log format string with aggregate-budget clamping (F013)."""
    enabled = resolve_enabled_fields(log_fields_config)
    limits = log_fields_config.get("field_limits") or {}
    custom_fields = [cf for cf in log_fields_config.get("custom_fields", []) if cf.get("enabled", True)]

    budget = FASTLY_LOG_LINE_DELIVER_MAX - (2 + len(enabled) * 5 + len(custom_fields) * 5)
    for field in LOG_FIELD_CATALOG:
        if field["id"] in enabled and field["id"] not in ("url", "ua", "referer"):
            budget -= int(field.get("typical_bytes", 0) or 0)

    parts = []
    for field in LOG_FIELD_CATALOG:
        if field["id"] not in enabled or field["vcl"] is None:
            continue
        vcl = field["vcl"]
        fid = field["id"]
        if fid == "url":
            limit = max(0, min(int(limits.get("url", 2000)), budget))
            budget -= limit
            vcl = vcl.replace("substr(req.url, 0, 2000)", f"substr(req.url, 0, {limit // 6})")
        elif fid in ("ua", "referer"):
            lim = max(0, min(int(limits.get(fid, 1000)), budget))
            budget -= lim
            hdr = "User-Agent" if fid == "ua" else "Referer"
            vcl = (
                f'"{fid}":"%{{json.escape(substr(if(req.http.x-fos-edge-data:{fid} != "",'
                f' req.http.x-fos-edge-data:{fid}, req.http.{hdr}), 0, {lim // 6}))}}V"'
            )
        parts.append(vcl)

    for cf in sorted(custom_fields, key=lambda x: x["name"]):
        name = cf["name"]
        stage = cf.get("collection_stage", "edge")
        value_type = cf.get("value_type", "string")
        estimate = cf.get("bytes_estimate")
        default_limit = min(max((estimate or 0) * 4, 40), 2000) if estimate else 2000
        cf_limit = int(cf.get("byte_limit") or limits.get(name) or default_limit)
        if value_type in ("numeric", "boolean"):
            cf_limit = min(cf_limit, _NUMERIC_FIELD_MAX_LEN)
        cf_limit = max(0, min(cf_limit, budget))
        budget -= cf_limit

        if stage == "deliver":
            raw_expr = cf.get("vcl_log_expression") or f"req.http.x-fos-edge-data:{name}"
            if value_type in ("numeric", "boolean"):
                vcl_macro = (
                    f"if(fastly.ff.visits_this_service == 0 && "
                    f'substr({raw_expr}, 0, {cf_limit}) ~ "^-?[0-9]+(\\\\.[0-9]+)?$", substr({raw_expr}, 0, {cf_limit}), "null")'
                )
                parts.append(f'"{name}":%{{{vcl_macro}}}V')
            else:
                vcl_macro = (
                    f'json.escape(if(fastly.ff.visits_this_service == 0, substr({raw_expr}, 0, {cf_limit // 6}), ""))'
                )
                parts.append(f'"{name}":"%{{{vcl_macro}}}V"')
            continue

        expr = f"req.http.x-fos-origin-data:{name}" if stage == "origin" else f"req.http.x-fos-edge-data:{name}"
        if value_type in ("numeric", "boolean"):
            vcl_macro = (
                f'if(substr({expr}, 0, {cf_limit}) ~ "^-?[0-9]+(\\\\.[0-9]+)?$", substr({expr}, 0, {cf_limit}), "null")'
            )
            parts.append(f'"{name}":%{{{vcl_macro}}}V')
        else:
            parts.append(f'"{name}":"%{{json.escape(substr({expr}, 0, {cf_limit // 6}))}}V"')

    return re.sub(r"\s+", " ", "{" + ",".join(parts) + "}").strip()


def format_hash(log_fields_config: dict) -> str:
    """Return a SHA256 fingerprint of the generated log format for drift detection."""
    return "sha256:" + hashlib.sha256(generate_log_format(log_fields_config).encode()).hexdigest()


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
    """Write ``vcl_text`` to a tempfile and run ``falco [-v] lint <file>``."""
    argv = [falco_bin]
    if verbose:
        argv.append("-v")
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
    """Convert a Fastly log format template string to a VCL log concatenation."""
    parts = re.split(r"%\{(.*?)\}V", raw, flags=re.DOTALL)
    vcl_parts = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            part = part.replace("\\{", "{").replace("\\}", "}")
            if part:
                if '"}' in part:
                    part = part.replace('"}', '"} + {"}')
                vcl_parts.append(f'{{"{part}"}}')
        else:
            validate_log_format_macro(part)
            var = part.replace("\\{", "{").replace("\\}", "}").strip().replace('\\"', '"')
            vcl_parts.append(var)

    return " + ".join(vcl_parts)


def lint_log_format(format_str: str, snippets: dict[str, str] | None = None) -> tuple[bool, str]:
    """Validate a log format string for JSON structure and VCL syntax."""
    if not format_str or not format_str.strip():
        return False, "Format is empty"

    if "\n" in format_str or "\r" in format_str:
        return False, "Format contains newlines. It must be a single-line string."

    unescaped_str = format_str.replace("\\{", "{").replace("\\}", "}")

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

    falco_bin = shutil.which("falco")
    if not falco_bin:
        return True, "Valid JSON (falco linter not found in PATH for VCL validation)"

    try:
        log_body = log_format_to_vcl_log(format_str)
    except ValueError as e:
        return False, f"VCL Error: {str(e)}"

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

        return False, "VCL Error: falco rejected the VCL (exit status nonzero)"

    return True, "Valid VCL configuration"


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


def _auth_err(code: str, msg: str) -> HTTPException:
    return HTTPException(status_code=401, detail={"error": code, "message": msg})


def validate_destructive_token(token: str, *, service_id: str) -> dict[str, Any]:
    """Validate that ``token`` is allowed to perform destructive ops on ``service_id``."""
    token = (token or "").strip()
    if not token:
        raise _auth_err(
            "token_required",
            "A Fastly API token with the 'global' scope is required for destructive operations. Server-stored credentials are not accepted here.",
        )

    try:
        token_data = fastly("GET", "/tokens/self", token=token)
    except Exception as e:
        logger.warning("[fastly-auth] /tokens/self call failed: %s", e)
        raise _auth_err("token_validation_failed", "Could not validate token with Fastly.")

    if not isinstance(token_data, dict):
        logger.warning("[fastly-auth] /tokens/self returned non-dict: %r", type(token_data))
        raise _auth_err("token_validation_failed", "Unexpected token response shape.")

    scopes = _parse_scopes(token_data.get("scope"))
    if _DESTRUCTIVE_SCOPE not in scopes:
        logger.warning(
            "[fastly-auth] token (id=%s, user=%s) missing 'global' scope; got=%r",
            token_data.get("id"),
            token_data.get("user_id") or "(automation)",
            scopes,
        )
        raise _auth_err(
            "insufficient_scope",
            "Token does not have the 'global' scope required for destructive operations. Use a Fastly token with full 'global' permissions, not 'global:read', 'purge_select', or 'purge_all'.",
        )

    bound_services = token_data.get("services")
    if isinstance(bound_services, list) and bound_services and service_id not in bound_services:
        logger.warning(
            "[fastly-auth] token (id=%s) bound to %d services but not target service_id=%s",
            token_data.get("id"),
            len(bound_services),
            service_id,
        )
        raise _auth_err(
            "service_not_authorized",
            "Token is bound to a service list that does not include the target service. Use a token with global access or with this service in its allow-list.",
        )

    token_customer = (token_data.get("customer_id") or "").strip()
    try:
        service_data = fastly("GET", f"/service/{service_id}", token=token)
    except Exception as e:
        logger.warning("[fastly-auth] /service/%s call failed during tenant verification: %s", service_id, e)
        raise _auth_err("tenant_verification_failed", "Could not verify token tenant against target service.")

    service_customer = (service_data.get("customer_id") or "").strip() if isinstance(service_data, dict) else ""
    if not token_customer or not service_customer or token_customer != service_customer:
        logger.warning(
            "[fastly-auth] tenant mismatch: token customer=%r vs service customer=%r (token id=%s, service=%s)",
            token_customer or "(missing)",
            service_customer or "(missing)",
            token_data.get("id"),
            service_id,
        )
        raise _auth_err(
            "tenant_mismatch",
            "The supplied token is not authorized for the target service's tenant. Use a token issued under the same Fastly account that owns this service.",
        )

    logger.info(
        "[fastly-auth] destructive op authorized: token id=%s user=%s service=%s",
        token_data.get("id"),
        token_data.get("user_id") or "(automation)",
        service_id,
    )
    return token_data
