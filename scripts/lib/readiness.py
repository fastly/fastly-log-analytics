"""Bounded app readiness: frontend HTML + backend health + admin bootstrap.

Proves the app tier answers on BOTH sides before data checks run. It does not
prove ingestion or data freshness; those stay with the later health/verify
gates. Uses ``admin_urlopen`` so a configured mTLS origin presents its client
identity and every other origin is plain HTTP(S). Output is sanitized: status
codes, exception type names and shape verdicts only, never response bodies.
"""

import argparse
import json
import re
import sys
import time
import tomllib
from collections.abc import Callable
from pathlib import Path
from urllib.error import HTTPError

# Keep direct `python scripts/lib/readiness.py` invocations working.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.lib.admin_tls import admin_urlopen, validate_admin_tls_config  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
_MAX_HTML_BYTES = 2 * 1024 * 1024
_MAX_JSON_BYTES = 32 * 1024 * 1024


def normalize_version(version: str) -> str:
    """Compare PEP 440 and display spellings (3.0.0b3 == 3.0.0-beta3)."""
    value = version.strip().lower().lstrip("v")
    value = re.sub(r"[-_.]?(alpha|a)[-_.]?(?=\d)", "a", value)
    value = re.sub(r"[-_.]?(beta|b)[-_.]?(?=\d)", "b", value)
    value = re.sub(r"[-_.]?(rc|c)[-_.]?(?=\d)", "rc", value)
    return value


def pyproject_version(path: Path = ROOT / "pyproject.toml") -> str:
    with path.open("rb") as handle:
        return tomllib.load(handle)["project"]["version"]


def _get(url: str, timeout: float, limit: int) -> tuple[int, str, bytes]:
    try:
        with admin_urlopen(url, timeout=timeout) as response:
            return response.status, response.headers.get("Content-Type", ""), response.read(limit + 1)
    except HTTPError as exc:
        try:
            return exc.code, "", b""
        finally:
            exc.close()


def _safe(value: object, limit: int = 40) -> str:
    """Bounded, printable rendering of an untrusted response value."""
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    cleaned = "".join(ch if ch.isprintable() else "?" for ch in value[:limit])
    return cleaned + ("..." if len(value) > limit else "")


def check_frontend(origin: str, timeout: float) -> str | None:
    status, content_type, body = _get(f"{origin}/dashboard", timeout, _MAX_HTML_BYTES)
    if status != 200:
        return f"frontend /dashboard HTTP {status}"
    if "text/html" not in content_type.lower():
        return "frontend /dashboard is not HTML"
    if len(body) > _MAX_HTML_BYTES:
        return "frontend /dashboard response exceeds size limit"
    if not body.strip():
        return "frontend /dashboard returned an empty body"
    return None


def _json(url: str, timeout: float, label: str) -> tuple[dict | None, str | None]:
    status, _, body = _get(url, timeout, _MAX_JSON_BYTES)
    if status != 200:
        return None, f"{label} HTTP {status}"
    if len(body) > _MAX_JSON_BYTES:
        return None, f"{label} response exceeds size limit"
    try:
        data = json.loads(body)
    except ValueError:
        return None, f"{label} is not JSON"
    if not isinstance(data, dict):
        return None, f"{label} is not a JSON object"
    return data, None


def check_backend(origin: str, timeout: float, expect_version: str | None) -> str | None:
    data, error = _json(f"{origin}/api/health", timeout, "backend /api/health")
    if error:
        return error
    if data.get("status") != "ok":
        return f"backend /api/health status={_safe(data.get('status'))}"
    version = data.get("version")
    if expect_version and (
        not isinstance(version, str) or normalize_version(version) != normalize_version(expect_version)
    ):
        return f"backend version {_safe(version)} != expected {_safe(expect_version)}"
    return None


def check_admin_bootstrap(origin: str, timeout: float, min_services: int) -> str | None:
    data, error = _json(f"{origin}/api/bootstrap", timeout, "backend /api/bootstrap")
    if error:
        return error
    settings = data.get("settings")
    if not isinstance(settings, dict):
        return "bootstrap has no settings object"
    if settings.get("needs_login") or settings.get("is_remote_analyst") is not False:
        return "bootstrap is not an admin session (classified as analyst/login)"
    services = data.get("services")
    if not isinstance(services, list):
        return "bootstrap services is not a list"
    if len(services) < min_services:
        return f"bootstrap lists {len(services)} services, expected >= {min_services}"
    return None


def wait_ready(
    name: str,
    frontend: str,
    backend: str,
    *,
    attempts: int,
    interval: float,
    timeout: float,
    bootstrap_timeout: float,
    expect_version: str | None,
    admin: bool,
    min_services: int,
    log: Callable[[str], None] = print,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    checks: list[tuple[str, Callable[[], str | None]]] = [
        ("frontend", lambda: check_frontend(frontend, timeout)),
        ("backend", lambda: check_backend(backend, timeout, expect_version)),
    ]
    if admin:
        checks.append(("admin", lambda: check_admin_bootstrap(backend, bootstrap_timeout, min_services)))
    last: list[str] = []
    for attempt in range(1, attempts + 1):
        failures = []
        for phase, check in checks:
            try:
                error = check()
            except (OSError, ValueError) as exc:
                error = f"{phase} request failed: {type(exc).__name__}"
            if error:
                failures.append(error)
        if not failures:
            log(f"[{name}] ready on attempt {attempt}/{attempts}: " + ", ".join(phase for phase, _ in checks) + " OK")
            return True
        last = failures
        log(f"[{name}] attempt {attempt}/{attempts} not ready: {'; '.join(failures)}")
        if attempt < attempts:
            sleep(interval)
    log(f"[{name}] FAILED readiness after {attempts} attempts: {'; '.join(last)}")
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bounded frontend + backend (+ admin bootstrap) readiness")
    parser.add_argument("--name", required=True)
    parser.add_argument("--frontend", required=True, help="Frontend origin; /dashboard is probed")
    parser.add_argument("--backend", required=True, help="Backend origin; /api/health and /api/bootstrap")
    parser.add_argument("--attempts", type=int, default=40)
    parser.add_argument("--interval", type=float, default=3)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--bootstrap-timeout", type=float, default=20)
    version = parser.add_mutually_exclusive_group()
    version.add_argument("--expect-version")
    version.add_argument("--expect-pyproject-version", action="store_true")
    parser.add_argument("--no-admin", action="store_true", help="Skip the admin bootstrap check")
    parser.add_argument("--min-services", type=int, default=1)
    args = parser.parse_args(argv)
    if (
        args.attempts < 1
        or args.interval < 0
        or args.timeout <= 0
        or args.bootstrap_timeout <= 0
        or args.min_services < 0
    ):
        parser.error(
            "attempts >= 1, interval >= 0, timeout > 0, bootstrap-timeout > 0 and min-services >= 0 are required"
        )
    try:
        validate_admin_tls_config()
        expect = pyproject_version() if args.expect_pyproject_version else args.expect_version
    except (OSError, ValueError, KeyError) as exc:
        print(f"[{args.name}] readiness configuration invalid: {type(exc).__name__}", file=sys.stderr)
        return 2
    ready = wait_ready(
        args.name,
        args.frontend.rstrip("/"),
        args.backend.rstrip("/"),
        attempts=args.attempts,
        interval=args.interval,
        timeout=args.timeout,
        bootstrap_timeout=args.bootstrap_timeout,
        expect_version=expect,
        admin=not args.no_admin,
        min_services=args.min_services,
        log=lambda line: print(line, flush=True),
    )
    return 0 if ready else 1


if __name__ == "__main__":
    sys.exit(main())
