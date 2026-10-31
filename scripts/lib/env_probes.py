"""Multi-environment backend health, tenancy isolation, and RBAC verification probes."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from scripts.lib.environments import (
    ENVIRONMENT_SPECS,
    add_common_args,
    load_dotenv,
    remediation_command,
    service_id,
)

try:
    from scripts.lib.readiness import probe
except ImportError:

    def probe(*args: Any, **kwargs: Any):  # type: ignore[misc]
        from scripts.lib.readiness import probe as _probe

        return _probe(*args, **kwargs)


load_dotenv()


def _env_attr(name: str, default: Any) -> Any:
    env_mod = sys.modules.get("scripts.lib.environments")
    if env_mod is not None and hasattr(env_mod, name):
        return getattr(env_mod, name)
    return default


@dataclass(frozen=True)
class EnvironmentTarget:
    name: str
    architecture: str
    backend_url: str
    frontend_url: str
    fastly_service_id: str
    remediation_cmd: str


ENVIRONMENTS: dict[str, EnvironmentTarget] = {
    name: EnvironmentTarget(
        name=spec.name,
        architecture=spec.architecture,
        backend_url=spec.backend_url,
        frontend_url=spec.frontend_url,
        fastly_service_id=service_id(name),
        remediation_cmd=remediation_command(name),
    )
    for name, spec in ENVIRONMENT_SPECS.items()
}


@dataclass
class HealthResult:
    target: EnvironmentTarget
    backend_ok: bool
    status_code: int | None
    latency_ms: float
    version: str | None
    error: str | None


@dataclass
class TenancyResult:
    target: EnvironmentTarget
    resolved_service_id: str | None
    service_match: bool
    rejection_ok: bool
    rejection_code: int | None
    error: str | None


@dataclass
class RbacResult:
    target: EnvironmentTarget
    auth_config_ok: bool
    unauth_analyst_blocked: bool
    unauth_analyst_code: int | None
    error: str | None


def check_backend_health(target: EnvironmentTarget, timeout_s: float = 3.0) -> HealthResult:
    probe_fn = _env_attr("probe", probe)
    result = probe_fn(
        f"{target.backend_url}/api/health",
        timeout=timeout_s,
        headers={
            "User-Agent": "FLA-HealthCheck/1.0",
            "Accept": "application/json",
            "Connection": "close",
        },
    )
    if result.http_error:
        return HealthResult(
            target=target,
            backend_ok=False,
            status_code=result.status,
            latency_ms=result.latency_ms,
            version=None,
            error=f"HTTPError {result.status}: {result.reason}",
        )
    if result.failed:
        return HealthResult(target, False, None, result.latency_ms, None, result.error)
    try:
        data = json.loads(result.body.decode("utf-8"))
        version = data.get("version")
        is_ok = result.status == 200 and data.get("status") == "ok"
    except (ValueError, AttributeError) as exc:
        return HealthResult(target, False, None, result.latency_ms, None, str(exc))
    error_msg = None if is_ok else f"Unexpected payload: {result.body.decode('utf-8')[:100]}"
    return HealthResult(
        target=target,
        backend_ok=is_ok,
        status_code=result.status,
        latency_ms=result.latency_ms,
        version=version,
        error=error_msg,
    )


def check_tenancy(target: EnvironmentTarget, timeout_s: float = 5.0) -> TenancyResult:
    probe_fn = _env_attr("probe", probe)
    bootstrap = probe_fn(
        f"{target.backend_url}/api/bootstrap",
        timeout=timeout_s,
        headers={
            "User-Agent": "FLA-TenancyCheck/1.0",
            "Accept": "application/json",
            "Connection": "close",
        },
    )
    resolved_id: str | None = None
    fetch_error = bootstrap.error
    if not bootstrap.failed:
        try:
            resolved_id = json.loads(bootstrap.body.decode("utf-8")).get("active_service_id")
        except (ValueError, AttributeError) as exc:
            fetch_error = str(exc)
    if fetch_error is not None:
        return TenancyResult(
            target=target,
            resolved_service_id=None,
            service_match=False,
            rejection_ok=False,
            rejection_code=None,
            error=f"Failed to fetch bootstrap: {fetch_error}",
        )

    service_match = resolved_id == target.fastly_service_id

    rejection = probe_fn(
        f"{target.backend_url}/api/dashboard/bundle?service_id=nonexistent_tenant_xyz",
        timeout=timeout_s,
        data=json.dumps({"time_range": "24h"}).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "FLA-TenancyCheck/1.0",
            "Connection": "close",
        },
    )
    if rejection.status is None:
        return TenancyResult(
            target=target,
            resolved_service_id=resolved_id,
            service_match=service_match,
            rejection_ok=False,
            rejection_code=None,
            error=f"Unexpected error during tenancy isolation probe: {rejection.error}",
        )
    rejection_code = rejection.status
    rejection_ok = rejection.http_error and rejection_code in (400, 401, 403, 404)

    err = None
    if not service_match:
        err = f"Service mismatch: got '{resolved_id}', expected '{target.fastly_service_id}'"
    elif not rejection_ok:
        err = f"Tenancy breach: fake tenant returned HTTP {rejection_code} (expected 400/401/403/404)"

    return TenancyResult(
        target=target,
        resolved_service_id=resolved_id,
        service_match=service_match,
        rejection_ok=rejection_ok,
        rejection_code=rejection_code,
        error=err,
    )


def check_rbac(target: EnvironmentTarget, timeout_s: float = 5.0) -> RbacResult:
    probe_fn = _env_attr("probe", probe)
    analyst_headers = {
        "User-Agent": "FLA-RbacCheck/1.0",
        "X-Remote-Analyst": "1",
        "Connection": "close",
    }
    result = probe_fn(
        f"{target.backend_url}/api/share/auth-config",
        timeout=timeout_s,
        headers=analyst_headers,
    )
    auth_config_ok = False
    error = result.error
    if not result.failed:
        try:
            body_json = json.loads(result.body.decode("utf-8"))
            auth_config_ok = result.status == 200 and "passcode_enabled" in body_json
        except ValueError as exc:
            error = str(exc)
    if error is not None:
        return RbacResult(
            target=target,
            auth_config_ok=False,
            unauth_analyst_blocked=False,
            unauth_analyst_code=None,
            error=f"Failed to query /api/share/auth-config: {error}",
        )

    protected = probe_fn(
        f"{target.backend_url}/api/services",
        timeout=timeout_s,
        headers=analyst_headers,
    )
    if protected.status is None:
        return RbacResult(
            target=target,
            auth_config_ok=auth_config_ok,
            unauth_analyst_blocked=False,
            unauth_analyst_code=None,
            error=f"Failed to query protected endpoint /api/services: {protected.error}",
        )
    unauth_code = protected.status
    unauth_blocked = unauth_code in (401, 403)
    err_msg: str | None = None
    if not auth_config_ok:
        err_msg = "Auth config endpoint failed or returned unexpected payload"
    elif not unauth_blocked:
        err_msg = (
            f"RBAC breach: unauthenticated analyst on /api/services returned HTTP {unauth_code} (expected 401/403)"
        )
    return RbacResult(
        target=target,
        auth_config_ok=auth_config_ok,
        unauth_analyst_blocked=unauth_blocked,
        unauth_analyst_code=unauth_code,
        error=err_msg,
    )


def _row_has_failure(h: HealthResult, tn: TenancyResult, rb: RbacResult) -> bool:
    return (
        not h.backend_ok
        or not tn.service_match
        or not tn.rejection_ok
        or not rb.unauth_analyst_blocked
        or not rb.auth_config_ok
    )


def health_main(argv: Sequence[str] | None = None) -> int:
    active_environments: dict[str, EnvironmentTarget] = _env_attr("ENVIRONMENTS", ENVIRONMENTS)
    parser = argparse.ArgumentParser(
        description="Verify backend /api/health, tenancy, and RBAC across FLA environments."
    )
    parser.add_argument(
        "--env",
        choices=[*active_environments.keys(), "all"],
        default="all",
        help="Environment to verify (default: all)",
    )
    add_common_args(parser, timeout_default=5.0, json_help="Output raw JSON results")
    parser.add_argument(
        "--audit",
        action="store_true",
        help="Run comprehensive pipeline and resource audit (scripts/dev/audit_environments.py)",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="When --audit is set, run in real-time continuous watch mode",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=5.0,
        help="Duration in minutes for continuous audit watch (default: 5.0)",
    )
    args = parser.parse_args(argv)

    if args.env == "all":
        targets = list(active_environments.values())
    else:
        targets = [active_environments[args.env]]

    check_health_fn = _env_attr("check_backend_health", check_backend_health)
    check_tenancy_fn = _env_attr("check_tenancy", check_tenancy)
    check_rbac_fn = _env_attr("check_rbac", check_rbac)

    health_results: list[HealthResult] = []
    tenancy_results: list[TenancyResult] = []
    rbac_results: list[RbacResult] = []
    for target in targets:
        health_results.append(check_health_fn(target, timeout_s=args.timeout))
        tenancy_results.append(check_tenancy_fn(target, timeout_s=args.timeout))
        rbac_results.append(check_rbac_fn(target, timeout_s=args.timeout))

    combined_rows = list(zip(health_results, tenancy_results, rbac_results))
    failed_count = sum(1 for h, tn, rb in combined_rows if _row_has_failure(h, tn, rb))

    if args.json:
        output_payload = []
        for h, tn, rb in combined_rows:
            needs_remediation = (
                not h.backend_ok or not tn.service_match or not tn.rejection_ok or not rb.unauth_analyst_blocked
            )
            output_payload.append(
                {
                    "env": h.target.name,
                    "architecture": h.target.architecture,
                    "service_id": h.target.fastly_service_id,
                    "backend_url": h.target.backend_url,
                    "health_ok": h.backend_ok,
                    "health_status_code": h.status_code,
                    "latency_ms": round(h.latency_ms, 2),
                    "version": h.version,
                    "tenancy_service_match": tn.service_match,
                    "tenancy_rejection_ok": tn.rejection_ok,
                    "tenancy_rejection_code": tn.rejection_code,
                    "rbac_auth_config_ok": rb.auth_config_ok,
                    "rbac_unauth_blocked": rb.unauth_analyst_blocked,
                    "rbac_unauth_code": rb.unauth_analyst_code,
                    "error": h.error or tn.error or rb.error,
                    "remediation": h.target.remediation_cmd if needs_remediation else None,
                }
            )
        print(json.dumps(output_payload, indent=2))
        return 1 if failed_count > 0 else 0

    print("\n" + "=" * 96)
    print("FLA Multi-Environment Health, Tenancy & RBAC Verification")
    print("=" * 96)
    print(f"{'ENVIRONMENT':<22} | {'HEALTH':<8} | {'SERVICE ID':<22} | {'ISOLATION':<11} | {'RBAC':<10} | {'LATENCY'}")
    print("-" * 96)

    for h, tn, rb in combined_rows:
        h_str = "PASS (200)" if h.backend_ok else "FAIL"
        svc_suffix = "OK" if tn.service_match else "FAIL"
        svc_str = f"{tn.resolved_service_id} ({svc_suffix})"
        iso_str = f"PASS ({tn.rejection_code})" if tn.rejection_ok else "FAIL"
        rbac_ok = rb.unauth_analyst_blocked and rb.auth_config_ok
        rbac_str = f"PASS ({rb.unauth_analyst_code})" if rbac_ok else "FAIL"
        lat_str = f"{h.latency_ms:.1f}ms"

        print(f"{h.target.name:<22} | {h_str:<8} | {svc_str:<22} | {iso_str:<11} | {rbac_str:<10} | {lat_str}")
        if _row_has_failure(h, tn, rb):
            if h.error:
                print(f"   [!] Health Error: {h.error}")
            if tn.error:
                print(f"   [!] Tenancy Error: {tn.error}")
            if rb.error:
                print(f"   [!] RBAC Error: {rb.error}")
            print(f"   [!] Fix command: {h.target.remediation_cmd}\n")

    print("=" * 96)
    if failed_count == 0:
        print("ALL TARGET ENVIRONMENTS HEALTHY, TENANCY VERIFIED & RBAC ENFORCED!")
        print("=" * 96 + "\n")
        if args.audit:
            audit_cmd = [sys.executable, "scripts/dev/audit_environments.py"]
            if args.watch:
                audit_cmd.extend(["--watch", "--duration", str(args.duration)])
            return subprocess.run(audit_cmd).returncode
    else:
        print(f"FAILURE: {failed_count}/{len(targets)} environments failed checks.")
        print("=" * 96 + "\n")
        return 1

    return 0


__all__ = [
    "ENVIRONMENTS",
    "EnvironmentTarget",
    "HealthResult",
    "RbacResult",
    "TenancyResult",
    "check_backend_health",
    "check_rbac",
    "check_tenancy",
    "health_main",
    "probe",
]
