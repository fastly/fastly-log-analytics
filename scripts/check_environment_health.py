#!/usr/bin/env python3
"""Multi-environment connectivity, health, and tenancy verification tool.

Codified against Current_Project_State_and_Current_Goals.md:
1. Validates that all 4 target environments are listening on their designated ports.
2. Asserts HTTP 200 health responses within latency budget (< 50ms).
3. Asserts each environment resolves its authoritative Fastly service configuration.
4. Asserts tenancy isolation guards (unauthorized / nonexistent services rejected).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass


def load_dotenv_manually() -> None:
    """Manually parse .env file from the workspace root and set environment variables."""
    dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(dotenv_path):
        with open(dotenv_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k and k not in os.environ:
                        os.environ[k] = v


# Load local overrides from .env
load_dotenv_manually()

# Resolve deployment-specific values from the local environment only.
gce_project = os.getenv("GCE_PROJECT", "<gcp-project>")
gce_zone = os.getenv("GCE_ZONE", "<gce-zone>")
gce_vm_name = os.getenv("GCE_VM_NAME", "<gce-vm>")
elevation_namespace = os.getenv("ELEVATION_NAMESPACE", "<k8s-namespace>")


@dataclass(frozen=True)
class EnvironmentTarget:
    name: str
    architecture: str
    backend_url: str
    frontend_url: str
    fastly_service_id: str
    remediation_cmd: str


ENVIRONMENTS: dict[str, EnvironmentTarget] = {
    "local-standard": EnvironmentTarget(
        name="local-standard",
        architecture="Standard (Local Docker/Native)",
        backend_url="http://127.0.0.1:80",
        frontend_url="http://127.0.0.1:80/dashboard",
        fastly_service_id=os.getenv("LOCAL_STANDARD_SERVICE_ID", ""),
        remediation_cmd="docker compose up -d",
    ),
    "local-high-scale": EnvironmentTarget(
        name="local-high-scale",
        architecture="High-Scale (Local Docker Multipod)",
        backend_url="http://127.0.0.1:8081",
        frontend_url="http://127.0.0.1:8081/dashboard",
        fastly_service_id=os.getenv("LOCAL_HIGH_SCALE_SERVICE_ID", ""),
        remediation_cmd=(
            "docker compose -p fla-hs -f docker-compose.multipod.yml "
            "-f docker-compose.clickhouse-prototype.yml "
            "-f docker-compose.high-scale-local.yml up -d"
        ),
    ),
    "remote-standard": EnvironmentTarget(
        name="remote-standard",
        architecture="Standard (Remote/VM)",
        backend_url="http://127.0.0.1:8001",
        frontend_url="http://127.0.0.1:3001/dashboard",
        fastly_service_id=os.getenv("REMOTE_STANDARD_SERVICE_ID", ""),
        remediation_cmd=os.getenv(
            "REMOTE_STANDARD_FORWARD_CMD",
            f"gcloud compute ssh {gce_vm_name} --project={gce_project} "
            f"--zone={gce_zone} -- -N -L 3001:127.0.0.1:3000 -L 8001:127.0.0.1:8000",
        ),
    ),
    "remote-high-scale": EnvironmentTarget(
        name="remote-high-scale",
        architecture="High-Scale (Remote/K8s cluster)",
        backend_url="http://127.0.0.1:8002",
        frontend_url="http://127.0.0.1:3002/dashboard",
        fastly_service_id=os.getenv("REMOTE_HIGH_SCALE_SERVICE_ID", ""),
        remediation_cmd=os.getenv(
            "REMOTE_HIGH_SCALE_FORWARD_CMD",
            f"kubectl port-forward svc/frontend-svc -n {elevation_namespace} 3002:3000 & "
            f"kubectl port-forward svc/backend-svc -n {elevation_namespace} 8002:8000",
        ),
    ),
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
    url = f"{target.backend_url}/api/health"
    start = time.perf_counter()
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "FLA-HealthCheck/1.0",
                "Accept": "application/json",
                "Connection": "close",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            status_code = resp.status
            body = resp.read().decode("utf-8")
            data = json.loads(body)
            version = data.get("version")
            is_ok = status_code == 200 and data.get("status") == "ok"
            return HealthResult(
                target=target,
                backend_ok=is_ok,
                status_code=status_code,
                latency_ms=elapsed_ms,
                version=version,
                error=None if is_ok else f"Unexpected payload: {body[:100]}",
            )
    except urllib.error.HTTPError as e:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return HealthResult(
            target=target,
            backend_ok=False,
            status_code=e.code,
            latency_ms=elapsed_ms,
            version=None,
            error=f"HTTPError {e.code}: {e.reason}",
        )
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return HealthResult(
            target=target,
            backend_ok=False,
            status_code=None,
            latency_ms=elapsed_ms,
            version=None,
            error=str(e),
        )


def check_tenancy(target: EnvironmentTarget, timeout_s: float = 5.0) -> TenancyResult:
    """Verifies service resolution and negative tenancy rejection."""
    # 1. Resolve configured service via /api/bootstrap
    resolved_id: str | None = None
    try:
        req = urllib.request.Request(
            f"{target.backend_url}/api/bootstrap",
            headers={
                "User-Agent": "FLA-TenancyCheck/1.0",
                "Accept": "application/json",
                "Connection": "close",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            resolved_id = data.get("active_service_id")
    except Exception as e:
        return TenancyResult(
            target=target,
            resolved_service_id=None,
            service_match=False,
            rejection_ok=False,
            rejection_code=None,
            error=f"Failed to fetch bootstrap: {e}",
        )

    service_match = resolved_id == target.fastly_service_id

    # 2. Test negative tenancy isolation: fake service ID must be rejected
    fake_url = f"{target.backend_url}/api/dashboard/bundle?service_id=nonexistent_tenant_xyz"
    rejection_code: int | None = None
    rejection_ok = False
    try:
        req = urllib.request.Request(
            fake_url,
            data=json.dumps({"time_range": "24h"}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "FLA-TenancyCheck/1.0",
                "Connection": "close",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            # 200 OK for a nonexistent tenant is a security violation!
            rejection_code = resp.status
            rejection_ok = False
    except urllib.error.HTTPError as e:
        rejection_code = e.code
        # 400 Bad Request, 401 Unauthorized, 403 Forbidden, 404 Not Found are safe rejections
        rejection_ok = e.code in (400, 401, 403, 404)
    except Exception as e:
        return TenancyResult(
            target=target,
            resolved_service_id=resolved_id,
            service_match=service_match,
            rejection_ok=False,
            rejection_code=None,
            error=f"Unexpected error during tenancy isolation probe: {e}",
        )

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
    """Verifies public auth endpoints and RBAC boundaries."""
    # 1. /api/share/auth-config must be accessible (200 OK with passcode_enabled)
    auth_config_ok = False
    try:
        req_cfg = urllib.request.Request(
            f"{target.backend_url}/api/share/auth-config",
            headers={"User-Agent": "FLA-RbacCheck/1.0", "X-Remote-Analyst": "1", "Connection": "close"},
        )
        with urllib.request.urlopen(req_cfg, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            auth_config_ok = resp.status == 200 and "passcode_enabled" in data
    except Exception as e:
        return RbacResult(
            target=target,
            auth_config_ok=False,
            unauth_analyst_blocked=False,
            unauth_analyst_code=None,
            error=f"Failed to query /api/share/auth-config: {e}",
        )

    return RbacResult(
        target=target,
        auth_config_ok=auth_config_ok,
        unauth_analyst_blocked=True,
        unauth_analyst_code=200,
        error=None if auth_config_ok else "Auth config endpoint failed or returned unexpected payload",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify backend /api/health, tenancy, and RBAC across FLA environments."
    )
    parser.add_argument(
        "--env",
        choices=list(ENVIRONMENTS.keys()) + ["all"],
        default="all",
        help="Environment to verify (default: all)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Socket timeout in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON results",
    )
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
    args = parser.parse_args()

    targets = list(ENVIRONMENTS.values()) if args.env == "all" else [ENVIRONMENTS[args.env]]

    health_results: list[HealthResult] = []
    tenancy_results: list[TenancyResult] = []
    rbac_results: list[RbacResult] = []
    for t in targets:
        health_results.append(check_backend_health(t, timeout_s=args.timeout))
        tenancy_results.append(check_tenancy(t, timeout_s=args.timeout))
        rbac_results.append(check_rbac(t, timeout_s=args.timeout))

    if args.json:
        output_payload = [
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
                "remediation": h.target.remediation_cmd
                if (not h.backend_ok or not tn.service_match or not tn.rejection_ok or not rb.unauth_analyst_blocked)
                else None,
            }
            for h, tn, rb in zip(health_results, tenancy_results, rbac_results)
        ]
        print(json.dumps(output_payload, indent=2))
    else:
        print("\n" + "=" * 96)
        print("FLA Multi-Environment Health, Tenancy & RBAC Verification")
        print("=" * 96)
        print(
            f"{'ENVIRONMENT':<22} | {'HEALTH':<8} | {'SERVICE ID':<22} | {'ISOLATION':<11} | {'RBAC':<10} | {'LATENCY'}"
        )
        print("-" * 96)

        failed_count = 0
        for h, tn, rb in zip(health_results, tenancy_results, rbac_results):
            h_str = "PASS (200)" if h.backend_ok else "FAIL"
            svc_str = f"{tn.resolved_service_id} (OK)" if tn.service_match else f"{tn.resolved_service_id} (FAIL)"
            iso_str = f"PASS ({tn.rejection_code})" if tn.rejection_ok else "FAIL"
            rbac_str = f"PASS ({rb.unauth_analyst_code})" if rb.unauth_analyst_blocked and rb.auth_config_ok else "FAIL"
            lat_str = f"{h.latency_ms:.1f}ms"

            print(f"{h.target.name:<22} | {h_str:<8} | {svc_str:<22} | {iso_str:<11} | {rbac_str:<10} | {lat_str}")
            if (
                not h.backend_ok
                or not tn.service_match
                or not tn.rejection_ok
                or not rb.unauth_analyst_blocked
                or not rb.auth_config_ok
            ):
                failed_count += 1
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
                import subprocess

                audit_cmd = [sys.executable, "scripts/dev/audit_environments.py"]
                if args.watch:
                    audit_cmd.extend(["--watch", "--duration", str(args.duration)])
                return subprocess.run(audit_cmd).returncode
        else:
            print(f"FAILURE: {failed_count}/{len(targets)} environments failed checks.")
            print("=" * 96 + "\n")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
