#!/usr/bin/env python3
"""Explicit companion gateway rollout, preserving existing application ownership.

No credentials in command arguments, Helm values, output or logs. The dedicated
CA signing keys never leave the operator machine. --dry-run validates and
inspects current objects without modifying the cluster.
"""

import argparse
import base64
import ipaddress
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

CHART = Path(__file__).resolve().parents[1] / "deploy/chart/admin-gateway"
DNS_NAME = re.compile(r"(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


def run(*command: str, payload: dict | None = None) -> str:
    result = subprocess.run(
        command,
        input=json.dumps(payload) if payload is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        # kubectl errors can echo request bodies containing Secret data.
        raise ValueError(f"{command[0]} {command[1]} failed (exit {result.returncode}); inspect cluster events")
    return result.stdout


def application_patch(deployment: dict, container_name: str, secret: str, host: str, backend: bool) -> dict:
    containers = deployment["spec"]["template"]["spec"]["containers"]
    container = next((item for item in containers if item["name"] == container_name), None)
    if container is None:
        raise ValueError(f"Container {container_name} does not exist in deployment")
    env = [
        {"name": "ADMIN_GATEWAY_REQUIRED", "value": "1"},
        {
            "name": "ADMIN_GATEWAY_SECRET",
            "valueFrom": {"secretKeyRef": {"name": secret, "key": "ADMIN_GATEWAY_SECRET"}},
        },
    ]
    if backend:
        hosts = next((item for item in container.get("env", []) if item["name"] == "LOCAL_HOSTS"), {})
        if "valueFrom" in hosts:
            raise ValueError("LOCAL_HOSTS uses valueFrom; update its source explicitly before enabling gateway")
        existing = [part.strip() for part in hosts.get("value", "").split(",") if part.strip()]
        if host not in existing:
            existing.append(host)
        env += [
            {"name": "LOCAL_HOSTS", "value": ",".join(existing)},
        ]
    # Delete the old env entries first so strategic merge doesn't retain
    # `value` alongside `valueFrom` after credential configuration changes.
    current = [item for item in container.get("env", []) if item["name"] not in {item["name"] for item in env}]
    return {
        "spec": {
            "template": {
                "spec": {"containers": [{"name": container_name, "env": [{"$patch": "replace"}, *current, *env]}]}
            }
        }
    }


def public_routes_patch(virtualserver: dict) -> dict:
    routes = virtualserver["spec"]["routes"]
    patched = []
    for route in routes:
        route = json.loads(json.dumps(route))
        action = route.get("action", {})
        if "pass" in action:
            action = {"proxy": {"upstream": action["pass"]}}
        if "proxy" not in action or set(action) != {"proxy"}:
            raise ValueError(
                "Public VirtualServer requires direct pass/proxy routes; unsupported route must be reviewed explicitly"
            )
        proxy = action["proxy"]
        headers = proxy.setdefault("requestHeaders", {})
        overrides = {"X-Admin-Gateway-Token": "", "X-Admin-Token": "", "X-Proxied-By-Caddy": "true"}
        keep = [
            entry
            for entry in headers.get("set", [])
            if entry["name"].lower() not in {name.lower() for name in overrides}
        ]
        headers["set"] = [*keep, *({"name": name, "value": value} for name, value in overrides.items())]
        # Empty proxy_set_header values suppress those headers in NGINX.
        # Preserve the analyst marker and all unrelated route settings.
        route["action"] = action
        patched.append(route)
    return {"spec": {"routes": patched}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--release", default="admin-gateway")
    parser.add_argument("--secret", default="admin-gateway")
    parser.add_argument("--phase", choices=["provision", "gateway", "activate"], default="gateway")
    parser.add_argument("--internal-gke", action="store_true", help="Use observed GKE internal-only LB annotations")
    parser.add_argument("--load-balancer-ip", default="", help="Operator-reserved private IPv4 address (optional)")
    parser.add_argument("--host")
    parser.add_argument("--cert-dir", type=Path)
    parser.add_argument("--backend-upstream")
    parser.add_argument("--frontend-upstream")
    parser.add_argument("--backend-deployment")
    parser.add_argument("--frontend-deployment")
    parser.add_argument("--backend-container", default="backend")
    parser.add_argument("--frontend-container", default="frontend")
    parser.add_argument("--public-virtualserver", help="Existing F5 NGINX public VirtualServer to sanitize")
    parser.add_argument(
        "--callers-ready", action="store_true", help="Confirm canonical callers already use direct client mTLS"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    for tool in ("openssl", "helm", "kubectl"):
        if not shutil.which(tool):
            parser.error(f"Missing required tool: {tool}; install it explicitly")
    if args.phase == "provision" and not args.internal_gke:
        parser.error("--phase provision requires --internal-gke (never allocate an external LB)")
    if args.phase != "provision":
        required = (
            "host",
            "cert_dir",
            "backend_upstream",
            "frontend_upstream",
            "backend_deployment",
            "frontend_deployment",
            "public_virtualserver",
        )
        for name in required:
            if not getattr(args, name):
                parser.error(f"--{name.replace('_', '-')} is required for {args.phase}")
        if not DNS_NAME.fullmatch(args.host):
            parser.error("--host must be one explicit lowercase DNS hostname or IPv4 address")
    if args.phase == "activate" and not args.callers_ready:
        parser.error("--phase activate requires --callers-ready after transport/browser readiness verification")
    try:
        exposure_args = ()
        if args.internal_gke:
            exposure_args = (
                "--set",
                "service.type=LoadBalancer",
                "--set-string",
                "service.internalProvider=gke-internal",
            )
        if args.load_balancer_ip:
            address = ipaddress.ip_address(args.load_balancer_ip)
            if address.version != 4 or not address.is_private:
                raise ValueError("--load-balancer-ip must be a private IPv4 address")
            exposure_args += ("--set-string", f"service.loadBalancerIP={address}")
        if args.phase == "provision":
            provision_args = ("--namespace", args.namespace, "--set", "gateway.enabled=false", *exposure_args)
            run("helm", "template", args.release, str(CHART), *provision_args)
            if args.dry_run:
                print("Validated internal-only Service allocation render; no gateway/app/Secret changes")
                return 0
            # Reservation only: the Service selects no gateway pods yet.
            # Never downgrade an existing active release to provision mode.
            releases = json.loads(run("helm", "list", "-n", args.namespace, "-o", "json"))
            if any(release["name"] == args.release for release in releases):
                raise ValueError("provision requires a new companion release; existing release must not be downgraded")
            run("helm", "install", args.release, str(CHART), *provision_args)
            deadline = time.monotonic() + 300
            while time.monotonic() < deadline:
                service = json.loads(run("kubectl", "get", "service", args.release, "-n", args.namespace, "-o", "json"))
                endpoints = service.get("status", {}).get("loadBalancer", {}).get("ingress", [])
                if endpoints:
                    ip = ipaddress.ip_address(endpoints[0]["ip"])
                    if ip.version != 4 or not ip.is_private:
                        raise ValueError("Allocated endpoint is not private; stop and review provider configuration")
                    print(f"Reserved internal gateway IP: {ip}; issue its IP SAN before --phase gateway")
                    return 0
                time.sleep(3)
            raise ValueError("Internal LoadBalancer allocation timed out; no app mode was changed")
        contents = {}
        for name in ("server.crt", "server.key", "client-ca.crt", "ADMIN_GATEWAY_SECRET"):
            path = (args.cert_dir / name).resolve(strict=True)
            if path.stat().st_mode & 0o077:
                raise ValueError(f"{name} must be owner-only (chmod 600)")
            contents[name] = path.read_bytes()
        credential = contents["ADMIN_GATEWAY_SECRET"].decode().strip()
        if len(credential) < 32 or not re.fullmatch(r"[A-Za-z0-9_-]+", credential):
            raise ValueError("ADMIN_GATEWAY_SECRET must contain at least 32 URL-safe characters")
        for cert in ("server.crt", "client-ca.crt"):
            run("openssl", "x509", "-in", str(args.cert_dir / cert), "-checkend", "0", "-noout")
        try:
            ipaddress.ip_address(args.host)
            identity_flag = "-checkip"
        except ValueError:
            identity_flag = "-checkhost"
        run("openssl", "x509", "-in", str(args.cert_dir / "server.crt"), identity_flag, args.host, "-noout")
        if run("openssl", "x509", "-in", str(args.cert_dir / "server.crt"), "-pubkey", "-noout") != run(
            "openssl", "pkey", "-in", str(args.cert_dir / "server.key"), "-passin", "pass:", "-pubout"
        ):
            raise ValueError("Server certificate and private key do not match")
        namespace = ("-n", args.namespace)
        patches = []
        for deployment, container, backend in (
            (args.backend_deployment, args.backend_container, True),
            (args.frontend_deployment, args.frontend_container, False),
        ):
            current = json.loads(run("kubectl", "get", "deployment", deployment, *namespace, "-o", "json"))
            patches.append((deployment, application_patch(current, container, args.secret, args.host, backend)))
        vs = json.loads(run("kubectl", "get", "virtualserver", args.public_virtualserver, *namespace, "-o", "json"))
        vs_patch = public_routes_patch(vs)
        helm_args = (
            "--namespace",
            args.namespace,
            "--set-string",
            f"host={args.host}",
            "--set-string",
            f"existingSecret={args.secret}",
            "--set-string",
            f"backendUpstream={args.backend_upstream}",
            "--set-string",
            f"frontendUpstream={args.frontend_upstream}",
            *exposure_args,
        )
        run("helm", "template", args.release, str(CHART), *helm_args)
        if args.dry_run:
            print(f"Validated {args.phase} bundle, companion render, app patches and public sanitizer; no changes made")
            return 0
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {"name": args.secret, "namespace": args.namespace},
            "data": {name: base64.b64encode(value).decode() for name, value in contents.items()},
        }
        # Server-side apply avoids kubectl's last-applied annotation duplicating
        # secret material. Feed stdin; neither shell argv nor logs contain it.
        run("kubectl", "apply", "--server-side", "--field-manager=admin-gateway", "-f", "-", payload=secret)
        # Sanitize public ingress before installing the credential in the app.
        run(
            "kubectl",
            "patch",
            "virtualserver",
            args.public_virtualserver,
            *namespace,
            "--type=merge",
            "--patch-file=/dev/stdin",
            payload=vs_patch,
        )
        run("helm", "upgrade", "--install", args.release, str(CHART), *helm_args, "--wait", "--timeout", "300s")
        if args.phase == "gateway":
            run("kubectl", "rollout", "restart", f"deployment/{args.release}", *namespace)
            run("kubectl", "rollout", "status", f"deployment/{args.release}", *namespace, "--timeout=300s")
            print(
                "Gateway deployed without app activation; verify direct TLS and migrate canonical callers before --phase activate"
            )
            return 0
        for deployment, patch in patches:
            run(
                "kubectl",
                "patch",
                "deployment",
                deployment,
                *namespace,
                "--type=strategic",
                "--patch-file=/dev/stdin",
                payload=patch,
            )
            # A Secret value can change without changing the Pod template's
            # secretKeyRef. Force an app restart so rotated credentials reach
            # backend/frontend environments even when the patch is identical.
            run("kubectl", "rollout", "restart", f"deployment/{deployment}", *namespace)
        for deployment, _ in patches:
            run("kubectl", "rollout", "status", f"deployment/{deployment}", *namespace, "--timeout=300s")
        # Secret file/env changes require a new Caddy process; existing TLS
        # sessions must also be dropped when replacing a client trust root.
        run("kubectl", "rollout", "restart", f"deployment/{args.release}", *namespace)
        run("kubectl", "rollout", "status", f"deployment/{args.release}", *namespace, "--timeout=300s")
        print(
            "Gateway rolled out; verify missing/invalid/valid client TLS and analyst ingress before removing legacy access"
        )
    except (OSError, ValueError, KeyError, StopIteration) as exc:
        print(f"admin-gateway: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
