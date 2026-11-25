"""Campaign D2 security hardening regression tests.

Covers:
- K8S-01: ClickHouse auth.accessManagement defaults to false and ClickHouse/Keeper
  NetworkPolicies are rendered by default.
- K8S-03: Frontend container enforces readOnlyRootFilesystem: true with writable /tmp
  and /app/.next/cache emptyDir mounts.
- K8S-04: FLA Helm chart defaults config.debugResponses to false.
- ROUTER-03: Remote /api/web-vitals and /api/ux-events require an authenticated
  analyst session in both RemoteAccessMiddleware and web_vitals router.
- DOCKER-02: backend/Dockerfile pins falco tarball SHA-256 and uv OCI digest;
  caddy/Dockerfile pins caddy-ratelimit module version.
"""

from __future__ import annotations

import re
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.routers import web_vitals as web_vitals_mod
from backend.utils import remote_access, tunnel

pytestmark = pytest.mark.no_postgres

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _helm_render(chart_rel_path: str, *extra_args: str) -> list[dict[str, Any]]:
    helm_bin = shutil.which("helm") or "/opt/homebrew/bin/helm"
    cmd = [helm_bin, "template", "d2-test", str(_REPO_ROOT / chart_rel_path), *extra_args]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return [doc for doc in yaml.safe_load_all(res.stdout) if isinstance(doc, dict)]


# ── K8S-01 ───────────────────────────────────────────────────────────────────


def test_k8s01_clickhouse_defaults_no_access_management_and_renders_network_policies() -> None:
    docs = _helm_render(
        "deploy/chart/clickhouse",
        "--set",
        "auth.existingSecret=d2-ch-secret",
        "--set",
        "keeper.enabled=true",
    )

    ch_sts = next(d for d in docs if d.get("kind") == "StatefulSet" and d["metadata"]["name"].endswith("-shard-0"))
    ch_env = {e["name"]: e.get("value") for e in ch_sts["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert ch_env["CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"] == "0"

    netpols = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "NetworkPolicy"}
    ch_np = next(np for name, np in netpols.items() if name.endswith("-clickhouse"))
    keeper_np = next(np for name, np in netpols.items() if name.endswith("-keeper"))

    assert ch_np["spec"]["policyTypes"] == ["Ingress"]
    ch_ports = {p["port"] for rule in ch_np["spec"]["ingress"] for p in rule["ports"]}
    assert ch_ports == {8123, 9000}

    assert set(keeper_np["spec"]["policyTypes"]) == {"Ingress", "Egress"}
    keeper_in_ports = {p["port"] for rule in keeper_np["spec"]["ingress"] for p in rule["ports"]}
    assert keeper_in_ports == {9181, 9444}
    keeper_egress_ports = {p["port"] for rule in keeper_np["spec"]["egress"] for p in rule["ports"]}
    assert {53, 9181, 9444}.issubset(keeper_egress_ports)


# ── K8S-03 & K8S-04 ──────────────────────────────────────────────────────────


def test_k8s03_k8s04_frontend_readonly_rootfs_and_debug_responses_false_by_default() -> None:
    docs = _helm_render("deploy/chart/fastly-log-analytics", "--set", "secrets.existingSecret=d2-secret")

    fe_dep = next(d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-frontend"))
    fe_container = fe_dep["spec"]["template"]["spec"]["containers"][0]
    assert fe_container["securityContext"]["readOnlyRootFilesystem"] is True
    assert fe_container["securityContext"]["allowPrivilegeEscalation"] is False
    fe_mounts = {m["mountPath"] for m in fe_container["volumeMounts"]}
    assert {"/app/.next/cache", "/tmp"}.issubset(fe_mounts)

    be_dep = next(d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-backend"))
    be_env = {e["name"]: e.get("value") for e in be_dep["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert "DEBUG_RESPONSES" not in be_env

    # Opt-in still works when explicitly requested.
    debug_docs = _helm_render(
        "deploy/chart/fastly-log-analytics",
        "--set",
        "secrets.existingSecret=d2-secret",
        "--set",
        "config.debugResponses=true",
    )
    be_debug = next(
        d for d in debug_docs if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-backend")
    )
    be_debug_env = {e["name"]: e.get("value") for e in be_debug["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert be_debug_env.get("DEBUG_RESPONSES") == "true"


# ── ROUTER-03 ────────────────────────────────────────────────────────────────


def test_router03_remote_telemetry_requires_authentication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert "/api/web-vitals" not in remote_access._UNAUTH_ANALYST_PATHS
    assert "/api/ux-events" not in remote_access._UNAUTH_ANALYST_PATHS

    con = sqlite3.connect(str(tmp_path / "share_d2.db"), check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    for ddl in share_db._SCHEMA:
        con.execute(ddl)
    con.commit()
    monkeypatch.setattr(share_db, "get_global_share_con", lambda: con)
    share_db.publish_tos_version("v1", "Test ToS", con=con)
    tunnel.reset_for_tests()
    web_vitals_mod._reset_web_vitals_limiter_for_tests()

    try:
        mgr = tunnel.get_tunnel_manager()
        mgr.start_sharing(public_endpoint="https://share.example.com")
        invite = share_db.create_remote_invite(
            name="D2 Analyst",
            email="d2@example.com",
            passcode="cedar-river-stone-99",
            expires_at_utc=None,
            ip_whitelist=None,
            service_ids=["svc-d2"],
        )
        share_db.mark_tos_accepted(invite["id"], "v1")
        inv_row = share_db.get_remote_invite(invite["id"])
        assert inv_row is not None
        ua = "Mozilla/5.0 Chrome/125.0"
        sess = mgr.create_session(invite=inv_row, ip_address="198.51.100.88", user_agent=ua, headers={"user-agent": ua})

        app = FastAPI()
        app.add_middleware(remote_access.RemoteAccessMiddleware)
        app.include_router(web_vitals_mod.router)

        wv_payload = {"name": "LCP", "value": 100.0, "rating": "good", "id": "d2-1", "pathname": "/", "delta": 1.0}
        ux_payload = {"event": "click", "pathname": "/", "details": {}}

        with TestClient(app, client=("198.51.100.88", 50000), base_url="https://share.example.com") as rem:
            unauth_headers = {"X-Proxied-By-Caddy": "true", "Origin": "https://share.example.com"}
            assert rem.post("/api/web-vitals", json=wv_payload, headers=unauth_headers).status_code == 401
            assert rem.post("/api/ux-events", json=ux_payload, headers=unauth_headers).status_code == 401

            auth_headers = {
                **unauth_headers,
                "User-Agent": ua,
                "Cookie": f"analyst_session_id={sess.session_id}",
            }
            assert rem.post("/api/web-vitals", json=wv_payload, headers=auth_headers).status_code == 200
            assert rem.post("/api/ux-events", json=ux_payload, headers=auth_headers).status_code == 200

        # Direct router call (bypassing middleware) also enforces 401 for unauthenticated remote callers.
        direct_app = FastAPI()
        direct_app.include_router(web_vitals_mod.router)
        with TestClient(direct_app, client=("198.51.100.88", 50000), base_url="https://share.example.com") as direct:
            unauth_headers = {"X-Proxied-By-Caddy": "true", "Origin": "https://share.example.com"}
            assert direct.post("/api/web-vitals", json=wv_payload, headers=unauth_headers).status_code == 401
            assert direct.post("/api/ux-events", json=ux_payload, headers=unauth_headers).status_code == 401
    finally:
        tunnel.reset_for_tests()
        con.close()


# ── DOCKER-02 ────────────────────────────────────────────────────────────────


def test_docker02_dockerfiles_pin_falco_uv_and_caddy_ratelimit() -> None:
    backend_df = (_REPO_ROOT / "backend" / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"ARG\s+FALCO_SHA256=[0-9a-f]{64}\b", backend_df)
    assert "sha256sum -c -" in backend_df
    assert re.search(r"COPY\s+--from=ghcr\.io/astral-sh/uv:0\.6\.14@sha256:[0-9a-f]{64}\b", backend_df)

    caddy_df = (_REPO_ROOT / "caddy" / "Dockerfile").read_text(encoding="utf-8")
    assert "github.com/mholt/caddy-ratelimit@v0.1.0" in caddy_df
