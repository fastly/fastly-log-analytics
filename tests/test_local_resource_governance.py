"""Local-stack DuckDB parallelism governance.

Local Standard runs on the shared Colima host (6 vCPU). With ``DUCKDB_THREADS`` unset
the backend defaults to ``min(cpu_count, 8) = 6`` threads per DuckDB op and the pool defaults
to 8 connections, so in-process ingest/rollup/compaction/insights-prewarmer
crons can fan out to ~48 CPU-bound DuckDB threads. This can drive host load
to ~100 on 6 cores and the single-threaded asyncio serving loop is starved so
hard that even ``/api/health`` times out (measured: HTTP 000 at 20s+), which
fails the dashboard-render verify.

The ``.env`` file already expresses the intent (``DUCKDB_THREADS=4``) but
compose never injects it — the backend services use an explicit
``environment:`` list with no ``env_file``, so the value never reaches
the container. These tests pin the caps directly on the compose
services so a future edit can't silently drop them and reintroduce the
serving-loop starvation. Production (docker-compose.prod.yml) already
caps the pool to 4; this brings the local stack in line.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


class _ComposeLoader(yaml.SafeLoader):
    """SafeLoader that tolerates compose merge tags (``!override``/``!reset``)."""


def _passthrough_tag(loader, node):
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_scalar(node)


for _tag in ("!override", "!reset"):
    _ComposeLoader.add_constructor(_tag, _passthrough_tag)


def _load_compose(compose_rel: str) -> dict:
    return yaml.load((REPO_ROOT / compose_rel).read_text(encoding="utf-8"), Loader=_ComposeLoader)


def _env_int(environment: list[str], key: str) -> int | None:
    prefix = f"{key}="
    for entry in environment:
        if entry.startswith(prefix):
            return int(entry[len(prefix) :])
    return None


def _service_env(compose_rel: str, service: str) -> list[str]:
    compose = yaml.safe_load((REPO_ROOT / compose_rel).read_text(encoding="utf-8"))
    return compose["services"][service]["environment"]


def _duration_seconds(value: str) -> int:
    value = value.strip()
    if value.endswith("ms"):
        return int(float(value[:-2]) / 1000)
    if value.endswith("m"):
        return int(float(value[:-1]) * 60)
    if value.endswith("h"):
        return int(float(value[:-1]) * 3600)
    if value.endswith("s"):
        return int(float(value[:-1]))
    return int(float(value))


def _service_healthcheck(compose_rel: str, service: str) -> dict:
    compose = yaml.safe_load((REPO_ROOT / compose_rel).read_text(encoding="utf-8"))
    return compose["services"][service]["healthcheck"]


def test_local_standard_backend_bounds_duckdb_parallelism():
    env = _service_env("docker-compose.yml", "backend")
    threads = _env_int(env, "DUCKDB_THREADS")
    pool = _env_int(env, "DUCKDB_POOL_MAX_SIZE")
    assert threads is not None and 1 <= threads <= 4, (
        "docker-compose.yml backend must pin a bounded DUCKDB_THREADS (<=4) "
        "so cron DuckDB ops don't monopolize all cores and starve serving"
    )
    assert pool is not None and 1 <= pool <= 4, (
        "docker-compose.yml backend must pin a bounded DUCKDB_POOL_MAX_SIZE (<=4)"
    )


def test_multipod_backend_bounds_duckdb_parallelism():
    env = _service_env("docker-compose.multipod.yml", "backend")
    threads = _env_int(env, "DUCKDB_THREADS")
    pool = _env_int(env, "DUCKDB_POOL_MAX_SIZE")
    assert threads is not None and 1 <= threads <= 4, (
        "docker-compose.multipod.yml backend must pin a bounded DUCKDB_THREADS (<=4)"
    )
    assert pool is not None and 1 <= pool <= 4, (
        "docker-compose.multipod.yml backend must pin a bounded DUCKDB_POOL_MAX_SIZE (<=4)"
    )


def test_multipod_worker_bounds_duckdb_parallelism():
    env = _service_env("docker-compose.multipod.yml", "worker")
    threads = _env_int(env, "DUCKDB_THREADS")
    assert threads is not None and 1 <= threads <= 2, (
        "docker-compose.multipod.yml worker must pin a bounded DUCKDB_THREADS "
        "(<=2); 6 celery procs x 6 default threads = 36 CPU-bound threads "
        "that starve the backend serving loop on the shared host"
    )


def test_local_standard_backend_healthcheck_covers_cold_start():
    """Base compose backend start_period must exceed the real cold-start time.

    Importing ``backend.main`` pulls in the full iceberg/ducklake stack
    (pyiceberg -> pyarrow -> pandas, s3fs/fsspec, duckdb); measured cold
    start to "Application startup complete" is ~107s on an IDLE Colima host
    and stretches well past that under the deploy's double-stack + build
    load. The original 45s start_period was never realistic: compose marks
    the still-importing backend ``unhealthy`` (start_period + retries x
    interval) and aborts ``docker compose up`` because frontend/caddy
    depend on ``service_healthy`` -- which under ``set -e`` kills the whole
    deploy_test_all.sh run before verification. prod (docker-compose.prod.yml)
    already uses a 25m start_period for the same reason; the base compose
    must likewise cover the cold start with margin.
    """
    hc = _service_healthcheck("docker-compose.yml", "backend")
    start_period = _duration_seconds(hc["start_period"])
    assert start_period >= 180, (
        "docker-compose.yml backend healthcheck start_period must be >= 180s "
        "to cover the measured ~107s cold start plus load margin, so compose "
        f"doesn't falsely mark it unhealthy mid-startup (got {hc['start_period']})"
    )


# Long-lived Python processes that run as container PID 1. uvicorn/celery are
# not init systems: an orphaned grandchild (scheduler watchdog subprocess, a
# crashed cold-start helper, an ad-hoc py-spy/pip during debugging) becomes an
# unreapable zombie under them. Docker then can't stop the container ("PID is
# zombie and can not be killed. Use the --init option ..."), so the next
# `docker compose up --force-recreate` fails non-zero and, under the deploy's
# `set -e`, aborts the entire run. `init: true` inserts tini as PID 1 to
# forward signals and reap zombies — Docker's own prescribed fix.
_INIT_REQUIRED_SERVICES = {
    "docker-compose.yml": ["backend"],
    "docker-compose.multipod.yml": ["backend", "worker"],
}


@pytest.mark.parametrize(
    ("compose_rel", "service"),
    [(compose_rel, service) for compose_rel, services in _INIT_REQUIRED_SERVICES.items() for service in services],
)
def test_long_lived_backend_services_run_an_init(compose_rel: str, service: str):
    compose = _load_compose(compose_rel)
    svc = compose["services"][service]
    assert svc.get("init") is True, (
        f"{compose_rel} service '{service}' runs a long-lived Python process as "
        "PID 1 and must set `init: true` so tini reaps orphaned zombies; "
        "without it a wedged child makes `docker stop`/force-recreate fail and "
        "aborts deploy_test_all.sh under set -e"
    )


def test_local_public_https_is_opt_in_and_published_by_namespace_owner():
    base = _load_compose("docker-compose.yml")["services"]
    overlay = _load_compose("docker-compose.public-https.yml")["services"]
    assert base["backend"]["ports"] == ["80:80"]
    assert base["caddy"]["network_mode"] == "service:backend"
    assert overlay["backend"]["ports"] == ["${LOCAL_PUBLIC_LISTEN_IP:-127.0.0.1}:${LOCAL_PUBLIC_PORT:-443}:443"]
    assert "ports" not in overlay["caddy"]
    assert "admin-gateway" in overlay["caddy"]["depends_on"]
    assert "./caddy/local-public-https.caddy:/etc/caddy/public-https/listener.caddy:ro" in overlay["caddy"]["volumes"]
    assert any(volume.endswith(":/certs/public:ro") for volume in overlay["caddy"]["volumes"])
    assert (
        "ADMIN_GATEWAY_REQUIRED=${ADMIN_GATEWAY_REQUIRED:?set 1 for enforced admin mTLS}"
        in overlay["caddy"]["environment"]
    )


def test_local_public_https_compose_merge_preserves_admin_and_http():
    if not shutil.which("docker"):
        pytest.skip("Docker CLI unavailable for static Compose configuration")
    env = {
        **os.environ,
        "ADMIN_GATEWAY_REQUIRED": "1",
        "ADMIN_GATEWAY_SECRET": "test-compose-only-" + "x" * 32,
        "ADMIN_GATEWAY_HOST": "admin.example.test",
        "ADMIN_GATEWAY_CERT_DIR": "/tmp/example-admin-certs",
        "ADMIN_GATEWAY_LISTEN_IP": "127.0.0.1",
        "ADMIN_GATEWAY_PORT": "9443",
        "LOCAL_HOSTS": "localhost,admin.example.test",
        "LOCAL_PUBLIC_HOST": "127.0.0.1",
        "LOCAL_PUBLIC_CERT_DIR": "/tmp/example-public-certs",
        "LOCAL_PUBLIC_LISTEN_IP": "127.0.0.1",
        "LOCAL_PUBLIC_PORT": "443",
    }
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            os.devnull,
            "-f",
            "docker-compose.yml",
            "-f",
            "docker-compose.admin-mtls.yml",
            "-f",
            "docker-compose.public-https.yml",
            "config",
            "--format",
            "json",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    services = json.loads(result.stdout)["services"]
    ports = services["backend"]["ports"]
    assert {(p.get("host_ip"), p["published"], p["target"]) for p in ports} == {
        (None, "80", 80),
        ("127.0.0.1", "443", 443),
        ("127.0.0.1", "9443", 8443),
    }
    for name in ("caddy", "frontend", "admin-gateway"):
        assert services[name]["network_mode"] == "service:backend"
        assert not services[name].get("ports")
    assert services["backend"]["environment"]["ADMIN_GATEWAY_REQUIRED"] == "1"
    assert services["frontend"]["environment"]["ADMIN_GATEWAY_REQUIRED"] == "1"
    mounts = {volume["target"]: volume for volume in services["caddy"]["volumes"]}
    assert mounts["/etc/caddy/Caddyfile"]["source"].endswith("/caddy/Caddyfile.local")
    assert mounts["/etc/caddy/public-https/listener.caddy"]["source"].endswith("/caddy/local-public-https.caddy")
    assert mounts["/certs/public"]["read_only"] is True
    admin_mounts = {volume["target"]: volume for volume in services["admin-gateway"]["volumes"]}
    assert admin_mounts["/certs"]["source"] == env["ADMIN_GATEWAY_CERT_DIR"]


@pytest.mark.parametrize("required", ["1", "0"])
def test_local_public_https_adapts_separately_from_http_admin(tmp_path, required):
    if not shutil.which("caddy"):
        pytest.skip("Caddy binary unavailable for static configuration adaptation")
    local = (REPO_ROOT / "caddy/Caddyfile.local").read_text()
    snippet = REPO_ROOT / "caddy/local-public-https.caddy"
    # The real container uses an optional glob mount. Expand only that import
    # for static adaptation; no server, certificate issuance, or listener starts.
    config = tmp_path / "Caddyfile"
    config.write_text(local.replace("/etc/caddy/public-https/*.caddy", str(snippet)))
    result = subprocess.run(
        ["caddy", "adapt", "--config", str(config), "--adapter", "caddyfile"],
        env={**os.environ, "LOCAL_PUBLIC_HOST": "127.0.0.1", "ADMIN_GATEWAY_REQUIRED": required},
        capture_output=True,
        text=True,
        check=True,
    )
    adapted = json.loads(result.stdout)
    servers = adapted["apps"]["http"]["servers"]
    (public,) = [server for server in servers.values() if server["listen"] == [":443"]]
    (http,) = [server for server in servers.values() if server["listen"] == [":80"]]
    # IP-origin TLS clients send no SNI. Every handshake must select the
    # supplied certificate, not fall through an empty catch-all TLS policy.
    (policy,) = public["tls_connection_policies"]
    assert "match" not in policy
    assert policy["certificate_selection"]["any_tag"]
    assert '"host": ["127.0.0.1"]' in json.dumps(public)
    public_json = json.dumps(public)
    http_json = json.dumps(http)
    assert "127.0.0.1:8000" in public_json and "127.0.0.1:3000" in public_json
    assert '"X-Proxied-By-Caddy": ["true"]' in public_json
    assert '"X-Forwarded-For": ["{http.request.remote.host}"]' in public_json
    assert '"delete": ["X-Admin-Gateway-Token"]' in public_json
    assert '"delete": ["X-Admin-Token"]' in public_json
    assert "X-Remote-Analyst" not in public_json
    assert '"status_code": 503' in public_json
    assert f'\\"{required}\\" != \\"1\\"' in public_json
    assert "client_authentication" not in public_json
    assert '"X-Forwarded-For": ["127.0.0.1"]' in http_json
    assert "X-Proxied-By-Caddy" not in http_json
    (certificate,) = adapted["apps"]["tls"]["certificates"]["load_files"]
    assert certificate["certificate"] == "/certs/public/server.crt"
    assert certificate["key"] == "/certs/public/server.key"
