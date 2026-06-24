"""A-4 (testing_suite_audit_2026-06-14.md): testcontainers-python smoke.

Most of the suite is happy with moto's in-process / ThreadedMotoServer
mocks. testcontainers-python is opt-in for the rare integration test
that needs a real-container dep — examples: a real Postgres for an
ORM-shape test, LocalStack for an S3 + IAM combo the in-process moto
doesn't cover, a real Redis for a cache-eviction race test.

This module is a smoke test that the dependency is wired correctly +
documents the pattern for the next test that opts in. Skipped when
Docker isn't on the developer's PATH (most local dev sessions have
Docker; CI runners are configured per workflow).
"""

from __future__ import annotations

import shutil
import socket

import pytest

testcontainers = pytest.importorskip("testcontainers.core.container")


def _docker_available() -> bool:
    """Skip the test if Docker CLI isn't usable.

    Two failure modes: the binary isn't installed, or it's installed
    but the daemon isn't running (Docker Desktop closed on macOS).
    Both warrant a skip rather than a hard failure — testcontainers
    is opt-in and shouldn't gate the suite on local environment shape.
    """
    if shutil.which("docker") is None:
        return False
    # Attempt a connect to the daemon's default Unix socket on macOS /
    # Linux. We don't actually issue a Docker API call here — just
    # check the socket is reachable, since `testcontainers` itself will
    # do a richer probe on first use.
    for path in (
        "/var/run/docker.sock",
        "/Users/" + (__import__("os").environ.get("USER") or "") + "/.docker/run/docker.sock",
    ):
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect(path)
            s.close()
            return True
        except OSError:
            continue
    return False


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="testcontainers requires a running Docker daemon",
)


def test_can_spawn_a_container_and_read_back_its_id():
    """Pin: testcontainers' DockerContainer lifecycle works in this
    repo's venv. The next integration test that needs a real container
    dep can crib this pattern."""
    from testcontainers.core.container import DockerContainer

    with DockerContainer("alpine:3.20").with_command("sleep 60") as c:
        # The wrapped container exposes its id once started.
        cid = c.get_wrapped_container().id
        assert cid, "expected a docker container id once the container started"
