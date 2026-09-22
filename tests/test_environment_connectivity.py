"""Integration test verifying all 4 environments are listening, healthy, and isolated.

Enforces that every deployment topology in Current_Project_State_and_Current_Goals.md:
1. Is operational and reachable via its assigned port forward.
2. Returns HTTP 200 on /api/health with version 3.0.0-beta3.
3. Resolves its exact designated Fastly service ID.
4. Strictly enforces multi-tenant isolation (rejecting unauthorized service IDs).
"""

from __future__ import annotations

import os

import pytest

from scripts.check_environment_health import (
    ENVIRONMENTS,
    check_backend_health,
    check_rbac,
    check_tenancy,
)

pytestmark = [
    pytest.mark.skipif(
        bool(os.getenv("CI") or os.getenv("GITHUB_ACTIONS")),
        reason="Environment connectivity tests require local/remote port-forwards not present in CI",
    ),
    pytest.mark.skipif(
        any(not target.fastly_service_id for target in ENVIRONMENTS.values()),
        reason="Environment connectivity tests require all four service ID environment variables",
    ),
]


@pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
def test_environment_backend_health(env_name: str) -> None:
    target = ENVIRONMENTS[env_name]
    result = check_backend_health(target, timeout_s=3.0)

    assert result.backend_ok, (
        f"Environment '{env_name}' failed health check: {result.error}\n"
        f"Backend URL: {target.backend_url}\n"
        f"Remediation: {target.remediation_cmd}"
    )
    assert result.status_code == 200
    assert result.version == "3.0.0-beta3"


@pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
def test_environment_service_and_tenancy(env_name: str) -> None:
    target = ENVIRONMENTS[env_name]
    result = check_tenancy(target, timeout_s=5.0)

    assert result.service_match, (
        f"Environment '{env_name}' service mismatch: {result.error}\n"
        f"Expected: {target.fastly_service_id}, Got: {result.resolved_service_id}\n"
        f"Remediation: {target.remediation_cmd}"
    )
    assert result.rejection_ok, (
        f"Environment '{env_name}' tenancy breach: {result.error}\nRemediation: {target.remediation_cmd}"
    )


@pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
def test_environment_rbac(env_name: str) -> None:
    target = ENVIRONMENTS[env_name]
    result = check_rbac(target, timeout_s=5.0)

    assert result.auth_config_ok, (
        f"Environment '{env_name}' auth config check failed: {result.error}\nRemediation: {target.remediation_cmd}"
    )
    assert result.unauth_analyst_blocked, (
        f"Environment '{env_name}' unauth analyst blocking failed: {result.error}\n"
        f"Remediation: {target.remediation_cmd}"
    )
