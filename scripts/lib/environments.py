"""Single source of truth for the three deployment environments.

Both ``scripts/check_environment_health.py`` (connectivity / tenancy / RBAC)
and ``scripts/dev/audit_environments.py`` (pipeline + resource audit) used to
redefine the same three environments, their forwarded ports/URLs, and a manual
``.env`` parser. They now import this registry so the topology is described in
exactly one place.

No real infrastructure values live here: service IDs come from the local
environment (``.env`` / shell), and GCE/K8s coordinates are read from env vars
with safe placeholders so the tracked tree stays clean for the public repo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def load_dotenv(path: str | Path | None = None) -> None:
    """Load the repo-root ``.env`` into ``os.environ`` without clobbering it.

    Uses python-dotenv (already a project dependency). Existing environment
    variables win, matching the previous hand-rolled ``k not in os.environ``
    behaviour so an explicit shell export always takes precedence.
    """
    from dotenv import load_dotenv as _load_dotenv

    dotenv_path = Path(path) if path is not None else ROOT / ".env"
    _load_dotenv(dotenv_path=str(dotenv_path), override=False)


@dataclass(frozen=True)
class EnvironmentSpec:
    """Static topology for one deployment environment.

    Dynamic, deployment-private values (service IDs, GCE/K8s coordinates) are
    resolved from the environment via :func:`service_id` / :func:`remediation_command`.
    """

    name: str
    architecture: str
    backend_url: str
    frontend_url: str
    service_id_env: str
    is_high_scale: bool


ENVIRONMENT_SPECS: dict[str, EnvironmentSpec] = {
    "local-standard": EnvironmentSpec(
        name="local-standard",
        architecture="Standard (Local Docker/Native)",
        backend_url="http://127.0.0.1:80",
        frontend_url="http://127.0.0.1:80/dashboard",
        service_id_env="LOCAL_STANDARD_SERVICE_ID",
        is_high_scale=False,
    ),
    "remote-standard": EnvironmentSpec(
        name="remote-standard",
        architecture="Standard (Remote/VM)",
        backend_url="http://127.0.0.1:8001",
        frontend_url="http://127.0.0.1:3001/dashboard",
        service_id_env="REMOTE_STANDARD_SERVICE_ID",
        is_high_scale=False,
    ),
    "remote-high-scale": EnvironmentSpec(
        name="remote-high-scale",
        architecture="High-Scale (Remote/K8s)",
        backend_url="http://127.0.0.1:8002",
        frontend_url="http://127.0.0.1:3002/dashboard",
        service_id_env="REMOTE_HIGH_SCALE_SERVICE_ID",
        is_high_scale=True,
    ),
}


def service_id(name: str) -> str:
    """Resolve an environment's Fastly service ID from the local environment."""
    return os.getenv(ENVIRONMENT_SPECS[name].service_id_env, "")


def remediation_command(name: str) -> str:
    """Human-runnable command to bring an environment's port-forward/stack up.

    Fully overridable per environment so operator-specific infra strings never
    need to be committed.
    """
    if name == "local-standard":
        return "docker compose up -d"

    gce_project = os.getenv("GCE_PROJECT", "<gcp-project>")
    gce_zone = os.getenv("GCE_ZONE", "<gce-zone>")
    gce_vm_name = os.getenv("GCE_VM_NAME", "<gce-vm>")
    elevation_namespace = os.getenv("ELEVATION_NAMESPACE", "<k8s-namespace>")

    if name == "remote-standard":
        return os.getenv(
            "REMOTE_STANDARD_FORWARD_CMD",
            f"gcloud compute ssh {gce_vm_name} --project={gce_project} "
            f"--zone={gce_zone} -- -N -L 3001:127.0.0.1:3000 -L 8001:127.0.0.1:8000",
        )
    if name == "remote-high-scale":
        return os.getenv(
            "REMOTE_HIGH_SCALE_FORWARD_CMD",
            f"kubectl port-forward svc/frontend-svc -n {elevation_namespace} 3002:3000 & "
            f"kubectl port-forward svc/backend-svc -n {elevation_namespace} 8002:8000",
        )
    raise KeyError(name)
