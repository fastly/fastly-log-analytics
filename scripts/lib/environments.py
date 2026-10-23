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
import stat
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

PERSISTED_DEPLOY_ENV_KEYS: frozenset[str] = frozenset(
    {
        "LOCAL_STANDARD_ADMIN_MTLS",
        "GCE_ADMIN_MTLS",
        "ELEVATION_ADMIN_MTLS",
        "ADMIN_GATEWAY_REQUIRED",
        "ADMIN_GATEWAY_CALLERS_READY",
        "ADMIN_GATEWAY_ENDPOINTS",
        "ADMIN_GATEWAY_HOST",
        "ADMIN_GATEWAY_CERT_DIR",
        "ADMIN_GATEWAY_SECRET",
        "LOCAL_HOSTS",
        "ELEVATION_ADMIN_GATEWAY_HOST",
        "ELEVATION_ADMIN_GATEWAY_CERT_DIR",
        "ELEVATION_ADMIN_GATEWAY_IMAGE",
        "GCE_ADMIN_BUNDLE_DIR",
        "ADMIN_GATEWAY_UID",
        "ADMIN_GATEWAY_GID",
        "NODE_EXTRA_CA_CERTS",
    }
)


def load_dotenv(path: str | Path | None = None) -> None:
    """Load the repo-root ``.env`` into ``os.environ`` without clobbering it.

    Uses python-dotenv (already a project dependency). Existing environment
    variables win, matching the previous hand-rolled ``k not in os.environ``
    behaviour so an explicit shell export always takes precedence.
    """
    if path is None and ("PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules):
        return

    from dotenv import load_dotenv as _load_dotenv

    dotenv_path = Path(path) if path is not None else ROOT / ".env"
    _load_dotenv(dotenv_path=str(dotenv_path), override=False)


def load_persisted_deploy_env(path: str | Path | None = None) -> dict[str, str]:
    """Load persisted admin-mTLS settings from ``FLA_ADMIN_DEPLOY_ENV`` without overwriting ``os.environ``.

    Matches the validation rules in ``scripts/dev/deploy_test_all.sh``:
    regular owner-only file (mode 0600), no interpolation, allow-listed keys only.
    """
    if (
        path is None
        and "FLA_ADMIN_DEPLOY_ENV" not in os.environ
        and ("PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules)
    ):
        return {}

    from dotenv import dotenv_values

    raw_path = (
        Path(path)
        if path is not None
        else Path(
            os.getenv(
                "FLA_ADMIN_DEPLOY_ENV",
                str(Path.home() / ".config/fastly-log-analytics/admin/deploy.env"),
            )
        )
    )
    if not raw_path.exists() and not raw_path.is_symlink():
        return {}
    st = os.lstat(raw_path)
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or (st.st_mode & 0o077):
        raise ValueError(f"{raw_path}: must be a regular file owned by you with mode 600")
    values = dotenv_values(raw_path, interpolate=False)
    oneshot = {"GCE_ADMIN_BOOTSTRAP", "GCE_ADMIN_FIREWALL_APPLY"} & values.keys()
    if oneshot:
        raise ValueError(f"{raw_path}: one-shot keys must not be persisted: {', '.join(sorted(oneshot))}")
    unknown = sorted(set(values) - PERSISTED_DEPLOY_ENV_KEYS)
    if unknown:
        raise ValueError(f"{raw_path}: unsupported keys: {', '.join(unknown)}")
    loaded: dict[str, str] = {}
    for key, value in values.items():
        if value is None or "\0" in value:
            raise ValueError(f"{raw_path}: {key} needs a value")
        if key in ("ADMIN_GATEWAY_UID", "ADMIN_GATEWAY_GID") and not value.isdigit():
            raise ValueError(f"{raw_path}: {key} must be a numeric id")
        if key == "NODE_EXTRA_CA_CERTS" and not (os.path.isabs(value) and ":" not in value and os.path.isfile(value)):
            raise ValueError(f"{raw_path}: {key} must be one absolute existing PEM file")
        loaded[key] = value
        if key not in os.environ:
            os.environ[key] = value
    return loaded


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
            f"python3 -m scripts.lib.port_forward --namespace {elevation_namespace} "
            "svc/frontend-svc:3002:3000 svc/backend-svc:8002:8000",
        )
    raise KeyError(name)


def parse_iso_utc(ts_str: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp string into a timezone-aware UTC ``datetime``."""
    if not ts_str or not isinstance(ts_str, str):
        return None
    try:
        clean_ts = ts_str.strip().replace("Z", "+00:00")
        if " " in clean_ts and "T" not in clean_ts:
            parts = clean_ts.split(" ")
            if len(parts) >= 2:
                suffix = "" if ("+" in parts[1] or parts[1].endswith("Z")) else "+00:00"
                clean_ts = f"{parts[0]}T{parts[1]}{suffix}"
        dt = datetime.fromisoformat(clean_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def percentile(values: list[float], p: float) -> float:
    """Return the linear-interpolated ``p``-th percentile (``p`` in ``0..100`` or ``0..1``)."""
    if not values:
        return 0.0
    s = sorted(values)
    frac = p / 100.0 if p > 1.0 else p
    frac = max(0.0, min(1.0, frac))
    k = (len(s) - 1) * frac
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (k - f) * (s[c] - s[f])
