"""Single source of truth for deployment environments, re-exporting admin mTLS, port-forward, credential, and health helpers."""

from __future__ import annotations

import argparse
import json
import os
import ssl
import stat
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener, urlopen

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

ROOT = Path(__file__).resolve().parents[2]

PERSISTED_DEPLOY_ENV_KEYS: frozenset[str] = frozenset(
    {
        "LOCAL_STANDARD_ADMIN_MTLS",
        "GCE_ADMIN_MTLS",
        "REMOTE_HS_ADMIN_MTLS",
        "ADMIN_GATEWAY_REQUIRED",
        "ADMIN_GATEWAY_CALLERS_READY",
        "ADMIN_GATEWAY_ENDPOINTS",
        "ADMIN_GATEWAY_HOST",
        "ADMIN_GATEWAY_CERT_DIR",
        "ADMIN_GATEWAY_SECRET",
        "LOCAL_HOSTS",
        "REMOTE_HS_ADMIN_GATEWAY_HOST",
        "REMOTE_HS_ADMIN_GATEWAY_CERT_DIR",
        "REMOTE_HS_ADMIN_GATEWAY_IMAGE",
        "GCE_ADMIN_BUNDLE_DIR",
        "ADMIN_GATEWAY_UID",
        "ADMIN_GATEWAY_GID",
        "NODE_EXTRA_CA_CERTS",
    }
)


def load_dotenv(path: str | Path | None = None) -> None:
    """Load the repo-root ``.env`` into ``os.environ`` without clobbering it."""
    if path is None and ("PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules):
        return

    from dotenv import load_dotenv as _load_dotenv

    dotenv_path = Path(path) if path is not None else ROOT / ".env"
    _load_dotenv(dotenv_path=str(dotenv_path), override=False)


def load_persisted_deploy_env(path: str | Path | None = None) -> dict[str, str]:
    """Load persisted admin-mTLS settings from ``FLA_ADMIN_DEPLOY_ENV`` without overwriting ``os.environ``."""
    is_pytest_run = "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules
    if path is None and "FLA_ADMIN_DEPLOY_ENV" not in os.environ and is_pytest_run:
        return {}

    from dotenv import dotenv_values

    if path is not None:
        raw_path = Path(path)
    else:
        default_env_path = str(Path.home() / ".config/fastly-log-analytics/admin/deploy.env")
        raw_path = Path(os.getenv("FLA_ADMIN_DEPLOY_ENV", default_env_path))

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
        if key == "NODE_EXTRA_CA_CERTS":
            is_valid_pem_path = os.path.isabs(value) and ":" not in value and os.path.isfile(value)
            if not is_valid_pem_path:
                raise ValueError(f"{raw_path}: {key} must be one absolute existing PEM file")
        loaded[key] = value
        if key not in os.environ:
            os.environ[key] = value
    return loaded


@dataclass(frozen=True)
class EnvironmentSpec:
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
    return os.getenv(ENVIRONMENT_SPECS[name].service_id_env, "")


def selected_environment_names() -> list[str]:
    """Canonical verification scope; rollout reuse must not deselect an environment."""
    selected = []
    if os.getenv("IGNORE_LOCAL_STD", "0") != "1":
        selected.append("local-standard")
    for name, verify, skip in (
        ("remote-standard", "VERIFY_REMOTE_STD", "SKIP_REMOTE_STD"),
        ("remote-high-scale", "VERIFY_REMOTE_HS", "SKIP_REMOTE_HS"),
    ):
        if os.getenv(verify, "0" if os.getenv(skip) == "1" else "1") == "1":
            selected.append(name)
    return selected


def remediation_command(name: str) -> str:
    if name == "local-standard":
        return "docker compose up -d"
    gce_project = os.getenv("GCE_PROJECT", "<gcp-project>")
    gce_zone = os.getenv("GCE_ZONE", "<gce-zone>")
    gce_vm_name = os.getenv("GCE_VM_NAME", "<gce-vm>")
    k8s_namespace = os.getenv("K8S_NAMESPACE", "<k8s-namespace>")
    if name == "remote-standard":
        default_cmd = (
            f"gcloud compute ssh {gce_vm_name} --project={gce_project} "
            f"--zone={gce_zone} -- -N -L 3001:127.0.0.1:3000 -L 8001:127.0.0.1:8000"
        )
        return os.getenv("REMOTE_STANDARD_FORWARD_CMD", default_cmd)
    if name == "remote-high-scale":
        default_cmd = (
            f"python3 -m scripts.lib.port_forward --namespace {k8s_namespace} "
            "svc/frontend-svc:3002:3000 svc/backend-svc:8002:8000"
        )
        return os.getenv("REMOTE_HIGH_SCALE_FORWARD_CMD", default_cmd)
    raise KeyError(name)


def parse_iso_utc(ts_str: str | None) -> datetime | None:
    if not ts_str or not isinstance(ts_str, str):
        return None
    try:
        clean_ts = ts_str.strip().replace("Z", "+00:00")
        if " " in clean_ts and "T" not in clean_ts:
            parts = clean_ts.split(" ")
            if len(parts) >= 2:
                has_tz_suffix = "+" in parts[1] or parts[1].endswith("Z")
                suffix = "" if has_tz_suffix else "+00:00"
                clean_ts = f"{parts[0]}T{parts[1]}{suffix}"
        dt = datetime.fromisoformat(clean_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    normalized_p = p / 100.0 if p > 1.0 else p
    frac = max(0.0, min(1.0, normalized_p))
    k = (len(sorted_vals) - 1) * frac
    lower_idx = int(k)
    upper_idx = min(lower_idx + 1, len(sorted_vals) - 1)
    return sorted_vals[lower_idx] + (k - lower_idx) * (sorted_vals[upper_idx] - sorted_vals[lower_idx])


def add_common_args(
    parser: argparse.ArgumentParser,
    *,
    timeout_default: float | None = None,
    timeout_label: str = "Socket",
    json_help: str | None = None,
) -> None:
    if timeout_default is not None:
        parser.add_argument(
            "--timeout",
            type=float,
            default=timeout_default,
            help=f"{timeout_label} timeout in seconds (default: {timeout_default})",
        )
    if json_help is not None:
        parser.add_argument("--json", action="store_true", help=json_help)


def histogram_quantile(quantile: float, bucket_metric: str, window: str = "5m") -> str:
    return f"histogram_quantile({quantile},sum by (le)(rate({bucket_metric}[{window}])))"


sys.modules.setdefault("scripts.lib.environments", sys.modules[__name__])

from scripts.lib.admin_mtls import (  # noqa: E402
    ALIASES,
    _context,
    _origin,
    _SameOriginRedirect,
    _settings,
    admin_origin,
    admin_request_context,
    admin_tls_config,
    admin_tls_main,
    admin_urlopen,
    async_admin_json,
    is_admin_url,
    validate_admin_tls_config,
)

_env_probes_mod = sys.modules.get("scripts.lib.env_probes")
if _env_probes_mod is None or hasattr(_env_probes_mod, "ENVIRONMENTS"):
    from scripts.lib.env_probes import (  # noqa: E402
        ENVIRONMENTS,
        EnvironmentTarget,
        HealthResult,
        RbacResult,
        TenancyResult,
        check_backend_health,
        check_rbac,
        check_tenancy,
        health_main,
        probe,
    )
from scripts.lib.k8s_gce import (  # noqa: E402
    _POLL_INTERVAL,
    _TAIL_BYTES,
    _TERM_GRACE,
    CHECK_SCRIPT,
    DEFAULT_MAX_RESTARTS,
    DEFAULT_READY_TIMEOUT,
    Forward,
    ForwardExited,
    ForwardSet,
    PortForwardError,
    PortInUse,
    ReadinessTimeout,
    _accepts,
    _kill_group,
    _load_deploy_env,
    _raise_terminated,
    _run_command,
    _Running,
    _Terminated,
    build_targets,
    compute_all,
    parse_forward,
    port_forward_main,
    port_in_use,
    run_check,
    validate_credentials_main,
)


def instant(base_url: str, query: str, timeout: float = 10) -> float | None:
    request = Request(f"{base_url.rstrip('/')}/api/v1/query?query={quote(query)}", method="GET")
    try:
        with admin_urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read())
        return float(payload["data"]["result"][0]["value"][1])
    except (OSError, URLError, TimeoutError, json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError):
        return None


def _is_port_forward_arg(arg: str) -> bool:
    pf_flags = ("--kubectl", "--ready-timeout", "--max-restarts", "--namespace", "-n", "--context")
    pf_prefixes = ("--kubectl=", "--ready-timeout=", "--max-restarts=", "--namespace=", "--context=")
    if arg in pf_flags or arg.startswith(pf_prefixes):
        return True
    return ":" in arg and "/" in arg and not arg.startswith(("http://", "https://"))


def main(argv: Sequence[str] | None = None) -> int:
    eff_argv = list(sys.argv[1:] if argv is None else argv)
    prog = Path(sys.argv[0]).name if sys.argv else ""
    spec_obj = globals().get("__spec__") if globals().get("__name__") == "__main__" else None
    spec_name = spec_obj.name if spec_obj else ""

    is_admin_tls_call = (
        spec_name == "scripts.lib.admin_tls"
        or "admin_tls" in prog
        or (bool(eff_argv) and (eff_argv[0].startswith(("http://", "https://")) or "--discard" in eff_argv))
    )
    if is_admin_tls_call:
        return admin_tls_main(argv)

    is_port_forward_call = (
        spec_name == "scripts.lib.port_forward"
        or "port_forward" in prog
        or any(_is_port_forward_arg(a) for a in eff_argv)
    )
    if is_port_forward_call:
        return port_forward_main(argv)

    is_validate_creds_call = (
        spec_name in ("scripts.dev.validate_credentials", "validate_credentials")
        or "validate_credentials" in prog
        or "--validate-credentials" in eff_argv
    )
    if is_validate_creds_call:
        return validate_credentials_main(argv)

    return health_main(argv)


__all__ = [
    "ALIASES",
    "CHECK_SCRIPT",
    "DEFAULT_MAX_RESTARTS",
    "DEFAULT_READY_TIMEOUT",
    "ENVIRONMENTS",
    "ENVIRONMENT_SPECS",
    "EnvironmentSpec",
    "EnvironmentTarget",
    "Forward",
    "ForwardExited",
    "ForwardSet",
    "HTTPRedirectHandler",
    "HTTPSHandler",
    "HealthResult",
    "PERSISTED_DEPLOY_ENV_KEYS",
    "PortForwardError",
    "PortInUse",
    "ROOT",
    "RbacResult",
    "ReadinessTimeout",
    "Request",
    "TenancyResult",
    "_POLL_INTERVAL",
    "_Running",
    "_SameOriginRedirect",
    "_TAIL_BYTES",
    "_TERM_GRACE",
    "_Terminated",
    "_accepts",
    "_context",
    "_kill_group",
    "_load_deploy_env",
    "_origin",
    "_raise_terminated",
    "_run_command",
    "_settings",
    "add_common_args",
    "admin_origin",
    "admin_request_context",
    "admin_tls_config",
    "admin_tls_main",
    "admin_urlopen",
    "async_admin_json",
    "build_opener",
    "build_targets",
    "check_backend_health",
    "check_rbac",
    "check_tenancy",
    "compute_all",
    "health_main",
    "histogram_quantile",
    "instant",
    "is_admin_url",
    "load_dotenv",
    "load_persisted_deploy_env",
    "main",
    "parse_forward",
    "parse_iso_utc",
    "percentile",
    "port_forward_main",
    "port_in_use",
    "probe",
    "remediation_command",
    "run_check",
    "service_id",
    "ssl",
    "urlopen",
    "validate_admin_tls_config",
    "validate_credentials_main",
]


if __name__ == "__main__":
    sys.exit(main())
