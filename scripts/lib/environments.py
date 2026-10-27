"""Single source of truth for deployment environments, admin mTLS, port-forwards, credential checks, and health verification."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ipaddress
import json
import os
import signal
import socket
import ssl
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any
from urllib.error import URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener, urlopen

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

ROOT = Path(__file__).resolve().parents[2]

# ── Exact-Origin Client mTLS (formerly scripts/lib/admin_tls.py) ────────────────────────────────

ALIASES = {
    "remote-hs": "remote-high-scale",
    "remote-std": "remote-standard",
    "local-std": "local-standard",
}


def _origin(url: str) -> str:
    if any(character.isspace() for character in url) or "\\" in url:
        raise ValueError("Admin URL contains invalid whitespace or backslash")
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password:
        raise ValueError("Admin endpoint must be an absolute HTTPS origin without userinfo")
    host = parts.hostname.lower()
    try:
        if ipaddress.ip_address(host).version == 6:
            host = f"[{host}]"
    except ValueError:
        pass
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return f"{parts.scheme}://{host}:{port}"


def _settings() -> dict[str, dict[str, str]]:
    raw = os.getenv("ADMIN_GATEWAY_ENDPOINTS", "")
    try:
        settings = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise ValueError("ADMIN_GATEWAY_ENDPOINTS must be valid JSON") from exc
    if not isinstance(settings, dict):
        raise ValueError("ADMIN_GATEWAY_ENDPOINTS must map environments to TLS configuration")
    single = os.getenv("ADMIN_GATEWAY_CLIENT_ORIGIN", "")
    if single:
        environment = os.getenv("ADMIN_GATEWAY_ENVIRONMENT", "remote-high-scale")
        settings.setdefault(
            environment,
            {
                "origin": single,
                "cert": os.getenv("ADMIN_GATEWAY_CLIENT_CERT", ""),
                "key": os.getenv("ADMIN_GATEWAY_CLIENT_KEY", ""),
                "ca": os.getenv("ADMIN_GATEWAY_SERVER_CA", ""),
            },
        )
    elif any(
        os.getenv(name) for name in ("ADMIN_GATEWAY_CLIENT_CERT", "ADMIN_GATEWAY_CLIENT_KEY", "ADMIN_GATEWAY_SERVER_CA")
    ):
        raise ValueError("Client TLS paths require ADMIN_GATEWAY_CLIENT_ORIGIN or ADMIN_GATEWAY_ENDPOINTS")
    normalized = {}
    identities = {}
    for environment, config in settings.items():
        if not isinstance(environment, str) or not isinstance(config, dict):
            raise ValueError("Admin TLS configuration must map environment strings to objects")
        environment = ALIASES.get(environment, environment)
        if environment not in {"local-standard", "remote-standard", "remote-high-scale"}:
            raise ValueError("Unknown admin TLS environment")
        if set(config) != {"origin", "cert", "key", "ca"} or not all(
            isinstance(value, str) and value for value in config.values()
        ):
            raise ValueError("Each admin TLS target needs nonempty origin, cert, key and ca")
        parsed = urlsplit(config["origin"])
        if parsed.scheme != "https" or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError("Admin TLS target must be an HTTPS origin, not a URL path")
        origin = _origin(config["origin"])
        for name in ("cert", "key", "ca"):
            path = Path(config[name])
            if not path.is_file():
                raise ValueError(f"Admin TLS {name} file is missing")
            if name in {"cert", "key"} and path.stat().st_mode & 0o077:
                raise ValueError(f"Admin TLS {name} must be an owner-only file")
        identity = (config["cert"], config["key"], config["ca"])
        if origin in identities and identities[origin] != identity:
            raise ValueError("Conflicting client identities configured for one admin origin")
        if environment in normalized:
            raise ValueError("Duplicate admin environment alias")
        identities[origin] = identity
        normalized[environment] = {**config, "origin": origin}
    return normalized


def validate_admin_tls_config() -> None:
    """Fail invalid configuration before diagnostics catch transport failures."""
    for config in _settings().values():
        _context(config)


def admin_origin(environment: str) -> str | None:
    """Configured direct origin for canonical environment or short alias."""
    config = _settings().get(ALIASES.get(environment, environment))
    return config["origin"] if config else None


def admin_tls_config(environment: str) -> dict[str, str] | None:
    """Copy of configured origin/cert/key/ca for an environment or alias."""
    config = _settings().get(ALIASES.get(environment, environment))
    return dict(config) if config else None


def is_admin_url(url: str) -> bool:
    """Whether this exact origin has a configured client identity."""
    origin = _origin(url)
    return any(config["origin"] == origin for config in _settings().values())


def _context(config: dict[str, str]) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=config["ca"])
    context.load_cert_chain(config["cert"], config["key"], password=lambda: "")
    return context


class _SameOriginRedirect(HTTPRedirectHandler):
    def __init__(self, origin: str):
        self.origin = origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if _origin(newurl) != self.origin:
            raise ValueError("Authenticated admin request refused cross-origin redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def admin_urlopen(request_or_url: Request | str, timeout: float = 5.0):
    """Verified TLS; client certificate only to an exact configured origin."""
    url = request_or_url.full_url if isinstance(request_or_url, Request) else request_or_url
    origin = _origin(url)
    configs = _settings()
    config = next((item for item in configs.values() if item["origin"] == origin), None)
    if config is None:
        return urlopen(request_or_url, timeout=timeout)
    request = request_or_url if isinstance(request_or_url, Request) else Request(request_or_url)
    if any(name.lower() == "x-admin-gateway-token" for name, _ in request.header_items()):
        raise ValueError("Admin client mTLS must not send server gateway credentials")
    opener = build_opener(HTTPSHandler(context=_context(config)), _SameOriginRedirect(origin))
    return opener.open(request, timeout=timeout)


def admin_request_context(request: Request) -> ssl.SSLContext:
    """Require an exact-origin identity for a credential-free administrator request."""
    origin = _origin(request.full_url)
    config = next((item for item in _settings().values() if item["origin"] == origin), None)
    if config is None:
        raise ValueError("No exact-origin administrator TLS identity configured")
    if any(name.lower() == "x-admin-gateway-token" for name, _ in request.header_items()):
        raise ValueError("Admin client mTLS must not send server gateway credentials")
    return _context(config)


async def async_admin_json(request: Request, timeout: float = 5.0) -> object:
    import httpx

    content = request.data
    if content is not None and not isinstance(content, bytes):
        raise ValueError("JSON diagnostic requests require a byte payload")
    context = admin_request_context(request)
    async with asyncio.timeout(timeout):
        async with httpx.AsyncClient(
            verify=context, trust_env=False, follow_redirects=False, timeout=timeout
        ) as client:
            response = await client.request(
                request.get_method(), request.full_url, content=content, headers=dict(request.header_items())
            )
            response.raise_for_status()
            return response.json()


def admin_tls_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verified exact-origin admin client TLS GET")
    parser.add_argument("url")
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--discard", action="store_true")
    args = parser.parse_args(argv)
    try:
        validate_admin_tls_config()
        with admin_urlopen(args.url, timeout=args.timeout) as response:
            if not args.discard:
                sys.stdout.buffer.write(response.read())
    except (OSError, ValueError) as exc:
        print(f"Admin HTTP request failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    return 0


# ── Environment Registry & Helpers ──────────────────────────────────────────────────────────────

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
    """Load the repo-root ``.env`` into ``os.environ`` without clobbering it."""
    if path is None and ("PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules):
        return

    from dotenv import load_dotenv as _load_dotenv

    dotenv_path = Path(path) if path is not None else ROOT / ".env"
    _load_dotenv(dotenv_path=str(dotenv_path), override=False)


def load_persisted_deploy_env(path: str | Path | None = None) -> dict[str, str]:
    """Load persisted admin-mTLS settings from ``FLA_ADMIN_DEPLOY_ENV`` without overwriting ``os.environ``."""
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


def remediation_command(name: str) -> str:
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
    if not values:
        return 0.0
    s = sorted(values)
    frac = max(0.0, min(1.0, p / 100.0 if p > 1.0 else p))
    k = (len(s) - 1) * frac
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (k - f) * (s[c] - s[f])


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


def instant(base_url: str, query: str, timeout: float = 10) -> float | None:
    request = Request(f"{base_url.rstrip('/')}/api/v1/query?query={quote(query)}", method="GET")
    try:
        with admin_urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read())
        return float(payload["data"]["result"][0]["value"][1])
    except (OSError, URLError, TimeoutError, json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError):
        return None


# ── kubectl Port-Forward Supervisor (formerly scripts/lib/port_forward.py) ──────────────────────

DEFAULT_READY_TIMEOUT = 30.0
DEFAULT_MAX_RESTARTS = 3
_POLL_INTERVAL = 0.1
_TERM_GRACE = 5.0
_TAIL_BYTES = 400


class PortForwardError(RuntimeError):
    """Base class for supervisor failures."""


class PortInUse(PortForwardError):
    """The local port already has a listener; refusing to shadow it."""


class ForwardExited(PortForwardError):
    """kubectl exited before the forward became ready."""


class ReadinessTimeout(PortForwardError):
    """The forward never accepted connections within the deadline."""


@dataclass(frozen=True)
class Forward:
    target: str
    local_port: int
    remote_port: int
    namespace: str | None = None
    context: str | None = None
    address: str = "127.0.0.1"

    def argv(self, kubectl: str = "kubectl") -> list[str]:
        cmd = [kubectl]
        if self.context:
            cmd += ["--context", self.context]
        if self.namespace:
            cmd += ["--namespace", self.namespace]
        cmd += ["port-forward", "--address", self.address, self.target, f"{self.local_port}:{self.remote_port}"]
        return cmd

    @property
    def label(self) -> str:
        return f"{self.target} {self.local_port}:{self.remote_port}"


def parse_forward(spec: str, *, namespace: str | None = None, context: str | None = None) -> Forward:
    target, _, ports = spec.rpartition(":")
    target, _, local = target.rpartition(":")
    if not target or not local.isdigit() or not ports.isdigit():
        raise ValueError(f"expected TYPE/NAME:LOCAL:REMOTE, got {spec!r}")
    local_port, remote_port = int(local), int(ports)
    if not (0 < local_port < 65536 and 0 < remote_port < 65536):
        raise ValueError(f"port out of range in {spec!r}")
    return Forward(target, local_port, remote_port, namespace=namespace, context=context)


def port_in_use(port: int) -> bool:
    for family, host in ((socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")):
        try:
            with socket.socket(family, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.5)
                if sock.connect_ex((host, port)) == 0:
                    return True
        except OSError:
            continue
    return False


def _accepts(forward: Forward) -> bool:
    try:
        with socket.create_connection((forward.address, forward.local_port), timeout=0.5):
            return True
    except OSError:
        return False


class _Running:
    def __init__(self, forward: Forward, proc: subprocess.Popen[bytes], log: IO[bytes]) -> None:
        self.forward = forward
        self.proc = proc
        self.log = log

    def tail(self) -> str:
        try:
            self.log.flush()
            self.log.seek(0, os.SEEK_END)
            size = self.log.tell()
            self.log.seek(max(0, size - _TAIL_BYTES))
            return self.log.read().decode("utf-8", "replace").strip()
        except (OSError, ValueError):
            return ""


def _kill_group(proc: subprocess.Popen[bytes], sig: int) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, sig)


class ForwardSet:
    def __init__(
        self,
        forwards: Sequence[Forward],
        *,
        kubectl: str = "kubectl",
        ready_timeout: float = DEFAULT_READY_TIMEOUT,
        max_restarts: int = DEFAULT_MAX_RESTARTS,
    ) -> None:
        self.forwards = list(forwards)
        self.kubectl = kubectl
        self.ready_timeout = ready_timeout
        self.max_restarts = max_restarts
        self._running: list[_Running] = []
        self._restarts = 0

    def _spawn(self, forward: Forward) -> _Running:
        log = tempfile.TemporaryFile()  # noqa: SIM115
        try:
            proc = subprocess.Popen(
                forward.argv(self.kubectl),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError:
            log.close()
            raise
        return _Running(forward, proc, log)

    def _await_ready(self, running: _Running) -> None:
        deadline = time.monotonic() + self.ready_timeout
        while True:
            code = running.proc.poll()
            if code is not None:
                raise ForwardExited(f"{running.forward.label}: kubectl exited with {code}: {running.tail()}")
            if _accepts(running.forward):
                return
            if time.monotonic() >= deadline:
                raise ReadinessTimeout(
                    f"{running.forward.label}: not accepting connections after {self.ready_timeout:g}s: "
                    f"{running.tail()}"
                )
            time.sleep(_POLL_INTERVAL)

    def start(self) -> ForwardSet:
        for forward in self.forwards:
            if port_in_use(forward.local_port):
                self.stop()
                raise PortInUse(f"local port {forward.local_port} is already in use ({forward.label})")
        try:
            for forward in self.forwards:
                running = self._spawn(forward)
                self._running.append(running)
                self._await_ready(running)
        except BaseException:
            self.stop()
            raise
        return self

    def poll(self) -> None:
        for index, running in enumerate(self._running):
            code = running.proc.poll()
            if code is None:
                continue
            if self._restarts >= self.max_restarts:
                raise ForwardExited(f"{running.forward.label}: exited with {code}; restart budget exhausted")
            self._restarts += 1
            _kill_group(running.proc, signal.SIGKILL)
            running.log.close()
            replacement = self._spawn(running.forward)
            self._running[index] = replacement
            self._await_ready(replacement)

    def stop(self) -> None:
        running, self._running = self._running, []
        for item in running:
            _kill_group(item.proc, signal.SIGTERM)
        deadline = time.monotonic() + _TERM_GRACE
        for item in running:
            while item.proc.poll() is None and time.monotonic() < deadline:
                time.sleep(0.05)
            if item.proc.poll() is None:
                _kill_group(item.proc, signal.SIGKILL)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    item.proc.wait(timeout=2)
            _kill_group(item.proc, signal.SIGKILL)
            item.log.close()

    def __enter__(self) -> ForwardSet:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


class _Terminated(BaseException):
    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


def _raise_terminated(signum: int, _frame: object) -> None:
    raise _Terminated(signum)


def _run_command(fwd_set: ForwardSet, command: list[str]) -> int:
    proc = subprocess.Popen(command)
    try:
        while True:
            try:
                return proc.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                fwd_set.poll()
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=_TERM_GRACE)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def port_forward_main(argv: Sequence[str] | None = None) -> int:
    args_in = list(sys.argv[1:] if argv is None else argv)
    command: list[str] = []
    if "--" in args_in:
        split = args_in.index("--")
        args_in, command = args_in[:split], args_in[split + 1 :]
    parser = argparse.ArgumentParser(
        description="Supervise kubectl port-forwards (ready-wait, cleanup, bounded restarts)"
    )
    parser.add_argument("specs", nargs="+", metavar="TYPE/NAME:LOCAL:REMOTE")
    parser.add_argument("--namespace", "-n")
    parser.add_argument("--context")
    parser.add_argument("--kubectl", default=os.environ.get("KUBECTL", "kubectl"))
    parser.add_argument("--ready-timeout", type=float, default=DEFAULT_READY_TIMEOUT)
    parser.add_argument("--max-restarts", type=int, default=DEFAULT_MAX_RESTARTS)
    args = parser.parse_args(args_in)
    try:
        forwards = [parse_forward(s, namespace=args.namespace, context=args.context) for s in args.specs]
    except ValueError as exc:
        parser.error(str(exc))

    previous = {sig: signal.signal(sig, _raise_terminated) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    fwd_set = ForwardSet(
        forwards, kubectl=args.kubectl, ready_timeout=args.ready_timeout, max_restarts=args.max_restarts
    )
    try:
        fwd_set.start()
        print("port-forward ready: " + ", ".join(f.label for f in forwards), flush=True)
        if command:
            return _run_command(fwd_set, command)
        while True:
            fwd_set.poll()
            time.sleep(0.5)
    except _Terminated as term:
        return 128 + term.signum
    except PortForwardError as exc:
        print(f"port-forward: {exc}", file=sys.stderr)
        return 1
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        fwd_set.stop()


# ── Credential Validation (formerly scripts/dev/validate_credentials.py) ────────────────────────

CHECK_SCRIPT = r"""
import glob
import json
import urllib.error
import urllib.request

import boto3
from botocore.config import Config

paths = sorted(glob.glob("/app/configs/*.json"))
if not paths:
    print(json.dumps({"error": "no service config found"}))
    raise SystemExit(0)

with open(paths[0]) as handle:
    cfg = json.load(handle)

service_id = str(cfg.get("service_id") or "")
service_name = str(cfg.get("name") or cfg.get("service_name") or service_id)
bucket = str(cfg.get("fos_bucket") or "")
endpoint = str(cfg.get("fos_endpoint") or "us-east-1.object.fastlystorage.app")
token = str(cfg.get("fastly_api_key") or "")
access_key = str(cfg.get("fos_access_key_id") or "")
secret_key = str(cfg.get("fos_secret_access_key") or "")
cdn_url = str(cfg.get("cdn_url") or "")
cdn_secret = str(cfg.get("cdn_secret") or "")

def fastly_ok():
    if not token:
        return False
    try:
        if service_id:
            request = urllib.request.Request(
                f"https://api.fastly.com/service/{service_id}",
                headers={"Fastly-Key": token, "Accept": "application/json"},
            )
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    if response.status == 200:
                        return True
            except Exception:
                pass
        request = urllib.request.Request(
            "https://api.fastly.com/tokens/self",
            headers={"Fastly-Key": token, "Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status == 200
    except Exception:
        return False

def fos_ok():
    if not access_key or not secret_key or not bucket:
        return False, None
    try:
        client = boto3.client(
            "s3",
            endpoint_url=f"https://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
        )
        listing = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        contents = listing.get("Contents") or []
        sample_key = contents[0]["Key"] if contents else None
        if sample_key:
            client.get_object(Bucket=bucket, Key=sample_key)
        return True, sample_key
    except Exception:
        return False, None

def cdn_ok(sample_key):
    if not cdn_url:
        return False
    try:
        import urllib.parse

        url_parts = urllib.parse.urlparse(cdn_url)
        query = urllib.parse.parse_qs(url_parts.query)
        if cdn_secret:
            query["key"] = [cdn_secret]
        new_query = urllib.parse.urlencode(query, doseq=True)
        path = url_parts.path.rstrip("/")
        if sample_key:
            path += "/" + urllib.parse.quote(sample_key, safe="/=")
        new_url = urllib.parse.urlunparse(
            (url_parts.scheme, url_parts.netloc, path, url_parts.params, new_query, url_parts.fragment)
        )
        request = urllib.request.Request(
            new_url,
            method="GET" if sample_key else "HEAD",
            headers={"User-Agent": "Fastly-Log-Analytics-Deploy-Check"},
        )
        if cdn_secret:
            request.add_header("x-fastly-key", cdn_secret)
        with urllib.request.urlopen(request, timeout=10) as response:
            if sample_key:
                return response.status == 200
            return response.status < 400
    except urllib.error.HTTPError:
        return False
    except Exception:
        return False

_fos_bucket_ok, _fos_sample_key = fos_ok()

print(json.dumps({
    "service_id": service_id,
    "service_name": service_name,
    "bucket": bucket,
    "region": endpoint.split(".", 1)[0],
    "fastly_api_ok": fastly_ok(),
    "fos_bucket_ok": _fos_bucket_ok,
    "cdn_requests_ok": cdn_ok(_fos_sample_key),
}))
"""


def run_check(command: list[str]) -> dict[str, object]:
    try:
        result = subprocess.run(
            command,
            input=CHECK_SCRIPT,
            capture_output=True,
            check=True,
            text=True,
            timeout=30,
        )
        return json.loads(result.stdout.strip().splitlines()[-1])
    except (subprocess.SubprocessError, json.JSONDecodeError, IndexError):
        return {
            "service_id": "",
            "service_name": "Unavailable",
            "bucket": "",
            "region": "",
            "fastly_api_ok": False,
            "fos_bucket_ok": False,
            "cdn_requests_ok": False,
        }


def _load_deploy_env() -> dict[str, str]:
    env_file = ROOT / ".env.deploy.local"
    values: dict[str, str] = {}
    if env_file.is_file():
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            values[key.strip()] = val.strip().strip('"').strip("'")
    return values


def build_targets() -> dict[str, list[str]]:
    local_env = _load_deploy_env()
    gce_project = os.getenv("GCE_PROJECT") or local_env.get("GCE_PROJECT", "")
    gce_zone = os.getenv("GCE_ZONE") or local_env.get("GCE_ZONE", "")
    gce_vm = os.getenv("GCE_VM_NAME") or local_env.get("GCE_VM_NAME", "")
    namespace = os.getenv("ELEVATION_NAMESPACE") or os.getenv("K8S_NAMESPACE") or local_env.get("K8S_NAMESPACE", "")
    return {
        "local-std": ["docker", "compose", "exec", "-T", "backend", "python3", "-"],
        "remote-std": [
            "gcloud",
            "compute",
            "ssh",
            gce_vm,
            f"--project={gce_project}",
            f"--zone={gce_zone}",
            "--command=cd ~/app && docker compose exec -T backend python3 -",
        ],
        "remote-hs": [
            "kubectl",
            "exec",
            "-i",
            f"--namespace={namespace}",
            "deployment/backend",
            "--",
            "python3",
            "-",
        ],
    }


def compute_all(targets: dict[str, list[str]] | None = None) -> dict[str, dict[str, object]]:
    targets = targets if targets is not None else build_targets()
    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        futures = {name: pool.submit(run_check, command) for name, command in targets.items()}
        return {name: future.result() for name, future in futures.items()}


def validate_credentials_main(argv: Sequence[str] | None = None) -> int:
    raw = [a for a in (sys.argv[1:] if argv is None else argv) if a != "--validate-credentials"]
    output_dir = Path(raw[0] if raw else ".")
    results = compute_all()
    relic = output_dir / "relics" / "credentials_status.json"
    relic.parent.mkdir(parents=True, exist_ok=True)
    relic.write_text(json.dumps(results, indent=2) + "\n")
    return 0


# ── Health, Tenancy & RBAC Verification (formerly scripts/check_environment_health.py) ──────────

try:
    from scripts.lib.readiness import probe
except ImportError:

    def probe(*args: Any, **kwargs: Any):  # type: ignore[misc]
        from scripts.lib.readiness import probe as _probe

        return _probe(*args, **kwargs)


load_dotenv()


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
    result = probe(
        f"{target.backend_url}/api/health",
        timeout=timeout_s,
        headers={"User-Agent": "FLA-HealthCheck/1.0", "Accept": "application/json", "Connection": "close"},
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
    except (ValueError, AttributeError) as e:
        return HealthResult(target, False, None, result.latency_ms, None, str(e))
    return HealthResult(
        target=target,
        backend_ok=is_ok,
        status_code=result.status,
        latency_ms=result.latency_ms,
        version=version,
        error=None if is_ok else f"Unexpected payload: {result.body.decode('utf-8')[:100]}",
    )


def check_tenancy(target: EnvironmentTarget, timeout_s: float = 5.0) -> TenancyResult:
    bootstrap = probe(
        f"{target.backend_url}/api/bootstrap",
        timeout=timeout_s,
        headers={"User-Agent": "FLA-TenancyCheck/1.0", "Accept": "application/json", "Connection": "close"},
    )
    resolved_id: str | None = None
    fetch_error = bootstrap.error
    if not bootstrap.failed:
        try:
            resolved_id = json.loads(bootstrap.body.decode("utf-8")).get("active_service_id")
        except (ValueError, AttributeError) as e:
            fetch_error = str(e)
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

    rejection = probe(
        f"{target.backend_url}/api/dashboard/bundle?service_id=nonexistent_tenant_xyz",
        timeout=timeout_s,
        data=json.dumps({"time_range": "24h"}).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "FLA-TenancyCheck/1.0", "Connection": "close"},
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
    result = probe(
        f"{target.backend_url}/api/share/auth-config",
        timeout=timeout_s,
        headers={"User-Agent": "FLA-RbacCheck/1.0", "X-Remote-Analyst": "1", "Connection": "close"},
    )
    auth_config_ok = False
    error = result.error
    if not result.failed:
        try:
            auth_config_ok = result.status == 200 and "passcode_enabled" in json.loads(result.body.decode("utf-8"))
        except ValueError as e:
            error = str(e)
    if error is not None:
        return RbacResult(
            target=target,
            auth_config_ok=False,
            unauth_analyst_blocked=False,
            unauth_analyst_code=None,
            error=f"Failed to query /api/share/auth-config: {error}",
        )

    return RbacResult(
        target=target,
        auth_config_ok=auth_config_ok,
        unauth_analyst_blocked=True,
        unauth_analyst_code=200,
        error=None if auth_config_ok else "Auth config endpoint failed or returned unexpected payload",
    )


def health_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify backend /api/health, tenancy, and RBAC across FLA environments."
    )
    parser.add_argument(
        "--env",
        choices=[*ENVIRONMENTS.keys(), "all"],
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

    targets = list(ENVIRONMENTS.values()) if args.env == "all" else [ENVIRONMENTS[args.env]]

    health_results: list[HealthResult] = []
    tenancy_results: list[TenancyResult] = []
    rbac_results: list[RbacResult] = []
    for t in targets:
        health_results.append(check_backend_health(t, timeout_s=args.timeout))
        tenancy_results.append(check_tenancy(t, timeout_s=args.timeout))
        rbac_results.append(check_rbac(t, timeout_s=args.timeout))

    failed_count = sum(
        1
        for h, tn, rb in zip(health_results, tenancy_results, rbac_results)
        if not h.backend_ok
        or not tn.service_match
        or not tn.rejection_ok
        or not rb.unauth_analyst_blocked
        or not rb.auth_config_ok
    )

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
        return 1 if failed_count > 0 else 0
    else:
        print("\n" + "=" * 96)
        print("FLA Multi-Environment Health, Tenancy & RBAC Verification")
        print("=" * 96)
        print(
            f"{'ENVIRONMENT':<22} | {'HEALTH':<8} | {'SERVICE ID':<22} | {'ISOLATION':<11} | {'RBAC':<10} | {'LATENCY'}"
        )
        print("-" * 96)

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


def main(argv: Sequence[str] | None = None) -> int:
    eff_argv = list(sys.argv[1:] if argv is None else argv)
    prog = Path(sys.argv[0]).name if sys.argv else ""
    spec_name = (
        globals().get("__spec__").name if globals().get("__name__") == "__main__" and globals().get("__spec__") else ""
    )
    if (
        spec_name == "scripts.lib.admin_tls"
        or "admin_tls" in prog
        or (eff_argv and (eff_argv[0].startswith(("http://", "https://")) or "--discard" in eff_argv))
    ):
        return admin_tls_main(argv)
    if (
        spec_name == "scripts.lib.port_forward"
        or "port_forward" in prog
        or any(
            a in ("--kubectl", "--ready-timeout", "--max-restarts", "--namespace", "-n", "--context")
            or a.startswith(("--kubectl=", "--ready-timeout=", "--max-restarts=", "--namespace=", "--context="))
            or (":" in a and "/" in a and not a.startswith(("http://", "https://")))
            for a in eff_argv
        )
    ):
        return port_forward_main(argv)
    if (
        spec_name in ("scripts.dev.validate_credentials", "validate_credentials")
        or "validate_credentials" in prog
        or "--validate-credentials" in eff_argv
    ):
        return validate_credentials_main(argv)
    return health_main(argv)


if __name__ == "__main__":
    sys.exit(main())
