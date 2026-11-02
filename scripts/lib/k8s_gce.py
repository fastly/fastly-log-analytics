"""kubectl port-forward supervisor and multi-environment Docker/GCE/K8s credential verification."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

ROOT = Path(__file__).resolve().parents[2]

DEFAULT_READY_TIMEOUT = 30.0
DEFAULT_MAX_RESTARTS = 3
_POLL_INTERVAL = 0.1
_TERM_GRACE = 5.0
_TAIL_BYTES = 400


def _env_attr(name: str, default: Any) -> Any:
    env_mod = sys.modules.get("scripts.lib.environments")
    if env_mod is not None and hasattr(env_mod, name):
        return getattr(env_mod, name)
    return default


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
        cmd += [
            "port-forward",
            "--address",
            self.address,
            self.target,
            f"{self.local_port}:{self.remote_port}",
        ]
        return cmd

    @property
    def label(self) -> str:
        return f"{self.target} {self.local_port}:{self.remote_port}"


def parse_forward(spec: str, *, namespace: str | None = None, context: str | None = None) -> Forward:
    target, _, ports = spec.rpartition(":")
    target, _, local = target.rpartition(":")
    if not target or not local.isdigit() or not ports.isdigit():
        raise ValueError(f"expected TYPE/NAME:LOCAL:REMOTE, got {spec!r}")
    local_port = int(local)
    remote_port = int(ports)
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
        port_in_use_fn = _env_attr("port_in_use", port_in_use)
        for forward in self.forwards:
            if port_in_use_fn(forward.local_port):
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
        running = self._running
        self._running = []
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
        args_in = args_in[:split]
        command = list(sys.argv[1:] if argv is None else argv)[split + 1 :]
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

    handled_signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    previous = {sig: signal.signal(sig, _raise_terminated) for sig in handled_signals}
    fwd_set = ForwardSet(
        forwards,
        kubectl=args.kubectl,
        ready_timeout=args.ready_timeout,
        max_restarts=args.max_restarts,
    )
    try:
        fwd_set.start()
        labels = ", ".join(f.label for f in forwards)
        print(f"port-forward ready: {labels}", flush=True)
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
    check_script = _env_attr("CHECK_SCRIPT", CHECK_SCRIPT)
    try:
        result = subprocess.run(
            command,
            input=check_script,
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
    root_dir = _env_attr("ROOT", ROOT)
    env_file = root_dir / ".env.deploy.local"
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
    load_deploy_env_fn = _env_attr("_load_deploy_env", _load_deploy_env)
    local_env = load_deploy_env_fn()
    gce_project = os.getenv("GCE_PROJECT") or local_env.get("GCE_PROJECT", "")
    gce_zone = os.getenv("GCE_ZONE") or local_env.get("GCE_ZONE", "")
    gce_vm = os.getenv("GCE_VM_NAME") or local_env.get("GCE_VM_NAME", "")
    namespace = os.getenv("K8S_NAMESPACE") or local_env.get("K8S_NAMESPACE", "")
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
    build_targets_fn = _env_attr("build_targets", build_targets)
    run_check_fn = _env_attr("run_check", run_check)
    effective_targets = targets if targets is not None else build_targets_fn()
    with ThreadPoolExecutor(max_workers=len(effective_targets)) as pool:
        futures = {name: pool.submit(run_check_fn, command) for name, command in effective_targets.items()}
        return {name: future.result() for name, future in futures.items()}


def validate_credentials_main(argv: Sequence[str] | None = None) -> int:
    raw_args = sys.argv[1:] if argv is None else argv
    raw = [a for a in raw_args if a != "--validate-credentials"]
    output_dir = Path(raw[0] if raw else ".")
    compute_all_fn = _env_attr("compute_all", compute_all)
    results = compute_all_fn()
    relic = output_dir / "relics" / "credentials_status.json"
    relic.parent.mkdir(parents=True, exist_ok=True)
    relic.write_text(json.dumps(results, indent=2) + "\n")
    return 0


__all__ = [
    "CHECK_SCRIPT",
    "DEFAULT_MAX_RESTARTS",
    "DEFAULT_READY_TIMEOUT",
    "Forward",
    "ForwardExited",
    "ForwardSet",
    "PortForwardError",
    "PortInUse",
    "ReadinessTimeout",
    "_POLL_INTERVAL",
    "_Running",
    "_TAIL_BYTES",
    "_TERM_GRACE",
    "_Terminated",
    "_accepts",
    "_kill_group",
    "_load_deploy_env",
    "_raise_terminated",
    "_run_command",
    "build_targets",
    "compute_all",
    "parse_forward",
    "port_forward_main",
    "port_in_use",
    "run_check",
    "validate_credentials_main",
]
