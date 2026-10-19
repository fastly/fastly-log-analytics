"""Shared ``kubectl port-forward`` supervisor for operator scripts.

One implementation of: build the argv (never a shell string), refuse a local
port that is already taken, wait until every forward actually accepts TCP
connections, restart a dropped forward a bounded number of times, and tear down
whole process groups on exit or on SIGINT/SIGTERM/SIGHUP.

Library::

    with port_forwards([Forward("svc/backend-svc", 8002, 8000, namespace="ns")]):
        ...

CLI (foreground supervisor; with ``-- CMD`` runs CMD and returns its status)::

    python -m scripts.lib.port_forward --namespace ns \\
        svc/frontend-svc:3002:3000 svc/backend-svc:8002:8000 [-- CMD ARG...]
"""

from __future__ import annotations

import argparse
import contextlib
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import IO

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
    """``TYPE/NAME:LOCAL:REMOTE`` -> Forward (e.g. ``svc/backend-svc:8002:8000``)."""
    target, _, ports = spec.rpartition(":")
    target, _, local = target.rpartition(":")
    if not target or not local.isdigit() or not ports.isdigit():
        raise ValueError(f"expected TYPE/NAME:LOCAL:REMOTE, got {spec!r}")
    local_port, remote_port = int(local), int(ports)
    if not (0 < local_port < 65536 and 0 < remote_port < 65536):
        raise ValueError(f"port out of range in {spec!r}")
    return Forward(target, local_port, remote_port, namespace=namespace, context=context)


def port_in_use(port: int) -> bool:
    """Whether anything listens on ``port`` over IPv4 or IPv6 loopback."""
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
    """Start/monitor/stop a group of kubectl port-forwards."""

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
        log = tempfile.TemporaryFile()  # noqa: SIM115 - closed in stop()
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
        """Restart dropped forwards (bounded); raise once the budget is spent."""
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
            # Reap stragglers left in the group (children of kubectl).
            _kill_group(item.proc, signal.SIGKILL)
            item.log.close()

    def __enter__(self) -> ForwardSet:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


@contextlib.contextmanager
def port_forwards(
    forwards: Sequence[Forward],
    *,
    kubectl: str = "kubectl",
    ready_timeout: float = DEFAULT_READY_TIMEOUT,
    max_restarts: int = DEFAULT_MAX_RESTARTS,
) -> Iterator[ForwardSet]:
    with ForwardSet(forwards, kubectl=kubectl, ready_timeout=ready_timeout, max_restarts=max_restarts) as fwd_set:
        yield fwd_set


class _Terminated(BaseException):
    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


def _raise_terminated(signum: int, _frame: object) -> None:
    raise _Terminated(signum)


def main(argv: Sequence[str] | None = None) -> int:
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


if __name__ == "__main__":
    sys.exit(main())
