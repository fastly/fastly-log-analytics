#!/usr/bin/env python3
"""Stop only identified local-stack PIDs; never signal names or process groups."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path


class CleanupError(RuntimeError):
    pass


@dataclass(frozen=True)
class Process:
    pid: int
    ppid: int
    born: str
    executable: str
    command: str


def parse_ps(output):
    rows = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.split(None, 7)
        if len(fields) != 8 or not fields[0].isdigit() or not fields[1].isdigit():
            raise CleanupError("Unable to parse process snapshot; refusing cleanup")
        rows[int(fields[0])] = (int(fields[1]), " ".join(fields[2:7]), fields[7].strip())
    return rows


class System:
    signal = staticmethod(os.kill)
    sleep = staticmethod(time.sleep)
    monotonic = staticmethod(time.monotonic)

    @staticmethod
    def run(args, allow_empty=False):
        result = subprocess.run(args, capture_output=True, text=True, env={**os.environ, "LC_ALL": "C"}, timeout=10)
        if result.returncode and not (allow_empty and result.returncode == 1 and not result.stderr.strip()):
            raise CleanupError(f"{args[0]} failed; refusing unverified cleanup: {result.stderr.strip()}")
        return result.stdout

    def snapshot(self):
        commands = parse_ps(self.run(["ps", "-ww", "-axo", "pid=,ppid=,lstart=,command="]))
        executables = parse_ps(self.run(["ps", "-ww", "-axo", "pid=,ppid=,lstart=,comm="]))
        return {
            pid: Process(pid, ppid, born, executables[pid][2], command)
            for pid, (ppid, born, command) in commands.items()
            if pid in executables and executables[pid][:2] == (ppid, born)
        }

    def working_directory(self, pid):
        output = self.run(["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"], allow_empty=True)
        paths = [line[1:] for line in output.splitlines() if line.startswith("n")]
        return Path(paths[0]).resolve() if len(paths) == 1 else None

    def executable(self, process):
        # Next replaces its process title, including ps's comm field on macOS.
        if process.executable.startswith("next-server "):
            output = self.run(["lsof", "-a", "-p", str(process.pid), "-d", "txt", "-Fn"], allow_empty=True)
            for line in output.splitlines():
                if line.startswith("n") and Path(line[1:]).name in {"node", "nodejs"}:
                    return line[1:]
        return process.executable

    def listening_pids(self, port):
        output = self.run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"], allow_empty=True)
        return self.parse_pids(output)

    def holding_pids(self, paths):
        pids = set()
        for path in paths:
            pids.update(self.parse_pids(self.run(["lsof", "-t", "--", str(path)], allow_empty=True)))
        return pids

    @staticmethod
    def parse_pids(output):
        if any(not line.isdigit() or int(line) <= 1 for line in output.splitlines()):
            raise CleanupError("Invalid lsof PID output; refusing cleanup")
        return {int(line) for line in output.splitlines()}


def ancestors(snapshot, pid):
    excluded = {1}
    while pid > 1 and pid not in excluded:
        excluded.add(pid)
        process = snapshot.get(pid)
        if process is None:
            break
        pid = process.ppid
    return excluded


def descendants(snapshot, roots, excluded):
    selected = set(roots) - excluded - {0, 1}
    while True:
        children = {p.pid for p in snapshot.values() if p.ppid in selected and p.pid not in excluded}
        expanded = selected | children
        if expanded == selected:
            return selected
        selected = expanded


def runtime(executable):
    name = Path(executable).name.lower()
    if re.fullmatch(r"python(?:\d+(?:\.\d+)*)?|uvicorn", name):
        return "python"
    if name in {"node", "nodejs"}:
        return "node"
    return None


def command_arguments(command, checkout):
    # ps renders argv without quoting. Protect the known literal checkout path
    # before tokenizing so spaces/metacharacters are not treated as delimiters.
    marker = "__LOCAL_STACK_CHECKOUT__"
    if marker in command:
        return []
    try:
        return [part.replace(marker, str(checkout)) for part in shlex.split(command.replace(str(checkout), marker))]
    except ValueError:
        return []


def within(path, checkout):
    return path == str(checkout) or path.startswith(str(checkout) + "/")


def stack_command(process, checkout, kind):
    args = command_arguments(process.command, checkout)
    if not args:
        return False
    first = runtime(args[0])
    if kind == "python" and first == "python":
        if Path(args[0]).name == "uvicorn":
            return len(args) > 1 and args[1] == "backend.main:app"
        if len(args) > 3 and args[1:3] == ["-m", "uvicorn"]:
            return args[3] == "backend.main:app"
        return len(args) > 2 and Path(args[1]).name == "uvicorn" and args[2] == "backend.main:app"
    if kind == "node" and first == "node" and len(args) > 1:
        script = args[1]
        return script.endswith(
            (
                "/next/dist/bin/next",
                "/next/dist/server/start-server.js",
                "/node_modules/.bin/next",
                "/.next/standalone/server.js",
                "/.next/standalone/frontend/server.js",
            )
        ) or script in {
            ".next/standalone/server.js",
            ".next/standalone/frontend/server.js",
        }
    return False


def select_roots(snapshot, checkout, listeners, holders, system, excluded):
    kinds = {}
    commands = set()
    associated = {}
    for pid, process in snapshot.items():
        if pid in excluded:
            continue
        kind = runtime(system.executable(process))
        kinds[pid] = kind
        if kind and stack_command(process, checkout, kind):
            commands.add(pid)

    def owned(pid):
        if pid not in associated:
            process = snapshot[pid]
            args = command_arguments(process.command, checkout)
            # Only an executable/script argument establishes path ownership,
            # never an arbitrary later argument (such as a log message).
            path_owned = pid in commands and any(within(arg, checkout) for arg in args[:2])
            cwd = None if path_owned else system.working_directory(pid)
            associated[pid] = path_owned or (cwd is not None and within(str(cwd), checkout))
        return associated[pid]

    roots = {pid for pid in commands if owned(pid)}
    for port, pids in listeners.items():
        for pid in pids:
            if pid in excluded or pid not in snapshot or not kinds.get(pid) or not owned(pid):
                raise CleanupError(f"Unrelated or unverifiable listener PID {pid} on port {port}; refusing cleanup")
            # Prefer its validated reloader ancestor to avoid a respawn.
            parent = snapshot[pid].ppid
            seen = set()
            root = pid
            known_app = pid in commands or (kinds[pid] == "node" and snapshot[pid].command.startswith("next-server "))
            known_app = known_app or (pid in holders and kinds[pid] == "python")
            while parent in snapshot and parent not in seen and parent not in excluded:
                seen.add(parent)
                if parent in commands and owned(parent):
                    root = parent
                    known_app = True
                parent = snapshot[parent].ppid
            if not known_app:
                raise CleanupError(f"Unrelated listener PID {pid} on port {port}; not a recognized stack command")
            roots.add(root)
    roots.update(pid for pid in holders if pid not in excluded and kinds.get(pid) == "python")
    # One root per tree: TERM the reloader before its worker, regardless of the
    # order in which ps/lsof happened to report them.
    return {
        pid
        for pid in roots
        if not any(parent in roots for parent in ancestors(snapshot, snapshot[pid].ppid) - excluded)
    }


def same_process(expected, current):
    return current is not None and expected.pid == current.pid and expected.born == current.born


def terminate(system, roots, captured, excluded, grace=5.0):
    current = system.snapshot()
    live_roots = {pid for pid in roots if pid in captured and same_process(captured[pid], current.get(pid))}
    selected = descendants(current, live_roots, excluded)
    targets = {pid: current[pid] for pid in selected if pid in current}
    # Keep the original tree even after a dying parent reparents its children.
    targets.update({pid: p for pid, p in captured.items() if pid not in excluded and pid > 1})
    order = sorted(targets, key=lambda pid: (len(ancestors(targets, pid)), pid))

    def survivors():
        now = system.snapshot()
        return {pid: p for pid, p in targets.items() if same_process(p, now.get(pid))}

    def send(sig):
        for pid in order:
            expected = targets[pid]
            if not same_process(expected, system.snapshot().get(pid)):
                continue
            try:
                system.signal(pid, sig)
            except ProcessLookupError:
                pass
            except PermissionError as exc:
                raise CleanupError(f"Permission denied stopping PID {pid}") from exc

    def wait(seconds):
        deadline = system.monotonic() + seconds
        while survivors() and system.monotonic() < deadline:
            system.sleep(0.1)

    send(signal.SIGTERM)
    wait(grace)
    if survivors():
        send(signal.SIGKILL)
        wait(1.0)
    remaining = survivors()
    if remaining:
        raise CleanupError(f"Cleanup incomplete; surviving PIDs: {', '.join(map(str, sorted(remaining)))}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["stop", "capture", "children"])
    parser.add_argument("--checkout", required=True, type=Path)
    parser.add_argument("--port", type=int, action="append", default=[])
    parser.add_argument("--pid", type=int)
    parser.add_argument("--captured", action="append", default=[])
    args = parser.parse_args(argv)
    system = System()
    try:
        checkout = args.checkout.resolve(strict=True)
        snapshot = system.snapshot()
        excluded = ancestors(snapshot, os.getpid())
        if args.action == "capture":
            if args.pid not in snapshot:
                raise CleanupError("Launched process exited before its identity could be captured")
            if args.pid in excluded or snapshot[args.pid].ppid not in excluded - {1}:
                raise CleanupError("Capture target is not a child of this launcher")
            tree = descendants(snapshot, {args.pid}, excluded)
            print(json.dumps([asdict(snapshot[pid]) for pid in sorted(tree)]))
            return 0
        if args.action == "stop":
            if not args.port or any(not 1 <= port <= 65535 for port in args.port):
                raise CleanupError("Stop requires valid selected listener ports")
            listeners = {port: system.listening_pids(port) for port in args.port}
            db_dir = checkout / "data" / "services"
            paths = sorted(p for p in db_dir.glob("*.duckdb") if p.is_file() and p.resolve() == p)
            roots = select_roots(snapshot, checkout, listeners, system.holding_pids(paths), system, excluded)
            captured = {pid: snapshot[pid] for pid in descendants(snapshot, roots, excluded) if pid in snapshot}
        else:
            captured = {}
            for encoded in args.captured:
                for record in json.loads(encoded):
                    process = Process(**record)
                    captured[process.pid] = process
            roots = set(captured)
        terminate(system, roots, captured, excluded)
        if args.action == "stop":
            remaining = system.snapshot()
            listeners = {port: system.listening_pids(port) for port in args.port}
            roots = select_roots(remaining, checkout, listeners, system.holding_pids(paths), system, excluded)
            if roots or any(listeners.values()):
                raise CleanupError("Cleanup incomplete: stack processes or selected listeners appeared during shutdown")
        return 0
    except (CleanupError, OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"[!] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
