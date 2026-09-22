import json
import os
import signal
import subprocess
from pathlib import Path

import pytest

from scripts import local_stack_processes as stack

ROOT = Path("/work/project [test]+")


def process(pid, ppid=1, executable="python3", command=None, born="Mon Sep 7 12:00:00 2026"):
    return stack.Process(pid, ppid, born, executable, command or f"python3 {ROOT}/.venv/bin/uvicorn backend.main:app")


class FakeSystem:
    def __init__(self, processes, cwd=None, listeners=None, holders=None):
        self.processes = {p.pid: p for p in processes}
        self.cwd = cwd or {}
        self.listeners = listeners or {}
        self.holders = holders or {}
        self.signals = []
        self.time = 0
        self.on_signal = lambda pid, sig: None

    def snapshot(self):
        return dict(self.processes)

    def working_directory(self, pid):
        return self.cwd.get(pid)

    def executable(self, p):
        return p.executable

    def listening_pids(self, port):
        return self.listeners.get(port, set())

    def holding_pids(self, paths):
        return set().union(*(self.holders.get(p, set()) for p in paths))

    def signal(self, pid, sig):
        assert pid > 1
        self.signals.append((pid, sig))
        self.on_signal(pid, sig)

    def sleep(self, seconds):
        self.time += seconds

    def monotonic(self):
        return self.time


def test_parse_ps_preserves_space_paths_and_birth_identity():
    rows = stack.parse_ps(" 42  1 Mon Sep  7 12:00:00 2026 /work/a b/python3\n")
    assert rows[42] == (1, "Mon Sep 7 12:00:00 2026", "/work/a b/python3")
    with pytest.raises(stack.CleanupError):
        stack.parse_ps("broken output")


@pytest.mark.parametrize(
    "executable,command,owned",
    [
        ("python3", f"python3 {ROOT}/.venv/bin/uvicorn backend.main:app --reload", True),
        ("python3", f'python3 "{ROOT}/.venv/bin/uvicorn" backend.main:app', True),
        ("python3", f"{ROOT}/.venv/bin/python3 -m uvicorn backend.main:app", True),
        ("node", f"node {ROOT}/frontend/node_modules/next/dist/bin/next dev", True),
        ("node", f"node {ROOT}/frontend/node_modules/.bin/next dev", True),
        ("node", f"node {ROOT}/frontend/.next/standalone/server.js", True),
        ("python3", "python3 /work/project\\ [test]+/.venv/bin/uvicorn backend.main:app", True),
        ("bash", f"bash -c 'python3 {ROOT}/.venv/bin/uvicorn backend.main:app'", False),
        ("python3", f"python3 -c 'print(\"uvicorn {ROOT}\")'", False),
        ("node", f"node -e 'console.log(\"{ROOT}/next\")'", False),
        ("python3", f"python3 {ROOT}-other/.venv/bin/uvicorn backend.main:app", False),
        ("python3", "python3 /another/repo/.venv/bin/uvicorn backend.main:app", False),
        ("node", f"node {ROOT}/frontend/custom-next-script.js", False),
    ],
)
def test_root_selection_is_executable_aware_and_literal(executable, command, owned):
    p = process(42, executable=executable, command=command)
    system = FakeSystem([p])
    assert stack.select_roots(system.snapshot(), ROOT, {}, set(), system, set()) == ({42} if owned else set())


def test_listener_association_uses_cwd_and_captures_reloader():
    parent = process(41, command="python3 -m uvicorn backend.main:app --reload")
    child = process(42, 41, command="python3 -c 'spawn_main()'")
    system = FakeSystem([parent, child], cwd={41: ROOT, 42: ROOT})
    assert stack.select_roots(system.snapshot(), ROOT, {18002: {42}}, set(), system, set()) == {41}


def test_node_next_launcher_is_selected_before_renamed_listener():
    parent = process(41, executable="node", command=f"node {ROOT}/frontend/node_modules/.bin/next dev -H 127.0.0.1")
    child = process(42, 41, executable="node", command="next-server (v16.3.1)")
    system = FakeSystem([parent, child], cwd={42: ROOT / "frontend"})
    assert stack.select_roots(system.snapshot(), ROOT, {13002: {42}}, set(), system, set()) == {41}


def test_unrelated_listener_aborts_before_any_signal():
    system = FakeSystem([process(42)], cwd={42: ROOT.parent / "other"})
    other = process(43, executable="ssh", command="ssh -L 18002:localhost:8000 host")
    system.processes[43] = other
    with pytest.raises(stack.CleanupError, match="listener"):
        stack.select_roots(system.snapshot(), ROOT, {18002: {43}}, set(), system, set())
    assert system.signals == []


@pytest.mark.parametrize(
    "executable,command",
    [("python3", "python3 -m http.server 18002"), ("node", "node unrelated-app.js")],
)
def test_unrelated_runtime_listener_in_same_checkout_is_not_killed(executable, command):
    system = FakeSystem([process(42, executable=executable, command=command)], cwd={42: ROOT})
    with pytest.raises(stack.CleanupError, match="recognized stack command"):
        stack.select_roots(system.snapshot(), ROOT, {18002: {42}}, set(), system, set())
    assert system.signals == []


def test_duckdb_holder_requires_python_executable():
    python = process(42, command="python3 -c 'spawn_main()'")
    shell = process(43, executable="bash", command="bash -c 'echo python3'")
    system = FakeSystem([python, shell])
    assert stack.select_roots(system.snapshot(), ROOT, {}, {42, 43}, system, set()) == {42}


def test_self_ancestors_and_their_trees_are_not_selected():
    system = FakeSystem([process(40), process(41, 40), process(42, 41), process(50)])
    excluded = stack.ancestors(system.snapshot(), 42)
    assert excluded == {1, 40, 41, 42}
    assert stack.select_roots(system.snapshot(), ROOT, {}, set(), system, excluded) == {50}
    assert stack.descendants(system.snapshot(), {40}, excluded) == set()


def test_descendants_survive_reparenting_and_escalation():
    system = FakeSystem([process(42), process(43, 42), process(44, 43)])

    def terminate(pid, sig):
        if pid == 42:
            system.processes.pop(42, None)
            system.processes[43] = process(43)
        if sig == signal.SIGKILL:
            system.processes.pop(pid, None)

    system.on_signal = terminate
    stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)
    assert {pid for pid, sig in system.signals if sig == signal.SIGTERM} == {42, 43, 44}
    assert {pid for pid, sig in system.signals if sig == signal.SIGKILL} == {43, 44}


def test_graceful_shutdown_does_not_escalate():
    system = FakeSystem([process(42)])
    system.on_signal = lambda pid, sig: system.processes.pop(pid)
    stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)
    assert system.signals == [(42, signal.SIGTERM)]


def test_term_visits_reloader_before_child_even_after_pid_wrap():
    system = FakeSystem([process(99), process(42, 99)])
    system.on_signal = lambda pid, sig: system.processes.pop(pid)
    stack.terminate(system, {99}, system.snapshot(), set(), grace=0.2)
    assert system.signals == [(99, signal.SIGTERM), (42, signal.SIGTERM)]


def test_reused_pid_is_never_killed():
    system = FakeSystem([process(42)])
    system.on_signal = lambda pid, sig: system.processes.update({pid: process(pid, born="new birth")})
    stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)
    assert system.signals == [(42, signal.SIGTERM)]


def test_incomplete_shutdown_is_an_error():
    system = FakeSystem([process(42)])
    with pytest.raises(stack.CleanupError, match="surviv"):
        stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)


def test_captured_root_identity_reuse_does_not_select_new_descendants():
    system = FakeSystem([process(42, born="new birth"), process(43, 42)])
    stack.terminate(system, {42}, {42: process(42)}, set(), grace=0.2)
    assert system.signals == []


def test_pid_reuse_between_snapshot_and_term_is_not_signalled():
    system = FakeSystem([process(42)])
    captured = system.snapshot()
    system.processes[42] = process(42, born="new birth")
    stack.terminate(system, {42}, captured, set(), grace=0.2)
    assert system.signals == []


def test_permission_denied_is_not_silently_ignored():
    system = FakeSystem([process(42)])

    def denied(pid, sig):
        raise PermissionError("denied")

    system.on_signal = denied
    with pytest.raises(stack.CleanupError, match="Permission denied"):
        stack.terminate(system, {42}, system.snapshot(), set(), grace=0.2)


def test_system_uses_argument_arrays_for_ps_lsof_and_preserves_literal_paths(monkeypatch):
    commands = []
    path = ROOT / "data/services/file [a]+.duckdb"

    def fake_run(args, **kwargs):
        assert isinstance(args, list)
        assert not kwargs.get("shell")
        commands.append(args)
        if args[0] == "ps":
            tail = "python3" if "comm=" in args[-1] else f"python3 {ROOT}/.venv/bin/uvicorn backend.main:app"
            return subprocess.CompletedProcess(args, 0, f"42 1 Mon Sep 7 12:00:00 2026 {tail}\n", "")
        if "-d" in args:
            return subprocess.CompletedProcess(args, 0, f"p42\nfcwd\nn{ROOT}\n", "")
        return subprocess.CompletedProcess(args, 0, "42\n", "")

    monkeypatch.setattr(stack.subprocess, "run", fake_run)
    system = stack.System()
    assert system.snapshot() == {42: process(42)}
    assert system.working_directory(42) == ROOT
    assert system.listening_pids(18002) == {42}
    assert system.holding_pids([path]) == {42}
    assert ["lsof", "-t", "--", str(path)] in commands
    assert ["lsof", "-nP", "-iTCP:18002", "-sTCP:LISTEN", "-t"] in commands


def test_next_title_requires_actual_node_text_executable(monkeypatch):
    system = stack.System()
    monkeypatch.setattr(system, "run", lambda *args, **kwargs: "p42\nftxt\nn/usr/local/bin/node\n")
    p = process(42, executable="next-server (v16.3.1)", command="next-server (v16.3.1)")
    assert system.executable(p) == "/usr/local/bin/node"
    monkeypatch.setattr(system, "run", lambda *args, **kwargs: "p42\nftxt\nn/usr/local/bin/bash\n")
    assert stack.runtime(system.executable(p)) is None


@pytest.mark.parametrize("returncode,stderr", [(2, ""), (1, "permission denied")])
def test_lsof_failures_are_not_treated_as_an_empty_stack(monkeypatch, returncode, stderr):
    monkeypatch.setattr(
        stack.subprocess, "run", lambda args, **kwargs: subprocess.CompletedProcess(args, returncode, "", stderr)
    )
    with pytest.raises(stack.CleanupError):
        stack.System().listening_pids(18002)


def test_stop_only_inspects_exact_existing_service_databases(monkeypatch, tmp_path):
    checkout = tmp_path / "repo"
    db_dir = checkout / "data/services"
    db_dir.mkdir(parents=True)
    expected = db_dir / "example.duckdb"
    expected.touch()
    (db_dir / "example.duckdb.wal").touch()
    (db_dir / "nested").mkdir()
    (db_dir / "nested/other.duckdb").touch()
    outside = tmp_path / "outside.duckdb"
    outside.touch()
    (db_dir / "linked.duckdb").symlink_to(outside)
    system = FakeSystem([])
    calls = []

    def holders(paths):
        calls.append(paths)
        return set()

    system.holding_pids = holders
    monkeypatch.setattr(stack, "System", lambda: system)
    assert stack.main(["stop", "--checkout", str(checkout), "--port", "18002"]) == 0
    assert calls == [[expected], [expected]]
    assert system.signals == []


def test_stop_detects_respawn_after_termination(monkeypatch, tmp_path):
    system = FakeSystem([])
    monkeypatch.setattr(stack, "System", lambda: system)

    def respawn(*args):
        system.processes[42] = process(42, command=f"python3 {tmp_path}/.venv/bin/uvicorn backend.main:app")

    monkeypatch.setattr(stack, "terminate", respawn)
    assert stack.main(["stop", "--checkout", str(tmp_path), "--port", "18002"]) == 1


def test_capture_and_children_preserve_orphan_identity(monkeypatch, tmp_path, capsys):
    launcher = process(40, executable="bash", command="bash run.sh")
    helper = process(41, 40, command="python3 scripts/local_stack_processes.py capture")
    root = process(42, 40, executable="uv", command="uv run uvicorn backend.main:app")
    worker = process(43, 42)
    system = FakeSystem([launcher, helper, root, worker])
    monkeypatch.setattr(stack, "System", lambda: system)
    monkeypatch.setattr(stack.os, "getpid", lambda: 41)
    assert stack.main(["capture", "--checkout", str(tmp_path), "--pid", "42"]) == 0
    captured = capsys.readouterr().out.strip()
    assert {record["pid"] for record in json.loads(captured)} == {42, 43}
    system.processes.pop(42)
    system.processes[43] = process(43, 1)
    system.on_signal = lambda pid, sig: system.processes.pop(pid)
    assert stack.main(["children", "--checkout", str(tmp_path), "--captured", captured]) == 0
    assert system.signals == [(43, signal.SIGTERM)]


def test_capture_rejects_unrelated_process(monkeypatch, tmp_path):
    system = FakeSystem([process(42)])
    monkeypatch.setattr(stack, "System", lambda: system)
    assert stack.main(["capture", "--checkout", str(tmp_path), "--pid", "42"]) == 1
    assert system.signals == []


def test_launcher_contains_no_name_or_group_termination():
    source = Path("run.sh").read_text()
    assert "pkill" not in source
    assert "killall" not in source
    assert "PGID" not in source
    assert "xargs kill" not in source


@pytest.mark.parametrize("status", [0, 7])
def test_run_stop_uses_helper_and_propagates_failure_without_startup(tmp_path, status):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "scripts").mkdir()
    (checkout / "run.sh").write_text(Path("run.sh").read_text())
    (checkout / ".env").write_text("FRONTEND_PORT=13002\nBACKEND_PORT=18002\n")
    (checkout / "scripts/local_stack_processes.py").write_text(
        "import json, pathlib, sys\n"
        "pathlib.Path('called.json').write_text(json.dumps(sys.argv[1:]))\n"
        f"sys.exit({status})\n"
    )
    result = subprocess.run(["bash", "run.sh", "--stop"], cwd=checkout, capture_output=True, text=True, env=os.environ)
    assert result.returncode == status
    args = json.loads((checkout / "called.json").read_text())
    assert "stop" in args and str(checkout) in args and "18002" in args and "13002" in args
    assert "Syncing backend dependencies" not in result.stdout
    assert ("Stack stopped." in result.stdout) == (status == 0)
