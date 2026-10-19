"""scripts/lib/gce_ssh.sh builds the remote command from a quoted argv (S10).

A stub ``gcloud`` on PATH records the exact argv it receives and, like a real
remote login shell, re-parses ``--command`` with ``bash -c``. Hostile inputs
must arrive on the "remote" side byte-for-byte and never execute.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LIB = REPO / "scripts" / "lib" / "gce_ssh.sh"
TRACKED_SCRIPTS = [
    REPO / "scripts" / "backup_service_configs.sh",
    REPO / "scripts" / "dev" / "snapshot_prod_to_dev.sh",
    REPO / "scripts" / "dev" / "sync-from-remote.sh",
]

HOSTILE = [
    "plain",
    "with space",
    "it's",
    'dq"uote',
    "semi;colon",
    "$(touch CANARY)",
    "`touch CANARY`",
    "a && touch CANARY",
    "pipe | touch CANARY",
    "new\nline",
    "back\\slash",
    "glob*?[x]",
    "$HOME ${HOME}",
    "-leading-dash",
    "",
    "~/tilde",
]

STUB = """#!{python}
import json, os, subprocess, sys
argv = sys.argv[1:]
with open(os.environ["STUB_LOG"], "a") as fh:
    fh.write(json.dumps(argv) + "\\n")
if argv[:2] == ["compute", "ssh"]:
    cmd = [a for a in argv if a.startswith("--command=")][0][len("--command="):]
    sys.exit(subprocess.call(["bash", "-c", cmd]))
if argv[:2] == ["storage", "cp"]:
    import shutil
    shutil.copy(argv[2], os.environ["STUB_UPLOAD"])
    sys.exit(0)
if argv[:2] == ["storage", "ls"]:
    print("2048  " + argv[-1])
    sys.exit(0)
sys.exit(97)
"""

DUMP = """import os, sys
with open(os.environ["DUMP_OUT"], "wb") as fh:
    fh.write(b"\\0".join(a.encode() for a in sys.argv[1:]))
"""


@pytest.fixture
def stub(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    gcloud = bindir / "gcloud"
    gcloud.write_text(STUB.format(python=sys.executable))
    gcloud.chmod(0o755)
    sudo = bindir / "sudo"
    sudo.write_text('#!/bin/sh\nif [ "$1" = "-n" ]; then shift; fi\nexec "$@"\n')
    sudo.chmod(0o755)
    log = tmp_path / "calls.jsonl"
    env = {
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "STUB_LOG": str(log),
        "STUB_UPLOAD": str(tmp_path / "uploaded.tar.gz"),
        "DUMP_OUT": str(tmp_path / "dump.bin"),
    }

    def calls() -> list[list[str]]:
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    return env, calls, tmp_path


def run_bash(body: str, env: dict[str, str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "-c", f'source "{LIB}"\n{body}'],
        env=env,
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )


def test_exact_argv_for_simple_command(stub):
    env, calls, tmp = stub
    r = run_bash("gce_ssh_run my-vm us-central1-a --quiet -- echo /a b", env, tmp)
    assert r.returncode == 0, r.stderr
    assert calls() == [["compute", "ssh", "my-vm", "--zone=us-central1-a", "--quiet", "--command=echo /a b"]]


def test_exact_argv_quotes_hostile_arg(stub):
    env, calls, tmp = stub
    r = run_bash("gce_ssh_run vm z1 -- echo 'a b;$(x)'", env, tmp)
    assert r.returncode == 0, r.stderr
    (argv,) = calls()
    assert argv == ["compute", "ssh", "vm", "--zone=z1", r"--command=echo a\ b\;\$\(x\)"]


@pytest.mark.parametrize("value", HOSTILE)
def test_run_roundtrips_hostile_argument_without_executing(stub, value):
    env, calls, tmp = stub
    dump = tmp / "dump.py"
    dump.write_text(DUMP)
    r = subprocess.run(
        [
            "bash",
            "-c",
            f'source "{LIB}"\ngce_ssh_run vm z1 -- "$1" "$2" "$3" "$4"',
            "_",
            sys.executable,
            str(dump),
            value,
            value,
        ],
        env=env,
        cwd=tmp,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert r.returncode == 0, r.stderr
    assert (tmp / "dump.bin").read_bytes() == b"\0".join([value.encode(), value.encode()])
    assert not (tmp / "CANARY").exists()
    assert len(calls()) == 1


@pytest.mark.parametrize("value", HOSTILE)
def test_script_binds_hostile_args_as_positionals(stub, value):
    env, calls, tmp = stub
    script = 'printf "%s\\0" "$#" "$@" > "$DUMP_OUT"'
    r = subprocess.run(
        [
            "bash",
            "-c",
            f'source "{LIB}"\ngce_ssh_script vm z1 --quiet -- "$1" "$2" "$3"',
            "_",
            script,
            value,
            "second " + value,
        ],
        env=env,
        cwd=tmp,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert r.returncode == 0, r.stderr
    got = (tmp / "dump.bin").read_bytes().split(b"\0")[:-1]
    assert got == [b"2", value.encode(), ("second " + value).encode()]
    assert not (tmp / "CANARY").exists()
    (argv,) = calls()
    assert argv[:5] == ["compute", "ssh", "vm", "--zone=z1", "--quiet"]
    assert argv[5].startswith("--command=bash -c ")


@pytest.mark.parametrize(
    "bad",
    ["", "-oProxyCommand=x", "vm name", "vm;touch CANARY", "vm$(x)", "vm\nx", ".hidden", "a/b"],
)
@pytest.mark.parametrize("slot", ["instance", "zone"])
def test_rejects_unsafe_instance_or_zone(stub, bad, slot):
    env, calls, tmp = stub
    inst, zone = (bad, "z1") if slot == "instance" else ("vm", bad)
    r = subprocess.run(
        ["bash", "-c", f'source "{LIB}"\ngce_ssh_run "$1" "$2" -- true', "_", inst, zone],
        env=env,
        cwd=tmp,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert r.returncode == 2
    assert calls() == []


def test_rejects_non_flag_before_separator_and_missing_command(stub):
    env, calls, tmp = stub
    assert run_bash("gce_ssh_run vm z1 oops -- true", env, tmp).returncode == 2
    assert run_bash("gce_ssh_run vm z1 --quiet", env, tmp).returncode == 2
    assert run_bash("gce_ssh_run vm z1 --quiet --", env, tmp).returncode == 2
    assert run_bash("gce_ssh_script vm z1 --", env, tmp).returncode == 2
    assert calls() == []


def test_exit_status_and_stdout_pass_through(stub):
    env, _, tmp = stub
    r = run_bash("gce_ssh_script vm z1 -- 'echo out; exit 7'", env, tmp)
    assert r.returncode == 7
    assert r.stdout == "out\n"


def test_no_ssh_command_string_building_or_eval_in_lib():
    text = LIB.read_text()
    assert not re.search(r"\beval\b", re.sub(r"^\s*#.*$", "", text, flags=re.M))


@pytest.mark.parametrize("script", TRACKED_SCRIPTS, ids=lambda p: p.name)
def test_tracked_scripts_use_the_shared_helper(script):
    text = script.read_text()
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    assert "source" in code and "lib/gce_ssh.sh" in code
    assert not re.search(r"^\s*(if\s+!?\s*)?(\w+=\"?\$\()?gcloud compute ssh\b", code, re.M)
    assert "--command=" not in code


def test_backup_script_end_to_end_with_hostile_paths(stub):
    env, calls, tmp = stub
    configs = tmp / "it's a dir; $(touch CANARY)" / "configs"
    configs.mkdir(parents=True)
    (configs / "svc.json").write_bytes(os.urandom(4096))
    marker = tmp / "mark dir" / "m'arker.json"
    env = {
        **env,
        "BACKUP_BUCKET": "gs://b",
        "BACKUP_INSTANCE": "vm-1",
        "BACKUP_ZONE": "us-central1-a",
        "BACKUP_CONFIGS_DIR": str(configs),
        "BACKUP_MARKER_VM_PATH": str(marker),
    }
    r = subprocess.run(
        ["bash", str(REPO / "scripts" / "backup_service_configs.sh")],
        env=env,
        cwd=tmp,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert r.returncode == 0, r.stderr + r.stdout
    assert not (tmp / "CANARY").exists()
    with tarfile.open(tmp / "uploaded.tar.gz") as tf:
        assert [m.name for m in tf.getmembers() if m.isfile()] == ["configs/svc.json"]
    payload = json.loads(marker.read_text())
    assert payload["remote"].startswith("gs://b/configs/")
    ssh_calls = [c for c in calls() if c[:2] == ["compute", "ssh"]]
    assert len(ssh_calls) == 2
    assert all(c[2] == "vm-1" and c[3] == "--zone=us-central1-a" for c in ssh_calls)
