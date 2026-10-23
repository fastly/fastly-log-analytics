"""Behavior of the B11 ratchets: hand-rolled retry loops and inline COALESCE(."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
RETRY = "check_no_handrolled_retry.sh"
COALESCE = "check_no_inline_metric_coalesce.sh"


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    (tmp_path / "scripts" / "lib").mkdir(parents=True)
    for name in (RETRY, COALESCE, "check_no_handrolled_retry.py", "lib/ratchet_common.sh"):
        shutil.copy(SCRIPTS / name, tmp_path / "scripts" / name)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    for rel, body in files.items():
        f = tmp_path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(body)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def _run(root: Path, script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(root / "scripts" / script)], capture_output=True, text=True)


@pytest.mark.parametrize("script", [RETRY, COALESCE])
def test_guards_pass_on_this_repo(script: str) -> None:
    res = subprocess.run(["bash", str(SCRIPTS / script)], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr


RETRY_LOOP = """
import time

def f(op):
    for attempt in range(3):
        try:
            return op()
        except OSError:
            time.sleep(0.1)
"""


def test_retry_guard_flags_new_loop(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/core/new_thing.py": RETRY_LOOP})
    res = _run(root, RETRY)
    assert res.returncode == 1
    assert "backend/core/new_thing.py" in res.stderr


def test_retry_guard_flags_while_loop(tmp_path: Path) -> None:
    body = "import time\n\ndef f(op):\n    while True:\n        try:\n            return op()\n        except Exception:\n            time.sleep(1)\n"
    assert _run(_repo(tmp_path, {"backend/utils/x.py": body}), RETRY).returncode == 1


def test_retry_guard_ignores_sleep_without_handler_and_nested_functions(tmp_path: Path) -> None:
    body = (
        "import time\n\n"
        "def poll():\n    for _ in range(3):\n        time.sleep(1)\n\n"
        "def outer():\n    for _ in range(3):\n"
        "        def inner():\n            try:\n                pass\n            except OSError:\n                time.sleep(1)\n"
    )
    assert _run(_repo(tmp_path, {"backend/utils/ok.py": body}), RETRY).returncode == 0


def test_retry_guard_exempts_retry_module_and_tests(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/core/retry.py": RETRY_LOOP, "tests/test_x.py": RETRY_LOOP})
    assert _run(root, RETRY).returncode == 0


def test_retry_guard_flags_count_above_baseline(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/utils/valkey_publisher.py": RETRY_LOOP + RETRY_LOOP.replace("def f", "def g")})
    assert _run(root, RETRY).returncode == 1


def test_coalesce_guard_flags_new_inline_site(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/repositories/new.py": 'q = "SELECT COALESCE(SUM(x), 0) FROM t"\n'})
    res = _run(root, COALESCE)
    assert res.returncode == 1
    assert "backend/repositories/new.py" in res.stderr


def test_coalesce_guard_flags_rollup_writer(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/core/rollups/new_rollup.py": 'q = "COALESCE(a, 0)"\n'})
    assert _run(root, COALESCE).returncode == 1


def test_coalesce_guard_ignores_comments_sql_templates_and_helpers(tmp_path: Path) -> None:
    root = _repo(
        tmp_path,
        {
            "backend/repositories/ok.py": "# COALESCE(x, 0) was inline here\n",
            "backend/repositories/_sql/tpl.py": 'T = "COALESCE(a, 0)"\n',
            "backend/core/metric_expressions.py": 'f"COALESCE({a}, {b})"\n',
        },
    )
    assert _run(root, COALESCE).returncode == 0
