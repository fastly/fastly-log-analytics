"""Behavior of scripts/check_no_raw_dateparse.sh (B5/B11 ratchet)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "check_no_raw_dateparse.sh"


def _run(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(root / "scripts" / "check_no_raw_dateparse.sh")], capture_output=True, text=True)


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    (tmp_path / "scripts" / "lib").mkdir(parents=True)
    shutil.copy(SCRIPT, tmp_path / "scripts" / SCRIPT.name)
    shutil.copy(SCRIPT.parent / "lib" / "ratchet_common.sh", tmp_path / "scripts" / "lib" / "ratchet_common.sh")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    for rel, body in files.items():
        f = tmp_path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(body)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def test_guard_passes_on_this_repo() -> None:
    res = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr


def test_guard_flags_new_raw_site_in_new_file(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/repositories/new.py": 'x = datetime.strptime(s, "%Y-%m-%d-%H")\n'})
    res = _run(root)
    assert res.returncode == 1
    assert "backend/repositories/new.py" in res.stderr


def test_guard_flags_count_above_baseline(tmp_path: Path) -> None:
    body = "a = datetime.fromisoformat(x)\nb = datetime.fromisoformat(y)\n"
    root = _repo(tmp_path, {"backend/core/clickhouse_rows.py": body})
    assert _run(root).returncode == 1


def test_guard_ignores_comments_and_tests(tmp_path: Path) -> None:
    root = _repo(
        tmp_path,
        {
            "backend/core/ok.py": "# datetime.fromisoformat(x) was the bug\n",
            "backend/utils/date_utils.py": "datetime.fromisoformat(s)\n",
            "tests/test_x.py": "datetime.strptime(s, f)\n",
        },
    )
    assert _run(root).returncode == 0
