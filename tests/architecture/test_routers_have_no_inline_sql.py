"""Routers must not execute SQL; every query lives in ``backend/repositories`` (AGENTS.md convention)."""

from __future__ import annotations

import re
from pathlib import Path

ROUTERS = Path(__file__).resolve().parents[2] / "backend" / "routers"

_SQL_CALL = re.compile(
    r"\.execute\(|\.executemany\(|\.fetchall\(|\.fetchone\(|track_query\(|\bDESCRIBE\s+\{|"
    r"[\"'`]\s*(SELECT|INSERT INTO|DELETE FROM|UPDATE\s+\w+\s+SET|CREATE\s+(TEMP\s+)?TABLE|WITH\s+\w+\s+AS)\b"
)


def test_no_inline_sql_in_routers() -> None:
    offenders: list[str] = []
    for path in sorted(ROUTERS.rglob("*.py")):
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if line.lstrip().startswith("#"):
                continue
            if _SQL_CALL.search(line):
                offenders.append(f"{path.relative_to(ROUTERS.parent.parent)}:{lineno}: {line.strip()[:100]}")
    assert not offenders, "Move SQL into backend/repositories (+ _sql/):\n" + "\n".join(offenders)
