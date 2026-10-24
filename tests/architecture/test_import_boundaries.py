from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_capacity_and_recovery_contract_is_explicit() -> None:
    text = (ROOT / "docs/runbooks/high-scale-capacity-and-recovery.md").read_text()
    assert "2M request events/sec" in text
    assert "5M/sec" in text
    assert "500k RUM events/sec" in text
    assert "1M/sec RUM bursts" in text
    assert "15 minutes" in text
    assert "never silently samples or discards" in text
    assert "FOS is the durable source of truth" in text
    assert "request, RUM vitals, RUM errors, and CMCD" in text


# ── Merged from test_routers_have_no_inline_sql.py ──

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
