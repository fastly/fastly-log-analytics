"""Guard: high-scale ClickHouse readers must not hardcode a database name.

The ClickHouse client connects with a configured default database
(``CLICKHOUSE_DATABASE`` → ``clickhouse_client`` settings.database), which is
``fla_prototype`` for the local Docker high-scale stack and
``fastly_log_analytics`` on the remote (Elevation) cluster. Table references
must therefore stay UNQUALIFIED so they resolve against whatever default
database the connection was opened with — exactly how ``rum.py`` has always
worked.

Regression: ``network.py`` / ``security.py`` / ``sessions.py`` /
``insights.py`` / ``cmcd.py`` / ``query.py`` hardcoded
``FROM fastly_log_analytics.<table>``. That silently worked on Elevation
(whose DB is literally named ``fastly_log_analytics``) but on the local
stack threw ``UNKNOWN_DATABASE``, which the readers swallow via a bare
``except`` and render as "No data available" — the Network page showed no
data on Local High-Scale for exactly this reason. This guard would have
caught it and keeps the prod DB name from creeping back in.
"""

from __future__ import annotations

import pathlib

_HIGH_SCALE_DIR = pathlib.Path(__file__).resolve().parents[2] / "backend" / "high_scale"
_FORBIDDEN = "fastly_log_analytics."


def test_high_scale_readers_do_not_hardcode_prod_database_name() -> None:
    offenders: list[str] = []
    for py in sorted(_HIGH_SCALE_DIR.glob("*.py")):
        text = py.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if _FORBIDDEN in line:
                offenders.append(f"{py.name}:{lineno}: {line.strip()}")
    assert not offenders, (
        "High-scale readers must use unqualified table names so they resolve "
        "against the connection's configured default database (fla_prototype "
        "locally, fastly_log_analytics on Elevation). Hardcoding "
        f"'{_FORBIDDEN}' breaks every non-prod-named database. Offenders:\n" + "\n".join(offenders)
    )
