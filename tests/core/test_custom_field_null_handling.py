"""Custom-field NULL handling.

Pins that aggregates over an all-NULL custom-field column follow ANSI
SQL: COUNT(*) counts rows, COUNT(col) skips NULLs, AVG returns NULL.
The sibling type-mismatch suite covers structurally-bad values.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def _row(ts, idx, **extra):
    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S+0000"),
        "ip": f"10.0.0.{idx}",
        "status": 200,
        "url": f"/path/{idx}",
        "method": "GET",
        "cache": "HIT",
        "resp_bytes": 1024 + idx,
        "elapsed": 1500 + idx * 10,
        **extra,
    }


def _connect(env):
    """Open a DuckLake-attached connection with the ``logs`` view registered.
    Returns (con, view_name)."""
    from backend.core import iceberg as ice
    from backend.core.duckdb import get_connection
    from backend.repositories._base import _safe_table

    con = get_connection(env["fos_source"])
    ice.update_iceberg_view(con, env["fos_source"])
    return con, _safe_table(env["fos_source"]["name"])


def _ingest_and_commit(env, expected):
    from backend.core import iceberg as ice

    done = next((e for e in env["drain_ingest"]() if e["type"] == "done"), None)
    assert done is not None, "no 'done' event from ingest"
    assert done["rows_inserted"] == expected, f"expected {expected} rows, got {done}"
    ice.commit_buffer(env["fos_source"])


# ── Tests ────────────────────────────────────────────────────────────────────


def test_aggregation_over_all_null_column(custom_field_env):
    """COUNT(*)=3, COUNT(col)=0, AVG IS NULL — ANSI semantics so
    dashboards render "no data" rather than a misleading zero."""
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"]([{"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":0'}])
    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [_row(base + timedelta(seconds=i), i, score=None) for i in range(3)],
    )
    _ingest_and_commit(env, expected=3)

    con, view = _connect(env)
    n_rows, n_score, avg_score = con.execute(f"SELECT COUNT(*), COUNT(score), AVG(score) FROM {view}").fetchone()
    assert n_rows == 3, f"COUNT(*) must count NULL rows; got {n_rows}"
    assert n_score == 0, f"COUNT(score) must skip NULLs; got {n_score}"
    assert avg_score is None, f"AVG over all-NULL must be NULL, got {avg_score!r}"
