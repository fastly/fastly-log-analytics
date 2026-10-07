"""Regression: DuckDB ``threads`` is instance-global and must never change at runtime.

2026-10-07 GCE incident: ``get_connection`` set ``threads=DUCKDB_THREADS`` (2),
the pool's fresh-build path then set ``threads=1`` and the insights prewarmer
set ``threads=1`` on the SAME database instance. Every change makes DuckDB's
``TaskScheduler::RelaunchThreads`` join worker threads while holding the
scheduler mutex; a worker blocked on a DuckLake multi-file scan lock whose
holder was itself waiting on that mutex deadlocked the whole process.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb

import backend.core.duckdb as duckdb_mod
from backend.core.duckdb_pool import _Pool


def test_threads_setting_is_instance_global(tmp_path):
    db = str(tmp_path / "svc.duckdb")
    a = duckdb.connect(db)
    b = duckdb.connect(db)
    try:
        a.execute("SET threads = 1")
        b.execute("SET threads = 3")
        assert a.execute("SELECT current_setting('threads')").fetchone()[0] == 3
    finally:
        a.close()
        b.close()


def test_pool_fresh_build_never_sets_threads(monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_CONN_THREADS", "1")
    pool = _Pool(service_key="test_no_threads", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        patch("backend.core.duckdb.get_connection", return_value=mock_conn),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        pool.acquire(src={"name": "test_no_threads"}, max_wait=0.5)
    executed = [str(c.args[0]).lower() for c in mock_conn.execute.call_args_list if c.args]
    assert not any("threads" in s for s in executed), executed


def test_ensure_threads_only_sets_when_value_differs(tmp_path):
    con = duckdb.connect(str(tmp_path / "svc.duckdb"))
    try:
        con.execute("SET threads = 2")
        spy = MagicMock(wraps=con)
        duckdb_mod._ensure_threads(spy, 2)
        assert not any("SET threads" in str(c.args[0]) for c in spy.execute.call_args_list)
        duckdb_mod._ensure_threads(spy, 3)
        assert con.execute("SELECT current_setting('threads')").fetchone()[0] == 3
    finally:
        con.close()
