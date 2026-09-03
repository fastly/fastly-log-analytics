"""Boot-time 2.x → 3.0 self-migration: Postgres schema init + legacy adoption.

Two manual steps used to stand between "restart the stack" and "working":
``scripts/setup_pg_schema.py`` (skip it and every metadata query fails
with ``relation "cron_runs" does not exist``) and the DuckLake adoption
of pre-v3 parquet. Both now run themselves; these tests pin that they
run, that they are safe to repeat, and that a failure degrades loudly
instead of taking the process down.
"""

from __future__ import annotations

import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core.metadata import pg_schema

# ── Postgres schema bootstrap ────────────────────────────────────────────────


class _FakeCursor:
    """Records DDL. ``fail_with`` maps a substring → exception to raise."""

    def __init__(self, fail_with: dict | None = None):
        self.statements: list[str] = []
        self._fail_with = fail_with or {}

    def execute(self, sql: str):
        self.statements.append(sql)
        for needle, exc in self._fail_with.items():
            if needle in sql:
                raise exc

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, cursor: _FakeCursor):
        self._conn = _FakeConn(cursor)

    def connection(self):
        return self._conn


def _pg_error(sqlstate: str, message: str) -> Exception:
    exc = Exception(message)
    exc.sqlstate = sqlstate  # type: ignore[attr-defined]
    return exc


def test_pg_schema_statements_are_postgres_dialect():
    statements = pg_schema.pg_schema_statements()
    blob = "\n".join(statements)

    # The tables that only exist because of an explicit DDL entry (they are
    # created by SQLite migrations, not _SCHEMA) — the exact gap that lost
    # committed_buffers on Postgres before ADR-18.
    assert "cron_runs" in blob
    assert "slow_queries" in blob
    assert "committed_buffers" in blob
    assert "ingest_ledger" in blob

    # SQLite-only dialect must be translated away.
    assert "AUTOINCREMENT" not in blob
    assert "datetime('now')" not in blob
    assert not any(s.strip().startswith("CREATE TRIGGER") for s in statements)
    # Every statement must be re-runnable against a populated database.
    assert all("IF NOT EXISTS" in s or "DROP INDEX" in s for s in statements), (
        "a statement without IF NOT EXISTS makes the bootstrap non-idempotent"
    )


def test_apply_pg_schema_executes_every_statement():
    cur = _FakeCursor()
    applied, raced = pg_schema.apply_pg_schema(cur)

    assert applied == len(pg_schema.pg_schema_statements())
    assert raced == 0
    assert len(cur.statements) == applied


def test_apply_pg_schema_tolerates_a_concurrent_pod_winning():
    """Concurrent CREATE TABLE/INDEX IF NOT EXISTS can still collide on the
    shared catalog. Those SQLSTATEs mean another pod got there first, which
    is success, not failure."""
    cur = _FakeCursor(
        fail_with={
            "cron_runs (": _pg_error("42P07", 'relation "cron_runs" already exists'),
            "idx_cron_status": _pg_error("23505", "duplicate key value violates unique constraint"),
        }
    )
    applied, raced = pg_schema.apply_pg_schema(cur)

    assert raced == 2
    assert applied == len(pg_schema.pg_schema_statements()) - 2


def test_apply_pg_schema_raises_on_a_real_ddl_error():
    cur = _FakeCursor(fail_with={"cron_runs (": _pg_error("42601", "syntax error at or near")})
    with pytest.raises(RuntimeError, match="failed to apply schema statement"):
        pg_schema.apply_pg_schema(cur)


def test_ensure_pg_schema_is_a_noop_without_metadata_dsn(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    pg_schema.reset_ensure_flag_for_tests()
    assert pg_schema.ensure_pg_schema() is False


def test_ensure_pg_schema_applies_once_per_process(monkeypatch):
    """Cheap by construction: several hundred DDL statements at most, once."""
    from backend.core.metadata import pg_connection

    cur = _FakeCursor()
    monkeypatch.setenv("METADATA_DSN", "postgresql://stub/stub")
    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _FakePool(cur))
    pg_schema.reset_ensure_flag_for_tests()

    assert pg_schema.ensure_pg_schema() is True
    first_pass = len(cur.statements)
    assert first_pass > 0

    # Second call in the same process must not re-issue the DDL.
    assert pg_schema.ensure_pg_schema() is False
    assert len(cur.statements) == first_pass

    # ...and re-applying it is still safe (every statement is IF NOT EXISTS),
    # which is what makes a restart or an explicit ops run harmless.
    assert pg_schema.ensure_pg_schema(force=True) is True
    assert len(cur.statements) == first_pass * 2
    pg_schema.reset_ensure_flag_for_tests()


def test_ensure_pg_schema_never_raises_and_does_not_latch_on_failure(monkeypatch, caplog):
    """A transient Postgres blip at boot must not crash-loop the pod — but it
    must also not be latched as done, so the next caller retries."""
    from backend.core.metadata import pg_connection

    monkeypatch.setenv("METADATA_DSN", "postgresql://stub/stub")

    def _boom():
        raise OSError("connection refused")

    monkeypatch.setattr(pg_connection, "get_pg_pool", _boom)
    pg_schema.reset_ensure_flag_for_tests()

    with caplog.at_level("CRITICAL"):
        assert pg_schema.ensure_pg_schema() is False
    assert "schema bootstrap FAILED" in caplog.text

    cur = _FakeCursor()
    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _FakePool(cur))
    assert pg_schema.ensure_pg_schema() is True, "a failed attempt must not latch"
    pg_schema.reset_ensure_flag_for_tests()


def test_lifespan_and_worker_boot_call_ensure_pg_schema():
    """Both entry points must be wired — a worker can boot before (or
    without) the API pod, so neither can rely on the other."""
    import inspect

    from backend import celery_app, main

    assert "ensure_pg_schema" in inspect.getsource(main.lifespan)
    assert "ensure_pg_schema" in inspect.getsource(celery_app._worker_process_init)


# ── Legacy adoption on boot ──────────────────────────────────────────────────


@pytest.fixture
def adoption_source(tmp_path, monkeypatch):
    from datetime import UTC, datetime, timedelta

    name = f"boot{uuid.uuid4().hex[:8]}"
    cache = tmp_path / f"cache_{name}"
    cache.mkdir(parents=True)
    src = {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == name else None)

    base = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    path = cache / "data" / "timestamp_hour=2026-08-30-12" / "legacy.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "timestamp": pa.array(
                    [base + timedelta(seconds=i) for i in range(4)], type=pa.timestamp("us", tz="UTC")
                ),
                "ip": pa.array([f"10.0.0.{i}" for i in range(4)]),
                "status": pa.array([200] * 4),
            }
        ),
        path,
    )
    return src


def _lake_count(src: dict) -> int:
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import ducklake_table_name

    con = get_connection(src)
    try:
        row = con.execute(f'SELECT count(*) FROM lake."{ducklake_table_name(src)}"').fetchone()
        return int(row[0]) if row else 0
    finally:
        con.close()


def _adoption_runs(service_id: str) -> list[tuple]:
    from backend.core.iceberg._ducklake_migration import ADOPTION_TASK
    from backend.core.metadata.base import get_con

    con = get_con(service_id)
    return con.execute(
        "SELECT status, rows_ingested, summary FROM cron_runs WHERE service_id = ? AND task = ?",
        (service_id, ADOPTION_TASK),
    ).fetchall()


def test_boot_adoption_runs_once_and_a_restart_does_not_re_adopt(adoption_source):
    """The guard is a durable cron_runs row, so it survives the process that
    wrote it — re-adopting would duplicate every row."""
    from backend.core.iceberg._ducklake_migration import legacy_adoption_completed, run_legacy_adoption_once

    sid = adoption_source["name"]
    assert legacy_adoption_completed(sid) is False

    first = run_legacy_adoption_once(sid)
    assert first is not None
    assert first["rows_adopted"] == 4
    assert _lake_count(adoption_source) == 4
    assert legacy_adoption_completed(sid) is True

    runs = _adoption_runs(sid)
    assert len(runs) == 1
    assert runs[0][0] == "success"
    assert runs[0][1] == 4

    # Second boot: skipped outright, no second cron row, no duplicated rows.
    assert run_legacy_adoption_once(sid) is None
    assert len(_adoption_runs(sid)) == 1
    assert _lake_count(adoption_source) == 4


def test_boot_adoption_of_a_fresh_v3_service_is_a_clean_noop(tmp_path, monkeypatch):
    """A service created on v3 has no legacy parquet at all. That must record
    a clean success (so it is never retried), not an error."""
    from backend.core.iceberg._ducklake_migration import run_legacy_adoption_once

    name = f"fresh{uuid.uuid4().hex[:8]}"
    cache = tmp_path / f"cache_{name}"
    cache.mkdir(parents=True)
    src = {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == name else None)

    res = run_legacy_adoption_once(name)
    assert res == {
        "adopted_files": 0,
        "skipped_files": 0,
        "rows_adopted": 0,
        "source": "none",
        "candidate_files": 0,
    }
    runs = _adoption_runs(name)
    assert len(runs) == 1 and runs[0][0] == "success"


def test_boot_adoption_failure_is_recorded_and_never_raises(adoption_source, monkeypatch):
    """Non-fatal by construction: a failed migration must leave a visible
    error row and let the process finish booting."""
    from backend.core.iceberg import _ducklake_migration as mig

    sid = adoption_source["name"]
    monkeypatch.setattr(
        mig,
        "adopt_iceberg_to_ducklake",
        lambda service_id: (_ for _ in ()).throw(RuntimeError("legacy table unreadable")),
    )

    assert mig.run_legacy_adoption_once(sid) is None

    runs = _adoption_runs(sid)
    assert len(runs) == 1
    assert runs[0][0] == "error"
    # Still not "completed" — the next boot retries a failure.
    assert mig.legacy_adoption_completed(sid) is False


def test_boot_adoption_respects_the_opt_out(adoption_source, monkeypatch):
    from backend.core.iceberg import _ducklake_migration as mig

    sid = adoption_source["name"]
    monkeypatch.setenv("FLA_SKIP_LEGACY_ADOPTION", "1")

    assert mig.run_legacy_adoption_once(sid) is None
    assert mig.start_legacy_adoption_sweep([sid]) is None
    assert _adoption_runs(sid) == []

    # The explicit admin endpoint still works — it is the "run it now" button.
    assert mig.run_legacy_adoption_once(sid, force=True) is not None


def test_adoption_sweep_runs_in_the_background_and_covers_every_service(adoption_source):
    from backend.core.iceberg._ducklake_migration import start_legacy_adoption_sweep

    sid = adoption_source["name"]
    thread = start_legacy_adoption_sweep([sid])
    assert thread is not None
    assert thread.daemon, "adoption must never hold up process exit"
    thread.join(timeout=60)
    assert not thread.is_alive()

    assert _lake_count(adoption_source) == 4
    assert len(_adoption_runs(sid)) == 1


def test_sweep_is_wired_into_background_startup():
    import inspect

    from backend import main

    assert "_start_legacy_adoption" in inspect.getsource(main._background_startup)
    assert "start_legacy_adoption_sweep" in inspect.getsource(main._start_legacy_adoption)


def test_empty_service_list_starts_no_thread():
    from backend.core.iceberg._ducklake_migration import start_legacy_adoption_sweep

    assert start_legacy_adoption_sweep([]) is None
    assert os.environ.get("FLA_SKIP_LEGACY_ADOPTION") != "1"
