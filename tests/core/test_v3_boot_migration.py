"""Boot-time Postgres schema initialization tests.

A manual ``scripts/setup_pg_schema.py`` step used to stand between "restart the
stack" and "working" (skip it and every metadata query fails with
``relation "cron_runs" does not exist``). It now runs on boot; these tests pin
that it runs, is safe to repeat, and degrades loudly instead of taking the
process down.
"""

from __future__ import annotations

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
    assert "metric_snapshots" in blob
    assert "high_scale_ownership" in blob
    assert "high_scale_source_objects" in blob
    assert "high_scale_publication_manifests" in blob

    # SQLite-only dialect must be translated away.
    assert "AUTOINCREMENT" not in blob
    assert "datetime('now')" not in blob
    assert not any(s.strip().startswith("CREATE TRIGGER") for s in statements)
    # Every statement must be re-runnable against a populated database.
    assert all(
        "IF NOT EXISTS" in s or "DROP INDEX" in s or "ON CONFLICT" in s or "ALTER TABLE" in s for s in statements
    ), "a statement without IF NOT EXISTS makes the bootstrap non-idempotent"


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

    assert "_application_lifespan(app)" in inspect.getsource(main.lifespan)
    assert "ensure_pg_schema" in inspect.getsource(main._application_lifespan)
    assert "ensure_pg_schema" in inspect.getsource(celery_app._worker_process_init)
