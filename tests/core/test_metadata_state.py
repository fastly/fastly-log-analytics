"""Tests for :mod:`backend.core.metadata.state`.

Covers the audit-log merge dedup path (used by state_sync to merge
remote audit rows into the local DB without clobbering analyst-side
entries) and the data-migration retry-on-locked-db path.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

import pytest

from backend.core import metadata_db
from backend.core.metadata import state


def _con(service_id: str):
    return metadata_db.get_con(service_id)


# ── merge_audit_for_service ──────────────────────────────────────────────────


def test_merge_audit_noop_when_empty():
    state.merge_audit_for_service("svc-merge-empty", [])
    # Sanity: didn't touch the DB at all (audit_logs still empty).
    con = _con("svc-merge-empty")
    rows = con.execute("SELECT count(*) FROM audit_logs").fetchone()
    assert rows[0] == 0


def test_merge_audit_inserts_new_rows():
    sid = "svc-merge-new"
    rows = [
        {
            "timestamp": "2026-06-12T10:00:00Z",
            "source_name": "fos",
            "event_type": "ingest_start",
            "details": "{}",
            "actor": "cron",
        },
        {
            "timestamp": "2026-06-12T10:01:00Z",
            "source_name": "fos",
            "event_type": "ingest_done",
            "details": "{}",
            "actor": "cron",
        },
    ]
    state.merge_audit_for_service(sid, rows)
    con = _con(sid)
    n = con.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
    assert n == 2


def test_merge_audit_dedups_on_composite_key():
    """If the (timestamp, source_name, event_type, actor) tuple already
    exists in the local DB, the merge skips that row rather than creating
    a duplicate."""
    sid = "svc-merge-dedup"
    row = {
        "timestamp": "2026-06-12T10:00:00Z",
        "source_name": "fos",
        "event_type": "ingest_start",
        "details": "{}",
        "actor": "cron",
    }
    state.merge_audit_for_service(sid, [row])
    state.merge_audit_for_service(sid, [row])  # second call → dedup, skip
    con = _con(sid)
    n = con.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
    assert n == 1


def test_merge_audit_distinguishes_by_actor():
    """Same timestamp + source_name + event_type but different actor →
    treated as a distinct event and inserted (the local analyst and the
    cron writer can both stamp ``ingest_start`` at the same second
    legitimately)."""
    sid = "svc-merge-actor"
    base = {
        "timestamp": "2026-06-12T10:00:00Z",
        "source_name": "fos",
        "event_type": "ingest_start",
        "details": "{}",
    }
    state.merge_audit_for_service(sid, [{**base, "actor": "cron"}, {**base, "actor": "analyst"}])
    con = _con(sid)
    n = con.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
    assert n == 2


# ── record_applied_data_migration retry path ─────────────────────────────────


def test_record_migration_succeeds_first_try(monkeypatch):
    """The default happy path doesn't retry."""
    sid = "svc-rec-mig"
    state.record_applied_data_migration(sid, "test-mig", duration_s=1.5)
    assert "test-mig" in state.list_applied_data_migrations(sid)


def test_record_migration_retries_on_locked_db(monkeypatch, caplog):
    """When SQLite raises ``database is locked``, the helper retries
    with exponential backoff (200ms / 800ms / 2s)."""
    sid = "svc-rec-mig-retry"
    # Patch time.sleep so retries don't actually wait.
    sleeps: list[float] = []
    monkeypatch.setattr(state.time, "sleep", lambda s: sleeps.append(s))

    # First two calls raise "database is locked", third succeeds.
    real_get_con = metadata_db.get_con
    call_count = {"n": 0}

    def _flaky_get_con(s):
        call_count["n"] += 1
        if call_count["n"] <= 2:
            # Return a connection whose execute() raises the locked error.
            mock_con = MagicMock()
            mock_con.execute.side_effect = sqlite3.OperationalError("database is locked")
            return mock_con
        return real_get_con(s)

    monkeypatch.setattr(state, "get_con", _flaky_get_con)

    import logging as _logging

    with caplog.at_level(_logging.INFO, logger=state.logger.name):
        state.record_applied_data_migration(sid, "retry-mig", duration_s=2.0)

    # The first two retries slept the configured backoffs.
    assert sleeps[:2] == [0.2, 0.8]
    # Migration recorded on the third attempt.
    assert "retry-mig" in state.list_applied_data_migrations(sid)


def test_record_migration_propagates_non_locked_errors(monkeypatch):
    """Other OperationalErrors (e.g. schema mismatch) propagate up
    immediately — only ``database is locked`` triggers retry."""
    sid = "svc-rec-mig-real-error"
    monkeypatch.setattr(state.time, "sleep", lambda s: None)

    mock_con = MagicMock()
    mock_con.execute.side_effect = sqlite3.OperationalError("no such table: applied_data_migrations")
    monkeypatch.setattr(state, "get_con", lambda s: mock_con)

    with pytest.raises(sqlite3.OperationalError) as ei:
        state.record_applied_data_migration(sid, "x", duration_s=0.1)
    assert "no such table" in str(ei.value)


def test_list_applied_returns_empty_on_missing_table(monkeypatch):
    """Defensive: if the schema isn't initialised yet (very first call),
    the SELECT fails with ``no such table`` and the helper returns an
    empty set rather than propagating."""
    sid = "svc-list-no-table"
    mock_con = MagicMock()
    mock_con.execute.side_effect = sqlite3.OperationalError("no such table: applied_data_migrations")
    monkeypatch.setattr(state, "get_con", lambda s: mock_con)

    assert state.list_applied_data_migrations(sid) == set()
