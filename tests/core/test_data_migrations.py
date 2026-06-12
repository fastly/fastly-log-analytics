"""Tests for :mod:`backend.core.data_migrations`.

The data-migrations framework: registers ordered Migration entries,
records applied state in per-service metadata, and runs pending ones
in a daemon thread. Tests stub the individual migration ``fn`` callables
so we don't actually backfill rollups, but exercise the registry +
applied-tracking + halt-on-failure semantics.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.core import data_migrations, metadata_db


def test_list_pending_returns_all_when_none_applied():
    """Fresh service has nothing applied → every registered migration is pending."""
    pending = data_migrations.list_pending("svc-fresh")
    assert len(pending) == len(data_migrations.MIGRATIONS)
    # Same order as the registry.
    assert [m.name for m in pending] == [m.name for m in data_migrations.MIGRATIONS]


def test_list_pending_excludes_applied():
    sid = "svc-some-applied"
    # Record the first migration as applied.
    first = data_migrations.MIGRATIONS[0].name
    metadata_db.record_applied_data_migration(sid, first, duration_s=1.0, status="success")

    pending = data_migrations.list_pending(sid)
    assert first not in [m.name for m in pending]
    assert len(pending) == len(data_migrations.MIGRATIONS) - 1


def test_list_pending_excludes_all_when_everything_applied():
    sid = "svc-all-applied"
    for m in data_migrations.MIGRATIONS:
        metadata_db.record_applied_data_migration(sid, m.name, duration_s=0.1, status="success")
    pending = data_migrations.list_pending(sid)
    assert pending == []


def test_run_pending_returns_early_when_nothing_pending(monkeypatch):
    sid = "svc-no-pending"
    for m in data_migrations.MIGRATIONS:
        metadata_db.record_applied_data_migration(sid, m.name, duration_s=0.1, status="success")

    thread_spawned = []

    def _fake_thread(*a, **kw):
        thread_spawned.append((a, kw))
        return MagicMock()

    monkeypatch.setattr(data_migrations.threading, "Thread", _fake_thread)
    data_migrations.run_pending(sid, {"name": sid})
    # No thread spawned — short-circuit on empty pending list.
    assert thread_spawned == []


def test_run_pending_spawns_daemon_thread(monkeypatch):
    sid = "svc-spawn-thread"
    captured = {}

    class _CapturingThread:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)
            captured["args"] = args

        def start(self):
            captured["started"] = True

    monkeypatch.setattr(data_migrations.threading, "Thread", _CapturingThread)
    data_migrations.run_pending(sid, {"name": sid})

    assert captured.get("daemon") is True
    assert captured.get("started") is True
    assert sid in captured.get("name", "")


def test_run_sequence_applies_all_when_each_succeeds(monkeypatch):
    sid = "svc-seq-success"
    # Stub each migration fn so it returns a marker note instead of doing work.
    migs = [
        data_migrations.Migration(
            name="test-mig-a",
            description="A",
            fn=lambda s, src: "note-a",
        ),
        data_migrations.Migration(
            name="test-mig-b",
            description="B",
            fn=lambda s, src: "note-b",
        ),
    ]

    data_migrations._run_sequence(sid, {"name": sid}, migs)

    applied = metadata_db.list_applied_data_migrations(sid)
    assert "test-mig-a" in applied
    assert "test-mig-b" in applied


def test_run_sequence_halts_on_failure(monkeypatch):
    """When a migration raises, _run_sequence stops — subsequent migrations
    that may depend on it are NOT run, and nothing is recorded for the
    failed one (it'll retry on the next boot)."""
    sid = "svc-seq-fail"

    def _bad_fn(s, src):
        raise RuntimeError("simulated migration failure")

    migs = [
        data_migrations.Migration(name="test-mig-c", description="C", fn=_bad_fn),
        data_migrations.Migration(
            name="test-mig-d",
            description="D",
            fn=lambda s, src: "should not run",
        ),
    ]

    data_migrations._run_sequence(sid, {"name": sid}, migs)

    applied = metadata_db.list_applied_data_migrations(sid)
    assert "test-mig-c" not in applied
    assert "test-mig-d" not in applied


def test_run_sequence_continues_when_record_fails(monkeypatch, caplog):
    """If the underlying migration succeeds but ``record_applied_data_migration``
    fails (e.g. transient SQLite lock), the runner logs a warning and
    moves to the next migration — it does NOT re-raise. The migration is
    idempotent so the next boot will re-run it without harm."""
    sid = "svc-record-fails"

    monkeypatch.setattr(
        metadata_db,
        "record_applied_data_migration",
        MagicMock(side_effect=RuntimeError("DB locked")),
    )

    migs = [
        data_migrations.Migration(name="test-mig-e", description="E", fn=lambda s, src: "ok"),
        data_migrations.Migration(name="test-mig-f", description="F", fn=lambda s, src: "ok-2"),
    ]
    import logging as _logging

    with caplog.at_level(_logging.WARNING, logger=data_migrations.logger.name):
        data_migrations._run_sequence(sid, {"name": sid}, migs)

    # Both migrations attempted (the runner continued past the record failure).
    assert metadata_db.record_applied_data_migration.call_count == 2


def test_migration_dataclass_is_frozen():
    """Migration is a frozen dataclass — attributes can't be mutated after
    creation. Tests this so accidental mutation in a fn closure can't
    flip a name mid-run."""
    m = data_migrations.MIGRATIONS[0]
    with pytest.raises(Exception):  # FrozenInstanceError subclasses Exception
        m.name = "different"  # type: ignore[misc]


def test_all_registered_migrations_have_unique_names():
    """The runner identifies migrations by name. Duplicate names would
    cause one to skip the other forever after the first applies."""
    names = [m.name for m in data_migrations.MIGRATIONS]
    assert len(names) == len(set(names)), f"duplicate migration names: {names}"


def test_all_registered_migrations_have_callable_fn():
    for m in data_migrations.MIGRATIONS:
        assert callable(m.fn), f"migration {m.name!r} fn is not callable"


def test_run_pending_actually_invokes_migration(monkeypatch):
    """End-to-end shape: run_pending → daemon thread → _run_sequence →
    fn called → applied marker recorded. Uses a synchronous Thread shim
    so the assertion happens without polling."""
    sid = "svc-end-to-end"
    called = []

    def _spy_fn(s: str, src: dict):
        called.append((s, src))
        return "spy ran"

    # Replace the registry briefly with a single test migration.
    test_mig = data_migrations.Migration(name="test-spy", description="spy", fn=_spy_fn)
    monkeypatch.setattr(data_migrations, "MIGRATIONS", [test_mig])

    # Run the daemon thread synchronously so we don't have to poll.
    class _SyncThread:
        def __init__(self, target, args=(), kwargs=None, daemon=False, name=""):
            self._target = target
            self._args = args
            self._kwargs = kwargs or {}

        def start(self):
            self._target(*self._args, **self._kwargs)

    monkeypatch.setattr(data_migrations.threading, "Thread", _SyncThread)

    data_migrations.run_pending(sid, {"name": sid})

    assert called == [(sid, {"name": sid})]
    assert "test-spy" in metadata_db.list_applied_data_migrations(sid)
