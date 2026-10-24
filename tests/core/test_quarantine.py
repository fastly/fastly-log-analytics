"""Tests for the quarantine metadata core layer."""

from __future__ import annotations

import pytest

from backend.core.metadata.quarantine import (
    get_con,
    get_quarantine_storage_total,
)


@pytest.fixture
def svc_id():
    svc = "test-quarantine-svc"
    get_con(svc).execute("SELECT 1")  # trigger migrations
    return svc


def test_quarantine_storage_total_default_zero(svc_id):
    assert get_quarantine_storage_total(svc_id) == 0


def test_service_lock_always_takes_the_postgres_advisory_lock(monkeypatch):
    from contextlib import contextmanager

    from backend.core import quarantine as core_quarantine
    from backend.core.metadata import pg_connection

    executed: list[tuple] = []

    class _Con:
        def execute(self, sql, params=None):
            executed.append((sql, params))

        @contextmanager
        def transaction(self):
            yield

    class _Pool:
        @contextmanager
        def connection(self):
            yield _Con()

    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _Pool())

    with core_quarantine._service_lock("svc-lock"):
        pass

    assert executed == [("SELECT pg_advisory_xact_lock(%s)", (core_quarantine._advisory_key("svc-lock"),))]
