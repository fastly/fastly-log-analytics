"""Pins the precondition that makes every ``is_postgres()`` SQLite arm dead code.

ADR-22: metadata lives in Postgres only. ``get_con`` / ``get_con_readonly`` (and the
``usage_log_db`` twins) cannot hand out a connection without ``METADATA_DSN``, and
``validate_deployment_mode`` refuses to boot a deployment without a Postgres DSN. So any code
that already holds a metadata connection is on Postgres, and a ``not is_postgres()`` branch
beside it can never run.
"""

from __future__ import annotations

import threading

import pytest

from backend import config
from backend.core.metadata import base, pg_connection, usage_log_db


def _in_fresh_thread(fn):
    """Run ``fn`` on a thread with no cached thread-local connection; return its exception."""
    box: dict[str, BaseException | None] = {"exc": None}

    def run():
        try:
            fn()
        except BaseException as e:  # noqa: BLE001 — the exception is the assertion target
            box["exc"] = e

    t = threading.Thread(target=run)
    t.start()
    t.join()
    return box["exc"]


@pytest.fixture
def no_dsn(monkeypatch):
    pg_connection.reset_pg_pool_for_tests()
    monkeypatch.delenv("METADATA_DSN", raising=False)
    yield
    pg_connection.reset_pg_pool_for_tests()


@pytest.mark.parametrize(
    "opener",
    [base.get_con, base.get_con_readonly, usage_log_db.get_con, usage_log_db.open_readonly],
    ids=["base.get_con", "base.get_con_readonly", "usage_log_db.get_con", "usage_log_db.open_readonly"],
)
def test_connection_openers_raise_without_dsn(no_dsn, opener):
    exc = _in_fresh_thread(lambda: opener("svc-1"))
    assert isinstance(exc, RuntimeError)
    assert "METADATA_DSN is not set" in str(exc)


def test_deployment_validation_refuses_a_non_postgres_metadata_dsn(monkeypatch):
    monkeypatch.setattr(config, "DEPLOYMENT_MODE", "standard")
    monkeypatch.setattr(config, "DUCKLAKE_CATALOG", "postgresql://u:p@h/db")
    monkeypatch.setenv("METADATA_DSN", "sqlite:///tmp/x.db")
    with pytest.raises(RuntimeError, match="requires METADATA_DSN to be a Postgres DSN"):
        config.validate_deployment_mode()
