"""Shared fixtures for the remote-share test suite.

Every test gets its own ``REMOTE_SHARE_DB_DIR`` (per-test tmp_path) and a
freshly reset ``TunnelManager`` singleton. The autouse fixture also drops
the cached share_db connection pool — otherwise a connection bound to a
previous test's tmp directory would leak across cases.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def isolate_share_db(tmp_path, monkeypatch):
    """Point the share DB at a per-test temp directory."""
    from backend.core import share_db
    from backend.utils import tunnel

    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    share_db.reset_for_tests()
    tunnel.reset_for_tests()
    yield
    share_db.close_all_connections()
    tunnel.reset_for_tests()


@pytest.fixture
def fresh_share_con(isolate_share_db):
    """Return a freshly initialised share DB connection."""
    from backend.core import share_db

    return share_db.get_global_share_con()
