"""``discard_idle`` / ``reset_pool_for_service`` — credential-change pool refresh.

Pool connections bake the S3 ``fos_proxy`` SECRET at build time and the checkout
rebind path only refreshes the iceberg VIEW, never the SECRET. So after a
re-provision (new FOS creds) idle pool conns would 401 every read until restart.
``reset_pool_for_service`` closes the idle conns so the next checkout rebuilds
with fresh creds — WITHOUT entering the heavyweight recycle drain mode.
"""

from unittest.mock import MagicMock

import duckdb

from backend.core import duckdb_pool as _pool
from backend.core.duckdb_pool import _Pool


def _mock_conn():
    return MagicMock(spec=duckdb.DuckDBPyConnection)


def test_discard_idle_closes_idle_and_zeroes_in_use():
    pool = _Pool(service_key="discard_idle", max_size=3)
    conns = [_mock_conn() for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3

    closed = pool.discard_idle()

    assert closed == 3
    assert pool._in_use == 0
    assert pool._idle.empty()
    for c in conns:
        c.close.assert_called_once()
    # Unlike begin_drain, discard_idle must NOT enter draining mode — new
    # acquires keep flowing and build fresh conns with the new credentials.
    assert pool._draining is False


def test_discard_idle_leaves_checked_out_conns():
    """In-flight (checked-out) conns aren't touched — only idle ones close.
    One checked out (in_use=1, none idle) → nothing closed, in_use unchanged."""
    pool = _Pool(service_key="discard_inflight", max_size=2)
    pool._in_use = 1
    assert pool.discard_idle() == 0
    assert pool._in_use == 1


def test_reset_pool_for_service_noop_without_pool():
    assert _pool.reset_pool_for_service("never-queried-service-xyz") == 0


def test_reset_pool_for_service_closes_idle():
    pool = _pool._get_pool("reset-svc-xyz", max_size=2)
    conns = [_mock_conn() for _ in range(2)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 2
    try:
        closed = _pool.reset_pool_for_service("reset-svc-xyz")
        assert closed == 2
        assert pool._idle.empty()
        for c in conns:
            c.close.assert_called_once()
    finally:
        # Don't leak the test pool into the module-global registry.
        with _pool._pools_lock:
            _pool._pools.pop("reset-svc-xyz", None)
