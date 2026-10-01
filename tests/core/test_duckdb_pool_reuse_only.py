"""Regression: the dashboard bundle's best-effort 2nd ("parallel") checkout
must NEVER trigger a fresh build.

Production wedge (Local Standard, 2026-10-01): under concurrent verify-phase
dashboard load every ``/api/dashboard/bundle`` 503'd. A py-spy dump of the
wedged process showed all 8 pool slots held by ``_checkout_second`` threads
stalled *inside* ``get_connection`` (the build-fresh path) — the 200 ms
``max_wait`` only bounds the saturated slot-wait, not an admitted cold build,
so concurrent second-checkouts were admitted under the cap, then all stalled
in the serialized DuckLake attach/configure and never drained.

The fix: the 2nd checkout is reuse-only — hand back an already-idle
connection if one is instantly available, otherwise raise ``_PoolBusy`` at
once so the caller falls back to sequential execution on ``ctx.con``. It must
never build fresh (which is what wedged the pool) and never block.
"""

import threading
from unittest.mock import MagicMock

import duckdb
import pytest

from backend.core.duckdb_pool import _Pool, _PoolBusy


def _mock_conn():
    return MagicMock(spec=duckdb.DuckDBPyConnection)


def test_reuse_only_rejects_when_no_idle_even_with_free_capacity():
    """idle empty + capacity available ⇒ reuse_only must reject, not build.

    This is the exact wedge condition: ``_in_use < max_size`` so the normal
    path would build fresh, but a reuse_only acquire must raise ``_PoolBusy``
    immediately without entering the build path or touching ``_in_use`` /
    ``_created_total``.
    """
    pool = _Pool(service_key="reuse_only_cap", max_size=4)
    assert pool._idle.empty()
    assert pool._in_use == 0

    with pytest.raises(_PoolBusy):
        pool.acquire(
            src={"name": "reuse_only_cap", "bucket": "b"},
            max_wait=0.2,
            reuse_only=True,
        )

    # Never built: counters untouched, no slot leaked.
    assert pool._in_use == 0
    assert pool._created_total == 0
    assert pool._idle.empty()


def test_reuse_only_rejects_immediately_without_blocking():
    """A saturated pool + reuse_only must not wait out ``max_wait``."""
    pool = _Pool(service_key="reuse_only_sat", max_size=1)
    pool._in_use = 1  # fully checked out, nothing idle

    done = threading.Event()

    def _try():
        with pytest.raises(_PoolBusy):
            # Large max_wait: if reuse_only honored, this returns at once;
            # if it fell through to the saturated wait it would block ~2s.
            pool.acquire(
                src={"name": "reuse_only_sat", "bucket": "b"},
                max_wait=2.0,
                reuse_only=True,
            )
        done.set()

    t = threading.Thread(target=_try)
    t.start()
    t.join(timeout=0.5)
    assert done.is_set(), "reuse_only acquire blocked instead of rejecting fast"
    # Reuse-only rejections are best-effort skips, not real saturation — they
    # must not inflate the saturated-reject counter the operator watches.
    assert pool._saturated_rejects_total == 0
