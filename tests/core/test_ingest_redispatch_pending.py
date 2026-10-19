"""``_redispatch_pending``: the shared lost-message guard + batched redispatch
used by both ``sweep_ledger_once`` and ``sweep_rum_ledger_once``."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from backend.core import ingest


def _depths(depth: int, ok: bool = True):
    return patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": depth}, ok))


@pytest.mark.parametrize("label", ["", "RUM "])
def test_dispatches_reclaimed_then_stuck_in_batches(label: str) -> None:
    dispatch = MagicMock()
    n = ingest.LEDGER_CONVERT_BATCH_SIZE
    reclaimed = [f"r{i}" for i in range(3)]
    stuck = [f"s{i}" for i in range(n)]
    with _depths(0):
        redispatched, broker_ok = ingest._redispatch_pending("svc", reclaimed, stuck, dispatch, label)
    assert (redispatched, broker_ok) == (n + 3, True)
    sent = [c.args for c in dispatch.call_args_list]
    assert all(a[0] == "svc" for a in sent)
    assert [k for a in sent for k in a[1]] == reclaimed + stuck
    assert [len(a[1]) for a in sent] == [n, 3]


def test_skips_when_queue_already_holds_enough_messages() -> None:
    dispatch = MagicMock()
    with _depths(10):
        assert ingest._redispatch_pending("svc", [], ["a"], dispatch, "") == (0, True)
    dispatch.assert_not_called()


def test_skips_when_broker_unavailable() -> None:
    dispatch = MagicMock()
    with _depths(0, ok=False):
        assert ingest._redispatch_pending("svc", ["a"], [], dispatch, "RUM ") == (0, False)
    dispatch.assert_not_called()


def test_broker_exception_is_treated_as_unavailable() -> None:
    dispatch = MagicMock()
    with patch("backend.celery_status.celery_queue_depths", side_effect=RuntimeError("boom")):
        assert ingest._redispatch_pending("svc", ["a"], [], dispatch, "") == (0, False)
    dispatch.assert_not_called()


def test_nothing_pending_is_a_noop() -> None:
    dispatch = MagicMock()
    assert ingest._redispatch_pending("svc", [], [], dispatch, "") == (0, True)
    dispatch.assert_not_called()


@pytest.mark.parametrize(
    ("sweep_name", "delay_name", "prefix"),
    [
        ("sweep_ledger_once", "convert_batch_files", "raw/2026/08/27"),
        ("sweep_rum_ledger_once", "convert_batch_rum_files", "raw/rum/2026/08/27"),
    ],
)
def test_sweep_dispatches_each_pending_key_once_with_5000_rows(sweep_name: str, delay_name: str, prefix: str) -> None:
    from tests._helpers.ledger import clear_ledger

    service_id = "test-celery-svc"
    con, cur = clear_ledger(service_id)
    now = time.time()
    stale_claim = now - ingest.LEDGER_RECLAIM_AFTER_S - 60
    old = now - 3600
    rows = []
    for i in range(2500):
        rows.append((service_id, f"{prefix}/claimed_{i:05d}.json.gz", "claimed", "dead", stale_claim, old))
    for i in range(2500):
        rows.append((service_id, f"{prefix}/stuck_{i:05d}.json.gz", "discovered", None, None, old))
    cur.executemany(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    con.commit()

    def _no_files(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    with (
        patch("backend.config.load_config", return_value={"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "b"}),
        patch("backend.core.ingest.list_fos_files", side_effect=_no_files),
        patch("backend.core.ingest.discover_prefix", return_value=0),
        patch("backend.core.ingest.discover_rum_prefix", return_value=0),
        patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
        patch(f"backend.core.ingest.{delay_name}.delay") as delay,
    ):
        summary = getattr(ingest, sweep_name)(service_id)

    keys = [k for c in delay.call_args_list for k in c.args[1]]
    assert summary["reclaimed"] == 2500
    assert summary["redispatched"] == 5000
    assert len(keys) == len(set(keys)) == 5000
    assert delay.call_count == -(-5000 // ingest.LEDGER_CONVERT_BATCH_SIZE)
