"""Celery task wrappers in ``backend/core/ingest.py``.

Each wrapper is a one-line delegation, but the delegation itself is the
load-bearing part: the regular-log and RUM ledger pipelines are deliberate
twins (``convert_batch_objects`` / ``convert_batch_rum_objects``). If a task
ever routes to its twin, RUM beacon files get handed to the regular-log JSON
parser (or the reverse) and the whole file fails — or worse, half-parses.

So these pin the wiring: task name, the function each task calls, the
arguments it forwards, and claim semantics when another worker holds a row.
"""

from unittest.mock import patch

from backend.core import ingest
from backend.core.metadata.base import get_con

SERVICE_ID = "test-ingest-task-wrappers"


def test_convert_batch_files_task_converts_the_whole_batch_in_one_call():
    """One call with N keys — not N calls — is the entire point of the
    batched task (one DuckLake catalog commit per batch)."""
    keys = ["raw/a.json.gz", "raw/b.json.gz", "raw/c.json.gz"]
    with (
        patch.object(ingest, "convert_batch_objects", return_value={"committed": 3}) as mock_fn,
        patch.object(ingest, "convert_batch_rum_objects") as mock_rum,
    ):
        assert ingest.convert_batch_files.apply(args=(SERVICE_ID, keys)).get() == {"committed": 3}
    mock_fn.assert_called_once()
    assert mock_fn.call_args.args[0] == SERVICE_ID
    assert mock_fn.call_args.args[1] == keys
    assert mock_fn.call_args.args[2]
    mock_rum.assert_not_called()


def test_convert_batch_rum_files_task_converts_beacon_batch_not_regular_batch():
    keys = ["raw/rum/a.json.gz", "raw/rum/b.json.gz"]
    with (
        patch.object(ingest, "convert_batch_rum_objects", return_value={"committed": 2}) as mock_rum,
        patch.object(ingest, "convert_batch_objects") as mock_regular,
    ):
        assert ingest.convert_batch_rum_files.apply(args=(SERVICE_ID, keys)).get() == {"committed": 2}
    mock_rum.assert_called_once()
    assert mock_rum.call_args.args[0] == SERVICE_ID
    assert mock_rum.call_args.args[1] == keys
    assert mock_rum.call_args.args[2]
    mock_regular.assert_not_called()


def test_every_ledger_task_is_registered_under_its_module_path():
    """The worker's queue routing keys off these names; a rename silently
    stops the task from ever being dispatched."""
    expected = {
        "backend.core.ingest.convert_batch_files": ingest.convert_batch_files,
        "backend.core.ingest.convert_batch_rum_files": ingest.convert_batch_rum_files,
    }
    for name, task in expected.items():
        assert task.name == name
        assert name in ingest.app.tasks
        assert ingest.app.tasks[name].name == name


def test_celery_ledger_claim_semantics():
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/05/logs.json.gz"

    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status) VALUES (?, ?, 'discovered')",
        (service_id, object_key),
    )
    cur.execute(
        "UPDATE ingest_ledger SET status='claimed', claimed_by='worker-1' WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    con.commit()

    with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test"}):
        ingest.convert_batch_files.push_request(id="worker-2")
        try:
            ingest.convert_batch_files(service_id, [object_key])
        finally:
            ingest.convert_batch_files.pop_request()

    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    row = cur.fetchone()
    assert row["status"] == "claimed"
    assert row["claimed_by"] == "worker-1"
