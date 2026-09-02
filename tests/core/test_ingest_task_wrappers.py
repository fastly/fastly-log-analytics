"""Celery task wrappers in ``backend/core/ingest.py``.

Each wrapper is a one-line delegation, but the delegation itself is the
load-bearing part: the regular-log and RUM ledger pipelines are deliberate
twins (``discover_prefix``/``discover_rum_prefix``,
``sweep_ledger_once``/``sweep_rum_ledger_once``,
``convert_batch_objects``/``convert_rum_object``) whose sweeps exclude each
other's keyspace by a ``LIKE``/``NOT LIKE`` pair. If a task ever routes to
its twin, RUM beacon files get handed to the regular-log JSON parser (or the
reverse) and the whole file fails — or worse, half-parses.

So these pin the wiring: task name, the function each task calls, and the
arguments it forwards. The underlying functions' behaviour is covered in
tests/core/test_step4_sweeper.py, tests/core/test_convert_batch.py,
tests/core/test_rum_ledger.py and tests/core/test_rum_ledger_edges.py.
"""

from unittest.mock import patch

from backend.core import ingest

SERVICE_ID = "test-ingest-task-wrappers"


def test_dispatch_minute_discovers_the_regular_log_prefix():
    with patch.object(ingest, "discover_prefix", return_value=3) as mock_fn:
        assert ingest.dispatch_minute.apply(args=(SERVICE_ID, "raw/2026/08/27/14/05/")).get() == 3
    mock_fn.assert_called_once_with(SERVICE_ID, prefix_subpath="raw/2026/08/27/14/05/")


def test_dispatch_rum_minute_discovers_the_rum_prefix_not_the_regular_one():
    with (
        patch.object(ingest, "discover_rum_prefix", return_value=2) as mock_rum,
        patch.object(ingest, "discover_prefix") as mock_regular,
    ):
        result = ingest.dispatch_rum_minute.apply(args=(SERVICE_ID, "rum/raw/2026/08/27/14/05/")).get()
    assert result == 2
    mock_rum.assert_called_once_with(SERVICE_ID, prefix_subpath="rum/raw/2026/08/27/14/05/")
    mock_regular.assert_not_called()


def test_convert_task_converts_one_regular_log_object():
    with patch.object(ingest, "convert_object", return_value="committed") as mock_fn:
        assert ingest.convert.apply(args=(SERVICE_ID, "raw/a.json.gz")).get() == "committed"
    args = mock_fn.call_args.args
    assert args[0] == SERVICE_ID
    assert args[1] == "raw/a.json.gz"
    assert args[2]  # a worker id is always supplied (task id, or the fallback)


def test_convert_batch_files_task_converts_the_whole_batch_in_one_call():
    """One call with N keys — not N calls — is the entire point of the
    batched task (one DuckLake catalog commit per batch)."""
    keys = ["raw/a.json.gz", "raw/b.json.gz", "raw/c.json.gz"]
    with patch.object(ingest, "convert_batch_objects", return_value={"committed": 3}) as mock_fn:
        assert ingest.convert_batch_files.apply(args=(SERVICE_ID, keys)).get() == {"committed": 3}
    mock_fn.assert_called_once()
    assert mock_fn.call_args.args[0] == SERVICE_ID
    assert mock_fn.call_args.args[1] == keys


def test_convert_rum_task_converts_a_beacon_file_not_a_regular_log_file():
    with (
        patch.object(ingest, "convert_rum_object", return_value="committed") as mock_rum,
        patch.object(ingest, "convert_object") as mock_regular,
        patch.object(ingest, "convert_batch_objects") as mock_batch,
    ):
        result = ingest.convert_rum.apply(args=(SERVICE_ID, "rum/raw/b.json.gz")).get()
    assert result == "committed"
    assert mock_rum.call_args.args[:2] == (SERVICE_ID, "rum/raw/b.json.gz")
    assert mock_rum.call_args.args[2]
    mock_regular.assert_not_called()
    mock_batch.assert_not_called()


def test_sweep_ledger_task_forwards_the_lookback_window():
    with patch.object(ingest, "sweep_ledger_once", return_value={"reclaimed": 0}) as mock_fn:
        ingest.sweep_ledger.apply(args=(SERVICE_ID,), kwargs={"lookback_hours": 12}).get()
    mock_fn.assert_called_once_with(SERVICE_ID, lookback_hours=12)


def test_sweep_rum_ledger_task_sweeps_only_the_rum_ledger():
    with (
        patch.object(ingest, "sweep_rum_ledger_once", return_value={"reclaimed": 1}) as mock_rum,
        patch.object(ingest, "sweep_ledger_once") as mock_regular,
    ):
        result = ingest.sweep_rum_ledger.apply(args=(SERVICE_ID,), kwargs={"lookback_hours": 6}).get()
    assert result == {"reclaimed": 1}
    mock_rum.assert_called_once_with(SERVICE_ID, lookback_hours=6)
    mock_regular.assert_not_called()


def test_sweep_tasks_default_to_a_four_hour_lookback():
    with patch.object(ingest, "sweep_ledger_once", return_value={}) as mock_regular:
        ingest.sweep_ledger.apply(args=(SERVICE_ID,)).get()
    assert mock_regular.call_args.kwargs == {"lookback_hours": 4}

    with patch.object(ingest, "sweep_rum_ledger_once", return_value={}) as mock_rum:
        ingest.sweep_rum_ledger.apply(args=(SERVICE_ID,)).get()
    assert mock_rum.call_args.kwargs == {"lookback_hours": 4}


def test_commit_batch_task_merges_lake_files():
    with patch.object(ingest, "merge_lake_files") as mock_fn:
        ingest.commit_batch.apply(args=(SERVICE_ID,)).get()
    mock_fn.assert_called_once_with(SERVICE_ID)


def test_commit_batch_task_propagates_a_merge_failure():
    """merge_lake_files raises by design so the run is recorded as failed —
    the wrapper must not swallow it into a silent success."""
    with patch.object(ingest, "merge_lake_files", side_effect=RuntimeError("attach failed")):
        result = ingest.commit_batch.apply(args=(SERVICE_ID,))
    assert result.failed()
    assert isinstance(result.result, RuntimeError)


def test_every_ledger_task_is_registered_under_its_module_path():
    """The beat schedule and the worker's queue routing both key off these
    names; a rename silently stops the task from ever being dispatched."""
    expected = {
        "backend.core.ingest.dispatch_minute": ingest.dispatch_minute,
        "backend.core.ingest.convert": ingest.convert,
        "backend.core.ingest.convert_batch_files": ingest.convert_batch_files,
        "backend.core.ingest.sweep_ledger": ingest.sweep_ledger,
        "backend.core.ingest.dispatch_rum_minute": ingest.dispatch_rum_minute,
        "backend.core.ingest.convert_rum": ingest.convert_rum,
        "backend.core.ingest.sweep_rum_ledger": ingest.sweep_rum_ledger,
        "backend.core.ingest.commit_batch": ingest.commit_batch,
    }
    for name, task in expected.items():
        assert task.name == name
        # app.tasks hands back a proxy, so identity comparison is out;
        # presence under the exact name is what the routing needs.
        assert name in ingest.app.tasks
        assert ingest.app.tasks[name].name == name
