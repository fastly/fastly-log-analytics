from unittest.mock import patch


def test_run_partial_hour_merge_calls_merge_and_does_not_mark_ready(monkeypatch):
    """C2 (final whole-branch review): a 30s tick — even a successful one —
    only proves the writer path is alive, not that this pod's local
    closed-hour rollup tree reflects a genuine backfill pass. That flag is
    now only raised by main.py's startup catch-up and by
    _run_rollup_hour_heal's success path."""
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country", "method"])

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.start_cron_run", return_value="run-1"),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.rollups.partial_hour.merge_partial_hour",
            return_value={"hour": "2026-09-09-14", "new_files": 3, "duration_ms": 12.5},
        ) as mock_merge,
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
        patch("backend.core.rollup_readiness.mark_rollup_coverage_ready") as mock_mark,
    ):
        _run_partial_hour_merge("svc-a")

    mock_merge.assert_called_once()
    mock_mark.assert_not_called()
    mock_log.assert_called_once()
    assert mock_log.call_args.kwargs.get("status") == "success" or mock_log.call_args[0][3] == "success"


def test_run_partial_hour_merge_filters_out_live_topn_skip_fields(monkeypatch):
    """M1 (final whole-branch review): the cron should aggregate only the
    fields execute_top_n_rollups's own live top-up would bother with, not
    the full FIELDS list (which includes high-cardinality identifier/raw
    columns with no top-N panel — see _LIVE_TOPN_SKIP_FIELDS)."""
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge
    from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    skip_field = next(iter(_LIVE_TOPN_SKIP_FIELDS))
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country", skip_field])

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.start_cron_run", return_value="run-1"),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.rollups.partial_hour.merge_partial_hour",
            return_value={"hour": "2026-09-09-14", "new_files": 1, "duration_ms": 1.0},
        ) as mock_merge,
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
    ):
        _run_partial_hour_merge("svc-a")

    merged_fields = mock_merge.call_args[0][2]
    assert "country" in merged_fields
    assert skip_field not in merged_fields


def test_run_partial_hour_merge_keeps_cron_runs_row_for_noop_tick(monkeypatch):
    """A no-op tick still records a successful heartbeat for the Cron UI."""
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country"])

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.start_cron_run", return_value="run-1"),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.rollups.partial_hour.merge_partial_hour",
            return_value={"hour": "2026-09-09-14", "new_files": 0, "duration_ms": 1.0},
        ),
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
        patch("backend.core.metadata.delete_cron_run") as mock_delete,
    ):
        _run_partial_hour_merge("svc-a")

    mock_log.assert_called_once()
    assert mock_log.call_args.kwargs.get("status") == "success" or mock_log.call_args[0][3] == "success"
    mock_delete.assert_not_called()


def test_run_partial_hour_merge_keeps_cron_runs_row_when_files_merged(monkeypatch):
    """Sibling to the no-op test above: a real tick's row must NOT be deleted."""
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country"])

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.start_cron_run", return_value="run-1"),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.rollups.partial_hour.merge_partial_hour",
            return_value={"hour": "2026-09-09-14", "new_files": 2, "duration_ms": 1.0},
        ),
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
        patch("backend.core.metadata.delete_cron_run") as mock_delete,
    ):
        _run_partial_hour_merge("svc-a")

    mock_delete.assert_not_called()


def test_run_partial_hour_merge_defers_when_requests_in_flight():
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=True),
        patch("backend.core.duckdb.start_cron_run") as mock_start,
    ):
        _run_partial_hour_merge("svc-a")

    mock_start.assert_not_called()
