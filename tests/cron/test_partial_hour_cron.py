from unittest.mock import patch


def test_run_partial_hour_merge_calls_merge_and_marks_ready_on_success():
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

    fake_src = {"service_id": "svc-a", "name": "svc-a"}

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
    mock_mark.assert_called_once_with("svc-a")
    mock_log.assert_called_once()
    assert mock_log.call_args.kwargs.get("status") == "success" or mock_log.call_args[0][3] == "success"


def test_run_partial_hour_merge_defers_when_requests_in_flight():
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=True),
        patch("backend.core.duckdb.start_cron_run") as mock_start,
    ):
        _run_partial_hour_merge("svc-a")

    mock_start.assert_not_called()
