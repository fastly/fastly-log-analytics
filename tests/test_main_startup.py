from unittest.mock import patch

from backend.core import rollup_readiness as rr


def test_initialize_service_marks_coverage_ready_for_durable_service():
    rr.reset_rollup_coverage_ready()
    cfg = {"service_id": "svc-durable"}
    fake_src = {"service_id": "svc-durable", "deployment_mode": "high_throughput"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.refresh_config_status"),
        patch("backend.main._ensure_persistent_view"),
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch(
            "backend.core.rollups.recompute.backfill_missing_hour_bundles",
            return_value={"missing": 0, "bundled": 0},
        ) as mock_backfill,
    ):
        from backend.main import _initialize_service

        _initialize_service(cfg)

    mock_backfill.assert_called_once_with("svc-durable", fake_src, lookback_days=30)
    assert rr.rollup_coverage_ready("svc-durable") is True


def test_initialize_service_skips_catchup_for_file_mode_service():
    rr.reset_rollup_coverage_ready()
    cfg = {"service_id": "svc-file"}
    fake_src = {"service_id": "svc-file", "deployment_mode": "standard"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.refresh_config_status"),
        patch("backend.main._ensure_persistent_view"),
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.config.is_durable_serving_mode", return_value=False),
        patch("backend.core.rollups.recompute.backfill_missing_hour_bundles") as mock_backfill,
    ):
        from backend.main import _initialize_service

        _initialize_service(cfg)

    mock_backfill.assert_not_called()
    # File mode never needed the flag, but it should read False rather than
    # crash — nothing in the file-mode gate checks it (Task 3 only adds the
    # `and not rollup_coverage_ready(...)` clause behind `is_durable_serving_mode`,
    # so this is short-circuited before the flag is ever read in production
    # code — this assertion just documents the flag stays untouched here).
    assert rr.rollup_coverage_ready("svc-file") is False
