from __future__ import annotations

from unittest.mock import MagicMock

from backend.cron.lifecycle import record_skip_row

SRC = {"name": "svc_test"}


def test_defaults_to_a_skipped_row_with_zero_duration(monkeypatch):
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)

    record_skip_row(SRC, "alerts", "No alerts configured")

    log.assert_called_once_with(
        SRC, "alerts", 0.0, "skipped", summary="No alerts configured", error_message=None, run_id=None
    )


def test_forwards_status_duration_error_and_run_id(monkeypatch):
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)

    record_skip_row(
        SRC,
        "rollup_hour_heal",
        "Skipped due to low disk space",
        status="warning",
        duration_s=1.5,
        error_message="Disk space critically low",
        run_id=42,
    )

    log.assert_called_once_with(
        SRC,
        "rollup_hour_heal",
        1.5,
        "warning",
        summary="Skipped due to low disk space",
        error_message="Disk space critically low",
        run_id=42,
    )
