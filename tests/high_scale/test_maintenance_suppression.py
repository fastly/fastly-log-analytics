from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from backend.core.clickhouse_schema import maintain_clickhouse_system_tables
from backend.high_scale.worker import HighScaleWorkerLoop


def test_maintain_clickhouse_system_tables_suppresses_http_413() -> None:
    client = MagicMock()
    # Simulate HTTP 413 Request Entity Too Large error on execute
    client.execute.side_effect = RuntimeError("ClickHouse HTTP 413: Request Entity Too Large")

    # Must not raise an exception
    maintain_clickhouse_system_tables(client)
    assert client.execute.called


def test_worker_run_once_continues_when_maintenance_fails(caplog: pytest.LogCaptureFixture) -> None:
    coordinator = MagicMock()
    coordinator.run_page.return_value = MagicMock()

    worker = HighScaleWorkerLoop(
        coordinator=coordinator,
        service_ids=("svc_test",),
        domains=("request",),
        interval_seconds=0.0,
    )

    # Force system cleanup interval
    worker._last_system_cleanup = 0.0

    mock_client = MagicMock()
    mock_client.execute.side_effect = RuntimeError("HTTP 413")

    with (
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=mock_client),
        patch("backend.utils.usage_logger.run_usage_log_cleanup", return_value=None),
        caplog.at_level(logging.DEBUG),
    ):
        results = worker.run_once()

    assert len(results) == 1
    assert results[0].error is None
    assert coordinator.run_page.called
