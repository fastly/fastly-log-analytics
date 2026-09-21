"""Tests for ``backend.cron.jobs.rum_ledger._run_rum_discovery_cron``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import rum_ledger as rum_ledger_mod

SERVICE_ID = "svc_test_rum_disc"

FAKE_CFG = {
    "service_id": SERVICE_ID,
    "name": "Test Service",
    "deployment_mode": "high_throughput",
    "rum": {"enabled": True},
}

FAKE_SRC = {
    "service_id": SERVICE_ID,
    "name": "Test Service",
    "access_level": "read_write",
    "deployment_mode": "high_throughput",
}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)


def test_rum_discovery_success(monkeypatch):
    """Under high-throughput mode:
    - Reconciles Faro bundle
    - Scans 5-minute sliding window via discover_rum_prefix
    - Records status 'success' with discovered file counts
    - Starts and ends progress cleanly
    """
    log_calls = []
    discovered_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://localhost:6379/0")
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 555)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)

    def mock_discover_rum_prefix(sid, prefix_subpath=None):
        discovered_calls.append(prefix_subpath)
        return 2  # 2 files per minute slice

    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", mock_discover_rum_prefix)

    start_progress = MagicMock()
    end_progress = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)

    start_progress.assert_called_once_with(555, service_id=SERVICE_ID, task="rum_discovery")
    end_progress.assert_called_once_with(555)
    assert len(discovered_calls) == 5  # 5 minute slices

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert kwargs.get("run_id") == 555
    assert args[3] == "success"
    assert kwargs.get("files_downloaded") == 10
    assert "Discovered 10 new RUM file(s)" in kwargs.get("summary", "")


def test_rum_discovery_faro_failure_records_warning(monkeypatch):
    """When Faro bundle reconciliation returns False or raises:
    - Discovery still proceeds to discover RUM beacons
    - Cron run status is marked 'warning'
    - Warning reason surfaces in error_message and summary
    """
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://localhost:6379/0")
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 556)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.cron.jobs.rum_sync._reconcile_faro_bundle",
        MagicMock(side_effect=RuntimeError("FOS bundle upload timeout")),
    )
    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", lambda sid, prefix_subpath=None: 1)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert kwargs.get("files_downloaded") == 5
    assert "Faro bundle reconcile issue" in kwargs.get("summary", "")
    assert "FOS bundle upload timeout" in kwargs.get("error_message", "")


def test_rum_discovery_skips_in_standard_mode(monkeypatch):
    """When the service is configured in standard (synchronous) mode:
    - Job returns immediately without starting a cron run
    """
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: False)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0


def test_rum_discovery_missing_broker_error(monkeypatch):
    """When CELERY_BROKER_URL is missing in high-throughput mode:
    - Status is recorded as 'error'
    - Progress is ended cleanly
    """
    log_calls = []
    end_progress = MagicMock()

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 557)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "error"
    assert "no broker URL" in kwargs.get("summary", "")
    end_progress.assert_called_once_with(557)
