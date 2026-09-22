"""Tests for ``backend.cron.jobs.rum_ledger._run_rum_ledger_sweep``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import rum_ledger as rum_ledger_mod

SERVICE_ID = "svc_test_rum_sweep"

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


def test_rum_ledger_sweep_success(monkeypatch):
    """Under high-throughput mode:
    - Runs sweep_rum_ledger_once
    - Records status 'success' with reclaim/redispatch/discovery counts
    - Starts and ends progress cleanly
    """
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 777)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)

    sweep_result = {
        "reclaimed": 3,
        "redispatched": 3,
        "discovered": 5,
        "broker_ok": True,
        "dead_letter": 0,
    }
    monkeypatch.setattr("backend.core.ingest.sweep_rum_ledger_once", lambda sid: sweep_result)

    start_progress = MagicMock()
    end_progress = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(SERVICE_ID)

    start_progress.assert_called_once_with(777, service_id=SERVICE_ID, task="ledger_rum_sweep")
    end_progress.assert_called_once_with(777)

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert kwargs.get("run_id") == 777
    assert args[3] == "success"
    assert kwargs.get("files_downloaded") == 5
    assert "reclaimed=3 redispatched=3 discovered=5" in kwargs.get("summary", "")


def test_rum_ledger_sweep_warning_on_dead_letter_or_broker_down(monkeypatch):
    """When dead-letter RUM rows exist or broker probe fails:
    - Status is recorded as 'warning'
    - Specific warning messages appear in summary and error_message
    """
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 778)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)

    sweep_result = {
        "reclaimed": 1,
        "redispatched": 0,
        "discovered": 0,
        "broker_ok": False,
        "dead_letter": 4,
    }
    monkeypatch.setattr("backend.core.ingest.sweep_rum_ledger_once", lambda sid: sweep_result)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(SERVICE_ID)

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert "Celery broker/queue depth probe failed" in kwargs.get("summary", "")
    assert "4 dead-letter/quarantined RUM row(s)" in kwargs.get("summary", "")
    assert "Celery broker/queue depth probe failed; 4 dead-letter/quarantined RUM row(s)" in kwargs.get(
        "error_message", ""
    )


def test_rum_ledger_sweep_skips_in_standard_mode(monkeypatch):
    """When the service is configured in standard (synchronous) mode:
    - Job returns immediately without starting a cron run
    """
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: False)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0


def test_rum_ledger_sweep_skips_when_read_only(monkeypatch):
    """When the service is read_only:
    - Job returns immediately without starting a cron run
    """
    start_calls = []
    ro_src = dict(FAKE_SRC, access_level="read_only")

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0


def test_rum_ledger_sweep_refuses_when_dev_no_crons(monkeypatch):
    """When FLA_DEV_NO_CRONS=1:
    - Job returns immediately
    """
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    start_calls = []
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0
