"""cron_run adoption in the RUM ledger jobs: lease, finalize ordering, error rows."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs import rum_ledger

SID = "svc-rum-lc"
CFG = {"service_id": SID, "name": "n", "rum": {"enabled": True}}
SRC = {"service_id": SID, "name": "n", "access_level": "read_write"}


@pytest.fixture
def env(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    calls: list[str] = []
    mocks = {
        "calls": calls,
        "start": MagicMock(return_value=41),
        "log": MagicMock(),
    }
    monkeypatch.setattr("backend.config.load_config", lambda sid: CFG)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://x")
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", mocks["start"])
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", mocks["log"])
    monkeypatch.setattr(
        "backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: calls.append(f"backstop:{a[1]}:{a[2]}")
    )
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda rid: calls.append(f"end_progress:{rid}"))
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "SHOULD-NOT-BE-ATTACHED")
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", lambda *a, **k: calls.append("duration"))
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    return mocks


def test_discovery_lease_held_is_clean_skip(env):
    env["start"].side_effect = CronLeaseHeldError("held")

    rum_ledger._run_rum_discovery_cron.__wrapped__(SID)

    env["log"].assert_not_called()
    assert env["calls"] == []


def test_discovery_failure_row_and_backstop_runs_before_end_progress(env, monkeypatch):
    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", MagicMock(side_effect=RuntimeError("list failed")))

    rum_ledger._run_rum_discovery_cron.__wrapped__(SID)

    env["log"].assert_called_once()
    args, kwargs = env["log"].call_args
    assert args[1] == "rum_discovery"
    assert args[3] == "error"
    assert kwargs["summary"] == "RUM discovery failed"
    assert kwargs["error_message"] == "list failed"
    assert kwargs["run_id"] == 41
    assert kwargs["log_output"] is None
    assert env["calls"] == ["backstop:rum_discovery:41", "end_progress:41"]


def test_discovery_success_row_counts_files(env, monkeypatch):
    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", MagicMock(return_value=2))

    rum_ledger._run_rum_discovery_cron.__wrapped__(SID)

    args, kwargs = env["log"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 10
    assert kwargs["summary"] == "Discovered 10 new RUM file(s); dispatched to ingest workers"
    assert kwargs["error_message"] is None


def test_sweep_lease_held_is_clean_skip(env):
    env["start"].side_effect = CronLeaseHeldError("held")

    rum_ledger._run_rum_ledger_sweep.__wrapped__(SID)

    env["log"].assert_not_called()
    assert env["calls"] == []


def test_sweep_failure_row_and_finalize_order(env, monkeypatch):
    monkeypatch.setattr("backend.core.ingest.sweep_rum_ledger_once", MagicMock(side_effect=RuntimeError("sweep broke")))

    rum_ledger._run_rum_ledger_sweep.__wrapped__(SID)

    env["log"].assert_called_once()
    args, kwargs = env["log"].call_args
    assert args[1] == "ledger_rum_sweep"
    assert args[3] == "error"
    assert kwargs["summary"] == "RUM ledger sweep failed"
    assert kwargs["error_message"] == "sweep broke"
    assert kwargs["log_output"] is None
    assert env["calls"] == ["backstop:ledger_rum_sweep:41", "end_progress:41", "duration"]
