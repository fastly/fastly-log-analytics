"""cron_run adoption in the sync.py jobs: identical cron_runs rows and lease behaviour."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import sync as sync_mod
from backend.routers.admin import SustainedLossAlert

SID = "svc-sync-lc"
SRC = {"service_id": SID, "name": SID, "access_level": "read_write", "bucket": "b"}


@pytest.fixture
def env(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    calls: list[str] = []
    mocks = {
        "calls": calls,
        "start": MagicMock(return_value=61),
        "log": MagicMock(),
    }
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", mocks["start"])
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", mocks["log"])
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **k: False)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda rid: calls.append(f"end_progress:{rid}"))
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.sync._log_and_add_progress", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "LOGTEXT")
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", lambda *a, **k: calls.append("duration"))
    sync_mod._GAP_HEAL_LAST_TRIGGER.clear()
    return mocks


def _sustained() -> SustainedLossAlert:
    return SustainedLossAlert(started_at="2026-10-08T10:00:00Z", n_buckets=3, max_gap_pct=0.15, total_lost_lines=12_000)


def test_gap_heal_lease_held_is_clean_skip(env):
    env["start"].side_effect = RuntimeError("held")

    sync_mod._run_gap_heal.__wrapped__(SID)

    env["log"].assert_not_called()
    assert env["calls"] == []


def test_gap_heal_no_loss_row_keeps_log_output_and_finalizes(env, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.admin.compute_log_accounting",
        lambda *a, **k: {"sustained_loss": None, "buckets": [], "totals": None},
    )

    sync_mod._run_gap_heal.__wrapped__(SID)

    env["log"].assert_called_once()
    args, kwargs = env["log"].call_args
    assert args[1] == "gap_heal"
    assert args[3] == "success"
    assert kwargs["summary"] == "No sustained loss detected"
    assert kwargs["run_id"] == 61
    assert kwargs["log_output"] == "LOGTEXT"
    assert env["calls"] == ["end_progress:61", "duration"]


def test_gap_heal_sweep_crash_after_warning_row_overwrites_with_error_row(env, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.admin.compute_log_accounting",
        lambda *a, **k: {"sustained_loss": _sustained(), "buckets": [], "totals": None},
    )
    monkeypatch.setattr("backend.cron.jobs.sync._run_full_sweep", MagicMock(side_effect=RuntimeError("sweep died")))

    sync_mod._run_gap_heal.__wrapped__(SID, force=True)

    statuses = [c.args[3] for c in env["log"].call_args_list]
    assert statuses == ["warning", "error"]
    _, kwargs = env["log"].call_args
    assert kwargs["summary"] == "Gap-heal evaluation crashed"
    assert kwargs["error_message"] == "sweep died"
    assert kwargs["log_output"] == "LOGTEXT"
    assert env["calls"] == ["end_progress:61", "duration"]
