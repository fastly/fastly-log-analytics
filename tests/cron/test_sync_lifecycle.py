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


def _full_sweep_env(monkeypatch, events=None, raises=None, high_scale=False):
    monkeypatch.setattr("backend.config.load_config", lambda sid: {})
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: high_scale)

    def fake_ingest(**kwargs):
        yield from events or []
        if raises is not None:
            raise raises

    monkeypatch.setattr("backend.core.ingest.ingest", fake_ingest)


def test_full_sweep_lease_held_is_clean_skip(env, monkeypatch):
    _full_sweep_env(monkeypatch)
    env["start"].side_effect = RuntimeError("held")

    sync_mod._run_full_sweep.__wrapped__(SID)

    env["log"].assert_not_called()
    assert env["calls"] == []


def test_full_sweep_success_row_has_log_output_and_counters(env, monkeypatch):
    done = {
        "type": "done",
        "new_files": 3,
        "rows_inserted": 0,
        "corrupt_rows": 0,
        "deleted_files": 2,
        "outcome_counters": {"objects_failed": 0},
    }
    _full_sweep_env(monkeypatch, events=[done])

    sync_mod._run_full_sweep.__wrapped__(SID)

    env["log"].assert_called_once()
    args, kwargs = env["log"].call_args
    assert args[1] == "full_sync"
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 3
    assert kwargs["files_deleted_fos"] == 2
    assert kwargs["log_output"] == "LOGTEXT"
    assert kwargs["outcome_counters"] == {"objects_failed": 0}
    assert kwargs["run_id"] == 61
    assert "reclaimed 2 raw file(s)" in kwargs["summary"]
    assert env["calls"] == ["end_progress:61", "duration"]


def test_full_sweep_error_event_records_error_row_and_stops(env, monkeypatch):
    events = [
        {"type": "file_done", "current": 4, "total_inserted": 40, "total_corrupt": 1},
        {"type": "error", "message": "s3 down", "outcome_counters": {"objects_failed": 1}},
        {"type": "done", "new_files": 9},
    ]
    _full_sweep_env(monkeypatch, events=events)

    sync_mod._run_full_sweep.__wrapped__(SID)

    env["log"].assert_called_once()
    args, kwargs = env["log"].call_args
    assert args[3] == "error"
    assert kwargs["summary"] == "Full-sweep failed"
    assert kwargs["error_message"] == "s3 down"
    assert (kwargs["files_downloaded"], kwargs["rows_ingested"], kwargs["corrupt_rows"]) == (4, 40, 1)
    assert kwargs["outcome_counters"] == {"objects_failed": 1}
    assert kwargs["log_output"] == "LOGTEXT"
    assert env["calls"][-2:] == ["end_progress:61", "duration"]


def test_full_sweep_crash_row_carries_partial_counters(env, monkeypatch):
    events = [{"type": "file_done", "current": 2, "total_inserted": 7, "total_corrupt": 0}]
    _full_sweep_env(monkeypatch, events=events, raises=RuntimeError("kaboom"))

    sync_mod._run_full_sweep.__wrapped__(SID)

    env["log"].assert_called_once()
    args, kwargs = env["log"].call_args
    assert args[3] == "error"
    assert kwargs["summary"] == "Full-sweep crashed"
    assert kwargs["error_message"] == "kaboom"
    assert (kwargs["files_downloaded"], kwargs["rows_ingested"], kwargs["corrupt_rows"]) == (2, 7, 0)
    assert kwargs["log_output"] == "LOGTEXT"


def test_full_sweep_high_scale_rows_have_no_log_output(env, monkeypatch):
    _full_sweep_env(monkeypatch, high_scale=True)
    discover = MagicMock(return_value=5)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)

    sync_mod._run_full_sweep.__wrapped__(SID)

    discover.assert_called_once_with(SID, run_id=61, originating_task="full_sync")
    env["log"].assert_called_once()
    args, kwargs = env["log"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 5
    assert kwargs["log_output"] is None
    assert kwargs["summary"] == "Full-prefix sweep discovered 5 unseen file(s); dispatched to ingest workers"


def test_full_sweep_high_scale_failure_row(env, monkeypatch):
    _full_sweep_env(monkeypatch, high_scale=True)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", MagicMock(side_effect=RuntimeError("list failed")))

    sync_mod._run_full_sweep.__wrapped__(SID)

    env["log"].assert_called_once()
    args, kwargs = env["log"].call_args
    assert args[3] == "error"
    assert kwargs["summary"] == "Full-prefix ledger sweep failed"
    assert kwargs["error_message"] == "list failed"
    assert kwargs["log_output"] is None
