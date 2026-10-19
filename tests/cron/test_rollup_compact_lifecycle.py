"""cron_run adoption in rollup_compact: lease name, progress label, cron_runs rows."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs import rollup_compact

SID = "svc-rc-1"


@pytest.fixture
def env(monkeypatch, tmp_path):
    src = {"name": "rc-svc", "service_id": SID, "_cache_dir_override": str(tmp_path), "cache_dir": str(tmp_path)}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **k: False)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: str(tmp_path))
    mocks = {
        "start": MagicMock(return_value=21),
        "log": MagicMock(),
        "start_progress": MagicMock(),
        "end_progress": MagicMock(),
    }
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", mocks["start"])
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", mocks["log"])
    monkeypatch.setattr("backend.cron_progress.start_progress", mocks["start_progress"])
    monkeypatch.setattr("backend.cron_progress.end_progress", mocks["end_progress"])
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "SHOULD-NOT-BE-ATTACHED")
    for name in ("backfill_day_bundles", "compact_closed_days_to_daily"):
        monkeypatch.setattr(f"backend.core.rollups.{name}", lambda *a, **k: 2)
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **k: {})
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **k: {})
    monkeypatch.setattr("backend.core.rollups.retire_compacted_hour_bundles", lambda *a, **k: (3, 2048))
    monkeypatch.setattr("backend.core.rollups.recompute.cleanup_old_rollups", lambda *a, **k: None)
    return mocks


def test_success_row_and_lease_use_daily_task_name_progress_uses_short_label(env):
    res = rollup_compact._run_rollup_compact_daily.__wrapped__(SID, manual=True)

    assert res["status"] == "success"
    assert (res["rebuilt"], res["bundled"], res["retired_files"]) == (2, 2, 3)
    assert env["start"].call_args.args[1] == "rollup_compact_daily"
    assert env["start_progress"].call_args.args == (21,)
    assert env["start_progress"].call_args.kwargs == {"service_id": SID, "task": "rollup_compact"}

    env["log"].assert_called_once()
    args, kwargs = env["log"].call_args
    assert args[1] == "rollup_compact_daily"
    assert args[3] == "success"
    assert kwargs["run_id"] == 21
    assert kwargs["log_output"] is None
    assert kwargs["error_message"] is None
    assert "Rebuilt 2" in kwargs["summary"]
    env["end_progress"].assert_called_once_with(21)


def test_lease_held_is_clean_skip_with_no_row(env):
    env["start"].side_effect = CronLeaseHeldError("lease held")

    res = rollup_compact._run_rollup_compact_daily.__wrapped__(SID, manual=True)

    assert res == {"status": "skipped", "service_id": SID, "summary": "lease held"}
    env["log"].assert_not_called()
    env["end_progress"].assert_not_called()


def test_provided_run_id_skips_lease(env):
    rollup_compact._run_rollup_compact_daily.__wrapped__(SID, manual=True, run_id=99)

    env["start"].assert_not_called()
    assert env["log"].call_args.kwargs["run_id"] == 99


def test_unguarded_step_failure_records_error_row_and_returns_error_dict(env, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("daily compaction exploded")

    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", boom)

    res = rollup_compact._run_rollup_compact_daily.__wrapped__(SID, manual=True)

    assert res == {
        "status": "error",
        "service_id": SID,
        "summary": "daily compaction exploded",
        "errors": ["daily compaction exploded"],
    }
    args, kwargs = env["log"].call_args
    assert args[3] == "error"
    assert kwargs["error_message"] == "daily compaction exploded"
    assert kwargs["run_id"] == 21
    env["end_progress"].assert_called_once_with(21)
