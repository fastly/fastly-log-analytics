"""cron_run adoption in rollup_hour_heal and rum_commit: lease, progress, cron_runs rows, backstop."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import compaction, rum_commit

SID = "svc-hh-1"


@pytest.fixture
def env(monkeypatch, tmp_path):
    src = {
        "name": "hh-svc",
        "service_id": SID,
        "access_level": "read_write",
        "cache_dir": str(tmp_path),
        "_cache_dir_override": str(tmp_path),
    }
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **k: False)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: str(tmp_path))
    mocks = {
        "start": MagicMock(return_value=31),
        "log": MagicMock(),
        "start_progress": MagicMock(),
        "end_progress": MagicMock(),
        "finalize": MagicMock(),
    }
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", mocks["start"])
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", mocks["log"])
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", mocks["finalize"])
    monkeypatch.setattr("backend.cron_progress.start_progress", mocks["start_progress"])
    monkeypatch.setattr("backend.cron_progress.end_progress", mocks["end_progress"])
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda *a, **k: (True, ""))
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"provisioning": {"cron_sync": {}}})
    monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: False)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles",
        lambda *a, **k: {"missing": 2, "rebuilt_fields": 3, "bundled": 2, "stamped_empty": 0},
    )
    monkeypatch.setattr("backend.core.rollups._common._hour_bundled_root", lambda s: str(tmp_path / "hours"))
    return mocks


# ── rollup_hour_heal ────────────────────────────────────────────────────────


def test_heal_success_row_and_progress_label(env):
    res = compaction._run_rollup_hour_heal.__wrapped__(SID, manual=True)

    assert res["status"] == "success"
    assert (res["missing"], res["rebuilt_fields"], res["bundled"]) == (2, 3, 2)
    assert res["run_id"] == 31
    assert env["start"].call_args.args[1] == "rollup_hour_heal"
    assert env["start_progress"].call_args.kwargs == {"service_id": SID, "task": "rollup_hour_heal"}
    args, kwargs = env["log"].call_args
    assert args[1] == "rollup_hour_heal"
    assert args[3] == "success"
    assert kwargs["run_id"] == 31
    assert "Healed 2 missing hour(s)" in kwargs["summary"]
    env["end_progress"].assert_called_once_with(31)


def test_heal_lease_held_is_clean_skip_with_no_row(env):
    env["start"].side_effect = RuntimeError("lease held")

    res = compaction._run_rollup_hour_heal.__wrapped__(SID, manual=True)

    assert res == {"status": "skipped", "service_id": SID, "summary": "lease held"}
    env["log"].assert_not_called()
    env["end_progress"].assert_not_called()


def test_heal_provided_run_id_skips_lease(env):
    compaction._run_rollup_hour_heal.__wrapped__(SID, manual=True, run_id=77)

    env["start"].assert_not_called()
    assert env["log"].call_args.kwargs["run_id"] == 77


def test_heal_failure_records_error_row_and_returns_error_dict(env, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("heal exploded")

    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", boom)

    res = compaction._run_rollup_hour_heal.__wrapped__(SID, manual=True)

    assert res["status"] == "error"
    assert res["error_message"] == "heal exploded"
    assert res["summary"] == "hour-bundle self-heal failed"
    assert res["run_id"] == 31
    args, kwargs = env["log"].call_args
    assert args[3] == "error"
    assert kwargs["error_message"] == "heal exploded"
    env["end_progress"].assert_called_once_with(31)


# ── rum_commit ──────────────────────────────────────────────────────────────


def _stub_commit(monkeypatch, result=None):
    result = result or {"files_committed": 1, "rows_committed": 5}
    monkeypatch.setattr("backend.core.iceberg.commit_buffer", lambda *a, **k: dict(result))
    monkeypatch.setattr("backend.core.iceberg.sync_data", lambda *a, **k: None)
    monkeypatch.setattr("backend.core.ingest._mark_ledger_published", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.core.duckdb.rum_source_for", lambda s: (_ for _ in ()).throw(RuntimeError("skip aggs"))
    )
    monkeypatch.setattr("backend.core.local_compaction.compact_local_partitions", lambda *a, **k: {})
    monkeypatch.setattr("backend.sync_status_snapshot.compute_sync_status_cached", lambda sid: None)


def test_rum_commit_success_row_backstop_and_progress(env, monkeypatch):
    _stub_commit(monkeypatch)

    rum_commit._run_rum_commit.__wrapped__(SID, force=True)

    assert env["start"].call_args.args[1] == "rum_commit"
    assert env["start_progress"].call_args.kwargs == {"service_id": SID, "task": "rum_commit"}
    args, kwargs = env["log"].call_args
    assert args[1] == "rum_commit"
    assert args[3] == "success"
    assert kwargs["rows_ingested"] == 10
    assert "Committed 1 vitals files" in kwargs["summary"]
    env["end_progress"].assert_called_once_with(31)
    env["finalize"].assert_called_once()
    assert env["finalize"].call_args.args[1:] == ("rum_commit", 31)


def test_rum_commit_lease_held_is_clean_skip_with_no_row(env, monkeypatch):
    _stub_commit(monkeypatch)
    env["start"].side_effect = RuntimeError("lease held")

    rum_commit._run_rum_commit.__wrapped__(SID, force=True)

    env["log"].assert_not_called()
    env["end_progress"].assert_not_called()


def test_rum_commit_disk_guard_records_abort_row_and_still_finalizes(env, monkeypatch):
    _stub_commit(monkeypatch)
    monkeypatch.setattr(
        "backend.cron.scheduler._check_disk_space", lambda *a, **k: (False, "Disk free space is below 500MB")
    )

    rum_commit._run_rum_commit.__wrapped__(SID, force=True)

    args, kwargs = env["log"].call_args
    assert args[3] == "error"
    assert kwargs["error_message"] == "Disk free space is below 500MB"
    assert "RUM commit aborted" in kwargs["summary"]
    env["end_progress"].assert_called_once_with(31)
    env["finalize"].assert_called_once()


def test_rum_commit_unexpected_failure_records_error_and_reraises(env, monkeypatch):
    _stub_commit(monkeypatch)
    monkeypatch.setattr(
        "backend.core.ingest._mark_ledger_published", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(RuntimeError, match="boom"):
        rum_commit._run_rum_commit.__wrapped__(SID, force=True)

    args, kwargs = env["log"].call_args
    assert args[3] == "error"
    assert kwargs["error_message"] == "boom"
    env["end_progress"].assert_called_once_with(31)
    env["finalize"].assert_called_once()
