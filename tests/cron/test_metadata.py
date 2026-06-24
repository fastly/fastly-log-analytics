"""Tests for :mod:`backend.cron.jobs.metadata`.

Pins ``_run_metadata_sync`` — the read-only-services-friendly job that
refreshes the Iceberg catalog from the cloud, syncs new data files, and
rebuilds the DuckDB view. The job is decorated by ``@cron_task`` (no
wrapped exposure here — the function isn't decorated), so we call it
directly. Tests stub the heavy iceberg + DB operations and pin the
wrapper's status / error / skip behaviour.

Other functions in this module (``_run_ngwaf_bot_sync``,
``_run_bot_data_refresh``, ``_run_rdns_enrichment``,
``_run_share_audit_purge``, ``_run_service_alerts_evaluation``) are
separate scheduler entries and out of scope here — they live in
backend.cron.jobs.metadata only by code organisation, not job grouping.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import metadata


@pytest.fixture
def stub_load_config(monkeypatch):
    cfg = {
        "service_id": "svc-1",
        "name": "svc-1",
        "provisioning": {},
    }
    load = MagicMock(return_value=cfg)
    save = MagicMock()
    monkeypatch.setattr("backend.config.load_config", load)
    monkeypatch.setattr("backend.config.save_config", save)
    return {"load": load, "save": save, "cfg": cfg}


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    src = {
        "name": "fos-test-svc",
        "service_id": "svc-1",
        "bucket": "fos-test-bkt",
        "access_level": "read_write",
    }
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    return src


@pytest.fixture
def stub_cron_envelope(monkeypatch) -> dict[str, MagicMock]:
    start = MagicMock(return_value=77)
    log = MagicMock()
    refresh = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
    monkeypatch.setattr("backend.core.duckdb.refresh_config_status", refresh)

    start_progress = MagicMock()
    end_progress = MagicMock()
    cleanup = MagicMock()
    log_event = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", cleanup)
    # _log_and_add_progress is bound at the metadata module via the
    # scheduler import — patch where the name lives.
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", log_event)

    # The DuckDB connection used for update_iceberg_view; just hand back
    # a MagicMock that supports .close() so the finally-block runs.
    fake_con = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda *a, **kw: fake_con)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    return {
        "start": start,
        "log": log,
        "refresh": refresh,
        "start_progress": start_progress,
        "end_progress": end_progress,
        "log_event": log_event,
        "fake_con": fake_con,
    }


def test_returns_when_config_missing(monkeypatch):
    monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=None))
    get_src = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
    metadata._run_metadata_sync("ghost")
    get_src.assert_not_called()


def test_returns_when_source_missing(monkeypatch, stub_load_config):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    start = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    metadata._run_metadata_sync("svc-1")
    start.assert_not_called()


def test_skips_when_start_cron_run_raises(monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
    stub_cron_envelope["start"].side_effect = RuntimeError("already running")
    init = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", init)

    metadata._run_metadata_sync("svc-1")

    init.assert_not_called()
    stub_cron_envelope["log"].assert_not_called()


def test_iceberg_table_missing_logs_success_with_skip_message(
    monkeypatch, stub_load_config, stub_source, stub_cron_envelope
):
    """A brand-new service has no Iceberg table yet — the
    ``not found`` / ``does not exist`` / ``nosuchtable`` error class
    is treated as a friendly skip, NOT a failure. Pinned because
    every cron tick for a new service would otherwise raise red in
    the admin dashboard."""
    monkeypatch.setattr(
        "backend.core.iceberg.init_iceberg_table",
        MagicMock(side_effect=ValueError("Table foo.bar not found in catalog")),
    )
    sync_data = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_data)

    metadata._run_metadata_sync("svc-1")

    sync_data.assert_not_called()
    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "success"
    assert "not found" in kwargs["summary"].lower()
    # ``end_progress`` runs once on the early-return path AND again in
    # the function-level ``finally`` block — assert it ran AT LEAST
    # once, not exactly once.
    assert stub_cron_envelope["end_progress"].called


def test_happy_path_records_files_and_rows(monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
    """``sync_data`` returns counts → log_cron_run records success
    with files_downloaded + rows_ingested, refresh_config_status fires,
    and the DuckDB view rebuild runs through get_connection."""
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock())
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 5, "rows_downloaded": 1_234}),
    )
    update_view = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", update_view)
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-1")

    update_view.assert_called_once()
    stub_cron_envelope["refresh"].assert_called_once_with("svc-1")
    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 5
    assert kwargs["rows_ingested"] == 1_234
    assert "5 new Iceberg data file" in kwargs["summary"]


def test_happy_path_with_zero_new_files_still_succeeds(monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
    """``sync_data`` returns 0 files (already up to date) → still
    status='success' with a stripped-down summary, refresh_config_status
    still runs (status row may have other fields to refresh)."""
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock())
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0}),
    )
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 0
    # Summary should NOT mention "synced N data files" when N=0.
    assert "new Iceberg data file" not in kwargs["summary"]


def test_unexpected_exception_logged_as_error_and_end_progress_fires(
    monkeypatch, stub_load_config, stub_source, stub_cron_envelope
):
    monkeypatch.setattr(
        "backend.core.iceberg.init_iceberg_table",
        MagicMock(side_effect=RuntimeError("S3 5xx storm")),
    )

    metadata._run_metadata_sync("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "error"
    assert "S3 5xx storm" in kwargs["error_message"]
    assert "Metadata sync failed" in kwargs["summary"]
    stub_cron_envelope["end_progress"].assert_called_once()


def test_manual_sync_all_clears_pinned_time_range(monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
    """Manual trigger (run_id is not None) with no start_time means
    'Sync All' — any previously pinned time_range in config must be
    cleared so the next ingest sees the full window. Pinned because a
    silent stale time_range is the failure mode operator-rerunning
    "Sync All" was trying to fix."""
    stub_load_config["cfg"]["provisioning"]["time_range"] = {"start": "2026-01-01T00:00:00Z"}
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock())
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 1, "rows_downloaded": 1}),
    )
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-1", run_id=999)

    # save_config was called and the time_range key was dropped.
    save = stub_load_config["save"]
    save.assert_called()
    saved_cfg = save.call_args[0][1]
    assert "time_range" not in saved_cfg["provisioning"]


def test_explicit_start_end_persisted_to_config_time_range(
    monkeypatch, stub_load_config, stub_source, stub_cron_envelope
):
    """Explicit ``start_time`` / ``end_time`` → config gets a
    ``time_range`` row with both, so the DuckDB view can strictly
    bound to it on the next read. Pin the contract."""
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock())
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0}),
    )
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync(
        "svc-1",
        run_id=42,
        start_time="2026-05-01T00:00:00Z",
        end_time="2026-05-08T00:00:00Z",
    )

    save = stub_load_config["save"]
    save.assert_called()
    saved_cfg = save.call_args[0][1]
    tr = saved_cfg["provisioning"]["time_range"]
    assert tr["start"] == "2026-05-01T00:00:00Z"
    assert tr["end"] == "2026-05-08T00:00:00Z"


def test_admin_state_import_failure_does_not_break_run(monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
    """``import_admin_state`` is best-effort: if it raises, log a
    warning but the whole job must still succeed and the DuckDB view
    refresh + cron_runs row must still complete. Pinned because a
    transient state-sync failure shouldn't trip the metadata-sync
    health indicator."""
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock())
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0}),
    )
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock(side_effect=RuntimeError("share_db locked")))

    metadata._run_metadata_sync("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "success"
