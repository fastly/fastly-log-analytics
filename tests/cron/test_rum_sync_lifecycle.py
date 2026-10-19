"""``cron_run`` lifecycle contract for the rum_sync job wrapper.

The wrapper owns the lease + progress lifecycle; ``ingest_rum_logs`` reuses the leased
run id and still writes its own terminal ``cron_runs`` rows.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs import rum_sync as rum_sync_mod

SERVICE_ID = "svc_test"


@pytest.fixture
def rum_lease(monkeypatch):
    lease = MagicMock()
    lease.source = {"name": SERVICE_ID}
    lease.start = MagicMock(return_value=7)
    lease.log_cron_run = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: lease.source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lease.start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lease.log_cron_run)
    return lease


@pytest.fixture
def progress(monkeypatch) -> dict[str, MagicMock]:
    mocks = {
        name: MagicMock() for name in ("start_progress", "add_progress", "end_progress", "cleanup_progress_and_reap")
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(f"backend.cron_progress.{name}", mock)
    return mocks


def test_acquires_lease_once_and_hands_run_id_to_ingest(monkeypatch, rum_lease, progress):
    monkeypatch.setattr(rum_sync_mod, "_reconcile_faro_bundle", lambda sid, rid: True)
    seen: dict[str, Any] = {}

    def fake_ingest(service_id, *args, **kwargs):
        seen.update(kwargs)
        yield ("started", kwargs["run_id"])
        yield ("done", 0)

    monkeypatch.setattr(rum_sync_mod, "ingest_rum_logs", fake_ingest)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)

    rum_lease.start.assert_called_once_with(rum_lease.source, "rum_sync")
    assert seen["run_id"] == 7
    progress["start_progress"].assert_called_once_with(7, service_id=SERVICE_ID, task="rum_sync")
    progress["end_progress"].assert_called_once_with(7)
    progress["cleanup_progress_and_reap"].assert_called_once()


def test_reuses_externally_owned_run_id(monkeypatch, rum_lease, progress):
    monkeypatch.setattr(rum_sync_mod, "_reconcile_faro_bundle", lambda sid, rid: True)
    seen: dict[str, Any] = {}

    def fake_ingest(service_id, *args, **kwargs):
        seen.update(kwargs)
        yield ("done", 0)

    monkeypatch.setattr(rum_sync_mod, "ingest_rum_logs", fake_ingest)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID, run_id=99)

    rum_lease.start.assert_not_called()
    assert seen["run_id"] == 99
    assert seen["max_seconds"] == 240


def test_skips_cleanly_when_lease_already_held(monkeypatch, rum_lease, progress):
    rum_lease.start.side_effect = CronLeaseHeldError("rum_sync is already running")
    ingest = MagicMock()
    monkeypatch.setattr(rum_sync_mod, "ingest_rum_logs", ingest)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)

    ingest.assert_not_called()
    rum_lease.log_cron_run.assert_not_called()
    progress["start_progress"].assert_not_called()
    progress["end_progress"].assert_not_called()


def test_unknown_service_is_a_clean_skip(monkeypatch, rum_lease, progress):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    ingest = MagicMock()
    monkeypatch.setattr(rum_sync_mod, "ingest_rum_logs", ingest)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)

    ingest.assert_not_called()
    rum_lease.start.assert_not_called()


def test_tears_down_progress_when_ingest_raises_midstream(monkeypatch, rum_lease, progress):
    monkeypatch.setattr(rum_sync_mod, "_reconcile_faro_bundle", lambda sid, rid: True)

    def exploding_ingest(service_id, *args, **kwargs):
        yield ("started", kwargs["run_id"])
        raise ValueError("boom")

    monkeypatch.setattr(rum_sync_mod, "ingest_rum_logs", exploding_ingest)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)  # must not raise

    progress["end_progress"].assert_called_once_with(7)
    progress["cleanup_progress_and_reap"].assert_called_once()
    errors = [c.args[1]["message"] for c in progress["add_progress"].call_args_list if c.args[1]["type"] == "error"]
    assert any("RUM sync failed: boom" in m for m in errors)
