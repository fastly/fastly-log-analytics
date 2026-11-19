"""Unit, snapshot, and contract tests for admin job snapshots and service resolution."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

from backend.cron.scheduler import lifecycle
from backend.models.admin.health_and_sync import SystemJobsResponse
from backend.routers.admin import _helpers as admin_helpers


def test_attach_debug_db_snapshot_toggles_with_debug_responses(monkeypatch) -> None:
    sample_queries = [{"seq": 1, "sql": "SELECT 1", "time_ms": 0.4}]

    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: False)
    payload_off = {"series": {"a": 1}}
    out_off = admin_helpers.attach_debug_db_snapshot(payload_off)
    assert out_off is payload_off
    assert out_off == {"series": {"a": 1}}

    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: True)
    monkeypatch.setattr("backend.utils.telemetry.get_sqlite_queries", lambda: sample_queries)
    payload_on = {"series": {"a": 1}}
    out_on = admin_helpers.attach_debug_db_snapshot(payload_on)
    assert out_on == {
        "series": {"a": 1},
        "_debug_sqlite": sample_queries,
        "_debug_postgres": sample_queries,
    }
    # Returned lists are independent copies of the underlying telemetry list.
    assert out_on["_debug_sqlite"] is not sample_queries


def test_get_system_jobs_snapshot_and_contract(monkeypatch) -> None:
    saved_status = dict(lifecycle._status)
    lifecycle._status.clear()
    try:
        lifecycle.record_job_run("bot_data_refresh", "success", 1.234, "Updated 3 source(s)")

        fake_jobs = {
            "bot_data_refresh": SimpleNamespace(next_run_time=datetime(2026, 11, 18, 13, 0, 0, tzinfo=UTC)),
            "rdns_enrichment": None,
        }

        class _FakeScheduler:
            def get_job(self, job_id: str):
                if job_id == "share_audit_purge":
                    raise RuntimeError("scheduler lookup error")
                return fake_jobs.get(job_id)

        with patch("backend.cron.scheduler.runner.get_scheduler", return_value=_FakeScheduler()):
            snap = lifecycle.get_system_jobs_snapshot()

        assert len(snap) == 3
        by_id = {entry["id"]: entry for entry in snap}
        assert by_id["bot_data_refresh"]["name"] == "Bot Data Refresh"
        assert by_id["bot_data_refresh"]["status"] == "success"
        assert by_id["bot_data_refresh"]["duration_s"] == 1.23
        assert by_id["bot_data_refresh"]["detail"] == "Updated 3 source(s)"
        assert by_id["bot_data_refresh"]["next_run_at"] == "2026-11-18T13:00:00Z"
        assert isinstance(by_id["bot_data_refresh"]["last_run_at"], str)

        assert by_id["rdns_enrichment"] == {
            "id": "rdns_enrichment",
            "name": "rDNS Enrichment",
            "next_run_at": None,
            "last_run_at": None,
            "status": None,
            "duration_s": None,
            "detail": "",
        }
        assert by_id["share_audit_purge"]["next_run_at"] is None

        validated = SystemJobsResponse.with_telemetry(jobs=snap)
        assert [j.id for j in validated.jobs] == ["bot_data_refresh", "rdns_enrichment", "share_audit_purge"]
    finally:
        lifecycle._status.clear()
        lifecycle._status.update(saved_status)


def test_admin_helpers_reexports_resolve_source() -> None:
    from backend.core.request_context import _resolve_source

    assert admin_helpers._resolve_source is _resolve_source
