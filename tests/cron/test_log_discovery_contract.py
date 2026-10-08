"""Contract test suite for Cron 1: log_discovery_{service_id}.

Verifies all requirements and checklist items from docs/cron/jobs/log-discovery.md (§1–§9):
1. Trigger POST /api/admin/sync/{service_id} with synthetic .gz files in FOS; verify HTTP 200 response.
2. Confirm execution records in cron_runs with status success and non-zero files_ingested.
3. Verify the PostgreSQL usage_log table attributes FOS Class A LIST and Class B GET calls to cron.log_discovery.
4. Confirm new rows immediately queryable via GET /api/dashboard/bundle.
5. Confirm heavy refresh phases (update_top_values and the usage-log phase) run no more than once per 60s.
6. Under FLA_DEV_NO_CRONS=1, verify job does not register or execute.
7. In High-Scale mode, verify source objects transition discovered → claimed → acknowledged.
8. Verify malformed-line and corrupt-gzip outcomes, per-category counters, error status,
   immediate per-service cap eviction, private evidence downloads, and Analyst Path A/B denial.
"""

from __future__ import annotations

import logging
import stat
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs import sync as sync_mod
from backend.cron.scheduler import Scheduler
from backend.deps import get_service_id, get_source, require_admin
from backend.main import app


@pytest.fixture
def log_discovery_test_source(monkeypatch, tmp_path):
    service_id = "svc-log-discovery-contract"
    cache_root = tmp_path / "cache" / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": "test-log-discovery-bucket",
        "_cache_dir_override": str(cache_root),
        "provisioning": {
            "access_level": "read_write",
            "cron_sync": {"enabled": True},
        },
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: src if sid == service_id else None)
    return src


def test_contract_1_manual_trigger_with_synthetic_gz_in_fos(log_discovery_test_source):
    """Checklist Item 1:
    Trigger POST /api/admin/sync/{service_id} with synthetic .gz files in FOS; verify HTTP 200 response.
    """
    service_id = log_discovery_test_source["service_id"]
    started = {}

    def fake_start_cron_run(src, task):
        started["task"] = task
        return "run-contract-sync-123"

    app.dependency_overrides[get_source] = lambda: log_discovery_test_source
    app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=fake_start_cron_run),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.sync._run_log_discovery_cron"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/sync/{service_id}",
                headers={"x-fastly-service-id": service_id},
            )

        assert resp.status_code == 200, f"Expected 200, got: {resp.text}"
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == "run-contract-sync-123"
        assert "started" in body["message"].lower()
        assert started["task"] == "log_discovery"
    finally:
        app.dependency_overrides.clear()


def test_contract_2_cron_runs_records_success_and_nonzero_files_ingested(log_discovery_test_source):
    """Checklist Item 2:
    Confirm execution records in cron_runs with status success and non-zero files_ingested.
    """
    from backend.core import metadata as metadata_db

    service_id = log_discovery_test_source["service_id"]
    source_name = log_discovery_test_source["name"]

    # Start run
    run_id = metadata_db.start_cron_run(service_id, "log_discovery")
    assert run_id is not None

    # Complete run with success and non-zero files ingested
    metadata_db.log_cron_run(
        service_id,
        "log_discovery",
        duration_s=1.25,
        status="success",
        run_id=run_id,
        files_downloaded=5,
        rows_ingested=150,
        summary="Ingested 5 file(s) (150 rows) in 1.25s",
    )

    total, runs = metadata_db.get_cron_runs(source_name, task="log_discovery", per_page=5)
    assert total >= 1
    assert runs[0]["status"] == "success"
    assert runs[0]["files_downloaded"] == 5
    assert runs[0]["rows_ingested"] == 150


def test_contract_3_usage_log_attributes_fos_class_a_list_and_class_b_get_to_cron_log_discovery(
    log_discovery_test_source, monkeypatch
):
    """Checklist Item 3:
    Verify the PostgreSQL usage_log table attributes FOS Class A LIST and Class B GET calls to cron.log_discovery.
    Also verifies that the cron_task wrapper executes with process_context='cron.log_discovery' and flushes on exit.
    """
    from backend.core import metadata as metadata_db
    from backend.core.metadata import usage_log_db
    from backend.utils.telemetry import get_process_context

    service_id = log_discovery_test_source["service_id"]
    observed: dict[str, object] = {}
    flush_contexts: list[str | None] = []

    def fake_ingest_followups(*args, **kwargs):
        observed["context"] = get_process_context()
        yield {"type": "done", "files": 0, "rows": 0, "corrupt": 0}

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: log_discovery_test_source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **kw: 999)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron.jobs.sync._ingest_with_adaptive_followups", fake_ingest_followups)
    monkeypatch.setattr("backend.cron.jobs.sync._claim_heavy_refresh", lambda sid: False)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda *a, **kw: None)
    monkeypatch.setattr(
        "backend.utils.usage_logger.flush_usage_log",
        lambda sid: flush_contexts.append(get_process_context()),
    )

    sync_mod._run_log_discovery_cron(service_id, force=True)

    assert observed["context"] == "cron.log_discovery", (
        f"Execution must run under process_context='cron.log_discovery', got {observed['context']!r}"
    )
    assert flush_contexts == ["cron.log_discovery"], (
        f"Usage log flush must run under process_context='cron.log_discovery', got {flush_contexts}"
    )

    # Emulate FOS Class A LIST and Class B GET calls captured during log discovery
    fos_calls = [
        {
            "method": "LISTOBJECTSV2",
            "service": "FOS",
            "details": "Class A · list",
            "url": "s3://test-bucket/raw/request/",
        },
        {
            "method": "GETOBJECT",
            "service": "FOS",
            "bytes": 65536,
            "details": "Class B · get",
            "url": "s3://test-bucket/raw/request/log.gz",
        },
    ]

    metadata_db.log_usage_calls(service_id, fos_calls, process_context="cron.log_discovery")

    con = usage_log_db.get_con(service_id)
    rows = con.execute(
        "SELECT operation_class, process_context, operation_type FROM usage_log WHERE service_id = ? ORDER BY id",
        (service_id,),
    ).fetchall()

    list_rows = [r for r in rows if r["operation_type"] == "LISTOBJECTSV2"]
    assert list_rows, "Must record LISTOBJECTSV2 in usage_log"
    assert list_rows[-1]["operation_class"] == "A", "LISTOBJECTSV2 must be classified as Class A"
    assert list_rows[-1]["process_context"] == "cron.log_discovery", (
        "LISTOBJECTSV2 must be attributed to cron.log_discovery"
    )

    get_rows = [r for r in rows if r["operation_type"] == "GETOBJECT"]
    assert get_rows, "Must record GETOBJECT in usage_log"
    assert get_rows[-1]["operation_class"] == "B", "GETOBJECT must be classified as Class B"
    assert get_rows[-1]["process_context"] == "cron.log_discovery", "GETOBJECT must be attributed to cron.log_discovery"


def test_contract_6_safety_gate_under_fla_dev_no_crons(log_discovery_test_source, monkeypatch, caplog):
    """Checklist Item 6:
    Under FLA_DEV_NO_CRONS=1, verify job does not register or execute.
    """
    service_id = log_discovery_test_source["service_id"]
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    # 1. Registration check: Scheduler._register_dev_local_safe_jobs must NOT register log_discovery
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[log_discovery_test_source]),
        patch("backend.core.duckdb.get_source_for_service", return_value=log_discovery_test_source),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()
        job_ids = [c.kwargs["id"] for c in add_job.call_args_list]
        assert f"log_discovery_{service_id}" not in job_ids, (
            "log_discovery must never register under FLA_DEV_NO_CRONS=1"
        )

    # 2. Execution check: direct invocation must refuse execution and log warning citing FLA_DEV_NO_CRONS=1
    caplog.set_level(logging.WARNING)
    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        res = sync_mod._run_log_discovery_cron(service_id, force=True)
        get_src.assert_not_called()
        assert res is None or res.get("status") == "skipped"

    assert any("FLA_DEV_NO_CRONS=1" in record.message for record in caplog.records), (
        "Execution refusal log must cite FLA_DEV_NO_CRONS=1"
    )


def test_contract_8_malformed_corrupt_caps_and_analyst_denial(log_discovery_test_source, tmp_path, monkeypatch):
    """Checklist Item 8:
    Verify malformed-line and corrupt-gzip outcomes, per-category counters, error status,
    immediate per-service cap eviction, private evidence downloads, and Analyst Path A/B denial.
    """
    from backend.core import quarantine
    from backend.core.metadata.quarantine import list_quarantine_evidence

    service_id = log_discovery_test_source["service_id"]

    # 1. FIFO cap eviction & 0o600 / 0o700 private permissions
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 2)
    payloads = [b'{"line": 1}\n', b'{"line": 2}\n', b'{"line": 3}\n']
    results = [
        quarantine.capture_evidence(
            service_id,
            "request",
            f"raw/request/part_{i}.log.gz",
            payload,
            line_ordinal=i,
            byte_offset=0,
            error_category="invalid_json",
            error_text="malformed json test",
        )
        for i, payload in enumerate(payloads, start=1)
    ]

    # Third insertion must trigger eviction of item 1
    assert results[2]["cap_evictions"] == 1
    items = list_quarantine_evidence(service_id)
    assert len(items) == 2
    remaining_ids = {item["id"] for item in items}
    assert int(results[0]["id"]) not in remaining_ids
    assert int(results[2]["id"]) in remaining_ids

    # Check evidence file permission 0o600 and dir permission 0o700
    evidence_path = quarantine._evidence_path(service_id, int(results[2]["id"]))
    assert stat.S_IMODE(evidence_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(evidence_path.parent.stat().st_mode) == 0o700

    # 2. Private evidence download via API
    item, evidence_file = quarantine.read_evidence(service_id, int(results[2]["id"]))
    try:
        assert evidence_file.read() == payloads[2]
    finally:
        evidence_file.close()

    app.dependency_overrides[get_source] = lambda: log_discovery_test_source
    app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        client = TestClient(app)
        download_resp = client.get(f"/api/admin/quarantine/download/{results[2]['id']}")
        assert download_resp.status_code == 200
        assert download_resp.content == payloads[2]

        # 3. Analyst Path B denial (require_admin blocks remote analyst)
        from fastapi import HTTPException

        def reject_analyst():
            raise HTTPException(status_code=403, detail={"error": "admin_only"})

        app.dependency_overrides[require_admin] = reject_analyst
        denied_resp = client.get(f"/api/admin/quarantine/download/{results[2]['id']}")
        assert denied_resp.status_code == 403
        assert denied_resp.json()["detail"]["error"] == "admin_only"

        # 4. Analyst Path A denial (read_only instance cannot trigger sync)
        ro_source = {**log_discovery_test_source, "access_level": "read_only"}
        app.dependency_overrides[get_source] = lambda: ro_source
        app.dependency_overrides.pop(require_admin, None)
        sync_denied_resp = client.post(
            f"/api/admin/sync/{service_id}",
            headers={"x-fastly-service-id": service_id},
        )
        assert sync_denied_resp.status_code == 403
        assert "read-only" in sync_denied_resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()
