"""Request-event extents must never be replaced by object filename times."""

import asyncio
import copy
import importlib
import json
from unittest.mock import MagicMock

import pytest

from backend.sync_status_snapshot import compute_sync_status_cached


@pytest.fixture
def snapshot_state(monkeypatch):
    state = {
        "latest_log_at": "2026-09-05T01:00:00Z",
        "local_rows": 7,
        "request": {"latest_log_at": "2026-09-07T19:31:22Z", "total_rows": 7},
        "rum": {"latest_log_at": "2026-09-08T01:00:00Z", "total_rows": 3},
    }
    summary = {"latest_file_name": "logs_2026-09-06T17-19-00.gz", "total_rows": 99}
    ledger = MagicMock()
    ledger.execute.return_value.fetchone.return_value = None
    monkeypatch.setattr("backend.config.get_status", lambda _: copy.deepcopy(state))
    monkeypatch.setattr("backend.config.load_config", lambda _: {})
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda _: {"name": "snapshot-svc", "duckdb_path": __file__},
    )
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: ".")
    monkeypatch.setattr("backend.sync_status_snapshot._get_dir_size", lambda _: 0)
    monkeypatch.setattr("backend.core.metadata.get_ingested_files_status_summary", lambda _: summary)
    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda _: ledger)
    monkeypatch.setattr("backend.cron_progress.get_latest_progress_for_service", lambda _: None)

    def no_scan(*args, **kwargs):
        pytest.fail("cached status must not acquire DuckDB")

    monkeypatch.setattr("backend.core.duckdb.get_connection", no_scan)
    return state, summary, ledger


@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
@pytest.mark.parametrize("use_ledger", [False, True])
def test_file_overlay_preserves_current_request_extent(snapshot_state, request_latest, use_ledger):
    state, summary, ledger = snapshot_state
    state["request"]["latest_log_at"] = request_latest
    if request_latest is None:
        state["request"]["total_rows"] = state["local_rows"] = 0
    if use_ledger:
        ledger.execute.return_value.fetchone.return_value = (summary["latest_file_name"],)
        summary["latest_file_name"] = None

    snap = compute_sync_status_cached("snapshot-svc")

    assert snap["latest_log_at"] == snap["request"]["latest_log_at"] == request_latest
    assert snap["latest_ingested_file_at"] == "2026-09-06 17:19:00"
    assert snap["latest_available_file_at"] == "2026-09-06 17:19:00"
    assert snap["local_rows"] == state["local_rows"]
    assert snap["rum"] == state["rum"]


@pytest.mark.parametrize("flat_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
def test_legacy_flat_extent_is_not_a_filename_fallback(snapshot_state, flat_latest):
    state, _, _ = snapshot_state
    del state["request"]
    state["latest_log_at"] = flat_latest
    state["local_rows"] = 0
    snap = compute_sync_status_cached("snapshot-svc")
    assert snap["latest_log_at"] == flat_latest
    assert snap["local_rows"] == 0


def test_high_scale_snapshot_uses_clickhouse_header_metrics(monkeypatch):
    from datetime import UTC, datetime

    from backend.high_scale.archive_models import ServingWatermark
    from backend.high_scale.registry import HighScaleService

    class Client:
        def execute(self, sql, params=None):
            domain = (params or {}).get("domain")
            totals = {"request": 12, "rum_vitals": 8, "rum_errors": 2}
            latest = {
                "request": datetime(2026, 9, 15, 19, 0, tzinfo=UTC),
                "rum_vitals": datetime(2026, 9, 15, 19, 1, tzinfo=UTC),
                "rum_errors": datetime(2026, 9, 15, 19, 2, tzinfo=UTC),
            }
            return [{"total_rows": totals[domain], "latest_log_at": latest[domain]}]

    service = HighScaleService(
        service_id="high-scale-svc",
        client=Client(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="high-scale-svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda _: {"name": "high-scale-svc", "access_level": "read_write"},
    )
    monkeypatch.setattr(
        "backend.high_scale.registry.get_high_scale_service_registry",
        lambda: type("Registry", (), {"resolve": lambda _, service_id: service})(),
    )
    monkeypatch.setattr(
        "backend.config.get_status",
        lambda _: pytest.fail("high-scale snapshots must not use stale config status"),
    )

    snapshot = compute_sync_status_cached("high-scale-svc")

    assert snapshot["request"]["total_rows"] == 12
    assert snapshot["rum"]["total_rows"] == 10
    assert snapshot["local_rows"] == 22
    assert snapshot["latest_log_at"] == "2026-09-15T19:00:00+00:00"


@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
def test_cached_rest_and_extents_use_request_snapshot(snapshot_state, request_latest):
    state, _, _ = snapshot_state
    state["request"]["latest_log_at"] = request_latest
    if request_latest is None:
        state["local_rows"] = state["request"]["total_rows"] = 0
    router = importlib.import_module("backend.routers.admin.sync_status")
    payload = router.sync_status("snapshot-svc", skip_fos=True, force=False).model_dump()
    assert payload["latest_log_at"] == payload["request"]["latest_log_at"] == request_latest
    assert payload["local_rows"] == state["local_rows"]
    assert router.log_extents("snapshot-svc").latest_log_at == request_latest


@pytest.mark.asyncio
@pytest.mark.parametrize("admin", [False, True])
@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", None])
async def test_initial_sse_uses_request_extent(snapshot_state, admin, request_latest):
    state, _, _ = snapshot_state
    state["request"]["latest_log_at"] = request_latest

    class Request:
        async def is_disconnected(self):
            return False

    if admin:
        from backend.routers.admin.events import admin_events_stream

        response = await admin_events_stream(Request(), channels="sync-status", service_id="snapshot-svc")
    else:
        from backend.routers.admin.sync_status import log_extents_stream

        response = await log_extents_stream(Request(), service_id="snapshot-svc")
    iterator = response.body_iterator
    try:
        payload = json.loads(await asyncio.wait_for(anext(iterator), timeout=2))
        if admin:
            payload = payload["data"]
        assert payload["latest_log_at"] == payload["request"]["latest_log_at"] == request_latest
        if not admin:
            assert "latest_ingested_file_at" not in payload
    finally:
        await iterator.aclose()
