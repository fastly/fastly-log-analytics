from __future__ import annotations

import datetime as dt
import json
from collections import namedtuple
from unittest.mock import patch

from fastapi.encoders import jsonable_encoder

from tests.conftest import MOCK_SERVICE_ID

# ── From test_typed_response_admin_actions.py ──

"""Admin action / probe / beacon routes keep their pre-typing wire payloads.

Each test drives the real handler with its producer patched and asserts the response body equals the
producer's dict byte-for-byte after JSON round-tripping (underscore envelope keys excluded), so the
``response_model`` added to the route can't drop, rename or re-encode a field.
"""


def _body(resp, *, status=200):
    assert resp.status_code == status, resp.text
    data = resp.json()
    return {k: v for k, v in data.items() if not k.startswith("_")} if isinstance(data, dict) else data


def test_refresh_single_bot_source_matches_producer(client):
    meta = {"id": "arcjet", "entry_count": 42, "last_updated": "2026-10-11T00:00:00Z", "enabled": True}
    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=meta):
        resp = client.post("/api/admin/bot-sources/arcjet/refresh")
    assert _body(resp) == {"ok": True, "source": meta}


def test_refresh_all_bot_sources_matches_producer_on_both_paths(client):
    results = [{"id": "a", "entry_count": 3}, {"id": "b", "failed": True, "error": "boom"}]
    expected = {"ok": False, "sources": results, "updated_count": 1, "failed_count": 1}
    for path in ("/api/admin/bot-sources/refresh", "/api/admin/bots/refresh"):
        with patch("backend.utils.bot_sources.refresh_all_sources", return_value=results):
            assert _body(client.post(path)) == expected


def test_rdns_enrich_matches_producer(client):
    summary = {"resolved": 4, "errors": 1, "discovered": 7, "batch_limit": 100}
    with patch("backend.utils.rdns_cache.enrich_batch", return_value=summary) as enrich:
        resp = client.post("/api/admin/rdns/enrich?limit=100")
    enrich.assert_called_once_with(limit=100)
    assert _body(resp) == {"ok": True, "summary": summary}


def test_duckdb_recycle_skipped_and_recycled_match_producer(client):
    mb = 1024 * 1024
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * mb),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * mb),
    ):
        skipped = _body(client.post("/api/admin/duckdb/recycle"))
    assert skipped == {
        "ok": True,
        "status": "skipped",
        "reason": "RSS 100MB < threshold 500MB",
        "rss_mb": 100.0,
        "threshold_mb": 500.0,
    }

    with (
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[300 * mb, 200 * mb]),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * mb),
        patch("backend.core.duckdb_recycle.recycle_once", return_value="manual_admin: recycled 1/1"),
    ):
        recycled = _body(client.post("/api/admin/duckdb/recycle?force=true"))
    assert recycled == {
        "ok": True,
        "status": "recycled",
        "detail": "manual_admin: recycled 1/1",
        "rss_before_mb": 300.0,
        "rss_after_mb": 200.0,
        "freed_mb": 100.0,
    }


def test_ngwaf_sync_matches_producer(client):
    stats = {"total_cached_bots": 2, "workspaces": {"w": None}, "top_bots": {"bot": 2}}
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-n"}),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.cron.jobs.metadata._run_ngwaf_bot_sync"),
        patch("backend.utils.ngwaf_bot_cache.get_cache_stats", return_value=stats),
    ):
        resp = client.post("/api/admin/ngwaf/sync/svc-n")
    assert _body(resp) == {"ok": True, "service_id": "svc-n", "workspace_id": "ws-1", "stats": stats}


def test_share_purge_matches_producer(client):
    stats = {
        "deleted_audit_logs": 3,
        "deleted_expired_invites": 1,
        "deleted_stale_sessions": 2,
        "deleted_claim_tokens": 0,
        "audit_retention_days": 90,
        "max_idle_session_days": 30,
    }
    with patch("backend.core.share_db.purge_all_share_records", return_value=stats):
        resp = client.post("/api/admin/share/purge")
    assert _body(resp) == {"ok": True, **stats}


def test_iceberg_calendar_matches_producer(client):
    calendar = {"2026-10-01": {"data_files": 4, "size_bytes": 0}, "2026-10-02": {"data_files": 9, "size_bytes": 0}}
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=calendar):
        resp = client.get("/api/admin/iceberg-calendar")
    assert _body(resp) == calendar


def test_vcl_health_matches_producer(client):
    Snippet = namedtuple("Snippet", "name")
    snippets = [Snippet("Fastly Log Analytics - capture"), Snippet("Fastly Log Analytics - scoring")]
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-a", "admin_token": "tok"}),
        patch("backend.provision.declarative.fastly_integration.fetch_active_version", return_value=7),
        patch("backend.provision.declarative.fastly_integration.fetch_snippets", return_value=snippets),
    ):
        clean = _body(client.get("/api/admin/vcl-health?service_id=svc-a"))
    assert clean == {
        "service_id": "svc-a",
        "active_version": 7,
        "legacy_snippets_found": 0,
        "legacy_names": [],
        "consolidated_snippets_found": 2,
        "consolidated_names": [s.name for s in snippets],
        "is_clean": True,
        "recommendation": "✓ Clean state — VCL migration complete. Ready for production.",
    }
    with patch("backend.config.load_config", return_value=None):
        missing = _body(client.get("/api/admin/vcl-health?service_id=nope"))
    assert missing == {"service_id": "nope", "error": "Service nope not configured", "is_clean": False}


def test_page_telemetry_matches_producer(client):
    payload = {
        "queries": [{"engine": "duckdb", "sql_query": "SELECT 1", "duration_ms": 1.5}],
        "sections": [{"section_name": "summary", "duration_ms": 2.0}],
        "calls": [
            {
                "service": "FOS",
                "method": "GET",
                "path": "/b/k",
                "status": "200",
                "time_ms": 3.0,
                "caller": "fn",
                "bytes": 10,
            }
        ],
    }
    with (
        patch("backend.config.get_active_service_id", return_value="svc-a"),
        patch("backend.core.metadata.usage_log.get_page_telemetry", return_value=payload),
    ):
        assert _body(client.get("/api/debug/page-telemetry?page_load_id=p1")) == payload
    with patch("backend.config.get_active_service_id", return_value=None):
        assert _body(client.get("/api/debug/page-telemetry?page_load_id=p1")) == {
            "queries": [],
            "sections": [],
            "calls": [],
        }


def test_health_shallow_and_deep_match_producer(client):
    from backend.main import _APP_VERSION

    assert _body(client.get("/api/health")) == {"status": "ok", "version": _APP_VERSION}
    with patch("backend.config.list_service_ids", return_value=[]):
        assert _body(client.get("/api/health?deep=1")) == {"status": "ok", "version": _APP_VERSION, "services": []}


def test_backfill_window_matches_producer(client):
    counters = {
        "files_downloaded": 2,
        "rows_downloaded": 10,
        "files_removed": 0,
    }
    with patch("backend.core.iceberg.sync_data", return_value=counters):
        resp = client.post("/api/admin/backfill-window?start_time=2026-10-01T00:00:00Z&end_time=2026-10-01T02:00:00Z")
    assert _body(resp) == counters
    failure = {"error": "Could not load table: x", "files_downloaded": 0}
    with patch("backend.core.iceberg.sync_data", return_value=failure):
        resp = client.post("/api/admin/backfill-window?start_time=2026-10-01T00:00:00Z&end_time=2026-10-01T02:00:00Z")
    assert _body(resp) == failure


def test_cron_starters_match_producer(client):
    started = {"ok": True, "message": "started", "run_id": 41}
    with patch("backend.utils.router_utils.start_or_resume_cron", return_value=started):
        assert _body(client.post("/api/admin/commit-iceberg"), status=202) == started
        assert _body(client.post("/api/admin/ingest-logs")) == started


def test_rebuild_local_view_matches_producer(client):
    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb.start_cron_run", return_value=77),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
    ):
        resp = client.post("/api/admin/rebuild-local-view")
    assert _body(resp, status=202) == {"ok": True, "message": "Local view rebuild started.", "run_id": 77}


def test_teardown_frontend_matches_producer(client):
    cfg = {"service_id": "svc-t", "remote_frontend": {"service_id": "remote-1"}}
    with (
        patch("backend.utils.router_utils.load_service_config", return_value=cfg),
        patch("backend.routers.sharing_domain.delete_remote_frontend"),
        patch("backend.config.mutate_config", return_value=cfg),
    ):
        resp = client.post("/api/sharing/teardown-frontend", json={"service_id": "svc-t", "token_override": "t"})
    assert _body(resp) == {"ok": True, "message": "Successfully deleted remote frontend service remote-1"}


def test_beacon_acks_match_producer(client):
    ux = client.post("/api/ux-events", json={"event": "column_reordered", "pathname": "/x"})
    assert _body(ux) == {"ok": True}
    wv = client.post("/api/web-vitals", json={"id": "v1", "name": "LCP", "value": 1.2, "rating": "good"})
    assert _body(wv) == {"ok": True}


# ── From test_typed_response_admin_status.py ──


def _expected(payload):
    return json.loads(json.dumps(jsonable_encoder(payload)))


def test_celery_status_matches_producer(client, monkeypatch):
    payload = {
        "broker_reachable": True,
        "workers": {
            "active_tasks": {"w1": [{"id": "t1", "name": "n", "args": [1], "time_start": 2.5, "worker_pid": 7}]},
            "stats": {"w1": {"started": dt.datetime(2026, 1, 1, tzinfo=dt.UTC)}},
            "registered": {"w1": ["a.b"]},
            "scheduled": {},
            "count": 1,
        },
        "queues": {"q.ingest": 3},
        "inprocess_schedules": [{"name": "job", "trigger": "interval[0:01:00]", "next_run_at": 1700000000.0}],
        "ledger": {"committed": 5},
    }
    monkeypatch.setattr("backend.routers.admin.celery_status.get_celery_status", lambda: payload)
    resp = client.get("/api/admin/celery/status")
    assert resp.status_code == 200
    assert _body(resp) == _expected(payload)


def test_celery_status_workers_error_branch(client, monkeypatch):
    payload = {
        "broker_reachable": False,
        "workers": {"active_tasks": {}, "stats": {}, "registered": {}, "scheduled": {}, "count": 0, "error": "down"},
        "queues": {},
        "inprocess_schedules": [],
        "ledger": {},
    }
    monkeypatch.setattr("backend.routers.admin.celery_status.get_celery_status", lambda: payload)
    assert _body(client.get("/api/admin/celery/status")) == payload


def test_duckdb_status_matches_producer_keys(client):
    resp = client.get("/api/admin/duckdb/status")
    assert resp.status_code == 200
    body = _body(resp)
    assert set(body) == {"ok", "memory", "barrier", "pools", "retired_pools"}
    assert body["ok"] is True
    assert set(body["memory"]) == {"current_rss_mb", "recycle_threshold_mb", "recycle_interval_min"}
    assert set(body["barrier"]) == {"is_active", "active_paths"}


def test_ngwaf_status_and_rdns_stats(client, monkeypatch):
    stats = {"total_cached_bots": 2, "workspaces": {"w": None, "x": "2026-01-01"}, "top_bots": {"bot": 2}}
    monkeypatch.setattr("backend.utils.ngwaf_bot_cache.get_cache_stats", lambda: dict(stats))
    resp = client.get("/api/admin/ngwaf/status")
    assert resp.status_code == 200
    assert _body(resp) == {"ok": True, **stats}

    monkeypatch.setattr("backend.utils.rdns_cache.get_stats", lambda: {"entries": 4, "hit_rate": 0.5})
    rd = client.get("/api/admin/rdns/stats")
    assert rd.status_code == 200
    assert _body(rd) == {"ok": True, "stats": {"entries": 4, "hit_rate": 0.5}}


# ── From test_typed_response_control_room.py ──


def test_control_room_tabs_match_stub_payloads(client):
    from backend.routers.control_room import TAB_STUBS

    for tab, stub in TAB_STUBS.items():
        resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}")
        assert resp.status_code == 200, tab
        assert _body(resp) == stub, tab


def test_control_room_models_roundtrip_producer_shapes():
    from backend.models.control_room import (
        CorrelateResponse,
        LogFieldAuditResponse,
        RealtimeSeedResponse,
        WizardStateResponse,
        WizardStepResponse,
    )

    cases = [
        (RealtimeSeedResponse, {"ticks": [{"ts": 1, "rps": 2.5, "nested": {"a": [1]}}]}),
        (
            LogFieldAuditResponse,
            {
                "service_id": "svc",
                "enabled_fields": ["ip", "url"],
                "features": [
                    {"name": "Insights", "required_fields": ["ip"], "status": "ok", "missing_fields": []},
                ],
            },
        ),
        (
            CorrelateResponse,
            {
                "dimension": "country",
                "top": [{"value": "US", "count": 3}],
                "freshness": {"latest_log_at": None, "lag_seconds": 0},
            },
        ),
        (WizardStateResponse, {"service_id": "svc", "completed_steps": ["a"], "is_complete": False}),
        (WizardStepResponse, {"recorded": True, "step": "a"}),
    ]
    for model, payload in cases:
        assert model.model_validate(payload).model_dump(mode="json") == payload, model.__name__
