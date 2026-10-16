"""Admin status reads keep their pre-typing wire payloads (compared to the producer's dict)."""

from __future__ import annotations

import datetime as dt
import json

from fastapi.encoders import jsonable_encoder


def _body(resp):
    return {k: v for k, v in resp.json().items() if not k.startswith("_")}


def _expected(payload):
    return json.loads(json.dumps(jsonable_encoder(payload)))


def test_celery_status_matches_producer(client, monkeypatch):
    payload = {
        "broker_reachable": True,
        "workers": {"active_tasks": {"w1": []}, "stats": {}, "registered": {}, "scheduled": {}, "count": 0},
        "queues": {"q.ingest": 3},
        "schedules": [{"name": "job", "next_run": dt.datetime(2026, 1, 1, tzinfo=dt.UTC)}],
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
        "schedules": [],
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
