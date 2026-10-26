"""Tests for Admin Task Queue, Ingest Ledger, ClickHouse, Compaction, Downloads, Quarantine, and Query Monitor APIs."""

from __future__ import annotations

import csv
import importlib
import io
import json
import os
import time as _time
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from backend.core.clickhouse_publication import PAYLOAD_COLUMNS, FosArtifacts
from backend.core.query_attribution import Attribution, current_attribution
from backend.deps import get_source, require_admin
from backend.routers.admin_queries import _admin_id_from_request
from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend import config as _svc_config
from backend.core import metadata as _meta
from backend.core import metric_snapshots
from backend.core.query_registry import query_registry
from backend.main import app
from backend.routers import admin_queries as mod
from backend.routers.admin import clickhouse as api
from backend.routers.usage import _extract_fos_ops, _fastly_api
from backend.utils.remote_access import _is_blocked_path
from tests._helpers.config_fakes import fake_mutate_config as _fake_mutate
from tests._helpers.sources import TinyFos
from tests.conftest import MOCK_SERVICE_ID
from tests.core.test_clickhouse import fixture_manifest

_PATCH_KEYS = {"side_effect", "return_value", "new", "autospec", "create"}


@contextmanager
def _patches(*specs):
    with ExitStack() as stack:
        yield [
            stack.enter_context(
                patch(t, **kw) if isinstance(kw, dict) and kw and set(kw) <= _PATCH_KEYS else patch(t, return_value=kw)
            )
            for t, kw in specs
        ]


def _mock_redis(queues: dict[str, int]):
    r = MagicMock()
    r.scan_iter.side_effect = lambda match=None, count=None: (
        iter([k for k in queues if k.startswith("q.")]) if match == "q.*" else iter([])
    )
    r.type.side_effect = lambda k: "list" if k in queues else "none"
    r.llen.side_effect = lambda k: queues.get(k, 0)
    return r


def _assert_analyst_rbac_403(client, method: str, path: str, *, skip_http_for: tuple[str, ...] = ()) -> None:
    raw_path = path.split("?", 1)[0]
    assert _is_blocked_path(raw_path) is True, f"Expected {raw_path} to be blocked for Analyst Path B"
    sess = SimpleNamespace(session_id="sess_analyst")
    fake_req = SimpleNamespace(
        state=SimpleNamespace(analyst_session=sess), headers={}, client=SimpleNamespace(host="203.0.113.55")
    )
    with pytest.raises(HTTPException) as exc:
        require_admin(fake_req)  # type: ignore[arg-type]
    assert exc.value.status_code == 403 and exc.value.detail["error"] == "admin_only"
    if raw_path not in skip_http_for:
        client.app.dependency_overrides[require_admin] = lambda: require_admin(fake_req)  # type: ignore[arg-type]
        try:
            resp = client.request(method, path)
            assert resp.status_code == 403 and resp.json()["detail"]["error"] == "admin_only"
        finally:
            client.app.dependency_overrides.pop(require_admin, None)


def test_high_scale_celery_status_full_payload(client):
    queues = {"q.ingest": 14, "q.control": 1, "celery": 3, "q.custom": 2}
    ledger_rows_by_svc = {
        "svc-a": [("discovered", 4), ("claimed", 2), ("committed", 120), ("quarantined", 1)],
        "svc-b": [("committed", 80), ("dead_letter", 3)],
    }

    def _fake_get_con(sid: str):
        con, cur = MagicMock(), MagicMock()

        def _exec(sql: str, params=()):
            assert "WHERE service_id = ?" in sql and params == (sid,)
            cur.fetchall.return_value = ledger_rows_by_svc.get(sid, [])

        cur.execute.side_effect = _exec
        con.cursor.return_value = cur
        return con

    mock_i = MagicMock()
    t_name = "backend.tasks.ingest.ingest_batch"
    task = {"id": "task-uuid-1", "name": t_name, "args": ["svc-a", 12], "time_start": 1700000000.5}
    workers = ("celery@worker-1", "celery@worker-2")
    mock_i.active.return_value = {"celery@worker-1": [task], "celery@worker-2": []}
    mock_i.stats.return_value = {w: {"pool": {"max-concurrency": 8}} for w in workers}
    mock_i.registered.return_value = {w: [t_name] for w in workers}
    mock_i.scheduled.return_value = {w: [] for w in workers}
    svcs = [{"service_id": "svc-a"}, {"service_id": "svc-b"}]
    with _patches(
        ("backend.celery_app.app.control.inspect", mock_i),
        ("backend.celery_queue._get_redis", _mock_redis(queues)),
        ("backend.config.list_configs", svcs),
        ("backend.core.metadata.base.get_con", {"side_effect": _fake_get_con}),
    ):
        resp = client.get("/api/admin/celery/status", headers={"X-Service-Id": "svc-a"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["broker_reachable"] is True and data["workers"]["count"] == 2
        assert data["workers"]["active_tasks"]["celery@worker-1"][0]["id"] == "task-uuid-1"
        assert data["workers"]["active_tasks"]["celery@worker-2"] == []
        assert data["queues"] == {"celery": 3, "q.control": 1, "q.custom": 2, "q.ingest": 14}
        assert data["ledger"] == {"discovered": 4, "claimed": 2, "committed": 200, "quarantined": 1, "dead_letter": 3}


def test_standard_mode_idle_and_unreachable_broker_fallbacks(client):
    mock_i = MagicMock()
    mock_i.active.return_value = mock_i.stats.return_value = {}
    mock_i.registered.return_value = mock_i.scheduled.return_value = {}
    with _patches(
        ("backend.celery_app.app.control.inspect", mock_i),
        ("backend.celery_queue._get_redis", _mock_redis({})),
        ("backend.config.list_configs", []),
    ):
        resp = client.get("/api/admin/celery/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["broker_reachable"] is True and data["workers"]["count"] == 0
        assert data["queues"] == {"celery": 0, "q.control": 0, "q.ingest": 0} and data["ledger"] == {}

    broken_redis = MagicMock()
    broken_redis.scan_iter.side_effect = ConnectionError("Valkey connection refused")
    with _patches(
        ("backend.celery_app.app.control.inspect", {"side_effect": RuntimeError("Broker connection timeout")}),
        ("backend.celery_queue._get_redis", broken_redis),
        ("backend.config.list_configs", []),
    ):
        resp2 = client.get("/api/admin/celery/status")
        assert resp2.status_code == 200
        d2 = resp2.json()
        assert d2["broker_reachable"] is False and d2["workers"]["count"] == 0
        assert "Broker connection timeout" in d2["workers"]["error"] and d2["queues"] == {} and d2["ledger"] == {}


def test_standard_mode_inprocess_scheduler_and_metric_history(client):
    nxt = SimpleNamespace(timestamp=lambda: 1700000010.0)
    fake_job = SimpleNamespace(
        id="log_discovery_svc-a", name="log_discovery_svc-a", trigger="interval[0:00:10]", next_run_time=nxt
    )
    fake_scheduler = SimpleNamespace(scheduler=SimpleNamespace(get_jobs=lambda: [fake_job]))
    active_entry = [{"run_id": 101, "service_id": "svc-a", "task": "sync", "started_at": 1700000000.0}]
    err = ConnectionError("Celery broker not configured in standard mode")
    std_summary = {"ingested_files": 420, "quarantined_items": 1}
    with _patches(
        ("backend.celery_queue._get_redis", {"side_effect": err}),
        ("backend.config.is_high_scale_mode", False),
        ("backend.config.list_configs", [{"service_id": "svc-a"}]),
        ("backend.cron.scheduler.get_scheduler", fake_scheduler),
        ("backend.celery_status.inprocess_active_runs_entries", active_entry),
        ("backend.celery_status.standard_ingest_summary", std_summary),
        ("backend.celery_status.ingest_ledger_summary", {"committed": 420, "quarantined": 1}),
    ):
        resp = client.get("/api/admin/celery/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["broker_reachable"] is False and data["deployment_mode"] == "standard"
        sched_exp = [{"name": "log_discovery_svc-a", "trigger": "interval[0:00:10]", "next_run_at": 1700000010.0}]
        assert data["inprocess_schedules"] == sched_exp
        assert data["inprocess_active_runs"] == active_entry and data["standard_ingest"] == std_summary
        assert data["ledger"] == {"committed": 420, "quarantined": 1}

    metric_snapshots.record_snapshot("celery_queue_depth", 7.0)
    metric_snapshots.record_snapshot("celery_active_tasks", 2.0)
    resp_hist = client.get("/api/admin/metric-history/batch?since=1h")
    assert resp_hist.status_code == 200
    series = resp_hist.json()["series"]
    assert series["celery_queue_depth"][-1]["value"] == 7.0 and series["celery_active_tasks"][-1]["value"] == 2.0


_ADMIN_403_ROUTES = [
    tuple(x.split(":"))
    for x in (
        "GET:/api/admin/celery/status GET:/api/admin/metric-history/batch?since=1h "
        "GET:/api/admin/events/stream?channels=celery-status GET:/api/admin/queries "
        "GET:/api/admin/queries/summary GET:/api/admin/queries/1 POST:/api/admin/queries/1/cancel "
        "GET:/api/admin/slow-queries?service_id=svc1 GET:/api/admin/slow-queries/count?service_id=svc1 "
        "DELETE:/api/admin/slow-queries?service_id=svc1 GET:/api/admin/app-config/query-monitor"
    ).split()
]


@pytest.mark.parametrize("method,path", _ADMIN_403_ROUTES)
def test_analyst_session_receives_403_on_admin_routes(client, method, path):
    _assert_analyst_rbac_403(client, method, path, skip_http_for=("/api/admin/events/stream",))


def test_list_retry_and_purge_ingest_ledger_entries(client):
    sid = "svc-ledger-test"
    con = _meta.get_con(sid)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id = ?", (sid,))
    rum_key = "raw/rum/year=2026/month=10/day=20/hour=04/minute=28/dead-rum.log.gz"
    req_key = "raw/request/year=2026/month=10/day=20/hour=04/minute=29/bad-req.log.gz"
    good_key = "raw/request/year=2026/month=10/day=20/hour=04/minute=30/good-req.log.gz"
    e_miss = "object missing from FOS (already ingested+deleted, or expired)"
    e_gzip = "Not a gzipped file"
    r1 = (rum_key, 1024, "dead_letter", 1, e_miss, "object_missing", 1729400010.0, 1729400000.0, "rum_discovery", 99)
    r2 = (req_key, 2048, "quarantined", 3, e_gzip, "corrupt_gzip", 1729400020.0, 1729400005.0, "log_discovery", 100)
    ins_sql = (
        "INSERT INTO ingest_ledger (service_id, object_key, size_bytes, status, attempts, last_error, "
        "error_kind, claimed_at, discovered_at, originating_task, originating_run_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    for row in (r1, r2):
        cur.execute(ins_sql, (sid, *row))
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, size_bytes, status, attempts, committed_at, discovered_at) "
        "VALUES (?, ?, ?, 'committed', 1, 1729400030.0, 1729400000.0)",
        (sid, good_key, 4096),
    )
    con.commit()

    try:
        with patch("backend.config.list_configs", return_value=[{"service_id": sid}]):
            body = client.get("/api/admin/celery/ledger").json()
            assert body["total"] == 2 and len(body["items"]) == 2
            by_key = {item["object_key"]: item for item in body["items"]}
            rum_item, req_item = by_key[rum_key], by_key[req_key]
            assert rum_item["source_type"] == "rum" and rum_item["status"] == "dead_letter"
            assert rum_item["error_kind"] == "object_missing" and "object missing from FOS" in rum_item["last_error"]
            assert rum_item["originating_task"] == "rum_discovery" and rum_item["originating_run_id"] == 99
            assert req_item["source_type"] == "request" and req_item["status"] == "quarantined"
            assert req_item["error_kind"] == "corrupt_gzip"

            with (
                patch("backend.core.ingest.convert_batch_files.delay") as mock_req_delay,
                patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_rum_delay,
            ):
                retry_resp = client.post(
                    "/api/admin/celery/ledger/retry", json={"items": [{"service_id": sid, "object_key": rum_key}]}
                )
                assert retry_resp.status_code == 200
                rd = retry_resp.json()
                assert rd["ok"] is True and rd["affected"] == 1 and rd["dispatched"] == 1
                mock_rum_delay.assert_called_once_with(sid, [rum_key])
                mock_req_delay.assert_not_called()

            after_retry = client.get("/api/admin/celery/ledger").json()
            assert after_retry["total"] == 1 and after_retry["items"][0]["status"] == "quarantined"

            purge_resp = client.post("/api/admin/celery/ledger/purge", json={"status": "problem"})
            pd = purge_resp.json()
            assert purge_resp.status_code == 200 and pd["ok"] is True and pd["affected"] == 1

            after_purge = client.get("/api/admin/celery/ledger").json()
            assert after_purge["total"] == 0 and after_purge["items"] == []

            all_rows = client.get("/api/admin/celery/ledger?status=all").json()
            assert all_rows["total"] == 2 and {r["status"] for r in all_rows["items"]} == {"discovered", "committed"}
    finally:
        cur.execute("DELETE FROM ingest_ledger WHERE service_id = ?", (sid,))
        con.commit()


def test_task_routes_and_worker_queues_coverage():
    from backend.celery_status import KNOWN_QUEUES

    from backend.celery_app import app as celery_app

    for mod_name in celery_app.conf.include:
        importlib.import_module(mod_name)
    registered = set(celery_app.tasks.keys())
    for pattern in celery_app.conf.task_routes:
        if pattern.endswith("*"):
            assert any(t.startswith(pattern[:-1]) for t in registered), f"Unmatched pattern {pattern!r}"
        else:
            assert pattern in registered, f"task_routes key {pattern!r} is not registered"

    default_q = celery_app.conf.task_default_queue or "celery"
    routed = {spec["queue"] for spec in celery_app.conf.task_routes.values()} | {default_q}
    assert routed <= set(KNOWN_QUEUES), f"queues {routed - set(KNOWN_QUEUES)} not covered by KNOWN_QUEUES"


# --- ClickHouse Admin Endpoints ---

SERVICE = "TestPrototype"
STATUS = "/api/admin/clickhouse/status"
REPLAY = "/api/admin/clickhouse/replay"


@pytest.fixture
def boundary(monkeypatch):
    cfg = {"service_id": SERVICE, "access_level": "read_write"}
    monkeypatch.setattr(api.config, "load_config", lambda sid: cfg if sid == SERVICE else None)
    store, ch, objects, audit = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    counts = {"pending": 1, "claimed": 0, "failed": 0, "published": 2}
    st = {"schema_version": api.CLICKHOUSE_SCHEMA_VERSION, "publication_counts": counts, "active_generation": None}
    st |= {"oldest_pending_age_seconds": 12.5, "last_published_at": "2026-09-07T00:00:00+00:00"}
    st["expired_dataset_count"] = 1
    store.admin_status.return_value = st
    store.replay_preview.return_value = {"dataset_id": "dataset", "artifact_count": 1, "planned_artifacts": 1}
    monkeypatch.setattr(api, "PgManifest", MagicMock(return_value=store))
    monkeypatch.setattr(api, "get_clickhouse_client", lambda: ch)
    monkeypatch.setattr(api, "target_identity", lambda client: "target")
    monkeypatch.setattr(api, "FosArtifacts", objects)
    monkeypatch.setattr(api.metadata, "record_audit", audit)
    sub_app = FastAPI()
    sub_app.add_middleware(TelemetryResponseBodyMiddleware)
    sub_app.include_router(api.router)
    with TestClient(sub_app) as client:
        yield client, store, ch, objects, audit, cfg


def test_registered_on_real_app_openapi():
    paths = app.openapi()["paths"]
    for path, verb, model in (
        (STATUS, "get", "ClickHouseStatusResponse"),
        (REPLAY, "post", "ClickHouseReplayResponse"),
    ):
        ref = paths[path][verb]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        assert ref.endswith("/" + model)


def test_status_disabled_and_active_generation_scoping(boundary, monkeypatch):
    client, store, *_ = boundary
    ag = {"generation": "g", "dataset_id": "d", "coverage_start": "2026-09-01T00:00:00+00:00", "expired": True}
    ag |= {"coverage_end": "2026-09-02T00:00:00+00:00", "expires_at": "2026-09-03T00:00:00+00:00"}
    ag |= {"target_matches": False, "coverage_age_seconds": 500000}
    store.admin_status.return_value["active_generation"] = ag
    response = client.get(STATUS, params={"service_id": SERVICE})
    assert response.status_code == 200
    rj = response.json()
    assert rj["active_generation"]["expired"] is True and rj["oldest_pending_age_seconds"] == 12.5
    store.admin_status.assert_called_once_with(SERVICE, "target")

    store.admin_status.reset_mock()
    monkeypatch.setattr(api, "get_clickhouse_client", lambda: None)
    resp_dis = client.get(STATUS, params={"service_id": SERVICE})
    assert resp_dis.status_code == 200
    data = resp_dis.json()
    assert data["enabled"] is False and data["health"] == "disabled"
    for key in "schema_version publication_counts oldest_pending_age_seconds last_published_at active_generation expired_dataset_count".split():
        assert data[key] is None
    store.admin_status.assert_not_called()


@pytest.mark.parametrize("failure", ["client", "manifest", "schema"])
def test_status_unavailable_is_sanitized_503(boundary, failure):
    client, store, ch, *_ = boundary
    if failure == "client":
        ch.health.side_effect = RuntimeError("postgresql://private-secret@example.invalid")
    elif failure == "manifest":
        store.admin_status.side_effect = RuntimeError("s3://private-bucket")
    else:
        store.admin_status.return_value["schema_version"] = 99
    response = client.get(STATUS, params={"service_id": SERVICE})
    assert response.status_code == 503
    rj = response.json()
    assert rj["health"] == "unavailable" and rj["enabled"] is True and "private" not in response.text
    if failure != "schema":
        assert rj["publication_counts"] is None


@pytest.mark.parametrize("sid", ["missing", "../secrets"])
def test_missing_or_invalid_service_and_config_failure(boundary, monkeypatch, sid):
    client, store, ch, objects, audit, _ = boundary
    assert client.get(STATUS, params={"service_id": sid}).status_code == 404
    assert client.post(REPLAY, json={"service_id": sid, "dataset_id": "d"}).status_code == 404
    assert not store.mock_calls and not ch.mock_calls and not objects.called and not audit.called

    monkeypatch.setattr(api.config, "load_config", MagicMock(side_effect=OSError("private path")))
    for response in (
        client.get(STATUS, params={"service_id": SERVICE}),
        client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d"}),
    ):
        assert response.status_code == 503 and "private" not in response.text


def test_replay_disabled_and_readonly_rejected(boundary, monkeypatch):
    client, store, _, objects, audit, cfg = boundary
    monkeypatch.setattr(api, "get_clickhouse_client", lambda: None)
    assert client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d"}).status_code == 409
    cfg["access_level"] = "read_only"
    assert client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d"}).status_code == 403
    assert not store.mock_calls and not objects.called and not audit.called


_BAD_REPLAY_ARGS = [{}, {"dataset_id": None}, {"dataset_id": "../other"}, {"dataset_id": "d", "generation": "also"}]
_BAD_REPLAY_ARGS += [
    {"dataset_id": "d", "export": True},
    *({"dataset_id": "d", "limit": v} for v in (0, 101, 1001, True, 1.5)),
]


@pytest.mark.parametrize("extra", _BAD_REPLAY_ARGS)
def test_replay_invalid_arguments_never_reach_manifest(boundary, extra):
    client, store, _, objects, audit, _ = boundary
    response = client.post(REPLAY, json={"service_id": SERVICE, **extra})
    assert response.status_code == 422 and not store.mock_calls and not objects.called and not audit.called


@pytest.mark.parametrize("reference", [{"dataset_id": "dataset"}, {"generation": "g"}])
def test_dry_run_validates_references_without_mutators_or_fos(boundary, monkeypatch, reference):
    client, store, _, objects, audit, _ = boundary
    rebuild, resume = MagicMock(), MagicMock()
    monkeypatch.setattr(api, "full_rebuild", rebuild)
    monkeypatch.setattr(api, "replay_unpublished", resume)
    response = client.post(REPLAY, json={"service_id": SERVICE, **reference})
    assert response.status_code == 200
    rj = response.json()
    assert rj["dry_run"] is True and rj["attempted"] == 0 and rj["planned_artifacts"] == 1
    assert [call[0] for call in store.mock_calls] == ["replay_preview"]
    ds_id, gen = reference.get("dataset_id"), reference.get("generation")
    store.replay_preview.assert_called_once_with(SERVICE, dataset_id=ds_id, generation=gen, target="target", limit=100)
    assert not objects.called and not audit.called and not rebuild.called and not resume.called


@pytest.mark.parametrize(
    "error,code", [(LookupError("missing"), 404), (ValueError("expired"), 409), (RuntimeError("private DSN"), 503)]
)
def test_replay_reference_failures_are_safe(boundary, error, code):
    client, store, _, objects, audit, _ = boundary
    store.replay_preview.side_effect = error
    response = client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d"})
    assert response.status_code == code and "private" not in response.text and not objects.called and not audit.called


@pytest.mark.parametrize("resume", [False, True])
def test_apply_uses_real_loader_and_publication_without_fos_writes(boundary, monkeypatch, resume):
    client, store, ch, _, audit, _ = boundary
    dataset, _, batch = fixture_manifest()
    fos = TinyFos()
    objects = FosArtifacts({"service_id": SERVICE, "bucket": "test-bucket"}, client=fos)
    artifact = objects.put(dataset, batch.rows)
    writes = fos.puts
    monkeypatch.setattr(api, "FosArtifacts", lambda source: objects)
    monkeypatch.setattr(api.config, "config_to_source", lambda config: {})
    monkeypatch.setattr("backend.core.clickhouse_publication.target_identity", lambda client: "target")
    store.begin_generation.return_value = "new-generation"
    store.generation.return_value = {"target_identity": "target", "dataset_id": "dataset"}
    store.dataset.return_value = dataset
    store.artifact.return_value = artifact
    store.artifacts.return_value = [artifact]
    store.claim.return_value = 1
    store.finish.return_value = store.activate.return_value = True
    store.pending.side_effect = [[artifact.batch_id], []]

    def execute(sql, params):
        assert params["service"] == SERVICE and "log_facts FINAL" in sql
        if "SELECT count()" in sql:
            return [{"n": len(batch.rows)}]
        return [dict(zip(PAYLOAD_COLUMNS, row, strict=True)) for row in batch.rows]

    ch.execute.side_effect = execute
    reference = {"generation": "g"} if resume else {"dataset_id": "dataset"}
    response = client.post(REPLAY, json={"service_id": SERVICE, **reference, "dry_run": False, "limit": 1})
    assert response.status_code == 200, response.text
    rj = response.json()
    assert rj["published"] == 1 and rj["activated"] is True
    assert rj["generation"] == ("g" if resume else "new-generation") and fos.puts == writes
    ch.insert_rows.assert_called_once()
    assert "s3://" not in response.text and "test-bucket" not in response.text
    details = audit.call_args.kwargs["details"]
    assert details["published"] == 1 and details["action"] == ("resume" if resume else "rebuild")
    if resume:
        store.begin_generation.assert_not_called()
    else:
        store.begin_generation.assert_called_once_with(SERVICE, "dataset", "target")


def test_apply_failure_sanitized_and_analyst_path_b_rejected(boundary, monkeypatch):
    client, store, ch, _, audit, _ = boundary
    monkeypatch.setattr(api.config, "config_to_source", lambda config: {})
    monkeypatch.setattr(api, "full_rebuild", MagicMock(side_effect=RuntimeError("s3://private-secret")))

    response = client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d", "dry_run": False})
    assert response.status_code == 503 and "private" not in response.text
    assert response.json()["detail"]["error"] == "replay_unavailable"
    assert audit.call_args.kwargs["details"]["phase"] == "failed"

    monkeypatch.setenv("DEBUG_RESPONSES", "true")
    debug_calls = MagicMock(return_value=[{"url": "s3://private-secret"}])
    monkeypatch.setattr("backend.utils.telemetry.get_tracked_calls", debug_calls)
    resp_dbg = client.post(
        REPLAY, json={"service_id": SERVICE, "dataset_id": "d", "dry_run": False}, headers={"x-debug-responses": "1"}
    )
    assert resp_dbg.status_code == 503 and "private" not in resp_dbg.text and resp_dbg.json()["_debug_calls"] == []
    debug_calls.assert_not_called()

    store.reset_mock()
    ch.reset_mock()
    sub_app = FastAPI()

    @sub_app.middleware("http")
    async def _attach_analyst_session(request, call_next):
        request.state.analyst_session = MagicMock(service_ids=[SERVICE])
        return await call_next(request)

    sub_app.include_router(api.router)
    with TestClient(sub_app) as a_client:
        for res in (
            a_client.get(STATUS, params={"service_id": SERVICE}),
            a_client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d"}),
        ):
            assert res.status_code == 403 and res.json()["detail"]["error"] == "admin_only"
        assert not store.mock_calls and not ch.mock_calls


# --- Compaction & Metadata Retention / Cleanup Endpoints ---


def test_optimize_now_local_compact_and_compaction_stats_endpoints(client):
    fake_result = {"files_rewritten": 4, "files_added": 1, "bytes_rewritten": 12345}
    with patch("backend.core.iceberg.optimize_table", return_value=fake_result) as m:
        resp = client.post("/api/admin/optimize-now")
    assert resp.status_code == 200
    body = resp.json()
    assert body["files_rewritten"] == 4 and body["files_added"] == 1 and body["bytes_rewritten"] == 12345
    assert m.call_args.kwargs.get("min_files_per_partition") is None

    with patch("backend.core.iceberg.optimize_table", return_value={"files_rewritten": 0}) as m2:
        resp2 = client.post("/api/admin/optimize-now?min_files=1")
    assert resp2.status_code == 200 and m2.call_args.kwargs.get("min_files_per_partition") == 1

    with patch("backend.core.local_compaction.compact_local_partitions", return_value={"rewritten": 2}) as m3:
        resp3 = client.post("/api/admin/local-compact-now")
    assert resp3.status_code == 200 and resp3.json()["rewritten"] == 2
    assert m3.call_args.kwargs["min_files_per_partition"] == 3 and m3.call_args.kwargs["dry_run"] is False

    with patch("backend.core.local_compaction.compact_local_partitions", return_value={"rewritten": 0}) as m4:
        resp4 = client.post("/api/admin/local-compact-now?min_files=0&dry_run=true")
    assert resp4.status_code == 200 and m4.call_args.kwargs["min_files_per_partition"] == 0
    assert m4.call_args.kwargs["dry_run"] is True
    assert client.post("/api/admin/local-compact-now?min_files=-1").status_code == 422

    fake_stats = {"partitions": 100, "partitions_above_3": 5, "avg_files_per_partition": 1.8}
    with patch("backend.core.local_compaction.compaction_stats", return_value=fake_stats):
        resp_s = client.get("/api/admin/compaction-stats")
    assert resp_s.status_code == 200 and resp_s.json()["partitions"] == 100 and resp_s.json()["partitions_above_3"] == 5


def _patch_retention(client, payload: dict, **opts):
    saved: dict = {}
    audit_calls: list[dict] = []
    base_cfg = opts.get("base") or {"name": "svc", "service_id": "svc"}
    load_val = opts["load_ret"] if "load_ret" in opts else base_cfg
    audit_fn = opts.get("audit_side_effect") or (lambda **kw: audit_calls.append(kw))
    with _patches(
        ("backend.config.load_config", load_val),
        ("backend.config.mutate_config", {"side_effect": _fake_mutate(base_cfg, saved=saved)}),
        ("backend.core.metadata.is_ingested_files_dedup_active", opts.get("dedup", True)),
        ("backend.core.metadata.record_audit", {"side_effect": audit_fn}),
    ):
        resp = client.patch("/api/admin/metadata-retention", json=payload)
    return resp, saved, audit_calls


def test_metadata_retention_patch_and_storage_endpoints(client):
    ret_in = {"usage_log_days": 14, "ingested_files_days": 30, "cron_runs_days": 7}
    resp, saved, audit = _patch_retention(client, ret_in)
    assert resp.status_code == 200
    ret = resp.json()["retention"]
    assert ret["usage_log_days"] == 14 and ret["ingested_files_days"] == 30 and ret["cron_runs_days"] == 7
    assert saved["metadata_retention"] == ret_in
    assert len(audit) == 1 and audit[0]["event_type"] == "metadata_retention_update"

    base_merge = {"name": "svc", "service_id": "svc", "metadata_retention": {"usage_log_days": 21, "cron_runs_days": 9}}
    resp2, saved2, _ = _patch_retention(
        client, {"ingested_files_days": 30}, base=base_merge, load_ret={"name": "svc", "service_id": "svc"}
    )
    assert resp2.status_code == 200
    assert saved2["metadata_retention"] == {"usage_log_days": 21, "cron_runs_days": 9, "ingested_files_days": 30}

    with patch("backend.config.load_config", return_value=None):
        resp_404 = client.patch("/api/admin/metadata-retention", json={"usage_log_days": 7})
    assert resp_404.status_code == 404 and resp_404.json()["detail"]["error"] == "Service not found"

    resp_clamp, saved_clamp, _ = _patch_retention(client, {"usage_log_days": -5, "cron_runs_days": "not-a-number"})
    mr_clamp = saved_clamp["metadata_retention"]
    assert resp_clamp.status_code == 200 and mr_clamp["usage_log_days"] == 0 and mr_clamp["cron_runs_days"] == 0

    resp_dedup, saved_dedup, _ = _patch_retention(client, {"ingested_files_days": 30}, dedup=False)
    assert resp_dedup.status_code == 200 and saved_dedup["metadata_retention"]["ingested_files_days"] == 0
    r_boom, _, _ = _patch_retention(client, {"usage_log_days": 7}, audit_side_effect=RuntimeError("audit-boom"))
    assert r_boom.status_code == 200

    st_full = {"by_table": {"usage_log": {"rows": 1000, "bytes": 50000}}}
    cfg_full = {"name": "svc", "service_id": "svc", "metadata_retention": {"usage_log_days": 14}}
    for stats, cfg, dedup, expected_lock in ((st_full, cfg_full, True, False), ({}, {"name": "svc"}, False, True)):
        with _patches(
            ("backend.core.metadata.get_metadata_storage_stats", stats),
            ("backend.config.load_config", cfg),
            ("backend.core.metadata.is_ingested_files_dedup_active", dedup),
        ):
            r_st = client.get("/api/admin/metadata-storage")
        assert r_st.status_code == 200 and r_st.json()["ingested_files_locked"] is expected_lock
        if stats:
            assert r_st.json()["by_table"]["usage_log"]["rows"] == 1000
            assert r_st.json()["retention"]["usage_log_days"] == 14


def _post_metadata_cleanup(client, *, cfg=None, start_ret=1, start_err=None, cleanup_ret=None, cleanup_err=None):
    log_calls: list = []
    c_ret = cleanup_ret if cleanup_ret is not None else {"deleted": {}, "vacuumed": False}
    with _patches(
        ("backend.config.load_config", cfg if cfg is not None else {"metadata_retention": {}}),
        ("backend.core.duckdb.start_cron_run", {"side_effect": start_err, "return_value": start_ret}),
        ("backend.core.duckdb.log_cron_run", {"side_effect": lambda *a, **kw: log_calls.append((a, kw))}),
        ("backend.core.metadata.cleanup_metadata", {"side_effect": cleanup_err, "return_value": c_ret}),
    ) as (_, _, mock_log, mock_cleanup):
        resp = client.post("/api/admin/metadata-cleanup")
    lines = resp.content.decode("utf-8", errors="replace").splitlines()
    events = [json.loads(ln[6:]) for ln in lines if ln.startswith("data: ")]
    return resp, events, log_calls, mock_log, mock_cleanup


def test_metadata_cleanup_streams_success_zero_rows_errors_and_headers(client):
    resp, events, *_ = _post_metadata_cleanup(
        client,
        cfg={"metadata_retention": {"usage_log_days": 7}},
        start_ret=42,
        cleanup_ret={"deleted": {"usage_log": 100, "cron_runs": 10}, "vacuumed": True},
    )
    assert resp.status_code == 200 and resp.headers["content-type"].startswith("text/event-stream")
    done = events[-1]
    assert done["type"] == "done" and done["result"]["vacuumed"] is True
    assert done["result"]["deleted"]["usage_log"] == 100 and "Trimmed 110 rows" in done["message"]

    resp_zero, events_zero, *_ = _post_metadata_cleanup(client, cfg={})
    assert events_zero[-1]["type"] == "done" and events_zero[-1]["message"] == "No rows older than retention windows."
    cc = resp_zero.headers.get("cache-control", "")
    assert ("no-store" in cc or "no-cache" in cc) and resp_zero.headers.get("x-accel-buffering") == "no"
    assert resp_zero.headers.get("connection") == "keep-alive"

    _, events_start_err, _, mock_log, mock_cleanup = _post_metadata_cleanup(
        client, start_err=RuntimeError("simulated SQLite locked")
    )
    assert any(e["type"] == "error" and "simulated SQLite locked" in e["message"] for e in events_start_err)
    assert not any(e.get("type") == "done" for e in events_start_err) and not mock_cleanup.called
    assert mock_log.called and mock_log.call_args.kwargs.get("run_id") is None

    _, events_clean_err, log_calls, _, _ = _post_metadata_cleanup(
        client, start_ret=99, cleanup_err=RuntimeError("simulated DB lock")
    )
    assert any(e["type"] == "error" and "simulated DB lock" in e["message"] for e in events_clean_err)
    assert not any(e.get("type") == "done" for e in events_clean_err)
    assert log_calls and log_calls[0][1].get("run_id") == 99 and log_calls[0][0][3] == "error"


_BUNDLE_KEYS = (
    "slow_urls origin_summary origin_dims origin_latency_ts network_rtt network_speed network_quality "
    "verified_bots_ts perf_latency pop_health security_dims security_proxies ngwaf_bots ngwaf_bots_ts "
    "overview network_summary"
).split()
_ROLLUP_BACKFILL_MAP = {
    **{f"backfill_{k}_bundles": k for k in _BUNDLE_KEYS},
    "backfill_perf_dims_bundles": "perf_ttl_dist",
    "backfill_wellknown_bots_rollup": "wellknown_bots",
}
_ROLLUP_COMPACT_MAP = {
    **{
        f"compact_{k}_closed_days_to_daily": (None if k == "pop_health" else f"{k}_days")
        for k in _BUNDLE_KEYS
        if k not in ("slow_urls", "network_summary")
    },
    "compact_perf_dims_closed_days_to_daily": "perf_ttl_dist_days",
}


def test_backfill_bundle_rollups_and_rebuild_time_series_rollups(client):
    all_rollup_fns = {**_ROLLUP_BACKFILL_MAP, **_ROLLUP_COMPACT_MAP}
    values = {name: 100 + i for i, name in enumerate(all_rollup_fns)}

    @contextmanager
    def mock_get_connection(*args, **kwargs):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchone.return_value = (1,)
        yield mock_con

    with (
        patch.multiple("backend.core.rollups", **{n: MagicMock(return_value=v) for n, v in values.items()}),
        patch("backend.core.rollups.rum.recompute_rum_aggregates", MagicMock()),
        patch("backend.core.duckdb.get_connection", mock_get_connection),
    ):
        resp = client.post("/api/admin/backfill-bundle-rollups")
    assert resp.status_code == 200
    body = resp.json()
    for name, key in all_rollup_fns.items():
        if key is not None:
            assert body[key] == values[name], f"{key} should come from {name}"

    current_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    hours = [(current_hour - timedelta(hours=offset)).strftime("%Y-%m-%d-%H") for offset in (3, 2, 1)]
    with (
        patch("backend.routers.admin.compaction.build_time_series_bundles", return_value=len(hours)) as rebuild,
        patch("backend.core.rollups.recompute.recompute_touched_hours") as recompute,
    ):
        response = client.post("/api/admin/rebuild-time-series-rollups", json={"hours": hours})
    assert response.status_code == 200 and response.json()["requested_hours"] == hours
    assert response.json()["rebuilt"] == len(hours)
    assert rebuild.call_count == 1 and rebuild.call_args.args[2] == hours and not recompute.called

    for bad_hours in (["not-an-hour"], ["2099-01-01-00"], [hours[-1], hours[-1]]):
        assert client.post("/api/admin/rebuild-time-series-rollups", json={"hours": bad_hours}).status_code == 400
    _assert_analyst_rbac_403(client, "POST", "/api/admin/rebuild-time-series-rollups")


# --- Downloads Endpoints ---

_DL_ERR_CASES = [
    ("", "Missing key parameter"),
    ("other_prefix/file.log", "invalid_key"),
    ("raw_prefix/../../../etc/passwd", "invalid_key"),
]


@pytest.mark.parametrize(("key", "expected_err"), _DL_ERR_CASES)
def test_download_file_validation_errors(client, override_source, key, expected_err):
    response = client.get(f"/api/download?key={key}")
    assert response.status_code == 400 and response.json()["detail"]["error"] == expected_err


def test_download_file_from_local_cache_fos_and_cdn(client, override_source, tmp_path):
    cache_dir = tmp_path / "cache" / "service123"
    local_file = cache_dir / "raw_prefix" / "file.log"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("local-cached-data")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)):
        resp_local = client.get("/api/download?key=raw_prefix/file.log")
        assert resp_local.status_code == 200 and resp_local.text == "local-cached-data"
        local_file.unlink()

        fake_body = MagicMock()
        fake_body.iter_chunks.return_value = [b"fos-data"]
        fake_fos = MagicMock()
        fake_fos.get_object.return_value = {"Body": fake_body, "ContentType": "text/plain", "ContentLength": 8}
        with (
            patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
            patch("backend.utils.telemetry.record_call") as mock_record,
        ):
            resp_fos = client.get("/api/download?key=raw_prefix/file.log")
            assert resp_fos.status_code == 200 and b"".join(resp_fos.iter_bytes()) == b"fos-data"
            mock_record.assert_called_once()
            fake_body.close.assert_called_once()

        override_source["cdn_url"], override_source["cdn_secret"] = "http://mycdn.com", "secret123"
        fake_resp = MagicMock(headers={"Content-Type": "text/plain", "Content-Length": "8"})
        fake_resp.read.side_effect = [b"cdn-data", b""]
        with (
            patch("urllib.request.urlopen", return_value=fake_resp) as mock_urlopen,
            patch("backend.utils.telemetry.record_cdn_call") as mock_cdn_rec,
        ):
            resp_cdn = client.get("/api/download?key=raw_prefix/file.log")
            assert resp_cdn.status_code == 200 and b"".join(resp_cdn.iter_bytes()) == b"cdn-data"
            assert mock_urlopen.called and mock_cdn_rec.called and fake_resp.close.called

        with patch("urllib.request.urlopen", side_effect=ValueError("connection refused")):
            assert client.get("/api/download?key=raw_prefix/file.log").status_code == 502


def test_download_folder_and_download_all_local_and_cloud(client, override_source, tmp_path):
    fake_fos, fake_paginator = MagicMock(), MagicMock()
    keys = ("raw_prefix/raw/foo/file1.log", "raw_prefix/raw/foo/sub/file2.log", "raw_prefix/raw/foo/dir_marker/")
    fake_paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    fake_fos.get_paginator.return_value = fake_paginator
    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.routers.admin.downloads._fetch_file_to_zip") as mock_fetch,
    ):
        response = client.get("/api/download-folder?prefix=foo&root=raw")
        assert response.status_code == 200 and response.headers["content-type"] == "application/zip"
        assert len(b"".join(response.iter_bytes())) > 0
        fake_fos.get_paginator.assert_called_with("list_objects_v2", caller_hint="download_zip")
        fake_paginator.paginate.assert_called_with(Bucket="test-bucket", Prefix="raw_prefix/raw/foo/")
        assert mock_fetch.call_count == 2

    db_file = tmp_path / "test.duckdb"
    db_file.write_text("duckdb-binary-data")
    override_source["duckdb_path"], override_source["prefix"] = str(db_file), "raw_prefix"
    cache_dir = tmp_path / "cache" / "service123"
    local_file = cache_dir / "raw_prefix" / "cached_file.parquet"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("parquet-bytes")
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "raw_prefix/raw/file1.log"}, {"Key": "raw_prefix/raw/file2.log"}]}
    ]
    with _patches(
        ("backend.core.duckdb._cache_dir", str(cache_dir)),
        ("backend.core.duckdb._get_fos_client", fake_fos),
        ("backend.routers.admin.downloads._fetch_file_to_zip", {}),
        ("backend.core.duckdb.get_source_for_service", override_source),
        ("backend.config.load_config", {"name": "service123", "service_id": "service123"}),
    ) as (_, _, mock_fetch_cloud, _, _):
        resp_local = client.get("/api/download-all", params={"service_id": "service123", "include": "local"})
        assert resp_local.status_code == 200 and resp_local.headers["content-type"] == "application/zip"
        assert len(b"".join(resp_local.iter_bytes())) > 0

        resp_cloud = client.get("/api/download-all", params={"service_id": "service123", "include": "cloud"})
        assert resp_cloud.status_code == 200 and resp_cloud.headers["content-type"] == "application/zip"
        assert len(b"".join(resp_cloud.iter_bytes())) > 0 and mock_fetch_cloud.call_count == 2


# --- Quarantine Endpoints ---


def clean_res(data: dict) -> dict:
    return {key: value for key, value in data.items() if not key.startswith("_")}


def test_quarantine_routes_list_summary_download_purge_and_rbac(client, override_source):
    paths = app.openapi()["paths"]
    for sfx in ("", "/summary", "/download/{item_id}", "/purge"):
        assert f"/api/admin/quarantine{sfx}" in paths

    item = {"id": 7, "source_type": "request", "original_key": "raw/request/batch.log.gz", "line_ordinal": 4}
    item |= {"byte_offset": 92, "byte_length": 11, "error_category": "invalid_json", "error_text": "malformed record"}
    item |= {"sha256": "a" * 64, "quarantined_at": "2026-09-29T12:00:00Z"}
    with (
        patch("backend.routers.admin.quarantine.list_quarantine_evidence", return_value=[item]) as list_items,
        patch("backend.routers.admin.quarantine.count_quarantine_evidence", return_value=1) as count_items,
    ):
        response = client.get("/api/admin/quarantine?limit=20&offset=5&error_category=invalid_json")
    assert response.status_code == 200 and clean_res(response.json()) == {"total": 1, "items": [item]}
    list_items.assert_called_once_with(
        "service123", limit=20, offset=5, error_category="invalid_json", source_type=None
    )
    count_items.assert_called_once_with("service123", error_category="invalid_json", source_type=None)

    summary = {"total_items": 3, "total_bytes": 18, "request_items": 2, "rum_items": 1}
    summary |= {"oldest_at": "2026-09-29T11:00:00Z", "newest_at": "2026-09-29T12:00:00Z"}
    summary |= {"category_counts": {"invalid_json": 2, "corrupt_container": 1}}
    with patch("backend.routers.admin.quarantine.get_quarantine_evidence_summary", return_value=summary) as get_summary:
        resp_sum = client.get("/api/admin/quarantine/summary")
    assert resp_sum.status_code == 200 and clean_res(resp_sum.json()) == summary
    get_summary.assert_called_once_with("service123")

    raw_bytes = b'{"invalid":"\x00\xff"}\r\n'
    dl_item = {"id": 7, "file_path": "7.dat", "byte_length": len(raw_bytes), "original_key": "raw/request/batch.log.gz"}
    with patch("backend.core.quarantine.read_evidence", return_value=(dl_item, BytesIO(raw_bytes))):
        resp_dl = client.get("/api/admin/quarantine/download/7")
    assert resp_dl.status_code == 200 and resp_dl.content == raw_bytes
    assert resp_dl.headers["content-disposition"] == 'attachment; filename="quarantine-item-7.dat"'

    with patch("backend.core.quarantine.read_evidence", return_value=None):
        resp_404 = client.get("/api/admin/quarantine/download/999")
    assert resp_404.status_code == 404 and resp_404.json()["detail"]["error"] == "quarantine_not_found"

    big_item = {"id": 7, "file_path": "7.dat", "byte_length": 50 * 1024 * 1024 + 1}
    with patch("backend.core.quarantine.read_evidence", return_value=(big_item, BytesIO(b"unused"))):
        assert client.get("/api/admin/quarantine/download/7").status_code == 413

    for item_id, expected in ((7, {"purged": 1}), (None, {"purged": 3})):
        with (
            patch("backend.routers.admin.quarantine.get_quarantine_evidence", return_value={"id": item_id}),
            patch("backend.core.quarantine.purge_evidence", return_value=expected) as purge,
        ):
            q = "" if item_id is None else f"?item_id={item_id}"
            resp_p = client.post(f"/api/admin/quarantine/purge{q}")
        assert resp_p.status_code == 200 and clean_res(resp_p.json()) == expected
        purge.assert_called_once_with("service123", item_id)

    with patch("backend.routers.admin.quarantine.get_quarantine_evidence", return_value=None):
        assert client.post("/api/admin/quarantine/purge?item_id=999").status_code == 404

    override_source["access_level"] = "read_only"
    with patch("backend.routers.admin.quarantine.list_quarantine_evidence") as list_ro:
        resp_ro = client.get("/api/admin/quarantine")
    assert resp_ro.status_code == 403 and resp_ro.json()["detail"]["error"] == "admin_only" and not list_ro.called
    override_source["access_level"] = "read_write"
    _assert_analyst_rbac_403(client, "GET", "/api/admin/quarantine/summary")

    from tests.remote_access.test_middleware import _login_analyst, _seed_invite, _start_share

    _start_share()
    invite = _seed_invite(service_ids=["service123"])
    with TestClient(app) as c:
        _login_analyst(c, invite)
        r_qa = c.get("/api/admin/quarantine/summary", headers={"X-Remote-Analyst": "1", "Host": "testserver"})
        assert r_qa.status_code == 403


# --- Query Monitor & Slow Queries Endpoints ---


@pytest.fixture
def _reset_registry():
    queries, history = dict(query_registry._queries), list(query_registry._history)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(queries)
    query_registry._history.clear()
    query_registry._history.extend(history)


_SLOW_DEFAULTS = dict.fromkeys(
    "attr_principal_id attr_request_path attr_request_id attr_cron_job attr_cron_run_id attr_pool_slot error_type error_message peak_memory_mb".split()
)


def _slow_row(svc_id, qid, db, start, end, dur, prev, full, **kw) -> dict:
    keys = "service_id query_id db_type started_at_utc ended_at_utc duration_ms sql_preview sql_full sql_len outcome".split()
    vals = (svc_id, qid, db, start, end, dur, prev, full, len(full), "ok")
    return {**_SLOW_DEFAULTS, **dict(zip(keys, vals, strict=True)), **kw}


@pytest.mark.usefixtures("_reset_registry")
class TestQueryMonitorAndSlowQueries:
    def test_app_config_and_disabled_404_behavior(self, client):
        resp = client.get("/api/admin/app-config/query-monitor")
        assert resp.status_code == 200 and resp.json()["enabled"] is True

        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            assert client.get("/api/admin/app-config/query-monitor").json()["enabled"] is False
            for method, path, check_detail in (
                ("GET", "/api/admin/queries", True),
                ("GET", "/api/admin/queries/summary", False),
                ("POST", "/api/admin/queries/1/cancel", False),
            ):
                r = client.request(method, path)
                assert r.status_code == 404
                if check_detail:
                    assert r.json().get("detail") == {"error": "query_monitor_disabled"}

    def test_snapshot_empty_active_completed_and_since_seq(self, client):
        query_registry._queries.clear()
        query_registry._history.clear()
        body0 = client.get("/api/admin/queries").json()
        assert body0["active"] == [] and body0["completed"] == [] and body0["last_seq"] == 0

        prev = current_attribution.get()
        current_attribution.set(Attribution.admin(admin_id="t", request_path="/api/x", request_id=None))
        try:
            qid = query_registry.register("DuckDB", "SELECT 1", con=None)
        finally:
            current_attribution.set(prev)

        body = client.get("/api/admin/queries").json()
        row = next(r for r in body["active"] if r["query_id"] == qid)
        assert row["db_type"] == "DuckDB" and row["attribution"]["kind"] == "admin"
        assert row["sql"] is None and "SELECT 1" in row["sql_preview"]
        query_registry.deregister(qid)

        qid2 = query_registry.register("SQLite", "DROP TABLE x", con=None)
        query_registry.deregister(qid2)
        completed = client.get("/api/admin/queries?include_completed=true").json()["completed"]
        assert any(r["query_id"] == qid2 for r in completed)

        query_registry._queries.clear()
        a, b = query_registry.register("SQLite", "A", con=None), query_registry.register("SQLite", "B", con=None)
        ids = [r["query_id"] for r in client.get(f"/api/admin/queries?since_seq={a}").json()["active"]]
        assert a in ids and b in ids
        query_registry.deregister(a)
        query_registry.deregister(b)

    def test_summary_per_query_and_cancel_endpoints(self, client):
        query_registry._queries.clear()
        body = client.get("/api/admin/queries/summary").json()
        assert body["active_total"] == 0 and body["by_db_type"] == {} and body["longest_ms"] == 0.0

        a = query_registry.register("DuckDB", "SELECT 1", con=None)
        b = query_registry.register("SQLite", "SELECT 2", con=None)
        body2 = client.get("/api/admin/queries/summary").json()
        assert body2["active_total"] == 2 and body2["by_db_type"] == {"DuckDB": 1, "SQLite": 1}
        assert body2["longest_ms"] >= 0
        query_registry.deregister(a)
        query_registry.deregister(b)

        resp_404 = client.get("/api/admin/queries/9999999")
        assert resp_404.status_code == 404 and resp_404.json().get("detail") == {"error": "query_not_found"}

        qid = query_registry.register("DuckDB", "SELECT something_specific_xyz", con=None)
        resp_q = client.get(f"/api/admin/queries/{qid}")
        assert resp_q.status_code == 200 and resp_q.json()["query_id"] == qid
        assert "something_specific_xyz" in resp_q.json()["sql"]
        query_registry.deregister(qid)

        resp_c1 = client.post("/api/admin/queries/9999999/cancel")
        assert resp_c1.status_code == 200 and resp_c1.json()["state"] == "not_found"
        assert resp_c1.json()["query_id"] == 9999999

        qid_nc = query_registry.register("SQLite", "SELECT 1", con=None)
        resp_c2 = client.post(f"/api/admin/queries/{qid_nc}/cancel")
        assert resp_c2.status_code == 200 and resp_c2.json()["state"] == "already_finished"
        query_registry.deregister(qid_nc)

        mod._cancel_history.clear()
        try:
            with patch("backend.routers.admin_queries.time.monotonic", return_value=1000.0):
                statuses = [client.post("/api/admin/queries/9999999/cancel").status_code for _ in range(11)]
            assert statuses[-1] == 429
        finally:
            mod._cancel_history.clear()

    def test_admin_id_fallbacks_and_cancel_audit(self, client):
        req_local = SimpleNamespace(headers={}, client=SimpleNamespace(host="127.0.0.1"))
        assert _admin_id_from_request(req_local) == "127.0.0.1"
        assert _admin_id_from_request(SimpleNamespace(headers={}, client=None)) == "unknown"

        svc_id = "test_svc_cancel_audit"
        _meta.clear_slow_queries(svc_id)
        interrupted: list[bool] = []
        fake_con = SimpleNamespace(interrupt=lambda: interrupted.append(True))
        prev = current_attribution.get()
        current_attribution.set(
            Attribution.admin(admin_id="127.0.0.1", request_path="/api/query", request_id="req-cancel-1")
        )
        try:
            qid = query_registry.register("DuckDB", "SELECT * FROM logs", con=fake_con, service_id=svc_id)
        finally:
            current_attribution.set(prev)

        resp = client.post(f"/api/admin/queries/{qid}/cancel")
        assert resp.status_code == 200 and resp.json()["state"] == "cancelled"
        assert resp.json()["query_id"] == qid and interrupted == [True]
        query_registry.deregister(qid, error=RuntimeError("Query interrupted"))
        assert any(a["event_type"] == "query_cancelled" for a in _meta.list_audit(svc_id, limit=20))

    def test_list_count_and_clear_slow_queries(self, client):
        svc_id = "test-service-id"
        _meta.clear_slow_queries(svc_id)
        now = _time.time()
        s1, s2 = "SELECT count(*) FROM logs", "SELECT count(*) FROM logs WHERE status >= 500"
        row1 = _slow_row(svc_id, 7001, "DuckDB", now - 120.0, now - 118.2, 1800.0, s1, s2)
        row1 |= {"attr_kind": "admin", "attr_label": "admin @ 127.0.0.1", "attr_principal_id": "127.0.0.1"}
        row1 |= {"attr_caller_qualname": "dashboard.aggregates", "attr_caller_file": "backend/routers/dashboard.py:42"}
        row1 |= {"attr_request_path": "/api/dashboard/aggregates", "attr_request_id": "req-slow-1"}
        row1 |= {"attr_pool_slot": "pool#1", "peak_memory_mb": 64.5}
        _meta.insert_slow_query(svc_id, row1)

        s3, s4 = "SELECT * FROM ingested_files", "SELECT * FROM ingested_files WHERE service_id = $1"
        row2 = _slow_row(svc_id, 7002, "Postgres", now - 60.0, now - 59.5, 500.0, s3, s4)
        row2 |= {"attr_kind": "cron", "attr_label": "cron:full_sync", "attr_cron_job": "full_sync"}
        row2 |= {"attr_caller_qualname": "ingest_log.list_ingested_files", "attr_cron_run_id": 99}
        row2["attr_caller_file"] = "backend/core/metadata/ingest_log.py:100"
        _meta.insert_slow_query(svc_id, row2)

        cnt_resp = client.get(f"/api/admin/slow-queries/count?service_id={svc_id}&since_hours=24&threshold_ms=1000")
        assert cnt_resp.status_code == 200
        assert clean_res(cnt_resp.json()) == {"count": 1, "since_hours": 24, "threshold_ms": 1000.0}

        list_resp = client.get(
            f"/api/admin/slow-queries?service_id={svc_id}&since_hours=24&threshold_ms=100&sort=duration"
        )
        assert list_resp.status_code == 200
        rows = list_resp.json()["rows"]
        assert len(rows) == 2 and rows[0]["query_id"] == 7001 and rows[0]["sql"] == s2
        assert rows[0]["attribution"]["kind"] == "admin" and rows[0]["attribution"]["pool_slot"] == "pool#1"
        assert rows[1]["attribution"]["cron_run_id"] == "99"

        pg_rows = client.get(
            f"/api/admin/slow-queries?service_id={svc_id}&since_hours=24&threshold_ms=250&db_type=Postgres"
        ).json()["rows"]
        assert len(pg_rows) == 1 and pg_rows[0]["query_id"] == 7002 and pg_rows[0]["db_type"] == "Postgres"

        del_resp = client.delete(f"/api/admin/slow-queries?service_id={svc_id}")
        assert del_resp.status_code == 200 and del_resp.json()["deleted"] >= 2
        assert del_resp.json()["service_id"] == svc_id
        cnt_after = client.get(f"/api/admin/slow-queries/count?service_id={svc_id}&since_hours=24&threshold_ms=100")
        assert cnt_after.json()["count"] == 0


# --- Usage & Log Accounting Endpoints ---

_DEFAULT_UL_CFG = {"enabled": True, "retention_days": 30, "class_a_rate_per_1k": 0.005, "class_b_rate_per_10k": 0.004}
_DEFAULT_UL_CFG |= {"cdn_egress_rate_per_gb": 0.12, "storage_rate_per_gb_month": 0.02}
_DEFAULT_UL_CFG |= {"min_billed_days": 30, "billing_zone": "MBZ100"}


@pytest.fixture
def _clear_log_accounting_ttl_caches():
    from backend.routers.admin import log_accounting as _la

    _la._FASTLY_COUNTS_CACHE.clear()
    _la._DUCKDB_COUNTS_CACHE.clear()
    yield
    _la._FASTLY_COUNTS_CACHE.clear()
    _la._DUCKDB_COUNTS_CACHE.clear()


@pytest.fixture
def log_accounting_source():
    return {"name": "test_service", "service_id": "test-service-id", "logging_service_id": "test-logging-svc-id"}


@pytest.fixture
def log_accounting_client(log_accounting_source, in_memory_duckdb):
    from backend.deps import get_con as dep_get_con

    app.dependency_overrides[dep_get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: log_accounting_source
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _fake_urlopen(payload: dict):
    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return json.dumps(payload).encode()

    return lambda req, timeout=30: _Resp()


def _complete_src(**overrides) -> dict:
    base = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "logging_service_id": MOCK_SERVICE_ID}
    base |= {"bucket": "test-bucket", "prefix": "", "region": "us-east-1"}
    base |= {"access_key_id": "k", "secret_access_key": "s", "endpoint": "us-east-1.object.fastlystorage.app"}
    return {**base, **overrides}


def _run_log_accounting(log_accounting_client, svc_id: str, counts: list[tuple[int, int, int]], hours: int):
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    buckets = [(now - timedelta(hours=h_off), our_rc, fastly_rc) for h_off, our_rc, fastly_rc in counts]
    con = _meta.get_con(svc_id)
    for i, (b, our_rc, _) in enumerate(buckets):
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) VALUES (?, ?, ?, ?, ?)",
            (f"raw/file-{i}.gz", svc_id, b.strftime("%Y-%m-%d %H:%M:%S"), our_rc, 1000),
        )
    con.commit()
    fastly_payload = {
        "data": [{"start_time": int(b.timestamp()), "requests": fastly_rc} for b, _, fastly_rc in buckets]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        return log_accounting_client.get(f"/api/admin/log-accounting?hours={hours}&by=hour")


@pytest.mark.usefixtures("frozen_clock", "_clear_log_accounting_ttl_caches")
def test_log_accounting_sustained_loss_normal_drift_and_missing_key(log_accounting_client, log_accounting_source):
    from backend.routers.admin import log_accounting as _la

    svc_name = log_accounting_source["name"]
    resp = _run_log_accounting(
        log_accounting_client, svc_name, [(3, 900, 1000), (2, 900, 1000), (1, 900, 1000)], hours=4
    )
    assert resp.status_code == 200
    body = resp.json()
    tots, sl = body["totals"], body["sustained_loss"]
    assert tots["fastly_requests"] == 3000 and tots["our_rows"] == 2700 and tots["gap"] == 300
    assert sl is not None and sl["n_buckets"] == 3 and sl["total_lost_lines"] == 300

    _la._FASTLY_COUNTS_CACHE.clear()
    _la._DUCKDB_COUNTS_CACHE.clear()
    _meta.get_con(svc_name).execute("DELETE FROM ingested_files WHERE source_name = ?", (svc_name,))
    _meta.get_con(svc_name).commit()
    resp2 = _run_log_accounting(log_accounting_client, svc_name, [(2, 1020, 1000), (1, 980, 1000)], hours=3)
    assert resp2.status_code == 200 and resp2.json()["sustained_loss"] is None

    _la._FASTLY_COUNTS_CACHE.clear()
    with patch("backend.config.get_fastly_api_key", return_value=None):
        assert log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour").status_code == 400


def test_usage_logging_settings_zones_and_validation(client, tmp_path, monkeypatch):
    monkeypatch.setattr(_svc_config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(_svc_config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    r0 = client.get("/api/admin/usage-logging")
    assert r0.status_code == 200 and "enabled" in r0.json()

    for zone, expected_rate in [("MBZ100", 0.08), ("MBZ200", 0.12), ("Global", 0.18)]:
        p = client.patch("/api/admin/usage-logging", json={"billing_zone": zone})
        assert p.status_code == 200 and p.json()["billing_zone"] == zone
        assert p.json()["cdn_egress_rate_per_gb"] == expected_rate

    assert client.patch("/api/admin/usage-logging", json={"billing_zone": "INVALID_ZONE"}).status_code in (400, 422)
    rate_fields = "retention_days class_a_rate_per_1k class_b_rate_per_10k cdn_egress_rate_per_gb storage_rate_per_gb_month min_billed_days"
    for field in rate_fields.split():
        for bad_val in (0, -1):
            assert client.patch("/api/admin/usage-logging", json={field: bad_val}).status_code in (400, 422)


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_usage_log_query_export_delete_and_system_jobs(mock_ul_cfg):
    svc = "svc-pg-usage-test"
    _meta.clear_usage_log(svc)
    try:
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T10:15:00Z"):
            call = {"service": "FOS", "method": "PutObject", "path": "=cmd|' /C calc'!A0", "bytes": 512}
            call |= {"time_ms": 5.0, "caller": "+sum(1,2)", "status": "-200"}
            _meta.log_usage_calls(svc, [call], process_context="@at_context")
        app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
        try:
            with TestClient(app) as c:
                params = {"start": "2026-06-01T10:00:00Z", "end": "2026-06-01T11:00:00Z"}
                res = c.get("/api/admin/usage-log", params=params)
                assert res.status_code == 200 and res.json()["total"] == 1

                csv_res = c.get("/api/admin/usage-log/export", params=params)
                assert csv_res.status_code == 200
                rows = list(csv.reader(io.StringIO(csv_res.text)))
                assert len(rows) == 2 and rows[1][4].startswith("'=") and rows[1][7].startswith("'+")
                assert c.delete("/api/admin/usage-log").status_code == 200
        finally:
            app.dependency_overrides.pop(get_source, None)
    finally:
        _meta.clear_usage_log(svc)

    fake_job = SimpleNamespace(next_run_time=datetime(2026, 6, 2, 3, 0, 0, tzinfo=UTC))
    sched_obj = MagicMock()
    sched_obj.get_job.side_effect = lambda jid: fake_job if jid == "bot_data_refresh" else None
    job_st = {"bot_data_refresh": {"last_run_at": "2026-06-01T10:00:00Z", "status": "ok", "duration_s": 1.25}}
    with (
        patch("backend.cron.decorators.get_system_job_status", return_value=job_st),
        patch("backend.cron.scheduler.get_scheduler", return_value=sched_obj),
        TestClient(app) as c,
    ):
        r = c.get("/api/admin/system-jobs")
    assert r.status_code == 200
    assert {j["id"] for j in r.json()["jobs"]} == {"bot_data_refresh", "rdns_enrichment", "share_audit_purge"}
    for path in ("/api/admin/usage-logging", "/api/admin/usage-log", "/api/admin/system-jobs"):
        assert _is_blocked_path(path) is True


_KA, _KB = "object_storage_class_a_operations_count", "object_storage_class_b_operations_count"
_NA, _NB = "class_a_operations_count", "class_b_operations_count"
_FOS_OPS_CASES = [
    ({_KA: 1234, _KB: 5678, "object_storage": {_NA: 1, _NB: 1}}, (1234, 5678)),
    ({"object_storage": {_NA: 100, _NB: 200}}, (100, 200)),
    ({_KA: None, _KB: None}, (0, 0)),
    ({"other": 42}, (0, 0)),
]


@pytest.mark.parametrize(("record", "expected"), _FOS_OPS_CASES)
def test_extract_fos_ops_and_fastly_api_helper(record, expected):
    assert _extract_fos_ops(record) == expected
    fake_resp = MagicMock()
    fake_resp.read.return_value = b'{"data": []}'
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=fake_resp) as mock_open:
        assert _fastly_api("/services/svc1/details", "test-token") == {"data": []}
    req = mock_open.call_args[0][0]
    assert req.full_url == "https://api.fastly.com/services/svc1/details"
    assert req.get_header("Fastly-key") == "test-token"


def test_usage_prefill_operations_and_rum_breakdown(client, tmp_path, monkeypatch):
    monkeypatch.setattr(_svc_config, "CONFIGS_DIR", tmp_path / "cfgs")
    _svc_config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "api-tok"})
    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-svc-id")
    row_d = {"start_time": 1767225600, "requests": 1000, "edge_requests": 800, _KA: 100, _KB: 50}
    try:
        with _patches(
            ("backend.routers.usage.get_fastly_api_key", "api-tok"),
            ("backend.core.fastly.service.get_active_version", None),
            ("backend.routers.usage._fastly_api", {"data": [row_d]}),
        ):
            r_pre = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
            assert r_pre.status_code == 200 and "storage_rate_per_gb_month" in r_pre.json()
            assert r_pre.json()["requests_per_day"] == 1000
            r_ops = client.get(
                "/api/usage/operations", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, params={"by": "minute"}
            )
            assert r_ops.status_code == 200

        svc = "test_usage_rum_breakdown_svc"
        con = _meta.get_con(svc)
        con.execute("DELETE FROM ingested_files WHERE source_name = ?", (svc,))
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, table_name, file_date) VALUES (?, ?, ?, ?, ?, ?)",
            ("s3://bucket/2026-08-05.parquet", svc, 10, 100, "client_vitals", "2026-08-05"),
        )
        con.commit()
        app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
        with patch("backend.config.load_usage_logging_config", return_value={"class_a_rate_per_1k": 0.01}):
            r_rum = client.get(
                "/api/usage/rum-breakdown", params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-07T00:00:00Z"}
            )
            assert r_rum.status_code == 200 and r_rum.json()["total_beacons"] == 10
        con.execute("DELETE FROM ingested_files WHERE source_name = ?", (svc,))
        con.commit()
    finally:
        app.dependency_overrides.pop(get_source, None)
