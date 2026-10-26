"""Tests for Admin Task Queue, Ingest Ledger, ClickHouse, Compaction, Downloads, Quarantine, and Query Monitor APIs."""

from __future__ import annotations

import importlib
import json
import os
import time as _time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from backend.core.query_attribution import Attribution, current_attribution
from backend.routers.admin_queries import _admin_id_from_request
from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from backend.core import metadata as _meta
from backend.core import metric_snapshots
from backend.core.clickhouse_publication import PAYLOAD_COLUMNS, FosArtifacts
from backend.core.metadata.base import get_con
from backend.core.query_registry import query_registry
from backend.deps import get_source, require_admin
from backend.main import app
from backend.routers import admin_queries as mod
from backend.routers.admin import clickhouse as api
from backend.routers.admin import router as admin_router
from backend.utils.remote_access import _is_blocked_path
from tests._helpers.config_fakes import fake_mutate_config as _fake_mutate
from tests._helpers.sources import TinyFos
from tests.core.test_clickhouse import fixture_manifest


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
    fake_req = SimpleNamespace(
        state=SimpleNamespace(analyst_session=SimpleNamespace(session_id="sess_analyst")),
        headers={},
        client=SimpleNamespace(host="203.0.113.55"),
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


class TestCeleryStatusHighScaleAndStandard:
    def test_high_scale_celery_status_full_payload(self, client):
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

        with (
            patch("backend.celery_app.app.control.inspect") as mock_inspect,
            patch("backend.celery_queue._get_redis", return_value=_mock_redis(queues)),
            patch("backend.config.list_configs", return_value=[{"service_id": "svc-a"}, {"service_id": "svc-b"}]),
            patch("backend.core.metadata.base.get_con", side_effect=_fake_get_con),
        ):
            mock_i = MagicMock()
            mock_i.active.return_value = {
                "celery@worker-1": [
                    {
                        "id": "task-uuid-1",
                        "name": "backend.tasks.ingest.ingest_batch",
                        "args": ["svc-a", 12],
                        "time_start": 1700000000.5,
                    }
                ],
                "celery@worker-2": [],
            }
            mock_i.stats.return_value = {
                "celery@worker-1": {"pool": {"max-concurrency": 8}},
                "celery@worker-2": {"pool": {"max-concurrency": 8}},
            }
            mock_i.registered.return_value = {
                "celery@worker-1": ["backend.tasks.ingest.ingest_batch"],
                "celery@worker-2": ["backend.tasks.ingest.ingest_batch"],
            }
            mock_i.scheduled.return_value = {"celery@worker-1": [], "celery@worker-2": []}
            mock_inspect.return_value = mock_i

            resp = client.get("/api/admin/celery/status", headers={"X-Service-Id": "svc-a"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["broker_reachable"] is True and data["workers"]["count"] == 2
            assert data["workers"]["active_tasks"]["celery@worker-1"][0]["id"] == "task-uuid-1"
            assert data["workers"]["active_tasks"]["celery@worker-2"] == []
            assert data["queues"] == {"celery": 3, "q.control": 1, "q.custom": 2, "q.ingest": 14}
            assert data["ledger"] == {
                "discovered": 4,
                "claimed": 2,
                "committed": 200,
                "quarantined": 1,
                "dead_letter": 3,
            }

    def test_standard_mode_idle_broker_fallback(self, client):
        with (
            patch("backend.celery_app.app.control.inspect") as mock_inspect,
            patch("backend.celery_queue._get_redis", return_value=_mock_redis({})),
            patch("backend.config.list_configs", return_value=[]),
        ):
            mock_i = MagicMock()
            mock_i.active.return_value = mock_i.stats.return_value = mock_i.registered.return_value = (
                mock_i.scheduled.return_value
            ) = {}
            mock_inspect.return_value = mock_i

            resp = client.get("/api/admin/celery/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["broker_reachable"] is True and data["workers"]["count"] == 0
            assert data["queues"] == {"celery": 0, "q.control": 0, "q.ingest": 0} and data["ledger"] == {}

    def test_standard_mode_unreachable_broker_fallback(self, client):
        broken_redis = MagicMock()
        broken_redis.scan_iter.side_effect = ConnectionError("Valkey connection refused")
        with (
            patch("backend.celery_app.app.control.inspect", side_effect=RuntimeError("Broker connection timeout")),
            patch("backend.celery_queue._get_redis", return_value=broken_redis),
            patch("backend.config.list_configs", return_value=[]),
        ):
            resp = client.get("/api/admin/celery/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["broker_reachable"] is False and data["workers"]["count"] == 0
            assert "Broker connection timeout" in data["workers"]["error"]
            assert data["queues"] == {} and data["ledger"] == {}

    def test_standard_mode_inprocess_scheduler_and_ingest_telemetry(self, client):
        fake_job = SimpleNamespace(
            id="log_discovery_svc-a",
            name="log_discovery_svc-a",
            trigger="interval[0:00:10]",
            next_run_time=SimpleNamespace(timestamp=lambda: 1700000010.0),
        )
        fake_scheduler = SimpleNamespace(scheduler=SimpleNamespace(get_jobs=lambda: [fake_job]))
        with (
            patch(
                "backend.celery_queue._get_redis",
                side_effect=ConnectionError("Celery broker not configured in standard mode"),
            ),
            patch("backend.config.is_high_scale_mode", return_value=False),
            patch("backend.config.list_configs", return_value=[{"service_id": "svc-a"}]),
            patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
            patch(
                "backend.celery_status.inprocess_active_runs_entries",
                return_value=[{"run_id": 101, "service_id": "svc-a", "task": "sync", "started_at": 1700000000.0}],
            ),
            patch(
                "backend.celery_status.standard_ingest_summary",
                return_value={"ingested_files": 420, "quarantined_items": 1},
            ),
            patch("backend.celery_status.ingest_ledger_summary", return_value={"committed": 420, "quarantined": 1}),
        ):
            resp = client.get("/api/admin/celery/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["broker_reachable"] is False and data["deployment_mode"] == "standard"
            assert data["inprocess_schedules"] == [
                {"name": "log_discovery_svc-a", "trigger": "interval[0:00:10]", "next_run_at": 1700000010.0}
            ]
            assert data["inprocess_active_runs"] == [
                {"run_id": 101, "service_id": "svc-a", "task": "sync", "started_at": 1700000000.0}
            ]
            assert data["standard_ingest"] == {"ingested_files": 420, "quarantined_items": 1}
            assert data["ledger"] == {"committed": 420, "quarantined": 1}


class TestQueueMetricHistoryBatch:
    def test_metric_history_batch_returns_celery_series(self, client):
        metric_snapshots.record_snapshot("celery_queue_depth", 7.0)
        metric_snapshots.record_snapshot("celery_active_tasks", 2.0)
        resp = client.get("/api/admin/metric-history/batch?since=1h")
        assert resp.status_code == 200
        body = resp.json()
        assert body["series"]["celery_queue_depth"][-1]["value"] == 7.0
        assert body["series"]["celery_active_tasks"][-1]["value"] == 2.0


class TestCeleryAnalystRbacRejection:
    @pytest.mark.parametrize(
        "method,path",
        [
            ("GET", "/api/admin/celery/status"),
            ("GET", "/api/admin/metric-history/batch?since=1h"),
            ("GET", "/api/admin/events/stream?channels=celery-status"),
        ],
    )
    def test_analyst_session_receives_403_on_queue_admin_routes(self, client, method, path):
        _assert_analyst_rbac_403(client, method, path, skip_http_for=("/api/admin/events/stream",))


class TestIngestLedgerInspectorEndpoints:
    def test_list_retry_and_purge_ingest_ledger_entries(self, client):
        sid = "svc-ledger-test"
        con = get_con(sid)
        cur = con.cursor()
        cur.execute("DELETE FROM ingest_ledger WHERE service_id = ?", (sid,))
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, size_bytes, status, attempts, last_error, error_kind, claimed_at, discovered_at, originating_task, originating_run_id) VALUES (?, ?, ?, 'dead_letter', 1, ?, 'object_missing', 1729400010.0, 1729400000.0, 'rum_discovery', 99)",
            (
                sid,
                "raw/rum/year=2026/month=10/day=20/hour=04/minute=28/dead-rum.log.gz",
                1024,
                "object missing from FOS (already ingested+deleted, or expired)",
            ),
        )
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, size_bytes, status, attempts, last_error, error_kind, claimed_at, discovered_at, originating_task, originating_run_id) VALUES (?, ?, ?, 'quarantined', 3, ?, 'corrupt_gzip', 1729400020.0, 1729400005.0, 'log_discovery', 100)",
            (sid, "raw/request/year=2026/month=10/day=20/hour=04/minute=29/bad-req.log.gz", 2048, "Not a gzipped file"),
        )
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, size_bytes, status, attempts, committed_at, discovered_at) VALUES (?, ?, ?, 'committed', 1, 1729400030.0, 1729400000.0)",
            (sid, "raw/request/year=2026/month=10/day=20/hour=04/minute=30/good-req.log.gz", 4096),
        )
        con.commit()

        try:
            with patch("backend.config.list_configs", return_value=[{"service_id": sid}]):
                resp = client.get("/api/admin/celery/ledger")
                assert resp.status_code == 200
                body = resp.json()
                assert body["total"] == 2 and len(body["items"]) == 2
                by_key = {item["object_key"]: item for item in body["items"]}
                rum_key = "raw/rum/year=2026/month=10/day=20/hour=04/minute=28/dead-rum.log.gz"
                rum_item = by_key[rum_key]
                assert (
                    rum_item["source_type"] == "rum"
                    and rum_item["status"] == "dead_letter"
                    and rum_item["error_kind"] == "object_missing"
                )
                assert "object missing from FOS" in rum_item["last_error"]
                assert rum_item["originating_task"] == "rum_discovery" and rum_item["originating_run_id"] == 99

                req_item = by_key["raw/request/year=2026/month=10/day=20/hour=04/minute=29/bad-req.log.gz"]
                assert (
                    req_item["source_type"] == "request"
                    and req_item["status"] == "quarantined"
                    and req_item["error_kind"] == "corrupt_gzip"
                )

                with (
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_req_delay,
                    patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_rum_delay,
                ):
                    retry_resp = client.post(
                        "/api/admin/celery/ledger/retry", json={"items": [{"service_id": sid, "object_key": rum_key}]}
                    )
                    assert retry_resp.status_code == 200
                    retry_data = retry_resp.json()
                    assert retry_data["ok"] is True and retry_data["affected"] == 1 and retry_data["dispatched"] == 1
                    mock_rum_delay.assert_called_once_with(sid, [rum_key])
                    mock_req_delay.assert_not_called()

                after_retry = client.get("/api/admin/celery/ledger").json()
                assert after_retry["total"] == 1 and after_retry["items"][0]["status"] == "quarantined"

                purge_resp = client.post("/api/admin/celery/ledger/purge", json={"status": "problem"})
                assert purge_resp.status_code == 200
                assert purge_resp.json()["ok"] is True and purge_resp.json()["affected"] == 1

                after_purge = client.get("/api/admin/celery/ledger").json()
                assert after_purge["total"] == 0 and after_purge["items"] == []

                all_rows = client.get("/api/admin/celery/ledger?status=all").json()
                assert all_rows["total"] == 2 and {r["status"] for r in all_rows["items"]} == {
                    "discovered",
                    "committed",
                }
        finally:
            cur.execute("DELETE FROM ingest_ledger WHERE service_id = ?", (sid,))
            con.commit()


def test_task_routes_match_registered_tasks():
    from backend.celery_app import app as celery_app

    for mod_name in celery_app.conf.include:
        importlib.import_module(mod_name)
    registered = set(celery_app.tasks.keys())
    for pattern in celery_app.conf.task_routes:
        if pattern.endswith("*"):
            assert any(t.startswith(pattern[:-1]) for t in registered), (
                f"task_routes pattern {pattern!r} matches no registered task"
            )
        else:
            assert pattern in registered, f"task_routes key {pattern!r} is not a registered task"


def test_worker_queues_cover_all_routed_queues():
    from backend.celery_app import app as celery_app
    from backend.celery_status import KNOWN_QUEUES

    routed = {spec["queue"] for spec in celery_app.conf.task_routes.values()} | {
        celery_app.conf.task_default_queue or "celery"
    }
    assert routed <= set(KNOWN_QUEUES), f"queues {routed - set(KNOWN_QUEUES)} not covered by KNOWN_QUEUES"


# ── Merged from tests/routers/test_admin_clickhouse.py ──
SERVICE = "TestPrototype"
STATUS = "/api/admin/clickhouse/status"
REPLAY = "/api/admin/clickhouse/replay"


@pytest.fixture
def boundary(monkeypatch):
    cfg = {"service_id": SERVICE, "access_level": "read_write"}
    monkeypatch.setattr(api.config, "load_config", lambda sid: cfg if sid == SERVICE else None)
    store, ch, objects, audit = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    store.admin_status.return_value = {
        "schema_version": api.CLICKHOUSE_SCHEMA_VERSION,
        "publication_counts": {"pending": 1, "claimed": 0, "failed": 0, "published": 2},
        "oldest_pending_age_seconds": 12.5,
        "last_published_at": "2026-09-07T00:00:00+00:00",
        "active_generation": None,
        "expired_dataset_count": 1,
    }
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
        schema = paths[path][verb]["responses"]["200"]["content"]["application/json"]["schema"]
        assert schema["$ref"].endswith("/" + model)


def test_status_disabled_is_unknown_not_zero(boundary, monkeypatch):
    client, store, *_ = boundary
    monkeypatch.setattr(api, "get_clickhouse_client", lambda: None)
    response = client.get(STATUS, params={"service_id": SERVICE})
    assert response.status_code == 200
    data = response.json()
    assert data["enabled"] is False and data["health"] == "disabled"
    for key in (
        "schema_version",
        "publication_counts",
        "oldest_pending_age_seconds",
        "last_published_at",
        "active_generation",
        "expired_dataset_count",
    ):
        assert data[key] is None
    store.admin_status.assert_not_called()


@pytest.mark.parametrize("failure", ["client", "manifest", "schema"])
def test_status_unavailable_is_sanitized_503(boundary, monkeypatch, failure):
    client, store, ch, *_ = boundary
    if failure == "client":
        ch.health.side_effect = RuntimeError("postgresql://private-secret@example.invalid")
    elif failure == "manifest":
        store.admin_status.side_effect = RuntimeError("s3://private-bucket")
    else:
        store.admin_status.return_value["schema_version"] = 99
    response = client.get(STATUS, params={"service_id": SERVICE})
    assert response.status_code == 503
    assert response.json()["health"] == "unavailable" and response.json()["enabled"] is True
    assert "private" not in response.text
    if failure != "schema":
        assert response.json()["publication_counts"] is None


def test_status_is_service_scoped_and_preserves_expired_flag(boundary):
    client, store, *_ = boundary
    store.admin_status.return_value["active_generation"] = {
        "generation": "g",
        "dataset_id": "d",
        "coverage_start": "2026-09-01T00:00:00+00:00",
        "coverage_end": "2026-09-02T00:00:00+00:00",
        "expires_at": "2026-09-03T00:00:00+00:00",
        "expired": True,
        "target_matches": False,
        "coverage_age_seconds": 500000,
    }
    response = client.get(STATUS, params={"service_id": SERVICE})
    assert response.status_code == 200
    assert (
        response.json()["active_generation"]["expired"] is True
        and response.json()["oldest_pending_age_seconds"] == 12.5
    )
    store.admin_status.assert_called_once_with(SERVICE, "target")


@pytest.mark.parametrize("sid", ["missing", "../secrets"])
def test_missing_or_invalid_service_never_touches_storage(boundary, sid):
    client, store, ch, objects, audit, _ = boundary
    assert client.get(STATUS, params={"service_id": sid}).status_code == 404
    assert client.post(REPLAY, json={"service_id": sid, "dataset_id": "d"}).status_code == 404
    assert not store.mock_calls and not ch.mock_calls
    objects.assert_not_called()
    audit.assert_not_called()


def test_configuration_failure_sanitized(boundary, monkeypatch):
    client, *_ = boundary
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
    assert not store.mock_calls
    objects.assert_not_called()
    audit.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        {"limit": 0},
        {"limit": 101},
        {"limit": 1001},
        {"limit": True},
        {"limit": 1.5},
        {"generation": "also"},
        {"dataset_id": None},
        {"dataset_id": "../other"},
        {"export": True},
    ],
)
def test_replay_invalid_arguments_never_reach_manifest(boundary, overrides):
    client, store, _, objects, audit, _ = boundary
    response = client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d", **overrides})
    assert response.status_code == 422 and not store.mock_calls
    objects.assert_not_called()
    audit.assert_not_called()


@pytest.mark.parametrize("reference", [{"dataset_id": "dataset"}, {"generation": "g"}])
def test_dry_run_validates_references_without_mutators_or_fos(boundary, monkeypatch, reference):
    client, store, _, objects, audit, _ = boundary
    rebuild, resume = MagicMock(), MagicMock()
    monkeypatch.setattr(api, "full_rebuild", rebuild)
    monkeypatch.setattr(api, "replay_unpublished", resume)
    response = client.post(REPLAY, json={"service_id": SERVICE, **reference})
    assert response.status_code == 200
    assert (
        response.json()["dry_run"] is True
        and response.json()["attempted"] == 0
        and response.json()["planned_artifacts"] == 1
    )
    assert [call[0] for call in store.mock_calls] == ["replay_preview"]
    store.replay_preview.assert_called_once_with(
        SERVICE,
        dataset_id=reference.get("dataset_id"),
        generation=reference.get("generation"),
        target="target",
        limit=100,
    )
    objects.assert_not_called()
    audit.assert_not_called()
    rebuild.assert_not_called()
    resume.assert_not_called()


@pytest.mark.parametrize(
    "error,code", [(LookupError("missing"), 404), (ValueError("expired"), 409), (RuntimeError("private DSN"), 503)]
)
def test_replay_reference_failures_are_safe(boundary, error, code):
    client, store, _, objects, audit, _ = boundary
    store.replay_preview.side_effect = error
    response = client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d"})
    assert response.status_code == code and "private" not in response.text
    objects.assert_not_called()
    audit.assert_not_called()


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
        return (
            [{"n": len(batch.rows)}]
            if "SELECT count()" in sql
            else [dict(zip(PAYLOAD_COLUMNS, row, strict=True)) for row in batch.rows]
        )

    ch.execute.side_effect = execute
    reference = {"generation": "g"} if resume else {"dataset_id": "dataset"}
    response = client.post(REPLAY, json={"service_id": SERVICE, **reference, "dry_run": False, "limit": 1})
    assert response.status_code == 200, response.text
    assert response.json()["published"] == 1 and response.json()["activated"] is True
    assert response.json()["generation"] == ("g" if resume else "new-generation")
    assert fos.puts == writes
    ch.insert_rows.assert_called_once()
    assert "s3://" not in response.text and "test-bucket" not in response.text
    assert audit.call_args.kwargs["details"]["published"] == 1
    assert audit.call_args.kwargs["details"]["action"] == ("resume" if resume else "rebuild")
    if resume:
        store.begin_generation.assert_not_called()
    else:
        store.begin_generation.assert_called_once_with(SERVICE, "dataset", "target")


def test_apply_failure_is_audited_without_echoing_error(boundary, monkeypatch):
    client, _, _, _, audit, _ = boundary
    monkeypatch.setattr(api.config, "config_to_source", lambda config: {})
    monkeypatch.setattr(api, "full_rebuild", MagicMock(side_effect=RuntimeError("s3://private-secret")))
    response = client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d", "dry_run": False})
    assert response.status_code == 503 and "private" not in response.text
    assert response.json()["detail"]["error"] == "replay_unavailable"
    assert audit.call_args.kwargs["details"]["phase"] == "failed"


def test_apply_error_never_injects_fos_debug_urls(boundary, monkeypatch):
    client, *_ = boundary
    monkeypatch.setenv("DEBUG_RESPONSES", "true")
    monkeypatch.setattr(api.config, "config_to_source", lambda config: {})
    monkeypatch.setattr(api, "full_rebuild", MagicMock(side_effect=RuntimeError("s3://private-secret")))
    debug_calls = MagicMock(return_value=[{"url": "s3://private-secret"}])
    monkeypatch.setattr("backend.utils.telemetry.get_tracked_calls", debug_calls)
    response = client.post(
        REPLAY, json={"service_id": SERVICE, "dataset_id": "d", "dry_run": False}, headers={"x-debug-responses": "1"}
    )
    assert response.status_code == 503 and "private" not in response.text
    assert response.json()["_debug_calls"] == []
    debug_calls.assert_not_called()


def test_replay_missing_both_dataset_id_and_generation_rejected(boundary):
    client, store, _, objects, audit, _ = boundary
    response = client.post(REPLAY, json={"service_id": SERVICE})
    assert response.status_code == 422 and not store.mock_calls
    objects.assert_not_called()
    audit.assert_not_called()


def test_analyst_path_b_rejected_with_403_admin_only(monkeypatch):
    cfg = {"service_id": SERVICE, "access_level": "read_write"}
    monkeypatch.setattr(api.config, "load_config", lambda sid: cfg if sid == SERVICE else None)
    store, ch = MagicMock(), MagicMock()
    monkeypatch.setattr(api, "PgManifest", MagicMock(return_value=store))
    monkeypatch.setattr(api, "get_clickhouse_client", lambda: ch)

    sub_app = FastAPI()

    @sub_app.middleware("http")
    async def _attach_analyst_session(request, call_next):
        request.state.analyst_session = MagicMock(service_ids=[SERVICE])
        return await call_next(request)

    sub_app.include_router(api.router)
    with TestClient(sub_app) as client:
        for res in (
            client.get(STATUS, params={"service_id": SERVICE}),
            client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d"}),
        ):
            assert res.status_code == 403 and res.json()["detail"]["error"] == "admin_only"
        assert not store.mock_calls and not ch.mock_calls


# ── Merged from tests/routers/test_admin_compaction.py ──
def test_optimize_now_forwards_to_iceberg_optimize_table(client):
    fake_result = {"files_rewritten": 4, "files_added": 1, "bytes_rewritten": 12345}
    with patch("backend.core.iceberg.optimize_table", return_value=fake_result) as m:
        resp = client.post("/api/admin/optimize-now")
    assert resp.status_code == 200
    body = resp.json()
    assert body["files_rewritten"] == 4 and body["files_added"] == 1 and body["bytes_rewritten"] == 12345
    assert m.call_args.kwargs.get("min_files_per_partition") is None


def test_optimize_now_passes_min_files_when_specified(client):
    with patch("backend.core.iceberg.optimize_table", return_value={"files_rewritten": 0}) as m:
        resp = client.post("/api/admin/optimize-now?min_files=1")
    assert resp.status_code == 200 and m.call_args.kwargs.get("min_files_per_partition") == 1


def test_local_compact_now_forwards_default_min_files_three(client):
    with patch("backend.core.local_compaction.compact_local_partitions", return_value={"rewritten": 2}) as m:
        resp = client.post("/api/admin/local-compact-now")
    assert resp.status_code == 200 and resp.json()["rewritten"] == 2
    assert m.call_args.kwargs["min_files_per_partition"] == 3 and m.call_args.kwargs["dry_run"] is False


def test_local_compact_now_min_files_zero_is_allowed(client):
    with patch("backend.core.local_compaction.compact_local_partitions", return_value={"rewritten": 0}) as m:
        resp = client.post("/api/admin/local-compact-now?min_files=0&dry_run=true")
    assert resp.status_code == 200
    assert m.call_args.kwargs["min_files_per_partition"] == 0 and m.call_args.kwargs["dry_run"] is True


def test_local_compact_now_rejects_negative_min_files(client):
    assert client.post("/api/admin/local-compact-now?min_files=-1").status_code == 422


def test_compaction_stats_returns_local_compaction_snapshot(client):
    fake = {"partitions": 100, "partitions_above_3": 5, "avg_files_per_partition": 1.8}
    with patch("backend.core.local_compaction.compaction_stats", return_value=fake):
        resp = client.get("/api/admin/compaction-stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["partitions"] == 100 and body["partitions_above_3"] == 5 and body["avg_files_per_partition"] == 1.8


def test_metadata_retention_patch_writes_config_and_audit(client):
    saved: dict = {}
    audit: list[dict] = []
    base = {"name": "svc", "service_id": "svc"}
    with (
        patch("backend.config.load_config", return_value=base),
        patch("backend.config.mutate_config", side_effect=_fake_mutate(base, saved=saved)),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit", side_effect=lambda **kw: audit.append(kw)),
    ):
        resp = client.patch(
            "/api/admin/metadata-retention", json={"usage_log_days": 14, "ingested_files_days": 30, "cron_runs_days": 7}
        )
    assert resp.status_code == 200
    body = resp.json()
    assert (
        body["retention"]["usage_log_days"] == 14
        and body["retention"]["ingested_files_days"] == 30
        and body["retention"]["cron_runs_days"] == 7
    )
    assert saved["metadata_retention"] == {"usage_log_days": 14, "ingested_files_days": 30, "cron_runs_days": 7}
    assert len(audit) == 1 and audit[0]["event_type"] == "metadata_retention_update"


def test_metadata_retention_patch_merges_onto_freshly_read_retention(client):
    saved: dict = {}
    base = {"name": "svc", "service_id": "svc", "metadata_retention": {"usage_log_days": 21, "cron_runs_days": 9}}
    with (
        patch("backend.config.load_config", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.config.mutate_config", side_effect=_fake_mutate(base, saved=saved)),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = client.patch("/api/admin/metadata-retention", json={"ingested_files_days": 30})
    assert resp.status_code == 200
    assert saved["metadata_retention"] == {"usage_log_days": 21, "cron_runs_days": 9, "ingested_files_days": 30}


def test_metadata_retention_patch_404_when_service_missing(client):
    with patch("backend.config.load_config", return_value=None):
        resp = client.patch("/api/admin/metadata-retention", json={"usage_log_days": 7})
    assert resp.status_code == 404 and resp.json()["detail"]["error"] == "Service not found"


def test_metadata_retention_clamps_negative_to_zero(client):
    saved: dict = {}
    base = {"name": "svc", "service_id": "svc"}
    with (
        patch("backend.config.load_config", return_value=base),
        patch("backend.config.mutate_config", side_effect=_fake_mutate(base, saved=saved)),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = client.patch(
            "/api/admin/metadata-retention", json={"usage_log_days": -5, "cron_runs_days": "not-a-number"}
        )
    assert resp.status_code == 200
    assert saved["metadata_retention"]["usage_log_days"] == 0 and saved["metadata_retention"]["cron_runs_days"] == 0


def test_metadata_retention_force_disables_ingested_files_days_when_dedup_inactive(client):
    saved: dict = {}
    base = {"name": "svc", "service_id": "svc"}
    with (
        patch("backend.config.load_config", return_value=base),
        patch("backend.config.mutate_config", side_effect=_fake_mutate(base, saved=saved)),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=False),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = client.patch("/api/admin/metadata-retention", json={"ingested_files_days": 30})
    assert resp.status_code == 200 and saved["metadata_retention"]["ingested_files_days"] == 0


def test_metadata_retention_audit_failure_does_not_break_patch(client):
    base = {"name": "svc", "service_id": "svc"}
    with (
        patch("backend.config.load_config", return_value=base),
        patch("backend.config.mutate_config", side_effect=_fake_mutate(base)),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit", side_effect=RuntimeError("audit-boom")),
    ):
        assert client.patch("/api/admin/metadata-retention", json={"usage_log_days": 7}).status_code == 200


def test_metadata_storage_returns_stats_plus_retention_and_lock(client):
    fake_stats = {"by_table": {"usage_log": {"rows": 1000, "bytes": 50000}}}
    with (
        patch("backend.core.metadata.get_metadata_storage_stats", return_value=fake_stats),
        patch(
            "backend.config.load_config",
            return_value={"name": "svc", "service_id": "svc", "metadata_retention": {"usage_log_days": 14}},
        ),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
    ):
        resp = client.get("/api/admin/metadata-storage")
    assert resp.status_code == 200
    body = resp.json()
    assert (
        body["by_table"]["usage_log"]["rows"] == 1000
        and body["retention"]["usage_log_days"] == 14
        and body["ingested_files_locked"] is False
    )


def test_metadata_storage_locks_when_dedup_inactive(client):
    with (
        patch("backend.core.metadata.get_metadata_storage_stats", return_value={}),
        patch("backend.config.load_config", return_value={"name": "svc"}),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=False),
    ):
        resp = client.get("/api/admin/metadata-storage")
    assert resp.status_code == 200 and resp.json()["ingested_files_locked"] is True


def _collect_sse_events(body: bytes) -> list[dict]:
    events: list[dict] = []
    for line in body.decode("utf-8", errors="replace").splitlines():
        if line.startswith("data: "):
            try:
                events.append(json.loads(line[len("data: ") :]))
            except json.JSONDecodeError:
                continue
    return events


def test_metadata_cleanup_streams_done_event_on_success(client):
    fake_result = {"deleted": {"usage_log": 100, "cron_runs": 10}, "vacuumed": True}
    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {"usage_log_days": 7}}),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.core.metadata.cleanup_metadata", return_value=fake_result),
    ):
        resp = client.post("/api/admin/metadata-cleanup")
    assert resp.status_code == 200 and resp.headers["content-type"].startswith("text/event-stream")
    events = _collect_sse_events(resp.content)
    assert events
    done = events[-1]
    assert (
        done["type"] == "done" and done["result"]["vacuumed"] is True and done["result"]["deleted"]["usage_log"] == 100
    )
    assert "Trimmed 110 rows" in done["message"]


def test_metadata_cleanup_streams_done_with_zero_rows_summary(client):
    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {}}),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.core.metadata.cleanup_metadata", return_value={"deleted": {}, "vacuumed": False}),
    ):
        resp = client.post("/api/admin/metadata-cleanup")
    done = _collect_sse_events(resp.content)[-1]
    assert done["type"] == "done" and done["message"] == "No rows older than retention windows."


def test_metadata_cleanup_handles_start_cron_run_failure(client):
    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {}}),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("simulated SQLite locked")),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.core.metadata.cleanup_metadata") as mock_cleanup,
    ):
        resp = client.post("/api/admin/metadata-cleanup")
    events = _collect_sse_events(resp.content)
    assert any(e["type"] == "error" for e in events) and not any(e.get("type") == "done" for e in events)
    assert "simulated SQLite locked" in next(e for e in events if e["type"] == "error")["message"]
    mock_cleanup.assert_not_called()
    assert mock_log.called and mock_log.call_args.kwargs.get("run_id") is None


def test_metadata_cleanup_streams_error_event_when_cleanup_raises(client):
    log_calls: list = []
    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {}}),
        patch("backend.core.duckdb.start_cron_run", return_value=99),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch("backend.core.metadata.cleanup_metadata", side_effect=RuntimeError("simulated DB lock")),
    ):
        resp = client.post("/api/admin/metadata-cleanup")
    events = _collect_sse_events(resp.content)
    assert any(e["type"] == "error" for e in events) and not any(e.get("type") == "done" for e in events)
    assert "simulated DB lock" in next(e for e in events if e["type"] == "error")["message"]
    assert log_calls and log_calls[0][1].get("run_id") == 99 and log_calls[0][0][3] == "error"


def test_metadata_cleanup_sets_no_buffering_headers(client):
    with (
        patch("backend.config.load_config", return_value={}),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.core.metadata.cleanup_metadata", return_value={"deleted": {}, "vacuumed": False}),
    ):
        resp = client.post("/api/admin/metadata-cleanup")
    cache_control = resp.headers.get("cache-control", "")
    assert (
        ("no-store" in cache_control or "no-cache" in cache_control)
        and resp.headers.get("x-accel-buffering") == "no"
        and resp.headers.get("connection") == "keep-alive"
    )


_ROLLUP_BACKFILL_MAP = {
    "backfill_slow_urls_bundles": "slow_urls",
    "backfill_origin_summary_bundles": "origin_summary",
    "backfill_origin_dims_bundles": "origin_dims",
    "backfill_origin_latency_ts_bundles": "origin_latency_ts",
    "backfill_network_rtt_bundles": "network_rtt",
    "backfill_network_speed_bundles": "network_speed",
    "backfill_network_quality_bundles": "network_quality",
    "backfill_verified_bots_ts_bundles": "verified_bots_ts",
    "backfill_perf_latency_bundles": "perf_latency",
    "backfill_pop_health_bundles": "pop_health",
    "backfill_security_dims_bundles": "security_dims",
    "backfill_security_proxies_bundles": "security_proxies",
    "backfill_perf_dims_bundles": "perf_ttl_dist",
    "backfill_wellknown_bots_rollup": "wellknown_bots",
    "backfill_ngwaf_bots_bundles": "ngwaf_bots",
    "backfill_ngwaf_bots_ts_bundles": "ngwaf_bots_ts",
    "backfill_overview_bundles": "overview",
    "backfill_network_summary_bundles": "network_summary",
}
_ROLLUP_COMPACT_MAP = {
    "compact_origin_summary_closed_days_to_daily": "origin_summary_days",
    "compact_origin_dims_closed_days_to_daily": "origin_dims_days",
    "compact_origin_latency_ts_closed_days_to_daily": "origin_latency_ts_days",
    "compact_network_rtt_closed_days_to_daily": "network_rtt_days",
    "compact_network_speed_closed_days_to_daily": "network_speed_days",
    "compact_network_quality_closed_days_to_daily": "network_quality_days",
    "compact_verified_bots_ts_closed_days_to_daily": "verified_bots_ts_days",
    "compact_perf_latency_closed_days_to_daily": "perf_latency_days",
    "compact_pop_health_closed_days_to_daily": None,
    "compact_security_dims_closed_days_to_daily": "security_dims_days",
    "compact_security_proxies_closed_days_to_daily": "security_proxies_days",
    "compact_perf_dims_closed_days_to_daily": "perf_ttl_dist_days",
    "compact_ngwaf_bots_closed_days_to_daily": "ngwaf_bots_days",
    "compact_ngwaf_bots_ts_closed_days_to_daily": "ngwaf_bots_ts_days",
    "compact_overview_closed_days_to_daily": "overview_days",
}


def test_backfill_bundle_rollups_success(client):
    @contextmanager
    def mock_get_connection(*args, **kwargs):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchone.return_value = (1,)
        yield mock_con

    with (
        patch.multiple(
            "backend.core.rollups",
            **{name: MagicMock(return_value=1) for name in [*_ROLLUP_BACKFILL_MAP, *_ROLLUP_COMPACT_MAP]},
        ),
        patch("backend.core.duckdb.get_connection", mock_get_connection),
        patch("backend.core.rollups.rum.recompute_rum_aggregates", MagicMock()),
    ):
        resp = client.post("/api/admin/backfill-bundle-rollups")
        assert resp.status_code == 200
        body = resp.json()
        assert body["slow_urls"] == 1 and body["origin_summary"] == 1 and body["origin_summary_days"] == 1


def test_backfill_bundle_rollups_response_maps_each_backfiller_and_compactor_to_its_key(client):
    all_Rollup_fns = {**_ROLLUP_BACKFILL_MAP, **_ROLLUP_COMPACT_MAP}
    values = {name: 100 + i for i, name in enumerate(all_Rollup_fns)}

    @contextmanager
    def mock_get_connection(*args, **kwargs):
        yield MagicMock()

    with (
        patch.multiple("backend.core.rollups", **{n: MagicMock(return_value=v) for n, v in values.items()}),
        patch("backend.core.rollups.rum.recompute_rum_aggregates", MagicMock()),
        patch("backend.core.duckdb.get_connection", mock_get_connection),
    ):
        resp = client.post("/api/admin/backfill-bundle-rollups")
    assert resp.status_code == 200
    body = resp.json()
    for name, key in all_Rollup_fns.items():
        if key is not None:
            assert body[key] == values[name], f"{key} should come from {name}"


def test_rebuild_time_series_rollups_rebuilds_only_requested_closed_hours(client):
    current_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    hours = [(current_hour - timedelta(hours=offset)).strftime("%Y-%m-%d-%H") for offset in (3, 2, 1)]
    with (
        patch("backend.routers.admin.compaction.build_time_series_bundles", return_value=len(hours)) as rebuild,
        patch("backend.core.rollups.recompute.recompute_touched_hours") as recompute,
    ):
        response = client.post("/api/admin/rebuild-time-series-rollups", json={"hours": hours})
    assert response.status_code == 200
    assert response.json()["requested_hours"] == hours and response.json()["rebuilt"] == len(hours)
    rebuild.assert_called_once()
    assert rebuild.call_args.args[2] == hours
    recompute.assert_not_called()


def test_rebuild_time_series_rollups_rejects_invalid_or_future_hours(client):
    for hours in (["not-an-hour"], ["2099-01-01-00"]):
        assert client.post("/api/admin/rebuild-time-series-rollups", json={"hours": hours}).status_code == 400
    closed_hour = (datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)).strftime(
        "%Y-%m-%d-%H"
    )
    assert (
        client.post("/api/admin/rebuild-time-series-rollups", json={"hours": [closed_hour, closed_hour]}).status_code
        == 400
    )


def test_rebuild_time_series_rollups_rejects_analyst_path_b():
    analyst_app = FastAPI()

    @analyst_app.middleware("http")
    async def stamp_analyst(request: Request, call_next):
        request.state.analyst_session = SimpleNamespace(
            session_id="sess-analyst-1", service_ids=["svc-a"], email="analyst@example.com"
        )
        return await call_next(request)

    analyst_app.include_router(admin_router)
    with TestClient(analyst_app) as analyst_client:
        response = analyst_client.post("/api/admin/rebuild-time-series-rollups", json={"hours": ["2026-10-24-04"]})
    assert response.status_code == 403 and response.json()["detail"]["error"] == "admin_only"


# ── Merged from tests/routers/test_admin_downloads.py ──
def test_download_file_missing_key(client, override_source):
    response = client.get("/api/download?key=")
    assert response.status_code == 400 and response.json()["detail"]["error"] == "Missing key parameter"


def test_download_file_invalid_key_prefix(client, override_source):
    response = client.get("/api/download?key=other_prefix/file.log")
    assert response.status_code == 400 and response.json()["detail"]["error"] == "invalid_key"


def test_download_file_invalid_path_traversal(client, override_source):
    response = client.get("/api/download?key=raw_prefix/../../../etc/passwd")
    assert response.status_code == 400 and response.json()["detail"]["error"] == "invalid_key"


def test_download_file_from_local_cache(client, override_source, tmp_path):
    cache_dir = tmp_path / "cache" / "service123"
    local_file = cache_dir / "raw_prefix" / "file.log"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("local-cached-data")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 200 and response.text == "local-cached-data"


def test_download_file_from_fos_success(client, override_source, tmp_path):
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)
    fake_body = MagicMock()
    fake_body.iter_chunks.return_value = [b"fos-data"]
    fake_fos = MagicMock()
    fake_fos.get_object.return_value = {"Body": fake_body, "ContentType": "text/plain", "ContentLength": 8}
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.utils.telemetry.record_call") as mock_record,
    ):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 200 and b"".join(response.iter_bytes()) == b"fos-data"
        mock_record.assert_called_once()
        fake_body.close.assert_called_once()


def test_download_file_from_cdn_success(client, override_source, tmp_path):
    override_source["cdn_url"], override_source["cdn_secret"] = "http://mycdn.com", "secret123"
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)
    fake_resp = MagicMock()
    fake_resp.headers = {"Content-Type": "text/plain", "Content-Length": "8"}
    fake_resp.read.side_effect = [b"cdn-data", b""]
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("urllib.request.urlopen", return_value=fake_resp) as mock_urlopen,
        patch("backend.utils.telemetry.record_cdn_call") as mock_record,
    ):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 200 and b"".join(response.iter_bytes()) == b"cdn-data"
        mock_urlopen.assert_called_once()
        mock_record.assert_called_once()
        fake_resp.close.assert_called_once()


def test_download_file_from_cdn_fails_raises_internal(client, override_source, tmp_path):
    override_source["cdn_url"], override_source["cdn_secret"] = "http://mycdn.com", "secret123"
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("urllib.request.urlopen", side_effect=ValueError("connection refused")),
    ):
        assert client.get("/api/download?key=raw_prefix/file.log").status_code == 502


def test_download_folder(client, override_source):
    fake_fos, fake_paginator = MagicMock(), MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw_prefix/raw/foo/file1.log"},
                {"Key": "raw_prefix/raw/foo/sub/file2.log"},
                {"Key": "raw_prefix/raw/foo/dir_marker/"},
            ]
        }
    ]
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


def test_download_all_local_include(client, override_source, tmp_path):
    db_file = tmp_path / "test.duckdb"
    db_file.write_text("duckdb-binary-data")
    override_source["duckdb_path"], override_source["prefix"] = str(db_file), "raw_prefix"
    cache_dir = tmp_path / "cache" / "service123"
    local_file = cache_dir / "raw_prefix" / "cached_file.parquet"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("parquet-bytes")
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb.get_source_for_service", return_value=override_source),
        patch("backend.config.load_config", return_value={"name": "service123", "service_id": "service123"}),
    ):
        response = client.get("/api/download-all", params={"service_id": "service123", "include": "local"})
        assert response.status_code == 200 and response.headers["content-type"] == "application/zip"
        assert len(b"".join(response.iter_bytes())) > 0


def test_download_all_cloud_include(client, override_source):
    fake_fos, fake_paginator = MagicMock(), MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "raw_prefix/raw/file1.log"}, {"Key": "raw_prefix/raw/file2.log"}]}
    ]
    fake_fos.get_paginator.return_value = fake_paginator
    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.routers.admin.downloads._fetch_file_to_zip") as mock_fetch,
        patch("backend.core.duckdb.get_source_for_service", return_value=override_source),
        patch("backend.config.load_config", return_value={"name": "service123", "service_id": "service123"}),
    ):
        response = client.get("/api/download-all", params={"service_id": "service123", "include": "cloud"})
        assert response.status_code == 200 and response.headers["content-type"] == "application/zip"
        assert len(b"".join(response.iter_bytes())) > 0 and mock_fetch.call_count == 2


# ── Merged from tests/routers/test_admin_quarantine.py ──
def clean_res(data: dict) -> dict:
    return {key: value for key, value in data.items() if not key.startswith("_")}


def test_quarantine_evidence_routes_are_registered():
    paths = app.openapi()["paths"]
    for p in (
        "/api/admin/quarantine",
        "/api/admin/quarantine/summary",
        "/api/admin/quarantine/download/{item_id}",
        "/api/admin/quarantine/purge",
    ):
        assert p in paths


def test_list_quarantine_evidence_filters_and_paginates(client, override_source):
    item = {
        "id": 7,
        "source_type": "request",
        "original_key": "raw/request/batch.log.gz",
        "line_ordinal": 4,
        "byte_offset": 92,
        "byte_length": 11,
        "error_category": "invalid_json",
        "error_text": "malformed record",
        "sha256": "a" * 64,
        "quarantined_at": "2026-09-29T12:00:00Z",
    }
    with (
        patch("backend.routers.admin.quarantine.list_quarantine_evidence", return_value=[item]) as list_items,
        patch("backend.routers.admin.quarantine.count_quarantine_evidence", return_value=1) as count_items,
    ):
        response = client.get("/api/admin/quarantine?limit=20&offset=5&error_category=invalid_json")
    assert response.status_code == 200
    payload = clean_res(response.json())
    assert payload["total"] == 1 and payload["items"][0] == item
    list_items.assert_called_once_with(
        "service123", limit=20, offset=5, error_category="invalid_json", source_type=None
    )
    count_items.assert_called_once_with("service123", error_category="invalid_json", source_type=None)


def test_quarantine_summary_uses_item_evidence(client, override_source):
    summary = {
        "total_items": 3,
        "total_bytes": 18,
        "request_items": 2,
        "rum_items": 1,
        "oldest_at": "2026-09-29T11:00:00Z",
        "newest_at": "2026-09-29T12:00:00Z",
        "category_counts": {"invalid_json": 2, "corrupt_container": 1},
    }
    with patch("backend.routers.admin.quarantine.get_quarantine_evidence_summary", return_value=summary) as get_summary:
        response = client.get("/api/admin/quarantine/summary")
    assert response.status_code == 200 and clean_res(response.json()) == summary
    get_summary.assert_called_once_with("service123")


def test_analyst_path_a_read_only_source_is_denied(client, override_source):
    override_source["access_level"] = "read_only"
    with patch("backend.routers.admin.quarantine.list_quarantine_evidence") as list_items:
        response = client.get("/api/admin/quarantine")
    assert response.status_code == 403 and response.json()["detail"]["error"] == "admin_only"
    list_items.assert_not_called()


def test_quarantine_routes_apply_endpoint_admin_dependency(client, override_source):
    def reject_admin():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = reject_admin
    try:
        response = client.get("/api/admin/quarantine/summary")
    finally:
        app.dependency_overrides.pop(require_admin, None)
    assert response.status_code == 403 and response.json()["detail"]["error"] == "admin_only"


def test_download_quarantine_evidence_streams_exact_bytes(client, override_source):
    raw_bytes = b'{"invalid":"\x00\xff"}\r\n'
    item = {"id": 7, "file_path": "7.dat", "byte_length": len(raw_bytes), "original_key": "raw/request/batch.log.gz"}
    with patch("backend.core.quarantine.read_evidence", return_value=(item, BytesIO(raw_bytes))):
        response = client.get("/api/admin/quarantine/download/7")
    assert response.status_code == 200 and response.content == raw_bytes
    assert response.headers["content-disposition"] == 'attachment; filename="quarantine-item-7.dat"'


def test_download_quarantine_evidence_returns_not_found(client, override_source):
    with patch("backend.core.quarantine.read_evidence", return_value=None):
        response = client.get("/api/admin/quarantine/download/999")
    assert response.status_code == 404 and response.json()["detail"]["error"] == "quarantine_not_found"


def test_download_quarantine_evidence_enforces_size_limit(client, override_source):
    item = {"id": 7, "file_path": "7.dat", "byte_length": 50 * 1024 * 1024 + 1}
    with patch("backend.core.quarantine.read_evidence", return_value=(item, BytesIO(b"unused"))):
        assert client.get("/api/admin/quarantine/download/7").status_code == 413


@pytest.mark.parametrize(("item_id", "expected"), [(7, {"purged": 1}), (None, {"purged": 3})])
def test_purge_quarantine_evidence(client, override_source, item_id, expected):
    with (
        patch("backend.routers.admin.quarantine.get_quarantine_evidence", return_value={"id": item_id}),
        patch("backend.core.quarantine.purge_evidence", return_value=expected) as purge,
    ):
        query = "" if item_id is None else f"?item_id={item_id}"
        response = client.post(f"/api/admin/quarantine/purge{query}")
    assert response.status_code == 200 and clean_res(response.json()) == expected
    purge.assert_called_once_with("service123", item_id)


def test_purge_quarantine_evidence_unknown_item_returns_404(client, override_source):
    with patch("backend.routers.admin.quarantine.get_quarantine_evidence", return_value=None):
        assert client.post("/api/admin/quarantine/purge?item_id=999").status_code == 404


def test_quarantine_admin_dependency_denies_analyst_path_b(override_source):
    from tests.remote_access.test_middleware import _login_analyst, _seed_invite, _start_share

    _start_share()
    invite = _seed_invite(service_ids=["service123"])
    with TestClient(app) as client:
        _login_analyst(client, invite)
        response = client.get("/api/admin/quarantine/summary", headers={"X-Remote-Analyst": "1", "Host": "testserver"})
    assert response.status_code == 403


# ── Merged from tests/routers/test_admin_queries.py ──
@pytest.fixture
def _reset_registry():
    queries = dict(query_registry._queries)
    history = list(query_registry._history)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(queries)
    query_registry._history.clear()
    query_registry._history.extend(history)


@pytest.mark.usefixtures("_reset_registry")
class TestFeatureFlag:
    def test_app_config_endpoint_returns_enabled_when_unset(self, client):
        resp = client.get("/api/admin/app-config/query-monitor")
        assert resp.status_code == 200 and resp.json()["enabled"] is True

    def test_app_config_endpoint_returns_false_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.get("/api/admin/app-config/query-monitor")
        assert resp.status_code == 200 and resp.json()["enabled"] is False

    def test_queries_endpoint_returns_404_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.get("/api/admin/queries")
        assert resp.status_code == 404 and resp.json().get("detail") == {"error": "query_monitor_disabled"}

    def test_summary_endpoint_returns_404_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            assert client.get("/api/admin/queries/summary").status_code == 404

    def test_cancel_endpoint_returns_404_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            assert client.post("/api/admin/queries/1/cancel").status_code == 404


@pytest.mark.usefixtures("_reset_registry")
class TestSnapshotEndpoint:
    def test_empty_state_returns_zero_active(self, client):
        query_registry._queries.clear()
        query_registry._history.clear()
        resp = client.get("/api/admin/queries")
        assert resp.status_code == 200
        body = resp.json()
        assert body["active"] == [] and body["completed"] == [] and body["last_seq"] == 0

    def test_active_query_appears_in_snapshot(self, client):
        query_registry._queries.clear()
        prev = current_attribution.get()
        current_attribution.set(Attribution.admin(admin_id="t", request_path="/api/x", request_id=None))
        try:
            qid = query_registry.register("DuckDB", "SELECT 1", con=None)
        finally:
            current_attribution.set(prev)

        resp = client.get("/api/admin/queries")
        assert resp.status_code == 200
        body = resp.json()
        assert qid in [r["query_id"] for r in body["active"]]
        row = next(r for r in body["active"] if r["query_id"] == qid)
        assert row["db_type"] == "DuckDB" and row["attribution"]["kind"] == "admin"
        assert row["sql"] is None and "SELECT 1" in row["sql_preview"]
        query_registry.deregister(qid)

    def test_include_completed_returns_history(self, client):
        query_registry._queries.clear()
        query_registry._history.clear()
        qid = query_registry.register("SQLite", "DROP TABLE x", con=None)
        query_registry.deregister(qid)
        body = client.get("/api/admin/queries?include_completed=true").json()
        assert any(r["query_id"] == qid for r in body["completed"])

    def test_since_seq_keeps_active_visible_across_polls(self, client):
        query_registry._queries.clear()
        a = query_registry.register("SQLite", "A", con=None)
        b = query_registry.register("SQLite", "B", con=None)
        ids = [r["query_id"] for r in client.get(f"/api/admin/queries?since_seq={a}").json()["active"]]
        assert b in ids and a in ids
        query_registry.deregister(a)
        query_registry.deregister(b)


@pytest.mark.usefixtures("_reset_registry")
class TestSummaryEndpoint:
    def test_summary_shape(self, client):
        query_registry._queries.clear()
        resp = client.get("/api/admin/queries/summary")
        assert resp.status_code == 200
        body = resp.json()
        assert body["active_total"] == 0 and body["by_db_type"] == {} and body["longest_ms"] == 0.0

    def test_summary_counts_active_queries(self, client):
        query_registry._queries.clear()
        a = query_registry.register("DuckDB", "SELECT 1", con=None)
        b = query_registry.register("SQLite", "SELECT 2", con=None)
        body = client.get("/api/admin/queries/summary").json()
        assert (
            body["active_total"] == 2
            and body["by_db_type"]["DuckDB"] == 1
            and body["by_db_type"]["SQLite"] == 1
            and body["longest_ms"] >= 0
        )
        query_registry.deregister(a)
        query_registry.deregister(b)


@pytest.mark.usefixtures("_reset_registry")
class TestPerQueryEndpoint:
    def test_unknown_qid_returns_404(self, client):
        resp = client.get("/api/admin/queries/9999999")
        assert resp.status_code == 404 and resp.json().get("detail") == {"error": "query_not_found"}

    def test_known_qid_returns_full_row_with_sql(self, client):
        query_registry._queries.clear()
        qid = query_registry.register("DuckDB", "SELECT something_specific_xyz", con=None)
        resp = client.get(f"/api/admin/queries/{qid}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["query_id"] == qid and body["sql"] is not None and "something_specific_xyz" in body["sql"]
        query_registry.deregister(qid)


@pytest.mark.usefixtures("_reset_registry")
class TestCancelEndpoint:
    def test_cancel_unknown_qid_returns_state_not_found(self, client):
        resp = client.post("/api/admin/queries/9999999/cancel")
        assert resp.status_code == 200 and resp.json()["state"] == "not_found" and resp.json()["query_id"] == 9999999

    def test_cancel_query_with_no_connection_returns_already_finished(self, client):
        qid = query_registry.register("SQLite", "SELECT 1", con=None)
        resp = client.post(f"/api/admin/queries/{qid}/cancel")
        assert resp.status_code == 200 and resp.json()["state"] == "already_finished"
        query_registry.deregister(qid)

    def test_cancel_rate_limit_kicks_in_after_10_per_second(self, client):
        mod._cancel_history.clear()
        try:
            last_status = None
            with patch("backend.routers.admin_queries.time.monotonic", return_value=1000.0):
                for _ in range(11):
                    last_status = client.post("/api/admin/queries/9999999/cancel").status_code
            assert last_status == 429
        finally:
            mod._cancel_history.clear()


@pytest.mark.usefixtures("_reset_registry")
class TestAdminIdHelper:
    def test_admin_id_falls_back_to_client_host(self):
        assert (
            _admin_id_from_request(SimpleNamespace(headers={}, client=SimpleNamespace(host="127.0.0.1"))) == "127.0.0.1"
        )

    def test_admin_id_fallback_when_no_client(self):
        assert _admin_id_from_request(SimpleNamespace(headers={}, client=None)) == "unknown"


@pytest.mark.usefixtures("_reset_registry")
class TestCancelWithHandleAndAudit:
    def test_cancel_cancellable_duckdb_query_interrupts_and_records_audit(self, client):
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
        assert resp.status_code == 200 and resp.json()["state"] == "cancelled" and resp.json()["query_id"] == qid
        assert interrupted == [True]
        query_registry.deregister(qid, error=RuntimeError("Query interrupted"))
        assert any(a["event_type"] == "query_cancelled" for a in _meta.list_audit(svc_id, limit=20))


@pytest.mark.usefixtures("_reset_registry")
class TestSlowQueriesEndpoints:
    def test_list_count_and_clear_slow_queries(self, client):
        svc_id = "test-service-id"
        _meta.clear_slow_queries(svc_id)
        now = _time.time()
        base_row = {
            "service_id": svc_id,
            "outcome": "ok",
            "attr_principal_id": None,
            "attr_request_path": None,
            "attr_request_id": None,
            "attr_cron_job": None,
            "attr_cron_run_id": None,
            "attr_pool_slot": None,
            "error_type": None,
            "error_message": None,
            "peak_memory_mb": None,
        }
        _meta.insert_slow_query(
            svc_id,
            {
                **base_row,
                "query_id": 7001,
                "db_type": "DuckDB",
                "started_at_utc": now - 120.0,
                "ended_at_utc": now - 118.2,
                "duration_ms": 1800.0,
                "sql_preview": "SELECT count(*) FROM logs",
                "sql_full": "SELECT count(*) FROM logs WHERE status >= 500",
                "sql_len": 46,
                "attr_kind": "admin",
                "attr_label": "admin @ 127.0.0.1",
                "attr_principal_id": "127.0.0.1",
                "attr_caller_qualname": "dashboard.aggregates",
                "attr_caller_file": "backend/routers/dashboard.py:42",
                "attr_request_path": "/api/dashboard/aggregates",
                "attr_request_id": "req-slow-1",
                "attr_pool_slot": "pool#1",
                "peak_memory_mb": 64.5,
            },
        )
        _meta.insert_slow_query(
            svc_id,
            {
                **base_row,
                "query_id": 7002,
                "db_type": "Postgres",
                "started_at_utc": now - 60.0,
                "ended_at_utc": now - 59.5,
                "duration_ms": 500.0,
                "sql_preview": "SELECT * FROM ingested_files",
                "sql_full": "SELECT * FROM ingested_files WHERE service_id = $1",
                "sql_len": 50,
                "attr_kind": "cron",
                "attr_label": "cron:full_sync",
                "attr_caller_qualname": "ingest_log.list_ingested_files",
                "attr_caller_file": "backend/core/metadata/ingest_log.py:100",
                "attr_cron_job": "full_sync",
                "attr_cron_run_id": 99,
            },
        )

        cnt_resp = client.get(f"/api/admin/slow-queries/count?service_id={svc_id}&since_hours=24&threshold_ms=1000")
        assert cnt_resp.status_code == 200
        cnt_body = cnt_resp.json()
        assert cnt_body["count"] == 1 and cnt_body["since_hours"] == 24 and cnt_body["threshold_ms"] == 1000.0

        list_resp = client.get(
            f"/api/admin/slow-queries?service_id={svc_id}&since_hours=24&threshold_ms=100&sort=duration"
        )
        assert list_resp.status_code == 200
        rows = list_resp.json()["rows"]
        assert len(rows) == 2 and rows[0]["query_id"] == 7001
        assert rows[0]["sql"] == "SELECT count(*) FROM logs WHERE status >= 500"
        assert rows[0]["attribution"]["kind"] == "admin" and rows[0]["attribution"]["pool_slot"] == "pool#1"
        assert rows[1]["attribution"]["cron_run_id"] == "99"

        pg_resp = client.get(
            f"/api/admin/slow-queries?service_id={svc_id}&since_hours=24&threshold_ms=100&db_type=Postgres"
        )
        assert pg_resp.status_code == 200
        pg_rows = pg_resp.json()["rows"]
        assert len(pg_rows) == 1 and pg_rows[0]["query_id"] == 7002 and pg_rows[0]["db_type"] == "Postgres"

        del_resp = client.delete(f"/api/admin/slow-queries?service_id={svc_id}")
        assert (
            del_resp.status_code == 200 and del_resp.json()["deleted"] == 2 and del_resp.json()["service_id"] == svc_id
        )

        after_cnt = client.get(f"/api/admin/slow-queries/count?service_id={svc_id}&since_hours=24&threshold_ms=100")
        assert after_cnt.status_code == 200 and after_cnt.json()["count"] == 0


@pytest.mark.usefixtures("_reset_registry")
class TestCompactionAnalystRbacRejection:
    @pytest.mark.parametrize(
        "method,path",
        [
            ("GET", "/api/admin/queries"),
            ("GET", "/api/admin/queries/summary"),
            ("GET", "/api/admin/queries/1"),
            ("POST", "/api/admin/queries/1/cancel"),
            ("GET", "/api/admin/slow-queries?service_id=svc1"),
            ("GET", "/api/admin/slow-queries/count?service_id=svc1"),
            ("DELETE", "/api/admin/slow-queries?service_id=svc1"),
            ("GET", "/api/admin/app-config/query-monitor"),
        ],
    )
    def test_analyst_session_receives_403_on_all_query_monitor_routes(self, client, method, path):
        _assert_analyst_rbac_403(client, method, path)


# ── Merged from tests/routers/test_admin_usage_and_accounting.py ──
import csv
import io

from backend import config as _svc_config
from backend.core import metadata as metadata_db
from backend.core.metadata import get_con as _meta_get_con
from backend.routers.usage import _extract_fos_ops, _fastly_api
from tests.conftest import MOCK_SERVICE_ID

_DEFAULT_UL_CFG = {
    "enabled": True,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.005,
    "class_b_rate_per_10k": 0.004,
    "cdn_egress_rate_per_gb": 0.12,
    "storage_rate_per_gb_month": 0.02,
    "min_billed_days": 30,
    "billing_zone": "MBZ100",
}


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
    return {
        "name": "test_service",
        "service_id": "test-service-id",
        "logging_service_id": "test-logging-svc-id",
    }


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


def _seed_ingested(svc_id: str, rows: list[tuple[str, int]]):
    con = metadata_db.get_con(svc_id)
    for i, (ts_iso, rc) in enumerate(rows):
        ts = ts_iso.replace("T", " ")
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            (f"raw/file-{i}.gz", svc_id, ts, rc, 1000),
        )
    con.commit()


def _complete_src(**overrides) -> dict:
    base = {
        "name": "test_service",
        "service_id": MOCK_SERVICE_ID,
        "logging_service_id": MOCK_SERVICE_ID,
        "bucket": "test-bucket",
        "prefix": "",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "k",
        "secret_access_key": "s",
    }
    base.update(overrides)
    return base


def _setup_usage_svc(
    monkeypatch, tmp_path, *, api_key: str | None = "tok", name: str | None = None, **src_overrides
) -> None:
    monkeypatch.setattr(_svc_config, "CONFIGS_DIR", tmp_path / "cfgs")
    cfg_payload: dict = {"service_id": MOCK_SERVICE_ID}
    if name is not None:
        cfg_payload["name"] = name
    if api_key is not None:
        cfg_payload["fastly_api_key"] = api_key
    _svc_config.save_config(MOCK_SERVICE_ID, cfg_payload)
    app.dependency_overrides[get_source] = lambda: _complete_src(**src_overrides)


@pytest.mark.usefixtures("frozen_clock", "_clear_log_accounting_ttl_caches")
def test_log_accounting_aligns_buckets_and_detects_sustained_loss(log_accounting_client, log_accounting_source):
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0, b1, b2 = now - timedelta(hours=3), now - timedelta(hours=2), now - timedelta(hours=1)
    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 900),
            (b1.strftime("%Y-%m-%dT%H:%M:%S"), 900),
            (b2.strftime("%Y-%m-%dT%H:%M:%S"), 900),
        ],
    )
    fastly_payload = {
        "data": [
            {"start_time": int(b0.timestamp()), "requests": 1000},
            {"start_time": int(b1.timestamp()), "requests": 1000},
            {"start_time": int(b2.timestamp()), "requests": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    body = resp.json()
    assert body["totals"]["fastly_requests"] == 3000 and body["totals"]["our_rows"] == 2700
    assert body["totals"]["gap"] == 300 and body["sustained_loss"] is not None
    assert body["sustained_loss"]["n_buckets"] == 3 and body["sustained_loss"]["total_lost_lines"] == 300


@pytest.mark.usefixtures("frozen_clock", "_clear_log_accounting_ttl_caches")
def test_log_accounting_normal_drift_and_missing_api_key(log_accounting_client, log_accounting_source):
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0, b1 = now - timedelta(hours=2), now - timedelta(hours=1)
    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 1020),
            (b1.strftime("%Y-%m-%dT%H:%M:%S"), 980),
        ],
    )
    fastly_payload = {
        "data": [
            {"start_time": int(b0.timestamp()), "requests": 1000},
            {"start_time": int(b1.timestamp()), "requests": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=3&by=hour")
    assert resp.status_code == 200 and resp.json()["sustained_loss"] is None

    from backend.routers.admin import log_accounting as _la

    _la._FASTLY_COUNTS_CACHE.clear()
    with patch("backend.config.get_fastly_api_key", return_value=None):
        resp_no_key = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp_no_key.status_code == 400


def test_usage_logging_settings_and_billing_zones(client, tmp_path, monkeypatch):
    monkeypatch.setattr(_svc_config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(_svc_config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    r = client.get("/api/admin/usage-logging")
    assert r.status_code == 200 and "enabled" in r.json()

    for zone, expected_rate in [("MBZ100", 0.08), ("MBZ200", 0.12), ("Global", 0.18)]:
        p = client.patch("/api/admin/usage-logging", json={"billing_zone": zone})
        assert p.status_code == 200
        assert p.json()["billing_zone"] == zone and p.json()["cdn_egress_rate_per_gb"] == expected_rate

    bad_zone = client.patch("/api/admin/usage-logging", json={"billing_zone": "INVALID_ZONE"})
    assert bad_zone.status_code in (400, 422)


@pytest.mark.parametrize(
    "field",
    [
        "retention_days",
        "class_a_rate_per_1k",
        "class_b_rate_per_10k",
        "cdn_egress_rate_per_gb",
        "storage_rate_per_gb_month",
        "min_billed_days",
    ],
)
@pytest.mark.parametrize("bad_val", [0, -1])
def test_usage_logging_settings_rejects_non_positive(client, tmp_path, monkeypatch, field, bad_val):
    monkeypatch.setattr(_svc_config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(_svc_config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    resp = client.patch("/api/admin/usage-logging", json={field: bad_val})
    assert resp.status_code in (400, 422)


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_usage_log_query_export_and_delete(mock_ul_cfg):
    svc = "svc-pg-usage-test"
    metadata_db.clear_usage_log(svc)
    try:
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T10:15:00Z"):
            metadata_db.log_usage_calls(
                svc,
                [
                    {
                        "service": "FOS",
                        "method": "PutObject",
                        "path": "=cmd|' /C calc'!A0",
                        "bytes": 512,
                        "time_ms": 5.0,
                        "caller": "+sum(1,2)",
                        "status": "-200",
                    }
                ],
                process_context="@at_context",
            )
        app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
        try:
            with TestClient(app) as c:
                res = c.get(
                    "/api/admin/usage-log",
                    params={"start": "2026-06-01T10:00:00Z", "end": "2026-06-01T11:00:00Z"},
                )
                assert res.status_code == 200 and res.json()["total"] == 1

                csv_res = c.get(
                    "/api/admin/usage-log/export",
                    params={"start": "2026-06-01T10:00:00Z", "end": "2026-06-01T11:00:00Z"},
                )
                assert csv_res.status_code == 200
                rows = list(csv.reader(io.StringIO(csv_res.text)))
                assert len(rows) == 2 and rows[1][4].startswith("'=") and rows[1][7].startswith("'+")

                del_res = c.delete("/api/admin/usage-log")
                assert del_res.status_code == 200
        finally:
            app.dependency_overrides.pop(get_source, None)
    finally:
        metadata_db.clear_usage_log(svc)


@patch("backend.cron.decorators.get_system_job_status")
@patch("backend.cron.scheduler.get_scheduler")
def test_get_system_jobs_and_remote_access_blocks(mock_sched, mock_status):
    mock_status.return_value = {
        "bot_data_refresh": {"last_run_at": "2026-06-01T10:00:00Z", "status": "ok", "duration_s": 1.25}
    }
    fake_job = SimpleNamespace(next_run_time=datetime(2026, 6, 2, 3, 0, 0, tzinfo=UTC))
    sched_obj = MagicMock()
    sched_obj.get_job.side_effect = lambda jid: fake_job if jid == "bot_data_refresh" else None
    mock_sched.return_value = sched_obj

    with TestClient(app) as c:
        r = c.get("/api/admin/system-jobs")
    assert r.status_code == 200
    assert {j["id"] for j in r.json()["jobs"]} == {"bot_data_refresh", "rdns_enrichment", "share_audit_purge"}

    for path in ("/api/admin/usage-logging", "/api/admin/usage-log", "/api/admin/system-jobs"):
        assert _is_blocked_path(path) is True


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (
            {
                "object_storage_class_a_operations_count": 1234,
                "object_storage_class_b_operations_count": 5678,
                "object_storage": {"class_a_operations_count": 1, "class_b_operations_count": 1},
            },
            (1234, 5678),
        ),
        ({"object_storage": {"class_a_operations_count": 100, "class_b_operations_count": 200}}, (100, 200)),
        ({"object_storage_class_a_operations_count": None, "object_storage_class_b_operations_count": None}, (0, 0)),
        ({"other": 42}, (0, 0)),
    ],
)
def test_extract_fos_ops_variants(record, expected):
    assert _extract_fos_ops(record) == expected


def test_fastly_api_constructs_correct_url_with_auth_header():
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
    _setup_usage_svc(monkeypatch, tmp_path, api_key="api-tok", cdn_service_id="cdn-svc-id")
    fake_payload = {
        "data": [
            {
                "start_time": 1767225600,
                "requests": 1000,
                "edge_requests": 800,
                "object_storage_class_a_operations_count": 100,
                "object_storage_class_b_operations_count": 50,
            }
        ]
    }
    try:
        with (
            patch("backend.routers.usage.get_fastly_api_key", return_value="api-tok"),
            patch("backend.core.fastly.service.get_active_version", return_value=None),
            patch("backend.routers.usage._fastly_api", return_value=fake_payload),
        ):
            r_pre = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
            assert r_pre.status_code == 200 and "storage_rate_per_gb_month" in r_pre.json()
            assert r_pre.json()["requests_per_day"] == 1000

            r_ops = client.get(
                "/api/usage/operations",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                params={"by": "minute"},
            )
            assert r_ops.status_code == 200

        svc = "test_usage_rum_breakdown_svc"
        con = _meta_get_con(svc)
        con.execute("DELETE FROM ingested_files WHERE source_name = ?", (svc,))
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, table_name, file_date) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("s3://bucket/2026-08-05.parquet", svc, 10, 100, "client_vitals", "2026-08-05"),
        )
        con.commit()
        app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
        with patch("backend.config.load_usage_logging_config", return_value={"class_a_rate_per_1k": 0.01}):
            r_rum = client.get(
                "/api/usage/rum-breakdown",
                params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-07T00:00:00Z"},
            )
            assert r_rum.status_code == 200 and r_rum.json()["total_beacons"] == 10
        con.execute("DELETE FROM ingested_files WHERE source_name = ?", (svc,))
        con.commit()
    finally:
        app.dependency_overrides.pop(get_source, None)
