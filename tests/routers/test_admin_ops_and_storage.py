"""Tests for the Admin Task Queue & Ingest Ledger API surface:
``GET /api/admin/celery/status`` and ``GET /api/admin/metric-history/batch``.

Covers:
1. Queue depths across KNOWN_QUEUES ("celery", "q.ingest", "q.control") and
   dynamically discovered ``q.*`` queues.
2. Worker status (active_tasks, stats, registered, scheduled, count, error).
3. In-process APScheduler schedules (`inprocess_schedules`).
4. Ingest ledger state counts (discovered, claimed, committed, quarantined,
   dead_letter) scoped by ``service_id`` across multiple services in the shared
   ADR-22 PostgreSQL metadata database without double-counting.
5. Standard mode (idle / unreachable broker fallback) vs High-Scale mode
   (active workers, queues, schedules, ledger).
6. Metric history batch series for ``celery_queue_depth`` and
   ``celery_active_tasks``.
7. Strict Admin-only RBAC enforcement (Analyst Path B receives ``403 Forbidden``
   with ``{"error": "admin_only"}`` on both endpoints).
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from backend.core import metric_snapshots
from backend.core.clickhouse_publication import PAYLOAD_COLUMNS, FosArtifacts
from backend.core.query_attribution import Attribution, current_attribution
from backend.core.query_registry import query_registry
from backend.deps import require_admin
from backend.main import app
from backend.routers.admin import clickhouse as api
from backend.routers.admin import router as admin_router
from backend.utils.remote_access import _is_blocked_path
from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware
from tests._helpers.config_fakes import fake_mutate_config as _fake_mutate
from tests._helpers.sources import TinyFos
from tests.core.test_clickhouse import fixture_manifest


# ── Merged from tests/routers/test_admin_celery.py ──
def _mock_redis(queues: dict[str, int]):
    """Build a MagicMock redis client backing scan_iter/type/llen."""
    r = MagicMock()

    def scan_iter(match=None, count=None):
        if match == "q.*":
            return iter([k for k in queues if k.startswith("q.")])
        return iter([])

    r.scan_iter.side_effect = scan_iter
    r.type.side_effect = lambda k: "list" if k in queues else "none"
    r.llen.side_effect = lambda k: queues.get(k, 0)
    return r


class TestCeleryStatusHighScaleAndStandard:
    def test_high_scale_celery_status_full_payload(self, client):
        """High-Scale / active Celery deployment returns queue depths, active workers,
        and per-service ingest ledger state counts."""
        queues = {"q.ingest": 14, "q.control": 1, "celery": 3, "q.custom": 2}
        ledger_rows_by_svc = {
            "svc-a": [("discovered", 4), ("claimed", 2), ("committed", 120), ("quarantined", 1)],
            "svc-b": [("committed", 80), ("dead_letter", 3)],
        }

        def _fake_get_con(sid: str):
            con = MagicMock()
            cur = MagicMock()

            def _exec(sql: str, params=()):
                assert "WHERE service_id = ?" in sql
                assert params == (sid,)
                cur.fetchall.return_value = ledger_rows_by_svc.get(sid, [])

            cur.execute.side_effect = _exec
            con.cursor.return_value = cur
            return con

        with (
            patch("backend.celery_app.app.control.inspect") as mock_inspect,
            patch("backend.celery_queue._get_redis", return_value=_mock_redis(queues)),
            patch(
                "backend.config.list_configs",
                return_value=[{"service_id": "svc-a"}, {"service_id": "svc-b"}],
            ),
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

            assert data["broker_reachable"] is True
            assert data["workers"]["count"] == 2
            assert len(data["workers"]["active_tasks"]["celery@worker-1"]) == 1
            assert data["workers"]["active_tasks"]["celery@worker-1"][0]["id"] == "task-uuid-1"
            assert data["workers"]["active_tasks"]["celery@worker-2"] == []

            assert data["queues"] == {
                "celery": 3,
                "q.control": 1,
                "q.custom": 2,
                "q.ingest": 14,
            }
            # Verify multi-service ledger aggregation does not double-count shared rows
            assert data["ledger"] == {
                "discovered": 4,
                "claimed": 2,
                "committed": 200,
                "quarantined": 1,
                "dead_letter": 3,
            }

    def test_standard_mode_idle_broker_fallback(self, client):
        """Standard mode with reachable idle Valkey reports 0 queue depths, 0 workers,
        and empty ledger."""
        with (
            patch("backend.celery_app.app.control.inspect") as mock_inspect,
            patch("backend.celery_queue._get_redis", return_value=_mock_redis({})),
            patch("backend.config.list_configs", return_value=[]),
        ):
            mock_i = MagicMock()
            mock_i.active.return_value = {}
            mock_i.stats.return_value = {}
            mock_i.registered.return_value = {}
            mock_i.scheduled.return_value = {}
            mock_inspect.return_value = mock_i

            resp = client.get("/api/admin/celery/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["broker_reachable"] is True
            assert data["workers"]["count"] == 0
            assert data["queues"] == {"celery": 0, "q.control": 0, "q.ingest": 0}
            assert data["ledger"] == {}

    def test_standard_mode_unreachable_broker_fallback(self, client):
        """When Valkey/Redis is down or unconfigured, broker_reachable is False and
        workers.error records the inspect failure without raising a 500."""
        broken_redis = MagicMock()
        broken_redis.scan_iter.side_effect = ConnectionError("Valkey connection refused")
        with (
            patch("backend.celery_app.app.control.inspect") as mock_inspect,
            patch("backend.celery_queue._get_redis", return_value=broken_redis),
            patch("backend.config.list_configs", return_value=[]),
        ):
            mock_inspect.side_effect = RuntimeError("Broker connection timeout")

            resp = client.get("/api/admin/celery/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["broker_reachable"] is False
            assert data["workers"]["count"] == 0
            assert "Broker connection timeout" in data["workers"]["error"]
            assert data["queues"] == {}
            assert data["ledger"] == {}

    def test_standard_mode_inprocess_scheduler_and_ingest_telemetry(self, client):
        """Standard mode returns deployment_mode='standard',
        inprocess_schedules, inprocess_active_runs, and standard_ingest summary."""
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
                return_value=[
                    {
                        "run_id": 101,
                        "service_id": "svc-a",
                        "task": "sync",
                        "started_at": 1700000000.0,
                    }
                ],
            ),
            patch(
                "backend.celery_status.standard_ingest_summary",
                return_value={"ingested_files": 420, "quarantined_items": 1},
            ),
            patch(
                "backend.celery_status.ingest_ledger_summary",
                return_value={"committed": 420, "quarantined": 1},
            ),
        ):
            resp = client.get("/api/admin/celery/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["broker_reachable"] is False
            assert data["deployment_mode"] == "standard"
            assert data["inprocess_schedules"] == [
                {
                    "name": "log_discovery_svc-a",
                    "trigger": "interval[0:00:10]",
                    "next_run_at": 1700000010.0,
                }
            ]
            assert data["inprocess_active_runs"] == [
                {
                    "run_id": 101,
                    "service_id": "svc-a",
                    "task": "sync",
                    "started_at": 1700000000.0,
                }
            ]
            assert data["standard_ingest"] == {"ingested_files": 420, "quarantined_items": 1}
            assert data["ledger"] == {"committed": 420, "quarantined": 1}


class TestQueueMetricHistoryBatch:
    def test_metric_history_batch_returns_celery_series(self, client):
        """Verify /api/admin/metric-history/batch?since=1h returns celery_queue_depth
        and celery_active_tasks series used by the QueueClient sparklines."""
        metric_snapshots.record_snapshot("celery_queue_depth", 7.0)
        metric_snapshots.record_snapshot("celery_active_tasks", 2.0)

        resp = client.get("/api/admin/metric-history/batch?since=1h")
        assert resp.status_code == 200
        body = resp.json()
        assert "series" in body
        assert "celery_queue_depth" in body["series"]
        assert "celery_active_tasks" in body["series"]
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
        raw_path = path.split("?", 1)[0]
        assert _is_blocked_path(raw_path) is True, f"Expected {raw_path} to be blocked for Analyst Path B"

        fake_req = SimpleNamespace(
            state=SimpleNamespace(analyst_session=SimpleNamespace(session_id="sess_analyst_queue")),
            headers={},
            client=SimpleNamespace(host="203.0.113.55"),
        )
        with pytest.raises(HTTPException) as exc:
            require_admin(fake_req)  # type: ignore[arg-type]
        assert exc.value.status_code == 403
        assert exc.value.detail["error"] == "admin_only"

        if raw_path in ("/api/admin/celery/status", "/api/admin/metric-history/batch"):

            def _analyst_override():
                require_admin(fake_req)  # type: ignore[arg-type]

            client.app.dependency_overrides[require_admin] = _analyst_override
            try:
                resp = client.request(method, path)
                assert resp.status_code == 403
                assert resp.json()["detail"]["error"] == "admin_only"
            finally:
                client.app.dependency_overrides.pop(require_admin, None)


class TestIngestLedgerInspectorEndpoints:
    def test_list_retry_and_purge_ingest_ledger_entries(self, client):
        """Verify GET /api/admin/celery/ledger, POST /api/admin/celery/ledger/retry,
        and POST /api/admin/celery/ledger/purge list, requeue, and delete dead_letter
        and quarantined rows while leaving committed rows untouched."""
        from backend.core.metadata.base import get_con

        sid = "svc-ledger-test"
        con = get_con(sid)
        cur = con.cursor()
        cur.execute("DELETE FROM ingest_ledger WHERE service_id = ?", (sid,))
        cur.execute(
            "INSERT INTO ingest_ledger "
            "(service_id, object_key, size_bytes, status, attempts, last_error, error_kind, "
            "claimed_at, discovered_at, originating_task, originating_run_id) "
            "VALUES (?, ?, ?, 'dead_letter', 1, ?, 'object_missing', 1729400010.0, 1729400000.0, 'rum_discovery', 99)",
            (
                sid,
                "raw/rum/year=2026/month=10/day=20/hour=04/minute=28/dead-rum.log.gz",
                1024,
                "object missing from FOS (already ingested+deleted, or expired)",
            ),
        )
        cur.execute(
            "INSERT INTO ingest_ledger "
            "(service_id, object_key, size_bytes, status, attempts, last_error, error_kind, "
            "claimed_at, discovered_at, originating_task, originating_run_id) "
            "VALUES (?, ?, ?, 'quarantined', 3, ?, 'corrupt_gzip', 1729400020.0, 1729400005.0, 'log_discovery', 100)",
            (
                sid,
                "raw/request/year=2026/month=10/day=20/hour=04/minute=29/bad-req.log.gz",
                2048,
                "Not a gzipped file",
            ),
        )
        cur.execute(
            "INSERT INTO ingest_ledger "
            "(service_id, object_key, size_bytes, status, attempts, committed_at, discovered_at) "
            "VALUES (?, ?, ?, 'committed', 1, 1729400030.0, 1729400000.0)",
            (
                sid,
                "raw/request/year=2026/month=10/day=20/hour=04/minute=30/good-req.log.gz",
                4096,
            ),
        )
        con.commit()

        try:
            with patch("backend.config.list_configs", return_value=[{"service_id": sid}]):
                # 1. Default problem filter returns dead_letter + quarantined (2 rows)
                resp = client.get("/api/admin/celery/ledger")
                assert resp.status_code == 200
                body = resp.json()
                assert body["total"] == 2
                assert len(body["items"]) == 2
                by_key = {item["object_key"]: item for item in body["items"]}
                rum_item = by_key["raw/rum/year=2026/month=10/day=20/hour=04/minute=28/dead-rum.log.gz"]
                assert rum_item["source_type"] == "rum"
                assert rum_item["status"] == "dead_letter"
                assert rum_item["error_kind"] == "object_missing"
                assert "object missing from FOS" in rum_item["last_error"]
                assert rum_item["originating_task"] == "rum_discovery"
                assert rum_item["originating_run_id"] == 99

                req_item = by_key["raw/request/year=2026/month=10/day=20/hour=04/minute=29/bad-req.log.gz"]
                assert req_item["source_type"] == "request"
                assert req_item["status"] == "quarantined"
                assert req_item["error_kind"] == "corrupt_gzip"

                # 2. Retry the RUM dead_letter item -> resets to discovered & dispatches convert_batch_rum_files
                with (
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_req_delay,
                    patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_rum_delay,
                ):
                    retry_resp = client.post(
                        "/api/admin/celery/ledger/retry",
                        json={
                            "items": [
                                {
                                    "service_id": sid,
                                    "object_key": "raw/rum/year=2026/month=10/day=20/hour=04/minute=28/dead-rum.log.gz",
                                }
                            ]
                        },
                    )
                    assert retry_resp.status_code == 200
                    retry_data = retry_resp.json()
                    assert retry_data["ok"] is True
                    assert retry_data["affected"] == 1
                    assert retry_data["dispatched"] == 1
                    mock_rum_delay.assert_called_once_with(
                        sid, ["raw/rum/year=2026/month=10/day=20/hour=04/minute=28/dead-rum.log.gz"]
                    )
                    mock_req_delay.assert_not_called()

                # Now only 1 problem row (quarantined) remains
                after_retry = client.get("/api/admin/celery/ledger").json()
                assert after_retry["total"] == 1
                assert after_retry["items"][0]["status"] == "quarantined"

                # 3. Purge remaining problem rows -> deletes quarantined row, preserves committed & discovered
                purge_resp = client.post(
                    "/api/admin/celery/ledger/purge",
                    json={"status": "problem"},
                )
                assert purge_resp.status_code == 200
                purge_data = purge_resp.json()
                assert purge_data["ok"] is True
                assert purge_data["affected"] == 1

                after_purge = client.get("/api/admin/celery/ledger").json()
                assert after_purge["total"] == 0
                assert after_purge["items"] == []

                all_rows = client.get("/api/admin/celery/ledger?status=all").json()
                assert all_rows["total"] == 2
                statuses = {r["status"] for r in all_rows["items"]}
                assert statuses == {"discovered", "committed"}
        finally:
            cur.execute("DELETE FROM ingest_ledger WHERE service_id = ?", (sid,))
            con.commit()


def test_task_routes_match_registered_tasks():
    """Every task_routes key must correspond to a registered task (or a
    prefix pattern that matches at least one)."""
    import importlib

    from backend.celery_app import app

    for mod in app.conf.include:
        importlib.import_module(mod)

    registered = set(app.tasks.keys())
    for pattern in app.conf.task_routes:
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            assert any(t.startswith(prefix) for t in registered), (
                f"task_routes pattern {pattern!r} matches no registered task"
            )
        else:
            assert pattern in registered, f"task_routes key {pattern!r} is not a registered task"


def test_worker_queues_cover_all_routed_queues():
    """The queues KNOWN_QUEUES advertises must cover every queue task_routes
    can route to, plus the default queue."""
    from backend.celery_app import app
    from backend.celery_status import KNOWN_QUEUES

    routed = {spec["queue"] for spec in app.conf.task_routes.values()}
    routed.add(app.conf.task_default_queue or "celery")
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
    store.replay_preview.return_value = {
        "dataset_id": "dataset",
        "artifact_count": 1,
        "planned_artifacts": 1,
    }
    monkeypatch.setattr(api, "PgManifest", MagicMock(return_value=store))
    monkeypatch.setattr(api, "get_clickhouse_client", lambda: ch)
    monkeypatch.setattr(api, "target_identity", lambda client: "target")
    monkeypatch.setattr(api, "FosArtifacts", objects)
    monkeypatch.setattr(api.metadata, "record_audit", audit)
    app = FastAPI()
    app.add_middleware(TelemetryResponseBodyMiddleware)
    app.include_router(api.router)
    with TestClient(app) as client:
        yield client, store, ch, objects, audit, cfg


def test_registered_on_real_app_openapi():
    from backend.main import app

    paths = app.openapi()["paths"]
    for path, verb, model in (
        (STATUS, "get", "ClickHouseStatusResponse"),
        (REPLAY, "post", "ClickHouseReplayResponse"),
    ):
        schema = paths[path][verb]["responses"]["200"]["content"]["application/json"]["schema"]
        assert schema["$ref"].endswith("/" + model)


def test_status_disabled_is_unknown_not_zero(boundary, monkeypatch):
    client, store, _, _, _, _ = boundary
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
    client, store, ch, _, _, _ = boundary
    if failure == "client":
        ch.health.side_effect = RuntimeError("postgresql://private-secret@example.invalid")
    elif failure == "manifest":
        store.admin_status.side_effect = RuntimeError("s3://private-bucket")
    else:
        store.admin_status.return_value["schema_version"] = 99
    response = client.get(STATUS, params={"service_id": SERVICE})
    assert response.status_code == 503
    assert response.json()["health"] == "unavailable"
    assert response.json()["enabled"] is True
    assert "private" not in response.text
    if failure != "schema":
        assert response.json()["publication_counts"] is None


def test_status_is_service_scoped_and_preserves_expired_flag(boundary):
    client, store, _, _, _, _ = boundary
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
    assert response.json()["active_generation"]["expired"] is True
    assert response.json()["oldest_pending_age_seconds"] == 12.5
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
    assert response.status_code == 422
    assert not store.mock_calls
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
    assert response.json()["dry_run"] is True and response.json()["attempted"] == 0
    assert response.json()["planned_artifacts"] == 1
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
    client, store, ch, _, audit, cfg = boundary
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
    store.finish.return_value = True
    store.activate.return_value = True
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
    client, _, _, _, _, _ = boundary
    monkeypatch.setenv("DEBUG_RESPONSES", "true")
    monkeypatch.setattr(api.config, "config_to_source", lambda config: {})
    monkeypatch.setattr(api, "full_rebuild", MagicMock(side_effect=RuntimeError("s3://private-secret")))
    debug_calls = MagicMock(return_value=[{"url": "s3://private-secret"}])
    monkeypatch.setattr("backend.utils.telemetry.get_tracked_calls", debug_calls)
    response = client.post(
        REPLAY,
        json={"service_id": SERVICE, "dataset_id": "d", "dry_run": False},
        headers={"x-debug-responses": "1"},
    )
    assert response.status_code == 503 and "private" not in response.text
    assert response.json()["_debug_calls"] == []
    debug_calls.assert_not_called()


def test_replay_missing_both_dataset_id_and_generation_rejected(boundary):
    client, store, _, objects, audit, _ = boundary
    response = client.post(REPLAY, json={"service_id": SERVICE})
    assert response.status_code == 422
    assert not store.mock_calls
    objects.assert_not_called()
    audit.assert_not_called()


def test_analyst_path_b_rejected_with_403_admin_only(monkeypatch):
    cfg = {"service_id": SERVICE, "access_level": "read_write"}
    monkeypatch.setattr(api.config, "load_config", lambda sid: cfg if sid == SERVICE else None)
    store, ch = MagicMock(), MagicMock()
    monkeypatch.setattr(api, "PgManifest", MagicMock(return_value=store))
    monkeypatch.setattr(api, "get_clickhouse_client", lambda: ch)

    app = FastAPI()

    @app.middleware("http")
    async def _attach_analyst_session(request, call_next):
        request.state.analyst_session = MagicMock(service_ids=[SERVICE])
        return await call_next(request)

    app.include_router(api.router)
    with TestClient(app) as client:
        res_status = client.get(STATUS, params={"service_id": SERVICE})
        assert res_status.status_code == 403
        assert res_status.json()["detail"]["error"] == "admin_only"

        res_replay = client.post(REPLAY, json={"service_id": SERVICE, "dataset_id": "d"})
        assert res_replay.status_code == 403
        assert res_replay.json()["detail"]["error"] == "admin_only"

        assert not store.mock_calls
        assert not ch.mock_calls


# ── Merged from tests/routers/test_admin_compaction.py ──
def test_optimize_now_forwards_to_iceberg_optimize_table(client):
    """POST /admin/optimize-now triggers Iceberg compaction; the
    optimize_table result dict flows back as JSON (middleware may add
    _debug_* fields — we assert the payload is a superset)."""
    fake_result = {"files_rewritten": 4, "files_added": 1, "bytes_rewritten": 12345}
    with patch("backend.core.iceberg.optimize_table", return_value=fake_result) as m:
        resp = client.post("/api/admin/optimize-now")

    assert resp.status_code == 200
    body = resp.json()
    assert body["files_rewritten"] == 4
    assert body["files_added"] == 1
    assert body["bytes_rewritten"] == 12345
    # Caller-passed min_files_per_partition defaults to None.
    _, kwargs = m.call_args
    assert kwargs.get("min_files_per_partition") is None


def test_optimize_now_passes_min_files_when_specified(client):
    with patch("backend.core.iceberg.optimize_table", return_value={"files_rewritten": 0}) as m:
        resp = client.post("/api/admin/optimize-now?min_files=1")

    assert resp.status_code == 200
    _, kwargs = m.call_args
    assert kwargs.get("min_files_per_partition") == 1


def test_local_compact_now_forwards_default_min_files_three(client):
    """Default min_files=3 (normal cron behaviour) is forwarded
    explicitly so the test pins the surface."""
    with patch("backend.core.local_compaction.compact_local_partitions", return_value={"rewritten": 2}) as m:
        resp = client.post("/api/admin/local-compact-now")

    assert resp.status_code == 200
    assert resp.json()["rewritten"] == 2
    _, kwargs = m.call_args
    assert kwargs["min_files_per_partition"] == 3
    assert kwargs["dry_run"] is False


def test_local_compact_now_min_files_zero_is_allowed(client):
    """ge=0 — min_files=0 forces every partition through dedup
    (one-shot historical cleanup). 422 would break the FE button."""
    with patch("backend.core.local_compaction.compact_local_partitions", return_value={"rewritten": 0}) as m:
        resp = client.post("/api/admin/local-compact-now?min_files=0&dry_run=true")

    assert resp.status_code == 200
    _, kwargs = m.call_args
    assert kwargs["min_files_per_partition"] == 0
    assert kwargs["dry_run"] is True


def test_local_compact_now_rejects_negative_min_files(client):
    """ge=0 must reject negative values with 422 (otherwise the SQL
    deeper down would mishandle it)."""
    resp = client.post("/api/admin/local-compact-now?min_files=-1")
    assert resp.status_code == 422


def test_compaction_stats_returns_local_compaction_snapshot(client):
    # Field is "partitions" (count of leaf partitions); both the producer
    # backend.core.local_compaction.compaction_stats and the typed
    # CompactionStatsResponse model agree on this name. The pre-typed
    # version of the test used "partitions_total" which Pydantic silently
    # dropped (unknown key) so the assertion stayed broken.
    fake = {"partitions": 100, "partitions_above_3": 5, "avg_files_per_partition": 1.8}
    with patch("backend.core.local_compaction.compaction_stats", return_value=fake):
        resp = client.get("/api/admin/compaction-stats")

    assert resp.status_code == 200
    body = resp.json()
    assert body["partitions"] == 100
    assert body["partitions_above_3"] == 5
    assert body["avg_files_per_partition"] == 1.8


def test_metadata_retention_patch_writes_config_and_audit(client):
    """Body values flow into svcconfig.mutate_config; an audit row is
    written; the resolved retention (defaults merged with cfg) comes
    back so the UI can confirm what was saved."""
    saved: dict = {}
    audit: list[dict] = []

    def _record(**kwargs):
        audit.append(kwargs)

    base = {"name": "svc", "service_id": "svc"}
    with (
        patch("backend.config.load_config", return_value=base),
        patch("backend.config.mutate_config", side_effect=_fake_mutate(base, saved=saved)),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit", side_effect=_record),
    ):
        resp = client.patch(
            "/api/admin/metadata-retention",
            json={"usage_log_days": 14, "ingested_files_days": 30, "cron_runs_days": 7},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["retention"]["usage_log_days"] == 14
    assert body["retention"]["ingested_files_days"] == 30
    assert body["retention"]["cron_runs_days"] == 7

    assert saved["metadata_retention"] == {
        "usage_log_days": 14,
        "ingested_files_days": 30,
        "cron_runs_days": 7,
    }
    assert len(audit) == 1
    assert audit[0]["event_type"] == "metadata_retention_update"


def test_metadata_retention_patch_merges_onto_freshly_read_retention(client):
    """Keys the body omits survive from the config as read under the write lock."""
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

    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "Service not found"


def test_metadata_retention_clamps_negative_to_zero(client):
    """Negative / non-numeric retention values are clamped to 0
    (disables cleanup for that table). Pinned so a -1 in the JSON body
    doesn't end up as a negative day-count in cleanup_metadata."""
    saved: dict = {}
    base = {"name": "svc", "service_id": "svc"}

    with (
        patch("backend.config.load_config", return_value=base),
        patch("backend.config.mutate_config", side_effect=_fake_mutate(base, saved=saved)),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = client.patch(
            "/api/admin/metadata-retention",
            json={"usage_log_days": -5, "cron_runs_days": "not-a-number"},
        )

    assert resp.status_code == 200
    assert saved["metadata_retention"]["usage_log_days"] == 0
    assert saved["metadata_retention"]["cron_runs_days"] == 0


def test_metadata_retention_force_disables_ingested_files_days_when_dedup_inactive(client):
    """If the service's dedup gate (delete_after=False) means the
    ingested_files table is load-bearing, the writer-side override
    forces ingested_files_days back to 0 — preventing operator
    misconfiguration."""
    saved: dict = {}
    base = {"name": "svc", "service_id": "svc"}
    with (
        patch("backend.config.load_config", return_value=base),
        patch("backend.config.mutate_config", side_effect=_fake_mutate(base, saved=saved)),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=False),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = client.patch("/api/admin/metadata-retention", json={"ingested_files_days": 30})

    assert resp.status_code == 200
    assert saved["metadata_retention"]["ingested_files_days"] == 0


def test_metadata_retention_audit_failure_does_not_break_patch(client):
    """audit failure is swallowed — the save already happened, returning
    500 here would mislead the FE into thinking the write failed."""
    base = {"name": "svc", "service_id": "svc"}
    with (
        patch("backend.config.load_config", return_value=base),
        patch("backend.config.mutate_config", side_effect=_fake_mutate(base)),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        patch("backend.core.metadata.record_audit", side_effect=RuntimeError("audit-boom")),
    ):
        resp = client.patch("/api/admin/metadata-retention", json={"usage_log_days": 7})

    assert resp.status_code == 200


def test_metadata_storage_returns_stats_plus_retention_and_lock(client):
    fake_stats = {"by_table": {"usage_log": {"rows": 1000, "bytes": 50000}}}
    with (
        patch("backend.core.metadata.get_metadata_storage_stats", return_value=fake_stats),
        patch(
            "backend.config.load_config",
            return_value={
                "name": "svc",
                "service_id": "svc",
                "metadata_retention": {"usage_log_days": 14},
            },
        ),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
    ):
        resp = client.get("/api/admin/metadata-storage")

    assert resp.status_code == 200
    body = resp.json()
    assert body["by_table"]["usage_log"]["rows"] == 1000
    assert body["retention"]["usage_log_days"] == 14
    assert body["ingested_files_locked"] is False  # dedup ACTIVE → not locked


def test_metadata_storage_locks_when_dedup_inactive(client):
    """When delete_after=False (dedup INactive), the FE renders a
    tooltip explaining why the input is disabled — it relies on the
    ``ingested_files_locked: true`` field flipping correctly."""
    with (
        patch("backend.core.metadata.get_metadata_storage_stats", return_value={}),
        patch("backend.config.load_config", return_value={"name": "svc"}),
        patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=False),
    ):
        resp = client.get("/api/admin/metadata-storage")

    assert resp.status_code == 200
    assert resp.json()["ingested_files_locked"] is True


def _collect_sse_events(body: bytes) -> list[dict]:
    """Parse the SSE stream into a list of JSON event payloads."""
    events: list[dict] = []
    for line in body.decode("utf-8", errors="replace").splitlines():
        if not line.startswith("data: "):
            continue
        try:
            events.append(json.loads(line[len("data: ") :]))
        except json.JSONDecodeError:
            continue
    return events


def test_metadata_cleanup_streams_done_event_on_success(client):
    """Happy path: cleanup_metadata returns its result dict; the SSE
    stream ends with a {"type": "done", ...} event."""
    fake_result = {"deleted": {"usage_log": 100, "cron_runs": 10}, "vacuumed": True}

    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {"usage_log_days": 7}}),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.core.metadata.cleanup_metadata", return_value=fake_result),
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    events = _collect_sse_events(resp.content)
    assert events, "expected at least one SSE event"
    # Last event is "done" with the cleanup_metadata result attached.
    done = events[-1]
    assert done["type"] == "done"
    assert done["result"]["vacuumed"] is True
    assert done["result"]["deleted"]["usage_log"] == 100
    assert "Trimmed 110 rows" in done["message"]


def test_metadata_cleanup_streams_done_with_zero_rows_summary(client):
    """When no rows match retention, the summary text says so —
    the FE shows that string directly to the operator."""
    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {}}),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.core.metadata.cleanup_metadata",
            return_value={"deleted": {}, "vacuumed": False},
        ),
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    events = _collect_sse_events(resp.content)
    done = events[-1]
    assert done["type"] == "done"
    assert done["message"] == "No rows older than retention windows."


def test_metadata_cleanup_handles_start_cron_run_failure(client):
    """Finding 014: ``start_cron_run`` was called OUTSIDE the worker's
    try/except, so a metadata-DB write failure during cron-id allocation
    would kill the worker thread without pushing the sentinel ``None`` —
    leaving the SSE generator blocked forever on ``events.get()`` and
    leaking a thread from the FastAPI worker pool. The fix initialises
    ``run_id = None`` and moves ``start_cron_run`` inside the try so the
    finally block closes the queue and ``log_cron_run`` is invoked even
    when ``start_cron_run`` itself blows up."""
    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {}}),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("simulated SQLite locked")),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.core.metadata.cleanup_metadata") as mock_cleanup,
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    events = _collect_sse_events(resp.content)
    # Must end with an error event, not hang waiting for done.
    assert any(e["type"] == "error" for e in events), (
        f"start_cron_run failure must surface an error SSE event; events={events}"
    )
    assert not any(e.get("type") == "done" for e in events)
    err = next(e for e in events if e["type"] == "error")
    assert "simulated SQLite locked" in err["message"]

    # cleanup_metadata must NEVER have been invoked — start_cron_run failed
    # before the cleanup body runs.
    mock_cleanup.assert_not_called()

    # log_cron_run must still fire (closing the run as error) with run_id=None
    # since start_cron_run never returned an id.
    assert mock_log.called, "log_cron_run must be invoked even when start_cron_run fails"
    _args, kwargs = mock_log.call_args
    assert kwargs.get("run_id") is None


def test_metadata_cleanup_streams_error_event_when_cleanup_raises(client):
    """cleanup_metadata blowing up → SSE emits an error event AND the
    cron_runs row is closed with status=error so the schedule grid
    shows the failure."""
    log_calls: list = []

    with (
        patch("backend.config.load_config", return_value={"metadata_retention": {}}),
        patch("backend.core.duckdb.start_cron_run", return_value=99),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch("backend.core.metadata.cleanup_metadata", side_effect=RuntimeError("simulated DB lock")),
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    events = _collect_sse_events(resp.content)
    # One error event, no done event.
    assert any(e["type"] == "error" for e in events)
    assert not any(e.get("type") == "done" for e in events)
    err = next(e for e in events if e["type"] == "error")
    assert "simulated DB lock" in err["message"]

    # cron_runs got an "error" terminal row keyed to the start_cron_run id.
    assert log_calls, "log_cron_run must be invoked even on error"
    args, kwargs = log_calls[0]
    assert kwargs.get("run_id") == 99
    assert args[3] == "error"  # status positional


def test_metadata_cleanup_sets_no_buffering_headers(client):
    """Reverse-proxy / browser buffering would defeat streaming UX —
    pin the headers the SSE sender depends on."""
    with (
        patch("backend.config.load_config", return_value={}),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.core.metadata.cleanup_metadata",
            return_value={"deleted": {}, "vacuumed": False},
        ),
    ):
        resp = client.post("/api/admin/metadata-cleanup")

    # sse-starlette sets `no-store`; both prevent intermediate caching.
    cache_control = resp.headers.get("cache-control", "")
    assert "no-store" in cache_control or "no-cache" in cache_control
    assert resp.headers.get("x-accel-buffering") == "no"
    assert resp.headers.get("connection") == "keep-alive"


def test_backfill_bundle_rollups_success(client):
    """POST /admin/backfill-bundle-rollups invokes all rollup backfillers and closed-day compactors."""
    mocks = [
        "backfill_slow_urls_bundles",
        "backfill_origin_summary_bundles",
        "backfill_origin_dims_bundles",
        "backfill_origin_latency_ts_bundles",
        "backfill_network_rtt_bundles",
        "backfill_network_speed_bundles",
        "backfill_network_quality_bundles",
        "backfill_verified_bots_ts_bundles",
        "backfill_perf_latency_bundles",
        "backfill_pop_health_bundles",
        "backfill_security_dims_bundles",
        "backfill_security_proxies_bundles",
        "backfill_perf_dims_bundles",
        "backfill_wellknown_bots_rollup",
        "backfill_ngwaf_bots_bundles",
        "backfill_ngwaf_bots_ts_bundles",
        "backfill_overview_bundles",
        "backfill_network_summary_bundles",
        "compact_origin_summary_closed_days_to_daily",
        "compact_origin_dims_closed_days_to_daily",
        "compact_origin_latency_ts_closed_days_to_daily",
        "compact_network_rtt_closed_days_to_daily",
        "compact_network_speed_closed_days_to_daily",
        "compact_network_quality_closed_days_to_daily",
        "compact_verified_bots_ts_closed_days_to_daily",
        "compact_perf_latency_closed_days_to_daily",
        "compact_pop_health_closed_days_to_daily",
        "compact_security_dims_closed_days_to_daily",
        "compact_security_proxies_closed_days_to_daily",
        "compact_perf_dims_closed_days_to_daily",
        "compact_ngwaf_bots_closed_days_to_daily",
        "compact_ngwaf_bots_ts_closed_days_to_daily",
        "compact_overview_closed_days_to_daily",
    ]

    from contextlib import contextmanager

    @contextmanager
    def mock_get_connection(*args, **kwargs):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchone.return_value = (1,)
        yield mock_con

    with (
        patch.multiple("backend.core.rollups", **{name: MagicMock(return_value=1) for name in mocks}),
        patch("backend.core.duckdb.get_connection", mock_get_connection),
        patch("backend.core.rollups.rum.recompute_rum_aggregates", MagicMock()),
    ):
        resp = client.post("/api/admin/backfill-bundle-rollups")
        assert resp.status_code == 200
        body = resp.json()
        assert body["slow_urls"] == 1
        assert body["origin_summary"] == 1
        assert body["origin_summary_days"] == 1


def test_backfill_bundle_rollups_response_maps_each_backfiller_and_compactor_to_its_key(client):
    """Pins which function feeds which response key (the mapping the handler hand-wires)."""
    from contextlib import contextmanager

    backfills = {
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
    compactors = {
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
    values = {name: 100 + i for i, name in enumerate([*backfills, *compactors])}

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
    for name, key in {**backfills, **compactors}.items():
        if key is not None:
            assert body[key] == values[name], f"{key} should come from {name}"


def test_rebuild_time_series_rollups_rebuilds_only_requested_closed_hours(client):
    current_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    hours = [(current_hour - timedelta(hours=offset)).strftime("%Y-%m-%d-%H") for offset in (3, 2, 1)]
    with (
        patch(
            "backend.routers.admin.compaction.build_time_series_bundles",
            return_value=len(hours),
        ) as rebuild,
        patch("backend.core.rollups.recompute.recompute_touched_hours") as recompute,
    ):
        response = client.post(
            "/api/admin/rebuild-time-series-rollups",
            json={"hours": hours},
        )

    assert response.status_code == 200
    assert response.json()["requested_hours"] == hours
    assert response.json()["rebuilt"] == len(hours)
    rebuild.assert_called_once()
    assert rebuild.call_args.args[2] == hours
    recompute.assert_not_called()


def test_rebuild_time_series_rollups_rejects_invalid_or_future_hours(client):
    for hours in (["not-an-hour"], ["2099-01-01-00"]):
        response = client.post(
            "/api/admin/rebuild-time-series-rollups",
            json={"hours": hours},
        )
        assert response.status_code == 400

    current_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    closed_hour = (current_hour - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
    response = client.post(
        "/api/admin/rebuild-time-series-rollups",
        json={"hours": [closed_hour, closed_hour]},
    )
    assert response.status_code == 400


def test_rebuild_time_series_rollups_rejects_analyst_path_b():
    analyst_app = FastAPI()

    @analyst_app.middleware("http")
    async def stamp_analyst(request: Request, call_next):
        request.state.analyst_session = SimpleNamespace(
            session_id="sess-analyst-1",
            service_ids=["svc-a"],
            email="analyst@example.com",
        )
        return await call_next(request)

    analyst_app.include_router(admin_router)

    with TestClient(analyst_app) as analyst_client:
        response = analyst_client.post(
            "/api/admin/rebuild-time-series-rollups",
            json={"hours": ["2026-10-24-04"]},
        )

    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "admin_only"


# ── Merged from tests/routers/test_admin_downloads.py ──
def test_download_file_missing_key(client, override_source):

    # key is empty
    response = client.get("/api/download?key=")
    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "Missing key parameter"


def test_download_file_invalid_key_prefix(client, override_source):
    # key doesn't start with raw_prefix
    response = client.get("/api/download?key=other_prefix/file.log")
    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "invalid_key"


def test_download_file_invalid_path_traversal(client, override_source):
    # path traversal
    response = client.get("/api/download?key=raw_prefix/../../../etc/passwd")
    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "invalid_key"


def test_download_file_from_local_cache(client, override_source, tmp_path):
    # Create local cached file
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)
    local_file = cache_dir / "raw_prefix" / "file.log"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("local-cached-data")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 200
        assert response.text == "local-cached-data"


def test_download_file_from_fos_success(client, override_source, tmp_path):
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)

    fake_body = MagicMock()
    fake_body.iter_chunks.return_value = [b"fos-data"]
    fake_obj = {
        "Body": fake_body,
        "ContentType": "text/plain",
        "ContentLength": 8,
    }
    fake_fos = MagicMock()
    fake_fos.get_object.return_value = fake_obj

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.utils.telemetry.record_call") as mock_record,
    ):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 200
        # Consume stream
        content = b"".join(response.iter_bytes())
        assert content == b"fos-data"
        mock_record.assert_called_once()
        fake_body.close.assert_called_once()


def test_download_file_from_cdn_success(client, override_source, tmp_path):
    override_source["cdn_url"] = "http://mycdn.com"
    override_source["cdn_secret"] = "secret123"
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
        assert response.status_code == 200
        # Consume stream
        content = b"".join(response.iter_bytes())
        assert content == b"cdn-data"
        mock_urlopen.assert_called_once()
        mock_record.assert_called_once()
        fake_resp.close.assert_called_once()


def test_download_file_from_cdn_fails_raises_internal(client, override_source, tmp_path):
    override_source["cdn_url"] = "http://mycdn.com"
    override_source["cdn_secret"] = "secret123"
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("urllib.request.urlopen", side_effect=ValueError("connection refused")),
    ):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 502  # raise_internal maps to 502 as specified in download_file


def test_download_folder(client, override_source):
    fake_fos = MagicMock()
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw_prefix/raw/foo/file1.log"},
                {"Key": "raw_prefix/raw/foo/sub/file2.log"},
                {"Key": "raw_prefix/raw/foo/dir_marker/"},  # skipped
            ]
        }
    ]
    fake_fos.get_paginator.return_value = fake_paginator

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.routers.admin.downloads._fetch_file_to_zip") as mock_fetch,
    ):
        response = client.get("/api/download-folder?prefix=foo&root=raw")
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/zip"
        # Consume StreamingResponse to trigger populate generator
        content = b"".join(response.iter_bytes())
        assert len(content) > 0

        # Verify paginator was called with correct Prefix
        fake_fos.get_paginator.assert_called_with("list_objects_v2", caller_hint="download_zip")
        fake_paginator.paginate.assert_called_with(Bucket="test-bucket", Prefix="raw_prefix/raw/foo/")

        # Verify _fetch_file_to_zip was called for files (excluding folder marker)
        assert mock_fetch.call_count == 2


def test_download_all_local_include(client, override_source, tmp_path):
    db_file = tmp_path / "test.duckdb"
    db_file.write_text("duckdb-binary-data")

    override_source["duckdb_path"] = str(db_file)
    override_source["prefix"] = "raw_prefix"

    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)
    local_file = cache_dir / "raw_prefix" / "cached_file.parquet"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("parquet-bytes")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb.get_source_for_service", return_value=override_source),
        patch("backend.config.load_config", return_value={"name": "service123", "service_id": "service123"}),
    ):
        response = client.get("/api/download-all", params={"service_id": "service123", "include": "local"})
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/zip"
        content = b"".join(response.iter_bytes())
        assert len(content) > 0


def test_download_all_cloud_include(client, override_source):
    fake_fos = MagicMock()
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw_prefix/raw/file1.log"},
                {"Key": "raw_prefix/raw/file2.log"},
            ]
        }
    ]
    fake_fos.get_paginator.return_value = fake_paginator

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.routers.admin.downloads._fetch_file_to_zip") as mock_fetch,
        patch("backend.core.duckdb.get_source_for_service", return_value=override_source),
        patch("backend.config.load_config", return_value={"name": "service123", "service_id": "service123"}),
    ):
        response = client.get("/api/download-all", params={"service_id": "service123", "include": "cloud"})
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/zip"
        content = b"".join(response.iter_bytes())
        assert len(content) > 0
        assert mock_fetch.call_count == 2


# ── Merged from tests/routers/test_admin_quarantine.py ──
def clean_res(data: dict) -> dict:

    return {key: value for key, value in data.items() if not key.startswith("_")}


def test_quarantine_evidence_routes_are_registered():
    paths = app.openapi()["paths"]
    assert "/api/admin/quarantine" in paths
    assert "/api/admin/quarantine/summary" in paths
    assert "/api/admin/quarantine/download/{item_id}" in paths
    assert "/api/admin/quarantine/purge" in paths


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
        patch(
            "backend.routers.admin.quarantine.count_quarantine_evidence",
            return_value=1,
        ) as count_items,
    ):
        response = client.get("/api/admin/quarantine?limit=20&offset=5&error_category=invalid_json")

    assert response.status_code == 200
    payload = clean_res(response.json())
    assert payload["total"] == 1
    assert payload["items"][0] == item
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

    assert response.status_code == 200
    assert clean_res(response.json()) == summary
    get_summary.assert_called_once_with("service123")


def test_analyst_path_a_read_only_source_is_denied(client, override_source):
    override_source["access_level"] = "read_only"
    with patch("backend.routers.admin.quarantine.list_quarantine_evidence") as list_items:
        response = client.get("/api/admin/quarantine")

    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "admin_only"
    list_items.assert_not_called()


def test_quarantine_routes_apply_endpoint_admin_dependency(client, override_source):
    def reject_admin():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = reject_admin
    try:
        response = client.get("/api/admin/quarantine/summary")
    finally:
        app.dependency_overrides.pop(require_admin, None)

    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "admin_only"


def test_download_quarantine_evidence_streams_exact_bytes(client, override_source):
    raw_bytes = b'{"invalid":"\x00\xff"}\r\n'
    item = {
        "id": 7,
        "file_path": "7.dat",
        "byte_length": len(raw_bytes),
        "original_key": "raw/request/batch.log.gz",
    }
    with patch(
        "backend.core.quarantine.read_evidence",
        return_value=(item, BytesIO(raw_bytes)),
    ):
        response = client.get("/api/admin/quarantine/download/7")

    assert response.status_code == 200
    assert response.content == raw_bytes
    assert response.headers["content-disposition"] == 'attachment; filename="quarantine-item-7.dat"'


def test_download_quarantine_evidence_returns_not_found(client, override_source):
    with patch("backend.core.quarantine.read_evidence", return_value=None):
        response = client.get("/api/admin/quarantine/download/999")

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "quarantine_not_found"


def test_download_quarantine_evidence_enforces_size_limit(client, override_source):
    item = {"id": 7, "file_path": "7.dat", "byte_length": 50 * 1024 * 1024 + 1}
    with patch(
        "backend.core.quarantine.read_evidence",
        return_value=(item, BytesIO(b"unused")),
    ):
        response = client.get("/api/admin/quarantine/download/7")

    assert response.status_code == 413


@pytest.mark.parametrize(
    ("item_id", "expected"),
    [(7, {"purged": 1}), (None, {"purged": 3})],
)
def test_purge_quarantine_evidence(client, override_source, item_id, expected):
    with (
        patch("backend.routers.admin.quarantine.get_quarantine_evidence", return_value={"id": item_id}),
        patch("backend.core.quarantine.purge_evidence", return_value=expected) as purge,
    ):
        query = "" if item_id is None else f"?item_id={item_id}"
        response = client.post(f"/api/admin/quarantine/purge{query}")

    assert response.status_code == 200
    assert clean_res(response.json()) == expected
    purge.assert_called_once_with("service123", item_id)


def test_purge_quarantine_evidence_unknown_item_returns_404(client, override_source):
    with patch("backend.routers.admin.quarantine.get_quarantine_evidence", return_value=None):
        response = client.post("/api/admin/quarantine/purge?item_id=999")

    assert response.status_code == 404


def test_quarantine_admin_dependency_denies_analyst_path_b(override_source):
    from tests.remote_access.test_middleware import _login_analyst, _seed_invite, _start_share

    _start_share()
    invite = _seed_invite(service_ids=["service123"])
    with TestClient(app) as client:
        _login_analyst(client, invite)
        response = client.get(
            "/api/admin/quarantine/summary",
            headers={"X-Remote-Analyst": "1", "Host": "testserver"},
        )

    assert response.status_code == 403


# ── Merged from tests/routers/test_admin_queries.py ──
@pytest.fixture
def _reset_registry():
    """Snapshot + restore the singleton's internal state so tests don't
    bleed into each other. Mirrors the conftest in test_query_registry.py."""
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
        # No env override — defaults to True (see settings.py).
        resp = client.get("/api/admin/app-config/query-monitor")
        assert resp.status_code == 200
        body = resp.json()
        assert body["enabled"] is True

    def test_app_config_endpoint_returns_false_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.get("/api/admin/app-config/query-monitor")
        assert resp.status_code == 200
        assert resp.json()["enabled"] is False

    def test_queries_endpoint_returns_404_when_disabled(self, client):
        # Endpoints flip to 404 (not 503) so the frontend treats the
        # feature as absent rather than broken — matches the comment in
        # admin_queries._ensure_enabled.
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.get("/api/admin/queries")
        assert resp.status_code == 404
        assert resp.json().get("detail") == {"error": "query_monitor_disabled"}

    def test_summary_endpoint_returns_404_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.get("/api/admin/queries/summary")
        assert resp.status_code == 404

    def test_cancel_endpoint_returns_404_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.post("/api/admin/queries/1/cancel")
        assert resp.status_code == 404


@pytest.mark.usefixtures("_reset_registry")
class TestSnapshotEndpoint:
    def test_empty_state_returns_zero_active(self, client):
        # Clear registry for deterministic empty state.
        query_registry._queries.clear()
        query_registry._history.clear()
        resp = client.get("/api/admin/queries")
        assert resp.status_code == 200
        body = resp.json()
        assert body["active"] == []
        assert body["completed"] == []
        assert body["last_seq"] == 0

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
        ids = [r["query_id"] for r in body["active"]]
        assert qid in ids
        row = next(r for r in body["active"] if r["query_id"] == qid)
        assert row["db_type"] == "DuckDB"
        assert row["attribution"]["kind"] == "admin"
        # SQL preview, not full SQL on this endpoint
        assert row["sql"] is None
        assert "SELECT 1" in row["sql_preview"]
        query_registry.deregister(qid)

    def test_include_completed_returns_history(self, client):
        query_registry._queries.clear()
        query_registry._history.clear()
        qid = query_registry.register("SQLite", "DROP TABLE x", con=None)
        query_registry.deregister(qid)

        resp = client.get("/api/admin/queries?include_completed=true")
        body = resp.json()
        assert any(r["query_id"] == qid for r in body["completed"])

    def test_since_seq_keeps_active_visible_across_polls(self, client):
        """The /api/admin/queries endpoint must keep every currently-active
        query visible regardless of since_seq — the delta-poll UI tracks
        only the highest seq it's seen, and a long-running query with id
        below that watermark must NOT disappear from subsequent polls.

        (since_seq still filters the completed list — that's where the
        wire-byte savings come from.)
        """
        query_registry._queries.clear()
        a = query_registry.register("SQLite", "A", con=None)
        b = query_registry.register("SQLite", "B", con=None)

        resp = client.get(f"/api/admin/queries?since_seq={a}")
        body = resp.json()
        ids = [r["query_id"] for r in body["active"]]
        assert b in ids
        assert a in ids, "active query 'a' must remain visible even though its id is below since_seq"
        query_registry.deregister(a)
        query_registry.deregister(b)


@pytest.mark.usefixtures("_reset_registry")
class TestSummaryEndpoint:
    def test_summary_shape(self, client):
        query_registry._queries.clear()
        resp = client.get("/api/admin/queries/summary")
        assert resp.status_code == 200
        body = resp.json()
        assert body["active_total"] == 0
        assert body["by_db_type"] == {}
        assert body["longest_ms"] == 0.0

    def test_summary_counts_active_queries(self, client):
        query_registry._queries.clear()
        a = query_registry.register("DuckDB", "SELECT 1", con=None)
        b = query_registry.register("SQLite", "SELECT 2", con=None)
        resp = client.get("/api/admin/queries/summary")
        body = resp.json()
        assert body["active_total"] == 2
        assert body["by_db_type"]["DuckDB"] == 1
        assert body["by_db_type"]["SQLite"] == 1
        assert body["longest_ms"] >= 0
        query_registry.deregister(a)
        query_registry.deregister(b)


@pytest.mark.usefixtures("_reset_registry")
class TestPerQueryEndpoint:
    def test_unknown_qid_returns_404(self, client):
        resp = client.get("/api/admin/queries/9999999")
        assert resp.status_code == 404
        assert resp.json().get("detail") == {"error": "query_not_found"}

    def test_known_qid_returns_full_row_with_sql(self, client):
        query_registry._queries.clear()
        qid = query_registry.register("DuckDB", "SELECT something_specific_xyz", con=None)
        resp = client.get(f"/api/admin/queries/{qid}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["query_id"] == qid
        # /queries returns sql=None; /queries/{qid} returns the full SQL
        assert body["sql"] is not None
        assert "something_specific_xyz" in body["sql"]
        query_registry.deregister(qid)


@pytest.mark.usefixtures("_reset_registry")
class TestCancelEndpoint:
    def test_cancel_unknown_qid_returns_state_not_found(self, client):
        resp = client.post("/api/admin/queries/9999999/cancel")
        # Always 200 with structured state — endpoint is idempotent.
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "not_found"
        assert body["query_id"] == 9999999

    def test_cancel_query_with_no_connection_returns_already_finished(self, client):
        qid = query_registry.register("SQLite", "SELECT 1", con=None)
        resp = client.post(f"/api/admin/queries/{qid}/cancel")
        assert resp.status_code == 200
        body = resp.json()
        # Registered with con=None → no cancellable handle.
        assert body["state"] == "already_finished"
        query_registry.deregister(qid)

    def test_cancel_rate_limit_kicks_in_after_10_per_second(self, client):
        # Reset rate-limit history before AND after — the bucket is
        # process-global keyed on admin id, and the test client uses the
        # default testserver client.host, so without the post-test reset
        # the next test in the same xdist worker would inherit a primed
        # bucket and see a spurious 429.
        from unittest.mock import patch

        from backend.routers import admin_queries as mod

        mod._cancel_history.clear()
        try:
            # Fire 11 requests in rapid succession to one qid (which won't
            # exist — irrelevant, the rate-limiter runs before the registry).
            last_status = None
            with patch("backend.routers.admin_queries.time.monotonic", return_value=1000.0):
                for _ in range(11):
                    resp = client.post("/api/admin/queries/9999999/cancel")
                    last_status = resp.status_code
            # The 11th should trip the limiter (10 per second per admin id).
            assert last_status == 429
        finally:
            mod._cancel_history.clear()


@pytest.mark.usefixtures("_reset_registry")
class TestAdminIdHelper:
    def test_admin_id_falls_back_to_client_host(self):
        from backend.routers.admin_queries import _admin_id_from_request

        class FakeClient:
            host = "127.0.0.1"

        class FakeReq:
            headers = {}
            client = FakeClient()

        assert _admin_id_from_request(FakeReq()) == "127.0.0.1"

    def test_admin_id_fallback_when_no_client(self):
        from backend.routers.admin_queries import _admin_id_from_request

        class FakeReq:
            headers = {}
            client = None

        # The `or` chain returns "unknown" (truthy) so the further `or
        # "admin"` fallback is unreachable in practice — this test pins
        # the actual behavior so a future refactor of the helper doesn't
        # silently lose the no-client guard.
        assert _admin_id_from_request(FakeReq()) == "unknown"


@pytest.mark.usefixtures("_reset_registry")
class TestCancelWithHandleAndAudit:
    def test_cancel_cancellable_duckdb_query_interrupts_and_records_audit(self, client):
        import time as _time

        from backend.core import metadata as _meta

        svc_id = "test_svc_cancel_audit"
        _meta.clear_slow_queries(svc_id)

        interrupted = []

        class FakeDuckDBCon:
            def interrupt(self):
                interrupted.append(True)

        fake_con = FakeDuckDBCon()
        prev = current_attribution.get()
        current_attribution.set(
            Attribution.admin(
                admin_id="127.0.0.1",
                request_path="/api/query",
                request_id="req-cancel-1",
            )
        )
        try:
            qid = query_registry.register("DuckDB", "SELECT * FROM logs", con=fake_con, service_id=svc_id)
        finally:
            current_attribution.set(prev)

        resp = client.post(f"/api/admin/queries/{qid}/cancel")
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "cancelled"
        assert body["query_id"] == qid
        assert interrupted == [True]

        query_registry.deregister(qid, error=RuntimeError("Query interrupted"))
        # Verify audit log entry was written for the service
        audits = _meta.list_audit(svc_id, limit=20)
        assert any(a["event_type"] == "query_cancelled" for a in audits)
        _ = _time.time()


@pytest.mark.usefixtures("_reset_registry")
class TestSlowQueriesEndpoints:
    def test_list_count_and_clear_slow_queries(self, client):
        import time as _time

        from backend.core import metadata as _meta

        svc_id = "test-service-id"
        _meta.clear_slow_queries(svc_id)
        now = _time.time()

        _meta.insert_slow_query(
            svc_id,
            {
                "query_id": 7001,
                "db_type": "DuckDB",
                "service_id": svc_id,
                "started_at_utc": now - 120.0,
                "ended_at_utc": now - 118.2,
                "duration_ms": 1800.0,
                "outcome": "ok",
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
                "attr_cron_job": None,
                "attr_cron_run_id": None,
                "attr_pool_slot": "pool#1",
                "error_type": None,
                "error_message": None,
                "peak_memory_mb": 64.5,
            },
        )
        _meta.insert_slow_query(
            svc_id,
            {
                "query_id": 7002,
                "db_type": "Postgres",
                "service_id": svc_id,
                "started_at_utc": now - 60.0,
                "ended_at_utc": now - 59.5,
                "duration_ms": 500.0,
                "outcome": "ok",
                "sql_preview": "SELECT * FROM ingested_files",
                "sql_full": "SELECT * FROM ingested_files WHERE service_id = $1",
                "sql_len": 50,
                "attr_kind": "cron",
                "attr_label": "cron:full_sync",
                "attr_principal_id": None,
                "attr_caller_qualname": "ingest_log.list_ingested_files",
                "attr_caller_file": "backend/core/metadata/ingest_log.py:100",
                "attr_request_path": None,
                "attr_request_id": None,
                "attr_cron_job": "full_sync",
                "attr_cron_run_id": 99,
                "attr_pool_slot": None,
                "error_type": None,
                "error_message": None,
                "peak_memory_mb": None,
            },
        )

        # Count with default 1000ms threshold -> only query 7001 qualifies
        cnt_resp = client.get(f"/api/admin/slow-queries/count?service_id={svc_id}&since_hours=24&threshold_ms=1000")
        assert cnt_resp.status_code == 200
        cnt_body = cnt_resp.json()
        assert cnt_body["count"] == 1
        assert cnt_body["since_hours"] == 24
        assert cnt_body["threshold_ms"] == 1000.0

        # List with 100ms threshold -> both rows returned
        list_resp = client.get(
            f"/api/admin/slow-queries?service_id={svc_id}&since_hours=24&threshold_ms=100&sort=duration"
        )
        assert list_resp.status_code == 200
        list_body = list_resp.json()
        assert len(list_body["rows"]) == 2
        assert list_body["rows"][0]["query_id"] == 7001
        assert list_body["rows"][0]["sql"] == "SELECT count(*) FROM logs WHERE status >= 500"
        assert list_body["rows"][0]["attribution"]["kind"] == "admin"
        assert list_body["rows"][0]["attribution"]["pool_slot"] == "pool#1"
        assert list_body["rows"][1]["attribution"]["cron_run_id"] == "99"

        # Filter by db_type=Postgres
        pg_resp = client.get(
            f"/api/admin/slow-queries?service_id={svc_id}&since_hours=24&threshold_ms=100&db_type=Postgres"
        )
        assert pg_resp.status_code == 200
        pg_rows = pg_resp.json()["rows"]
        assert len(pg_rows) == 1
        assert pg_rows[0]["query_id"] == 7002
        assert pg_rows[0]["db_type"] == "Postgres"

        # Clear persisted slow queries via DELETE /api/admin/slow-queries
        del_resp = client.delete(f"/api/admin/slow-queries?service_id={svc_id}")
        assert del_resp.status_code == 200
        del_body = del_resp.json()
        assert del_body["deleted"] == 2
        assert del_body["service_id"] == svc_id

        # Verify count is now 0
        after_cnt = client.get(f"/api/admin/slow-queries/count?service_id={svc_id}&since_hours=24&threshold_ms=100")
        assert after_cnt.status_code == 200
        assert after_cnt.json()["count"] == 0


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
        from types import SimpleNamespace

        from fastapi import HTTPException

        from backend.deps import require_admin
        from backend.utils.remote_access import _is_blocked_path

        raw_path = path.split("?", 1)[0]
        assert _is_blocked_path(raw_path) is True, f"Expected {raw_path} to be blocked for Analyst Path B"

        fake_req = SimpleNamespace(
            state=SimpleNamespace(analyst_session=SimpleNamespace(session_id="sess_analyst_qmon")),
            headers={},
            client=SimpleNamespace(host="203.0.113.55"),
        )
        with pytest.raises(HTTPException) as exc:
            require_admin(fake_req)  # type: ignore[arg-type]
        assert exc.value.status_code == 403

        # Also verify HTTP-layer rejection when require_admin evaluates an Analyst Path B session
        def _analyst_override():
            require_admin(fake_req)  # type: ignore[arg-type]

        client.app.dependency_overrides[require_admin] = _analyst_override
        try:
            resp = client.request(method, path)
            assert resp.status_code == 403
        finally:
            client.app.dependency_overrides.pop(require_admin, None)
