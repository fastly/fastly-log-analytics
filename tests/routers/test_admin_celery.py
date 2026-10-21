"""Tests for the Admin Task Queue & Ingest Ledger API surface:
``GET /api/admin/celery/status`` and ``GET /api/admin/metric-history/batch``.

Covers:
1. Queue depths across KNOWN_QUEUES ("celery", "q.ingest", "q.control") and
   dynamically discovered ``q.*`` queues.
2. Worker status (active_tasks, stats, registered, scheduled, count, error).
3. RedBeat schedules (``redbeat:<name>`` hashes vs ``redbeat::*`` statics).
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
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from backend.core import metric_snapshots
from backend.deps import require_admin
from backend.utils.remote_access import _is_blocked_path


def _mock_redis(queues: dict[str, int], redbeat: dict[str, str]):
    """Build a MagicMock redis client backing scan_iter/type/llen/hget."""
    r = MagicMock()

    def scan_iter(match=None, count=None):
        if match == "q.*":
            return iter([k for k in queues if k.startswith("q.")])
        if match == "redbeat:*":
            return iter(list(redbeat.keys()) + ["redbeat::schedule", "redbeat::lock"])
        return iter([])

    r.scan_iter.side_effect = scan_iter
    r.type.side_effect = lambda k: "list" if k in queues else "none"
    r.llen.side_effect = lambda k: queues.get(k, 0)
    r.hget.side_effect = lambda k, f: json.dumps({"task": redbeat[k]}) if f == "definition" and k in redbeat else None
    return r


class TestCeleryStatusHighScaleAndStandard:
    def test_high_scale_celery_status_full_payload(self, client):
        """High-Scale / active Celery deployment returns queue depths, active workers,
        RedBeat schedules, and per-service ingest ledger state counts."""
        queues = {"q.ingest": 14, "q.control": 1, "celery": 3, "q.custom": 2}
        redbeat = {
            "redbeat:discover-svc-a": "backend.tasks.ingest.discover_service",
            "redbeat:commit-svc-a": "backend.tasks.commit.commit_service",
        }
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
            patch("backend.celery_status._get_redis", return_value=_mock_redis(queues, redbeat)),
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
            assert data["schedules"] == [
                {"name": "discover-svc-a", "task": "backend.tasks.ingest.discover_service"},
                {"name": "commit-svc-a", "task": "backend.tasks.commit.commit_service"},
            ]
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
        empty schedules, and empty ledger."""
        with (
            patch("backend.celery_app.app.control.inspect") as mock_inspect,
            patch("backend.celery_status._get_redis", return_value=_mock_redis({}, {})),
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
            assert data["schedules"] == []
            assert data["ledger"] == {}

    def test_standard_mode_unreachable_broker_fallback(self, client):
        """When Valkey/Redis is down or unconfigured, broker_reachable is False and
        workers.error records the inspect failure without raising a 500."""
        broken_redis = MagicMock()
        broken_redis.scan_iter.side_effect = ConnectionError("Valkey connection refused")
        with (
            patch("backend.celery_app.app.control.inspect") as mock_inspect,
            patch("backend.celery_status._get_redis", return_value=broken_redis),
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
            assert data["schedules"] == []
            assert data["ledger"] == {}


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


class TestAnalystRbacRejection:
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


def test_task_routes_and_known_queues_consistency():
    """Verify KNOWN_QUEUES covers all routed queues in backend.celery_app."""
    import importlib

    from backend.celery_app import app
    from backend.celery_status import KNOWN_QUEUES

    for mod in app.conf.include:
        importlib.import_module(mod)

    registered = set(app.tasks.keys())
    for pattern in app.conf.task_routes:
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            assert any(t.startswith(prefix) for t in registered)
        else:
            assert pattern in registered

    routed = {spec["queue"] for spec in app.conf.task_routes.values()}
    routed.add(app.conf.task_default_queue or "celery")
    assert routed <= set(KNOWN_QUEUES)


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
