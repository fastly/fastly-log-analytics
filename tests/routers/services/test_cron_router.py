"""HTTP-layer tests for backend/routers/services/cron.py.

Three endpoints:
- ``GET /api/cron-runs`` — paginated list with filtering
- ``DELETE /api/cron-runs/{log_id}`` — delete one run
- ``DELETE /api/cron-runs`` — bulk purge by task / age
"""

from __future__ import annotations

from backend.core import metadata_db
from tests.conftest import MOCK_SERVICE_ID


def _seed_cron_runs(service_id: str, runs: list[dict]) -> list[int]:
    """Insert raw rows into cron_runs and return their ids."""
    con = metadata_db.get_con(service_id)
    ids: list[int] = []
    for r in runs:
        cur = con.execute(
            "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) "
            "VALUES (?, ?, ?, ?, '[]', ?)",
            (
                r.get("task", "sync"),
                r.get("started_at", "2026-05-15T00:00:00Z"),
                r.get("duration_s", 1.0),
                r.get("status", "success"),
                r.get("summary", "ok"),
            ),
        )
        ids.append(int(cur.lastrowid or 0))
    con.commit()
    return ids


# ── GET /api/cron-runs ────────────────────────────────────────────────────────


def test_cron_logs_returns_seeded_runs(client, test_service_source):
    _seed_cron_runs(
        test_service_source["name"],
        [
            {"task": "sync", "status": "success"},
            {"task": "commit", "status": "success"},
            {"task": "sync", "status": "error"},
        ],
    )

    r = client.get("/api/cron-runs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 3
    assert len(body["entries"]) == 3


def test_cron_logs_filters_by_task(client, test_service_source):
    _seed_cron_runs(
        test_service_source["name"],
        [
            {"task": "sync"},
            {"task": "sync"},
            {"task": "commit"},
        ],
    )

    r = client.get(
        "/api/cron-runs",
        params={"task": "sync"},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 2
    assert all(e["task"] == "sync" for e in body["entries"])


def test_cron_logs_filters_by_status(client, test_service_source):
    _seed_cron_runs(
        test_service_source["name"],
        [
            {"status": "success"},
            {"status": "error"},
            {"status": "running"},
        ],
    )

    r = client.get(
        "/api/cron-runs",
        params={"status": "running"},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert body["entries"][0]["status"] == "running"


def test_cron_logs_pagination(client, test_service_source):
    _seed_cron_runs(test_service_source["name"], [{"task": "sync"} for _ in range(10)])

    r = client.get(
        "/api/cron-runs",
        params={"page": 2, "per_page": 4},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 10
    assert body["page"] == 2
    assert body["per_page"] == 4
    assert len(body["entries"]) == 4


def test_cron_logs_per_page_validation(client):
    r = client.get(
        "/api/cron-runs",
        params={"per_page": 9999},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert r.status_code == 422


# ── DELETE /api/cron-runs/{log_id} ───────────────────────────────────────────


def test_delete_cron_log_removes_row(client, test_service_source):
    [keep, kill] = _seed_cron_runs(test_service_source["name"], [{"task": "sync"}, {"task": "sync"}])

    r = client.delete(f"/api/cron-runs/{kill}", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert r.status_code == 200
    assert r.json()["ok"] is True

    con = metadata_db.get_con(test_service_source["name"])
    remaining = [row[0] for row in con.execute("SELECT id FROM cron_runs").fetchall()]
    assert kill not in remaining
    assert keep in remaining


def test_delete_cron_log_unknown_id_is_noop(client, test_service_source):
    """Per the impl: delete is best-effort; deleting a non-existent id returns 200."""
    r = client.delete("/api/cron-runs/9999999", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert r.status_code == 200


# ── DELETE /api/cron-runs (bulk purge) ───────────────────────────────────────


def test_purge_all_cron_logs(client, test_service_source):
    _seed_cron_runs(test_service_source["name"], [{"task": "sync"}, {"task": "commit"}])

    r = client.delete("/api/cron-runs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert r.status_code == 200
    assert r.json()["ok"] is True
    con = metadata_db.get_con(test_service_source["name"])
    assert con.execute("SELECT count(*) FROM cron_runs").fetchone()[0] == 0


def test_purge_by_task_only_removes_matching(client, test_service_source):
    _seed_cron_runs(
        test_service_source["name"],
        [
            {"task": "sync"},
            {"task": "sync"},
            {"task": "commit"},
        ],
    )

    r = client.delete(
        "/api/cron-runs",
        params={"task": "sync"},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert r.status_code == 200
    con = metadata_db.get_con(test_service_source["name"])
    remaining_tasks = [row[0] for row in con.execute("SELECT task FROM cron_runs").fetchall()]
    assert "sync" not in remaining_tasks
    assert "commit" in remaining_tasks
