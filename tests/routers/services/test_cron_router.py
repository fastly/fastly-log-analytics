"""HTTP-layer tests for backend/routers/services/cron.py.

Three endpoints:
- ``GET /api/cron-runs`` — paginated list with filtering
- ``DELETE /api/cron-runs/{log_id}`` — delete one run
- ``DELETE /api/cron-runs`` — bulk purge by task / age
"""

from __future__ import annotations

from backend.core import metadata as metadata_db
from tests.conftest import MOCK_SERVICE_ID


def _seed_cron_runs(service_id: str, runs: list[dict]) -> list[int]:
    """Insert raw rows into cron_runs and return their ids."""
    con = metadata_db.get_con(service_id)
    ids: list[int] = []
    for r in runs:
        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys, summary) "
            "VALUES (?, ?, ?, ?, ?, '[]', ?)",
            (
                service_id,
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


def test_cron_logs_since_id_returns_only_newer_rows(client, test_service_source):
    """O5 delta poll: passing ?since_id=X returns rows with id > X,
    plus any row whose status is still 'running' (visibility-keep so the
    client can detect completion of long-lived runs)."""
    ids = _seed_cron_runs(
        test_service_source["name"],
        [
            {"task": "sync", "status": "success"},
            {"task": "sync", "status": "running"},
            {"task": "commit", "status": "success"},
        ],
    )

    r = client.get(
        "/api/cron-runs",
        params={"since_id": ids[2]},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert r.status_code == 200
    body = r.json()
    returned_ids = {e["id"] for e in body["entries"]}
    # ids[2] excluded (id == since_id, not >); ids[1] included because
    # status='running' overrides the id cutoff; ids[0] excluded (old + done).
    assert returned_ids == {ids[1]}
    assert body["total"] == 1


def test_cron_logs_since_id_rejects_negative(client):
    """Validation: since_id must be >= 0 (run IDs are unsigned)."""
    r = client.get(
        "/api/cron-runs",
        params={"since_id": -1},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert r.status_code == 422


# ── DELETE /api/cron-runs/{log_id} ───────────────────────────────────────────


def test_delete_cron_log_removes_row(client, test_service_source):
    [keep, kill] = _seed_cron_runs(test_service_source["name"], [{"task": "sync"}, {"task": "sync"}])

    r = client.delete(f"/api/cron-runs/{kill}", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert r.status_code == 204
    assert r.content == b""

    con = metadata_db.get_con(test_service_source["name"])
    remaining = [row[0] for row in con.execute("SELECT id FROM cron_runs").fetchall()]
    assert kill not in remaining
    assert keep in remaining


def test_delete_cron_log_unknown_id_is_noop(client, test_service_source):
    """Per the impl: delete is best-effort; deleting a non-existent id succeeds."""
    r = client.delete("/api/cron-runs/9999999", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert r.status_code == 204


# ── DELETE /api/cron-runs (bulk purge) ───────────────────────────────────────


def test_purge_all_cron_logs(client, test_service_source):
    _seed_cron_runs(test_service_source["name"], [{"task": "sync"}, {"task": "commit"}])

    r = client.delete("/api/cron-runs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert r.status_code == 204
    assert r.content == b""
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

    assert r.status_code == 204
    con = metadata_db.get_con(test_service_source["name"])
    remaining_tasks = [row[0] for row in con.execute("SELECT task FROM cron_runs").fetchall()]
    assert "sync" not in remaining_tasks
    assert "commit" in remaining_tasks


# ── Error paths (pin the 500 fallbacks) ──────────────────────────────────────


def test_get_cron_logs_returns_500_on_repo_failure(client, monkeypatch):
    """The router catches any exception from the repository and surfaces
    it as a 500 with the ``raise_internal`` shape: generic ``error``
    code + ``error_id`` for correlation, never the raw exception
    string (that would leak repo internals / SQL fragments). Without
    this test the except branch is silently uncovered — a future
    refactor that drops the try/except would still pass CI.
    """
    from backend.routers.services import cron as _cron_router

    def _boom(*_a, **_kw):
        raise RuntimeError("simulated repo failure with internal SQL leak")

    monkeypatch.setattr(_cron_router, "get_cron_logs", _boom)
    r = client.get("/api/cron-runs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert r.status_code == 500
    body = r.json()["detail"]
    assert body["error"] == "cron_logs_read_failed"
    assert "error_id" in body
    assert "simulated repo failure" not in body["error"]


def test_delete_cron_log_returns_500_on_repo_failure(client, monkeypatch):
    from backend.routers.services import cron as _cron_router

    def _boom(*_a, **_kw):
        raise RuntimeError("simulated delete failure")

    monkeypatch.setattr(_cron_router, "delete_cron_log", _boom)
    r = client.delete("/api/cron-runs/1", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert r.status_code == 500
    body = r.json()["detail"]
    assert body["error"] == "cron_log_delete_failed"
    assert "error_id" in body
    assert "simulated delete failure" not in body["error"]


def test_purge_cron_logs_returns_500_on_repo_failure(client, monkeypatch):
    """Purge has a slightly different error shape (``ok: False``) so the
    body asserts both fields, not just the error."""
    from backend.routers.services import cron as _cron_router

    def _boom(*_a, **_kw):
        raise RuntimeError("simulated purge failure")

    monkeypatch.setattr(_cron_router, "purge_cron_logs", _boom)
    r = client.delete("/api/cron-runs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert r.status_code == 500
    detail = r.json()["detail"]
    # raise_internal hides the upstream exception message (security) and
    # returns a machine-readable code + correlation id instead. Adjusted
    # from the pre-refactor assertion that pinned the leaked exception text.
    assert detail["error"] == "cron_logs_purge_failed"
    assert "error_id" in detail and len(detail["error_id"]) == 8
