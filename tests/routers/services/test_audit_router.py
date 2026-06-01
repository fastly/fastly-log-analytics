"""HTTP-layer tests for backend/routers/services/audit.py.

Single endpoint: ``GET /api/audit-logs``. The audit log table is per-service
SQLite, so we use the existing ``client`` fixture (which routes get_source
to ``test_service_source``) and seed rows via ``metadata_db.record_audit``.
"""

from __future__ import annotations

from backend.core import metadata_db
from tests.conftest import MOCK_SERVICE_ID


def _seed_audit(source_name: str, n: int = 3, event_type: str = "service_renamed") -> None:
    for i in range(n):
        metadata_db.record_audit(source_name, event_type=event_type, details={"i": i})


def test_audit_logs_returns_seeded_entries(client, test_service_source):
    _seed_audit(test_service_source["name"], n=3, event_type="service_renamed")

    r = client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 3
    assert len(body["entries"]) == 3
    assert all(e["event_type"] == "service_renamed" for e in body["entries"])


def test_audit_logs_filters_by_event_type(client, test_service_source):
    _seed_audit(test_service_source["name"], n=2, event_type="service_renamed")
    _seed_audit(test_service_source["name"], n=4, event_type="time_range_cleared")

    r = client.get(
        "/api/audit-logs",
        params={"event_type": "time_range_cleared"},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 4
    assert all(e["event_type"] == "time_range_cleared" for e in body["entries"])


def test_audit_logs_pagination(client, test_service_source):
    _seed_audit(test_service_source["name"], n=10)

    r = client.get(
        "/api/audit-logs",
        params={"page": 2, "per_page": 3},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 10
    assert body["page"] == 2
    assert body["per_page"] == 3
    assert len(body["entries"]) == 3


def test_audit_logs_per_page_validation(client):
    """per_page > 1000 must be rejected by FastAPI Query validation."""
    r = client.get(
        "/api/audit-logs",
        params={"per_page": 5000},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert r.status_code == 422


def test_audit_logs_page_must_be_positive(client):
    r = client.get(
        "/api/audit-logs",
        params={"page": 0},
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert r.status_code == 422


def test_audit_logs_default_dir_is_descending(client, test_service_source):
    """Most-recent-first is the default — verify the entries come back DESC by ts."""
    _seed_audit(test_service_source["name"], n=5)

    r = client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert r.status_code == 200
    entries = r.json()["entries"]
    timestamps = [e["timestamp"] for e in entries]
    assert timestamps == sorted(timestamps, reverse=True), "default sort dir must be DESC"
