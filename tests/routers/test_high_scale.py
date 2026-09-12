from datetime import UTC, datetime
from types import SimpleNamespace

from backend.core.request_context import RequestContext, build_request_context
from backend.core.request_telemetry import RequestTelemetry
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry import HighScaleService, HighScaleServiceRegistry
from backend.main import app
from backend.utils.remote_access import TimeBounds


class _QueryClient:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=None):
        return self.rows


def _service(service_id="test-service-id"):
    watermark = ServingWatermark(
        service_id,
        "request",
        1,
        datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        datetime(2026, 9, 11, 20, 5, tzinfo=UTC),
        "cursor",
        "event-2",
        "event-2",
        True,
    )
    client = _QueryClient(
        [
            {
                "event_id": "event-1",
                "timestamp": datetime(2026, 9, 11, 20, 1, tzinfo=UTC),
                "service_id": service_id,
                "client_ip": "203.0.113.42",
                "url": "/",
            },
            {
                "event_id": "event-2",
                "timestamp": datetime(2026, 9, 11, 20, 2, tzinfo=UTC),
                "service_id": service_id,
                "client_ip": "203.0.113.43",
                "url": "/next",
            },
        ]
    )
    return HighScaleService(service_id, client, b"test-secret", watermark)


def _registry(service=None):
    return HighScaleServiceRegistry({service.service_id: service} if service else {})


def _analyst_context(source, con):
    session = SimpleNamespace(
        service_ids=[source["service_id"]],
        pii_policy={"mask_ips": True},
    )
    return RequestContext(
        service_id=source["service_id"],
        source=source,
        con=con,
        telemetry=RequestTelemetry("POST", "/api/high-scale/services/{service_id}/request-facts"),
        analyst_session=session,
        time_bounds=TimeBounds(
            start=datetime(2026, 9, 11, 19, 0, tzinfo=UTC),
            end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
        ),
    )


def test_request_facts_route_registered():
    assert "/api/high-scale/services/{service_id}/request-facts" in app.openapi()["paths"]


def test_admin_can_query_request_facts(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
            "limit": 1,
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["rows"][0]["client_ip"] == "203.0.113.42"
    assert body["metadata"]["watermark"]["service_id"] == test_service_source["service_id"]
    assert body["next_cursor"]


def test_analyst_request_facts_masks_client_ip(client, in_memory_duckdb, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    app.dependency_overrides[build_request_context] = lambda: _analyst_context(test_service_source, in_memory_duckdb)
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["rows"][0]["client_ip"] == "203.0.113.xxx"


def test_request_facts_rejects_service_binding_mismatch(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    class _MismatchRegistry:
        def resolve(self, service_id):
            return _service("other-service-id")

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _MismatchRegistry()
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "high_scale_service_mismatch"


def test_request_facts_rejects_non_high_scale_service(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry()
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_service_not_configured"


def test_request_facts_rejects_invalid_cursor(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
            "cursor": "not-a-valid-cursor",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "bad_request"
