from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from backend.core.request_context import RequestContext, build_request_context
from backend.core.request_telemetry import RequestTelemetry
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.exports import ExportManager, get_export_manager
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
    rum_watermark = ServingWatermark(
        service_id,
        "rum_vitals",
        1,
        watermark.coverage_start,
        watermark.coverage_end,
        "cursor",
        "rum-event-2",
        "rum-event-2",
        True,
    )
    cmcd_watermark = ServingWatermark(
        service_id,
        "cmcd",
        1,
        watermark.coverage_start,
        watermark.coverage_end,
        "cursor",
        "cmcd-event-2",
        "cmcd-event-2",
        True,
    )
    return HighScaleService(
        service_id,
        client,
        b"test-secret",
        watermark,
        {"rum_vitals": rum_watermark},
        cmcd_watermark,
    )


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


def test_rum_facts_route_registered():
    assert "/api/high-scale/services/{service_id}/rum-facts/{domain}" in app.openapi()["paths"]


def test_cmcd_facts_route_registered():
    assert "/api/high-scale/services/{service_id}/cmcd-facts" in app.openapi()["paths"]


def test_export_routes_registered():
    paths = app.openapi()["paths"]
    assert "/api/high-scale/services/{service_id}/exports" in paths
    assert "/api/high-scale/services/{service_id}/exports/{export_id}" in paths
    assert "/api/high-scale/services/{service_id}/exports/{export_id}/cancel" in paths


def test_admin_can_request_and_read_export(client, test_service_source, monkeypatch):
    from backend.high_scale.registry import get_high_scale_service_registry

    manager = ExportManager(max_workers=1, max_rows=10, max_bytes=10_000)
    monkeypatch.setattr("backend.routers.high_scale.metadata.record_audit", lambda *args, **kwargs: None)
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    app.dependency_overrides[get_export_manager] = lambda: manager
    try:
        response = client.post(
            f"/api/high-scale/services/{test_service_source['service_id']}/exports",
            json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
        )
        assert response.status_code == 200, response.text
        export_id = response.json()["export_id"]
        status = client.get(f"/api/high-scale/services/{test_service_source['service_id']}/exports/{export_id}")
        assert status.status_code == 200, status.text
        assert status.json()["state"] == "completed"
        assert status.json()["rows_written"] == 2
    finally:
        manager.shutdown()


def test_export_cancel_is_admin_only(client, test_service_source, monkeypatch):
    from fastapi import HTTPException

    from backend.deps import require_admin
    from backend.high_scale.registry import get_high_scale_service_registry

    manager = ExportManager(max_workers=1)
    monkeypatch.setattr("backend.routers.high_scale.metadata.record_audit", lambda *args, **kwargs: None)
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    app.dependency_overrides[get_export_manager] = lambda: manager

    def reject_non_admin():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = reject_non_admin
    try:
        response = client.post(
            f"/api/high-scale/services/{test_service_source['service_id']}/exports",
            json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
        )
        assert response.status_code == 403
    finally:
        manager.shutdown()
        app.dependency_overrides.pop(require_admin, None)


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


def test_rum_facts_queries_configured_domain(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/rum-facts/rum_vitals",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["metadata"]["watermark"]["domain"] == "rum_vitals"


def test_rum_facts_rejects_unconfigured_domain(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/rum-facts/rum_errors",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_domain_not_configured"


def test_cmcd_facts_queries_configured_domain(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/cmcd-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["metadata"]["watermark"]["domain"] == "cmcd"


class _AggregateQueryClient:
    def __init__(self, rows):
        self.rows = rows
        self.last_sql = None
        self.last_params = None

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params
        return self.rows


def test_aggregates_queries_configured_domain_and_dimension(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    service = _service()
    object.__setattr__(
        service,
        "client",
        _AggregateQueryClient([{"value": "/", "aggregate_count": 3}, {"value": "/next", "aggregate_count": 1}]),
    )
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/aggregates",
        json={
            "domain": "request",
            "dimension": "url",
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["domain"] == "request"
    assert body["dimension"] == "url"
    assert body["request_count"] == 4
    assert body["top_values"] == [{"value": "/", "count": 3}, {"value": "/next", "count": 1}]
    assert body["exact"] is True


def test_aggregates_rejects_unconfigured_domain(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/aggregates",
        json={
            "domain": "rum_errors",
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_domain_not_configured"


def test_aggregates_masks_client_ip_dimension_for_analysts(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    service = _service()
    object.__setattr__(
        service,
        "client",
        _AggregateQueryClient([{"value": "203.0.113.42", "aggregate_count": 1}]),
    )
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)
    app.dependency_overrides[build_request_context] = lambda: _analyst_context(test_service_source, None)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/aggregates",
        json={
            "domain": "request",
            "dimension": "client_ip",
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
        },
    )

    assert response.status_code == 200, response.text
    value = response.json()["top_values"][0]["value"]
    assert value != "203.0.113.42"


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def test_queries_recent_tier_returns_rows_immediately(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    service = _service()
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)
    now = datetime.now(UTC)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/queries",
        json={
            "domain": "request",
            "start_time": _iso(now - timedelta(hours=2)),
            "end_time": _iso(now - timedelta(hours=1)),
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["plan"]["tier"] == "recent_facts"
    assert body["job_id"] is None
    assert len(body["rows"]) == 2


def test_queries_cold_tier_without_archive_configured_reports_unavailable(client, test_service_source):
    from backend.high_scale.registry import get_high_scale_service_registry

    service = _service()
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)
    now = datetime.now(UTC)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/queries",
        json={
            "domain": "request",
            "start_time": _iso(now - timedelta(days=60)),
            "end_time": _iso(now - timedelta(days=60) + timedelta(hours=1)),
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_cold_query_not_configured"


def test_queries_cold_tier_submits_and_completes_a_historical_job(client, test_service_source):
    from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
    from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
    from backend.high_scale.registry import get_high_scale_service_registry

    now = datetime.now(UTC)
    coverage_start = now - timedelta(days=60)
    coverage_end = coverage_start + timedelta(minutes=1)
    rows = ({"event_id": "1", "timestamp": _iso(coverage_start + timedelta(seconds=10))},)
    artifact_bytes = _parquet_bytes(rows)
    source = ArchiveSourceObject("test-service-id", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        "manifest-1",
        source,
        ArchiveArtifact(
            "s3://archive/artifacts/manifest-1.parquet",
            f"sha256:{_sha256(artifact_bytes)}",
            len(artifact_bytes),
            len(rows),
            len(artifact_bytes),
            "digest",
            "request.v1",
            "normalize.v1",
        ),
        coverage_start,
        coverage_end,
        now + timedelta(days=1),
        now + timedelta(days=2),
        1,
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)

    class _Catalog:
        def manifests_covering(self, service_id, domain, start, end):
            return (manifest,)

    service = _service()
    object.__setattr__(service, "manifest_catalog", _Catalog())
    object.__setattr__(service, "archive", archive)
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/queries",
        json={
            "domain": "request",
            "start_time": _iso(coverage_start),
            "end_time": _iso(coverage_end),
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["plan"]["tier"] == "cold_job"
    job_id = body["job_id"]
    assert job_id

    import time

    deadline = time.monotonic() + 2
    job_body = None
    while time.monotonic() < deadline:
        job_response = client.get(f"/api/high-scale/services/{test_service_source['service_id']}/queries/{job_id}")
        job_body = job_response.json()
        if job_body["state"] in {"completed", "failed"}:
            break
        time.sleep(0.01)

    assert job_body["state"] == "completed", job_body
    assert job_body["rows"] == [{"event_id": "1", "timestamp": _iso(coverage_start + timedelta(seconds=10))}]


def _parquet_bytes(rows) -> bytes:
    from io import BytesIO

    import pyarrow as pa
    import pyarrow.parquet as pq

    output = BytesIO()
    pq.write_table(pa.Table.from_pylist(list(rows)), output)
    return output.getvalue()


def _sha256(value: bytes) -> str:
    import hashlib

    return hashlib.sha256(value).hexdigest()
