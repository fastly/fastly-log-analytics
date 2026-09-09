"""Admin API boundary, including real replay/loader code behind fake stores."""

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core.clickhouse_export import FosArtifacts
from backend.core.clickhouse_rows import PAYLOAD_COLUMNS
from backend.routers.admin import clickhouse as api
from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware
from tests.core.clickhouse_fos_fixture import TinyFos
from tests.core.test_clickhouse_publication import fixture_manifest

SERVICE = "TestPrototype"
STATUS = "/api/admin/clickhouse/status"
REPLAY = "/api/admin/clickhouse/replay"


@pytest.fixture
def boundary(monkeypatch):
    cfg = {"service_id": SERVICE, "access_level": "read_write"}
    monkeypatch.setattr(api.config, "load_config", lambda sid: cfg if sid == SERVICE else None)
    store, ch, objects, audit = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    store.admin_status.return_value = {
        "schema_version": 1,
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
