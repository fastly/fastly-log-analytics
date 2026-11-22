from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.core.fastly import client as platform_client
from backend.core.request_telemetry.middleware import TelemetryResponseBodyMiddleware
from backend.routers import usage

START = "2026-11-17T00:00:00Z"
END = "2026-11-19T00:00:00Z"
D0 = 1_794_873_600
D1 = D0 + 86_400


@pytest.fixture(autouse=True)
def clear_usage_cache():
    usage._FASTLY_STATS_CACHE.clear()


def source(*, service_id: str = "service-a") -> dict:
    return {
        "service_id": service_id,
        "logging_service_id": "logging-service",
        "cdn_service_id": "cdn-service",
    }


def configure(monkeypatch, token: str = "token-a", workspace: str | None = "workspace-a"):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _service_id: token)
    monkeypatch.setattr(usage, "api_key_or_mock", lambda value: value)
    monkeypatch.setattr(svcconfig, "get_ngwaf_workspace_id", lambda _service_id: workspace)


def test_platform_usage_prefers_storage_gauges_and_keeps_partial_failures(monkeypatch):
    configure(monkeypatch)
    calls = []

    def metrics(**kwargs):
        calls.append(kwargs)
        if kwargs["entity_id"] is None:
            return {
                "data": [
                    {"start_time": D0, "object_storage_bytes_count": 1_000},
                    {
                        "start_time": D1,
                        "object_storage_bytes_count": 2_000,
                        "object_storage_buckets_count": 4,
                    },
                ]
            }
        raise RuntimeError("workspace unavailable")

    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)
    response = usage.platform_usage(start=START, end=END, source=source())

    assert response.source == "partial"
    assert response.storage.status == "ok"
    assert response.storage.bytes == 2_000
    assert response.storage.buckets == 4
    assert response.storage.gb_hours == pytest.approx(3_000 / 1024**3 * 24)
    assert response.compute.status == "not_configured"
    assert response.ngwaf.status == "unavailable"
    assert [call["entity_id"] for call in calls] == [None, "workspace-a"]
    assert all(call["token"] == "token-a" for call in calls)


def test_platform_usage_sparse_storage_gauges_carry_observed_values(monkeypatch):
    configure(monkeypatch, workspace=None)

    def metrics(**_kwargs):
        return {
            "data": [
                {
                    "start_time": D0,
                    "object_storage_bytes_count": 1_000,
                    "object_storage_buckets_count": 2,
                },
                {
                    "start_time": D1,
                    "object_storage_bytes_count": None,
                    "object_storage_buckets_count": None,
                },
            ]
        }

    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)
    response = usage.platform_usage(start=START, end=END, source=source())

    assert response.source == "fastly"
    assert response.storage.bytes == 1_000
    assert response.storage.buckets == 2
    assert response.storage.gb_hours == pytest.approx(2_000 / 1024**3 * 24)
    assert response.ngwaf.status == "not_configured"


def test_platform_usage_without_token_does_not_call_platform(monkeypatch):
    configure(monkeypatch, token="")
    monkeypatch.setattr(
        platform_client,
        "fastly_platform_metrics",
        lambda **_kwargs: pytest.fail("missing token must prevent Platform calls"),
    )

    response = usage.platform_usage(start=START, end=END, source=source())

    assert response.source == "unavailable"
    assert response.storage.status == "unavailable"
    assert response.storage.reason == "missing_token"
    assert response.compute.status == "not_configured"
    assert response.ngwaf.status == "unavailable"
    assert response.ngwaf.reason == "missing_token"


def test_platform_usage_without_workspace_does_not_call_ngwaf(monkeypatch):
    configure(monkeypatch, workspace=None)
    calls = []

    def metrics(**kwargs):
        calls.append(kwargs)
        return {"data": [{"start_time": D0, "object_storage_bytes_count": 1}]}

    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)
    response = usage.platform_usage(start=START, end=END, source=source())

    assert response.ngwaf.status == "not_configured"
    assert len(calls) == 1
    assert calls[0]["entity_id"] is None


def test_platform_usage_no_storage_rows_is_unavailable_not_zero(monkeypatch):
    configure(monkeypatch, workspace=None)
    monkeypatch.setattr(platform_client, "fastly_platform_metrics", lambda **_kwargs: {"data": []})

    response = usage.platform_usage(start=START, end=END, source=source())

    assert response.storage.status == "unavailable"
    assert response.storage.bytes is None
    assert response.storage.gb_hours is None
    assert response.storage.reason == "no_data"


def test_platform_usage_sums_ngwaf_counters_independently(monkeypatch):
    configure(monkeypatch)

    def metrics(**kwargs):
        if kwargs["entity_id"] is None:
            return {"data": [{"start_time": D0, "object_storage_bytes_count": 1}]}
        return {
            "data": [
                {
                    "start_time": D0,
                    "ngwaf_requests_total_count": 10,
                    "ngwaf_requests_allowed_count": 6,
                    "ngwaf_requests_blocked_count": 2,
                    "ngwaf_requests_challenged_count": 1,
                    "ngwaf_requests_logged_count": 1,
                },
                {
                    "start_time": D1,
                    "ngwaf_requests_total_count": 5,
                    "ngwaf_requests_blocked_count": 3,
                    "ngwaf_requests_timeout_count": 1,
                    "ngwaf_requests_unknown_count": 1,
                },
            ]
        }

    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)
    response = usage.platform_usage(start=START, end=END, source=source())

    assert response.source == "fastly"
    assert response.ngwaf.status == "ok"
    assert response.ngwaf.total == 15
    assert response.ngwaf.allowed == 6
    assert response.ngwaf.blocked == 5
    assert response.ngwaf.challenged == 1
    assert response.ngwaf.logged == 1
    assert response.ngwaf.timeout == 1
    assert response.ngwaf.unknown == 1


def test_platform_usage_uses_token_scoped_cache_and_never_leaks_account_details(monkeypatch):
    workspace = "workspace-sensitive"
    tokens = iter(("token-first", "token-second"))
    configure(monkeypatch, token=workspace, workspace=workspace)
    calls = []

    def metrics(**kwargs):
        calls.append(kwargs["token"])
        return {"data": [{"start_time": D0, "object_storage_bytes_count": 1}]}

    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)
    first_token = next(tokens)
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _service_id: first_token)
    first = usage.platform_usage(start=START, end=END, source=source())
    usage.platform_usage(start=START, end=END, source=source())
    second_token = next(tokens)
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _service_id: second_token)
    second = usage.platform_usage(start=START, end=END, source=source())

    assert len(calls) == 4
    assert set(calls) == {"token-first", "token-second"}
    assert first.storage.scope == second.storage.scope == "account"
    wire = json.dumps(first.model_dump(mode="json"))
    assert workspace not in wire
    assert "token-first" not in wire
    assert "workspace_id" not in wire


def test_platform_usage_sanitizes_partial_source_errors(monkeypatch):
    configure(monkeypatch, token="token-sensitive")

    def metrics(**kwargs):
        raise RuntimeError(
            f"cursor=cursor-sensitive token={kwargs['token']} entity={kwargs['entity_id']} body=raw-sensitive"
        )

    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)
    response = usage.platform_usage(start=START, end=END, source=source())
    wire = json.dumps(response.model_dump(mode="json"))

    assert response.source == "unavailable"
    assert response.storage.reason == "request_failed"
    assert response.ngwaf.reason == "request_failed"
    for sensitive in ("token-sensitive", "cursor-sensitive", "workspace-a", "raw-sensitive"):
        assert sensitive not in wire
        assert sensitive not in str(response.storage.reason)


def test_platform_usage_does_not_attach_raw_tracked_calls(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setattr(platform_client, "fastly_platform_metrics", lambda **_kwargs: {"data": []})
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    monkeypatch.setattr(
        "backend.utils.telemetry.get_tracked_calls",
        lambda: [
            {
                "service": "Fastly API",
                "method": "GET",
                "path": "/metrics/v2/platform/entities/workspace-a/hourly",
            }
        ],
    )

    response = usage.platform_usage(start=START, end=END, source=source())

    assert response.debug_calls == []
    wire = json.dumps(response.model_dump(mode="json"))
    assert "workspace-a" not in wire


def test_platform_usage_middleware_does_not_inject_workspace_path(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    monkeypatch.setenv("DEBUG_RESPONSES_FORCE_INCLUDE", "1")
    monkeypatch.setattr(
        "backend.utils.telemetry.get_tracked_calls",
        lambda: [
            {
                "service": "Fastly API",
                "method": "GET",
                "path": "/metrics/v2/platform/entities/workspace-a/hourly",
            }
        ],
    )
    monkeypatch.setattr(platform_client, "fastly_platform_metrics", lambda **_kwargs: {"data": []})

    app = FastAPI()
    app.add_middleware(TelemetryResponseBodyMiddleware)
    app.include_router(usage.usage_router)
    app.dependency_overrides[usage.get_source] = source
    with TestClient(app) as client:
        response = client.get("/api/usage/platform", params={"start": START, "end": END})

    assert response.status_code == 200
    body = response.text
    assert "workspace-a" not in body
    assert '"_debug_calls":[]' in body
