from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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


def source(**overrides: Any) -> dict[str, Any]:
    value = {
        "service_id": "service-a",
        "logging_service_id": "logging-service",
        "cdn_service_id": "cdn-service",
    }
    value.update(overrides)
    return value


def configure(monkeypatch, token: str = "token-a"):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _service_id: token)
    monkeypatch.setattr(usage, "api_key_or_mock", lambda value: value)


def test_geography_groups_regions_and_computes_shares(monkeypatch):
    configure(monkeypatch)
    calls: list[dict[str, Any]] = []

    def metrics(**kwargs):
        calls.append(kwargs)
        return {
            "data": [
                {"start_time": D0, "region": "europe", "requests": 7, "bandwidth": 700},
                {"start_time": D1, "region": "usa", "requests": 3, "bandwidth": 300},
                {"start_time": D1, "region": "europe", "requests": 1, "bandwidth": 100},
            ]
        }

    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)

    response = usage.geography(start=START, end=END, group_by="region", source=source())

    assert response.source == "fastly"
    assert response.requests == 11
    assert response.bandwidth_bytes == 1100
    assert [(row.location, row.requests, row.bandwidth_bytes) for row in response.data] == [
        ("europe", 8, 800),
        ("usa", 3, 300),
    ]
    assert [(row.request_share, row.bandwidth_share) for row in response.data] == pytest.approx(
        [(8 / 11, 800 / 1100), (3 / 11, 300 / 1100)]
    )
    assert calls == [
        {
            "entity_id": "cdn-service",
            "granularity": "day",
            "from_epoch": D0,
            "to_epoch": D0 + 2 * 86_400,
            "metrics": ("requests", "bandwidth"),
            "token": "token-a",
            "group_by": "region",
        }
    ]


def test_geography_queries_datacenter_for_delivery_service(monkeypatch):
    configure(monkeypatch)
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        platform_client,
        "fastly_platform_metrics",
        lambda **kwargs: (
            calls.append(kwargs) or {"data": [{"start_time": D0, "datacenter": "LHR", "requests": 0, "bandwidth": 0}]}
        ),
    )

    response = usage.geography(
        start=START,
        end=END,
        group_by="datacenter",
        source=source(),
    )

    assert response.source == "fastly"
    assert response.data[0].location == "LHR"
    assert response.data[0].request_share == 0.0
    assert response.data[0].bandwidth_share == 0.0
    assert calls[0]["entity_id"] == "cdn-service"
    assert calls[0]["group_by"] == "datacenter"


def test_geography_does_not_fall_back_to_the_logging_service(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setattr(
        platform_client,
        "fastly_platform_metrics",
        lambda **_kwargs: pytest.fail("missing delivery service must not query"),
    )

    response = usage.geography(
        start=START,
        end=END,
        group_by="region",
        source=source(cdn_service_id=""),
    )

    assert response.source == "unavailable"
    assert response.reason == "not_configured"


@pytest.mark.parametrize(
    ("src", "token", "reason"),
    [
        (source(cdn_service_id="", logging_service_id="", service_id=""), "token-a", "not_configured"),
        (source(), "", "missing_token"),
    ],
)
def test_geography_missing_token_or_service_is_unavailable(monkeypatch, src, token, reason):
    configure(monkeypatch, token=token)
    monkeypatch.setattr(
        platform_client,
        "fastly_platform_metrics",
        lambda **_kwargs: pytest.fail("unavailable geography must not call Fastly"),
    )

    response = usage.geography(start=START, end=END, group_by="region", source=src)

    assert response.source == "unavailable"
    assert response.reason == reason
    assert response.data == []


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ({"data": []}, "no_data"),
        ({"data": [{"start_time": D0, "region": "", "requests": 1, "bandwidth": 2}]}, "invalid_data"),
        ({"data": [{"start_time": D0, "region": "europe", "requests": "bad", "bandwidth": 2}]}, "invalid_data"),
    ],
)
def test_geography_rejects_empty_or_malformed_data(monkeypatch, payload, reason):
    configure(monkeypatch)
    monkeypatch.setattr(platform_client, "fastly_platform_metrics", lambda **_kwargs: payload)

    response = usage.geography(start=START, end=END, group_by="region", source=source())

    assert response.source == "unavailable"
    assert response.reason == reason
    assert response.data == []


def test_geography_sanitizes_platform_errors(monkeypatch):
    configure(monkeypatch, token="token-sensitive")

    def metrics(**_kwargs):
        raise RuntimeError("token=token-sensitive cursor=cursor-sensitive body=raw-sensitive")

    monkeypatch.setattr(platform_client, "fastly_platform_metrics", metrics)

    response = usage.geography(start=START, end=END, group_by="region", source=source())
    wire = json.dumps(response.model_dump(mode="json"))

    assert response.source == "unavailable"
    assert response.reason == "request_failed"
    for sensitive in ("token-sensitive", "cursor-sensitive", "raw-sensitive", "cdn-service"):
        assert sensitive not in wire


def test_geography_cache_is_token_scoped(monkeypatch):
    calls: list[str] = []
    tokens = iter(("token-first", "token-second"))
    monkeypatch.setattr(usage, "api_key_or_mock", lambda value: value)
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _service_id: next(tokens))
    monkeypatch.setattr(
        platform_client,
        "fastly_platform_metrics",
        lambda **kwargs: (
            calls.append(kwargs["token"])
            or {"data": [{"start_time": D0, "region": "europe", "requests": 1, "bandwidth": 2}]}
        ),
    )

    usage.geography(start=START, end=END, group_by="region", source=source())
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _service_id: "token-first")
    usage.geography(start=START, end=END, group_by="region", source=source())
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _service_id: "token-second")
    usage.geography(start=START, end=END, group_by="region", source=source())

    assert calls == ["token-first", "token-second"]


def test_geography_rejects_unknown_group(monkeypatch):
    configure(monkeypatch)
    app = FastAPI()
    app.include_router(usage.usage_router)
    app.dependency_overrides[usage.get_source] = lambda: source()

    with TestClient(app) as client:
        response = client.get(
            "/api/usage/geography",
            params={"start": START, "end": END, "group_by": "country"},
        )

    assert response.status_code == 422


def test_geography_middleware_does_not_inject_entity_path(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    monkeypatch.setenv("DEBUG_RESPONSES_FORCE_INCLUDE", "1")
    monkeypatch.setattr(
        "backend.utils.telemetry.get_tracked_calls",
        lambda: [
            {
                "service": "Fastly API",
                "method": "GET",
                "path": "/metrics/v2/platform/entities/cdn-service/daily",
            }
        ],
    )
    monkeypatch.setattr(
        platform_client,
        "fastly_platform_metrics",
        lambda **_kwargs: {"data": [{"start_time": D0, "region": "europe", "requests": 1, "bandwidth": 2}]},
    )

    app = FastAPI()
    app.add_middleware(TelemetryResponseBodyMiddleware)
    app.include_router(usage.usage_router)
    app.dependency_overrides[usage.get_source] = lambda: source()
    with TestClient(app) as client:
        response = client.get(
            "/api/usage/geography",
            params={"start": START, "end": END, "group_by": "region"},
        )

    assert response.status_code == 200
    assert "cdn-service" not in response.text
    assert '"_debug_calls":[]' in response.text
