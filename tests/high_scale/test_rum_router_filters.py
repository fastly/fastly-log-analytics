"""The RUM routes hand the ``filters`` query parameter to the high-scale readers instead of dropping it."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import PropertyMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.high_scale.rum import parse_rum_filters
from tests.conftest import override_request_context

_SVC = "svcA"
_FILTERS = {"browser": {"mode": "include", "values": ["Chrome"]}}


@pytest.fixture
def client():
    from backend.core.request_context import build_request_context
    from backend.routers import rum

    app = FastAPI()
    app.include_router(rum.router)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    with TestClient(app) as c:
        yield c


class _Service:
    service_id = _SVC
    client: Any = None


@pytest.fixture
def high_scale():
    with patch("backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock) as hs:
        hs.return_value = _Service()
        yield hs


def test_parse_rum_filters_accepts_objects_and_rejects_everything_else() -> None:
    assert parse_rum_filters(None) == {} and parse_rum_filters("") == {}
    assert parse_rum_filters(json.dumps(_FILTERS)) == _FILTERS
    with pytest.raises(ValueError, match="valid JSON"):
        parse_rum_filters("{not json")
    with pytest.raises(ValueError, match="JSON object"):
        parse_rum_filters("[1, 2]")


def test_analytics_route_passes_the_parsed_filters_through(client, high_scale) -> None:
    seen: list[Any] = []

    def fake(service, start, end, filters=None):
        seen.append(filters)
        return {"is_mock": False, "no_data": True}

    with patch("backend.high_scale.rum.rum_analytics", fake):
        client.get(f"/api/services/{_SVC}/rum/analytics", params={"filters": json.dumps(_FILTERS)})

    assert seen == [_FILTERS]


def test_live_events_route_passes_the_parsed_filters_through(client, high_scale) -> None:
    seen: list[Any] = []

    def fake(service, start, end, limit, filters=None):
        seen.append(filters)
        return []

    with patch("backend.high_scale.rum.rum_live_events", fake):
        response = client.get(f"/api/services/{_SVC}/rum/live-events", params={"filters": json.dumps(_FILTERS)})

    assert response.status_code == 200 and seen == [_FILTERS]


@pytest.mark.parametrize("path", ["rum/analytics", "rum/live-events"])
@pytest.mark.parametrize("filters", ['{"metric_name": {"mode": "include", "values": ["LCP"]}}', "{broken"])
def test_unsupported_or_malformed_filters_are_a_client_error_not_a_server_error(
    client, high_scale, path, filters
) -> None:
    class _Client:
        def execute(self, sql, params=None):
            raise AssertionError("no query may run for a rejected filter")

    _Service.client = _Client()

    response = client.get(f"/api/services/{_SVC}/{path}", params={"filters": filters})

    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "rum_filters_unsupported"
