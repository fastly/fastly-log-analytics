from unittest.mock import Mock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.core.request_context import get_source, require_admin
from backend.core.rollups import country_day_repair
from backend.routers.admin import compaction

PATH = "/api/admin/rebuild-country-daily-rollup"
PAYLOAD = {"day": "2020-01-02", "expected_requests": 24, "expected_countries": {"US": 23, "CA": 1}}


@pytest.fixture
def repair_api(monkeypatch):
    app = FastAPI()
    app.include_router(compaction.router)
    app.dependency_overrides[get_source] = lambda: {"name": "repair-api"}
    app.dependency_overrides[require_admin] = lambda: None
    result = {
        "day": "2020-01-02",
        "snapshot_id": 7,
        "raw_requests": 24,
        "hourly_requests": [1] * 24,
        "country_counts": {"US": 23, "CA": 1},
        "raw_first_event": "2020-01-02T00:00:00+00:00",
        "raw_last_event": "2020-01-02T23:59:59+00:00",
    }
    rebuild = Mock(return_value=result)
    monkeypatch.setattr(country_day_repair, "rebuild_country_day", rebuild)
    with TestClient(app) as client:
        yield app, client, rebuild


def test_country_repair_registered_and_returns_receipt(repair_api):
    app, client, rebuild = repair_api
    assert PATH in app.openapi()["paths"]
    response = client.post(PATH, json=PAYLOAD)
    assert response.status_code == 200
    assert response.json()["country_counts"] == {"US": 23, "CA": 1}
    rebuild.assert_called_once_with(
        "repair-api",
        {"name": "repair-api"},
        "2020-01-02",
        expected_requests=24,
        expected_countries={"US": 23, "CA": 1},
    )


@pytest.mark.parametrize(
    "patch",
    [
        {"day": "2020-1-2"},
        {"day": "../bad"},
        {"day": "9999-01-01"},
        {"expected_requests": 0},
        {"expected_countries": {}},
        {"expected_countries": {"US": -1}},
    ],
)
def test_invalid_scope_or_oracle_does_not_run(repair_api, patch):
    _, client, rebuild = repair_api
    assert client.post(PATH, json={**PAYLOAD, **patch}).status_code in {400, 422}
    rebuild.assert_not_called()


def test_mismatched_source_refuses_without_exposing_exception(repair_api):
    _, client, rebuild = repair_api
    rebuild.side_effect = RuntimeError("private infrastructure diagnostic")
    response = client.post(PATH, json=PAYLOAD)
    assert response.status_code == 409
    assert "private infrastructure" not in response.text


def test_analyst_cannot_invoke_repair(repair_api):
    app, client, rebuild = repair_api

    def denied():
        raise HTTPException(status_code=403)

    app.dependency_overrides[require_admin] = denied
    assert client.post(PATH, json=PAYLOAD).status_code == 403
    rebuild.assert_not_called()


FIELDS_PATH = "/api/admin/rebuild-daily-rollup-fields"
FIELDS_PAYLOAD = {
    "day": "2020-01-02",
    "expected_requests": 24,
    "expected_fields": {"status": {"200": 23, "404": 1}, "method": {"GET": 24}, "pop": {"TEST": 24}},
}


@pytest.fixture
def field_repair_api(repair_api, monkeypatch):
    app, client, country_rebuild = repair_api
    result = {key: value for key, value in country_rebuild.return_value.items() if key != "country_counts"}
    result["field_counts"] = FIELDS_PAYLOAD["expected_fields"]
    rebuild = Mock(return_value=result)
    monkeypatch.setattr(country_day_repair, "rebuild_daily_fields", rebuild, raising=False)
    return app, client, rebuild


def test_selected_field_repair_registered_and_returns_receipt(field_repair_api):
    app, client, rebuild = field_repair_api
    assert FIELDS_PATH in app.openapi()["paths"]
    response = client.post(FIELDS_PATH, json=FIELDS_PAYLOAD)
    assert response.status_code == 200
    assert response.json()["field_counts"] == FIELDS_PAYLOAD["expected_fields"]
    assert "country_counts" not in response.json()
    rebuild.assert_called_once_with(
        "repair-api",
        {"name": "repair-api"},
        "2020-01-02",
        expected_requests=24,
        expected_fields=FIELDS_PAYLOAD["expected_fields"],
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"day": "2020-1-2"},
        {"day": "../bad"},
        {"day": "9999-01-01"},
        {"expected_requests": 0},
        {"expected_fields": {}},
        {"expected_fields": {"status": {"200": -1}}},
        {"expected_fields": {"status": {"200": 25}}},
        {"expected_fields": {"status": {"": 24}}},
        {"expected_fields": {"ip": {"192.0.2.1": 24}}},
        {"expected_fields": {"country": {f"country-{i}": 1 for i in range(501)}}},
    ],
)
def test_invalid_selected_field_oracle_does_not_run(field_repair_api, changes):
    _, client, rebuild = field_repair_api
    assert client.post(FIELDS_PATH, json={**FIELDS_PAYLOAD, **changes}).status_code in {400, 422}
    rebuild.assert_not_called()


def test_selected_field_repair_refuses_without_exposing_exception(field_repair_api):
    _, client, rebuild = field_repair_api
    rebuild.side_effect = RuntimeError("private infrastructure diagnostic")
    response = client.post(FIELDS_PATH, json=FIELDS_PAYLOAD)
    assert response.status_code == 409
    assert "private infrastructure" not in response.text


def test_selected_field_repair_accepts_zero_nonempty_population(field_repair_api):
    _, client, rebuild = field_repair_api
    expected = {"pop": {}}
    rebuild.return_value["field_counts"] = expected
    response = client.post(FIELDS_PATH, json={**FIELDS_PAYLOAD, "expected_fields": expected})
    assert response.status_code == 200
    assert response.json()["field_counts"] == expected
    assert rebuild.call_args.kwargs["expected_fields"] == expected


def test_analyst_cannot_invoke_selected_field_repair(field_repair_api):
    app, client, rebuild = field_repair_api

    def denied():
        raise HTTPException(status_code=403)

    app.dependency_overrides[require_admin] = denied
    assert client.post(FIELDS_PATH, json=FIELDS_PAYLOAD).status_code == 403
    rebuild.assert_not_called()
