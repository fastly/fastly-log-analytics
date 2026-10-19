"""``resolve_fastly_token`` / ``require_fastly_token``: one credential-resolution path for cmcd, scoring and RUM routes."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.utils.router_utils import require_fastly_token, resolve_fastly_token


@pytest.fixture
def configs(monkeypatch):
    store: dict[str, dict | None] = {}
    loaded: list[str] = []

    def _load(service_id):
        loaded.append(service_id)
        return store.get(service_id)

    monkeypatch.setattr("backend.config.load_config", _load)
    # ``get_fastly_api_key`` falls back to the ACTIVE service for an empty id; the helpers must never do that.
    monkeypatch.setattr("backend.config.get_active_service_id", lambda *a, **k: "active-svc")
    store["active-svc"] = {"fastly_api_key": "ACTIVE-KEY"}
    store["svc-a"] = {"fastly_api_key": "stored-a"}
    store["svc-nokey"] = {"service_id": "svc-nokey"}
    store["svc-nullkey"] = {"fastly_api_key": None}
    return loaded


def test_override_wins_without_reading_config(configs) -> None:
    assert resolve_fastly_token("svc-a", "override") == "override"
    assert require_fastly_token("svc-a", "override") == "override"
    assert configs == []


@pytest.mark.parametrize("empty", ["", None])
def test_stored_key_used_when_override_is_empty(configs, empty) -> None:
    assert resolve_fastly_token("svc-a", empty) == "stored-a"
    assert require_fastly_token("svc-a", empty) == "stored-a"


def test_lenient_variant_returns_empty_string_instead_of_raising(configs) -> None:
    assert resolve_fastly_token("svc-missing") == ""
    assert resolve_fastly_token("svc-nokey") == ""
    assert resolve_fastly_token("svc-nullkey") == ""


def test_empty_service_id_never_borrows_the_active_services_key(configs) -> None:
    assert resolve_fastly_token("") == ""
    with pytest.raises(HTTPException) as exc:
        require_fastly_token("")
    assert exc.value.status_code == 404


def test_strict_variant_errors_match_the_historical_rum_bodies(configs) -> None:
    with pytest.raises(HTTPException) as missing:
        require_fastly_token("svc-missing")
    assert (missing.value.status_code, missing.value.detail) == (404, {"error": "No config for service svc-missing"})
    with pytest.raises(HTTPException) as nokey:
        require_fastly_token("svc-nokey")
    assert (nokey.value.status_code, nokey.value.detail) == (400, {"error": "No Fastly API key for service svc-nokey"})
