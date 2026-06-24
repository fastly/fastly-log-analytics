"""R-3b: FASTLY_MOCK_MODE gate on backend.core.fastly.client.fastly()
and backend.utils.ngwaf.fetch_verified_bots_paged.

Production never sets ``FASTLY_MOCK_MODE``; the gate is a no-op outside
the E2E + contract suites. These tests confirm the gate fires when the
env is set and falls through otherwise.
"""

from __future__ import annotations

from backend.core.fastly.client import fastly
from backend.core.fastly.mock_fixtures import is_mock_mode, mock_response
from backend.utils.ngwaf import fetch_verified_bots_paged


def test_is_mock_mode_reads_env(monkeypatch):
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    assert is_mock_mode() is False
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    assert is_mock_mode() is True
    monkeypatch.setenv("FASTLY_MOCK_MODE", "0")
    assert is_mock_mode() is False


def test_mock_response_post_service_returns_id_and_name():
    out = mock_response("POST", "/service", {"name": "Test CDN"})
    assert out["id"] == "mock-svc-id"
    assert out["name"] == "Test CDN"
    assert out["version"] == 1


def test_mock_response_get_service_returns_empty_list():
    # find_service_by_name pages this — empty means "name is novel".
    assert mock_response("GET", "/service") == []


def test_mock_response_unknown_endpoint_returns_ok_envelope():
    # Default catch-all — keeps an unfamiliar journey moving without
    # making up specific fields a future test could false-pin against.
    assert mock_response("GET", "/totally/uncovered/endpoint") == {"ok": True}


def test_mock_response_access_key_create_shape():
    # The mock returns the FLAT shape `ensure_fos_access_key` reads
    # (`key["access_key"]` / `key["secret_key"]`), not the JSON:API
    # `data.attributes.*` wrapper. Mirrors the orchestrator's
    # actual access pattern (backend/provision/fos_setup.py:368-376).
    out = mock_response("POST", "/resources/object-storage/access-keys", {"description": "x"})
    assert out["access_key"] == "AKIA_MOCK"
    assert out["secret_key"] == "SECRET_MOCK"  # gitleaks:allow — canned mock secret, not real
    assert out["description"] == "x"


def test_fastly_client_short_circuits_in_mock_mode(monkeypatch):
    """In mock mode the production fastly() must skip the real urlopen
    path entirely and return the fixture shape. Sanity: any token works."""
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    out = fastly("POST", "/service", {"name": "Smoke"}, token="any-token")
    assert out["id"] == "mock-svc-id"
    assert out["name"] == "Smoke"


def test_ngwaf_short_circuits_in_mock_mode(monkeypatch):
    """In mock mode fetch_verified_bots_paged yields one empty page and
    stops — no real /ngwaf/v1/workspaces/:id/requests call is made."""
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    pages = list(
        fetch_verified_bots_paged(
            api_key="any",
            workspace_id="ws-mock",
            from_ts="2026-06-01T00:00:00Z",
        )
    )
    assert pages == [([], None, 0)]
