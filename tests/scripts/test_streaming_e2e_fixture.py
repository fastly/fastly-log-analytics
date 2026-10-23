import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripts import run_contract_backend
from scripts.lib.streaming_e2e_fixture import install_streaming_fixture


def test_streaming_fixture_requires_an_isolated_contract_sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTRACT_STREAMING_FIXTURES", "1")
    monkeypatch.delenv("CONTRACT_CONFIGS_DIR", raising=False)
    monkeypatch.delenv("CONTRACT_DATA_DIR", raising=False)
    monkeypatch.setattr(sys, "argv", ["run_contract_backend.py"])
    with pytest.raises(ValueError, match="isolated contract sandbox"):
        run_contract_backend.main()


def test_streaming_fixture_is_cookie_scoped_and_records_ssr_requests() -> None:
    app = FastAPI()

    @app.post("/api/cmcd/aggregates")
    def aggregates() -> dict:
        return {"available": False}

    install_streaming_fixture(app)
    with TestClient(app) as browser, TestClient(app) as other_browser:
        payload = {"available": True, "overview": {"active_sessions": 1234}}
        assert browser.post("/api/__e2e__/streaming-fixture", json=payload).status_code == 200
        body = {"range_token": "24h", "bucket_seconds": 300, "top_n": 30}
        with TestClient(app) as ssr:
            ssr.cookies.update(browser.cookies)
            assert ssr.post("/api/cmcd/aggregates", json=body).json() == payload
        assert browser.get("/api/__e2e__/streaming-fixture").json() == {"bodies": [body]}
        assert other_browser.post("/api/cmcd/aggregates", json=body).json() == {"available": False}
        assert browser.delete("/api/__e2e__/streaming-fixture").status_code == 200
        assert browser.get("/api/__e2e__/streaming-fixture").status_code == 404
        assert browser.post("/api/cmcd/aggregates", json=body).json() == {"available": False}


def test_unavailable_fixture_does_not_reuse_another_browsers_populated_response() -> None:
    app = FastAPI()
    install_streaming_fixture(app)
    with TestClient(app) as populated, TestClient(app) as unavailable:
        populated.post("/api/__e2e__/streaming-fixture", json={"available": True})
        unavailable.post("/api/__e2e__/streaming-fixture", json={"available": False, "has_data": False})
        assert populated.post("/api/cmcd/aggregates", json={}).json() == {"available": True}
        assert unavailable.post("/api/cmcd/aggregates", json={}).json() == {"available": False, "has_data": False}
