import io
import json
import sys
import threading
from http.server import ThreadingHTTPServer
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

with patch.dict(sys.modules, {"validate_credentials": Mock(name="operator_credentials")}):
    from scripts.dev import report_server


@pytest.fixture
def report(monkeypatch):
    origins = []
    requests = []
    monkeypatch.setattr(
        report_server,
        "admin_origin",
        lambda environment: origins.append(environment) or "https://admin.example.com:8443",
    )

    def upstream(request, timeout):
        requests.append((request.full_url, timeout, dict(request.header_items())))
        response = io.BytesIO(b'{"active_service_id":"test-service"}')
        response.status = 200
        return response

    monkeypatch.setattr(report_server, "admin_urlopen", upstream)
    server = ThreadingHTTPServer(("127.0.0.1", 0), report_server.ReportRequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests, origins
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_report_bootstrap_uses_exact_origin_certificate_transport(report):
    origin, requests, origins = report
    with urlopen(f"{origin}/relics/bootstrap_status.json?env=remote-hs", timeout=5) as response:
        body = json.load(response)
    assert body == {"ok": True, "status": 200, "data": {"active_service_id": "test-service"}}
    assert origins == ["remote-hs"]
    assert requests == [("https://admin.example.com:8443/api/bootstrap", 3, {"Accept": "application/json"})]


def test_report_rejects_unknown_environment_before_certificate_selection(report):
    origin, requests, origins = report
    with pytest.raises(HTTPError) as error:
        urlopen(f"{origin}/relics/bootstrap_status.json?env=unknown", timeout=5)
    assert error.value.code == 400
    error.value.close()
    assert not requests
    assert not origins
