from __future__ import annotations

import asyncio
import ssl
from urllib.request import Request

import httpx
import pytest

from scripts.lib import admin_http, admin_tls


def test_total_deadline_cancels_a_stalled_response_body(monkeypatch: pytest.MonkeyPatch) -> None:
    closed = []

    class Stalled(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"{"
            await asyncio.Event().wait()

        async def aclose(self) -> None:
            closed.append(True)

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=Stalled()))
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: ssl.create_default_context())
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(**kwargs, transport=transport))
    with pytest.raises(TimeoutError):
        asyncio.run(admin_http.async_admin_json(Request("https://example.com/api/test", data=b"{}"), timeout=0.05))
    assert closed


def test_deadline_transport_preserves_request_and_refuses_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"location": "https://other.example.com/private"})

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(respond)
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: ssl.create_default_context())
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(**kwargs, transport=transport))
    request = Request(
        "https://example.com/api/test", data=b'{"filters":{}}', headers={"X-Page-Load-ID": "test-correlation"}
    )
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(admin_http.async_admin_json(request, timeout=1))
    assert len(seen) == 1
    assert seen[0].method == "POST"
    assert seen[0].content == b'{"filters":{}}'
    assert seen[0].headers["x-page-load-id"] == "test-correlation"


@pytest.mark.parametrize(
    "url,headers",
    [
        ("https://other.example.com/api/test", {}),
        ("https://example.com/api/test", {"X-Admin-Gateway-Token": "do-not-send"}),
    ],
)
def test_deadline_transport_rejects_untrusted_identity_use_before_tls(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    headers: dict,
) -> None:
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})

    def forbidden(config: dict) -> ssl.SSLContext:
        pytest.fail("TLS identity must not be loaded")

    monkeypatch.setattr(admin_tls, "_context", forbidden)
    with pytest.raises(ValueError):
        asyncio.run(admin_http.async_admin_json(Request(url, headers=headers), timeout=1))
