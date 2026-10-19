from __future__ import annotations

import httpx
import pytest

from backend.core.realtime import poller as poller_mod
from backend.utils.http import make_client


def test_make_client_default_timeout_matches_httpx_default():
    with make_client() as c:
        assert c.timeout == httpx.Timeout(5.0)


def test_make_client_uses_injected_transport_and_timeout():
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json={"ok": True}))
    with make_client(httpx.Timeout(2.0), transport=transport) as c:
        assert c.timeout == httpx.Timeout(2.0)
        assert c.get("https://example.invalid/x").json() == {"ok": True}


@pytest.fixture
def rt_creds(monkeypatch):
    from backend import config as config_mod

    monkeypatch.setattr(config_mod, "get_fastly_api_key", lambda sid: "tok-test")
    monkeypatch.setattr(config_mod, "get_fastly_service_id", lambda sid: "FSID")
    monkeypatch.setattr(poller_mod, "_retry_sleep", lambda s: None)


def test_poller_fetch_retries_503_then_succeeds_and_sends_key(rt_creds):
    seen: list[tuple[str, str]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append((str(req.url), req.headers["Fastly-Key"]))
        if len(seen) == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"Timestamp": 5, "Data": []})

    with make_client(transport=httpx.MockTransport(handler)) as c:
        out = poller_mod.RealtimePoller()._fetch_realtime(c, "svc", 0)

    assert out == {"Timestamp": 5, "Data": []}
    assert len(seen) == 2
    assert seen[0][0] == "https://rt.fastly.com/v1/channel/FSID/ts/h?limit=120"
    assert all(k == "tok-test" for _, k in seen)


def test_poller_fetch_does_not_retry_403(rt_creds):
    calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(403)

    with make_client(transport=httpx.MockTransport(handler)) as c:
        with pytest.raises(httpx.HTTPStatusError):
            poller_mod.RealtimePoller()._fetch_realtime(c, "svc", 0)
    assert calls == 1


def test_poller_longpoll_read_timeout_raises_gap_signal(rt_creds):
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=req)

    with make_client(transport=httpx.MockTransport(handler)) as c:
        with pytest.raises(poller_mod._LongPollTimeout):
            poller_mod.RealtimePoller()._fetch_realtime(c, "svc", 123)
