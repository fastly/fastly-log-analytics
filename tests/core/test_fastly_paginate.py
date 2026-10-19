"""``fastly_paginate`` and ``Retry-After`` handling in the central Fastly client."""

from __future__ import annotations

import email.message
import io
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from backend.core.fastly import client as fastly_client


def _fake_resp(body: bytes = b'{"ok": true}') -> MagicMock:
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def _http_error(code: int, retry_after: str | None = None) -> urllib.error.HTTPError:
    hdrs = email.message.Message()
    if retry_after is not None:
        hdrs["Retry-After"] = retry_after
    return urllib.error.HTTPError("https://api.fastly.com/x", code, "err", hdrs, io.BytesIO(b"{}"))


# ── Retry-After ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("5", 5.0),
        ("0", 0.0),
        ("2.5", 2.5),
        ("999", 30.0),
        ("-3", None),
        ("nan", None),
        ("soon", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_retry_after(raw, expected):
    assert fastly_client.parse_retry_after(raw) == expected


def test_get_429_sleeps_for_retry_after_seconds():
    attempts = [_http_error(429, "7"), _fake_resp()]

    def _side_effect(req, timeout):
        x = attempts.pop(0)
        if isinstance(x, Exception):
            raise x
        return x

    with patch("urllib.request.urlopen", side_effect=_side_effect), patch("time.sleep") as sleep:
        assert fastly_client.fastly("GET", "/x", token="t") == {"ok": True}

    sleep.assert_called_once_with(7.0)


def test_get_429_retry_after_is_capped():
    attempts = [_http_error(429, "3600"), _fake_resp()]

    def _side_effect(req, timeout):
        x = attempts.pop(0)
        if isinstance(x, Exception):
            raise x
        return x

    with patch("urllib.request.urlopen", side_effect=_side_effect), patch("time.sleep") as sleep:
        fastly_client.fastly("GET", "/x", token="t")

    sleep.assert_called_once_with(fastly_client.RETRY_AFTER_CAP_S)


def test_get_429_without_header_uses_default_backoff():
    attempts = [_http_error(429), _fake_resp()]

    def _side_effect(req, timeout):
        x = attempts.pop(0)
        if isinstance(x, Exception):
            raise x
        return x

    with patch("urllib.request.urlopen", side_effect=_side_effect), patch("time.sleep") as sleep:
        fastly_client.fastly("GET", "/x", token="t")

    (waited,) = sleep.call_args[0]
    assert 1.0 <= waited <= 3.0  # exponential floor 1s + up to 2s jitter


def test_mutation_429_ignores_retry_after():
    attempts = [_http_error(429, "25"), _fake_resp()]

    def _side_effect(req, timeout):
        x = attempts.pop(0)
        if isinstance(x, Exception):
            raise x
        return x

    with patch("urllib.request.urlopen", side_effect=_side_effect), patch("time.sleep") as sleep:
        fastly_client.fastly("POST", "/x", {"a": 1}, token="t")

    (waited,) = sleep.call_args[0]
    assert 1.0 <= waited <= 3.0


# ── fastly_paginate ───────────────────────────────────────────────────────


def _pages(*bodies):
    it = iter(bodies)
    return MagicMock(side_effect=lambda method, path, **kw: next(it))


def test_cursor_three_pages_sends_cursor_and_stops_on_empty():
    f = _pages(
        {"data": [1], "meta": {"next_cursor": "c1"}},
        {"data": [2], "meta": {"next_cursor": "c2"}},
        {"data": [3], "meta": {"next_cursor": ""}},
    )
    with patch.object(fastly_client, "fastly", f):
        pages = list(fastly_client.fastly_paginate("/ngwaf/v1/w/requests", token="t", params={"q": "a b", "limit": 2}))

    assert [p["data"] for p in pages] == [[1], [2], [3]]
    paths = [c.args[1] for c in f.call_args_list]
    assert paths == [
        "/ngwaf/v1/w/requests?q=a%20b&limit=2",
        "/ngwaf/v1/w/requests?q=a%20b&limit=2&cursor=c1",
        "/ngwaf/v1/w/requests?q=a%20b&limit=2&cursor=c2",
    ]
    assert all(c.kwargs["token"] == "t" and c.args[0] == "GET" for c in f.call_args_list)


def test_cursor_repeat_is_cut_off():
    f = _pages(*[{"data": [i], "meta": {"next_cursor": "same"}} for i in range(10)])
    with patch.object(fastly_client, "fastly", f):
        pages = list(fastly_client.fastly_paginate("/x", token="t"))
    assert len(pages) == 2


def test_max_pages_bounds_iteration():
    f = _pages(*[{"data": [i], "meta": {"next_cursor": f"c{i}"}} for i in range(10)])
    with patch.object(fastly_client, "fastly", f):
        pages = list(fastly_client.fastly_paginate("/x", token="t", max_pages=3))
    assert len(pages) == 3


def test_page_style_stops_on_short_page():
    f = _pages({"data": [1, 2]}, {"data": [3, 4]}, {"data": [5]})
    with patch.object(fastly_client, "fastly", f):
        pages = list(fastly_client.fastly_paginate("/x?a=1", token="t", style="page", per_page=2))
    assert len(pages) == 3
    assert [c.args[1] for c in f.call_args_list] == [
        "/x?a=1&page=1&per_page=2",
        "/x?a=1&page=2&per_page=2",
        "/x?a=1&page=3&per_page=2",
    ]


def test_link_style_follows_relative_and_api_host_links():
    f = _pages(
        {"data": [1], "links": {"next": "/x?page=2"}},
        {"data": [2], "links": {"next": "https://api.fastly.com/x?page=3"}},
        {"data": [3], "links": {}},
    )
    with patch.object(fastly_client, "fastly", f):
        pages = list(fastly_client.fastly_paginate("/x", token="t", style="link"))
    assert len(pages) == 3
    assert [c.args[1] for c in f.call_args_list] == ["/x", "/x?page=2", "/x?page=3"]


def test_link_style_refuses_off_host_link():
    f = _pages({"data": [1], "links": {"next": "https://evil.example/x?page=2"}}, {"data": [2]})
    with patch.object(fastly_client, "fastly", f):
        pages = list(fastly_client.fastly_paginate("/x", token="t", style="link"))
    assert len(pages) == 1
    assert f.call_count == 1


def test_mock_mode_makes_no_network_calls(monkeypatch):
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    with patch("urllib.request.urlopen") as urlopen:
        pages = list(fastly_client.fastly_paginate("/services", token="t", max_pages=5))
    urlopen.assert_not_called()
    assert 1 <= len(pages) <= 5


def test_ngwaf_verified_bots_paged_uses_cursor_pagination():
    from backend.utils.ngwaf import fetch_verified_bots_paged

    bot = {"id": "r1", "timestamp": "2026-10-01T00:00:00Z", "signals": [{"id": "VERIFIED-BOT", "value": "Googlebot"}]}
    f = _pages(
        {"data": [bot, {"id": "r2", "signals": []}], "meta": {"next_cursor": "c1"}},
        {"data": [], "meta": {"next_cursor": ""}},
    )
    with patch.object(fastly_client, "fastly", f), patch("time.sleep"):
        out = list(fetch_verified_bots_paged("key", "ws1", "-1d"))

    assert [(len(recs), ts, raw) for recs, ts, raw in out] == [(1, "2026-10-01T00:00:00Z", 2), (0, None, 0)]
    assert out[0][0][0]["bot_name"] == "Googlebot"
    paths = [c.args[1] for c in f.call_args_list]
    assert paths[0].startswith("/ngwaf/v1/workspaces/ws1/requests?q=tag%3AVERIFIED-BOT%20from%3A")
    assert paths[1].endswith("&cursor=c1")
