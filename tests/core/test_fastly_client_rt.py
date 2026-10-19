import io
import json
from unittest.mock import patch

import pytest

from backend.core.fastly import client


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_fastly_rt_targets_rt_host_with_token(monkeypatch):
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    seen = {}

    def _urlopen(req, timeout):
        seen["url"] = req.full_url
        seen["key"] = req.get_header("Fastly-key")
        seen["timeout"] = timeout
        return _Resp(json.dumps({"Data": []}).encode())

    with patch("urllib.request.urlopen", side_effect=_urlopen):
        out = client.fastly_rt("/v1/channel/abc/ts/h?limit=120", token="tok")

    assert out == {"Data": []}
    assert seen == {"url": "https://rt.fastly.com/v1/channel/abc/ts/h?limit=120", "key": "tok", "timeout": 10}


def test_fastly_rt_raises_runtime_error_on_http_error(monkeypatch):
    import urllib.error

    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    err = urllib.error.HTTPError("https://rt.fastly.com/x", 403, "no", {}, io.BytesIO(b"denied"))
    with patch("urllib.request.urlopen", side_effect=err), pytest.raises(RuntimeError, match="HTTP 403 GET /x"):
        client.fastly_rt("/x", token="tok")


def test_fastly_rt_mock_mode_skips_network(monkeypatch):
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    with patch("urllib.request.urlopen", side_effect=AssertionError("network")):
        assert client.fastly_rt("/v1/channel/abc/ts/h", token="tok") == {"ok": True}
