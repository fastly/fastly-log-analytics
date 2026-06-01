"""Tests for ``backend.core.fastly.client.fastly`` — the Fastly API HTTP shim.

The shim is everywhere: every provisioning step, every config refresh,
every alert evaluation that needs Fastly metadata flows through it.
A regression in its retry/error logic surfaces as either silent data
loss (false-positive empty responses) or noisy failures the wizard
can't recover from.

The interesting branches:
  - HTTP 5xx / 429 → exponential backoff + retry (caps at max_retries)
  - non-retryable HTTP error → RuntimeError with status + body
  - network errors (URLError, ConnectionError, TimeoutError) → retry
  - ``expect_empty=True`` → return {} without JSON-parsing
  - empty response body → return {} regardless of expect_empty
  - body is JSON-serialised + Content-Type header added
  - missing telemetry module → skip the context manager
"""

from __future__ import annotations

import json
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from backend.core.fastly import client as fastly_client


def _fake_resp(body: bytes = b'{"ok": true}') -> MagicMock:
    """Build a urlopen() mock that returns ``body`` on read()."""
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# ── Happy path ─────────────────────────────────────────────────────────────


def test_fastly_returns_parsed_json_on_success():
    with patch("urllib.request.urlopen", return_value=_fake_resp(b'{"data": [1, 2]}')):
        out = fastly_client.fastly("GET", "/services", token="tkn")

    assert out == {"data": [1, 2]}


def test_fastly_includes_fastly_key_and_accept_headers():
    """The ``Fastly-Key`` header is what authenticates the request —
    losing it would 401 every call. Pinned alongside Accept so a
    refactor that changes header construction is forced through tests."""
    with patch("urllib.request.urlopen", return_value=_fake_resp()) as mock_open:
        fastly_client.fastly("GET", "/service/foo", token="my-token")

    req = mock_open.call_args[0][0]
    assert req.get_header("Fastly-key") == "my-token"
    assert req.get_header("Accept") == "application/json"


def test_fastly_serialises_body_to_json_and_adds_content_type():
    """When a body is provided, it's JSON-encoded and a
    Content-Type header is added. Pinned because Fastly's API
    rejects POST/PATCH without ``application/json``."""
    captured: dict = {}

    def _capture(req, timeout):
        captured["data"] = req.data
        captured["content_type"] = req.get_header("Content-type")
        return _fake_resp()

    with patch("urllib.request.urlopen", side_effect=_capture):
        fastly_client.fastly("POST", "/services", body={"name": "new-svc"}, token="t")

    assert captured["data"] == json.dumps({"name": "new-svc"}).encode()
    assert captured["content_type"] == "application/json"


def test_fastly_omits_content_type_when_no_body():
    """GET/DELETE shouldn't add a body OR a Content-Type — pinned
    because adding Content-Type to a body-less request causes
    Fastly's API gateway to log a warning."""
    captured: dict = {}

    def _capture(req, timeout):
        captured["data"] = req.data
        captured["content_type"] = req.get_header("Content-type")
        return _fake_resp()

    with patch("urllib.request.urlopen", side_effect=_capture):
        fastly_client.fastly("GET", "/service/foo", token="t")

    assert captured["data"] is None
    assert captured["content_type"] is None


# ── expect_empty / empty body ──────────────────────────────────────────────


def test_fastly_returns_empty_dict_when_expect_empty_is_true():
    """``expect_empty=True`` skips JSON parsing — used for DELETE
    endpoints that return 204 with an empty body. Pinned because
    json.loads('') raises and would crash every DELETE call."""
    with patch("urllib.request.urlopen", return_value=_fake_resp(b'{"trailing": "ignored"}')):
        out = fastly_client.fastly("DELETE", "/x", token="t", expect_empty=True)
    assert out == {}


def test_fastly_returns_empty_dict_when_response_body_is_blank():
    """Whitespace-only response → {}, not a JSON parse error.
    Pinned because some Fastly endpoints return ``\\n`` on success."""
    with patch("urllib.request.urlopen", return_value=_fake_resp(b"  \n  ")):
        out = fastly_client.fastly("PATCH", "/x", token="t")
    assert out == {}


# ── HTTP error mapping ────────────────────────────────────────────────────


def test_fastly_raises_runtime_error_on_4xx_with_body_message():
    """Non-retryable HTTP error (400, 401, 403, 404) → RuntimeError
    with status code and response body. Pinned because the provision
    wizard renders this exact string in its error pane."""
    error = urllib.error.HTTPError(
        url="https://api.fastly.com/x",
        code=403,
        msg="Forbidden",
        hdrs=None,  # type: ignore[arg-type]
        fp=None,
    )
    error.read = lambda: b'{"msg": "bad token"}'

    with patch("urllib.request.urlopen", side_effect=error):
        with pytest.raises(RuntimeError, match="HTTP 403"):
            fastly_client.fastly("GET", "/x", token="t")


def test_fastly_includes_path_and_method_in_error_message():
    """The error mentions both ``method`` and ``path`` so the operator
    can correlate it with a specific call site. Pinned because losing
    either of these would make wizard error messages much harder to
    debug."""
    error = urllib.error.HTTPError(
        url="https://api.fastly.com/services/abc",
        code=404,
        msg="Not Found",
        hdrs=None,  # type: ignore[arg-type]
        fp=None,
    )
    error.read = lambda: b"service not found"

    with patch("urllib.request.urlopen", side_effect=error):
        with pytest.raises(RuntimeError) as exc:
            fastly_client.fastly("DELETE", "/services/abc", token="t")

    assert "DELETE" in str(exc.value)
    assert "/services/abc" in str(exc.value)


# ── Retry: 5xx + 429 ────────────────────────────────────────────────────────


@pytest.mark.parametrize("retry_code", [429, 500, 502, 503, 504])
def test_fastly_retries_on_retryable_status_codes(retry_code):
    """Each of {429, 500, 502, 503, 504} triggers a retry. Pinned per
    code so a refactor that flattens the list (e.g. drops 502) is
    caught — losing 502 retries would manifest as wizard failures
    during Fastly's edge maintenance windows."""

    def _flaky():
        error = urllib.error.HTTPError(
            url="x",
            code=retry_code,
            msg="x",
            hdrs=None,
            fp=None,  # type: ignore[arg-type]
        )
        error.read = lambda: b""
        return error

    attempts = [_flaky(), _flaky(), _fake_resp(b'{"ok": true}')]

    def _side_effect(req, timeout):
        result = attempts.pop(0)
        if isinstance(result, urllib.error.HTTPError):
            raise result
        return result

    with patch("urllib.request.urlopen", side_effect=_side_effect), patch("time.sleep"):
        out = fastly_client.fastly("GET", "/x", token="t")

    assert out == {"ok": True}
    assert attempts == []  # all three attempts consumed


def test_fastly_does_not_retry_on_non_retryable_status():
    """A 400 (validation error) shouldn't retry — pinned because
    retrying validation errors wastes time and could rate-limit the
    legitimate request that follows."""
    error = urllib.error.HTTPError(url="x", code=400, msg="Bad Request", hdrs=None, fp=None)  # type: ignore[arg-type]
    error.read = lambda: b"validation failed"

    with patch("urllib.request.urlopen", side_effect=error) as mock_open, patch("time.sleep"):
        with pytest.raises(RuntimeError):
            fastly_client.fastly("POST", "/x", token="t")

    assert mock_open.call_count == 1  # no retries


def test_fastly_gives_up_after_max_retries_on_5xx():
    """After ``max_retries+1`` attempts, a persistent 5xx raises
    RuntimeError with the last response's body. Pinned because
    infinite retries would hang the wizard."""

    def _always_500():
        e = urllib.error.HTTPError(url="x", code=503, msg="Service Unavailable", hdrs=None, fp=None)  # type: ignore[arg-type]
        e.read = lambda: b"still down"
        return e

    with (
        patch("urllib.request.urlopen", side_effect=lambda req, timeout: (_ for _ in ()).throw(_always_500())),
        patch("time.sleep"),
    ):
        with pytest.raises(RuntimeError, match="HTTP 503"):
            fastly_client.fastly("GET", "/x", token="t", max_retries=2)


def test_fastly_uses_exponential_backoff_between_retries():
    """Retry delays are ``2**attempt`` — 1s, 2s, 4s, ... — to avoid
    hammering Fastly while it recovers. Pinned because a flat retry
    interval would worsen the load during an outage."""
    sleeps: list[float] = []

    def _flaky(req, timeout):
        if len(sleeps) < 2:
            e = urllib.error.HTTPError(url="x", code=500, msg="x", hdrs=None, fp=None)  # type: ignore[arg-type]
            e.read = lambda: b""
            raise e
        return _fake_resp()

    with (
        patch("urllib.request.urlopen", side_effect=_flaky),
        patch("time.sleep", side_effect=sleeps.append),
    ):
        fastly_client.fastly("GET", "/x", token="t")

    # First retry: 2^0=1, second retry: 2^1=2
    assert sleeps == [1, 2]


# ── Retry: network errors ─────────────────────────────────────────────────


def test_fastly_retries_on_url_error():
    """``URLError`` (DNS failures, connection refused) → retry.
    Pinned because DNS flaps and TCP RST are common during cluster
    maintenance and the wizard should tolerate them."""
    err = urllib.error.URLError("connection refused")
    attempts = [err, _fake_resp(b'{"ok": true}')]

    def _side_effect(req, timeout):
        x = attempts.pop(0)
        if isinstance(x, Exception):
            raise x
        return x

    with patch("urllib.request.urlopen", side_effect=_side_effect), patch("time.sleep"):
        out = fastly_client.fastly("GET", "/x", token="t")

    assert out == {"ok": True}


def test_fastly_retries_on_timeout_error():
    err = TimeoutError("read timed out")
    attempts = [err, _fake_resp()]

    def _side_effect(req, timeout):
        x = attempts.pop(0)
        if isinstance(x, Exception):
            raise x
        return x

    with patch("urllib.request.urlopen", side_effect=_side_effect), patch("time.sleep"):
        fastly_client.fastly("GET", "/x", token="t")  # must not raise


def test_fastly_retries_on_connection_error():
    err = ConnectionError("reset by peer")
    attempts = [err, _fake_resp()]

    def _side_effect(req, timeout):
        x = attempts.pop(0)
        if isinstance(x, Exception):
            raise x
        return x

    with patch("urllib.request.urlopen", side_effect=_side_effect), patch("time.sleep"):
        fastly_client.fastly("GET", "/x", token="t")


def test_fastly_raises_runtime_error_after_max_network_retries():
    """Persistent network failure → RuntimeError with the underlying
    cause. Pinned because losing the wrapping would leave the
    wizard to handle URLError directly (which it doesn't catch)."""

    def _always_fail(req, timeout):
        raise urllib.error.URLError("network unreachable")

    with patch("urllib.request.urlopen", side_effect=_always_fail), patch("time.sleep"):
        with pytest.raises(RuntimeError, match="Network error"):
            fastly_client.fastly("GET", "/x", token="t", max_retries=1)


# ── Telemetry context manager ──────────────────────────────────────────────


def test_fastly_wraps_call_in_telemetry_context_when_available():
    """When ``backend.utils.telemetry.tracked_call`` is importable,
    the call runs inside the context manager so the request gets
    counted against the per-request telemetry budget."""
    fake_ctx = MagicMock()
    fake_ctx.__enter__ = MagicMock(return_value=None)
    fake_ctx.__exit__ = MagicMock(return_value=False)

    with (
        patch("backend.utils.telemetry.tracked_call", return_value=fake_ctx) as mock_tracker,
        patch("urllib.request.urlopen", return_value=_fake_resp()),
    ):
        fastly_client.fastly("GET", "/services/abc", token="t")

    mock_tracker.assert_called_once()
    args, kwargs = mock_tracker.call_args
    assert args[0] == "GET"
    assert args[1] == "/services/abc"
    assert kwargs["service"] == "Fastly API"
    # The context manager was entered + exited
    fake_ctx.__enter__.assert_called_once()
    fake_ctx.__exit__.assert_called_once()
