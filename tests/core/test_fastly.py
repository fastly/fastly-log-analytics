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

import email.message
import io
import json
import os
import urllib.error
from unittest.mock import MagicMock, patch

import pytest
import vcr

from backend.core.fastly import client as fastly_client
from backend.core.fastly import service

# ── Merged from tests/core/test_fastly_client.py ──
CASSETTE_DIR = os.path.join(os.path.dirname(__file__), "..", "cassettes")

_my_vcr = vcr.VCR(
    cassette_library_dir=CASSETTE_DIR,
    record_mode="none",
    filter_headers=[("Fastly-Key", "REDACTED"), ("Authorization", "REDACTED")],
    match_on=["method", "scheme", "host", "port", "path", "query"],
)


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _fake_resp(body: bytes = b'{"ok": true}') -> MagicMock:
    """Build a urlopen() mock that returns ``body`` on read()."""
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


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
    """Retry delays follow ``2**attempt`` (1s, 2s, 4s, …) PLUS a
    [0, 2)s jitter component so two concurrent admin retries don't
    collide in lock-step against Fastly's per-token+minute rate-limit
    window. Pinned because a flat retry interval would worsen load
    during an outage AND because un-jittered retries thrash the
    rate-limit ceiling."""
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

    # First retry: ~1s + [0,2) jitter, second: ~2s + [0,2) jitter.
    # Cap is exp_max=8 + jitter_max=2 = 10s.
    assert len(sleeps) == 2
    assert 1.0 <= sleeps[0] < 3.0, f"first sleep {sleeps[0]} out of [1, 3)"
    assert 2.0 <= sleeps[1] < 4.0, f"second sleep {sleeps[1]} out of [2, 4)"


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


@pytest.mark.parametrize("exc", [TimeoutError("read timed out"), ConnectionError("reset by peer")])
def test_fastly_retries_on_timeout_and_connection_errors(exc):
    attempts = [exc, _fake_resp()]

    def _side_effect(req, timeout):
        x = attempts.pop(0)
        if isinstance(x, Exception):
            raise x
        return x

    with (
        patch("urllib.request.urlopen", side_effect=_side_effect) as mock_urlopen,
        patch("time.sleep") as mock_sleep,
    ):
        fastly_client.fastly("GET", "/x", token="t")  # must not raise

    assert mock_urlopen.call_count == 2
    mock_sleep.assert_called_once()


def test_fastly_raises_runtime_error_after_max_network_retries():
    """Persistent network failure → RuntimeError with the underlying
    cause. Pinned because losing the wrapping would leave the
    wizard to handle URLError directly (which it doesn't catch)."""

    def _always_fail(req, timeout):
        raise urllib.error.URLError("network unreachable")

    with patch("urllib.request.urlopen", side_effect=_always_fail), patch("time.sleep"):
        with pytest.raises(RuntimeError, match="Network error"):
            fastly_client.fastly("GET", "/x", token="t", max_retries=1)


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


def test_fastly_rt_targets_rt_host_with_token(monkeypatch):
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    seen = {}

    def _urlopen(req, timeout):
        seen["url"] = req.full_url
        seen["key"] = req.get_header("Fastly-key")
        seen["timeout"] = timeout
        return _Resp(json.dumps({"Data": []}).encode())

    with patch("urllib.request.urlopen", side_effect=_urlopen):
        out = fastly_client.fastly_rt("/v1/channel/abc/ts/h?limit=120", token="tok")

    assert out == {"Data": []}
    assert seen == {"url": "https://rt.fastly.com/v1/channel/abc/ts/h?limit=120", "key": "tok", "timeout": 10}


def test_fastly_rt_raises_runtime_error_on_http_error(monkeypatch):
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    err = urllib.error.HTTPError("https://rt.fastly.com/x", 403, "no", {}, io.BytesIO(b"denied"))
    with patch("urllib.request.urlopen", side_effect=err), pytest.raises(RuntimeError, match="HTTP 403 GET /x"):
        fastly_client.fastly_rt("/x", token="tok")


def test_fastly_rt_mock_mode_skips_network(monkeypatch):
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    with patch("urllib.request.urlopen", side_effect=AssertionError("network")):
        assert fastly_client.fastly_rt("/v1/channel/abc/ts/h", token="tok") == {"ok": True}


def test_get_service_returns_parsed_json():
    """Happy path: 200 OK with a JSON body. The client must return the
    parsed dict, not the raw bytes."""
    with _my_vcr.use_cassette("fastly_get_service_success.yaml"):
        result = fastly_client.fastly("GET", "/service/SU3xxxxxxxxxxxxxx0000", token="test-token-not-real")

    assert result["id"] == "SU3xxxxxxxxxxxxxx0000"
    assert result["name"] == "Test CDN"
    assert result["active_version"]["number"] == 42


def test_503_is_retried_then_succeeds():
    """The client's retry policy: 503 triggers an exponential-backoff
    retry. The cassette has 503 then 200; the call should succeed and
    surface the 200 body."""
    with patch("tenacity.nap.time.sleep"):
        with _my_vcr.use_cassette("fastly_503_then_success.yaml"):
            result = fastly_client.fastly("GET", "/account", token="test-token-not-real")

    assert result == {"id": "acct-1", "name": "Test Account"}


def test_429_is_retried_then_succeeds():
    """429 (rate limit) is treated identically to 5xx by
    ``_is_retryable_fastly_error``. The cassette serves 429 then 200; the
    client must retry and surface the 200 body."""
    with patch("tenacity.nap.time.sleep"):
        with _my_vcr.use_cassette("fastly_429_then_success.yaml"):
            result = fastly_client.fastly("GET", "/service/SU3xxxxxxxxxxxxxx0000", token="test-token-not-real")

    assert result["id"] == "SU3xxxxxxxxxxxxxx0000"
    assert result["name"] == "Test CDN"


# ── Merged from tests/core/test_fastly_edge_writes_backfill.py ──
@pytest.fixture
def seeded_metadata_db():
    """Pre-populate the per-service SQLite ``ingested_files`` table.

    Returns the service_id (which keys the per-service metadata file).
    """
    from backend.core import metadata as metadata_db

    service_id = "svc1"
    metadata_db.insert_ingested_files(
        service_id,
        [
            ("s3://bkt/raw/2026-05-13/21/2026-05-13T21:46:00.000-aaa.log.gz", 100, 1234),
            ("s3://bkt/raw/2026-05-13/21/2026-05-13T21:47:00.000-bbb.log.gz", 200, 2345),
            ("s3://bkt/raw/2026-05-13/22/2026-05-13T22:00:00.000-ccc.log.gz", 300, 3456),
            ("__seeding_attempted__", 0, 0),
        ],
    )
    return service_id


@patch("backend.config.is_usage_logging_enabled", return_value=True)
@patch("backend.core.metadata.log_synthetic_usage")
def test_backfill_calls_metadata_db(_log_synth, _enabled, seeded_metadata_db):
    """backfill should hand a list of synthesised PUT_OBJECT calls to metadata_db.log_synthetic_usage."""
    from backend.core import duckdb as _db

    _log_synth.return_value = 3
    inserted = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})

    assert inserted == 3
    assert _log_synth.called
    calls = _log_synth.call_args[0][1]
    # 3 real files; the __seeding_attempted__ sentinel must be excluded.
    assert len(calls) == 3
    paths = sorted(c["path"] for c in calls)
    assert paths[0].endswith("aaa.log.gz")
    assert "__seeding_attempted__" not in paths

    by_path = {c["path"]: c for c in calls}
    assert by_path[paths[0]]["method"] == "PUT_OBJECT"
    assert by_path[paths[0]]["bytes"] == 1234
    assert by_path[paths[0]]["_timestamp_override"] == "2026-05-13T21:46:00Z"


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_backfill_is_idempotent_end_to_end(_enabled, seeded_metadata_db):
    """Running backfill multiple times should not insert duplicate rows."""
    from backend.core import duckdb as _db

    first = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
    second = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
    third = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})

    assert first == 3
    assert second == 0
    assert third == 0

    # usage_log lives in its own SQLite file post-2026-06-12.
    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con(seeded_metadata_db)
    total = con.execute("SELECT count(*) FROM usage_log WHERE function_name = 'fastly.edge'").fetchone()[0]
    assert total == 3


@patch("backend.config.is_usage_logging_enabled", return_value=False)
@patch("backend.core.metadata.log_synthetic_usage")
def test_backfill_skips_when_usage_logging_disabled(_log_synth, _enabled, seeded_metadata_db):
    from backend.core import duckdb as _db

    inserted = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})

    assert inserted == 0
    assert not _log_synth.called


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_backfill_incremental_skips_already_backfilled_files(_enabled, seeded_metadata_db):
    """After the first backfill, subsequent calls should hand log_synthetic_usage
    an empty list — the NOT EXISTS filter prunes already-backfilled files at the
    SQL level, so we don't even pay the dedup IN-clause cost.
    """
    from backend.core import duckdb as _db
    from backend.core import metadata as metadata_db

    first = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
    assert first == 3

    # Add one more ingested file — only THAT file should be handed to log_synthetic_usage.
    metadata_db.insert_ingested_files(
        seeded_metadata_db,
        [("s3://bkt/raw/2026-05-13/22/2026-05-13T22:05:00.000-ddd.log.gz", 400, 4567)],
    )

    with patch("backend.core.metadata.log_synthetic_usage") as log_synth:
        log_synth.return_value = 1
        _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
        calls = log_synth.call_args[0][1]
        assert len(calls) == 1
        assert calls[0]["path"].endswith("ddd.log.gz")

    # And after a real second backfill, a third pass sees nothing new.
    _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
    with patch("backend.core.metadata.log_synthetic_usage") as log_synth:
        _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
        # log_synthetic_usage should NOT be called when the unbackfilled list is empty
        assert not log_synth.called


def test_unbackfilled_lookup_scopes_usage_log_to_candidate_files(seeded_metadata_db):
    """The anti-join must not load every historical fastly.edge URL each tick.

    On a long-lived service usage_log holds ~450k fastly.edge rows while the
    1h ``since`` window holds a few hundred files; fetching the whole URL set
    cost ~5 s per log_discovery tick and delayed the next LIST.
    """
    from backend.core import metadata as metadata_db
    from backend.core.metadata import usage_log_db

    real_open = usage_log_db.open_readonly
    fetched: list[int] = []

    def _spy_open(service_id):
        con = real_open(service_id)
        orig_execute = con.execute

        def _execute(sql, params=()):
            cur = orig_execute(sql, params)
            if "fastly.edge" in sql:
                rows = cur.fetchall()
                fetched.append(len(rows))

                class _Rows:
                    def fetchall(self_inner):
                        return rows

                return _Rows()
            return cur

        con.execute = _execute
        return con

    metadata_db.log_synthetic_usage(
        seeded_metadata_db,
        [{"method": "PUT_OBJECT", "path": f"s3://bkt/raw/old/{i}.log.gz", "bytes": 1} for i in range(50)],
    )

    with patch.object(usage_log_db, "open_readonly", _spy_open):
        files = metadata_db.list_unbackfilled_fastly_edge_files(seeded_metadata_db, since="2000-01-01 00:00:00")

    assert len(files) == 3
    assert fetched and sum(fetched) == 0


# ── Merged from tests/core/test_fastly_paginate.py ──
def _http_error(code: int, retry_after: str | None = None) -> urllib.error.HTTPError:
    hdrs = email.message.Message()
    if retry_after is not None:
        hdrs["Retry-After"] = retry_after
    return urllib.error.HTTPError("https://api.fastly.com/x", code, "err", hdrs, io.BytesIO(b"{}"))


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


# ── Merged from tests/core/test_fastly_service.py ──
def test_get_active_version_returns_active_number():
    fake_versions = [
        {"number": 1, "active": False},
        {"number": 7, "active": True},
        {"number": 3, "active": False},
    ]
    with patch("backend.core.fastly.service.fastly", return_value=fake_versions):
        assert service.get_active_version("svc-1", "tkn") == 7


def test_get_active_version_returns_none_when_no_active():
    """Service exists but no version is active (between deploys) →
    None. Pinned because raising would block re-provisioning and the
    wizard relies on this distinction to render a "configure first"
    state."""
    with patch("backend.core.fastly.service.fastly", return_value=[{"number": 1, "active": False}]):
        assert service.get_active_version("svc-1", "tkn") is None


def test_get_active_version_returns_none_on_api_error():
    """RuntimeError (401, 404, network) → None. Pinned because the
    teardown path calls this on best-effort basis; raising would
    leave half-provisioned services dangling."""
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("401")):
        assert service.get_active_version("svc-1", "tkn") is None


def test_get_active_version_returns_first_active_when_multiple():
    """If two versions are flagged active (data corruption), the
    first wins. Pinned to lock the precedence — flipping it would
    cause the wizard to read from the wrong version."""
    fake = [{"number": 5, "active": True}, {"number": 9, "active": True}]
    with patch("backend.core.fastly.service.fastly", return_value=fake):
        assert service.get_active_version("svc-1", "tkn") == 5


def test_get_generated_vcl_returns_content():
    """generated_vcl returns JSON {"content": "<vcl>"} — the wrapper unwraps
    the content string. Pinned because the proactive rate-limit detector reads
    the account-level ``ratelimit_opt_in`` pragma out of this body."""
    vcl = "sub vcl_recv {\n  pragma optional_param ratelimit_opt_in true;\n}\n"
    with patch("backend.core.fastly.service.fastly", return_value={"content": vcl, "version": 5}):
        assert service.get_generated_vcl("svc-1", 5, "tkn") == vcl


def test_get_generated_vcl_pins_endpoint_path():
    """The GET path must be exactly /service/{id}/version/{ver}/generated_vcl —
    a typo (generated-vcl) would silently return None forever."""
    calls = []

    def fake(method, path, **kwargs):
        calls.append((method, path))
        return {"content": "x"}

    with patch("backend.core.fastly.service.fastly", side_effect=fake):
        service.get_generated_vcl("svc-9", 42, "tkn")
    assert calls == [("GET", "/service/svc-9/version/42/generated_vcl")]


def test_get_generated_vcl_returns_none_on_api_error():
    """RuntimeError (Compute/wasm services 404 here, plus 401/network) → None.
    The detector treats None as 'undetermined' and falls through."""
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("404")):
        assert service.get_generated_vcl("svc-1", 1, "tkn") is None


def test_get_generated_vcl_returns_none_on_empty_or_nondict():
    """Empty content or a non-dict response → None (no crash)."""
    with patch("backend.core.fastly.service.fastly", return_value={"content": ""}):
        assert service.get_generated_vcl("svc-1", 1, "tkn") is None
    with patch("backend.core.fastly.service.fastly", return_value={}):
        assert service.get_generated_vcl("svc-1", 1, "tkn") is None
    with patch("backend.core.fastly.service.fastly", return_value="not-a-dict"):
        assert service.get_generated_vcl("svc-1", 1, "tkn") is None


def test_find_service_by_name_returns_match():
    services = [
        {"id": "svc-a", "name": "Production"},
        {"id": "svc-b", "name": "Staging"},
    ]
    with patch("backend.core.fastly.service.fastly", return_value=services):
        assert service.find_service_by_name("Staging", "tkn")["id"] == "svc-b"


def test_find_service_by_name_returns_none_for_unknown():
    with patch("backend.core.fastly.service.fastly", return_value=[]):
        assert service.find_service_by_name("ghost", "tkn") is None


def test_find_service_by_name_returns_none_on_error():
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("403")):
        assert service.find_service_by_name("anything", "tkn") is None


def test_find_condition_iterates_and_matches_by_name():
    conditions = [
        {"name": "Cond-A", "statement": 'req.url == "/"'},
        {"name": "Cond-B", "statement": 'req.method == "POST"'},
    ]
    with patch("backend.core.fastly.service.fastly", return_value=conditions):
        out = service.find_condition("Cond-B", "svc-1", 5, "tkn")
    assert out["statement"] == 'req.method == "POST"'


def test_find_condition_returns_none_when_no_match():
    with patch("backend.core.fastly.service.fastly", return_value=[]):
        assert service.find_condition("absent", "svc-1", 5, "tkn") is None


def test_ensure_condition_creates_when_absent():
    """No existing condition → POST. Pinned because a regression that
    sent PUT on a missing condition would 404 instead of creating."""
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path, payload))
        if method == "GET":
            return []  # no existing
        return {"id": "new"}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_condition("My Cond", 'req.url ~ "/"', "REQUEST", "svc-1", 5, "tkn")

    methods = [c[0] for c in calls]
    assert "POST" in methods
    assert "PUT" not in methods


def test_ensure_condition_noops_when_statement_and_type_match():
    """If the existing condition has the same statement AND type, no
    write. Pinned because re-saving an unchanged condition increments
    the Fastly version number for nothing."""
    existing = {"name": "X", "statement": "S", "type": "REQUEST"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append(method)
        return [existing]  # GET returns the existing match

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        out = service.ensure_condition("X", "S", "REQUEST", "svc-1", 5, "tkn")

    assert out == existing
    # Only the GET happened; no POST/PUT
    assert calls.count("POST") == 0
    assert calls.count("PUT") == 0


def test_ensure_condition_updates_when_statement_changes():
    """Existing condition + different statement → PUT to update.
    Pinned because re-creating (POST) instead would fail with a
    name-conflict 400."""
    existing = {"name": "X", "statement": "OLD", "type": "REQUEST"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "GET":
            return [existing]
        return {"id": "updated"}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_condition("X", "NEW", "REQUEST", "svc-1", 5, "tkn")

    put_calls = [c for c in calls if c[0] == "PUT"]
    assert len(put_calls) == 1


def test_ensure_condition_url_encodes_condition_name():
    """Condition names can contain ``/`` and ``%`` — those must be
    URL-encoded in the PUT path or the request routes to the wrong
    resource (or 404s). Pinned to prevent naming-edge regressions."""
    existing = {"name": "Path / With % Spaces", "statement": "S", "type": "REQ"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "GET":
            return [existing]
        return {}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_condition("Path / With % Spaces", "NEW", "REQ", "svc-1", 5, "tkn")

    put_path = next(c[1] for c in calls if c[0] == "PUT")
    # ``/`` → %2F, space → %20, ``%`` → %25
    assert "Path%20%2F%20With%20%25%20Spaces" in put_path
    # Crucially: no literal slash from the name leaks into the path
    # (which would break the route)
    assert put_path.endswith("Path%20%2F%20With%20%25%20Spaces")


def test_list_s3_endpoints_returns_names_list():
    fake = [{"name": "logs-fos"}, {"name": "audit-logs"}]
    with patch("backend.core.fastly.service.fastly", return_value=fake):
        assert service.list_s3_endpoints("svc", 5, "tkn") == ["logs-fos", "audit-logs"]


def test_list_s3_endpoints_returns_empty_on_error():
    """RuntimeError → empty list. Pinned because the cron's
    teardown-detector reads this to know what to remove; a raise
    would abort the whole teardown."""
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("404")):
        assert service.list_s3_endpoints("svc", 5, "tkn") == []


def test_list_vcl_snippets_returns_names_list():
    fake = [{"name": "init"}, {"name": "recv"}]
    with patch("backend.core.fastly.service.fastly", return_value=fake):
        assert service.list_vcl_snippets("svc", 5, "tkn") == ["init", "recv"]


def test_list_vcl_snippets_returns_empty_on_error():
    with patch("backend.core.fastly.service.fastly", side_effect=RuntimeError("403")):
        assert service.list_vcl_snippets("svc", 5, "tkn") == []


def test_ensure_vcl_snippet_creates_when_absent():
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path, payload))
        if method == "GET":
            return []
        return {"id": "snip-new"}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("my-snip", "init", "set req.x = 1;", 100, "svc", 5, "tkn")

    methods = [c[0] for c in calls]
    assert "POST" in methods
    # Payload must include the dynamic=0 flag (Fastly distinguishes static vs dynamic)
    post_payload = next(c[2] for c in calls if c[0] == "POST")
    assert post_payload["dynamic"] == 0


def test_ensure_vcl_snippet_noops_when_all_fields_match():
    """A snippet that already exists with the same type, content,
    AND priority → return existing, no write. Pinned because
    re-saving bumps the Fastly service version, polluting the
    change history with no-op deploys."""
    existing = {"name": "x", "type": "init", "content": "C", "priority": 100, "id": "old"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append(method)
        return [existing]

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        out = service.ensure_vcl_snippet("x", "init", "C", 100, "svc", 5, "tkn")

    assert out == existing
    assert calls.count("POST") == 0
    assert calls.count("PUT") == 0


def test_ensure_vcl_snippet_updates_when_content_changes():
    """Differing content → PUT update. Pinned because the most common
    re-provision case is "VCL changed" and POST instead of PUT would
    fail with a name-conflict 400."""
    existing = {"name": "x", "type": "init", "content": "OLD", "priority": 100, "id": "old"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "GET":
            return [existing]
        return {}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("x", "init", "NEW", 100, "svc", 5, "tkn")

    put_calls = [c for c in calls if c[0] == "PUT"]
    assert len(put_calls) == 1


def test_ensure_vcl_snippet_updates_when_priority_changes():
    existing = {"name": "x", "type": "init", "content": "C", "priority": 100, "id": "old"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append(method)
        if method == "GET":
            return [existing]
        return {}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("x", "init", "C", 200, "svc", 5, "tkn")

    assert "PUT" in calls


def test_ensure_vcl_snippet_url_encodes_snippet_name():
    """Same URL-encoding requirement as ensure_condition."""
    existing = {"name": "edge / case", "type": "init", "content": "OLD", "priority": 100, "id": "x"}
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "GET":
            return [existing]
        return {}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("edge / case", "init", "NEW", 100, "svc", 5, "tkn")

    put_path = next(c[1] for c in calls if c[0] == "PUT")
    assert "edge%20%2F%20case" in put_path


def test_ensure_vcl_snippet_creates_on_get_failure():
    """If the GET fails (404, network), fall through to the POST
    create. Pinned because a transient GET error shouldn't lock us
    out of provisioning."""
    calls: list = []

    def _spy(method, path, payload=None, token=None, **kwargs):
        calls.append(method)
        if method == "GET":
            raise RuntimeError("404")
        return {"id": "new"}

    with patch("backend.core.fastly.service.fastly", side_effect=_spy):
        service.ensure_vcl_snippet("x", "init", "C", 100, "svc", 5, "tkn")

    assert "POST" in calls


# ── Merged from tests/core/test_realtime.py ──

"""Realtime poller + transform unit tests.

Covers:
- transform_rt_response aggregation math
- error_tick_payload shape
- mock mode returns valid shape
- poller lifecycle (start / suspend on no subscribers)
- cursor advances between polls
"""


import asyncio

import pytest

from backend.core.realtime.transform import error_tick_payload, gap_tick_payload, transform_rt_response


# ── Merged from tests/core/test_realtime_poller.py ──
class TestTransformRtResponse:
    def test_basic_aggregation(self):
        rt = {
            "Data": [
                {
                    "aggregated": {
                        "requests": 100,
                        "status_2xx": 90,
                        "status_4xx": 7,
                        "status_5xx": 3,
                        "hits": 80,
                        "miss": 20,
                        "pass": 0,
                        "synth": 0,
                        "resp_body_bytes": 1_000_000,
                        "resp_header_bytes": 10_000,
                        "shield": 10,
                        "shield_resp_body_bytes": 200_000,
                        "shield_resp_header_bytes": 2000,
                        "bereq_header_bytes": 5000,
                        "bereq_body_bytes": 0,
                        "waf_blocked": 1,
                        "waf_logged": 2,
                        "waf_passed": 97,
                    }
                }
            ],
            "Timestamp": 1720000000,
            "AggregateDelay": 3,
        }
        tick = transform_rt_response(rt)

        assert tick["event"] == "metrics_tick"
        assert tick["event_schema_version"] == 2
        assert tick["status"] == "ok"
        assert tick["aggregate_delay"] == 3

        data = tick["data"]
        assert data["requests_per_second"] == 100.0
        assert data["error_rate"] == pytest.approx(0.1, abs=0.001)
        assert data["cache_hit_ratio"] == pytest.approx(0.8, abs=0.001)
        assert data["bandwidth_mbps"] > 0
        assert "status_4xx" in data["status_breakdown"]
        assert "status_5xx" in data["status_breakdown"]
        assert data["estimated_cost_usd"] >= 0
        assert data["total_requests"] == 100
        assert data["total_hits"] == 80
        assert data["total_miss"] == 20
        assert data["total_pass"] == 0
        assert data["total_errors"] == 10
        assert data["origin_requests_per_second"] == 20.0
        assert data["shield_hit_ratio"] == pytest.approx(0.1, abs=0.001)
        assert data["waf_blocked"] == 1
        assert data["waf_logged"] == 2
        assert data["pop_count"] == 0

    def test_multiple_data_points(self):
        rt = {
            "Data": [
                {
                    "aggregated": {
                        "requests": 50,
                        "hits": 40,
                        "miss": 10,
                        "resp_body_bytes": 500_000,
                        "resp_header_bytes": 5000,
                    }
                },
                {
                    "aggregated": {
                        "requests": 50,
                        "hits": 40,
                        "miss": 10,
                        "resp_body_bytes": 500_000,
                        "resp_header_bytes": 5000,
                    }
                },
            ],
            "Timestamp": 1720000001,
        }
        tick = transform_rt_response(rt)
        data = tick["data"]
        assert data["requests_per_second"] == 50.0
        assert data["cache_hit_ratio"] == pytest.approx(0.8, abs=0.001)

    def test_empty_data_array(self):
        tick = transform_rt_response({"Data": [], "Timestamp": 0})
        data = tick["data"]
        assert data["requests_per_second"] == 0
        assert data["error_rate"] == 0.0
        assert data["cache_hit_ratio"] == 0.0
        assert data["bandwidth_mbps"] == 0.0

    def test_missing_data_key(self):
        tick = transform_rt_response({"Timestamp": 0})
        assert tick["data"]["requests_per_second"] == 0

    def test_datacenter_format(self):
        rt = {
            "Data": [
                {
                    "datacenter": {
                        "SJC": {
                            "requests": 200,
                            "status_5xx": 0,
                            "hits": 180,
                            "miss": 20,
                            "resp_body_bytes": 2_000_000,
                            "resp_header_bytes": 20_000,
                        },
                        "DCA": {
                            "requests": 100,
                            "status_5xx": 10,
                            "hits": 80,
                            "miss": 20,
                            "resp_body_bytes": 1_000_000,
                            "resp_header_bytes": 10_000,
                        },
                    }
                }
            ],
            "Timestamp": 1720000002,
        }
        tick = transform_rt_response(rt)
        data = tick["data"]
        assert data["pop_count"] == 2
        assert "DCA" in data["degraded_pops"]
        assert "SJC" not in data["degraded_pops"]


class TestErrorTickPayload:
    def test_no_last_good(self):
        tick = error_tick_payload()
        assert tick["status"] == "rt_down"
        assert tick["data"]["requests_per_second"] == 0

    def test_with_last_good(self):
        last_good = {
            "data": {
                "requests_per_second": 42,
                "error_rate": 0.01,
                "cache_hit_ratio": 0.95,
                "bandwidth_mbps": 1.5,
                "status_breakdown": {},
                "estimated_cost_usd": 0.001,
            }
        }
        tick = error_tick_payload(last_good)
        assert tick["status"] == "rt_down"
        assert tick["data"]["requests_per_second"] == 42


def _test_mock_rt_response() -> dict:
    return {
        "Data": [
            {
                "aggregated": {
                    "requests": 150,
                    "status_2xx": 140,
                    "status_3xx": 5,
                    "status_4xx": 3,
                    "status_5xx": 2,
                    "hits": 120,
                    "miss": 30,
                    "pass": 0,
                    "synth": 0,
                    "resp_body_bytes": 5_000_000,
                    "resp_header_bytes": 50_000,
                    "shield": 10,
                    "shield_resp_body_bytes": 2_000_000,
                    "shield_resp_header_bytes": 20_000,
                    "bereq_header_bytes": 10_000,
                    "bereq_body_bytes": 0,
                    "waf_blocked": 1,
                    "waf_logged": 2,
                    "waf_passed": 147,
                    "origin_offload": 0.78,
                    "hit_time": 0.001,
                    "miss_time": 0.045,
                    "pass_time": 0.032,
                    "http2": 95,
                    "http3": 40,
                    "ipv6": 20,
                    "tls_v12": 15,
                    "tls_v13": 120,
                    "status_200": 130,
                    "status_204": 5,
                    "status_301": 3,
                    "status_304": 2,
                    "status_404": 2,
                    "status_503": 1,
                    "object_size_10k": 45,
                    "object_size_100k": 50,
                    "object_size_1m": 25,
                    "origin_fetches": 15,
                    "origin_revalidations": 8,
                    "origin_cache_fetches": 5,
                    "shield_hit_requests": 8,
                    "shield_miss_requests": 2,
                    "shield_revalidations": 3,
                    "shield_fetch_body_bytes": 500_000,
                    "request_collapse_usable_count": 12,
                    "request_collapse_unusable_count": 3,
                    "segblock_origin_fetches": 2,
                    "segblock_shield_fetches": 1,
                    "bot_challenge_starts": 5,
                    "bot_challenges_issued": 4,
                    "bot_challenges_succeeded": 3,
                    "bot_challenges_failed": 1,
                    "bot_edge_requests_detected_count": 8,
                    "bot_edge_requests_verified_count": 2,
                    "bot_edge_requests_ai_crawler_count": 1,
                    "compute_execution_time_ms": 12.5,
                    "compute_request_time_ms": 18.3,
                    "compute_ram_used": 4_200_000,
                    "compute_bereq_errors": 0,
                    "compute_guest_errors": 0,
                    "restarts": 0,
                    "recv_sub_count": 150,
                    "recv_sub_time": 0.002,
                    "fetch_sub_count": 30,
                    "fetch_sub_time": 0.045,
                    "deliver_sub_count": 150,
                    "deliver_sub_time": 0.001,
                    "error_sub_count": 2,
                    "error_sub_time": 0.001,
                    "miss_histogram": {"10": 5, "20": 8, "30": 6, "60": 4, "120": 3, "250": 2},
                },
                "datacenter": {
                    "SJC": {
                        "requests": 80,
                        "status_5xx": 1,
                        "hits": 65,
                        "miss": 15,
                    },
                    "DCA": {
                        "requests": 70,
                        "status_5xx": 1,
                        "hits": 55,
                        "miss": 15,
                    },
                },
            }
        ],
        "Timestamp": 1720000000,
        "AggregateDelay": 3,
    }


def test_mock_rt_response_valid_shape():
    """Sanity-check the shared test fixture used to fake RT responses below."""
    rt = _test_mock_rt_response()
    assert "Data" in rt
    assert "Timestamp" in rt
    assert len(rt["Data"]) > 0

    tick = transform_rt_response(rt)
    assert tick["status"] == "ok"
    assert tick["data"]["requests_per_second"] > 0


def test_poller_fetch_without_credentials_skips_network(monkeypatch):
    """No Fastly credentials configured -> short-circuit, never touch rt.fastly.com.

    This is the real (non-mocked) behavior E2E relies on when a service has no
    real Fastly API key/service id seeded: the poller must not retry against
    the live API with garbage credentials (that caused a 403 retry storm that
    cascaded into unrelated E2E page-load timeouts).
    """
    from backend import config as config_mod
    from backend.core.realtime import poller as poller_mod

    monkeypatch.setattr(config_mod, "get_fastly_api_key", lambda service_id: None)
    monkeypatch.setattr(config_mod, "get_fastly_logging_service_id", lambda service_id: None)

    class _NoNetworkSession:
        headers: dict[str, str] = {}

        def get(self, *args, **kwargs):
            raise AssertionError("fetch with no credentials attempted network I/O")

    rt = poller_mod.RealtimePoller()._fetch_realtime(_NoNetworkSession(), "svc", 0)

    assert rt is None


@pytest.mark.asyncio
async def test_poller_starts_and_suspends(monkeypatch):
    """Poller starts on ensure_polling, suspends after 2 idle cycles."""
    from backend.core.realtime import RealtimeMetricsPublisher
    from backend.core.realtime import poller as poller_mod

    test_publisher = RealtimeMetricsPublisher()
    test_publisher.bind_loop(asyncio.get_running_loop())
    monkeypatch.setattr(poller_mod, "publisher", test_publisher)
    monkeypatch.setattr(poller_mod, "POLL_INTERVAL", 0)

    fetch_count = 0
    original_fetch = poller_mod.RealtimePoller._fetch_realtime

    def _fake_fetch(self, session, service_id, cursor):
        nonlocal fetch_count
        fetch_count += 1
        return _test_mock_rt_response()

    monkeypatch.setattr(poller_mod.RealtimePoller, "_fetch_realtime", _fake_fetch)

    p = poller_mod.RealtimePoller()
    p.ensure_polling("test-svc")

    state = p._services.get("test-svc")
    assert state is not None
    for _ in range(100):
        await asyncio.sleep(0.05)
        if state.thread is None or not state.thread.is_alive():
            break
    assert state.thread is None or not state.thread.is_alive()
    assert fetch_count >= 1


async def _drain(sub_iter):
    """Consume published ticks until cancelled."""
    try:
        async for _ in sub_iter:
            pass
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_cursor_advances(monkeypatch):
    """Cursor from RT response is used in subsequent polls."""
    import threading

    from backend.core.realtime import RealtimeMetricsPublisher
    from backend.core.realtime import poller as poller_mod

    test_publisher = RealtimeMetricsPublisher()
    test_publisher.bind_loop(asyncio.get_running_loop())
    monkeypatch.setattr(poller_mod, "publisher", test_publisher)
    monkeypatch.setattr(poller_mod, "POLL_INTERVAL", 0)

    cursors_seen: list[int] = []
    got_two = threading.Event()

    def _fake_fetch(self, session, service_id, cursor):
        cursors_seen.append(cursor)
        resp = _test_mock_rt_response()
        resp["Timestamp"] = 1720000000 + len(cursors_seen)
        if len(cursors_seen) >= 2:
            got_two.set()
        return resp

    monkeypatch.setattr(poller_mod.RealtimePoller, "_fetch_realtime", _fake_fetch)

    p = poller_mod.RealtimePoller()

    # Keep a subscriber alive by draining ticks in the background so the
    # poller thread doesn't suspend between polls.
    sub_iter = test_publisher.subscribe("test-svc")
    drain_task = asyncio.ensure_future(_drain(sub_iter))
    # Yield so the drain coroutine enters `async for` and registers the
    # subscriber queue — otherwise the thread starts and sees
    # subscriber_count=0 before the event loop has a tick.
    await asyncio.sleep(0)

    p.ensure_polling("test-svc")

    # Wait for the thread to poll at least twice without blocking the
    # asyncio loop (publish uses call_soon_threadsafe, which needs the
    # loop to be running for the drain subscriber to consume ticks and
    # keep subscriber_count > 0).
    await asyncio.get_running_loop().run_in_executor(None, got_two.wait, 2.0)

    drain_task.cancel()
    try:
        await drain_task
    except (asyncio.CancelledError, StopAsyncIteration):
        pass

    assert len(cursors_seen) >= 2
    assert cursors_seen[0] == 0
    assert cursors_seen[1] > 0


# ── Merged from tests/core/test_realtime_transform.py ──
FULL_AGG = {
    "requests": 1000,
    "hits": 800,
    "miss": 150,
    "pass": 40,
    "synth": 10,
    "status_1xx": 0,
    "status_2xx": 900,
    "status_3xx": 50,
    "status_4xx": 30,
    "status_5xx": 20,
    "resp_body_bytes": 5000000,
    "resp_header_bytes": 50000,
    "bereq_header_bytes": 10000,
    "bereq_body_bytes": 1000,
    "shield": 200,
    "shield_resp_body_bytes": 1000000,
    "shield_resp_header_bytes": 10000,
    "waf_blocked": 5,
    "waf_logged": 3,
    "waf_passed": 992,
    "origin_offload": 0.85,
    "hit_time": 0.001,
    "miss_time": 0.15,
    "pass_time": 0.08,
    "http2": 600,
    "http3": 300,
    "ipv6": 150,
    "tls_v12": 200,
    "tls_v13": 800,
    "ddos_action_blackhole": 2,
    "ddos_action_tarpit": 1,
    "ddos_action_close": 3,
    "ddos_action_downgrade": 0,
    "ddos_protection_requests_detect_count": 10,
    "ddos_protection_requests_mitigate_count": 6,
    "status_200": 850,
    "status_204": 50,
    "status_301": 20,
    "status_302": 15,
    "status_304": 15,
    "status_400": 10,
    "status_401": 5,
    "status_403": 8,
    "status_404": 5,
    "status_429": 2,
    "status_500": 8,
    "status_502": 5,
    "status_503": 4,
    "status_504": 3,
    "object_size_1k": 100,
    "object_size_10k": 300,
    "object_size_100k": 250,
    "object_size_1m": 200,
    "object_size_10m": 100,
    "object_size_100m": 30,
    "object_size_1g": 5,
    "object_size_other": 15,
    "origin_fetches": 160,
    "origin_revalidations": 40,
    "origin_cache_fetches": 20,
    "shield_hit_requests": 180,
    "shield_miss_requests": 20,
    "shield_revalidations": 10,
    "shield_fetch_body_bytes": 500000,
    "request_collapse_usable_count": 50,
    "request_collapse_unusable_count": 5,
}

RT_RESPONSE = {
    "Data": [{"aggregated": FULL_AGG, "datacenter": {}}],
    "Timestamp": 1720000000,
    "AggregateDelay": 7,
}


def test_new_fields_extracted():
    result = transform_rt_response(RT_RESPONSE)
    d = result["data"]

    # P4: Byte-Hit-Ratio
    assert d["origin_offload"] == 0.85

    # P1: Cache Path Latency (s → ms)
    assert d["hit_latency_ms"] == 1.0
    assert d["miss_latency_ms"] == 150.0
    assert d["pass_latency_ms"] == 80.0

    # T1: Protocol Adoption — raw counts
    assert d["http2"] == 600
    assert d["http3"] == 300
    assert d["ipv6"] == 150
    assert d["tls_v12"] == 200
    assert d["tls_v13"] == 800

    # T1: Protocol Adoption — percentages
    assert d["h2_pct"] == 60.0
    assert d["h3_pct"] == 30.0
    assert d["ipv6_pct"] == 15.0
    assert d["tls12_pct"] == 20.0
    assert d["tls13_pct"] == 80.0

    # S1: DDoS Mitigation
    assert d["ddos_action_blackhole"] == 2
    assert d["ddos_action_tarpit"] == 1
    assert d["ddos_action_close"] == 3
    assert d["ddos_action_downgrade"] == 0
    assert d["ddos_detect"] == 10
    assert d["ddos_mitigate"] == 6

    # S3: Individual Status Codes
    assert d["status_detail"]["200"] == 850
    assert d["status_detail"]["204"] == 50
    assert d["status_detail"]["301"] == 20
    assert d["status_detail"]["404"] == 5
    assert d["status_detail"]["503"] == 4

    # P3: Object Size Distribution
    assert d["object_size_distribution"]["1k"] == 100
    assert d["object_size_distribution"]["10k"] == 300
    assert d["object_size_distribution"]["100k"] == 250
    assert d["object_size_distribution"]["1g"] == 5

    # O1: Origin Fetch Detail
    assert d["origin_fetches"] == 160
    assert d["origin_revalidations"] == 40
    assert d["origin_cache_fetches"] == 20

    # O2: Shield Efficiency
    assert d["shield_hit_requests"] == 180
    assert d["shield_miss_requests"] == 20
    assert d["shield_revalidations"] == 10
    assert d["shield_fetch_body_bytes"] == 500000

    # T3: Request Collapse
    assert d["request_collapse_usable"] == 50
    assert d["request_collapse_unusable"] == 5


def test_gap_tick_has_new_fields():
    gap = gap_tick_payload()
    d = gap["data"]

    assert d["origin_offload"] == 0.0
    assert d["hit_latency_ms"] == 0.0
    assert d["miss_latency_ms"] == 0.0
    assert d["pass_latency_ms"] == 0.0
    assert d["http2"] == 0
    assert d["http3"] == 0
    assert d["ipv6"] == 0
    assert d["tls_v12"] == 0
    assert d["tls_v13"] == 0
    assert d["h2_pct"] == 0.0
    assert d["h3_pct"] == 0.0
    assert d["ipv6_pct"] == 0.0
    assert d["tls12_pct"] == 0.0
    assert d["tls13_pct"] == 0.0
    assert d["ddos_action_blackhole"] == 0
    assert d["ddos_action_tarpit"] == 0
    assert d["ddos_action_close"] == 0
    assert d["ddos_action_downgrade"] == 0
    assert d["ddos_detect"] == 0
    assert d["ddos_mitigate"] == 0
    assert d["status_detail"] == {}
    assert d["object_size_distribution"] == {}
    assert d["origin_fetches"] == 0
    assert d["origin_revalidations"] == 0
    assert d["origin_cache_fetches"] == 0
    assert d["shield_hit_requests"] == 0
    assert d["shield_miss_requests"] == 0
    assert d["shield_revalidations"] == 0
    assert d["shield_fetch_body_bytes"] == 0
    assert d["request_collapse_usable"] == 0
    assert d["request_collapse_unusable"] == 0


def test_missing_new_fields_default_to_zero():
    minimal_agg = {
        "requests": 500,
        "hits": 400,
        "miss": 80,
        "pass": 15,
        "synth": 5,
        "status_1xx": 0,
        "status_2xx": 450,
        "status_3xx": 20,
        "status_4xx": 20,
        "status_5xx": 10,
        "resp_body_bytes": 2000000,
        "resp_header_bytes": 20000,
        "bereq_header_bytes": 5000,
        "bereq_body_bytes": 500,
        "shield": 100,
        "shield_resp_body_bytes": 500000,
        "shield_resp_header_bytes": 5000,
        "waf_blocked": 2,
        "waf_logged": 1,
        "waf_passed": 497,
    }
    rt = {
        "Data": [{"aggregated": minimal_agg, "datacenter": {}}],
        "Timestamp": 1720000000,
        "AggregateDelay": 5,
    }
    result = transform_rt_response(rt)
    d = result["data"]

    assert d["origin_offload"] == 0.0
    assert d["hit_latency_ms"] == 0.0
    assert d["miss_latency_ms"] == 0.0
    assert d["pass_latency_ms"] == 0.0
    assert d["http2"] == 0
    assert d["http3"] == 0
    assert d["ipv6"] == 0
    assert d["tls_v12"] == 0
    assert d["tls_v13"] == 0
    assert d["h2_pct"] == 0.0
    assert d["h3_pct"] == 0.0
    assert d["ipv6_pct"] == 0.0
    assert d["tls12_pct"] == 0.0
    assert d["tls13_pct"] == 0.0
    assert d["ddos_action_blackhole"] == 0
    assert d["ddos_action_tarpit"] == 0
    assert d["ddos_action_close"] == 0
    assert d["ddos_action_downgrade"] == 0
    assert d["ddos_detect"] == 0
    assert d["ddos_mitigate"] == 0
    assert d["status_detail"] == {}
    assert d["object_size_distribution"] == {}
    assert d["origin_fetches"] == 0
    assert d["origin_revalidations"] == 0
    assert d["origin_cache_fetches"] == 0
    assert d["shield_hit_requests"] == 0
    assert d["shield_miss_requests"] == 0
    assert d["shield_revalidations"] == 0
    assert d["shield_fetch_body_bytes"] == 0
    assert d["request_collapse_usable"] == 0
    assert d["request_collapse_unusable"] == 0


def test_multi_second_aggregation():
    agg1 = {
        "requests": 1000,
        "hits": 800,
        "miss": 150,
        "pass": 40,
        "synth": 10,
        "status_2xx": 900,
        "status_4xx": 30,
        "status_5xx": 20,
        "resp_body_bytes": 5000000,
        "resp_header_bytes": 50000,
        "bereq_header_bytes": 10000,
        "bereq_body_bytes": 1000,
        "shield": 200,
        "shield_resp_body_bytes": 1000000,
        "shield_resp_header_bytes": 10000,
        "waf_blocked": 5,
        "waf_logged": 3,
        "waf_passed": 992,
        "origin_offload": 0.90,
        "hit_time": 0.002,
        "miss_time": 0.10,
        "pass_time": 0.06,
        "http2": 600,
        "http3": 300,
        "ipv6": 100,
        "tls_v12": 200,
        "tls_v13": 800,
        "ddos_action_blackhole": 2,
        "ddos_action_tarpit": 1,
        "ddos_action_close": 3,
        "ddos_action_downgrade": 0,
        "ddos_protection_requests_detect_count": 10,
        "ddos_protection_requests_mitigate_count": 6,
        "status_200": 800,
        "status_404": 5,
        "object_size_1k": 100,
        "object_size_10k": 200,
        "origin_fetches": 100,
        "origin_revalidations": 30,
        "origin_cache_fetches": 10,
        "shield_hit_requests": 150,
        "shield_miss_requests": 30,
        "shield_revalidations": 5,
        "shield_fetch_body_bytes": 400000,
        "request_collapse_usable_count": 40,
        "request_collapse_unusable_count": 3,
    }
    agg2 = {
        "requests": 1000,
        "hits": 700,
        "miss": 200,
        "pass": 80,
        "synth": 20,
        "status_2xx": 850,
        "status_4xx": 50,
        "status_5xx": 30,
        "resp_body_bytes": 6000000,
        "resp_header_bytes": 60000,
        "bereq_header_bytes": 12000,
        "bereq_body_bytes": 2000,
        "shield": 180,
        "shield_resp_body_bytes": 900000,
        "shield_resp_header_bytes": 9000,
        "waf_blocked": 3,
        "waf_logged": 2,
        "waf_passed": 995,
        "origin_offload": 0.80,
        "hit_time": 0.004,
        "miss_time": 0.20,
        "pass_time": 0.10,
        "http2": 400,
        "http3": 200,
        "ipv6": 200,
        "tls_v12": 300,
        "tls_v13": 700,
        "ddos_action_blackhole": 1,
        "ddos_action_tarpit": 0,
        "ddos_action_close": 2,
        "ddos_action_downgrade": 1,
        "ddos_protection_requests_detect_count": 8,
        "ddos_protection_requests_mitigate_count": 4,
        "status_200": 750,
        "status_404": 10,
        "object_size_1k": 50,
        "object_size_10k": 300,
        "origin_fetches": 120,
        "origin_revalidations": 20,
        "origin_cache_fetches": 15,
        "shield_hit_requests": 130,
        "shield_miss_requests": 40,
        "shield_revalidations": 8,
        "shield_fetch_body_bytes": 600000,
        "request_collapse_usable_count": 30,
        "request_collapse_unusable_count": 7,
    }

    rt = {
        "Data": [
            {"aggregated": agg1, "datacenter": {}},
            {"aggregated": agg2, "datacenter": {}},
        ],
        "Timestamp": 1720000000,
        "AggregateDelay": 5,
    }
    result = transform_rt_response(rt)
    d = result["data"]

    # Count fields are SUMMED
    assert d["http2"] == 1000
    assert d["http3"] == 500
    assert d["ipv6"] == 300
    assert d["tls_v12"] == 500
    assert d["tls_v13"] == 1500
    assert d["ddos_action_blackhole"] == 3
    assert d["ddos_action_tarpit"] == 1
    assert d["ddos_action_close"] == 5
    assert d["ddos_action_downgrade"] == 1
    assert d["ddos_detect"] == 18
    assert d["ddos_mitigate"] == 10
    assert d["origin_fetches"] == 220
    assert d["origin_revalidations"] == 50
    assert d["origin_cache_fetches"] == 25
    assert d["shield_hit_requests"] == 280
    assert d["shield_miss_requests"] == 70
    assert d["shield_revalidations"] == 13
    assert d["shield_fetch_body_bytes"] == 1000000
    assert d["request_collapse_usable"] == 70
    assert d["request_collapse_unusable"] == 10

    # Status detail summed
    assert d["status_detail"]["200"] == 1550
    assert d["status_detail"]["404"] == 15

    # Object size distribution summed
    assert d["object_size_distribution"]["1k"] == 150
    assert d["object_size_distribution"]["10k"] == 500

    # Latency fields are AVERAGED (sum / n * 1000)
    assert d["hit_latency_ms"] == round((0.002 + 0.004) / 2 * 1000, 2)  # 3.0
    assert d["miss_latency_ms"] == round((0.10 + 0.20) / 2 * 1000, 2)  # 150.0
    assert d["pass_latency_ms"] == round((0.06 + 0.10) / 2 * 1000, 2)  # 80.0

    # Origin offload averaged
    assert d["origin_offload"] == round((0.90 + 0.80) / 2, 4)  # 0.85

    # Percentages computed from totals (total_requests = 2000)
    assert d["h2_pct"] == 50.0  # 1000/2000 * 100
    assert d["h3_pct"] == 25.0  # 500/2000 * 100
    assert d["ipv6_pct"] == 15.0  # 300/2000 * 100
    assert d["tls12_pct"] == 25.0  # 500/2000 * 100
    assert d["tls13_pct"] == 75.0  # 1500/2000 * 100
