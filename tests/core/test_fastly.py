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
