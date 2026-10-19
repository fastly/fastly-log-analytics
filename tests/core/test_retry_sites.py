"""Characterization of the hand-written retry loops folded into ``retry_call`` (B4).

Each test pins attempt count, backoff delays and terminal-exception semantics of
one call site so the retry-policy unification stays behavior-neutral. Sleeps are
captured (never slept) by patching both ``time.sleep`` and any module-level
``_retry_sleep`` seam the site exposes.
"""

from __future__ import annotations

import sys
from contextlib import ExitStack, contextmanager
from unittest.mock import MagicMock, patch

import duckdb
import httpx
import pytest

from backend.core import duckdb as core_duckdb
from backend.core.realtime import poller as poller_mod
from backend.repositories._base import QueryRunner


@contextmanager
def _captured_sleeps(*extra_targets: str, only_from: str | None = None):
    """Record sleep delays instead of sleeping.

    ``only_from`` restricts the global ``time.sleep`` capture to calls made
    (within a few frames) from a file whose path ends with that suffix (other threads in the
    process also sleep). Seam targets in ``extra_targets`` record unconditionally.
    """
    delays: list[float] = []

    def _global_sleep(seconds):
        if only_from is None:
            delays.append(seconds)
            return
        frame = sys._getframe(1)
        for _ in range(8):
            if frame is None:
                return
            if frame.f_code.co_filename.endswith(only_from):
                delays.append(seconds)
                return
            frame = frame.f_back

    with ExitStack() as stack:
        stack.enter_context(patch("time.sleep", side_effect=_global_sleep))
        for target in extra_targets:
            stack.enter_context(patch(target, side_effect=delays.append, create=True))
        yield delays


# ── realtime poller ──────────────────────────────────────────────────────────


def _Resp(status: int, body: dict | None = None) -> httpx.Response:
    return httpx.Response(status, json=body or {}, request=httpx.Request("GET", "https://rt.example.test/"))


class _ScriptedSession:
    def __init__(self, script: list):
        self.headers: dict[str, str] = {}
        self.script = list(script)
        self.calls = 0

    def get(self, url, headers=None, timeout=None):
        self.calls += 1
        step = self.script.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step


@pytest.fixture
def _poller_creds(monkeypatch):
    from backend import config as config_mod

    monkeypatch.setattr(config_mod, "get_fastly_api_key", lambda service_id: "k")
    monkeypatch.setattr(config_mod, "get_fastly_service_id", lambda service_id: "fsid")


def _fetch(session, cursor=0):
    return poller_mod.RealtimePoller()._fetch_realtime(session, "svc", cursor)


def test_poller_retries_503_with_1s_2s_backoff_then_succeeds(_poller_creds):
    session = _ScriptedSession([_Resp(503), _Resp(503), _Resp(200, {"ok": 1})])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        assert _fetch(session) == {"ok": 1}
    assert session.calls == 3
    assert delays == [1, 2]


def test_poller_exhausted_retryable_status_raises_http_error_without_final_sleep(_poller_creds):
    session = _ScriptedSession([_Resp(429), _Resp(502), _Resp(500)])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(httpx.HTTPStatusError) as ei:
            _fetch(session)
    assert ei.value.response.status_code == 500
    assert session.calls == 3
    assert delays == [1, 2]


def test_poller_non_retryable_status_raises_immediately(_poller_creds):
    session = _ScriptedSession([_Resp(404)])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(httpx.HTTPStatusError):
            _fetch(session)
    assert session.calls == 1
    assert delays == []


def test_poller_connection_error_retries_then_raises_last(_poller_creds):
    errs = [httpx.ConnectError("a"), httpx.ConnectError("b"), httpx.ConnectError("c")]
    session = _ScriptedSession(errs)
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(httpx.ConnectError, match="c"):
            _fetch(session)
    assert session.calls == 3
    assert delays == [1, 2]


def test_poller_non_longpoll_read_timeout_is_retried(_poller_creds):
    session = _ScriptedSession([httpx.ReadTimeout("timed out"), _Resp(200, {"ok": 2})])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        assert _fetch(session, cursor=0) == {"ok": 2}
    assert delays == [1]


def test_poller_longpoll_read_timeout_short_circuits(_poller_creds):
    session = _ScriptedSession([httpx.ReadTimeout("timed out")])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(poller_mod._LongPollTimeout):
            _fetch(session, cursor=123)
    assert session.calls == 1
    assert delays == []


def test_poller_unrelated_exception_is_not_retried(_poller_creds):
    session = _ScriptedSession([ValueError("boom")])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(ValueError):
            _fetch(session)
    assert session.calls == 1
    assert delays == []


# ── FOS proxy secret (write-write conflict) ──────────────────────────────────


def _configure(con) -> None:
    with (
        patch("backend.core.duckdb._load_httpfs"),
        patch("backend.utils.telemetry_proxy.start_proxy_server"),
        patch("backend.utils.telemetry_proxy.proxy_endpoint", return_value="http://127.0.0.1:1"),
    ):
        core_duckdb._configure_fos(con, {"name": "svc-retry", "access_key_id": "a", "secret_access_key": "b"})


def _con_failing_secret(errors: list[BaseException]) -> MagicMock:
    con = MagicMock()
    pending = list(errors)

    def _execute(sql, *args, **kwargs):
        if "CREATE OR REPLACE SECRET" in str(sql) and pending:
            raise pending.pop(0)
        return MagicMock()

    con.execute.side_effect = _execute
    return con


def _secret_calls(con) -> int:
    return sum(1 for c in con.execute.call_args_list if "CREATE OR REPLACE SECRET" in str(c.args[0]))


def test_fos_secret_retries_write_write_conflict_with_rollback_each_attempt():
    con = _con_failing_secret([Exception("Write-Write Conflict on x"), Exception("write-write conflict")])
    with _captured_sleeps() as delays:
        _configure(con)
    assert _secret_calls(con) == 3
    assert con.rollback.call_count == 3
    assert delays == []


def test_fos_secret_third_conflict_is_raised():
    con = _con_failing_secret([Exception("write-write conflict")] * 3)
    with _captured_sleeps():
        with pytest.raises(Exception, match="write-write conflict"):
            _configure(con)
    assert _secret_calls(con) == 3


def test_fos_secret_other_error_raises_immediately():
    con = _con_failing_secret([RuntimeError("bad key")])
    with pytest.raises(RuntimeError, match="bad key"):
        _configure(con)
    assert _secret_calls(con) == 1


# ── QueryRunner stale-view retry ─────────────────────────────────────────────


def _stale() -> Exception:
    return duckdb.IOException("No files found: batch_x.parquet")


def _runner(side_effects: list) -> QueryRunner:
    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc"})
    runner.con = MagicMock()
    runner.con.execute.side_effect = side_effects
    return runner


def test_execute_with_retry_success_does_not_refresh():
    runner = _runner(["cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.execute_with_retry("SELECT 1") == "cur"
    refresh.assert_not_called()


def test_execute_with_retry_stale_then_success_refreshes_once():
    runner = _runner([_stale(), "cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.execute_with_retry("SELECT 1") == "cur"
    assert refresh.call_count == 1
    assert runner.con.execute.call_count == 2


def test_execute_with_retry_non_stale_raises_without_refresh():
    runner = _runner([RuntimeError("syntax")])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        with pytest.raises(RuntimeError, match="syntax"):
            runner.execute_with_retry("SELECT 1")
    refresh.assert_not_called()


def test_execute_with_retry_outer_retry_after_inner_self_heal_exhausted():
    # QueryRunner.execute self-heals once internally; the outer retry only
    # fires when that inner retry also surfaces a stale-view error.
    runner = _runner([_stale(), _stale(), "cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.execute_with_retry("SELECT 1") == "cur"
    assert refresh.call_count == 2
    assert runner.con.execute.call_count == 3


def test_execute_with_retry_non_stale_failure_after_outer_refresh_returns_none():
    runner = _runner([_stale(), _stale(), RuntimeError("late")])
    with patch("backend.core.iceberg.update_iceberg_view"):
        assert runner.execute_with_retry("SELECT 1") is None
    assert runner.con.execute.call_count == 3


def test_execute_with_retry_refresh_failure_returns_none():
    runner = _runner([_stale()])
    with patch("backend.core.iceberg.update_iceberg_view", side_effect=RuntimeError("refresh")):
        assert runner.execute_with_retry("SELECT 1") is None
    assert runner.con.execute.call_count == 1


def test_create_temp_table_stale_then_success_is_true_after_one_refresh():
    runner = _runner([_stale(), "cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is True
    assert refresh.call_count == 1


def test_create_temp_table_outer_retry_after_inner_self_heal_exhausted():
    runner = _runner([_stale(), _stale(), "cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is True
    assert refresh.call_count == 2


def test_create_temp_table_non_stale_after_outer_refresh_is_false():
    runner = _runner([_stale(), _stale(), RuntimeError("late")])
    with patch("backend.core.iceberg.update_iceberg_view"):
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is False


def test_create_temp_table_refresh_failure_is_false():
    runner = _runner([_stale()])
    with patch("backend.core.iceberg.update_iceberg_view", side_effect=RuntimeError("refresh")):
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is False


# ── iceberg sync: post-sync cache update rate-limit loop ─────────────────────


def _sync_with_lock_errors(fos_source, tmp_path, errors: list[BaseException]):
    from backend.core import iceberg as ice

    source = {**fos_source, "name": "retry-sync-svc", "prefix": "logs"}
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m.json"
    mock_table.location.return_value = "s3://b"
    scan = MagicMock()
    scan.filter.return_value = scan
    scan.plan_files.return_value = []
    mock_table.scan.return_value = scan
    catalog = MagicMock()
    catalog.load_table.return_value = mock_table

    pending = list(errors)
    calls = {"n": 0}
    real_lock = MagicMock()
    real_lock.__enter__ = MagicMock(return_value=None)
    real_lock.__exit__ = MagicMock(return_value=False)

    def _get_lock(_key):
        calls["n"] += 1
        # Call 1 is the Phase-1 catalog lock (outside the retry loop).
        if calls["n"] > 1 and pending:
            raise pending.pop(0)
        return real_lock

    with (
        patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True),
        patch("backend.core.iceberg._get_catalog", return_value=catalog),
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.iceberg._core._get_service_lock", side_effect=_get_lock),
        _captured_sleeps("backend.core.iceberg.sync._retry_sleep", only_from="iceberg/sync.py") as delays,
    ):
        ice._snapshot_files_cache.pop(source["name"], None)
        try:
            ice.sync_data(source)
        finally:
            ice._snapshot_files_cache.pop(source["name"], None)
    return calls["n"] - 1, delays


def test_sync_cache_update_retries_rate_limit_with_half_second_doubling(fos_source, tmp_path):
    attempts, delays = _sync_with_lock_errors(
        fos_source, tmp_path, [OSError("[Errno 16] Reduce your request rate"), Exception("SlowDown")]
    )
    assert attempts == 3
    assert delays == [0.5, 1.0]


def test_sync_cache_update_gives_up_after_three_rate_limit_failures(fos_source, tmp_path):
    attempts, delays = _sync_with_lock_errors(fos_source, tmp_path, [Exception("too many requests")] * 5)
    assert attempts == 3
    assert delays == [0.5, 1.0]


def test_sync_cache_update_non_rate_limit_error_is_not_retried(fos_source, tmp_path):
    attempts, delays = _sync_with_lock_errors(fos_source, tmp_path, [RuntimeError("catalog corrupt")])
    assert attempts == 1
    assert delays == []
