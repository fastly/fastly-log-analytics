from __future__ import annotations

import sys
import time
from contextlib import ExitStack, contextmanager
from unittest.mock import MagicMock, patch

import duckdb
import httpx
import pytest

from backend.core import duckdb as core_duckdb
from backend.core.iceberg import execute_with_stale_view_retry, is_stale_view_error
from backend.core.realtime import poller as poller_mod
from backend.core.retry import retry_call
from backend.repositories._base import QueryRunner

# ── From test_iceberg_self_heal.py ──

"""Tests for the iceberg.execute_with_stale_view_retry helper.

The helper exists so background-job code paths (rdns_cache discovery,
rollups DESCRIBE) — which open raw DuckDB connections instead of going
through QueryRunner — can recover from the same buffer-deletion race
that QueryRunner.execute already handles. Production incident
2026-06-10 (~8 hours of 100%-failing rdns discovery runs spamming
the log) is the regression these tests pin against.
"""


class TestIsStaleViewError:
    @pytest.mark.parametrize(
        "msg",
        [
            'IO Error: No files found that match the pattern "cache/foo/batch_abc.parquet"',
            "Catalog Error: Table with name logs_xyz does not exist",
            "No such file or directory: /tmp/buf.parquet",
            # DuckLake's brief DETACH window during commit/optimize: a
            # reader mid-checkout hits this exact CatalogException text.
            'Catalog Error: Table with name "lake.logs" does not exist because schema "lake" does not exist.',
        ],
    )
    def test_recognises_known_messages(self, msg: str) -> None:
        assert is_stale_view_error(Exception(msg)) is True

    def test_returns_false_for_unrelated_errors(self) -> None:
        assert is_stale_view_error(Exception("Syntax error at line 1")) is False
        assert is_stale_view_error(Exception("Permission denied")) is False
        assert is_stale_view_error(Exception("Connection refused")) is False

    def test_recognises_real_duckdb_exception_from_detached_ducklake_catalog(self) -> None:
        """Not just a string fixture — a REAL DuckDB error from the exact
        DuckLake DETACH-then-query race (commit_batch/optimize briefly
        detach 'lake' on a connection other readers share), confirming the
        matcher isn't accidentally coupled to a specific error phrasing
        that a future DuckDB version could change."""
        import duckdb as _duckdb

        con = _duckdb.connect()
        con.execute("ATTACH ':memory:' AS lake")
        con.execute("CREATE TABLE lake.logs (x INT)")
        con.execute("DETACH lake")
        try:
            con.execute("SELECT * FROM lake.logs")
        except Exception as e:
            assert is_stale_view_error(e) is True
        else:
            pytest.fail("expected querying a detached catalog to raise")


class TestExecuteWithStaleViewRetry:
    def test_passthrough_when_fn_succeeds(self) -> None:
        """No retry / no cache bust when the first call succeeds."""
        con = MagicMock()
        src = {"name": "svc-a"}
        fn = MagicMock(return_value="ok")

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
        ):
            result = execute_with_stale_view_retry(con, src, fn)

        assert result == "ok"
        fn.assert_called_once_with(con)
        mock_clear.assert_not_called()
        mock_update.assert_not_called()

    def test_retries_once_after_stale_view_error(self) -> None:
        """First call raises a stale-view error → bust caches + force rebind + retry."""
        con = MagicMock()
        src = {"name": "svc-b"}
        fn = MagicMock(
            side_effect=[
                Exception('IO Error: No files found that match the pattern "buffer/batch_x.parquet"'),
                ["row1", "row2"],
            ]
        )

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
        ):
            result = execute_with_stale_view_retry(con, src, fn)

        assert result == ["row1", "row2"]
        assert fn.call_count == 2
        # Both calls receive the same con — i.e. retry isn't allocating a new connection.
        for call in fn.call_args_list:
            assert call.args[0] is con
        mock_clear.assert_called_once_with("svc-b", keep_snapshot_cache=True)
        mock_update.assert_called_once()
        # update_iceberg_view called with (con, src, force=True)
        upd_call = mock_update.call_args
        assert upd_call.args[0] is con
        assert upd_call.args[1] is src
        assert upd_call.kwargs.get("force") is True

    def test_non_stale_error_propagates_without_retry(self) -> None:
        """A non-stale error must surface immediately — don't waste a rebind on a real failure."""
        con = MagicMock()
        src = {"name": "svc-c"}
        fn = MagicMock(side_effect=ValueError("bad SQL"))

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
        ):
            with pytest.raises(ValueError, match="bad SQL"):
                execute_with_stale_view_retry(con, src, fn)

        assert fn.call_count == 1
        mock_clear.assert_not_called()
        mock_update.assert_not_called()

    def test_second_attempt_failure_propagates(self) -> None:
        """If the retry itself fails, propagate — caller chooses fallback (log + skip vs raise).

        Pins the contract so a tempting "also swallow on retry failure" change
        wouldn't slip past silently — the caller-side `except duckdb.Error`
        block stays in charge of the user-visible behaviour.
        """
        con = MagicMock()
        src = {"name": "svc-d"}
        fn = MagicMock(
            side_effect=[
                Exception("No files found"),
                RuntimeError("still broken"),
            ]
        )

        with (
            patch("backend.core.iceberg._core.clear_source_caches"),
            patch("backend.core.iceberg._core.update_iceberg_view"),
        ):
            with pytest.raises(RuntimeError, match="still broken"):
                execute_with_stale_view_retry(con, src, fn)

        assert fn.call_count == 2

    def test_passes_through_extra_args_and_kwargs(self) -> None:
        """The helper must forward *args and **kwargs to ``fn``."""
        con = MagicMock()
        src = {"name": "svc-e"}
        fn = MagicMock(return_value=42)

        with (
            patch("backend.core.iceberg._core.clear_source_caches"),
            patch("backend.core.iceberg._core.update_iceberg_view"),
        ):
            result = execute_with_stale_view_retry(con, src, fn, "a", 1, key="value")

        assert result == 42
        fn.assert_called_once_with(con, "a", 1, key="value")

    def test_default_source_key_when_name_missing(self) -> None:
        """``src`` without a ``name`` key falls back to ``"default"`` for the
        cache-bust call — never raises KeyError on the retry path."""
        con = MagicMock()
        src: dict = {}
        fn = MagicMock(side_effect=[Exception("No files found"), "ok"])

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view"),
        ):
            result = execute_with_stale_view_retry(con, src, fn)

        assert result == "ok"
        mock_clear.assert_called_once_with("default", keep_snapshot_cache=True)


class TestConfigureDuckdbS3:
    def test_configure_duckdb_s3_loads_extensions(self) -> None:
        from backend.core.iceberg.view import configure_duckdb_s3

        con = MagicMock()
        configure_duckdb_s3(con)
        calls = [c.args[0] for c in con.execute.call_args_list]
        assert any("LOAD httpfs; LOAD parquet;" in c for c in calls)

    def test_configure_duckdb_s3_installs_httpfs_on_load_failure(self) -> None:
        from backend.core.iceberg.view import configure_duckdb_s3

        con = MagicMock()
        seen: list[str] = []

        def side_effect(sql, *args):
            seen.append(sql)
            if sql == "LOAD httpfs; LOAD parquet;" and seen.count("LOAD httpfs; LOAD parquet;") == 1:
                raise RuntimeError("Extension not installed")

        con.execute.side_effect = side_effect
        configure_duckdb_s3(con)
        assert "INSTALL httpfs; INSTALL parquet;" in seen

    def test_configure_duckdb_s3_reraises_when_install_also_fails(self) -> None:
        from backend.core.iceberg.view import configure_duckdb_s3

        con = MagicMock()

        def side_effect(sql, *args):
            if "httpfs" in sql:
                raise RuntimeError("offline")

        con.execute.side_effect = side_effect
        with pytest.raises(RuntimeError, match="offline"):
            configure_duckdb_s3(con)


# ── From test_retry.py ──


class _Transient(Exception):
    pass


class _Fatal(Exception):
    pass


def _is_transient(exc: BaseException) -> bool:
    return isinstance(exc, _Transient)


def test_success_passthrough_no_sleep() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> str:
        calls["n"] += 1
        return "ok"

    result = retry_call(fn, retry_on=_is_transient, attempts=5, sleep=sleeps.append)

    assert result == "ok"
    assert calls["n"] == 1
    assert sleeps == []


def test_retryable_retried_to_budget_then_reraises() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=4,
            base_delay=1.0,
            multiplier=2.0,
            sleep=sleeps.append,
        )

    # 4 attempts -> 3 sleeps between them
    assert calls["n"] == 4
    assert sleeps == [1.0, 2.0, 4.0]


def test_fatal_not_retried() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Fatal("nope")

    with pytest.raises(_Fatal):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=5,
            base_delay=1.0,
            sleep=sleeps.append,
        )

    assert calls["n"] == 1
    assert sleeps == []


def test_eventual_success_stops_retrying() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Transient("retry me")
        return "done"

    result = retry_call(
        fn,
        retry_on=_is_transient,
        attempts=5,
        base_delay=1.0,
        multiplier=2.0,
        sleep=sleeps.append,
    )

    assert result == "done"
    assert calls["n"] == 3
    assert sleeps == [1.0, 2.0]


def test_max_delay_clamps_backoff() -> None:
    sleeps: list[float] = []

    def fn() -> None:
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=5,
            base_delay=1.0,
            multiplier=10.0,
            max_delay=5.0,
            sleep=sleeps.append,
        )

    # 1, min(10,5)=5, min(100,5)=5, min(1000,5)=5
    assert sleeps == [1.0, 5.0, 5.0, 5.0]


def test_constant_backoff_via_multiplier_one() -> None:
    sleeps: list[float] = []

    def fn() -> None:
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=3,
            base_delay=1.0,
            multiplier=1.0,
            sleep=sleeps.append,
        )

    assert sleeps == [1.0, 1.0]


def test_before_retry_fires_between_attempts_with_exception() -> None:
    sleeps: list[float] = []
    seen: list[BaseException] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient(f"attempt-{calls['n']}")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=3,
            base_delay=1.0,
            before_retry=seen.append,
            sleep=sleeps.append,
        )

    # Hook runs once per retry (not after the final give-up attempt).
    assert len(seen) == 2
    assert all(isinstance(e, _Transient) for e in seen)
    assert [str(e) for e in seen] == ["attempt-1", "attempt-2"]


def test_before_retry_not_called_on_fatal() -> None:
    seen: list[BaseException] = []

    def fn() -> None:
        raise _Fatal("nope")

    with pytest.raises(_Fatal):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=5,
            before_retry=seen.append,
            sleep=lambda _d: None,
        )

    assert seen == []


def test_max_elapsed_budget_stops_early() -> None:
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    # tenacity's stop_after_delay reads real monotonic time, so a real (tiny)
    # sleep is required for the wall-clock budget to advance. Kept small so the
    # test stays fast while still proving the budget cuts off well before the
    # attempt cap.
    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=1000,
            base_delay=0.02,
            multiplier=1.0,
            max_elapsed=0.05,
            sleep=time.sleep,
        )

    # Budget cut it off well before the 1000-attempt cap.
    assert calls["n"] < 1000


def test_attempts_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="attempts must be >= 1"):
        retry_call(lambda: None, retry_on=_is_transient, attempts=0)


def test_attempts_one_calls_once_and_propagates() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=1,
            base_delay=1.0,
            sleep=sleeps.append,
        )

    assert calls["n"] == 1
    assert sleeps == []


# ── From test_retry_sites.py ──


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
