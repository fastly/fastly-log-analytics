"""Consolidated tests for test_active_requests."""

from __future__ import annotations

import logging
import threading
import time

import pytest
from backend.utils.active_requests import (
    _reset_for_tests,
    active_request_count,
    decrement_active_requests,
    increment_active_requests,
    should_defer_cron,
    yield_to_api,
)

from backend.utils import active_requests


@pytest.fixture(autouse=True)
def _clean_state():
    """Reset module-level counter + defer window between every test."""
    _reset_for_tests()
    yield
    _reset_for_tests()


# ---------------------------------------------------------------------------
# Counter primitives
# ---------------------------------------------------------------------------


def test_initial_count_is_zero_after_reset():
    assert active_request_count() == 0


def test_increment_bumps_count():
    increment_active_requests()
    assert active_request_count() == 1
    increment_active_requests()
    assert active_request_count() == 2


def test_decrement_reduces_count():
    increment_active_requests()
    increment_active_requests()
    decrement_active_requests()
    assert active_request_count() == 1


def test_decrement_clamps_at_zero():
    """Belt-and-suspenders: a stray decrement must NOT take the counter
    negative, otherwise should_defer_cron's `_active_count == 0` branch
    would never fire again."""
    decrement_active_requests()
    decrement_active_requests()
    assert active_request_count() == 0


def test_increment_then_decrement_back_to_zero():
    for _ in range(5):
        increment_active_requests()
    assert active_request_count() == 5
    for _ in range(5):
        decrement_active_requests()
    assert active_request_count() == 0


def test_reset_for_tests_clears_counter_and_window():
    increment_active_requests()
    increment_active_requests()
    # Seed the defer-window dict via a deferral
    assert should_defer_cron("sync", "svc-A") is True
    assert active_requests._defer_started_at  # populated

    _reset_for_tests()

    assert active_request_count() == 0
    assert active_requests._defer_started_at == {}


# ---------------------------------------------------------------------------
# Thread safety (light coverage — `with _lock` is the contract here)
# ---------------------------------------------------------------------------


def test_concurrent_increments_are_atomic():
    """Spawn 50 threads, each calls increment_active_requests once.
    Without the lock, the read-modify-write would race and lose updates."""
    workers = []
    barrier = threading.Barrier(50)

    def _worker():
        barrier.wait()  # maximize contention
        increment_active_requests()

    for _ in range(50):
        t = threading.Thread(target=_worker)
        workers.append(t)
        t.start()
    for t in workers:
        t.join()

    assert active_request_count() == 50


def test_concurrent_decrements_clamp_at_zero():
    """Over-decrementing across threads must still bottom out at 0."""
    for _ in range(10):
        increment_active_requests()

    workers = []
    barrier = threading.Barrier(30)

    def _worker():
        barrier.wait()
        decrement_active_requests()

    for _ in range(30):
        t = threading.Thread(target=_worker)
        workers.append(t)
        t.start()
    for t in workers:
        t.join()

    assert active_request_count() == 0


# ---------------------------------------------------------------------------
# should_defer_cron — branch coverage
# ---------------------------------------------------------------------------


def test_should_defer_returns_false_when_no_active_requests():
    assert active_request_count() == 0
    assert should_defer_cron("sync", "svc-A") is False


def test_should_defer_clears_prior_window_when_quiet():
    """If a deferral window was set during a busy period but the system
    has since gone idle, the next call must clear that window so the
    NEXT contention round starts a fresh max_defer_secs clock."""
    increment_active_requests()
    assert should_defer_cron("sync", "svc-A") is True
    assert ("sync", "svc-A") in active_requests._defer_started_at

    # Drain in-flight requests.
    decrement_active_requests()
    assert active_request_count() == 0

    assert should_defer_cron("sync", "svc-A") is False
    assert ("sync", "svc-A") not in active_requests._defer_started_at


def test_should_defer_returns_true_and_seeds_window_when_busy():
    increment_active_requests()
    assert ("sync", "svc-A") not in active_requests._defer_started_at

    assert should_defer_cron("sync", "svc-A") is True

    # Window must now exist with a monotonic timestamp.
    started = active_requests._defer_started_at[("sync", "svc-A")]
    assert isinstance(started, float)
    assert started > 0


def test_should_defer_returns_true_while_window_within_budget():
    """Second call within max_defer_secs of the first must still defer,
    and the window must NOT be reset (otherwise starvation safeguard
    would never fire on a sustained-busy service)."""
    increment_active_requests()
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    first_started = active_requests._defer_started_at[("sync", "svc-A")]

    # A subsequent call still well within the 30 s budget should defer.
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    second_started = active_requests._defer_started_at[("sync", "svc-A")]
    assert second_started == first_started  # window preserved


def test_should_defer_returns_false_after_starvation_window_expires(caplog):
    """Force the window to age past max_defer_secs by passing a tiny
    budget + sleeping. The third call must return False, clear the
    window, and emit a WARN."""
    increment_active_requests()

    # Seed window.
    assert should_defer_cron("sync", "svc-A", max_defer_secs=0.01) is True
    assert ("sync", "svc-A") in active_requests._defer_started_at

    time.sleep(0.02)

    with caplog.at_level(logging.WARNING, logger="backend.utils.active_requests"):
        result = should_defer_cron("sync", "svc-A", max_defer_secs=0.01)

    assert result is False
    # Starvation path must clear the window so the next round starts fresh.
    assert ("sync", "svc-A") not in active_requests._defer_started_at

    warn_records = [
        r for r in caplog.records if r.levelno == logging.WARNING and r.name == "backend.utils.active_requests"
    ]
    assert len(warn_records) == 1
    msg = warn_records[0].getMessage()
    assert "active-request-gate" in msg
    assert "sync" in msg
    assert "svc-A" in msg


def test_should_defer_starvation_via_monkeypatched_monotonic(monkeypatch):
    """Belt-and-suspenders: drive the starvation branch deterministically
    by stubbing time.monotonic — avoids any flakiness from wall-clock
    jitter on a loaded CI box."""
    increment_active_requests()

    fake_now = [1000.0]

    def _fake_monotonic():
        return fake_now[0]

    monkeypatch.setattr(active_requests.time, "monotonic", _fake_monotonic)

    # t=1000.0 — seed window
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True
    assert active_requests._defer_started_at[("optimize", "svc-Z")] == 1000.0

    # t=1010.0 — within budget, still defers
    fake_now[0] = 1010.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True

    # t=1040.0 — exceeds 30 s budget, runs anyway
    fake_now[0] = 1040.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is False
    assert ("optimize", "svc-Z") not in active_requests._defer_started_at


def test_should_defer_tracks_windows_per_key_independently():
    """Concurrent services must not share a starvation clock — deferring
    job=sync for svc-A must NOT mark job=sync for svc-B as deferred."""
    increment_active_requests()

    assert should_defer_cron("sync", "svc-A") is True
    assert ("sync", "svc-A") in active_requests._defer_started_at
    assert ("sync", "svc-B") not in active_requests._defer_started_at

    assert should_defer_cron("sync", "svc-B") is True
    assert ("sync", "svc-B") in active_requests._defer_started_at

    # Distinct jobs on the same service are also independent.
    assert should_defer_cron("optimize", "svc-A") is True
    assert ("optimize", "svc-A") in active_requests._defer_started_at

    assert len(active_requests._defer_started_at) == 3


# ---------------------------------------------------------------------------
# yield_to_api — cooperative mid-tick yield
# ---------------------------------------------------------------------------


def test_yield_returns_immediately_when_no_active_requests():
    """Hot path: zero in-flight means zero sleep — must NOT call time.sleep."""
    assert active_request_count() == 0
    t0 = time.monotonic()
    slept = yield_to_api(max_wait_secs=1.0, poll_interval=0.05)
    elapsed = time.monotonic() - t0
    assert slept == 0.0
    assert elapsed < 0.02  # generous; the function should be essentially free


def test_yield_returns_early_when_requests_drain_during_sleep():
    """Spawn a thread that decrements after a short delay. yield_to_api must
    notice the count dropped and return before max_wait_secs expires."""
    increment_active_requests()

    def _drain_soon():
        time.sleep(0.1)
        decrement_active_requests()

    t = threading.Thread(target=_drain_soon)
    t.start()

    t0 = time.monotonic()
    slept = yield_to_api(max_wait_secs=2.0, poll_interval=0.05)
    elapsed = time.monotonic() - t0
    t.join()

    # Drained after ~100ms; helper should have returned shortly after that
    # — well under the 2 s max.
    assert elapsed < 0.5, f"helper waited {elapsed:.3f}s after drain"
    assert 0.05 <= slept < 0.5


def test_yield_caps_at_max_wait_secs_under_sustained_load():
    """Sustained in-flight requests must not stall sync forever. The helper
    returns after max_wait_secs even if count stays > 0."""
    increment_active_requests()
    try:
        t0 = time.monotonic()
        slept = yield_to_api(max_wait_secs=0.2, poll_interval=0.05)
        elapsed = time.monotonic() - t0
        # Bounded by max_wait_secs + one extra poll_interval slack.
        assert 0.15 <= elapsed < 0.4, f"helper waited {elapsed:.3f}s"
        # slept counter tracks the budget consumed; allow one extra tick.
        assert 0.15 <= slept <= 0.3
        # Counter was NOT touched — yield observes, doesn't mutate state.
        assert active_request_count() == 1
    finally:
        decrement_active_requests()


def test_yield_polls_at_configured_interval():
    """Verify the helper actually polls — i.e. uses small sleeps in a loop
    so an early drain is observed promptly, rather than one big sleep that
    blocks past the drain moment."""
    increment_active_requests()
    sleep_calls: list[float] = []

    def _tracking_sleep(secs: float) -> None:
        sleep_calls.append(secs)

    try:
        yield_to_api(max_wait_secs=0.2, poll_interval=0.05, sleep_fn=_tracking_sleep)
    finally:
        decrement_active_requests()

    assert len(sleep_calls) >= 2, f"expected multiple polls, got {sleep_calls}"
    assert all(s == 0.05 for s in sleep_calls), f"unexpected sleep durations: {sleep_calls}"


# ── Merged from test_sqlite_profiler.py ──────────────────────────────────────────────

import pytest

from backend.utils import sqlite_profiler


@pytest.fixture(autouse=True)
def _clear_buffer():
    """Every test starts with an empty ring buffer."""
    sqlite_profiler.clear()
    yield
    sqlite_profiler.clear()


# ── Statement recording ──────────────────────────────────────────────────────


def test_execute_captures_a_statement():
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.5, 0, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1
    assert snap["queries"][0]["sql"] == "CREATE TABLE t (x INTEGER)"
    assert snap["queries"][0]["op"] == "execute"
    assert snap["queries"][0]["time_ms"] >= 0
    assert snap["queries"][0]["seq"] >= 1


def test_executemany_captures_one_entry_with_seq_length():
    sqlite_profiler._record("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)], 1.2, 3, "executemany")
    snap = sqlite_profiler.get_recent()
    assert len(snap["queries"]) == 1
    assert snap["queries"][0]["op"] == "executemany"
    assert snap["queries"][0]["params_kind"] == "seq[3]"


def test_params_are_not_captured_as_values():
    """PII safety: params must never appear in the captured SQL or anywhere
    in the entry body. Only shape metadata."""
    secret = "user-pii-do-not-capture@example.com"
    sqlite_profiler._record("INSERT INTO t VALUES (?)", (secret,), 0.4, 1, "execute")
    snap = sqlite_profiler.get_recent()
    entry = snap["queries"][0]
    assert secret not in entry["sql"]
    assert all(secret not in str(v) for v in entry.values())


# ── Ring buffer mechanics ────────────────────────────────────────────────────


def test_ring_buffer_caps_at_max(monkeypatch):
    """Push past cap, verify buffer_size never exceeds cap and dropped grows."""
    monkeypatch.setattr(sqlite_profiler, "_buffer", type(sqlite_profiler._buffer)(maxlen=10))
    monkeypatch.setattr(sqlite_profiler, "_dropped", 0)
    for _ in range(25):
        sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 10
    assert snap["dropped"] >= 15


def test_since_seq_filters():
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    sqlite_profiler._record("SELECT 2", None, 0.1, 1, "execute")
    midpoint = sqlite_profiler.get_recent()["last_seq"]
    sqlite_profiler._record("SELECT 3", None, 0.1, 1, "execute")
    sqlite_profiler._record("SELECT 4", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent(since_seq=midpoint)
    sqls = [q["sql"] for q in snap["queries"]]
    assert "SELECT 3" in sqls
    assert "SELECT 4" in sqls
    assert "SELECT 1" not in sqls
    assert "SELECT 2" not in sqls


def test_limit_returns_most_recent():
    for i in range(20):
        sqlite_profiler._record(f"SELECT {i}", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent(limit=5)
    assert len(snap["queries"]) == 5
    sqls = [q["sql"] for q in snap["queries"]]
    assert sqls == [f"SELECT {i}" for i in range(15, 20)]


def test_clear_resets_buffer_but_not_seq():
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    last_seq_before = sqlite_profiler.get_recent()["last_seq"]
    sqlite_profiler.clear()
    snap_after_clear = sqlite_profiler.get_recent()
    assert snap_after_clear["buffer_size"] == 0
    assert snap_after_clear["last_seq"] == 0
    sqlite_profiler._record("SELECT 2", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["last_seq"] > last_seq_before


# ── End-to-end via get_con() ─────────────────────────────────────────────────


def test_get_con_returns_instrumented_connection():
    """metadata_db.get_con() must hand out PgConnectionWrapper under Postgres."""
    from backend.core.metadata import base, pg_connection

    con = base.get_con("test-svc")
    assert isinstance(con, pg_connection.PgConnectionWrapper)


def test_metadata_db_traffic_appears_in_buffer():
    """Sanity end-to-end: a real metadata_db call populates the ring buffer."""
    from backend.core.metadata import base

    con = base.get_con("test-svc-traffic")
    con.execute("SELECT 1")
    snap = sqlite_profiler.get_recent()
    sqls = [q["sql"] for q in snap["queries"]]
    assert any("SELECT 1" in s for s in sqls)


# ── Helper-function direct tests (cover the small surface) ──────────────────


def test_summarize_sql_truncates_long_strings():
    long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(1000))
    out = sqlite_profiler._summarize_sql(long_sql)
    assert "chars]" in out
    assert len(out) < len(long_sql)


def test_summarize_sql_coerces_non_strings():
    """Some callers pass bytes / int; the profiler must not raise."""
    out = sqlite_profiler._summarize_sql(b"SELECT 1")
    assert isinstance(out, str)
    assert "SELECT 1" in out
    assert sqlite_profiler._summarize_sql(42) == "42"


def test_describe_params_covers_each_shape():
    assert sqlite_profiler._describe_params(None) == "none"
    assert sqlite_profiler._describe_params([1, 2, 3]) == "seq[3]"
    assert sqlite_profiler._describe_params((1,)) == "seq[1]"
    assert sqlite_profiler._describe_params({"a": 1, "b": 2}) == "map[2]"
    assert sqlite_profiler._describe_params(42) == "int"


def test_record_swallows_internal_exceptions(monkeypatch):
    """The profiler's hard contract: any internal failure must never
    reach the caller's SQL path."""
    from unittest.mock import MagicMock

    bad = MagicMock()
    bad.append.side_effect = RuntimeError("buffer borked")
    monkeypatch.setattr(sqlite_profiler, "_buffer", bad)
    sqlite_profiler._record("SELECT 1", None, 0.5, 1, "execute")


def test_live_register_returns_sentinel_on_registry_failure(monkeypatch):
    """When the registry import / call raises, _live_register returns -1
    (a sentinel that _live_deregister treats as a no-op)."""
    from backend.core import query_registry

    monkeypatch.setattr(
        query_registry.query_registry,
        "register",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("registry down")),
    )
    qid = sqlite_profiler._live_register("SQLite", "SELECT 1", con=None)
    assert qid == -1


def test_live_deregister_is_noop_for_negative_qid():
    sqlite_profiler._live_deregister(-1, error=None)


def test_live_deregister_swallows_registry_errors(monkeypatch):
    from unittest.mock import MagicMock

    from backend.core import query_registry

    monkeypatch.setattr(
        query_registry.query_registry,
        "deregister",
        MagicMock(side_effect=RuntimeError("dereg failed")),
    )
    sqlite_profiler._live_deregister(42, error=None)


# ── Per-request collector (Debug Panel "This page" scoping) ──────────────────


@pytest.fixture()
def _request_collector():
    """Isolate the telemetry request-scoped SQLite collector per test —
    start_call_tracking() sets a ContextVar that would otherwise leak
    into subsequent tests running in the same context."""
    from backend.utils.telemetry import _SQLITE_QUERIES

    token = _SQLITE_QUERIES.set(None)
    yield
    _SQLITE_QUERIES.reset(token)


def test_statement_lands_in_request_collector_when_tracking_active(_request_collector):
    """The page-scoped Debug Panel view: a statement executed while a
    request is being tracked must land in BOTH the process-global ring
    buffer AND the request's contextvar collector."""
    from backend.utils import telemetry

    telemetry.start_call_tracking()
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.2, 0, "execute")

    entries = telemetry.get_sqlite_queries()
    assert len(entries) == 1
    assert entries[0]["sql"] == "CREATE TABLE t (x INTEGER)"
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1
    assert snap["queries"][0]["seq"] == entries[0]["seq"]


def test_statement_skips_request_collector_outside_tracking(_request_collector):
    """Cron/startup statements (no start_call_tracking) must NOT create or
    populate a request collector — only the ring buffer sees them."""
    from backend.utils.telemetry import _SQLITE_QUERIES

    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")

    assert _SQLITE_QUERIES.get() is None
    assert sqlite_profiler.get_recent()["buffer_size"] == 1


def test_request_collector_failure_does_not_break_sql_path(_request_collector, monkeypatch):
    """The profiler contract: observability failures never propagate into
    the calling SQL path. A broken record_sqlite_query must not raise."""
    from backend.utils import telemetry

    monkeypatch.setattr(
        telemetry,
        "record_sqlite_query",
        lambda entry: (_ for _ in ()).throw(RuntimeError("collector broken")),
    )
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")


# ── Merged from test_structlog_config.py ──────────────────────────────────────────────

import json
from unittest.mock import patch

import pytest
import structlog
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from backend.utils import structlog_config


@pytest.fixture(autouse=True)
def _reset_structlog():
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()


def test_configure_structlog_is_idempotent():
    structlog_config.configure_structlog()
    structlog_config.configure_structlog()
    # No exception. structlog.is_configured() should be True after.
    assert structlog.is_configured()


def test_otel_trace_processor_skips_when_no_active_span():
    """When no recording span is active, the processor leaves the event
    dict unchanged."""
    event_dict: dict = {"event": "test", "key": "value"}
    out = structlog_config._add_otel_trace_context(None, "info", event_dict)
    assert "trace_id" not in out
    assert "span_id" not in out


def test_otel_trace_processor_injects_trace_id_when_span_recording():
    """When a recording span is active, the processor adds 32-hex
    trace_id and 16-hex span_id keys."""
    provider = TracerProvider()
    tracer = provider.get_tracer("test")

    with patch.object(trace, "get_current_span", wraps=trace.get_current_span):
        with tracer.start_as_current_span("test-span"):
            event_dict: dict = {"event": "test"}
            out = structlog_config._add_otel_trace_context(None, "info", event_dict)

    assert "trace_id" in out
    assert "span_id" in out
    assert len(out["trace_id"]) == 32
    assert len(out["span_id"]) == 16
    # Both are hex.
    int(out["trace_id"], 16)
    int(out["span_id"], 16)


def test_configure_structlog_with_json_format(monkeypatch):
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()
    log = structlog.get_logger("json-test")
    # Calling .info() on a json-configured logger does not raise.
    log.info("event", k=1)


def test_bridge_uvicorn_loggers_repoints_them_at_root(monkeypatch, capsys):
    """uvicorn's loggers must propagate to the structlog root handler with no
    private handlers of their own, so their lines render as JSON (not uvicorn's
    plaintext ``INFO: …``) and carry the shared processor chain.

    Regression guard: without ``bridge_uvicorn_loggers`` the access log stays on
    uvicorn's own handler (propagate=False) and emits plaintext into an
    otherwise-JSON stream — see backend/utils/structlog_config.py.
    """
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()

    # Simulate uvicorn's default LOGGING_CONFIG: a private handler +
    # propagate=False on each uvicorn logger.
    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        lg.handlers = [logging.NullHandler()]
        lg.propagate = False

    structlog_config.bridge_uvicorn_loggers()

    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        assert lg.handlers == [], f"{name} should have no private handlers"
        assert lg.propagate is True, f"{name} should propagate to root"

    # End-to-end: an access-style record now renders through the root structlog
    # handler as JSON, not uvicorn's plaintext formatter.
    logging.getLogger("uvicorn.access").info('127.0.0.1 - "GET /x HTTP/1.1" 200')
    line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == '127.0.0.1 - "GET /x HTTP/1.1" 200'


def test_stdlib_extra_fields_are_rendered(monkeypatch, capsys):
    """Fields passed via stdlib ``logging``'s ``extra={...}`` must survive
    the bridge into structlog and appear in the rendered output.

    Regression guard: without ``structlog.stdlib.ExtraAdder`` on the
    foreign pre-chain these keys are silently dropped, collapsing every
    web_vitals metric to a bare ``web_vitals`` line (see
    backend/routers/web_vitals.py).
    """
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()

    logging.getLogger("backend.web_vitals").info(
        "web_vitals",
        extra={"web_vitals_name": "LCP", "web_vitals_rating": "good"},
    )

    line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "web_vitals"
    assert record["web_vitals_name"] == "LCP"
    assert record["web_vitals_rating"] == "good"


def test_console_exception_traceback_never_renders_local_secrets(monkeypatch, capsys):
    """A stdlib ``logger.warning(..., exc_info=True)`` call whose failing
    frame holds a secret-bearing local (e.g. a service ``cfg`` dict with FOS
    keys / a Fastly API token — see backend/cron/jobs/rum_sync.py's
    ``_reconcile_faro_bundle``) must never render those values into
    container logs.

    Regression guard: structlog's ``ConsoleRenderer`` defaults its
    exception formatter to ``rich.traceback`` with ``show_locals=True``
    (rich is a project dependency, so this fires in prod's default console
    format, not just when explicitly opted in). Every frame's local
    variables — including live secrets — used to get dumped verbatim into
    the traceback on any exception. ``configure_structlog`` must pin
    ``show_locals=False`` so this can't regress silently.
    """
    monkeypatch.delenv("STRUCTLOG_FORMAT", raising=False)  # console is the default
    structlog_config.configure_structlog()

    def _load_cfg():
        # Built separately from the raise site so the secret values are
        # only ever present as a LOCAL VARIABLE in the failing frame below —
        # never as a literal in the source line rich's code-context view
        # would echo back regardless of show_locals. That isolates this
        # test to the vulnerability class it's guarding: locals rendering.
        return {
            "fos_access_key_id": "AKIAFAKEEXAMPLE00000",
            "fos_secret_access_key": "fake-fos-secret-do-not-leak-00000000",
            "fastly_api_key": "fake-fastly-token-do-not-leak-11111111",
        }

    def _reconcile(cfg):
        if not cfg.get("nonexistent_marker"):
            raise RuntimeError("boom")

    try:
        _reconcile(_load_cfg())
    except RuntimeError:
        logging.getLogger("test.rum_sync").warning("Faro reconcile failed for svc-fake", exc_info=True)

    output = capsys.readouterr().err

    assert "AKIAFAKEEXAMPLE00000" not in output
    assert "fake-fos-secret-do-not-leak-00000000" not in output
    assert "fake-fastly-token-do-not-leak-11111111" not in output
    # The traceback itself, and the log message, must still render — this
    # isn't a test that exceptions stop being logged, only that locals don't.
    assert "RuntimeError" in output
    assert "Faro reconcile failed for svc-fake" in output


def test_uvicorn_color_message_extra_is_dropped(monkeypatch, capsys):
    """uvicorn attaches ``extra={"color_message": ...}`` (ANSI + ``%``-template)
    to its boot/access records for its own colorizer. ExtraAdder would surface
    it as a noisy ``color_message='...'`` kv pair on every uvicorn line; the
    bridge must strip it.

    Regression guard for the ``Uvicorn running on ... color_message='...'``
    line that showed up in container logs once uvicorn was bridged through
    structlog (see ``_drop_uvicorn_color_message``).
    """
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()

    logging.getLogger("uvicorn.error").info(
        "Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)",
        extra={"color_message": "Uvicorn running on \x1b[1m%s://%s:%d\x1b[0m (Press CTRL+C to quit)"},
    )

    line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)"
    assert "color_message" not in record


# ── Merged from test_system_jobs.py ──────────────────────────────────────────────


from backend.cron import decorators as system_jobs


def _reset() -> None:
    with system_jobs._status_lock:
        system_jobs._status.clear()


@pytest.fixture(autouse=True)
def _clear_system_jobs() -> None:
    _reset()


def test_initial_status_is_empty():
    assert system_jobs.get_system_job_status() == {}


def test_record_job_run_writes_snapshot():
    system_jobs.record_job_run("rdns", status="success", duration_s=1.234, detail="processed 12")
    snap = system_jobs.get_system_job_status()
    assert "rdns" in snap
    entry = snap["rdns"]
    assert entry["status"] == "success"
    assert entry["duration_s"] == 1.23  # rounded to 2dp
    assert entry["detail"] == "processed 12"
    assert entry["last_run_at"].endswith("Z")


def test_record_job_run_overwrites_previous():
    system_jobs.record_job_run("rdns", "success", 1.0)
    system_jobs.record_job_run("rdns", "error", 2.0, detail="boom")
    snap = system_jobs.get_system_job_status()
    assert snap["rdns"]["status"] == "error"
    assert snap["rdns"]["duration_s"] == 2.0
    assert snap["rdns"]["detail"] == "boom"


def test_get_returns_a_copy_not_live_state():
    """The returned dict must not let callers mutate the internal store."""
    system_jobs.record_job_run("rdns", "success", 0.5)
    snap = system_jobs.get_system_job_status()
    snap.pop("rdns")
    # Original store still has it
    assert "rdns" in system_jobs.get_system_job_status()


def test_last_run_at_format():
    with patch("backend.cron.decorators.iso_z_now", return_value="2026-05-15T12:30:45Z"):
        system_jobs.record_job_run("bots", "success", 0.1)
    assert system_jobs.get_system_job_status()["bots"]["last_run_at"] == "2026-05-15T12:30:45Z"


# ── Merged from test_testcontainers_smoke.py ──────────────────────────────────────────────

import shutil
import socket

import pytest


def _docker_available() -> bool:
    """Skip the test if Docker CLI isn't usable.

    Two failure modes: the binary isn't installed, or it's installed
    but the daemon isn't running (Docker Desktop closed on macOS).
    Both warrant a skip rather than a hard failure — testcontainers
    is opt-in and shouldn't gate the suite on local environment shape.
    """
    if shutil.which("docker") is None:
        return False
    # Attempt a connect to the daemon's default Unix socket on macOS /
    # Linux. We don't actually issue a Docker API call here — just
    # check the socket is reachable, since `testcontainers` itself will
    # do a richer probe on first use.
    for path in (
        "/var/run/docker.sock",
        "/Users/" + (__import__("os").environ.get("USER") or "") + "/.docker/run/docker.sock",
    ):
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect(path)
            s.close()
            return True
        except OSError:
            continue
    return False


@pytest.mark.skipif(
    not _docker_available(),
    reason="testcontainers requires a running Docker daemon",
)
def test_can_spawn_a_container_and_read_back_its_id():
    """Pin: testcontainers' DockerContainer lifecycle works in this
    repo's venv. The next integration test that needs a real container
    dep can crib this pattern."""
    pytest.importorskip("testcontainers.core.container")
    from testcontainers.core.container import DockerContainer

    with DockerContainer("alpine:3.20").with_command("sleep 60") as c:
        # The wrapped container exposes its id once started.
        cid = c.get_wrapped_container().id
        assert cid, "expected a docker container id once the container started"
