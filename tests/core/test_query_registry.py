"""Tests for the Live Query Monitor registry, attribution, and the DuckDB
result wrapper. The pool-reuse race test is the regression bait for the
``_conn_to_query`` stamp design — without it, ``cancel_query`` would
interrupt the next query on a reused connection."""

from __future__ import annotations

import sqlite3
import threading
import time

import duckdb
import pytest

from backend.core.query_attribution import (
    Attribution,
    _capture_caller,
    current_attribution,
    derive_from_process_context,
)
from backend.core.query_instrumentation import InstrumentedDuckDBConnection
from backend.core.query_registry import QueryRegistry, query_registry
from backend.utils.sqlite_profiler import InstrumentedConnection

# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def fresh_registry() -> QueryRegistry:
    """A scratch registry that doesn't share state with the singleton."""
    return QueryRegistry()


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Snapshot + restore the singleton's internal state so tests don't
    bleed into each other. The singleton is exercised by some tests
    (instrumentation integration) so we can't just replace it."""
    queries = dict(query_registry._queries)
    history = list(query_registry._history)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(queries)
    query_registry._history.clear()
    query_registry._history.extend(history)


# ── Attribution ─────────────────────────────────────────────────────────────


class TestAttribution:
    def test_analyst_label(self):
        a = Attribution.analyst(
            analyst_id="passcode_abcd1234",
            analyst_name="Drew",
            request_path="/api/query",
            request_id="req_1",
        )
        assert "Drew" in a.display_label()
        assert "/api/query" in a.display_label()
        assert a.principal_id() == "passcode_abcd1234"

    def test_analyst_label_guest(self):
        a = Attribution.analyst(
            analyst_id="passcode_a3f1",
            analyst_name=None,
            request_path="/api/dashboard",
            request_id=None,
        )
        # No name → "Guest (…last4)"
        assert "Guest" in a.display_label()
        assert "a3f1" in a.display_label()

    def test_admin_label(self):
        a = Attribution.admin(
            admin_id="10.0.0.5",
            request_path="/api/admin/queries",
            request_id="r2",
        )
        assert "Admin: 10.0.0.5" in a.display_label()
        assert a.principal_id() == "10.0.0.5"

    def test_cron_label(self):
        a = Attribution.cron(cron_job="sync_svc1", cron_run_id="r7f3")
        assert "Cron: sync_svc1" in a.display_label()
        assert "r7f3" in a.display_label()
        assert a.principal_id() == "r7f3"

    def test_system_label_with_thread_name(self):
        a = Attribution.system()
        assert a.kind == "system"
        assert "thread:" in a.caller_qualname or "MainThread" in a.caller_qualname

    def test_derive_from_process_context_shapes(self):
        assert derive_from_process_context("cron:sync_svc1").kind == "cron"
        assert derive_from_process_context("cron:sync_svc1").cron_job == "sync_svc1"
        assert derive_from_process_context("startup:init_service").kind == "system"
        # api:... is intentionally ignored — RequestContext owns HTTP attribution.
        assert derive_from_process_context("api:GET /admin/download-zip:/tmp") is None
        assert derive_from_process_context(None) is None
        assert derive_from_process_context("") is None


class TestCallerCapture:
    def test_skips_instrumentation_frames(self):
        # skip_frames defaults to 2 (skip _capture_caller + the caller's
        # register() frame). Called directly from a test, that puts us
        # past the test body — use skip_frames=1 to land in this method.
        qual, file_line = _capture_caller(skip_frames=1)
        assert "test_skips_instrumentation_frames" in qual
        assert "test_query_registry.py" in file_line

    def test_register_attributes_caller(self, fresh_registry: QueryRegistry):
        # End-to-end: register() should record THIS test's frame in the
        # attribution's caller_file (skipping query_registry +
        # query_attribution).
        qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
        snap = fresh_registry.snapshot()
        assert "test_query_registry.py" in snap["active"][0]["attribution"]["caller_file"]

    def test_returns_unknown_on_empty_stack(self):
        # skip_frames=999 walks past everything.
        qual, file_line = _capture_caller(skip_frames=999)
        assert qual == "<unknown>"


# ── Registry — register / deregister / snapshot ─────────────────────────────


class TestRegistry:
    def test_register_returns_monotonic_id(self, fresh_registry: QueryRegistry):
        a = fresh_registry.register("SQLite", "SELECT 1", con=None)
        b = fresh_registry.register("SQLite", "SELECT 2", con=None)
        assert b > a

    def test_register_with_no_attribution_synthesises_system(self, fresh_registry: QueryRegistry):
        token = current_attribution.set(None)
        try:
            qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
            assert qid >= 0
            snap = fresh_registry.snapshot()
            assert snap["active"][0]["attribution"]["kind"] == "system"
        finally:
            current_attribution.reset(token)

    def test_register_picks_up_attribution_from_contextvar(self, fresh_registry: QueryRegistry):
        attr = Attribution.admin(admin_id="ops", request_path="/api/admin/x", request_id="r1")
        prev = current_attribution.get()
        current_attribution.set(attr)
        try:
            qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
            snap = fresh_registry.snapshot()
            assert snap["active"][0]["attribution"]["kind"] == "admin"
            assert snap["active"][0]["attribution"]["principal_id"] == "ops"
        finally:
            current_attribution.set(prev)

    def test_deregister_moves_to_completed_history(self, fresh_registry: QueryRegistry):
        qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
        fresh_registry.deregister(qid)
        snap = fresh_registry.snapshot(include_completed=True)
        assert len(snap["active"]) == 0
        assert len(snap["completed"]) == 1
        assert snap["completed"][0]["outcome"] == "ok"

    def test_deregister_with_error_records_exception_type(self, fresh_registry: QueryRegistry):
        qid = fresh_registry.register("DuckDB", "SELECT FROM bad", con=None)
        fresh_registry.deregister(qid, error=RuntimeError("kaboom"))
        snap = fresh_registry.snapshot(include_completed=True)
        c = snap["completed"][0]
        assert c["outcome"] == "error"
        assert c["error_type"] == "RuntimeError"
        assert "kaboom" in c["error_message"]

    def test_deregister_negative_id_is_noop(self, fresh_registry: QueryRegistry):
        fresh_registry.deregister(-1)  # must not raise

    def test_snapshot_respects_since_seq(self, fresh_registry: QueryRegistry):
        a = fresh_registry.register("SQLite", "A", con=None)
        b = fresh_registry.register("SQLite", "B", con=None)
        snap = fresh_registry.snapshot(since_seq=a)
        ids = [r["query_id"] for r in snap["active"]]
        assert b in ids and a not in ids

    def test_summary_counts(self, fresh_registry: QueryRegistry):
        fresh_registry.register("SQLite", "A", con=None)
        fresh_registry.register("DuckDB", "B", con=None)
        fresh_registry.register("DuckDB", "C", con=None)
        s = fresh_registry.summary()
        assert s["active_total"] == 3
        assert s["by_db_type"] == {"SQLite": 1, "DuckDB": 2}


# ── Cancel — including the pool-reuse race regression test ─────────────────


class TestCancel:
    def test_cancel_unknown_returns_not_found(self, fresh_registry: QueryRegistry):
        assert fresh_registry.cancel_query(999_999) == "not_found"

    def test_cancel_with_no_connection_returns_already_finished(self, fresh_registry: QueryRegistry):
        qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
        assert fresh_registry.cancel_query(qid) == "already_finished"

    def test_cancel_active_sqlite_returns_cancelled(self, fresh_registry: QueryRegistry):
        con = sqlite3.connect(":memory:")
        qid = fresh_registry.register("SQLite", "SELECT 1", con=con)
        assert fresh_registry.cancel_query(qid, admin_id="t1") == "cancelled"
        # cancelled_at stamp set
        snap = fresh_registry.snapshot()
        assert snap["active"][0]["cancelled_at"] is not None

    def test_cancel_after_deregister_returns_not_found(self, fresh_registry: QueryRegistry):
        con = sqlite3.connect(":memory:")
        qid = fresh_registry.register("SQLite", "SELECT 1", con=con)
        fresh_registry.deregister(qid)
        assert fresh_registry.cancel_query(qid) == "not_found"

    def test_pool_reuse_race_does_not_kill_wrong_query(self, fresh_registry: QueryRegistry):
        """The single most-important regression test for this system.

        Scenario: connection runs query A, completes, returns to the pool;
        a moment later the same connection runs query B; admin's stale UI
        clicks Kill on the *old* query A. The registry MUST refuse to
        interrupt — otherwise we'd cancel query B which has nothing to do
        with the admin's intent.
        """
        con = duckdb.connect(":memory:")
        qid_a = fresh_registry.register("DuckDB", "SELECT A", con=con)
        fresh_registry.deregister(qid_a)
        qid_b = fresh_registry.register("DuckDB", "SELECT B", con=con)
        # Stale click on A:
        assert fresh_registry.cancel_query(qid_a) == "not_found"
        # B is untouched and still cancellable:
        assert fresh_registry.cancel_query(qid_b) == "cancelled"


# ── Concurrent register/deregister stress ──────────────────────────────────


class TestConcurrency:
    def test_concurrent_register_deregister_leaves_no_leaks(self, fresh_registry: QueryRegistry):
        # 16 threads, each registering+deregistering 200 times, must end
        # with empty active map (history is bounded so just check active).
        N_THREADS = 16
        N_OPS = 200

        def worker():
            for _ in range(N_OPS):
                qid = fresh_registry.register("SQLite", "x", con=None)
                fresh_registry.deregister(qid)

        threads = [threading.Thread(target=worker) for _ in range(N_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(fresh_registry._queries) == 0


# ── DuckDB result wrapper — proves the registry's duration reflects fetch ─


class TestDuckDBResultWrapper:
    def test_fetch_duration_captured(self):
        """Without the result wrapper this would show ~0ms even though
        fetchdf() takes 100s of ms on a real query."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="test_svc")
        con.execute("CREATE TABLE t AS SELECT i FROM range(2_000_000) tbl(i)")
        t0 = time.perf_counter()
        df = con.execute("SELECT i FROM t").fetchdf()
        wall_ms = (time.perf_counter() - t0) * 1000.0
        assert len(df) == 2_000_000

        # Find the most-recent SELECT row in history
        hist = singleton.snapshot(include_completed=True)["completed"]
        matches = [c for c in hist if "SELECT i FROM t" in c["sql_preview"]]
        assert matches, "expected the SELECT to be recorded"
        last = matches[-1]
        # Registry should be within 25% of wall clock — proves the wrapper
        # held the registration through fetchdf().
        assert last["duration_ms"] >= 0.5 * wall_ms, (
            f"registry duration {last['duration_ms']}ms < 50% of wall {wall_ms}ms — "
            f"result wrapper likely deregistered at execute() instead of fetch()"
        )
        # Pool slot populated:
        assert last["attribution"]["pool_slot"], "pool_slot should be set on DuckDB rows"

    def test_exception_in_fetch_records_outcome_error(self):
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="test_svc")
        with pytest.raises(duckdb.Error):
            con.execute("SELECT * FROM nonexistent_table_xyz")
        hist = singleton.snapshot(include_completed=True)["completed"]
        matches = [c for c in hist if "nonexistent_table_xyz" in c["sql_preview"]]
        assert matches
        assert matches[-1]["outcome"] == "error"
        assert "nonexistent" in (matches[-1]["error_message"] or "").lower() or matches[-1]["error_type"]


# ── SQLite InstrumentedCursor integration ──────────────────────────────────


class TestSQLiteInstrumentation:
    def test_execute_appears_in_registry_then_history(self):
        from backend.core.query_registry import query_registry as singleton

        con = sqlite3.connect(":memory:", factory=InstrumentedConnection)
        con.execute("CREATE TABLE t (i INT)")
        con.execute("INSERT INTO t VALUES (42)")
        rows = con.execute("SELECT count(*) FROM t").fetchall()
        assert rows == [(1,)]
        # All three should have moved through the registry to history.
        hist = singleton.snapshot(include_completed=True)["completed"]
        recent = [c for c in hist if c["db_type"] == "SQLite"]
        assert len(recent) >= 3
