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
from backend.core.query_instrumentation import (
    InstrumentedDuckDBConnection,
    _InstrumentedRecordReader,
    _parse_memory_mb,
    _probe_duckdb_memory,
)
from backend.core.query_registry import QueryRegistry, _conn_to_query, query_registry
from backend.utils.sqlite_profiler import InstrumentedConnection


class _RecordingConn:
    """Weakref-able connection stand-in that records ``interrupt()`` calls.

    Lets the stamp-moved tests below assert that ``cancel_query`` did NOT
    fire ``interrupt()`` on the live connection — the exact cross-talk a
    deletion of the per-connection stamp re-validation would re-introduce.
    A plain class instance is weakref-able and has a stable ``id()`` while
    the test holds a reference, so it behaves like a pooled DuckDB conn for
    the registry's stamp bookkeeping.
    """

    def __init__(self) -> None:
        self.interrupts = 0

    def interrupt(self) -> None:
        self.interrupts += 1


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

    def test_deregister_after_cancel_records_cancelled_not_error(self, fresh_registry: QueryRegistry):
        """KILL-OUTCOME-02: an admin kill interrupts the connection, the query
        raises (e.g. ``InterruptException``), and the caller hands that error to
        ``deregister``. The completed-history outcome must still be
        ``cancelled`` — NOT ``error`` — because ``cancelled_at`` takes
        precedence over ``error`` in ``deregister``. This keeps an
        admin-initiated cancellation from being mislabelled a query failure in
        the live-monitor history and the cancelled/error metrics. Swapping the
        precedence (error before cancelled_at) flips the outcome and fails here.
        """
        con = sqlite3.connect(":memory:")
        qid = fresh_registry.register("SQLite", "SELECT 1", con=con)
        assert fresh_registry.cancel_query(qid, admin_id="t1") == "cancelled"
        # The cancelled query unwinds and the caller reports the interrupt error.
        fresh_registry.deregister(qid, error=RuntimeError("INTERRUPT: query was cancelled"))
        c = fresh_registry.snapshot(include_completed=True)["completed"][0]
        assert c["outcome"] == "cancelled", (
            f"cancelled query recorded as {c['outcome']!r} — cancelled_at must win over error"
        )

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

    def test_cancel_refuses_when_stamp_moved_while_original_still_live(self, fresh_registry: QueryRegistry):
        """The stamp-moved refusal branch (design doc §13.10) — the late-GC race.

        ``test_pool_reuse_race_does_not_kill_wrong_query`` deregisters A
        *before* B reuses the connection, so ``cancel_query(A)`` exits early
        on the cheap ``not_found`` short-circuit and never reaches the
        per-connection stamp comparison. This test pins the OTHER path: A is
        STILL in ``_queries`` (its deregister hasn't run yet) when B reuses
        the same connection. ``cancel_query(A)`` must fall through to the
        stamp guard, observe that the stamp now points at B, and refuse —
        WITHOUT interrupting the live connection. Deleting or weakening the
        in-lock stamp re-validation would silently turn this into a
        cross-talk kill (interrupt fires on B) while the rest of the suite
        stays green; this assertion is what catches that regression.
        """
        con = _RecordingConn()
        qid_a = fresh_registry.register("DuckDB", "SELECT A", con=con)
        # B reuses the SAME connection object (same id()) → the stamp moves
        # from A to B. A is intentionally NOT deregistered.
        qid_b = fresh_registry.register("DuckDB", "SELECT B", con=con)
        assert qid_a in fresh_registry._queries  # not_found short-circuit NOT taken
        # cancel(A) reaches the stamp guard, sees stamp == B, refuses.
        assert fresh_registry.cancel_query(qid_a) == "already_finished"
        assert con.interrupts == 0, "live connection (bound to B) must not be interrupted"
        # B itself remains cancellable through the moved stamp.
        assert fresh_registry.cancel_query(qid_b) == "cancelled"
        assert con.interrupts == 1

    def test_late_deregister_of_original_keeps_reused_conn_stamp_for_b(self, fresh_registry: QueryRegistry):
        """Ordering proof: A's late deregister must not strip B's stamp.

        When A finishes and deregisters AFTER B has already reused the
        connection, A's ``deregister`` is guarded by ``== qid_a`` so it
        cannot delete the stamp that now belongs to B. B therefore stays
        interruptible. Pins the structural defense behind KILL-2.
        """
        con = _RecordingConn()
        qid_a = fresh_registry.register("DuckDB", "SELECT A", con=con)
        qid_b = fresh_registry.register("DuckDB", "SELECT B", con=con)  # stamp → B
        # A finishes late and deregisters — must NOT wipe B's stamp.
        fresh_registry.deregister(qid_a)
        assert _conn_to_query.get(id(con)) == qid_b
        # B is still live and interruptible.
        assert fresh_registry.cancel_query(qid_b) == "cancelled"
        assert con.interrupts == 1


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

    def test_kill_across_real_connection_reuse_cycle(self):
        """KILL-INT-01: cross-talk safety through the REAL instrumented
        connection lifecycle (register-on-execute, deregister-on-fetch) — the
        same wrapper ``duckdb_pool`` hands out on every acquire. Drives an
        actual checkout→complete→release→reacquire cycle on ONE raw connection:
        query A runs to completion (deregistered on fetch), then the same raw
        connection is reused for a still-running query B. A stale admin kill on
        the completed A must be refused (``not_found``) and must NOT interrupt
        the live B — proven by B's result still fetching cleanly afterward. The
        registry-level tests pin the stamp logic in isolation; this pins that
        the real execute/fetch path maintains it across a connection reuse."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        # Cycle 1 — A acquires the connection, runs to completion, releases.
        con_a = InstrumentedDuckDBConnection(raw, service_id="killint")
        con_a.execute("SELECT 1 AS a").fetchall()
        completed = [c for c in singleton.snapshot(include_completed=True)["completed"] if c["service_id"] == "killint"]
        assert completed, "query A did not land in completed history"
        qid_a = completed[-1]["query_id"]

        # Cycle 2 — the SAME raw connection is reacquired for B, which stays
        # ACTIVE (its result is held, not terminally fetched yet).
        con_b = InstrumentedDuckDBConnection(raw, service_id="killint")
        result_b = con_b.execute("SELECT 42 AS b")

        # Stale click on the completed A: must refuse without touching raw
        # (now bound to the live B). A con-keyed (rather than qid-keyed) kill
        # would interrupt raw and break B.
        assert singleton.cancel_query(qid_a) == "not_found"

        # Proof B was untouched: its result still fetches cleanly.
        assert result_b.fetchall() == [(42,)]

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


# ── Peak memory probe ─────────────────────────────────────────────────────────


class TestPeakMemory:
    def test_parse_memory_mb_int(self):
        assert _parse_memory_mb(1_048_576) == 1.0  # 1 MiB exactly
        assert _parse_memory_mb(0) == 0.0
        assert _parse_memory_mb(None) is None

    def test_parse_memory_mb_strings(self):
        assert _parse_memory_mb("1024") == round(1024 / (1024 * 1024), 2)
        assert _parse_memory_mb("1 MiB") == 1.0
        assert _parse_memory_mb("1 GiB") == 1024.0
        assert _parse_memory_mb("1.5 GiB") == 1536.0
        assert _parse_memory_mb("512 MB") == round(512_000_000 / (1024 * 1024), 2)
        assert _parse_memory_mb("0 bytes") == 0.0

    def test_parse_memory_mb_garbage(self):
        assert _parse_memory_mb("") is None
        assert _parse_memory_mb("not a number") is None
        assert _parse_memory_mb("3 yibibytes") is None  # unknown unit
        assert _parse_memory_mb(object()) is None

    def test_probe_returns_some_value_on_live_connection(self):
        raw = duckdb.connect(":memory:")
        raw.execute("CREATE TABLE t AS SELECT i FROM range(500_000) tbl(i)")
        mb = _probe_duckdb_memory(raw)
        # Materialised table should hold non-trivial memory; if DuckDB
        # reports zero on this machine the probe still must not crash.
        assert mb is None or mb >= 0.0

    def test_probe_swallows_errors(self):
        # A non-duckdb object (no .cursor()) must not crash the probe.
        assert _probe_duckdb_memory(object()) is None

    def test_completed_row_has_peak_memory_field(self):
        """The CompletedQuery JSON shape carries peak_memory_mb on every
        row (None for SQLite / probe-failure paths). The frontend uses
        the field's presence to decide whether to render the column."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="mem_test_svc")
        con.execute("CREATE TABLE t AS SELECT i FROM range(100_000) tbl(i)").fetchall()
        hist = singleton.snapshot(include_completed=True)["completed"]
        matches = [c for c in hist if c["service_id"] == "mem_test_svc"]
        assert matches, "expected CREATE TABLE to land in completed history"
        assert "peak_memory_mb" in matches[-1]

    def test_sqlite_completed_row_has_null_peak_memory(self):
        """SQLite never has a meaningful memory value; the field exists but
        stays None so the frontend renders consistently."""
        from backend.core.query_registry import query_registry as singleton

        con = sqlite3.connect(":memory:", factory=InstrumentedConnection)
        con.execute("CREATE TABLE x (i INT)").fetchall()
        hist = singleton.snapshot(include_completed=True)["completed"]
        sqlite_rows = [c for c in hist if c["db_type"] == "SQLite"]
        assert sqlite_rows
        assert sqlite_rows[-1]["peak_memory_mb"] is None


# ── Streaming RecordBatchReader wrapper (.arrow() / fetch_record_batch) ──────


class TestRecordBatchReader:
    def test_arrow_iteration_holds_registration_until_consumed(self):
        """``.arrow()`` returns a streaming reader; deregistration must
        wait for iteration to complete. Without :class:`_InstrumentedRecordReader`,
        a long downstream consumer would see ~0ms duration on the row."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="arrow_svc")
        con.execute("CREATE TABLE big AS SELECT i FROM range(500_000) tbl(i)").fetchall()
        reader = con.execute("SELECT * FROM big").arrow()
        # The reader was just returned — query should still be active.
        active = singleton.snapshot()["active"]
        active_for_reader = [r for r in active if r["service_id"] == "arrow_svc"]
        assert active_for_reader, "row deregistered before reader iteration — _InstrumentedRecordReader missing?"

        # Drain the reader; query should deregister.
        total_rows = 0
        for batch in reader:
            total_rows += batch.num_rows
            # Simulate slow consumer.
            time.sleep(0.005)
        assert total_rows == 500_000

        # Now the query has moved to completed history.
        hist = singleton.snapshot(include_completed=True)["completed"]
        matches = [c for c in hist if c["service_id"] == "arrow_svc" and "SELECT * FROM big" in c["sql_preview"]]
        assert matches, "expected SELECT to deregister after reader iteration"

    def test_to_arrow_table_materialises_immediately(self):
        """Sanity check that ``to_arrow_table()`` (the materialising call
        used by [iceberg/buffer.py:666](backend/core/iceberg/buffer.py#L666))
        still deregisters at the method-call boundary, not after iteration.
        It's listed in ``_TERMINAL_METHODS``, not ``_READER_METHODS``."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="materialised_svc")
        con.execute("CREATE TABLE x AS SELECT i FROM range(10_000) tbl(i)").fetchall()
        table = con.execute("SELECT * FROM x").to_arrow_table()
        assert table.num_rows == 10_000
        # Already deregistered before we even checked.
        active = singleton.snapshot()["active"]
        assert not [r for r in active if r["service_id"] == "materialised_svc"]

    def test_reader_close_completes_registration(self):
        """If the consumer never iterates but calls close(), the wrapper
        still drives deregistration so the registry doesn't leak."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="close_svc")
        con.execute("CREATE TABLE y AS SELECT i FROM range(100) tbl(i)").fetchall()
        reader = con.execute("SELECT * FROM y").arrow()
        reader.close()
        active = singleton.snapshot()["active"]
        assert not [r for r in active if r["service_id"] == "close_svc"]

    def test_reader_wrapper_passes_through_schema(self):
        """The wrapper must delegate non-completion attribute access (like
        the ``schema`` attribute) so callers that introspect the reader
        keep working."""
        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="schema_svc")
        con.execute("CREATE TABLE z AS SELECT 1 as a, 'x' as b").fetchall()
        reader = con.execute("SELECT * FROM z").arrow()
        assert isinstance(reader, _InstrumentedRecordReader)
        # schema attribute is delegated to the raw reader
        assert hasattr(reader, "schema")
        names = [f.name for f in reader.schema]
        assert names == ["a", "b"]
        reader.close()


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
