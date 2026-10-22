"""A request the client abandons must not leave its DuckDB queries running in worker threads.

Starlette cancels the handler coroutine when the browser disconnects (a reload), but the synchronous DuckDB work
already running in a thread keeps going, holding one of the few pool slots for as long as it takes (up to the 120 s
pool watchdog). Every reload on a slow query then adds another orphan until the pool is saturated.
"""

from __future__ import annotations

import asyncio
import contextvars

import pytest
from starlette.requests import Request

from backend.core.query_registry import query_registry


class _Conn:
    def __init__(self) -> None:
        self.interrupts = 0

    def interrupt(self) -> None:
        self.interrupts += 1


def _request(path: str = "/api/dashboard/bundle") -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "client": ("203.0.113.9", 4242),
        "server": ("testserver", 80),
        "scheme": "http",
        "state": {},
    }
    return Request(scope)


@pytest.fixture(autouse=True)
def _clean_registry():
    before = dict(query_registry._queries)
    before_keys = list(query_registry._cancelled_keys)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(before)
    query_registry._cancelled_keys.clear()
    query_registry._cancelled_keys.extend(before_keys)


def test_a_cancelled_request_interrupts_the_queries_it_started():
    from backend.main import telemetry_middleware

    conn = _Conn()

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT slow()", service_id="svc", con=conn, pool_slot="svc#1")
        raise asyncio.CancelledError  # what Starlette does to the handler when the client disconnects

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(telemetry_middleware(_request(), call_next))

    assert conn.interrupts == 1


def test_a_request_that_completes_normally_interrupts_nothing():
    from backend.main import telemetry_middleware

    conn = _Conn()

    class _Response:
        status_code = 200

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT 1", service_id="svc", con=conn, pool_slot="svc#1")
        return _Response()

    asyncio.run(telemetry_middleware(_request(), call_next))

    assert conn.interrupts == 0


def test_a_cancelled_request_never_interrupts_another_requests_queries():
    from backend.main import telemetry_middleware

    mine, theirs = _Conn(), _Conn()
    started = asyncio.Event
    other_registered: list[int] = []

    async def other_request_query():
        # A concurrent request registers its own query under its own attribution.
        async def inner(request):
            qid = query_registry.register("DuckDB", "SELECT other()", service_id="svc", con=theirs, pool_slot="svc#2")
            other_registered.append(qid)

            class _R:
                status_code = 200

            return _R()

        await telemetry_middleware(_request("/api/network-health"), inner)

    async def scenario():
        await other_request_query()

        async def call_next(request):
            query_registry.register("DuckDB", "SELECT mine()", service_id="svc", con=mine, pool_slot="svc#1")
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await telemetry_middleware(_request(), call_next)

    del started
    asyncio.run(scenario())

    assert mine.interrupts == 1
    assert theirs.interrupts == 0


def test_a_real_duckdb_query_is_killed_when_its_request_is_abandoned():
    """End to end: real DuckDB statement in a worker thread, real telemetry middleware, request task cancelled."""
    import threading
    import time

    import duckdb
    import httpx
    from fastapi import FastAPI

    from backend.core.query_instrumentation import InstrumentedDuckDBConnection
    from backend.main import telemetry_middleware

    app = FastAPI()
    app.middleware("http")(telemetry_middleware)
    outcome: dict[str, object] = {}
    started = threading.Event()
    finished = threading.Event()

    @app.get("/slow")
    async def slow():
        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="svc")
        # Safety net so a regression fails the assertion below instead of hanging the suite.
        safety = threading.Timer(15, raw.interrupt)
        safety.daemon = True
        safety.start()

        def run() -> None:
            # Runs through asyncio.to_thread, like production work, so it inherits the request's context.
            started.set()
            t0 = time.monotonic()
            try:
                con.execute("SELECT count(*) FROM range(100000000000) a, range(100000000000) b").fetchall()
                outcome["result"] = "finished"
            except Exception as exc:  # noqa: BLE001
                outcome["result"] = type(exc).__name__
            finally:
                outcome["seconds"] = time.monotonic() - t0
                safety.cancel()
                finished.set()

        asyncio.get_running_loop().run_in_executor(None, contextvars.copy_context().run, run)
        await asyncio.sleep(3600)

    async def scenario() -> None:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            request_task = asyncio.create_task(client.get("/slow"))
            deadline = time.monotonic() + 10
            while not started.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.3)  # let the statement actually start inside DuckDB
            request_task.cancel()  # the browser reloaded: Starlette cancels the handler
            with pytest.raises(asyncio.CancelledError):
                await request_task
            await asyncio.get_running_loop().run_in_executor(None, finished.wait, 12)

    asyncio.run(scenario())

    assert finished.is_set(), "the orphaned query must be interrupted, not left to run to completion"
    assert outcome["result"] == "InterruptException"
    assert outcome["seconds"] < 8  # type: ignore[operator]


def test_tcp_disconnect_via_asgi_receive_interrupts_sync_duckdb_query():
    """Uvicorn signals client TCP disconnect via the ASGI receive channel (request.is_disconnected())
    rather than cancelling the handler coroutine directly when a sync route is running in a threadpool.
    Verify _watch_disconnect() polls request.is_disconnected() and interrupts the query."""
    from backend.main import telemetry_middleware

    conn = _Conn()
    disconnected = False

    async def receive():
        nonlocal disconnected
        if disconnected:
            return {"type": "http.disconnect"}
        await asyncio.sleep(0.05)
        return {"type": "http.disconnect"}

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/network-health",
        "raw_path": b"/api/network-health",
        "query_string": b"",
        "headers": [],
        "client": ("203.0.113.9", 4242),
        "server": ("testserver", 80),
        "scheme": "http",
        "state": {},
    }
    req = Request(scope, receive=receive)
    req._stream_consumed = True  # type: ignore[attr-defined]

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT slow_post()", service_id="svc", con=conn, pool_slot="svc#1")
        nonlocal disconnected
        disconnected = True
        for _ in range(20):
            if conn.interrupts > 0:
                break
            await asyncio.sleep(0.05)

        class _R:
            status_code = 499

        return _R()

    asyncio.run(telemetry_middleware(req, call_next))
    assert conn.interrupts == 1


def test_cancelled_key_blocks_subsequent_queries_but_allows_drop_table_cleanup():
    """Once cancel_for_key(cancel_key) has fired, any subsequent query registered under the same
    cancel_key must raise duckdb.InterruptException immediately, EXCEPT DROP TABLE cleanup statements."""
    import duckdb

    from backend.core.query_instrumentation import InstrumentedDuckDBConnection
    from backend.core.query_registry import Attribution, current_attribution

    raw = duckdb.connect(":memory:")
    con = InstrumentedDuckDBConnection(raw, service_id="svc")
    con.execute("CREATE TEMP TABLE tmp_cleanup AS SELECT 1 AS x")

    token = current_attribution.set(
        Attribution(
            kind="admin", admin_id="local", request_path="/api/security/aggregates", cancel_key="ck-multi-query"
        )
    )
    try:
        query_registry.cancel_for_key("ck-multi-query")
        with pytest.raises(duckdb.InterruptException):
            con.execute("SELECT * FROM tmp_cleanup").fetchall()

        # Cleanup DROP statements in finally: blocks must still be allowed to run
        con.execute('DROP TABLE IF EXISTS "tmp_cleanup"')
    finally:
        current_attribution.reset(token)
        raw.close()


def test_query_runner_lazy_con_factory_defers_connection_checkout():
    """QueryRunner(con_factory, src) must not call con_factory() until a SQL statement actually executes."""
    import duckdb

    from backend.repositories._base import QueryRunner

    calls = 0
    raw = duckdb.connect(":memory:")

    def factory():
        nonlocal calls
        calls += 1
        return raw

    try:
        runner = QueryRunner(factory, {"name": "test_svc"})
        assert calls == 0, "con_factory must not be called on QueryRunner construction"
        # Rollup eligibility check that returns None early without touching DuckDB
        res = runner.try_network_rtt_from_rollup(
            "2026-01-01T00:00:00Z",
            "2026-01-01T01:00:00Z",
            top_asns=[15169],
            has_filters=True,
        )
        assert res is None
        assert calls == 0, "con_factory must not be called when rollup eligibility returns early"

        # Executing SQL triggers lazy connection acquisition exactly once
        assert runner.execute("SELECT 42").fetchone()[0] == 42
        assert calls == 1
        assert runner.execute("SELECT 43").fetchone()[0] == 43
        assert calls == 1
    finally:
        raw.close()
