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
    yield
    query_registry._queries.clear()
    query_registry._queries.update(before)


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
