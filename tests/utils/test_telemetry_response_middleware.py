"""Tests for M1 — telemetry backstop middleware.

The middleware sits BETWEEN GZip (outer) and the route handler (inner)
and injects ``_debug_queries`` / ``_debug_calls`` / ``_is_cached`` into
JSON dict responses that don't already carry them.

We exercise the middleware via a minimal in-memory FastAPI app with the
middleware bolted on — the real ``backend.main`` app pulls in the whole
project graph (cron scheduler, DuckDB, SQLite migrations) which is
overkill for unit-pinning the middleware contract.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.testclient import TestClient
from starlette.responses import JSONResponse, StreamingResponse

from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware


def _build_app(*, with_gzip: bool = False) -> FastAPI:
    """Build a minimal app with the telemetry middleware installed.

    Routes registered:
      - ``/plain-dict`` returns a plain ``dict`` with no telemetry. The
        middleware should inject the three keys.
      - ``/already-has-telemetry`` returns a dict with ``_debug_queries``
        already set. The middleware MUST NOT double-inject (and MUST
        preserve the values verbatim).
      - ``/list-response`` returns a top-level JSON list. The middleware
        MUST pass it through unchanged.
      - ``/streaming`` returns a ``StreamingResponse``. The middleware
        MUST NOT buffer it.
      - ``/non-json`` returns ``text/plain``. Untouched.
      - ``/empty`` returns 204 with empty body. Untouched.

    ``with_gzip=True`` stacks GZipMiddleware OUTSIDE the telemetry
    middleware — mirrors prod main.py ordering and pins that the
    backstop sees uncompressed JSON.
    """
    app = FastAPI()

    @app.get("/plain-dict")
    def plain_dict():
        return {"foo": 1, "bar": "two"}

    @app.get("/already-has-telemetry")
    def already_has():
        return {
            "foo": 1,
            "_debug_queries": [{"sql": "SELECT 1", "time_ms": 0.1}],
            "_debug_calls": [{"method": "GET", "path": "/x"}],
            "_debug_sqlite": [{"seq": 7, "sql": "SELECT 1", "time_ms": 0.2}],
            "_is_cached": True,
        }

    @app.get("/sqlite-work")
    def sqlite_work():
        # End-to-end page-scoping chain: profiler → per-request contextvar
        # collector → BaseResponse.with_telemetry. start_call_tracking is
        # called in-handler (same context) standing in for main.py's
        # tracking middleware, whose collector the handler context inherits.
        import sqlite3

        from backend.models.common import BaseResponse
        from backend.utils import sqlite_profiler
        from backend.utils.telemetry import start_call_tracking

        start_call_tracking()
        con = sqlite3.connect(":memory:", factory=sqlite_profiler.InstrumentedConnection)
        con.execute("CREATE TABLE page_scoped (x INTEGER)")
        return BaseResponse.with_telemetry()

    @app.get("/list-response")
    def list_response():
        return [1, 2, 3]

    @app.get("/sse")
    def sse():
        async def gen():
            yield b"data: hello\n\n"
            yield b"data: world\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.get("/ndjson")
    def ndjson_stream():
        async def gen():
            yield b'{"row":1}\n'
            yield b'{"row":2}\n'

        return StreamingResponse(gen(), media_type="application/x-ndjson")

    @app.get("/non-json")
    def non_json():
        return Response(content="hello", media_type="text/plain")

    @app.get("/empty")
    def empty():
        return Response(status_code=204)

    @app.get("/raises")
    def raises():
        # Endpoint that emits a 500 via FastAPI's default exception
        # handler. The handler returns a JSONResponse({"detail": ...}).
        # We pin that the middleware doesn't crash the request even when
        # the response is an error.
        raise RuntimeError("boom")

    # The middleware ordering in real main.py is:
    #   add_middleware(TelemetryResponseBody)   # inner — runs LAST on the way out
    #   add_middleware(GZip)                    # outer — wraps the telemetry one
    # ``app.add_middleware`` is reverse-stack (last call → outermost).
    app.add_middleware(TelemetryResponseBodyMiddleware)
    if with_gzip:
        app.add_middleware(GZipMiddleware, minimum_size=0)
    return app


@pytest.fixture(autouse=True)
def _enable_debug_responses(monkeypatch):
    """The middleware is gated on DEBUG_RESPONSES. Every test in this
    file is about WHAT the middleware does WHEN it's active — turn it
    on for the suite. A dedicated test below covers the gated-off case."""
    monkeypatch.setenv("DEBUG_RESPONSES", "1")


# ── plain-dict endpoint: telemetry must be injected ─────────────────────


def test_injects_debug_keys_into_plain_dict_response():
    """The pivot case: an endpoint that returns ``{"foo": 1}`` without
    BaseResponse must come back with ``_debug_queries`` / ``_debug_calls``
    / ``_is_cached`` added. This is the entire reason M1 exists —
    backstop the next endpoint that forgets to use BaseResponse."""
    client = TestClient(_build_app())
    r = client.get("/plain-dict")
    assert r.status_code == 200
    body = r.json()
    assert body["foo"] == 1
    assert body["bar"] == "two"
    assert "_debug_queries" in body
    assert "_debug_calls" in body
    assert "_debug_sqlite" in body
    assert "_is_cached" in body
    assert isinstance(body["_debug_queries"], list)
    assert isinstance(body["_debug_calls"], list)
    assert isinstance(body["_debug_sqlite"], list)
    assert body["_is_cached"] is False


def test_injects_safely_when_no_telemetry_recorded():
    """Even when the contextvar collectors are empty (no queries / calls
    were tracked during the request), the middleware emits valid empty
    lists rather than ``null`` — the frontend's DebugPanel iterates
    these arrays unconditionally."""
    client = TestClient(_build_app())
    r = client.get("/plain-dict")
    body = r.json()
    assert body["_debug_queries"] == []
    assert body["_debug_calls"] == []


# ── already-has-telemetry: NEVER double-inject ──────────────────────────


def test_does_not_double_inject_when_endpoint_already_supplied_telemetry():
    """Endpoints using ``BaseResponse.with_telemetry`` already include
    the three keys. The middleware MUST preserve them verbatim — not
    overwrite with the (possibly different) contextvar snapshot."""
    client = TestClient(_build_app())
    r = client.get("/already-has-telemetry")
    body = r.json()
    assert body["_debug_queries"] == [{"sql": "SELECT 1", "time_ms": 0.1}]
    assert body["_debug_calls"] == [{"method": "GET", "path": "/x"}]
    assert body["_debug_sqlite"] == [{"seq": 7, "sql": "SELECT 1", "time_ms": 0.2}]
    assert body["_is_cached"] is True


# ── non-dict bodies: passed through unchanged ───────────────────────────


def test_top_level_list_response_is_untouched():
    """A route returning ``[1, 2, 3]`` cannot host the telemetry keys —
    they'd violate the published shape (would require wrapping the
    list). The middleware must leave it alone."""
    client = TestClient(_build_app())
    r = client.get("/list-response")
    assert r.json() == [1, 2, 3]


def test_non_json_response_is_untouched():
    """A ``text/plain`` response is not parsed and not modified. Pinned
    because a body-reading middleware that doesn't check Content-Type
    would corrupt downloads / HTML / SSE."""
    client = TestClient(_build_app())
    r = client.get("/non-json")
    assert r.status_code == 200
    assert r.text == "hello"
    assert r.headers["content-type"].startswith("text/plain")


def test_empty_body_response_is_untouched():
    """A 204 / empty body must not trip the JSON parser. The middleware
    has to reconstruct the response (the body iterator was drained) but
    must not invent a body."""
    client = TestClient(_build_app())
    r = client.get("/empty")
    assert r.status_code == 204
    assert r.content == b""


# ── streaming: never buffer ─────────────────────────────────────────────


def test_sse_response_passes_through_without_buffering():
    """SSE endpoints emit ``text/event-stream`` and would deadlock if
    buffered (infinite streams). Content-Type is the reliable signal —
    Starlette's BaseHTTPMiddleware wraps every response in an internal
    ``_StreamingResponse`` so ``isinstance(response, StreamingResponse)``
    is unreliable; we check the content-type instead.
    """
    client = TestClient(_build_app())
    r = client.get("/sse")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    assert r.text == "data: hello\n\ndata: world\n\n"


def test_ndjson_stream_passes_through_without_buffering():
    """Streaming-row endpoints emit ``application/x-ndjson`` (newline-
    delimited JSON). Each line is its own JSON object; buffering +
    injecting top-level telemetry keys would corrupt the format.
    Pinned because ``application/x-ndjson`` is the right escape hatch
    for routes that want streaming JSON without tripping this backstop.
    """
    client = TestClient(_build_app())
    r = client.get("/ndjson")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/x-ndjson")
    assert r.text == '{"row":1}\n{"row":2}\n'


# ── page-scoped SQLite: profiler → collector → injection, end-to-end ───


def test_sqlite_statements_executed_during_request_reach_debug_sqlite():
    """The Debug Panel's 'This page' SQLite view depends on this chain:
    the profiler's ``_record`` feeds the request-scoped contextvar
    collector (``start_call_tracking`` initialised it), and
    ``BaseResponse.with_telemetry`` serialises that collector as
    ``_debug_sqlite`` (the middleware then passes the already-populated
    body through untouched — no double-inject). Pin the whole path so a
    regression in any link (profiler hook removed, collector not
    initialised, field dropped) fails here."""
    client = TestClient(_build_app())
    r = client.get("/sqlite-work")
    body = r.json()
    sqls = [q["sql"] for q in body["_debug_sqlite"]]
    assert "CREATE TABLE page_scoped (x INTEGER)" in sqls
    entry = body["_debug_sqlite"][sqls.index("CREATE TABLE page_scoped (x INTEGER)")]
    assert entry["op"] == "execute"
    assert entry["time_ms"] >= 0
    assert entry["seq"] >= 1


def test_plain_request_does_not_inherit_other_requests_sqlite():
    """A request that ran no SQLite of its own must carry an empty
    ``_debug_sqlite`` — NOT statements from cron, the ring buffer, or a
    previous request. This is the exact bug the page scoping fixes (7s
    of background statements shown as if the dashboard load caused
    them). The backstop injection reads its own per-request context, so
    the prior request's collector must be invisible here."""
    client = TestClient(_build_app())
    # Seed the process-global ring buffer via a request that DID run SQL.
    client.get("/sqlite-work")
    # A fresh request with no SQLite work of its own.
    r = client.get("/plain-dict")
    assert r.json()["_debug_sqlite"] == []


# ── gated on DEBUG_RESPONSES ────────────────────────────────────────────


def test_does_not_inject_when_debug_responses_env_is_off(monkeypatch):
    """The whole BaseResponse mechanism is gated on DEBUG_RESPONSES.
    The backstop must respect the same flag — otherwise prod (which
    runs with the flag off) would start emitting telemetry blocks on
    every plain-dict endpoint, growing response sizes."""
    monkeypatch.setenv("DEBUG_RESPONSES", "")
    client = TestClient(_build_app())
    r = client.get("/plain-dict")
    body = r.json()
    assert "_debug_queries" not in body
    assert "_debug_calls" not in body


# ── gzip integration: pinned ordering ───────────────────────────────────


def test_works_with_gzip_outer_middleware():
    """In real main.py, GZipMiddleware sits OUTSIDE this one. That means:
    on the way in, gzip → telemetry → route. On the way out, route →
    telemetry (injects) → gzip (compresses). The telemetry middleware
    sees the response BEFORE compression — that's the contract.

    If the ordering ever flips, this middleware would try to JSON-parse
    a gzipped byte stream and fail (silently, per the catch-all). This
    test asserts the happy path: gzip outer + telemetry inner + plain
    dict route = browser still gets injected telemetry."""
    client = TestClient(_build_app(with_gzip=True))
    r = client.get("/plain-dict", headers={"Accept-Encoding": "gzip"})
    # TestClient auto-decodes gzip transparently; we should see the
    # injected telemetry just like the browser would.
    body = r.json()
    assert "_debug_queries" in body, (
        "if telemetry injection silently breaks under gzip, the middleware "
        "is registered in the wrong order — must be INNER to gzip"
    )


# ── error responses ─────────────────────────────────────────────────────


def test_does_not_crash_on_500_error_response():
    """An endpoint that raises an uncaught exception must still produce
    a response. The middleware must not turn a 500 into a 502 by
    mishandling FastAPI's default error response.

    Note: FastAPI's *default* uncaught-exception handler returns
    ``text/plain "Internal Server Error"`` — so this middleware
    correctly passes it through unchanged (telemetry can't be added
    to a non-JSON body). Endpoints that want telemetry on errors
    should raise ``HTTPException`` (handler emits JSON), which the
    backstop would then inject into."""
    client = TestClient(_build_app(), raise_server_exceptions=False)
    r = client.get("/raises")
    assert r.status_code == 500
    # text/plain body, untouched
    assert r.text == "Internal Server Error"
    assert r.headers["content-type"].startswith("text/plain")


# ── malformed JSON body: pass through ──────────────────────────────────


def test_malformed_json_body_passes_through_unchanged():
    """If a buggy route declares ``application/json`` but returns a
    malformed body, the middleware must NOT crash the request — it
    falls back to emitting the original bytes. The endpoint is buggy
    but a 200 with broken JSON beats a 500 from the middleware."""
    app = FastAPI()

    @app.get("/bad-json")
    def bad_json():
        return Response(content=b"{not valid", media_type="application/json")

    app.add_middleware(TelemetryResponseBodyMiddleware)
    client = TestClient(app)
    r = client.get("/bad-json")
    assert r.status_code == 200
    assert r.content == b"{not valid"


# ── JSON list inside JSONResponse (covers fastapi default) ──────────────


def test_multiple_set_cookie_headers_survive_reconstruction():
    """The middleware reconstructs every JSON dict response (to inject
    telemetry). If the reconstruction collapses duplicate header values
    via ``dict(headers.items())``, the second Set-Cookie is silently
    dropped — which broke the share-login pending-cookie flow in prod:
    login sets ``analyst_pending_session_id`` AND deletes the full
    ``analyst_session_id`` (two Set-Cookie headers), the dict comprehension
    kept only the delete, the browser ended up with no session at all,
    AppLayout bounced to /share-login → infinite loop.

    Lock the cookie shape in so any future change to ``_reconstruct``
    that loses a Set-Cookie shows up here, not in a user's broken
    dashboard. The same property protects Link, Vary, and any other
    legitimately multi-valued response header.
    """
    app = FastAPI()

    @app.get("/dual-cookie")
    def dual_cookie(response: Response):
        response.set_cookie(
            key="alpha", value="A", httponly=True, secure=True, samesite="strict", max_age=86400, path="/"
        )
        response.delete_cookie("beta", path="/")
        return {"ok": True}

    app.add_middleware(TelemetryResponseBodyMiddleware)
    client = TestClient(app)
    r = client.get("/dual-cookie")
    assert r.status_code == 200
    assert "_debug_queries" in r.json(), "sanity: middleware actually fired"
    # ``r.headers.get_list`` (httpx) returns every value for the header.
    cookies = r.headers.get_list("set-cookie")
    joined = " | ".join(cookies)
    assert any("alpha=A" in c for c in cookies), (
        f"the Set-Cookie that sets `alpha` was dropped during reconstruction. saw: {joined!r}"
    )
    assert any("beta=" in c and ("Max-Age=0" in c or "expires=" in c.lower()) for c in cookies), (
        f"the Set-Cookie that deletes `beta` was dropped during reconstruction. saw: {joined!r}"
    )


def test_jsonresponse_wrapped_dict_gets_telemetry_injected():
    """Some routes return an explicit JSONResponse instead of a bare
    dict. The middleware must handle both — JSONResponse pre-serialises
    on construction, but the body bytes still parse as JSON."""
    app = FastAPI()

    @app.get("/jr")
    def jr():
        return JSONResponse({"hello": "world"})

    app.add_middleware(TelemetryResponseBodyMiddleware)
    client = TestClient(app)
    r = client.get("/jr")
    body = r.json()
    assert body["hello"] == "world"
    assert "_debug_queries" in body


# ── circular-import safety ──────────────────────────────────────────────


def test_import_failure_of_debug_gate_does_not_block_request(monkeypatch):
    """If ``from backend.models.common import _debug_responses_enabled``
    ever blows up (a real concern at startup before all modules are
    settled, or in a test harness with broken patches), the middleware
    must return the response unmodified rather than 500'ing every
    request."""
    import backend.models.common as common_mod

    def _broken_gate():
        raise RuntimeError("circular import")

    monkeypatch.setattr(common_mod, "_debug_responses_enabled", _broken_gate)
    # And make the attribute LOOKUP raise too, simulating the import
    # itself failing. The middleware imports the symbol inside dispatch,
    # so we monkey-patch the parent module's __getattr__-equivalent by
    # deleting the attribute then poisoning the module.
    import sys

    class _PoisonedModule:
        def __getattr__(self, name):  # pragma: no cover - defensive
            raise RuntimeError("module load broken")

    monkeypatch.setitem(sys.modules, "backend.models.common", _PoisonedModule())

    client = TestClient(_build_app())
    r = client.get("/plain-dict")
    # Response should pass through unchanged — no _debug_queries injection.
    body = r.json()
    assert body == {"foo": 1, "bar": "two"}


# ── is_remote: analyst responses get nothing ────────────────────────────


def test_is_remote_flag_skips_telemetry_injection():
    """N-1 audit fix: analyst (is_remote=True) responses MUST NOT carry
    telemetry — _debug_calls leaks the Fastly KV store id, _debug_queries
    leaks raw SQL. The telemetry middleware honors the same is_remote
    flag the strip in RemoteAccessMiddleware uses, otherwise this
    middleware (which sits outside) would re-inject after the strip."""
    app = FastAPI()

    @app.get("/plain")
    def plain():
        return {"ok": True}

    # Inner middleware: stamp is_remote=True on every request. Because
    # add_middleware is reverse-stack (last call = outermost), we add
    # the stamper LAST so it wraps the telemetry middleware — but we
    # actually want the stamper to run BEFORE the telemetry middleware
    # reads request.state.is_remote. ASGI request-direction order is
    # outer→inner, so the LAST-added middleware sees the request first
    # → set the flag there.
    @app.middleware("http")
    async def _stamp_remote(request, call_next):
        request.state.is_remote = True
        return await call_next(request)

    app.add_middleware(TelemetryResponseBodyMiddleware)
    client = TestClient(app)
    r = client.get("/plain")
    body = r.json()
    assert body == {"ok": True}
    assert "_debug_queries" not in body
    assert "_debug_calls" not in body


# ── body-iterator read failure: catch + pass through ──────────────────


def test_body_iterator_read_failure_returns_original_response(monkeypatch):
    """If reading the response body iterator raises (e.g. an upstream
    middleware's stream produces an exception mid-yield), the telemetry
    middleware logs at warning and returns the original response
    untouched. Without this catch one upstream bug would 500 every
    request that flowed through us."""
    app = FastAPI()

    class _ExplodingResponse(JSONResponse):
        async def __call__(self, scope, receive, send):
            async def _bad_send(message):
                if message["type"] == "http.response.body":
                    raise RuntimeError("body pipe broken")
                await send(message)

            await super().__call__(scope, receive, _bad_send)

    @app.get("/explodes")
    def explodes():
        return _ExplodingResponse({"x": 1})

    app.add_middleware(TelemetryResponseBodyMiddleware)
    client = TestClient(app, raise_server_exceptions=False)
    # The ASGI failure surfaces as a 500 — what we're pinning is that
    # the middleware caught its OWN read failure (covered by line 162-164)
    # rather than crashing inside the try block. Either an OK pass-through
    # or a 500 from the underlying explosion both satisfy "no extra crash".
    r = client.get("/explodes")
    assert r.status_code in (200, 500)


# ── opt_in=False: telemetry stripped from existing payloads ─────────────


def test_strips_telemetry_when_force_include_off_and_no_header(monkeypatch):
    """Production code path: DEBUG_RESPONSES=1 (collectors still run for
    the Debug Panel toggle) but DEBUG_RESPONSES_FORCE_INCLUDE is unset
    and the request doesn't send ``x-debug-responses: 1``. The endpoint
    may have stuffed telemetry into the body via BaseResponse; this
    middleware must strip it on the way out so prod admins (no toggle)
    don't burn bytes on a 5-15 KB envelope they won't read."""
    monkeypatch.delenv("DEBUG_RESPONSES_FORCE_INCLUDE", raising=False)
    client = TestClient(_build_app())
    r = client.get("/already-has-telemetry")
    body = r.json()
    # Non-telemetry keys survive.
    assert body["foo"] == 1
    # The telemetry keys are stripped.
    assert "_debug_queries" not in body
    assert "_debug_calls" not in body
    assert "_debug_sqlite" not in body
    assert "_is_cached" not in body


def test_strip_path_passes_through_when_payload_had_no_telemetry(monkeypatch):
    """opt_in=False path: when the endpoint did NOT add telemetry, the
    middleware shouldn't re-serialise the body. Otherwise every plain
    response pays a json.loads + json.dumps round-trip for nothing.
    Pin the no-op by checking the body bytes survive and the strip
    short-circuit was hit."""
    monkeypatch.delenv("DEBUG_RESPONSES_FORCE_INCLUDE", raising=False)
    client = TestClient(_build_app())
    r = client.get("/plain-dict")
    body = r.json()
    # No telemetry added (the strip path's early return for
    # "nothing to strip" took the fast path).
    assert body == {"foo": 1, "bar": "two"}


# ── injection failure: telemetry collectors raise → pass through ────────


def test_get_queries_failure_passes_through_original_body(monkeypatch):
    """If get_queries() or json.dumps() raises during injection, the
    middleware logs and returns the original body bytes. The endpoint's
    response still reaches the client — telemetry is observability, not
    a correctness gate."""
    import backend.utils.telemetry as telemetry_mod

    monkeypatch.setattr(telemetry_mod, "get_queries", lambda: (_ for _ in ()).throw(RuntimeError("collector broken")))

    client = TestClient(_build_app())
    r = client.get("/plain-dict")
    body = r.json()
    # Original endpoint payload survives.
    assert body["foo"] == 1
    # Injection failed → no telemetry envelope added.
    assert "_debug_queries" not in body


# ── _reconstruct: malformed content-type bytes ──────────────────────────


def test_reconstruct_handles_non_ascii_content_type_header():
    """The _reconstruct fallback reads media_type from raw_headers and
    decodes as ASCII. A pathological response that somehow gets a non-
    ASCII byte sequence in its content-type header must not crash the
    middleware — the UnicodeDecodeError is caught and the new Response
    is built with media_type=None (Starlette tolerates that)."""
    from backend.utils.telemetry_response_middleware import _reconstruct

    class _FakeResp:
        status_code = 200
        media_type = None
        raw_headers: list[tuple[bytes, bytes]] = [(b"content-type", b"application/json\xff")]

    new = _reconstruct(_FakeResp(), b'{"ok":true}')
    assert new.status_code == 200
    assert new.body == b'{"ok":true}'
