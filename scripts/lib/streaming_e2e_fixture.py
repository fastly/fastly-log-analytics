"""Per-browser CMCD responses shared by SSR and browser requests in isolated E2E tests."""

from collections.abc import Awaitable, Callable
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

_PATH = "/api/__e2e__/streaming-fixture"
_COOKIE = "fla_streaming_fixture"


def install_streaming_fixture(app: FastAPI) -> None:
    fixtures: dict[str, dict] = {}

    @app.middleware("http")
    async def streaming_fixture(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        token = request.cookies.get(_COOKIE, "")
        fixture = fixtures.get(token)
        if request.url.path == _PATH:
            if request.method == "POST":
                token = uuid4().hex
                fixtures[token] = {"payload": await request.json(), "bodies": []}
                response = JSONResponse({"created": True})
                response.set_cookie(_COOKIE, token, httponly=True, samesite="strict")
                return response
            if request.method == "GET" and fixture is not None:
                return JSONResponse({"bodies": fixture["bodies"]})
            if request.method == "DELETE":
                fixtures.pop(token, None)
                response = JSONResponse({"deleted": True})
                response.delete_cookie(_COOKIE)
                return response
            return JSONResponse({"error": "fixture_not_found"}, status_code=404)
        if request.url.path == "/api/cmcd/aggregates" and request.method == "POST" and fixture is not None:
            fixture["bodies"].append(await request.json())
            return JSONResponse(fixture["payload"])
        return await call_next(request)
