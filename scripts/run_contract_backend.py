"""R-13 helper: launch the FastAPI backend with sandboxed DATA_* paths.

With CONTRACT_CONFIGS_DIR / CONTRACT_DATA_DIR set, require sibling configs/
and data/ directories, change CWD to their sandbox parent, and redirect config
constants BEFORE backend imports. CWD isolation also covers modules with
literal data/ and cache/ paths. With neither override, launch unchanged.

Usage (typically invoked by frontend/tests/setup-backend.ts):

    CONTRACT_CONFIGS_DIR=.test-sandbox/configs CONTRACT_DATA_DIR=.test-sandbox/data \
        uv run python scripts/run_contract_backend.py --port 13003
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

_STREAMING_FIXTURE_PATH = "/api/__e2e__/streaming-fixture"
_STREAMING_FIXTURE_COOKIE = "fla_streaming_fixture"


def install_streaming_fixture(app: FastAPI) -> None:
    fixtures: dict[str, dict] = {}

    @app.middleware("http")
    async def streaming_fixture(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        token = request.cookies.get(_STREAMING_FIXTURE_COOKIE, "")
        fixture = fixtures.get(token)
        if request.url.path == _STREAMING_FIXTURE_PATH:
            if request.method == "POST":
                token = uuid4().hex
                fixtures[token] = {"payload": await request.json(), "bodies": []}
                response = JSONResponse({"created": True})
                response.set_cookie(_STREAMING_FIXTURE_COOKIE, token, httponly=True, samesite="strict")
                return response
            if request.method == "GET" and fixture is not None:
                return JSONResponse({"bodies": fixture["bodies"]})
            if request.method == "DELETE":
                fixtures.pop(token, None)
                response = JSONResponse({"deleted": True})
                response.delete_cookie(_STREAMING_FIXTURE_COOKIE)
                return response
            return JSONResponse({"error": "fixture_not_found"}, status_code=404)
        if request.url.path == "/api/cmcd/aggregates" and request.method == "POST" and fixture is not None:
            fixture["bodies"].append(await request.json())
            return JSONResponse(fixture["payload"])
        return await call_next(request)


def _maybe_redirect_config_paths() -> None:
    configs_dir = os.environ.get("CONTRACT_CONFIGS_DIR")
    data_dir = os.environ.get("CONTRACT_DATA_DIR")
    if configs_dir is None and data_dir is None:
        return
    if not configs_dir or not data_dir:
        raise ValueError("CONTRACT_CONFIGS_DIR and CONTRACT_DATA_DIR must both be non-empty")

    configs = Path(configs_dir).resolve()
    root = Path(data_dir).resolve()
    sandbox = root.parent
    if root.name != "data" or configs != sandbox / "configs":
        raise ValueError("Contract overrides must be sibling sandbox/configs and sandbox/data directories")

    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    services = root / "services"
    ngwaf = root / "ngwaf"
    cache = root / "cache"
    system = root / "system"
    for directory in (configs, services, ngwaf, cache, system):
        directory.mkdir(parents=True, exist_ok=True)
    os.chdir(sandbox)
    os.environ["REMOTE_SHARE_DB_DIR"] = str(system)

    from backend import config as svcconfig

    svcconfig.CONFIGS_DIR = configs
    svcconfig.DATA_DIR = root
    svcconfig.SERVICES_DATA_DIR = services
    svcconfig.NGWAF_DATA_DIR = ngwaf
    svcconfig.CACHE_DATA_DIR = cache
    svcconfig.SYSTEM_DATA_DIR = system
    svcconfig._USAGE_LOGGING_CONFIG_PATH = system / "usage_logging.json"
    # Clear the per-path memo so any later mkdir actually runs against
    # the sandboxed tree instead of being skipped because the original
    # repo `configs/` mkdir was already cached.
    svcconfig._ensured_dirs = set()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=13003)
    args = parser.parse_args()

    _maybe_redirect_config_paths()

    import uvicorn

    app: str | FastAPI = "backend.main:app"
    if os.environ.get("CONTRACT_STREAMING_FIXTURES") == "1":
        if not os.environ.get("CONTRACT_CONFIGS_DIR") or not os.environ.get("CONTRACT_DATA_DIR"):
            raise ValueError("Streaming fixtures require an isolated contract sandbox")
        from backend.main import app as backend_app

        install_streaming_fixture(backend_app)
        app = backend_app

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="warning",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
