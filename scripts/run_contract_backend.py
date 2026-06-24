"""R-13 helper: launch the FastAPI backend with sandboxed DATA_* paths.

Reads CONTRACT_CONFIGS_DIR / CONTRACT_DATA_DIR from the environment,
mutates the backend.config module-level constants BEFORE the routers
import any of them, then hands off to uvicorn. This is the surgical
fix for the contract suite — production config still reads from
backend/config.py's module constants, but the test harness can sandbox
them per-launch without modifying production code.

Usage (typically invoked by frontend/tests/setup-backend.ts):

    CONTRACT_CONFIGS_DIR=/tmp/cfg CONTRACT_DATA_DIR=/tmp/data \
        uv run python scripts/run_contract_backend.py --port 13003
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _maybe_redirect_config_paths() -> None:
    configs_dir = os.environ.get("CONTRACT_CONFIGS_DIR")
    data_dir = os.environ.get("CONTRACT_DATA_DIR")
    if not configs_dir and not data_dir:
        return

    from backend import config as svcconfig

    if configs_dir:
        Path(configs_dir).mkdir(parents=True, exist_ok=True)
        svcconfig.CONFIGS_DIR = Path(configs_dir)
    if data_dir:
        root = Path(data_dir)
        services = root / "services"
        ngwaf = root / "ngwaf"
        cache = root / "cache"
        system = root / "system"
        for d in (root, services, ngwaf, cache, system):
            d.mkdir(parents=True, exist_ok=True)
        svcconfig.DATA_DIR = root
        svcconfig.SERVICES_DATA_DIR = services
        svcconfig.NGWAF_DATA_DIR = ngwaf
        svcconfig.CACHE_DATA_DIR = cache
        svcconfig.SYSTEM_DATA_DIR = system
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

    uvicorn.run(
        "backend.main:app",
        host=args.host,
        port=args.port,
        log_level="warning",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
