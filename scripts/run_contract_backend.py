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
from pathlib import Path


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

    uvicorn.run(
        "backend.main:app",
        host=args.host,
        port=args.port,
        log_level="warning",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
