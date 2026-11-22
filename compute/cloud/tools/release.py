"""Thin wrapper delegating to scripts.release_compute."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import scripts.release_compute as _release_compute  # noqa: E402
from scripts.release_compute import (  # noqa: E402, F401
    app_tree_sha,
    build,
    check_clean_app_tree,
    check_static_assets,
    frozen_source,
    git,
    prepare_static_export_tree,
    restore_node_modules,
    restore_web_out,
    save_web_caches,
    save_web_out,
    snapshot_sources,
    stabilize_mtimes,
    verify_source,
    web_out_cache_key,
)


def main() -> None:
    _release_compute.main(file_path=__file__)


if __name__ == "__main__":
    main()
