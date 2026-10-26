from __future__ import annotations

import sys as _sys

_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.core.faro_versions": "backend.core.rum_ingest",
            "backend.core.clickhouse_manifest": "backend.core.clickhouse",
        }
    )
