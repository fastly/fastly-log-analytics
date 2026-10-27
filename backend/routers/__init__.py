"""FastAPI routers package with compatibility aliases for consolidated router modules."""

import contextlib
import importlib
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend.routers import bootstrap as debug  # noqa: F401
    from backend.routers import share_admin as share_auth  # noqa: F401
    from backend.routers import share_admin as share_oauth  # noqa: F401
    from backend.routers.admin import admin_queries as admin_queries  # noqa: F401
    from backend.routers.admin import high_scale as high_scale  # noqa: F401
    from backend.routers.admin import ingest as admin_usage  # noqa: F401

# Unregister any consolidated router aliases from backend._MODULE_ALIASES so
# standard Python module resolution finds the dedicated files in backend/routers/.
_backend_mod = sys.modules.get("backend")
_global_aliases = getattr(_backend_mod, "_MODULE_ALIASES", None)
if isinstance(_global_aliases, dict):
    for _unwound_router in (
        "backend.routers.origin",
        "backend.routers.network",
        "backend.routers.security",
        "backend.routers.rum",
        "backend.routers.web_vitals",
        "backend.routers.query",
        "backend.routers.performance",
        "backend.routers.insights",
        "backend.routers.provision",
        "backend.routers.bootstrap",
        "backend.routers.services.core",
        "backend.routers.debug",
        "backend.routers.alerts",
        "backend.routers.usage",
        "backend.routers.audit",
        "backend.routers.control_room",
        "backend.routers.admin._router",
        "backend.routers.admin._helpers",
        "backend.routers.admin.bot_sources",
        "backend.routers.admin.ingest",
        "backend.routers.admin.quarantine",
        "backend.routers.admin.downloads",
        "backend.routers.admin.high_scale",
        "backend.routers.admin.compaction",
        "backend.routers.admin.clickhouse",
        "backend.routers.admin.sync_status",
        "backend.routers.admin.admin_queries",
        "backend.routers.admin.events",
        "backend.routers.admin.log_accounting",
        "backend.routers.admin.health",
    ):
        _global_aliases.pop(_unwound_router, None)
    _global_aliases["backend.routers.high_scale"] = "backend.routers.admin.high_scale"
    _global_aliases["backend.routers.admin_queries"] = "backend.routers.admin.admin_queries"
    _global_aliases["backend.routers.admin_usage"] = "backend.routers.admin.ingest"

_ROUTER_ALIASES: dict[str, str] = {
    "backend.routers.share_auth": "backend.routers.share_admin",
    "backend.routers.share_oauth": "backend.routers.share_admin",
    "backend.routers.debug": "backend.routers.bootstrap",
    "backend.routers.high_scale": "backend.routers.admin.high_scale",
    "backend.routers.admin_queries": "backend.routers.admin.admin_queries",
    "backend.routers.admin_usage": "backend.routers.admin.ingest",
}

for _alias, _target in _ROUTER_ALIASES.items():
    if _alias not in sys.modules:
        with contextlib.suppress(Exception):
            sys.modules[_alias] = importlib.import_module(_target)
