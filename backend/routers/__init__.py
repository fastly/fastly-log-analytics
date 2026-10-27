"""FastAPI routers package with compatibility aliases for consolidated router modules."""

import contextlib
import importlib
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend.routers import bootstrap as debug  # noqa: F401
    from backend.routers import control_room as alerts  # noqa: F401
    from backend.routers import control_room as usage  # noqa: F401
    from backend.routers import share_admin as share_auth  # noqa: F401
    from backend.routers import share_admin as share_oauth  # noqa: F401
    from backend.routers.admin import compaction as high_scale  # noqa: F401

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
    ):
        _global_aliases.pop(_unwound_router, None)

_ROUTER_ALIASES: dict[str, str] = {
    "backend.routers.alerts": "backend.routers.control_room",
    "backend.routers.usage": "backend.routers.control_room",
    "backend.routers.share_auth": "backend.routers.share_admin",
    "backend.routers.share_oauth": "backend.routers.share_admin",
    "backend.routers.debug": "backend.routers.bootstrap",
}

for _alias, _target in _ROUTER_ALIASES.items():
    if _alias not in sys.modules:
        with contextlib.suppress(Exception):
            sys.modules[_alias] = importlib.import_module(_target)
