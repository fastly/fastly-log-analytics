"""Ensure ``backend.models.common`` is initialized first regardless of which
submodule a caller imports first (``common`` defines ``BaseResponse`` at top
and imports leaf response models near the bottom for ``BootstrapResponse``).
"""

import sys

from backend.models import _base
from backend.models import admin as _admin  # noqa: F401
from backend.models import analytics as _analytics  # noqa: F401
from backend.models import common as _common  # noqa: F401
from backend.models import dashboard as _dashboard  # noqa: F401
from backend.models import services as _services  # noqa: F401

sys.modules["backend.models.errors"] = _base
sys.modules["backend.models.security"] = _dashboard
sys.modules["backend.models.admin_actions"] = _analytics
sys.modules["backend.models.origin"] = _analytics
sys.modules["backend.models.network"] = _analytics
sys.modules["backend.models.performance"] = _analytics
sys.modules["backend.models.rum"] = _analytics
sys.modules["backend.models.share_admin"] = _admin
sys.modules["backend.models.provision"] = _services
sys.modules["backend.models.service"] = _services

_backend_pkg = sys.modules.get("backend")
_aliases = getattr(_backend_pkg, "_MODULE_ALIASES", None)
if isinstance(_aliases, dict):
    _aliases.update(
        {
            "backend.routers.admin.downloads": "backend.routers.admin.ingest",
            "backend.routers.admin_queries": "backend.routers.admin.health",
            "backend.routers.performance": "backend.routers.dashboard",
            "backend.models.network": "backend.models.analytics",
            "backend.models.performance": "backend.models.analytics",
            "backend.models.rum": "backend.models.analytics",
            "backend.models.share_admin": "backend.models.admin",
            "backend.models.provision": "backend.models.services",
            "backend.models.service": "backend.models.services",
            "backend.provision.utils": "backend.provision.cli",
            "backend.provision.declarative.iac_reconciler": "backend.provision.declarative.reconciler",
            "backend.provision.cmcd_fields": "backend.provision.orchestrator",
            "backend.core.fastly.rum_provisioning": "backend.core.fastly.client",
            "backend.core.fastly.utils": "backend.core.fastly.client",
            "backend.core.fastly.api": "backend.core.fastly.client",
            "backend.routers.session_scoring_admin": "backend.routers.session_scoring",
        }
    )
