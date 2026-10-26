import sys

from backend.core.fastly import client

service = client
mock_fixtures = client
utils = client
rum_provisioning = client
api = client
sys.modules[f"{__name__}.service"] = client
sys.modules[f"{__name__}.mock_fixtures"] = client
sys.modules[f"{__name__}.utils"] = client
sys.modules[f"{__name__}.rum_provisioning"] = client
sys.modules[f"{__name__}.api"] = client

import backend as _backend_pkg  # noqa: E402

if hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.core.fastly.utils": "backend.core.fastly.client",
            "backend.core.fastly.rum_provisioning": "backend.core.fastly.client",
            "backend.core.fastly.api": "backend.core.fastly.client",
            "backend.core.fastly.service": "backend.core.fastly.client",
            "backend.core.fastly.mock_fixtures": "backend.core.fastly.client",
        }
    )
