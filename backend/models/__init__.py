"""Ensure ``backend.models.common`` is initialized first regardless of which
submodule a caller imports first (``common`` defines ``BaseResponse`` at top
and imports leaf response models near the bottom for ``BootstrapResponse``).
"""

import sys

from backend.models import _base
from backend.models import common as _common  # noqa: F401
from backend.models import dashboard as _dashboard  # noqa: F401

sys.modules["backend.models.errors"] = _base
sys.modules["backend.models.security"] = _dashboard
