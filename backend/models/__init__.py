"""Ensure ``backend.models.common`` is initialized first regardless of which
submodule a caller imports first (``common`` defines ``BaseResponse`` at top
and imports leaf response models near the bottom for ``BootstrapResponse``).
"""
# ruff: noqa: F401, I001

from backend.models import common as _common
from backend.models import analytics as _analytics
from backend.models import admin as _admin
