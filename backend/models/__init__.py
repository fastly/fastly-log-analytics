"""Ensure ``backend.models.common`` is initialized first regardless of which
submodule a caller imports first (``common`` defines ``BaseResponse`` at top
and imports leaf response models near the bottom for ``BootstrapResponse``).
"""

from backend.models import common as _common  # noqa: F401
