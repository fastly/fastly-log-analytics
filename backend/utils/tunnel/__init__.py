"""Direct-mode share manager package — backward-compatible re-exports.

Every public name that was importable from the original
``backend.utils.tunnel`` module remains importable from the same path so
existing callers (``from backend.utils.tunnel import get_tunnel_manager``,
``from backend.utils import tunnel; tunnel.LOGIN_FAILURE_THRESHOLD``,
``tunnel._LoginRateLimiter()``, etc.) keep working.

The SSH-to-localhost.run code path was removed in v2.0.
"""

from __future__ import annotations

# Re-import the ``time`` module so existing tests that monkeypatch
# ``tunnel.time.time`` for time-travel still work.
import time  # noqa: F401  — re-exported for monkeypatch compat

from .fingerprint import compute_fingerprint
from .manager import TunnelManager, get_tunnel_manager, reset_for_tests
from .rate_limiter import (
    LOGIN_FAILURE_THRESHOLD,
    LOGIN_FAILURE_WINDOW_S,
    LOGIN_LOCKOUT_S,
    _LoginRateLimiter,
)
from .session import (
    ABSOLUTE_TIMEOUT_S,
    IDLE_TIMEOUT_S,
    AnalystSession,
)
from .state import TunnelState

__all__ = [
    "ABSOLUTE_TIMEOUT_S",
    "AnalystSession",
    "IDLE_TIMEOUT_S",
    "LOGIN_FAILURE_THRESHOLD",
    "LOGIN_FAILURE_WINDOW_S",
    "LOGIN_LOCKOUT_S",
    "TunnelManager",
    "TunnelState",
    "_LoginRateLimiter",
    "compute_fingerprint",
    "get_tunnel_manager",
    "reset_for_tests",
]
