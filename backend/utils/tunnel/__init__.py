"""Direct-mode share manager package — backward-compatible re-exports.

Every public name that was importable from the original
``backend.utils.tunnel`` module remains importable from the same path so
existing callers (``from backend.utils.tunnel import get_tunnel_manager``,
``from backend.utils import tunnel; tunnel.LOGIN_FAILURE_THRESHOLD``,
``tunnel._LoginRateLimiter()``, etc.) keep working.

The SSH-to-localhost.run code path was removed in v2.0.
"""

from __future__ import annotations

from .manager import (
    ABSOLUTE_TIMEOUT_S,
    IDLE_TIMEOUT_S,
    LOGIN_FAILURE_THRESHOLD,
    LOGIN_FAILURE_WINDOW_S,
    LOGIN_LOCKOUT_S,
    AnalystSession,
    TunnelCapacityError,
    TunnelManager,
    TunnelState,
    _LoginRateLimiter,
    build_share_live_payload,
    clear_persisted_state,
    compute_fingerprint,
    get_tunnel_manager,
    persist_direct_state,
    reset_for_tests,
    restore_direct_state,
)

__all__ = [
    "ABSOLUTE_TIMEOUT_S",
    "AnalystSession",
    "IDLE_TIMEOUT_S",
    "LOGIN_FAILURE_THRESHOLD",
    "LOGIN_FAILURE_WINDOW_S",
    "LOGIN_LOCKOUT_S",
    "TunnelCapacityError",
    "TunnelManager",
    "TunnelState",
    "_LoginRateLimiter",
    "build_share_live_payload",
    "clear_persisted_state",
    "compute_fingerprint",
    "get_tunnel_manager",
    "persist_direct_state",
    "reset_for_tests",
    "restore_direct_state",
]
