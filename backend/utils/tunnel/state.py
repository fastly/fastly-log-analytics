"""Direct-mode share state — in-memory dataclass + disk persistence.

Backend restarts (deploys, crashes) would otherwise drop the registered
``public_endpoint``, causing analyst traffic to start failing
host-allowed checks. Persisting the two fields needed to rebuild
direct-mode state (``public_endpoint``, ``forward_port``) on every
change and reloading on ``TunnelManager`` ``__init__`` re-arms the
public endpoint automatically.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TunnelState:
    public_endpoint: str | None = None
    started_at: str | None = None
    forward_port: int = 3000
    direct_socket_addr: str | None = None  # for direct-expose mode


def _state_file_path() -> str:
    from backend.config import DATA_DIR

    return str(DATA_DIR / "tunnel_state.json")


def persist_direct_state(state: TunnelState) -> None:
    """Persist the minimum fields needed to restore direct-mode on restart."""
    try:
        import json

        with open(_state_file_path(), "w") as f:
            json.dump(
                {
                    "public_endpoint": state.public_endpoint,
                    "forward_port": state.forward_port,
                },
                f,
            )
    except Exception:
        logger.exception("[tunnel] failed to persist direct-mode state")


def clear_persisted_state() -> None:
    """Remove the persisted state file (called on stop / panic)."""
    try:
        path = _state_file_path()
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        logger.exception("[tunnel] failed to clear persisted state")


def restore_direct_state(state: TunnelState) -> bool:
    """Reload persisted direct-mode state into ``state`` in-place.

    Returns ``True`` iff a public_endpoint was successfully restored.
    """
    from backend.utils.date_utils import iso_z_now

    try:
        import json

        path = _state_file_path()
        if not os.path.exists(path):
            return False
        with open(path) as f:
            data = json.load(f)
        endpoint = data.get("public_endpoint")
        if not endpoint:
            return False
        state.public_endpoint = endpoint
        state.forward_port = data.get("forward_port", 3000)
        state.direct_socket_addr = "0.0.0.0"
        state.started_at = iso_z_now()
        logger.info("[tunnel] restored direct-mode share state for %s", endpoint)
        return True
    except Exception:
        logger.exception("[tunnel] failed to restore direct-mode state")
        return False
