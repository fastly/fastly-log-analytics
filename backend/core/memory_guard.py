"""Process-level RSS guard: a CLEAN restart before the cgroup OOM-kills us.

STOPGAP, not a fix. The backend's RSS climbs unbounded under load and hits
the ``docker-compose.prod.yml`` ``mem_limit: 12g`` cap, where the kernel
OOM-SIGKILLs uvicorn (exit 137) — a *destructive* kill that strands the
in-flight sync row + its raw ``.gz`` (self-healed later, but noisy). The
true allocation source is still under investigation: the object_cache /
recycle / jemalloc theories were all empirically disproven (destroying the
DuckDB instance frees ~0 of the 12GB). See [[backend-oom-restart-loop]].

Until the leak is attributed and fixed, convert the destructive OOM into a
graceful self-restart: when RSS crosses ``BACKEND_GRACEFUL_RESTART_RSS_MB``
we SIGTERM our own process. uvicorn (a single process, PID 1 in the
container — no ``--workers``) catches SIGTERM, drains in-flight requests,
runs lifespan shutdown, and exits 0; docker's ``restart: unless-stopped``
then brings up a fresh process with reclaimed RSS. A clean ~15s restart
instead of a mid-write SIGKILL.

Disabled by default (threshold 0). Prod sets the threshold ~3GB under the
cap so the drain has headroom. Remove this guard once the leak is fixed.
"""

from __future__ import annotations

import logging
import os
import signal

logger = logging.getLogger(__name__)


def graceful_restart_rss_threshold_bytes() -> int:
    """Restart threshold (``BACKEND_GRACEFUL_RESTART_RSS_MB``, MB→bytes).

    0 (default) disables the guard entirely.
    """
    raw = os.getenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "0")
    try:
        return max(0, int(raw)) * 1024 * 1024
    except (TypeError, ValueError):
        return 0


def maybe_graceful_restart() -> bool:
    """SIGTERM self for a clean docker restart if RSS is at/over the threshold.

    Returns True iff a restart was triggered. Never raises — a guard that
    crashes the scheduler tick would defeat its purpose.
    """
    threshold = graceful_restart_rss_threshold_bytes()
    if threshold <= 0:
        return False
    # Lazy import: keep this module import-cheap and free of duckdb import-order
    # concerns. current_rss_bytes reads /proc/self/statm (Linux); None off-Linux.
    try:
        from backend.core.duckdb import current_rss_bytes

        rss = current_rss_bytes()
    except Exception:
        return False
    if rss is None or rss < threshold:
        return False
    logger.warning(
        "\U0001f501 [memguard] RSS %.0fMB >= %.0fMB restart threshold — sending "
        "SIGTERM for a graceful restart (uvicorn drains + exits 0; docker "
        "restart:unless-stopped brings up a fresh process). STOPGAP for the "
        "unresolved memory growth; see backend-oom-restart-loop.",
        rss / 1e6,
        threshold / 1e6,
    )
    os.kill(os.getpid(), signal.SIGTERM)
    return True
