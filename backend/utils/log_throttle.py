"""Rate limit for log sites that can fire per request/row while a failure persists."""

from __future__ import annotations

import threading
import time

_MAX_KEYS = 1024
_last: dict[str, float] = {}
_lock = threading.Lock()


def log_once_per(key: str, interval_s: float = 60.0) -> bool:
    """True when ``key`` has not been allowed in the last ``interval_s`` seconds (and records this call)."""
    now = time.monotonic()
    with _lock:
        prev = _last.get(key)
        if prev is not None and now - prev < interval_s:
            return False
        if len(_last) >= _MAX_KEYS:
            cutoff = now - interval_s
            for k in [k for k, t in _last.items() if t < cutoff]:
                del _last[k]
            if len(_last) >= _MAX_KEYS:
                _last.clear()
        _last[key] = now
        return True
