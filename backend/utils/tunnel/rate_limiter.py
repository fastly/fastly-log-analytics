"""Thread-safe sliding-window login-failure tracker per client IP.

5 failures within a 60s window triggers a 5-minute IP lockout. Used by
the share-login endpoint to slow credential-stuffing attacks without
locking individual analyst emails (which would be a denial-of-service
vector against a known address).
"""

from __future__ import annotations

import threading
import time

# Login rate-limit: 5 failures / 60s → 5-minute lockout.
LOGIN_FAILURE_WINDOW_S = 60
LOGIN_FAILURE_THRESHOLD = 5
LOGIN_LOCKOUT_S = 5 * 60


class _LoginRateLimiter:
    """Thread-safe sliding-window failure tracker per client IP."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._failures: dict[str, list[float]] = {}
        self._lockouts: dict[str, float] = {}

    def is_locked(self, ip: str) -> tuple[bool, int]:
        """Returns ``(locked, remaining_seconds)``."""
        with self._lock:
            until = self._lockouts.get(ip)
            if until is None:
                return False, 0
            now = time.time()
            if now >= until:
                self._lockouts.pop(ip, None)
                return False, 0
            return True, int(until - now)

    def snapshot(self) -> dict:
        """Best-effort snapshot of recent failure activity for the admin UI.

        Returns ``{"failures": [...], "lockouts": [...]}`` — each list element
        carries the IP, count/remaining, and a window in seconds. Self-prunes
        expired lockouts on the way out so the snapshot reflects current state.
        """
        with self._lock:
            now = time.time()
            window_start = now - LOGIN_FAILURE_WINDOW_S
            failures = []
            for ip, history in list(self._failures.items()):
                pruned = [t for t in history if t >= window_start]
                if pruned:
                    failures.append(
                        {
                            "ip": ip,
                            "count": len(pruned),
                            "window_s": LOGIN_FAILURE_WINDOW_S,
                        }
                    )
                    self._failures[ip] = pruned
                else:
                    self._failures.pop(ip, None)
            lockouts = []
            for ip, until in list(self._lockouts.items()):
                if until <= now:
                    self._lockouts.pop(ip, None)
                    continue
                lockouts.append({"ip": ip, "remaining_s": int(until - now)})
            return {"failures": failures, "lockouts": lockouts}

    def record_failure(self, ip: str) -> bool:
        """Record a failure and return True if a NEW lockout was triggered."""
        with self._lock:
            now = time.time()
            window_start = now - LOGIN_FAILURE_WINDOW_S
            history = [t for t in self._failures.get(ip, []) if t >= window_start]
            history.append(now)
            self._failures[ip] = history
            if len(history) >= LOGIN_FAILURE_THRESHOLD and ip not in self._lockouts:
                self._lockouts[ip] = now + LOGIN_LOCKOUT_S
                return True
            return False

    def clear(self, ip: str) -> None:
        with self._lock:
            self._failures.pop(ip, None)
            self._lockouts.pop(ip, None)
