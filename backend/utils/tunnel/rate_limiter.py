"""Thread-safe sliding-window login-failure tracker per client IP.

5 failures within a 60s window triggers a 5-minute IP lockout. Used by
the share-login endpoint to slow credential-stuffing attacks without
locking individual analyst emails (which would be a denial-of-service
vector against a known address).
"""

from __future__ import annotations

from backend.utils.rate_limit import SlidingWindowLimiter

# Login rate-limit: 5 failures / 60s → 5-minute lockout.
LOGIN_FAILURE_WINDOW_S = 60
LOGIN_FAILURE_THRESHOLD = 5
LOGIN_LOCKOUT_S = 5 * 60


MAX_TRACKED_IPS = 10000


class _LoginRateLimiter:
    """Per-client-IP failure tracker with lockout (facade over ``SlidingWindowLimiter``)."""

    def __init__(self) -> None:
        self._limiter = SlidingWindowLimiter(window_s=LOGIN_FAILURE_WINDOW_S, max_keys=MAX_TRACKED_IPS)

    @property
    def _failures(self):
        return self._limiter._events

    @property
    def _lockouts(self):
        return self._limiter._locks

    def is_locked(self, ip: str) -> tuple[bool, int]:
        """Returns ``(locked, remaining_seconds)``."""
        return self._limiter.is_locked(ip)

    def snapshot(self) -> dict:
        """Best-effort snapshot of recent failure activity for the admin UI.

        Returns ``{"failures": [...], "lockouts": [...]}`` — each list element
        carries the IP, count/remaining, and a window in seconds. Self-prunes
        expired lockouts on the way out so the snapshot reflects current state.
        """
        counts, locks = self._limiter.snapshot()
        return {
            "failures": [{"ip": ip, "count": n, "window_s": LOGIN_FAILURE_WINDOW_S} for ip, n in counts.items()],
            "lockouts": [{"ip": ip, "remaining_s": rem} for ip, rem in locks.items()],
        }

    def record_failure(self, ip: str) -> bool:
        """Record a failure and return True if a NEW lockout was triggered."""
        return self._limiter.strike(ip, threshold=LOGIN_FAILURE_THRESHOLD, lockout_s=LOGIN_LOCKOUT_S)
