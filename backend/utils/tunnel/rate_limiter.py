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


class _LoginRateLimiter(SlidingWindowLimiter):
    """Per-client-IP failure tracker with lockout."""

    def __init__(self) -> None:
        super().__init__(window_s=LOGIN_FAILURE_WINDOW_S, max_keys=MAX_TRACKED_IPS)

    @property
    def _failures(self):
        return self._events

    @property
    def _lockouts(self):
        return self._locks

    def snapshot(self) -> dict:  # type: ignore[override]
        """Best-effort snapshot of recent failure activity for the admin UI."""
        counts, locks = super().snapshot()
        return {
            "failures": [{"ip": ip, "count": n, "window_s": LOGIN_FAILURE_WINDOW_S} for ip, n in counts.items()],
            "lockouts": [{"ip": ip, "remaining_s": rem} for ip, rem in locks.items()],
        }

    def record_failure(self, ip: str) -> bool:
        """Record a failure and return True if a NEW lockout was triggered."""
        return self.strike(ip, threshold=LOGIN_FAILURE_THRESHOLD, lockout_s=LOGIN_LOCKOUT_S)
