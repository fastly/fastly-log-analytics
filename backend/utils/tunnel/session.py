"""Analyst session lifecycle — dataclass, validation, multi-device boot,
browser/OS fingerprint hashing, and sliding-window login rate limiter.

Session writes are mirrored to ``remote_sessions`` in ``share_db`` so a
backend restart does not silently log every analyst out. The
``validate_session`` permission re-sync is security-critical: tightening
``pii_policy``, ``query_window_hours``, ``query_start_time/end_time``, or
``service_ids`` mid-session takes effect on the very next request rather
than waiting for the session to naturally time out.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field

from backend.utils.rate_limit import SlidingWindowLimiter

# Idle and absolute timeouts (2h idle, 24h absolute).
IDLE_TIMEOUT_S = 2 * 60 * 60
ABSOLUTE_TIMEOUT_S = 24 * 60 * 60

# Login rate-limit: 5 failures / 60s → 5-minute lockout.
LOGIN_FAILURE_WINDOW_S = 60
LOGIN_FAILURE_THRESHOLD = 5
LOGIN_LOCKOUT_S = 5 * 60
MAX_TRACKED_IPS = 10000

_UA_RE = re.compile(r"(Chrome|Firefox|Safari|Edge|OPR)/(\d+)")
_OS_RE = re.compile(r"(Macintosh|Mac OS X|Windows|Linux|X11|iPhone|iPad|Android)")


def compute_fingerprint(headers: dict[str, str]) -> str:
    """Narrowed SHA-256 over browser family + major version + OS family."""
    ua = headers.get("user-agent", "") or headers.get("User-Agent", "") or ""
    browser_match = _UA_RE.search(ua)
    os_match = _OS_RE.search(ua)
    parts = [
        browser_match.group(1) if browser_match else "unknown-browser",
        browser_match.group(2) if browser_match else "0",
        os_match.group(1) if os_match else "unknown-os",
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


class _LoginRateLimiter(SlidingWindowLimiter):
    """Per-client-IP failure tracker with lockout."""

    def __init__(self) -> None:
        super().__init__(window_s=LOGIN_FAILURE_WINDOW_S, max_keys=MAX_TRACKED_IPS)

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


@dataclass
class AnalystSession:
    session_id: str
    invite_id: str
    name: str
    email: str
    ip_address: str
    user_agent: str
    fingerprint_signature: str
    pii_policy: dict
    query_window_hours: int | None
    query_start_time: str | None
    query_end_time: str | None
    login_time: str
    last_active_time: str
    last_activity: str | None = None
    tos_pending: bool = False
    service_ids: list[str] = field(default_factory=list)
    # How this session authenticated. Carried from the invite so the admin
    # Sessions tab can tell OAuth from passcode without a DB round-trip; an
    # OAuth session is otherwise indistinguishable downstream (same RBAC /
    # masking / TOS / boot / revoke). Not persisted to remote_sessions — on
    # restart these default to 'passcode' and validate_session re-syncs them
    # from the invite on the next request.
    auth_method: str = "passcode"
    oauth_provider: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_row(cls, row: dict) -> AnalystSession:
        return cls(
            session_id=row["session_id"],
            invite_id=row["invite_id"],
            name=row["name"],
            email=row["email"],
            ip_address=row["ip_address"],
            user_agent=row["user_agent"],
            fingerprint_signature=row["fingerprint_signature"],
            pii_policy=row.get("pii_policy") or {},
            query_window_hours=row.get("query_window_hours"),
            query_start_time=row.get("query_start_time"),
            query_end_time=row.get("query_end_time"),
            login_time=row["login_time"],
            last_active_time=row["last_active_time"],
            last_activity=row.get("last_activity"),
            tos_pending=row.get("tos_pending", False),
            service_ids=row.get("service_ids", []),
            auth_method=row.get("auth_method", "passcode"),
            oauth_provider=row.get("oauth_provider"),
        )
