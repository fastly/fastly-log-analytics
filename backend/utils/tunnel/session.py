"""Analyst session lifecycle — dataclass, validation, multi-device boot.

Session writes are mirrored to ``remote_sessions`` in ``share_db`` so a
backend restart does not silently log every analyst out. The
``validate_session`` permission re-sync is security-critical: tightening
``pii_policy``, ``query_window_hours``, ``query_start_time/end_time``, or
``service_ids`` mid-session takes effect on the very next request rather
than waiting for the session to naturally time out.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

# Idle and absolute timeouts (2h idle, 24h absolute).
IDLE_TIMEOUT_S = 2 * 60 * 60
ABSOLUTE_TIMEOUT_S = 24 * 60 * 60


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
    service_ids: list[str] = field(default_factory=list)

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
            service_ids=[],
        )


def parse_iso_z(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
