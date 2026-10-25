"""Direct-mode share manager + analyst session orchestration.

Singleton ``TunnelManager`` owns:
- the registered public HTTPS endpoint (direct-mode only; the SSH-tunnel
  path was removed in v2.0)
- in-memory ``AnalystSession`` dict (rehydrated from ``share_db`` at
  startup)
- sliding-window login rate limiter (per client IP)
- session timeout enforcement (2h idle / 24h absolute)
- multi-device boot (one active session per invite at a time, unless the
  invite opts into shared logins via ``allow_concurrent_sessions``)
- direct-mode state persistence so a backend restart re-arms the public
  endpoint automatically

Use :func:`get_tunnel_manager` to access the singleton.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import secrets
import threading
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from backend.core import share_db
from backend.utils.date_utils import iso_z_now, parse_iso_utc
from backend.utils.rate_limit import SlidingWindowLimiter

logger = logging.getLogger(__name__)

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


@dataclass
class TunnelState:
    public_endpoint: str | None = None
    started_at: str | None = None
    forward_port: int = 3000


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
    try:
        import json

        path = _state_file_path()
        if not os.path.exists(path):
            try:
                from backend import config

                for cfg in config.list_configs():
                    rf = cfg.get("remote_frontend")
                    if isinstance(rf, dict) and rf.get("domain_name"):
                        domain = rf["domain_name"].strip()
                        state.public_endpoint = f"https://{domain}"
                        state.forward_port = 3000
                        state.started_at = iso_z_now()
                        logger.info(
                            "[tunnel] restored direct-mode share state from remote_frontend config for %s",
                            state.public_endpoint,
                        )
                        return True
            except Exception:
                logger.debug("restore_direct_state: step failed (ignored)", exc_info=True)
            return False
        with open(path) as f:
            data = json.load(f)
        endpoint = data.get("public_endpoint")
        if not endpoint:
            return False
        state.public_endpoint = endpoint
        state.forward_port = data.get("forward_port", 3000)
        state.started_at = iso_z_now()
        logger.info("[tunnel] restored direct-mode share state for %s", endpoint)
        return True
    except Exception:
        logger.exception("[tunnel] failed to restore direct-mode state")
        return False


class TunnelCapacityError(Exception):
    """Raised by :meth:`TunnelManager.create_session` when ``cap`` is at capacity.

    Carries the observed count so the caller can build the same
    ``capacity_exceeded`` error body it used before this was atomic.
    """

    def __init__(self, current: int, cap: int) -> None:
        super().__init__(f"session capacity exceeded: {current} >= {cap}")
        self.current = current
        self.cap = cap


class TunnelManager:
    """Process-wide singleton. Use ``get_tunnel_manager()`` to access."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sessions: dict[str, AnalystSession] = {}
        self._rate_limiter = _LoginRateLimiter()
        self._state = TunnelState()
        # Restore direct-mode share state from disk so a backend restart
        # doesn't drop the registered public_endpoint.
        restore_direct_state(self._state)
        # Observability counters. In-memory only; reset on process restart.
        # `_heartbeat_unauth_count` increments every time /api/share/heartbeat
        # returns 401/403, i.e. every time an analyst gets bounced to the
        # login page. Lets the admin distinguish "session expired naturally"
        # from "tunnel died" without parsing the audit log.
        # `_tunnel_uptime_history` stores past sharing-session durations so
        # the admin can spot flakiness even before the current share is
        # stopped.
        self._heartbeat_unauth_count: int = 0
        self._tunnel_uptime_history: list[dict] = []  # [{started, ended, duration_s}]

    # ── Rehydration ────────────────────────────────────────────────────

    def rehydrate_sessions(self) -> int:
        """Reload persisted sessions from share_db and prune expired rows.

        Returns count rehydrated (after pruning). Called at startup so a
        uvicorn --reload bounce doesn't log every analyst out.
        """
        kept = 0
        rows = share_db.get_all_sessions()
        now = datetime.now(UTC)
        with self._lock:
            for row in rows:
                login = parse_iso_utc(row["login_time"])
                last = parse_iso_utc(row["last_active_time"])
                if login is None or last is None:
                    share_db.delete_session(row["session_id"])
                    continue
                if (now - login).total_seconds() > ABSOLUTE_TIMEOUT_S:
                    share_db.log_share_audit_event(
                        event_type="SESSION_TIMEOUT",
                        email=row.get("email"),
                        ip_address=row.get("ip_address", "0.0.0.0"),
                        details="expired during backend restart (absolute lifetime)",
                    )
                    share_db.delete_session(row["session_id"])
                    continue
                if (now - last).total_seconds() > IDLE_TIMEOUT_S:
                    share_db.log_share_audit_event(
                        event_type="SESSION_TIMEOUT",
                        email=row.get("email"),
                        ip_address=row.get("ip_address", "0.0.0.0"),
                        details="expired during backend restart (idle)",
                    )
                    share_db.delete_session(row["session_id"])
                    continue
                session = AnalystSession.from_row(row)
                session.service_ids = share_db.get_remote_invite_services(row["invite_id"])
                self._sessions[session.session_id] = session
                kept += 1
        return kept

    # ── Session ops ────────────────────────────────────────────────────

    def get_session(self, session_id: str | None) -> AnalystSession | None:
        if not session_id:
            return None
        with self._lock:
            return self._sessions.get(session_id)

    def list_sessions(self) -> list[AnalystSession]:
        with self._lock:
            return list(self._sessions.values())

    def active_session_count(self) -> int:
        with self._lock:
            return len(self._sessions)

    def create_session(
        self,
        *,
        invite: dict,
        ip_address: str,
        user_agent: str,
        headers: dict[str, str],
        cap: int | None = None,
    ) -> AnalystSession:
        """Register a new session, booting any existing one for the same invite.

        If ``cap`` is given, the capacity check and the session insert happen
        under the SAME lock acquisition, closing a TOCTOU window where two
        concurrent logins (valid credentials, an invite with
        ``allow_concurrent_sessions``, or two distinct invites) could both
        observe ``active_session_count() < cap`` and both proceed, transiently
        overshooting the configured cap (audit finding 016). Raises
        :class:`TunnelCapacityError` instead of inserting when at capacity —
        callers that previously pre-checked ``active_session_count() >= cap``
        themselves should pass ``cap`` here instead and drop that check.
        """
        invite_id = invite["id"]
        with self._lock:
            if cap is not None and len(self._sessions) >= cap:
                raise TunnelCapacityError(len(self._sessions), cap)
            # Multi-device boot — single-seat invites only. When the invite opts
            # into shared logins (allow_concurrent_sessions), skip the boot so
            # several analysts can be active under one invite at once (still
            # bounded by the global max_concurrent_analyst_sessions cap the
            # caller enforces).
            if not invite.get("allow_concurrent_sessions"):
                booted = [s for s in self._sessions.values() if s.invite_id == invite_id]
                for prev in booted:
                    self._sessions.pop(prev.session_id, None)
                    try:
                        share_db.delete_session(prev.session_id)
                        share_db.log_share_audit_event(
                            event_type="SESSION_BOOT",
                            email=prev.email,
                            ip_address=prev.ip_address,
                            details="concurrent login booted previous session",
                        )
                    except Exception:
                        logger.exception("[tunnel] failed to record SESSION_BOOT for %s", prev.session_id)

            now = iso_z_now()
            session = AnalystSession(
                session_id=secrets.token_urlsafe(32),
                invite_id=invite_id,
                name=invite.get("name", ""),
                email=invite.get("email", ""),
                ip_address=ip_address,
                user_agent=user_agent,
                fingerprint_signature=compute_fingerprint(headers),
                pii_policy=invite.get("pii_policy") or {"mask_ips": False},
                query_window_hours=invite.get("query_window_hours"),
                query_start_time=invite.get("query_start_time"),
                query_end_time=invite.get("query_end_time"),
                login_time=now,
                last_active_time=now,
                last_activity=None,
                service_ids=invite.get("service_ids", []) or [],
                auth_method=invite.get("auth_method", "passcode"),
                oauth_provider=invite.get("oauth_provider"),
            )
            self._sessions[session.session_id] = session
            try:
                share_db.upsert_session(session.to_dict())
            except Exception:
                logger.exception("[tunnel] failed to persist new session")
            return session

    def touch_session(
        self,
        session_id: str,
        *,
        last_activity: str | None = None,
        new_ip: str | None = None,
        bump_active: bool = True,
    ) -> bool:
        """Bump ``last_active_time`` (and optionally last_activity / ip).

        ``bump_active=False`` updates the row (e.g. a roamed ``new_ip``) WITHOUT
        resetting the idle clock. A network IP change is not user activity — and
        on a rotating-egress proxy (per-request NAT) it fires on nearly every
        request, so bumping there would defeat the idle timeout regardless of
        the X-User-Active gate.
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return False
            if bump_active:
                session.last_active_time = iso_z_now()
            if last_activity is not None:
                session.last_activity = last_activity
            if new_ip is not None and new_ip != session.ip_address:
                session.ip_address = new_ip
            try:
                share_db.upsert_session(session.to_dict())
            except Exception:
                logger.exception("[tunnel] failed to persist touched session")
        return True

    def validate_session(self, session_id: str | None) -> AnalystSession | None:
        """Return the session iff it's still valid; otherwise evict + None.

        Verifies idle + absolute timeouts and that the linked invite is still
        unrevoked and unexpired. Re-syncs the mutable permission fields from
        the current invite so an admin who tightens an analyst's
        ``pii_policy`` / ``query_window_hours`` / ``query_start_time`` /
        ``query_end_time`` / ``service_ids`` sees those bounds enforced on
        the very next request (rather than waiting for the session to
        naturally time out).
        """
        if not session_id:
            return None
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                try:
                    row = share_db.get_session(session_id)
                    if row:
                        rehydrated = AnalystSession.from_row(row)
                        rehydrated.service_ids = share_db.get_remote_invite_services(row["invite_id"])
                        self._sessions[session_id] = rehydrated
                        session = rehydrated
                except Exception:
                    logger.exception(
                        "[tunnel] failed to rehydrate session %s on demand",
                        session_id[:8] if session_id else "",
                    )
            if session is None:
                return None
            now = datetime.now(UTC)
            login = parse_iso_utc(session.login_time)
            last = parse_iso_utc(session.last_active_time)
            if login is None or last is None:
                self._evict(session, reason="invalid timestamp", event="SESSION_TIMEOUT")
                return None
            if (now - login).total_seconds() > ABSOLUTE_TIMEOUT_S:
                self._evict(session, reason="24h absolute lifetime", event="SESSION_TIMEOUT")
                return None
            if (now - last).total_seconds() > IDLE_TIMEOUT_S:
                self._evict(session, reason="2h idle", event="SESSION_TIMEOUT")
                return None

            invite = share_db.get_remote_invite(session.invite_id)
            if invite is None or invite.get("revoked"):
                self._evict(session, reason="invite revoked or removed", event="SESSION_BOOT")
                return None
            if invite.get("expires_at") and invite["expires_at"] < iso_z_now():
                self._evict(session, reason="invite expired", event="SESSION_TIMEOUT")
                return None

            # Re-sync mutable permission fields from the current invite (see
            # docstring): without this, tightening an analyst's permissions
            # mid-session would not take effect until natural timeout.
            #
            # F014: when an admin EXPLICITLY revokes by setting pii_policy
            # or service_ids to None on the invite, the prior
            # ``is not None`` guards silently dropped the revocation and
            # the session kept the cached permissive values until the
            # 2h-idle / 24h-absolute timeout. Treat key-presence as the
            # signal so an explicit null propagates: service_ids None
            # becomes the empty list (no services accessible);
            # pii_policy None disables masking (matches the schema's "no
            # policy set" meaning, and is the safer of the two
            # interpretations because masking is additive).
            if "pii_policy" in invite:
                session.pii_policy = invite["pii_policy"]
            session.query_window_hours = invite.get("query_window_hours")
            session.query_start_time = invite.get("query_start_time")
            session.query_end_time = invite.get("query_end_time")
            if "service_ids" in invite:
                fresh_service_ids = invite["service_ids"]
                session.service_ids = list(fresh_service_ids) if fresh_service_ids is not None else []
            # Auth-method display fields aren't persisted to remote_sessions, so
            # a session rehydrated after a restart defaults to 'passcode' — re-sync
            # from the invite here so the Sessions tab is accurate on next request.
            session.auth_method = invite.get("auth_method", "passcode")
            session.oauth_provider = invite.get("oauth_provider")

            tos = share_db.get_latest_tos()
            session.tos_pending = bool(
                tos and (invite.get("tos_accepted_at") is None or (invite.get("tos_version") or "") != tos["version"])
            )
            return session

    def boot_session(self, session_id: str, *, reason: str = "admin boot") -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return False
            self._evict(session, reason=reason, event="SESSION_BOOT")
            return True

    def rotate_session_id(self, session_id: str) -> str | None:
        """Mint a fresh ``session_id`` for an existing session and persist it.

        Used at the TOS-acceptance boundary so the cookie value an attacker
        could have observed pre-acceptance (during the TOS-pending window)
        can no longer be replayed once the session is fully scoped. Returns
        the new session_id, or None if the old id no longer maps to a live
        session (caller should treat that as "the session expired between
        validate and rotate, ask the user to log in again").

        The session row is the same logical session — same invite, same
        login_time, same fingerprint, same IP whitelist — only the opaque
        identifier changes.
        """
        with self._lock:
            session = self._sessions.pop(session_id, None)
            if session is None:
                return None
            try:
                share_db.delete_session(session_id)
            except Exception:
                logger.exception("[tunnel] failed to delete old session row on rotate")
            new_id = secrets.token_urlsafe(32)
            session.session_id = new_id
            self._sessions[new_id] = session
            try:
                share_db.upsert_session(session.to_dict())
            except Exception:
                logger.exception("[tunnel] failed to persist rotated session")
            try:
                share_db.log_share_audit_event(
                    event_type="SESSION_ROTATED",
                    email=session.email,
                    ip_address=session.ip_address,
                    details="TOS-acceptance rotation",
                )
            except Exception:
                logger.exception("[tunnel] failed to write audit log on rotate")
            return new_id

    def boot_sessions_for_invite(self, invite_id: str, *, reason: str = "invite revoked") -> int:
        n = 0
        with self._lock:
            for sid in [s.session_id for s in self._sessions.values() if s.invite_id == invite_id]:
                if self.boot_session(sid, reason=reason):
                    n += 1
        return n

    def _evict(self, session: AnalystSession, *, reason: str, event: str) -> None:
        # Called under self._lock.
        self._sessions.pop(session.session_id, None)
        try:
            share_db.delete_session(session.session_id)
        except Exception:
            logger.exception("[tunnel] failed to delete session row")
        try:
            share_db.log_share_audit_event(
                event_type=event,
                email=session.email,
                ip_address=session.ip_address,
                details=reason,
            )
        except Exception:
            logger.exception("[tunnel] failed to write audit log on evict")

    # ── Rate limiter passthrough ──────────────────────────────────────

    def check_rate_limit(self, ip: str) -> tuple[bool, int]:
        return self._rate_limiter.is_locked(ip)

    def record_login_failure(self, ip: str, email: str | None) -> bool:
        triggered = self._rate_limiter.record_failure(ip)
        if triggered:
            try:
                share_db.log_share_audit_event(
                    event_type="LOCKOUT",
                    email=email,
                    ip_address=ip,
                    details=f"too many failures within {LOGIN_FAILURE_WINDOW_S}s",
                )
            except Exception:
                logger.exception("[tunnel] failed to write LOCKOUT audit log")
        return triggered

    # ── Direct-mode share state ───────────────────────────────────────

    @property
    def state(self) -> TunnelState:
        with self._lock:
            return self._state

    def is_sharing_active(self) -> bool:
        with self._lock:
            return bool(self._state.public_endpoint)

    def record_heartbeat_unauth(self) -> None:
        """Increment the heartbeat-rejection counter (called from /heartbeat 401)."""
        with self._lock:
            self._heartbeat_unauth_count += 1

    def get_rate_limit_snapshot(self) -> dict:
        return self._rate_limiter.snapshot()

    def get_telemetry(self) -> dict:
        """Snapshot of in-memory observability counters for the admin UI."""
        with self._lock:
            current_uptime_s: int | None = None
            if self._state.started_at:
                started = parse_iso_utc(self._state.started_at)
                current_uptime_s = (
                    max(0, int((datetime.now(UTC) - started).total_seconds())) if started is not None else None
                )
            return {
                "heartbeat_unauth_count": self._heartbeat_unauth_count,
                "current_uptime_s": current_uptime_s,
                "tunnel_uptime_history": list(self._tunnel_uptime_history[-20:]),
            }

    def public_url(self) -> str | None:
        with self._lock:
            return self._state.public_endpoint

    def start_sharing(
        self,
        *,
        public_endpoint: str | None = None,
        forward_port: int = 3000,
    ) -> dict:
        """Start direct-mode sharing.

        Validates that ``public_endpoint`` is HTTPS (analyst cookies require
        ``secure=True``). Persists state so a backend restart re-arms the
        endpoint automatically.
        """
        if not public_endpoint:
            raise ValueError(
                "public_endpoint is required — provide either a hostname "
                "(https://logs.example.com) or an IP "
                "(https://203.0.113.42:8443)."
            )
        if not public_endpoint.lower().startswith("https://"):
            raise ValueError(
                "public_endpoint must use HTTPS — analyst cookies require secure=True. "
                "Front your hostname with TLS (Caddy, Cloudflare, Let's Encrypt) or, for "
                "IP-only mode, serve a self-signed cert."
            )

        with self._lock:
            self._state.public_endpoint = public_endpoint
            self._state.forward_port = forward_port
            self._state.started_at = iso_z_now()
            # Persist so a backend restart re-arms automatically.
            persist_direct_state(self._state)

        try:
            share_db.log_share_audit_event(
                event_type="SHARE_START",
                email=None,
                ip_address="127.0.0.1",
                details=f"port={forward_port} endpoint={public_endpoint!r}",
            )
        except Exception:
            logger.exception("[tunnel] failed to write SHARE_START audit")
        return {"public_url": self.public_url()}

    def stop_sharing(self) -> None:
        with self._lock:
            self._record_uptime_history(reason="stop")
            self._state.public_endpoint = None
            self._state.started_at = None
            # Clear persisted state so a restart doesn't re-arm.
            clear_persisted_state()
            # Boot all sessions.
            ids = list(self._sessions.keys())
            for sid in ids:
                self.boot_session(sid, reason="sharing stopped")
        try:
            share_db.log_share_audit_event(
                event_type="SHARE_STOP",
                email=None,
                ip_address="127.0.0.1",
                details=f"sessions booted: {len(ids)}",
            )
        except Exception:
            logger.exception("[tunnel] failed to write SHARE_STOP audit")

    def panic(self) -> dict:
        with self._lock:
            n = len(self._sessions)
            self._record_uptime_history(reason="panic")
            for sid in list(self._sessions.keys()):
                self.boot_session(sid, reason="panic")
            self._state.public_endpoint = None
            self._state.started_at = None
            clear_persisted_state()
        try:
            share_db.log_share_audit_event(
                event_type="PANIC_TRIGGERED",
                email=None,
                ip_address="127.0.0.1",
                details=f"booted {n} sessions",
            )
        except Exception:
            logger.exception("[tunnel] failed to write PANIC audit")
        return {"sessions_booted": n}

    def _record_uptime_history(self, *, reason: str) -> None:
        """Append a completed-share duration to in-memory history. Caller holds lock."""
        if not self._state.started_at:
            return
        try:
            started = parse_iso_utc(self._state.started_at)
            if started is None:
                return
            ended = datetime.now(UTC)
            self._tunnel_uptime_history.append(
                {
                    "started_at": self._state.started_at,
                    "ended_at": iso_z_now(),
                    "duration_s": max(0, int((ended - started).total_seconds())),
                    "reason": reason,
                }
            )
            # Bounded — keep last 50 sessions in memory.
            if len(self._tunnel_uptime_history) > 50:
                self._tunnel_uptime_history = self._tunnel_uptime_history[-50:]
        except Exception:
            logger.exception("[tunnel] could not record uptime history")


# ── Singleton accessor ──────────────────────────────────────────────────────

_singleton: TunnelManager | None = None
_singleton_lock = threading.Lock()


def get_tunnel_manager() -> TunnelManager:
    global _singleton
    if _singleton is None:
        with _singleton_lock:
            if _singleton is None:
                _singleton = TunnelManager()
    return _singleton


def build_share_live_payload() -> dict:
    """Compose the lean share-dashboard "live" payload — one source of truth
    shared by the ``/api/admin/share/live`` poll endpoint and the multiplexed
    admin event stream's ``share`` channel (``backend/routers/admin/events.py``).

    Lives here (not in the share router) so the admin-events router can import
    it without an import-linter ``routers.admin -> routers.share_admin`` edge.
    Pure in-memory tunnel-manager getters — no SQLite / DuckDB / FOS — so it's
    cheap enough to sample per connection."""
    mgr = get_tunnel_manager()
    return {
        "sharing_active": mgr.is_sharing_active(),
        "public_url": mgr.public_url(),
        "active_session_count": mgr.active_session_count(),
        "rate_limits": mgr.get_rate_limit_snapshot(),
        "telemetry": mgr.get_telemetry(),
    }


def reset_for_tests() -> None:
    """Drop the singleton so each test starts fresh.

    Does NOT touch share_db persistence — ``restart`` tests need the
    persisted rows to still exist after the in-memory singleton is
    dropped (the autouse share-db fixture handles DB-level isolation by
    pointing at a fresh tmp_path each test).
    """
    global _singleton
    with _singleton_lock:
        _singleton = None
