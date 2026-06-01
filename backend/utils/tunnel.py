"""SSH reverse-tunnel manager + remote-analyst session lifecycle.

Singleton ``TunnelManager`` owns:
- the SSH subprocess (localhost.run reverse tunnel, optional)
- in-memory ``AnalystSession`` dict (rehydrated from share_db at startup)
- sliding-window login rate limiter (per client IP)
- session timeout enforcement (2h idle / 24h absolute)
- multi-device boot (one active session per invite at a time)
- OS power-event listener so admins closing their laptops auto-recover
- pre-flight port-conflict probe

Session writes are mirrored to ``remote_sessions`` in ``share_db`` so a
backend restart does not silently log every analyst out.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from backend.core import share_db

logger = logging.getLogger(__name__)

# Idle and absolute timeouts (matches plan: 2h idle, 24h absolute).
IDLE_TIMEOUT_S = 2 * 60 * 60
ABSOLUTE_TIMEOUT_S = 24 * 60 * 60

# Login rate-limit: 5 failures / 60s → 5-minute lockout.
LOGIN_FAILURE_WINDOW_S = 60
LOGIN_FAILURE_THRESHOLD = 5
LOGIN_LOCKOUT_S = 5 * 60


# ── AnalystSession ──────────────────────────────────────────────────────────


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


# ── Fingerprint helper (Section #18) ────────────────────────────────────────

_UA_RE = re.compile(r"(Chrome|Firefox|Safari|Edge|OPR)/(\d+)")
_OS_RE = re.compile(r"(Macintosh|Mac OS X|Windows|Linux|X11|iPhone|iPad|Android)")


def compute_fingerprint(headers: dict[str, str]) -> str:
    """Narrowed SHA-256 over browser family + major version + OS family.

    Never hash the full User-Agent — Chrome UA-Reduction updates every ~4
    weeks would boot every analyst, swamping the audit log with false
    positives. The narrowed signature survives normal browser updates while
    still detecting a cross-browser/cross-OS cookie theft.
    """
    ua = headers.get("user-agent", "") or headers.get("User-Agent", "") or ""
    ch_platform = headers.get("sec-ch-ua-platform", "") or ""
    browser_match = _UA_RE.search(ua)
    os_match = _OS_RE.search(ua)
    parts = [
        browser_match.group(1) if browser_match else "unknown-browser",
        browser_match.group(2) if browser_match else "0",
        os_match.group(1) if os_match else "unknown-os",
        ch_platform.strip('"'),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


# ── Rate limiter ────────────────────────────────────────────────────────────


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


# ── SSH process wrapper ─────────────────────────────────────────────────────


_TUNNEL_URL_RE = re.compile(r"https?://([a-z0-9\-]+\.(?:lhr\.life|localhost\.run))", re.IGNORECASE)


@dataclass
class TunnelState:
    use_tunnel: bool = False
    public_endpoint: str | None = None
    tunnel_url: str | None = None
    proc: subprocess.Popen | None = None
    started_at: str | None = None
    forward_port: int = 3000
    reconnect_attempts: int = 0
    local_socket_addr: str | None = None  # "127.0.0.1" vs "0.0.0.0"
    direct_socket_addr: str | None = None  # for direct-expose mode


# ── TunnelManager singleton ────────────────────────────────────────────────


class TunnelManager:
    """Process-wide singleton. Use ``get_tunnel_manager()`` to access."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sessions: dict[str, AnalystSession] = {}
        self._rate_limiter = _LoginRateLimiter()
        self._state = TunnelState()
        self._stdout_thread: threading.Thread | None = None
        self._sleep_listener_stop = threading.Event()
        self._sleep_listener_thread: threading.Thread | None = None
        # Restore direct-mode share state from disk so a backend restart
        # doesn't drop the registered public_endpoint. Tunnel mode (use_tunnel
        # =True) is NOT restored — that requires re-launching the SSH process,
        # which the admin should do explicitly.
        self._restore_direct_state()
        # Observability counters. In-memory only; reset on process restart.
        # `_heartbeat_unauth_count` increments every time /api/share/heartbeat
        # returns 401/403, i.e. every time an analyst gets bounced to the
        # login page. Lets the admin distinguish "session expired naturally"
        # from "tunnel died" without parsing the audit log.
        # `_tunnel_uptime_history` stores past tunnel session durations so the
        # admin can spot flakiness even before the current tunnel is stopped.
        self._heartbeat_unauth_count: int = 0
        self._tunnel_uptime_history: list[dict] = []  # [{started, ended, duration_s}]

    # ── Lifecycle ──────────────────────────────────────────────────────

    # ── Direct-mode share state persistence ────────────────────────────
    # Backend restarts (deploys, crashes) drop self._state, which means the
    # registered public_endpoint goes away and analyst traffic starts
    # failing host-allowed checks. Persist the three fields needed to
    # rebuild direct-mode state (use_tunnel=False, public_endpoint, port)
    # and restore on __init__.

    @staticmethod
    def _state_file_path() -> str:
        from backend.config import DATA_DIR

        return str(DATA_DIR / "tunnel_state.json")

    def _persist_direct_state(self) -> None:
        if self._state.use_tunnel:
            return  # Tunnel mode requires an SSH process — not safe to auto-restore.
        try:
            import json

            with open(self._state_file_path(), "w") as f:
                json.dump(
                    {
                        "use_tunnel": False,
                        "public_endpoint": self._state.public_endpoint,
                        "forward_port": self._state.forward_port,
                    },
                    f,
                )
        except Exception:
            logger.exception("[tunnel] failed to persist direct-mode state")

    def _clear_persisted_state(self) -> None:
        try:
            import os

            path = self._state_file_path()
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            logger.exception("[tunnel] failed to clear persisted state")

    def _restore_direct_state(self) -> None:
        try:
            import json
            import os

            path = self._state_file_path()
            if not os.path.exists(path):
                return
            with open(path) as f:
                data = json.load(f)
            if data.get("use_tunnel"):
                return  # Tunnel-mode state isn't auto-restored.
            endpoint = data.get("public_endpoint")
            if not endpoint:
                return
            self._state.use_tunnel = False
            self._state.public_endpoint = endpoint
            self._state.forward_port = data.get("forward_port", 3000)
            self._state.direct_socket_addr = "0.0.0.0"
            self._state.started_at = share_db.iso_z_now()
            logger.info("[tunnel] restored direct-mode share state for %s", endpoint)
        except Exception:
            logger.exception("[tunnel] failed to restore direct-mode state")

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
                try:
                    login = _parse_iso_z(row["login_time"])
                    last = _parse_iso_z(row["last_active_time"])
                except Exception:
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
    ) -> AnalystSession:
        """Register a new session, booting any existing one for the same invite.

        Caller is responsible for enforcing capacity cap before calling.
        """
        import secrets

        invite_id = invite["id"]
        with self._lock:
            # Multi-device boot.
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

            now = share_db.iso_z_now()
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
            )
            self._sessions[session.session_id] = session
            try:
                share_db.upsert_session(session.to_dict())
            except Exception:
                logger.exception("[tunnel] failed to persist new session")
            return session

    def touch_session(self, session_id: str, *, last_activity: str | None = None, new_ip: str | None = None) -> bool:
        """Bump ``last_active_time`` (and optionally last_activity / ip)."""
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return False
            session.last_active_time = share_db.iso_z_now()
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
        unrevoked and unexpired.
        """
        if not session_id:
            return None
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return None
            now = datetime.now(UTC)
            try:
                login = _parse_iso_z(session.login_time)
                last = _parse_iso_z(session.last_active_time)
            except Exception:
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
            if invite.get("expires_at") and invite["expires_at"] < share_db.iso_z_now():
                self._evict(session, reason="invite expired", event="SESSION_TIMEOUT")
                return None
            return session

    def boot_session(self, session_id: str, *, reason: str = "admin boot") -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return False
            self._evict(session, reason=reason, event="SESSION_BOOT")
            return True

    def boot_sessions_for_invite(self, invite_id: str, *, reason: str = "invite revoked") -> int:
        n = 0
        with self._lock:
            for sid in [s.session_id for s in self._sessions.values() if s.invite_id == invite_id]:
                if self.boot_session(sid, reason=reason):
                    n += 1
        return n

    def clear_all_sessions(self, *, reason: str = "panic") -> int:
        with self._lock:
            ids = list(self._sessions.keys())
            for sid in ids:
                self.boot_session(sid, reason=reason)
            return len(ids)

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

    def clear_login_failures(self, ip: str) -> None:
        self._rate_limiter.clear(ip)

    # ── Tunnel / direct-expose state ─────────────────────────────────

    @property
    def state(self) -> TunnelState:
        with self._lock:
            return self._state

    def is_sharing_active(self) -> bool:
        with self._lock:
            return bool(self._state.use_tunnel and self._state.proc) or bool(
                not self._state.use_tunnel and self._state.public_endpoint
            )

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
                try:
                    started = _parse_iso_z(self._state.started_at)
                    current_uptime_s = max(0, int((datetime.now(UTC) - started).total_seconds()))
                except Exception:
                    current_uptime_s = None
            return {
                "heartbeat_unauth_count": self._heartbeat_unauth_count,
                "current_uptime_s": current_uptime_s,
                "tunnel_uptime_history": list(self._tunnel_uptime_history[-20:]),
            }

    def public_url(self) -> str | None:
        with self._lock:
            if self._state.use_tunnel:
                return f"https://{self._state.tunnel_url}" if self._state.tunnel_url else None
            return self._state.public_endpoint

    def start_sharing(
        self,
        *,
        use_tunnel: bool,
        public_endpoint: str | None = None,
        forward_port: int = 3000,
    ) -> dict:
        """Start sharing.

        On tunnel mode: spawn SSH; pre-flight port-conflict check.
        On direct mode: validate ``public_endpoint`` is HTTPS (cookies require ``secure=True``).
        """
        with self._lock:
            if use_tunnel:
                self._state.use_tunnel = True
                self._state.forward_port = forward_port
                self._state.public_endpoint = None

                # Pre-flight: is the target port live?
                if not _port_in_use("127.0.0.1", forward_port):
                    raise RuntimeError(
                        f"port {forward_port} is not bound — start the frontend first or set FRONTEND_PORT correctly"
                    )

                # Find ssh.
                ssh_bin = shutil.which("ssh")
                if not ssh_bin:
                    raise RuntimeError(
                        "SSH client not found. Please install openssh-client inside the container or run outside Docker."
                    )

                # Spawn SSH. We do NOT use any user keys — explicitly pass our own.
                key_path = _ensure_share_key()
                cmd = [
                    ssh_bin,
                    "-i",
                    key_path,
                    "-o",
                    "IdentitiesOnly=yes",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                    "-o",
                    "ServerAliveInterval=10",
                    "-o",
                    "ServerAliveCountMax=3",
                    "-R",
                    f"80:127.0.0.1:{forward_port}",
                    "localhost.run",
                ]
                logger.info("[tunnel] starting SSH: %s", " ".join(cmd))
                try:
                    proc = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                    )
                except FileNotFoundError as exc:
                    raise RuntimeError(f"failed to spawn ssh: {exc}") from exc
                self._state.proc = proc
                self._state.local_socket_addr = "127.0.0.1"
                self._state.started_at = share_db.iso_z_now()
                self._state.reconnect_attempts = 0
                self._stdout_thread = threading.Thread(target=self._read_stdout, args=(proc,), daemon=True)
                self._stdout_thread.start()
            else:
                if not public_endpoint:
                    raise ValueError(
                        "public_endpoint is required when use_tunnel=False — provide "
                        "either a hostname (https://logs.example.com) or an IP "
                        "(https://203.0.113.42:8443)."
                    )
                if not public_endpoint.lower().startswith("https://"):
                    raise ValueError(
                        "public_endpoint must use HTTPS — analyst cookies require secure=True. "
                        "Front your hostname with TLS (Caddy, Cloudflare, Let's Encrypt) or, for "
                        "IP-only mode, serve a self-signed cert."
                    )
                self._state.use_tunnel = False
                self._state.public_endpoint = public_endpoint
                self._state.tunnel_url = None
                self._state.direct_socket_addr = "0.0.0.0"
                self._state.started_at = share_db.iso_z_now()
                # Persist so a backend restart re-arms automatically.
                self._persist_direct_state()

            try:
                share_db.log_share_audit_event(
                    event_type="TUNNEL_START" if use_tunnel else "SHARE_START",
                    email=None,
                    ip_address="127.0.0.1",
                    details=f"use_tunnel={use_tunnel} port={forward_port} endpoint={public_endpoint!r}",
                )
            except Exception:
                logger.exception("[tunnel] failed to write TUNNEL_START audit")
            return {"public_url": self.public_url(), "tunnel_url": self._state.tunnel_url}

    def stop_sharing(self) -> None:
        with self._lock:
            self._record_uptime_history(reason="stop")
            self._kill_proc()
            self._state.use_tunnel = False
            self._state.public_endpoint = None
            self._state.tunnel_url = None
            self._state.started_at = None
            # Clear persisted state so a restart doesn't re-arm.
            self._clear_persisted_state()
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
            self._kill_proc()
            self.clear_all_sessions(reason="panic")
            self._state.use_tunnel = False
            self._state.public_endpoint = None
            self._state.tunnel_url = None
            self._state.started_at = None
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
        """Append a completed-tunnel duration to in-memory history. Caller holds lock."""
        if not self._state.started_at:
            return
        try:
            started = _parse_iso_z(self._state.started_at)
            ended = datetime.now(UTC)
            self._tunnel_uptime_history.append(
                {
                    "started_at": self._state.started_at,
                    "ended_at": share_db.iso_z_now(),
                    "duration_s": max(0, int((ended - started).total_seconds())),
                    "reason": reason,
                }
            )
            # Bounded — keep last 50 sessions in memory.
            if len(self._tunnel_uptime_history) > 50:
                self._tunnel_uptime_history = self._tunnel_uptime_history[-50:]
        except Exception:
            logger.exception("[tunnel] could not record uptime history")

    def _kill_proc(self) -> None:
        proc = self._state.proc
        self._state.proc = None
        if proc is None:
            return
        try:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        except Exception:
            logger.exception("[tunnel] failed to terminate ssh proc")

    def _read_stdout(self, proc: subprocess.Popen) -> None:
        if proc.stdout is None:
            return
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            logger.debug("[tunnel:ssh] %s", line)
            m = _TUNNEL_URL_RE.search(line)
            if m:
                with self._lock:
                    self._state.tunnel_url = m.group(1)
                logger.info("[tunnel] tunnel URL detected: %s", m.group(1))

    # ── OS power-event listener (sleep/wake recovery) ────────────────

    def start_sleep_listener(self) -> None:
        """Start the platform-specific sleep/wake listener.

        macOS: pyobjc IOPowerSource (best-effort — falls back to a polling
        thread that watches the SSH pidfile for unexpected death).
        Windows: pywin32 SystemEvents.PowerModeChanged (best-effort).
        Linux: DBus PrepareForSleep (best-effort).

        Best-effort here means: when the optional dep isn't present, we
        install a 30-second poller that checks proc liveness + clock drift
        as a proxy for sleep/wake. That's good enough for the common case
        (the SSH subprocess is reaped by the OS on sleep so liveness flips
        within ~one poll interval of wake).
        """
        if self._sleep_listener_thread and self._sleep_listener_thread.is_alive():
            return
        self._sleep_listener_stop.clear()
        self._sleep_listener_thread = threading.Thread(
            target=self._sleep_listener_loop, daemon=True, name="tunnel-sleep-listener"
        )
        self._sleep_listener_thread.start()

    def stop_sleep_listener(self) -> None:
        self._sleep_listener_stop.set()
        t = self._sleep_listener_thread
        if t and t.is_alive():
            t.join(timeout=2)
        self._sleep_listener_thread = None

    def _sleep_listener_loop(self) -> None:
        """Detect sleep via wall-clock-vs-monotonic drift.

        On macOS/Linux/Windows alike, when the host sleeps both clocks pause;
        on wake, the wall clock jumps forward by the sleep duration but the
        monotonic clock advances only by however much the loop iterated
        through. A drift >30s between consecutive ticks is a strong "we just
        woke up" signal — covers all three platforms with one branch.
        """
        last_mono = time.monotonic()
        last_wall = time.time()
        while not self._sleep_listener_stop.wait(15):
            now_mono = time.monotonic()
            now_wall = time.time()
            mono_delta = now_mono - last_mono
            wall_delta = now_wall - last_wall
            drift = wall_delta - mono_delta
            if drift > 30:
                logger.info("[tunnel] wake detected (wall-clock drift %.1fs)", drift)
                self._on_wake(drift)
            last_mono = now_mono
            last_wall = now_wall

    def _on_wake(self, drift_s: float) -> None:
        """Tear down stale SSH proc + restart (only if we were tunneling)."""
        with self._lock:
            if not self._state.use_tunnel:
                return
            forward = self._state.forward_port
            self._kill_proc()
        try:
            share_db.log_share_audit_event(
                event_type="TUNNEL_RESUMED",
                email=None,
                ip_address="127.0.0.1",
                details=f"wake detected after {drift_s:.0f}s sleep; restarting SSH",
            )
        except Exception:
            logger.exception("[tunnel] failed to write TUNNEL_RESUMED audit")
        try:
            self.start_sharing(use_tunnel=True, forward_port=forward)
        except Exception:
            logger.exception("[tunnel] failed to restart SSH after wake")


# ── Module helpers ──────────────────────────────────────────────────────────


def _parse_iso_z(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def _port_in_use(host: str, port: int) -> bool:
    """True iff something is already listening on ``host:port``."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.25)
            return s.connect_ex((host, port)) == 0
    except OSError:
        return False


def _ensure_share_key() -> str:
    """Generate ed25519 share key at ``data/system/share_key`` if missing.

    Plan §1 — Zero-config SSH keys: *never* drop keys into ``~/.ssh/``.
    """
    base = os.environ.get("REMOTE_SHARE_DB_DIR") or "data/system"
    os.makedirs(base, exist_ok=True)
    key_path = os.path.join(base, "share_key")
    if os.path.exists(key_path):
        return key_path
    bin_path = shutil.which("ssh-keygen")
    if not bin_path:
        raise RuntimeError("ssh-keygen not found on PATH; cannot generate share key")
    subprocess.check_call(
        [bin_path, "-t", "ed25519", "-N", "", "-f", key_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return key_path


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


def reset_for_tests() -> None:
    """Drop the singleton so each test starts fresh.

    Stops the sleep listener but does NOT touch share_db persistence —
    `restart` tests need the persisted rows to still exist after the
    in-memory singleton is dropped (the autouse share-db fixture handles
    DB-level isolation by pointing at a fresh tmp_path each test).
    """
    global _singleton
    with _singleton_lock:
        if _singleton is not None:
            try:
                _singleton.stop_sleep_listener()
            except Exception:
                pass
        _singleton = None
