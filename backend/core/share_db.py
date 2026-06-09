"""Global remote-share SQLite store.

Singleton DB at ``data/system/remote_share.db`` holding remote-analyst
invitations, service scopes, audit logs, share settings, persisted analyst
sessions, one-time claim tokens, and TOS versions.

Distinct from ``backend.core.metadata_db`` (per-service operational state)
intentionally: different lifecycle (one file, app-global), different lock
contention pattern, different audit scope (security material).

Concurrency: thread-local pooled connection. ``PRAGMA foreign_keys=ON`` is
re-asserted on every open (SQLite resets it per-connection). WAL +
``synchronous=NORMAL`` matches our production metadata DB standard.

Corruption self-heal: ``get_safe_share_db_connection`` catches only
open-time ``sqlite3.DatabaseError`` and quarantines the corrupt file. Query
time exceptions surface normally — catching them would mask real bugs.

Migrations: a private ``MIGRATIONS`` dict with its own integer key sequence,
applied via ``apply_pending(con)`` on first open. Uses ``PRAGMA user_version``
on this file (the per-service framework's user_version lives in the per-service
files, so namespaces never collide).
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import logging
import os
import secrets
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)

# ── Locations ────────────────────────────────────────────────────────────────

_DATA_DIR = "data/system"
_DB_FILENAME = "remote_share.db"

_local = threading.local()
_init_lock = threading.Lock()
_initialized: set[str] = set()
_all_connections: list[sqlite3.Connection] = []
_all_connections_lock = threading.Lock()
# Maps id(con) -> quarantine path for connections that were rebuilt after
# corruption. Read once by _init_db and removed. sqlite3.Connection has no
# __dict__ so we can't tag the connection object directly.
_recovery_marker: dict[int, str] = {}


def db_path() -> str:
    """Absolute path to the global share DB file.

    Honors ``REMOTE_SHARE_DB_DIR`` for test isolation; defaults to
    ``data/system/remote_share.db``.
    """
    base = os.environ.get("REMOTE_SHARE_DB_DIR") or _DATA_DIR
    return os.path.join(base, _DB_FILENAME)


# ── Connection management ────────────────────────────────────────────────────


def _conn_pool() -> dict[str, sqlite3.Connection]:
    if not hasattr(_local, "conns"):
        _local.conns = {}
    return _local.conns


def get_safe_share_db_connection(path: str) -> sqlite3.Connection:
    """Open a connection to ``path``. On open-time corruption, quarantine the
    file aside and rebuild from scratch.

    Mirrors TESTING_PLAN_3 Item 1: ONLY catches ``sqlite3.DatabaseError``
    raised during open (e.g., "file is not a database"). Query-time errors
    are not handled here.
    """
    try:
        con = sqlite3.connect(path, timeout=30.0)
        # Force header read so a corrupt file fails here, not on first query.
        con.execute("SELECT 1").fetchone()
        return con
    except sqlite3.DatabaseError as exc:
        # Security: ``DatabaseError`` is the parent of
        # ``OperationalError``, which fires for transient conditions like
        # "database is locked" / "disk I/O error" / FD exhaustion. The
        # quarantine path renames the DB out from under any other open
        # connections AND wipes all share state — running it on a transient
        # error means a single lock-timeout under load can permanently
        # delete every invite, session, and audit row in the share DB.
        #
        # Restrict the quarantine to actual file-corruption signatures from
        # SQLite: "file is not a database" / "database disk image is malformed"
        # / "unsupported file format". Anything else (lock timeout, I/O error,
        # full disk, missing parent dir) is re-raised so the caller sees the
        # real error instead of silently nuking the DB.
        msg = str(exc).lower()
        is_corruption = (
            "malformed" in msg
            or "not a database" in msg
            or "unsupported file format" in msg
            or "image is malformed" in msg
        )
        if not is_corruption:
            # ERROR (not WARNING) so this near-miss is alertable from the
            # existing log-error monitoring without needing a new metric
            # plumbing — quarantine-skipped events should be rare; if we
            # start seeing them at volume it's a signal that the
            # is_corruption substrings need updating.
            logger.error(
                "[share_db] DatabaseError on open of %s NOT classified as corruption (err_type=%s); re-raising: %s",
                path,
                type(exc).__name__,
                exc,
            )
            raise

        epoch = int(time.time())
        corrupt_path = f"{path}.corrupt-{epoch}"
        try:
            os.replace(path, corrupt_path)
            logger.error(
                "[share_db] corrupt DB at %s quarantined to %s (reason=corruption, %s)",
                path,
                corrupt_path,
                exc,
            )
        except OSError:
            logger.exception("[share_db] failed to quarantine corrupt DB at %s", path)
            raise
        con = sqlite3.connect(path, timeout=30.0)
        # Write a recovery marker once schema is initialized — caller does that
        # in _init_db. sqlite3.Connection has no __dict__, so we keep the
        # mapping out-of-band keyed by id(con).
        _recovery_marker[id(con)] = corrupt_path
        return con


def get_global_share_con() -> sqlite3.Connection:
    """Return a thread-local connection to the global share DB."""
    pool = _conn_pool()
    con = pool.get("__global_share__")
    if con is not None:
        # Re-assert per-connection PRAGMA on every borrow — SQLite resets it
        # if anyone toggles it during the connection's lifetime.
        try:
            con.execute("PRAGMA foreign_keys=ON")
        except sqlite3.ProgrammingError:
            # closed; fall through to reopen.
            pool.pop("__global_share__", None)
            con = None
        else:
            return con

    path = db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if not _init_lock.acquire(timeout=10):
        raise sqlite3.OperationalError(
            "share_db._init_lock contended >10s — another thread is stuck inside connect+PRAGMA"
        )
    try:
        con = get_safe_share_db_connection(path)
        with _all_connections_lock:
            _all_connections.append(con)
        try:
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA foreign_keys=ON")
            con.execute("PRAGMA busy_timeout=30000")
            # 64MB page cache — keeps the share-flow's invite/session
            # lookups + audit-log writes hot in memory under concurrent
            # heartbeat polling from multiple analysts. Architecture-
            # review Dimension 2.
            con.execute("PRAGMA cache_size=-64000")

            if path not in _initialized:
                _init_db(con)
                _initialized.add(path)
        except Exception:
            try:
                con.close()
            except Exception:
                pass
            raise
    finally:
        _init_lock.release()

    pool["__global_share__"] = con
    return con


def close_all_connections() -> None:
    """Close every open share DB connection. Used by test fixtures."""
    with _all_connections_lock:
        for con in _all_connections:
            try:
                con.close()
            except Exception:
                pass
        _all_connections.clear()
    if hasattr(_local, "conns"):
        _local.conns.pop("__global_share__", None)


def reset_for_tests() -> None:
    """Drop the in-memory init cache so the next ``get_global_share_con`` rebuilds.

    Pytest fixtures that swap ``REMOTE_SHARE_DB_DIR`` per-test rely on this to
    avoid carrying over a connection bound to the previous test's path.
    """
    close_all_connections()
    _initialized.clear()


# ── Schema + migrations ──────────────────────────────────────────────────────


def _init_db(con: sqlite3.Connection) -> None:
    """Create schema from the latest snapshot, then apply migrations forward.

    Idempotent: ``CREATE ... IF NOT EXISTS`` on every statement plus
    ``apply_pending`` which is itself idempotent.
    """
    for stmt in _SCHEMA:
        con.execute(stmt)
    con.commit()
    apply_pending(con)

    # If the connection was rebuilt by ``get_safe_share_db_connection`` after
    # quarantining a corrupt file, write a single recovery audit row.
    corrupt_from = _recovery_marker.pop(id(con), None)
    if corrupt_from:
        log_share_audit_event(
            event_type="SHARE_DB_RECOVERED",
            email=None,
            ip_address="127.0.0.1",
            details=f"previous file quarantined to {corrupt_from}",
            con=con,
        )


_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS remote_invites (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        passcode TEXT NOT NULL,
        expires_at TEXT,
        ip_whitelist TEXT,
        pii_policy TEXT NOT NULL DEFAULT '{"mask_ips": false}',
        query_window_hours INTEGER,
        query_start_time TEXT,
        query_end_time TEXT,
        created_at TEXT NOT NULL,
        revoked INTEGER NOT NULL DEFAULT 0,
        tos_accepted_at TEXT,
        tos_version TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_remote_invites_email ON remote_invites(email)",
    """CREATE TABLE IF NOT EXISTS invite_services (
        invite_id TEXT NOT NULL,
        service_id TEXT NOT NULL,
        PRIMARY KEY (invite_id, service_id),
        FOREIGN KEY (invite_id) REFERENCES remote_invites(id) ON DELETE CASCADE
    )""",
    "CREATE INDEX IF NOT EXISTS idx_invite_services_invite_id ON invite_services(invite_id)",
    """CREATE TABLE IF NOT EXISTS remote_share_audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        event_type TEXT NOT NULL,
        email TEXT,
        ip_address TEXT NOT NULL,
        details TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_remote_share_audit_logs_timestamp ON remote_share_audit_logs(timestamp)",
    """CREATE TABLE IF NOT EXISTS share_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS remote_sessions (
        session_id TEXT PRIMARY KEY,
        invite_id TEXT NOT NULL,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        ip_address TEXT NOT NULL,
        user_agent TEXT NOT NULL,
        fingerprint_signature TEXT NOT NULL,
        pii_policy TEXT NOT NULL,
        query_window_hours INTEGER,
        query_start_time TEXT,
        query_end_time TEXT,
        login_time TEXT NOT NULL,
        last_active_time TEXT NOT NULL,
        last_activity TEXT,
        FOREIGN KEY (invite_id) REFERENCES remote_invites(id) ON DELETE CASCADE
    )""",
    """CREATE TABLE IF NOT EXISTS remote_invite_claim_tokens (
        token TEXT PRIMARY KEY,
        invite_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        claimed_at TEXT,
        claimed_from_ip TEXT,
        FOREIGN KEY (invite_id) REFERENCES remote_invites(id) ON DELETE CASCADE
    )""",
    """CREATE TABLE IF NOT EXISTS share_tos_versions (
        version TEXT PRIMARY KEY,
        text TEXT NOT NULL,
        published_at TEXT NOT NULL
    )""",
]


def _migration_001_seed_default_settings(con: sqlite3.Connection) -> None:
    """Seed default ``max_concurrent_analyst_sessions=10`` if unset."""
    row = con.execute("SELECT 1 FROM share_settings WHERE key=?", ("max_concurrent_analyst_sessions",)).fetchone()
    if row is None:
        con.execute(
            "INSERT INTO share_settings(key, value) VALUES(?, ?)",
            ("max_concurrent_analyst_sessions", "10"),
        )


def _migration_002_seed_initial_tos(con: sqlite3.Connection) -> None:
    """Seed the initial TOS text used by the acknowledgment gate."""
    row = con.execute("SELECT 1 FROM share_tos_versions WHERE version=?", ("v1",)).fetchone()
    if row is None:
        con.execute(
            "INSERT INTO share_tos_versions(version, text, published_at) VALUES(?, ?, ?)",
            (
                "v1",
                (
                    "I acknowledge that I am viewing third-party operational log data, "
                    "that my access is logged, and that I will not retain, redistribute, "
                    "or use this data outside the scope of my engagement."
                ),
                iso_z_now(),
            ),
        )


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _migration_001_seed_default_settings,
    2: _migration_002_seed_initial_tos,
}

LATEST_VERSION = max(MIGRATIONS) if MIGRATIONS else 0


def get_current_version(con: sqlite3.Connection) -> int:
    return con.execute("PRAGMA user_version").fetchone()[0]


def apply_pending(con: sqlite3.Connection) -> int:
    """Apply every migration whose version is greater than the file's ``user_version``."""
    current = get_current_version(con)
    applied = 0
    for version in sorted(MIGRATIONS):
        if version <= current:
            continue
        func = MIGRATIONS[version]
        logger.info("[share_db] applying migration v%d (%s)", version, func.__name__)
        try:
            with con:
                func(con)
                con.execute(f"PRAGMA user_version = {version}")
            applied += 1
        except Exception:
            logger.exception("[share_db] migration v%d failed", version)
            raise
    return applied


# ── Time helpers ─────────────────────────────────────────────────────────────
# Handled via backend.utils.date_utils imports above to avoid duplication.


# ── Passcode hashing (constant-time scrypt) ─────────────────────────────────

_SCRYPT_N = 2**14
_SCRYPT_R = 8
_SCRYPT_P = 1
_SCRYPT_DKLEN = 32
_SALT_BYTES = 16


def hash_passcode(passcode: str) -> str:
    """Salted scrypt hash. Stored as ``scrypt$N$r$p$saltHex$digestHex``."""
    salt = secrets.token_bytes(_SALT_BYTES)
    dk = hashlib.scrypt(
        passcode.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=_SCRYPT_DKLEN,
    )
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${salt.hex()}${dk.hex()}"


def verify_passcode(passcode: str, stored: str) -> bool:
    """Constant-time scrypt verify."""
    try:
        parts = stored.split("$")
        if len(parts) != 6 or parts[0] != "scrypt":
            return False
        _, n, r, p, salt_hex, digest_hex = parts
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
        candidate = hashlib.scrypt(
            passcode.encode("utf-8"),
            salt=salt,
            n=int(n),
            r=int(r),
            p=int(p),
            dklen=len(expected),
        )
        return hmac.compare_digest(candidate, expected)
    except (ValueError, TypeError):
        return False


# ── Passcode entropy validation ──────────────────────────────────────────────

# A tiny seed list of obvious weak passcodes. Production should swap in a
# breached-list lookup (HIBP k-anonymity API or a downloaded RockYou snippet).
_BREACHED_TOP_LIST = {
    "password",
    "passw0rd",
    "letmein",
    "welcome",
    "admin",
    "iloveyou",
    "qwerty",
    "qwerty123",
    "abc123",
    "monkey",
    "dragon",
    "master",
    "sunshine",
    "princess",
    "football",
    "111111",
    "123123",
    "123456",
    "12345678",
    "1234567890",
    "000000",
    "trustno1",
    "starwars",
    "1q2w3e4r",
    "passwordpassword",
    "secret",
    "shadow",
}


class WeakPasscodeError(ValueError):
    """Raised by ``validate_passcode_strength`` for obvious weak inputs."""


def validate_passcode_strength(passcode: str) -> None:
    """Reject all-digit PINs, anything <10 chars, and breached-list matches.

    Raises ``WeakPasscodeError`` with a UI-ready message on failure. Successful
    return means the passcode passed the minimum bar.
    """
    if not passcode or len(passcode) < 10:
        raise WeakPasscodeError("passcode too weak — use the wordphrase generator instead (≥10 characters required)")
    if passcode.isdigit():
        raise WeakPasscodeError(
            "passcode too weak — use the wordphrase generator instead (all-digit PINs are rejected)"
        )
    if passcode.lower() in _BREACHED_TOP_LIST:
        raise WeakPasscodeError(
            "passcode too weak — use the wordphrase generator instead (matches a common breached passcode)"
        )


# ── Wordphrase generator ─────────────────────────────────────────────────────


def generate_wordphrase() -> str:
    """Secure random string with >100 bits of entropy."""
    return f"{secrets.token_hex(4)}-{secrets.token_hex(4)}-{secrets.token_hex(4)}-{secrets.token_hex(4)}"


# ── Name / email validation (XSS hardening, Section #19a) ───────────────────

import re

# Conservative ASCII-leaning name regex. Refuses HTML special chars
# (<, >, &, ", '), NULL bytes, and control characters. Allows international
# letters, digits, spaces, periods, commas, apostrophes, hyphens.
_NAME_RE = re.compile(r"^[\w .,'\-]{1,80}$", re.UNICODE)
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


class InvalidNameError(ValueError):
    pass


class InvalidEmailError(ValueError):
    pass


def validate_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        raise InvalidNameError("name is required")
    # Reject HTML metacharacters that have no business in a person's name.
    # Straight apostrophes are KEPT so Irish/Italian/Polynesian names work
    # (O'Brien, D'Angelo, Le'aupepe). React + the backend never interpolate
    # these into raw HTML attributes; they go through proper escaping.
    if "<" in name or ">" in name or "&" in name or '"' in name:
        raise InvalidNameError("name contains disallowed characters (HTML special characters not permitted)")
    if "\x00" in name or any(ord(c) < 32 for c in name):
        raise InvalidNameError("name contains control characters")
    if not _NAME_RE.match(name):
        raise InvalidNameError(
            "name must be 1-80 characters; letters, digits, spaces, periods, commas, apostrophes, hyphens only"
        )
    return name


def validate_email(email: str) -> str:
    email = (email or "").strip().lower()
    if not _EMAIL_RE.match(email):
        raise InvalidEmailError("email is not in a valid format")
    return email


# ── PII policy validation (Pydantic-equivalent without the dep) ─────────────


class InvalidPiiPolicyError(ValueError):
    pass


def validate_pii_policy(policy: dict | None) -> dict:
    """Coerce + validate the PII policy dict.

    Today's only known key is ``mask_ips: bool``. Unknown keys are dropped
    with a debug log (forward-compatibility: new fields are added here, never
    rejected silently).
    """
    if policy is None:
        return {"mask_ips": False}
    if not isinstance(policy, dict):
        raise InvalidPiiPolicyError("pii_policy must be an object")
    out: dict[str, Any] = {"mask_ips": bool(policy.get("mask_ips", False))}
    # Reserved future keys — accept now so old clients don't break later.
    for k in ("mask_user_agent", "mask_geo"):
        if k in policy:
            out[k] = bool(policy[k])
    if "redact_fields" in policy:
        rf = policy["redact_fields"]
        if not isinstance(rf, list) or not all(isinstance(x, str) for x in rf):
            raise InvalidPiiPolicyError("redact_fields must be a list of strings")
        out["redact_fields"] = rf
    return out


# ── IP whitelist parsing ────────────────────────────────────────────────────


def parse_ip_whitelist(s: str | None) -> list[str]:
    """Parse a comma-separated list of IPs/CIDRs; validates each entry.

    Returns the list of normalized entries. Raises ``ValueError`` on any
    malformed entry.
    """
    if not s or not s.strip():
        return []
    out: list[str] = []
    for raw in s.split(","):
        item = raw.strip()
        if not item:
            continue
        try:
            if "/" in item:
                net = ipaddress.ip_network(item, strict=False)
                out.append(str(net))
            else:
                ip = ipaddress.ip_address(item)
                out.append(str(ip))
        except ValueError as exc:
            raise ValueError(f"invalid IP/CIDR entry {item!r}: {exc}") from exc
    return out


def ip_in_whitelist(ip: str, whitelist_csv: str | None) -> bool:
    """True iff ``ip`` is permitted by the comma-separated whitelist.

    Empty / None whitelist allows all (existing call sites encode "no
    restriction" as NULL on the invite row).
    """
    if not whitelist_csv:
        return True
    try:
        client = ipaddress.ip_address(ip)
    except ValueError:
        return False
    for raw in whitelist_csv.split(","):
        item = raw.strip()
        if not item:
            continue
        try:
            if "/" in item:
                net = ipaddress.ip_network(item, strict=False)
                if client in net:
                    return True
            else:
                if client == ipaddress.ip_address(item):
                    return True
        except ValueError:
            continue
    return False


# ── Invite accessors ────────────────────────────────────────────────────────


def create_remote_invite(
    *,
    name: str,
    email: str,
    passcode: str,
    expires_at_utc: str | None,
    ip_whitelist: str | None,
    service_ids: list[str],
    pii_policy: dict | None = None,
    query_window_hours: int | None = None,
    query_start_time: str | None = None,
    query_end_time: str | None = None,
    con: sqlite3.Connection | None = None,
) -> dict:
    """Insert a new invite with its service scope and return the row dict.

    Validates name / email / passcode / pii_policy / ip_whitelist before insert.
    """
    name = validate_name(name)
    email = validate_email(email)
    validate_passcode_strength(passcode)
    policy = validate_pii_policy(pii_policy)
    parse_ip_whitelist(ip_whitelist)  # raises on malformed entries

    invite_id = str(uuid.uuid4())
    con = con or get_global_share_con()
    with con:
        con.execute(
            """INSERT INTO remote_invites
                (id, name, email, passcode, expires_at, ip_whitelist, pii_policy,
                 query_window_hours, query_start_time, query_end_time, created_at, revoked)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
            (
                invite_id,
                name,
                email,
                hash_passcode(passcode),
                expires_at_utc,
                ip_whitelist or None,
                json.dumps(policy, separators=(",", ":")),
                query_window_hours,
                query_start_time,
                query_end_time,
                iso_z_now(),
            ),
        )
        for sid in service_ids or []:
            con.execute(
                "INSERT OR IGNORE INTO invite_services(invite_id, service_id) VALUES(?, ?)",
                (invite_id, sid),
            )
    created = get_remote_invite(invite_id, con=con)
    assert created is not None, "invite vanished immediately after insert"
    return created


def get_remote_invite(invite_id: str, *, con: sqlite3.Connection | None = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT * FROM remote_invites WHERE id=?", (invite_id,)).fetchone()
    if row is None:
        return None
    out = dict(row)
    out["pii_policy"] = json.loads(out.get("pii_policy") or '{"mask_ips": false}')
    out["service_ids"] = get_remote_invite_services(invite_id, con=con)
    return out


def get_remote_invite_services(invite_id: str, *, con: sqlite3.Connection | None = None) -> list[str]:
    con = con or get_global_share_con()
    rows = con.execute(
        "SELECT service_id FROM invite_services WHERE invite_id=? ORDER BY service_id",
        (invite_id,),
    ).fetchall()
    return [r["service_id"] for r in rows]


def get_remote_invites(*, con: sqlite3.Connection | None = None) -> list[dict]:
    con = con or get_global_share_con()
    rows = con.execute("SELECT * FROM remote_invites ORDER BY created_at DESC").fetchall()
    out: list[dict] = []
    for row in rows:
        rec = dict(row)
        rec["pii_policy"] = json.loads(rec.get("pii_policy") or '{"mask_ips": false}')
        rec["service_ids"] = get_remote_invite_services(rec["id"], con=con)
        out.append(rec)
    return out


def get_remote_invite_by_email_passcode(
    email: str, passcode: str, *, con: sqlite3.Connection | None = None
) -> dict | None:
    """Constant-time lookup. Returns the invite dict on success, else None.

    Security: when no invite exists for ``email`` (e.g., email
    enumeration attack), still run one scrypt verification against a dummy
    hash with the same parameters so the response time matches the
    invite-exists branch (~30 ms). Without this, an attacker measuring the
    response latency can distinguish "email is registered, passcode wrong"
    (slow) from "email never invited" (fast) and enumerate emails.
    """
    con = con or get_global_share_con()
    norm_email = (email or "").strip().lower()
    rows = con.execute(
        "SELECT * FROM remote_invites WHERE lower(email)=? AND revoked=0",
        (norm_email,),
    ).fetchall()
    now = iso_z_now()
    match: dict | None = None
    for row in rows:
        # always run the verify so timing is roughly constant across the rows
        if verify_passcode(passcode, row["passcode"]):
            if row["expires_at"] and row["expires_at"] < now:
                continue
            if match is None:
                match = dict(row)
    if match is None:
        # Equalize timing ONLY when the email has no invite at all. If
        # rows existed (email present, passcode wrong) we already paid one
        # scrypt per row inside the loop — running the dummy verification
        # again would push the wrong-passcode branch to ``(N+1)×scrypt``
        # while the no-email branch stays at ``1×scrypt``, recreating
        # the 2× timing side-channel this function is meant to close.
        if not rows:
            _equalize_passcode_timing(passcode)
        return None
    match["pii_policy"] = json.loads(match.get("pii_policy") or '{"mask_ips": false}')
    match["service_ids"] = get_remote_invite_services(match["id"], con=con)
    return match


_dummy_hash: str | None = None


def _equalize_passcode_timing(passcode: str) -> None:
    """Run one scrypt verification against a fixed dummy hash so the timing
    of the "no email match" branch matches the "email match, wrong passcode"
    branch.

    The dummy hash uses the same _SCRYPT_N/_R/_P/_DKLEN parameters as
    ``hash_passcode`` so verification cost is identical. Generated once per
    process and reused — generating per-call would add measurable extra cost
    to the miss branch."""
    global _dummy_hash
    if _dummy_hash is None:
        # Synthesize via the real hash function so any future parameter
        # change in ``hash_passcode`` is automatically reflected here.
        _dummy_hash = hash_passcode("__dummy_for_timing_equalization__")
    verify_passcode(passcode, _dummy_hash)


def update_remote_invite_services(
    invite_id: str, service_ids: list[str], *, con: sqlite3.Connection | None = None
) -> None:
    con = con or get_global_share_con()
    with con:
        con.execute("DELETE FROM invite_services WHERE invite_id=?", (invite_id,))
        for sid in service_ids:
            con.execute(
                "INSERT OR IGNORE INTO invite_services(invite_id, service_id) VALUES(?, ?)",
                (invite_id, sid),
            )


def update_remote_invite_passcode(invite_id: str, passcode: str, *, con: sqlite3.Connection | None = None) -> bool:
    """Rotate the passcode on an existing invite without changing anything else.

    Validates strength via the same rules as create. Returns True on success,
    False if no invite with that id exists. Raises ValueError for a weak
    passcode (caller maps to HTTP 400).
    """
    validate_passcode_strength(passcode)
    con = con or get_global_share_con()
    cur = con.execute(
        "UPDATE remote_invites SET passcode=? WHERE id=?",
        (hash_passcode(passcode), invite_id),
    )
    con.commit()
    return cur.rowcount > 0


def revoke_remote_invite(invite_id: str, *, con: sqlite3.Connection | None = None) -> bool:
    con = con or get_global_share_con()
    cur = con.execute("UPDATE remote_invites SET revoked=1 WHERE id=?", (invite_id,))
    con.commit()
    return cur.rowcount > 0


def delete_remote_invite(invite_id: str, *, con: sqlite3.Connection | None = None) -> bool:
    """Hard-delete an invite. Cascades to invite_services, remote_sessions, and
    remote_invite_claim_tokens via ON DELETE CASCADE. Audit log rows are
    preserved (no FK to remote_invites), so the deletion trail survives.

    Returns True if a row was deleted, False if no invite with that id existed.
    """
    con = con or get_global_share_con()
    cur = con.execute("DELETE FROM remote_invites WHERE id=?", (invite_id,))
    con.commit()
    return cur.rowcount > 0


def mark_tos_accepted(invite_id: str, version: str, *, con: sqlite3.Connection | None = None) -> None:
    con = con or get_global_share_con()
    with con:
        con.execute(
            "UPDATE remote_invites SET tos_accepted_at=?, tos_version=? WHERE id=?",
            (iso_z_now(), version, invite_id),
        )


def get_latest_tos(*, con: sqlite3.Connection | None = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute(
        "SELECT version, text, published_at FROM share_tos_versions ORDER BY published_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


# ── Audit logs ──────────────────────────────────────────────────────────────


def log_share_audit_event(
    *,
    event_type: str,
    email: str | None,
    ip_address: str,
    details: str,
    con: sqlite3.Connection | None = None,
) -> None:
    con = con or get_global_share_con()
    con.execute(
        """INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details)
           VALUES (?, ?, ?, ?, ?)""",
        (iso_z_now(), event_type, email, ip_address or "0.0.0.0", details),
    )
    con.commit()


def get_share_audit_logs(
    limit: int = 200,
    *,
    event_type: str | None = None,
    email_substr: str | None = None,
    since: str | None = None,
    until: str | None = None,
    con: sqlite3.Connection | None = None,
) -> list[dict]:
    """Return audit log rows ordered newest-first.

    Optional filters compose with AND. ``since`` / ``until`` are ISO-Z strings
    compared lexicographically (the column is stored as ``iso_z_now()`` text,
    which is monotonic enough for prefix/range comparison without parsing).
    """
    con = con or get_global_share_con()
    clauses: list[str] = []
    params: list = []
    if event_type:
        clauses.append("event_type = ?")
        params.append(event_type)
    if email_substr:
        clauses.append("email LIKE ?")
        params.append(f"%{email_substr}%")
    if since:
        clauses.append("timestamp >= ?")
        params.append(since)
    if until:
        clauses.append("timestamp <= ?")
        params.append(until)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"SELECT * FROM remote_share_audit_logs{where} ORDER BY id DESC LIMIT ?"
    params.append(int(limit))
    rows = con.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def purge_old_audit_logs(retention_days: int = 90, *, con: sqlite3.Connection | None = None) -> int:
    """Delete audit rows older than the retention window. Returns row count."""
    con = con or get_global_share_con()
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=int(retention_days)))
    cur = con.execute("DELETE FROM remote_share_audit_logs WHERE timestamp < ?", (cutoff,))
    con.commit()
    return cur.rowcount or 0


# ── Settings (key/value) ────────────────────────────────────────────────────


def get_setting(key: str, default: str | None = None, *, con: sqlite3.Connection | None = None) -> str | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT value FROM share_settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str, *, con: sqlite3.Connection | None = None) -> None:
    con = con or get_global_share_con()
    con.execute(
        "INSERT INTO share_settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
    con.commit()


def get_max_concurrent_sessions(*, con: sqlite3.Connection | None = None) -> int:
    raw = get_setting("max_concurrent_analyst_sessions", "10", con=con)
    try:
        return max(1, int(raw or "10"))
    except (TypeError, ValueError):
        return 10


# ── Session persistence ─────────────────────────────────────────────────────


def upsert_session(session: dict, *, con: sqlite3.Connection | None = None) -> None:
    con = con or get_global_share_con()
    con.execute(
        """INSERT INTO remote_sessions(
            session_id, invite_id, name, email, ip_address, user_agent,
            fingerprint_signature, pii_policy, query_window_hours,
            query_start_time, query_end_time, login_time, last_active_time, last_activity)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(session_id) DO UPDATE SET
            ip_address=excluded.ip_address,
            user_agent=excluded.user_agent,
            last_active_time=excluded.last_active_time,
            last_activity=excluded.last_activity""",
        (
            session["session_id"],
            session["invite_id"],
            session["name"],
            session["email"],
            session["ip_address"],
            session["user_agent"],
            session["fingerprint_signature"],
            json.dumps(session.get("pii_policy") or {}, separators=(",", ":")),
            session.get("query_window_hours"),
            session.get("query_start_time"),
            session.get("query_end_time"),
            session["login_time"],
            session["last_active_time"],
            session.get("last_activity"),
        ),
    )
    con.commit()


def delete_session(session_id: str, *, con: sqlite3.Connection | None = None) -> None:
    con = con or get_global_share_con()
    con.execute("DELETE FROM remote_sessions WHERE session_id=?", (session_id,))
    con.commit()


def get_session(session_id: str, *, con: sqlite3.Connection | None = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT * FROM remote_sessions WHERE session_id=?", (session_id,)).fetchone()
    if row is None:
        return None
    rec = dict(row)
    rec["pii_policy"] = json.loads(rec.get("pii_policy") or "{}")
    return rec


def get_all_sessions(*, con: sqlite3.Connection | None = None) -> list[dict]:
    con = con or get_global_share_con()
    rows = con.execute("SELECT * FROM remote_sessions").fetchall()
    out: list[dict] = []
    for r in rows:
        rec = dict(r)
        rec["pii_policy"] = json.loads(rec.get("pii_policy") or "{}")
        out.append(rec)
    return out


# ── Claim tokens (one-time-view invite credential URL) ──────────────────────


def create_claim_token(invite_id: str, *, ttl_hours: int = 24, con: sqlite3.Connection | None = None) -> str:
    con = con or get_global_share_con()
    token = secrets.token_urlsafe(24)
    expires_at = iso_z(datetime.now(UTC) + timedelta(hours=int(ttl_hours)))
    con.execute(
        "INSERT INTO remote_invite_claim_tokens(token, invite_id, created_at, expires_at) VALUES(?,?,?,?)",
        (token, invite_id, iso_z_now(), expires_at),
    )
    con.commit()
    return token


def claim_token(token: str, ip: str, *, con: sqlite3.Connection | None = None) -> dict | None:
    """Mark a claim token as claimed (one-shot) and return its invite_id.

    Returns the row dict on success; ``None`` if the token does not exist, is
    expired, or was already claimed.

    Security (TOCTOU): use a single atomic UPDATE with the
    ``claimed_at IS NULL`` predicate baked into the WHERE clause. Earlier
    versions ran SELECT-then-check-then-UPDATE under the same transaction,
    but two concurrent claims could both pass the SELECT before either
    UPDATE landed and end up double-redeeming. Now whichever transaction's
    UPDATE commits first wins (rowcount == 1); the loser sees rowcount == 0
    and returns None.

    The SELECT after UPDATE re-reads the just-claimed row so we can return
    the invite_id to the caller. Doing it inside the same ``with con:``
    block keeps it in the same write transaction.
    """
    con = con or get_global_share_con()
    now = iso_z_now()
    with con:
        cur = con.execute(
            """
            UPDATE remote_invite_claim_tokens
               SET claimed_at = ?, claimed_from_ip = ?
             WHERE token = ?
               AND claimed_at IS NULL
               AND expires_at >= ?
            """,
            (now, ip, token, now),
        )
        if cur.rowcount != 1:
            return None
        row = con.execute("SELECT * FROM remote_invite_claim_tokens WHERE token=?", (token,)).fetchone()
        if row is None:
            return None
    return dict(row)


# ── Backup / restore (AES-256-GCM with scrypt-derived key) ──────────────────


def export_backup(passphrase: str, *, con: sqlite3.Connection | None = None) -> bytes:
    """Encrypted JSON envelope of invites + service scopes + share settings.

    Audit logs and active sessions are intentionally excluded (logs are
    append-only forensic record; sessions are ephemeral).

    Format (bytes):
        b"FOSBACKUP\\x01" + 16-byte salt + 12-byte nonce + ciphertext+tag
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    con = con or get_global_share_con()
    invites = [dict(r) for r in con.execute("SELECT * FROM remote_invites").fetchall()]
    invite_services = [dict(r) for r in con.execute("SELECT * FROM invite_services").fetchall()]
    settings = [dict(r) for r in con.execute("SELECT * FROM share_settings").fetchall()]
    payload = {
        "schema_version": LATEST_VERSION,
        "exported_at": iso_z_now(),
        "invites": invites,
        "invite_services": invite_services,
        "share_settings": settings,
    }
    salt = secrets.token_bytes(16)
    key = hashlib.scrypt(passphrase.encode("utf-8"), salt=salt, n=_SCRYPT_N, r=8, p=1, dklen=32)
    nonce = secrets.token_bytes(12)
    ct = AESGCM(key).encrypt(nonce, json.dumps(payload).encode("utf-8"), None)
    return b"FOSBACKUP\x01" + salt + nonce + ct


def import_backup(
    blob: bytes, passphrase: str, *, mode: str = "skip-collisions", con: sqlite3.Connection | None = None
) -> dict:
    """Decrypt + validate + apply a backup envelope.

    ``mode``: one of ``skip-collisions`` (default), ``merge-services-on-collision``,
    or ``abort`` (reject if any email collision).
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if not blob.startswith(b"FOSBACKUP\x01"):
        raise ValueError("not a recognised backup envelope")
    body = blob[len(b"FOSBACKUP\x01") :]
    if len(body) < 16 + 12 + 16:
        raise ValueError("envelope is truncated")
    salt, nonce, ct = body[:16], body[16:28], body[28:]
    key = hashlib.scrypt(passphrase.encode("utf-8"), salt=salt, n=_SCRYPT_N, r=8, p=1, dklen=32)
    try:
        plain = AESGCM(key).decrypt(nonce, ct, None)
    except Exception as exc:  # cryptography raises InvalidTag here
        raise ValueError(f"failed to decrypt backup (wrong passphrase?): {exc}") from exc
    payload = json.loads(plain)
    if int(payload.get("schema_version", 0)) > LATEST_VERSION:
        raise ValueError(
            f"backup schema_version {payload['schema_version']} is newer than this build's {LATEST_VERSION}"
        )

    con = con or get_global_share_con()
    existing_by_email = {
        r["email"].lower(): r["id"]
        for r in con.execute("SELECT id, email FROM remote_invites WHERE revoked=0").fetchall()
    }

    inserted = 0
    skipped = 0
    merged = 0
    with con:
        for inv in payload.get("invites", []):
            email_lc = (inv.get("email") or "").lower()
            collision_id = existing_by_email.get(email_lc)
            if collision_id is not None:
                if mode == "abort":
                    raise ValueError(f"email collision on import: {email_lc}")
                if mode == "merge-services-on-collision":
                    # Re-attach services from the backup row to the existing invite.
                    src_id = inv["id"]
                    rows = [r for r in payload.get("invite_services", []) if r["invite_id"] == src_id]
                    for r in rows:
                        con.execute(
                            "INSERT OR IGNORE INTO invite_services(invite_id, service_id) VALUES(?, ?)",
                            (collision_id, r["service_id"]),
                        )
                    merged += 1
                else:  # skip-collisions
                    skipped += 1
                continue

            # Re-run validation rather than trusting the blob.
            try:
                validate_name(inv.get("name", ""))
                validate_email(inv.get("email", ""))
            except (InvalidNameError, InvalidEmailError):
                skipped += 1
                continue

            con.execute(
                """INSERT INTO remote_invites
                    (id, name, email, passcode, expires_at, ip_whitelist, pii_policy,
                     query_window_hours, query_start_time, query_end_time, created_at,
                     revoked, tos_accepted_at, tos_version)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    inv["id"],
                    inv["name"],
                    inv["email"],
                    inv["passcode"],
                    inv.get("expires_at"),
                    inv.get("ip_whitelist"),
                    inv.get("pii_policy") or '{"mask_ips": false}',
                    inv.get("query_window_hours"),
                    inv.get("query_start_time"),
                    inv.get("query_end_time"),
                    inv.get("created_at") or iso_z_now(),
                    int(inv.get("revoked") or 0),
                    inv.get("tos_accepted_at"),
                    inv.get("tos_version"),
                ),
            )
            for r in payload.get("invite_services", []):
                if r["invite_id"] == inv["id"]:
                    con.execute(
                        "INSERT OR IGNORE INTO invite_services(invite_id, service_id) VALUES(?, ?)",
                        (inv["id"], r["service_id"]),
                    )
            inserted += 1

        for s in payload.get("share_settings", []):
            con.execute(
                "INSERT INTO share_settings(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (s["key"], s["value"]),
            )

    return {"inserted": inserted, "skipped": skipped, "merged": merged}


# ── GDPR right-to-be-forgotten ──────────────────────────────────────────────


def gdpr_erase(email: str, reason: str, *, admin_actor: str = "admin", con: sqlite3.Connection | None = None) -> dict:
    """Delete the analyst's invite row + cascade, redact older audit logs.

    Returns ``{deleted_invites, redacted_log_rows, retained_recent_rows}``.

    Recent (last 24h) audit rows are intentionally preserved unredacted so an
    active-incident investigation isn't accidentally tampered with by a
    request that came from inside the house.
    """
    con = con or get_global_share_con()
    email_lc = (email or "").strip().lower()
    if not email_lc:
        raise ValueError("email is required")
    recent_cutoff = iso_z(datetime.now(UTC) - timedelta(hours=24))

    with con:
        deleted = con.execute("DELETE FROM remote_invites WHERE lower(email)=?", (email_lc,)).rowcount or 0
        # Cascade also removes invite_services, remote_sessions, claim tokens via FK.
        redacted = (
            con.execute(
                "UPDATE remote_share_audit_logs SET email='[GDPR-ERASED]', ip_address='[GDPR-ERASED]' "
                "WHERE lower(coalesce(email,''))=? AND timestamp < ?",
                (email_lc, recent_cutoff),
            ).rowcount
            or 0
        )
        retained = con.execute(
            "SELECT COUNT(*) FROM remote_share_audit_logs WHERE lower(coalesce(email,''))=? AND timestamp >= ?",
            (email_lc, recent_cutoff),
        ).fetchone()[0]
        con.execute(
            "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                iso_z_now(),
                "GDPR_ERASURE",
                None,
                "127.0.0.1",
                json.dumps(
                    {
                        "admin_actor": admin_actor,
                        "erased_email": email_lc,
                        "reason": reason,
                        "deleted_invites": deleted,
                        "redacted_log_rows": redacted,
                        "retained_recent_rows": retained,
                    },
                    separators=(",", ":"),
                ),
            ),
        )
    return {
        "deleted_invites": deleted,
        "redacted_log_rows": redacted,
        "retained_recent_rows": retained,
    }


# ── PII masking helpers ─────────────────────────────────────────────────────


def mask_ip(ip: str) -> str:
    """Mask the final octet of IPv4, last 80 bits of IPv6.

    Used by the middleware when ``session.pii_policy.mask_ips`` is True.
    """
    try:
        addr = ipaddress.ip_address(ip)
    except (ValueError, TypeError):
        return ip
    if isinstance(addr, ipaddress.IPv4Address):
        parts = str(addr).split(".")
        return ".".join(parts[:3] + ["xxx"])
    # IPv6: keep first 48 bits, zero the rest.
    packed = bytearray(addr.packed)
    for i in range(6, 16):
        packed[i] = 0
    return str(ipaddress.IPv6Address(bytes(packed)))


def apply_pii_policy(obj, policy: dict):
    """Walk a JSON-serialisable object, masking by policy.

    Today: ``mask_ips`` masks anything that string-parses as an IP in fields
    named ``ip``, ``ip_address``, ``client_ip``, ``remote_addr``.
    """
    if not policy or not policy.get("mask_ips"):
        return obj
    masked_keys = {"ip", "ip_address", "client_ip", "remote_addr"}

    def _walk(node, parent_key=None):
        if isinstance(node, dict):
            return {
                k: (mask_ip(v) if isinstance(v, str) and k in masked_keys else _walk(v, parent_key=k))
                for k, v in node.items()
            }
        if isinstance(node, list):
            # Array fields inherit the parent dict key for masking — e.g.
            # ``{"client_ip": ["1.2.3.4", "5.6.7.8"]}`` must mask each string
            # the same way the scalar form would. Without threading the
            # parent key through, list-of-string IP fields slipped past the
            # masker entirely.
            return [
                (mask_ip(x) if isinstance(x, str) and parent_key in masked_keys else _walk(x, parent_key=parent_key))
                for x in node
            ]
        return node

    return _walk(obj)
