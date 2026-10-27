"""Global remote-share store: connection, schema, TOS, settings, sessions, audit, invites, passcode, and validation."""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import logging
import re
import secrets
import sys
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHash, VerifyMismatchError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictStr,
    TypeAdapter,
    ValidationError,
)

import backend as _backend_pkg
from backend.core.metadata.base import close_all_pg_connections, get_pg_thread_connection
from backend.utils.date_utils import iso_z, iso_z_now

if hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.setdefault("backend.routers.share_auth", "backend.routers.share_admin")
    _backend_pkg._MODULE_ALIASES.setdefault("backend.routers.remote_share", "backend.routers.share_admin")
logger = logging.getLogger(__name__)


# ── Connection layer & Schema definitions ───────────────────────────────────
def get_global_share_con() -> Any:
    return get_pg_thread_connection()


def close_all_connections() -> None:
    close_all_pg_connections()


def _exec_rowcount(sql: str, params: tuple[Any, ...], *, con: Any = None) -> bool:
    c = con or get_global_share_con()
    cur = c.execute(sql, params)
    c.commit()
    return (cur.rowcount or 0) > 0


_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS remote_invites (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, passcode TEXT NOT NULL,
        expires_at TEXT, ip_whitelist TEXT, pii_policy TEXT NOT NULL DEFAULT '{"mask_ips": false}',
        query_window_hours INTEGER, query_start_time TEXT, query_end_time TEXT,
        created_at TEXT NOT NULL, revoked INTEGER NOT NULL DEFAULT 0,
        tos_accepted_at TEXT, tos_version TEXT, allow_concurrent_sessions INTEGER NOT NULL DEFAULT 0,
        auth_method TEXT NOT NULL DEFAULT 'passcode', oauth_provider TEXT, oauth_subject TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_remote_invites_email ON remote_invites(email)",
    """CREATE TABLE IF NOT EXISTS invite_services (
        invite_id TEXT NOT NULL, service_id TEXT NOT NULL,
        PRIMARY KEY (invite_id, service_id),
        FOREIGN KEY (invite_id) REFERENCES remote_invites(id) ON DELETE CASCADE
    )""",
    "CREATE INDEX IF NOT EXISTS idx_invite_services_invite_id ON invite_services(invite_id)",
    """CREATE TABLE IF NOT EXISTS remote_share_audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
        event_type TEXT NOT NULL, email TEXT, ip_address TEXT NOT NULL, details TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_remote_share_audit_logs_timestamp ON remote_share_audit_logs(timestamp)",
    "CREATE TABLE IF NOT EXISTS share_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    """CREATE TABLE IF NOT EXISTS remote_sessions (
        session_id TEXT PRIMARY KEY, invite_id TEXT NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL,
        ip_address TEXT NOT NULL, user_agent TEXT NOT NULL, fingerprint_signature TEXT NOT NULL,
        pii_policy TEXT NOT NULL, query_window_hours INTEGER, query_start_time TEXT, query_end_time TEXT,
        login_time TEXT NOT NULL, last_active_time TEXT NOT NULL, last_activity TEXT,
        FOREIGN KEY (invite_id) REFERENCES remote_invites(id) ON DELETE CASCADE
    )""",
    """CREATE TABLE IF NOT EXISTS remote_invite_claim_tokens (
        token TEXT PRIMARY KEY, invite_id TEXT NOT NULL, created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL, claimed_at TEXT, claimed_from_ip TEXT,
        FOREIGN KEY (invite_id) REFERENCES remote_invites(id) ON DELETE CASCADE
    )""",
    "CREATE TABLE IF NOT EXISTS share_tos_versions (version TEXT PRIMARY KEY, text TEXT NOT NULL, published_at TEXT NOT NULL)",
]
LATEST_VERSION = 4


# ── Terms-of-service & share_settings KV accessors ──────────────────────────
def get_latest_tos(*, con: Any = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute(
        "SELECT version, text, published_at FROM share_tos_versions ORDER BY published_at DESC, version DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def publish_tos_version(version: str, text: str, *, con: Any = None) -> None:
    con = con or get_global_share_con()
    if con.execute("SELECT 1 FROM share_tos_versions WHERE version=?", (version,)).fetchone() is not None:
        return
    con.execute(
        "INSERT INTO share_tos_versions(version, text, published_at) VALUES(?, ?, ?)", (version, text, iso_z_now())
    )
    con.commit()


MAX_CONCURRENT_ANALYST_SESSIONS_KEY = "max_concurrent_analyst_sessions"
SHARE_AUDIT_RETENTION_DAYS_KEY = "share_audit_retention_days"


def get_setting(key: str, default: str | None = None, *, con: Any = None) -> str | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT value FROM share_settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str, *, con: Any = None) -> None:
    con = con or get_global_share_con()
    con.execute(
        "INSERT INTO share_settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
    con.commit()


def get_max_concurrent_sessions(*, con: Any = None) -> int:
    raw = get_setting(MAX_CONCURRENT_ANALYST_SESSIONS_KEY, "10", con=con)
    try:
        return max(1, int(raw or "10"))
    except (TypeError, ValueError):
        return 10


# ── Persisted analyst session CRUD ──────────────────────────────────────────
def upsert_session(session: dict, *, con: Any = None) -> None:
    con = con or get_global_share_con()
    con.execute(
        """INSERT INTO remote_sessions(
            session_id, invite_id, name, email, ip_address, user_agent,
            fingerprint_signature, pii_policy, query_window_hours,
            query_start_time, query_end_time, login_time, last_active_time, last_activity)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(session_id) DO UPDATE SET
            ip_address=excluded.ip_address, user_agent=excluded.user_agent,
            last_active_time=excluded.last_active_time, last_activity=excluded.last_activity""",
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


def delete_session(session_id: str, *, con: Any = None) -> None:
    _exec_rowcount("DELETE FROM remote_sessions WHERE session_id=?", (session_id,), con=con)


_JSON_DICT_ADAPTER: TypeAdapter[dict[str, Any]] = TypeAdapter(dict[str, Any])


def _parse_json_dict(v: Any, default: dict[str, Any]) -> dict[str, Any]:
    return (
        _JSON_DICT_ADAPTER.validate_json(v)
        if isinstance(v, (str, bytes)) and v
        else (v if isinstance(v, dict) else dict(default))
    )


class _SessionRowModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    pii_policy: Annotated[dict[str, Any], BeforeValidator(lambda v: _parse_json_dict(v, {}))] = Field(
        default_factory=dict
    )


def get_session(session_id: str, *, con: Any = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT * FROM remote_sessions WHERE session_id=?", (session_id,)).fetchone()
    return _SessionRowModel.model_validate(dict(row)).model_dump() if row is not None else None


def get_all_sessions(*, con: Any = None) -> list[dict]:
    con = con or get_global_share_con()
    return [
        _SessionRowModel.model_validate(dict(r)).model_dump()
        for r in con.execute("SELECT * FROM remote_sessions").fetchall()
    ]


# ── Audit log writes + filtered reads ───────────────────────────────────────
def log_share_audit_event(
    *, event_type: str, email: str | None, ip_address: str, details: str, con: Any = None
) -> None:
    con = con or get_global_share_con()
    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?, ?, ?, ?, ?)",
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
    con: Any = None,
) -> list[dict]:
    con = con or get_global_share_con()
    clauses: list[str] = []
    params: list[Any] = []
    for col, op, val in (
        ("event_type", "=", event_type),
        ("email", "LIKE", f"%{email_substr}%" if email_substr else None),
        ("timestamp", ">=", since),
        ("timestamp", "<=", until),
    ):
        if val:
            clauses.append(f"{col} {op} ?")
            params.append(val)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))
    rows = con.execute(
        f"SELECT * FROM remote_share_audit_logs{where} ORDER BY id DESC LIMIT ?", tuple(params)
    ).fetchall()
    return [dict(r) for r in rows]


_LOGIN_SUCCESS_EVENTS = ("LOGIN_SUCCESS", "LOGIN_SUCCESS_OAUTH")


def get_last_login_by_email(*, con: Any = None) -> dict[str, str]:
    con = con or get_global_share_con()
    placeholders = ",".join("?" * len(_LOGIN_SUCCESS_EVENTS))
    rows = con.execute(
        f"SELECT lower(email) AS email_lc, MAX(timestamp) AS last_login_at FROM remote_share_audit_logs "
        f"WHERE event_type IN ({placeholders}) AND email IS NOT NULL GROUP BY lower(email)",
        _LOGIN_SUCCESS_EVENTS,
    ).fetchall()
    return {r["email_lc"]: r["last_login_at"] for r in rows if r["email_lc"]}


def purge_old_audit_logs(retention_days: int = 90, *, con: Any = None) -> int:
    con = con or get_global_share_con()
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=int(retention_days)))
    cur = con.execute("DELETE FROM remote_share_audit_logs WHERE timestamp < ?", (cutoff,))
    con.commit()
    return cur.rowcount or 0


def purge_stale_share_records(max_idle_session_days: int = 30, *, con: Any = None) -> dict[str, int]:
    con = con or get_global_share_con()
    now_iso = iso_z_now()
    session_cutoff = iso_z(datetime.now(UTC) - timedelta(days=int(max_idle_session_days)))
    deleted_tokens = (
        con.execute("DELETE FROM remote_invite_claim_tokens WHERE expires_at < ?", (now_iso,)).rowcount or 0
    )
    deleted_invites = (
        con.execute("DELETE FROM remote_invites WHERE expires_at IS NOT NULL AND expires_at < ?", (now_iso,)).rowcount
        or 0
    )
    deleted_sessions = (
        con.execute("DELETE FROM remote_sessions WHERE last_active_time < ?", (session_cutoff,)).rowcount or 0
    )
    con.commit()
    return {
        "deleted_claim_tokens": deleted_tokens,
        "deleted_expired_invites": deleted_invites,
        "deleted_stale_sessions": deleted_sessions,
    }


def purge_all_share_records(
    audit_retention_days: int = 90, max_idle_session_days: int = 30, *, con: Any = None
) -> dict[str, int]:
    con = con or get_global_share_con()
    deleted_audit = purge_old_audit_logs(retention_days=audit_retention_days, con=con)
    stale_res = purge_stale_share_records(max_idle_session_days=max_idle_session_days, con=con)
    return {
        "deleted_audit_logs": deleted_audit,
        "deleted_expired_invites": stale_res["deleted_expired_invites"],
        "deleted_stale_sessions": stale_res["deleted_stale_sessions"],
        "deleted_claim_tokens": stale_res["deleted_claim_tokens"],
        "audit_retention_days": audit_retention_days,
        "max_idle_session_days": max_idle_session_days,
    }


# ── Validation & PII masking (folded from validation.py) ────────────────────
_NAME_RE = re.compile(r"^[\w .,'\-]{1,80}$", re.UNICODE)
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
IP_FAMILY_KEYS = frozenset({"ip", "client_ip", "ip_address", "remote_addr"})
SESSION_ID_KEYS = frozenset({"cookie_session", "cid"})
_NONWORD_RE = re.compile(r"[^\w]")
_MAYBE_IP_RE = re.compile(r"[0-9A-Fa-f:.]+")


class InvalidNameError(ValueError):
    pass


class InvalidEmailError(ValueError):
    pass


class InvalidPiiPolicyError(ValueError):
    pass


def _check_name(val: str) -> str:
    name = (val or "").strip()
    if not name:
        raise ValueError("name is required")
    if "<" in name or ">" in name or "&" in name or '"' in name:
        raise ValueError("name contains disallowed characters (HTML special characters not permitted)")
    if "\x00" in name or any(ord(c) < 32 for c in name):
        raise ValueError("name contains control characters")
    if not _NAME_RE.fullmatch(name):
        raise ValueError(
            "name must be 1-80 characters; letters, digits, spaces, periods, commas, apostrophes, hyphens only"
        )
    return name


def _check_email(val: str) -> str:
    email = (val or "").strip().lower()
    if not _EMAIL_RE.fullmatch(email):
        raise ValueError("email is not in a valid format")
    return email


_NAME_ADAPTER: TypeAdapter[str] = TypeAdapter(Annotated[str, AfterValidator(_check_name)])
_EMAIL_ADAPTER: TypeAdapter[str] = TypeAdapter(Annotated[str, AfterValidator(_check_email)])


def validate_name(name: str) -> str:
    try:
        return _NAME_ADAPTER.validate_python(name or "")
    except ValidationError as exc:
        raise InvalidNameError(exc.errors()[0].get("msg", "Invalid name").removeprefix("Value error, ")) from exc


def validate_email(email: str) -> str:
    try:
        return _EMAIL_ADAPTER.validate_python(email or "")
    except ValidationError as exc:
        raise InvalidEmailError(exc.errors()[0].get("msg", "Invalid email").removeprefix("Value error, ")) from exc


class _PiiPolicyModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    mask_ips: bool = False
    redact_fields: list[StrictStr] | None = None


def validate_pii_policy(policy: dict | None) -> dict:
    if policy is None:
        return {"mask_ips": False}
    if not isinstance(policy, dict):
        raise InvalidPiiPolicyError("pii_policy must be an object")
    for k in ("mask_user_agent", "mask_geo"):
        if policy.get(k):
            raise InvalidPiiPolicyError(f"pii_policy.{k} is not supported yet — it would not be enforced. Remove it.")
    try:
        m = _PiiPolicyModel.model_validate(policy)
    except ValidationError as exc:
        raise InvalidPiiPolicyError("redact_fields must be a list of strings") from exc
    if m.redact_fields:
        raise InvalidPiiPolicyError(
            "pii_policy.redact_fields is not supported yet — it would not be enforced. Remove it."
        )
    return {"mask_ips": m.mask_ips}


def parse_ip_whitelist(s: str | None) -> list[str]:
    if not s or not s.strip():
        return []
    out: list[str] = []
    for raw in s.split(","):
        item = raw.strip()
        if not item:
            continue
        try:
            out.append(str(ipaddress.ip_network(item, strict=False) if "/" in item else ipaddress.ip_address(item)))
        except ValueError as exc:
            raise ValueError(f"invalid IP/CIDR entry {item!r}: {exc}") from exc
    return out


def ip_in_whitelist(ip: str, whitelist_csv: str | None) -> bool:
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
                if client in ipaddress.ip_network(item, strict=False):
                    return True
            elif client == ipaddress.ip_address(item):
                return True
        except ValueError:
            continue
    return False


def mask_ip(ip: str) -> str:
    if not ip:
        return ip
    if ip.endswith(".xxx"):
        try:
            ipaddress.IPv4Address(ip[:-4] + ".0")
        except ValueError:
            pass
        else:
            return ip
    try:
        addr = ipaddress.ip_address(ip)
    except (ValueError, TypeError):
        return "[redacted]"
    if isinstance(addr, ipaddress.IPv4Address):
        return ".".join(str(addr).split(".")[:3] + ["xxx"])
    packed = bytearray(addr.packed)
    for i in range(6, 16):
        packed[i] = 0
    return str(ipaddress.IPv6Address(bytes(packed)))


def apply_pii_policy(obj: Any, policy: dict) -> Any:
    if not policy or not policy.get("mask_ips"):
        return obj
    masked_keys = IP_FAMILY_KEYS

    def _walk(node: Any, parent_key: Any = None, field_ctx: Any = None) -> Any:
        if isinstance(node, dict):
            out: dict[str, Any] = {}
            declared_field = node.get("field")
            canon_field = _NONWORD_RE.sub("", declared_field).lower() if isinstance(declared_field, str) else None
            eff_ctx = (
                canon_field
                if canon_field is not None and (canon_field in SESSION_ID_KEYS or canon_field in masked_keys)
                else field_ctx
            )
            for k, v in node.items():
                if isinstance(v, str) and k in SESSION_ID_KEYS:
                    out[k] = "[redacted]" if v else v
                elif isinstance(v, str) and k in masked_keys:
                    out[k] = mask_ip(v)
                elif k in ("min", "max") and isinstance(v, str):
                    out[k] = _mask_ip_scalar(v)
                elif k == "value" and isinstance(v, str) and eff_ctx in masked_keys:
                    out[k] = _mask_ip_scalar(v)
                elif k == "value" and isinstance(v, str) and eff_ctx in SESSION_ID_KEYS:
                    out[k] = "[redacted]" if v else v
                else:
                    canon_k = _NONWORD_RE.sub("", k).lower() if isinstance(k, str) else k
                    next_ctx = canon_k if (canon_k in masked_keys or canon_k in SESSION_ID_KEYS) else eff_ctx
                    out[k] = _walk(v, parent_key=k, field_ctx=next_ctx)
            return out
        if isinstance(node, list):
            return [
                (
                    ("[redacted]" if x else x)
                    if isinstance(x, str) and parent_key in SESSION_ID_KEYS
                    else mask_ip(x)
                    if isinstance(x, str) and parent_key in masked_keys
                    else _walk(x, parent_key=parent_key, field_ctx=field_ctx)
                )
                for x in node
            ]
        return node

    return _walk(obj)


def _mask_ip_scalar(value: str) -> str:
    bare = value.strip()
    if _MAYBE_IP_RE.fullmatch(bare):
        try:
            ipaddress.ip_address(bare)
        except ValueError:
            pass
        else:
            return mask_ip(bare)
    if "," in value:
        parts = [p.strip() for p in value.split(",")]
        if len(parts) > 1 and all(parts):
            masked: list[str] = []
            for p in parts:
                try:
                    ipaddress.ip_address(p)
                except ValueError:
                    return value
                masked.append(mask_ip(p))
            return ", ".join(masked)
    return value


def mask_ip_values(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: mask_ip_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [mask_ip_values(x) for x in obj]
    if isinstance(obj, str):
        return _mask_ip_scalar(obj)
    return obj


# ── argon2id Passcode & Invite CRUD (folded from invites.py) ─────────────────
_HASHER = PasswordHasher(memory_cost=65536, time_cost=3, parallelism=4, hash_len=32)
_ARGON2_HASHER = _HASHER
_BREACHED_TOP_LIST = set(
    "password passw0rd letmein welcome admin iloveyou qwerty qwerty123 abc123 monkey dragon master "
    "sunshine princess football 111111 123123 123456 12345678 1234567890 000000 trustno1 starwars "
    "1q2w3e4r passwordpassword secret shadow".split()
)
_BACKUP_SCRYPT_N = 2**14
_BACKUP_SCRYPT_R = 8
_BACKUP_SCRYPT_P = 1
_BACKUP_SCRYPT_DKLEN = 32
_dummy_hash: str | None = None


class WeakPasscodeError(ValueError):
    """Raised by ``validate_passcode_strength`` for obvious weak inputs."""


def hash_passcode(passcode: str) -> str:
    return _HASHER.hash(passcode)


def verify_passcode(passcode: str, stored: str) -> bool:
    if not stored or not stored.startswith("$argon2"):
        return False
    try:
        _HASHER.verify(stored, passcode)
        return True
    except (VerifyMismatchError, InvalidHash):
        return False
    except Exception:
        return False


def needs_rehash(stored: str) -> bool:
    if not stored or not stored.startswith("$argon2"):
        return False
    try:
        return _HASHER.check_needs_rehash(stored)
    except InvalidHash:
        return False


def validate_passcode_strength(passcode: str) -> None:
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


def generate_wordphrase() -> str:
    return f"{secrets.token_hex(4)}-{secrets.token_hex(4)}-{secrets.token_hex(4)}-{secrets.token_hex(4)}"


def _equalize_passcode_timing(passcode: str) -> None:
    global _dummy_hash
    if _dummy_hash is None:
        _dummy_hash = hash_passcode("__dummy_for_timing_equalization__")
    verify_passcode(passcode, _dummy_hash)


class _InviteRowModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    pii_policy: Annotated[dict[str, Any], BeforeValidator(lambda v: _parse_json_dict(v, {"mask_ips": False}))] = Field(
        default_factory=lambda: {"mask_ips": False}
    )
    allow_concurrent_sessions: bool = False
    service_ids: list[str] = Field(default_factory=list)


def _row_to_invite_dict(row: Any, service_ids: list[str]) -> dict:
    return _InviteRowModel.model_validate({**dict(row), "service_ids": service_ids}).model_dump()


def create_remote_invite(
    *,
    name: str,
    email: str,
    passcode: str | None = None,
    expires_at_utc: str | None,
    ip_whitelist: str | None,
    service_ids: list[str],
    pii_policy: dict | None = None,
    query_window_hours: int | None = None,
    query_start_time: str | None = None,
    query_end_time: str | None = None,
    allow_concurrent_sessions: bool = False,
    auth_method: str = "passcode",
    oauth_provider: str | None = None,
    con: Any = None,
) -> dict:
    auth_method = (auth_method or "passcode").strip().lower()
    if auth_method not in ("passcode", "oauth"):
        raise ValueError(f"unknown auth_method: {auth_method!r}")
    name = validate_name(name)
    email = validate_email(email)
    if auth_method == "oauth":
        if not oauth_provider or not oauth_provider.strip():
            raise ValueError("oauth_provider is required for auth_method='oauth'")
        oauth_provider = oauth_provider.strip()
        passcode_to_hash = secrets.token_urlsafe(32)
    else:
        if not passcode:
            raise ValueError("passcode is required for auth_method='passcode'")
        validate_passcode_strength(passcode)
        passcode_to_hash = passcode
        oauth_provider = None
    policy = validate_pii_policy(pii_policy)
    parse_ip_whitelist(ip_whitelist)
    invite_id = str(uuid.uuid4())
    con = con or get_global_share_con()
    with con:
        con.execute(
            """INSERT INTO remote_invites
                (id, name, email, passcode, expires_at, ip_whitelist, pii_policy,
                 query_window_hours, query_start_time, query_end_time, created_at,
                 revoked, allow_concurrent_sessions, auth_method, oauth_provider)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""",
            (
                invite_id,
                name,
                email,
                hash_passcode(passcode_to_hash),
                expires_at_utc,
                ip_whitelist or None,
                json.dumps(policy, separators=(",", ":")),
                query_window_hours,
                query_start_time,
                query_end_time,
                iso_z_now(),
                int(bool(allow_concurrent_sessions)),
                auth_method,
                oauth_provider,
            ),
        )
        for sid in service_ids or []:
            con.execute("INSERT OR IGNORE INTO invite_services(invite_id, service_id) VALUES(?, ?)", (invite_id, sid))
    created = get_remote_invite(invite_id, con=con)
    if created is None:
        raise RuntimeError("invite vanished immediately after insert")
    return created


def get_remote_invite(invite_id: str, *, con: Any = None) -> dict | None:
    con = con or get_global_share_con()
    row = con.execute("SELECT * FROM remote_invites WHERE id=?", (invite_id,)).fetchone()
    return _row_to_invite_dict(row, get_remote_invite_services(invite_id, con=con)) if row else None


def get_remote_invite_services(invite_id: str, *, con: Any = None) -> list[str]:
    con = con or get_global_share_con()
    rows = con.execute(
        "SELECT service_id FROM invite_services WHERE invite_id=? ORDER BY service_id", (invite_id,)
    ).fetchall()
    return [r["service_id"] for r in rows]


def get_remote_invites(*, con: Any = None) -> list[dict]:
    con = con or get_global_share_con()
    rows = con.execute("SELECT * FROM remote_invites ORDER BY created_at DESC").fetchall()
    services_by_invite: dict[str, list[str]] = {}
    if rows:
        invite_ids = [r["id"] for r in rows]
        placeholders = ",".join("?" * len(invite_ids))
        for r in con.execute(
            f"SELECT invite_id, service_id FROM invite_services WHERE invite_id IN ({placeholders}) ORDER BY service_id",
            invite_ids,
        ).fetchall():
            services_by_invite.setdefault(r["invite_id"], []).append(r["service_id"])
    return [_row_to_invite_dict(row, services_by_invite.get(row["id"], [])) for row in rows]


def get_remote_invite_by_email_passcode(email: str, passcode: str, *, con: Any = None) -> dict | None:
    con = con or get_global_share_con()
    norm_email = (email or "").strip().lower()
    rows = con.execute(
        "SELECT * FROM remote_invites WHERE lower(email)=? AND revoked=0 AND auth_method='passcode'", (norm_email,)
    ).fetchall()
    now = iso_z_now()
    match: dict | None = None
    matched_row_id: str | None = None
    matched_stored_hash: str | None = None
    expensive_verify_run = False
    for row in rows:
        if row["passcode"] and row["passcode"].startswith("$argon2"):
            expensive_verify_run = True
        if verify_passcode(passcode, row["passcode"]):
            if row["expires_at"] and row["expires_at"] < now:
                continue
            if match is None:
                match = dict(row)
                matched_row_id = row["id"]
                matched_stored_hash = row["passcode"]
    if match is None:
        if not expensive_verify_run:
            _equalize_passcode_timing(passcode)
        return None
    if matched_stored_hash is not None and matched_row_id is not None and needs_rehash(matched_stored_hash):
        try:
            new_hash = hash_passcode(passcode)
            con.execute("UPDATE remote_invites SET passcode=? WHERE id=?", (new_hash, matched_row_id))
            con.commit()
            match["passcode"] = new_hash
        except Exception:
            logger.exception("[share_db] rehash-on-login failed for invite_id=%s", matched_row_id)
    return _row_to_invite_dict(match, get_remote_invite_services(match["id"], con=con))


def get_remote_invite_oauth(email: str, provider: str, *, con: Any = None) -> dict | None:
    con = con or get_global_share_con()
    norm_email = (email or "").strip().lower()
    norm_provider = (provider or "").strip()
    if not norm_email or not norm_provider:
        return None
    rows = con.execute(
        "SELECT * FROM remote_invites WHERE lower(email)=? AND revoked=0 AND auth_method='oauth' AND oauth_provider=?",
        (norm_email, norm_provider),
    ).fetchall()
    now = iso_z_now()
    for row in rows:
        if row["expires_at"] and row["expires_at"] < now:
            continue
        return _row_to_invite_dict(row, get_remote_invite_services(row["id"], con=con))
    return None


def bind_invite_oauth_subject(invite_id: str, subject: str, *, con: Any = None) -> bool:
    con = con or get_global_share_con()
    if not subject:
        return False
    with con:
        con.execute(
            "UPDATE remote_invites SET oauth_subject=? WHERE id=? AND oauth_subject IS NULL", (subject, invite_id)
        )
        row = con.execute("SELECT oauth_subject FROM remote_invites WHERE id=?", (invite_id,)).fetchone()
    if row is None or not row["oauth_subject"]:
        return False
    return hmac.compare_digest(str(row["oauth_subject"]), str(subject))


def update_remote_invite_services(invite_id: str, service_ids: list[str], *, con: Any = None) -> None:
    con = con or get_global_share_con()
    with con:
        con.execute("DELETE FROM invite_services WHERE invite_id=?", (invite_id,))
        for sid in service_ids:
            con.execute("INSERT OR IGNORE INTO invite_services(invite_id, service_id) VALUES(?, ?)", (invite_id, sid))


def update_remote_invite_passcode(invite_id: str, passcode: str, *, con: Any = None) -> bool:
    validate_passcode_strength(passcode)
    return _exec_rowcount(
        "UPDATE remote_invites SET passcode=? WHERE id=?", (hash_passcode(passcode), invite_id), con=con
    )


def update_remote_invite_pii(invite_id: str, pii_policy: dict | None, *, con: Any = None) -> bool:
    policy = validate_pii_policy(pii_policy)
    return _exec_rowcount(
        "UPDATE remote_invites SET pii_policy=? WHERE id=?",
        (json.dumps(policy, separators=(",", ":")), invite_id),
        con=con,
    )


def set_invite_concurrent_sessions(invite_id: str, allow: bool, *, con: Any = None) -> bool:
    return _exec_rowcount(
        "UPDATE remote_invites SET allow_concurrent_sessions=? WHERE id=?", (int(bool(allow)), invite_id), con=con
    )


def revoke_remote_invite(invite_id: str, *, con: Any = None) -> bool:
    return _exec_rowcount("UPDATE remote_invites SET revoked=1 WHERE id=?", (invite_id,), con=con)


def reactivate_remote_invite(invite_id: str, *, con: Any = None) -> bool:
    return _exec_rowcount("UPDATE remote_invites SET revoked=0 WHERE id=?", (invite_id,), con=con)


def delete_remote_invite(invite_id: str, *, con: Any = None) -> bool:
    return _exec_rowcount("DELETE FROM remote_invites WHERE id=?", (invite_id,), con=con)


def mark_tos_accepted(invite_id: str, version: str, *, con: Any = None) -> None:
    con = con or get_global_share_con()
    with con:
        con.execute(
            "UPDATE remote_invites SET tos_accepted_at=?, tos_version=? WHERE id=?", (iso_z_now(), version, invite_id)
        )


def create_claim_token(invite_id: str, *, ttl_hours: int = 24, con: Any = None) -> str:
    con = con or get_global_share_con()
    token = secrets.token_urlsafe(24)
    expires_at = iso_z(datetime.now(UTC) + timedelta(hours=int(ttl_hours)))
    con.execute(
        "INSERT INTO remote_invite_claim_tokens(token, invite_id, created_at, expires_at) VALUES(?,?,?,?)",
        (token, invite_id, iso_z_now(), expires_at),
    )
    con.commit()
    return token


def claim_token(token: str, ip: str, *, con: Any = None) -> dict | None:
    con = con or get_global_share_con()
    now = iso_z_now()
    with con:
        cur = con.execute(
            """UPDATE remote_invite_claim_tokens
               SET claimed_at = ?, claimed_from_ip = ?
             WHERE token = ? AND claimed_at IS NULL AND expires_at >= ?""",
            (now, ip, token, now),
        )
        if cur.rowcount != 1:
            return None
        row = con.execute("SELECT * FROM remote_invite_claim_tokens WHERE token=?", (token,)).fetchone()
        return dict(row) if row else None


def _derive_backup_key(passphrase: str, salt: bytes) -> bytes:
    return hashlib.scrypt(
        passphrase.encode("utf-8"),
        salt=salt,
        n=_BACKUP_SCRYPT_N,
        r=_BACKUP_SCRYPT_R,
        p=_BACKUP_SCRYPT_P,
        dklen=_BACKUP_SCRYPT_DKLEN,
    )


def export_backup(passphrase: str, *, con: Any = None) -> bytes:
    if not passphrase or len(passphrase) < 8:
        raise ValueError("passphrase must be at least 8 characters")
    con = con or get_global_share_con()
    payload = {
        "schema_version": LATEST_VERSION,
        "exported_at": iso_z_now(),
        "invites": [dict(r) for r in con.execute("SELECT * FROM remote_invites").fetchall()],
        "invite_services": [dict(r) for r in con.execute("SELECT * FROM invite_services").fetchall()],
        "share_settings": [dict(r) for r in con.execute("SELECT * FROM share_settings").fetchall()],
    }
    salt = secrets.token_bytes(16)
    nonce = secrets.token_bytes(12)
    ct = AESGCM(_derive_backup_key(passphrase, salt)).encrypt(nonce, json.dumps(payload).encode("utf-8"), None)
    return b"FOSBACKUP\x01" + salt + nonce + ct


class _BackupInviteModel(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = ""
    name: str = ""
    email: str = ""
    passcode: str | None = None
    expires_at: str | None = None
    ip_whitelist: str | None = None
    pii_policy: Annotated[str, BeforeValidator(lambda v: v or '{"mask_ips": false}')] = '{"mask_ips": false}'
    query_window_hours: int | None = None
    query_start_time: str | None = None
    query_end_time: str | None = None
    created_at: Annotated[str, BeforeValidator(lambda v: v or iso_z_now())] = Field(default_factory=iso_z_now)
    revoked: Annotated[int, BeforeValidator(lambda v: int(v or 0))] = 0
    tos_accepted_at: str | None = None
    tos_version: int | None = None
    allow_concurrent_sessions: Annotated[int, BeforeValidator(lambda v: int(v or 0))] = 0
    auth_method: Annotated[str, BeforeValidator(lambda v: v or "passcode")] = "passcode"
    oauth_provider: str | None = None
    oauth_subject: str | None = None


class _BackupPayloadModel(BaseModel):
    model_config = ConfigDict(extra="ignore")
    schema_version: int = 0
    exported_at: str | None = None
    invites: list[_BackupInviteModel] = Field(default_factory=list)
    invite_services: list[dict[str, Any]] = Field(default_factory=list)
    share_settings: list[dict[str, Any]] = Field(default_factory=list)


def import_backup(blob: bytes, passphrase: str, *, mode: str = "skip-collisions", con: Any = None) -> dict:
    if not blob.startswith(b"FOSBACKUP\x01"):
        raise ValueError("not a recognised backup envelope")
    body = blob[len(b"FOSBACKUP\x01") :]
    if len(body) < 16 + 12 + 16:
        raise ValueError("envelope is truncated")
    salt, nonce, ct = body[:16], body[16:28], body[28:]
    try:
        plain = AESGCM(_derive_backup_key(passphrase, salt)).decrypt(nonce, ct, None)
    except Exception as exc:
        raise ValueError(f"failed to decrypt backup (wrong passphrase?): {exc}") from exc
    payload = _BackupPayloadModel.model_validate_json(plain)
    if payload.schema_version > LATEST_VERSION:
        raise ValueError(f"backup schema_version {payload.schema_version} is newer than this build's {LATEST_VERSION}")
    con = con or get_global_share_con()
    existing_by_email = {
        r["email"].lower(): r["id"]
        for r in con.execute("SELECT id, email FROM remote_invites WHERE revoked=0").fetchall()
    }
    inserted = skipped = merged = 0
    with con:
        for inv in payload.invites:
            email_lc = inv.email.lower()
            collision_id = existing_by_email.get(email_lc)
            if collision_id is not None:
                if mode == "abort":
                    raise ValueError(f"email collision on import: {email_lc}")
                if mode == "merge-services-on-collision":
                    for r in payload.invite_services:
                        if r["invite_id"] == inv.id:
                            con.execute(
                                "INSERT OR IGNORE INTO invite_services(invite_id, service_id) VALUES(?, ?)",
                                (collision_id, r["service_id"]),
                            )
                    merged += 1
                else:
                    skipped += 1
                continue
            try:
                validate_name(inv.name)
                validate_email(inv.email)
            except (InvalidNameError, InvalidEmailError):
                skipped += 1
                continue
            con.execute(
                """INSERT INTO remote_invites
                    (id, name, email, passcode, expires_at, ip_whitelist, pii_policy,
                     query_window_hours, query_start_time, query_end_time, created_at,
                     revoked, tos_accepted_at, tos_version, allow_concurrent_sessions,
                     auth_method, oauth_provider, oauth_subject)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                tuple(inv.model_dump().values()),
            )
            for r in payload.invite_services:
                if r["invite_id"] == inv.id:
                    con.execute(
                        "INSERT OR IGNORE INTO invite_services(invite_id, service_id) VALUES(?, ?)",
                        (inv.id, r["service_id"]),
                    )
            inserted += 1
        for s in payload.share_settings:
            con.execute(
                "INSERT INTO share_settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (s["key"], s["value"]),
            )
    return {"inserted": inserted, "skipped": skipped, "merged": merged}


def gdpr_erase(email: str, reason: str, *, admin_actor: str = "admin", con: Any = None) -> dict:
    con = con or get_global_share_con()
    email_lc = (email or "").strip().lower()
    if not email_lc:
        raise ValueError("email is required")
    recent_cutoff = iso_z(datetime.now(UTC) - timedelta(hours=24))
    with con:
        deleted = con.execute("DELETE FROM remote_invites WHERE lower(email)=?", (email_lc,)).rowcount or 0
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
        details = json.dumps(
            {
                "admin_actor": admin_actor,
                "erased_email": email_lc,
                "reason": reason,
                "deleted_invites": deleted,
                "redacted_log_rows": redacted,
                "retained_recent_rows": retained,
            },
            separators=(",", ":"),
        )
        con.execute(
            "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?, ?, ?, ?, ?)",
            (iso_z_now(), "GDPR_ERASURE", None, "127.0.0.1", details),
        )
    return {"deleted_invites": deleted, "redacted_log_rows": redacted, "retained_recent_rows": retained}


_self = sys.modules[__name__]
for _sub in ("connection", "schema", "tos", "settings", "sessions", "audit", "passcode", "invites", "validation"):
    setattr(_self, _sub, _self)
    sys.modules[f"{__name__}.{_sub}"] = _self
