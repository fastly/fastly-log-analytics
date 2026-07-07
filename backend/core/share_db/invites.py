"""Remote-share invite CRUD, claim tokens, encrypted backup envelope, GDPR erase.

The login lookup ``get_remote_invite_by_email_passcode`` is the security-
critical entry point: it runs in constant time across the email-match and
no-email-match branches (see ``passcode._equalize_passcode_timing``) AND
transparently upgrades any legacy scrypt hash to argon2id on successful
login (the ``needs_rehash`` check).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta

from backend.core.share_db.connection import get_global_share_con
from backend.core.share_db.passcode import (
    _equalize_passcode_timing,
    hash_passcode,
    needs_rehash,
    validate_passcode_strength,
    verify_passcode,
)
from backend.core.share_db.schema import LATEST_VERSION
from backend.core.share_db.validation import (
    InvalidEmailError,
    InvalidNameError,
    parse_ip_whitelist,
    validate_email,
    validate_name,
    validate_pii_policy,
)
from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)

# Backup-envelope key derivation params. scrypt is fine for one-shot
# passphrase-to-key derivation (no rotation pressure like per-user
# passcodes); kept as-is to preserve backward compatibility with
# previously exported backups.
_BACKUP_SCRYPT_N = 2**14
_BACKUP_SCRYPT_R = 8
_BACKUP_SCRYPT_P = 1
_BACKUP_SCRYPT_DKLEN = 32


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
    con: sqlite3.Connection | None = None,
) -> dict:
    """Insert a new invite with its service scope and return the row dict.

    Validates name / email / passcode / pii_policy / ip_whitelist before insert.

    ``allow_concurrent_sessions`` opts the invite into shared logins: when set,
    multiple analysts can be logged in under it at once instead of each login
    booting the previous session.

    ``auth_method`` selects the redemption path:

    * ``'passcode'`` (default) — the caller supplies a ``passcode`` which is
      strength-validated and argon2id-hashed. Unchanged legacy behavior.
    * ``'oauth'`` — the analyst redeems the invite via the OIDC handshake
      (§2.5 of the design). ``oauth_provider`` (a configured registry key) is
      required; any ``passcode`` argument is ignored. The ``passcode`` column is
      ``NOT NULL``, so we fill it with a machine-generated 256-bit secret that
      is never returned or communicated. It can never be used to log in: the
      positive ``auth_method`` gate rejects passcode login on an OAuth invite
      (and vice-versa), so this is belt-and-suspenders, not the control.
    """
    auth_method = (auth_method or "passcode").strip().lower()
    if auth_method not in ("passcode", "oauth"):
        raise ValueError(f"unknown auth_method: {auth_method!r}")

    name = validate_name(name)
    email = validate_email(email)
    if auth_method == "oauth":
        if not oauth_provider or not oauth_provider.strip():
            raise ValueError("oauth_provider is required for auth_method='oauth'")
        oauth_provider = oauth_provider.strip()
        # Unguessable placeholder — never communicated, never a valid login.
        passcode_to_hash = secrets.token_urlsafe(32)
    else:
        if not passcode:
            raise ValueError("passcode is required for auth_method='passcode'")
        validate_passcode_strength(passcode)
        passcode_to_hash = passcode
        oauth_provider = None
    policy = validate_pii_policy(pii_policy)
    parse_ip_whitelist(ip_whitelist)  # raises on malformed entries

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
    out["allow_concurrent_sessions"] = bool(out.get("allow_concurrent_sessions"))
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
    # Bulk-fetch all invite_services in one query (was: per-invite SELECT in
    # the loop below, an N+1 the admin/share status payload paid on every
    # mount). Bucket the rows in Python so the per-invite list build stays
    # the same shape downstream.
    services_by_invite: dict[str, list[str]] = {}
    if rows:
        invite_ids = [r["id"] for r in rows]
        placeholders = ",".join("?" * len(invite_ids))
        svc_rows = con.execute(
            f"SELECT invite_id, service_id FROM invite_services "
            f"WHERE invite_id IN ({placeholders}) ORDER BY service_id",
            invite_ids,
        ).fetchall()
        for r in svc_rows:
            services_by_invite.setdefault(r["invite_id"], []).append(r["service_id"])
    out: list[dict] = []
    for row in rows:
        rec = dict(row)
        rec["pii_policy"] = json.loads(rec.get("pii_policy") or '{"mask_ips": false}')
        rec["service_ids"] = services_by_invite.get(rec["id"], [])
        rec["allow_concurrent_sessions"] = bool(rec.get("allow_concurrent_sessions"))
        out.append(rec)
    return out


def get_remote_invite_by_email_passcode(
    email: str, passcode: str, *, con: sqlite3.Connection | None = None
) -> dict | None:
    """Constant-time lookup. Returns the invite dict on success, else None.

    Security: when no invite exists for ``email`` (e.g., email
    enumeration attack), still run one verification against a dummy hash
    so the response time matches the invite-exists branch (~30 ms).
    Without this, an attacker measuring the response latency can
    distinguish "email is registered, passcode wrong" (slow) from "email
    never invited" (fast) and enumerate emails.

    Transparent rehash-on-login: on a successful verify against a legacy
    ``scrypt$...`` hash (or any argon2 hash whose cost is below the
    current default), the row's passcode column is rewritten in place
    with a fresh ``hash_passcode(passcode)`` so the next login uses the
    current algorithm. This is the migration path off scrypt — once every
    active user has logged in once post-cutover, the DB is fully argon2id.
    """
    con = con or get_global_share_con()
    norm_email = (email or "").strip().lower()
    # Positive auth-method gate (design §2.5): the passcode path only ever
    # considers passcode invites. An OAuth invite carries a random placeholder
    # in ``passcode`` that no one holds, so entropy alone would already reject
    # it — but the explicit ``auth_method='passcode'`` filter makes the control
    # positive (not entropy-dependent) and keeps the two auth methods disjoint.
    rows = con.execute(
        "SELECT * FROM remote_invites WHERE lower(email)=? AND revoked=0 AND auth_method='passcode'",
        (norm_email,),
    ).fetchall()
    now = iso_z_now()
    match: dict | None = None
    matched_row_id: str | None = None
    matched_stored_hash: str | None = None
    expensive_verify_run = False
    for row in rows:
        # always run the verify so timing is roughly constant across the rows
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
        # Equalize timing when no expensive argon2 verify ran. If rows
        # existed and triggered a verify (email present, passcode wrong)
        # we already paid one verify per row inside the loop — running the
        # dummy verification again would push the wrong-passcode branch to
        # ``(N+1)×verify`` while the no-email branch stays at ``1×verify``,
        # recreating the 2× timing side-channel this function is meant to close.
        if not expensive_verify_run:
            _equalize_passcode_timing(passcode)
        return None

    # Transparent rehash-on-login. Done AFTER the match is committed to
    # ``match`` so a write failure here doesn't break the login — the
    # next successful login will retry the upgrade.
    if matched_stored_hash is not None and matched_row_id is not None and needs_rehash(matched_stored_hash):
        try:
            new_hash = hash_passcode(passcode)
            con.execute(
                "UPDATE remote_invites SET passcode=? WHERE id=?",
                (new_hash, matched_row_id),
            )
            con.commit()
            match["passcode"] = new_hash
        except Exception:
            # Don't let a rehash hiccup break login. Log and move on; the
            # next successful login will retry the upgrade.
            logger.exception("[share_db] rehash-on-login failed for invite_id=%s", matched_row_id)

    match["pii_policy"] = json.loads(match.get("pii_policy") or '{"mask_ips": false}')
    match["service_ids"] = get_remote_invite_services(match["id"], con=con)
    return match


def get_remote_invite_oauth(email: str, provider: str, *, con: sqlite3.Connection | None = None) -> dict | None:
    """Look up a live OAuth invite by ``(email, provider)`` — NO passcode check.

    Deliberately NOT ``get_remote_invite_by_email_passcode``: for the OAuth path
    identity is already established by the upstream ``id_token`` verification
    (signature / iss / aud / nonce / email_verified), so there is no passcode to
    verify and no argon2 timing surface to equalize. The positive ``auth_method``
    gate (``auth_method='oauth'``) is what guarantees a passcode invite can never
    be redeemed through the callback and vice-versa.

    Filters to ``revoked=0``, the exact ``oauth_provider``, and drops rows past
    ``expires_at`` in Python (mirrors the passcode path's expiry handling).
    Returns the enriched invite dict (``pii_policy`` parsed, ``service_ids``
    attached) for the first live match, else ``None``. Caller still re-applies
    the IP-whitelist + capacity gates and pins ``oauth_subject`` (see
    :func:`bind_invite_oauth_subject`).
    """
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
        match = dict(row)
        match["pii_policy"] = json.loads(match.get("pii_policy") or '{"mask_ips": false}')
        match["service_ids"] = get_remote_invite_services(match["id"], con=con)
        match["allow_concurrent_sessions"] = bool(match.get("allow_concurrent_sessions"))
        return match
    return None


def bind_invite_oauth_subject(invite_id: str, subject: str, *, con: sqlite3.Connection | None = None) -> bool:
    """Pin the id_token ``sub`` on first OAuth login; enforce it thereafter.

    Google's own guidance warns ``email`` can change over time, so identity is
    bound on the stable ``(provider, sub)`` pair — email is display/lookup only
    (§2.9). On the invite's first successful login ``oauth_subject`` is NULL and
    gets set to ``subject``; on every later login it must equal the stored value.

    The set is a single atomic ``UPDATE ... WHERE oauth_subject IS NULL`` so two
    concurrent first-logins can't both win — whichever commits first pins the
    subject, and the loser is then compared against it. Returns ``True`` iff the
    stored subject now equals ``subject`` (constant-time compare); ``False`` on
    mismatch (invite reused for a different account) — the caller treats a
    ``False`` exactly like invite-not-found (generic 403, no enumeration).
    """
    con = con or get_global_share_con()
    if not subject:
        return False
    with con:
        con.execute(
            "UPDATE remote_invites SET oauth_subject=? WHERE id=? AND oauth_subject IS NULL",
            (subject, invite_id),
        )
        row = con.execute("SELECT oauth_subject FROM remote_invites WHERE id=?", (invite_id,)).fetchone()
    if row is None or not row["oauth_subject"]:
        return False
    return hmac.compare_digest(str(row["oauth_subject"]), str(subject))


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


def update_remote_invite_pii(invite_id: str, pii_policy: dict | None, *, con: sqlite3.Connection | None = None) -> bool:
    """Update the PII policy (e.g. ``mask_ips``) on an existing invite.

    Lets an admin toggle IP masking after the invite was created. Validates
    via the same rules as create. Returns True on success, False if no invite
    with that id exists. Raises ``InvalidPiiPolicyError`` for an invalid policy
    (caller maps to HTTP 400). The next session validate re-syncs the live
    analyst session's policy from the invite, so an active analyst picks up the
    change without re-login.
    """
    policy = validate_pii_policy(pii_policy)
    con = con or get_global_share_con()
    cur = con.execute(
        "UPDATE remote_invites SET pii_policy=? WHERE id=?",
        (json.dumps(policy, separators=(",", ":")), invite_id),
    )
    con.commit()
    return cur.rowcount > 0


def set_invite_concurrent_sessions(invite_id: str, allow: bool, *, con: sqlite3.Connection | None = None) -> bool:
    """Toggle the invite's shared-login (concurrent-session) opt-in.

    When ``allow`` is True, later logins under this invite no longer boot the
    previous session, so multiple analysts can share the link (bounded by the
    global ``max_concurrent_analyst_sessions`` cap). Turning it back off only
    affects *future* logins — already-live sessions are left to age out.

    Returns True on success, False if no invite with that id exists.
    """
    con = con or get_global_share_con()
    cur = con.execute(
        "UPDATE remote_invites SET allow_concurrent_sessions=? WHERE id=?",
        (int(bool(allow)), invite_id),
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

    Key derivation: scrypt is retained here (NOT argon2) because the
    envelope is a one-shot passphrase-to-key derivation and changing the
    KDF would silently break old backup files. The per-invite passcode
    hash migration to argon2id is a separate concern (see passcode.py).
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
    key = hashlib.scrypt(
        passphrase.encode("utf-8"),
        salt=salt,
        n=_BACKUP_SCRYPT_N,
        r=_BACKUP_SCRYPT_R,
        p=_BACKUP_SCRYPT_P,
        dklen=_BACKUP_SCRYPT_DKLEN,
    )
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
    key = hashlib.scrypt(
        passphrase.encode("utf-8"),
        salt=salt,
        n=_BACKUP_SCRYPT_N,
        r=_BACKUP_SCRYPT_R,
        p=_BACKUP_SCRYPT_P,
        dklen=_BACKUP_SCRYPT_DKLEN,
    )
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
                     revoked, tos_accepted_at, tos_version, allow_concurrent_sessions,
                     auth_method, oauth_provider, oauth_subject)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    # Carry columns the hardcoded list historically dropped:
                    # allow_concurrent_sessions (added in migration 003) was
                    # silently lost on restore, and auth_method/oauth_provider/
                    # oauth_subject (migration 004) would convert every restored
                    # OAuth invite into an unusable passcode invite (permanent
                    # lockout) without this. auth_method defaults to 'passcode'.
                    int(inv.get("allow_concurrent_sessions") or 0),
                    inv.get("auth_method") or "passcode",
                    inv.get("oauth_provider"),
                    inv.get("oauth_subject"),
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
