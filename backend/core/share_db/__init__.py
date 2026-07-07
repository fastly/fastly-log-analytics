"""Global remote-share SQLite store.

Singleton DB at ``data/system/remote_share.db`` holding remote-analyst
invitations, service scopes, audit logs, share settings, persisted analyst
sessions, one-time claim tokens, and TOS versions.

Distinct from ``backend.core.metadata`` (per-service operational state)
intentionally: different lifecycle (one file, app-global), different lock
contention pattern, different audit scope (security material).

This module is the back-compat surface for the carved-up share_db package.
The implementation lives in per-concern submodules:

- ``connection`` — thread-local pool, PRAGMA setup, corruption self-heal
- ``schema`` — _SCHEMA tables + MIGRATIONS dict + apply_pending
- ``passcode`` — argon2id hashing + verify + cost-upgrade rehash + timing eq.
- ``validation`` — name/email/PII/IP-whitelist parsing
- ``invites`` — invite CRUD + claim tokens + GDPR erase + backup envelope
- ``sessions`` — analyst session CRUD
- ``audit`` — audit log writes + filtered reads + retention purge
- ``tos`` — TOS version reads
- ``settings`` — share_settings KV accessors

Every public symbol the rest of the codebase imported pre-carveup is
re-exported below so ``from backend.core import share_db`` keeps working.
"""

from __future__ import annotations

from backend.core.share_db.audit import (
    get_last_login_by_email,
    get_share_audit_logs,
    log_share_audit_event,
    purge_old_audit_logs,
)
from backend.core.share_db.connection import (
    close_all_connections,
    db_path,
    get_global_share_con,
    get_safe_share_db_connection,
    reset_for_tests,
)
from backend.core.share_db.invites import (
    bind_invite_oauth_subject,
    claim_token,
    create_claim_token,
    create_remote_invite,
    delete_remote_invite,
    export_backup,
    gdpr_erase,
    get_remote_invite,
    get_remote_invite_by_email_passcode,
    get_remote_invite_oauth,
    get_remote_invite_services,
    get_remote_invites,
    import_backup,
    mark_tos_accepted,
    revoke_remote_invite,
    set_invite_concurrent_sessions,
    update_remote_invite_passcode,
    update_remote_invite_pii,
    update_remote_invite_services,
)
from backend.core.share_db.passcode import (
    WeakPasscodeError,
    _equalize_passcode_timing,
    generate_wordphrase,
    hash_passcode,
    needs_rehash,
    validate_passcode_strength,
    verify_passcode,
)
from backend.core.share_db.schema import (
    _SCHEMA,
    LATEST_VERSION,
    MIGRATIONS,
    _init_db,
    apply_pending,
    get_current_version,
)
from backend.core.share_db.sessions import (
    delete_session,
    get_all_sessions,
    get_session,
    upsert_session,
)
from backend.core.share_db.settings import (
    MAX_CONCURRENT_ANALYST_SESSIONS_KEY,
    SHARE_AUDIT_RETENTION_DAYS_KEY,
    get_max_concurrent_sessions,
    get_setting,
    set_setting,
)
from backend.core.share_db.tos import get_latest_tos, publish_tos_version
from backend.core.share_db.validation import (
    InvalidEmailError,
    InvalidNameError,
    InvalidPiiPolicyError,
    apply_pii_policy,
    ip_in_whitelist,
    mask_ip,
    parse_ip_whitelist,
    validate_email,
    validate_name,
    validate_pii_policy,
)

# Re-export the date helpers — pre-carveup callers reach for them via
# ``share_db.iso_z_now()``.
from backend.utils.date_utils import iso_z, iso_z_now

__all__ = [
    # Date helpers (legacy re-export).
    "iso_z",
    "iso_z_now",
    # Connection layer.
    "db_path",
    "get_global_share_con",
    "get_safe_share_db_connection",
    "close_all_connections",
    "reset_for_tests",
    # Schema + migrations.
    "_SCHEMA",
    "_init_db",
    "MIGRATIONS",
    "LATEST_VERSION",
    "apply_pending",
    "get_current_version",
    # Passcode.
    "hash_passcode",
    "verify_passcode",
    "needs_rehash",
    "validate_passcode_strength",
    "WeakPasscodeError",
    "generate_wordphrase",
    "_equalize_passcode_timing",
    # Validation.
    "validate_name",
    "validate_email",
    "validate_pii_policy",
    "parse_ip_whitelist",
    "ip_in_whitelist",
    "InvalidNameError",
    "InvalidEmailError",
    "InvalidPiiPolicyError",
    "mask_ip",
    "apply_pii_policy",
    # Invites + claim tokens + backup + GDPR.
    "create_remote_invite",
    "get_remote_invite",
    "get_remote_invite_services",
    "get_remote_invites",
    "get_remote_invite_by_email_passcode",
    "get_remote_invite_oauth",
    "bind_invite_oauth_subject",
    "update_remote_invite_services",
    "update_remote_invite_passcode",
    "update_remote_invite_pii",
    "set_invite_concurrent_sessions",
    "revoke_remote_invite",
    "delete_remote_invite",
    "mark_tos_accepted",
    "create_claim_token",
    "claim_token",
    "export_backup",
    "import_backup",
    "gdpr_erase",
    # Sessions.
    "upsert_session",
    "delete_session",
    "get_session",
    "get_all_sessions",
    # Audit.
    "log_share_audit_event",
    "get_share_audit_logs",
    "get_last_login_by_email",
    "purge_old_audit_logs",
    # TOS.
    "get_latest_tos",
    "publish_tos_version",
    # Settings.
    "get_setting",
    "set_setting",
    "get_max_concurrent_sessions",
    "MAX_CONCURRENT_ANALYST_SESSIONS_KEY",
    "SHARE_AUDIT_RETENTION_DAYS_KEY",
]
