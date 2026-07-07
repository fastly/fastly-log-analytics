"""Unit tests for the share_db module — schema init, migrations, passcode hashing,
invite CRUD, audit logs, claim tokens, backup envelope, GDPR erase, PII masking.

These tests run against a per-test tmp_path SQLite file via the autouse
``isolate_share_db`` fixture; nothing touches the real ``data/system/`` dir.
"""

from __future__ import annotations

import json

import pytest

from backend.core import share_db

# ── Schema + migrations ─────────────────────────────────────────────────────


def test_init_creates_all_tables_and_seeds_settings(fresh_share_con):
    con = fresh_share_con
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    expected = {
        "remote_invites",
        "invite_services",
        "remote_share_audit_logs",
        "share_settings",
        "remote_sessions",
        "remote_invite_claim_tokens",
        "share_tos_versions",
    }
    assert expected.issubset(tables), tables

    # max_concurrent_analyst_sessions seeded by migration 001
    assert share_db.get_max_concurrent_sessions() == 10
    # TOS v1 seeded by migration 002
    tos = share_db.get_latest_tos()
    assert tos and tos["version"] == "v1"


def test_user_version_advances_to_latest(fresh_share_con):
    assert share_db.get_current_version(fresh_share_con) == share_db.LATEST_VERSION


def test_apply_pending_is_idempotent(fresh_share_con):
    """Calling apply_pending again is a no-op once we're at LATEST_VERSION."""
    n = share_db.apply_pending(fresh_share_con)
    assert n == 0  # nothing applied second time


def test_publish_tos_version_appends_and_is_idempotent(fresh_share_con):
    share_db.publish_tos_version("v2", "Updated terms.", con=fresh_share_con)
    tos = share_db.get_latest_tos(con=fresh_share_con)
    assert tos["version"] == "v2"
    assert tos["text"] == "Updated terms."

    # Re-publishing the same version is a no-op (doesn't append a duplicate row).
    share_db.publish_tos_version("v2", "Anything.", con=fresh_share_con)
    rows = fresh_share_con.execute("SELECT COUNT(*) FROM share_tos_versions WHERE version=?", ("v2",)).fetchone()
    assert rows[0] == 1


def test_share_setting_constants_have_reader_defaults(fresh_share_con):
    """Module constants resolve to a usable default even on a fresh DB.

    The legacy migration that seeded ``max_concurrent_analyst_sessions=10``
    was deleted in commit 8e0a8b6 ("drop legacy share-db migration") on
    the grounds that ``settings.get_max_concurrent_sessions`` already
    returns 10 when the row is missing — making the seed pure dev
    scaffolding. Pin the new contract: the reader hands back a sane
    integer default on an empty share_settings table.
    """
    keys = {r[0] for r in fresh_share_con.execute("SELECT key FROM share_settings").fetchall()}
    assert share_db.MAX_CONCURRENT_ANALYST_SESSIONS_KEY not in keys, (
        "post-8e0a8b6 the share-DB no longer seeds this row; if it did, the test contract needs revisiting"
    )
    # The reader is the source of truth — must default sensibly.
    assert share_db.get_max_concurrent_sessions(con=fresh_share_con) == 10


# ── Corruption self-heal ────────────────────────────────────────────────────


@pytest.mark.security_regression
def test_quarantines_corrupt_file_and_rebuilds(tmp_path, monkeypatch):
    """A garbage file at the DB path is moved aside and a fresh DB is created."""
    path = tmp_path / "system"
    path.mkdir(parents=True)
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(path))
    share_db.reset_for_tests()

    db_file = path / "remote_share.db"
    db_file.write_bytes(b"this is not a sqlite database, just garbage bytes")
    assert db_file.exists()

    # Should NOT raise — it quarantines the file and starts over.
    con = share_db.get_global_share_con()
    assert con is not None
    # Quarantine file exists.
    quarantined = list(path.glob("remote_share.db.corrupt-*"))
    assert len(quarantined) == 1
    # New DB has the schema.
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "remote_invites" in tables
    # Recovery audit row exists.
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "SHARE_DB_RECOVERED" for a in audits)


# ── Passcode hashing ────────────────────────────────────────────────────────


@pytest.mark.security_regression
def test_hash_then_verify_succeeds():
    h = share_db.hash_passcode("correct-horse-battery-staple")
    # New hashes use argon2id (OWASP 2026 default). Legacy ``scrypt$...``
    # is verify-only; see test_legacy_scrypt_hash_still_verifies below.
    assert h.startswith("$argon2")
    assert share_db.verify_passcode("correct-horse-battery-staple", h)


@pytest.mark.security_regression
def test_verify_wrong_passcode_fails():
    h = share_db.hash_passcode("right-one-here")
    assert not share_db.verify_passcode("wrong-one-here", h)


def test_hash_is_unique_per_call():
    h1 = share_db.hash_passcode("ocean-breeze-cabin-42")
    h2 = share_db.hash_passcode("ocean-breeze-cabin-42")
    assert h1 != h2  # different salt → different ciphertext


@pytest.mark.security_regression
def test_verify_rejects_malformed_stored():
    assert not share_db.verify_passcode("anything", "not-a-recognised-hash")
    assert not share_db.verify_passcode("anything", "scrypt$bad$format")
    assert not share_db.verify_passcode("anything", "$argon2id$broken")
    assert not share_db.verify_passcode("anything", "")


# ── Argon2id ────────────────────────────────────────────────────────────────


@pytest.mark.security_regression
def test_argon2id_hash_verifies():
    """The current default produces an argon2id hash that round-trips."""
    h = share_db.hash_passcode("ocean-breeze-cabin-42")
    assert h.startswith("$argon2id$")
    assert share_db.verify_passcode("ocean-breeze-cabin-42", h)


@pytest.mark.security_regression
def test_needs_rehash_only_flags_lower_cost_argon2():
    current = share_db.hash_passcode("ocean-breeze-cabin-42")
    assert share_db.needs_rehash(current) is False
    assert share_db.needs_rehash("") is False
    assert share_db.needs_rehash("garbage-not-a-hash") is False
    # Legacy scrypt format is no longer recognised — verify returns False
    # and needs_rehash returns False (nothing to rehash from a string we
    # can't even parse). The scrypt cutover is long since complete.
    assert share_db.needs_rehash("scrypt$16384$8$1$deadbeef$cafebabe") is False
    assert share_db.verify_passcode("anything", "scrypt$16384$8$1$deadbeef$cafebabe") is False


# ── Passcode strength validator ─────────────────────────────────────────────


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "weak",
    [
        "short1",
        "1234567890",  # all-digit
        "password",
        "PASSWORD",  # case-insensitive breach
        "letmein",
    ],
)
def test_validate_passcode_rejects_weak(weak):
    with pytest.raises(share_db.WeakPasscodeError):
        share_db.validate_passcode_strength(weak)


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "ok",
    [
        "ocean-breeze-cabin-42",
        "ThisIsLongEnough!",
        "summit-spark-haven-09",
    ],
)
def test_validate_passcode_accepts_strong(ok):
    share_db.validate_passcode_strength(ok)  # raises on failure


# ── Wordphrase ──────────────────────────────────────────────────────────────


def test_generate_wordphrase_shape_and_strength():
    phrase = share_db.generate_wordphrase()
    parts = phrase.split("-")
    assert len(parts) == 4
    # All wordphrases must pass the validator.
    share_db.validate_passcode_strength(phrase)


# ── Name / email validators ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "bad",
    ["", "   ", "<script>", "Drew\x00Bad", 'Bad"name', "Tag<a>", "amp&er"],
)
def test_validate_name_rejects_bad(bad):
    with pytest.raises(share_db.InvalidNameError):
        share_db.validate_name(bad)


@pytest.mark.parametrize("ok", ["Drew Michael", "Émilie O'Brien", "D'Angelo", "First-Last", "Mary J."])
def test_validate_name_accepts_ok(ok):
    assert share_db.validate_name(ok) == ok


@pytest.mark.parametrize("bad", ["", "not-an-email", "@bad.com", "user@", "user@bad"])
def test_validate_email_rejects_bad(bad):
    with pytest.raises(share_db.InvalidEmailError):
        share_db.validate_email(bad)


def test_validate_email_lowercases():
    assert share_db.validate_email("Drew@Example.COM") == "drew@example.com"


# ── IP whitelist parser ─────────────────────────────────────────────────────


def test_parse_ip_whitelist_handles_ips_and_cidrs():
    out = share_db.parse_ip_whitelist("1.2.3.4, 10.0.0.0/8, 2001:db8::/32")
    assert out == ["1.2.3.4", "10.0.0.0/8", "2001:db8::/32"]


def test_parse_ip_whitelist_rejects_garbage():
    with pytest.raises(ValueError):
        share_db.parse_ip_whitelist("not-an-ip")


def test_ip_in_whitelist_empty_allows_all():
    assert share_db.ip_in_whitelist("1.2.3.4", None)
    assert share_db.ip_in_whitelist("1.2.3.4", "")


def test_ip_in_whitelist_exact_match():
    assert share_db.ip_in_whitelist("10.0.0.1", "10.0.0.1,11.0.0.1")
    assert not share_db.ip_in_whitelist("12.0.0.1", "10.0.0.1,11.0.0.1")


def test_ip_in_whitelist_cidr_match():
    assert share_db.ip_in_whitelist("10.5.6.7", "10.0.0.0/8")
    assert not share_db.ip_in_whitelist("11.5.6.7", "10.0.0.0/8")


# ── PII policy validation: reject unenforced controls (silent no-op guard) ───


def test_validate_pii_policy_accepts_mask_ips():
    from backend.core.share_db.validation import validate_pii_policy

    assert validate_pii_policy({"mask_ips": True}) == {"mask_ips": True}
    assert validate_pii_policy(None) == {"mask_ips": False}
    assert validate_pii_policy({}) == {"mask_ips": False}


def test_validate_pii_policy_rejects_unenforced_controls_when_enabled():
    """mask_user_agent / mask_geo / redact_fields are not enforced anywhere;
    enabling them must error rather than be silently accepted-and-ignored."""
    import pytest

    from backend.core.share_db.validation import InvalidPiiPolicyError, validate_pii_policy

    for policy in (
        {"mask_user_agent": True},
        {"mask_geo": True},
        {"redact_fields": ["ip", "ua"]},
    ):
        with pytest.raises(InvalidPiiPolicyError):
            validate_pii_policy(policy)


def test_validate_pii_policy_allows_disabled_unenforced_controls():
    """Turning the unenforced knobs OFF (or an empty redact_fields) is a
    harmless no-op — accepted, and dropped from the stored policy."""
    from backend.core.share_db.validation import validate_pii_policy

    out = validate_pii_policy({"mask_ips": True, "mask_user_agent": False, "mask_geo": False, "redact_fields": []})
    assert out == {"mask_ips": True}


def test_validate_pii_policy_redact_fields_shape_error_precedes_rejection():
    import pytest

    from backend.core.share_db.validation import InvalidPiiPolicyError, validate_pii_policy

    with pytest.raises(InvalidPiiPolicyError, match="list of strings"):
        validate_pii_policy({"redact_fields": "ip"})


# ── Value-shape PII masking (H2) ─────────────────────────────────────────────


def test_mask_ip_is_idempotent_on_already_masked_v4():
    """The /api/query path masks by value, then the analyst-response middleware
    runs the key-name masker over the SAME body. Without the idempotency guard
    an ``ip`` column would degrade from '1.2.3.xxx' to '[redacted]'."""
    from backend.core.share_db.validation import mask_ip

    assert mask_ip("1.2.3.xxx") == "1.2.3.xxx"


def test_mask_ip_values_masks_by_value_not_key():
    from backend.core.share_db.validation import mask_ip_values

    out = mask_ip_values({"addr": "1.2.3.4", "url": "https://x/var/y", "country": "US"})
    assert out["addr"] == "1.2.3.xxx"  # masked despite the non-ip column name
    assert out["url"] == "https://x/var/y"  # non-IP string untouched
    assert out["country"] == "US"


def test_mask_ip_values_handles_ipv6_nested_and_xff():
    from backend.core.share_db.validation import mask_ip_values

    out = mask_ip_values(
        {
            "rows": [
                {"a": "2001:db8::1"},  # IPv6 → masked
                {"a": "9.9.9.9, 8.8.8.8"},  # XFF list → each element masked
                {"a": "not-an-ip"},  # untouched
                {"a": "256.1.1.1"},  # invalid octet → untouched
            ],
            "n": 5,
        }
    )
    assert out["rows"][0]["a"] == "2001:db8::"
    assert out["rows"][1]["a"] == "9.9.9.xxx, 8.8.8.xxx"
    assert out["rows"][2]["a"] == "not-an-ip"
    assert out["rows"][3]["a"] == "256.1.1.1"
    assert out["n"] == 5  # non-string values pass through


# ── Invite CRUD ─────────────────────────────────────────────────────────────


def test_create_invite_round_trips():
    inv = share_db.create_remote_invite(
        name="Drew Michael",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist="10.0.0.0/8",
        service_ids=["svcA", "svcB"],
    )
    fetched = share_db.get_remote_invite(inv["id"])
    assert fetched is not None
    assert fetched["email"] == "drew@example.com"
    assert fetched["name"] == "Drew Michael"
    assert fetched["service_ids"] == ["svcA", "svcB"]
    assert fetched["pii_policy"] == {"mask_ips": False}
    assert fetched["revoked"] == 0
    # Passcode is hashed, not stored plaintext. New invites use argon2id;
    # the legacy ``scrypt$...`` format is verify-only post-cutover.
    assert fetched["passcode"].startswith("$argon2")
    assert "ocean-breeze-cabin-42" not in fetched["passcode"]


def test_create_invite_defaults_allow_concurrent_sessions_false():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    fetched = share_db.get_remote_invite(inv["id"])
    assert fetched["allow_concurrent_sessions"] is False


def test_create_invite_allow_concurrent_sessions_persists():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        allow_concurrent_sessions=True,
    )
    fetched = share_db.get_remote_invite(inv["id"])
    assert fetched["allow_concurrent_sessions"] is True
    # Also surfaced (as a real bool) by the list accessor.
    listed = share_db.get_remote_invites()
    assert any(r["id"] == inv["id"] and r["allow_concurrent_sessions"] is True for r in listed)


def test_set_invite_concurrent_sessions_toggles():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    assert share_db.set_invite_concurrent_sessions(inv["id"], True) is True
    assert share_db.get_remote_invite(inv["id"])["allow_concurrent_sessions"] is True
    assert share_db.set_invite_concurrent_sessions(inv["id"], False) is True
    assert share_db.get_remote_invite(inv["id"])["allow_concurrent_sessions"] is False
    # Unknown invite id → False (no row updated).
    assert share_db.set_invite_concurrent_sessions("does-not-exist", True) is False


def test_migration_003_adds_allow_concurrent_sessions_column():
    """A pre-migration remote_invites table (without the column) gains it,
    defaulting to 0, and the migration is idempotent."""
    import sqlite3

    from backend.core.share_db import schema

    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """CREATE TABLE remote_invites (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL,
            passcode TEXT NOT NULL, created_at TEXT NOT NULL,
            revoked INTEGER NOT NULL DEFAULT 0
        )"""
    )
    con.execute(
        "INSERT INTO remote_invites(id, name, email, passcode, created_at) "
        "VALUES ('i1', 'n', 'e', 'p', '2020-01-01T00:00:00Z')"
    )
    con.commit()

    schema._migration_003_add_allow_concurrent_sessions(con)
    cols = {r[1] for r in con.execute("PRAGMA table_info(remote_invites)").fetchall()}
    assert "allow_concurrent_sessions" in cols
    existing = con.execute("SELECT allow_concurrent_sessions FROM remote_invites WHERE id='i1'").fetchone()
    assert existing["allow_concurrent_sessions"] == 0

    # Idempotent: the _has_column guard makes a second run a no-op.
    schema._migration_003_add_allow_concurrent_sessions(con)


def test_init_reconciles_column_when_user_version_ahead(tmp_path, monkeypatch):
    """Field-observed drift: a DB stamped user_version=3 but MISSING the column
    (migration skipped by the version gate). _init_db must self-heal it via the
    additive-column reconcile so create_remote_invite doesn't 500."""
    import sqlite3

    from backend.core.share_db import connection as conn
    from backend.core.share_db.schema import _init_db

    db_dir = tmp_path / "drift"
    db_dir.mkdir()
    db_file = db_dir / "remote_share.db"

    # Pre-seed a "migrated but column-missing" DB: version already at LATEST,
    # remote_invites without allow_concurrent_sessions.
    raw = sqlite3.connect(str(db_file))
    raw.executescript(
        """
        CREATE TABLE remote_invites (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL,
            passcode TEXT NOT NULL, created_at TEXT NOT NULL,
            revoked INTEGER NOT NULL DEFAULT 0
        );
        PRAGMA user_version = 3;
        """
    )
    raw.commit()
    raw.close()

    # Run the real init path against this file.
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(db_dir))
    conn.reset_for_tests()
    try:
        con = conn.get_global_share_con()
        cols = {r[1] for r in con.execute("PRAGMA table_info(remote_invites)")}
        assert "allow_concurrent_sessions" in cols
        # Reconcile is idempotent on a second open.
        _init_db(con)
    finally:
        conn.close_all_connections()
        conn.reset_for_tests()


def test_create_invite_weak_passcode_raises():
    with pytest.raises(share_db.WeakPasscodeError):
        share_db.create_remote_invite(
            name="Drew",
            email="drew@example.com",
            passcode="weak",
            expires_at_utc=None,
            ip_whitelist=None,
            service_ids=[],
        )


def test_get_by_email_passcode_succeeds():
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    found = share_db.get_remote_invite_by_email_passcode("drew@example.com", "ocean-breeze-cabin-42")
    assert found is not None
    assert found["email"] == "drew@example.com"


def test_get_by_email_passcode_wrong_passcode_returns_none():
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    assert share_db.get_remote_invite_by_email_passcode("drew@example.com", "wrong-one") is None


def test_get_by_email_passcode_revoked_returns_none():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    share_db.revoke_remote_invite(inv["id"])
    assert share_db.get_remote_invite_by_email_passcode("drew@example.com", "ocean-breeze-cabin-42") is None


def test_get_by_email_passcode_expired_returns_none():
    """An expired invite is not returned by the login lookup."""
    from datetime import UTC, datetime, timedelta

    past = share_db.iso_z(datetime.now(UTC) - timedelta(days=1))
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=past,
        ip_whitelist=None,
        service_ids=[],
    )
    assert share_db.get_remote_invite_by_email_passcode("drew@example.com", "ocean-breeze-cabin-42") is None


def test_update_invite_services_replaces_set():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["a", "b"],
    )
    share_db.update_remote_invite_services(inv["id"], ["c"])
    assert share_db.get_remote_invite_services(inv["id"]) == ["c"]


# ── OAuth invites (migration 004: auth_method / oauth_provider / oauth_subject) ─


def test_migration_004_adds_oauth_columns():
    """A pre-migration remote_invites table gains the OAuth columns, existing
    rows backfill to auth_method='passcode', and the migration is idempotent."""
    import sqlite3

    from backend.core.share_db import schema

    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """CREATE TABLE remote_invites (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL,
            passcode TEXT NOT NULL, created_at TEXT NOT NULL,
            revoked INTEGER NOT NULL DEFAULT 0
        )"""
    )
    con.execute(
        "INSERT INTO remote_invites(id, name, email, passcode, created_at) "
        "VALUES ('i1', 'n', 'e', 'p', '2020-01-01T00:00:00Z')"
    )
    con.commit()

    schema._migration_004_add_oauth_columns(con)
    cols = {r[1] for r in con.execute("PRAGMA table_info(remote_invites)").fetchall()}
    assert {"auth_method", "oauth_provider", "oauth_subject"}.issubset(cols)
    row = con.execute("SELECT auth_method, oauth_provider, oauth_subject FROM remote_invites WHERE id='i1'").fetchone()
    assert row["auth_method"] == "passcode"  # backfilled via DEFAULT
    assert row["oauth_provider"] is None
    assert row["oauth_subject"] is None

    # Idempotent: the _has_column guard makes a second run a no-op.
    schema._migration_004_add_oauth_columns(con)


def test_fresh_db_has_oauth_columns(fresh_share_con):
    """The _SCHEMA snapshot (fresh DB) carries the OAuth columns without needing
    the ALTER migration to run."""
    cols = {r[1] for r in fresh_share_con.execute("PRAGMA table_info(remote_invites)").fetchall()}
    assert {"auth_method", "oauth_provider", "oauth_subject"}.issubset(cols)


def test_create_passcode_invite_defaults_auth_method():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    fetched = share_db.get_remote_invite(inv["id"])
    assert fetched["auth_method"] == "passcode"
    assert fetched["oauth_provider"] is None
    assert fetched["oauth_subject"] is None


def test_create_oauth_invite_synthesizes_placeholder_passcode():
    """An OAuth invite needs no passcode; the NOT NULL column is filled with an
    unguessable argon2id-hashed placeholder that is never communicated."""
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode=None,
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        auth_method="oauth",
        oauth_provider="google",
    )
    fetched = share_db.get_remote_invite(inv["id"])
    assert fetched["auth_method"] == "oauth"
    assert fetched["oauth_provider"] == "google"
    assert fetched["oauth_subject"] is None
    # Placeholder is a real argon2id hash (satisfies NOT NULL) but is not a
    # usable login credential — no plaintext is ever returned.
    assert fetched["passcode"].startswith("$argon2")


def test_create_oauth_invite_requires_provider():
    with pytest.raises(ValueError, match="oauth_provider is required"):
        share_db.create_remote_invite(
            name="Drew",
            email="drew@example.com",
            passcode=None,
            expires_at_utc=None,
            ip_whitelist=None,
            service_ids=["svcA"],
            auth_method="oauth",
            oauth_provider=None,
        )


def test_create_passcode_invite_requires_passcode():
    with pytest.raises(ValueError, match="passcode is required"):
        share_db.create_remote_invite(
            name="Drew",
            email="drew@example.com",
            passcode=None,
            expires_at_utc=None,
            ip_whitelist=None,
            service_ids=["svcA"],
        )


def test_create_invite_rejects_unknown_auth_method():
    with pytest.raises(ValueError, match="unknown auth_method"):
        share_db.create_remote_invite(
            name="Drew",
            email="drew@example.com",
            passcode="ocean-breeze-cabin-42",
            expires_at_utc=None,
            ip_whitelist=None,
            service_ids=["svcA"],
            auth_method="saml",
        )


def _make_oauth_invite(email="analyst@corp.com", provider="google", **kw):
    """Build an OAuth invite through the real producer (not a hand-shaped dict)
    so tests exercise the same NOT-NULL placeholder path production uses."""
    return share_db.create_remote_invite(
        name="Analyst",
        email=email,
        passcode=None,
        expires_at_utc=kw.get("expires_at_utc"),
        ip_whitelist=kw.get("ip_whitelist"),
        service_ids=kw.get("service_ids", ["svcA"]),
        auth_method="oauth",
        oauth_provider=provider,
    )


def test_get_remote_invite_oauth_matches_only_oauth_and_provider():
    _make_oauth_invite(email="analyst@corp.com", provider="google")
    # A passcode invite for the SAME email must never be returned by the OAuth lookup.
    share_db.create_remote_invite(
        name="Analyst",
        email="pass@corp.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    found = share_db.get_remote_invite_oauth("analyst@corp.com", "google")
    assert found is not None
    assert found["auth_method"] == "oauth"
    assert found["oauth_provider"] == "google"
    assert found["service_ids"] == ["svcA"]
    # Wrong provider, unknown email, and the passcode invite's email all miss.
    assert share_db.get_remote_invite_oauth("analyst@corp.com", "okta") is None
    assert share_db.get_remote_invite_oauth("nobody@corp.com", "google") is None
    assert share_db.get_remote_invite_oauth("pass@corp.com", "google") is None


def test_get_remote_invite_oauth_excludes_revoked_and_expired():
    from datetime import UTC, datetime, timedelta

    inv = _make_oauth_invite(email="revoked@corp.com")
    share_db.revoke_remote_invite(inv["id"])
    assert share_db.get_remote_invite_oauth("revoked@corp.com", "google") is None

    past = share_db.iso_z(datetime.now(UTC) - timedelta(days=1))
    _make_oauth_invite(email="expired@corp.com", expires_at_utc=past)
    assert share_db.get_remote_invite_oauth("expired@corp.com", "google") is None


def test_get_remote_invite_oauth_case_insensitive_email():
    _make_oauth_invite(email="mixed@corp.com")
    assert share_db.get_remote_invite_oauth("MiXeD@Corp.Com", "google") is not None


@pytest.mark.security_regression
def test_passcode_lookup_never_returns_oauth_invite():
    """Positive auth-method gate: the passcode login lookup must never resolve
    an OAuth invite, even if an attacker somehow guessed the machine-generated
    placeholder passcode (they can't — it's never revealed). Filtered at the SQL
    layer via auth_method='passcode'."""
    from unittest.mock import patch

    inv = _make_oauth_invite(email="gate@corp.com", provider="google")
    # Even handed the exact stored hash's plaintext is impossible; assert the
    # lookup returns None for ANY passcode against an OAuth invite. The email
    # has no passcode invite, so the no-rows timing-equalization path runs.
    with patch("backend.core.share_db.invites._equalize_passcode_timing") as eq:
        assert share_db.get_remote_invite_by_email_passcode("gate@corp.com", "anything-at-all-here") is None
        eq.assert_called_once()  # treated as "email not invited" (no passcode row)
    # Sanity: the invite genuinely exists on the OAuth path.
    assert share_db.get_remote_invite_oauth("gate@corp.com", "google") is not None
    _ = inv


def test_bind_invite_oauth_subject_pins_then_enforces():
    inv = _make_oauth_invite(email="pin@corp.com")
    # First login pins the subject.
    assert share_db.bind_invite_oauth_subject(inv["id"], "google-sub-123") is True
    assert share_db.get_remote_invite(inv["id"])["oauth_subject"] == "google-sub-123"
    # Subsequent login with the same subject passes.
    assert share_db.bind_invite_oauth_subject(inv["id"], "google-sub-123") is True
    # A different subject is rejected and does NOT overwrite the pin.
    assert share_db.bind_invite_oauth_subject(inv["id"], "attacker-sub-999") is False
    assert share_db.get_remote_invite(inv["id"])["oauth_subject"] == "google-sub-123"
    # Empty subject never binds.
    assert share_db.bind_invite_oauth_subject(inv["id"], "") is False


def test_backup_round_trip_preserves_oauth_invite(monkeypatch, tmp_path):
    """An OAuth invite must survive export→import with auth_method/provider
    intact — otherwise a restore silently converts it to a passcode invite with
    an unguessable placeholder → permanent lockout (design §2.5)."""
    inv = _make_oauth_invite(email="restore@corp.com", provider="google")
    share_db.bind_invite_oauth_subject(inv["id"], "google-sub-abc")
    blob = share_db.export_backup("very-long-strong-passphrase")

    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "wipe"))
    share_db.reset_for_tests()
    out = share_db.import_backup(blob, "very-long-strong-passphrase")
    assert out["inserted"] == 1
    restored = share_db.get_remote_invite_oauth("restore@corp.com", "google")
    assert restored is not None
    assert restored["auth_method"] == "oauth"
    assert restored["oauth_provider"] == "google"
    assert restored["oauth_subject"] == "google-sub-abc"


def test_backup_round_trip_preserves_allow_concurrent_sessions(monkeypatch, tmp_path):
    """Regression: the hardcoded import_backup column list historically dropped
    allow_concurrent_sessions, silently flipping shared invites back to
    single-seat on restore."""
    inv = share_db.create_remote_invite(
        name="Drew",
        email="shared@corp.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        allow_concurrent_sessions=True,
    )
    blob = share_db.export_backup("very-long-strong-passphrase")

    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "wipe"))
    share_db.reset_for_tests()
    share_db.import_backup(blob, "very-long-strong-passphrase")
    restored = [r for r in share_db.get_remote_invites() if r["email"] == "shared@corp.com"]
    assert restored and restored[0]["allow_concurrent_sessions"] is True


# ── Audit logs ──────────────────────────────────────────────────────────────


def test_log_and_fetch_audit_logs():
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS",
        email="drew@example.com",
        ip_address="10.0.0.5",
        details="ok",
    )
    rows = share_db.get_share_audit_logs(limit=10)
    assert rows and rows[0]["event_type"] == "LOGIN_SUCCESS"
    assert rows[0]["ip_address"] == "10.0.0.5"


def test_purge_old_audit_logs():
    """Manually insert a row with an old timestamp, confirm it's purged."""
    con = share_db.get_global_share_con()
    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?, ?, ?, ?, ?)",
        ("2020-01-01T00:00:00Z", "ANCIENT", "drew@x.com", "1.2.3.4", "x"),
    )
    con.commit()
    n = share_db.purge_old_audit_logs(retention_days=30)
    assert n >= 1


def test_get_last_login_by_email():
    """Last login is the MAX timestamp over successful-login events, per email."""
    con = share_db.get_global_share_con()
    # alice: two passcode logins + one OAuth login; bob: one; a FAIL for alice
    # and a non-login event must NOT count toward the last-login timestamp.
    rows = [
        ("2026-06-01T00:00:00Z", "LOGIN_SUCCESS", "alice@corp.com"),
        ("2026-06-02T00:00:00Z", "LOGIN_SUCCESS", "alice@corp.com"),
        ("2026-06-05T09:30:00Z", "LOGIN_SUCCESS_OAUTH", "Alice@corp.com"),  # case-insensitive
        ("2026-06-09T00:00:00Z", "LOGIN_FAIL", "alice@corp.com"),  # later, but a FAIL
        ("2026-06-10T00:00:00Z", "TOS_ACCEPTED", "alice@corp.com"),  # later, non-login
        ("2026-05-20T12:00:00Z", "LOGIN_SUCCESS", "bob@corp.com"),
    ]
    for ts, event, email in rows:
        con.execute(
            "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) "
            "VALUES (?, ?, ?, ?, ?)",
            (ts, event, email, "1.2.3.4", "x"),
        )
    con.commit()

    last = share_db.get_last_login_by_email()
    # OAuth login is the most recent SUCCESS; the later FAIL/TOS rows are ignored.
    assert last["alice@corp.com"] == "2026-06-05T09:30:00Z"
    assert last["bob@corp.com"] == "2026-05-20T12:00:00Z"
    # An email with only failed logins never appears.
    assert "nobody@corp.com" not in last


def test_get_last_login_by_email_empty():
    """No login events → empty map (not an error)."""
    share_db.get_global_share_con()
    assert share_db.get_last_login_by_email() == {}


# ── Claim token (one-time-view) ─────────────────────────────────────────────


def test_claim_token_one_shot():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    token = share_db.create_claim_token(inv["id"], ttl_hours=1)
    # First claim succeeds.
    row1 = share_db.claim_token(token, "1.2.3.4")
    assert row1 is not None and row1["invite_id"] == inv["id"]
    # Second claim fails (already claimed).
    assert share_db.claim_token(token, "1.2.3.4") is None


def test_claim_token_expired_returns_none():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    # Manually backdate expires_at.
    token = share_db.create_claim_token(inv["id"], ttl_hours=1)
    con = share_db.get_global_share_con()
    con.execute(
        "UPDATE remote_invite_claim_tokens SET expires_at=? WHERE token=?",
        ("2020-01-01T00:00:00Z", token),
    )
    con.commit()
    assert share_db.claim_token(token, "1.2.3.4") is None


# ── Backup / restore ────────────────────────────────────────────────────────


def test_backup_round_trip(monkeypatch, tmp_path):
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    blob = share_db.export_backup("very-long-strong-passphrase")
    assert blob.startswith(b"FOSBACKUP\x01")

    # Wipe the DB and re-import.
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "wipe"))
    share_db.reset_for_tests()
    out = share_db.import_backup(blob, "very-long-strong-passphrase")
    assert out["inserted"] == 1
    invs = share_db.get_remote_invites()
    assert len(invs) == 1
    assert invs[0]["email"] == "drew@example.com"
    assert invs[0]["service_ids"] == ["svcA"]


def test_import_wrong_passphrase_raises():
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    blob = share_db.export_backup("right-passphrase-here")
    with pytest.raises(ValueError, match="failed to decrypt"):
        share_db.import_backup(blob, "wrong-passphrase-here")


def test_import_truncated_blob_raises():
    with pytest.raises(ValueError, match="truncated"):
        share_db.import_backup(b"FOSBACKUP\x01" + b"\x00" * 5, "anything-long-enough")


def test_import_skips_collisions_by_default():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    blob = share_db.export_backup("very-long-strong-passphrase")
    out = share_db.import_backup(blob, "very-long-strong-passphrase", mode="skip-collisions")
    assert out["skipped"] == 1
    assert out["inserted"] == 0


def test_import_merges_services_on_collision():
    inv = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    blob = share_db.export_backup("very-long-strong-passphrase")
    share_db.update_remote_invite_services(inv["id"], ["svcB"])
    out = share_db.import_backup(blob, "very-long-strong-passphrase", mode="merge-services-on-collision")
    assert out["merged"] == 1
    # Both services attached now.
    services = share_db.get_remote_invite_services(inv["id"])
    assert set(services) == {"svcA", "svcB"}


def test_import_abort_on_collision_raises():
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    blob = share_db.export_backup("very-long-strong-passphrase")
    with pytest.raises(ValueError, match="email collision"):
        share_db.import_backup(blob, "very-long-strong-passphrase", mode="abort")


def test_import_rejects_newer_schema():
    payload = {
        "schema_version": share_db.LATEST_VERSION + 99,
        "exported_at": share_db.iso_z_now(),
        "invites": [],
        "invite_services": [],
        "share_settings": [],
    }
    import hashlib
    import secrets

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    salt = secrets.token_bytes(16)
    key = hashlib.scrypt(b"long-good-passphrase", salt=salt, n=2**14, r=8, p=1, dklen=32)
    nonce = secrets.token_bytes(12)
    ct = AESGCM(key).encrypt(nonce, json.dumps(payload).encode(), None)
    blob = b"FOSBACKUP\x01" + salt + nonce + ct
    with pytest.raises(ValueError, match="newer than this build"):
        share_db.import_backup(blob, "long-good-passphrase")


# ── GDPR right-to-be-forgotten ──────────────────────────────────────────────


def test_gdpr_erase_deletes_invite_and_redacts_old_audits():
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    # An ancient row that gets redacted.
    con = share_db.get_global_share_con()
    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?, ?, ?, ?, ?)",
        ("2020-01-01T00:00:00Z", "LOGIN_SUCCESS", "drew@example.com", "1.2.3.4", "x"),
    )
    # A recent row that's PRESERVED.
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS",
        email="drew@example.com",
        ip_address="2.3.4.5",
        details="recent",
    )

    out = share_db.gdpr_erase("drew@example.com", reason="DSR-2026-001")
    assert out["deleted_invites"] == 1
    assert out["redacted_log_rows"] == 1
    assert out["retained_recent_rows"] >= 1

    # Erased: invite gone.
    assert share_db.get_remote_invite_by_email_passcode("drew@example.com", "ocean-breeze-cabin-42") is None
    # Ancient audit is redacted.
    rows = share_db.get_share_audit_logs()
    ancient = [r for r in rows if r["timestamp"].startswith("2020")]
    assert ancient and ancient[0]["email"] == "[GDPR-ERASED]"


def test_gdpr_erase_requires_email():
    with pytest.raises(ValueError):
        share_db.gdpr_erase("", "DSR-2026-002")


# ── PII masking ─────────────────────────────────────────────────────────────


def test_mask_ip_v4():
    assert share_db.mask_ip("192.168.1.42") == "192.168.1.xxx"


def test_mask_ip_v6():
    masked = share_db.mask_ip("2001:db8::dead:beef")
    assert masked.startswith("2001:db8:")
    assert masked.endswith("::")


def test_mask_ip_unparseable_fails_closed():
    """Finding 019: a value that doesn't cleanly parse as an IP must NOT be
    returned verbatim — that fails open and leaks PII the mask exists to
    hide. Garbage, malformed octets, an XFF list, and trailing whitespace
    all redact. Empty/None stay empty (nothing to leak)."""
    assert share_db.mask_ip("not-an-ip") == "[redacted]"
    assert share_db.mask_ip("192.168.1.1 ") == "[redacted]"
    assert share_db.mask_ip("1.2.3.4, 5.6.7.8") == "[redacted]"
    assert share_db.mask_ip("999.999.999.999") == "[redacted]"
    assert share_db.mask_ip("") == ""


def test_apply_pii_policy_walks_nested_dicts():
    obj = {
        "rows": [
            {"ip": "10.0.0.1", "path": "/login"},
            {"client_ip": "192.168.1.50", "path": "/health"},
        ],
        "summary": {"ip_address": "1.2.3.4"},
    }
    out = share_db.apply_pii_policy(obj, {"mask_ips": True})
    assert out["rows"][0]["ip"] == "10.0.0.xxx"
    assert out["rows"][1]["client_ip"] == "192.168.1.xxx"
    assert out["summary"]["ip_address"] == "1.2.3.xxx"


def test_apply_pii_policy_off_passes_through():
    obj = {"ip": "10.0.0.1"}
    out = share_db.apply_pii_policy(obj, {"mask_ips": False})
    assert out == obj


def test_apply_pii_policy_walks_lists_and_arrays():
    obj = {
        "client_ip": ["1.2.3.4", "5.6.7.8"],
        "nested_list": [{"ip_address": "10.0.0.1"}, {"ip_address": "192.168.1.1"}],
    }
    out = share_db.apply_pii_policy(obj, {"mask_ips": True})
    assert out["client_ip"] == ["1.2.3.xxx", "5.6.7.xxx"]
    assert out["nested_list"][0]["ip_address"] == "10.0.0.xxx"
    assert out["nested_list"][1]["ip_address"] == "192.168.1.xxx"


@pytest.mark.security_regression
def test_get_remote_invite_timing_equalization():
    """Closes the email-enumeration 2x timing side-channel.

    Patched at the invites module (the actual call site) rather than the
    share_db package re-export, because ``invites.py`` binds the symbol
    at import time and would not see a patch applied to the package
    namespace.
    """
    from unittest.mock import patch

    # 1. Call with a non-existent email -> must equalize timing once
    with patch("backend.core.share_db.invites._equalize_passcode_timing") as mock_equalize:
        res = share_db.get_remote_invite_by_email_passcode("nonexistent@example.com", "some-passcode")
        assert res is None
        mock_equalize.assert_called_once_with("some-passcode")

    # 2. Call with an existing email but wrong passcode -> must NOT equalize timing because we already paid
    # verify cost in loop
    share_db.create_remote_invite(
        name="Drew",
        email="existing_timing_test@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    with patch("backend.core.share_db.invites._equalize_passcode_timing") as mock_equalize:
        res = share_db.get_remote_invite_by_email_passcode("existing_timing_test@example.com", "wrong-passcode")
        assert res is None
        mock_equalize.assert_not_called()
