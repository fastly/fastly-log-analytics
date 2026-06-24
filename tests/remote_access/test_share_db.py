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
