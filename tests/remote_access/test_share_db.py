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
    tables = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema()"
        ).fetchall()
    }
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


# ── Passcode hashing ────────────────────────────────────────────────────────


@pytest.mark.security_regression
def test_hash_then_verify_succeeds():
    h = share_db.hash_passcode("correct-horse-battery-staple")
    # New hashes use argon2id (OWASP 2026 default).
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
    # Non-argon2 strings are never recognised: verify returns False and
    # needs_rehash returns False.
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
    assert share_db.validate_passcode_strength(ok) is None


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
    from backend.core.share_db import validate_pii_policy

    assert validate_pii_policy({"mask_ips": True}) == {"mask_ips": True}
    assert validate_pii_policy(None) == {"mask_ips": False}
    assert validate_pii_policy({}) == {"mask_ips": False}


def test_validate_pii_policy_rejects_unenforced_controls_when_enabled():
    """mask_user_agent / mask_geo / redact_fields are not enforced anywhere;
    enabling them must error rather than be silently accepted-and-ignored."""
    import pytest

    from backend.core.share_db import InvalidPiiPolicyError, validate_pii_policy

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
    from backend.core.share_db import validate_pii_policy

    out = validate_pii_policy({"mask_ips": True, "mask_user_agent": False, "mask_geo": False, "redact_fields": []})
    assert out == {"mask_ips": True}


def test_validate_pii_policy_redact_fields_shape_error_precedes_rejection():
    import pytest

    from backend.core.share_db import InvalidPiiPolicyError, validate_pii_policy

    with pytest.raises(InvalidPiiPolicyError, match="list of strings"):
        validate_pii_policy({"redact_fields": "ip"})


# ── Value-shape PII masking (H2) ─────────────────────────────────────────────


def test_mask_ip_is_idempotent_on_already_masked_v4():
    """The /api/query path masks by value, then the analyst-response middleware
    runs the key-name masker over the SAME body. Without the idempotency guard
    an ``ip`` column would degrade from '1.2.3.xxx' to '[redacted]'."""
    from backend.core.share_db import mask_ip

    assert mask_ip("1.2.3.xxx") == "1.2.3.xxx"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "value",
    ["203.0.113.7, 198.51.100.xxx", "999.0.0.xxx"],
)
def test_mask_ip_redacts_malformed_values_with_xxx_suffix(value):
    from backend.core.share_db import apply_pii_policy, mask_ip

    assert mask_ip(value) == "[redacted]"
    assert apply_pii_policy({"client_ip": value}, {"mask_ips": True})["client_ip"] == "[redacted]"


def test_mask_ip_values_masks_by_value_not_key():
    from backend.core.share_db import mask_ip_values

    out = mask_ip_values({"addr": "1.2.3.4", "url": "https://x/var/y", "country": "US"})
    assert out["addr"] == "1.2.3.xxx"  # masked despite the non-ip column name
    assert out["url"] == "https://x/var/y"  # non-IP string untouched
    assert out["country"] == "US"


def test_mask_ip_values_handles_ipv6_nested_and_xff():
    from backend.core.share_db import mask_ip_values

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
    # Passcode is hashed, not stored plaintext. Invites use argon2id.
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


def test_fresh_db_has_oauth_columns(fresh_share_con):
    """The _SCHEMA snapshot (fresh DB) carries the OAuth columns without needing
    the ALTER migration to run."""
    cols = {
        r[0]
        for r in fresh_share_con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='remote_invites' AND table_schema=current_schema()"
        ).fetchall()
    }
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
    with patch("backend.core.share_db._equalize_passcode_timing") as eq:
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

    con = share_db.get_global_share_con()
    con.execute("DELETE FROM remote_invites")
    con.commit()
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

    con = share_db.get_global_share_con()
    con.execute("DELETE FROM remote_invites")
    con.commit()
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
    con = share_db.get_global_share_con()
    con.execute("DELETE FROM remote_invites")
    con.commit()
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
    with patch("backend.core.share_db._equalize_passcode_timing") as mock_equalize:
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
    with patch("backend.core.share_db._equalize_passcode_timing") as mock_equalize:
        res = share_db.get_remote_invite_by_email_passcode("existing_timing_test@example.com", "wrong-passcode")
        assert res is None
        mock_equalize.assert_not_called()


# ── Merged from tests/remote_access/test_tunnel.py ──

"""Unit tests for the TunnelManager: fingerprint, rate limiter, session
lifecycle, multi-device boot, validate_session timeouts, persistence
round-trip via share_db, panic, and start_sharing input validation.

The SSH-to-localhost.run code path was removed in v2.0 — only direct-mode
(HTTPS public_endpoint) is exercised here.
"""


import time

import pytest

from backend.utils import tunnel

# ── Fingerprint ─────────────────────────────────────────────────────────────


def test_fingerprint_stable_across_minor_ua_updates():
    """A patch-level Chrome update should NOT change the fingerprint."""
    base = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) Chrome/126.0.6478.127 Safari/537.36",
        "sec-ch-ua-platform": '"macOS"',
    }
    patched = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) Chrome/126.0.6478.200 Safari/537.36",
        "sec-ch-ua-platform": '"macOS"',
    }
    assert tunnel.compute_fingerprint(base) == tunnel.compute_fingerprint(patched)


def test_fingerprint_changes_with_major_browser_bump():
    a = tunnel.compute_fingerprint({"user-agent": "Chrome/126.0.0.0", "sec-ch-ua-platform": '"macOS"'})
    b = tunnel.compute_fingerprint({"user-agent": "Chrome/127.0.0.0", "sec-ch-ua-platform": '"macOS"'})
    assert a != b


def test_fingerprint_changes_with_os():
    a = tunnel.compute_fingerprint({"user-agent": "Chrome/126 Macintosh", "sec-ch-ua-platform": '"macOS"'})
    b = tunnel.compute_fingerprint({"user-agent": "Chrome/126 Windows NT 10.0", "sec-ch-ua-platform": '"Windows"'})
    assert a != b


def test_fingerprint_immune_to_client_hints():
    """Absence or variance of sec-ch-ua-platform must NOT alter fingerprint."""
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) Chrome/126.0.0.0 Safari/537.36"
    a = tunnel.compute_fingerprint({"user-agent": ua, "sec-ch-ua-platform": '"macOS"'})
    b = tunnel.compute_fingerprint({"user-agent": ua})
    c = tunnel.compute_fingerprint({"user-agent": ua, "sec-ch-ua-platform": ""})
    assert a == b == c


# ── Rate limiter ────────────────────────────────────────────────────────────


def test_rate_limiter_locks_after_threshold():
    rl = tunnel._LoginRateLimiter()
    # Below threshold = no lock.
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD - 1):
        triggered = rl.record_failure("1.2.3.4")
        assert not triggered
    locked, _ = rl.is_locked("1.2.3.4")
    assert not locked
    # Threshold-th failure triggers.
    assert rl.record_failure("1.2.3.4") is True
    locked, remaining = rl.is_locked("1.2.3.4")
    assert locked
    assert 0 < remaining <= tunnel.LOGIN_LOCKOUT_S


def test_rate_limiter_failures_persist_across_lockout():
    """Per-IP failure counters must NOT be clearable by a successful login —
    otherwise an attacker with valid credentials can interleave brute-force
    guesses with valid logins to keep the counter below threshold indefinitely
    (finding 016). The clear() method was removed."""
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("1.2.3.4")
    locked, _ = rl.is_locked("1.2.3.4")
    assert locked
    assert not hasattr(rl, "clear"), "clear() method must not exist (finding 016)"


def test_rate_limiter_per_ip_isolation():
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("1.2.3.4")
    assert rl.is_locked("1.2.3.4")[0]
    assert not rl.is_locked("5.6.7.8")[0]


# ── Session lifecycle ──────────────────────────────────────────────────────


def _seed_invite(service_ids=None, *, allow_concurrent_sessions=False) -> dict:
    return share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=service_ids or ["svcA"],
        allow_concurrent_sessions=allow_concurrent_sessions,
    )


def test_create_session_persists_and_returns_fingerprint():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X", "sec-ch-ua-platform": '"macOS"'},
    )
    assert session.session_id
    assert session.email == "drew@example.com"
    assert session.service_ids == ["svcA"]
    assert session.fingerprint_signature
    # Persisted.
    rows = share_db.get_all_sessions()
    assert len(rows) == 1


@pytest.mark.security_regression
def test_create_session_cap_raises_when_at_capacity():
    mgr = tunnel.get_tunnel_manager()
    invite_a = _seed_invite(allow_concurrent_sessions=True)
    invite_b = share_db.create_remote_invite(
        name="Bea",
        email="bea@example.com",
        passcode="seagull-mountain-river-77",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        allow_concurrent_sessions=True,
    )
    mgr.create_session(invite=invite_a, ip_address="1.2.3.4", user_agent="ua", headers={}, cap=1)
    with pytest.raises(tunnel.TunnelCapacityError) as exc_info:
        mgr.create_session(invite=invite_b, ip_address="5.6.7.8", user_agent="ua", headers={}, cap=1)
    assert exc_info.value.current == 1
    assert exc_info.value.cap == 1
    # The rejected attempt must not have been inserted.
    assert mgr.active_session_count() == 1


def test_create_session_cap_none_preserves_uncapped_behavior():
    """Callers that don't pass ``cap`` (or pass None) keep the old
    unconditional-insert behavior — cap is opt-in, not a breaking change."""
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite(allow_concurrent_sessions=True)
    for i in range(5):
        mgr.create_session(invite=invite, ip_address=f"1.2.3.{i}", user_agent="ua", headers={})
    assert mgr.active_session_count() == 5


@pytest.mark.security_regression
def test_create_session_cap_is_atomic_under_concurrent_logins():
    """Regression for finding 016: concurrent logins racing the capacity
    check must not both observe count < cap and both insert. Before the
    fix, active_session_count() and create_session() were two separate
    lock acquisitions in the caller (share_auth.py / share_oauth.py); now
    the check-and-insert happens under ONE acquisition inside
    create_session(cap=...), so the cap is never transiently exceeded no
    matter how many threads race it."""
    import concurrent.futures

    mgr = tunnel.get_tunnel_manager()
    cap = 3
    n_workers = 20
    invites = [
        share_db.create_remote_invite(
            name=f"Analyst{i}",
            email=f"analyst{i}@example.com",
            passcode="ocean-breeze-cabin-42",
            expires_at_utc=None,
            ip_whitelist=None,
            service_ids=["svcA"],
            allow_concurrent_sessions=True,
        )
        for i in range(n_workers)
    ]

    def attempt(invite):
        try:
            mgr.create_session(invite=invite, ip_address="1.2.3.4", user_agent="ua", headers={}, cap=cap)
            return "ok"
        except tunnel.TunnelCapacityError:
            return "rejected"

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
        outcomes = list(pool.map(attempt, invites))

    assert outcomes.count("ok") == cap
    assert outcomes.count("rejected") == n_workers - cap
    assert mgr.active_session_count() == cap


def test_validate_session_returns_session_when_fresh():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    assert mgr.validate_session(session.session_id) is not None


def test_validate_session_idle_timeout_evicts():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Force last_active to far in the past.
    session.last_active_time = "2020-01-01T00:00:00Z"
    assert mgr.validate_session(session.session_id) is None
    # Audit row written.
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "SESSION_TIMEOUT" for a in audits)


def test_validate_session_absolute_lifetime_evicts():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Login >24h ago.
    session.login_time = "2020-01-01T00:00:00Z"
    session.last_active_time = "2020-01-01T00:00:00Z"
    assert mgr.validate_session(session.session_id) is None


def test_validate_session_revoked_invite_evicts():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    share_db.revoke_remote_invite(invite["id"])
    assert mgr.validate_session(session.session_id) is None


def test_multi_device_boot():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    first = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    second = mgr.create_session(
        invite=invite,
        ip_address="5.6.7.8",
        user_agent="Firefox",
        headers={"user-agent": "Firefox/120 Windows"},
    )
    assert mgr.get_session(first.session_id) is None
    assert mgr.get_session(second.session_id) is not None
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "SESSION_BOOT" for a in audits)


def test_shared_invite_allows_concurrent_sessions():
    """An invite opted into shared logins keeps both sessions alive instead of
    booting the first — and writes no SESSION_BOOT audit row."""
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite(allow_concurrent_sessions=True)
    first = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    second = mgr.create_session(
        invite=invite,
        ip_address="5.6.7.8",
        user_agent="Firefox",
        headers={"user-agent": "Firefox/120 Windows"},
    )
    # Both sessions coexist under the same invite.
    assert mgr.get_session(first.session_id) is not None
    assert mgr.get_session(second.session_id) is not None
    assert first.session_id != second.session_id
    assert mgr.active_session_count() == 2
    # No boot audit event was written for the shared invite.
    audits = share_db.get_share_audit_logs()
    assert not any(a["event_type"] == "SESSION_BOOT" for a in audits)


def test_touch_session_bumps_last_active_and_ip():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    original = "2026-05-31T20:00:00Z"
    session.last_active_time = original
    # Force forward without real-time sleep
    mgr.touch_session(session.session_id, last_activity="GET /api/bootstrap", new_ip="5.6.7.8")
    assert session.last_activity == "GET /api/bootstrap"
    assert session.ip_address == "5.6.7.8"
    assert session.last_active_time > original


def test_touch_session_ip_roam_does_not_bump_idle_clock():
    """bump_active=False updates the roamed IP WITHOUT resetting the idle clock.

    The IP-roaming touch fires on every request from a new egress IP — on a
    rotating-NAT proxy that's nearly every request, so bumping last_active_time
    there would pin the session alive forever and bypass the X-User-Active idle
    gate. The IP must still update (whitelist re-validation); the clock must not.
    """
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    original = "2026-05-31T20:00:00Z"
    session.last_active_time = original
    mgr.touch_session(session.session_id, new_ip="9.9.9.9", bump_active=False)
    assert session.ip_address == "9.9.9.9", "roamed IP must still update"
    assert session.last_active_time == original, "IP roam must NOT reset the idle clock"


def test_boot_sessions_for_invite_counts_correctly():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    s = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    n = mgr.boot_sessions_for_invite(invite["id"], reason="revoked")
    assert n == 1
    assert mgr.get_session(s.session_id) is None


def test_rehydrate_after_restart_keeps_fresh_sessions():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    s = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Drop the singleton (simulates uvicorn restart).
    tunnel.reset_for_tests()
    mgr2 = tunnel.get_tunnel_manager()
    kept = mgr2.rehydrate_sessions()
    assert kept == 1
    assert mgr2.get_session(s.session_id) is not None
    # And the service scope was re-attached.
    assert mgr2.get_session(s.session_id).service_ids == ["svcA"]


def test_rehydrate_prunes_expired_sessions():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Force the row's login_time to be ancient.
    con = share_db.get_global_share_con()
    con.execute(
        "UPDATE remote_sessions SET login_time=?, last_active_time=?", ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
    )
    con.commit()

    tunnel.reset_for_tests()
    mgr2 = tunnel.get_tunnel_manager()
    kept = mgr2.rehydrate_sessions()
    assert kept == 0
    # And the row is deleted from the persistence table.
    assert share_db.get_all_sessions() == []


def test_panic_boots_all_and_writes_audit():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    result = mgr.panic()
    assert result["sessions_booted"] == 1
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "PANIC_TRIGGERED" for a in audits)


# ── start_sharing input validation ─────────────────────────────────────────


def test_start_sharing_rejects_bare_http():
    mgr = tunnel.get_tunnel_manager()
    with pytest.raises(ValueError, match="HTTPS"):
        mgr.start_sharing(public_endpoint="http://insecure.example.com")


def test_start_sharing_requires_public_endpoint():
    mgr = tunnel.get_tunnel_manager()
    with pytest.raises(ValueError, match="public_endpoint"):
        mgr.start_sharing(public_endpoint=None)


def test_direct_expose_https_records_audit_and_returns_url():
    mgr = tunnel.get_tunnel_manager()
    out = mgr.start_sharing(public_endpoint="https://demo.example.com")
    assert out["public_url"] == "https://demo.example.com"
    assert "tunnel_url" not in out
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "SHARE_START" for a in audits)
    mgr.stop_sharing()
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "SHARE_STOP" for a in audits)


# ── Concurrent session capacity ────────────────────────────────────────────


def test_active_session_count_tracks_dict_size():
    mgr = tunnel.get_tunnel_manager()
    assert mgr.active_session_count() == 0
    a = share_db.create_remote_invite(
        name="A",
        email="a@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    b = share_db.create_remote_invite(
        name="B",
        email="b@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    mgr.create_session(invite=a, ip_address="1.1.1.1", user_agent="x", headers={"user-agent": "Chrome/1 Mac OS X"})
    mgr.create_session(invite=b, ip_address="2.2.2.2", user_agent="x", headers={"user-agent": "Chrome/1 Mac OS X"})
    assert mgr.active_session_count() == 2


# ── Telemetry & observability ──────────────────────────────────────────────


def test_record_heartbeat_unauth_increments_counter():
    mgr = tunnel.get_tunnel_manager()
    assert mgr.get_telemetry()["heartbeat_unauth_count"] == 0
    mgr.record_heartbeat_unauth()
    mgr.record_heartbeat_unauth()
    assert mgr.get_telemetry()["heartbeat_unauth_count"] == 2


def test_rate_limit_snapshot_reports_failures_and_lockouts():
    mgr = tunnel.get_tunnel_manager()
    # 1 failure (sub-threshold) and a full lockout from a second ip.
    mgr.record_login_failure("1.2.3.4", "a@example.com")
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        mgr.record_login_failure("5.6.7.8", "b@example.com")
    snap = mgr.get_rate_limit_snapshot()
    fail_ips = {row["ip"] for row in snap["failures"]}
    lock_ips = {row["ip"] for row in snap["lockouts"]}
    assert "1.2.3.4" in fail_ips
    assert "5.6.7.8" in lock_ips
    # The locked IP also still has a failure record (until pruned by window).
    locked_row = next(r for r in snap["lockouts"] if r["ip"] == "5.6.7.8")
    assert locked_row["remaining_s"] > 0


def test_rate_limit_snapshot_prunes_expired_lockouts(monkeypatch):
    mgr = tunnel.get_tunnel_manager()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        mgr.record_login_failure("9.9.9.9", "c@example.com")
    # Time-travel past the lockout window.
    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + tunnel.LOGIN_LOCKOUT_S + 1)
    snap = mgr.get_rate_limit_snapshot()
    assert all(row["ip"] != "9.9.9.9" for row in snap["lockouts"])


def test_telemetry_records_uptime_history_on_stop():
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://demo.example.com")
    mgr.stop_sharing()
    history = mgr.get_telemetry()["tunnel_uptime_history"]
    assert len(history) == 1
    assert history[0]["reason"] == "stop"
    assert history[0]["duration_s"] >= 0
    assert history[0]["started_at"]
    assert history[0]["ended_at"]


def test_telemetry_records_uptime_history_on_panic():
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://demo.example.com")
    mgr.panic()
    history = mgr.get_telemetry()["tunnel_uptime_history"]
    assert any(entry["reason"] == "panic" for entry in history)


def test_telemetry_history_is_bounded():
    mgr = tunnel.get_tunnel_manager()
    # Cycle the tunnel 55 times; ring should retain only the last 50.
    for _ in range(55):
        mgr.start_sharing(public_endpoint="https://demo.example.com")
        mgr.stop_sharing()
    # Internal buffer is bounded; the exposed slice is the last 20.
    assert len(mgr._tunnel_uptime_history) == 50
    assert len(mgr.get_telemetry()["tunnel_uptime_history"]) == 20


def test_telemetry_current_uptime_reflects_running_tunnel():
    mgr = tunnel.get_tunnel_manager()
    assert mgr.get_telemetry()["current_uptime_s"] is None
    mgr.start_sharing(public_endpoint="https://demo.example.com")
    uptime = mgr.get_telemetry()["current_uptime_s"]
    assert uptime is not None and uptime >= 0
    mgr.stop_sharing()
    assert mgr.get_telemetry()["current_uptime_s"] is None


# ── Audit-log filtering ────────────────────────────────────────────────────


def test_get_share_audit_logs_filters_by_event_type():
    share_db.log_share_audit_event(event_type="LOGIN_FAIL", email="a@example.com", ip_address="1.1.1.1", details="x")
    share_db.log_share_audit_event(event_type="LOGIN_SUCCESS", email="a@example.com", ip_address="1.1.1.1", details="x")
    fails = share_db.get_share_audit_logs(event_type="LOGIN_FAIL")
    assert fails and all(r["event_type"] == "LOGIN_FAIL" for r in fails)


def test_get_share_audit_logs_filters_by_email_substring():
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL", email="alice@example.com", ip_address="1.1.1.1", details="x"
    )
    share_db.log_share_audit_event(event_type="LOGIN_FAIL", email="bob@example.com", ip_address="1.1.1.1", details="x")
    rows = share_db.get_share_audit_logs(email_substr="alice")
    assert rows and all("alice" in (r["email"] or "") for r in rows)


def test_get_share_audit_logs_filters_by_time_window():
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    before = (now - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    after = (now + timedelta(seconds=5)).isoformat().replace("+00:00", "Z")

    share_db.log_share_audit_event(event_type="LOGIN_FAIL", email="t@example.com", ip_address="1.1.1.1", details="x")
    rows = share_db.get_share_audit_logs(since=before, until=after, email_substr="t@example.com")
    assert rows
    # `until` before window excludes everything.
    rows = share_db.get_share_audit_logs(until=before, email_substr="t@example.com")
    assert not rows


# ── LRU Eviction Under Capacity ────────────────────────────────────────────


def test_rate_limiter_lru_eviction(monkeypatch):
    try:
        import backend.utils.tunnel as rate_limiter
    except ImportError:
        import backend.utils.tunnel as rate_limiter

    # Set MAX_TRACKED_IPS to 3 for testing.
    monkeypatch.setattr(rate_limiter, "MAX_TRACKED_IPS", 3)

    rl = rate_limiter._LoginRateLimiter()

    # Record 1 failure for 3 different IPs.
    rl.record_failure("1.1.1.1")
    rl.record_failure("2.2.2.2")
    rl.record_failure("3.3.3.3")

    # Order of self._events should be: "1.1.1.1", "2.2.2.2", "3.3.3.3"
    assert list(rl._events.keys()) == ["1.1.1.1", "2.2.2.2", "3.3.3.3"]

    # Touch "1.1.1.1" again (moves it to the end/MRU).
    rl.record_failure("1.1.1.1")
    assert list(rl._events.keys()) == ["2.2.2.2", "3.3.3.3", "1.1.1.1"]

    # Record failure for a 4th IP. "2.2.2.2" (oldest/LRU) should be evicted.
    rl.record_failure("4.4.4.4")
    assert list(rl._events.keys()) == ["3.3.3.3", "1.1.1.1", "4.4.4.4"]
    assert "2.2.2.2" not in rl._events

    # Trigger lockout for 3 different IPs.
    for _ in range(rate_limiter.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("3.3.3.3")
    for _ in range(rate_limiter.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("1.1.1.1")
    for _ in range(rate_limiter.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("4.4.4.4")

    # Order of lockouts should be: "3.3.3.3", "1.1.1.1", "4.4.4.4"
    assert list(rl._locks.keys()) == ["3.3.3.3", "1.1.1.1", "4.4.4.4"]

    # Trigger a lockout for "5.5.5.5". "3.3.3.3" (oldest lockout) should be evicted.
    for _ in range(rate_limiter.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("5.5.5.5")

    assert list(rl._locks.keys()) == ["1.1.1.1", "4.4.4.4", "5.5.5.5"]
    assert "3.3.3.3" not in rl._locks


# ── F014: mid-session permission revocation propagation ──────────────────


def test_validate_session_propagates_explicit_pii_policy_null_revocation(monkeypatch):
    """An admin who clears ``pii_policy`` (sets it to None on the invite)
    must immediately downgrade an active session's pii_policy on the next
    validate_session() call.

    Regression for F014 (audit run 7ba15352): the prior
    ``if new_pii_policy is not None: session.pii_policy = ...`` silently
    dropped the revocation. The session kept its cached permissive
    pii_policy until the 2h-idle / 24h-absolute timeout, which defeats
    the entire point of mid-session re-sync.
    """
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Pretend the session started with a permissive policy.
    session.pii_policy = {"mode": "passthrough"}

    # Admin explicitly clears the policy on the invite (None means
    # "no policy", not "leave alone").
    fake_invite = {**invite, "pii_policy": None, "service_ids": ["svcA"]}
    monkeypatch.setattr(share_db, "get_remote_invite", lambda _: fake_invite)

    out = mgr.validate_session(session.session_id)
    assert out is not None, "session should still be valid; only permissions downgrade"
    assert out.pii_policy is None, (
        f"explicit pii_policy=None on the invite must propagate to the session; "
        f"got {out.pii_policy!r} (F014 regression)"
    )


def test_validate_session_propagates_explicit_service_ids_null_revocation(monkeypatch):
    """An admin who clears ``service_ids`` (sets it to None on the invite)
    must immediately empty the session's service_ids on the next
    validate_session() call — i.e. no services accessible.

    Regression for F014 (audit run 7ba15352): the prior
    ``if fresh_service_ids is not None: session.service_ids = ...``
    silently dropped the revocation, leaving the cached permissive list
    in place until natural session timeout.
    """
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite(service_ids=["svcA", "svcB"])
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    assert session.service_ids == ["svcA", "svcB"]

    # Admin clears service_ids — should empty the session immediately.
    fake_invite = {**invite, "service_ids": None}
    monkeypatch.setattr(share_db, "get_remote_invite", lambda _: fake_invite)

    out = mgr.validate_session(session.session_id)
    assert out is not None, "session is still valid; only permissions downgrade"
    assert out.service_ids == [], (
        f"explicit service_ids=None on the invite must empty the session's list; "
        f"got {out.service_ids!r} (F014 regression)"
    )


# ── Merged from test_tunnel_state.py ──

from pathlib import Path

import pytest

from backend.utils.tunnel import (
    TunnelState,
    clear_persisted_state,
    persist_direct_state,
    restore_direct_state,
)


@pytest.fixture
def state_file(monkeypatch, tmp_path: Path) -> Path:
    """Redirect the persisted-state path to a per-test temp file."""
    # backend.config.DATA_DIR is what _state_file_path resolves against.
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path)
    return tmp_path / "tunnel_state.json"


def test_tunnel_state_default_values():
    s = TunnelState()
    assert s.public_endpoint is None
    assert s.forward_port == 3000
    assert not hasattr(s, "direct_socket_addr")


def test_persist_then_restore_round_trip(state_file: Path):
    s = TunnelState(public_endpoint="share.example.com", forward_port=8080)
    persist_direct_state(s)

    assert state_file.exists()
    data = json.loads(state_file.read_text())
    assert data == {
        "public_endpoint": "share.example.com",
        "forward_port": 8080,
    }

    restored = TunnelState()
    ok = restore_direct_state(restored)
    assert ok is True
    assert restored.public_endpoint == "share.example.com"
    assert restored.forward_port == 8080
    # started_at is a recent ISO timestamp string.
    assert isinstance(restored.started_at, str)
    assert restored.started_at  # non-empty


def test_persist_swallows_io_errors(monkeypatch):
    """persist_direct_state is best-effort — an IO failure must not
    propagate up to the tunnel manager and break setup."""
    import backend.utils.tunnel as state_mod

    monkeypatch.setattr(state_mod, "_state_file_path", lambda: "/nonexistent/dir/x.json")
    # Must not raise — exception is logged + swallowed.
    persist_direct_state(TunnelState(public_endpoint="x"))


def test_clear_removes_existing_file(state_file: Path):
    state_file.write_text("{}")
    assert state_file.exists()
    clear_persisted_state()
    assert not state_file.exists()


def test_clear_is_noop_when_file_missing(state_file: Path):
    assert not state_file.exists()
    # Must not raise.
    clear_persisted_state()


def test_clear_swallows_io_errors(monkeypatch):
    import backend.utils.tunnel as state_mod

    # Make the path point to something that exists but can't be removed
    # (a directory, in this case — os.remove on a dir raises OSError).
    monkeypatch.setattr(state_mod, "_state_file_path", lambda: "/")
    # Must not raise — error is logged.
    clear_persisted_state()


def test_restore_returns_false_when_no_file(state_file: Path):
    s = TunnelState()
    assert restore_direct_state(s) is False
    # State unchanged.
    assert s.public_endpoint is None


def test_restore_returns_false_when_endpoint_empty(state_file: Path):
    state_file.write_text(json.dumps({"forward_port": 3000}))
    s = TunnelState()
    assert restore_direct_state(s) is False
    assert s.public_endpoint is None


def test_restore_returns_false_when_json_invalid(state_file: Path):
    state_file.write_text("{not-json")
    s = TunnelState()
    # JSON parse error swallowed → returns False.
    assert restore_direct_state(s) is False


def test_restore_defaults_forward_port_when_missing(state_file: Path):
    state_file.write_text(json.dumps({"public_endpoint": "a.b.c"}))
    s = TunnelState()
    assert restore_direct_state(s) is True
    assert s.forward_port == 3000  # default kicks in


def test_tos_and_settings_ttl_caches_hit_and_invalidate_when_pytest_env_unset(fresh_share_con, monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    share_db._TOS_CACHE.clear()
    share_db._SETTINGS_CACHE.clear()

    tos1 = share_db.get_latest_tos()
    assert tos1 is not None
    assert "latest" in share_db._TOS_CACHE
    tos2 = share_db.get_latest_tos()
    assert tos2 == tos1

    share_db.publish_tos_version("v99-cache-test", "Cached TOS body.")
    assert len(share_db._TOS_CACHE) == 0
    tos3 = share_db.get_latest_tos()
    assert tos3 is not None and tos3["version"] == "v99-cache-test"
    assert "latest" in share_db._TOS_CACHE

    val1 = share_db.get_setting("custom_cache_key", "fallback")
    assert val1 == "fallback"
    assert "custom_cache_key" in share_db._SETTINGS_CACHE
    assert share_db.get_setting("custom_cache_key", "other") == "other"

    share_db.set_setting("custom_cache_key", "persisted-val")
    assert len(share_db._SETTINGS_CACHE) == 0
    assert share_db.get_setting("custom_cache_key", "fallback") == "persisted-val"
    assert share_db._SETTINGS_CACHE.get("custom_cache_key") == "persisted-val"
