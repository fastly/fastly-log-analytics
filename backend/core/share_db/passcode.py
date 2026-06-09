"""Passcode hashing, verification, strength validation, and wordphrase
generation for the share flow.

Algorithm policy:
- New hashes (``hash_passcode``) use argon2id with OWASP 2026 parameters.
  Stored format is the PasswordHasher modular-crypt string, which starts
  with ``$argon2id$``.
- ``verify_passcode`` accepts BOTH the new argon2id format and the legacy
  ``scrypt$N$r$p$saltHex$digestHex`` format so pre-cutover invites keep
  working. ``needs_rehash`` returns True for any stored hash that isn't
  argon2id at the current cost — the login handler (``get_remote_invite_
  by_email_passcode``) uses that signal to transparently rehash on
  successful login.

Timing equalization: ``_equalize_passcode_timing`` runs one verify against
a dummy hash so the no-email-match branch has the same ~30ms cost as the
email-match branch. The dummy is created via the real ``hash_passcode``
function so a future cost-parameter change is automatically reflected.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHash, VerifyMismatchError

# ── argon2id (current default) ──────────────────────────────────────────────

# OWASP 2026 recommended argon2id parameters. memory_cost is in KiB —
# 65536 = 64 MiB per hash. time_cost=3 lands at ~30ms on the GCE
# n2-standard-2 (matches the scrypt cost the timing-equalization path
# was tuned for). parallelism=4 fits the typical 2-4 vCPU prod sizing.
# hash_len=32 matches the legacy scrypt dklen.
_HASHER = PasswordHasher(
    memory_cost=65536,
    time_cost=3,
    parallelism=4,
    hash_len=32,
)

# ── Legacy scrypt parameters (verify-only) ──────────────────────────────────

# Kept so the verify branch can parse pre-cutover stored hashes. New
# hashes go through argon2 — these constants are no longer used to
# produce hashes from the public API.
_SCRYPT_N = 2**14
_SCRYPT_R = 8
_SCRYPT_P = 1
_SCRYPT_DKLEN = 32


def hash_passcode(passcode: str) -> str:
    """Hash via argon2id (current default).

    Returns the argon2 PasswordHasher modular-crypt string
    (``$argon2id$v=19$m=...,t=...,p=...$saltB64$digestB64``). ``verify_
    passcode`` accepts this format alongside the legacy ``scrypt$...``
    format so pre-cutover stored hashes keep working.
    """
    return _HASHER.hash(passcode)


def _verify_scrypt(passcode: str, stored: str) -> bool:
    """Constant-time verify against the legacy ``scrypt$N$r$p$saltHex$digestHex`` format."""
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


def verify_passcode(passcode: str, stored: str) -> bool:
    """Constant-time verify. Accepts argon2id AND legacy scrypt formats.

    Returns False for any unrecognized prefix, malformed payload, or
    mismatch. The caller (``get_remote_invite_by_email_passcode``) can
    then check ``needs_rehash(stored)`` to opportunistically upgrade
    legacy scrypt hashes to argon2id on successful login.
    """
    if not stored:
        return False
    # argon2 hashes always start with $argon2 — the PasswordHasher emits
    # both ``$argon2id$`` (default) and ``$argon2i$``/``$argon2d$``
    # depending on type; we accept whatever argon2-cffi can parse.
    if stored.startswith("$argon2"):
        try:
            _HASHER.verify(stored, passcode)
            return True
        except (VerifyMismatchError, InvalidHash):
            return False
        except Exception:
            # Defensive: any unexpected argon2 error treated as mismatch
            # rather than re-raised, matching the legacy scrypt branch's
            # behavior (return False on any parse / compute failure).
            return False
    if stored.startswith("scrypt$"):
        return _verify_scrypt(passcode, stored)
    return False


def needs_rehash(stored: str) -> bool:
    """True when ``stored`` is on an older algorithm or weaker params than the current default.

    Used by the login flow to transparently upgrade scrypt → argon2id on
    successful login. Returns True for:
    - Any legacy ``scrypt$...`` hash (always upgrade off scrypt).
    - Any argon2 hash whose parameters fall below the current ``_HASHER``
      cost (argon2-cffi tells us this via ``check_needs_rehash``).
    Returns False if the hash is already at the current default or if the
    string isn't a recognised hash at all (no point trying to rotate
    something we can't even parse).
    """
    if not stored:
        return False
    if stored.startswith("scrypt$"):
        return True
    if stored.startswith("$argon2"):
        try:
            return _HASHER.check_needs_rehash(stored)
        except InvalidHash:
            return False
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


def generate_wordphrase() -> str:
    """Secure random string with >100 bits of entropy."""
    return f"{secrets.token_hex(4)}-{secrets.token_hex(4)}-{secrets.token_hex(4)}-{secrets.token_hex(4)}"


# ── Timing equalization ─────────────────────────────────────────────────────

_dummy_hash: str | None = None


def _equalize_passcode_timing(passcode: str) -> None:
    """Run one verification against a fixed dummy argon2id hash so the
    timing of the "no email match" branch matches the "email match, wrong
    passcode" branch.

    The dummy hash is generated via the real ``hash_passcode`` function so
    any future parameter change in argon2 config is automatically
    reflected. Generated once per process and reused — generating per-call
    would add measurable extra cost to the miss branch.
    """
    global _dummy_hash
    if _dummy_hash is None:
        _dummy_hash = hash_passcode("__dummy_for_timing_equalization__")
    verify_passcode(passcode, _dummy_hash)
