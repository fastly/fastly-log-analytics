"""Passcode hashing, verification, strength validation, and wordphrase
generation for the share flow.

Argon2id with OWASP 2026 parameters. ``verify_passcode`` accepts the
argon2 PasswordHasher modular-crypt string format; ``needs_rehash``
returns True for argon2 hashes whose parameters fall below the current
``_HASHER`` cost so the login handler can rotate on successful login.

Timing equalization: ``_equalize_passcode_timing`` runs one verify against
a dummy hash so the no-email-match branch has the same ~30ms cost as the
email-match branch. The dummy is created via the real ``hash_passcode``
function so a future cost-parameter change is automatically reflected.
"""

from __future__ import annotations

import secrets

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHash, VerifyMismatchError

# ── argon2id ────────────────────────────────────────────────────────────────

# OWASP 2026 recommended argon2id parameters. memory_cost is in KiB —
# 65536 = 64 MiB per hash. time_cost=3 lands at ~30ms on the GCE
# n2-standard-2. parallelism=4 fits the typical 2-4 vCPU prod sizing.
_HASHER = PasswordHasher(
    memory_cost=65536,
    time_cost=3,
    parallelism=4,
    hash_len=32,
)


def hash_passcode(passcode: str) -> str:
    """Hash via argon2id.

    Returns the argon2 PasswordHasher modular-crypt string
    (``$argon2id$v=19$m=...,t=...,p=...$saltB64$digestB64``).
    """
    return _HASHER.hash(passcode)


def verify_passcode(passcode: str, stored: str) -> bool:
    """Constant-time verify against an argon2id stored hash.

    Returns False for any unrecognized prefix, malformed payload, or
    mismatch. The caller (``get_remote_invite_by_email_passcode``) can
    then check ``needs_rehash(stored)`` to opportunistically rotate
    argon2 hashes whose cost parameters fall below the current default.
    """
    if not stored or not stored.startswith("$argon2"):
        return False
    try:
        _HASHER.verify(stored, passcode)
        return True
    except (VerifyMismatchError, InvalidHash):
        return False
    except Exception:
        # Defensive: any unexpected argon2 error treated as mismatch
        # rather than re-raised.
        return False


def needs_rehash(stored: str) -> bool:
    """True when ``stored`` is an argon2 hash whose parameters fall below
    the current ``_HASHER`` cost (argon2-cffi tells us via
    ``check_needs_rehash``). Returns False otherwise.
    """
    if not stored or not stored.startswith("$argon2"):
        return False
    try:
        return _HASHER.check_needs_rehash(stored)
    except InvalidHash:
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
