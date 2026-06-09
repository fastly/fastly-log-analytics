# Architectural Design Specification: Share DB Refactoring (`share_db.py` Carve-up + argon2 Adoption)

## 1. Context & Motivation

The `backend/core/share_db.py` file is a 1,312-line module that owns the global remote-share SQLite store at `data/system/remote_share.db`. It handles invitations, service-scope mappings, analyst sessions, audit logs, share settings, one-time claim tokens, TOS versions, passcode hashing, name/email/PII/IP-whitelist validation, and corruption self-heal.

### Problems with the Current Shape

1. **Eight unrelated concerns in one file.** Connection management, schema/migrations, passcode crypto, input validation, IP-whitelist parsing, invite CRUD, session CRUD, audit logs, TOS management — all in one module.
2. **Security-critical code interleaved with plain CRUD.** Passcode timing equalization (`_equalize_passcode_timing` — closes a 2× scrypt timing side-channel) sits next to ordinary invite-list queries.
3. **scrypt is fine; argon2id is 2026-current.** Current passcode hashing uses scrypt (N=2^14, r=8, p=1, dklen=32). OWASP 2026 recommends argon2id as the primary algorithm. argon2-cffi is pure-Python pip-installable, no SaaS, no SaaS dependency. The carveup is the right moment to swap.
4. **Test isolation is awkward.** Any test touching share_db pulls in the whole module — including scrypt costs that slow time-dependent tests (passcode hash is ~30ms per call by design).
5. **Reuse signal lost.** Validation helpers (`validate_name`, `validate_email`, `validate_pii_policy`, `parse_ip_whitelist`, `ip_in_whitelist`) are generic enough to be useful elsewhere; today they're buried.

### Decision (per planning round)

- **Carve into a `share_db/` package** along functional boundaries (8 modules).
- **Adopt argon2-cffi** for new passcode hashing; keep scrypt verification working for existing stored hashes (graceful migration on next login).
- **Adopt freezegun** in tests for the TTL / session timeout / claim-token expiry paths.
- Preserve all current behavior: corruption self-heal, thread-local pooling, scrypt-fallback verify, timing equalization, migrations framework (PRAGMA user_version namespace separate from per-service `metadata_db`).

---

## 2. Refactored Package Directory

```
backend/core/share_db/
├── __init__.py          # Re-exports for 100% backward compat
├── connection.py        # Pool, PRAGMA setup, corruption self-heal + quarantine
├── schema.py            # _SCHEMA tables + migrations framework + apply_pending
├── passcode.py          # argon2id (default) + scrypt (legacy verify) + strength + wordphrase + timing eq.
├── validation.py        # name/email/PII/IP-whitelist parsing + validation
├── invites.py           # Invite CRUD + claim tokens + TOS-accept marker
├── sessions.py          # Session CRUD (upsert/get/delete/get_all/prune)
├── audit.py             # log_share_audit_event + get_share_audit_logs
├── tos.py               # Get latest TOS + publish new TOS version
└── settings.py          # share_settings KV (e.g. max_concurrent_analyst_sessions)
```

The original `backend/core/share_db.py` becomes a single-line re-export shim:

```python
# backend/core/share_db.py
from backend.core.share_db import *  # noqa: F401,F403 — back-compat shim
```

All current imports (e.g., `from backend.core import share_db; share_db.upsert_session(...)`) continue to work.

---

## 3. Module Responsibilities

### `share_db/connection.py` (~150 lines)

Owns:
- `_DATA_DIR`, `_DB_FILENAME` constants + `db_path()`
- Thread-local connection pool (`_local`, `_conn_pool()`, `_init_lock`, `_initialized`, `_all_connections`, `_all_connections_lock`)
- `get_safe_share_db_connection()` — corruption self-heal with quarantine (the restricted-to-actual-corruption-signature guard is preserved verbatim — it's an explicit security feature against transient errors triggering DB wipe)
- `get_global_share_con()` — opens, applies WAL+busy_timeout+cache_size PRAGMAs, runs `_init_db` on first open per path
- `close_all_connections()`, `reset_for_tests()`
- `_recovery_marker` plumbing for the SHARE_DB_RECOVERED audit row

No public API change.

### `share_db/schema.py` (~200 lines)

Owns:
- `_SCHEMA` list (7 CREATE TABLE statements with `IF NOT EXISTS` + their indexes)
- `_init_db(con)` — runs schema, calls `apply_pending`, writes SHARE_DB_RECOVERED audit row if applicable
- `MIGRATIONS` dict + `_migration_001_seed_default_settings` + `_migration_002_seed_initial_tos`
- `get_current_version(con)`, `apply_pending(con)`, `LATEST_VERSION`
- New schema migrations land here by appending to `MIGRATIONS` (e.g., `_migration_003_argon2_hash_marker` — see §4)

### `share_db/passcode.py` (~250 lines)

**The argon2 adoption lives here.** Hashing format change:

```python
# New (argon2id by default):
#   argon2id$v=19$m=65536,t=3,p=4$saltB64$digestB64

# Legacy (scrypt — keeps working for old stored hashes):
#   scrypt$N$r$p$saltHex$digestHex
```

Public surface:

```python
def hash_passcode(passcode: str) -> str:
    """Hash via argon2id (current default). Returns the modular crypt format
    string `argon2id$v=19$m=...$t=...$p=...$saltB64$digestB64`."""
    # Uses argon2.PasswordHasher with OWASP 2026 params:
    #   memory_cost=65536 (64 MiB), time_cost=3, parallelism=4, hash_len=32
    ...

def verify_passcode(passcode: str, stored: str) -> bool:
    """Constant-time verify. Dispatches on the `algo$` prefix:
    `argon2id$...` → argon2.PasswordHasher.verify
    `scrypt$...` → existing scrypt verify (kept for backward compat)
    Anything else → False."""
    ...

def needs_rehash(stored: str) -> bool:
    """True when the stored hash uses an older algorithm or weaker params
    than the current default. Used by login flow to opportunistically
    upgrade scrypt → argon2id on successful login (transparent migration)."""
    ...
```

Migration approach (transparent, no down-time):
1. New invites use argon2id immediately.
2. Existing scrypt invites verify with the scrypt branch.
3. On successful login against a scrypt hash, the login handler calls `update_remote_invite_passcode_hash(invite_id, new_hash)` to rehash with argon2id — happens silently in `get_remote_invite_by_email_passcode`. By the time most users have logged in once post-deploy, the DB is mostly argon2id.
4. Once a fixed grace period passes (e.g., 30 days) the scrypt branch can be removed if desired (out of scope for this carve-up; doc this as a follow-up).

Also owns:
- `_BREACHED_TOP_LIST` (the in-memory weak-passcode seed set)
- `WeakPasscodeError` + `validate_passcode_strength`
- `generate_wordphrase()` — 32 hex chars in 4 groups of 8, > 100 bits entropy
- `_equalize_passcode_timing()` + `_dummy_hash` — preserved verbatim. argon2id verify is also ~30ms which keeps the timing equivalence valid. Test ensures both paths land in the same latency band.

### `share_db/validation.py` (~120 lines)

Owns:
- `_NAME_RE`, `_EMAIL_RE`
- `InvalidNameError`, `InvalidEmailError`, `InvalidPiiPolicyError`
- `validate_name`, `validate_email`, `validate_pii_policy`
- `parse_ip_whitelist`, `ip_in_whitelist`

Self-contained — no DB access. Could move to `backend/utils/validation.py` in a future pass, but staying in `share_db/` keeps locality with its primary consumer.

### `share_db/invites.py` (~280 lines)

Owns:
- `create_remote_invite`, `get_remote_invite`, `get_remote_invite_services`, `get_remote_invites`
- `get_remote_invite_by_email_passcode` (with `_equalize_passcode_timing` integration)
- `update_remote_invite_services`, `update_remote_invite_passcode`
- `revoke_remote_invite`, `delete_remote_invite`
- `mark_tos_accepted`
- Claim-token operations (`create_claim_token`, `claim_token`, `prune_expired_claim_tokens`)

### `share_db/sessions.py` (~150 lines)

Owns:
- `upsert_session`, `get_session`, `delete_session`, `get_all_sessions`
- `prune_expired_sessions` (used by the periodic prune job)
- Helpers for serializing/deserializing the JSON `pii_policy` column

### `share_db/audit.py` (~80 lines)

Owns:
- `log_share_audit_event`
- `get_share_audit_logs` with the optional `event_type`/`email_substr`/`since`/`until` filters

### `share_db/tos.py` (~50 lines)

Owns:
- `get_latest_tos`
- `publish_tos_version(version, text)` — explicit helper instead of inline INSERT in scattered tests / migrations

### `share_db/settings.py` (~40 lines)

Owns:
- `get_share_setting(key, default=None)`
- `set_share_setting(key, value)`
- Constants for known settings (`MAX_CONCURRENT_ANALYST_SESSIONS_KEY`)

### `share_db/__init__.py` (~50 lines)

Re-exports everything the rest of the codebase imports today:

```python
# Connection
from backend.core.share_db.connection import (
    db_path, get_global_share_con, get_safe_share_db_connection,
    close_all_connections, reset_for_tests,
)
# Schema + migrations
from backend.core.share_db.schema import (
    _SCHEMA, _init_db, MIGRATIONS, LATEST_VERSION, apply_pending, get_current_version,
)
# Passcode
from backend.core.share_db.passcode import (
    hash_passcode, verify_passcode, needs_rehash,
    validate_passcode_strength, WeakPasscodeError, generate_wordphrase,
)
# Validation
from backend.core.share_db.validation import (
    validate_name, validate_email, validate_pii_policy,
    parse_ip_whitelist, ip_in_whitelist,
    InvalidNameError, InvalidEmailError, InvalidPiiPolicyError,
)
# Invites
from backend.core.share_db.invites import (
    create_remote_invite, get_remote_invite, get_remote_invite_services,
    get_remote_invites, get_remote_invite_by_email_passcode,
    update_remote_invite_services, update_remote_invite_passcode,
    revoke_remote_invite, delete_remote_invite, mark_tos_accepted,
    create_claim_token, claim_token, prune_expired_claim_tokens,
)
# Sessions
from backend.core.share_db.sessions import (
    upsert_session, get_session, delete_session, get_all_sessions,
    prune_expired_sessions,
)
# Audit
from backend.core.share_db.audit import log_share_audit_event, get_share_audit_logs
# TOS
from backend.core.share_db.tos import get_latest_tos, publish_tos_version
# Settings
from backend.core.share_db.settings import (
    get_share_setting, set_share_setting,
    MAX_CONCURRENT_ANALYST_SESSIONS_KEY,
)
# Time helper re-export (existing pattern, sourced from date_utils)
from backend.utils.date_utils import iso_z, iso_z_now
```

---

## 4. Schema Migration: argon2 marker

A new migration `_migration_003_passcode_algo_marker` lands in `share_db/schema.py`:

```python
def _migration_003_passcode_algo_marker(con: sqlite3.Connection) -> None:
    """Stamp the share_settings table with `passcode_default_algo=argon2id`
    so a future migration can know when argon2id became the default. No
    schema change — just a single share_settings INSERT.
    """
    row = con.execute(
        "SELECT 1 FROM share_settings WHERE key=?", ("passcode_default_algo",)
    ).fetchone()
    if row is None:
        con.execute(
            "INSERT INTO share_settings(key, value) VALUES(?, ?)",
            ("passcode_default_algo", "argon2id"),
        )

MIGRATIONS[3] = _migration_003_passcode_algo_marker
```

No DDL change (uses existing `share_settings` KV table). Test: run the migration on a v1.x DB snapshot, verify `passcode_default_algo` row exists post-migration; existing scrypt hashes still verify; new hashes use argon2id.

---

## 5. argon2-cffi Configuration

```python
# share_db/passcode.py
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, InvalidHash

# OWASP 2026 recommended argon2id parameters.
# memory_cost=65536 KiB (64 MiB) — balances DoS resistance with VM memory budget.
# time_cost=3 — empirically ~30ms on the GCE n2-standard-2 (matches scrypt cost).
# parallelism=4 — fits the typical 2-4 vCPU prod sizing.
# hash_len=32 — matches existing scrypt dklen.
_HASHER = PasswordHasher(
    memory_cost=65536,
    time_cost=3,
    parallelism=4,
    hash_len=32,
)
```

Memory budget note: argon2's `memory_cost=65536` means each concurrent hash consumes ~64 MiB. With the LOGIN_FAILURE_THRESHOLD=5 and a single-threaded login handler, peak concurrent hashing is bounded. Worth verifying against the container memory cap during Phase 10 verification.

---

## 6. Test Strategy (Phase 10 test cleanup)

Per the planning decision (full unit coverage on touched modules + freezegun for time-dependent tests):

- **tests/core/share_db/test_connection.py** — pool lifecycle, corruption self-heal (with deliberately corrupted DB files), the restricted-to-actual-corruption-signature guard (lock-timeout should re-raise, not quarantine)
- **tests/core/share_db/test_schema.py** — migration ordering, idempotence, the new argon2 marker migration
- **tests/core/share_db/test_passcode.py** — argon2id round-trip, scrypt-legacy verify, the rehash path (login against scrypt → returns success → next read shows argon2id), timing-equalization assertions (`_equalize_passcode_timing` keeps the no-email-match branch within 20% of the email-match branch)
- **tests/core/share_db/test_validation.py** — name/email/PII/IP-whitelist edge cases (existing tests likely cover most)
- **tests/core/share_db/test_invites.py** — full invite lifecycle, multi-service invites, expired-invite filtering, claim-token TTL with `freezegun`
- **tests/core/share_db/test_sessions.py** — upsert/get/delete, prune-expired with `freezegun`
- **tests/core/share_db/test_audit.py** — append + filter queries
- **tests/core/share_db/test_tos.py** — get_latest, publish ordering
- **tests/core/share_db/test_settings.py** — get/set, default seeding via migration 001
- **tests/core/share_db/test_back_compat_shim.py** — every public symbol still importable via `from backend.core import share_db`

Tag with `@pytest.mark.security_regression`:
- Timing-equalization tests (closes a known side-channel)
- Corruption self-heal restricted-to-corruption-signature test (prevents transient-error wipe)
- Passcode strength validation (rejects breached list + all-digit PINs)
- IP-whitelist enforcement
- Invite revocation immediacy (revoked invite cannot create new sessions)

Existing tests to **delete** (per Phase 0.4 audit map):
- Any test asserting the literal `scrypt$N$r$p$...` format string of the hash (the migration changes this for new hashes)
- Any test importing `_dummy_hash` directly (private state, replaced by argon2-aware equivalent)

---

## 7. Migration Order (in Phase 10.2)

1. Add `argon2-cffi` to `pyproject.toml` dependencies. Add `freezegun` to dev-dependencies.
2. Create the `backend/core/share_db/` package directory + all module files (initially empty)
3. Move `connection.py` first (foundational — every other module imports it)
4. Move `schema.py` (depends on connection)
5. Move `validation.py` (no DB deps)
6. Move `passcode.py` — same file structure, add argon2 primary + scrypt-fallback verify (do NOT yet change `hash_passcode` to argon2 — split this into a follow-up sub-step so the verify path is exercised first)
7. Move `audit.py`
8. Move `tos.py`, `settings.py`
9. Move `invites.py`, `sessions.py`
10. Replace `backend/core/share_db.py` content with the re-export shim
11. Run full pytest — every existing test must pass
12. Flip `hash_passcode` to use argon2id (the actual algorithm swap). Add migration 003. Run tests again.
13. Add the rehash-on-login transparent migration in `get_remote_invite_by_email_passcode`
14. Commit + deploy + verify share-login flow end-to-end with both old scrypt and new argon2id passcodes

---

## 8. Risk & Mitigation

| Risk | Mitigation |
|---|---|
| argon2-cffi build failure on prod VM (Cython compilation) | argon2-cffi ships pre-built wheels for cpython 3.10–3.13 on linux_x86_64. Docker image build will catch any wheel-availability gap pre-deploy. |
| argon2's 64 MiB-per-hash memory cost causes OOM during a login flood | LOGIN_FAILURE_THRESHOLD=5 with 5-min lockout caps the per-IP rate; the single-threaded login handler serializes hashing. Worth a brief load test (concurrent share-login attempts) during Phase 10 verification. |
| Transparent rehash-on-login bug rehashes the wrong invite's passcode | Wrapped in the existing `_equalize_passcode_timing`'s same-`con` transaction; rehash uses `update_remote_invite_passcode(invite_id, plaintext)` which already validates strength and writes only that row. |
| Migration 003 runs on a DB that already has `passcode_default_algo` set | `IF NOT EXISTS` semantics in the migration (the `SELECT 1 FROM share_settings WHERE key=?` check). Idempotent. |
| Corruption self-heal's restricted-signature guard regresses during the move | `tests/core/share_db/test_connection.py` covers: open with a real corrupt file → quarantine + rebuild; open with a lock-timeout error → re-raise, no quarantine. Both tagged `@pytest.mark.security_regression`. |
| A caller imports a private symbol (`_dummy_hash`, `_SCRYPT_N`, etc.) | Grep across the codebase as part of the migration. If any caller does this, surface in the carveup PR and either expose properly or refactor the caller. |
| Existing share_db tests fail because they hard-code scrypt parameters | Phase 0.4 test audit lists these. Rewrite them to assert on the `verify_passcode(plaintext, hash)` contract, not on the hash format. |
