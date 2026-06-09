# Architectural Design Specification: Tunnel Manager Refactoring (`tunnel.py` Carve-up + SSH Deletion)

## 1. Context & Motivation

The `backend/utils/tunnel.py` file is a 1,022-line monolithic module that handles three orthogonal concerns: analyst session lifecycle, login rate limiting, and SSH-reverse-tunnel orchestration (via localhost.run). It was originally built to support two sharing modes:

1. **Direct-mode** — the production path on GCE+Fastly+Caddy. Admin provides a public HTTPS endpoint; analysts connect through it.
2. **SSH-tunnel mode** — for laptop-admin sharing. Spawns an `ssh` subprocess to localhost.run, parses the assigned tunnel URL from stdout, manages reconnects, and uses an OS sleep-listener to recover after laptop wake.

### Problems with the Current Shape

1. **Two execution models in one class.** `TunnelManager` carries both `proc: subprocess.Popen` (SSH-only) and `public_endpoint: str` (direct-only) fields. ~285 lines exist solely for the SSH path.
2. **Single Responsibility Violation.** Session lifecycle, rate limiting, state persistence, SSH subprocess management, and OS power-event handling all live in the same class.
3. **Security-critical code mixed with subprocess plumbing.** The `validate_session` invite-permission re-sync logic (which prevents an admin's tightened pii_policy from being ignored until session timeout) lives next to SSH stdout parsing.
4. **SSH path is unused in production.** Prod is GCE+Fastly+Caddy direct-mode. The SSH-to-localhost.run path is a laptop-admin convenience that has never been exercised in production.
5. **Failure modes of the SSH path are operationally fragile** — localhost.run reliability, SSH known-hosts pinning maintenance ([configs/ssh_known_hosts](../configs/ssh_known_hosts)), OS sleep-listener platform branches.

### Decision (per planning round)

- **Delete the SSH-to-localhost.run code path entirely** (~285 lines, see "What gets deleted" below).
- **Direct-mode only** as the supported sharing model in v2.0.
- **Carve the remaining ~737 lines into a `tunnel/` package** along functional boundaries.
- No managed-tunnel-service replacement (no Cloudflare Tunnel, no Tailscale, no ngrok) per "no SaaS" rule.

---

## 2. What Gets Deleted (~285 lines)

| Removed | Lines | Reason |
|---|---|---|
| `_ensure_known_hosts()` | ~60 | SSH known_hosts file lookup — only used by SSH path |
| `_ensure_share_key()` | ~25 | ed25519 keygen for SSH — only used by SSH path |
| `_read_stdout()` | ~15 | Parses `_TUNNEL_URL_RE` from SSH stdout — only used by SSH path |
| `_kill_proc()` | ~15 | SSH subprocess termination — only used by SSH path |
| `_TUNNEL_URL_RE` | 1 | localhost.run URL regex |
| `start_sharing()` `use_tunnel=True` branch | ~80 | SSH spawn + pre-flight port check + `_stdout_thread` setup |
| `start_sleep_listener()` + `stop_sleep_listener()` + `_sleep_listener_loop()` + `_on_wake()` | ~80 | OS power-event detection (only matters for SSH re-spawn on laptop wake) |
| `_port_in_use()` | ~10 | Pre-flight only used by SSH path |
| `TunnelState.proc` + `tunnel_url` + `reconnect_attempts` + `local_socket_addr` fields | ~5 | SSH-only state |
| Imports: `shutil`, `subprocess`, `socket`, `_TUNNEL_URL_RE` definitions | ~5 | No longer used after deletion |

Also drops the soft dep on `pyobjc` / `pywin32` / `dbus-python` that the platform-specific sleep listener referenced (the existing code already falls back to wall-clock-vs-monotonic drift detection, which goes away with the SSH path).

---

## 3. Refactored Package Directory

```
backend/utils/tunnel/
├── __init__.py          # Re-exports for 100% backward compat
├── manager.py           # TunnelManager singleton (direct-mode only)
├── session.py           # AnalystSession + lifecycle + validation + permission re-sync
├── rate_limiter.py      # _LoginRateLimiter (sliding-window + lockouts)
├── state.py             # TunnelState dataclass + direct-mode state persistence
└── fingerprint.py       # compute_fingerprint + UA/OS regex helpers
```

The original `backend/utils/tunnel.py` becomes a single-line re-export shim:

```python
# backend/utils/tunnel.py
from backend.utils.tunnel import *  # noqa: F401,F403 — back-compat shim
```

All current imports (`from backend.utils.tunnel import get_tunnel_manager, AnalystSession, compute_fingerprint`) continue to work.

---

## 4. Module Responsibilities

### `tunnel/state.py` (~80 lines)

Owns:
- `TunnelState` dataclass — fields trimmed to `use_tunnel: bool = False` (always False post-cleanup; kept for one release for compat), `public_endpoint: str | None`, `started_at: str | None`, `forward_port: int = 3000`, `direct_socket_addr: str | None`. **Removed:** `proc`, `tunnel_url`, `reconnect_attempts`, `local_socket_addr`.
- `_state_file_path()` — resolves `data/tunnel_state.json`
- `_persist_direct_state()`, `_clear_persisted_state()`, `_restore_direct_state()` — disk persistence so backend restart re-arms the public endpoint automatically

### `tunnel/fingerprint.py` (~30 lines)

Owns:
- `_UA_RE`, `_OS_RE` regexes
- `compute_fingerprint(headers: dict[str, str]) -> str` — narrowed SHA-256 (per Section #18 security guidance)

Stable, simple, no dependencies — ideal extraction.

### `tunnel/rate_limiter.py` (~100 lines)

Owns:
- `_LoginRateLimiter` class — sliding-window failure tracker + 5-min lockouts per IP
- Constants: `LOGIN_FAILURE_WINDOW_S`, `LOGIN_FAILURE_THRESHOLD`, `LOGIN_LOCKOUT_S`
- Thread-safe (`threading.Lock`)
- `is_locked()`, `snapshot()`, `record_failure()`, `clear()`

### `tunnel/session.py` (~280 lines)

Owns:
- `AnalystSession` dataclass + `from_row()` classmethod + `to_dict()`
- `IDLE_TIMEOUT_S`, `ABSOLUTE_TIMEOUT_S` constants
- `_parse_iso_z()` helper
- The **session lifecycle methods** — extracted from `TunnelManager` to avoid the god-class:
  - `create_session()` — multi-device boot of existing sessions for same invite
  - `touch_session()` — bump `last_active_time`, optional IP/activity update
  - `validate_session()` — TTL check + invite-permission re-sync (security-critical: tightens applied immediately, not at session timeout)
  - `boot_session()`, `boot_sessions_for_invite()`, `clear_all_sessions()`
  - `_evict()` — DB delete + audit log
- These take `share_db` calls as collaborators — same as today.

### `tunnel/manager.py` (~250 lines)

Owns:
- `TunnelManager` class — singleton orchestrator. Now ~half the size of the original.
- `RLock` for thread-safe state mutations
- Composes `_LoginRateLimiter` + `TunnelState` + session methods (delegated to `tunnel.session`)
- Direct-mode lifecycle: `start_sharing()` (direct-mode only — validates HTTPS endpoint, persists state, logs audit event), `stop_sharing()`, `panic()` (boots all sessions, clears state, audit log)
- Public accessors: `public_url()`, `state`, `is_sharing_active()`, `record_heartbeat_unauth()`, `get_rate_limit_snapshot()`, `get_telemetry()`
- Rehydration: `rehydrate_sessions()` — reload persisted sessions from share_db, prune expired
- `get_tunnel_manager()` singleton accessor + `reset_for_tests()` — unchanged

### `tunnel/__init__.py` (~30 lines)

Re-exports the symbols the rest of the codebase imports today:

```python
# Public surface (matches current imports across the codebase)
from backend.utils.tunnel.manager import TunnelManager, get_tunnel_manager, reset_for_tests
from backend.utils.tunnel.session import AnalystSession, IDLE_TIMEOUT_S, ABSOLUTE_TIMEOUT_S
from backend.utils.tunnel.fingerprint import compute_fingerprint
from backend.utils.tunnel.rate_limiter import (
    LOGIN_FAILURE_WINDOW_S,
    LOGIN_FAILURE_THRESHOLD,
    LOGIN_LOCKOUT_S,
)
from backend.utils.tunnel.state import TunnelState

__all__ = [
    "TunnelManager",
    "get_tunnel_manager",
    "reset_for_tests",
    "AnalystSession",
    "IDLE_TIMEOUT_S",
    "ABSOLUTE_TIMEOUT_S",
    "compute_fingerprint",
    "TunnelState",
    "LOGIN_FAILURE_WINDOW_S",
    "LOGIN_FAILURE_THRESHOLD",
    "LOGIN_LOCKOUT_S",
]
```

---

## 5. start_sharing() After Cleanup

The new direct-only `start_sharing` is ~30 lines (down from ~110):

```python
def start_sharing(
    self,
    *,
    public_endpoint: str,
    forward_port: int = 3000,
) -> dict:
    """Start direct-mode sharing.

    Validates that `public_endpoint` is HTTPS (analyst cookies require
    `secure=True`). Persists state so a backend restart re-arms automatically.
    Logs a SHARE_START audit event.
    """
    if not public_endpoint:
        raise ValueError("public_endpoint is required")
    if not public_endpoint.lower().startswith("https://"):
        raise ValueError(
            "public_endpoint must use HTTPS — analyst cookies require secure=True"
        )

    with self._lock:
        self._state.use_tunnel = False  # Always False post-cleanup, kept for compat
        self._state.public_endpoint = public_endpoint
        self._state.direct_socket_addr = "0.0.0.0"
        self._state.started_at = share_db.iso_z_now()
        self._persist_direct_state()

    share_db.log_share_audit_event(
        event_type="SHARE_START",
        email=None,
        ip_address="127.0.0.1",
        details=f"port={forward_port} endpoint={public_endpoint!r}",
    )
    return {"public_url": public_endpoint, "tunnel_url": None}
```

The `tunnel_url` field stays in the response for one release (clients may inspect it) but is always `None`.

---

## 6. API Surface — Backward Compatibility

Every public symbol currently imported elsewhere remains importable from the same path:

| Caller import | After carveup |
|---|---|
| `from backend.utils.tunnel import get_tunnel_manager` | ✓ (via shim + `__init__.py`) |
| `from backend.utils.tunnel import AnalystSession` | ✓ |
| `from backend.utils.tunnel import compute_fingerprint` | ✓ |
| `from backend.utils.tunnel import IDLE_TIMEOUT_S` | ✓ |
| `manager.start_sharing(use_tunnel=True, ...)` | **Removed.** Callers must use `start_sharing(public_endpoint=..., forward_port=...)` — frontend admin UI updated in the same PR. |
| `manager.start_sleep_listener()` | **Removed.** No-op stub kept for one release. |
| `manager.panic()` | ✓ (no SSH cleanup branch, just session boot + state clear) |

API surface that **breaks**:
- `start_sharing(use_tunnel=True, ...)` — admin UI updated in the same PR; no external integrations call this.
- `start_sleep_listener` / `stop_sleep_listener` — only called from `main.py` lifespan; updated in the same PR.

---

## 7. Test Strategy (Phase 10 test cleanup)

Per the planning decision (full unit coverage on touched modules + freezegun for time-dependent tests):

- **tests/utils/tunnel/test_session.py** — `freezegun` for the 2h idle / 24h absolute timeout paths; covers `validate_session` permission re-sync, multi-device boot
- **tests/utils/tunnel/test_rate_limiter.py** — sliding-window correctness, lockout TTL via freezegun
- **tests/utils/tunnel/test_state.py** — persist + restore round-trips, malformed JSON handling
- **tests/utils/tunnel/test_fingerprint.py** — UA/OS regex coverage including Chrome UA-Reduction stability
- **tests/utils/tunnel/test_manager.py** — start/stop/panic flows (direct-mode only), audit log emission
- **tests/utils/tunnel/test_no_ssh_left.py** — grep assertion that no `subprocess`, no `ssh`, no `localhost.run`, no `_TUNNEL_URL_RE` references remain in the carved-out package

Existing tests to **delete** (per Phase 0.4 audit map):
- Any test asserting on `use_tunnel=True` behavior
- Any test asserting on `_ensure_known_hosts` / `_ensure_share_key` / `_kill_proc` / `_read_stdout` / `_sleep_listener_loop` / `_on_wake`
- Any test using `_port_in_use` as a fixture

Tag tests covering session-permission re-sync + rate-limiter lockout + IP-whitelist enforcement with `@pytest.mark.security_regression`.

---

## 8. Migration Order (in Phase 10.1)

1. Create the `backend/utils/tunnel/` package directory + all 5 module files (initially empty)
2. Move `_LoginRateLimiter` + its constants to `rate_limiter.py` (smallest, isolated)
3. Move `compute_fingerprint` + regexes to `fingerprint.py`
4. Move `TunnelState` + persistence helpers to `state.py` (drop the SSH-only fields)
5. Move `AnalystSession` + lifecycle methods to `session.py`
6. Move `TunnelManager` (now slimmed) to `manager.py`
7. Replace `backend/utils/tunnel.py` content with the re-export shim
8. Update the 2 known SSH-only callers (admin UI `start_sharing` payload, `main.py` lifespan) to drop `use_tunnel` + sleep-listener calls
9. Delete the obsolete SSH config: `configs/ssh_known_hosts` (still referenced by `_ensure_known_hosts` which is now gone)
10. Run full pytest + smoke against share-login flow + verify direct-mode share works end-to-end
11. Commit + deploy + verify prod

---

## 9. Risk & Mitigation

| Risk | Mitigation |
|---|---|
| A laptop-admin somewhere is actively using SSH-tunnel sharing | User confirmed prod is GCE+Fastly direct-mode only; SSH path is unused. Plan announces the removal in CHANGELOG/README. |
| `configs/ssh_known_hosts` deletion breaks a deploy that still references it | The file is only read by `_ensure_known_hosts()` which is deleted in the same PR. Audit grep proves no other reference. |
| An external script imports `start_sleep_listener` | One-release no-op stub keeps the call site from crashing; `Deprecation` log line announces the removal. After v2.0, the stub is removed too. |
| Permission re-sync logic in `validate_session` regresses during the move | Dedicated `@pytest.mark.security_regression` test covers: tightening `pii_policy.mask_ips`, `query_window_hours`, `query_start_time`/`end_time`, `service_ids` mid-session takes effect on the next request. |
| Hidden test using `_port_in_use` for unrelated assertions | Phase 0.4 test audit catches it; either inline the 4-line helper or rewrite the test. |
