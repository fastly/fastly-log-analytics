"""Service configuration management for multi-service support.

Each service configuration is persisted in PostgreSQL (``service_configs`` table).
"""

import contextlib
import copy
import dataclasses
import datetime as _dt
import json
import logging
import math
import os
import re
import secrets
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from ipaddress import ip_address
from pathlib import Path
from typing import Annotated, Any

from cachetools import LRUCache
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

from backend.core.service_defaults import DEFAULT_LOG_PERIOD_SECONDS
from backend.utils.bounded_cache import BoundedLRUCache, CacheRegistry
from backend.utils.telemetry import atomic_write_json

logger = logging.getLogger(__name__)

_ROOT_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CONFIGS_DIR = _ROOT_DIR / "configs"
DATA_DIR = _ROOT_DIR / "data"

# Sub-directories for organized data storage
SERVICES_DATA_DIR = DATA_DIR / "services"
NGWAF_DATA_DIR = DATA_DIR / "ngwaf"
CACHE_DATA_DIR = DATA_DIR / "cache"
SYSTEM_DATA_DIR = DATA_DIR / "system"


@dataclass(frozen=True)
class ClickHouseConfig:
    """Deployment-only HTTP settings; loaded at startup, never from service JSON."""

    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)
    secure: bool = True
    connect_timeout_s: float = 5.0
    query_timeout_s: float = 30.0
    insert_timeout_s: float = 30.0
    pool_timeout_s: float = 30.0
    pool_max_size: int = 24


def _clickhouse_bool(name: str, default: str) -> bool:
    raw = os.getenv(name, default).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _clickhouse_number(name: str, default: float, maximum: float, *, integer: bool = False):
    try:
        raw = os.getenv(name, str(default))
        value = int(raw) if integer else float(raw)
        if math.isfinite(value) and (1 if integer else 0.01) <= value <= maximum:
            return value
    except (ValueError, OverflowError):
        pass
    raise ValueError(f"{name} must be finite and between {1 if integer else 0.01} and {maximum}")


def load_clickhouse_config() -> ClickHouseConfig | None:
    """CLICKHOUSE_ENABLED defaults to true when ClickHouse settings are present.

    Enabled mode requires HOST (bare DNS/IP), DATABASE and USER. SECURE defaults
    to true (verified HTTPS, port 8443; HTTP uses 8123). A missing PASSWORD is
    permitted only for explicitly insecure loopback test instances. Operators
    must opt into SECURE=false for a trusted private-network HTTP deployment.
    CONNECT/POOL_TIMEOUT_S are 0.01..60; QUERY/INSERT_TIMEOUT_S 0.01..300;
    POOL_MAX_SIZE is 1..64. Environment changes require a process restart.
    """
    if not _clickhouse_bool("CLICKHOUSE_ENABLED", "true"):
        return None
    if os.getenv("CLICKHOUSE_ENABLED") is None and not any(
        os.getenv(f"CLICKHOUSE_{k}", "").strip() for k in ("HOST", "DATABASE", "USER")
    ):
        return None
    required = {}
    for key in ("HOST", "DATABASE", "USER"):
        value = os.getenv(f"CLICKHOUSE_{key}", "").strip()
        if not value:
            raise ValueError(f"CLICKHOUSE_{key} is required when enabled")
        required[key] = value
    host = required["HOST"]
    loopback = host in ("localhost", "clickhouse")
    if not loopback:
        try:
            loopback = ip_address(host).is_loopback
        except ValueError:
            if len(host) > 253 or not all(
                re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label) for label in host.split(".")
            ):
                raise ValueError("CLICKHOUSE_HOST must be a bare hostname or IP address") from None
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", required["DATABASE"]):
        raise ValueError("CLICKHOUSE_DATABASE must be a simple identifier")
    if not re.fullmatch(r"[A-Za-z0-9_.@-]{1,128}", required["USER"]):
        raise ValueError("CLICKHOUSE_USER contains unsupported characters")
    secure = _clickhouse_bool("CLICKHOUSE_SECURE", "true")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    if not password.strip() and (secure or not loopback):
        raise ValueError("CLICKHOUSE_PASSWORD is required except for explicit HTTP loopback tests")
    query_timeout = _clickhouse_number("CLICKHOUSE_QUERY_TIMEOUT_S", 30, 300)
    return ClickHouseConfig(
        host=host,
        port=_clickhouse_number("CLICKHOUSE_PORT", 8443 if secure else 8123, 65535, integer=True),
        database=required["DATABASE"],
        user=required["USER"],
        password=password,
        secure=secure,
        connect_timeout_s=_clickhouse_number("CLICKHOUSE_CONNECT_TIMEOUT_S", 5, 60),
        query_timeout_s=query_timeout,
        insert_timeout_s=_clickhouse_number("CLICKHOUSE_INSERT_TIMEOUT_S", query_timeout, 300),
        pool_timeout_s=_clickhouse_number("CLICKHOUSE_POOL_TIMEOUT_S", 30, 60),
        pool_max_size=_clickhouse_number("CLICKHOUSE_POOL_MAX_SIZE", 24, 64, integer=True),
    )


def load_clickhouse_provisioner_config() -> ClickHouseConfig | None:
    """Credentials for the startup-only ClickHouse provisioner, or None to provision as the application user.

    Creating users, profiles and views needs access management, which the long-running application user should
    not hold (anything that can run SQL as it could then mint users). CLICKHOUSE_PROVISION_USER/PASSWORD name a
    separate account used only during startup provisioning and then closed; every other connection setting is
    shared with the application's own CLICKHOUSE_* configuration.
    """
    app = load_clickhouse_config()
    user = os.getenv("CLICKHOUSE_PROVISION_USER", "").strip()
    if app is None or not user:
        return None
    if not re.fullmatch(r"[A-Za-z0-9_.@-]{1,128}", user):
        raise ValueError("CLICKHOUSE_PROVISION_USER contains unsupported characters")
    password = os.getenv("CLICKHOUSE_PROVISION_PASSWORD", "")
    if not password.strip() and (app.secure or app.host not in ("localhost", "clickhouse")):
        raise ValueError("CLICKHOUSE_PROVISION_PASSWORD is required except for explicit HTTP loopback tests")
    return dataclasses.replace(app, user=user, password=password)


# Cache for Fastly service names: {service_id: {"name": str, "fetched_at": float}}
_NAME_CACHE_TTL = 300  # 5 minutes
_name_cache: BoundedLRUCache[str, dict[str, Any]] = BoundedLRUCache(maxsize=256)
_name_cache_lock = threading.Lock()
_name_refresh_in_flight: set[str] = set()
_name_refresh_lock = threading.Lock()

# Cache for parsed configs: {service_id: (revision, parsed_dict)}.
# Snapshot-cached across the table for `_DEFAULT_CONFIG_SNAPSHOT_TTL_S` seconds
# (optional sub-second Valkey epoch invalidation when CELERY_BROKER_URL is set),
# revision-revalidated on refresh, and immediately invalidated on every write.
_DEFAULT_CONFIG_SNAPSHOT_TTL_S = 5.0
_VALKEY_EPOCH_KEY = "fla:service_configs:epoch"
_VALKEY_EPOCH_CHECK_INTERVAL_S = 0.5


@dataclass(frozen=True)
class _ConfigTableSnapshot:
    fetched_mono: float
    epoch_checked_mono: float
    epoch: int | None
    pool_token: Any
    by_id: dict[str, tuple[int, dict]]
    service_ids: tuple[str, ...]
    cdn_map: dict[str, str]


_config_cache: LRUCache[str, tuple[int, dict]] = LRUCache(maxsize=256)
_config_cache_lock = threading.Lock()
_snapshot_refresh_lock = threading.Lock()
_snapshot_refresh_tls = threading.local()
_EMPTY_TABLE_SNAPSHOT = _ConfigTableSnapshot(
    fetched_mono=0.0,
    epoch_checked_mono=0.0,
    epoch=None,
    pool_token=None,
    by_id={},
    service_ids=(),
    cdn_map={},
)
_table_snapshot: _ConfigTableSnapshot | None = None
_valkey_client_lock = threading.Lock()
_valkey_client: Any = None
_valkey_backoff_until = 0.0

# Path-keyed memo for _ensure_dirs().
_ensured_dirs: set[Path] = set()

# Retained for test/cache-reset compatibility.
_service_ids_cache: tuple[Path, int, list[str]] | None = None
_service_ids_cache_lock = threading.Lock()


def _snapshot_ttl_seconds() -> float:
    raw = os.getenv("FLA_CONFIG_CACHE_TTL_S")
    if raw is None or not raw.strip():
        return _DEFAULT_CONFIG_SNAPSHOT_TTL_S
    try:
        val = float(raw)
    except ValueError:
        return _DEFAULT_CONFIG_SNAPSHOT_TTL_S
    return val if math.isfinite(val) and val >= 0 else _DEFAULT_CONFIG_SNAPSHOT_TTL_S


def _snapshot_pool_token() -> Any:
    from backend.core.metadata import pg_pool

    pool_obj = getattr(pg_pool, "_pool", None)
    sqlite_obj = getattr(pg_pool, "_sqlite_shim_conn", None)
    return (
        id(pg_pool),
        id(pool_obj) if pool_obj is not None else None,
        id(sqlite_obj) if sqlite_obj is not None else None,
        os.getenv("METADATA_DSN", ""),
        id(pg_pool.get_pg_readonly_connection),
    )


def _get_valkey_epoch_client() -> Any | None:
    global _valkey_client, _valkey_backoff_until
    if "pytest" in sys.modules:
        return None
    broker_url = os.getenv("CELERY_BROKER_URL", "").strip()
    if not broker_url:
        return None
    now = time.monotonic()
    if now < _valkey_backoff_until:
        return None
    with _valkey_client_lock:
        if time.monotonic() < _valkey_backoff_until:
            return None
        if _valkey_client is None:
            try:
                from redis import Redis as SyncRedis

                _valkey_client = SyncRedis.from_url(
                    broker_url,
                    socket_connect_timeout=0.2,
                    socket_timeout=0.2,
                    decode_responses=True,
                )
            except Exception:
                _valkey_backoff_until = time.monotonic() + 10.0
                return None
        return _valkey_client


def _read_valkey_epoch() -> int | None:
    global _valkey_backoff_until
    client = _get_valkey_epoch_client()
    if client is None:
        return None
    try:
        raw = client.get(_VALKEY_EPOCH_KEY)
        return int(raw) if raw is not None else 0
    except Exception:
        _valkey_backoff_until = time.monotonic() + 10.0
        return None


def _bump_valkey_epoch() -> int | None:
    global _valkey_backoff_until
    client = _get_valkey_epoch_client()
    if client is None:
        return None
    try:
        return int(client.incr(_VALKEY_EPOCH_KEY))
    except Exception:
        _valkey_backoff_until = time.monotonic() + 10.0
        return None


def _invalidate_service_ids_cache() -> None:
    global _service_ids_cache
    with _service_ids_cache_lock:
        _service_ids_cache = None


def invalidate_config_cache(service_id: str | None = None) -> None:
    """Invalidate in-process (and optional Valkey epoch) config caches."""
    global _cdn_service_id_map, _table_snapshot
    with _config_cache_lock:
        _table_snapshot = None
        if service_id is None:
            _config_cache.clear()
        else:
            _config_cache.pop(service_id, None)
    _cdn_service_id_map = None
    _invalidate_service_ids_cache()
    _bump_valkey_epoch()
    for cache_name in (
        "routers.bootstrap._bootstrap_cache",
        "services.vcl._logging_settings_cache",
        "services.core._cron_schedule_cache",
    ):
        with contextlib.suppress(Exception):
            CacheRegistry.clear(cache_name)


def _ensure_dirs():
    dirs = (CONFIGS_DIR, DATA_DIR, SERVICES_DATA_DIR, NGWAF_DATA_DIR, CACHE_DATA_DIR, SYSTEM_DATA_DIR)
    missing = [d for d in dirs if d not in _ensured_dirs]
    if missing:
        for d in missing:
            d.mkdir(exist_ok=True)
            _ensured_dirs.add(d)


# Alphabet shared with backend.deps.ServiceId. Public so the FastAPI
# Path validator can reuse the same pattern instead of redefining it.
SERVICE_ID_PATTERN = r"^[A-Za-z0-9_-]+$"
_SERVICE_ID_RE = re.compile(SERVICE_ID_PATTERN)
_SERVICE_ID_MAX_LEN = 64


def _validate_service_id(service_id: str) -> str:
    """Security: defense in depth against path traversal in any helper
    that builds a path from ``service_id``.

    Real Fastly service IDs are opaque 22-char alphanumeric strings, but the
    test suite and a handful of legacy provisioning paths use hyphenated
    IDs like ``svc-1`` / ``test-service-id``. The regex therefore accepts
    ``[A-Za-z0-9_-]+`` — every character allowed is safe inside a filename
    and contains no path-separator / dot / null-byte. Without this,
    ``service_id="/etc/passwd"`` or ``service_id="../../tmp/x"`` would
    compose with ``pathlib`` semantics — absolute paths discard the base
    entirely, relative ``..`` traverses out, and ``\\x00`` truncates on
    some kernels.

    Length cap (64) is well above the longest legitimate Fastly ID (22)
    and bounds memory in error-logging paths.
    """
    if not isinstance(service_id, str):
        raise ValueError(f"invalid service_id type {type(service_id).__name__}: must be str (security)")
    if not service_id or len(service_id) > _SERVICE_ID_MAX_LEN:
        raise ValueError(
            f"invalid service_id length {len(service_id) if service_id else 0}: "
            f"1..{_SERVICE_ID_MAX_LEN} characters required (security)"
        )
    if not _SERVICE_ID_RE.fullmatch(service_id):
        raise ValueError(f"invalid service_id {service_id!r}: must be alphanumeric / dash / underscore (security)")
    return service_id


def config_path(service_id: str) -> Path:
    _validate_service_id(service_id)
    return CONFIGS_DIR / f"{service_id}.json"


class ConfigLockTimeout(TimeoutError):
    """The per-service config lock could not be acquired within ``_LOCK_TIMEOUT_S``."""


class CidSaltReadError(RuntimeError):
    """The persisted config exists but could not be read, so the cid salt cannot be verified."""


_LOCK_TIMEOUT_S = 5.0

_lock_state = threading.local()
_shim_locks: dict[str, threading.RLock] = {}
_shim_locks_guard = threading.Lock()


def _get_shim_lock(service_id: str) -> threading.RLock:
    with _shim_locks_guard:
        lock = _shim_locks.get(service_id)
        if lock is None:
            lock = threading.RLock()
            _shim_locks[service_id] = lock
        return lock


def _get_held_lock_conn(service_id: str) -> Any | None:
    held = getattr(_lock_state, "held", None)
    if not isinstance(held, dict):
        return None
    entry = held.get(service_id)
    if entry is None:
        return None
    return entry[0]


@contextlib.contextmanager
def _open_or_reuse_conn(service_id: str):
    """Yield the current thread's held lock connection if active, else checkout and close one."""
    held_conn = _get_held_lock_conn(service_id)
    if held_conn is not None:
        yield held_conn, False
        return
    from backend.core.metadata import pg_pool

    with contextlib.closing(pg_pool.get_pg_readonly_connection()) as conn:
        yield conn, True


def config_lock_degraded() -> bool:
    """PostgreSQL advisory locking is never degraded in-process."""
    return False


@contextlib.contextmanager
def config_lock(service_id: str):
    """Cross-pod exclusive lock for read-modify-write of one service config."""
    from psycopg.errors import LockNotAvailable
    from psycopg_pool import PoolTimeout

    from backend.core.metadata import pg_pool

    sid = _validate_service_id(service_id)
    held: dict[str, tuple[Any, int]] = _lock_state.__dict__.setdefault("held", {})
    existing = held.get(sid)
    if existing is not None:
        conn, depth = existing
        held[sid] = (conn, depth + 1)
        try:
            yield
        finally:
            held[sid] = (conn, depth)
        return

    try:
        conn = pg_pool.get_pg_readonly_connection()
    except PoolTimeout:
        raise ConfigLockTimeout(
            f"timed out after {_LOCK_TIMEOUT_S:.0f}s waiting for the config lock of service {service_id}"
        ) from None

    with contextlib.closing(conn):
        if isinstance(conn, pg_pool.PgConnectionWrapper):
            try:
                with conn.transaction():
                    conn.execute(f"SET LOCAL lock_timeout = '{int(_LOCK_TIMEOUT_S * 1000)}ms'")
                    conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended('service_config:' || ?, 0))",
                        (sid,),
                    )
                    held[sid] = (conn, 1)
                    try:
                        yield
                    finally:
                        held.pop(sid, None)
            except LockNotAvailable:
                raise ConfigLockTimeout(
                    f"timed out after {_LOCK_TIMEOUT_S:.0f}s waiting for the config lock of service {service_id}"
                ) from None
        else:
            shim_lock = _get_shim_lock(sid)
            if not shim_lock.acquire(timeout=_LOCK_TIMEOUT_S):
                raise ConfigLockTimeout(
                    f"timed out after {_LOCK_TIMEOUT_S:.0f}s waiting for the config lock of service {service_id}"
                )
            try:
                held[sid] = (conn, 1)
                try:
                    yield
                    if hasattr(conn, "commit"):
                        conn.commit()
                except BaseException:
                    if hasattr(conn, "rollback"):
                        conn.rollback()
                    raise
                finally:
                    held.pop(sid, None)
            finally:
                shim_lock.release()


def _parse_config_row(service_id: str, row: Any, *, use_cache: bool) -> dict:
    rev = int(row["revision"])
    if use_cache:
        with _config_cache_lock:
            cached = _config_cache.get(service_id)
            if cached is not None and cached[0] == rev:
                return copy.deepcopy(cached[1])
    parsed = json.loads(row["config_json"])
    if not isinstance(parsed, dict):
        raise ValueError(f"Config for {service_id!r} must be a JSON object")
    with _config_cache_lock:
        _config_cache[service_id] = (rev, parsed)
    return copy.deepcopy(parsed)


def _get_valid_table_snapshot() -> _ConfigTableSnapshot | None:
    global _table_snapshot
    ttl = _snapshot_ttl_seconds()
    if ttl <= 0:
        return None
    pool_token = _snapshot_pool_token()
    now = time.monotonic()
    with _config_cache_lock:
        snap = _table_snapshot
    if snap is None or snap.pool_token != pool_token or (now - snap.fetched_mono) >= ttl:
        return None
    if (now - snap.epoch_checked_mono) >= _VALKEY_EPOCH_CHECK_INTERVAL_S:
        remote_epoch = _read_valkey_epoch()
        if remote_epoch is not None and snap.epoch is not None and remote_epoch != snap.epoch:
            with _config_cache_lock:
                if _table_snapshot is snap:
                    _table_snapshot = None
            return None
        updated = dataclasses.replace(
            snap,
            epoch_checked_mono=now,
            epoch=remote_epoch if remote_epoch is not None else snap.epoch,
        )
        with _config_cache_lock:
            if _table_snapshot is snap:
                _table_snapshot = updated
        return updated
    return snap


def _refresh_table_snapshot_locked() -> _ConfigTableSnapshot:
    global _cdn_service_id_map, _table_snapshot
    from backend.core.metadata import pg_pool

    pool_token = _snapshot_pool_token()
    with contextlib.closing(pg_pool.get_pg_readonly_connection()) as conn:
        rev_rows = conn.execute("SELECT service_id, revision FROM service_configs ORDER BY service_id").fetchall()
        revisions = [(str(r["service_id"]), int(r["revision"])) for r in rev_rows]

        by_id: dict[str, tuple[int, dict]] = {}
        missing_ids: list[str] = []
        with _config_cache_lock:
            for sid, rev in revisions:
                hit = _config_cache.get(sid)
                if hit is not None and hit[0] == rev:
                    by_id[sid] = hit
                else:
                    missing_ids.append(sid)

        if missing_ids:
            placeholders = ", ".join("?" for _ in missing_ids)
            full_rows = conn.execute(
                f"SELECT service_id, revision, config_json FROM service_configs "  # nosec B608
                f"WHERE service_id IN ({placeholders}) ORDER BY service_id",
                tuple(missing_ids),
            ).fetchall()
            for r in full_rows:
                sid = str(r["service_id"])
                rev = int(r["revision"])
                parsed = json.loads(r["config_json"])
                if not isinstance(parsed, dict):
                    raise ValueError(f"Config for {sid!r} must be a JSON object")
                by_id[sid] = (rev, parsed)

    ordered_ids = tuple(sid for sid, _ in revisions if sid in by_id)
    cdn_map = {str(by_id[sid][1]["cdn_service_id"]): sid for sid in ordered_ids if by_id[sid][1].get("cdn_service_id")}
    now = time.monotonic()
    snap = _ConfigTableSnapshot(
        fetched_mono=now,
        epoch_checked_mono=now,
        epoch=_read_valkey_epoch(),
        pool_token=pool_token,
        by_id={sid: by_id[sid] for sid in ordered_ids},
        service_ids=ordered_ids,
        cdn_map=cdn_map,
    )
    with _config_cache_lock:
        present = set(ordered_ids)
        for stale_sid in [k for k in list(_config_cache.keys()) if k not in present]:
            _config_cache.pop(stale_sid, None)
        for sid in ordered_ids:
            _config_cache[sid] = snap.by_id[sid]
        if _snapshot_ttl_seconds() > 0:
            _table_snapshot = snap
    _cdn_service_id_map = dict(cdn_map)
    return snap


def _refresh_table_snapshot() -> _ConfigTableSnapshot:
    if getattr(_snapshot_refresh_tls, "active", False):
        with _config_cache_lock:
            return _table_snapshot if _table_snapshot is not None else _EMPTY_TABLE_SNAPSHOT

    with _snapshot_refresh_lock:
        cached = _get_valid_table_snapshot()
        if cached is not None:
            return cached

        _snapshot_refresh_tls.active = True
        try:
            return _refresh_table_snapshot_locked()
        finally:
            _snapshot_refresh_tls.active = False


def _get_or_refresh_snapshot() -> _ConfigTableSnapshot:
    snap = _get_valid_table_snapshot()
    if snap is not None:
        return snap
    if getattr(_snapshot_refresh_tls, "active", False):
        with _config_cache_lock:
            return _table_snapshot if _table_snapshot is not None else _EMPTY_TABLE_SNAPSHOT
    return _refresh_table_snapshot()


def load_config(service_id: str | None) -> dict | None:
    """Load a single service config by ID from PostgreSQL (cached across pod hot paths)."""
    if service_id is None:
        return None
    try:
        sid = _validate_service_id(service_id)
    except ValueError:
        return None

    held_conn = _get_held_lock_conn(sid)
    if held_conn is not None:
        row = held_conn.execute(
            "SELECT revision, config_json FROM service_configs WHERE service_id = ?",
            (sid,),
        ).fetchone()
        if row is None:
            with _config_cache_lock:
                _config_cache.pop(sid, None)
            return None
        return _parse_config_row(sid, row, use_cache=True)

    snap = _get_or_refresh_snapshot()
    entry = snap.by_id.get(sid)
    if entry is None:
        return None
    return copy.deepcopy(entry[1])


def _read_config_fresh(service_id: str) -> dict | None:
    """``load_config`` that bypasses the in-process cache and updates it from DB."""
    if service_id is None:
        return None
    try:
        sid = _validate_service_id(service_id)
    except ValueError:
        return None

    with _config_cache_lock:
        _config_cache.pop(sid, None)

    with _open_or_reuse_conn(sid) as (conn, _):
        row = conn.execute(
            "SELECT revision, config_json FROM service_configs WHERE service_id = ?",
            (sid,),
        ).fetchone()
    if row is None:
        return None
    return _parse_config_row(sid, row, use_cache=False)


def _extract_cid_salt(cfg: Any) -> str | None:
    rum = cfg.get("rum") if isinstance(cfg, dict) else None
    salt = rum.get("cid_salt") if isinstance(rum, dict) else None
    return salt if isinstance(salt, str) and salt else None


def _db_cid_salt(service_id: str, conn: Any) -> str | None:
    try:
        row = conn.execute(
            "SELECT config_json FROM service_configs WHERE service_id = ?",
            (service_id,),
        ).fetchone()
    except Exception as e:
        raise CidSaltReadError(f"cannot read existing config for {service_id}; refusing to save: {e}") from e
    if row is None:
        return None
    try:
        disk = json.loads(row["config_json"])
    except Exception as e:
        raise CidSaltReadError(f"cannot read existing config for {service_id}; refusing to save: {e}") from e
    return _extract_cid_salt(disk)


def _apply_write_once_cid_salt(
    service_id: str,
    cfg: dict,
    conn: Any,
    *,
    allow_cid_salt_change: bool = False,
    mint_if_missing: bool = True,
) -> None:
    """Preserve write-once ``rum.cid_salt`` from the DB row and optionally mint when absent."""
    existing_salt = None if allow_cid_salt_change else _db_cid_salt(service_id, conn)
    rum = cfg.get("rum")
    if existing_salt:
        if not isinstance(rum, dict):
            rum = {}
            cfg["rum"] = rum
        if rum.get("cid_salt") != existing_salt:
            if rum.get("cid_salt"):
                logger.warning("cid_salt_overwrite_blocked service_id=%s", service_id)
            rum["cid_salt"] = existing_salt
    elif mint_if_missing and not (isinstance(rum, dict) and rum.get("cid_salt")):
        if not isinstance(rum, dict):
            rum = {}
            cfg["rum"] = rum
        rum["cid_salt"] = secrets.token_hex(32)


def _upsert_config_json(conn: Any, service_id: str, config_json: str, now_iso: str) -> None:
    conn.execute(
        """
        INSERT INTO service_configs (service_id, config_json, revision, updated_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT (service_id) DO UPDATE SET
            config_json = EXCLUDED.config_json,
            revision = service_configs.revision + 1,
            updated_at = EXCLUDED.updated_at
        """,
        (service_id, config_json, now_iso),
    )


def _commit_if_standalone_non_pg(conn: Any, opened_here: bool) -> None:
    if not opened_here:
        return
    from backend.core.metadata import pg_pool

    if not isinstance(conn, pg_pool.PgConnectionWrapper) and hasattr(conn, "commit"):
        conn.commit()


def _save_config_unlocked(service_id: str, cfg: dict, *, allow_cid_salt_change: bool = False):
    sid = _validate_service_id(service_id)
    if "created_at" not in cfg or not cfg["created_at"]:
        cfg["created_at"] = _dt.datetime.now(_dt.UTC).isoformat()

    from backend.core import catalog

    with _open_or_reuse_conn(sid) as (conn, opened_here):
        _apply_write_once_cid_salt(
            sid,
            cfg,
            conn,
            allow_cid_salt_change=allow_cid_salt_change,
            mint_if_missing=True,
        )
        cfg["active_log_field_ids"] = catalog.resolve_active_log_field_ids(catalog.get_lf_config(cfg))
        now_iso = _dt.datetime.now(_dt.UTC).isoformat()
        _upsert_config_json(conn, sid, json.dumps(cfg), now_iso)
        _commit_if_standalone_non_pg(conn, opened_here)

    invalidate_config_cache(sid)


def active_log_field_ids(cfg: dict) -> list[str]:
    """The service's enabled log field ids, as stored by the last save of its log field settings."""
    from backend.core import catalog

    stored = cfg.get("active_log_field_ids")
    if isinstance(stored, list):
        return list(stored)
    return catalog.resolve_active_log_field_ids(catalog.get_lf_config(cfg))


def save_config(service_id: str, cfg: dict):
    """Write a service config atomically to PostgreSQL under per-service lock."""
    with config_lock(service_id):
        _save_config_unlocked(service_id, cfg)


def mutate_config(
    service_id: str, mutate: Callable[[dict], None], *, allow_cid_salt_change: bool = False
) -> dict | None:
    """Locked read-modify-write: fresh-load, apply ``mutate`` in place, save.

    Returns the saved config, or ``None`` when the service has no config.
    Use for partial edits so a concurrent writer's changes are not clobbered.
    ``allow_cid_salt_change`` is reserved for ``backend.core.fos_client.rotate_cid_salt``; every other
    writer has the persisted cid salt re-adopted.
    """
    with config_lock(service_id):
        cfg = _read_config_fresh(service_id)
        if cfg is None:
            return None
        mutate(cfg)
        _save_config_unlocked(service_id, cfg, allow_cid_salt_change=allow_cid_salt_change)
        return cfg


def delete_config(service_id: str) -> bool:
    """Delete a service config row from ``service_configs`` under per-service lock."""
    sid = _validate_service_id(service_id)
    with config_lock(sid), _open_or_reuse_conn(sid) as (conn, _):
        cur = conn.execute("DELETE FROM service_configs WHERE service_id = ?", (sid,))
        deleted = bool(cur.rowcount and cur.rowcount > 0)
    invalidate_config_cache(sid)
    return deleted


def snapshot_config(service_id: str, *, prefer_deployed: bool = False) -> str | None:
    """Raw persisted config JSON text (``None`` when absent)."""
    sid = _validate_service_id(service_id)
    with config_lock(sid), _open_or_reuse_conn(sid) as (conn, _):
        row = conn.execute(
            "SELECT config_json, deployed_config_json FROM service_configs WHERE service_id = ?",
            (sid,),
        ).fetchone()
    if row is None:
        return None
    if prefer_deployed and row["deployed_config_json"] is not None:
        return str(row["deployed_config_json"])
    return str(row["config_json"]) if row["config_json"] is not None else None


def restore_config_snapshot(service_id: str, raw: str) -> None:
    """Atomically restore a previously captured raw service config."""
    sid = _validate_service_id(service_id)
    snapshot = json.loads(raw)
    if not isinstance(snapshot, dict):
        raise ValueError(f"Snapshot for {service_id!r} must be a JSON object")

    from backend.core import catalog

    with config_lock(sid), _open_or_reuse_conn(sid) as (conn, _):
        _apply_write_once_cid_salt(
            sid,
            snapshot,
            conn,
            allow_cid_salt_change=False,
            mint_if_missing=False,
        )
        snapshot["active_log_field_ids"] = catalog.resolve_active_log_field_ids(catalog.get_lf_config(snapshot))
        now_iso = _dt.datetime.now(_dt.UTC).isoformat()
        _upsert_config_json(conn, sid, json.dumps(snapshot), now_iso)
    invalidate_config_cache(sid)


def record_deployed_config(
    service_id: str,
    activated_version: int | None = None,
    *,
    previous_version: int | None = None,
) -> None:
    """Record the currently-saved config as deployed, preserving the prior deployed snapshot."""
    sid = _validate_service_id(service_id)
    activated_version = (
        activated_version if isinstance(activated_version, int) and not isinstance(activated_version, bool) else None
    )
    previous_version = (
        previous_version if isinstance(previous_version, int) and not isinstance(previous_version, bool) else None
    )
    from backend.core import catalog

    with config_lock(sid), _open_or_reuse_conn(sid) as (conn, _):
        row = conn.execute(
            """
            SELECT config_json, deployed_config_json, previous_deployed_config_json, previous_activated_version
            FROM service_configs
            WHERE service_id = ?
            """,
            (sid,),
        ).fetchone()
        if row is None:
            raise RuntimeError(f"Cannot record deployed config: service {sid!r} not found")
        cfg = json.loads(row["config_json"])
        if activated_version is not None:
            cfg["last_activated_version"] = activated_version
        cfg["active_log_field_ids"] = catalog.resolve_active_log_field_ids(catalog.get_lf_config(cfg))
        new_json = json.dumps(cfg)

        cur_deployed = row["deployed_config_json"]
        prev_deployed = row["previous_deployed_config_json"]
        prev_ver = row["previous_activated_version"]

        if cur_deployed is not None and (activated_version is not None or cur_deployed != new_json):
            prev_deployed = cur_deployed
        if previous_version is not None and previous_version != activated_version:
            prev_ver = previous_version

        now_iso = _dt.datetime.now(_dt.UTC).isoformat()
        conn.execute(
            """
            UPDATE service_configs
            SET config_json = ?,
                deployed_config_json = ?,
                previous_deployed_config_json = ?,
                previous_activated_version = ?,
                revision = revision + 1,
                updated_at = ?
            WHERE service_id = ?
            """,
            (new_json, new_json, prev_deployed, prev_ver, now_iso, sid),
        )
    invalidate_config_cache(sid)


def rollback_deployed_config(service_id: str) -> tuple[dict, int | None]:
    """Roll back both draft and deployed config to the previously deployed snapshot."""
    sid = _validate_service_id(service_id)
    from backend.core import catalog

    with config_lock(sid), _open_or_reuse_conn(sid) as (conn, _):
        row = conn.execute(
            """
            SELECT config_json, previous_deployed_config_json, previous_activated_version
            FROM service_configs
            WHERE service_id = ?
            """,
            (sid,),
        ).fetchone()
        if row is None or row["previous_deployed_config_json"] is None:
            raise ValueError(f"No previous deployed config snapshot recorded for {service_id}")

        restored = json.loads(row["previous_deployed_config_json"])
        prev_ver: int | None = row["previous_activated_version"]

        current_cfg = json.loads(row["config_json"]) if row["config_json"] is not None else {}
        existing_salt = _extract_cid_salt(current_cfg)
        if existing_salt:
            rum = restored.get("rum")
            if not isinstance(rum, dict):
                rum = {}
                restored["rum"] = rum
            rum["cid_salt"] = existing_salt

        if prev_ver is not None:
            restored["last_activated_version"] = prev_ver
        restored["active_log_field_ids"] = catalog.resolve_active_log_field_ids(catalog.get_lf_config(restored))
        restored_json = json.dumps(restored)
        now_iso = _dt.datetime.now(_dt.UTC).isoformat()

        conn.execute(
            """
            UPDATE service_configs
            SET config_json = ?,
                deployed_config_json = ?,
                previous_deployed_config_json = NULL,
                previous_activated_version = NULL,
                revision = revision + 1,
                updated_at = ?
            WHERE service_id = ?
            """,
            (restored_json, restored_json, now_iso, sid),
        )
    invalidate_config_cache(sid)
    return (restored, prev_ver)


def migrate_legacy_file_configs_once(configs_dir: Path | None = None) -> int:
    """Import legacy ``configs/*.json`` files once if ``service_configs`` is empty."""
    from backend.core.metadata import pg_pool

    with contextlib.closing(pg_pool.get_pg_readonly_connection()) as conn:
        if conn.execute("SELECT 1 FROM service_configs LIMIT 1").fetchone() is not None:
            return 0

    target_dir = configs_dir or CONFIGS_DIR
    if not target_dir.is_dir():
        return 0

    imported = 0
    for path in sorted(target_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("migrate_legacy_file_configs_once: skip unreadable %s: %s", path.name, e)
            continue
        if not isinstance(data, dict):
            logger.warning("migrate_legacy_file_configs_once: skip non-object %s", path.name)
            continue
        raw_sid = data.get("service_id") or path.stem
        try:
            sid = _validate_service_id(raw_sid)
        except ValueError as e:
            logger.warning("migrate_legacy_file_configs_once: skip invalid service_id in %s: %s", path.name, e)
            continue
        data.setdefault("service_id", sid)
        save_config(sid, data)
        activated_ver = data.get("last_activated_version")
        if not isinstance(activated_ver, int) or isinstance(activated_ver, bool):
            activated_ver = None
        record_deployed_config(sid, activated_ver)
        imported += 1
    return imported


def update_status(service_id: str, status: dict):
    """Partially update the 'status' section of a service config."""

    def _apply(cfg: dict) -> None:
        current_status = cfg.get("status", {})
        current_status.update(status)
        current_status["updated_at"] = time.time()
        cfg["status"] = current_status

    mutate_config(service_id, _apply)


def get_status(service_id: str) -> dict:
    """Return the 'status' section of a service config."""
    cfg = load_config(service_id)
    if not cfg:
        return {}
    return cfg.get("status", {})


def _atomic_write_json(path, data: dict) -> None:
    """Write ``data`` to ``path`` atomically (unique tmp file + os.replace)."""
    atomic_write_json(path, data, indent=2, fallback_in_place_errnos={13, 16})


_cdn_service_id_map: dict[str, str] | None = None


def get_cdn_service_id_map() -> dict[str, str]:
    """Return a mapping of CDN service IDs to their logging service IDs."""
    snap = _get_or_refresh_snapshot()
    return dict(snap.cdn_map)


def list_service_ids() -> list[str]:
    """Return all configured service IDs (sorted)."""
    snap = _get_or_refresh_snapshot()
    return list(snap.service_ids)


def list_configs() -> list[dict]:
    """Return all service configs as a list, sorted by service ID."""
    snap = _get_or_refresh_snapshot()
    return [copy.deepcopy(snap.by_id[sid][1]) for sid in snap.service_ids]


def get_active_service_id(fallback_to_first: bool = True) -> str | None:
    """Return the first configured service ID as a fallback default.

    The actual active service is tracked client-side in localStorage and
    passed via the ?service= query param. This is used server-side when
    no service is specified (e.g. health checks, cron jobs).
    """
    ids = list_service_ids()
    if not ids:
        return None
    return ids[0] if fallback_to_first else None


class _SourceConfigModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: Any = Field("default", validation_alias="service_id")
    service_id: Any = "default"
    service_name: Any = Field("", validation_alias="name")
    endpoint: Any = ""
    fos_native_endpoint: Any = Field(None, validation_alias="fos_endpoint")
    access_key_id: Any = Field("", validation_alias="fos_access_key_id")
    secret_access_key: Any = Field("", validation_alias="fos_secret_access_key")
    bucket: Any = Field("", validation_alias="fos_bucket")
    prefix: str = ""
    region: Any = Field("us-east-1", validation_alias="fos_region")
    cdn_url: Any = ""
    cdn_secret: Any = ""
    cdn_service_id: Any = ""
    logging_service_id: Any = Field("", validation_alias="service_id")
    storage_mode: Any = "cloud"
    log_period: int = DEFAULT_LOG_PERIOD_SECONDS
    log_fields: Annotated[dict[str, Any], BeforeValidator(lambda v: dict(v) if v else {})] = Field(default_factory=dict)
    provisioning: dict[str, Any] = Field(default_factory=dict)
    rum_enabled: bool = False
    rum: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _finalize(self) -> "_SourceConfigModel":
        # Native FOS endpoint — used by boto3 for ListObjectsV2/PutObject/DeleteObject
        # etc. The CDN VCL only proxies GET/HEAD on object keys, not bucket-listing
        # requests, so writes and lists must go direct.
        if "fos_native_endpoint" not in self.model_fields_set:
            self.fos_native_endpoint = f"{self.region}.object.fastlystorage.app"
        # Prefer CDN URL as the S3 endpoint for FOS object reads
        # so we benefit from caching and free egress.
        self.endpoint = (
            self.cdn_url.replace("https://", "").replace("http://", "").split("/")[0]
            if self.cdn_url
            else self.fos_native_endpoint
        )
        if self.log_fields and "format_hash" not in self.log_fields:
            with contextlib.suppress(Exception):
                from backend.core import log_fields as lf_module

                self.log_fields["format_hash"] = lf_module.format_hash(self.log_fields)
        return self


def config_to_source(cfg: dict) -> dict:
    """Convert a service config dict to the normalized 'source' dict format."""
    out = _SourceConfigModel.model_validate(cfg).model_dump()
    out["rum_enabled"] = is_rum_enabled(cfg)
    return out


def fetch_service_name(service_id: str, api_key: str) -> str | None:
    """Fetch the human-readable service name from the Fastly API.

    Routes through the shared ``fastly()`` client with ``timeout=10`` +
    ``max_retries=1`` so the worst-case cold-path tail stays bounded
    (~21 s vs the client default of ~127 s). The caller is behind a
    300 s name cache (see :func:`refresh_service_name`) so steady-state
    cost is one call per service per 5 min.

    Returns None on failure (caller should use the cached name).
    """
    from backend.core.fastly.client import fastly

    try:
        data = fastly("GET", f"/service/{service_id}", token=api_key, timeout=10, max_retries=1)
        return data.get("name")
    except Exception:
        return None


def refresh_service_name(service_id: str, api_key: str | None = None) -> str:
    """Return service name, using cache. Refreshes from API if cache is stale.

    Falls back to name in config, then to service_id itself.
    """
    now = time.time()
    with _name_cache_lock:
        entry = _name_cache.get(service_id)
        if entry and (now - entry["fetched_at"]) < _NAME_CACHE_TTL:
            return entry["name"]

    # Try to fetch from API
    api_name = None
    if api_key and api_key.strip():
        api_name = fetch_service_name(service_id, api_key)
        if api_name:
            with _name_cache_lock:
                _name_cache[service_id] = {"name": api_name, "fetched_at": now}

            # Persist to config if it has changed or was the ID
            cfg = load_config(service_id)
            if cfg and (cfg.get("name") != api_name or cfg.get("name") == service_id):

                def _update_name(c: dict) -> None:
                    if c.get("name") != api_name or c.get("name") == service_id:
                        c["name"] = api_name

                mutate_config(service_id, _update_name)
            return api_name

    # Fall back to config
    cfg = load_config(service_id)
    if cfg:
        name = cfg.get("name") or service_id
        with _name_cache_lock:
            # On failure, don't try again for 2 minutes
            _name_cache[service_id] = {"name": name, "fetched_at": now - _NAME_CACHE_TTL + 120}
        return name

    return service_id


def _refresh_service_names_bg(misses_args: list[tuple[str, str | None]]) -> None:
    try:
        for sid, api_key in misses_args:
            try:
                refresh_service_name(sid, api_key)
            except Exception:
                pass
    finally:
        with _name_refresh_lock:
            for sid, _ in misses_args:
                _name_refresh_in_flight.discard(sid)


def refresh_all_service_names(configs: list[dict], _sync: bool = False) -> dict[str, str]:
    """Refresh service names for all configs. Returns {service_id: name}.

    Stale-while-revalidate asynchronous refresh by default to completely avoid
    blocking web requests or spawning a ThreadPoolExecutor on the request thread.
    Returns the best available name (cached or from JSON config) immediately
    and spins off a background thread to update the cache in the background.

    For testing/synchronous usage, specify _sync=True.
    """
    now = time.time()
    name_map: dict[str, str] = {}
    misses: list[dict] = []
    with _name_cache_lock:
        for cfg in configs:
            sid = cfg.get("service_id", "")
            entry = _name_cache.get(sid)
            if entry and (now - entry["fetched_at"]) < _NAME_CACHE_TTL:
                name_map[sid] = entry["name"]
            else:
                name_map[sid] = entry["name"] if entry else (cfg.get("name") or sid)
                misses.append(cfg)

    if not misses:
        return name_map

    if _sync:
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(refresh_service_name, cfg.get("service_id", ""), cfg.get("fastly_api_key", "")): cfg
                for cfg in misses
            }
            for future in concurrent.futures.as_completed(futures):
                cfg = futures[future]
                sid = cfg.get("service_id", "")
                try:
                    name_map[sid] = future.result()
                except Exception:
                    name_map[sid] = cfg.get("name") or sid
        return name_map

    # Asynchronous stale-while-revalidate path
    to_schedule = []
    with _name_refresh_lock:
        for cfg in misses:
            sid = cfg.get("service_id", "")
            if sid not in _name_refresh_in_flight:
                _name_refresh_in_flight.add(sid)
                to_schedule.append((sid, cfg.get("fastly_api_key")))

    if to_schedule:
        try:
            threading.Thread(
                target=_refresh_service_names_bg,
                args=(to_schedule,),
                name="refresh-service-names-bg",
                daemon=True,
            ).start()
        except Exception:
            with _name_refresh_lock:
                for sid, _ in to_schedule:
                    _name_refresh_in_flight.discard(sid)

    return name_map


# ── Fastly credential helpers ─────────────────────────────────────────────────


def _get_cfg_field(field: str, service_id: str | None, default):
    """Return ``cfg[field]`` for ``service_id`` (or the active service), or
    ``default`` when nothing is configured.

    Shared by the four near-identical fastly + ngwaf accessors below. All
    four used to paste the same ``sid = service_id or get_active_service_id();
    if sid: cfg = load_config(sid); if cfg: return cfg.get(field, ...)``
    block. Pulling the dispatch up means adding another accessor is a
    one-line tuple addition.
    """
    sid = service_id or get_active_service_id()
    if sid:
        cfg = load_config(sid)
        if cfg:
            return cfg.get(field, default)
    return default


def is_rum_enabled(cfg: dict | None) -> bool:
    """Whether RUM is enabled for a service config.

    ``cfg["rum"]["enabled"]`` wins whenever it is present and not ``None``; the root
    ``cfg["rum_enabled"]`` is only consulted otherwise. This is the precedence the scheduler
    and the RUM cron jobs use, so every status/health surface must use it too.
    """
    cfg = cfg or {}
    rum = cfg.get("rum")
    enabled = rum.get("enabled") if isinstance(rum, dict) else None
    if enabled is not None:
        return bool(enabled)
    return bool(cfg.get("rum_enabled", False))


def set_rum_enabled(cfg: dict, enabled: bool) -> None:
    """Set the canonical RUM enabled state on both ``rum_enabled`` and ``rum.enabled``."""
    flag = bool(enabled)
    cfg["rum_enabled"] = flag
    rum = cfg.get("rum")
    if not isinstance(rum, dict):
        rum = {}
        cfg["rum"] = rum
    rum["enabled"] = flag


def is_scoring_enabled(cfg: dict | None) -> bool:
    """Whether edge session scoring is enabled for a service config."""
    scoring = (cfg or {}).get("scoring")
    val = scoring.get("enabled") if isinstance(scoring, dict) else None
    return bool(val) if val is not None else bool((cfg or {}).get("scoring_enabled", False))


def is_cmcd_enabled(cfg: dict | None) -> bool:
    """Whether CMCD telemetry is enabled for a service config."""
    cmcd = (cfg or {}).get("cmcd")
    val = cmcd.get("enabled") if isinstance(cmcd, dict) else None
    return bool(val) if val is not None else bool((cfg or {}).get("cmcd_enabled", False))


def is_logging_enabled(cfg: dict | None) -> bool:
    """Whether access logging is enabled for a service config (defaults to True)."""
    val = (cfg or {}).get("logging_enabled")
    return True if val is None else bool(val)


def get_fastly_api_key(service_id: str | None = None) -> str:
    """Return the Fastly API key for a service (or the active service)."""
    return _get_cfg_field("fastly_api_key", service_id, "")


def get_fastly_service_id(service_id: str | None = None) -> str:
    """Return the CDN service ID for a service."""
    return _get_cfg_field("cdn_service_id", service_id, "")


def get_fastly_logging_service_id(service_id: str | None = None) -> str:
    """Return the logging service ID (the FOS logging endpoint's parent service)."""
    return _get_cfg_field("service_id", service_id, "")


def get_ngwaf_workspace_id(service_id: str) -> str | None:
    """Return the ngwaf_workspace_id for a service, or None if not configured."""
    return _get_cfg_field("ngwaf_workspace_id", service_id, None) or None


# ── Global usage logging config ────────────────────────────────────────────────

_USAGE_LOGGING_CONFIG_PATH = SYSTEM_DATA_DIR / "usage_logging.json"


class _UsageLoggingConfigModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: bool = False
    retention_days: int = 30
    class_a_rate_per_1k: float = 0.0025
    class_b_rate_per_10k: float = 0.004
    cdn_egress_rate_per_gb: float = 0.12
    storage_rate_per_gb_month: float = 0.02
    min_billed_days: int = 30
    billing_zone: str = "MBZ200"


_USAGE_LOGGING_DEFAULTS: dict = _UsageLoggingConfigModel().model_dump()


def load_usage_logging_config() -> dict:
    """Return the global usage logging config, creating defaults if missing."""
    _ensure_dirs()
    if _USAGE_LOGGING_CONFIG_PATH.exists():
        try:
            return _UsageLoggingConfigModel.model_validate_json(_USAGE_LOGGING_CONFIG_PATH.read_bytes()).model_dump()
        except Exception:
            pass
    return dict(_USAGE_LOGGING_DEFAULTS)


def save_usage_logging_config(cfg: dict):
    """Persist the global usage logging config atomically. See
    :func:`_atomic_write_json`."""
    _ensure_dirs()
    _atomic_write_json(_USAGE_LOGGING_CONFIG_PATH, cfg)


def is_usage_logging_enabled() -> bool:
    """Quick check: is usage logging enabled globally?

    Always returns False during unit tests to avoid pollution and overhead.
    """
    if "pytest" in sys.modules:
        return False
    return load_usage_logging_config().get("enabled", False)


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402


class _ConfigScalarCaches:
    @staticmethod
    def clear() -> None:
        global _cdn_service_id_map, _table_snapshot
        with _config_cache_lock:
            _table_snapshot = None
        _cdn_service_id_map = None
        _invalidate_service_ids_cache()


_CacheRegistry.register("config._name_cache", _name_cache)
_CacheRegistry.register("config._name_refresh_in_flight", _name_refresh_in_flight)
_CacheRegistry.register("config._config_cache", _config_cache)
_CacheRegistry.register("config._scalar_caches", _ConfigScalarCaches())

# Cloud / Scalability Overrides (Phase 2+)
# Valkey URL (SSE backplane, log flusher); the name predates the Celery worker's removal.
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "")
HOT_S3_ENDPOINT = os.getenv("HOT_S3_ENDPOINT", "")
HOT_S3_KEY = os.getenv("HOT_S3_KEY", "")
HOT_S3_SECRET = os.getenv("HOT_S3_SECRET", "")


def _default_sse_backplane() -> str:
    if explicit := os.getenv("SSE_BACKPLANE"):
        return explicit.strip()
    return "valkey" if bool(CELERY_BROKER_URL) else "local"


SSE_BACKPLANE = _default_sse_backplane()


def resolve_raw_delete_after(cfg: dict | None, requested: bool | None = None) -> bool:
    """Resolve the effective delete_after decision for raw FOS objects.

    ``requested`` lets a caller pass an explicit override; ``None`` falls back to
    the configured ``provisioning.cron_sync.delete_after`` (default True), enforced
    by the archive deletion controller.
    """
    if requested is not None:
        return bool(requested)
    return bool((cfg or {}).get("provisioning", {}).get("cron_sync", {}).get("delete_after", True))


def resolve_snapshot_interval_minutes(cfg: dict | None, requested: int | None = None) -> int:
    """Resolve the effective snapshot_interval_minutes (5, 15, 30, 60, 360, 1440; default 5)."""
    from backend.core.service_defaults import (
        DEFAULT_SNAPSHOT_INTERVAL_MINUTES,
        VALID_SNAPSHOT_INTERVAL_MINUTES,
    )

    raw: Any = requested
    if raw is None:
        prov = cfg.get("provisioning") if isinstance(cfg, dict) else None
        cron_sync = prov.get("cron_sync") if isinstance(prov, dict) else None
        raw = cron_sync.get("snapshot_interval_minutes") if isinstance(cron_sync, dict) else None

    if raw is None or isinstance(raw, bool):
        return DEFAULT_SNAPSHOT_INTERVAL_MINUTES
    if isinstance(raw, float):
        if not math.isfinite(raw) or not raw.is_integer():
            return DEFAULT_SNAPSHOT_INTERVAL_MINUTES
        val = int(raw)
    elif isinstance(raw, int):
        val = raw
    elif isinstance(raw, str):
        try:
            val = int(raw.strip())
        except ValueError:
            return DEFAULT_SNAPSHOT_INTERVAL_MINUTES
    else:
        return DEFAULT_SNAPSHOT_INTERVAL_MINUTES

    if val in VALID_SNAPSHOT_INTERVAL_MINUTES:
        return val
    return DEFAULT_SNAPSHOT_INTERVAL_MINUTES


def service_id_from_source(
    source: dict | None,
    service_id: str | None = None,
    *,
    prefer_name: bool = False,
) -> str:
    """Resolve ``service_id`` from an explicit override or the ``source`` dict."""
    if service_id:
        return service_id
    if not source:
        return ""
    if prefer_name:
        return source.get("name") or source.get("service_id") or ""
    return source.get("service_id") or source.get("name") or ""


def path_within_dir(base_dir: str, candidate: str) -> bool:
    """Return ``True`` iff ``candidate`` resolves to a path inside ``base_dir``."""
    base_real = os.path.realpath(base_dir)
    candidate_real = os.path.realpath(candidate)
    try:
        return os.path.commonpath([base_real, candidate_real]) == base_real
    except ValueError:
        return False
