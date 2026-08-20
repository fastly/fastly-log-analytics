"""Service configuration management for multi-service support.

Each service is stored as a JSON file in the configs/ directory.
The filename is the Fastly logging service ID (e.g. abcdefg12345abcdef1234.json).

Config file schema:
{
    "service_id": "abcdefg12345abcdef1234",   # Fastly logging service ID (matches filename)
    "name": "My Production Site",               # Human-readable label (from Fastly API)
    "access_level": "read_write",               # "read_write" or "read_only"
    "fos_endpoint": "us-east-1.object.fastlystorage.app",
    "fos_access_key_id": "...",
    "fos_secret_access_key": "...",
    "fos_bucket": "...",
    "fos_prefix": "",
    "fos_region": "us-east-1",
    "cdn_url": "https://...",
    "cdn_secret": "...",
    "cdn_service_id": "...",                    # The CDN VCL service fronting FOS
    "fastly_api_key": "...",                    # Account-wide Fastly API key
    "provisioning": {                           # Embedded provisioning state for teardown
        "endpoint_name": "Fastly Object Storage Logs",
        "temp_admin_key_id": null
    }
}
"""

import json
import os
import re
import sys
import tempfile
import threading
import time
from pathlib import Path

_ROOT_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CONFIGS_DIR = _ROOT_DIR / "configs"
DATA_DIR = _ROOT_DIR / "data"

# Sub-directories for organized data storage
SERVICES_DATA_DIR = DATA_DIR / "services"
NGWAF_DATA_DIR = DATA_DIR / "ngwaf"
CACHE_DATA_DIR = DATA_DIR / "cache"
SYSTEM_DATA_DIR = DATA_DIR / "system"

# Cache for Fastly service names: {service_id: {"name": str, "fetched_at": float}}
_name_cache: dict = {}
_name_cache_lock = threading.Lock()
_name_refresh_in_flight: set[str] = set()
_name_refresh_lock = threading.Lock()
_NAME_CACHE_TTL = 300  # 5 minutes

# Cache for parsed configs: {service_id: (mtime_ns, raw_bytes)}.
# Revalidated by stat() on each load_config call AND explicitly invalidated
# by save_config — mtime alone is not enough because Linux ext4/tmpfs can
# produce identical st_mtime_ns for two writes within the same microsecond.
# Hot paths (sync-status polls, every cron tick, every dashboard request)
# hit this enough that skipping the open+read on the no-change path adds up.
_config_cache: dict[str, tuple[int, bytes]] = {}
_config_cache_lock = threading.Lock()

# Path-keyed memo for _ensure_dirs(). Called from every list_configs /
# save_config / save_usage_logging_config so the mkdir(exist_ok=True)
# syscall storm shows up under load. Tests that monkeypatch CONFIGS_DIR
# get fresh entries because the tmp_path is a different Path object.
_ensured_dirs: set[Path] = set()


def _ensure_dirs():
    dirs = (CONFIGS_DIR, DATA_DIR, SERVICES_DATA_DIR, NGWAF_DATA_DIR, CACHE_DATA_DIR, SYSTEM_DATA_DIR)
    missing = [d for d in dirs if d not in _ensured_dirs]
    if not missing:
        return
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
    if not _SERVICE_ID_RE.match(service_id):
        raise ValueError(f"invalid service_id {service_id!r}: must be alphanumeric / dash / underscore (security)")
    return service_id


def config_path(service_id: str) -> Path:
    _validate_service_id(service_id)
    return CONFIGS_DIR / f"{service_id}.json"


def duckdb_path(service_id: str) -> str:
    _validate_service_id(service_id)
    return str(SERVICES_DATA_DIR / f"{service_id}.duckdb")


def load_config(service_id: str | None) -> dict | None:
    """Load a single service config by ID. Returns None if not found.

    Returns a freshly-parsed dict on every call — callers that mutate the
    result (e.g. update_status) won't poison the cache. The on-disk file is
    revalidated via st_mtime_ns, so external edits and save_config writes
    are picked up on the next call without explicit invalidation.

    Returns ``None`` (not a raised exception) for invalid service IDs,
    including ``None`` itself — several call sites in the iceberg/
    submodules pass ``src.get("name")`` (typed as ``Any | None``) and
    rely on the None response to mean "no config". Security's validation
    in ``config_path`` is still what blocks path-traversal; this just
    makes the helper friendlier at call sites that don't pre-validate.
    """
    if service_id is None:
        return None
    try:
        path = config_path(service_id)
    except ValueError:
        return None
    try:
        mtime_ns = path.stat().st_mtime_ns
    except FileNotFoundError:
        return None

    cached = _config_cache.get(service_id)
    if cached is not None and cached[0] == mtime_ns:
        return json.loads(cached[1])

    with open(path, "rb") as f:
        raw = f.read()
    parsed = json.loads(raw)
    with _config_cache_lock:
        _config_cache[service_id] = (mtime_ns, raw)
    return parsed


def _atomic_write_json(path, data: dict) -> None:
    """Write ``data`` to ``path`` atomically (unique tmp file + os.replace).

    Shared by :func:`save_config` and :func:`save_usage_logging_config`,
    which used to paste identical mkstemp / fdopen / json.dump / os.replace
    blocks. Uses a UNIQUE tmp file (per-call random suffix) so concurrent
    callers cannot clobber each other's tmp file mid-write. The shared-tmp
    variant produced corrupted JSON (a stray ``}`` appended to the rendered
    file) and bricked the backend on next read; we hit it twice in one
    debugging session before fixing.
    """
    fd, tmp_path = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def save_config(service_id: str, cfg: dict):
    """Write a service config atomically. See :func:`_atomic_write_json`."""
    global _cdn_service_id_map
    _ensure_dirs()
    if "created_at" not in cfg or not cfg["created_at"]:
        import datetime as _dt

        cfg["created_at"] = _dt.datetime.now(_dt.UTC).isoformat()
    _atomic_write_json(config_path(service_id), cfg)
    # Invalidate the load_config cache. The cache uses st_mtime_ns as its
    # revalidation key, which is normally fine — but on Linux ext4/tmpfs two
    # os.replace() calls within the same microsecond can produce identical
    # mtime_ns values, so a cached (old_mtime, old_raw) entry can stay
    # "fresh" after this write and serve stale bytes. update_status() does
    # exactly that: load → mutate → save → and the next load can see the
    # pre-mutation cached copy. Drop the entry so the next load re-reads.
    with _config_cache_lock:
        _config_cache.pop(service_id, None)
    _cdn_service_id_map = None


def update_status(service_id: str, status: dict):
    """Partially update the 'status' section of a service config."""
    cfg = load_config(service_id)
    if not cfg:
        return

    current_status = cfg.get("status", {})
    current_status.update(status)
    current_status["updated_at"] = time.time()
    cfg["status"] = current_status
    save_config(service_id, cfg)


def get_status(service_id: str) -> dict:
    """Return the 'status' section of a service config."""
    cfg = load_config(service_id)
    if not cfg:
        return {}
    return cfg.get("status", {})


def delete_config(service_id: str):
    """Remove a service config file."""
    global _cdn_service_id_map
    path = config_path(service_id)
    if path.exists():
        path.unlink()
    _cdn_service_id_map = None
    with _config_cache_lock:
        _config_cache.pop(service_id, None)


_cdn_service_id_map: dict[str, str] | None = None


def get_cdn_service_id_map() -> dict[str, str]:
    """Return a mapping of CDN service IDs to their logging service IDs. Memoized."""
    global _cdn_service_id_map
    if _cdn_service_id_map is not None:
        return _cdn_service_id_map

    m = {}
    for c in list_configs():
        cdn_sid = c.get("cdn_service_id")
        if cdn_sid:
            m[cdn_sid] = c["service_id"]
    _cdn_service_id_map = m
    return m


def list_service_ids() -> list[str]:
    """Return all configured service IDs (sorted)."""
    _ensure_dirs()
    return sorted(p.stem for p in CONFIGS_DIR.glob("*.json"))


def list_configs() -> list[dict]:
    """Return all service configs as a list, sorted by service ID."""
    return [c for sid in list_service_ids() if (c := load_config(sid)) is not None]


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


def config_to_source(cfg: dict) -> dict:
    """Convert a service config dict to the db.py 'source' dict format."""
    actual_db_path = duckdb_path(cfg.get("service_id", "default"))

    region = cfg.get("fos_region", "us-east-1")

    # Native FOS endpoint — used by boto3 for ListObjectsV2/PutObject/DeleteObject
    # etc. The CDN VCL only proxies GET/HEAD on object keys, not bucket-listing
    # requests, so writes and lists must go direct.
    native_endpoint = cfg.get("fos_endpoint", f"{region}.object.fastlystorage.app")

    # Prefer CDN URL as the S3 endpoint for DuckDB httpfs reads (parquet GETs)
    # so we benefit from caching and free egress.
    cdn_url = cfg.get("cdn_url")
    if cdn_url:
        endpoint = cdn_url.replace("https://", "").replace("http://", "").split("/")[0]
    else:
        endpoint = native_endpoint

    prov = cfg.get("provisioning", {})

    return {
        "name": cfg.get("service_id", "default"),
        "service_id": cfg.get("service_id", "default"),
        "service_name": cfg.get("name", ""),
        "endpoint": endpoint,
        "fos_native_endpoint": native_endpoint,
        "access_key_id": cfg.get("fos_access_key_id", ""),
        "secret_access_key": cfg.get("fos_secret_access_key", ""),
        "bucket": cfg.get("fos_bucket", ""),
        "prefix": cfg.get("fos_prefix", ""),
        "region": region,
        "cdn_url": cfg.get("cdn_url", ""),
        "cdn_secret": cfg.get("cdn_secret", ""),
        "cdn_service_id": cfg.get("cdn_service_id", ""),
        "logging_service_id": cfg.get("service_id", ""),
        "duckdb_path": actual_db_path,
        "access_level": cfg.get("access_level", "read_write"),
        "storage_mode": cfg.get("storage_mode", "cloud"),
        "log_period": int(cfg.get("log_period", 60)),
        "log_fields": cfg.get("log_fields", {}),
        "provisioning": prov,
        "rum_enabled": cfg.get("rum_enabled", False) or (cfg.get("rum") or {}).get("enabled", False),
        "rum": cfg.get("rum", {}),
    }


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
                cfg["name"] = api_name
                save_config(service_id, cfg)
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
            entry = _name_cache.get(sid)
            best_effort = sid
            if entry:
                best_effort = entry["name"]
            elif cfg.get("name"):
                best_effort = cfg["name"]
            name_map[sid] = best_effort

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


def get_fastly_api_key(service_id: str | None = None) -> str:
    """Return the Fastly API key for a service (or the active service)."""
    return _get_cfg_field("fastly_api_key", service_id, "")


def get_fastly_service_id(service_id: str | None = None) -> str:
    """Return the CDN service ID for a service."""
    return _get_cfg_field("cdn_service_id", service_id, "")


def get_fastly_logging_service_id(service_id: str | None = None) -> str:
    """Return the logging service ID (the FOS logging endpoint's parent service)."""
    return _get_cfg_field("service_id", service_id, "")


def ngwaf_db_path() -> str:
    """Return the path to the shared NGWAF bot cache SQLite file."""
    return str(DATA_DIR / "ngwaf_bot_cache.db")


def get_ngwaf_workspace_id(service_id: str) -> str | None:
    """Return the ngwaf_workspace_id for a service, or None if not configured."""
    return _get_cfg_field("ngwaf_workspace_id", service_id, None) or None


# ── Global usage logging config ────────────────────────────────────────────────

_USAGE_LOGGING_CONFIG_PATH = SYSTEM_DATA_DIR / "usage_logging.json"

_USAGE_LOGGING_DEFAULTS: dict = {
    "enabled": False,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.005,
    "class_b_rate_per_10k": 0.01,
    "cdn_egress_rate_per_gb": 0.12,
    "storage_rate_per_gb_month": 0.02,
    "min_billed_days": 30,
    # When true, every DuckDB connection enables HTTP-only structured logging
    # so we can record httpfs FOS GETs (parquet scans) as Class B ops. Adds
    # tiny per-query overhead (one in-memory log scan + truncate at close).
    "track_duckdb_httpfs": True,
}


def load_usage_logging_config() -> dict:
    """Return the global usage logging config, creating defaults if missing."""
    _ensure_dirs()
    if _USAGE_LOGGING_CONFIG_PATH.exists():
        try:
            with open(_USAGE_LOGGING_CONFIG_PATH) as f:
                stored = json.load(f)
            return {**_USAGE_LOGGING_DEFAULTS, **stored}
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


from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("config._name_cache", _name_cache)
_CacheRegistry.register("config._name_refresh_in_flight", _name_refresh_in_flight)
