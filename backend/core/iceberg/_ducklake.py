"""DuckLake catalog attach + registration helpers.

The v3 storage layer replaces the pyiceberg write path with a DuckLake
catalog attached as ``lake`` on each DuckDB connection. This module owns
the attach contract:

- **Local Storage Architecture**: Standard deployments store all committed
  DuckLake parquet files locally under ``/app/data/services/{service_id}/ducklake/``
  on fast local disk for instant analytical queries with 0 network latency.
  FOS / S3 serves as the edge ingress destination for raw ``.gz`` logs as well
  as the durable cloud backup/sync store for periodic commits (every
  ``commit_interval_mins``), archive Parquet, and synchronized admin state.
  Remote S3 parquet scans are prohibited during analytical queries unless
  explicitly permitted via ``ALLOW_REMOTE_S3_PARQUET_SCAN=1``.
- **Tenant isolation**: when ``DUCKLAKE_CATALOG`` points every service at
  one shared catalog (e.g. a Postgres DSN), a bare ``lake.logs`` would mix
  tenants. :func:`ducklake_table_name` derives a per-service table name
  that ALL readers and writers must use (view builder, buffer commit,
  ingest). Per-service file catalogs use it too — harmless there.
- **Size cap**: DuckLake's ``target_file_size`` is pinned to the same cap
  local compaction uses (``LOCAL_COMPACT_MAX_PARTITION_MB``, 256 MB
  default) so ``ducklake_merge_adjacent_files`` / rewrites never collapse
  parquet into fewer-larger files past the cap. MANY small files are GOOD
  for DuckDB scan parallelism — never raise this to "merge harder".

Trap #4 applies: every interpolated value in the ATTACH /
``ducklake_add_data_files`` SQL is escaped via ``escape_sql_literal`` or
validated as a bare identifier.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

from backend import config
from backend.utils.sql_validator import escape_sql_literal, require_safe_ident

logger = logging.getLogger(__name__)


# Serializes ``ATTACH ... AS lake`` across every connection in this process.
# Concurrent connections attaching a DuckLake catalog under the SAME alias
# name ("lake") race and silently corrupt each other's local catalog
# metadata — the ATTACH statement on EVERY racing connection reports
# success (no exception), but ``ducklake_snapshots('lake')`` (and anything
# else touching the catalog) then fails forever on that connection with
# ``Catalog "__ducklake_metadata_lake" does not exist``. Verified
# empirically: 6 threads each opening their own read-only connection to the
# same on-disk .duckdb file and calling ``_ducklake_attach`` concurrently
# reproduced the corruption on ALL 6 connections every time; serializing
# the attach with this lock made all 6 succeed, every time. This is what
# poisons pooled connections built during startup, when several requests
# concurrently trigger fresh connection builds. A single-file DuckDB attach
# to the same physical file is unaffected (that case is already
# process-exclusive per ADR-18) — this is specifically about the ``lake``
# alias/extension state, not the main database file.
_attach_lock = threading.Lock()

# Bounds how long a WAITING caller will queue for _attach_lock before giving
# up. The lock itself must stay unconditional mutual exclusion (see above) —
# this only lets an impatient caller stop waiting and fail fast instead of
# hanging its DuckDB pool connection slot forever when the current holder's
# ATTACH is blocked on a genuinely slow or stuck OS-level file lock (e.g. a
# concurrent cron writer mid-ingest). That blocking call has no internal
# timeout of its own — DuckDB's ATTACH can sit in an uninterruptible kernel
# wait for the file lock — so without this bound a queued reader can wait
# indefinitely, and pool exhaustion cascades from there.
_ATTACH_LOCK_TIMEOUT_S = float(os.environ.get("DUCKLAKE_ATTACH_LOCK_TIMEOUT_S", "60") or "60")

# Bounds how long a caller retries the ATTACH statement itself after hitting
# a "unique file handle conflict" — a DIFFERENT connection in this process
# still holds an open read-write attach on the same local .ducklake file
# (the lock above only serializes the brief ATTACH *call*, not how long a
# winner keeps its attach open while it does real work). Observed in
# production under bursty concurrent cron activity (log_discovery + commit
# + rum_commit + rollups overlapping): the conflict reliably clears within
# one or two 10s cron ticks, so the previous 5-attempt/1.5s (~7.5s total)
# budget was too short and surfaced as a logged error + a skipped ingest
# tick even though the very next tick always succeeded. Each sleep releases
# _attach_lock first (see the loop below) so an unrelated connection isn't
# blocked queuing behind this one's wait for a THIRD, unrelated connection.
_ATTACH_CONFLICT_RETRY_ATTEMPTS = int(os.environ.get("DUCKLAKE_ATTACH_CONFLICT_RETRY_ATTEMPTS", "45") or "45")
_ATTACH_CONFLICT_RETRY_SLEEP_S = float(os.environ.get("DUCKLAKE_ATTACH_CONFLICT_RETRY_SLEEP_S", "1.5") or "1.5")

# Same knob the local tiered compaction honors (backend/core/local_compaction.py
# _MAX_PARTITION_BYTES). Keeping the two caps on one env var means DuckLake
# merges and local compaction can never disagree about the ceiling.
_DUCKLAKE_TARGET_FILE_SIZE_MB = int(os.environ.get("LOCAL_COMPACT_MAX_PARTITION_MB", "256") or "256")


def _safe_ident(name: str) -> str:
    """Sanitize an arbitrary string into a bare SQL identifier fragment."""
    clean = re.sub(r"[^A-Za-z0-9_]", "_", name or "").strip("_").lower()
    return clean or "default"


def ducklake_table_name(src: dict, table_name: str = "logs") -> str:
    """Per-service DuckLake table name for ``src``.

    Under a shared catalog (``DUCKLAKE_CATALOG`` set) every service attaches
    the SAME ``lake`` — a bare ``lake.logs`` mixes tenants. This helper is
    the single naming authority used by the view builder, the buffer
    commit, and ingest; anything that reads or writes ``lake.*`` must go
    through it. Non-``logs`` tables (RUM ``client_vitals`` /
    ``client_errors``) get a per-service suffix too.
    """
    service_id = src.get("service_id") or src.get("name") or "default"
    base = f"logs_{_safe_ident(str(service_id))}"
    if table_name != "logs":
        base = f"{base}__{_safe_ident(table_name)}"
    return base


def _is_local_only(source: dict) -> bool:
    """Mirror of ``_core._is_local_only_source`` (kept inline to avoid a
    module-load cycle: _core imports this module during package init)."""
    if source.get("fos_local_warehouse") is True:
        return True
    endpoint = source.get("fos_endpoint") or source.get("endpoint") or ""
    return endpoint in ("http://localhost:0", "http://127.0.0.1:0")


def _default_data_path(source: dict) -> str:
    """Default DuckLake DATA_PATH for ``source``.

    High-scale workers and the serving pod may not share a filesystem,
    so cloud-backed sources use durable FOS storage in that topology. Standard
    deployments keep analytical data local for low-latency reads.
    """
    force_local = os.getenv("DUCKLAKE_LOCAL_STORAGE", "").lower() in ("1", "true", "yes")
    if config.is_high_scale_mode(source) and not force_local and source.get("bucket") and not _is_local_only(source):
        prefix = (source.get("prefix") or "").strip("/")
        base = f"{prefix}/ducklake" if prefix else "ducklake"
        return f"s3://{source['bucket']}/{base}/"

    service_id = source.get("service_id") or source.get("name", "default")
    local_path = config.SERVICES_DATA_DIR / str(service_id) / "ducklake"
    local_path.mkdir(parents=True, exist_ok=True)
    return str(local_path)


def _resolve_catalog_dsn(service_id: object) -> str | None:
    """Resolve and normalize the mandatory PostgreSQL DuckLake catalog DSN."""
    dsn = config.DUCKLAKE_CATALOG
    if not dsn:
        logger.error(
            "[ducklake] %s: DUCKLAKE_CATALOG is unset; PostgreSQL DSN is required",
            service_id,
        )
        return None
    if dsn.startswith(("postgres://", "postgresql://")):
        return f"postgres:{dsn}"
    if dsn.startswith("postgres:"):
        return dsn
    env_dsn = os.environ.get("DUCKLAKE_CATALOG", "")
    if env_dsn.startswith(("postgres://", "postgresql://")):
        return f"postgres:{env_dsn}"
    logger.error(
        "[ducklake] %s: DUCKLAKE_CATALOG must be a postgres DSN, got: %s",
        service_id,
        dsn,
    )
    return None


def _resolve_data_path(source: dict, service_id: object) -> str:
    """Resolve DATA_PATH for ``source`` and enforce the remote-S3 scan guard."""
    data_path = config.DUCKLAKE_DATA_PATH or _default_data_path(source)
    if data_path.startswith(("s3://", "http://", "https://")):
        allow_remote = os.getenv("ALLOW_REMOTE_S3_PARQUET_SCAN", "0").lower() in (
            "1",
            "true",
            "yes",
        ) or config.is_high_scale_mode(source)
        if not allow_remote and not source.get("allow_remote_parquet_scan"):
            local_fallback = _default_data_path(source)
            logger.error(
                "🚨 [storage-guard] BLOCKED remote S3 DuckLake parquet scan for %s (data_path=%s). "
                "All analytical data must be stored locally on disk. Falling back to local data path: %s. "
                "Set ALLOW_REMOTE_S3_PARQUET_SCAN=1 if emergency S3 access is strictly required.",
                service_id,
                data_path,
                local_fallback,
            )
            return local_fallback
        logger.warning(
            "⚠️ [storage-guard] Remote S3 DuckLake parquet scan is actively allowed via ALLOW_REMOTE_S3_PARQUET_SCAN=1 "
            "(service=%s, data_path=%s). Queries will incur S3 network latency.",
            service_id,
            data_path,
        )
    return data_path


def _acquire_attach_lock(service_id: object, reason: str) -> bool:
    """Acquire the process-wide ``_attach_lock`` with timeout logging."""
    if _attach_lock.acquire(timeout=_ATTACH_LOCK_TIMEOUT_S):
        return True
    logger.warning("[ducklake] %s: %s", service_id, reason)
    return False


def _sleep_releasing_attach_lock(service_id: object, reason: str) -> bool:
    """Release ``_attach_lock``, sleep for the conflict backoff, and re-acquire."""
    _attach_lock.release()
    time.sleep(_ATTACH_CONFLICT_RETRY_SLEEP_S)
    return _acquire_attach_lock(
        service_id,
        f"timed out re-acquiring the attach lock while retrying after {reason}",
    )


def _detach_unlocked(con, aliases: tuple[str, ...] = ("lake",), *, ignore_errors: bool = True) -> None:
    """Execute ``DETACH <alias>`` for each alias when caller already holds ``_attach_lock``."""
    for alias in aliases:
        try:
            con.execute(f"DETACH {alias}")
        except Exception:
            if not ignore_errors:
                raise
            logger.debug("_ducklake: DETACH %s failed (ignored)", alias, exc_info=True)


def _probe_lake_usable(con) -> Exception | None:
    """Return None when ``ducklake_snapshots('lake')`` succeeds, else the exception."""
    try:
        con.execute("SELECT 1 FROM ducklake_snapshots('lake') LIMIT 1").fetchone()
        return None
    except Exception as exc:
        return exc


def _load_ducklake_extension(con, service_id: object) -> bool:
    """Configure extension directory and load the DuckLake extension."""
    bundled_extension = os.getenv("DUCKLAKE_EXTENSION_PATH")
    try:
        extension_directory = os.getenv("DUCKDB_EXTENSION_DIRECTORY")
        if extension_directory:
            os.makedirs(extension_directory, exist_ok=True)
            con.execute(f"SET extension_directory = '{escape_sql_literal(extension_directory)}';")
        if bundled_extension:
            con.execute(f"LOAD '{escape_sql_literal(bundled_extension)}';")
        else:
            try:
                con.execute("LOAD ducklake;")
            except Exception:
                con.execute("INSTALL ducklake; LOAD ducklake;")
        return True
    except Exception as e:
        source_description = "bundled" if bundled_extension else "installed"
        logger.warning("[ducklake] %s: failed to load %s DuckLake extension: %s", service_id, source_description, e)
        return False


def _ducklake_attach(con, source: dict, read_only: bool = False) -> bool:
    """Attach the DuckLake catalog for ``source`` as ``lake`` on ``con``.

    Returns True when the catalog is attached (including "already
    attached"), False on failure. On a read-write attach the size-cap
    option is (re)asserted — see module docstring.
    """
    service_id = source.get("service_id") or source.get("name", "default")
    dsn = _resolve_catalog_dsn(service_id)
    if not dsn:
        return False
    data_path = _resolve_data_path(source, service_id)

    if not _acquire_attach_lock(
        service_id,
        f"timed out after {_ATTACH_LOCK_TIMEOUT_S:.0f}s waiting for the process-wide attach lock — "
        "another connection's ATTACH is taking unusually long (e.g. a concurrent cron writer "
        "mid-ingest). Failing fast instead of hanging this caller's pool connection indefinitely.",
    ):
        return False

    lock_held = True
    try:
        if not _load_ducklake_extension(con, service_id):
            return False

        try:
            attached = con.execute("SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'").fetchone()
        except Exception as e:
            logger.warning("[ducklake] %s: failed to inspect existing lake attachment: %s", service_id, e)
            return False
        if attached:
            existing_read_only = bool(attached[0])
            # Read-only callers reuse an existing read-write attach without detaching;
            # see module docstring and ADR-18 on avoiding unique file handle conflicts.
            if existing_read_only or not read_only:
                if existing_read_only == read_only:
                    probe_err = _probe_lake_usable(con)
                    if probe_err is None:
                        if not read_only:
                            _apply_target_file_size(con)
                        return True
                    try:
                        _detach_unlocked(con, ("lake",), ignore_errors=False)
                    except Exception as detach_err:
                        logger.warning(
                            "[ducklake] %s: existing lake catalog is unusable (%s) and detach failed: %s",
                            service_id,
                            probe_err,
                            detach_err,
                        )
                        return False
                else:
                    try:
                        _detach_unlocked(con, ("lake",), ignore_errors=False)
                    except Exception as detach_err:
                        logger.warning(
                            "[ducklake] %s: failed to detach mismatched lake catalog: %s",
                            service_id,
                            detach_err,
                        )
                        return False
            else:
                probe_err = _probe_lake_usable(con)
                if probe_err is not None:
                    logger.warning(
                        "[ducklake] %s: existing read-write lake catalog is unusable for read: %s",
                        service_id,
                        probe_err,
                    )
                    return False
                return True

        try:
            orphaned_metadata = con.execute(
                "SELECT 1 FROM duckdb_databases() WHERE database_name = '__ducklake_metadata_lake'"
            ).fetchone()
        except Exception as e:
            logger.warning("[ducklake] %s: failed to inspect DuckLake metadata attachment: %s", service_id, e)
            return False
        if orphaned_metadata:
            try:
                _detach_unlocked(con, ("__ducklake_metadata_lake",), ignore_errors=False)
            except Exception as detach_err:
                logger.warning(
                    "[ducklake] %s: failed to detach orphaned DuckLake metadata catalog: %s",
                    service_id,
                    detach_err,
                )
                return False

        ro = ", READ_ONLY" if read_only else ""
        base_opts = f"DATA_PATH '{escape_sql_literal(data_path)}', OVERRIDE_DATA_PATH TRUE, DATA_INLINING_ROW_LIMIT 0"
        attach_sql = f"ATTACH 'ducklake:{escape_sql_literal(dsn)}' AS lake ({base_opts}{ro});"
        for attempt in range(_ATTACH_CONFLICT_RETRY_ATTEMPTS):
            try:
                con.execute(attach_sql)
                break
            except Exception as e:
                msg = str(e).lower()
                if read_only and attempt == 0 and ("does not exist" in msg or "explicitly disabled" in msg):
                    try:
                        con.execute(f"ATTACH 'ducklake:{escape_sql_literal(dsn)}' AS __lake_init ({base_opts});")
                        _detach_unlocked(con, ("__lake_init",), ignore_errors=False)
                        continue
                    except Exception as init_err:
                        if "already attached" not in str(init_err) and "already exists" not in str(init_err):
                            logger.info(
                                "[ducklake] %s: could not pre-create catalog for read-only attach: %s",
                                service_id,
                                init_err,
                            )
                if "unique file handle conflict" in msg or "already attached by database" in msg:
                    _detach_unlocked(con, ("lake", "__ducklake_metadata_lake"))
                    if attempt < _ATTACH_CONFLICT_RETRY_ATTEMPTS - 1:
                        lock_held = _sleep_releasing_attach_lock(service_id, "a file-handle conflict")
                        if not lock_held:
                            return False
                        continue
                    logger.warning(
                        "[ducklake] %s: failed to attach ducklake catalog after %d attempts over ~%.0fs "
                        "(zombie lock timeout): %s",
                        service_id,
                        _ATTACH_CONFLICT_RETRY_ATTEMPTS,
                        _ATTACH_CONFLICT_RETRY_ATTEMPTS * _ATTACH_CONFLICT_RETRY_SLEEP_S,
                        e,
                    )
                    return False
                if ("database with name" in msg and "already exists" in msg) or ("already attached" in msg):
                    try:
                        row = con.execute(
                            "SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'"
                        ).fetchone()
                        if row and bool(row[0]) == read_only:
                            return True
                        if row:
                            try:
                                _detach_unlocked(con, ("lake",), ignore_errors=False)
                                if attempt < _ATTACH_CONFLICT_RETRY_ATTEMPTS - 1:
                                    lock_held = _sleep_releasing_attach_lock(service_id, "a mode mismatch")
                                    if not lock_held:
                                        return False
                                    continue
                            except Exception as detach_err:
                                logger.warning(
                                    "[ducklake] %s: failed to detach mismatched lake catalog: %s",
                                    service_id,
                                    detach_err,
                                )
                                return False
                    except Exception:
                        logger.debug("_ducklake_attach: step failed (ignored)", exc_info=True)
                    return False
                logger.warning("[ducklake] %s: failed to attach ducklake catalog: %s", service_id, e)
                return False
        else:
            logger.warning("[ducklake] %s: failed to attach ducklake catalog (retries exhausted)", service_id)
            return False

        if not read_only:
            _apply_target_file_size(con)
        return True
    finally:
        if lock_held:
            _attach_lock.release()


def _ducklake_detach(con, aliases: tuple[str, ...] = ("lake",), service_id: str = "default") -> None:
    """Detach ``aliases`` from ``con`` under the same process-wide lock ``_ducklake_attach`` uses."""
    if not _acquire_attach_lock(
        service_id,
        f"timed out after {_ATTACH_LOCK_TIMEOUT_S:.0f}s waiting for the process-wide attach lock to detach {aliases}",
    ):
        return
    try:
        _detach_unlocked(con, aliases)
    finally:
        _attach_lock.release()


def _apply_target_file_size(con, alias: str = "lake") -> None:
    """Pin DuckLake's target file size to the compaction cap (idempotent).

    ``ducklake_merge_adjacent_files`` and table rewrites honor this option,
    so setting it at attach time is what keeps DuckLake-side compaction
    size-capped like the local tiered compaction. Checked before set so a
    steady-state attach doesn't write the catalog every time.
    """
    require_safe_ident(alias, "invalid ducklake alias")
    target = f"{_DUCKLAKE_TARGET_FILE_SIZE_MB}MiB"
    target_bytes = str(_DUCKLAKE_TARGET_FILE_SIZE_MB * 1024 * 1024)
    try:
        row = con.execute(f"SELECT value FROM {alias}.options() WHERE option_name = 'target_file_size'").fetchone()
        if row and str(row[0]) == target_bytes:
            return
        con.execute(f"CALL {alias}.set_option('target_file_size', '{target}')")
    except Exception as e:
        logger.warning("[ducklake] failed to apply target_file_size=%s: %s", target, e)


def ducklake_current_snapshot_id(con, alias: str = "lake") -> int | None:
    """Latest snapshot id of the attached DuckLake catalog, or None.

    Used as the fast-path staleness token for the per-service ``logs``
    view: a commit (from any process sharing the catalog) bumps the
    snapshot id, which invalidates the cached view SQL. Catalog-wide by
    design — under a shared catalog this is conservative (another
    tenant's commit forces a rebuild) but never stale.
    """
    require_safe_ident(alias, "invalid ducklake alias")
    try:
        row = con.execute(
            f"SELECT snapshot_id FROM ducklake_snapshots('{alias}') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        return int(row[0]) if row else None
    except Exception as e:
        logger.info("[ducklake] snapshot-id probe failed (lake not attached?): %s", e)
        return None


# ── Bounded per-service admission for DuckLake writers ───────────────────────

DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S = float(os.getenv("DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S", "30"))


class DuckLakeAdmissionTimeout(TimeoutError):
    """A DuckLake writer could not acquire the service admission in time."""


_locks: dict[str, threading.RLock] = {}
_locks_guard = threading.Lock()
_stats: dict[str, dict[str, float]] = {}
_stats_guard = threading.Lock()


def _local_lock(service_id: str) -> threading.RLock:
    with _locks_guard:
        return _locks.setdefault(service_id, threading.RLock())


def _advisory_key(service_id: str) -> int:
    # PostgreSQL advisory locks take signed BIGINT values.
    value = int.from_bytes(hashlib.sha256(service_id.encode()).digest()[:8], "big", signed=False)
    return value - (1 << 64) if value >= (1 << 63) else value


def _record_stat(service_id: str, key: str, value: float = 1.0) -> None:
    with _stats_guard:
        bucket = _stats.setdefault(
            service_id,
            {"acquisitions": 0.0, "timeouts": 0.0, "wait_ms_total": 0.0, "hold_ms_total": 0.0},
        )
        bucket[key] += value


def get_admission_stats() -> dict[str, dict[str, float]]:
    """Return cumulative writer-admission counters for diagnostics."""
    with _stats_guard:
        return {service_id: dict(values) for service_id, values in _stats.items()}


@contextmanager
def ducklake_write_admission(
    service_id: str,
    *,
    timeout_s: float = DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S,
) -> Iterator[None]:
    """Serialize one service's DuckLake writer across threads and processes.

    Besides the process-local reentrant lock, the same service also takes a
    PostgreSQL advisory lock (metadata is Postgres-only, ADR-22), fencing writers
    in separate worker processes/pods.
    """
    lock = _local_lock(service_id)
    wait_started = time.monotonic()
    if not lock.acquire(timeout=max(0.0, timeout_s)):
        _record_stat(service_id, "timeouts")
        raise DuckLakeAdmissionTimeout(f"DuckLake writer admission timed out for {service_id}")
    _record_stat(service_id, "wait_ms_total", (time.monotonic() - wait_started) * 1000.0)

    pg_con = None
    advisory = False
    deadline = time.monotonic() + max(0.0, timeout_s)
    key = _advisory_key(service_id)
    hold_started = time.monotonic()
    try:
        import psycopg

        from backend.core.metadata.pg_connection import get_pg_thread_connection

        pg_con = get_pg_thread_connection()
        if pg_con is None:
            raise RuntimeError("pg_con is unexpectedly None")
        # Fair FIFO acquisition: block in PostgreSQL's lock-wait queue
        # (pg_advisory_lock) bounded by lock_timeout, instead of busy-polling
        # the non-blocking pg_try_advisory_lock. Polling has no queue, so under
        # write saturation whichever writer keeps losing the 0.1s race starves
        # and discards its whole already-parsed batch — a measured congestion
        # collapse (RUM 229 lock timeouts vs requests' 58, 792 files wedged).
        # lock_timeout is a session GUC; reset it afterward since these
        # connections are long-lived and reused across services.
        remaining_ms = int(max(1.0, (deadline - time.monotonic()) * 1000.0))
        pg_con.execute(f"SET lock_timeout = {remaining_ms}")
        try:
            pg_con.execute("SELECT pg_advisory_lock(?)", (key,))
            advisory = True
        except psycopg.errors.LockNotAvailable as exc:
            _record_stat(service_id, "timeouts")
            raise DuckLakeAdmissionTimeout(f"DuckLake advisory lock timed out for {service_id}") from exc
        finally:
            try:
                pg_con.execute("SET lock_timeout = 0")
            except Exception:
                logger.exception("Failed to reset lock_timeout for %s", service_id)
        yield
    finally:
        if advisory and pg_con is not None:
            try:
                pg_con.execute("SELECT pg_advisory_unlock(?)", (key,))
            except Exception:
                logger.exception("Failed to release DuckLake advisory lock for %s", service_id)
        lock.release()
        _record_stat(service_id, "acquisitions")
        _record_stat(service_id, "hold_ms_total", (time.monotonic() - hold_started) * 1000.0)


def _reset_for_tests() -> None:
    with _locks_guard:
        _locks.clear()
    with _stats_guard:
        _stats.clear()
