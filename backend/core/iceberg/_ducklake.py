"""DuckLake catalog attach + registration helpers.

The v3 storage layer replaces the pyiceberg write path with a DuckLake
catalog attached as ``lake`` on each DuckDB connection. This module owns
the attach contract:

- **Durability**: for cloud-backed sources the default ``DATA_PATH`` is
  ``s3://{bucket}/{prefix}/ducklake/`` so committed parquet lands in FOS.
  The raw ``.gz`` files are deleted after ingest, so a local-disk default
  would leave the ONLY copy of the data on the VM's ephemeral disk.
  Local-only sources (and an explicit ``DUCKLAKE_DATA_PATH`` override)
  keep a local data path.
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

import logging
import os
import re
import threading
import time

from backend import config
from backend.utils.sql_validator import escape_sql_literal

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

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
_ATTACH_LOCK_TIMEOUT_S = float(os.environ.get("DUCKLAKE_ATTACH_LOCK_TIMEOUT_S", "20") or "20")

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
_ATTACH_CONFLICT_RETRY_ATTEMPTS = int(os.environ.get("DUCKLAKE_ATTACH_CONFLICT_RETRY_ATTEMPTS", "20") or "20")
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

    Cloud-backed sources (same condition the old FOS iceberg write used)
    default to durable object storage: the pipeline deletes raw ``.gz``
    files after ingest, so a local default would leave the sole copy of
    the data on the VM's ephemeral disk. Local-only sources keep local
    parquet under SERVICES_DATA_DIR.
    """
    bucket = source.get("bucket")
    if bucket and not _is_local_only(source):
        prefix = (source.get("prefix") or "").strip("/")
        base = f"{prefix}/ducklake" if prefix else "ducklake"
        return f"s3://{bucket}/{base}/"
    service_id = source.get("service_id") or source.get("name", "default")
    return str(config.SERVICES_DATA_DIR / str(service_id) / "parquet")


def _ducklake_attach(con, source: dict, read_only: bool = False) -> bool:
    """Attach the DuckLake catalog for ``source`` as ``lake`` on ``con``.

    Returns True when the catalog is attached (including "already
    attached"), False on failure. On a read-write attach the size-cap
    option is (re)asserted — see module docstring.
    """
    # DUCKLAKE_CATALOG only — deliberately does NOT fall back to
    # METADATA_DSN. The two are separate concerns (commit-path catalog vs.
    # cron/ingest bookkeeping) and ADR-15 §2 states the code does not assume
    # they coincide. A fallback made that false in the one configuration where
    # it was reachable: DEPLOYMENT_MODE=standard with METADATA_DSN set (the documented
    # halfway point of the SQLite→Postgres metadata migration), where it would
    # silently plant DuckLake's catalog tables inside the metadata database
    # AND abandon the per-service .ducklake file that held the real table
    # state — a silent catalog swap, which fails empty rather than loud. In
    # high-throughput mode it was already unreachable: validate_deployment_mode() requires
    # a Postgres DUCKLAKE_CATALOG, so the left operand is never falsy there.
    dsn = config.DUCKLAKE_CATALOG
    service_id = source.get("service_id") or source.get("name", "default")

    if not dsn:
        dsn = str(config.SERVICES_DATA_DIR / f"{service_id}.ducklake")
    elif dsn.startswith(("postgres://", "postgresql://")):
        dsn = f"postgres:{dsn}"

    data_path = config.DUCKLAKE_DATA_PATH or _default_data_path(source)

    # Everything below — extension load included — races with any other
    # connection in this process attaching the same "lake" alias
    # concurrently; see _attach_lock's docstring. A hang was observed in
    # testing when only the ATTACH statements were serialized and
    # INSTALL/LOAD ducklake was left outside the lock, so the whole
    # function body is covered.
    if not _attach_lock.acquire(timeout=_ATTACH_LOCK_TIMEOUT_S):
        logger.warning(
            "[ducklake] %s: timed out after %.0fs waiting for the process-wide attach lock — "
            "another connection's ATTACH is taking unusually long (e.g. a concurrent cron writer "
            "mid-ingest). Failing fast instead of hanging this caller's pool connection indefinitely.",
            service_id,
            _ATTACH_LOCK_TIMEOUT_S,
        )
        return False
    # Tracks whether THIS thread currently holds _attach_lock. The
    # file-handle-conflict retry loop below releases the lock during its
    # backoff sleep and re-acquires it afterward, so the single `finally`
    # release at the bottom must only fire when we're actually still
    # holding it (otherwise a timed-out re-acquire would double-release).
    lock_held = True
    try:
        try:
            extension_directory = os.getenv("DUCKDB_EXTENSION_DIRECTORY")
            if extension_directory:
                os.makedirs(extension_directory, exist_ok=True)
                escaped_extension_directory = extension_directory.replace("'", "''")
                con.execute(f"SET extension_directory = '{escaped_extension_directory}';")
            try:
                con.execute("LOAD ducklake;")
            except Exception:
                con.execute("INSTALL ducklake; LOAD ducklake;")
        except Exception as e:
            logger.warning("[ducklake] %s: failed to INSTALL/LOAD ducklake extension: %s", service_id, e)
            return False

        try:
            attached = con.execute("SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'").fetchone()
        except Exception as e:
            logger.warning("[ducklake] %s: failed to inspect existing lake attachment: %s", service_id, e)
            return False
        if attached:
            existing_read_only = bool(attached[0])
            # A read-only CALLER never needs to force a downgrade: an
            # existing READ-WRITE attach already supports SELECT queries
            # just fine, and — critically — "lake" is a per-db-path shared
            # attach, not a per-Python-connection one (duckdb.connect() to
            # the SAME service .duckdb file reuses one underlying instance,
            # so every connection to that file sees the SAME attached
            # "lake"). Unconditionally detaching on a mode mismatch used to
            # mean: any get_connection() reader that raced a live writer
            # (ingest/buffer/commit, which needs read_only=False) would rip
            # the writer's in-flight attach out from under it, mid-use, to
            # re-attach its own read-only preference — which is exactly the
            # DuckDB-core "Unique file handle conflict ... in the process of
            # being detached" race observed at backend cold start, when the
            # pool warm-up / view pre-warm / legacy-adoption sweep / first
            # cron tick all open connections in the same narrow window.
            # Only an actual write CALLER (read_only=False) still needs to
            # force the upgrade below when it finds a read-only attach —
            # DuckDB itself has no "upgrade in place" primitive.
            if existing_read_only or not read_only:
                if bool(attached[0]) == read_only:
                    try:
                        con.execute("SELECT 1 FROM ducklake_snapshots('lake') LIMIT 1").fetchone()
                    except Exception as e:
                        try:
                            con.execute("DETACH lake")
                        except Exception as detach_err:
                            logger.warning(
                                "[ducklake] %s: existing lake catalog is unusable (%s) and detach failed: %s",
                                service_id,
                                e,
                                detach_err,
                            )
                            return False
                    else:
                        if not read_only:
                            _apply_target_file_size(con)
                        return True
                else:
                    try:
                        con.execute("DETACH lake")
                    except Exception as detach_err:
                        logger.warning(
                            "[ducklake] %s: failed to detach mismatched lake catalog: %s",
                            service_id,
                            detach_err,
                        )
                        return False
            else:
                # existing is READ-WRITE, caller only wants READ-ONLY:
                # reuse the existing (stronger) attach as-is rather than
                # detaching it.
                try:
                    con.execute("SELECT 1 FROM ducklake_snapshots('lake') LIMIT 1").fetchone()
                except Exception as e:
                    logger.warning(
                        "[ducklake] %s: existing read-write lake catalog is unusable for read: %s",
                        service_id,
                        e,
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
                con.execute("DETACH __ducklake_metadata_lake")
            except Exception as detach_err:
                logger.warning(
                    "[ducklake] %s: failed to detach orphaned DuckLake metadata catalog: %s",
                    service_id,
                    detach_err,
                )
                return False

        if read_only and (dsn.startswith("postgres:") or not os.path.exists(dsn)):
            # A read-only attach of a not-yet-initialized catalog fails ("does
            # not exist - and creating a new DuckLake is explicitly disabled") —
            # for a local file we can cheaply detect that via os.path.exists;
            # for a Postgres DSN we can't, so always pre-create idempotently.
            # Create with a transient read-write attach so fresh services get a
            # queryable (empty) lake immediately.
            try:
                con.execute(
                    f"ATTACH 'ducklake:{escape_sql_literal(dsn)}' AS __lake_init "
                    f"(DATA_PATH '{escape_sql_literal(data_path)}', OVERRIDE_DATA_PATH TRUE);"
                )
                con.execute("DETACH __lake_init")
            except Exception as e:
                if "already attached" not in str(e) and "already exists" not in str(e):
                    logger.info("[ducklake] %s: could not pre-create catalog for read-only attach: %s", service_id, e)

        ro = ", READ_ONLY" if read_only else ""
        attach_sql = (
            f"ATTACH 'ducklake:{escape_sql_literal(dsn)}' AS lake "
            f"(DATA_PATH '{escape_sql_literal(data_path)}'{ro}, OVERRIDE_DATA_PATH TRUE);"
        )
        for attempt in range(_ATTACH_CONFLICT_RETRY_ATTEMPTS):
            try:
                con.execute(attach_sql)
                break
            except Exception as e:
                msg = str(e).lower()
                if "unique file handle conflict" in msg or "already attached by database" in msg:
                    for alias in ("lake", "__ducklake_metadata_lake"):
                        try:
                            con.execute(f"DETACH {alias}")
                        except Exception:
                            pass
                    if attempt < _ATTACH_CONFLICT_RETRY_ATTEMPTS - 1:
                        # Release the process-wide lock while we wait: the
                        # conflict is with a DIFFERENT connection's already
                        # -completed (and thus already-unlocked) attach, so
                        # holding _attach_lock here only forces an unrelated
                        # third connection to needlessly queue behind our
                        # wait. Re-acquire before the next attempt.
                        _attach_lock.release()
                        lock_held = False
                        time.sleep(_ATTACH_CONFLICT_RETRY_SLEEP_S)
                        if not _attach_lock.acquire(timeout=_ATTACH_LOCK_TIMEOUT_S):
                            logger.warning(
                                "[ducklake] %s: timed out re-acquiring the attach lock while retrying "
                                "after a file-handle conflict",
                                service_id,
                            )
                            return False
                        lock_held = True
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
                    # Check if the mode matches what we want
                    try:
                        row = con.execute(
                            "SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'"
                        ).fetchone()
                        if row and bool(row[0]) == read_only:
                            return True
                        elif row:
                            # Mode mismatch. Detach and let the loop retry.
                            try:
                                con.execute("DETACH lake")
                                if attempt < _ATTACH_CONFLICT_RETRY_ATTEMPTS - 1:
                                    _attach_lock.release()
                                    lock_held = False
                                    time.sleep(_ATTACH_CONFLICT_RETRY_SLEEP_S)
                                    if not _attach_lock.acquire(timeout=_ATTACH_LOCK_TIMEOUT_S):
                                        logger.warning(
                                            "[ducklake] %s: timed out re-acquiring the attach lock while retrying "
                                            "after a mode mismatch",
                                            service_id,
                                        )
                                        return False
                                    lock_held = True
                                    continue
                            except Exception as detach_err:
                                logger.warning(
                                    "[ducklake] %s: failed to detach mismatched lake catalog: %s",
                                    service_id,
                                    detach_err,
                                )
                                return False
                    except Exception:
                        pass
                    return False
                logger.warning("[ducklake] %s: failed to attach ducklake catalog: %s", service_id, e)
                return False
        else:
            # If the loop exhausts all attempts without breaking (e.g., continue on last attempt)
            logger.warning("[ducklake] %s: failed to attach ducklake catalog (retries exhausted)", service_id)
            return False

        if not read_only:
            _apply_target_file_size(con)
        return True
    finally:
        if lock_held:
            _attach_lock.release()


def _ducklake_detach(con, aliases: tuple[str, ...] = ("lake",), service_id: str = "default") -> None:
    """Detach ``aliases`` from ``con`` under the same process-wide lock ``_ducklake_attach`` uses.

    DETACH mutates the same shared/racy DuckLake catalog state that ATTACH
    does (see ``_attach_lock``'s docstring — the race was verified
    empirically across connections attached to the same on-disk file). A
    raw, unlocked ``con.execute("DETACH lake")`` from one connection can rip
    the catalog out from under another connection concurrently inside a
    locked ``_ducklake_attach`` call on a different connection, which is
    what produced production's "Schema with name lake does not exist!"
    commit failures — the OTHER connection's attach/inspection assumed
    "lake" would stay attached and was never re-verified afterward. Every
    caller that used to call ``con.execute("DETACH lake")`` directly must
    route through this helper instead.
    """
    if not _attach_lock.acquire(timeout=_ATTACH_LOCK_TIMEOUT_S):
        logger.warning(
            "[ducklake] %s: timed out after %.0fs waiting for the process-wide attach lock to detach %s",
            service_id,
            _ATTACH_LOCK_TIMEOUT_S,
            aliases,
        )
        return
    try:
        for alias in aliases:
            try:
                con.execute(f"DETACH {alias}")
            except Exception:
                pass
    finally:
        _attach_lock.release()


def _apply_target_file_size(con, alias: str = "lake") -> None:
    """Pin DuckLake's target file size to the compaction cap (idempotent).

    ``ducklake_merge_adjacent_files`` and table rewrites honor this option,
    so setting it at attach time is what keeps DuckLake-side compaction
    size-capped like the local tiered compaction. Checked before set so a
    steady-state attach doesn't write the catalog every time.
    """
    if not _IDENT_RE.match(alias):
        raise ValueError(f"invalid ducklake alias: {alias!r}")
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
    if not _IDENT_RE.match(alias):
        raise ValueError(f"invalid ducklake alias: {alias!r}")
    try:
        row = con.execute(
            f"SELECT snapshot_id FROM ducklake_snapshots('{alias}') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        return int(row[0]) if row else None
    except Exception as e:
        logger.info("[ducklake] snapshot-id probe failed (lake not attached?): %s", e)
        return None


def _ducklake_add_data_files(con, s3_paths: list[str], alias: str = "lake", table: str = "logs") -> None:
    """Register existing parquet files into ``alias.table``.

    NOT idempotent at the DuckLake level — re-adding a path duplicates its
    rows (verified against ducklake d318a545). Callers must skip paths
    already present in ``ducklake_list_files`` (see
    ``_ducklake_migration.adopt_iceberg_to_ducklake``).
    """
    if not s3_paths:
        return
    if not _IDENT_RE.match(alias):
        raise ValueError(f"invalid ducklake alias: {alias!r}")
    if not _IDENT_RE.match(table):
        raise ValueError(f"invalid ducklake table name: {table!r}")
    paths_str = ", ".join(f"'{escape_sql_literal(p)}'" for p in s3_paths)
    con.execute(f"CALL ducklake_add_data_files('{alias}', '{table}', [{paths_str}]);")
