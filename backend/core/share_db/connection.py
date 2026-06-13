"""Thread-local connection pool + corruption self-heal for the global share DB.

Owns the on-disk location, the per-thread sqlite3 connection pool, PRAGMA
setup, and the quarantine-on-corruption recovery path. Everything else in
``backend.core.share_db`` borrows a connection from here.

Concurrency: thread-local pool keyed by ``"__global_share__"``. ``PRAGMA
foreign_keys=ON`` is re-asserted on every borrow because SQLite resets it
per-connection. WAL + ``synchronous=NORMAL`` matches the production
metadata DB standard.

Corruption self-heal: ``get_safe_share_db_connection`` catches only
open-time ``sqlite3.DatabaseError`` with a corruption-signature message and
quarantines the file aside. Lock timeouts / FD-exhaustion / disk-full
errors re-raise so a transient condition cannot silently delete the share
state.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time

logger = logging.getLogger(__name__)

# ── Locations ────────────────────────────────────────────────────────────────

_DATA_DIR = "data/system"
_DB_FILENAME = "remote_share.db"

_local = threading.local()
_init_lock = threading.Lock()
_initialized: set[str] = set()
_all_connections: list[sqlite3.Connection] = []
_all_connections_lock = threading.Lock()
# Maps id(con) -> quarantine path for connections that were rebuilt after
# corruption. Read once by _init_db and removed. sqlite3.Connection has no
# __dict__ so we can't tag the connection object directly.
_recovery_marker: dict[int, str] = {}


def db_path() -> str:
    """Absolute path to the global share DB file.

    Honors ``REMOTE_SHARE_DB_DIR`` for test isolation; defaults to
    ``data/system/remote_share.db``.
    """
    base = os.environ.get("REMOTE_SHARE_DB_DIR") or _DATA_DIR
    return os.path.join(base, _DB_FILENAME)


def _conn_pool() -> dict[str, sqlite3.Connection]:
    if not hasattr(_local, "conns"):
        _local.conns = {}
    return _local.conns


def get_safe_share_db_connection(path: str) -> sqlite3.Connection:
    """Open a connection to ``path``. On open-time corruption, quarantine the
    file aside and rebuild from scratch.

    Mirrors TESTING_PLAN_3 Item 1: ONLY catches ``sqlite3.DatabaseError``
    raised during open (e.g., "file is not a database"). Query-time errors
    are not handled here.
    """
    try:
        con = sqlite3.connect(path, timeout=30.0)
        # Force header read so a corrupt file fails here, not on first query.
        con.execute("SELECT 1").fetchone()
        return con
    except sqlite3.DatabaseError as exc:
        # Security: ``DatabaseError`` is the parent of
        # ``OperationalError``, which fires for transient conditions like
        # "database is locked" / "disk I/O error" / FD exhaustion. The
        # quarantine path renames the DB out from under any other open
        # connections AND wipes all share state — running it on a transient
        # error means a single lock-timeout under load can permanently
        # delete every invite, session, and audit row in the share DB.
        #
        # Restrict the quarantine to actual file-corruption signatures from
        # SQLite: "file is not a database" / "database disk image is malformed"
        # / "unsupported file format". Anything else (lock timeout, I/O error,
        # full disk, missing parent dir) is re-raised so the caller sees the
        # real error instead of silently nuking the DB.
        msg = str(exc).lower()
        is_corruption = (
            "malformed" in msg
            or "not a database" in msg
            or "unsupported file format" in msg
            or "image is malformed" in msg
        )
        if not is_corruption:
            # ERROR (not WARNING) so this near-miss is alertable from the
            # existing log-error monitoring without needing a new metric
            # plumbing — quarantine-skipped events should be rare; if we
            # start seeing them at volume it's a signal that the
            # is_corruption substrings need updating.
            logger.error(
                "[share_db] DatabaseError on open of %s NOT classified as corruption (err_type=%s); re-raising: %s",
                path,
                type(exc).__name__,
                exc,
            )
            raise

        epoch = int(time.time())
        corrupt_path = f"{path}.corrupt-{epoch}"
        try:
            os.replace(path, corrupt_path)
            logger.error(
                "[share_db] corrupt DB at %s quarantined to %s (reason=corruption, %s)",
                path,
                corrupt_path,
                exc,
            )
        except OSError:
            logger.exception("[share_db] failed to quarantine corrupt DB at %s", path)
            raise
        con = sqlite3.connect(path, timeout=30.0)
        # Write a recovery marker once schema is initialized — caller does that
        # in _init_db. sqlite3.Connection has no __dict__, so we keep the
        # mapping out-of-band keyed by id(con).
        _recovery_marker[id(con)] = corrupt_path
        return con


def get_global_share_con() -> sqlite3.Connection:
    """Return a thread-local connection to the global share DB."""
    # Local import to break the connection <-> schema circular dependency.
    # schema._init_db only runs on first-open per path, so the cost is
    # bounded.
    from backend.core.share_db.schema import _init_db

    pool = _conn_pool()
    con = pool.get("__global_share__")
    if con is not None:
        # Re-assert per-connection PRAGMA on every borrow — SQLite resets it
        # if anyone toggles it during the connection's lifetime.
        try:
            con.execute("PRAGMA foreign_keys=ON")
        except sqlite3.ProgrammingError:
            # closed; fall through to reopen.
            pool.pop("__global_share__", None)
            con = None
        else:
            return con

    path = db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if not _init_lock.acquire(timeout=10):
        raise sqlite3.OperationalError(
            "share_db._init_lock contended >10s — another thread is stuck inside connect+PRAGMA"
        )
    try:
        con = get_safe_share_db_connection(path)
        with _all_connections_lock:
            _all_connections.append(con)
        try:
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA foreign_keys=ON")
            con.execute("PRAGMA busy_timeout=30000")
            # 64MB page cache — keeps the share-flow's invite/session
            # lookups + audit-log writes hot in memory under concurrent
            # heartbeat polling from multiple analysts. Architecture-
            # review Dimension 2.
            con.execute("PRAGMA cache_size=-64000")

            if path not in _initialized:
                _init_db(con)
                _initialized.add(path)
        except Exception:
            try:
                con.close()
            except Exception:
                pass
            raise
    finally:
        _init_lock.release()

    pool["__global_share__"] = con
    return con


def close_all_connections() -> None:
    """Close every open share DB connection. Used by test fixtures."""
    with _all_connections_lock:
        for con in _all_connections:
            try:
                con.close()
            except Exception:
                pass
        _all_connections.clear()
    if hasattr(_local, "conns"):
        _local.conns.pop("__global_share__", None)


def reset_for_tests() -> None:
    """Drop the in-memory init cache so the next ``get_global_share_con`` rebuilds.

    Pytest fixtures that swap ``REMOTE_SHARE_DB_DIR`` per-test rely on this to
    avoid carrying over a connection bound to the previous test's path.
    """
    close_all_connections()
    _initialized.clear()
