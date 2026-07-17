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

Observability: as of the PR-9 pool consolidation, share_db queries are
opened through :class:`backend.utils.sqlite_profiler.InstrumentedConnection`
(the default factory for :class:`backend.core.sqlite_pool.ThreadLocalPool`).
Statements show up in ``/admin/queries`` tagged with
``service='__global_share__'``. Previously share_db ran on the bare
``sqlite3.Connection`` factory so its invite/session lookups and audit-log
writes were invisible to the Live Query Monitor.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import threading
import time

from backend.core.sqlite_pool import ThreadLocalPool

logger = logging.getLogger(__name__)

# ── Locations ────────────────────────────────────────────────────────────────

_DATA_DIR = "data/system"
_DB_FILENAME = "remote_share.db"

# Module-level state retained for symmetry with the per-service pools and
# to keep the door open for future ``monkeypatch.setattr`` use. The pool
# reads through providers (see ``_pool`` below) so any swap takes effect.
_local = threading.local()
_init_lock = threading.Lock()
_initialized: set[str] = set()
# Maps id(con) -> quarantine path for connections that were rebuilt after
# corruption. Read once by _init_db and removed. sqlite3.Connection has no
# __dict__ so we can't tag the connection object directly.
_recovery_marker: dict[int, str] = {}


def db_path(_key: str | None = None) -> str:
    """Absolute path to the global share DB file.

    Honors ``REMOTE_SHARE_DB_DIR`` for test isolation; defaults to
    ``data/system/remote_share.db``. The keyword argument is ignored —
    the pool passes the cache key (``"__global_share__"``) through, but
    every share_db connection points at the same singleton file.
    """
    base = os.environ.get("REMOTE_SHARE_DB_DIR") or _DATA_DIR
    return os.path.join(base, _DB_FILENAME)


def get_safe_share_db_connection(path: str) -> sqlite3.Connection:
    """Open a connection to ``path``. On open-time corruption, quarantine the
    file aside and rebuild from scratch.

    Mirrors TESTING_PLAN_3 Item 1: ONLY catches ``sqlite3.DatabaseError``
    raised during open (e.g., "file is not a database"). Query-time errors
    are not handled here.

    Returned connections go through :class:`InstrumentedConnection` so the
    share DB's statements show up in the Live Query Monitor — see the
    module docstring for the observability flip context.
    """
    # Local import: sqlite_profiler imports back through backend.core.
    from backend.utils.sqlite_profiler import InstrumentedConnection

    try:
        con = sqlite3.connect(
            path,
            timeout=30.0,
            factory=InstrumentedConnection,
            check_same_thread=False,
        )
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
        con = sqlite3.connect(
            path,
            timeout=30.0,
            factory=InstrumentedConnection,
            check_same_thread=False,
        )
        # Write a recovery marker once schema is initialized — caller does that
        # in _init_db. sqlite3.Connection has no __dict__, so we keep the
        # mapping out-of-band keyed by id(con).
        _recovery_marker[id(con)] = corrupt_path
        return con


def _share_db_on_borrow(con: sqlite3.Connection) -> sqlite3.Connection | None:
    """Re-assert ``PRAGMA foreign_keys=ON`` on every borrow.

    SQLite resets the FK pragma per-connection if any caller toggles it
    during the lifetime; the share_db FK-driven cascades (e.g.
    ``invite_services`` → ``remote_invites``) silently stop firing if
    we don't re-assert. Returning ``None`` on a closed connection tells
    the pool to evict the cache entry and reopen — preserves the
    pre-extraction self-heal behavior on closed handles.
    """
    try:
        con.execute("PRAGMA foreign_keys=ON")
    except sqlite3.ProgrammingError:
        return None
    return con


def _share_db_init(con: sqlite3.Connection) -> None:
    # Local import to break the connection <-> schema circular dependency.
    # schema._init_db only runs on first-open per path, so the cost is
    # bounded.
    from backend.core.share_db.schema import _init_db

    _init_db(con)


_module = sys.modules[__name__]
_pool = ThreadLocalPool(
    name="share_db",
    path_fn=db_path,
    schema_fn=_share_db_init,
    connect_fn=get_safe_share_db_connection,
    on_borrow_fn=_share_db_on_borrow,
    initialized_provider=lambda: _module._initialized,
    local_provider=lambda: _module._local,
)


def get_global_share_con() -> sqlite3.Connection:
    """Return a thread-local connection to the global share DB."""
    return _pool.get("__global_share__")


def close_all_connections() -> None:
    """Close every open share DB connection. Used by test fixtures."""
    _pool.close_all()


def reset_for_tests() -> None:
    """Drop the in-memory init cache so the next ``get_global_share_con`` rebuilds.

    Pytest fixtures that swap ``REMOTE_SHARE_DB_DIR`` per-test rely on this to
    avoid carrying over a connection bound to the previous test's path.
    """
    _pool.reset()
