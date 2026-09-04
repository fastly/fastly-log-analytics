"""Postgres metadata backend: connection lifecycle + SQLite-dialect shim.

Activated by setting ``METADATA_DSN`` — the multi-writer requirement for
running more than one backend/worker pod (see ``docs/adr/`` and
``scripts/setup_pg_schema.py`` / ``scripts/migrate_metadata_to_pg.py`` for
the schema-creation and data-migration runbook). The tables whose queries
are service-scoped gained a ``service_id`` column (migration 015 —
``cron_runs`` and ``local_compacted_files``) so ALL services can share one
Postgres database: callers keep passing ``service_id`` and it flows into the
same ``WHERE``/``INSERT`` predicates that already scope every query, and
this module never partitions by service. That is NOT every table —
``committed_buffers`` has no ``service_id`` and
``filter_uncommitted_buffers`` applies no such predicate, so it is
cross-tenant by buffer basename under a shared database (safe only because
those basenames are uuid-derived). See ADR-18.

Two connection shapes, mirroring how callers already use SQLite:

- **Long-lived, one per thread** (:func:`get_pg_thread_connection`) — for
  ``metadata.base.get_con(service_id)`` callers, which hold onto the
  connection indefinitely and never call ``.close()``. Checked out once via
  ``getconn()`` and never returned until the process exits or a test fixture
  calls :func:`close_all_pg_connections`. Safe to share across services on
  one thread because the pool runs ``autocommit=True`` — no transaction
  outlives a single statement, so there is nothing for an unrelated
  service's query on the same thread to see half-committed.
- **Short-lived, checked out fresh** (:func:`get_pg_readonly_connection`) —
  for ``get_con_readonly()`` callers, which always do
  ``contextlib.closing(get_con_readonly(...))``. ``.close()`` on the
  returned wrapper returns the connection to the pool instead of severing
  the socket.

Trap fixed here: the previous version of this module handed
``get_pg_pool().connection()`` — a bare ``@contextmanager`` object, not a
connection — straight to ``PgConnectionWrapper``. Every attribute access
(``.cursor()`` etc.) would have raised ``AttributeError`` the first time
Postgres mode was actually exercised, and even entering it properly would
have leaked a pool slot on every call (the CM's ``finally: putconn()`` never
ran because nothing called ``__exit__``). Neither path was covered by a
test that runs against a real pool, which is how this shipped broken.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import weakref
from typing import Any

import psycopg
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

_pool: ConnectionPool | None = None


def is_postgres() -> bool:
    return bool(os.environ.get("METADATA_DSN"))


class _CompatRow(dict):
    """A row supporting BOTH ``row["col"]`` and ``row[0]``, like ``sqlite3.Row``.

    The point of this module is that the existing body of SQLite-shaped
    metadata SQL runs unchanged against Postgres. psycopg's plain
    ``dict_row`` broke half of that contract: every ``.fetchone()[0]`` /
    ``row[0]`` call site — and there are ~10 modules' worth, across cron
    jobs, the ingest ledger, reconciliation and quarantine — raises
    ``KeyError: 0``. The suite runs on SQLite, so none of it surfaced until
    METADATA_DSN was actually set; observed live as the log_ingest cron
    failing every tick with ``KeyError: 0`` from ``cron/jobs/commit.py``'s
    ``.fetchone()[0]``, which pinned the service to "degraded".

    ``sqlite3.Row`` allows both styles, so this mirrors it rather than
    asking every call site to change. Integer/slice keys index the original
    column order; anything else is a normal dict lookup.
    """

    __slots__ = ("_values",)

    def __init__(self, keys, values):
        super().__init__(zip(keys, values, strict=False))
        self._values = tuple(values)

    def __getitem__(self, key):
        if isinstance(key, int | slice):
            return self._values[key]
        return super().__getitem__(key)


def _compat_row_factory(cursor):
    """psycopg row factory producing :class:`_CompatRow`."""
    desc = cursor.description
    if desc is None:
        return lambda values: values
    keys = [d.name for d in desc]
    return lambda values: _CompatRow(keys, values)


def get_pg_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        dsn = os.environ.get("METADATA_DSN")
        if not dsn:
            raise RuntimeError("METADATA_DSN is not set")
        # autocommit=True: every statement commits immediately, matching the
        # "one write unit per execute()+commit() pair" shape every SQLite
        # call site already assumes, without needing a real transaction
        # manager here. See PgConnectionWrapper.commit()/rollback().
        # Sizing is load-bearing, not a tuning nicety. get_pg_thread_connection()
        # holds ONE connection per thread for the life of the thread (mirroring
        # SQLite's per-thread pool semantics) and never returns it, so the pool
        # must be at least as large as the number of threads that ever touch
        # metadata: FastAPI's anyio threadpool (40 by default) plus the
        # scheduler and RT-poller threads. psycopg_pool defaults to
        # min_size=4 / max_size=None, and None means "same as min_size" — so
        # the unsized pool capped at 4 and the 5th thread blocked for
        # timeout seconds and then raised PoolTimeout. Observed live the moment
        # metadata moved to Postgres: every metadata-touching route hung 30 s
        # and 503'd.
        #
        # The default MUST exceed the process's thread ceiling or the excess
        # threads block for `timeout` and then fail. Concretely, for the API
        # process that ceiling is FastAPI/anyio's default threadpool of 40,
        # plus the scheduler and RT-poller threads — measured at 36 live
        # threads against a pool of 32, which is exactly how PoolTimeout came
        # back after the first sizing pass. 64 clears 40 + slack.
        #
        # Keep METADATA_PG_POOL_MAX * (backend + worker + beat processes) below
        # the server's max_connections (192 of 300 at this default), and
        # remember DuckLake's own catalog connections share that budget.
        max_size = int(os.environ.get("METADATA_PG_POOL_MAX", "64"))
        min_size = min(int(os.environ.get("METADATA_PG_POOL_MIN", "2")), max_size)
        _pool = ConnectionPool(
            conninfo=dsn,
            kwargs={"row_factory": _compat_row_factory, "autocommit": True},
            min_size=min_size,
            max_size=max_size,
            timeout=float(os.environ.get("METADATA_PG_POOL_TIMEOUT", "30")),
        )
    return _pool


def reset_pg_pool_for_tests() -> None:
    """Drop the cached pool so a test using a different (fake) DSN/pool
    doesn't reuse a stale one. Does not touch checked-out connections —
    call :func:`close_all_pg_connections` first if any are outstanding."""
    global _pool
    _pool = None


# ── Thread-local long-lived connection (metadata.base.get_con) ──────────────

_thread_local = threading.local()
# WEAK references, deliberately — a plain list here would itself keep every
# wrapper alive forever, defeating PgConnectionWrapper's GC finalizer for
# any thread that dies without calling release_pg_thread_connection() (e.g.
# an anyio HTTP-threadpool worker anyio itself prunes after 10s idle: see
# the finalizer's docstring). A live incident found _checked_out holding 27
# wrappers while only 3 of their owning threads still existed — the
# finalizer was correctly implemented but could never fire because this
# list held a permanent strong reference regardless. A WeakSet drops an
# entry automatically the instant nothing else references the wrapper, so
# close_all_pg_connections() (test teardown) only ever sees genuinely-live
# wrappers, and dead ones are free to finalize immediately.
_checked_out: weakref.WeakSet[PgConnectionWrapper] = weakref.WeakSet()
_checked_out_lock = threading.Lock()


def get_pg_thread_connection() -> PgConnectionWrapper:
    """One connection per thread, checked out once and held indefinitely —
    mirrors ``ThreadLocalPool``'s SQLite semantics so every existing
    ``get_con(service_id)`` call site (which never explicitly closes its
    connection) keeps working unchanged under a Postgres metadata backend.
    """
    wrapper = getattr(_thread_local, "wrapper", None)
    if wrapper is not None:
        return wrapper
    pool = get_pg_pool()
    raw = pool.getconn()
    wrapper = PgConnectionWrapper(raw, pool=pool)
    _thread_local.wrapper = wrapper
    with _checked_out_lock:
        _checked_out.add(wrapper)
    return wrapper


def release_pg_thread_connection() -> None:
    """Return the CALLING thread's long-lived connection to the pool, if it
    has one, and clear it from thread-local storage.

    Unlike :func:`close_all_pg_connections` (which drains every thread's
    tracked connection — test-teardown only, never safe to call from a live
    thread that isn't the sole owner of the process), this touches only the
    current thread's own connection. Safe to call from a short-lived worker
    thread that used ``get_con()``/``get_pg_thread_connection()`` and is
    about to exit for good — e.g. a cron job's per-invocation heartbeat
    thread (see ``backend.cron.decorators``). A prompt, deterministic
    release: ``PgConnectionWrapper``'s GC finalizer is the backstop for
    threads that die WITHOUT calling this (anyio's HTTP threadpool prunes
    idle workers on its own schedule; nothing in application code observes
    that), but relying on GC timing alone means a slot sits pinned until
    the next collection happens to run. Call this explicitly wherever the
    thread's end-of-life is known. No-op if the calling thread has no
    connection.
    """
    wrapper = getattr(_thread_local, "wrapper", None)
    if wrapper is None:
        return
    del _thread_local.wrapper
    with _checked_out_lock:
        _checked_out.discard(wrapper)
    _return_to_pool_once(wrapper._conn, wrapper._returned)


def get_pg_readonly_connection() -> PgConnectionWrapper:
    """Fresh checkout for ``get_con_readonly()`` callers. The returned
    wrapper's ``.close()`` returns the connection to the pool (callers use
    ``contextlib.closing(...)``, never a bare ``.close()`` expecting the
    socket to die)."""
    pool = get_pg_pool()
    raw = pool.getconn()
    return PgConnectionWrapper(raw, pool=pool, return_to_pool_on_close=True)


def close_all_pg_connections() -> None:
    """Return every thread-checked-out long-lived connection to the pool.
    Test-fixture teardown counterpart to ``ThreadLocalPool.close_all()``.
    Safe to call from a different thread than the owner: idle psycopg
    connections (no query in flight) tolerate a cross-thread ``putconn``,
    unlike SQLite's ``check_same_thread`` handles.
    """
    with _checked_out_lock:
        wrappers = list(_checked_out)
        _checked_out.clear()
    for wrapper in wrappers:
        _return_to_pool_once(wrapper._conn, wrapper._returned)
    if hasattr(_thread_local, "wrapper"):
        del _thread_local.wrapper


# ── SQLite -> Postgres dialect shim ──────────────────────────────────────────

# Tables whose SQLite writer uses INSERT OR REPLACE / INSERT OR IGNORE.
# Each entry becomes ``INSERT ... ON CONFLICT(<key>) DO <action>``.
# key=None means "ON CONFLICT DO NOTHING" (no specific conflict target
# needed because the table's only constraint is what the INSERT already
# collides on) — used for the *_IGNORE_NO_KEY set below.
_REPLACE_ON_CONFLICT_UPDATE = {
    # table_name: (conflict_key_columns, [update_columns])
    # Conflict target must be the table's real UNIQUE — (service_id,
    # file_name). It was "id", which insert_quarantined_file never supplies
    # (it is autoincrement), so ON CONFLICT could never fire and a
    # re-quarantine of the same file INSERTed a duplicate under Postgres
    # while SQLite's OR REPLACE correctly replaced it.
    "quarantined_files": (
        "service_id, file_name",
        [
            "source_name",
            "fos_key",
            "error_key",
            "meta_key",
            "valid_rows",
            "corrupt_rows",
            "file_size_bytes",
            "corrupt_samples",
            "reason_counts",
            "error_size_bytes",
        ],
    ),
    "views": (
        "id",
        [
            "service_id",
            "name",
            "filters_json",
            "time_range_type",
            "start_time",
            "end_time",
            "page",
        ],
    ),
}

# Tables whose SQLite writer uses INSERT OR IGNORE with no update — becomes
# a plain ON CONFLICT DO NOTHING.
_IGNORE_TABLES = ("local_compacted_files", "committed_buffers", "sources")

_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _rewrite_insert_or(sql: str) -> str:
    """Rewrite ``INSERT OR REPLACE|IGNORE INTO <table>`` to the Postgres
    ``INSERT ... ON CONFLICT`` equivalent. Table-driven so a new SQLite
    writer only needs a dict entry here, not a new ad hoc string check."""
    m = re.match(r"\s*INSERT OR (REPLACE|IGNORE) INTO (\w+)", sql, re.IGNORECASE)
    if not m:
        return sql
    verb, table = m.group(1).upper(), m.group(2)
    stripped = re.sub(r"^\s*INSERT OR (REPLACE|IGNORE) INTO", "INSERT INTO", sql, count=1, flags=re.IGNORECASE)

    if verb == "IGNORE" and table in _IGNORE_TABLES:
        return stripped + " ON CONFLICT DO NOTHING"
    if verb == "REPLACE" and table in _REPLACE_ON_CONFLICT_UPDATE:
        key, cols = _REPLACE_ON_CONFLICT_UPDATE[table]
        set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols)
        return f"{stripped} ON CONFLICT({key}) DO UPDATE SET {set_clause}"

    # Unmapped table: fail loudly rather than silently dropping the
    # OR REPLACE/IGNORE semantics (a plain INSERT would then raise a unique
    # violation on the very next conflicting row, or worse, succeed and
    # duplicate). A new SQLite writer must add itself to the tables above.
    raise NotImplementedError(
        f"_rewrite_sql: no Postgres ON CONFLICT mapping for 'INSERT OR {verb} INTO {table}' — "
        "add it to _REPLACE_ON_CONFLICT_UPDATE or _IGNORE_TABLES in pg_connection.py"
    )


def _replace_placeholders_outside_literals(sql: str) -> str:
    """Replace SQLite's ``?`` positional placeholder with Postgres's
    ``%s``, skipping any ``?`` inside a single-quoted string literal (a
    blind ``str.replace`` would corrupt a literal question mark AND
    desynchronize every placeholder after it). SQL string literals escape
    an embedded quote as ``''``, which this scanner treats as
    quote-close-then-reopen — equivalent for the purpose of tracking
    in/out of literal state.
    """
    out = []
    in_string = False
    for ch in sql:
        if ch == "'":
            in_string = not in_string
            out.append(ch)
        elif ch == "?" and not in_string:
            out.append("%s")
        else:
            out.append(ch)
    return "".join(out)


def _rewrite_sql(sql: str) -> str:
    sql = _replace_placeholders_outside_literals(sql)
    sql = _rewrite_insert_or(sql)

    if "excluded." in sql and "ON CONFLICT" in sql:
        sql = sql.replace("excluded.", "EXCLUDED.")

    sql = sql.replace("datetime('now')", "current_timestamp AT TIME ZONE 'UTC'")
    sql = re.sub(r"datetime\('now',\s*'(.*?)'\)", r"current_timestamp AT TIME ZONE 'UTC' + INTERVAL '\1'", sql)
    sql = sql.replace(
        "strftime('%Y-%m-%dT%H:%M:%SZ', 'now')",
        "to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
    )

    sql = re.sub(r"\binstr\(", "strpos(", sql)
    sql = re.sub(r"\bsubstr\(", "substring(", sql)

    return sql


# INSERTs that need the generated id back, per table -> id column. Postgres
# has no sqlite3-style cursor.lastrowid; emulate it with RETURNING.
_RETURNING_ID_TABLES = {
    "cron_runs": "id",
}


def _maybe_add_returning(sql: str) -> tuple[str, str | None]:
    """Return (sql, id_column) — id_column is set when this INSERT needs a
    RETURNING clause added to emulate ``cursor.lastrowid``."""
    if "RETURNING" in sql.upper():
        return sql, None
    m = re.match(r"\s*INSERT INTO (\w+)", sql, re.IGNORECASE)
    if not m:
        return sql, None
    id_col = _RETURNING_ID_TABLES.get(m.group(1))
    if id_col is None:
        return sql, None
    return f"{sql} RETURNING {id_col}", id_col


class PgCursorWrapper:
    def __init__(self, cursor: psycopg.Cursor):
        self._cursor = cursor
        self._lastrowid: int | None = None

    def execute(self, sql: str, params: Any = None):
        sql = _rewrite_sql(sql)
        if params is None:
            self._cursor.execute(sql)
        else:
            self._cursor.execute(sql, params)
        return self

    def executemany(self, sql: str, seq_of_params: list[Any]):
        self._cursor.executemany(_rewrite_sql(sql), seq_of_params)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def lastrowid(self):
        return self._lastrowid


def _return_to_pool_once(conn: psycopg.Connection, returned: list[bool], pool=None) -> None:
    """Idempotent connection-return: whichever caller reaches this FIRST
    (an explicit release/close, or the GC finalizer below) wins; the other
    is a safe no-op. Required because a connection can legitimately be
    returned through either path for the same wrapper, and calling
    ``pool.putconn()`` twice on one connection would let two callers hold
    the same live connection concurrently — silent, hard-to-diagnose query
    interleaving corruption, not a raised error.
    """
    if returned[0]:
        return
    returned[0] = True
    try:
        (pool or get_pg_pool()).putconn(conn)
    except Exception as e:
        logger.warning("[pg_connection] failed to return connection to pool: %s", e)


class PgConnectionWrapper:
    def __init__(
        self,
        conn: psycopg.Connection,
        is_readonly: bool = False,
        return_to_pool_on_close: bool = False,
        pool=None,
    ):
        self._conn = conn
        self._is_readonly = is_readonly
        self._return_to_pool_on_close = return_to_pool_on_close
        # Mutable, not per-connection: under Postgres one connection serves
        # every service on a thread (see get_pg_thread_connection), so this
        # is stamped by metadata.base.get_con(service_id) on each call
        # rather than fixed at construction like SQLite's per-service
        # connection. Safe because usage on one thread is always sequential
        # — the attribute is read here immediately, before the next
        # get_con() call (possibly for a different service) can retag it.
        # Named to match sqlite_profiler._live_register's getattr lookup so
        # both backends feed the same Live Query Monitor code path.
        self._service_id: str | None = None
        # GC safety net for the long-lived thread-affinity connections
        # (get_pg_thread_connection): if this wrapper is ever garbage
        # collected without an explicit release, the pool would otherwise
        # think the connection is checked out forever. This is not a
        # theoretical concern — a live incident (2026-09-03) traced it to
        # TWO independent sources: a cron heartbeat thread that never
        # returned its connection, AND every ordinary sync HTTP route
        # handler, because FastAPI runs those on anyio's threadpool, which
        # PRUNES idle worker threads after 10s on its own schedule
        # (anyio._backends._asyncio.WorkerThread.MAX_IDLE_TIME) — a normal
        # part of anyio's lifecycle that no request-scoped
        # dependency/middleware cleanup can reliably catch, since a
        # request's dependencies and endpoint are not guaranteed to run on
        # the same worker thread, and threads can be recycled between
        # requests regardless of any single request's boundaries. A
        # `weakref.finalize` (not `__del__`, which can resurrect the
        # object and complicates subclassing) fires via refcounting the
        # instant the owning thread's `threading.local()` slot is cleared
        # — no reference cycle is involved, so CPython collects it
        # immediately, not on some later gc.collect() cycle.
        self._returned = [False]
        self._pool = pool
        self._finalizer = weakref.finalize(self, _return_to_pool_once, conn, self._returned, pool)

    def execute(self, sql: str, params: Any = None):
        from backend.utils.sqlite_profiler import _live_deregister, _live_register

        cur = self._conn.cursor()
        sql = _rewrite_sql(sql)
        sql, id_col = _maybe_add_returning(sql)

        qid = _live_register("Postgres", sql, self)
        try:
            cur.execute(sql, params if params is not None else ())
        except Exception as e:
            _live_deregister(qid, e)
            raise
        _live_deregister(qid, None)

        wrap = PgCursorWrapper(cur)
        if id_col is not None:
            res = cur.fetchone()
            wrap._lastrowid = res[0] if isinstance(res, tuple | list) else (res[id_col] if res else None)
        return wrap

    def executemany(self, sql: str, seq_of_params: list[Any]):
        from backend.utils.sqlite_profiler import _live_deregister, _live_register

        cur = self._conn.cursor()
        sql = _rewrite_sql(sql)
        qid = _live_register("Postgres", sql, self)
        try:
            cur.executemany(sql, seq_of_params)
        except Exception as e:
            _live_deregister(qid, e)
            raise
        _live_deregister(qid, None)
        return PgCursorWrapper(cur)

    def cursor(self):
        return PgCursorWrapper(self._conn.cursor())

    def commit(self):
        # autocommit=True: every statement already committed. A no-op here
        # (rather than raising) matches the SQLite call sites' expectation
        # that commit() is always safe to call.
        pass

    def rollback(self):
        pass

    def close(self):
        if self._return_to_pool_on_close:
            _return_to_pool_once(self._conn, self._returned, self._pool)
        # Long-lived thread connections are NOT actively returned here —
        # they live in _checked_out and are returned by
        # release_pg_thread_connection() / close_all_pg_connections(), or
        # by the GC finalizer as a last resort if neither ever runs.

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
