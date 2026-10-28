"""PostgreSQL connection pooling, SQLite-to-Postgres dialect shim, and connection wrappers.

Postgres (via ``METADATA_DSN``) is the only connection backend: every
concern-specific module in ``backend.core.metadata`` imports ``get_con`` and
writes through it, landing on the shared Postgres pool here.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import sys
import threading
import weakref
from typing import Any

import psycopg
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

pg_connection: Any = sys.modules[__name__]

_pool: ConnectionPool | None = globals().get("_pool")


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

    def __iter__(self):
        # ``sqlite3.Row.__iter__`` yields VALUES in column order (so
        # ``tuple(row)`` / ``dict([row1, row2, ...])`` on a two-column
        # result set works the same way a plain tuple cursor would). A
        # plain ``dict`` subclass's default ``__iter__`` yields KEYS
        # instead — silently wrong here: it would make ``dict(cur.
        # fetchall())`` on a 2-column query build ``{col_name: col_name}``
        # from the column names themselves rather than ``{value0: value1}``
        # per row. Override so this row is a true sqlite3.Row drop-in.
        return iter(self._values)


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
        # Keep METADATA_PG_POOL_MAX * (backend + worker processes) below
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
            open=True,
        )
        setattr(sys.modules[__name__], "_pool", _pool)
    return _pool


def reset_pg_pool_for_tests() -> None:
    """Drop and close the cached pool so a test using a different (fake) DSN/pool
    doesn't reuse a stale one."""
    global _pool
    if _pool is not None:
        try:
            _pool.close(timeout=1.0)
        except Exception:
            logger.debug("reset_pg_pool_for_tests: _pool.close failed (ignored)", exc_info=True)
        _pool = None
        setattr(sys.modules[__name__], "_pool", None)


# ── Thread-local long-lived connection (metadata.base.get_con) ──────────────

_thread_local = globals().get("_thread_local") or threading.local()
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
_checked_out: weakref.WeakSet[PgConnectionWrapper] = globals().get("_checked_out") or weakref.WeakSet()
_checked_out_lock = threading.Lock()


def get_pg_thread_connection() -> PgConnectionWrapper:
    """One connection per thread, checked out once and held indefinitely —
    mirrors ``ThreadLocalPool``'s SQLite semantics so every existing
    ``get_con(service_id)`` call site (which never explicitly closes its
    connection) keeps working unchanged under a Postgres metadata backend.
    """
    wrapper = getattr(_thread_local, "wrapper", None)
    if wrapper is not None:
        if not wrapper._returned[0]:
            return wrapper
        # close_all_pg_connections() (called from another thread) already returned this
        # connection to the pool; the cached wrapper is stale. Drop it and check out a fresh one
        # instead of reusing a connection some other caller may now hold.
        del _thread_local.wrapper
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
    for wrapper in wrappers:
        # A connection with a statement in flight stays with its owner; returning it now would free
        # it under the running query. It remains tracked so a later drain can return it.
        if not wrapper._busy.acquire(blocking=False):
            continue
        try:
            with _checked_out_lock:
                _checked_out.discard(wrapper)
            _return_to_pool_once(wrapper._conn, wrapper._returned)
        finally:
            wrapper._busy.release()
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
    "ngwaf_bots": (
        "waf_req_id",
        [
            "bot_name",
            "category",
            "wellknown_bot_id",
            "wellknown_bot_name",
            "synced_at",
        ],
    ),
    "ngwaf_sync_state": (
        "workspace_id",
        [
            "last_timestamp_synced",
        ],
    ),
}

_IGNORE_TABLES = (
    "sources",
    "invite_services",
    "rdns",
    "ingested_files",
)


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

    sql = re.sub(
        r"datetime\('now',\s*'(.*?)'\)",
        r"to_char(current_timestamp AT TIME ZONE 'UTC' + INTERVAL '\1', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
        sql,
    )
    sql = re.sub(
        r"datetime\('now',\s*(%s|\$\d+|\?)\)",
        r"to_char(current_timestamp AT TIME ZONE 'UTC' + (\1)::interval, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
        sql,
    )
    sql = re.sub(
        r"\bdatetime\('now'\)",
        "to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
        sql,
    )
    sql = sql.replace(
        "strftime('%Y-%m-%dT%H:%M:%SZ', 'now')",
        "to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
    )

    sql = re.sub(r"\binstr\(", "strpos(", sql)
    sql = re.sub(r"\bsubstr\(", "substring(", sql)
    sql = re.sub(r",\s*rowid\s+(ASC|DESC)", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\browid\b", "id", sql, flags=re.IGNORECASE)

    # SQLite's ``LIKE ... COLLATE NOCASE`` (case-insensitive LIKE) has no
    # Postgres COLLATE equivalent — ``COLLATE "NOCASE"`` isn't a real
    # Postgres collation (``UndefinedObject``). Postgres's own
    # case-insensitive LIKE variant is the ``ILIKE`` operator; rewrite the
    # whole ``LIKE <param> COLLATE NOCASE`` shape to ``ILIKE <param>``.
    sql = re.sub(r"\bLIKE\s+(%s|\$\d+)\s+COLLATE\s+NOCASE\b", r"ILIKE \1", sql, flags=re.IGNORECASE)

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

    def __iter__(self):
        return iter(self._cursor)

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def lastrowid(self):
        return self._lastrowid


_return_once_lock = threading.Lock()


def _return_to_pool_once(conn: psycopg.Connection, returned: list[bool], pool=None) -> None:
    """Idempotent connection-return: whichever caller reaches this FIRST
    (an explicit release/close, or the GC finalizer below) wins; the other
    is a safe no-op. Required because a connection can legitimately be
    returned through either path for the same wrapper, and calling
    ``pool.putconn()`` twice on one connection would let two callers hold
    the same live connection concurrently — silent, hard-to-diagnose query
    interleaving corruption, not a raised error.
    """
    # Check-and-set under a lock: the explicit-release path, close_all_pg_connections() from another
    # thread and the GC finalizer can race, and an unlocked check lets two of them both putconn()
    # the same connection (double return -> use-after-return in libpq).
    with _return_once_lock:
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
        return_to_pool_on_close: bool = False,
        pool=None,
    ):
        self._conn = conn
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
        # Held while a statement is in flight. close_all_pg_connections() only returns a connection it
        # can take this for: handing a busy libpq connection back to the pool from another thread
        # segfaults the owner mid-query.
        self._busy = threading.RLock()

    def _get_cursor(self):
        try:
            return self._conn.cursor()
        except psycopg.OperationalError:
            pool = self._pool or get_pg_pool()
            if pool is not None:
                _return_to_pool_once(self._conn, self._returned, pool)
                self._conn = pool.getconn()
                self._returned = [False]
                self._pool = pool
                self._finalizer = weakref.finalize(self, _return_to_pool_once, self._conn, self._returned, pool)
                return self._conn.cursor()
            raise

    @contextlib.contextmanager
    def transaction(self):
        tx_fn = getattr(self._conn, "transaction", None)
        if callable(tx_fn):
            with self._busy, tx_fn():
                yield self
        else:
            with self._busy:
                yield self

    def execute(self, sql: str, params: Any = None):
        with self._busy:
            return self._execute(sql, params)

    def _execute(self, sql: str, params: Any = None):
        import time

        from backend.utils.telemetry import _live_deregister, _live_register, _record

        cur = self._get_cursor()
        rewritten_sql = _rewrite_sql(sql)
        rewritten_sql, id_col = _maybe_add_returning(rewritten_sql)

        qid = _live_register("Postgres", rewritten_sql, self)
        t0 = time.perf_counter()
        try:
            cur.execute(rewritten_sql, params if params is not None else ())
        except Exception as e:
            _live_deregister(qid, e)
            raise
        dur_ms = (time.perf_counter() - t0) * 1000.0
        _live_deregister(qid, None)
        _record(sql, params, dur_ms, cur.rowcount or 0, "execute")

        wrap = PgCursorWrapper(cur)
        if id_col is not None:
            res = cur.fetchone()
            if isinstance(res, tuple | list):
                wrap._lastrowid = res[0]
            elif res:
                wrap._lastrowid = res[id_col]
            else:
                wrap._lastrowid = None
        return wrap

    def executemany(self, sql: str, seq_of_params: list[Any]):
        with self._busy:
            return self._executemany(sql, seq_of_params)

    def _executemany(self, sql: str, seq_of_params: list[Any]):
        import time

        from backend.utils.telemetry import _live_deregister, _live_register, _record

        cur = self._get_cursor()
        rewritten_sql = _rewrite_sql(sql)
        qid = _live_register("Postgres", rewritten_sql, self)
        t0 = time.perf_counter()
        try:
            cur.executemany(rewritten_sql, seq_of_params)
        except Exception as e:
            _live_deregister(qid, e)
            raise
        dur_ms = (time.perf_counter() - t0) * 1000.0
        _live_deregister(qid, None)
        _record(sql, seq_of_params, dur_ms, cur.rowcount or 0, "executemany")
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


# ``_DATA_DIR`` is retained (rather than deleted outright) because
# ``backend.core.metadata.usage_log_db.db_path`` imports it directly from this
# module, and ``db_path()`` (below) is still called by
# ``alerts.list_alerts_cross_service``, ``reconciliation`` and
# ``repositories._base.attach_metadata_db``, which build the legacy per-service
# SQLite path for their own purposes. Nothing in THIS module's connection path
# (``get_con`` et al., all Postgres-only below) reads it.
_DATA_DIR = globals().get("_DATA_DIR", "data/services")

# Process-wide cache of {service_id: set[file_name]} for ingest dedup.
# ``get_ingested_filenames`` populates lazily on the first bounded read
# (cron hot path passes ``limit=200_000``); ``insert_ingested_files`` keeps
# it in sync. Unbounded reads (admin teardown / repair tools) bypass and
# invalidate the cache. Eliminates the ~640 ms SQL fetchall on every ~5 s
# sync tick for services with >1 M ingested_files.
_ingested_filenames_cache: dict[str | tuple[str, str], set[str]] = {}
_ingested_filenames_cache_lock = threading.Lock()


# Pre-compiled for the per-insert file_date parse. The canonical Fastly
# basename is `...<YYYY-MM-DD>T<HH:MM:SS>.<ms>-<rand>.log.gz`; locate the
# first 'T' and use the 10 chars before it when they look like a date.
# Matches the GLOB in _migration_002 / get_log_accounting_counts so legacy
# and runtime parsing agree.
_FILE_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})T")
_RUM_FILE_DATE_RE = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")


def _parse_file_date(file_name: str) -> str | None:
    """Return 'YYYY-MM-DD' parsed from filename or None if no match.

    Cheap regex on the basename — runs per-insert, called from the bulk
    INSERT in `insert_ingested_files`. Same semantics as the SQL backfill
    in `_migration_002_add_ingested_files_file_date`.
    """
    if not file_name:
        return None
    m = _FILE_DATE_RE.search(file_name)
    if m:
        return m.group(1)
    m_rum = _RUM_FILE_DATE_RE.search(file_name)
    if m_rum:
        return f"{m_rum.group(1)}-{m_rum.group(2)}-{m_rum.group(3)}"
    m_loose = re.search(r"(\d{4})-(\d{2})-(\d{2})", file_name)
    if m_loose:
        return f"{m_loose.group(1)}-{m_loose.group(2)}-{m_loose.group(3)}"
    return None


def _clear_ingested_filenames_cache(service_id: str | None = None) -> None:
    """Drop the dedup cache for one service or all services.

    Called from the pytest ``isolate_metadata_db`` fixture (every test gets a
    clean slate) and from ``teardown`` so deleted services don't keep
    phantom dedup state.
    """
    with _ingested_filenames_cache_lock:
        if service_id is None:
            _ingested_filenames_cache.clear()
        else:
            to_remove = [k for k in _ingested_filenames_cache if isinstance(k, tuple) and k[0] == service_id]
            for k in to_remove:
                _ingested_filenames_cache.pop(k, None)
            _ingested_filenames_cache.pop(service_id, None)


class _InvalidServiceIdErrorDef(ValueError):
    """Raised by ``_validate_service_id_or_raise`` when ``service_id`` fails
    format validation. Every ``get_con`` / ``get_con_readonly`` call
    validates through this, Postgres included — a malformed ``service_id``
    still must not reach a WHERE/INSERT predicate unvalidated.

    Fastly service IDs are 22-character lowercase alphanumeric strings, but
    legacy fixtures and Admin-provisioned identifiers also use hyphens and
    mixed case, so we accept the union (``[A-Za-z0-9_-]{1,64}``). Anything
    outside that — non-ASCII characters, path separators, null bytes — is
    rejected at this chokepoint so every caller is safe (historically this
    also guarded the per-service SQLite file path from traversal; that
    file no longer exists, but the same input can still reach ``db_path()``,
    used by a handful of SQLite-flavored callers that predate this
    migration). The shared FastAPI exception handler in ``backend.main``
    converts this into a 422 instead of a 500.
    """


InvalidServiceIdError = globals().get("InvalidServiceIdError", _InvalidServiceIdErrorDef)

# Anchored, length-bounded. Hyphens and underscores allowed for legacy
# fixtures (e.g. "test-service-id"). 1-64 chars covers Fastly's 22-char
# native IDs with headroom for Admin-assigned suffixes.
_SERVICE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _validate_service_id_or_raise(service_id: str) -> None:
    if not isinstance(service_id, str):
        raise TypeError(f"service_id must be a string, got {type(service_id).__name__}: {service_id!r}")
    if not _SERVICE_ID_RE.fullmatch(service_id):
        raise InvalidServiceIdError(f"service_id must match {_SERVICE_ID_RE.pattern!r}; got {service_id!r}")


# ── Connection management ─────────────────────────────────────────────────────


def db_path(service_id: str) -> str:
    """Absolute path to where the per-service metadata SQLite file USED to
    live, before this module moved onto Postgres.

    Retained — not used by anything in this module's own connection path —
    because a handful of callers that predate the Postgres migration still
    build this path for their own (still-SQLite-shaped) purposes:
    ``alerts.list_alerts_cross_service`` (ATTACHes each service's file to a
    scratch in-memory SQLite DB) and ``repositories._base.attach_metadata_db``
    (ATTACHes it into a DuckDB query). Those call sites are unowned by this
    task; removing this function out from under them would break both at
    import time or first call. A non-string ``service_id`` would silently
    produce a junk path containing the object's repr (e.g.
    ``<...0x...>.metadata.db``); reject at the boundary so the bad caller is
    pinpointed immediately. A malformed-string ``service_id`` raises
    :class:`InvalidServiceIdError` for the same reason — see that class's
    docstring for the threat model.
    """
    _validate_service_id_or_raise(service_id)
    return os.path.join(_DATA_DIR, f"{service_id}.metadata.db")


def get_con(service_id: str) -> PgConnectionWrapper:
    """Return this thread's shared Postgres connection, tagged for ``service_id``.

    ``service_id`` is validated but otherwise unused for connection
    selection — every service shares ONE connection per thread (checked out
    once via :func:`pg_connection.get_pg_thread_connection` and held for the
    life of the thread), since rows are scoped by the ``service_id`` column
    migration 015 added, not by which physical database they live in. See
    :mod:`backend.core.metadata.pg_connection` for the full rationale.
    """
    _validate_service_id_or_raise(service_id)
    wrapper = pg_connection.get_pg_thread_connection()
    # Retag for the Live Query Monitor on every call — see
    # PgConnectionWrapper.__init__'s docstring on why this is safe for
    # a connection shared across services on one thread.
    wrapper._service_id = service_id
    return wrapper


def get_con_readonly(service_id: str) -> PgConnectionWrapper:
    """Return a short-lived read-only connection for the given service.

    This connection is not pooled and should be closed immediately (callers
    use ``contextlib.closing(...)``). ``.close()`` returns the connection to
    the pool rather than severing a socket.
    """
    _validate_service_id_or_raise(service_id)
    wrapper = pg_connection.get_pg_readonly_connection()
    wrapper._service_id = service_id
    return wrapper


def release_thread_connection() -> None:
    """Return the CALLING thread's ``get_con()`` connection, if any, so a
    thread that will never be reused doesn't permanently pin pool capacity.

    Returns the connection to the bounded ``METADATA_PG_POOL_MAX``-sized
    pool (see :func:`pg_connection.release_pg_thread_connection` for the
    full rationale — this is what a per-cron-tick heartbeat thread must call
    before exiting).
    """
    pg_connection.release_pg_thread_connection()


def close_all_connections() -> None:
    """Close every connection opened by ``get_con`` in any thread.

    Used by the pytest fixture in tests/conftest.py to drain connections
    opened on FastAPI TestClient worker threads — the fixture only has
    access to its own thread's state and would otherwise leak those.
    """
    pg_connection.close_all_pg_connections()


def teardown(service_id: str) -> None:
    """Clear this service's dedup cache. No-op otherwise.

    Called from ``backend/provision.py`` during service teardown. There is
    no per-service file to delete under Postgres (rows for every service
    share one database), and row-level deletion for a torn-down service is
    a data-retention decision the provisioning teardown flow does not
    currently make — this matches the no-op behavior Postgres mode already
    had before SQLite was removed. See this task's report for why a
    destructive row-deletion semantic was NOT added here without an
    explicit product decision to do so.
    """
    _clear_ingested_filenames_cache(service_id)
