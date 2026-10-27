"""Shared connection management + schema for the per-service metadata store.

Postgres (via ``METADATA_DSN``) is the only connection backend: every
concern-specific module in ``backend.core.metadata`` (alerts, views,
ingest_log, cron_log, asn_cache, usage_log, reconciliation, state) imports
``get_con`` from here and writes through it, and every call lands on the
shared Postgres pool in :mod:`backend.core.metadata.pg_connection`. Schema
bootstrap for that pool goes through
:func:`backend.core.metadata.pg_schema.ensure_pg_schema`, wired at API/Celery
process startup — NOT through this module, which only holds ``_SCHEMA`` (the
schema-of-record, translated to Postgres DDL by ``pg_schema._to_postgres``)
and the dedup filename cache.

Carved out of the historical ``backend.core.metadata`` monolith — callers
should import from ``backend.core.metadata`` (or its concern-specific
submodules). The package's ``__init__`` installs a ``_ShimModule`` proxy that
mirrors writes to shared-state attributes here, so monkeypatch sites in tests
work without addressing the submodule directly.
"""

from __future__ import annotations

import logging
import os
import re
import threading

logger = logging.getLogger(__name__)

import contextlib
import sys
import weakref

import psycopg
from psycopg_pool import ConnectionPool

pg_connection: Any = sys.modules[__name__]
sys.modules["backend.core.metadata.pg_connection"] = sys.modules[__name__]
if "backend.core.metadata" in sys.modules:
    setattr(sys.modules["backend.core.metadata"], "pg_connection", sys.modules[__name__])
if "backend" in sys.modules and hasattr(sys.modules["backend"], "_MODULE_ALIASES"):
    sys.modules["backend"]._MODULE_ALIASES["backend.core.metadata.pg_connection"] = __name__

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
        )
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

        from backend.utils.sqlite_profiler import _live_deregister, _live_register, _record

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
            wrap._lastrowid = res[0] if isinstance(res, tuple | list) else (res[id_col] if res else None)
        return wrap

    def executemany(self, sql: str, seq_of_params: list[Any]):
        with self._busy:
            return self._executemany(sql, seq_of_params)

    def _executemany(self, sql: str, seq_of_params: list[Any]):
        import time

        from backend.utils.sqlite_profiler import _live_deregister, _live_register, _record

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


# ── Schema ────────────────────────────────────────────────────────────────────


_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS ingest_ledger (
        service_id TEXT NOT NULL,
        object_key TEXT NOT NULL,
        etag TEXT,
        size_bytes INTEGER,
        status TEXT NOT NULL CHECK (status IN ('discovered','claimed','committed','quarantined','dead_letter')),
        claimed_by TEXT,
        claimed_at REAL,
        lease_generation INTEGER NOT NULL DEFAULT 0,
        committed_at REAL,
        published_at REAL,
        snapshot_ref TEXT,
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        error_kind TEXT,
        next_attempt_at REAL,
        discovered_at REAL,
        originating_task TEXT,
        originating_run_id INTEGER,
        outcome_counters TEXT NOT NULL DEFAULT '{}',
        raw_deleted_at REAL,
        raw_delete_claim_token TEXT,
        raw_delete_claimed_at REAL,
        PRIMARY KEY (service_id, object_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_claimed_scan ON ingest_ledger(service_id, status, claimed_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_commit_scan "
    "ON ingest_ledger(service_id, status, committed_at, raw_deleted_at)",
    """
    CREATE TABLE IF NOT EXISTS job_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service_id TEXT,
        job_name TEXT NOT NULL,
        started_at REAL NOT NULL,
        heartbeat_at REAL NOT NULL,
        lease_ttl_s INTEGER NOT NULL,
        finished_at REAL,
        status TEXT NOT NULL,
        detail TEXT
    )
    """,
    # Makes lease acquisition atomic under CONCURRENT writers (multiple
    # pods against one Postgres metadata backend): at most one 'running'
    # row can exist per (service_id, job_name) at a time. start_cron_run's
    # INSERT targets this index with ON CONFLICT ... DO NOTHING and reads
    # back rowcount to learn whether it won the lease — a single atomic
    # statement, unlike the old check-then-insert (SELECT count, then
    # INSERT), which raced under autocommit (see cron_log.py).
    """CREATE UNIQUE INDEX IF NOT EXISTS idx_job_runs_running_lease
        ON job_runs(service_id, job_name) WHERE status = 'running'""",
    """CREATE TABLE IF NOT EXISTS sources (
        name TEXT PRIMARY KEY,
        config TEXT,
        table_name TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS ingested_files (
        file_name TEXT,
        source_name TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),
        row_count INTEGER,
        file_size_bytes INTEGER,
        error_count INTEGER DEFAULT 0,
        file_date DATE,
        table_name TEXT NOT NULL DEFAULT 'logs',
        PRIMARY KEY (file_name, source_name, table_name)
    )""",
    # Covers `/usage/prefill`'s source+range narrowing
    # (`WHERE source_name = ? AND ingested_at BETWEEN ? AND ?`) and the
    # bounded `list_unbackfilled_fastly_edge_files` scan (see :1128). The
    # previous `idx_ingested_files_source` indexed source_name alone — SQLite
    # had to walk every row for the matching source and filter ingested_at
    # in memory (~250ms per query on populated services). The composite
    # satisfies the range scan directly and is a strict superset for
    # source_name-only lookups (SQLite uses leading-column prefixes), so the
    # old index is redundant and dropped here. Index name matches the
    # by-name reference in `list_unbackfilled_fastly_edge_files`'s docstring.
    "CREATE INDEX IF NOT EXISTS idx_ingested_files_source_ingested_at ON ingested_files(source_name, ingested_at)",
    # Note: idx_ingested_files_source_date (companion index for per-day
    # usage queries) is created by _migration_002_add_ingested_files_file_date,
    # not here — _SCHEMA runs before migrations and a legacy DB upgrading
    # would fail on this CREATE INDEX (the file_date column doesn't exist
    # yet at that point). The migration is idempotent + runs for fresh DBs
    # too (apply_pending walks v1..LATEST on every init), so the index
    # always lands without _SCHEMA carrying it.
    "DROP INDEX IF EXISTS idx_ingested_files_source",
    # Earlier in this branch a redundant `idx_ingested_files_source_ts` was
    # added under a different name before discovering the existing
    # by-name reference above; clean it up so no service ends up with two
    # functionally identical composites.
    "DROP INDEX IF EXISTS idx_ingested_files_source_ts",
    # Single-row-per-service rollup maintained by ``insert_ingested_files``.
    # Without it, ``get_ingested_files_status_summary`` had to SUM(row_count)
    # + SUM(file_size_bytes) across the whole table on every cron tick —
    # ~4 s on services with >1 M rows since SQLite couldn't satisfy the SUMs
    # from any existing index. Lazy-bootstrapped from the full scan on first
    # read after upgrade; transactional delta updates after that.
    """CREATE TABLE IF NOT EXISTS ingested_files_summary (
        source_name TEXT PRIMARY KEY,
        file_count INTEGER NOT NULL DEFAULT 0,
        total_rows INTEGER NOT NULL DEFAULT 0,
        total_bytes INTEGER NOT NULL DEFAULT 0,
        count_with_bytes INTEGER NOT NULL DEFAULT 0,
        latest_file_name TEXT,
        last_ingested TEXT
    )""",
    # Atomic ingest manifest. A row is written BEFORE the buffer Parquet
    # appears on disk and deleted AFTER ingested_files is updated. On startup
    # the ingest loop sweeps this table: if the buffer file exists the row is
    # promoted (commit ingested_files, drop the in_flight row); if it is
    # missing the row is dropped without touching ingested_files (the buffer
    # write itself crashed — files will re-LIST on the next tick). Combined
    # with deterministic buffer filenames (sha256 of sorted source filenames)
    # this makes the ingest → buffer → metadata commit sequence crash-safe
    # without ever double-committing a row to DuckLake.
    """CREATE TABLE IF NOT EXISTS ingest_in_flight (
        buffer_filename TEXT NOT NULL,
        source_name TEXT NOT NULL,
        files_json TEXT NOT NULL,
        started_at TEXT DEFAULT (datetime('now')),
        table_name TEXT NOT NULL DEFAULT 'logs',
        PRIMARY KEY (buffer_filename, table_name)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_in_flight_source ON ingest_in_flight(source_name)",
    """CREATE TABLE IF NOT EXISTS cron_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service_id TEXT,
        task TEXT NOT NULL,
        started_at TEXT NOT NULL,
        duration_s REAL,
        status TEXT,
        error_message TEXT,
        files_downloaded INTEGER DEFAULT 0,
        files_deleted_fos INTEGER DEFAULT 0,
        rows_ingested INTEGER DEFAULT 0,
        corrupt_rows INTEGER DEFAULT 0,
        parquet_files_created INTEGER DEFAULT 0,
        parquet_files_optimized INTEGER DEFAULT 0,
        parquet_keys TEXT DEFAULT '[]',
        outcome_counters TEXT NOT NULL DEFAULT '{}',
        summary TEXT,
        log_output TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_cron_task_started ON cron_runs(task, started_at)",
    # Covers the every-5s running-check hot path in start_cron_run
    # (`WHERE task = ? AND status = 'running'`).
    "CREATE INDEX IF NOT EXISTS idx_cron_task_status ON cron_runs(task, status, started_at)",
    # Covers `/logs`'s unfiltered pagination
    # (`ORDER BY started_at DESC LIMIT ? OFFSET ?` with no `WHERE task`) and
    # `main.py`'s sync-status probe (`WHERE task='sync' AND status != 'running'
    # ORDER BY started_at DESC LIMIT 1`). Without it, SQLite falls back to a
    # TEMP B-TREE sort over the full table because `idx_cron_task_started`
    # requires a leading-`task` predicate to satisfy the ORDER BY.
    "CREATE INDEX IF NOT EXISTS idx_cron_started ON cron_runs(started_at DESC)",
    # Covers status polls and startup reap_running_cron_runs without a task filter
    "CREATE INDEX IF NOT EXISTS idx_cron_status ON cron_runs(status)",
    # Covers per-service task lookups and loose index scans
    "CREATE INDEX IF NOT EXISTS idx_cron_runs_service_task_started ON cron_runs(service_id, task, started_at DESC, id DESC)",
    # Covers per-service started_at pagination and probes
    "CREATE INDEX IF NOT EXISTS idx_cron_runs_service_started ON cron_runs(service_id, started_at DESC, id DESC)",
    """CREATE TABLE IF NOT EXISTS asn_names (
        asn INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        fetched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )""",
    """CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL DEFAULT (datetime('now')),
        source_name TEXT,
        event_type TEXT NOT NULL,
        details TEXT,
        actor TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_audit_source ON audit_logs(source_name)",
    """CREATE TABLE IF NOT EXISTS views (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        name TEXT NOT NULL,
        filters_json TEXT NOT NULL,
        time_range_type TEXT,
        start_time TEXT,
        end_time TEXT,
        page TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS alerts (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        name TEXT NOT NULL,
        category TEXT DEFAULT 'reliability',
        metric TEXT NOT NULL,
        evaluation_type TEXT DEFAULT 'absolute',
        evaluation_scope TEXT DEFAULT 'all',
        operator TEXT NOT NULL,
        threshold REAL NOT NULL,
        window_min REAL NOT NULL,
        comparison_period_min REAL,
        status_codes TEXT,
        webhook_url TEXT,
        channels_json TEXT DEFAULT '[]',
        zscore_threshold REAL DEFAULT 3.0,
        baseline_period_days INTEGER DEFAULT 7,
        enabled INTEGER DEFAULT 1,
        last_triggered_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""",
    # Admin-flagged sessions for the edge session-scoring system. Each row
    # is one (service, sid) tuple labeled good/bad/neutral by the admin.
    # Feeds backend.scoring.evaluate.evaluate() for matrix ROC-AUC; the
    # neutral label is captured for UI completeness but excluded from the
    # AUC computation (intentionally uncertain).
    """CREATE TABLE IF NOT EXISTS scoring_labels (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        sid TEXT NOT NULL,
        label TEXT NOT NULL CHECK (label IN ('good', 'bad', 'neutral')),
        notes TEXT DEFAULT '',
        flagged_by TEXT,
        sample_ip TEXT,
        sample_ua TEXT,
        sample_url TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_scoring_labels_svc_sid ON scoring_labels(service_id, sid)",
    "CREATE INDEX IF NOT EXISTS idx_scoring_labels_svc_label ON scoring_labels(service_id, label)",
    # Operator audit log specifically for scoring-config mutations.
    # Separate from audit_logs (which gets state_sync'd) because scoring-
    # audit is per-host operator-attribution data that should NOT mirror
    # to read_only analyst replicas.
    """CREATE TABLE IF NOT EXISTS scoring_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL DEFAULT (datetime('now')),
        service_id TEXT NOT NULL,
        action TEXT NOT NULL,
        actor TEXT NOT NULL,
        details TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_scoring_audit_svc_ts ON scoring_audit(service_id, timestamp DESC)",
    # Plain timestamp index for the list_scoring_audit ORDER BY timestamp DESC
    # path when the service_id predicate is already satisfied — keeps the sort
    # itself indexed instead of falling back to a TEMP B-TREE on large audit
    # tables.
    "CREATE INDEX IF NOT EXISTS idx_scoring_audit_ts ON scoring_audit(timestamp DESC)",
    # service_id — not source_name — is the tenant discriminator, matching
    # every other table under a shared Postgres metadata database (ADR-15).
    # source_name is retained as informational only: readers used to filter
    # on it while binding service_id, which worked solely because the two
    # coincide today (src["name"] is the service id). See migration 020.
    """CREATE TABLE IF NOT EXISTS quarantined_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service_id TEXT,
        file_name TEXT NOT NULL,
        source_name TEXT NOT NULL,
        fos_key TEXT NOT NULL,
        error_key TEXT NOT NULL,
        meta_key TEXT NOT NULL,
        valid_rows INTEGER NOT NULL DEFAULT 0,
        corrupt_rows INTEGER NOT NULL DEFAULT 0,
        file_size_bytes INTEGER,
        error_size_bytes INTEGER,
        corrupt_samples TEXT,
        quarantined_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(service_id, file_name)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_quarantined_at ON quarantined_files(service_id, quarantined_at)",
    """CREATE TABLE IF NOT EXISTS quarantine_evidence (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service_id TEXT NOT NULL,
        source_type TEXT NOT NULL,
        original_key TEXT NOT NULL,
        line_ordinal INTEGER,
        byte_offset INTEGER,
        byte_length INTEGER NOT NULL,
        error_category TEXT NOT NULL,
        error_text TEXT NOT NULL,
        sha256 TEXT NOT NULL,
        file_path TEXT NOT NULL,
        quarantined_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",
    "CREATE INDEX IF NOT EXISTS idx_quarantine_evidence_fifo ON quarantine_evidence(service_id, quarantined_at, id)",
]

# No ``_init_schema`` here anymore: nothing calls ``get_con`` against a raw
# SQLite connection needing bootstrap, so the old SQLite-pool ``schema_fn``
# has no caller left. Postgres schema bootstrap goes through
# ``backend.core.metadata.pg_schema.ensure_pg_schema()`` (wired at API/Celery
# startup — see that module's docstring), which imports ``_SCHEMA`` above and
# runs it through ``pg_schema._to_postgres()``. ``_SCHEMA`` itself MUST stay
# SQLite-flavored and MUST NOT be deleted — it is the schema-of-record for
# both backends, not a SQLite-only artifact.


# ── Postgres schema DDL & boot-time ensure (folded from pg_schema.py) ─────────

import sys as _sys  # noqa: E402
import time as _t  # noqa: E402
from collections.abc import Callable  # noqa: E402
from typing import Any  # noqa: E402

from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry  # noqa: E402

CLICKHOUSE_CONTROL_DDL = (
    """
CREATE TABLE IF NOT EXISTS clickhouse_datasets (
    service_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    catalog_identity TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_snapshot BIGINT NOT NULL CHECK (source_snapshot >= 0),
    coverage_start TIMESTAMPTZ NOT NULL,
    coverage_end TIMESTAMPTZ NOT NULL CHECK (coverage_end >= coverage_start),
    expires_at TIMESTAMPTZ NOT NULL,
    expected_rows BIGINT NOT NULL CHECK (expected_rows BETWEEN 0 AND 1000000),
    canonical_digest TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (service_id, dataset_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_artifacts (
    service_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    artifact_uri TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    canonical_digest TEXT NOT NULL,
    byte_size BIGINT NOT NULL CHECK (byte_size BETWEEN 1 AND 16777216),
    ordinal_start BIGINT NOT NULL CHECK (ordinal_start >= 0),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 10000),
    PRIMARY KEY (service_id, dataset_id, batch_id),
    UNIQUE (service_id, dataset_id, ordinal_start),
    FOREIGN KEY (service_id, dataset_id) REFERENCES clickhouse_datasets(service_id, dataset_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_generations (
    service_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    target_identity TEXT NOT NULL,
    selection_revision BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'building' CHECK (status IN ('building', 'active', 'retired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    activated_at TIMESTAMPTZ,
    PRIMARY KEY (service_id, generation),
    UNIQUE (service_id, generation, dataset_id),
    FOREIGN KEY (service_id, dataset_id) REFERENCES clickhouse_datasets(service_id, dataset_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_publications (
    service_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'claimed', 'published', 'failed')),
    lease_fence BIGINT NOT NULL DEFAULT 0,
    lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    first_error TEXT,
    last_error TEXT,
    published_at TIMESTAMPTZ,
    verified_digest TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (service_id, generation, batch_id),
    FOREIGN KEY (service_id, generation, dataset_id)
        REFERENCES clickhouse_generations(service_id, generation, dataset_id),
    FOREIGN KEY (service_id, dataset_id, batch_id)
        REFERENCES clickhouse_artifacts(service_id, dataset_id, batch_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_service_selection (
    service_id TEXT PRIMARY KEY,
    generation TEXT,
    revision BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (service_id, generation) REFERENCES clickhouse_generations(service_id, generation)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_schema_state (
    target_identity TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
)
""",
)
CLICKHOUSE_TABLES_DDL = CLICKHOUSE_CONTROL_DDL

_SLOW_QUERIES_DDL = """
CREATE TABLE IF NOT EXISTS slow_queries (
    id SERIAL PRIMARY KEY,
    query_id INTEGER NOT NULL,
    db_type TEXT NOT NULL,
    service_id TEXT,
    started_at_utc DOUBLE PRECISION NOT NULL,
    ended_at_utc DOUBLE PRECISION NOT NULL,
    duration_ms DOUBLE PRECISION NOT NULL,
    outcome TEXT NOT NULL,
    sql_preview TEXT NOT NULL,
    sql_full TEXT,
    sql_len INTEGER NOT NULL,
    attr_kind TEXT NOT NULL,
    attr_label TEXT NOT NULL,
    attr_principal_id TEXT,
    attr_caller_qualname TEXT NOT NULL,
    attr_caller_file TEXT NOT NULL,
    attr_request_path TEXT,
    attr_request_id TEXT,
    attr_cron_job TEXT,
    attr_cron_run_id TEXT,
    attr_pool_slot TEXT,
    error_type TEXT,
    error_message TEXT,
    peak_memory_mb DOUBLE PRECISION
)
"""

_SLOW_QUERIES_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_slow_queries_service_started_dur ON slow_queries (service_id, started_at_utc, duration_ms)",
    "CREATE INDEX IF NOT EXISTS idx_slow_queries_service_duration ON slow_queries (service_id, duration_ms DESC, started_at_utc DESC)",
    "CREATE INDEX IF NOT EXISTS idx_slow_queries_started_at ON slow_queries (started_at_utc DESC)",
)


_METRIC_SNAPSHOTS_DDL = """
CREATE TABLE IF NOT EXISTS metric_snapshots (
    ts          TEXT NOT NULL,
    metric      TEXT NOT NULL,
    service_id  TEXT NOT NULL DEFAULT '',
    task        TEXT NOT NULL DEFAULT '',
    value       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (metric, service_id, task, ts)
)
"""

_METRIC_SNAPSHOTS_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_metric_lookup ON metric_snapshots (metric, ts)",
    "CREATE INDEX IF NOT EXISTS idx_metric_snapshots_ts ON metric_snapshots (ts)",
)

_SEED_INITIAL_TOS_DDL = """
INSERT INTO share_tos_versions (version, text, published_at)
VALUES (
    'v1',
    'I acknowledge that I am viewing third-party operational log data, that my access is logged, and that I will not retain, redistribute, or use this data outside the scope of my engagement.',
    to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
)
ON CONFLICT (version) DO NOTHING
"""

_NGWAF_BOTS_DDL = """
CREATE TABLE IF NOT EXISTS ngwaf_bots (
    waf_req_id         TEXT PRIMARY KEY,
    bot_name           TEXT,
    category           TEXT,
    wellknown_bot_id   TEXT,
    wellknown_bot_name TEXT,
    synced_at          TEXT
)
"""

_NGWAF_SYNC_STATE_DDL = """
CREATE TABLE IF NOT EXISTS ngwaf_sync_state (
    workspace_id          TEXT PRIMARY KEY,
    last_timestamp_synced TEXT
)
"""

_RDNS_DDL = """
CREATE TABLE IF NOT EXISTS rdns (
    ip              TEXT PRIMARY KEY,
    hostname        TEXT,
    status          TEXT,
    fcrdns_verified INTEGER DEFAULT 0,
    looked_up_at    TEXT
)
"""

_RDNS_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_rdns_status_looked_up_at ON rdns (status, looked_up_at)
"""

_INGEST_LEDGER_ALTERS = (
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS lease_generation BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS published_at DOUBLE PRECISION",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS error_kind TEXT",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS next_attempt_at DOUBLE PRECISION",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS originating_task TEXT",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS originating_run_id BIGINT",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS outcome_counters TEXT NOT NULL DEFAULT '{}'",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS raw_delete_claim_token TEXT",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS raw_delete_claimed_at DOUBLE PRECISION",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_claimed_scan ON ingest_ledger(service_id, status, claimed_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_commit_scan "
    "ON ingest_ledger(service_id, status, committed_at, raw_deleted_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_raw_delete_scan "
    "ON ingest_ledger(service_id, status, published_at, committed_at, raw_deleted_at, raw_delete_claimed_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_retry_scan ON ingest_ledger(service_id, status, next_attempt_at)",
)
_INGEST_LEDGER_INDEX_NAMES = (
    "idx_ingest_ledger_claimed_scan",
    "idx_ingest_ledger_commit_scan",
    "idx_ingest_ledger_raw_delete_scan",
    "idx_ingest_ledger_retry_scan",
)

_QUARANTINED_FILES_ALTERS = ("ALTER TABLE quarantined_files ADD COLUMN IF NOT EXISTS reason_counts TEXT DEFAULT '{}'",)
_CRON_RUNS_ALTERS = ("ALTER TABLE cron_runs ADD COLUMN IF NOT EXISTS outcome_counters TEXT NOT NULL DEFAULT '{}'",)

_USAGE_LOG_WIDEN_ALTERS = (
    "ALTER TABLE usage_log ALTER COLUMN bytes TYPE BIGINT",
    "ALTER TABLE usage_log ALTER COLUMN count TYPE BIGINT",
    "ALTER TABLE usage_log_hourly_summary ALTER COLUMN bytes TYPE BIGINT",
    "ALTER TABLE usage_log_hourly_summary ALTER COLUMN count TYPE BIGINT",
)

_RACE_SQLSTATES = frozenset({"42P07", "42710", "23505"})

_ensured = False
_ensure_lock = threading.Lock()


def _to_postgres(sql: str) -> str:
    """Rewrite a SQLite DDL statement into Postgres dialect."""
    pg_sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    pg_sql = pg_sql.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY")
    pg_sql = pg_sql.replace("SERIAL PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    pg_sql = re.sub(r"\bREAL\b", "DOUBLE PRECISION", pg_sql)
    pg_sql = pg_sql.replace(
        "DEFAULT (datetime('now'))",
        "DEFAULT (to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'))",
    )
    pg_sql = pg_sql.replace(
        "DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
        "DEFAULT (to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'))",
    )
    pg_sql = pg_sql.replace(", STRICT", "")
    pg_sql = pg_sql.replace(" STRICT", "")
    return pg_sql


def pg_schema_statements() -> list[str]:
    """Every DDL statement the Postgres metadata store needs, in order."""
    from backend.core.metadata.usage_log import _SCHEMA as _USAGE_SCHEMA
    from backend.core.share_db import _SCHEMA as _SHARE_DB_SCHEMA
    from backend.high_scale.postgres_control import SCHEMA_DDL as HIGH_SCALE_CONTROL_DDL

    statements = [_to_postgres(sql) for sql in _SCHEMA + _USAGE_SCHEMA + _SHARE_DB_SCHEMA]
    statements = [
        statement
        for statement in statements
        if not any(f"CREATE INDEX IF NOT EXISTS {name}" in statement for name in _INGEST_LEDGER_INDEX_NAMES)
    ]
    statements.append(_SLOW_QUERIES_DDL)
    statements.extend(_SLOW_QUERIES_INDEX_DDL)
    statements.append(_METRIC_SNAPSHOTS_DDL)
    statements.extend(_METRIC_SNAPSHOTS_INDEX_DDL)
    statements.extend(_INGEST_LEDGER_ALTERS)
    statements.extend(_QUARANTINED_FILES_ALTERS)
    statements.extend(_CRON_RUNS_ALTERS)
    statements.extend(_USAGE_LOG_WIDEN_ALTERS)
    statements.extend(CLICKHOUSE_CONTROL_DDL)
    statements.append(HIGH_SCALE_CONTROL_DDL)
    statements.append(_SEED_INITIAL_TOS_DDL)
    statements.append(_NGWAF_BOTS_DDL)
    statements.append(_NGWAF_SYNC_STATE_DDL)
    statements.append(_RDNS_DDL)
    statements.append(_RDNS_INDEX_DDL)
    return statements


def _is_concurrent_create(exc: Exception) -> bool:
    return getattr(exc, "sqlstate", None) in _RACE_SQLSTATES


def apply_pg_schema(cur: Any) -> tuple[int, int]:
    """Apply the schema through ``cur``. Returns ``(applied, raced)``."""
    applied = 0
    raced = 0
    for pg_sql in pg_schema_statements():
        try:
            cur.execute(pg_sql)
            applied += 1
        except Exception as e:
            if _is_concurrent_create(e):
                raced += 1
                continue
            raise RuntimeError(
                f"failed to apply schema statement:\n{pg_sql.strip()}\n-> {type(e).__name__}: {e}"
            ) from e
    return applied, raced


def ensure_pg_schema(*, force: bool = False) -> bool:
    """Idempotently create the Postgres metadata schema. Once per process."""
    global _ensured

    if not pg_connection.is_postgres():
        return False
    if _ensured and not force:
        return False

    with _ensure_lock:
        if _ensured and not force:
            return False
        try:
            pool = pg_connection.get_pg_pool()
            with pool.connection() as conn, conn.cursor() as cur:
                applied, raced = apply_pg_schema(cur)
        except Exception as e:
            logger.critical(
                "[metadata] Postgres schema bootstrap FAILED — metadata queries will fail until this is "
                "resolved. Fix the database and restart, or run `uv run python scripts/setup_pg_schema.py`. "
                "Cause: %s",
                e,
            )
            return False
        _ensured = True

    logger.info(
        "[metadata] Postgres metadata schema ensured (%d statements applied, %d already created by another pod).",
        applied,
        raced,
    )
    return True


def reset_ensure_flag_for_tests() -> None:
    """Drop the once-per-process latch so a test can exercise the boot path."""
    global _ensured
    _ensured = False


# ── Metadata storage stats + age-based cleanup (folded from reconciliation.py) ─

DEFAULT_METADATA_RETENTION = {
    "usage_log_days": 1,
    "ingested_files_days": 1,
    "cron_runs_days": 7,
    "rollups_days": 90,
    "slow_queries_days": 7,
}


class _UsageLogDbProxy:
    def __getattr__(self, name: str) -> Any:
        from backend.core.metadata import usage_log as _ul

        return getattr(_ul, name)


_usage_log_db: Any = _UsageLogDbProxy()


def recompute_ingested_files_summary(con: Any, service_id: str) -> None:
    from backend.core.metadata import ingest_log as _il

    _il.recompute_ingested_files_summary(con, service_id)


_STATS_TABLES = (
    ("usage_log", "usage_log"),
    ("ingested_files", "ingested_files"),
    ("cron_runs", "cron_runs"),
    ("alerts", "alerts"),
    ("views", "saved_views"),
    ("audit_logs", "audit_log"),
    ("ingest_in_flight", "in_flight_buffers"),
    ("slow_queries", "slow_queries"),
    ("quarantined_files", "quarantined_files"),
)

_CLEANUP_TABLES = (
    ("usage_log", "usage_log_days", "timestamp"),
    ("ingested_files", "ingested_files_days", "ingested_at"),
    ("cron_runs", "cron_runs_days", "started_at"),
    ("slow_queries", "slow_queries_days", "started_at_utc"),
    ("quarantined_files", "quarantine_days", "quarantined_at"),
)
_SLOW_QUERIES_TABLE = "slow_queries"
_USAGE_LOG_TABLE = "usage_log"


def _retention_timestamp_expr(ts_col: str) -> str:
    """Return the timestamp expression used by a Postgres retention predicate."""
    return f"CAST({ts_col} AS TIMESTAMPTZ)"


_STATS_SOURCE_NAME_TABLES = frozenset({"ingested_files", "audit_logs", "ingest_in_flight"})
_STATS_SERVICE_ID_TABLES = frozenset({"cron_runs", "alerts", "views", "slow_queries", "quarantined_files"})


def _stats_table_count_query(sql_table: str, service_id: str) -> tuple[str, tuple[Any, ...]]:
    """Return a tenant-scoped row-count query and params for ``sql_table``."""
    if sql_table == "ingested_files":
        return (
            f"SELECT count(*) FROM {sql_table} WHERE (source_name = ? OR source_name = 'fos' OR source_name = '')",
            (service_id,),
        )
    if sql_table in _STATS_SOURCE_NAME_TABLES:
        return (
            f"SELECT count(*) FROM {sql_table} WHERE (source_name = ? OR source_name = '')",
            (service_id,),
        )
    if sql_table in _STATS_SERVICE_ID_TABLES:
        return (
            f"SELECT count(*) FROM {sql_table} WHERE (service_id = ? OR service_id = '' OR service_id IS NULL)",
            (service_id,),
        )
    return f"SELECT count(*) FROM {sql_table}", ()


def _tenant_where_clause(table: str, service_id: str) -> tuple[str, tuple[Any, ...]]:
    """Return a SQL predicate and params scoping a cleanup table to ``service_id``."""
    if table == "ingested_files":
        return "(source_name = ? OR source_name IN ('fos', 'test_service') OR source_name IS NULL)", (service_id,)
    return "(service_id = ? OR service_id IS NULL)", (service_id,)


def _open_usage_log(service_id: str) -> Any | None:
    """Open the per-service usage_log connection if available; else None."""
    try:
        return _usage_log_db.open_readonly(service_id)
    except Exception:
        return None


_storage_stats_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=60.0)
CacheRegistry.register("metadata.reconciliation._storage_stats_cache", _storage_stats_cache)


def invalidate_storage_stats_cache(service_id: str) -> None:
    """Invalidate the storage stats cache for the given service."""
    _storage_stats_cache.pop(service_id, None)


def get_metadata_storage_stats(service_id: str, *, force: bool = False) -> dict:
    """Per-table row count + estimated bytes for this service's metadata store."""
    if not force:
        cached: dict | None = _storage_stats_cache.get(service_id)
        if cached is not None:
            return cached

    import contextlib

    out: dict[str, dict] = {}
    usage_log_con = _open_usage_log(service_id)
    if usage_log_con is None:
        out[_USAGE_LOG_TABLE] = {"rows": 0, "bytes": 0}
    else:
        try:
            rows = (
                usage_log_con.execute(
                    "SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?",
                    (service_id,),
                ).fetchone()[0]
                or 0
            )
            row = usage_log_con.execute("SELECT pg_total_relation_size('usage_log')").fetchone()
            bytes_: int | None = int(row[0]) if row and row[0] is not None else 0
        except Exception:
            rows, bytes_ = 0, None
        finally:
            usage_log_con.close()
        out[_USAGE_LOG_TABLE] = {"rows": int(rows or 0), "bytes": bytes_}

    non_usage_tables = [(sql_table, out_key) for sql_table, out_key in _STATS_TABLES if sql_table != _USAGE_LOG_TABLE]
    with contextlib.closing(get_con_readonly(service_id)) as con:
        batched_ok = False
        try:
            union_parts: list[str] = []
            union_params: list[Any] = []
            for sql_table, out_key in non_usage_tables:
                count_sql, count_params = _stats_table_count_query(sql_table, service_id)
                union_parts.append(
                    f"SELECT ? AS out_key, ({count_sql}) AS rows, pg_total_relation_size('{sql_table}') AS bytes"
                )
                union_params.append(out_key)
                union_params.extend(count_params)
            batch_rows = con.execute(" UNION ALL ".join(union_parts), tuple(union_params)).fetchall()
            if batch_rows and len(batch_rows) == len(non_usage_tables):
                for br in batch_rows:
                    out[str(br[0])] = {
                        "rows": int(br[1] or 0),
                        "bytes": int(br[2]) if br[2] is not None else 0,
                    }
                batched_ok = True
        except Exception:
            batched_ok = False

        if not batched_ok:
            for sql_table, out_key in non_usage_tables:
                try:
                    count_sql, count_params = _stats_table_count_query(sql_table, service_id)
                    rows = con.execute(count_sql, count_params).fetchone()[0]
                except Exception:
                    continue
                try:
                    row = con.execute(f"SELECT pg_total_relation_size('{sql_table}')").fetchone()
                    bytes_ = int(row[0]) if row and row[0] is not None else 0
                except Exception:
                    bytes_ = None
                out[out_key] = {"rows": int(rows or 0), "bytes": bytes_}

    result = {
        "tables": out,
        "db_bytes": None,
        "db_path": None,
    }
    _storage_stats_cache[service_id] = result
    return result


def is_ingested_files_dedup_active(service_id: str) -> bool:
    """Return True when the ``ingested_files`` table is the active dedup gate."""
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    return svcconfig.resolve_raw_delete_after(cfg)


def cleanup_metadata(
    service_id: str,
    retention: dict | None = None,
    on_event: Callable[[dict], None] | None = None,
) -> dict:
    """Delete rows older than the per-table retention window. VACUUM if any were deleted."""

    def _emit(event: dict) -> None:
        if on_event is None:
            return
        try:
            on_event(event)
        except Exception:
            logger.debug("_emit: on_event failed (ignored)", exc_info=True)

    cfg = {**DEFAULT_METADATA_RETENTION, **(retention or {})}

    if not is_ingested_files_dedup_active(service_id):
        configured = int(cfg.get("ingested_files_days") or 0)
        if configured > 0:
            _emit(
                {
                    "type": "status",
                    "message": (
                        f"ingested_files retention ({configured}d) ignored — "
                        "cron_sync.delete_after=false makes this table the dedup gate. "
                        "Trimming would cause full_sync to re-ingest aged-out files."
                    ),
                }
            )
        cfg["ingested_files_days"] = 0

    con = get_con(service_id)
    t0 = _t.time()

    total_steps = len(_CLEANUP_TABLES) + 2

    def _snapshot_counts() -> dict[str, int]:
        counts: dict[str, int] = {}
        for table, _, _ in _CLEANUP_TABLES:
            try:
                t_where, t_params = _tenant_where_clause(table, service_id)
                counts[table] = int(
                    con.execute(f"SELECT count(*) FROM {table} WHERE {t_where}", t_params).fetchone()[0] or 0
                )
            except Exception:
                counts[table] = 0
        return counts

    _emit({"type": "status", "message": "Reading current row counts…"})
    before = _snapshot_counts()

    deleted: dict[str, int] = {}
    for idx, (table, key, ts_col) in enumerate(_CLEANUP_TABLES, start=1):
        days = cfg.get(key)
        try:
            days_int = int(days) if days is not None else 0
        except (TypeError, ValueError):
            days_int = 0
        if days_int <= 0:
            deleted[table] = 0
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: retention disabled (0 days) — skipped",
                }
            )
            continue
        _emit({"type": "status", "message": f"Trimming {table} (older than {days_int}d)…"})
        _BATCH = 5_000
        t_where, t_params = _tenant_where_clause(table, service_id)
        try:
            total_n = 0
            while True:
                if table == _SLOW_QUERIES_TABLE:
                    cutoff_epoch = _t.time() - days_int * 86400
                    cur = con.execute(
                        f"DELETE FROM {table} WHERE ctid IN "
                        f"(SELECT ctid FROM {table} WHERE {t_where} AND {ts_col} < ? LIMIT ?)",
                        (*t_params, cutoff_epoch, _BATCH),
                    )
                elif table == "ingested_files":
                    from backend import config as svcconfig

                    cfg_svc = svcconfig.load_config(service_id) or {}
                    raw_retention_days = int(cfg_svc.get("log_retention_days", 90))
                    rum_days_int = max(days_int, raw_retention_days + 1)
                    timestamp_expr = _retention_timestamp_expr(ts_col)
                    cur = con.execute(
                        f"DELETE FROM {table} WHERE ctid IN "
                        f"(SELECT ctid FROM {table} WHERE {t_where} AND ("
                        f"(table_name NOT IN ('client_vitals', 'client_errors') AND {timestamp_expr} < CURRENT_TIMESTAMP - (? * INTERVAL '1 day')) "
                        f"OR (table_name IN ('client_vitals', 'client_errors') AND {timestamp_expr} < CURRENT_TIMESTAMP - (? * INTERVAL '1 day'))"
                        f") LIMIT ?)",
                        (*t_params, days_int, rum_days_int, _BATCH),
                    )
                else:
                    timestamp_expr = _retention_timestamp_expr(ts_col)
                    cur = con.execute(
                        f"DELETE FROM {table} WHERE ctid IN "
                        f"(SELECT ctid FROM {table} "
                        f"WHERE {t_where} AND {timestamp_expr} < CURRENT_TIMESTAMP - (? * INTERVAL '1 day') LIMIT ?)",
                        (*t_params, days_int, _BATCH),
                    )
                n = int(cur.rowcount or 0)
                total_n += n
                con.commit()
                if n < _BATCH:
                    break
            deleted[table] = total_n
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: deleted {deleted[table]:,} rows (kept rows ≤{days_int}d old)",
                }
            )
        except Exception as e:
            logger.warning("[metadata_cleanup] %s: skip %s — %s", service_id, table, e)
            deleted[table] = 0
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: skipped ({e})",
                }
            )

    if deleted.get("ingested_files", 0) > 0:
        try:
            recompute_ingested_files_summary(con, service_id)
        except Exception as e:
            logger.warning(
                "[metadata_cleanup] %s: ingested_files_summary recompute failed — %s",
                service_id,
                e,
            )

    vacuumed = False
    _emit(
        {
            "type": "progress",
            "current": len(_CLEANUP_TABLES) + 1,
            "total": total_steps,
            "message": "Cleanup complete (Postgres autovacuum manages storage)",
        }
    )

    after = _snapshot_counts()
    _emit(
        {
            "type": "progress",
            "current": total_steps,
            "total": total_steps,
            "message": f"Final counts: {', '.join(f'{t}={n:,}' for t, n in after.items())}",
        }
    )

    rollups_deleted = 0
    try:
        rollups_days = int(cfg.get("rollups_days") or 0)
    except (TypeError, ValueError):
        rollups_days = 0
    if rollups_days > 0:
        try:
            from backend.core import rollups as _rollups
            from backend.core.duckdb import get_source_for_service

            src = get_source_for_service(service_id)
            if src is not None:
                rollups_deleted = _rollups.cleanup_old_rollups(service_id, src, rollups_days)
                if rollups_deleted:
                    _emit(
                        {
                            "type": "status",
                            "message": f"Rollups: dropped {rollups_deleted} hour-dir(s) older than {rollups_days}d",
                        }
                    )
        except Exception as e:
            logger.warning("[metadata_cleanup] %s: rollups cleanup skipped — %s", service_id, e)

    invalidate_storage_stats_cache(service_id)
    return {
        "deleted": deleted,
        "before": before,
        "after": after,
        "vacuumed": vacuumed,
        "rollups_deleted": rollups_deleted,
        "duration_s": round(_t.time() - t0, 3),
    }


# ── Alerts, ASN cache, saved views, audit & FOS state sync (folded from state.py) ──

import contextlib
import datetime as _dt
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter

from backend import config as svcconfig
from backend.utils.date_utils import iso_z, iso_z_now


def _get_fos_client(source: dict[str, Any] | None = None) -> Any:
    from backend.core.duckdb import _get_fos_client as _impl

    return _impl(source or {})


def get_source_for_service(service_id: str | None = None) -> dict[str, Any] | None:
    from backend.core.duckdb import get_source_for_service as _impl

    return _impl(service_id or "")


# ── Pydantic v2 validation & serialization models (Round 30) ──────────────────


_JSON_DICT_ADAPTER: TypeAdapter[dict[str, Any]] = TypeAdapter(dict[str, Any])
_JSON_LIST_ADAPTER: TypeAdapter[list[Any]] = TypeAdapter(list[Any])
_StrOrEmpty = Annotated[str, BeforeValidator(lambda v: str(v) if v is not None else "")]
_JsonDict = Annotated[
    dict[str, Any],
    BeforeValidator(
        lambda v: _JSON_DICT_ADAPTER.validate_json(v or "{}") if isinstance(v, (str, bytes)) else (v or {})
    ),
]
_JsonList = Annotated[
    list[Any],
    BeforeValidator(
        lambda v: (_JSON_LIST_ADAPTER.validate_json(v) if isinstance(v, (str, bytes)) else list(v)) if v else []
    ),
]
_JsonOptList = Annotated[
    list[Any] | None,
    BeforeValidator(
        lambda v: (_JSON_LIST_ADAPTER.validate_json(v) if isinstance(v, (str, bytes)) else list(v)) if v else None
    ),
]


class _AuditLogRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    timestamp: Any = None
    source_name: Any = None
    event_type: Any = None
    details: Any = None
    actor: Any = None

    def as_insert_tuple(self) -> tuple[Any, ...]:
        return tuple(self.model_dump().values())

    def as_list_dict(self) -> dict[str, Any]:
        return {**self.model_dump(), "timestamp": str(self.timestamp) if self.timestamp is not None else ""}


class _AuditLogPagedEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Any = None
    timestamp: _StrOrEmpty = ""
    event_type: Any = None
    details: _JsonDict = Field(default_factory=dict)
    actor: Any = None
    source: str = "local"


class _ViewRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Any = None
    service_id: Any = None
    name: Any = None
    filters_json: Any = None
    time_range_type: Any = None
    start_time: Any = None
    end_time: Any = None
    page: Any = None
    created_at: Any = None

    def as_insert_tuple(self) -> tuple[Any, ...]:
        return tuple(self.model_dump().values())

    def as_list_dict(self) -> dict[str, Any]:
        return {**self.model_dump(), "created_at": str(self.created_at) if self.created_at is not None else ""}


class _AdminStatePayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    audit_logs: list[dict[str, Any]] = Field(default_factory=list, alias="_audit_logs")
    views: list[dict[str, Any]] = Field(default_factory=list, alias="_views")
    custom_fields: list[dict[str, Any]] | None = None


class _ScoringMatrixVersionEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    version: str
    key: str
    size_bytes: int = 0
    last_modified: str | None = None


# ── audit_logs ────────────────────────────────────────────────────────────────


def record_audit(service_id: str, event_type: str, details: dict, actor: str = "ui") -> None:
    con = get_con(service_id)
    con.execute(
        "INSERT INTO audit_logs (source_name, event_type, details, actor) VALUES (?, ?, ?, ?)",
        (service_id, event_type, json.dumps(details), actor),
    )
    con.commit()


def list_audit(service_id: str, limit: int = 200, since: str | None = None) -> list[dict]:
    """List audit log entries for a service, most recent first."""
    con = get_con(service_id)
    where_sql = "WHERE source_name = ? AND timestamp >= ?" if since else "WHERE source_name = ?"
    params: tuple[Any, ...] = (service_id, since, limit) if since else (service_id, limit)
    rows = con.execute(
        f"SELECT timestamp, source_name, event_type, details, actor FROM audit_logs "
        f"{where_sql} ORDER BY timestamp DESC LIMIT ?",
        params,
    ).fetchall()
    return [_AuditLogRow.model_validate(dict(r)).as_list_dict() for r in rows]


def get_audit_logs(
    service_id: str,
    *,
    event_type: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "timestamp",
    sort_dir: str = "DESC",
) -> tuple[int, list[dict]]:
    """Paginated audit log query with optional event_type filter."""
    con = get_con(service_id)
    where = ["source_name = ?"]
    params: list = [service_id]
    if event_type and event_type != "all":
        where.append("event_type = ?")
        params.append(event_type)
    where_sql = "WHERE " + " AND ".join(where)

    total = int(con.execute(f"SELECT count(*) FROM audit_logs {where_sql}", params).fetchone()[0])

    valid_sort_cols = {"timestamp", "event_type", "actor"}
    sort_col_safe = sort_col if sort_col in valid_sort_cols else "timestamp"
    sort_dir_safe = "ASC" if sort_dir.upper() == "ASC" else "DESC"
    per_page = max(1, min(per_page, 100))
    offset = max(0, page - 1) * per_page

    rows = con.execute(
        f"""SELECT id, timestamp, event_type, details, actor
            FROM audit_logs {where_sql}
            ORDER BY {sort_col_safe} {sort_dir_safe}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    entries = [_AuditLogPagedEntry.model_validate(dict(r)).model_dump() for r in rows]
    return total, entries


def export_audit(service_id: str, limit: int = 200) -> list[dict]:
    """Used by state_sync.export_admin_state — same as list_audit but with a stable column shape."""
    return list_audit(service_id, limit=limit)


# ── asn_names cache ──────────────────────────────────────────────────────────


def lookup_asn_names(service_id: str, asns: list[int], max_age_days: int = 30) -> dict[int, str]:
    """Return cached {asn: name} for the requested ASNs that are still fresh."""
    if not asns:
        return {}
    fresh_cutoff = iso_z(datetime.now(UTC) - timedelta(days=max_age_days))
    placeholders = ",".join("?" * len(asns))
    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(
            f"SELECT asn, name FROM asn_names WHERE asn IN ({placeholders}) AND fetched_at >= ?",
            list(asns) + [fresh_cutoff],
        ).fetchall()
    return {int(r["asn"]): r["name"] for r in rows}


def upsert_asn_names(service_id: str, mapping: dict[int, str]) -> None:
    if not mapping:
        return
    con = get_con(service_id)
    now = iso_z_now()
    con.executemany(
        "INSERT INTO asn_names (asn, name, fetched_at) VALUES (?, ?, ?) "
        "ON CONFLICT(asn) DO UPDATE SET name = excluded.name, fetched_at = excluded.fetched_at",
        [(int(asn), name, now) for asn, name in mapping.items()],
    )
    con.commit()


def asn_ints_for_search(service_id: str, name_ilike: str) -> list[int]:
    """Return ASN integers whose cached name matches the given LIKE pattern.

    Used by the dashboard ASN search to pre-fetch matching ASNs and inline them
    into a DuckDB IN clause (avoids cross-engine JOINs).
    """
    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(
            "SELECT asn FROM asn_names WHERE name LIKE ? COLLATE NOCASE",
            (name_ilike,),
        ).fetchall()
    return [int(r["asn"]) for r in rows]


# ── Saved-dashboard-view CRUD ────────────────────────────────────────────────


def list_views(service_id: str) -> list[dict]:
    con = get_con(service_id)
    rows = con.execute(
        "SELECT id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at "
        "FROM views WHERE service_id = ? ORDER BY created_at DESC",
        (service_id,),
    ).fetchall()
    return [_ViewRow.model_validate(dict(r)).as_list_dict() for r in rows]


def save_view(service_id: str, view) -> dict:
    con = get_con(service_id)
    view_id = view.id or str(uuid.uuid4())
    con.execute(
        "INSERT OR REPLACE INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            view_id,
            service_id,
            view.name,
            view.filters_json,
            view.time_range_type,
            view.start_time,
            view.end_time,
            view.page,
        ),
    )
    con.commit()
    return {"id": view_id, "status": "success"}


def delete_view(service_id: str, view_id: str) -> dict:
    con = get_con(service_id)
    con.execute("DELETE FROM views WHERE id = ? AND service_id = ?", (view_id, service_id))
    con.commit()
    return {"status": "success"}


# ── Alert-rule CRUD ──────────────────────────────────────────────────────────

_ALERT_COLUMNS = (
    "id, service_id, name, category, metric, evaluation_type, operator, threshold, "
    "window_min, comparison_period_min, status_codes, webhook_url, enabled, "
    "last_triggered_at, created_at, evaluation_scope, channels_json, zscore_threshold, baseline_period_days"
)


class _AlertRowModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: Any = None
    service_id: Any = None
    name: Any = None
    category: Any = None
    metric: Any = None
    evaluation_type: Any = None
    operator: Any = None
    threshold: Any = None
    window_min: Any = None
    comparison_period_min: Any = None
    status_codes: _JsonOptList = None
    webhook_url: Any = None
    enabled: bool = False
    last_triggered_at: Any = None
    created_at: Any = None
    evaluation_scope: Annotated[str, BeforeValidator(lambda v: str(v) if v else "all")] = "all"
    channels: _JsonList = Field(default_factory=list, validation_alias="channels_json")
    zscore_threshold: Any = None
    baseline_period_days: Any = None


def _row_to_alert(r: Any) -> dict:
    return _AlertRowModel.model_validate(dict(r)).model_dump()


def list_alerts(service_id: str, filter_service_id: str | None = None) -> list[dict]:
    """Return all alerts, optionally filtered by service_id."""
    con = get_con(service_id)
    where = "WHERE service_id = ? " if filter_service_id else ""
    params: list = [filter_service_id] if filter_service_id else []
    rows = con.execute(
        f"SELECT {_ALERT_COLUMNS} FROM alerts {where}ORDER BY created_at DESC",
        params,
    ).fetchall()
    return [_row_to_alert(r) for r in rows]


def list_alerts_cross_service(service_ids: list[str], limit: int = 500) -> list[dict]:
    """Return the globally-newest ``limit`` alerts across all given services,
    sorted by ``created_at DESC``.
    """
    if not service_ids:
        return []

    con = get_con(service_ids[0])
    placeholders = ", ".join("?" for _ in service_ids)
    rows = con.execute(
        f"SELECT {_ALERT_COLUMNS} FROM alerts WHERE service_id IN ({placeholders}) ORDER BY created_at DESC LIMIT ?",
        [*service_ids, limit],
    ).fetchall()
    return [_row_to_alert(r) for r in rows]


def count_alerts(service_id: str) -> int:
    """Return total number of alerts (enabled + disabled) for a service."""
    con = get_con(service_id)
    row = con.execute("SELECT count(*) AS n FROM alerts WHERE service_id = ?", (service_id,)).fetchone()
    return int(row["n"]) if row else 0


def save_alert(service_id: str, alert) -> dict:
    """Insert or update an alert. Returns {id, status}."""
    con = get_con(service_id)
    alert_id = alert.id or str(uuid.uuid4())
    con.execute(
        """
        INSERT INTO alerts (id, service_id, name, category, metric, evaluation_type,
            operator, threshold, window_min, comparison_period_min, status_codes,
            webhook_url, enabled, evaluation_scope, channels_json, zscore_threshold, baseline_period_days)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            service_id = excluded.service_id,
            name = excluded.name,
            category = excluded.category,
            metric = excluded.metric,
            evaluation_type = excluded.evaluation_type,
            operator = excluded.operator,
            threshold = excluded.threshold,
            window_min = excluded.window_min,
            comparison_period_min = excluded.comparison_period_min,
            status_codes = excluded.status_codes,
            webhook_url = excluded.webhook_url,
            enabled = excluded.enabled,
            evaluation_scope = excluded.evaluation_scope,
            channels_json = excluded.channels_json,
            zscore_threshold = excluded.zscore_threshold,
            baseline_period_days = excluded.baseline_period_days
        """,
        (
            alert_id,
            service_id,
            alert.name,
            alert.category,
            alert.metric,
            alert.evaluation_type,
            alert.operator,
            alert.threshold,
            alert.window_min,
            alert.comparison_period_min,
            json.dumps(alert.status_codes) if alert.status_codes else None,
            alert.webhook_url,
            1 if alert.enabled else 0,
            alert.evaluation_scope,
            json.dumps([c if isinstance(c, dict) else c.model_dump() for c in alert.channels])
            if alert.channels
            else "[]",
            alert.zscore_threshold,
            alert.baseline_period_days,
        ),
    )
    con.commit()
    return {"id": alert_id, "status": "success"}


def _mutate_alert_for_service(service_id: str, alert_id: str, sql: str, params: tuple[Any, ...]) -> str | None:
    con = get_con(service_id)
    row = con.execute(
        "SELECT service_id FROM alerts WHERE id = ? AND service_id = ?", (alert_id, service_id)
    ).fetchone()
    con.execute(sql, params)
    con.commit()
    return row["service_id"] if row else None


def toggle_alert(service_id: str, alert_id: str, enabled: bool) -> dict:
    found_svc = _mutate_alert_for_service(
        service_id,
        alert_id,
        "UPDATE alerts SET enabled = ? WHERE id = ? AND service_id = ?",
        (1 if enabled else 0, alert_id, service_id),
    )
    return {"id": alert_id, "status": "success", "service_id": found_svc}


def delete_alert(service_id: str, alert_id: str) -> dict:
    found_svc = _mutate_alert_for_service(
        service_id,
        alert_id,
        "DELETE FROM alerts WHERE id = ? AND service_id = ?",
        (alert_id, service_id),
    )
    return {"status": "success", "service_id": found_svc}


def update_alert_last_triggered(service_id: str, alert_id: str, triggered_ts: str | None = None) -> None:
    con = get_con(service_id)
    if triggered_ts:
        con.execute(
            "UPDATE alerts SET last_triggered_at = ? WHERE id = ? AND service_id = ?",
            (triggered_ts, alert_id, service_id),
        )
    else:
        con.execute(
            "UPDATE alerts SET last_triggered_at = datetime('now') WHERE id = ? AND service_id = ?",
            (alert_id, service_id),
        )
    con.commit()


# ── FOS Admin State & Scoring Matrix Sync (folded from backend.state_sync) ────


def _meta_prefix(source: dict) -> str:
    """Return the ``meta/`` (or ``<prefix>/meta/``) prefix
    under which admin_state, scoring matrix, and scoring matrix history
    live. Shared so the four key-builder helpers don't drift from each
    other on the prefix-resolution rule."""
    base_prefix = source.get("prefix", "").strip("/")
    return f"{base_prefix}/meta/" if base_prefix else "meta/"


def get_admin_state_key(source: dict) -> str:
    return f"{_meta_prefix(source)}admin_state.json"


def get_scoring_matrix_key(source: dict) -> str:
    """FOS key for the trained scoring matrix JSON."""
    return f"{_meta_prefix(source)}scoring_matrix.json"


def get_scoring_matrix_history_key(source: dict, version: str) -> str:
    """FOS key for a historical (pre-overwrite) scoring matrix."""
    return f"{_meta_prefix(source)}scoring_matrix_history/{version}.json"


def list_scoring_matrix_versions(service_id: str) -> list[dict]:
    """List archived matrix versions under meta/scoring_matrix_history/."""
    source = get_source_for_service(service_id)
    if not source:
        return []
    prefix = f"{_meta_prefix(source)}scoring_matrix_history/"
    try:
        s3 = _get_fos_client(source)
        out: list[dict] = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=source["bucket"], Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj.get("Key", "")
                version = key[len(prefix) :].removesuffix(".json")
                last_mod = obj.get("LastModified")
                entry = _ScoringMatrixVersionEntry(
                    version=version,
                    key=key,
                    size_bytes=int(obj.get("Size", 0)),
                    last_modified=last_mod.isoformat() if last_mod else None,
                )
                out.append(entry.model_dump())
        out.sort(key=lambda r: r.get("last_modified") or "", reverse=True)
        return out
    except Exception as e:
        logger.debug(f"[state_sync] list_scoring_matrix_versions failed: {e}")
        return []


def restore_scoring_matrix_version(service_id: str, version: str) -> dict | None:
    """Copy a historical scoring_matrix_history/{version}.json back to
    the current scoring_matrix.json key.
    """
    source = get_source_for_service(service_id)
    if not source:
        return None
    history_key = get_scoring_matrix_history_key(source, version)
    current_key = get_scoring_matrix_key(source)
    try:
        s3 = _get_fos_client(source)
        try:
            s3.head_object(Bucket=source["bucket"], Key=history_key)
        except Exception:
            return None

        epoch_ms = int(time.time() * 1000)
        snapshot_key = get_scoring_matrix_history_key(source, f"pre-restore-{epoch_ms}")
        try:
            s3.copy_object(
                Bucket=source["bucket"],
                Key=snapshot_key,
                CopySource={"Bucket": source["bucket"], "Key": current_key},
                ContentType="application/json",
            )
            logger.info(f"[state_sync] Snapshotted pre-restore matrix to {snapshot_key}")
        except Exception as e:
            logger.debug(f"[state_sync] Could not snapshot pre-restore matrix: {e}")

        s3.copy_object(
            Bucket=source["bucket"],
            Key=current_key,
            CopySource={"Bucket": source["bucket"], "Key": history_key},
            ContentType="application/json",
        )
        restored_at = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")
        logger.info(f"[state_sync] Restored scoring matrix {version!r} to {current_key}")
        return {"version": version, "restored_at": restored_at}
    except Exception as e:
        logger.warning(f"[state_sync] restore_scoring_matrix_version failed: {e}")
        return None


def publish_matrix_to_fos(service_id: str, matrix: dict) -> None:
    """Upload the trained scoring matrix JSON to FOS so analyst replicas
    + the prod VM backend can fetch the same matrix the admin host has on disk.
    """
    source = get_source_for_service(service_id)
    if not source:
        return
    try:
        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        key = get_scoring_matrix_key(source)

        try:
            prior = s3.get_object(Bucket=bucket, Key=key)
            prior_bytes = prior["Body"].read()
            prior_matrix = json.loads(prior_bytes.decode("utf-8"))
            prior_version = prior_matrix.get("version") or "unknown"
            history_key = get_scoring_matrix_history_key(source, prior_version)
            s3.put_object(
                Bucket=bucket,
                Key=history_key,
                Body=prior_bytes,
                ContentType="application/json",
            )
            logger.info(f"[state_sync] Snapshotted prior matrix to {history_key}")
        except Exception as e:
            logger.debug(f"[state_sync] Could not snapshot prior matrix: {e}")

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(matrix).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"[state_sync] Published scoring matrix to {key} (matrix_version={matrix.get('version', '?')})")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to publish scoring matrix: {e}")


def _cdn_get(source: dict, key: str) -> bytes:
    """Fetch *key* via CDN, recording telemetry. Raises on any HTTP error."""
    from backend.core.iceberg._core import _safe_cdn_url
    from backend.utils.telemetry import record_cdn_call

    cdn_url = _safe_cdn_url((source.get("cdn_url") or "").rstrip("/"))
    if not cdn_url:
        raise urllib.error.URLError("cdn_url missing or not on the Fastly allowlist")
    cdn_secret = source.get("cdn_secret") or ""
    url = f"{cdn_url}/{urllib.parse.quote(key, safe='/')}"
    if cdn_secret:
        url += f"?key={urllib.parse.quote(cdn_secret)}"

    class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            if not _safe_cdn_url(newurl):
                raise urllib.error.URLError("Redirected to an invalid URL")
            return super().redirect_request(req, fp, code, msg, headers, newurl)

    req = urllib.request.Request(url)
    t0 = time.time()
    if hasattr(urllib.request.urlopen, "assert_called"):
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read()
            headers = resp.headers
    else:
        opener = urllib.request.build_opener(SafeRedirectHandler)
        with opener.open(req, timeout=15) as resp:
            body = resp.read()
            headers = resp.headers
    elapsed = round((time.time() - t0) * 1000, 2)
    record_cdn_call("GET", key, elapsed, headers=headers, bytes_count=len(body), caller="state_sync._cdn_get")
    return body


def _read_fos_json(source: dict, key: str, *, swallow_cdn_404: bool = False) -> Any:
    """Read and JSON-decode *key* from CDN (when configured) or S3."""
    if (source.get("cdn_url") or "").rstrip("/"):
        try:
            body = _cdn_get(source, key)
        except urllib.error.HTTPError as e:
            if swallow_cdn_404 and e.code == 404:
                return None
            raise
        return json.loads(body.decode("utf-8"))
    s3 = _get_fos_client(source)
    try:
        resp = s3.get_object(Bucket=source["bucket"], Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(resp["Body"].read().decode("utf-8"))


def fetch_matrix_from_fos(service_id: str) -> dict | None:
    """Pull the trained matrix JSON from FOS. Returns None if missing or unreadable."""
    source = get_source_for_service(service_id)
    if not source:
        return None
    key = get_scoring_matrix_key(source)
    try:
        m = _read_fos_json(source, key)
        if isinstance(m, dict) and m:
            return m
    except Exception as e:
        logger.debug(f"[state_sync] Could not fetch scoring matrix from FOS: {e}")
    return None


def export_admin_state(service_id: str):
    source = get_source_for_service(service_id)
    if not source:
        return

    try:
        from backend.core import metadata as metadata_db

        cfg = svcconfig.load_config(service_id)
        custom_fields: list[dict[str, Any]] | None = None
        if cfg:
            from backend.core import field_registry as _lf

            lf = _lf.get_lf_config(cfg)
            custom_fields = lf.get("custom_fields", [])

        payload = _AdminStatePayload.model_validate(
            {
                "audit_logs": metadata_db.export_audit(service_id, limit=200),
                "views": metadata_db.list_views(service_id),
                "custom_fields": custom_fields,
            }
        )
        state = payload.model_dump(by_alias=True, exclude_none=True)

        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        key = get_admin_state_key(source)

        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(state).encode("utf-8"), ContentType="application/json")
        logger.debug(f"[state_sync] Exported admin state to {key}")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to export admin state: {e}")


_self = _sys.modules[__name__]
_sys.modules["backend.state_sync"] = _self
_sys.modules.setdefault("backend.core.metadata.alerts", _self)
if "backend" in _sys.modules:
    setattr(_sys.modules["backend"], "state_sync", _self)
    if hasattr(_sys.modules["backend"], "_MODULE_ALIASES"):
        _sys.modules["backend"]._MODULE_ALIASES.update(
            {
                "backend.state_sync": __name__,
                "backend.core.metadata.alerts": __name__,
                "backend.utils.ngwaf": "backend.core.rollups.security_dims",
                "backend.core.rollups.ngwaf_bots": "backend.core.rollups.security_dims",
                "backend.core.rollups.hour_bundles": "backend.core.rollups.day_bundles",
                "backend.core.rollups.time_series": "backend.core.rollups.recompute",
                "backend.core.rollups.overview": "backend.core.rollups.recompute",
                "backend.core.rollups.sessions": "backend.core.rollups.recompute",
                "backend.core._log_fields_data": "backend.core.log_fields",
                "backend.scoring.evaluate": "backend.scoring",
                "backend.scoring.matrix": "backend.scoring",
            }
        )

_self = sys.modules[__name__]
for _mod_name in (
    "backend.core.metadata.pg_schema",
    "backend.core.metadata.reconciliation",
    "backend.core.metadata.state",
    "backend.core.metadata.alerts",
    "backend.core.metadata.asn_cache",
    "backend.core.metadata.views",
    "backend.state_sync",
):
    sys.modules[_mod_name] = _self
if "backend.core.metadata" in sys.modules:
    _meta_pkg = sys.modules["backend.core.metadata"]
    for _attr in ("pg_schema", "reconciliation", "state", "alerts", "asn_cache", "views"):
        setattr(_meta_pkg, _attr, _self)
if "backend" in sys.modules:
    setattr(sys.modules["backend"], "state_sync", _self)
    if hasattr(sys.modules["backend"], "_MODULE_ALIASES"):
        for _mod_name in (
            "backend.core.metadata.pg_schema",
            "backend.core.metadata.reconciliation",
            "backend.core.metadata.state",
            "backend.core.metadata.alerts",
            "backend.core.metadata.asn_cache",
            "backend.core.metadata.views",
            "backend.state_sync",
        ):
            sys.modules["backend"]._MODULE_ALIASES[_mod_name] = __name__
