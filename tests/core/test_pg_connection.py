"""Postgres metadata backend: dialect shim + connection lifecycle.

No live Postgres needed — these pin the SQL rewriting and the
get/put-conn lifecycle against a fake pool/cursor. The one thing they
deliberately do NOT cover is whether the rewritten SQL actually executes
against a real Postgres server; that's the manual verification step in
the migration runbook.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.core.metadata import pg_connection as pgc

# ── _rewrite_sql / helpers ────────────────────────────────────────────────


def test_placeholder_replace_skips_string_literals():
    sql = "SELECT * FROM t WHERE a = ? AND b = 'literal ? mark' AND c = ?"
    out = pgc._replace_placeholders_outside_literals(sql)
    assert out == "SELECT * FROM t WHERE a = %s AND b = 'literal ? mark' AND c = %s"


def test_placeholder_replace_no_placeholders():
    sql = "SELECT 1"
    assert pgc._replace_placeholders_outside_literals(sql) == "SELECT 1"


@pytest.mark.parametrize(
    "table,cols_hint",
    [
        ("local_compacted_files", None),
        ("committed_buffers", None),
        ("sources", None),
    ],
)
def test_insert_or_ignore_known_tables_become_on_conflict_do_nothing(table, cols_hint):
    sql = f"INSERT OR IGNORE INTO {table} (a, b) VALUES (?, ?)"
    out = pgc._rewrite_insert_or(sql)
    assert out == f"INSERT INTO {table} (a, b) VALUES (?, ?) ON CONFLICT DO NOTHING"


def test_insert_or_replace_quarantined_files_becomes_on_conflict_update():
    """The conflict target must be the table's real UNIQUE — (service_id,
    file_name). This previously asserted ``ON CONFLICT(id)``, pinning a bug:
    ``insert_quarantined_file`` never supplies ``id`` (it is autoincrement),
    so the clause could never fire and a re-quarantine of the same file
    INSERTed a duplicate under Postgres while SQLite's OR REPLACE correctly
    replaced it. ``file_name`` is no longer in the update list because it is
    now part of the conflict key.
    """
    sql = "INSERT OR REPLACE INTO quarantined_files (service_id, file_name) VALUES (?, ?)"
    out = pgc._rewrite_insert_or(sql)
    assert out.startswith(
        "INSERT INTO quarantined_files (service_id, file_name) VALUES (?, ?) "
        "ON CONFLICT(service_id, file_name) DO UPDATE SET"
    )
    assert "source_name=EXCLUDED.source_name" in out
    assert "corrupt_rows=EXCLUDED.corrupt_rows" in out


def test_insert_or_replace_views_becomes_on_conflict_update():
    sql = "INSERT OR REPLACE INTO views (id, service_id, name) VALUES (?, ?, ?)"
    out = pgc._rewrite_insert_or(sql)
    assert "ON CONFLICT(id) DO UPDATE SET" in out
    assert "service_id=EXCLUDED.service_id" in out


def test_insert_or_unmapped_table_raises_loudly():
    """A new SQLite writer using INSERT OR * must be taught to this module —
    silently dropping the semantics would let a unique-violation crash (or
    worse, a silent duplicate) reach production instead of failing at
    review/test time."""
    with pytest.raises(NotImplementedError, match="brand_new_table"):
        pgc._rewrite_insert_or("INSERT OR IGNORE INTO brand_new_table (x) VALUES (?)")


def test_rewrite_sql_passes_through_plain_insert():
    sql = "INSERT INTO cron_runs (service_id, task) VALUES (?, ?)"
    assert pgc._rewrite_sql(sql) == "INSERT INTO cron_runs (service_id, task) VALUES (%s, %s)"


def test_rewrite_sql_datetime_now():
    assert "current_timestamp AT TIME ZONE 'UTC'" in pgc._rewrite_sql("SELECT datetime('now')")


def test_rewrite_sql_datetime_now_with_modifier():
    out = pgc._rewrite_sql("SELECT datetime('now', '-15 minutes')")
    assert "current_timestamp AT TIME ZONE 'UTC' + INTERVAL '-15 minutes'" in out


def test_rewrite_sql_strftime_iso():
    out = pgc._rewrite_sql("SELECT strftime('%Y-%m-%dT%H:%M:%SZ', 'now')")
    assert "to_char(current_timestamp AT TIME ZONE 'UTC'" in out


def test_rewrite_sql_instr_and_substr():
    out = pgc._rewrite_sql("SELECT instr(a, b), substr(a, 1, 2) FROM t")
    assert "strpos(a, b)" in out
    assert "substring(a, 1, 2)" in out


def test_rewrite_sql_excluded_lowercase_to_upper_only_with_on_conflict():
    # Only rewritten inside an actual ON CONFLICT clause (already-Postgres
    # SQL passed straight through must not get corrupted by a bare
    # substring match on unrelated text containing "excluded.").
    sql = "INSERT INTO t VALUES (?) ON CONFLICT(id) DO UPDATE SET a = excluded.a"
    assert "EXCLUDED.a" in pgc._rewrite_sql(sql)
    # No ON CONFLICT clause present — must stay untouched, not uppercased.
    assert pgc._rewrite_sql("SELECT 'excluded.something'") == "SELECT 'excluded.something'"


def test_maybe_add_returning_cron_runs():
    sql, id_col = pgc._maybe_add_returning("INSERT INTO cron_runs (task) VALUES (%s)")
    assert sql.endswith("RETURNING id")
    assert id_col == "id"


def test_maybe_add_returning_other_table_untouched():
    sql, id_col = pgc._maybe_add_returning("INSERT INTO ingested_files (a) VALUES (%s)")
    assert sql == "INSERT INTO ingested_files (a) VALUES (%s)"
    assert id_col is None


def test_maybe_add_returning_idempotent_if_already_present():
    sql, id_col = pgc._maybe_add_returning("INSERT INTO cron_runs (task) VALUES (%s) RETURNING id")
    assert sql.count("RETURNING") == 1
    assert id_col is None  # already has it — caller's own RETURNING wins, no double-fetch


# ── PgConnectionWrapper / PgCursorWrapper against a fake psycopg conn ──────


class _FakeCursor:
    def __init__(self, fetchone_result=None):
        self.executed: list[tuple[str, tuple]] = []
        self._fetchone_result = fetchone_result
        self.rowcount = 1

    def execute(self, sql, params=()):
        self.executed.append((sql, tuple(params)))

    def executemany(self, sql, seq):
        self.executed.append((sql, tuple(seq)))

    def fetchone(self):
        return self._fetchone_result

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self, fetchone_result=None):
        self._cursor = _FakeCursor(fetchone_result)

    def cursor(self):
        return self._cursor


def test_wrapper_execute_rewrites_and_runs():
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn)
    wrapper.execute("SELECT * FROM t WHERE a = ?", (1,))
    sql, params = conn._cursor.executed[0]
    assert sql == "SELECT * FROM t WHERE a = %s"
    assert params == (1,)


def test_wrapper_execute_registers_with_live_query_monitor():
    """Postgres queries must appear in the same Live Query Monitor
    (query_registry) SQLite queries already feed — this was a documented
    blind spot: PgConnectionWrapper bypassed InstrumentedConnection
    entirely, so a slow Postgres metadata query was invisible to
    /admin/queries and never persisted to slow_queries."""
    from backend.core import query_registry

    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn)
    wrapper._service_id = "svc-1"

    seen: dict = {}
    orig_register = query_registry.query_registry.register

    def _spy_register(db_type, sql, **kwargs):
        seen["db_type"] = db_type
        seen["service_id"] = kwargs.get("service_id")
        return orig_register(db_type, sql, **kwargs)

    with patch.object(query_registry.query_registry, "register", side_effect=_spy_register):
        wrapper.execute("SELECT 1")

    assert seen["db_type"] == "Postgres"
    assert seen["service_id"] == "svc-1"


def test_wrapper_execute_deregisters_on_error():
    from backend.core import query_registry

    class _RaisingCursor(_FakeCursor):
        def execute(self, sql, params=()):
            raise RuntimeError("boom")

    conn = _FakeConn()
    conn._cursor = _RaisingCursor()
    wrapper = pgc.PgConnectionWrapper(conn)

    with patch.object(query_registry.query_registry, "deregister") as mock_deregister:
        with pytest.raises(RuntimeError, match="boom"):
            wrapper.execute("SELECT 1")

    assert mock_deregister.call_count == 1
    _qid, kwargs = mock_deregister.call_args
    assert isinstance(kwargs.get("error"), RuntimeError)


def test_wrapper_execute_cron_runs_returns_lastrowid():
    conn = _FakeConn(fetchone_result={"id": 42})
    wrapper = pgc.PgConnectionWrapper(conn)
    cur = wrapper.execute("INSERT INTO cron_runs (task) VALUES (?)", ("log_discovery",))
    assert cur.lastrowid == 42
    sql, _ = conn._cursor.executed[0]
    assert "RETURNING id" in sql


def test_wrapper_execute_non_cron_runs_no_lastrowid():
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn)
    cur = wrapper.execute("INSERT INTO ingested_files (a) VALUES (?)", (1,))
    assert cur.lastrowid is None


def test_wrapper_commit_rollback_are_noops_under_autocommit():
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn)
    wrapper.commit()  # must not raise
    wrapper.rollback()  # must not raise


def test_wrapper_close_returns_to_pool_only_when_flagged(monkeypatch):
    conn = _FakeConn()
    pool = MagicMock()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    long_lived = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=False)
    long_lived.close()
    pool.putconn.assert_not_called()

    checked_out = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=True)
    checked_out.close()
    pool.putconn.assert_called_once_with(conn)


# ── Thread-local long-lived connection lifecycle ───────────────────────────


@pytest.fixture(autouse=True)
def _reset_pg_state():
    pgc.close_all_pg_connections()
    pgc.reset_pg_pool_for_tests()
    yield
    pgc.close_all_pg_connections()
    pgc.reset_pg_pool_for_tests()


def test_get_pg_thread_connection_reuses_within_thread(monkeypatch):
    pool = MagicMock()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    first = pgc.get_pg_thread_connection()
    second = pgc.get_pg_thread_connection()
    assert first is second
    pool.getconn.assert_called_once()


def test_close_all_pg_connections_returns_and_clears(monkeypatch):
    pool = MagicMock()
    fake = _FakeConn()
    pool.getconn.side_effect = [fake]
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    wrapper = pgc.get_pg_thread_connection()
    assert wrapper is not None
    pgc.close_all_pg_connections()
    pool.putconn.assert_called_once_with(fake)

    # A subsequent get must check out a NEW connection, not reuse the
    # returned one — proves the thread-local cache was actually cleared.
    pool.getconn.side_effect = [_FakeConn()]
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 2


def test_release_pg_thread_connection_returns_and_clears(monkeypatch):
    """The heartbeat-thread leak fix: a thread that will never be reused
    must be able to hand its connection back WITHOUT disturbing any other
    thread's connection (unlike close_all_pg_connections, which is
    test-teardown-only and drains every thread)."""
    pool = MagicMock()
    fake = _FakeConn()
    pool.getconn.side_effect = [fake]
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    pgc.get_pg_thread_connection()
    pgc.release_pg_thread_connection()
    pool.putconn.assert_called_once_with(fake)

    # A subsequent get_pg_thread_connection on the SAME thread must check
    # out a fresh connection, not reuse the one just returned.
    pool.getconn.side_effect = [_FakeConn()]
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 2


def test_release_pg_thread_connection_noop_when_no_connection(monkeypatch):
    pool = MagicMock()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)
    pgc.release_pg_thread_connection()  # must not raise
    pool.putconn.assert_not_called()


def test_release_pg_thread_connection_only_affects_calling_thread(monkeypatch):
    """Two threads each hold their own connection; releasing thread A's
    must not touch thread B's — the actual bug this fixes was a per-tick
    heartbeat thread returning ALL threads' connections (or none)."""
    import threading

    pool = MagicMock()
    conns = [_FakeConn(), _FakeConn()]
    pool.getconn.side_effect = conns
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    pgc.get_pg_thread_connection()  # this test's own thread: conns[0]

    other_thread_wrapper = {}

    def other_thread():
        other_thread_wrapper["w"] = pgc.get_pg_thread_connection()

    t = threading.Thread(target=other_thread)
    t.start()
    t.join()

    pgc.release_pg_thread_connection()  # only this thread's (conns[0])
    pool.putconn.assert_called_once_with(conns[0])

    # This thread's own connection is gone; a re-fetch checks out fresh.
    pool.getconn.side_effect = [_FakeConn()]
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 3


def test_wrapper_finalizer_returns_connection_when_garbage_collected(monkeypatch):
    """The GC safety net: a wrapper that is dropped WITHOUT any explicit
    release/close must still return its connection to the pool. This is
    what actually protects against the leak in cases release_thread_
    connection() can't reach — a sync HTTP route handler running on an
    anyio threadpool worker that gets pruned (anyio kills idle workers
    after 10s on its own schedule; no request-scoped cleanup can
    reliably catch that, since a request's dependencies/endpoint aren't
    guaranteed to share one worker thread)."""
    import gc

    pool = MagicMock()
    conn = _FakeConn()
    pool.getconn.return_value = conn
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    wrapper = pgc.PgConnectionWrapper(conn)
    assert conn not in [c.args[0] for c in pool.putconn.call_args_list]

    del wrapper
    gc.collect()

    # A stray finalizer from an unrelated, already-torn-down test can also
    # fire during this gc.collect() (module-global _checked_out state is
    # shared across tests in this file) and call the SAME patched
    # get_pg_pool() with ITS OWN leftover connection — assert our specific
    # conn was returned exactly once, not that it's the pool's only call.
    our_calls = [c for c in pool.putconn.call_args_list if c.args[0] is conn]
    assert len(our_calls) == 1, f"expected exactly one putconn(conn) call, got {pool.putconn.call_args_list}"


def test_wrapper_finalizer_is_noop_after_explicit_close(monkeypatch):
    """An explicitly-closed (return_to_pool_on_close) wrapper must not
    ALSO return its connection a second time when later garbage
    collected — that would let two callers hold the same live connection
    concurrently."""
    import gc

    pool = MagicMock()
    conn = _FakeConn()
    pool.getconn.return_value = conn
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    wrapper = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=True)
    wrapper.close()
    our_calls = [c for c in pool.putconn.call_args_list if c.args[0] is conn]
    assert len(our_calls) == 1

    del wrapper
    gc.collect()

    # Still just the one call for OUR conn — the finalizer must not fire a
    # second putconn() for the same connection after an explicit close().
    our_calls = [c for c in pool.putconn.call_args_list if c.args[0] is conn]
    assert len(our_calls) == 1, f"expected exactly one putconn(conn) call, got {pool.putconn.call_args_list}"


def test_checked_out_drops_dead_threads_wrapper_without_explicit_release(monkeypatch):
    """Live incident: _checked_out was a plain list, so it held a permanent
    STRONG reference to every wrapper ever created — including threads that
    died naturally without ever calling release_pg_thread_connection() (the
    dominant case: anyio's HTTP threadpool prunes idle workers on its own
    schedule, and nothing in application code observes that). That
    permanent reference meant the wrapper's GC finalizer could never fire,
    no matter how thoroughly it was implemented — a real, measured case had
    27 wrappers in _checked_out while only 3 of their owning threads still
    existed. _checked_out must be a WeakSet so a dead thread's wrapper can
    actually be collected (and its connection returned) the moment nothing
    else references it, with no explicit release call required."""
    import gc
    import threading

    pool = MagicMock()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    def dies_without_releasing():
        pgc.get_pg_thread_connection()  # never calls release_pg_thread_connection()

    t = threading.Thread(target=dies_without_releasing)
    t.start()
    t.join()

    # CPython's refcounting drops the wrapper the instant the thread's
    # threading.local() slot is cleared on thread exit — no reference
    # cycle is involved here, so this doesn't even need a gc.collect() to
    # observe; the call below is defensive insurance, not load-bearing.
    gc.collect()

    assert len(pgc._checked_out) == 0, (
        "a WeakSet must drop the dead thread's wrapper once nothing else references it — "
        "if this is 1, _checked_out is (again) holding a strong reference and the leak is back"
    )


def test_get_pg_readonly_connection_is_fresh_each_call(monkeypatch):
    pool = MagicMock()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    a = pgc.get_pg_readonly_connection()
    b = pgc.get_pg_readonly_connection()
    assert a is not b
    assert pool.getconn.call_count == 2


def test_get_pg_readonly_connection_close_returns_to_pool(monkeypatch):
    pool = MagicMock()
    fake = _FakeConn()
    pool.getconn.side_effect = [fake]
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    con = pgc.get_pg_readonly_connection()
    con.close()
    pool.putconn.assert_called_once_with(fake)


def test_is_postgres_toggles_on_env(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    assert pgc.is_postgres() is False
    monkeypatch.setenv("METADATA_DSN", "postgresql://x/y")
    assert pgc.is_postgres() is True


def test_compat_row_supports_positional_and_named_access():
    """The seam exists so SQLite-shaped metadata SQL runs unchanged against
    Postgres. psycopg's plain ``dict_row`` broke half of that: ~10 modules
    do ``.fetchone()[0]`` / ``row[0]``, and every one raised ``KeyError: 0``
    under Postgres while passing on SQLite (where the suite runs). It
    surfaced live as the log_ingest cron failing every tick from
    ``cron/jobs/commit.py``'s ``.fetchone()[0]``, pinning the service to
    "degraded". ``sqlite3.Row`` allows both styles; so must this.
    """
    row = pgc._CompatRow(["count", "name"], [7, "svc-a"])

    # Positional — the access style that was broken.
    assert row[0] == 7
    assert row[1] == "svc-a"
    assert row[0:2] == (7, "svc-a")

    # Named — must keep working.
    assert row["count"] == 7
    assert row["name"] == "svc-a"
    assert row.get("count") == 7
    assert row.get("absent", "dflt") == "dflt"

    # Still a real mapping for callers that treat it as one.
    assert dict(row) == {"count": 7, "name": "svc-a"}
    assert list(row.keys()) == ["count", "name"]

    with pytest.raises(KeyError):
        row["nope"]
    with pytest.raises(IndexError):
        row[9]


def test_compat_row_factory_handles_no_description():
    """A statement with no result set (INSERT/DDL) has ``description is
    None``; the factory must not blow up building keys from it."""
    from unittest.mock import MagicMock

    cur = MagicMock()
    cur.description = None
    make = pgc._compat_row_factory(cur)
    assert make((1, 2)) == (1, 2)

    cur.description = [MagicMock(name="d1"), MagicMock(name="d2")]
    cur.description[0].name = "a"
    cur.description[1].name = "b"
    made = pgc._compat_row_factory(cur)((10, 20))
    assert made[0] == 10 and made["b"] == 20
