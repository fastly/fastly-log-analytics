"""Postgres metadata backend: dialect shim + connection lifecycle.

No live Postgres needed — these pin the SQL rewriting and the
get/put-conn lifecycle against a fake pool/cursor. The one thing they
deliberately do NOT cover is whether the rewritten SQL actually executes
against a real Postgres server; that's the manual verification step in
the migration runbook.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from backend.core import metadata as metadata_db
from backend.core.metadata import pg_connection as pgc
from backend.core.metadata import reconciliation
from backend.core.metadata import usage_log_db as _usage_log_db
from backend.utils.date_utils import iso_z

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
        ("sources", None),
        ("rdns", None),
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
# Direct wrappers own a fake pool: exception tracebacks can delay their GC
# until another test has patched the global pool resolver.


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
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
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
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
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
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())

    with patch.object(query_registry.query_registry, "deregister") as mock_deregister:
        with pytest.raises(RuntimeError, match="boom"):
            wrapper.execute("SELECT 1")

    assert mock_deregister.call_count == 1
    _qid, kwargs = mock_deregister.call_args
    assert isinstance(kwargs.get("error"), RuntimeError)


def test_wrapper_execute_cron_runs_returns_lastrowid():
    conn = _FakeConn(fetchone_result={"id": 42})
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
    cur = wrapper.execute("INSERT INTO cron_runs (task) VALUES (?)", ("log_discovery",))
    assert cur.lastrowid == 42
    sql, _ = conn._cursor.executed[0]
    assert "RETURNING id" in sql


def test_wrapper_execute_non_cron_runs_no_lastrowid():
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
    cur = wrapper.execute("INSERT INTO ingested_files (a) VALUES (?)", (1,))
    assert cur.lastrowid is None


def test_wrapper_commit_rollback_are_noops_under_autocommit():
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
    wrapper.commit()  # must not raise
    wrapper.rollback()  # must not raise


def test_wrapper_close_returns_to_pool_only_when_flagged(monkeypatch):
    conn = _FakeConn()
    pool = MagicMock()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    long_lived = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=False, pool=pool)
    long_lived.close()
    pool.putconn.assert_not_called()

    checked_out = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=True, pool=pool)
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

    # An unrelated wrapper can finalize while get_pg_pool is patched.
    unrelated = pgc.PgConnectionWrapper(_FakeConn())
    unrelated._finalizer()

    pgc.release_pg_thread_connection()  # only this thread's (conns[0])
    owned_returns = [call.args[0] for call in pool.putconn.call_args_list if call.args[0] in conns]
    assert owned_returns == [conns[0]]
    assert other_thread_wrapper["w"]._returned == [False]

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
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    wrapper = pgc.PgConnectionWrapper(conn, pool=pool)
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
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    wrapper = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=True, pool=pool)
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


# ── Metadata reconciliation (storage stats + cleanup) ──────────────────────


def _con(service_id: str):
    return metadata_db.get_con(service_id)


def _seed_usage_log(service_id: str, rows: int, days_ago: int = 0) -> None:
    """Insert ``rows`` usage_log entries dated ``days_ago`` in the past.

    ``usage_log`` lives in its own per-service database; seed
    via :func:`backend.core.metadata.usage_log_db.get_con` so the row
    counts/cleanup paths see them.
    """
    con = _usage_log_db.get_con(service_id)
    now_dt = datetime.now(UTC) - timedelta(days=days_ago)
    now_ts = iso_z(now_dt)
    hour = now_ts[:13]
    con.executemany(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, bytes, count) "
        "VALUES (?, ?, 'A', 'PUT_OBJECT', 0, 1)",
        [(now_ts, service_id) for _ in range(rows)],
    )
    con.execute(
        """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
           VALUES (?, ?, 'A', 'PUT_OBJECT', ?, 0, ?)
           ON CONFLICT (service_id, hour, operation_class, operation_type)
           DO UPDATE SET count = usage_log_hourly_summary.count + EXCLUDED.count, last_updated = EXCLUDED.last_updated""",
        (service_id, hour, rows, now_ts),
    )
    con.commit()


def _seed_ingested_file(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = _con(service_id)
    con.executemany(
        "INSERT OR IGNORE INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        f"VALUES (?, 'fos', datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"raw/{days_ago}d-{i}.gz",) for i in range(rows)],
    )
    con.commit()


def _seed_cron_run(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = _con(service_id)
    con.executemany(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
        f"VALUES ('sync', datetime('now', '-{days_ago} days'), 1.0, 'success', '[]')",
        [() for _ in range(rows)],
    )
    con.commit()


# ── get_metadata_storage_stats ────────────────────────────────────────────────


def test_storage_stats_returns_zero_rows_on_fresh_db():
    sid = "svc-stats-fresh"
    stats = reconciliation.get_metadata_storage_stats(sid)
    assert "tables" in stats
    # All expected tables are present in the schema (initialised by get_con).
    for table in ("usage_log", "ingested_files", "cron_runs"):
        assert table in stats["tables"]
        assert stats["tables"][table]["rows"] == 0
    assert stats["db_bytes"] is None
    assert stats["db_path"] is None


def test_storage_stats_counts_seeded_rows():
    sid = "svc-stats-seeded"
    _seed_usage_log(sid, 7)
    _seed_ingested_file(sid, 3)
    _seed_cron_run(sid, 2)
    stats = reconciliation.get_metadata_storage_stats(sid)
    assert stats["tables"]["usage_log"]["rows"] == 7
    assert stats["tables"]["ingested_files"]["rows"] == 3
    assert stats["tables"]["cron_runs"]["rows"] == 2


def test_stats_tables_sql_names_all_exist_in_schema():
    """Regression guard for the silent-drop bug.

    Every ``sql_table`` in ``_STATS_TABLES`` must be a real table in the
    metadata schema. Four entries previously named non-existent tables
    (``saved_views``/``audit_log``/``in_flight_buffers``/
    ``locally_compacted_files``); ``get_metadata_storage_stats`` swallowed
    the resulting ``OperationalError`` and silently dropped those four rows
    from the admin storage panel. Pin every name against ``sqlite_master``
    so a future rename can't regress the same way. ``usage_log`` lives in a
    separate per-service file and is exempt.
    """
    sid = "svc-stats-schema"
    con = _con(sid)  # initialises base schema + pending migrations
    existing = {
        r[0] for r in con.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()
    }
    for sql_table, _out_key in reconciliation._STATS_TABLES:
        if sql_table == "usage_log":
            continue
        assert sql_table in existing, f"_STATS_TABLES names missing table: {sql_table}"


def test_storage_stats_reports_reference_tables_under_ui_keys():
    """The reference tables surface under their UI-facing keys. Each table
    exists on a fresh DB, so each key must be present with rows >= 0 — not absent."""
    sid = "svc-stats-refs"
    stats = reconciliation.get_metadata_storage_stats(sid)
    for ui_key in ("saved_views", "audit_log", "in_flight_buffers"):
        assert ui_key in stats["tables"], f"reference table dropped from stats: {ui_key}"
        assert stats["tables"][ui_key]["rows"] == 0


# ── is_ingested_files_dedup_active ───────────────────────────────────────────


def test_dedup_active_default_when_no_config():
    # Service has no config file → defaults to "safe to trim" (True).
    assert reconciliation.is_ingested_files_dedup_active("svc-no-cfg") is True


def test_dedup_active_when_delete_after_true(monkeypatch):
    sid = "svc-delete-after-true"

    def _fake_load_config(s: str):
        return {"provisioning": {"cron_sync": {"delete_after": True}}}

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", _fake_load_config)
    assert reconciliation.is_ingested_files_dedup_active(sid) is True


def test_dedup_active_returns_false_when_delete_after_false(monkeypatch):
    sid = "svc-delete-after-false"

    def _fake_load_config(s: str):
        return {"provisioning": {"cron_sync": {"delete_after": False}}}

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", _fake_load_config)
    # Returns False — i.e. ``ingested_files`` is the dedup gate, must not
    # be trimmed.
    assert reconciliation.is_ingested_files_dedup_active(sid) is False


def test_dedup_active_returns_false_for_high_scale_shared_source_even_with_delete_after_true(monkeypatch):
    """A high-scale shared source forces raw deletion off regardless of
    ``delete_after`` — since raw files persist, ``ingested_files`` remains
    the only thing stopping full_sync from re-ingesting everything."""
    sid = "svc-high-scale-shared"

    def _fake_load_config(s: str):
        return {
            "provisioning": {"cron_sync": {"delete_after": True, "high_scale_shared_source": True}},
        }

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", _fake_load_config)
    assert reconciliation.is_ingested_files_dedup_active(sid) is False


# ── cleanup_metadata ─────────────────────────────────────────────────────────


def test_cleanup_deletes_aged_usage_log_rows():
    sid = "svc-cleanup-aged"
    _seed_usage_log(sid, 5, days_ago=10)
    _seed_usage_log(sid, 3, days_ago=0)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 7})

    assert result["deleted"]["usage_log"] == 5
    assert result["after"]["usage_log"] == 3
    # Vacuum is a SQLite-file concept skipped entirely under Postgres (see
    # test_cleanup_skips_vacuum_under_postgres) — Postgres reclaims space via
    # its own autovacuum daemon, outside application control.
    assert result["vacuumed"] is False
    assert result["duration_s"] >= 0


def test_cleanup_zero_retention_disables_table():
    sid = "svc-cleanup-zero"
    _seed_usage_log(sid, 5, days_ago=10)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 0})

    # Retention=0 → skip deletion for this table, nothing trimmed.
    assert result["deleted"]["usage_log"] == 0
    assert result["after"]["usage_log"] == 5
    assert result["vacuumed"] is False  # No deletes → no VACUUM


def test_cleanup_uses_default_retention_when_key_missing():
    sid = "svc-cleanup-default"
    # No retention passed → defaults apply. DEFAULT_METADATA_RETENTION
    # picks safe positive values, so rows older than that get trimmed.
    _seed_cron_run(sid, 4, days_ago=400)  # well past any reasonable default
    result = reconciliation.cleanup_metadata(sid)
    assert result["deleted"]["cron_runs"] == 4


def test_cleanup_emits_progress_events():
    sid = "svc-cleanup-events"
    _seed_usage_log(sid, 2, days_ago=10)
    events: list[dict] = []

    def _on_event(e: dict) -> None:
        events.append(e)

    reconciliation.cleanup_metadata(
        sid,
        retention={"usage_log_days": 1, "ingested_files_days": 1, "cron_runs_days": 1},
        on_event=_on_event,
    )

    # At least one status event AND at least one progress event fired.
    assert any(e["type"] == "status" for e in events)
    assert any(e["type"] == "progress" for e in events)
    # Final progress event hits the total step count.
    last_progress = [e for e in events if e["type"] == "progress"][-1]
    assert last_progress["current"] == last_progress["total"]


def test_cleanup_force_disables_ingested_files_when_delete_after_false(monkeypatch):
    sid = "svc-cleanup-forced-off"
    _seed_ingested_file(sid, 5, days_ago=400)

    from backend import config as svcconfig

    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda s: {"provisioning": {"cron_sync": {"delete_after": False}}},
    )

    events: list[dict] = []
    result = reconciliation.cleanup_metadata(
        sid,
        retention={"ingested_files_days": 30},  # Caller wants trim
        on_event=events.append,
    )

    # Forced override: nothing deleted from ingested_files.
    assert result["deleted"]["ingested_files"] == 0
    assert result["after"]["ingested_files"] == 5
    # The override surfaces as a status event so the operator sees why.
    override_msgs = [e for e in events if e["type"] == "status" and "dedup gate" in e.get("message", "")]
    assert override_msgs, "expected status event explaining the override"


def test_cleanup_trims_rum_ingested_files_past_raw_retention_window():
    """RUM raw objects are deleted inline as they go (gap 2), and any age-based
    backstop deletes within log_retention_days (default 90d). Metadata cleanup
    trims RUM ingested_files older than that window so ingested_files does not
    grow without limit."""
    sid = "svc-cleanup-rum-dedup"
    _seed_ingested_file(sid, 3, days_ago=400)
    con = _con(sid)
    # Seed 4 RUM files 400 days ago (past the 91-day raw retention window)
    # and 2 RUM files 10 days ago (within the 91-day window)
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'fos', ?, datetime('now', '-400 days'), 1, 100)",
        [(f"raw/rum/old-{t}-{i}.gz", t) for t in ("client_vitals", "client_errors") for i in range(2)],
    )
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'fos', ?, datetime('now', '-10 days'), 1, 100)",
        [(f"raw/rum/recent-{t}.gz", t) for t in ("client_vitals", "client_errors")],
    )
    con.commit()

    result = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 1})

    # 3 request files (400d old, cutoff 1d) + 4 RUM files (400d old, cutoff 91d) = 7 deleted
    assert result["deleted"]["ingested_files"] == 7
    # 2 recent RUM files (10d old, within 91d window) survive
    assert result["after"]["ingested_files"] == 2
    remaining = [
        r["file_name"]
        for r in _con(sid).execute("SELECT file_name FROM ingested_files WHERE source_name = 'fos'").fetchall()
    ]
    assert all("recent-" in fn for fn in remaining)


def test_cleanup_rum_trimming_causes_no_reingest(monkeypatch):
    """Gap 6 & Gap 2: Proving that trimming aged RUM ingested_files rows does not
    cause re-ingest because raw objects were deleted inline and discovery is incremental."""
    from unittest.mock import MagicMock

    from backend.core.rum_ingest import ingest_rum_logs

    sid = "svc-cleanup-no-reingest"
    con = _con(sid)
    # 1. File was previously ingested 400 days ago
    old_file_key = "s3://test-bucket/raw/rum/year=2025/month=08/day=01/hour=12/minute=00/old.json.gz"
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'test_service', 'client_vitals', datetime('now', '-400 days'), 1, 100)",
        (old_file_key,),
    )
    con.commit()

    # 2. Cleanup runs and trims the 400d old record
    reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 1})
    assert con.execute("SELECT COUNT(*) FROM ingested_files WHERE file_name = ?", (old_file_key,)).fetchone()[0] == 0

    # 3. Next rum_sync runs against FOS:
    # Per the Gap 2 contract, old.json.gz was deleted upon ingest, so FOS has no such file.
    # Furthermore, incremental minute-prefix listing only scans recent minutes.
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": []}]
    mock_s3.get_paginator.return_value = mock_paginator

    monkeypatch.setattr("backend.core.rum_ingest._get_fos_client", lambda src: mock_s3)
    monkeypatch.setattr(
        "backend.core.rum_ingest.get_source_for_service",
        lambda s: {"name": sid, "bucket": "test-bucket", "service_id": sid},
    )
    monkeypatch.setattr("backend.core.metadata.get_con", lambda s: con)
    monkeypatch.setattr("backend.core.rum_ingest.start_cron_run", lambda s, t: 1)
    monkeypatch.setattr("backend.core.rum_ingest.log_cron_run", lambda *a, **kw: None)
    monkeypatch.setattr("backend.core.rum_ingest.finalize_cron_run_if_running", lambda *a, **kw: None)

    events = list(ingest_rum_logs(sid))
    # Must report 0 rows ingested — NO re-ingest!
    assert events == [("started", 1), ("done", 0)]
    assert con.execute("SELECT COUNT(*) FROM ingested_files").fetchone()[0] == 0


def test_cleanup_on_event_callback_failure_does_not_abort():
    sid = "svc-cleanup-bad-callback"
    _seed_usage_log(sid, 3, days_ago=10)

    def _bad_callback(e: dict) -> None:
        raise RuntimeError("callback fail")

    # Must not raise — the implementation swallows callback errors so a
    # buggy SSE consumer can't break the cleanup itself.
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1}, on_event=_bad_callback)
    assert result["deleted"]["usage_log"] == 3


def test_cleanup_negative_retention_is_treated_as_disabled():
    sid = "svc-cleanup-neg"
    _seed_usage_log(sid, 5, days_ago=400)
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": -5})
    # Negative → skipped (same as 0).
    assert result["deleted"]["usage_log"] == 0


def test_cleanup_non_int_retention_falls_back_to_disabled():
    sid = "svc-cleanup-bad-type"
    _seed_usage_log(sid, 3, days_ago=400)
    # Garbage value → coerce-fails → 0 days → skip.
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": "not-a-number"})
    assert result["deleted"]["usage_log"] == 0


def test_postgres_retention_casts_text_timestamps():
    assert reconciliation._retention_timestamp_expr("ingested_at") == "CAST(ingested_at AS TIMESTAMPTZ)"
    assert reconciliation._retention_timestamp_expr("started_at") == "CAST(started_at AS TIMESTAMPTZ)"


def test_cleanup_rollups_skipped_when_rollups_days_zero():
    sid = "svc-cleanup-no-rollups"
    _seed_usage_log(sid, 1, days_ago=400)
    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1, "rollups_days": 0})
    assert result["rollups_deleted"] == 0


def test_cleanup_skips_vacuum_under_postgres():
    """auto_vacuum/incremental_vacuum/freelist_count are SQLite-file
    concepts with no Postgres equivalent (Postgres reclaims space via its
    own autovacuum daemon). Under the (now sole) Postgres metadata backend
    the DELETE trim must still run, but the vacuum branch must be skipped
    rather than executing PRAGMA statements a Postgres connection can't run.
    """
    sid = "svc-cleanup-pg-skip"
    _seed_usage_log(sid, 5, days_ago=10)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 7})

    assert result["deleted"]["usage_log"] == 5
    assert result["vacuumed"] is False


def test_cleanup_rollups_skipped_when_source_missing(monkeypatch):
    sid = "svc-cleanup-no-src"
    _seed_usage_log(sid, 1, days_ago=400)

    from backend.core import duckdb as _db

    monkeypatch.setattr(_db, "get_source_for_service", lambda s: None)

    result = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1, "rollups_days": 7})
    assert result["rollups_deleted"] == 0


# ── ingested_files_summary drift fix ─────────────────────────────────────────


def _seed_ingested_file_for(service_id: str, rows: int, days_ago: int = 0) -> None:
    """Like ``_seed_ingested_file`` but stamps ``source_name=service_id``."""
    con = _con(service_id)
    con.executemany(
        "INSERT OR IGNORE INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        f"VALUES (?, ?, datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"raw/{service_id}-{days_ago}d-{i}.gz", service_id) for i in range(rows)],
    )
    con.commit()


def _poison_summary(service_id: str, *, file_count: int, total_rows: int, total_bytes: int) -> None:
    """Force ``ingested_files_summary`` to a value that does NOT match the
    table — simulates the cumulative-ever drift the retention DELETE used to
    cause (it never decremented the incrementally-maintained rollup)."""
    con = _con(service_id)
    con.execute(
        "INSERT INTO ingested_files_summary "
        "(source_name, file_count, total_rows, total_bytes, count_with_bytes, latest_file_name, last_ingested) "
        "VALUES (?, ?, ?, ?, ?, 'raw/poison.gz', datetime('now')) "
        "ON CONFLICT(source_name) DO UPDATE SET "
        "file_count=excluded.file_count, total_rows=excluded.total_rows, "
        "total_bytes=excluded.total_bytes, count_with_bytes=excluded.count_with_bytes",
        (service_id, file_count, total_rows, total_bytes, file_count),
    )
    con.commit()


def test_cleanup_recomputes_ingested_files_summary_after_trim():
    """The retention DELETE on ingested_files must leave the summary rollup
    consistent with the surviving rows, not the pre-trim (drifted) totals."""
    sid = "svc-rollup-recompute"
    _seed_ingested_file_for(sid, 5, days_ago=10)  # aged out → trimmed
    _seed_ingested_file_for(sid, 3, days_ago=0)  # recent → survive
    # Drifted rollup far larger than the table (the bug we're fixing).
    _poison_summary(sid, file_count=999_999, total_rows=999_999, total_bytes=99_999_900)

    result = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})

    assert result["deleted"]["ingested_files"] == 5
    assert result["after"]["ingested_files"] == 3

    # Rollup recomputed to the surviving rows (row_count=1, file_size=100 each).
    summary = metadata_db.get_ingested_files_status_summary(sid)
    assert summary["file_count"] == 3
    assert summary["total_rows"] == 3
    assert summary["total_bytes"] == 300


def test_cleanup_skips_summary_recompute_when_nothing_trimmed():
    """The recompute is gated on an actual deletion: when no ingested_files
    rows are trimmed (all recent / delete_after=false), the rollup is left
    untouched so delete_after=false services don't pay a daily full rescan."""
    sid = "svc-rollup-noop"
    _seed_ingested_file_for(sid, 3, days_ago=0)  # all recent → none trimmed at 7d
    _poison_summary(sid, file_count=42, total_rows=4242, total_bytes=4200)

    result = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})
    assert result["deleted"]["ingested_files"] == 0

    # Untouched — no deletion means no recompute.
    summary = metadata_db.get_ingested_files_status_summary(sid)
    assert summary["file_count"] == 42
    assert summary["total_rows"] == 4242


def test_storage_stats_report_postgres_relation_sizes_for_every_table():
    sid = "svc-stats-bytes"
    _seed_usage_log(sid, 2)
    _seed_cron_run(sid, 1)
    stats = reconciliation.get_metadata_storage_stats(sid, force=True)
    for table in ("usage_log", "cron_runs", "ingested_files"):
        size = stats["tables"][table]["bytes"]
        assert isinstance(size, int) and size >= 0, f"{table} bytes should be a pg_total_relation_size int"
    assert stats["tables"]["cron_runs"]["bytes"] > 0
    assert stats["db_bytes"] is None
