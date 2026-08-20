"""Tests for the persistent slow-query history layer.

The metadata helpers + cleanup are the boring parts; the interesting
contract is "query_registry.deregister persists ONLY queries above the
threshold and ONLY for queries with a service_id". A regression here
would silently fill (or fail to fill) the table — neither shows up in
a smoke test, so this file pins the exact triggering conditions.
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from backend.core import metadata as _meta
from backend.core import query_registry as qr_mod


@pytest.fixture
def svc_id(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.config.DATA_DIR", tmp_path)
    monkeypatch.setattr("backend.core.metadata.base.DATA_DIR", tmp_path, raising=False)
    # Set the slow-query threshold to a massive value by default so real queries executed
    # under test CPU starvation never get flagged as slow queries and pollute the database.
    monkeypatch.setattr("backend.core.query_registry._SLOW_QUERY_PERSIST_THRESHOLD_MS", 999999.0)
    svc = "test-slow-queries-svc"
    _meta.get_con(svc).execute("SELECT 1")  # trigger migrations
    return svc


@pytest.fixture
def fresh_registry(monkeypatch):
    """Use a fresh QueryRegistry so cross-test history doesn't bleed.
    The module-level singleton has shared state; replacing it here
    isolates each test."""
    new_reg = qr_mod.QueryRegistry()
    monkeypatch.setattr(qr_mod, "query_registry", new_reg)
    # The hot-path persist call resolves the registry via the import
    # cycle so any test using it must also see the same instance.
    return new_reg


# ── Metadata helpers ─────────────────────────────────────────────────────


def _row(*, qid: int, duration_ms: float, started: float | None = None, kind: str = "admin") -> dict:
    started = started if started is not None else time.time()
    return {
        "query_id": qid,
        "db_type": "DuckDB",
        "service_id": "test-slow-queries-svc",
        "started_at_utc": started,
        "ended_at_utc": started + duration_ms / 1000.0,
        "duration_ms": duration_ms,
        "outcome": "ok",
        "sql_preview": "SELECT 1",
        "sql_full": "SELECT 1",
        "sql_len": 8,
        "attr_kind": kind,
        "attr_label": f"{kind}: test",
        "attr_caller_qualname": "tests.helper",
        "attr_caller_file": "test_slow_queries_persist.py:1",
    }


def test_insert_then_list_round_trip(svc_id):
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=250))
    rows = _meta.list_slow_queries(svc_id, since_utc=time.time() - 60)
    assert len(rows) == 1
    assert rows[0]["query_id"] == 1
    assert rows[0]["duration_ms"] == 250
    assert rows[0]["attr_kind"] == "admin"


def test_list_filters_by_threshold(svc_id):
    """threshold_ms is applied SQL-side via the duration_ms index. Rows
    below the threshold must not appear in the result."""
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=120))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=600))
    _meta.insert_slow_query(svc_id, _row(qid=3, duration_ms=1500))
    rows = _meta.list_slow_queries(svc_id, since_utc=time.time() - 60, threshold_ms=500)
    qids = {r["query_id"] for r in rows}
    assert qids == {2, 3}


def test_list_filters_by_kind(svc_id):
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=200, kind="admin"))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=200, kind="cron"))
    rows = _meta.list_slow_queries(svc_id, since_utc=time.time() - 60, kind="cron")
    assert [r["query_id"] for r in rows] == [2]


def test_list_default_sort_is_recent_first(svc_id):
    now = time.time()
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=200, started=now - 30))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=200, started=now - 10))
    rows = _meta.list_slow_queries(svc_id, since_utc=now - 60)
    assert [r["query_id"] for r in rows] == [2, 1]


def test_list_sort_by_duration_flips_order(svc_id):
    now = time.time()
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=2000, started=now - 30))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=300, started=now - 10))
    rows = _meta.list_slow_queries(svc_id, since_utc=now - 60, sort_by_duration=True)
    assert [r["query_id"] for r in rows] == [1, 2]


def test_purge_old_uses_epoch_cutoff(svc_id):
    """``started_at_utc`` is a unix-epoch REAL, not an ISO string. The
    cleanup pass MUST compare to an epoch value — using ``datetime(
    'now', '-Nd')`` (the path used by the other tables) silently
    matches nothing on this table."""
    now = time.time()
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=200, started=now - 86400 * 8))  # 8d old
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=200, started=now - 3600))  # 1h old
    n = _meta.purge_old_slow_queries(svc_id, older_than_utc=now - 86400 * 7)
    assert n == 1
    rows = _meta.list_slow_queries(svc_id, since_utc=now - 86400 * 30)
    assert [r["query_id"] for r in rows] == [2]


def test_count_matches_list_length(svc_id):
    """count_slow_queries is the cheap row count for the overview card.
    It must agree with list_slow_queries under the same filters."""
    now = time.time()
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=120, started=now - 100))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=600, started=now - 50))
    _meta.insert_slow_query(svc_id, _row(qid=3, duration_ms=2000, started=now - 10))
    assert _meta.count_slow_queries(svc_id, since_utc=now - 200, threshold_ms=500) == 2
    rows = _meta.list_slow_queries(svc_id, since_utc=now - 200, threshold_ms=500)
    assert len(rows) == 2


# ── Writer hook in query_registry ────────────────────────────────────────


def test_deregister_persists_only_above_threshold(svc_id, fresh_registry, monkeypatch):
    """The hot-path filter: queries faster than the persist threshold
    must NOT write to SQLite. Otherwise the per-tick cron noise (sub-ms
    SQLite queries by the dozen) would flood the table — exactly the
    motivation for having a threshold."""
    monkeypatch.setattr(qr_mod, "_SLOW_QUERY_PERSIST_THRESHOLD_MS", 100.0)
    fast_qid = fresh_registry.register("DuckDB", "SELECT 1", service_id=svc_id)
    # Pretend the query took 50 ms — under threshold.
    fresh_registry._queries[fast_qid].started_at_mono = time.monotonic() - 0.05
    fresh_registry.deregister(fast_qid)
    assert _meta.count_slow_queries(svc_id, since_utc=time.time() - 60) == 0


def test_deregister_persists_slow_query(svc_id, fresh_registry, monkeypatch):
    monkeypatch.setattr(qr_mod, "_SLOW_QUERY_PERSIST_THRESHOLD_MS", 100.0)
    slow_qid = fresh_registry.register("DuckDB", "SELECT pg_sleep(1)", service_id=svc_id)
    # Pretend the query took 500 ms — over threshold.
    fresh_registry._queries[slow_qid].started_at_mono = time.monotonic() - 0.5
    fresh_registry.deregister(slow_qid)
    rows = _meta.list_slow_queries(svc_id, since_utc=time.time() - 60)
    assert len(rows) == 1
    assert rows[0]["duration_ms"] >= 100
    assert rows[0]["sql_preview"] == "SELECT pg_sleep(1)"


def test_deregister_skips_when_no_service_id(svc_id, fresh_registry, monkeypatch):
    """System-level queries with no service_id have nowhere to land in
    the per-service metadata DB. They MUST be skipped, not crashed on."""
    monkeypatch.setattr(qr_mod, "_SLOW_QUERY_PERSIST_THRESHOLD_MS", 100.0)
    qid = fresh_registry.register("DuckDB", "SELECT 1", service_id=None)
    fresh_registry._queries[qid].started_at_mono = time.monotonic() - 0.5
    fresh_registry.deregister(qid)  # must not raise
    # Nothing landed in this service's db.
    assert _meta.count_slow_queries(svc_id, since_utc=time.time() - 60) == 0


def test_deregister_persist_failure_does_not_propagate(svc_id, fresh_registry, monkeypatch):
    """A SQLite write failure on the persist path MUST NOT propagate
    back into the SQL hot path. Observability is best-effort —
    correctness is not."""
    monkeypatch.setattr(qr_mod, "_SLOW_QUERY_PERSIST_THRESHOLD_MS", 100.0)

    def _boom(*_a, **_kw):
        raise sqlite3.OperationalError("simulated metadata DB outage")

    monkeypatch.setattr(qr_mod._persist_slow_query.__globals__["__name__"], None, raising=False)  # noqa: SLF001 — defensive no-op
    # Monkeypatch the metadata helper used inside _persist_slow_query.
    monkeypatch.setattr("backend.core.metadata.insert_slow_query", _boom)
    qid = fresh_registry.register("DuckDB", "SELECT 1", service_id=svc_id)
    fresh_registry._queries[qid].started_at_mono = time.monotonic() - 0.5
    # The deregister call must complete cleanly even though the persist
    # raises internally.
    fresh_registry.deregister(qid)
    # And no row landed.
    assert _meta.count_slow_queries(svc_id, since_utc=time.time() - 60) == 0
