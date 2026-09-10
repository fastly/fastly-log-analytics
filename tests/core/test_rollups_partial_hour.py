import os
import time
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from backend.core.rollups import partial_hour as ph


def _make_source(tmp_path):
    return {"service_id": "svc-a", "name": "svc-a", "cache_dir_override": str(tmp_path)}


@pytest.fixture(autouse=True)
def _patch_cache_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: str(tmp_path / src["name"]))
    yield


def test_watermark_defaults_to_zero(tmp_path):
    src = _make_source(tmp_path)
    assert ph.read_partial_hour_watermark(src, "2026-09-09-14") == 0.0


def test_all_fields_defaults_to_empty(tmp_path):
    src = _make_source(tmp_path)
    assert ph.read_partial_hour_all_fields(src, "2026-09-09-14") == []


def test_total_defaults_to_zero(tmp_path):
    src = _make_source(tmp_path)
    assert ph.read_partial_hour_total(src, "2026-09-09-14") == 0


def _write_hourly_parquet(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        # DuckDB's session TimeZone defaults to the OS locale, not UTC — on a
        # non-UTC dev machine, binding a tz-aware Python datetime into this
        # naive TIMESTAMP column would silently localize it instead of
        # storing the UTC wall-clock value. Match the codebase's own
        # connection convention (backend/core/duckdb.py, recompute.py) so
        # this fixture's data means what the assertions below assume.
        con.execute("SET TimeZone='UTC';")
        con.execute("CREATE TABLE t (timestamp TIMESTAMP, country VARCHAR)")
        con.executemany("INSERT INTO t VALUES (?, ?)", rows)
        con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _write_multi_field_hourly_parquet(path, rows):
    """rows: list of (timestamp, country, method) tuples."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone='UTC';")
        con.execute("CREATE TABLE t (timestamp TIMESTAMP, country VARCHAR, method VARCHAR)")
        con.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
        con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


def test_merge_partial_hour_writes_a_total_row_not_per_field_summable(tmp_path):
    """C1 (final whole-branch review, empirically verified): summing
    read_partial_hour_all_fields's rows across every populated field counts
    each request once PER POPULATED FIELD, not once. merge_partial_hour must
    write a separate, correct total that survives this trap."""
    src = _make_source(tmp_path)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    cache_dir = str(tmp_path / "svc-a")
    hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")

    now = datetime.now(UTC)
    # 3 requests, each with BOTH country and method populated.
    _write_multi_field_hourly_parquet(
        os.path.join(hourly_dir, "batch1.parquet"),
        [(now, "US", "GET"), (now, "US", "POST"), (now, "CA", "GET")],
    )

    stats = ph.merge_partial_hour("svc-a", src, ["country", "method"])
    assert stats["new_files"] == 1

    # The trap: summing per-field rows gives 3 (country) + 3 (method) = 6,
    # double the real request count.
    per_field_rows = ph.read_partial_hour_all_fields(src, active_hour)
    assert sum(c for _, _, c in per_field_rows) == 6, (
        "sanity check: summing per-field rows should reproduce the trap this test guards against"
    )

    # The real total, read correctly, is 3 — not subject to per-field
    # summation, and not one of the rows read_partial_hour_all_fields
    # returns (it must not leak the synthetic __total__ field into the
    # per-field result either).
    assert ph.read_partial_hour_total(src, active_hour) == 3
    assert all(field != ph.TOTAL_FIELD for field, _, _ in per_field_rows)


def test_read_partial_hour_functions_reuse_caller_connection(tmp_path, monkeypatch):
    """I3 (final whole-branch review): read_partial_hour_all_fields /
    read_partial_hour_total must reuse the caller's DuckDB connection when
    given one, instead of opening a fresh ``:memory:`` connection per call
    (measured ~16ms overhead per call, paid 3x per dashboard request on the
    hot path this whole feature exists to speed up)."""
    src = _make_source(tmp_path)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    cache_dir = str(tmp_path / "svc-a")
    hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")
    now = datetime.now(UTC)
    _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "CA")])
    ph.merge_partial_hour("svc-a", src, ["country"])

    real_connect = duckdb.connect
    connect_calls = []

    def _tracking_connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return real_connect(*args, **kwargs)

    caller_con = duckdb.connect(":memory:")
    # Patch the shared `duckdb` module's `connect` AFTER opening the
    # caller's own connection (so that call isn't itself counted), then any
    # `import duckdb; duckdb.connect(...)` inside partial_hour.py's fallback
    # branch — which must NOT run when `con` is supplied — would show up
    # here too.
    monkeypatch.setattr(duckdb, "connect", _tracking_connect)
    try:
        rows = ph.read_partial_hour_all_fields(src, active_hour, con=caller_con)
        total = ph.read_partial_hour_total(src, active_hour, con=caller_con)
    finally:
        caller_con.close()

    assert dict((v, c) for _, v, c in rows) == {"US": 1, "CA": 1}
    assert total == 2
    assert connect_calls == [], (
        f"expected no fresh duckdb.connect() calls when a connection is supplied, got {connect_calls}"
    )


def test_merge_partial_hour_total_accumulates_across_ticks(tmp_path):
    src = _make_source(tmp_path)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    cache_dir = str(tmp_path / "svc-a")
    hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")
    now = datetime.now(UTC)

    _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "CA")])
    ph.merge_partial_hour("svc-a", src, ["country"])
    assert ph.read_partial_hour_total(src, active_hour) == 2

    time.sleep(0.05)
    _write_hourly_parquet(os.path.join(hourly_dir, "batch2.parquet"), [(now, "US")])
    ph.merge_partial_hour("svc-a", src, ["country"])
    assert ph.read_partial_hour_total(src, active_hour) == 3

    # No-op tick leaves the total unchanged.
    ph.merge_partial_hour("svc-a", src, ["country"])
    assert ph.read_partial_hour_total(src, active_hour) == 3


def test_merge_partial_hour_is_incremental_and_idempotent(tmp_path):
    src = _make_source(tmp_path)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    cache_dir = str(tmp_path / "svc-a")
    hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")

    now = datetime.now(UTC)
    _write_hourly_parquet(
        os.path.join(hourly_dir, "batch1.parquet"),
        [(now, "US"), (now, "US"), (now, "CA")],
    )

    stats1 = ph.merge_partial_hour("svc-a", src, ["country"])
    assert stats1["new_files"] == 1
    rows = dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour))
    assert rows == {"US": 2, "CA": 1}

    # Re-running with no new files is a cheap no-op — result unchanged.
    stats2 = ph.merge_partial_hour("svc-a", src, ["country"])
    assert stats2["new_files"] == 0
    rows_again = dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour))
    assert rows_again == {"US": 2, "CA": 1}

    # A second batch of NEW rows merges in additively — not a full
    # recompute (batch1 is not re-read: the watermark already passed it).
    time.sleep(0.05)
    _write_hourly_parquet(
        os.path.join(hourly_dir, "batch2.parquet"),
        [(now, "US"), (now, "DE")],
    )
    stats3 = ph.merge_partial_hour("svc-a", src, ["country"])
    assert stats3["new_files"] == 1
    rows_final = dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour))
    assert rows_final == {"US": 3, "CA": 1, "DE": 1}


def test_merge_partial_hour_excludes_rows_before_hour_start(tmp_path):
    """Regression test for the hour-boundary WHERE-clause fix.

    A buffer/active-hour-partition parquet is included by FILE mtime, not
    by row-level timestamp certainty (see _list_new_source_files) — a file
    finalized right at the hour boundary can straddle it, holding some rows
    from the previous hour alongside this hour's rows. merge_partial_hour's
    merge SQL must filter those straddling rows out via an explicit
    ``timestamp >= hour_start AND timestamp < hour_end`` predicate. If that
    predicate ever regressed to an unfiltered ``1=1``, this test would be
    the one to catch it: it plants one row timestamped a few seconds BEFORE
    the active hour's start (deliberately not "now", which is always inside
    the active hour and so can never exercise the filter) inside the SAME
    file as an in-hour row, then asserts the pre-hour row's value never
    appears in the persisted rollup.
    """
    src = _make_source(tmp_path)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    cache_dir = str(tmp_path / "svc-a")
    hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")

    hour_start = datetime.strptime(active_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    just_before_hour = hour_start - timedelta(seconds=5)
    inside_hour = datetime.now(UTC)

    _write_hourly_parquet(
        os.path.join(hourly_dir, "straddling.parquet"),
        [(just_before_hour, "PREV_HOUR_ONLY"), (inside_hour, "US")],
    )

    stats = ph.merge_partial_hour("svc-a", src, ["country"])
    assert stats["new_files"] == 1

    rows = dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour))
    assert "PREV_HOUR_ONLY" not in rows
    assert rows == {"US": 1}


def test_gc_stale_partial_hours_removes_old_but_not_active(tmp_path):
    src = _make_source(tmp_path)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    stale_hour = (datetime.now(UTC) - timedelta(hours=5)).strftime("%Y-%m-%d-%H")

    os.makedirs(ph.partial_hour_dir(src, active_hour))
    os.makedirs(ph.partial_hour_dir(src, stale_hour))

    removed = ph.gc_stale_partial_hours(src, max_age_hours=2)

    assert removed == 1
    assert os.path.isdir(ph.partial_hour_dir(src, active_hour))
    assert not os.path.isdir(ph.partial_hour_dir(src, stale_hour))


def test_gc_stale_partial_hours_ignores_malformed_hour_token(tmp_path):
    """A dir whose ``hour=`` suffix isn't a parseable token must be left
    alone (and must not raise) — parse_hour_token(...) is None short-
    circuits before the age comparison ever runs on it."""
    src = _make_source(tmp_path)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    stale_hour = (datetime.now(UTC) - timedelta(hours=5)).strftime("%Y-%m-%d-%H")
    malformed_dir = os.path.join(ph.partial_hour_root(src), "hour=garbage")

    os.makedirs(ph.partial_hour_dir(src, active_hour))
    os.makedirs(ph.partial_hour_dir(src, stale_hour))
    os.makedirs(malformed_dir)

    removed = ph.gc_stale_partial_hours(src, max_age_hours=2)

    assert removed == 1
    assert os.path.isdir(ph.partial_hour_dir(src, active_hour))
    assert not os.path.isdir(ph.partial_hour_dir(src, stale_hour))
    assert os.path.isdir(malformed_dir)
