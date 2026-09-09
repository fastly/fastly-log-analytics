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
