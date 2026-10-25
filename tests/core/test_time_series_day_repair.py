"""Scoped day repair must rebuild raw hours, never compact retired remnants."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core.rollups import day_bundles, time_series
from backend.models.admin import RebuildTimeSeriesRollupsResponse
from backend.routers.admin import compaction as api
from tests._helpers.rollups import writer_patches

DAY = "2020-01-02"
START = datetime(2020, 1, 2, tzinfo=UTC)


def _stale_day(root: Path, day: str) -> Path:
    path = root / "rollups" / "day_bundled" / f"day={day}" / "time_series.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"bucket": [START], "requests": [999]}), path)
    return path


def _raw_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE logs_repair (timestamp TIMESTAMPTZ, status INTEGER, "
        "cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    # Include an empty hour and a next-day event: both are boundary guards.
    con.executemany(
        "INSERT INTO logs_repair VALUES (?, 200, 'HIT', 100, 0.1)",
        [[START + timedelta(hours=h, minutes=7)] for h in range(24) if h != 12] + [[START + timedelta(days=1)]],
    )
    return con


def test_day_repair_recomputes_retired_hours_and_leaves_other_day_untouched(tmp_path):
    stale = _stale_day(tmp_path, DAY)
    other = _stale_day(tmp_path, "2020-01-01")
    other_bytes = other.read_bytes()
    assert not (tmp_path / "rollups" / "hour_bundled").exists()
    with writer_patches(tmp_path, "logs_repair", _raw_connection()):
        from backend.core.duckdb import get_connection

        rebuilt = day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [DAY])
        get_connection.assert_called_once()
    assert rebuilt == 1
    rows = pq.ParquetFile(stale).read().to_pylist()
    assert sum(row["requests"] for row in rows) == 23
    assert {row["bucket"].hour for row in rows} == set(range(24)) - {12}
    assert all(START <= row["bucket"] < START + timedelta(days=1) for row in rows)
    assert other.read_bytes() == other_bytes
    assert not (tmp_path / "rollups" / "hour_bundled").exists()
    assert not list(stale.parent.glob(".tmp_ts_repair_*"))


def test_incomplete_hour_recompute_preserves_existing_day(tmp_path, monkeypatch):
    stale = _stale_day(tmp_path, DAY)
    old_bytes = stale.read_bytes()
    original = time_series.build_time_series_bundles

    def incomplete(sid, source, hours, **kwargs):
        return original(sid, source, hours[:-1], **kwargs)

    monkeypatch.setattr(time_series, "build_time_series_bundles", incomplete)
    with writer_patches(tmp_path, "logs_repair", _raw_connection()):
        with pytest.raises(RuntimeError, match="incomplete"):
            day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [DAY])
    assert stale.read_bytes() == old_bytes
    assert not list(stale.parent.glob(".tmp_ts_day_*"))


def test_interleaved_compactor_cannot_publish_incomplete_repair(tmp_path, monkeypatch):
    # One old official hour remains; the other 23 have been retired.
    hour = tmp_path / "rollups" / "hour_bundled" / f"hour={DAY}-00" / "time_series.parquet"
    hour.parent.mkdir(parents=True)
    con = _raw_connection()
    con.execute(
        "COPY (SELECT timestamp AS bucket, 999::BIGINT AS requests, "
        "0::BIGINT AS status_4xx, 0::BIGINT AS status_5xx, 0::BIGINT AS hits, "
        "0::BIGINT AS cache_total, 0::BIGINT AS resp_bytes_sum, 0.0::DOUBLE AS ttfb_sum, "
        "0::BIGINT AS ttfb_count FROM logs_repair LIMIT 1) TO ? (FORMAT PARQUET)",
        [str(hour)],
    )
    os.utime(hour, (1, 1))
    old_hour = hour.read_bytes()
    stale = _stale_day(tmp_path, DAY)
    old_day = stale.read_bytes()
    original = time_series.build_time_series_bundles
    observed = []

    def interrupted(sid, source, hours, **kwargs):
        written = original(sid, source, hours[:-1], **kwargs)
        observed.append(day_bundles.compact_time_series_closed_days_to_daily(sid, source))
        assert stale.read_bytes() == old_day
        return written

    monkeypatch.setattr(time_series, "build_time_series_bundles", interrupted)
    with writer_patches(tmp_path, "logs_repair", con):
        with pytest.raises(RuntimeError, match="incomplete"):
            day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [DAY])
    assert observed == [0]
    assert stale.read_bytes() == old_day
    assert hour.read_bytes() == old_hour
    assert not list(stale.parent.glob(".tmp_ts_repair_*"))


def test_day_copy_failure_preserves_existing_day(tmp_path, monkeypatch):
    stale = _stale_day(tmp_path, DAY)
    old_bytes = stale.read_bytes()
    monkeypatch.setattr(day_bundles, "_time_series_copy_sql", lambda *_: "SELECT missing_column")
    with writer_patches(tmp_path, "logs_repair", _raw_connection()):
        with pytest.raises(RuntimeError, match="day rebuild failed"):
            day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [DAY])
    assert stale.read_bytes() == old_bytes
    assert not list(stale.parent.glob(".tmp_ts_day_*"))


def test_delayed_compactor_cannot_overwrite_published_repair(tmp_path, monkeypatch):
    from backend.core.rollups import _common

    hour = tmp_path / "rollups" / "hour_bundled" / f"hour={DAY}-00" / "time_series.parquet"
    hour.parent.mkdir(parents=True)
    stale = _stale_day(tmp_path, DAY)
    con = _raw_connection()
    con.execute(
        "COPY (SELECT timestamp AS bucket, 999::BIGINT AS requests, "
        "0::BIGINT AS status_4xx, 0::BIGINT AS status_5xx, 0::BIGINT AS hits, "
        "0::BIGINT AS cache_total, 0::BIGINT AS resp_bytes_sum, 0.0::DOUBLE AS ttfb_sum, "
        "0::BIGINT AS ttfb_count FROM logs_repair LIMIT 1) TO ? (FORMAT PARQUET)",
        [str(hour)],
    )
    os.utime(stale, (1, 1))  # Ordinary compactor decides to rebuild.
    original_publish = _common.publish_temp_parquet
    repaired = []

    def repair_before_compactor_publish(tmp_file, day_file, lock_key, **kwargs):
        # COPY has already finished against stale official hours.
        # Restore before repair: its private hourly writes use this helper too.
        monkeypatch.setattr(_common, "publish_temp_parquet", original_publish)
        assert day_bundles.rebuild_time_series_days("repair", {"name": "repair"}, [DAY]) == 1
        repaired.append(stale.read_bytes())
        return original_publish(tmp_file, day_file, lock_key, **kwargs)

    monkeypatch.setattr(_common, "publish_temp_parquet", repair_before_compactor_publish)
    with writer_patches(tmp_path, "logs_repair", con):
        assert day_bundles.compact_time_series_closed_days_to_daily("repair", {"name": "repair"}) == 0
    assert stale.read_bytes() == repaired[0]
    assert sum(pq.ParquetFile(stale).read()["requests"].to_pylist()) == 23
    assert not list(stale.parent.glob(".tmp_ts_day_*"))


@pytest.fixture
def repair_client(monkeypatch):
    hours = Mock(return_value=1)
    days = Mock(return_value=1)
    monkeypatch.setattr(api, "build_time_series_bundles", hours)
    monkeypatch.setattr(day_bundles, "rebuild_time_series_days", days, raising=False)
    app = FastAPI()
    app.add_api_route(
        "/repair",
        api.rebuild_time_series_rollups,
        methods=["POST"],
        response_model=RebuildTimeSeriesRollupsResponse,
        response_model_exclude_unset=True,
    )
    app.dependency_overrides[api.get_source] = lambda: {"name": "repair", "service_id": "repair"}
    app.dependency_overrides[api.require_admin] = lambda: None
    with TestClient(app) as client:
        yield client, hours, days


@pytest.mark.parametrize("actual_days", [0, 1])
def test_endpoint_day_only_reports_actual_day_count(repair_client, actual_days):
    client, hours, days = repair_client
    days.return_value = actual_days
    response = client.post("/repair", json={"days": [DAY]})
    assert response.status_code == 200
    assert response.json() == {
        "service_id": "repair",
        "requested_hours": [],
        "rebuilt": 0,
        "requested_days": [DAY],
        "rebuilt_days": actual_days,
    }
    hours.assert_not_called()
    days.assert_called_once_with("repair", {"name": "repair", "service_id": "repair"}, [DAY])


def test_endpoint_hour_only_preserves_contract(repair_client):
    client, hours, days = repair_client
    response = client.post("/repair", json={"hours": [f"{DAY}-04"]})
    assert response.status_code == 200
    assert response.json() == {"service_id": "repair", "requested_hours": [f"{DAY}-04"], "rebuilt": 1}
    hours.assert_called_once()
    days.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"hours": [], "days": []},
        {"days": [DAY, DAY]},
        {"days": ["2020-1-02"]},
        {"days": ["2020-02-30"]},
        {"days": [datetime.now(UTC).strftime("%Y-%m-%d")]},
        {"days": [(datetime.now(UTC) + timedelta(days=1)).strftime("%Y-%m-%d")]},
        {"days": [DAY] * 8},
        {"hours": [f"{DAY}-04", f"{DAY}-04"], "days": [DAY]},
        {"hours": ["bad"], "days": [DAY]},
    ],
)
def test_endpoint_rejects_invalid_scope_before_writing(repair_client, payload):
    client, hours, days = repair_client
    response = client.post("/repair", json=payload)
    assert response.status_code in (400, 422)
    hours.assert_not_called()
    days.assert_not_called()


def test_endpoint_incomplete_recompute_fails_explicitly(repair_client):
    client, _, days = repair_client
    days.side_effect = RuntimeError("incomplete hourly recompute: private detail")
    response = client.post("/repair", json={"days": [DAY]})
    assert response.status_code == 500
    assert response.json()["detail"]["error"] == "time_series_day_rebuild_failed"
    assert "private detail" not in response.text
