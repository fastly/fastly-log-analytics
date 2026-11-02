from __future__ import annotations

import os
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core.rollups import country_day_repair as repair
from backend.core.rollups import day_bundles

DAY = "2020-01-02"
START = datetime(2020, 1, 2, tzinfo=UTC)


@pytest.fixture
def repair_data(tmp_path, monkeypatch):
    source = {"name": f"repair_{tmp_path.name}", "_cache_dir_override": str(tmp_path)}
    con = duckdb.connect()
    con.execute("CREATE TABLE raw (timestamp TIMESTAMPTZ, country VARCHAR)")
    # One verified empty hour is legitimate; sparse minutes are not source loss.
    con.executemany(
        "INSERT INTO raw VALUES (?, ?)",
        [(START + timedelta(hours=h, minutes=7), "US") for h in range(24) if h != 12]
        + [(START + timedelta(hours=1, minutes=8), "CA"), (START + timedelta(days=1), "US")],
    )
    day_file = tmp_path / "rollups" / "day_bundled" / f"day={DAY}" / "all_fields.parquet"
    day_file.parent.mkdir(parents=True)
    pq.write_table(
        pa.table({"field": ["country", "url"], "value": ["US", "/keep"], "count": [999, 987]}),
        day_file,
    )
    hourly = tmp_path / "rollups" / "hour_bundled"
    for h in range(24):
        path = hourly / f"hour={DAY}-{h:02d}" / "time_series.parquet"
        path.parent.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "bucket": pa.array([START + timedelta(hours=h)], type=pa.timestamp("us", tz="UTC")),
                    "requests": [0 if h == 12 else 2 if h == 1 else 1],
                }
            ),
            path,
        )
    monkeypatch.setattr(repair, "get_connection", Mock(return_value=con))
    monkeypatch.setattr(repair, "_pinned_source_sql", Mock(return_value=("SELECT * FROM raw", 7)))
    monkeypatch.setattr(repair, "_get_service_lock", lambda _: nullcontext())
    yield source, con, day_file
    con.close()


def _run(data, **kwargs):
    source, _, _ = data
    return repair.rebuild_country_day(
        "test-service", source, DAY, expected_requests=24, expected_countries={"US": 23, "CA": 1}, **kwargs
    )


def test_stages_all_hours_preserves_other_fields_and_refreshes_aliases(repair_data):
    source, _, day_file = repair_data
    original_hourly = {p: p.read_bytes() for p in (day_file.parents[2] / "hour_bundled").rglob("*.parquet")}
    result = _run(repair_data)
    assert result["raw_requests"] == 24
    assert result["snapshot_id"] == 7
    assert result["country_counts"] == {"CA": 1, "US": 23}
    assert result["raw_first_event"] == (START + timedelta(minutes=7)).isoformat()
    assert result["raw_last_event"] == (START + timedelta(hours=23, minutes=7)).isoformat()
    assert result["hourly_requests"][12] == 0
    assert sorted(pq.ParquetFile(day_file).read().to_pylist(), key=lambda r: (r["field"], r["value"])) == [
        {"field": "country", "value": "CA", "count": 1},
        {"field": "country", "value": "US", "count": 23},
        {"field": "url", "value": "/keep", "count": 987},
    ]
    for alias in (day_file.parent / f"day_bundle_{DAY}.parquet", day_file.parent.parent / f"day_bundle_{DAY}.parquet"):
        assert alias.read_bytes() == day_file.read_bytes()
    assert all(p.read_bytes() == content for p, content in original_hourly.items())
    assert not list(day_file.parent.glob(".tmp_country_repair_*"))
    repair.get_connection.assert_called_once_with(source=source, read_only=True)


@pytest.mark.parametrize("failure", ["missing_hour", "raw_loss", "wrong_total", "wrong_vector", "copy", "publish"])
def test_refusal_or_failure_keeps_old_bundle(repair_data, monkeypatch, failure):
    source, con, day_file = repair_data
    old = day_file.read_bytes()
    if failure == "missing_hour":
        (day_file.parents[2] / "hour_bundled" / f"hour={DAY}-04" / "time_series.parquet").unlink()
    elif failure == "raw_loss":
        con.execute("DELETE FROM raw WHERE timestamp = ?", [START + timedelta(hours=4, minutes=7)])
    elif failure in {"wrong_total", "wrong_vector"}:
        expected = 25 if failure == "wrong_total" else 24
        vector = {"US": 24} if failure == "wrong_vector" else {"US": 23, "CA": 1}
        with pytest.raises(RuntimeError):
            repair.rebuild_country_day(
                "test-service", source, DAY, expected_requests=expected, expected_countries=vector
            )
        assert day_file.read_bytes() == old
        return
    elif failure == "copy":
        monkeypatch.setattr(repair, "_build_copy_query", lambda *args: "SELECT nonexistent FROM raw")
    elif failure == "publish":
        monkeypatch.setattr(repair, "publish_temp_parquet", Mock(side_effect=OSError("test")))
    with pytest.raises(RuntimeError):
        _run(repair_data)
    assert day_file.read_bytes() == old
    assert not list(day_file.parent.glob(".tmp_country_repair_*"))


def test_concurrent_daily_change_refuses_stale_preservation(repair_data, monkeypatch):
    _, _, day_file = repair_data
    actual_publish = repair.publish_temp_parquet

    def competing_publish(*args, **kwargs):
        pq.write_table(pa.table({"field": ["url"], "value": ["/new"], "count": [123]}), day_file)
        return actual_publish(*args, **kwargs)

    monkeypatch.setattr(repair, "publish_temp_parquet", competing_publish)
    with pytest.raises(RuntimeError, match="changed"):
        _run(repair_data)
    assert pq.ParquetFile(day_file).read().to_pylist() == [{"field": "url", "value": "/new", "count": 123}]


@pytest.mark.parametrize("day", ["2020-1-2", "../bad", "9999-01-01"])
def test_invalid_or_open_day_rejected_before_io(day):
    with pytest.raises(ValueError):
        repair.rebuild_country_day("test", {}, day, expected_requests=1, expected_countries={"US": 1})


def test_unpinned_serving_view_is_refused(monkeypatch):
    con = duckdb.connect()
    con.execute("CREATE VIEW logs_test AS SELECT current_timestamp AS timestamp, 'US' AS country")
    monkeypatch.setattr(repair, "update_storage_view", lambda *args, **kwargs: None)
    with pytest.raises(RuntimeError, match="snapshot"):
        repair._pinned_source_sql(con, {"name": "test"})
    con.close()


def test_real_ducklake_select_stays_on_captured_snapshot(tmp_path, monkeypatch):
    with duckdb.connect() as con:
        con.execute("LOAD ducklake")
        con.execute(f"ATTACH 'ducklake:{tmp_path}/catalog.ducklake' AS lake (DATA_PATH '{tmp_path}/data')")
        con.execute("CREATE TABLE lake.events (timestamp TIMESTAMPTZ, country VARCHAR)")
        con.execute("INSERT INTO lake.events VALUES ('2020-01-02T00:00:00Z', 'US')")
        snapshot = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
        con.execute(f"CREATE VIEW logs_test AS SELECT * FROM lake.events AT (VERSION => {snapshot})")
        monkeypatch.setattr(repair, "update_storage_view", lambda *args, **kwargs: None)
        sql, captured = repair._pinned_source_sql(con, {"name": "test"})
        con.execute("INSERT INTO lake.events VALUES ('2020-01-02T01:00:00Z', 'CA')")
        con.execute("CREATE OR REPLACE VIEW logs_test AS SELECT * FROM lake.events")
        assert captured == snapshot
        assert con.execute(f"SELECT country FROM ({sql})").fetchall() == [("US",)]
        assert con.execute("SELECT count(*) FROM logs_test").fetchone()[0] == 2


def test_alias_failure_rolls_back_main_bundle(repair_data, monkeypatch):
    _, _, day_file = repair_data
    old = day_file.read_bytes()
    link = repair._link_day_bundle_alias
    calls = 0

    def fail_once(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("test alias failure")
        return link(*args)

    monkeypatch.setattr(repair, "_link_day_bundle_alias", fail_once)
    with pytest.raises(RuntimeError):
        _run(repair_data)
    assert day_file.read_bytes() == old
    for alias in (day_file.parent / f"day_bundle_{DAY}.parquet", day_file.parent.parent / f"day_bundle_{DAY}.parquet"):
        assert alias.read_bytes() == old


def test_delayed_all_fields_bundler_cannot_overwrite_repair(repair_data, monkeypatch):
    source, _, day_file = repair_data
    url_file = day_file.parents[2] / "day" / "field=url" / f"day={DAY}" / "compacted.parquet"
    url_file.parent.mkdir(parents=True)
    pq.write_table(pa.table({"field": ["url"], "value": ["/update"], "count": [11]}), url_file)
    os.utime(day_file, (1, 1))
    publish = day_bundles.publish_temp_parquet

    def repair_before_stale_publish(*args, **kwargs):
        _run(repair_data)
        return publish(*args, **kwargs)

    monkeypatch.setattr(day_bundles, "publish_temp_parquet", repair_before_stale_publish)
    day_bundles.bundle_days("test-service", source, [DAY])
    assert [r["count"] for r in pq.ParquetFile(day_file).read().to_pylist() if r["value"] == "US"] == [23]
    assert url_file.exists(), "A refused stale bundle must not delete its newer inputs."


def test_maintenance_cannot_recreate_country_from_older_partial_inputs(repair_data):
    source, _, day_file = repair_data
    partial = day_file.parents[2] / "hour" / "field=country" / f"hour={DAY}-01" / "part.parquet"
    partial.parent.mkdir(parents=True)
    pq.write_table(pa.table({"value": ["US"], "count": [999]}), partial)
    os.utime(partial, (1, 1))
    _run(repair_data)
    repaired = day_file.read_bytes()
    day_bundles.compact_closed_days_to_daily("test-service", source)
    day_bundles.bundle_days("test-service", source, [DAY])
    assert day_file.read_bytes() == repaired


def _daily_reference(data, *, retire_hours=True):
    _, con, day_file = data
    path = day_file.parent / "time_series.parquet"
    # Actual canonical minute-grain schema, including an explicit zero minute.
    con.execute(
        "CREATE TEMP TABLE daily_reference AS "
        "SELECT time_bucket(INTERVAL '1 minute', timestamp) AS bucket, "
        "count(*)::BIGINT AS requests, 0::BIGINT AS status_4xx, 0::BIGINT AS status_5xx, "
        "0::BIGINT AS hits, 0::BIGINT AS cache_total, 0::BIGINT AS resp_bytes_sum, "
        "0.0::DOUBLE AS ttfb_sum, 0::BIGINT AS ttfb_count "
        "FROM raw WHERE timestamp >= ? AND timestamp < ? GROUP BY 1",
        [START, START + timedelta(days=1)],
    )
    con.execute(
        "INSERT INTO daily_reference VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0)",
        [START + timedelta(hours=12)],
    )
    con.execute("COPY daily_reference TO ? (FORMAT PARQUET)", [str(path)])
    if retire_hours:
        for hourly in (day_file.parents[2] / "hour_bundled").rglob("time_series.parquet"):
            hourly.unlink()
    return path


def test_retired_hourly_references_accept_complete_daily_minutes(repair_data):
    reference = _daily_reference(repair_data)
    original_reference = reference.read_bytes()
    result = _run(repair_data)
    assert result["country_counts"] == {"CA": 1, "US": 23}
    assert result["hourly_requests"] == [2 if h == 1 else 0 if h == 12 else 1 for h in range(24)]
    assert reference.read_bytes() == original_reference
    day_file = repair_data[2]
    assert {"field": "url", "value": "/keep", "count": 987} in pq.ParquetFile(day_file).read().to_pylist()
    assert not list(day_file.parent.glob(".tmp_country_repair_*"))


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "wrong_day",
        "null_bucket",
        "not_minute",
        "duplicate",
        "negative",
        "null_requests",
        "fractional_requests",
        "bad_metric",
        "nan_metric",
        "impossible_metric",
        "missing_metrics",
        "wrong_hour",
        "partial",
        "hourly_collapsed",
    ],
)
def test_invalid_daily_reference_preserves_original(repair_data, change):
    reference = _daily_reference(repair_data)
    _, con, day_file = repair_data
    original = day_file.read_bytes()
    if change == "missing":
        reference.unlink()
    else:
        if change == "wrong_day":
            con.execute("UPDATE daily_reference SET bucket = bucket + INTERVAL '1 day'")
        elif change == "null_bucket":
            con.execute("UPDATE daily_reference SET bucket = NULL WHERE requests = 1")
        elif change == "not_minute":
            con.execute("UPDATE daily_reference SET bucket = bucket + INTERVAL '1 second'")
        elif change == "duplicate":
            con.execute("INSERT INTO daily_reference SELECT * FROM daily_reference WHERE requests = 0")
        elif change == "negative":
            con.execute("UPDATE daily_reference SET requests = -1 WHERE requests = 1")
        elif change == "null_requests":
            con.execute("UPDATE daily_reference SET requests = NULL WHERE requests = 1")
        elif change == "fractional_requests":
            con.execute("ALTER TABLE daily_reference ALTER requests TYPE DOUBLE")
            con.execute("UPDATE daily_reference SET requests = 0.5 WHERE requests = 1")
        elif change == "bad_metric":
            con.execute("UPDATE daily_reference SET resp_bytes_sum = -1")
        elif change == "nan_metric":
            con.execute("UPDATE daily_reference SET ttfb_sum = 'NaN'::DOUBLE")
        elif change == "impossible_metric":
            con.execute("UPDATE daily_reference SET hits = requests + 1")
        elif change == "missing_metrics":
            con.execute("ALTER TABLE daily_reference DROP status_4xx")
        elif change == "wrong_hour":
            con.execute(
                "UPDATE daily_reference SET bucket = bucket + INTERVAL '1 hour' WHERE bucket = ?",
                [START + timedelta(hours=1, minutes=8)],
            )
        elif change == "partial":
            con.execute("DELETE FROM daily_reference WHERE requests = 1")
        elif change == "hourly_collapsed":
            con.execute(
                "CREATE OR REPLACE TEMP TABLE daily_reference AS "
                "SELECT date_trunc('hour', bucket) AS bucket, sum(requests)::BIGINT AS requests, "
                "0::BIGINT AS status_4xx, 0::BIGINT AS status_5xx, 0::BIGINT AS hits, "
                "0::BIGINT AS cache_total, 0::BIGINT AS resp_bytes_sum, "
                "0.0::DOUBLE AS ttfb_sum, 0::BIGINT AS ttfb_count FROM daily_reference GROUP BY 1"
            )
        reference.unlink()
        con.execute("COPY daily_reference TO ? (FORMAT PARQUET)", [str(reference)])
    with pytest.raises(RuntimeError, match="daily.*reference|daily.*count"):
        _run(repair_data)
    assert day_file.read_bytes() == original
    assert not list(day_file.parent.glob(".tmp_country_repair_*"))


def test_present_hourly_references_ignore_unused_bad_daily_file(repair_data):
    path = repair_data[2].parent / "time_series.parquet"
    path.write_bytes(b"not parquet")
    assert _run(repair_data)["raw_requests"] == 24


def test_daily_fallback_never_hides_existing_hourly_mismatch(repair_data):
    _daily_reference(repair_data, retire_hours=False)
    _, _, day_file = repair_data
    hourly = day_file.parents[2] / "hour_bundled"
    (hourly / f"hour={DAY}-00" / "time_series.parquet").unlink()
    wrong = hourly / f"hour={DAY}-01" / "time_series.parquet"
    pq.write_table(
        pa.table(
            {"bucket": pa.array([START + timedelta(hours=1)], type=pa.timestamp("us", tz="UTC")), "requests": [99]}
        ),
        wrong,
    )
    original = day_file.read_bytes()
    with pytest.raises(RuntimeError, match="hourly count mismatch"):
        _run(repair_data)
    assert day_file.read_bytes() == original


@pytest.fixture
def field_repair_data(repair_data):
    source, con, day_file = repair_data
    for field, value in (("method", "GET"), ("status", "200"), ("pop", "ABC")):
        con.execute(f"ALTER TABLE raw ADD COLUMN {field} VARCHAR DEFAULT '{value}'")
    con.execute("UPDATE raw SET status = '503', pop = 'XYZ' WHERE country = 'CA'")
    con.execute("UPDATE raw SET method = NULL, pop = NULL WHERE timestamp = ?", [START + timedelta(minutes=7)])
    con.execute("UPDATE raw SET method = '' WHERE timestamp = ?", [START + timedelta(hours=1, minutes=7)])
    original = pq.ParquetFile(day_file).read().to_pylist()
    original.extend({"field": field, "value": "wrong", "count": 999} for field in ("method", "pop", "status"))
    pq.write_table(pa.Table.from_pylist(original), day_file)
    return source, con, day_file


def _run_fields(data, expected_fields=None, expected_requests=24):
    return repair.rebuild_daily_fields(
        "test-service",
        data[0],
        DAY,
        expected_requests=expected_requests,
        expected_fields=(
            {"method": {"GET": 22}, "pop": {"ABC": 22, "XYZ": 1}, "status": {"200": 23, "503": 1}}
            if expected_fields is None
            else expected_fields
        ),
    )


@pytest.mark.parametrize("daily_reference", [False, True])
def test_selected_fields_share_one_frozen_raw_day_and_preserve_country(field_repair_data, monkeypatch, daily_reference):
    source, con, day_file = field_repair_data
    if daily_reference:
        _daily_reference(field_repair_data)
    hourly_before = {p: p.read_bytes() for p in (day_file.parents[2] / "hour_bundled").rglob("*.parquet")}
    actual_builder = repair._build_copy_query
    calls = []

    def observe_staging(table, field, where):
        assert {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()} == {"timestamp", "method", "status", "pop"}
        assert con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 24
        if not calls:
            # A source change after capture must not leak into later staged hours.
            con.execute("UPDATE raw SET method = 'LATER'")
        calls.append((field, where))
        return actual_builder(table, field, where)

    monkeypatch.setattr(repair, "_build_copy_query", observe_staging)
    result = _run_fields(field_repair_data)
    assert result["field_counts"] == {
        "method": {"GET": 22},
        "pop": {"ABC": 22, "XYZ": 1},
        "status": {"200": 23, "503": 1},
    }
    assert set(result) == {
        "day",
        "snapshot_id",
        "raw_requests",
        "hourly_requests",
        "raw_first_event",
        "raw_last_event",
        "field_counts",
    }
    assert result["raw_requests"] == 24 and result["hourly_requests"][12] == 0
    assert len(calls) == 72 and len({where for _, where in calls}) == 24
    rows = pq.ParquetFile(day_file).read().to_pylist()
    assert {"field": "country", "value": "US", "count": 999} in rows
    assert {"field": "url", "value": "/keep", "count": 987} in rows
    assert not any(row["value"] == "wrong" for row in rows)
    assert all(p.read_bytes() == content for p, content in hourly_before.items())
    for alias in (day_file.parent / f"day_bundle_{DAY}.parquet", day_file.parent.parent / f"day_bundle_{DAY}.parquet"):
        assert alias.read_bytes() == day_file.read_bytes()
    repair.get_connection.assert_called_once_with(source=source, read_only=True)
    repair._pinned_source_sql.assert_called_once_with(con, source)
    assert not list(day_file.parent.glob(".tmp_country_repair_*"))


def test_combined_country_and_core_repair_uses_independent_vectors(field_repair_data):
    expected = {"country": {"US": 23, "CA": 1}, "method": {"GET": 22}, "pop": {"ABC": 22, "XYZ": 1}}
    result = _run_fields(field_repair_data, expected)
    assert result["field_counts"] == expected
    rows = pq.ParquetFile(field_repair_data[2]).read().to_pylist()
    assert {"field": "status", "value": "wrong", "count": 999} in rows
    assert {"field": "country", "value": "CA", "count": 1} in rows


def test_selected_field_with_legitimately_zero_nonempty_population(field_repair_data):
    _, con, day_file = field_repair_data
    con.execute("UPDATE raw SET pop = NULL")
    assert _run_fields(field_repair_data, {"pop": {}})["field_counts"] == {"pop": {}}
    assert all(row["field"] != "pop" for row in pq.ParquetFile(day_file).read().to_pylist())


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"url": {"bad": 1}},
        {"waf_sig_ind": {"bad": 1}},
        {'status"; DROP TABLE raw; --': {"200": 1}},
        {"status": {"": 1}},
        {"status": {"200": -1}},
        {"status": {"200": 0.5}},
        {"status": {"200": True}},
    ],
)
def test_generic_repair_rejects_bad_or_unapproved_fields_before_io(monkeypatch, fields):
    get_connection = Mock()
    monkeypatch.setattr(repair, "get_connection", get_connection)
    with pytest.raises(ValueError):
        repair.rebuild_daily_fields("test", {}, DAY, expected_requests=1, expected_fields=fields)
    get_connection.assert_not_called()


@pytest.mark.parametrize("failure", ["raw_loss", "wrong_vector", "capacity", "truncated_stage", "missing_hour"])
def test_generic_refusal_preserves_whole_old_bundle(field_repair_data, monkeypatch, failure):
    _, con, day_file = field_repair_data
    original = day_file.read_bytes()
    expected = None
    if failure == "raw_loss":
        con.execute("DELETE FROM raw WHERE timestamp = ?", [START + timedelta(hours=4, minutes=7)])
    elif failure == "wrong_vector":
        # Matching total/population is not sufficient: the entire map must match.
        expected = {"status": {"200": 22, "503": 2}}
    elif failure == "capacity":
        monkeypatch.setattr(repair, "DAY_BUNDLE_TOP_K", 1)
    elif failure == "truncated_stage":
        builder = repair._build_copy_query
        monkeypatch.setattr(repair, "_build_copy_query", lambda *args: f"SELECT * FROM ({builder(*args)}) WHERE false")
    elif failure == "missing_hour":
        (day_file.parents[2] / "hour_bundled" / f"hour={DAY}-04" / "time_series.parquet").unlink()
    with pytest.raises(RuntimeError):
        _run_fields(field_repair_data, expected)
    assert day_file.read_bytes() == original
    assert not list(day_file.parent.glob(".tmp_country_repair_*"))


@pytest.mark.parametrize("failure", ["cas", "alias"])
def test_generic_publication_cas_and_alias_rollback(field_repair_data, monkeypatch, failure):
    _, _, day_file = field_repair_data
    original = day_file.read_bytes()
    if failure == "cas":
        publish = repair.publish_temp_parquet

        def competing_publish(*args, **kwargs):
            pq.write_table(pa.table({"field": ["url"], "value": ["/new"], "count": [123]}), day_file)
            return publish(*args, **kwargs)

        monkeypatch.setattr(repair, "publish_temp_parquet", competing_publish)
    else:
        link = repair._link_day_bundle_alias
        calls = 0

        def fail_once(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("test alias failure")
            return link(*args)

        monkeypatch.setattr(repair, "_link_day_bundle_alias", fail_once)
    with pytest.raises(RuntimeError):
        _run_fields(field_repair_data)
    if failure == "cas":
        assert pq.ParquetFile(day_file).read().to_pylist() == [{"field": "url", "value": "/new", "count": 123}]
    else:
        assert day_file.read_bytes() == original
        for alias in (
            day_file.parent / f"day_bundle_{DAY}.parquet",
            day_file.parent.parent / f"day_bundle_{DAY}.parquet",
        ):
            assert alias.read_bytes() == original


def test_country_wrapper_keeps_legacy_receipt_and_delegates(monkeypatch):
    generic = {
        "day": DAY,
        "snapshot_id": 7,
        "raw_requests": 1,
        "hourly_requests": [1] + [0] * 23,
        "raw_first_event": START.isoformat(),
        "raw_last_event": START.isoformat(),
        "field_counts": {"country": {"US": 1}},
    }
    engine = Mock(return_value=generic)
    monkeypatch.setattr(repair, "rebuild_daily_fields", engine)
    result = repair.rebuild_country_day("test", {}, DAY, expected_requests=1, expected_countries={"US": 1})
    engine.assert_called_once_with("test", {}, DAY, expected_requests=1, expected_fields={"country": {"US": 1}})
    assert "field_counts" not in result and result["country_counts"] == {"US": 1}
    assert set(result) == set(generic) - {"field_counts"} | {"country_counts"}
