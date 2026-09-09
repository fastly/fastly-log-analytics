"""Real DuckLake request observations, including committed inlined rows."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import duckdb
import pytest

from backend import config
from backend.core import request_metrics
from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name


@pytest.fixture
def durable_metrics(monkeypatch, tmp_path):
    sid = f"metrics-{uuid.uuid4().hex}"
    source = {
        "name": sid,
        "service_id": sid,
        "serving_mode": "durable",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(tmp_path / "cache"),
        "duckdb_path": str(tmp_path / "must-not-open.duckdb"),
    }
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "configs")
    monkeypatch.setattr(config, "SERVICES_DATA_DIR", tmp_path / "services")
    config.SERVICES_DATA_DIR.mkdir()
    monkeypatch.setattr(config, "DUCKLAKE_CATALOG", str(tmp_path / "catalog.ducklake"))
    # Use a real isolated file catalog for the engine test, while selecting
    # the production durable-only serving branch (production requires PG).
    monkeypatch.setattr(config, "is_durable_serving_mode", lambda source=None: True)
    monkeypatch.setattr(config, "config_to_source", lambda cfg: source)
    config.save_config(
        sid,
        {
            "service_id": sid,
            "status": {
                "rum": {"latest_log_at": "2026-09-09T00:00:00Z", "total_rows": 3},
                "request": {"latest_log_at": "2026-09-08T00:00:00Z", "total_rows": 99},
                "latest_log_at": "2026-09-08T00:00:00Z",
                "local_rows": 102,
            },
        },
    )
    cron = {"log_discovery": {"started_at": "2026-09-08T18:00:00Z"}}
    monkeypatch.setattr("backend.core.metadata.cron_log.latest_cron_per_task", lambda sid: cron)
    table = ducklake_table_name(source)

    def replace_rows(timestamps):
        con = duckdb.connect(":memory:")
        try:
            assert _ducklake_attach(con, source, read_only=False)
            con.execute(f'CREATE TABLE IF NOT EXISTS lake."{table}" (timestamp TIMESTAMPTZ)')
            con.execute(f'DELETE FROM lake."{table}"')
            for ts in timestamps:
                con.execute(f'INSERT INTO lake."{table}" VALUES (?)', [ts])
            info = con.execute(
                "SELECT file_count FROM ducklake_table_info('lake') WHERE table_name = ?", [table]
            ).fetchone()
            assert info == (0,), "test must exercise inlined rows, not parquet"
        finally:
            con.close()

    return source, replace_rows, cron, tmp_path


def test_observation_reads_inlined_rows_ignoring_local_parquet_and_native_file(durable_metrics):
    source, replace_rows, _, path = durable_metrics
    first = datetime(2026, 9, 7, 12, tzinfo=UTC)
    last = datetime(2026, 9, 7, 19, tzinfo=UTC)
    replace_rows([first, last])
    # A valid local parquet with a later event MUST NOT enter durable metrics.
    (path / "cache" / "buffer").mkdir(parents=True)
    con = duckdb.connect(":memory:")
    con.execute(
        "COPY (SELECT TIMESTAMPTZ '2026-09-10 00:00:00+00' AS timestamp) TO ? (FORMAT PARQUET)",
        [str(path / "cache" / "buffer" / "batch_local.parquet")],
    )
    con.close()
    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["request"] == {
        "latest_log_at": last.isoformat(),
        "total_rows": 2,
        "last_sync_at": "2026-09-08T18:00:00Z",
    }
    assert observed["earliest_log_at"] == first.isoformat()
    assert observed["latest_log_at"] == last.isoformat()
    assert observed["local_rows"] == 5  # two REQUEST + three existing RUM
    assert not (path / "must-not-open.duckdb").exists()
    assert config.get_status(source["name"])["request"] == observed["request"]


def test_observation_reads_durable_rum_extents(durable_metrics):
    source, replace_rows, _, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, 12, tzinfo=UTC)])
    con = duckdb.connect(":memory:")
    try:
        assert _ducklake_attach(con, source, read_only=False)
        vitals_table = ducklake_table_name(source, "client_vitals")
        errors_table = ducklake_table_name(source, "client_errors")
        for table in (vitals_table, errors_table):
            con.execute(
                f'CREATE TABLE IF NOT EXISTS lake."{table}" (timestamp TIMESTAMPTZ, req_id VARCHAR, cid VARCHAR)'
            )
            con.execute(f'DELETE FROM lake."{table}"')
        con.execute(
            f'INSERT INTO lake."{vitals_table}" VALUES '
            "(TIMESTAMPTZ '2026-09-08 01:00:00+00', '', 'vital-1'), "
            "(TIMESTAMPTZ '2026-09-08 02:00:00+00', '', 'vital-2')"
        )
        con.execute(f"INSERT INTO lake.\"{errors_table}\" VALUES (TIMESTAMPTZ '2026-09-08 03:00:00+00', '', 'error-1')")
    finally:
        con.close()

    observed = request_metrics.refresh_durable_request_metrics(source)

    assert observed["rum"] == {
        "latest_log_at": "2026-09-08T03:00:00+00:00",
        "total_rows": 3,
        "last_sync_at": None,
    }
    assert config.get_status(source["name"])["rum"] == observed["rum"]


def test_observation_accepts_backward_extent_reset_and_idle_cron(durable_metrics):
    source, replace_rows, cron, _ = durable_metrics
    latest = datetime(2026, 9, 7, tzinfo=UTC)
    older = datetime(2026, 9, 2, tzinfo=UTC)
    replace_rows([latest])
    request_metrics.refresh_durable_request_metrics(source)
    replace_rows([older])
    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["latest_log_at"] == older.isoformat()
    replace_rows([])
    cron["log_discovery"]["started_at"] = "2026-09-08T18:01:00Z"
    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["request"] == {
        "latest_log_at": None,
        "total_rows": 0,
        "last_sync_at": "2026-09-08T18:01:00Z",
    }
    assert observed["earliest_log_at"] is observed["latest_log_at"] is None
    assert observed["local_rows"] == 3  # RUM was not cleared by a request reset


def test_observation_failure_preserves_last_success(durable_metrics, monkeypatch):
    source, replace_rows, _, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, tzinfo=UTC)])
    request_metrics.refresh_durable_request_metrics(source)
    previous = config.get_status(source["name"])
    con = MagicMock()
    con.execute.side_effect = RuntimeError("catalog unavailable")
    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", lambda *a, **kw: con)
    with pytest.raises(RuntimeError, match="catalog unavailable"):
        request_metrics.refresh_durable_request_metrics(source)
    assert config.get_status(source["name"]) == previous
    con.close.assert_called_once()


def test_legacy_heavy_refresh_cannot_overwrite_durable_request_metrics(durable_metrics, monkeypatch):
    from backend.core._duckdb_status import refresh_config_status

    source, replace_rows, _, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, tzinfo=UTC)])
    monkeypatch.setattr(
        "backend.core.duckdb.get_sync_status",
        MagicMock(side_effect=AssertionError("local/stale stats path called")),
    )
    monkeypatch.setattr("backend.core.iceberg.get_table_info", lambda src: {})
    monkeypatch.setattr("backend.repositories.usage.get_edge_ratio", lambda *a: (None, None))
    refresh_config_status(source["name"], include_top_values=False)
    assert config.get_status(source["name"])["request"]["total_rows"] == 1
    replace_rows([])
    refresh_config_status(source["name"], include_top_values=False)
    status = config.get_status(source["name"])
    assert status["request"]["total_rows"] == 0
    assert status["request"]["latest_log_at"] is status["latest_log_at"] is None


def test_shared_refresh_coalesces_other_status_refreshes(durable_metrics, monkeypatch):
    source, _, _, _ = durable_metrics
    open_con = MagicMock(side_effect=AssertionError("overlapping scan"))
    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", open_con)
    with request_metrics._refresh_lock:
        assert request_metrics.refresh_durable_request_metrics(source) is None
    open_con.assert_not_called()


def test_query_deadline_interrupts_without_persisting_zero(durable_metrics, monkeypatch):
    source, _, _, _ = durable_metrics
    previous = config.get_status(source["name"])
    raw = duckdb.connect(":memory:")
    interrupted = []

    class SlowConnection:
        def execute(self, sql, *args, **kwargs):
            if sql.startswith("SELECT count(*)"):
                sql = "SELECT sum(sin(i)) FROM range(1000000000) t(i)"
            return raw.execute(sql, *args, **kwargs)

        def interrupt(self):
            interrupted.append(True)
            raw.interrupt()

        def close(self):
            raw.close()

    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", lambda *a, **kw: SlowConnection())
    monkeypatch.setattr(request_metrics, "QUERY_TIMEOUT_S", 0.05)
    with pytest.raises(duckdb.InterruptException):
        request_metrics.refresh_durable_request_metrics(source)
    assert interrupted
    assert config.get_status(source["name"]) == previous


def test_real_observation_is_persisted_before_sse_publication(durable_metrics, monkeypatch):
    from backend import request_metrics_observer as module

    source, replace_rows, _, _ = durable_metrics
    latest = datetime(2026, 9, 7, tzinfo=UTC)
    replace_rows([latest])
    published = []

    def publish(service_id, payload):
        persisted = config.get_status(service_id)
        assert persisted["request"] == payload["request"]
        assert persisted["latest_log_at"] == payload["latest_log_at"]
        published.append(payload["request"])

    monkeypatch.setattr(module, "compute_sync_status_cached", config.get_status)
    monkeypatch.setattr(module.publisher, "publish", publish)
    observer = module.RequestMetricsObserver()
    observer.reconcile()
    observer.reconcile()
    assert len(published) == 1
    assert published[0]["latest_log_at"] == latest.isoformat()
    replace_rows([])
    observer.reconcile()
    assert len(published) == 2
    assert published[-1]["total_rows"] == 0
    assert published[-1]["latest_log_at"] is None
