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
        "deployment_mode": "high_scale",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(tmp_path / "cache"),
        "duckdb_path": str(tmp_path / "must-not-open.duckdb"),
    }
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "configs")
    monkeypatch.setattr(config, "SERVICES_DATA_DIR", tmp_path / "services")
    config.SERVICES_DATA_DIR.mkdir()
    # Use the isolated worker Postgres DuckLake catalog while selecting
    # the production durable-only serving branch.
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
            assert info[0] in (0, 1, 2), "file_count is either inlined (0) or parquet (>=1)"
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


def test_observation_uses_durable_table_when_serving_view_is_missing(durable_metrics, monkeypatch):
    source, replace_rows, _, _ = durable_metrics
    latest = datetime(2026, 9, 7, 19, tzinfo=UTC)
    replace_rows([latest])

    from backend.core.duckdb import open_serving_connection as real_open

    def open_without_serving_view(*args, **kwargs):
        con = real_open(*args, **kwargs)
        con.execute(f'DROP VIEW IF EXISTS "{ducklake_table_name(source)}"')
        return con

    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", open_without_serving_view)

    observed = request_metrics.refresh_durable_request_metrics(source)

    assert observed["request"]["total_rows"] == 1
    assert observed["request"]["latest_log_at"] == latest.isoformat()


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


def test_observation_refreshes_rum_sync_time_from_latest_cron_run(durable_metrics):
    source, replace_rows, cron, _ = durable_metrics
    status = config.get_status(source["name"])
    status["rum"]["last_sync_at"] = "2026-09-07T00:00:00Z"
    config.update_status(source["name"], status)
    cron["rum_sync"] = {"started_at": "2026-09-12T15:00:00Z"}
    replace_rows([datetime(2026, 9, 7, 12, tzinfo=UTC)])

    observed = request_metrics.refresh_durable_request_metrics(source)

    assert observed["rum"]["last_sync_at"] == "2026-09-12T15:00:00Z"


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


# ── Merged from test_operational_metrics.py ──

from backend.core import operational_metrics


class _FakeGauge:
    def __init__(self, callbacks):
        self.callbacks = callbacks


class _FakeMeter:
    def __init__(self):
        self.gauges = {}

    def create_observable_gauge(self, *, name, callbacks, description, unit):
        gauge = _FakeGauge(callbacks)
        self.gauges[name] = (gauge, description, unit)
        return gauge


def test_record_exposes_latest_labeled_observation(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)

    operational_metrics.record("celery_queue_depth", 7, queue="q.ingest")

    gauge, description, unit = meter.gauges["fla_celery_queue_depth"]
    observations = gauge.callbacks[0](None)
    assert [(item.value, item.attributes) for item in observations] == [(7.0, {"queue": "q.ingest"})]
    assert description == "Celery broker queue depth"
    assert unit == "{messages}"


def test_record_keeps_distinct_label_sets(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)

    operational_metrics.record("ingest_ledger_rows", 3, status="discovered")
    operational_metrics.record("ingest_ledger_rows", 2, status="claimed")

    gauge, _, _ = meter.gauges["fla_ingest_ledger_rows"]
    observations = gauge.callbacks[0](None)
    assert {(item.value, item.attributes["status"]) for item in observations} == {
        (3.0, "discovered"),
        (2.0, "claimed"),
    }


def test_high_scale_metrics_are_exported_with_domain_labels(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)

    operational_metrics.record(
        "high_scale_publication_lag_seconds",
        12.5,
        service_id="svc",
        domain="request",
    )

    gauge, description, unit = meter.gauges["fla_high_scale_publication_lag_seconds"]
    observation = gauge.callbacks[0](None)[0]
    assert observation.value == 12.5
    assert observation.attributes == {"domain": "request", "service_id": "svc"}
    assert description == "Age of the oldest pending high-scale publication"
    assert unit == "s"


# ── Merged from test_telemetry_schema.py ──

from fastapi.testclient import TestClient

from backend.core.metadata import usage_log, usage_log_db
from backend.main import app
from backend.utils.telemetry import get_page_load_id


def test_telemetry_schema_and_helpers(monkeypatch, tmp_path):
    # Setup temporary directory for metadata DB
    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))

    # Initialize connection which runs schemas
    con = usage_log_db.get_con("test_service")

    # Verify tables exist
    tables = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
        ).fetchall()
    ]
    assert "telemetry_queries" in tables
    assert "telemetry_sections" in tables

    # Test logging queries
    usage_log.log_telemetry_query(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        engine="duckdb",
        sql="SELECT 1",
        duration_ms=45.2,
    )

    # Test logging sections
    usage_log.log_telemetry_section(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        section_name="dashboard.aggregates",
        duration_ms=12.5,
    )

    # Test retrieval
    telemetry = usage_log.get_page_telemetry("test_service", "page-123")
    assert len(telemetry["queries"]) == 1
    assert telemetry["queries"][0]["sql_query"] == "SELECT 1"
    assert telemetry["queries"][0]["engine"] == "duckdb"
    assert telemetry["queries"][0]["duration_ms"] == 45.2

    assert len(telemetry["sections"]) == 1
    assert telemetry["sections"][0]["section_name"] == "dashboard.aggregates"
    assert telemetry["sections"][0]["duration_ms"] == 12.5


def test_page_load_id_propagation_middleware():
    client = TestClient(app)
    response = client.get("/api/health", headers={"X-Page-Load-ID": "test-page-999"})
    assert response.status_code == 200
    assert get_page_load_id() is None


# ── Merged from test_memory_guard.py ──

import signal

import pytest

from backend.core import memory_guard


@pytest.fixture(autouse=True)
def _no_real_kill(monkeypatch):
    """Hard safety net: never let a real signal escape this module under test."""
    calls = []
    monkeypatch.setattr(memory_guard.os, "kill", lambda pid, sig: calls.append((pid, sig)))
    return calls


def _set_rss(monkeypatch, value):
    """Patch the lazily-imported current_rss_bytes to return ``value`` bytes."""
    monkeypatch.setattr("backend.core.duckdb.current_rss_bytes", lambda: value)


def test_disabled_by_default(monkeypatch, _no_real_kill):
    monkeypatch.delenv("BACKEND_GRACEFUL_RESTART_RSS_MB", raising=False)
    _set_rss(monkeypatch, 50_000 * 1024 * 1024)  # huge, but guard is off
    assert memory_guard.maybe_graceful_restart() is False
    assert _no_real_kill == []


def test_below_threshold_does_not_restart(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, 5000 * 1024 * 1024)  # 5GB < 9GB
    assert memory_guard.maybe_graceful_restart() is False
    assert _no_real_kill == []


def test_at_or_above_threshold_sends_sigterm(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, 9500 * 1024 * 1024)  # 9.5GB >= 9GB
    assert memory_guard.maybe_graceful_restart() is True
    assert len(_no_real_kill) == 1
    pid, sig = _no_real_kill[0]
    assert sig == signal.SIGTERM
    assert pid > 0


def test_unreadable_rss_does_not_restart(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, None)  # /proc unreadable (e.g. off-Linux)
    assert memory_guard.maybe_graceful_restart() is False
    assert _no_real_kill == []


def test_threshold_parse_bytes(monkeypatch):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 9000 * 1024 * 1024
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "garbage")
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 0
    monkeypatch.delenv("BACKEND_GRACEFUL_RESTART_RSS_MB", raising=False)
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 0


# ── Merged from test_web_vitals_store.py ──

import json

import pytest

from backend.core import web_vitals_store


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", "On"])
def test_collection_enabled_truthy_values(monkeypatch, value):
    monkeypatch.setenv("WEB_VITALS_COLLECT", value)
    assert web_vitals_store.collection_enabled() is True


@pytest.mark.parametrize("value", ["0", "false", "no", "off", ""])
def test_collection_enabled_falsey_values(monkeypatch, value):
    monkeypatch.setenv("WEB_VITALS_COLLECT", value)
    assert web_vitals_store.collection_enabled() is False


def test_collection_disabled_when_unset(monkeypatch):
    monkeypatch.delenv("WEB_VITALS_COLLECT", raising=False)
    assert web_vitals_store.collection_enabled() is False


def test_append_sample_writes_one_json_line_each(monkeypatch, tmp_path):
    sink = tmp_path / "nested" / "web_vitals.jsonl"  # parent dir is created lazily
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)

    web_vitals_store.append_sample({"name": "LCP", "value": 1200.0, "rating": "good"})
    web_vitals_store.append_sample({"name": "CLS", "value": 0.05, "rating": "good"})

    lines = sink.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["name"] == "LCP"
    assert json.loads(lines[1])["name"] == "CLS"


def test_append_sample_never_raises_on_io_error(monkeypatch, tmp_path):
    # Point LOG_PATH at a path whose parent is a file, so mkdir/open fails;
    # the helper must swallow the error rather than break the request.
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", blocker / "child" / "wv.jsonl")
    web_vitals_store.append_sample({"name": "LCP", "value": 1.0, "rating": "good"})  # no exception


def test_max_bytes_defaults_to_200mb(monkeypatch):
    monkeypatch.delenv("WEB_VITALS_MAX_MB", raising=False)
    assert web_vitals_store._max_bytes() == 200 * 1024 * 1024


def test_max_bytes_invalid_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "not-a-number")
    assert web_vitals_store._max_bytes() == 200 * 1024 * 1024


def test_max_bytes_zero_disables_cap(monkeypatch):
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0")
    assert web_vitals_store._max_bytes() == 0


def test_rotation_creates_single_backup_at_cap(monkeypatch, tmp_path):
    """Once the active file reaches the cap it's rotated to a single .1
    backup and a fresh file continues — recent data is retained, disk is
    bounded at ~2x the cap."""
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", tmp_path / "wv.jsonl")
    # ~52-byte cap: smaller than one sample line, so each write past the
    # first rotates. Exercises the rotation path deterministically.
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0.00005")
    for i in range(4):
        web_vitals_store.append_sample({"name": "LCP", "value": i, "rating": "good"})

    active = tmp_path / "wv.jsonl"
    backup = web_vitals_store.rotated_path()
    assert active.exists()
    assert backup.exists()  # rotation happened
    # Only ONE backup is kept, and the active file holds the most recent
    # write(s) — not all four.
    assert len(active.read_text(encoding="utf-8").strip().splitlines()) < 4
    assert not (tmp_path / "wv.jsonl.2").exists()


def test_cap_zero_disables_rotation(monkeypatch, tmp_path):
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", tmp_path / "wv.jsonl")
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0")
    for i in range(5):
        web_vitals_store.append_sample({"name": "LCP", "value": i, "rating": "good"})
    assert not web_vitals_store.rotated_path().exists()
    assert len((tmp_path / "wv.jsonl").read_text(encoding="utf-8").strip().splitlines()) == 5
