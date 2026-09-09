from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from backend import config as svcconfig
from backend.repositories._base import QueryRunner, collect_hourly_bundle_paths


def _durable_source() -> dict:
    return {
        "name": "durable_rollup_svc",
        "service_id": "durable_rollup_svc",
        "deployment_mode": "high_throughput",
    }


def _enable_durable(monkeypatch):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_throughput")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://catalog/ducklake")


def test_durable_rollup_path_is_unavailable_even_when_local_bundle_exists(monkeypatch, tmp_path):
    _enable_durable(monkeypatch)
    src = _durable_source()
    root = tmp_path / "rollups" / "hour_bundled"
    (root / "hour=2026-09-06-12").mkdir(parents=True)
    bundle = root / "hour=2026-09-06-12" / "time_series.parquet"
    bundle.touch()

    out = collect_hourly_bundle_paths(
        src,
        datetime(2026, 9, 6, 12, tzinfo=UTC),
        datetime(2026, 9, 6, 13, tzinfo=UTC),
        str(root),
        "time_series.parquet",
    )

    assert out is None


def test_durable_top_n_rollup_returns_unavailable_without_querying_local_files(monkeypatch):
    _enable_durable(monkeypatch)
    con = MagicMock()
    runner = QueryRunner(con, _durable_source())

    rows, fields = runner.execute_top_n_rollups(["country"], None, None)

    assert rows == []
    assert fields == ["country"]
    con.execute.assert_not_called()


def test_durable_ip_spread_rollup_returns_unavailable(monkeypatch):
    _enable_durable(monkeypatch)
    runner = QueryRunner(MagicMock(), _durable_source())

    counts, metadata = runner.execute_ip_spread_rollups(["tls_ciphers_sha"], None, None)

    assert counts == {}
    assert metadata == {}


def test_durable_slow_url_rollup_returns_unavailable(monkeypatch):
    _enable_durable(monkeypatch)
    runner = QueryRunner(MagicMock(), _durable_source())

    result = runner.try_slow_urls_from_rollup(
        "2026-09-01T00:00:00Z",
        "2026-09-07T00:00:00Z",
        has_filters=False,
        min_requests=1,
        limit=10,
    )

    assert result is None


def test_durable_network_health_uses_ducklake_fallback_when_rollups_are_missing(
    monkeypatch, in_memory_duckdb, test_service_source
):
    _enable_durable(monkeypatch)
    src = {**test_service_source, "name": "durable_network_svc", "deployment_mode": "high_throughput"}
    table = src["name"]
    in_memory_duckdb.execute(
        f'CREATE TABLE "{table}" ('
        "timestamp TIMESTAMPTZ, asn INTEGER, country VARCHAR, city VARCHAR, "
        "lat DOUBLE, lon DOUBLE, tcp_rtt DOUBLE, rtt_min DOUBLE, "
        "rtt_var DOUBLE, ploss DOUBLE, status INTEGER, cache VARCHAR, "
        "elapsed DOUBLE, resp_bytes BIGINT, c_speed DOUBLE)"
    )
    in_memory_duckdb.execute(
        f'INSERT INTO "{table}" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
            datetime(2026, 9, 6, 12, tzinfo=UTC),
            7922,
            "US",
            "Denver",
            39.7,
            -104.9,
            20_000,
            10_000,
            100,
            0.0,
            200,
            "HIT",
            5.0,
            100,
            1000,
        ],
    )
    monkeypatch.setattr(
        QueryRunner,
        "get_schema_cols",
        lambda _self: [
            "timestamp",
            "asn",
            "country",
            "city",
            "lat",
            "lon",
            "tcp_rtt",
            "rtt_min",
            "rtt_var",
            "ploss",
            "status",
            "cache",
            "elapsed",
            "resp_bytes",
            "c_speed",
        ],
    )
    in_memory_duckdb.execute(f'CREATE TEMP TABLE durable_network_temp AS SELECT * FROM "{table}"')
    monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", lambda *_args, **_kwargs: "durable_network_temp")
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda *_args, **_kwargs: {})

    with patch.object(QueryRunner, "try_network_heatmap_from_rollup", return_value=None) as heatmap:
        from backend.repositories.network import get_health

        result = get_health(
            in_memory_duckdb,
            src,
            "2026-09-06T00:00:00Z",
            "2026-09-07T00:00:00Z",
            {},
            sections={"heatmap"},
        )

    heatmap.assert_called_once()
    assert result["available"] is not False
    assert result["heatmap"]


def test_durable_log_activity_reads_ducklake_instead_of_local_rollup_or_metadata(monkeypatch):
    _enable_durable(monkeypatch)
    src = _durable_source()
    con = MagicMock()
    describe_result = MagicMock()
    describe_result.fetchall.return_value = [("timestamp",), ("resp_bytes",)]
    query_result = MagicMock()
    query_result.fetchall.return_value = [
        (datetime(2026, 9, 6, 12, tzinfo=UTC), 3, 1200),
    ]
    con.execute.side_effect = [describe_result, query_result]

    with (
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.repositories.usage._log_activity_fallback") as local_fallback,
    ):
        from backend.repositories.usage import get_log_activity

        result = get_log_activity(
            src,
            "2026-09-06T00:00:00Z",
            "2026-09-07T00:00:00Z",
            "hour",
        )

    local_fallback.assert_not_called()
    con.close.assert_called_once()
    assert result["data"] == [{"time": "2026-09-06T12:00", "row_count": 3, "bytes": 1200}]
    assert result["total_rows"] == 3
    assert result["total_bytes"] == 1200
