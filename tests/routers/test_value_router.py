"""HTTP-level tests for the Fastly Value router."""

from __future__ import annotations

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_value_summary_accepts_default_request(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    # Test empty sections (all)
    resp = client.post(
        "/api/value/summary",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": None},
    )
    assert resp.status_code == 200

    # Test 'summary' tab sections
    resp = client.post(
        "/api/value/summary",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["overview", "caching", "network"]},
    )
    assert resp.status_code == 200

    # Test individual sections
    for section in ["overview", "caching", "security", "bots", "performance", "network", "io"]:
        resp = client.post(
            "/api/value/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sections": [section]},
        )
        assert resp.status_code == 200, f"Section {section} failed with {resp.status_code}: {resp.text}"


def test_value_summary_empty_table(client, in_memory_duckdb, test_service_source):
    from backend.core.log_fields import LOG_FIELD_CATALOG

    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")

    resp = client.post(
        "/api/value/summary",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}},
    )
    assert resp.status_code == 200, f"Empty table failed with {resp.status_code}: {resp.text}"


def test_value_summary_missing_network_columns(client, in_memory_duckdb, test_service_source):
    # Only create a table with timestamp, to trigger missing network columns
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (timestamp TIMESTAMPTZ, req_bytes INT)")

    # Insert a dummy row
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (TIMESTAMPTZ '2026-07-23 12:00:00Z', 100)")

    # Create the hour_bundled folder to trigger the rollup path check
    import os

    from backend.core.rollups import _hour_bundled_root

    bundled_root = _hour_bundled_root(test_service_source)
    os.makedirs(bundled_root, exist_ok=True)

    # We need to ensure crosses_active is True. Since crosses_active is True when st/et span the active hour,
    # let's make start_time, end_time close to now
    from datetime import UTC, datetime, timedelta

    from backend.utils.date_utils import iso_z

    now = datetime.now(UTC)
    start_time = iso_z(now - timedelta(hours=1))
    end_time = iso_z(now + timedelta(hours=1))

    try:
        resp = client.post(
            "/api/value/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "sections": ["network"],
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        assert resp.status_code == 200, f"Missing columns failed with {resp.status_code}: {resp.text}"
    finally:
        # Clean up directory
        import shutil

        shutil.rmtree(os.path.dirname(bundled_root), ignore_errors=True)


def test_value_summary_tls_and_verified_bots_fallbacks(client, in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])

    # Create schema with tls (fallback for is_ssl) and waf_sig (for verified_bots)
    in_memory_duckdb.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMPTZ,
            tls VARCHAR,
            waf_sig VARCHAR,
            _bot_name VARCHAR,
            resp_bytes INT
        )
    """)

    # Insert mock rows
    # Row 1: TLS enabled, verified bot
    # Row 2: No TLS, no bot
    in_memory_duckdb.execute(f"""
        INSERT INTO {table_name} VALUES
        (TIMESTAMPTZ '2026-07-23 12:00:00Z', '1.3', 'SHIELD,VERIFIED-BOT.googlebot', 'Googlebot', 100),
        (TIMESTAMPTZ '2026-07-23 12:05:00Z', '', NULL, '', 200)
    """)

    # Make the request close to the inserted timestamps

    start_time = "2026-07-23T11:00:00Z"
    end_time = "2026-07-23T13:00:00Z"

    resp = client.post(
        "/api/value/summary",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "sections": ["network", "bots"],
            "start_time": start_time,
            "end_time": end_time,
        },
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    # Verify tls_pct uses the 'tls' column successfully and finds 50%
    assert data["network"]["tls_pct"] == 50.0

    # Verify verified_bots is correctly computed as 1
    assert data["bots"]["verified_bots"] == 1


def test_value_summary_range_token(client, in_memory_duckdb, test_service_source, monkeypatch):
    from unittest.mock import patch

    from backend import config as svcconfig

    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    monkeypatch.setattr(svcconfig, "get_status", lambda sid: {"earliest_log_at": "2026-08-19T00:00:00Z"})

    with (
        patch("backend.utils.time_window.is_valid_range_token", return_value=True),
        patch(
            "backend.utils.time_window.resolve_window", return_value=("2026-08-19T11:00:00Z", "2026-08-19T13:00:00Z")
        ),
    ):
        resp = client.post(
            "/api/value/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "range_token": "24h",
                "anchor": "2026-08-19T12:00:00Z",
            },
        )
    assert resp.status_code == 200
    assert "overview" in resp.json()


def test_value_summary_analyst_session_clamps_window(client, in_memory_duckdb, test_service_source):
    """An analyst session causes the value router to clamp requested time window to the analyst bounds."""
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from unittest.mock import patch

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds
    from tests.conftest import override_request_context

    start = datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-analyst-1",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )

    captured: dict = {}

    def _fake_get_summary(**kw):
        captured.update(kw)
        return {
            "overview": {
                "total_requests": 100,
                "origin_offload_pct": 95.0,
            }
        }

    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/value/summary",
        time_bounds=TimeBounds(start=start, end=end),
    )
    try:
        with patch("backend.repositories.value.get_summary", side_effect=_fake_get_summary):
            resp = client.post(
                "/api/value/summary",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                json={
                    "filters": {},
                    "start_time": "2026-06-17T00:00:00Z",
                    "end_time": "2026-06-17T23:59:59Z",
                },
            )
        assert resp.status_code == 200, resp.text
        assert captured["start_time"] == start.isoformat()
        assert captured["end_time"] == end.isoformat()
    finally:
        app.dependency_overrides.pop(build_request_context, None)


def test_value_summary_rollup_crosses_active_hour(client, in_memory_duckdb, test_service_source):
    """Verify _try_overview_from_rollup and _try_network_from_rollup succeed when crosses_active=True
    (including GROUP BY 1 on the active-hour raw branch and protocol_time_series generation)."""
    import os
    import shutil
    from datetime import UTC, datetime, timedelta

    from backend.core.rollups import (
        NETWORK_SUMMARY_BUNDLE_FILENAME,
        OVERVIEW_BUNDLE_FILENAME,
        _hour_bundled_root,
    )
    from backend.utils.date_utils import iso_z

    table_name = _safe_table(test_service_source["name"])
    now = datetime.now(UTC).replace(minute=10, second=0, microsecond=0)
    prev_hour = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    in_memory_duckdb.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMPTZ,
            cache VARCHAR,
            resp_bytes BIGINT,
            is_shield BOOLEAN,
            waf INT,
            waf_resp INT,
            elapsed DOUBLE,
            protocol VARCHAR,
            is_ssl BOOLEAN,
            ipv6 BOOLEAN
        )
    """)
    in_memory_duckdb.execute(
        f"""
        INSERT INTO {table_name} VALUES
        (TIMESTAMPTZ '{iso_z(now)}', 'HIT', 1000, true, 0, 200, 1200.0, 'HTTP/3', true, true),
        (TIMESTAMPTZ '{iso_z(now)}', 'MISS', 2000, false, 1, 406, 4800.0, 'HTTP/2', true, false)
        """
    )

    bundled_root = _hour_bundled_root(test_service_source)
    hour_dir = os.path.join(bundled_root, f"hour={prev_hour.strftime('%Y-%m-%d-%H')}")
    os.makedirs(hour_dir, exist_ok=True)
    ov_path = os.path.join(hour_dir, OVERVIEW_BUNDLE_FILENAME)
    net_path = os.path.join(hour_dir, NETWORK_SUMMARY_BUNDLE_FILENAME)

    try:
        in_memory_duckdb.execute(
            f"""
            COPY (
                SELECT
                    TIMESTAMPTZ '{iso_z(prev_hour)}' AS hour_start,
                    CAST(10 AS BIGINT) AS requests,
                    CAST(8 AS BIGINT) AS hit_requests,
                    CAST(2 AS BIGINT) AS miss_requests,
                    CAST(0 AS BIGINT) AS pass_requests,
                    CAST(0 AS BIGINT) AS synth_requests,
                    CAST(2 AS BIGINT) AS origin_requests,
                    CAST(8000 AS BIGINT) AS bandwidth_saved_bytes,
                    CAST(10000 AS BIGINT) AS total_bandwidth_bytes,
                    CAST(4 AS BIGINT) AS shield_hit_requests,
                    CAST(5 AS BIGINT) AS shield_total_requests,
                    CAST(1 AS BIGINT) AS threats_blocked,
                    CAST(8000.0 AS DOUBLE) AS hit_elapsed_sum,
                    CAST(8 AS BIGINT) AS hit_elapsed_count,
                    CAST(8000.0 AS DOUBLE) AS miss_elapsed_sum,
                    CAST(2 AS BIGINT) AS miss_elapsed_count
            ) TO '{ov_path}' (FORMAT PARQUET)
            """
        )
        in_memory_duckdb.execute(
            f"""
            COPY (
                SELECT
                    TIMESTAMPTZ '{iso_z(prev_hour)}' AS hour_start,
                    CAST(10 AS BIGINT) AS requests,
                    CAST(6 AS BIGINT) AS http3_requests,
                    CAST(3 AS BIGINT) AS h2_requests,
                    CAST(1 AS BIGINT) AS h11_requests,
                    CAST(0 AS BIGINT) AS other_proto_requests,
                    CAST(10 AS BIGINT) AS tls_requests,
                    CAST(5 AS BIGINT) AS ipv6_requests
            ) TO '{net_path}' (FORMAT PARQUET)
            """
        )

        resp = client.post(
            "/api/value/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "sections": ["overview", "caching", "network"],
                "start_time": iso_z(prev_hour),
                "end_time": iso_z(now + timedelta(minutes=5)),
                "chart_interval": "1 hour",
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["overview"]["total_requests"] == 12
        assert body["caching"]["hit_requests"] == 9
        assert body["network"]["total_requests"] == 12
        assert len(body["network"]["protocol_time_series"]) > 0
    finally:
        shutil.rmtree(os.path.dirname(bundled_root), ignore_errors=True)
