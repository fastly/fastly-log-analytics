"""
Integration tests verifying that filters actually narrow results through the full
repository layer — not just that the WHERE clause SQL is generated correctly.
"""

from backend.models.common import FilterSpec
from backend.repositories._base import _safe_table, clear_schema_cols_cache
from backend.repositories.dashboard import get_aggregates
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _logs_with_pops(src, n_jfk: int, n_lhr: int) -> list[dict]:
    """Return logs with deterministic POP values (first n_jfk=JFK, rest=LHR)."""
    logs = generate_mock_logs(src, num_logs=n_jfk + n_lhr)
    for i, log in enumerate(logs):
        log["pop"] = "JFK" if i < n_jfk else "LHR"
    return logs


def _run(con, src, filters):
    return get_aggregates(
        con=con,
        src=src,
        start_time=None,
        end_time=None,
        filters=filters,
        chart_interval="1 minute",
        chart_metric="requests",
    )


class TestIncludeFilter:
    def test_narrows_to_matching_rows(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="include", values=["JFK"])})
        assert result["total_rows"] == 10

    def test_no_match_returns_zero(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="include", values=["SYD"])})
        assert result["total_rows"] == 0

    def test_multi_value_accepts_any_match(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=8, n_lhr=4)
        for log in logs[:3]:
            log["pop"] = "SYD"  # replace first 3 JFK → SYD: 5 JFK + 4 LHR + 3 SYD
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="include", values=["JFK", "LHR"])})
        assert result["total_rows"] == 9  # 5 JFK + 4 LHR


class TestExcludeFilter:
    def test_removes_matching_rows(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="exclude", values=["JFK"])})
        assert result["total_rows"] == 5

    def test_exclude_all_returns_zero(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=0)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="exclude", values=["JFK"])})
        assert result["total_rows"] == 0


class TestFilterViaHttpEndpoint:
    def test_include_filter_applied_through_api(self, client, in_memory_duckdb, test_service_source):
        """Filters must flow from the HTTP request body through to the DuckDB query."""
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        response = client.post(
            "/api/dashboard/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {"pop": {"mode": "include", "values": ["JFK"]}},
                "chart_interval": "1 minute",
                "chart_metric": "requests",
            },
        )

        assert response.status_code == 200, response.text
        assert response.json()["total_rows"] == 10

    def test_exclude_filter_applied_through_api(self, client, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        response = client.post(
            "/api/dashboard/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {"pop": {"mode": "exclude", "values": ["JFK"]}},
                "chart_interval": "1 minute",
                "chart_metric": "requests",
            },
        )

        assert response.status_code == 200, response.text
        assert response.json()["total_rows"] == 5


class TestTunnelRequestsFilter:
    def test_tunnel_requests_include_exclude(self, in_memory_duckdb, test_service_source):
        clear_schema_cols_cache()
        from backend.core.duckdb import _clear_schema_cache

        _clear_schema_cache()

        table_name = _safe_table(test_service_source["name"])

        # Re-create mock logs table with required columns for _tunnel_requests filter
        in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        in_memory_duckdb.execute(f"""
            CREATE TABLE {table_name} (
                ip VARCHAR, pop VARCHAR, rtt_min INTEGER, tcp_rtt INTEGER,
                lat DOUBLE, lon DOUBLE, asn INTEGER, timestamp TIMESTAMPTZ,
                status INTEGER, elapsed INTEGER, cache VARCHAR, resp_bytes INTEGER
            )
        """)

        in_memory_duckdb.execute(f"""
            INSERT INTO {table_name} VALUES
            ('1.1.1.1', 'SJC', 1000, 1000, 37.0, -121.0, 1111, '2026-08-01 12:00:00Z', 200, 1000, 'HIT', 100),
            ('2.2.2.2', 'SJC', 5, 1000, 37.0, -121.0, 2222, '2026-08-01 12:01:00Z', 200, 1000, 'HIT', 100),
            ('3.3.3.3', 'SJC', 10, 1000, 37.0, -121.0, 2222, '2026-08-01 12:02:00Z', 200, 1000, 'HIT', 100),
            ('4.4.4.4', 'SJC', 500, 1000, 37.0, -121.0, 3333, '2026-08-01 12:03:00Z', 200, 1000, 'HIT', 100)
        """)

        # Filter include
        result_inc = _run(
            in_memory_duckdb, test_service_source, {"_tunnel_requests": FilterSpec(mode="include", values=["true"])}
        )
        # Should match rows 2 and 3 (2 rows)
        assert result_inc["total_rows"] == 2

        # Filter exclude
        result_exc = _run(
            in_memory_duckdb, test_service_source, {"_tunnel_requests": FilterSpec(mode="exclude", values=["true"])}
        )
        # Should match rows 1 and 4 (2 rows)
        assert result_exc["total_rows"] == 2


class TestBotNameIpFilter:
    def test_bot_name_ip_containment_filtering(self, in_memory_duckdb, test_service_source):
        from unittest.mock import patch

        from backend.utils import bot_sources

        # Create table with IP column
        table_name = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        in_memory_duckdb.execute(f"""
            CREATE TABLE {table_name} (
                ip VARCHAR, timestamp TIMESTAMPTZ
            )
        """)

        # Insert some IPs inside and outside the subnet
        in_memory_duckdb.execute(f"""
            INSERT INTO {table_name} VALUES
            ('192.168.10.5', '2026-08-01 12:00:00Z'),
            ('10.0.0.1', '2026-08-01 12:01:00Z'),
            ('192.168.10.100', '2026-08-01 12:02:00Z'),
            ('172.16.0.1', '2026-08-01 12:03:00Z')
        """)

        # Mock an IP-based bot in BOT_SOURCES and its cached data
        fake_sources = [
            {"id": "tor-exit-nodes", "name": "Tor Exit Nodes", "url": "https://x", "enabled": True, "type": "ip"},
        ]

        def fake_get_bot(bot_id):
            if bot_id == "tor-exit-nodes":
                return {
                    "id": "tor-exit-nodes",
                    "name": "Tor Exit Nodes",
                    "type": "ip",
                    "verification": {"domains": [], "cidrs": ["192.168.10.0/24"]},
                }
            return None

        with (
            patch.object(bot_sources, "BOT_SOURCES", fake_sources),
            patch("backend.utils.bot_sources.get_bot_by_id", side_effect=fake_get_bot),
        ):
            # Include filter should match '192.168.10.5' and '192.168.10.100' (2 rows)
            res_inc = _run(
                in_memory_duckdb,
                test_service_source,
                {"_bot_name": FilterSpec(mode="include", values=["tor-exit-nodes"])},
            )
            assert res_inc["total_rows"] == 2

            # Exclude filter should match '10.0.0.1' and '172.16.0.1' (2 rows)
            res_exc = _run(
                in_memory_duckdb,
                test_service_source,
                {"_bot_name": FilterSpec(mode="exclude", values=["tor-exit-nodes"])},
            )
            assert res_exc["total_rows"] == 2


class TestEmptyIpExclusion:
    def test_excludes_null_and_empty_ip_records(self, in_memory_duckdb, test_service_source):
        clear_schema_cols_cache()
        from backend.core.duckdb import _clear_schema_cache

        _clear_schema_cache()

        table_name = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        in_memory_duckdb.execute(f"""
            CREATE TABLE {table_name} (
                ip VARCHAR, pop VARCHAR, timestamp TIMESTAMPTZ
            )
        """)

        # Insert 2 valid requests, 1 with null IP, and 1 with empty string IP
        in_memory_duckdb.execute(f"""
            INSERT INTO {table_name} VALUES
            ('1.1.1.1', 'SJC', '2026-08-01 12:00:00Z'),
            (NULL, 'SJC', '2026-08-01 12:01:00Z'),
            ('2.2.2.2', 'SJC', '2026-08-01 12:02:00Z'),
            ('', 'SJC', '2026-08-01 12:03:00Z')
        """)

        # Run get_aggregates with no filters. Total rows should be exactly 2 (the valid ones)
        result = _run(in_memory_duckdb, test_service_source, {})
        assert result["total_rows"] == 2
