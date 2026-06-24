"""
Integration tests verifying that filters actually narrow results through the full
repository layer — not just that the WHERE clause SQL is generated correctly.
"""

from backend.models.common import FilterSpec
from backend.repositories._base import _safe_table
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
