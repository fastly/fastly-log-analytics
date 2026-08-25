from unittest.mock import patch

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _seed_performance_table(con, src) -> None:
    logs = generate_mock_logs(src, num_logs=10, hours_ago=1)
    for log in logs:
        log["elapsed"] = 120
        log["ttfb"] = 40.0
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def test_performance_aggregates_default(client, in_memory_duckdb, test_service_source):
    _seed_performance_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}},
    )
    assert resp.status_code == 200
    assert "top_urls" in resp.json()


def test_performance_aggregates_range_token(client, in_memory_duckdb, test_service_source):
    _seed_performance_table(in_memory_duckdb, test_service_source)
    with (
        patch("backend.config.get_status", return_value={"earliest_log_at": "2026-08-24T00:00:00Z"}),
        patch("backend.utils.time_window.is_valid_range_token", return_value=True),
        patch(
            "backend.utils.time_window.resolve_window", return_value=("2026-08-24T12:00:00Z", "2026-08-24T13:00:00Z")
        ),
    ):
        resp = client.post(
            "/api/performance/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "range_token": "30d", "anchor": "2026-08-24T13:00:00Z"},
        )
    assert resp.status_code == 200
    assert "top_urls" in resp.json()
