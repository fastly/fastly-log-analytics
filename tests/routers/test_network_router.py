from unittest.mock import patch

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_network_health_full_response(client, in_memory_duckdb, test_service_source):
    """sections omitted → full response with shielding_analysis present
    (or null on best-effort failure) and all core keys populated."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, table, logs)

    response = client.post(
        "/api/network-health",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}},
    )
    assert response.status_code == 200
    data = response.json()
    assert "shielding_analysis" in data
    assert "summary" in data
    assert "heatmap" in data


def test_network_health_rejects_unknown_section(client, in_memory_duckdb, test_service_source):
    """sections=['not_a_section'] returns 400 (router) or 422 (Pydantic
    Literal). Either is an explicit reject so the FE never gets a
    silently-degraded 200."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    insert_mock_logs(in_memory_duckdb, table, logs)

    response = client.post(
        "/api/network-health",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["not_a_section"]},
    )
    assert response.status_code in (400, 422)


def test_network_health_shielding_only_skips_core_temp(client, in_memory_duckdb, test_service_source):
    """sections=['shielding_analysis'] must skip the network temp table
    materialization entirely — proves the router-level CORE_SECTIONS
    gate works. Confirmed via absence of network-temp section_timings
    entries (the shielding code path runs through origin and has its
    own telemetry shape)."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, table, logs)

    response = client.post(
        "/api/network-health",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["shielding_analysis"]},
    )
    assert response.status_code == 200
    data = response.json()
    # shielding key must be present (best-effort; may be null when origin
    # join produces no rows but the field is always emitted under the gate)
    assert "shielding_analysis" in data
    timings = {t["section"] for t in data.get("_section_timings", [])}
    # None of the network-temp section_timings should fire — those are
    # only appended inside repo.get_health which the router skips when no
    # core section is requested.
    for blocked in {"temp_table_create", "heatmap_query", "map_query", "metro_query"}:
        assert blocked not in timings, f"core query {blocked} fired despite shielding-only selector; got {timings}"


def test_network_health_core_plus_shielding_selector(client, in_memory_duckdb, test_service_source):
    """Multi-section selector covers both code paths in one request."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, table, logs)

    response = client.post(
        "/api/network-health",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["leaderboard", "shielding_analysis"]},
    )
    assert response.status_code == 200
    data = response.json()
    assert "leaderboard" in data
    assert "shielding_analysis" in data


def test_network_health_shielding_failure_returns_error_sentinel(client, in_memory_duckdb, test_service_source):
    """M2 (shielding audit 2026-06-30): a handler-level failure in
    get_shielding_analysis must NOT be swallowed into a null/empty payload
    that's indistinguishable from "no data". The router logs it and returns
    an explicit ``{error: true}`` sentinel so the UI can surface it."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    insert_mock_logs(in_memory_duckdb, table, logs)

    with patch(
        "backend.repositories.origin.get_shielding_analysis",
        side_effect=RuntimeError("boom"),
    ):
        response = client.post(
            "/api/network-health",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sections": ["shielding_analysis"]},
        )

    assert response.status_code == 200
    sa = response.json()["shielding_analysis"]
    assert sa is not None
    assert sa["error"] is True
    assert sa["has_data"] is False


def test_get_pop_health_returns_data(client, in_memory_duckdb, test_service_source):
    from datetime import UTC, datetime, timedelta

    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["pop"] = "IAD"
        log["status"] = 200
        log["tcp_rtt"] = 12000
        log["ttfb"] = 50.0
        log["cache"] = "HIT"
        log["resp_bytes"] = 1000
    insert_mock_logs(in_memory_duckdb, table, logs)

    now = datetime.now(UTC)
    st = (now - timedelta(hours=3)).replace(tzinfo=None).isoformat()
    et = (now + timedelta(hours=3)).replace(tzinfo=None).isoformat()

    response = client.get(
        f"/api/network/pop-health?start_time={st}&end_time={et}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) > 0
    assert data[0]["pop"] == "IAD"
    assert data[0]["requests"] == 5
    assert data[0]["errors"] == 0
    assert data[0]["cache_hit_rate"] == 100.0
