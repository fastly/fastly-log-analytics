import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from backend.core.log_fields import LOG_FIELD_CATALOG
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


def test_get_pop_health_self_heals_on_lake_detach_race(client, in_memory_duckdb, test_service_source):
    """A DuckLake ``lake``-detach-race CatalogException on the raw base-table
    fallback must self-heal (one retry) instead of surfacing a 500 (Trap #35)."""
    from datetime import UTC, datetime, timedelta

    import duckdb

    from backend.routers import network as network_mod

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

    real_track_query = network_mod.track_query
    calls = {"n": 0}

    class _RaiseOnEnter:
        def __enter__(self):
            raise duckdb.CatalogException(
                'Catalog Error: Table with name "lake.logs_x" does not exist because schema "lake" does not exist'
            )

        def __exit__(self, *_a):
            return False

    def flaky_track_query(con, query, params, label):
        if label == "pop_health":
            calls["n"] += 1
            if calls["n"] == 1:
                return _RaiseOnEnter()
        return real_track_query(con, query, params, label)

    with (
        patch.object(network_mod, "track_query", flaky_track_query),
        patch("backend.core.iceberg._core.update_iceberg_view", lambda *a, **k: None),
        patch("backend.core.iceberg._core.clear_source_caches", lambda *a, **k: None),
    ):
        response = client.get(
            f"/api/network/pop-health?start_time={st}&end_time={et}",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data[0]["pop"] == "IAD"
    assert data[0]["requests"] == 5
    assert calls["n"] == 2  # failed once, self-healed, retried once


def test_network_bundle_post_full(client, in_memory_duckdb, test_service_source):
    """POST /api/network/bundle returns all requested sections."""
    from datetime import UTC, datetime, timedelta

    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for i, log in enumerate(logs):
        log["pop"] = "IAD" if i < 5 else "SJC"
        log["status"] = 200
        log["tcp_rtt"] = 20_000  # 20ms in µs
        log["ttfb"] = 45.0
        log["cache"] = "HIT"
        log["resp_bytes"] = 2000
        log["retrans"] = 1
        log["ploss"] = 0.005
        log["delivery_rate"] = 10_000_000  # 10 MB/s -> 80 Mbps
        log["proto"] = "HTTP/2"
        log["asn"] = 7922
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, table, logs)

    now = datetime.now(UTC)
    st = (now - timedelta(hours=3)).replace(tzinfo=None).isoformat()
    et = (now + timedelta(hours=3)).replace(tzinfo=None).isoformat()

    response = client.post(
        "/api/network/bundle",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": st,
            "end_time": et,
            "sections": ["health", "quality", "pop_health", "transport"],
            "filters": {},
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "health" in data and data["health"] is not None
    assert "quality" in data and data["quality"] is not None
    assert "pop_health" in data and data["pop_health"] is not None
    assert "transport" in data and data["transport"] is not None
    assert len(data["pop_health"]) == 2

    transport = data["transport"]
    assert transport["total_requests"] == 10
    assert transport["tcp_rtt_p50_ms"] == 20.0
    assert transport["retrans_rate_pct"] == 1.0
    assert transport["packet_loss_pct"] == 0.5  # 0.005 * 100
    assert transport["delivery_rate_mbps"] == 80.0
    assert len(transport["protocol_distribution"]) > 0


def test_network_bundle_get_full(client, in_memory_duckdb, test_service_source):
    """GET /api/network/bundle returns 200 with default sections."""
    from datetime import UTC, datetime, timedelta

    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["pop"] = "LHR"
        log["status"] = 200
        log["tcp_rtt"] = 30_000
        log["proto"] = "HTTP/3"
        log["asn"] = 13335
        log["country"] = "GB"
    insert_mock_logs(in_memory_duckdb, table, logs)

    now = datetime.now(UTC)
    st = (now - timedelta(hours=2)).replace(tzinfo=None).isoformat()
    et = (now + timedelta(hours=2)).replace(tzinfo=None).isoformat()

    response = client.get(
        f"/api/network/bundle?start_time={st}&end_time={et}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert response.status_code == 200
    data = response.json()
    assert "health" in data
    assert "transport" in data


def test_network_bundle_selective_sections(client, in_memory_duckdb, test_service_source):
    """POST /api/network/bundle with sections=['transport'] executes only transport."""
    from datetime import UTC, datetime, timedelta

    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["tcp_rtt"] = 15_000
        log["proto"] = "HTTP/1.1"
    insert_mock_logs(in_memory_duckdb, table, logs)

    now = datetime.now(UTC)
    st = (now - timedelta(hours=1)).replace(tzinfo=None).isoformat()
    et = (now + timedelta(hours=1)).replace(tzinfo=None).isoformat()

    response = client.post(
        "/api/network/bundle",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": st,
            "end_time": et,
            "sections": ["transport"],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["transport"] is not None
    assert data["health"] is None
    assert data["quality"] is None
    assert data["pop_health"] is None


def test_network_bundle_missing_optional_fields_fallback(client, in_memory_duckdb, test_service_source):
    """Logs without tcp_rtt/retrans/ploss/delivery_rate must return zeroed TransportMetrics without crashing."""
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table}")

    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["status"] = 200
        # Omit tcp_rtt, retrans, ploss, delivery_rate
        log.pop("tcp_rtt", None)
        log.pop("retrans", None)
        log.pop("ploss", None)
        log.pop("delivery_rate", None)

    # Create table schema excluding the 4 optional transport fields
    excluded = {"tcp_rtt", "retrans", "ploss", "delivery_rate"}
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None and f["id"] not in excluded]
    columns_map = {f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"]) for f in raw_fields}
    for log in logs:
        for k, v in log.items():
            if k not in columns_map and k not in excluded:
                if isinstance(v, bool):
                    columns_map[k] = "BOOLEAN"
                elif isinstance(v, int):
                    columns_map[k] = "BIGINT"
                elif isinstance(v, float):
                    columns_map[k] = "DOUBLE"
                else:
                    columns_map[k] = "VARCHAR"
    schema_def = ", ".join([f'"{col}" {dtype}' for col, dtype in columns_map.items()])
    in_memory_duckdb.execute(f"CREATE TABLE {table} ({schema_def})")

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
        tf_path = tf.name
        for log in logs:
            tf.write(json.dumps(log, default=str) + "\n")
    try:
        cols_sql = "{" + ", ".join(f"'{k}': '{v}'" for k, v in columns_map.items()) + "}"
        in_memory_duckdb.execute(
            f"INSERT INTO {table} BY NAME SELECT * FROM read_json_auto('{tf_path}', columns={cols_sql})"
        )
    finally:
        os.unlink(tf_path)

    now = datetime.now(UTC)
    st = (now - timedelta(hours=1)).replace(tzinfo=None).isoformat()
    et = (now + timedelta(hours=1)).replace(tzinfo=None).isoformat()

    response = client.post(
        "/api/network/bundle",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": st,
            "end_time": et,
            "sections": ["transport"],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["transport"] is not None
    transport = data["transport"]
    assert transport["total_requests"] == 5
    assert transport["has_transport_data"] is False
    assert len(transport["missing_fields"]) == 4
    assert "tcp_rtt" in transport["missing_fields"]


def test_network_bundle_analyst_write_allowed():
    """Verify that /api/network/ is in _ANALYST_ALLOWED_WRITE_PREFIXES so Analyst Path B POSTs succeed."""
    from backend.utils.remote_access import _ANALYST_ALLOWED_WRITE_PREFIXES

    assert any("/api/network/bundle".startswith(prefix) for prefix in _ANALYST_ALLOWED_WRITE_PREFIXES), (
        "/api/network/bundle must be in _ANALYST_ALLOWED_WRITE_PREFIXES"
    )


def test_network_bundle_surfaces_section_queries_in_debug_envelope(client, in_memory_duckdb, test_service_source):
    """Queries run by a bundle section (transport here) must appear in the bundle's
    own ``_debug_queries`` — they were dropped because only the section sub-model
    carried them."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["tcp_rtt"] = 20_000
        log["proto"] = "HTTP/2"
    insert_mock_logs(in_memory_duckdb, table, logs)

    now = datetime.now(UTC)
    response = client.post(
        "/api/network/bundle",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID, "x-debug-responses": "1"},
        json={
            "start_time": (now - timedelta(hours=3)).replace(tzinfo=None).isoformat(),
            "end_time": (now + timedelta(hours=3)).replace(tzinfo=None).isoformat(),
            "sections": ["transport"],
            "filters": {},
        },
    )
    assert response.status_code == 200
    assert len(response.json()["_debug_queries"]) > 0
