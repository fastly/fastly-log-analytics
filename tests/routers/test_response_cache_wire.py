from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from backend.repositories._base import _safe_table
from backend.utils import bot_sources
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


@pytest.fixture
def observation_request(in_memory_duckdb, test_service_source, tmp_path, monkeypatch) -> dict:
    source_dir = tmp_path / "bot-sources"
    source_dir.mkdir()
    (source_dir / "well-known-bots.json").write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "id": "googlebot",
                        "name": "Googlebot",
                        "pattern": {"accepted": ["Googlebot"]},
                        "verification": {"domains": [], "cidrs": []},
                    }
                ]
            }
        )
    )
    monkeypatch.setattr(bot_sources, "_CACHE_DIR", source_dir)
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    for log in logs:
        log.update(
            ua="Googlebot/2.1",
            tcp_rtt=1000,
            asn=15169,
            country="US",
            cmcd_sid="test-session",
            cmcd_bl=1000,
            cmcd_bs=False,
            cmcd_br=1000,
            cmcd_mtp=2000,
            cmcd_ot="v",
            cmcd_sf="h",
            cmcd_cid="test-content",
        )
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    now = datetime.now(UTC)
    return {"start_time": (now - timedelta(hours=2)).isoformat(), "end_time": now.isoformat(), "filters": {}}


@pytest.mark.parametrize(
    "path,sections",
    [
        ("/api/security/aggregates", ["ipv6_adoption"]),
        ("/api/performance/aggregates", ["ttl_dist"]),
        ("/api/cmcd/aggregates", ["overview"]),
        ("/api/dashboard/aggregates", ["core"]),
    ],
)
def test_standard_owner_miss_and_router_peek_hit_survive_wire_serialization(
    client,
    observation_request: dict,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    sections: list[str],
) -> None:
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    body = {**observation_request, "sections": sections}
    headers = {"x-fastly-service-id": MOCK_SERVICE_ID}
    first = client.post(path, json=body, headers=headers)
    assert first.status_code == 200, first.text
    assert first.json()["_response_cache"]["state"] == "miss"
    second = client.post(path, json=body, headers=headers)
    assert second.status_code == 200, second.text
    assert second.json()["_response_cache"]["state"] == "hit"
    assert second.json()["_response_cache"]["prewarmer"] is False
    assert second.json()["_is_cached"] is True
    assert "_debug_queries" not in second.json()


def test_dashboard_bundle_components_keep_independent_observations(client, observation_request: dict) -> None:
    headers = {"x-fastly-service-id": MOCK_SERVICE_ID}
    first = client.post("/api/dashboard/bundle", json=observation_request, headers=headers)
    assert first.status_code == 200, first.text
    assert first.json()["_response_cache"]["state"] == "bypass"
    assert first.json()["aggregates"]["_response_cache"]["state"] == "miss"
    assert first.json()["top_bots"]["_response_cache"]["state"] == "miss"
    assert first.json()["top_bots"]["bots"][0]["name"] == "Googlebot"
    assert first.json()["top_bots"]["bots"][0]["request_count"] == 40
    second = client.post("/api/dashboard/bundle", json=observation_request, headers=headers)
    assert second.status_code == 200, second.text
    assert second.json()["aggregates"]["_response_cache"]["state"] == "hit"
    assert second.json()["top_bots"]["_response_cache"]["state"] == "hit"


def test_origin_composite_is_bypass_not_a_response_miss(client, observation_request: dict) -> None:
    for _ in range(2):
        response = client.post(
            "/api/origin/aggregates",
            json={**observation_request, "sections": ["summary"]},
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )
        assert response.status_code == 200, response.text
        assert response.json()["_response_cache"] == {"state": "bypass", "prewarmer": False}


def test_network_bundle_does_not_confuse_health_with_independent_shielding(client, observation_request: dict) -> None:
    headers = {"x-fastly-service-id": MOCK_SERVICE_ID}
    for expected in ("miss", "hit"):
        response = client.post("/api/network/bundle", json=observation_request, headers=headers)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["_response_cache"]["state"] == "bypass"
        assert body["health"]["_response_cache"]["state"] == expected
        assert body["health"]["_response_cache"]["scope"] == "self"
        assert body["quality"]["_response_cache"]["state"] == "bypass"
        assert body["transport"]["_response_cache"]["state"] == "bypass"
        assert body["health"]["shielding_analysis"]["_response_cache"]["state"] in {"miss", "hit"}
