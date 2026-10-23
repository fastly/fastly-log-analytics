from types import SimpleNamespace

import pytest

from backend.core.high_scale_sections import current_clickhouse_scope


class _Client:
    def __init__(self) -> None:
        self.scopes: list[dict] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.scopes.append(current_clickhouse_scope())
        return []


@pytest.mark.parametrize("reader", ["origin", "performance", "security"])
def test_selected_reader_sections_attribute_real_dispatch_without_extra_queries(reader: str) -> None:
    from backend.high_scale import origin, performance, security

    client = _Client()
    service = SimpleNamespace(service_id="service-test", client=client)
    req = SimpleNamespace(filters={}, bucket_seconds=3600, sort_by="p99")
    start, end = "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    if reader == "origin":
        result = origin.aggregates(service, req, start, end, sections={"status_codes"})
        timings = result["section_timings"]
        source = "projection"
    elif reader == "performance":
        result = performance.performance_aggregates(service, req, start, end, sections={"ttl_dist"})
        timings = result.section_timings
        source = "projection"
    else:
        result = security.security_aggregates(service, req, start, end, sections={"req_size_dist"})
        timings = result.section_timings
        source = "fact"
    assert len(client.scopes) == 1
    assert client.scopes[0]["service_id"] == "service-test"
    assert client.scopes[0]["source"] == source
    assert len(timings) == 1
    assert timings[0]["section_id"] == client.scopes[0]["section_id"]
    assert timings[0]["server_cpu_ms"] is None
    assert timings[0]["timing_kind"] == "client_section_wall"


@pytest.mark.parametrize("reader,expected_queries", [("transport", 2), ("quality", 5)])
def test_network_queries_have_named_fact_sections(reader: str, expected_queries: int) -> None:
    from backend.high_scale import network
    from backend.models.network import NetworkQualityRequest

    client = _Client()
    service = SimpleNamespace(service_id="service-test", client=client)
    start, end = "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    if reader == "transport":
        result = network.get_transport_metrics(service, start, end, {})
    else:
        result = network.network_quality(service, NetworkQualityRequest(), start, end)
    assert len(client.scopes) == expected_queries
    assert all(scope["service_id"] == "service-test" and scope["source"] == "fact" for scope in client.scopes)
    assert len({scope["section_id"] for scope in client.scopes}) == expected_queries
    assert len(result.section_timings) == expected_queries


def test_bot_component_reports_mixed_dispatch_while_queries_keep_concrete_sources() -> None:
    from backend.high_scale import security

    client = _Client()
    service = SimpleNamespace(service_id="service-test", client=client)
    response = security.top_bots(service, SimpleNamespace(filters={}), "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    assert response.section_timings[0]["source"] == "mixed"
    assert [scope["source"] for scope in client.scopes] == ["fact", "projection"]
    assert len({scope["section_id"] for scope in client.scopes}) == 1


def test_network_bundle_keeps_pop_section_identity_without_changing_list_payload() -> None:
    from backend.high_scale import network
    from backend.models.network import NetworkBundleRequest

    client = _Client()
    service = SimpleNamespace(service_id="service-test", client=client)
    response = network.network_bundle(
        service, NetworkBundleRequest(sections=["pop_health"]), "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    )
    assert response.pop_health == []
    assert len(client.scopes) == 1
    assert len(response.section_timings) == 1
    assert response.section_timings[0]["section_id"] == client.scopes[0]["section_id"]
    assert response.section_timings[0]["section"] == "network.pop_health"
