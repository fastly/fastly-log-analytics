import asyncio

import pytest

from backend import config
from backend.core import metadata
from backend.core.fastly import client as fastly_client
from backend.routers import usage


@pytest.mark.parametrize(("requests", "edge"), [(1000, None), (1000, 0), (1000, 800), (0, 0)])
@pytest.mark.parametrize("include_logging_id", [True, False])
def test_prefill_measures_selected_logging_service_without_inventing_missing_edges(
    monkeypatch, requests, edge, include_logging_id
):
    source = config.config_to_source({"service_id": "selected-service", "cdn_service_id": "archive-delivery-service"})
    if not include_logging_id:
        source.pop("logging_service_id")
    row = {"start_time": 1767225600, "requests": requests}
    if edge is not None:
        row["edge_requests"] = edge

    def stats(path, api_key):
        if "/stats/service/selected-service?" in path:
            return {"data": [row]}
        return {"data": [{"start_time": 1767225600, "requests": 50, "edge_requests": 0}]}

    monkeypatch.setattr(usage, "get_fastly_api_key", lambda service_id: "test-token")
    monkeypatch.setattr(usage, "api_key_or_mock", lambda key: key)
    monkeypatch.setattr(usage, "_fastly_api", stats)
    monkeypatch.setattr(fastly_client, "get_active_version", lambda *args: None)
    monkeypatch.setattr(config, "load_config", lambda *args: None)
    monkeypatch.setattr(config, "get_status", lambda *args: {"edge_ratio": 80})
    monkeypatch.setattr(config, "load_usage_logging_config", lambda: {})
    monkeypatch.setattr(config, "get_fastly_logging_service_id", lambda: "other-service")
    monkeypatch.setattr(metadata, "get_node_count_avg", lambda *args: None)
    monkeypatch.setattr(metadata, "get_rum_beacons_avg_per_day", lambda *args: None)
    result = asyncio.run(usage.prefill(source))
    assert result.requests_per_day == requests
    assert result.edge_requests_per_day == edge
