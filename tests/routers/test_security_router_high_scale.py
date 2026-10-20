"""High-scale dispatch of the Security router: sections, masking and the analyst window reach ClickHouse."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from backend.core.request_context import build_request_context
from backend.main import app
from backend.models.security import SecurityAggregatesResponse, SecurityProxiesResponse
from backend.utils.remote_access import TimeBounds
from tests.conftest import MOCK_SERVICE_ID, override_request_context


def _register_high_scale(monkeypatch, test_service_source):
    service = SimpleNamespace(service_id=test_service_source["service_id"])
    monkeypatch.setattr(
        "backend.high_scale.registry.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda _service_id: service),
    )
    return service


def _analyst_override(test_service_source, in_memory_duckdb, *, mask_ips: bool, time_bounds=None):
    analyst_session = SimpleNamespace(
        service_ids=[MOCK_SERVICE_ID],
        time_bounds=time_bounds,
        pii_policy={"mask_ips": mask_ips},
    )
    return override_request_context(
        source=test_service_source,
        con=in_memory_duckdb,
        session=analyst_session,
        time_bounds=time_bounds,
    )


def test_aggregates_receive_expanded_sections_and_mask_flag(client, monkeypatch, in_memory_duckdb, test_service_source):
    service = _register_high_scale(monkeypatch, test_service_source)
    hs_aggregates = MagicMock(return_value=SecurityAggregatesResponse())
    monkeypatch.setattr("backend.high_scale.security.security_aggregates", hs_aggregates)
    app.dependency_overrides[build_request_context] = _analyst_override(
        test_service_source, in_memory_duckdb, mask_ips=True
    )
    try:
        resp = client.post(
            "/api/security/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sections": ["tls_fingerprints", "ngwaf_verified_bots"]},
        )
    finally:
        app.dependency_overrides.pop(build_request_context, None)

    assert resp.status_code == 200, resp.text
    hs_aggregates.assert_called_once()
    assert hs_aggregates.call_args.args[0] is service
    assert hs_aggregates.call_args.kwargs["sections"] == {
        "tls_fingerprints",
        "fingerprint_coverage",
        "ngwaf_verified_bots",
        "ngwaf_verified_bots_ts",
    }
    assert hs_aggregates.call_args.kwargs["mask_ips"] is True


def test_threat_intel_is_clamped_to_the_analyst_window(client, monkeypatch, in_memory_duckdb, test_service_source):
    _register_high_scale(monkeypatch, test_service_source)
    hs_threat_intel = MagicMock(return_value={"data": []})
    monkeypatch.setattr("backend.high_scale.security.get_security_threat_intel", hs_threat_intel)
    ceiling_start = datetime.now(UTC) - timedelta(hours=2)
    ceiling_end = datetime.now(UTC)
    app.dependency_overrides[build_request_context] = _analyst_override(
        test_service_source,
        in_memory_duckdb,
        mask_ips=False,
        time_bounds=TimeBounds(start=ceiling_start, end=ceiling_end),
    )
    try:
        resp = client.get(
            "/api/security/threat-intel",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={
                "start_time": (ceiling_start - timedelta(days=30)).isoformat(),
                "end_time": ceiling_end.isoformat(),
            },
        )
    finally:
        app.dependency_overrides.pop(build_request_context, None)

    assert resp.status_code == 200, resp.text
    queried_start = datetime.fromisoformat(hs_threat_intel.call_args.args[1].replace("Z", "+00:00"))
    assert queried_start >= ceiling_start - timedelta(seconds=1)


def test_proxies_export_streams_masked_high_risk_clients(client, monkeypatch, in_memory_duckdb, test_service_source):
    _register_high_scale(monkeypatch, test_service_source)
    hs_proxies = MagicMock(
        return_value=SecurityProxiesResponse(
            active_clients=[
                {"ip": "198.51.100.xxx", "risk_level": "High", "impossible_distance": True},
                {"ip": "198.51.101.xxx", "risk_level": "Medium"},
            ]
        )
    )
    monkeypatch.setattr("backend.high_scale.security.get_proxies_data", hs_proxies)
    app.dependency_overrides[build_request_context] = _analyst_override(
        test_service_source, in_memory_duckdb, mask_ips=True
    )
    try:
        resp = client.get(
            "/api/security/proxies/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={
                "start_time": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
                "end_time": datetime.now(UTC).isoformat(),
                "threshold": "High",
                "format": "plain",
            },
        )
    finally:
        app.dependency_overrides.pop(build_request_context, None)

    assert resp.status_code == 200, resp.text
    assert resp.text.split() == ["198.51.100.xxx"]
    assert hs_proxies.call_args.kwargs["mask_ips"] is True
