"""Typed ``response_model``s must not change the wire payload.

Each test compares the HTTP body against what the handler's producer builds
(json-roundtripped), so a model that drops, renames or null-fills a key fails.
"""

from __future__ import annotations

import json


def _roundtrip(obj):
    return json.loads(json.dumps(obj))


def _body(resp):
    """Response JSON minus the telemetry-middleware ``_debug_*``/``_section_timings`` envelope."""
    data = resp.json()
    return {k: v for k, v in data.items() if not k.startswith("_")} if isinstance(data, dict) else data


def test_log_fields_catalog_route_matches_producer(client):
    from backend.routers.bootstrap import _compute_log_fields_catalog

    resp = client.get("/api/log-fields/catalog")
    assert resp.status_code == 200
    assert _body(resp) == _roundtrip(_compute_log_fields_catalog(None))


def test_log_fields_catalog_model_keeps_custom_entry_shape():
    from backend.core import log_fields as lf
    from backend.models.log_fields_catalog import LogFieldsCatalogResponse

    custom = lf.get_custom_fields_catalog_entries(
        {"custom_fields": [{"name": "my_custom", "label": "My", "description": "d"}]}
    )
    payload = {"groups": [], "fields": custom, "insights": [], "presets": {}}
    dumped = LogFieldsCatalogResponse.model_validate(payload).model_dump(mode="json")
    assert dumped == _roundtrip(payload)
    assert "typical_bytes" not in dumped["fields"][0]


def test_bootstrap_response_embeds_catalog_unchanged():
    from backend.models.common import BootstrapResponse
    from backend.routers.bootstrap import _compute_log_fields_catalog

    catalog = _compute_log_fields_catalog(None)
    resp = BootstrapResponse(active_service_id=None, services=[], log_fields_catalog=catalog)
    assert resp.model_dump(mode="json", by_alias=True)["log_fields_catalog"] == _roundtrip(catalog)
