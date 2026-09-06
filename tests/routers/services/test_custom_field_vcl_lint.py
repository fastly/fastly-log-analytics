"""Contract test for POST /api/services/{id}/custom-fields/validate-vcl.

The frontend's CustomFieldDrawer relies on this endpoint to surface
validation errors before save. The existing
[frontend/__tests__/components/CustomFieldDrawer.test.tsx]
mocks the API; this file pins the actual response *shape* so the mock
can't drift away from production reality.

Covers:
- 404 when the service doesn't exist
- Valid expression → ``valid: True``, no errors, ``format_length`` populated
- Invalid expression → ``valid: False``, at least one error
- The response model exposes the fields the drawer reads
  (``valid``, ``errors``, ``warnings``, ``format_length`` / ``format_length_limit``)
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import app

_CFG = {
    "service_id": "svc-lint",
    "name": "Lint Test",
    "log_fields": {
        "preset": "standard",
        "groups": ["A"],
        "schema_version": 2,
        "custom_fields": [],
    },
}


def _client():
    return TestClient(app)


def test_validate_vcl_404_for_missing_service():
    with patch("backend.config.load_config", return_value=None):
        r = _client().post(
            "/api/services/missing/custom-fields/validate-vcl",
            json={"vcl_log_expression": "req.http.Host", "collection_stage": "edge"},
        )
    assert r.status_code == 404


def test_validate_vcl_valid_expression_returns_valid_true():
    """A simple, well-formed expression must produce the contract the
    drawer relies on: ``valid=True``, empty ``errors``, a numeric
    ``format_length``, and a ``format_length_limit``."""
    with patch("backend.config.load_config", return_value=dict(_CFG)):
        r = _client().post(
            "/api/services/svc-lint/custom-fields/validate-vcl",
            json={
                "vcl_log_expression": "req.http.Host",
                "collection_stage": "edge",
            },
        )
    assert r.status_code == 200
    body = r.json()
    assert body["valid"] is True
    assert body["errors"] == []
    assert isinstance(body["warnings"], list)
    assert isinstance(body["format_length"], int)
    assert body["format_length"] > 0
    assert body["format_length_limit"] == 12000


def test_validate_vcl_invalid_expression_returns_errors():
    """An obviously broken expression (unbalanced braces) must produce
    ``valid: False`` with at least one entry in ``errors``."""
    with patch("backend.config.load_config", return_value=dict(_CFG)):
        r = _client().post(
            "/api/services/svc-lint/custom-fields/validate-vcl",
            json={
                # Unbalanced + references no real VCL identifier — both
                # the AST validator and the format-length validator
                # should reject this.
                "vcl_log_expression": "req.http.{{ not closed",
                "collection_stage": "edge",
            },
        )
    assert r.status_code == 200
    body = r.json()
    assert body["valid"] is False
    assert len(body["errors"]) >= 1, f"expected at least one error, got: {body}"


def test_validate_vcl_response_shape_matches_drawer_contract():
    """The drawer reads ``valid``, ``errors``, ``warnings``,
    ``format_length_limit``. Any rename breaks the UI silently because the
    component reads the fields untyped (``data as any``)."""
    with patch("backend.config.load_config", return_value=dict(_CFG)):
        r = _client().post(
            "/api/services/svc-lint/custom-fields/validate-vcl",
            json={"vcl_log_expression": "req.http.Host", "collection_stage": "edge"},
        )
    keys = set(r.json().keys())
    required = {"valid", "errors", "warnings", "format_length_limit"}
    missing = required - keys
    assert not missing, f"drawer-contract response keys missing: {missing}"


def test_validate_vcl_collection_stage_origin_accepted():
    """``origin`` stage is a different code path (custom field is
    promoted via beresp headers). The endpoint must accept it."""
    with patch("backend.config.load_config", return_value=dict(_CFG)):
        r = _client().post(
            "/api/services/svc-lint/custom-fields/validate-vcl",
            json={
                "vcl_log_expression": "beresp.http.x-something",
                "collection_stage": "origin",
            },
        )
    assert r.status_code == 200
    assert "valid" in r.json()


def test_validate_vcl_rejects_unknown_collection_stage():
    """Pydantic enforces the ``Literal['edge', 'origin']`` — a typo
    becomes a 422, not a silent passthrough."""
    with patch("backend.config.load_config", return_value=dict(_CFG)):
        r = _client().post(
            "/api/services/svc-lint/custom-fields/validate-vcl",
            json={
                "vcl_log_expression": "req.http.Host",
                "collection_stage": "midway",  # not a real stage
            },
        )
    assert r.status_code == 422
