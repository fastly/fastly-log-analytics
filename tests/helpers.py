"""Shared Python test helpers re-exported from a single importable location."""

from __future__ import annotations

from typing import Any

from tests._helpers.wire import (
    assert_rows_wire_identical,
    assert_sections_wire_identical,
    wire,
)
from tests.conftest import (
    MOCK_SERVICE_ID,
    make_test_request_context,
    override_request_context,
)


def make_source(
    service_id: str = MOCK_SERVICE_ID,
    access_level: str = "read_write",
    **overrides: Any,
) -> dict[str, Any]:
    """Construct a standard test service source dictionary."""
    base: dict[str, Any] = {
        "name": service_id,
        "service_id": service_id,
        "display_name": "Admin Overview Test Service",
        "bucket": "test-bucket",
        "prefix": "fastly-logs",
        "endpoint": "https://us-east.object.fastlystorage.app",
        "access_key": "AKIAIOSFODNN7EXAMPLE",
        "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "access_level": access_level,
    }
    base.update(overrides)
    return base


def fake_mutate_config(saved: dict[str, Any], base: dict[str, Any] | None = None):
    """``backend.config.mutate_config`` stand-in: applies ``fn`` to ``base`` and records the result."""

    def _mutate(sid: str, fn, **_kwargs: Any) -> dict[str, Any]:
        cfg = dict(base if base is not None else {"name": "svc", "service_id": "svc"})
        fn(cfg)
        saved["sid"] = sid
        saved["cfg"] = cfg
        return cfg

    return _mutate


__all__ = [
    "MOCK_SERVICE_ID",
    "assert_rows_wire_identical",
    "assert_sections_wire_identical",
    "fake_mutate_config",
    "make_source",
    "make_test_request_context",
    "override_request_context",
    "wire",
]
