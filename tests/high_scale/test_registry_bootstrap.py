from datetime import UTC, datetime

import pytest

from backend.high_scale.registry import HighScaleServiceRegistry
from backend.high_scale.registry_bootstrap import (
    register_high_scale_services,
    register_high_scale_services_from_environment,
)


class FakeClickHouse:
    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        assert params == {"service_id": "svc"}
        if "FROM request_facts" in sql:
            return [
                {
                    "coverage_start": datetime(2026, 9, 1, tzinfo=UTC),
                    "coverage_end": datetime(2026, 9, 12, tzinfo=UTC),
                    "last_visible_event_id": "00000000-0000-0000-0000-000000000123",
                }
            ]
        return []


class EmptyClickHouse:
    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        return [{"coverage_start": None, "coverage_end": None, "last_visible_event_id": None}]


class StringClickHouse:
    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        return [
            {
                "coverage_start": "2026-09-12 20:50:12.123",
                "coverage_end": "2026-09-12 20:50:12.123",
                "last_visible_event_id": "a4dc715f-733c-4535-8090-3dd5637e9e0b",
            }
        ]


def test_register_high_scale_services_exposes_clickhouse_watermark() -> None:
    registry = HighScaleServiceRegistry()

    register_high_scale_services(
        registry,
        client=FakeClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        owner_epoch=7,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.cursor_secret == b"local-test-secret"
    watermark = service.watermark()
    assert watermark.service_id == "svc"
    assert watermark.domain == "request"
    assert watermark.owner_epoch == 7
    assert watermark.coverage_start == datetime(2026, 9, 1, tzinfo=UTC)
    assert watermark.coverage_end == datetime(2026, 9, 12, tzinfo=UTC)
    assert watermark.last_visible_event_id == "00000000-0000-0000-0000-000000000123"


def test_register_high_scale_services_parses_clickhouse_timestamp_strings() -> None:
    registry = HighScaleServiceRegistry()

    register_high_scale_services(
        registry,
        client=StringClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
    )

    service = registry.resolve("svc")
    assert service is not None
    watermark = service.watermark()
    assert watermark.coverage_start == datetime(2026, 9, 12, 20, 50, 12, 123000, tzinfo=UTC)
    assert watermark.coverage_end == datetime(2026, 9, 12, 20, 50, 12, 123000, tzinfo=UTC)
    assert watermark.last_visible_event_id == "a4dc715f-733c-4535-8090-3dd5637e9e0b"


def test_register_high_scale_services_rejects_empty_secret() -> None:
    registry = HighScaleServiceRegistry()

    try:
        register_high_scale_services(
            registry,
            client=FakeClickHouse(),
            service_ids=("svc",),
            cursor_secret=b"",
        )
    except ValueError as exc:
        assert str(exc) == "high-scale cursor secret is required"
    else:
        raise AssertionError("empty cursor secrets must be rejected")


def test_environment_registration_requires_explicit_service_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_ENABLED", "1")
    monkeypatch.delenv("HIGH_SCALE_SERVICE_IDS", raising=False)

    with pytest.raises(ValueError, match="HIGH_SCALE_SERVICE_IDS"):
        register_high_scale_services_from_environment(HighScaleServiceRegistry(), client=FakeClickHouse())


def test_empty_serving_table_has_no_visible_event_watermark() -> None:
    registry = HighScaleServiceRegistry()
    register_high_scale_services(
        registry,
        client=EmptyClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
    )

    service = registry.resolve("svc")
    assert service is not None
    watermark = service.watermark()
    assert watermark.coverage_start is None
    assert watermark.coverage_end is None
    assert watermark.last_visible_event_id is None
