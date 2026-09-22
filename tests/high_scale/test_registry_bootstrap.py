from datetime import UTC, datetime

import pytest

from backend.high_scale.registry import HighScaleServiceRegistry
from backend.high_scale.registry_bootstrap import (
    register_high_scale_services,
    register_high_scale_services_from_environment,
)


class FakeClickHouse:
    def __init__(self) -> None:
        self.request_coverage_end = datetime(2026, 9, 12, tzinfo=UTC)
        self.calls: list[str] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        assert params is not None
        assert params["service_id"] == "svc"
        self.calls.append(sql)
        if "max(event_timestamp)" in sql:
            return [
                {
                    "coverage_start": datetime(2026, 9, 1, tzinfo=UTC),
                    "coverage_end": self.request_coverage_end,
                }
            ]
        if "last_visible_event_id" in sql:
            return [{"last_visible_event_id": "00000000-0000-0000-0000-000000000123"}]
        return []


class EmptyClickHouse:
    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        return [{"coverage_start": None, "coverage_end": None, "last_visible_event_id": None}]


class StringClickHouse:
    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        if "max(event_timestamp)" in sql:
            return [
                {
                    "coverage_start": "2026-09-12 20:50:12.123",
                    "coverage_end": "2026-09-12 20:50:12.123",
                }
            ]
        return [{"last_visible_event_id": "a4dc715f-733c-4535-8090-3dd5637e9e0b"}]


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


def test_registered_watermark_reflects_new_clickhouse_visibility() -> None:
    registry = HighScaleServiceRegistry()
    client = FakeClickHouse()

    register_high_scale_services(
        registry,
        client=client,
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        owner_epoch=7,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.watermark().coverage_end == datetime(2026, 9, 12, tzinfo=UTC)

    client.request_coverage_end = datetime(2026, 9, 22, 4, 20, tzinfo=UTC)

    assert service.watermark().coverage_end == datetime(2026, 9, 22, 4, 20, tzinfo=UTC)


def test_register_high_scale_services_avoids_argmax_watermark_scan() -> None:
    registry = HighScaleServiceRegistry()
    client = FakeClickHouse()

    register_high_scale_services(
        registry,
        client=client,
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        owner_epoch=7,
    )

    service = registry.resolve("svc")
    assert service is not None
    service.watermark()

    assert len(client.calls) == 2
    assert all("argMax" not in sql for sql in client.calls)


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


class FakeCatalog:
    def manifests_covering(self, service_id, domain, start, end):
        return ()


def test_register_high_scale_services_wires_cold_tier_when_both_are_available() -> None:
    registry = HighScaleServiceRegistry()
    catalog = FakeCatalog()
    archive = object()

    register_high_scale_services(
        registry,
        client=FakeClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        manifest_catalog=catalog,
        archive_for=lambda service_id: archive,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is catalog
    assert service.archive is archive


def test_register_high_scale_services_leaves_cold_tier_unconfigured_without_archive() -> None:
    registry = HighScaleServiceRegistry()

    register_high_scale_services(
        registry,
        client=FakeClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        manifest_catalog=FakeCatalog(),
        archive_for=lambda service_id: None,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is None
    assert service.archive is None


def test_register_high_scale_services_defaults_cold_tier_to_unconfigured() -> None:
    registry = HighScaleServiceRegistry()

    register_high_scale_services(
        registry,
        client=FakeClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is None
    assert service.archive is None


def test_environment_registration_wires_cold_tier_via_injected_factories(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_ENABLED", "1")
    monkeypatch.setenv("HIGH_SCALE_SERVICE_IDS", "svc")
    monkeypatch.setenv("HIGH_SCALE_CURSOR_SECRET", "local-test-secret")
    registry = HighScaleServiceRegistry()
    catalog = FakeCatalog()
    archive = object()

    register_high_scale_services_from_environment(
        registry,
        client=FakeClickHouse(),
        control_factory=lambda: catalog,
        archive_factory=lambda service_id: archive,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is catalog
    assert service.archive is archive


def test_environment_registration_degrades_when_archive_factory_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_ENABLED", "1")
    monkeypatch.setenv("HIGH_SCALE_SERVICE_IDS", "svc")
    monkeypatch.setenv("HIGH_SCALE_CURSOR_SECRET", "local-test-secret")
    registry = HighScaleServiceRegistry()

    def _boom(service_id: str):
        raise RuntimeError("no FOS source configured")

    register_high_scale_services_from_environment(
        registry,
        client=FakeClickHouse(),
        control_factory=lambda: FakeCatalog(),
        archive_factory=_boom,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is None
    assert service.archive is None


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
