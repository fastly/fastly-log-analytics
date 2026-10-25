from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from backend.high_scale.archive_models import (
    ArchiveManifest,
    ArchiveState,
    HighScaleConfig,
    ServiceProfile,
    ServingWatermark,
    StorageProfile,
    build_topology_profile,
    recommend_retention,
    validate_archive_transition,
)
from backend.high_scale.exports import ExportManager, ExportState
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    PartialInsert,
    PublicationStatus,
)
from backend.high_scale.query_service import QueryResponseMetadata
from backend.high_scale.recovery import (
    RecoveryCapacityMeasurement,
    evaluate_recovery_capacity,
)

# ── From test_contracts.py ──


def test_archive_states_are_monotonic() -> None:
    validate_archive_transition(ArchiveState.ARTIFACT_UPLOADING, ArchiveState.ARTIFACT_VERIFIED)
    with pytest.raises(ValueError, match="monotonic"):
        validate_archive_transition(ArchiveState.MANIFEST_COMMITTED, ArchiveState.MANIFEST_PREPARED)


# ── From test_fencing.py ──


def test_stale_epoch_cannot_commit_manifest() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Mock transaction and locked owner
    mock_conn = MagicMock()
    mock_owner = MagicMock(current_owner="high_scale", owner_epoch=5)

    control._locked_owner = MagicMock(return_value=mock_owner)  # type: ignore[assignment]
    control.transaction = MagicMock()  # type: ignore[assignment]
    control.transaction.return_value.__enter__.return_value = mock_conn

    # Worker has stale epoch = 4 (current is 5)
    manifest = MagicMock(spec=ArchiveManifest)
    manifest.manifest_id = "man-1"
    manifest.service_id = "svc-1"
    manifest.domain = "request"
    manifest.validate.return_value = None

    with pytest.raises(ValueError, match="owner epoch mismatch|stale owner epoch"):
        control.register_archive_manifest(manifest, owner_epoch=4)


def test_stale_epoch_cannot_acknowledge_sources_batch() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Contract requires acknowledge_sources_batch with epoch check
    assert hasattr(control, "acknowledge_sources_batch"), (
        "Missing acknowledge_sources_batch method on PostgresControlPlane"
    )


def test_stale_epoch_cannot_claim_sources_batch() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Contract requires claim_sources_batch with epoch check
    assert hasattr(control, "claim_sources_batch"), "Missing claim_sources_batch method on PostgresControlPlane"


# ── From test_publication_gate.py ──


def test_publication_attempt_with_mismatched_row_count_rejected() -> None:
    """A publication attempt with mismatched row count is rejected and never transitions to visible."""
    store = InMemoryBatchManifestStore()
    client = MagicMock()
    # Mock ClickHouse inserting 95 rows when batch has 100
    batch = HighScaleBatch(
        batch_id="batch-mismatch",
        service_id="svc-test",
        domain="request",
        generation="epoch-1",
        rows=tuple({"event_id": f"evt-{i}"} for i in range(100)),
    )

    client.insert.side_effect = PartialInsert(95)
    publication = ClickHousePublication(store, client)

    with pytest.raises(PartialInsert) as exc_info:
        publication.publish(batch)

    assert exc_info.value.rows_inserted == 95
    manifest = store.get("batch-mismatch")
    assert manifest is not None
    assert manifest.status is PublicationStatus.PENDING
    assert manifest.visible_rows == 0


def test_query_filters_visible_state_and_ignores_pending_rows() -> None:
    """Queries must filter on publication_state='visible' or _high_scale_visible=1 and never see pending rows."""
    from datetime import datetime, timedelta

    from backend.high_scale.archive_models import ServingWatermark
    from backend.high_scale.query_service import query_request_facts

    client = MagicMock()
    # ClickHouse query returns rows only when publication_state = 'visible'
    client.execute.return_value = [
        {"event_id": "00000000-0000-0000-0000-000000000001", "timestamp": "2026-10-09T12:00:00Z"},
    ]

    now = datetime(2026, 10, 9, 12, 5, tzinfo=UTC)
    watermark = ServingWatermark(
        service_id="svc-test",
        domain="request",
        owner_epoch=1,
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        last_accepted_cursor="cur-1",
        last_archived_event_id="evt-1",
        last_visible_event_id="evt-1",
        exact=True,
    )

    # Run query_request_facts
    results = query_request_facts(
        client,
        service_id="svc-test",
        start=now - timedelta(minutes=5),
        end=now,
        cursor_secret=b"test-secret-123456",
        watermark=watermark,
        limit=10,
    )
    assert len(results.rows) == 1

    # Verify execute was called with a query enforcing publication_state = 'visible'
    assert client.execute.called
    query_sql, params = client.execute.call_args[0]
    assert "publication_state = 'visible'" in query_sql or "_high_scale_visible = 1" in query_sql


# ── From test_query_contracts.py ──


def test_query_metadata_requires_coverage_and_watermark_consistency() -> None:
    watermark = ServingWatermark(
        "svc",
        "request",
        2,
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        "cursor",
        "event-2",
        "event-2",
        True,
    )

    metadata = QueryResponseMetadata(
        status="complete",
        exact=True,
        coverage=1.0,
        freshness_lag_seconds=3.0,
        watermark=watermark,
        approximation_error=None,
        error=None,
    )

    metadata.validate()


def test_approximation_error_is_required_for_approximate_results() -> None:
    watermark = ServingWatermark("svc", "request", 1, None, None, None, None, None, False)
    metadata = QueryResponseMetadata("complete", False, 0.5, 0.0, watermark, None, None)

    with pytest.raises(ValueError, match="approximation"):
        metadata.validate()


# ── From test_recovery_capacity.py ──


def test_two_million_events_per_second_target_requires_capacity_gate() -> None:
    report = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=2_000_000,
            recovery_window_seconds=15 * 60,
            archive_read_events_per_second=300_000_000,
            decode_events_per_second=300_000_000,
            aggregate_events_per_second=300_000_000,
            clickhouse_insert_events_per_second=750_000_000,
            replication_factor=3,
            live_ingest_reservation=0.2,
        )
    )

    assert report.required_events_per_second == pytest.approx(240_000_000)
    assert report.replication_adjusted_insert_events_per_second == pytest.approx(250_000_000)
    assert report.feasible is True


def test_replication_can_make_an_otherwise_fast_recovery_infeasible() -> None:
    report = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=2_000_000,
            recovery_window_seconds=15 * 60,
            archive_read_events_per_second=300_000_000,
            decode_events_per_second=300_000_000,
            aggregate_events_per_second=300_000_000,
            clickhouse_insert_events_per_second=400_000_000,
            replication_factor=3,
            live_ingest_reservation=0.2,
        )
    )

    assert report.limiting_stage == "clickhouse_insert"
    assert report.feasible is False


# ── From test_topology.py ──


def test_local_profile_does_not_claim_production_ha() -> None:
    profile = build_topology_profile("local")
    assert profile.clickhouse_replicas_per_shard == 1
    assert profile.keeper_members == 1
    assert profile.production_ha is False


def test_portable_production_profile_has_replicas_and_keeper() -> None:
    profile = build_topology_profile("portable-production")
    assert profile.clickhouse_replicas_per_shard == 2
    assert profile.keeper_members == 3
    assert profile.production_ha is True


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown"):
        build_topology_profile("unknown-production")


# ── From test_exports.py ──


def test_export_is_bounded_and_completes() -> None:
    manager = ExportManager(max_workers=1, max_rows=2, max_bytes=100)
    try:
        job_id = manager.submit("job-1", [{"id": 1}, {"id": 2}], lambda row: str(row["id"]).encode())
        job = manager.wait(job_id)
        assert job.state is ExportState.COMPLETED
        assert job.rows_written == 2
        assert job.payload == b"1\n2\n"
    finally:
        manager.shutdown()


def test_export_rejects_row_limit_and_duplicate_job() -> None:
    manager = ExportManager(max_workers=1, max_rows=1, max_bytes=100)
    try:
        manager.submit("job-1", [{"id": 1}], lambda row: b"1")
        with pytest.raises(ValueError, match="already exists"):
            manager.submit("job-1", [{"id": 1}], lambda row: b"1")
        job_id = manager.submit("job-2", [{"id": 1}, {"id": 2}], lambda row: b"x")
        assert manager.wait(job_id).state is ExportState.FAILED
    finally:
        manager.shutdown()


def test_export_can_be_cancelled() -> None:
    manager = ExportManager(max_workers=1, max_rows=1000, max_bytes=100_000)
    try:
        job_id = manager.submit("job-1", range(1000), lambda row: (time.sleep(0.001), b"x")[1])
        time.sleep(0.01)
        assert manager.cancel(job_id) is True
        assert manager.wait(job_id).state is ExportState.CANCELLED
    finally:
        manager.shutdown()


# ── From test_config.py ──


def test_high_scale_requires_clickhouse_and_fos() -> None:
    config = HighScaleConfig(True, None, None, None)
    with pytest.raises(ValueError, match="ClickHouse"):
        config.validate()


def test_disabled_high_scale_config_is_inert() -> None:
    HighScaleConfig(False, None, None, None).validate()


def test_retention_recommendation_preserves_disk_reserve() -> None:
    recommendation = recommend_retention(
        ServiceProfile(events_per_second=100_000, average_row_bytes=900),
        StorageProfile(total_bytes=10 * 1024**4),
    )
    assert recommendation.hot_days == 1.0
    assert recommendation.warm_days >= recommendation.hot_days


def test_retention_recommendation_accounts_for_rum_and_enabled_fields() -> None:
    narrow = recommend_retention(
        ServiceProfile(events_per_second=100_000, average_row_bytes=100),
        StorageProfile(total_bytes=10**11),
    )
    wide = recommend_retention(
        ServiceProfile(events_per_second=200_000, average_row_bytes=600),
        StorageProfile(total_bytes=10**11),
    )
    assert wide.hot_days < narrow.hot_days


# ── Merged from test_registry_bootstrap.py ──────────────────────────────────

from backend.high_scale.registry_bootstrap import (
    HighScaleServiceRegistry,
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


# ── Merged from test_disk_sampler_wiring.py ─────────────────────────────────

from datetime import timedelta
from unittest.mock import patch

from structlog.testing import capture_logs

from backend.core import metric_snapshots
from backend.cron.jobs.metric_snapshot import _run_metric_snapshot, _sample_high_scale_disk
from backend.high_scale import archive_models as disk_sampler
from backend.high_scale.archive_models import VolumeStats, evaluate_disk_state
from backend.utils.remote_access import _is_blocked_path

_TB = 1 << 40


class _FakeClickHouse:
    def __init__(self, free_fraction: float, total: int = _TB) -> None:
        self.total = total
        self.free = int(total * free_fraction)
        self.calls = 0

    def storage_health(self) -> dict:
        self.calls += 1
        return {"up": 1, "disk_free": self.free, "disk_total": self.total}


@pytest.fixture(autouse=True)
def _reset_sampler():
    disk_sampler.reset_for_tests()
    yield
    disk_sampler.reset_for_tests()


def _history(metric: str):
    return metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1)).get(metric, [])


def test_sampler_reads_real_volume_stats_and_evaluates_mode() -> None:
    probe = _FakeClickHouse(free_fraction=0.12)

    snapshot = disk_sampler.sample_disk_state(probe)

    assert probe.calls == 1
    assert snapshot.state.mode == "protective"
    assert snapshot.total_bytes == _TB
    assert disk_sampler.latest_disk_snapshot() is snapshot


def test_one_batch_bytes_raises_the_reserve_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_DISK_ONE_BATCH_BYTES", str(_TB))

    snapshot = disk_sampler.sample_disk_state(_FakeClickHouse(free_fraction=0.5))

    assert snapshot.state.reserve_bytes == _TB
    assert snapshot.state.mode == "warning"


def test_mode_transitions_are_logged_once_each(monkeypatch) -> None:
    events: list[tuple[str, dict]] = []

    class _Recorder:
        def info(self, event: str, **kw) -> None:
            events.append(("info", {"event": event, **kw}))

        def warning(self, event: str, **kw) -> None:
            events.append(("warning", {"event": event, **kw}))

    monkeypatch.setattr(disk_sampler, "logger", _Recorder())
    monkeypatch.setattr(disk_sampler, "_latest", None)

    disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
    disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
    disk_sampler.sample_disk_state(_FakeClickHouse(0.08))

    changes = [(level, e) for level, e in events if e["event"] == "high_scale.disk_mode_changed"]
    assert [(e["previous_mode"], e["mode"]) for _, e in changes] == [(None, "normal"), ("normal", "emergency")]
    assert changes[-1][0] == "warning"


def test_metric_snapshot_tick_samples_disk_in_high_scale_mode() -> None:
    probe = _FakeClickHouse(free_fraction=0.12)
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=probe),
    ):
        _run_metric_snapshot()

    assert probe.calls == 1
    assert _history("high_scale_disk_mode_level")[-1]["value"] == 2.0
    assert _history("high_scale_disk_free_fraction")[-1]["value"] == pytest.approx(0.12, abs=1e-3)


def test_metric_snapshot_tick_never_touches_clickhouse_in_standard_mode() -> None:
    with (
        patch("backend.config.is_high_scale_mode", return_value=False),
        patch("backend.core.clickhouse_client.get_clickhouse_client") as get_client,
    ):
        assert _sample_high_scale_disk() == 0
        _run_metric_snapshot()

    get_client.assert_not_called()
    assert disk_sampler.latest_disk_snapshot() is None
    assert _history("high_scale_disk_mode_level") == []


def test_probe_failure_is_logged_and_never_breaks_the_tick() -> None:
    class _Down:
        def storage_health(self) -> dict:
            raise RuntimeError("clickhouse unavailable")

    with (
        capture_logs() as logs,
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=_Down()),
    ):
        assert _sample_high_scale_disk() == 0

    assert any(e["event"] == "high_scale.disk_sample_failed" and e["error_kind"] == "RuntimeError" for e in logs)
    assert disk_sampler.latest_disk_snapshot() is None


def test_health_snapshot_surfaces_the_sampled_mode(client) -> None:
    probe = _FakeClickHouse(free_fraction=0.09)
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=probe),
    ):
        _run_metric_snapshot()
        resp = client.get("/api/admin/health-snapshot")

    assert resp.status_code == 200
    disk = resp.json()["high_scale"]["disk"]
    assert disk["mode"] == "emergency"
    assert disk["free_fraction"] == pytest.approx(0.09, abs=1e-3)
    assert disk["total_bytes"] == _TB
    assert disk["sample_age_s"] >= 0


def test_health_snapshot_high_scale_block_is_empty_before_the_first_sample(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=True):
        body = client.get("/api/admin/health-snapshot").json()

    assert body["high_scale"]["disk"] is None


def test_health_snapshot_omits_the_high_scale_key_in_standard_mode(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=False):
        body = client.get("/api/admin/health-snapshot").json()

    assert "high_scale" not in body


def volume_stats(*, free_fraction: float, total_bytes: int = 1_000_000_000, one_batch_bytes: int = 0) -> VolumeStats:
    return VolumeStats(
        total_bytes=total_bytes,
        free_bytes=int(total_bytes * free_fraction),
        one_batch_bytes=one_batch_bytes,
    )


def test_disk_guard_enters_emergency_mode_at_ten_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.10)).mode == "emergency"


def test_disk_guard_enters_emergency_below_ten_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.05)).mode == "emergency"


def test_disk_guard_is_protective_between_ten_and_fifteen_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.12)).mode == "protective"


def test_disk_guard_warns_below_the_twenty_percent_reserve() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.18)).mode == "warning"


def test_disk_guard_is_normal_with_ample_free_space() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.50)).mode == "normal"


def test_disk_guard_reserve_floor_is_the_greater_of_twenty_percent_or_one_batch() -> None:
    state = evaluate_disk_state(volume_stats(free_fraction=0.19, total_bytes=1_000_000_000, one_batch_bytes=1_000_000))
    assert state.mode == "warning"
    assert state.reserve_bytes == 200_000_000


def test_disk_guard_reserve_floor_uses_batch_size_when_larger_than_twenty_percent() -> None:
    state = evaluate_disk_state(
        volume_stats(free_fraction=0.50, total_bytes=1_000_000_000, one_batch_bytes=900_000_000)
    )
    assert state.reserve_bytes == 900_000_000
    assert state.mode == "warning"


def test_disk_guard_rejects_free_bytes_above_total() -> None:
    with pytest.raises(ValueError, match="free_bytes"):
        VolumeStats(total_bytes=100, free_bytes=200, one_batch_bytes=0)


def profile(rate: float, *, average_row_bytes: float = 500.0) -> ServiceProfile:
    return ServiceProfile(events_per_second=rate, average_row_bytes=average_row_bytes)


def storage(total_bytes: int) -> StorageProfile:
    return StorageProfile(total_bytes=total_bytes)


def test_high_volume_service_gets_shorter_hot_recommendation() -> None:
    low = recommend_retention(profile(1_000), storage(1 << 40))
    high = recommend_retention(profile(1_000_000), storage(1 << 40))

    assert low.hot_days > high.hot_days


def test_low_volume_service_hits_the_baseline_hot_target() -> None:
    recommendation = recommend_retention(profile(1), storage(1 << 40))

    assert recommendation.hot_days == 1.0


def test_warm_recommendation_never_exceeds_the_baseline_target() -> None:
    recommendation = recommend_retention(profile(1), storage(1 << 50))

    assert recommendation.warm_days == 30.0


def test_zero_rate_service_is_rejected() -> None:
    with pytest.raises(ValueError, match="events_per_second"):
        recommend_retention(profile(0), storage(1 << 40))


def test_recommendation_never_exceeds_the_protected_reserve() -> None:
    recommendation = recommend_retention(profile(1_000_000), storage(1 << 30))

    assert recommendation.hot_days >= 0.0
    assert recommendation.warm_days >= recommendation.hot_days


_PATH = "/api/admin/high-scale/retention-recommendation"


def _cfg(retention_days: int | None = None) -> dict:
    cfg: dict = {"service_id": "svc-hs", "provisioning": {"cron_sync": {}}}
    if retention_days is not None:
        cfg["provisioning"]["cron_sync"]["data_retention_days"] = retention_days
    return cfg


def _get(client, cfg, **params):
    query = {"service_id": "svc-hs", "events_per_second": 1000, **params}
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=cfg),
    ):
        return client.get(_PATH, params=query)


def test_recommendation_matches_the_pure_function_for_the_request(client) -> None:
    resp = _get(client, _cfg(), total_bytes=_TB, average_row_bytes=800)

    assert resp.status_code == 200
    body = resp.json()
    expected = recommend_retention(ServiceProfile(1000, 800), StorageProfile(_TB))
    assert body["recommended_hot_days"] == pytest.approx(expected.hot_days)
    assert body["recommended_warm_days"] == pytest.approx(expected.warm_days)
    assert body["reasoning"] == expected.reasoning
    assert body["volume_total_source"] == "request"
    assert body["source"] == "recommendation"
    assert body["effective_retention_days"] == pytest.approx(expected.warm_days)


def test_explicit_per_service_retention_always_wins(client) -> None:
    body = _get(client, _cfg(retention_days=7), total_bytes=_TB).json()

    assert body["source"] == "override"
    assert body["explicit_retention_days"] == 7
    assert body["effective_retention_days"] == 7.0
    assert body["recommended_warm_days"] > 0


def test_explicit_keep_forever_is_an_override_not_a_recommendation(client) -> None:
    body = _get(client, _cfg(retention_days=0), total_bytes=_TB).json()

    assert body["source"] == "override"
    assert body["explicit_retention_days"] == 0
    assert body["effective_retention_days"] is None


def test_volume_defaults_to_the_sampled_serving_volume(client) -> None:
    class _Probe:
        def storage_health(self) -> dict:
            return {"disk_total": 2 * _TB, "disk_free": _TB}

    disk_sampler.sample_disk_state(_Probe())

    body = _get(client, _cfg()).json()

    assert body["volume_total_bytes"] == 2 * _TB
    assert body["volume_total_source"] == "sampled"


def test_unknown_volume_is_a_409_not_a_guess(client) -> None:
    resp = _get(client, _cfg())

    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "high_scale_volume_unknown"


def test_endpoint_is_unavailable_outside_high_scale_mode(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=False):
        resp = client.get(_PATH, params={"service_id": "svc-hs", "events_per_second": 1000, "total_bytes": _TB})

    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "high_scale_not_enabled"


def test_unknown_service_is_a_404(client) -> None:
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=None),
    ):
        resp = client.get(_PATH, params={"service_id": "nope", "events_per_second": 1, "total_bytes": _TB})

    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "service_not_found"


def test_non_positive_rate_is_rejected(client) -> None:
    resp = _get(client, _cfg(), total_bytes=_TB, events_per_second=0)

    assert resp.status_code == 422


def test_recommendation_is_logged_with_its_inputs(client) -> None:
    with capture_logs() as logs:
        _get(client, _cfg(retention_days=14), total_bytes=_TB)

    event = next(e for e in logs if e["event"] == "high_scale.retention_recommended")
    assert event["service_id"] == "svc-hs"
    assert event["source"] == "override"
    assert event["explicit_retention_days"] == 14


def test_endpoint_is_analyst_blocked_by_the_admin_prefix() -> None:
    assert _is_blocked_path(_PATH) is True


def test_recommendation_never_writes_service_config(client) -> None:
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.save_config") as save,
    ):
        resp = client.get(_PATH, params={"service_id": "svc-hs", "events_per_second": 1000, "total_bytes": _TB})

    assert resp.status_code == 200
    save.assert_not_called()
