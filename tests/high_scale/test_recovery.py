from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest
from backend.high_scale.replay import replay_performance_projections

from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.publication import ClickHousePublication, InMemoryBatchManifestStore
from backend.high_scale.recovery import (
    rebuild_cmcd_projections_from_fos,
    rebuild_origin_projections_from_fos,
    rebuild_performance_projections_from_fos,
    rebuild_rum_facts_from_fos,
    rebuild_serving_state_from_fos,
)
from tests.high_scale.conftest import (
    InMemoryManifestCatalog,
    make_test_archive_manifest,
)
from tests.high_scale.conftest import (
    ReplayClickHouseClient as ReplayClient,
)

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
_manifest = partial(
    make_test_archive_manifest,
    retention_deadline=NOW + timedelta(days=1),
    deletion_deadline=NOW + timedelta(days=2),
)
_Catalog = partial(InMemoryManifestCatalog, filter_source=False)


def test_recovery_prioritizes_recent_history_and_reports_full_completion() -> None:
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    warm, warm_bytes = _manifest(
        "warm", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    archive.publish(warm, warm_bytes)
    catalog = _Catalog((recent, warm))
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient())

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.manifests_replayed == 2
    assert report.rows_replayed == 2
    assert report.triage_available is True
    assert report.raw_query_available is True
    assert report.warm_history_available is True
    assert report.full_rebuild_complete is True
    assert report.errors == ()


def test_recovery_reports_partial_availability_when_warm_tier_fails() -> None:
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    warm, warm_bytes = _manifest(
        "warm", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    archive.publish(warm, warm_bytes)
    catalog = _Catalog((recent, warm))
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient(fail_on={"warm"}))

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.triage_available is True
    assert report.raw_query_available is True
    assert report.warm_history_available is False
    assert report.full_rebuild_complete is False
    assert len(report.errors) == 1
    assert "warm" in report.errors[0]


def test_recovery_reports_no_triage_when_recent_tier_fails() -> None:
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    catalog = _Catalog((recent,))
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient(fail_on={"recent"}))

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.triage_available is False
    assert report.raw_query_available is False
    assert report.full_rebuild_complete is False


def test_recovery_reinserts_rows_whose_manifest_was_already_replayed_once() -> None:
    """Reproduces the live-drill finding: a manifest replayed once before
    (e.g. an earlier partial recovery attempt) is durably marked visible in
    the manifest store. If ClickHouse data is lost again after that, a
    second rebuild must not silently treat the stale visibility record as
    proof the rows are still there — it must actually re-insert them."""
    from backend.high_scale.replay import replay_manifest

    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    catalog = _Catalog((recent,))
    manifests = InMemoryBatchManifestStore()
    client = ReplayClient()
    publication = ClickHousePublication(manifests, client)

    # Simulate an earlier, already-completed replay of this exact manifest
    # under the default (non-rebuild-scoped) batch id.
    first = replay_manifest(recent, archive, publication)
    assert first.publication.duplicate is False
    assert client.insert_calls == ["replay:recent"]

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.rows_replayed == 1
    assert report.manifests_replayed == 1
    assert report.full_rebuild_complete is True
    # The real assertion: the rebuild must issue a FRESH insert rather than
    # trusting the stale "already visible" record from the earlier replay —
    # otherwise data lost from ClickHouse after that first replay would
    # never actually come back.
    assert len(client.insert_calls) == 2
    assert client.insert_calls[0] != client.insert_calls[1]


def test_resuming_a_rebuild_with_the_same_rebuild_id_stays_idempotent() -> None:
    """A rebuild retried with its OWN rebuild_id (resuming after a crash
    mid-rebuild, say) must NOT duplicate rows already inserted this
    attempt — only a rebuild_id-less (genuinely new) call should force a
    fresh insert."""
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    catalog = _Catalog((recent,))
    client = ReplayClient()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)

    first = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert len(client.insert_calls) == 1

    second = rebuild_serving_state_from_fos(
        catalog, archive, publication, service_id="svc", domain="request", now=NOW, rebuild_id=first.rebuild_id
    )

    assert len(client.insert_calls) == 1
    assert second.manifests_replayed == 1
    assert second.rebuild_id == first.rebuild_id


def test_recovery_with_no_archived_history_is_a_trivially_complete_rebuild() -> None:
    catalog = _Catalog(())
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient())

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.manifests_replayed == 0
    assert report.full_rebuild_complete is True
    assert report.ingest_accepted is True


def test_origin_projection_rebuild_is_resumable_without_replaying_request_facts() -> None:
    recent, recent_bytes = _manifest(
        "recent-origin",
        NOW - timedelta(hours=1),
        NOW - timedelta(minutes=59),
        (
            {
                "timestamp": (NOW - timedelta(hours=1)).isoformat(),
                "url": "/slow",
                "ottfb": 2500,
                "ost": 503,
            },
        ),
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    catalog = _Catalog((recent,))
    client = ReplayClient()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)

    first = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id="svc",
        now=NOW,
    )
    second = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id="svc",
        now=NOW,
        rebuild_id=first.rebuild_id,
    )

    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 3
    assert client.insert_calls == [
        f"rebuild-origin:{first.rebuild_id}:recent-origin:summary",
        f"rebuild-origin:{first.rebuild_id}:recent-origin:dimensions",
    ]


def _performance_events() -> tuple[dict, ...]:
    return (
        {
            "event_id": "perf-1",
            "timestamp": (NOW - timedelta(hours=1)).isoformat(),
            "url": "/slow",
            "cache": "MISS",
            "elapsed": 250_000,
            "ttfb": 0.1,
            "ottfb": 40_000,
            "ottlb": 90_000,
        },
    )


def test_performance_projection_rebuild_publishes_only_the_new_tables_and_is_resumable() -> None:
    recent, recent_bytes = _manifest(
        "recent-perf", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _performance_events()
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(recent, recent_bytes)
    catalog = _Catalog((recent,))
    client = ReplayClient()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)

    first = rebuild_performance_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    second = rebuild_performance_projections_from_fos(
        catalog, archive, publication, service_id="svc", now=NOW, rebuild_id=first.rebuild_id
    )

    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 2
    assert first.full_rebuild_complete is True
    assert client.insert_calls == [
        f"rebuild-performance:{first.rebuild_id}:recent-perf:summary",
        f"rebuild-performance:{first.rebuild_id}:recent-perf:scatter",
    ]


def test_performance_projection_rebuild_reports_a_failed_manifest_without_stopping() -> None:
    bad, bad_bytes = _manifest("bad-perf", NOW - timedelta(hours=2), NOW - timedelta(hours=1), _performance_events())
    good, good_bytes = _manifest(
        "good-perf", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _performance_events()
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(bad, bad_bytes)
    archive.publish(good, good_bytes)
    client = ReplayClient(fail_on={"bad-perf"})
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)

    report = rebuild_performance_projections_from_fos(
        _Catalog((bad, good)), archive, publication, service_id="svc", now=NOW
    )

    assert report.manifests_replayed == 1
    assert report.full_rebuild_complete is False
    assert len(report.errors) == 1 and "bad-perf" in report.errors[0]


def test_performance_projection_replay_rejects_non_request_manifests() -> None:
    rum = SimpleNamespace(source=SimpleNamespace(domain="rum_vitals"))
    archive = ArchivePublication(InMemoryObjectStore())
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient())

    with pytest.raises(ValueError, match="request manifests"):
        replay_performance_projections(rum, archive, publication, rebuild_id="r")


def test_purge_origin_projections_deletes_only_the_service_window_from_origin_tables() -> None:
    from backend.high_scale.recovery import purge_origin_projections

    class _Recorder:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []

        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            self.calls.append((sql, params or {}))
            return []

    client = _Recorder()

    purge_origin_projections(client, service_id="svc", since=NOW - timedelta(days=30))

    assert [sql.split()[2].strip("`") for sql, _ in client.calls] == [
        "origin_minute_summary",
        "origin_minute_dimensions",
    ]
    for sql, params in client.calls:
        assert sql.startswith("ALTER TABLE")
        assert "DELETE WHERE service_id={service_id:String} AND bucket_start >= {since:DateTime64(3)}" in sql
        assert "mutations_sync=2" in sql
        assert params == {"service_id": "svc", "since": NOW - timedelta(days=30)}


class _RecordingReplayClient(ReplayClient):
    def __init__(self) -> None:
        super().__init__()
        self.batches: list = []

    def insert(self, batch):
        self.batches.append(batch)
        return super().insert(batch)


def _cmcd_events() -> tuple[dict, ...]:
    return (
        {
            "event_id": "evt-1",
            "service_id": "svc",
            "timestamp": (NOW - timedelta(hours=1)).isoformat(),
            "country": "US",
            "asn": 7922,
            "cmcd": {"sid": "s-1", "br": "1200", "bs": None},
        },
        {
            "event_id": "evt-2",
            "service_id": "svc",
            "timestamp": (NOW - timedelta(hours=1)).isoformat(),
            "country": "DE",
            "asn": None,
            "cmcd": {"sid": None, "br": None, "bs": None},
        },
        {
            "event_id": "evt-3",
            "service_id": "svc",
            "timestamp": (NOW - timedelta(hours=1)).isoformat(),
            "country": "FR",
            "asn": 3215,
            "cmcd": None,
        },
    )


def test_cmcd_projection_rebuild_publishes_geography_enriched_rows_and_is_resumable() -> None:
    recent, recent_bytes = _manifest(
        "recent-cmcd", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _cmcd_events()
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(recent, recent_bytes)
    client = _RecordingReplayClient()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)

    first = rebuild_cmcd_projections_from_fos(_Catalog((recent,)), archive, publication, service_id="svc", now=NOW)
    second = rebuild_cmcd_projections_from_fos(
        _Catalog((recent,)), archive, publication, service_id="svc", now=NOW, rebuild_id=first.rebuild_id
    )

    assert first.full_rebuild_complete is True
    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 1
    assert client.insert_calls == [f"rebuild-cmcd:{first.rebuild_id}:recent-cmcd"]
    (batch,) = client.batches
    assert batch.domain == "cmcd"
    (row,) = batch.rows
    assert row["request_event_id"] == "evt-1"
    assert row["country"] == "US" and row["asn"] == 7922
    assert row["fields"] == {"sid": "s-1", "br": "1200"}


def test_cmcd_projection_rebuild_reports_a_failing_manifest_without_stopping() -> None:
    recent, recent_bytes = _manifest(
        "recent-cmcd", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _cmcd_events()
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(recent, recent_bytes)
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient(fail_on={"rebuild-cmcd"}))

    report = rebuild_cmcd_projections_from_fos(_Catalog((recent,)), archive, publication, service_id="svc", now=NOW)

    assert report.full_rebuild_complete is False
    assert report.manifests_replayed == 0
    assert "recent-cmcd" in report.errors[0]


def test_purge_cmcd_projections_deletes_only_the_service_window() -> None:
    from backend.high_scale.recovery import purge_cmcd_projections

    class _Recorder:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []

        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            self.calls.append((sql, params or {}))
            return []

    client = _Recorder()

    purge_cmcd_projections(client, service_id="svc", since=NOW - timedelta(days=30))

    ((sql, params),) = client.calls
    assert sql.startswith("ALTER TABLE `cmcd_projection_facts` DELETE WHERE service_id={service_id:String}")
    assert "event_timestamp >= {since:DateTime64(3)}" in sql
    assert "mutations_sync=2" in sql
    assert params == {"service_id": "svc", "since": NOW - timedelta(days=30)}


@pytest.mark.parametrize(("domain", "table"), [("rum_vitals", "rum_vitals_facts"), ("rum_errors", "rum_error_facts")])
def test_purge_rum_facts_deletes_only_the_service_window_of_one_rum_table(domain: str, table: str) -> None:
    from backend.high_scale.recovery import purge_rum_facts

    class _Recorder:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []

        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            self.calls.append((sql, params or {}))
            return []

    client = _Recorder()

    purge_rum_facts(client, domain=domain, service_id="svc", since=NOW - timedelta(days=30))

    ((sql, params),) = client.calls
    assert sql.startswith(f"ALTER TABLE `{table}` DELETE WHERE service_id={{service_id:String}}")
    assert "event_timestamp >= {since:DateTime64(3)}" in sql and "mutations_sync=2" in sql
    assert params == {"service_id": "svc", "since": NOW - timedelta(days=30)}


def test_purge_rum_facts_rejects_other_domains() -> None:
    from backend.high_scale.recovery import purge_rum_facts

    with pytest.raises(ValueError, match="rum_vitals or rum_errors"):
        purge_rum_facts(object(), domain="request", service_id="svc", since=NOW)  # type: ignore[arg-type]


def test_rum_fact_rebuild_replays_the_rum_domain_over_the_requested_window() -> None:
    recent, recent_bytes = _manifest(
        "rum-1", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    warm, warm_bytes = _manifest(
        "rum-2", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(recent, recent_bytes)
    archive.publish(warm, warm_bytes)
    seen: list[str] = []

    class _Catalog:
        def manifests_covering(self, service_id, domain, start, end):
            seen.append(domain)
            return tuple(m for m in (recent, warm) if m.coverage_end > start and m.coverage_start < end)

    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient())

    report = rebuild_rum_facts_from_fos(
        _Catalog(), archive, publication, service_id="svc", domain="rum_vitals", now=NOW, window=timedelta(days=30)
    )

    assert set(seen) == {"rum_vitals"}
    assert report.full_rebuild_complete is True and report.manifests_replayed == 2

    with pytest.raises(ValueError, match="rum_vitals or rum_errors"):
        rebuild_rum_facts_from_fos(_Catalog(), archive, publication, service_id="svc", domain="request", now=NOW)

    short = rebuild_rum_facts_from_fos(
        _Catalog(), archive, publication, service_id="svc", domain="rum_errors", now=NOW, window=timedelta(hours=6)
    )
    assert short.full_rebuild_complete is True


def test_performance_projection_rebuild_backfills_asn_rows_but_never_republishes_url_rows() -> None:
    events = tuple({**event, "asn": 7922, "url": "/slow"} for event in _performance_events())
    recent, recent_bytes = _manifest("asn-perf", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), events)
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(recent, recent_bytes)
    client = ReplayClient()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)

    report = rebuild_performance_projections_from_fos(
        _Catalog((recent,)), archive, publication, service_id="svc", now=NOW, rebuild_id="r1"
    )

    assert report.full_rebuild_complete is True
    assert client.insert_calls == [
        "rebuild-performance:r1:asn-perf:summary",
        "rebuild-performance:r1:asn-perf:scatter",
        "rebuild-performance:r1:asn-perf:asn",
    ]


# ── Merged from test_replay.py ───────────────────────────────────────

from backend.high_scale.replay import replay_manifest

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.publication import InsertReceipt
from tests.high_scale.conftest import (
    ReplayClickHouseClient as ReplayClient,
)
from tests.high_scale.conftest import (
    event_digest as _event_digest,
)
from tests.high_scale.conftest import (
    make_archive_parquet_bytes as _parquet,
)
from tests.high_scale.conftest import (
    sha256_checksum as _checksum,
)


def test_replay_rebuilds_visibility_from_archive_only() -> None:
    rows = ({"event_id": "event-1", "status": 200}, {"event_id": "event-2", "status": 500})
    artifact_bytes = _parquet(rows)
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        "manifest-1",
        source,
        ArchiveArtifact(
            "s3://archive/artifacts/manifest-1.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(rows),
            2,
            _event_digest(rows),
            "request.v1",
            "normalize.v1",
        ),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        4,
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient())

    result = replay_manifest(manifest, archive, publication)

    assert result.rows_read == 2
    assert result.publication.rows_visible == 2


def test_replay_with_a_custom_batch_id_reinserts_after_a_prior_replay_is_marked_visible() -> None:
    """A recovery rebuild must not silently skip a manifest just because an
    EARLIER replay of it (under the default batch id) is still recorded as
    visible in the manifest store — that bookkeeping can outlive the actual
    ClickHouse data (e.g. after a real ClickHouse volume loss, or in this
    test, an explicit wipe), so a rebuild needs a fresh batch id scope."""
    rows = ({"event_id": "event-1", "status": 200},)
    artifact_bytes = _parquet(rows)
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        "manifest-1",
        source,
        ArchiveArtifact(
            "s3://archive/artifacts/manifest-1.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(rows),
            1,
            _event_digest(rows),
            "request.v1",
            "normalize.v1",
        ),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        4,
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)

    inserted: list[int] = []

    class CountingClient:
        def insert(self, batch):
            inserted.append(len(batch.rows))
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    manifests = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifests, CountingClient())

    first = replay_manifest(manifest, archive, publication)
    assert first.publication.duplicate is False
    assert inserted == [1]

    # Default batch id: already visible per the manifest store, so a second
    # replay under the SAME id must skip re-inserting (the existing,
    # intentional idempotency contract for a genuine retry).
    second = replay_manifest(manifest, archive, publication)
    assert second.publication.duplicate is True
    assert inserted == [1]

    # A rebuild-scoped batch id has never been seen before, so it correctly
    # re-inserts even though the manifest itself was already replayed.
    third = replay_manifest(manifest, archive, publication, batch_id=f"rebuild:xyz:{manifest.manifest_id}")
    assert third.publication.duplicate is False
    assert inserted == [1, 1]


def test_replay_manifest_filters_mixed_rum_rows_by_serving_domain() -> None:
    """Raw RUM objects share raw/rum/ across vitals and errors, so a rum_errors
    archive manifest may contain only vitals rows (empty error_message) or a mix.
    replay_manifest must filter with row_matches_serving_domain and skip publishing
    empty batches."""
    vitals_only_rows = (
        {
            "event_id": "vital-1",
            "metric_name": "LCP",
            "metric_value": 1200.0,
            "error_message": "",
        },
    )
    artifact_bytes = _parquet(vitals_only_rows)
    source = ArchiveSourceObject("svc", "rum_errors", "raw/rum/a.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        "manifest-rum-err-empty",
        source,
        ArchiveArtifact(
            "s3://archive/artifacts/manifest-rum-err-empty.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(vitals_only_rows),
            1,
            _event_digest(vitals_only_rows),
            "rum_errors.v1",
            "normalize.v1",
        ),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        4,
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)

    inserted: list[int] = []

    class StrictRumErrorClient:
        def insert(self, batch):
            for row in batch.rows:
                if not row.get("error_message"):
                    raise ValueError("rum_errors batch contained non-error row")
            inserted.append(len(batch.rows))
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    publication = ClickHousePublication(InMemoryBatchManifestStore(), StrictRumErrorClient())
    result = replay_manifest(manifest, archive, publication)

    assert result.rows_read == 0
    assert result.publication.rows_visible == 0
    assert inserted == []


# ── Merged from test_recovery_capacity_wiring.py ─────────────────────

import json
from unittest.mock import MagicMock

from structlog.testing import capture_logs

from backend.high_scale.recovery import RecoveryCapacityMeasurement


def _rum_repair_archive(tmp_path, *, faro_body=None):
    import hashlib

    from backend.high_scale.archive_models import ArchiveSourceObject
    from backend.high_scale.archive_publication import write_batch_archive_checkpoint
    from backend.high_scale.decoder import decode_source_object

    source_payload = (
        "\n"
        + json.dumps(
            {
                "timestamp": "2026-09-14T11:00:00Z",
                "rum_body": json.dumps(
                    faro_body or {"measurements": [{"type": "web-vitals", "values": {"lcp": 12, "cls": 0.1}}]}
                ),
            }
        )
        + "\n"
        + json.dumps(
            {
                "timestamp": "2026-09-14T11:00:01Z",
                "rum_body": json.dumps({"exceptions": [{"value": "boom"}]}),
            }
        )
    ).encode()
    source = ArchiveSourceObject(
        "svc", "rum_errors", "raw/rum/repair.gz", hashlib.sha256(source_payload).hexdigest(), len(source_payload), "v1"
    )
    decoded = decode_source_object(source, source_payload, transform_version="normalize.v1")
    rows = [dict(event, _record_kind="event") for event in decoded.events]
    manifest = write_batch_archive_checkpoint(
        tmp_path,
        service_id="svc",
        domain="rum_errors",
        source_batches=[(source, rows)],
        archive_epoch=9,  # Deliberately NOT the owner epoch.
        schema_version="rum.v1",
        transform_version="normalize.v1",
        coverage_start=NOW,
        coverage_end=NOW,
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(manifest, Path(manifest.artifact.uri.removeprefix("file://")).read_bytes())
    return manifest, archive


def test_scoped_rum_repair_uses_actual_standard_faro_event_projections(tmp_path) -> None:
    from backend.core.rum_ingest import extract_metrics_from_faro_payload
    from backend.high_scale.rum_recovery import plan_rum_recovery

    body = {
        "measurements": [{"type": "web-vitals", "values": {"lcp": 12, "cls": 0.1, "delta": 9}}],
        "events": [
            {"name": "click"},
            {"name": "faro.performance.navigation", "attributes": {"domContentLoaded": 40, "loadEvent": 50}},
        ],
    }
    manifest, archive = _rum_repair_archive(tmp_path, faro_body=body)
    plan = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    vitals = [row for batch in plan.batches if batch.domain == "rum_vitals" for row in batch.rows]
    assert [(row["metric_name"], row["metric_value"]) for row in vitals] == [
        (metric["metric_name"], metric["metric_value"]) for metric in extract_metrics_from_faro_payload(body, {})
    ]
    # Two measurement fields, one standard 1.0 custom event, two navigation
    # attributes. Input entries are NOT one-to-one with projected vital rows.
    assert plan.candidate_counts == {"rum_vitals": 5, "rum_errors": 1}
    assert next(row for row in vitals if row["metric_name"] == "event_click")["metric_value"] == 1.0
    assert all(row["metric_name"] not in {"exception", "delta"} for row in vitals)


def test_scoped_rum_repair_checks_other_batches_and_is_idempotent(tmp_path) -> None:
    from backend.high_scale.rum_recovery import plan_rum_recovery

    manifest, archive = _rum_repair_archive(tmp_path)
    empty = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    assert empty.candidate_counts == {"rum_vitals": 2, "rum_errors": 1}
    assert all(batch.generation == "2" for batch in empty.batches)
    assert {row["line_ordinal"] for batch in empty.batches for row in batch.rows} == {1, 2}

    # Fact rows are compared across ALL batch IDs, including an earlier regroup.
    persisted = [dict(row, repair_visible=True) for row in empty.expected_facts]
    for row in persisted:
        row["batch_id"] = "00000000-0000-0000-0000-000000000099"
    errors_only = [row for row in persisted if "error_message" in row]
    plan = plan_rum_recovery(
        manifest,
        archive,
        owner_epoch=2,
        read_facts=lambda domain, _: errors_only if domain == "rum_errors" else (),
        now=NOW,
    )
    assert plan.missing_counts == {"rum_vitals": 2, "rum_errors": 0}
    assert plan.exact_identity_counts == {"rum_vitals": 0, "rum_errors": 1}
    assert len(plan.batches) == 1
    assert plan.plan_digest == empty.plan_digest  # Receipt changes cannot broaden the approved cohort.
    assert plan.batches[0].batch_id == empty.batches[0].batch_id
    complete = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: persisted, now=NOW)
    assert complete.batches == ()
    assert complete.missing_counts == {"rum_vitals": 0, "rum_errors": 0}

    # A regroup/retransform cannot authorize a duplicate merely by changing IDs.
    for index, fact in enumerate(persisted, start=1):
        fact["event_id"] = f"00000000-0000-0000-0000-{index:012x}"
        fact["transform_version"] = "older-transform"
    equivalent = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: persisted, now=NOW)
    assert equivalent.batches == ()
    assert equivalent.equivalent_identity_counts == {"rum_vitals": 2, "rum_errors": 1}


@pytest.mark.parametrize("visible,copies", [(False, 1), (True, 2)])
def test_scoped_rum_repair_refuses_hidden_or_duplicate_existing_identity(tmp_path, visible, copies) -> None:
    from backend.high_scale.rum_recovery import plan_rum_recovery

    manifest, archive = _rum_repair_archive(tmp_path)
    plan = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    fact = dict(plan.expected_facts[0], repair_visible=visible)
    with pytest.raises(ValueError, match="ambiguous or unpublished"):
        plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: [fact] * copies, now=NOW)


def test_scoped_rum_repair_refuses_unretained_event_times(tmp_path) -> None:
    from backend.high_scale.rum_recovery import plan_rum_recovery

    manifest, archive = _rum_repair_archive(tmp_path)
    with pytest.raises(ValueError, match="outside retained"):
        plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW + timedelta(days=31))


@pytest.mark.parametrize("domain", ["rum_vitals", "rum_errors"])
def test_scoped_rum_content_normalizes_float64_json_integer_representation(domain) -> None:
    from backend.high_scale.rum_recovery import _content

    expected = {"event_timestamp": NOW, "ttfb": 25.0, "metric_value": 12.0}
    # ClickHouse JSON may encode exact integral Float64 values without ".0".
    # This is the same persisted numeric value, not a changed payload.
    physical = {"event_timestamp": NOW, "ttfb": 25, "metric_value": 12}
    assert _content(expected, domain) == _content(physical, domain)
    assert _content(expected, domain) != _content(dict(physical, ttfb=26), domain)


def test_scoped_rum_apply_requires_reviewed_digest_and_verifies_retry(tmp_path) -> None:
    from backend.high_scale.rum_recovery import execute_rum_recovery, plan_rum_recovery

    manifest, archive = _rum_repair_archive(tmp_path)
    persisted = []

    def make_plan():
        return plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: persisted, now=NOW)

    initial = make_plan()
    publication = MagicMock()
    assert execute_rum_recovery(make_plan, publication, apply=False)["applied"] is False
    publication.publish.assert_not_called()
    with pytest.raises(ValueError, match="reviewed plan"):
        execute_rum_recovery(make_plan, publication, apply=True, approve_digest="wrong")
    publication.publish.assert_not_called()

    def publish(batch):
        from backend.high_scale.publication import _event_uuid

        ids = {_event_uuid(row["event_id"], field="event_id") for row in batch.rows}
        persisted.extend(dict(row, repair_visible=True) for row in initial.expected_facts if row["event_id"] in ids)

    publication.publish.side_effect = publish
    result = execute_rum_recovery(make_plan, publication, apply=True, approve_digest=initial.plan_digest)
    assert result["remaining_counts"] == {"rum_vitals": 0, "rum_errors": 0}
    assert publication.publish.call_count == 2
    execute_rum_recovery(make_plan, publication, apply=True, approve_digest=initial.plan_digest)
    assert publication.publish.call_count == 2


def test_scoped_rum_cli_defaults_to_dryrun_and_redacts_dependency_errors(monkeypatch, capsys) -> None:
    from scripts import recover_high_scale_rum as cli

    run = MagicMock(return_value={"applied": False, "candidate_counts": {"rum_vitals": 2, "rum_errors": 1}})
    monkeypatch.setattr(cli, "recover", run)
    assert cli.main(["--manifest-prefix", "a" * 8]) == 0
    run.assert_called_once_with("a" * 8, apply=False, approve_digest=None)
    assert json.loads(capsys.readouterr().out)["applied"] is False
    run.side_effect = ValueError("private dependency response must not escape")
    assert cli.main(["--manifest-prefix", "a" * 8]) == 1
    assert json.loads(capsys.readouterr().out) == {"status": "blocked", "error_type": "ValueError"}


def test_scoped_rum_cli_uses_canonical_source_owner_epoch_column(tmp_path, monkeypatch) -> None:
    from dataclasses import make_dataclass
    from types import SimpleNamespace

    import psycopg

    from backend import config
    from backend.core import clickhouse_client
    from backend.high_scale import postgres_control, publication, registry_bootstrap, rum_recovery
    from scripts import recover_high_scale_rum as cli

    manifest, archive = _rum_repair_archive(tmp_path)
    connection = MagicMock()
    connection.__enter__.return_value = connection

    def execute(query, params=None):
        cursor = MagicMock()
        if "FROM high_scale_archive_manifests" in query:
            cursor.fetchall.return_value = [{}]
        elif "FROM high_scale_ownership" in query:
            cursor.fetchone.return_value = {"owner_epoch": 2, "current_owner": "high_scale"}
        elif "FROM high_scale_source_objects" in query:
            # These are the canonical high_scale_source_objects columns.
            # There is no archive_owner_epoch column or migration for one.
            assert "archive_owner_epoch" not in query
            assert "archive_manifest_id,owner_epoch" in query
            cursor.fetchall.return_value = [
                {
                    "object_key": source.key,
                    "status": "source_deleted",
                    "archive_manifest_id": manifest.manifest_id,
                    "owner_epoch": 2,
                }
                for source in manifest.sources
            ]
        return cursor

    connection.execute.side_effect = execute
    monkeypatch.setenv("METADATA_DSN", "postgresql://unused")
    monkeypatch.setattr(psycopg, "connect", MagicMock(return_value=connection))
    monkeypatch.setattr(
        postgres_control,
        "_archive_from_row",
        lambda _: SimpleNamespace(manifest=manifest, state="source_deleted", owner_epoch=2),
    )
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", MagicMock())
    monkeypatch.setattr(registry_bootstrap, "_default_archive_factory", lambda _: archive)
    settings_type = make_dataclass("RecoveryTestSettings", [("query_timeout_s", int)])
    monkeypatch.setattr(config, "load_clickhouse_config", lambda: settings_type(5))
    client = MagicMock()
    initial_plan = rum_recovery.plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    error_fact = next(fact for fact in initial_plan.expected_facts if fact["_repair_domain"] == "rum_errors")

    def read_clickhouse(query, params=None, *, settings=None):
        if "high_scale_batch_publications" in query:
            # Receipt batch IDs contain UUID-formatted text but their canonical
            # storage/lookup type is String, unlike the physical fact UUID.
            assert "batch_id={batch_id:String}" in query
            assert "batch_id:UUID" not in query
            return [{"publication_state": "visible", "expected_rows": 1, "visible_rows": 1}]
        if "rum_error_facts" in query:
            return [dict(error_fact)]
        return []

    client.execute.side_effect = read_clickhouse
    monkeypatch.setattr(clickhouse_client, "ClickHouseClient", MagicMock(return_value=client))
    monkeypatch.setattr(publication, "PostgresBatchManifestStore", MagicMock())
    serving = MagicMock()
    monkeypatch.setattr(publication, "ClickHousePublication", MagicMock(return_value=serving))
    original_plan = rum_recovery.plan_rum_recovery
    monkeypatch.setattr(rum_recovery, "plan_rum_recovery", lambda *a, **kw: original_plan(*a, **kw, now=NOW))

    report = cli.recover("a" * 8, apply=False, approve_digest=None)
    assert report["read_only"] is True
    assert report["candidate_counts"] == {"rum_vitals": 2, "rum_errors": 1}
    assert report["missing_counts"] == {"rum_vitals": 2, "rum_errors": 0}
    serving.publish.assert_not_called()


def test_scoped_rum_apply_cannot_report_success_on_missing_insert(tmp_path) -> None:
    from backend.high_scale.rum_recovery import execute_rum_recovery, plan_rum_recovery

    manifest, archive = _rum_repair_archive(tmp_path)

    def make_plan():
        return plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)

    with pytest.raises(ValueError, match="post-apply identity verification"):
        execute_rum_recovery(make_plan, MagicMock(), apply=True, approve_digest=make_plan().plan_digest)


class _PartitionedRumClient:
    def __init__(self, rows=()):
        self.rows = [dict(row) for row in rows]
        self.partitions = {"20260901": 71_167, "20260905": 60_000, "20260914": 70_000}
        self.queries = []
        self.inserts = []
        self.receipt = None
        self.truncate_insert = False
        self.change_inventory = False
        self.inventory_reads = 0

    def execute(self, query, params=None, *, settings=None):
        self.queries.append((query, params, settings))
        if "system.parts" in query:
            self.inventory_reads += 1
            rows = [{"partition_id": key, "partition_rows": count} for key, count in self.partitions.items()]
            if self.change_inventory and self.inventory_reads % 2 == 0:
                rows.append({"partition_id": "20260930", "partition_rows": 1})
            return rows
        if "high_scale_batch_publications" in query:
            return [] if self.receipt is None else [dict(self.receipt)]
        assert "_partition_id IN" in query, "global count must use complete bounded partition groups"
        assert "event_timestamp >" not in query and "source_object_key IN" not in query
        partitions = {value for key, value in params.items() if key.startswith("part")}
        return [
            dict(row)
            for row in self.rows
            if row["event_timestamp"].strftime("%Y%m%d") in partitions
            and str(row["batch_id"]) == params["batch_id"]
            and row["service_id"] == params["service_id"]
        ]

    def insert_rows(self, table, columns, rows):
        self.inserts.append((table, len(rows)))
        if table == "high_scale_batch_publications":
            self.receipt = dict(zip(columns, rows[0], strict=True))
        else:
            selected = rows[:1] if self.truncate_insert else rows
            self.rows.extend(dict(zip(columns, row, strict=True)) for row in selected)

    def delete_batch_rows(self, *_):
        raise AssertionError("repair must never silently delete a partial/conflicting batch")


def _partitioned_rum_adapter(tmp_path, *, physical=()):
    from backend.high_scale.publication import _DOMAIN_COLUMNS, _batch_uuid, _rows_for_domain
    from backend.high_scale.rum_recovery import RumRecoveryBatchAdapter, plan_rum_recovery

    manifest, archive = _rum_repair_archive(tmp_path)
    plan = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    batch = next(batch for batch in plan.batches if batch.domain == "rum_vitals")
    rows = [
        dict(zip(_DOMAIN_COLUMNS[batch.domain], row, strict=True))
        for row in _rows_for_domain(batch, _batch_uuid(batch))
    ]
    client = _PartitionedRumClient(physical)
    return RumRecoveryBatchAdapter(client), client, batch, rows


def test_partitioned_rum_count_preserves_global_empty_insert_and_retry(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    assert adapter.inspect_batch(batch) == (0, None)
    assert adapter.insert(batch).rows_inserted == 2
    assert adapter.insert(batch).rows_inserted == 2
    assert len(client.rows) == 2
    assert client.inserts.count(("rum_vitals_facts", 2)) == 1
    selected_partitions = set()
    for query, params, settings in client.queries:
        if "system.parts" not in query and "high_scale_batch_publications" not in query:
            selected_partitions.update(value for key, value in params.items() if key.startswith("part"))
        if settings is not None:
            assert settings["max_rows_to_read"] == "100000"
            assert settings["read_overflow_mode"] == "throw"
    assert selected_partitions == set(client.partitions)  # Includes dates OUTSIDE candidate time bounds.


@pytest.mark.parametrize("kind", ["partial", "duplicate", "conflict", "outside_time", "outside_source", "unknown_id"])
def test_partitioned_rum_count_refuses_bad_existing_rows_without_writes(tmp_path, kind) -> None:
    from backend.high_scale.publication import PartialInsert

    adapter, client, batch, rows = _partitioned_rum_adapter(tmp_path)
    if kind == "partial":
        rows = rows[:1]
    elif kind == "duplicate":
        rows = [rows[0], rows[0]]
    elif kind == "conflict":
        rows[0]["metric_value"] = 999
    elif kind == "outside_time":
        rows[0]["event_timestamp"] = NOW - timedelta(days=13)  # Different, still globally enumerated partition.
    elif kind == "outside_source":
        rows[0]["source_object_key"] = "raw/rum/unexpected.gz"
    else:
        rows[0]["event_id"] = "00000000-0000-0000-0000-000000000099"
        rows[0]["event_timestamp"] = NOW - timedelta(days=13)
    client.rows = rows
    with pytest.raises((ValueError, PartialInsert)):
        adapter.insert(batch)
    assert client.inserts == []


def test_partitioned_rum_count_does_not_delete_or_publish_partial_insert(tmp_path) -> None:
    from backend.high_scale.publication import PartialInsert

    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    client.truncate_insert = True
    with pytest.raises(PartialInsert):
        adapter.insert(batch)
    assert client.receipt["publication_state"] == "pending"
    assert len(client.rows) == 1
    before = list(client.inserts)
    with pytest.raises(PartialInsert):
        adapter.insert(batch)
    assert client.inserts == before


def test_partitioned_rum_count_checks_physical_rows_even_with_visible_receipt(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    adapter.insert(batch)
    client.rows.append(dict(client.rows[0]))
    before = list(client.inserts)
    with pytest.raises(ValueError, match="duplicate"):
        adapter.insert(batch)
    assert client.inserts == before


def test_partitioned_rum_count_recovers_full_valid_lost_ack_without_reinserting(tmp_path) -> None:
    adapter, client, batch, rows = _partitioned_rum_adapter(tmp_path)
    client.rows = rows
    assert adapter.inspect_batch(batch) == (2, None)
    assert adapter.insert(batch).rows_inserted == 2
    assert not any(table == "rum_vitals_facts" for table, _ in client.inserts)
    assert client.receipt["publication_state"] == "visible"


def test_partitioned_rum_count_never_swallows_failed_partition_read(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    original = client.execute

    def fail(query, *args, **kwargs):
        if "_partition_id IN" in query:
            raise RuntimeError("bounded scan failed")
        return original(query, *args, **kwargs)

    client.execute = fail
    with pytest.raises(RuntimeError, match="bounded scan failed"):
        adapter.inspect_batch(batch)
    assert client.inserts == []


@pytest.mark.parametrize(
    "unsafe", ["changing_inventory", "oversized_partition", "too_many_groups", "too_many_partitions"]
)
def test_partitioned_rum_count_refuses_unprovable_inventory(tmp_path, unsafe) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    if unsafe == "changing_inventory":
        client.change_inventory = True
    elif unsafe == "oversized_partition":
        client.partitions = {"20260914": 100_001}
    elif unsafe == "too_many_groups":
        client.partitions = {f"partition-{i}": 90_000 for i in range(7)}
    else:
        client.partitions = {f"partition-{i}": 1 for i in range(33)}
    with pytest.raises(ValueError):
        adapter.inspect_batch(batch)
    assert client.inserts == []


def _measurement(**overrides) -> RecoveryCapacityMeasurement:
    values = {
        "events_per_second": 2_000_000,
        "recovery_window_seconds": 15 * 60,
        "archive_read_events_per_second": 300_000_000,
        "decode_events_per_second": 300_000_000,
        "aggregate_events_per_second": 300_000_000,
        "clickhouse_insert_events_per_second": 750_000_000,
        "replication_factor": 3,
        "live_ingest_reservation": 0.2,
    }
    values.update(overrides)
    return RecoveryCapacityMeasurement(**values)


def _recovery_env(*, fail_on: set[str] | None = None):
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    warm, warm_bytes = _manifest(
        "warm", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(recent, recent_bytes)
    archive.publish(warm, warm_bytes)
    client = ReplayClient(fail_on=fail_on)
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)
    return _Catalog((recent, warm)), archive, publication, client


def _rebuild(env, **kwargs):
    catalog, archive, publication, _ = env
    return rebuild_serving_state_from_fos(
        catalog, archive, publication, service_id="svc", domain="request", now=NOW, **kwargs
    )


def test_feasible_measurement_with_a_complete_rebuild_makes_the_claim_valid() -> None:
    with capture_logs() as logs:
        report = _rebuild(_recovery_env(), capacity=_measurement())

    assert report.full_rebuild_complete is True
    assert report.capacity is not None and report.capacity.feasible is True
    assert report.recovery_claim_valid is True
    gate = next(e for e in logs if e["event"] == "high_scale.recovery_capacity_gate")
    assert gate["feasible"] is True and gate["log_level"] == "info"


def test_infeasible_measurement_flags_the_claim_but_never_blocks_recovery() -> None:
    env = _recovery_env()

    with capture_logs() as logs:
        report = _rebuild(env, capacity=_measurement(clickhouse_insert_events_per_second=400_000_000))

    assert report.full_rebuild_complete is True
    assert report.rows_replayed == 2
    assert len(env[3].insert_calls) == 2
    assert report.capacity is not None and report.capacity.limiting_stage == "clickhouse_insert"
    assert report.recovery_claim_valid is False
    gate = next(e for e in logs if e["event"] == "high_scale.recovery_capacity_gate")
    assert gate["log_level"] == "warning" and gate["limiting_stage"] == "clickhouse_insert"


def test_feasible_capacity_does_not_validate_an_incomplete_rebuild() -> None:
    report = _rebuild(_recovery_env(fail_on={"warm"}), capacity=_measurement())

    assert report.capacity is not None and report.capacity.feasible is True
    assert report.full_rebuild_complete is False
    assert report.recovery_claim_valid is False


def test_without_a_measurement_the_report_carries_no_claim() -> None:
    with capture_logs() as logs:
        report = _rebuild(_recovery_env())

    assert report.capacity is None
    assert report.recovery_claim_valid is None
    assert not [e for e in logs if e["event"] == "high_scale.recovery_capacity_gate"]


def test_invalid_measurement_fails_before_any_replay_work() -> None:
    env = _recovery_env()

    with pytest.raises(ValueError, match="event rate and recovery window must be positive"):
        _rebuild(env, capacity=_measurement(events_per_second=0))

    assert env[3].insert_calls == []


def test_origin_projection_rebuild_evaluates_the_same_gate() -> None:
    catalog, archive, publication, _ = _recovery_env()

    report = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id="svc",
        now=NOW,
        window=timedelta(hours=2),
        capacity=_measurement(decode_events_per_second=1_000),
    )

    assert report.capacity is not None and report.capacity.limiting_stage == "decode"
    assert report.recovery_claim_valid is False


# ── CLI: scripts/backfill_high_scale.py --capacity-json ──────────────


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch):
    from scripts import backfill_high_scale as script

    service = SimpleNamespace(manifest_catalog=object(), archive=object(), client=MagicMock())
    registry = MagicMock()
    registry.resolve.return_value = service
    monkeypatch.setattr(script.app_config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(script, "ClickHouseClient", lambda settings: MagicMock())
    monkeypatch.setattr(script, "get_high_scale_service_registry", lambda: registry)
    monkeypatch.setattr(script, "register_high_scale_services_from_environment", lambda *a, **k: None)
    monkeypatch.setattr(script, "PostgresControlPlane", MagicMock)
    monkeypatch.setattr(script, "PostgresBatchManifestStore", lambda control: MagicMock())
    monkeypatch.setattr(script, "AggregateBatchAdapter", lambda client: MagicMock())
    monkeypatch.setattr(script, "ClickHousePublication", lambda manifests, adapter: MagicMock())
    return script


def _run_cli(cli, monkeypatch, tmp_path, measurement: dict | None, *, complete: bool = True):
    seen: dict = {}

    def fake_rebuild(*args, **kwargs):
        seen.update(kwargs)
        capacity = kwargs.get("capacity")
        from backend.high_scale.recovery import RecoveryReport, evaluate_recovery_capacity

        report = evaluate_recovery_capacity(capacity) if capacity else None
        return RecoveryReport(
            True, complete, True, complete, complete, 1, 1, (), "rb-1",
            capacity=report,
            recovery_claim_valid=None if report is None else report.feasible and complete,
        )  # fmt: skip

    monkeypatch.setattr(cli, "rebuild_origin_projections_from_fos", fake_rebuild)
    argv = ["backfill", "--service-id", "svc"]
    if measurement is not None:
        path = tmp_path / "capacity.json"
        path.write_text(json.dumps(measurement))
        argv += ["--capacity-json", str(path)]
    monkeypatch.setattr("sys.argv", argv)
    return cli.main(), seen


_GOOD = {
    "events_per_second": 2_000_000,
    "recovery_window_seconds": 900,
    "archive_read_events_per_second": 300_000_000,
    "decode_events_per_second": 300_000_000,
    "aggregate_events_per_second": 300_000_000,
    "clickhouse_insert_events_per_second": 750_000_000,
    "replication_factor": 3,
    "live_ingest_reservation": 0.2,
}


def test_cli_passes_the_measurement_through_and_exits_zero_when_valid(cli, monkeypatch, tmp_path) -> None:
    code, seen = _run_cli(cli, monkeypatch, tmp_path, _GOOD)

    assert code == 0
    assert seen["capacity"] == RecoveryCapacityMeasurement(**_GOOD)


def test_cli_exits_three_when_the_rebuild_completes_but_the_claim_is_invalid(cli, monkeypatch, tmp_path) -> None:
    code, _ = _run_cli(cli, monkeypatch, tmp_path, {**_GOOD, "decode_events_per_second": 1_000})

    assert code == 3


def test_cli_without_a_measurement_keeps_the_previous_exit_codes(cli, monkeypatch, tmp_path) -> None:
    assert _run_cli(cli, monkeypatch, tmp_path, None)[0] == 0
    assert _run_cli(cli, monkeypatch, tmp_path, None, complete=False)[0] == 1


def test_cli_rejects_a_malformed_measurement_file(cli, monkeypatch, tmp_path) -> None:
    with pytest.raises(SystemExit) as excinfo:
        _run_cli(cli, monkeypatch, tmp_path, {**_GOOD, "events_per_second": 0})

    assert excinfo.value.code == 2


def test_cli_replace_existing_purges_origin_rows_before_the_rebuild(cli, monkeypatch, tmp_path) -> None:
    order: list[str] = []
    monkeypatch.setattr(
        cli, "purge_origin_projections", lambda client, **kwargs: order.append(f"purge:{kwargs['service_id']}")
    )

    def fake_rebuild(*args, **kwargs):
        order.append("rebuild")
        from backend.high_scale.recovery import RecoveryReport

        return RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1")

    monkeypatch.setattr(cli, "rebuild_origin_projections_from_fos", fake_rebuild)
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--replace-existing"])

    assert cli.main() == 0
    assert order == ["purge:svc", "rebuild"]


def test_cli_without_replace_existing_never_purges(cli, monkeypatch, tmp_path) -> None:
    purged: list[str] = []
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kwargs: purged.append("purge"))

    _run_cli(cli, monkeypatch, tmp_path, None)

    assert purged == []


def test_cli_rejects_replace_existing_with_a_resumed_rebuild_id(cli, monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--replace-existing", "--rebuild-id", "rb-1"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    assert excinfo.value.code == 2


def test_cli_projection_cmcd_rebuilds_through_the_facts_adapter_and_purges_cmcd_rows(cli, monkeypatch) -> None:
    calls: list[str] = []
    adapters: list[str] = []
    monkeypatch.setattr(cli, "ClickHouseBatchAdapter", lambda client: adapters.append("facts") or MagicMock())
    monkeypatch.setattr(cli, "AggregateBatchAdapter", lambda client: adapters.append("aggregate") or MagicMock())
    monkeypatch.setattr(cli, "purge_cmcd_projections", lambda client, **kwargs: calls.append("purge-cmcd"))
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kwargs: calls.append("purge-origin"))

    def fake_cmcd_rebuild(*args, **kwargs):
        calls.append("rebuild-cmcd")
        from backend.high_scale.recovery import RecoveryReport

        return RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1")

    monkeypatch.setattr(cli, "rebuild_cmcd_projections_from_fos", fake_cmcd_rebuild)
    monkeypatch.setattr(
        cli,
        "rebuild_origin_projections_from_fos",
        lambda *a, **k: pytest.fail("origin rebuild must not run for --projection cmcd"),
    )
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--projection", "cmcd", "--replace-existing"])

    assert cli.main() == 0
    assert calls == ["purge-cmcd", "rebuild-cmcd"]
    assert adapters == ["facts"]


def test_cli_projection_rum_replays_the_rum_domain_through_the_facts_adapter(cli, monkeypatch) -> None:
    seen: dict = {}
    adapters: list[str] = []
    monkeypatch.setattr(cli, "ClickHouseBatchAdapter", lambda client: adapters.append("facts") or MagicMock())
    monkeypatch.setattr(cli, "purge_rum_facts", lambda client, **kwargs: seen.setdefault("purge", kwargs))

    def fake_rum_rebuild(*args, **kwargs):
        seen["rebuild"] = kwargs
        from backend.high_scale.recovery import RecoveryReport

        return RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1")

    monkeypatch.setattr(cli, "rebuild_rum_facts_from_fos", fake_rum_rebuild)
    monkeypatch.setattr(
        "sys.argv", ["backfill", "--service-id", "svc", "--projection", "rum_errors", "--replace-existing"]
    )

    assert cli.main() == 0
    assert adapters == ["facts"]
    assert seen["purge"]["domain"] == "rum_errors" and seen["purge"]["service_id"] == "svc"
    assert seen["rebuild"]["domain"] == "rum_errors" and seen["rebuild"]["service_id"] == "svc"
