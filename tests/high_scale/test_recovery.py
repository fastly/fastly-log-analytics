import hashlib
import json
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.publication import ClickHousePublication, InMemoryBatchManifestStore, InsertReceipt
from backend.high_scale.recovery import (
    rebuild_cmcd_projections_from_fos,
    rebuild_origin_projections_from_fos,
    rebuild_performance_projections_from_fos,
    rebuild_rum_facts_from_fos,
    rebuild_serving_state_from_fos,
)
from backend.high_scale.replay import replay_performance_projections

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)


class ReplayClient:
    def __init__(self, *, fail_on: set[str] | None = None) -> None:
        self._fail_on = fail_on or set()
        self.insert_calls: list[str] = []

    def insert(self, batch):
        if any(needle in batch.batch_id for needle in self._fail_on):
            raise RuntimeError(f"simulated insert failure for {batch.batch_id}")
        self.insert_calls.append(batch.batch_id)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


def _parquet(rows: tuple[dict, ...]) -> bytes:
    output = BytesIO()
    pq.write_table(pa.Table.from_pylist(list(rows)), output)
    return output.getvalue()


def _checksum(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _event_digest(rows: tuple[dict, ...]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return f"sha256:{digest.hexdigest()}"


def _manifest(manifest_id: str, coverage_start: datetime, coverage_end: datetime, rows: tuple[dict, ...]):
    artifact_bytes = _parquet(rows)
    source = ArchiveSourceObject("svc", "request", f"raw/request/{manifest_id}.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        manifest_id,
        source,
        ArchiveArtifact(
            f"s3://archive/artifacts/{manifest_id}.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(rows),
            len(artifact_bytes),
            _event_digest(rows),
            "request.v1",
            "normalize.v1",
        ),
        coverage_start,
        coverage_end,
        NOW + timedelta(days=1),
        NOW + timedelta(days=2),
        1,
    )
    return manifest, artifact_bytes


class _Catalog:
    def __init__(self, manifests: tuple[ArchiveManifest, ...]) -> None:
        self._manifests = manifests

    def manifests_covering(self, service_id, domain, start, end):
        return tuple(m for m in self._manifests if m.coverage_end > start and m.coverage_start < end)


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
