import gzip
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core.high_scale_contracts import ArchiveState
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.deletion import DeletionController
from backend.high_scale.ingest_controller import HighScaleIngestController
from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.publication import (
    ClickHousePublication,
    InMemoryBatchManifestStore,
    InsertReceipt,
)
from backend.high_scale.replay import replay_manifest


class _ClickHouse:
    def __init__(self) -> None:
        self.batches = []

    def insert(self, batch):
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


class _PostgresControl:
    def __init__(self) -> None:
        self.owner_record = type("Owner", (), {"current_owner": "high_scale", "owner_epoch": 1})()
        self.calls: list[str] = []
        self.archive_state = type("State", (), {"state": ArchiveState.ARTIFACT_UPLOADING})()

    def owner(self, service_id: str):
        return self.owner_record

    def discover_source(self, service_id: str, domain: str, object_key: str, checksum: str, **kwargs: object):
        self.calls.append("discover")
        return type(
            "Source",
            (),
            {
                "object_id": "object-1",
                "service_id": service_id,
                "domain": domain,
                "object_key": object_key,
                "checksum": checksum,
                "size_bytes": 16,
                "version": "v1",
            },
        )()

    def claim_source(self, service_id: str, object_key: str, worker_id: str, **kwargs: object):
        self.calls.append("claim")
        return type("Claim", (), {"claimed": True, "lease_generation": 1})()

    def record_source_counts(self, service_id: str, object_key: str, **kwargs: object) -> None:
        self.calls.append("counts")

    def register_archive_manifest(self, manifest, *, owner_epoch: int):
        self.calls.append("register_manifest")
        return self.archive_state

    def archive_manifest(self, manifest_id: str):
        self.calls.append("archive_manifest_lookup")
        raise KeyError(manifest_id)

    def transition_archive_manifest(self, manifest_id: str, **kwargs: object):
        self.calls.append("transition_manifest")
        self.archive_state.state = kwargs["next_state"]
        return self.archive_state

    def mark_source_appended(self, service_id: str, object_key: str, **kwargs: object) -> None:
        self.calls.append("appended")

    def mark_source_archived(self, service_id: str, object_key: str, **kwargs: object) -> None:
        self.calls.append("archived")

    def acknowledge_source(self, service_id: str, object_key: str, *, manifest_id: str) -> None:
        self.calls.append("acknowledged")


def test_source_flows_through_fenced_archive_and_visible_serving() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    owner = ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    payload = gzip.compress(b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok"}\nnot-json\n')

    result = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    assert owner.owner_epoch == 1
    assert result.decoded.accepted_rows == 1
    assert result.decoded.quarantined_rows == 1
    assert result.publication.rows_visible == 1
    assert objects.exists(f"archive/manifests/{result.manifest.manifest_id}.commit")
    assert ledger.count() == 1
    assert clickhouse.batches[0].rows[0]["event_id"]
    archived_rows = pq.read_table(
        pa.BufferReader(ArchivePublication(objects).read_artifact(result.manifest))
    ).to_pylist()
    assert len(archived_rows) == 2
    assert any(row.get("_record_kind") == "dead_letter" for row in archived_rows)

    replay = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )
    assert replay.publication.duplicate is True
    assert len(clickhouse.batches) == 1

    rebuilt = _ClickHouse()
    replay_result = replay_manifest(
        result.manifest,
        ArchivePublication(objects),
        ClickHousePublication(InMemoryBatchManifestStore(), rebuilt),
    )
    assert replay_result.rows_read == 1
    assert len(rebuilt.batches) == 1
    assert rebuilt.batches[0].rows[0]["event_id"] == clickhouse.batches[0].rows[0]["event_id"]

    deletion = DeletionController(objects, ArchivePublication(objects), ledger)
    with pytest.raises(ValueError, match="grace period"):
        deletion.delete_source(
            result.manifest,
            current_owner_epoch=owner.owner_epoch,
            now=result.manifest.deletion_authorization_deadline - timedelta(seconds=1),
        )


def test_ingest_writes_dimension_aggregates_alongside_facts() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    aggregate_clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        aggregates=ClickHousePublication(InMemoryBatchManifestStore(), aggregate_clickhouse),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    payload = gzip.compress(
        b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok","country":"US","ottfb":1200,"ost":503,"obytes":64}\n'
    )

    controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    assert len(aggregate_clickhouse.batches) == 4
    batch = aggregate_clickhouse.batches[0]
    assert batch.domain == "request_aggregate"
    values = {(row["dimension"], row["value"]) for row in batch.rows}
    assert ("url", "/ok") in values
    assert ("country", "US") in values
    summary = aggregate_clickhouse.batches[1]
    assert summary.domain == "origin_summary"
    assert summary.rows[0]["latency_p50_us"] == 1200
    dimensions = aggregate_clickhouse.batches[2]
    assert dimensions.domain == "origin_dimensions"
    assert ("url", "/ok") in {(row["dimension"], row["value"]) for row in dimensions.rows}


def test_ingest_tolerates_aggregate_publish_failure_without_failing_the_ingest() -> None:
    """Aggregates are a best-effort triage feature layered on top of the
    durability-critical facts path — a failure writing them must never
    fail (or duplicate-retry) the source object's ingest."""

    class _BoomAggregates:
        def publish(self, batch):
            raise RuntimeError("aggregate table unavailable")

    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        aggregates=_BoomAggregates(),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    payload = gzip.compress(b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok"}\n')

    result = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    assert result.publication.rows_visible == 1


def test_ingest_uses_the_ledgers_domain_not_the_callers_when_they_diverge() -> None:
    """Regression test: rum_vitals and rum_errors both list the FOS prefix
    raw/rum/, so the same not-yet-terminal object can legitimately be
    re-attempted under a sibling domain's page before the first domain
    finishes it. discover_source/ledger.discover keep the first domain's
    attribution rather than erroring (a real checksum/version change is
    still rejected). ingest() must decode/archive/publish using that
    persisted attribution, not whichever domain happened to call it this
    time — using the caller's domain instead made every such retry fail
    downstream with "source identity does not match archive batch",
    observed live during RUM qualification."""
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    # Simulates rum_vitals' page having discovered (and, in a live crash,
    # started but not finished claiming/ingesting) this object first.
    ledger.discover("svc", "rum_vitals", "raw/rum/one.gz", "sha256:source", size_bytes=16, version="v1")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )

    # rum_errors' page retries the same object this tick. Shaped as a real
    # rum_vitals row (metric_value) since it must actually be attributed to
    # and published under rum_vitals — the domain this test pins.
    result = controller.ingest(
        service_id="svc",
        domain="rum_errors",
        object_key="raw/rum/one.gz",
        checksum="sha256:source",
        payload=gzip.compress(b'{"metric_name":"LCP","metric_value":1.25}\n'),
        version="v1",
    )

    assert result.source.domain == "rum_vitals"
    assert result.manifest.source.domain == "rum_vitals"
    assert clickhouse.batches[0].domain == "rum_vitals"


def test_ingest_filters_out_sibling_domain_rows_before_publishing() -> None:
    """A raw raw/rum/ object mixes vitals and error beacon lines from the
    same log period (both come from the same /rum-beacon logging
    condition), but the ledger attributes the WHOLE object to one domain.
    A live incident during RUM qualification: an object claimed by
    rum_vitals also contained an error-beacon line (no metric_value), and
    force-fitting it into a vitals row crashed the whole batch insert with
    "metric_value must be finite" — losing the real vitals row in the same
    batch too. The error-shaped row must be filtered out before the serving
    batch is built, not force-fit or allowed to crash the batch."""
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )
    payload = gzip.compress(
        b'{"event_id":"vital-1","timestamp":"2026-09-14T20:00:00Z","metric_name":"LCP","metric_value":1.25}\n'
        b'{"event_id":"error-1","timestamp":"2026-09-14T20:00:01Z","error_message":"boom"}\n'
    )

    result = controller.ingest(
        service_id="svc",
        domain="rum_vitals",
        object_key="raw/rum/mixed.gz",
        checksum="sha256:mixed",
        payload=payload,
        version="v1",
    )

    assert result.decoded.accepted_rows == 2
    assert len(clickhouse.batches) == 1
    assert len(clickhouse.batches[0].rows) == 1
    assert clickhouse.batches[0].rows[0]["metric_name"] == "LCP"
    assert result.publication.rows_visible == 1


def test_ingest_publishes_nothing_when_every_row_belongs_to_a_sibling_domain() -> None:
    """The inverse: an object claimed by rum_vitals whose every decoded row
    is actually error-shaped must archive cleanly and publish zero rows,
    not crash trying to build/insert an empty serving batch."""
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )
    payload = gzip.compress(b'{"event_id":"error-1","timestamp":"2026-09-14T20:00:01Z","error_message":"boom"}\n')

    result = controller.ingest(
        service_id="svc",
        domain="rum_vitals",
        object_key="raw/rum/all-errors.gz",
        checksum="sha256:all-errors",
        payload=payload,
        version="v1",
    )

    assert result.decoded.accepted_rows == 1
    assert len(clickhouse.batches) == 0
    assert result.publication.rows_visible == 0


def test_all_malformed_source_is_archived_without_serving_rows() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = InMemoryObjectStore()
    clickhouse = _ClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )

    result = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/malformed.gz",
        checksum="sha256:malformed",
        payload=gzip.compress(b"not-json\n"),
    )

    assert result.decoded.accepted_rows == 0
    assert result.decoded.quarantined_rows == 1
    assert result.publication.rows_visible == 0
    archived_rows = pq.read_table(
        pa.BufferReader(ArchivePublication(objects).read_artifact(result.manifest))
    ).to_pylist()
    assert len(archived_rows) == 1
    assert archived_rows[0]["_record_kind"] == "dead_letter"
    assert archived_rows[0]["line_ordinal"] == 0
    assert archived_rows[0]["raw_line_base64"] == "bm90LWpzb24="
    assert archived_rows[0]["source_object_key"] == "raw/request/malformed.gz"
    assert archived_rows[0]["reason"]


def test_controller_refuses_sources_owned_by_another_data_plane() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    ownership.initialize("svc", owner="standard", source_cursor="cursor-0")
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
        worker_id="worker-1",
    )

    with pytest.raises(RuntimeError, match="not the owner"):
        controller.ingest(
            service_id="svc",
            domain="request",
            object_key="raw/request/one.gz",
            checksum="sha256:source",
            payload=b'{"url":"/ok"}\n',
        )


class _ManifestIdentityCheckingControl(_PostgresControl):
    """Mirrors real PostgresControlPlane's ``register_archive_manifest``: the
    first registration for a manifest_id wins (``ON CONFLICT DO NOTHING``),
    and a later call with byte-different content for the SAME manifest_id
    raises — exactly the check that caught ingest() rebuilding a fresh
    manifest (different coverage_start/end) on every retry."""

    def __init__(self) -> None:
        super().__init__()
        self._stored: dict[str, object] = {}

    def register_archive_manifest(self, manifest, *, owner_epoch: int):
        self.calls.append("register_manifest")
        stored = self._stored.setdefault(manifest.manifest_id, manifest)
        if stored != manifest:
            raise ValueError("archive manifest identity changed")
        return self.archive_state

    def archive_manifest(self, manifest_id: str):
        self.calls.append("archive_manifest_lookup")
        stored = self._stored.get(manifest_id)
        if stored is None:
            raise KeyError(manifest_id)
        return type("Record", (), {"manifest": stored, "owner_epoch": 1})()


def test_ingest_reuses_the_registered_manifest_on_retry_instead_of_rebuilding() -> None:
    """Regression test: manifest_id is content-addressed from (service_id,
    domain, object_key, checksum, artifact_checksum) — stable across
    retries — but coverage_start/coverage_end come from wall-clock `now`,
    which is not. Before this fix, a retry (e.g. after a crash between
    archive publish and acknowledgement) rebuilt a fresh manifest with the
    SAME manifest_id but a different coverage window, and
    register_archive_manifest correctly rejected it as a changed identity —
    observed live as a real object retrying forever. ingest() must reuse
    the already-registered manifest's content on retry, not rebuild one."""
    control = _ManifestIdentityCheckingControl()
    clickhouse = _ClickHouse()
    controller = HighScaleIngestController(
        ownership=control,  # type: ignore[arg-type]
        ledger=HighScaleLedger(),
        control_plane=control,  # type: ignore[arg-type]
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )
    payload = gzip.compress(b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok"}\n')

    first = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    # A retry at a LATER wall-clock time must not raise, and must reuse the
    # original manifest's coverage window rather than a fresh one.
    second = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=datetime(2026, 9, 11, 20, 5, tzinfo=UTC),
    )

    assert second.manifest.manifest_id == first.manifest.manifest_id
    assert second.manifest.coverage_start == first.manifest.coverage_start
    assert second.manifest.coverage_end == first.manifest.coverage_end


def test_controller_can_use_postgres_control_plane_for_durable_state() -> None:
    control = _PostgresControl()
    clickhouse = _ClickHouse()
    controller = HighScaleIngestController(
        ownership=control,  # type: ignore[arg-type]
        ledger=HighScaleLedger(),
        control_plane=control,  # type: ignore[arg-type]
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        worker_id="worker-1",
    )

    result = controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=gzip.compress(b'{"timestamp":"2026-09-11T20:00:00Z","url":"/ok"}\n'),
        version="v1",
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )

    assert result.source.object_id == "object-1"
    assert control.calls == [
        "discover",
        "claim",
        "counts",
        "archive_manifest_lookup",
        "register_manifest",
        "transition_manifest",
        "transition_manifest",
        "transition_manifest",
        "appended",
        "archived",
        "acknowledged",
    ]
