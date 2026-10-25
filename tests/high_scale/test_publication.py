from uuid import UUID

import pytest

from backend.high_scale.publication import (
    BatchManifest,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PartialInsert,
    PostgresBatchManifestStore,
    PublicationStatus,
    row_matches_serving_domain,
)


def _batch() -> HighScaleBatch:
    return HighScaleBatch(
        batch_id="batch-1",
        service_id="svc",
        domain="request",
        generation="epoch-4",
        rows=({"event_id": "event-1"}, {"event_id": "event-2"}),
    )


class FakeClickHouse:
    def __init__(self) -> None:
        self.inserts: list[str] = []
        self.fail_after_insert = False
        self.partial_rows: int | None = None

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.inserts.append(batch.batch_id)
        if self.partial_rows is not None:
            raise PartialInsert(self.partial_rows)
        if self.fail_after_insert:
            self.fail_after_insert = False
            raise RuntimeError("lost acknowledgement")
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


def test_retry_after_lost_acknowledgement_publishes_once() -> None:
    store = InMemoryBatchManifestStore()
    client = FakeClickHouse()
    client.fail_after_insert = True
    publication = ClickHousePublication(store, client)
    batch = _batch()

    with pytest.raises(RuntimeError, match="lost acknowledgement"):
        publication.publish(batch)

    result = publication.publish(batch)

    assert result.status is PublicationStatus.VISIBLE
    assert result.rows_visible == 2
    assert client.inserts == ["batch-1", "batch-1"]
    assert store.get("batch-1").status is PublicationStatus.VISIBLE


def test_partial_insert_never_becomes_visible() -> None:
    store = InMemoryBatchManifestStore()
    client = FakeClickHouse()
    client.partial_rows = 1
    publication = ClickHousePublication(store, client)

    with pytest.raises(PartialInsert):
        publication.publish(_batch())

    assert store.get("batch-1").status is PublicationStatus.PENDING


def test_postgres_manifest_store_survives_store_recreation() -> None:
    from types import SimpleNamespace

    manifests: dict[str, BatchManifest] = {}
    control = SimpleNamespace(
        get_batch_manifest=manifests.get,
        put_batch_manifest=lambda m: manifests.__setitem__(m.batch_id, m),
    )
    first_store = PostgresBatchManifestStore(control)
    first_store.put(
        BatchManifest(
            batch_id="batch-1",
            service_id="svc",
            domain="request",
            generation="epoch-4",
            digest="sha256:batch",
            expected_rows=2,
            status=PublicationStatus.VISIBLE,
            visible_rows=2,
        )
    )

    recreated_store = PostgresBatchManifestStore(control)

    manifest = recreated_store.get("batch-1")
    assert manifest is not None
    assert manifest.status is PublicationStatus.VISIBLE
    assert manifest.visible_rows == 2


def test_same_batch_id_with_different_digest_is_rejected() -> None:
    store = InMemoryBatchManifestStore()
    client = FakeClickHouse()
    publication = ClickHousePublication(store, client)
    publication.publish(_batch())

    changed = HighScaleBatch(
        batch_id="batch-1",
        service_id="svc",
        domain="request",
        generation="epoch-4",
        rows=({"event_id": "different"},),
    )
    with pytest.raises(ValueError, match="digest"):
        publication.publish(changed)


def _request_batch() -> HighScaleBatch:
    return HighScaleBatch(
        batch_id="svc:request:object-1",
        service_id="svc",
        domain="request",
        generation="4",
        rows=(
            {
                "event_id": "event-1",
                "timestamp": "2026-09-11T20:00:00Z",
                "source_object_key": "raw/request/object-1.gz",
                "source_object_version": "sha256:object-1",
                "line_ordinal": 0,
                "transform_version": "normalize.v1",
                "country": "US",
                "client_ip": "192.0.2.1",
                "url": "/",
                "custom_fields": {"tenant": "demo"},
                "cmcd": {},
            },
        ),
    )


class FakeHttpClickHouse:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[str], list[tuple]]] = []
        self.publications: dict[str, dict] = {}
        self.facts: dict[str, list[tuple]] = {}
        self.fail_fact_ack = False
        self.fail_visible_ack = False

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        params = params or {}
        if "high_scale_batch_publications" in sql:
            row = self.publications.get(str(params["batch_id"]))
            return [] if row is None else [row]
        if "SELECT count()" in sql:
            return [{"row_count": len(self.facts.get(str(params["batch_id"]), []))}]
        raise AssertionError(f"unexpected query: {sql}")

    def insert_rows(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        self.inserts.append((table, columns, rows))
        if table == "high_scale_batch_publications":
            row = rows[0]
            publication = dict(zip(columns, row, strict=True))
            self.publications[str(row[0])] = publication
            if publication["publication_state"] == "visible" and self.fail_visible_ack:
                self.fail_visible_ack = False
                raise RuntimeError("lost acknowledgement")
        else:
            batch_id = str(rows[0][4] if table == "cmcd_projection_facts" else rows[0][8])
            self.facts.setdefault(batch_id, []).extend(rows)
            if self.fail_fact_ack:
                self.fail_fact_ack = False
                raise RuntimeError("lost acknowledgement")


def test_clickhouse_adapter_inserts_exact_allowlisted_shape() -> None:
    client = FakeHttpClickHouse()
    result = ClickHouseBatchAdapter(client).insert(_request_batch())

    assert result.rows_inserted == 1
    pending, facts, visible = client.inserts
    assert pending[0] == "high_scale_batch_publications"
    assert pending[1] == [
        "batch_id",
        "service_id",
        "domain",
        "generation",
        "batch_digest",
        "expected_rows",
        "visible_rows",
        "quorum_acked",
        "publication_state",
        "manifest_version",
        "updated_at",
    ]
    assert facts[0] == "request_facts"
    assert facts[1][-5:] == ["country", "client_ip", "url", "custom_fields", "cmcd"]
    assert UUID(facts[2][0][1])
    assert UUID(facts[2][0][8])
    assert visible[2][0][7:9] == (1, "visible")


def test_clickhouse_adapter_uses_raw_request_ip_for_client_ip() -> None:
    client = FakeHttpClickHouse()
    row = {key: value for key, value in _request_batch().rows[0].items() if key != "client_ip"}
    row["ip"] = "198.51.100.10"
    batch = HighScaleBatch("batch-raw-ip", "svc", "request", "4", (row,))

    ClickHouseBatchAdapter(client).insert(batch)

    facts = next(rows for table, _, rows in client.inserts if table == "request_facts")
    assert facts[0][-4] == "198.51.100.10"


def test_clickhouse_adapter_rejects_invalid_rows_before_http() -> None:
    batch = _request_batch()
    invalid = HighScaleBatch(
        batch.batch_id,
        batch.service_id,
        batch.domain,
        batch.generation,
        ({**batch.rows[0], "custom_fields": {"bad": 1}},),
    )
    client = FakeHttpClickHouse()

    with pytest.raises(ValueError, match="keys and values"):
        ClickHouseBatchAdapter(client).insert(invalid)

    assert client.inserts == []


def test_clickhouse_adapter_duplicate_retry_does_not_insert_facts_twice() -> None:
    client = FakeHttpClickHouse()
    adapter = ClickHouseBatchAdapter(client)
    batch = _request_batch()

    first = adapter.insert(batch)
    second = adapter.insert(batch)

    assert first == second
    assert [table for table, _, _ in client.inserts].count("request_facts") == 1


def test_clickhouse_adapter_lost_visible_ack_is_recovered_by_retry() -> None:
    client = FakeHttpClickHouse()
    client.fail_visible_ack = True
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ClickHouseBatchAdapter(client))
    batch = _request_batch()

    with pytest.raises(RuntimeError, match="lost acknowledgement"):
        publication.publish(batch)

    result = publication.publish(batch)

    assert result.status is PublicationStatus.VISIBLE
    assert [table for table, _, _ in client.inserts].count("request_facts") == 1


def test_clickhouse_adapter_lost_fact_ack_is_recovered_without_duplicate_rows() -> None:
    client = FakeHttpClickHouse()
    client.fail_fact_ack = True
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ClickHouseBatchAdapter(client))
    batch = _request_batch()

    with pytest.raises(RuntimeError, match="lost acknowledgement"):
        publication.publish(batch)

    result = publication.publish(batch)

    assert result.status is PublicationStatus.VISIBLE
    assert [table for table, _, _ in client.inserts].count("request_facts") == 1


@pytest.mark.parametrize(
    ("domain", "row", "table"),
    [
        (
            "rum_vitals",
            {
                "event_id": "rum-vital-1",
                "timestamp": "2026-09-11T20:00:00Z",
                "source_object_key": "raw/rum/vitals-1.gz",
                "source_object_version": "sha256:rum-1",
                "line_ordinal": 0,
                "transform_version": "normalize.v1",
                "metric_name": "LCP",
                "metric_value": 1.25,
            },
            "rum_vitals_facts",
        ),
        (
            "rum_errors",
            {
                "event_id": "rum-error-1",
                "timestamp": "2026-09-11T20:00:00Z",
                "source_object_key": "raw/rum/errors-1.gz",
                "source_object_version": "sha256:rum-2",
                "line_ordinal": 0,
                "transform_version": "normalize.v1",
                "error_message": "boom",
            },
            "rum_error_facts",
        ),
        (
            "cmcd",
            {
                "request_event_id": "request-event-1",
                "timestamp": "2026-09-11T20:00:00Z",
                "fields": {"br": "1000"},
            },
            "cmcd_projection_facts",
        ),
    ],
)
def test_clickhouse_adapter_maps_each_schema_domain(domain: str, row: dict, table: str) -> None:
    client = FakeHttpClickHouse()
    batch = HighScaleBatch("batch-" + domain, "svc", domain, "4", (row,))

    ClickHouseBatchAdapter(client).insert(batch)

    assert [name for name, _, _ in client.inserts].count(table) == 1


class TestRowMatchesServingDomain:
    """A raw raw/rum/ object mixes vitals and error beacon lines from the
    same log period, but the ledger attributes the WHOLE object to a single
    domain — whichever claims it first. Callers must filter with
    ``row_matches_serving_domain`` before building a serving HighScaleBatch,
    or a mismatched row either crashes ``_rows_for_domain`` (rum_vitals: no
    finite metric_value) or becomes a bogus, all-blank fact row (rum_errors:
    empty error_message defaulted silently)."""

    def test_rum_vitals_row_with_finite_metric_value_matches(self):
        assert row_matches_serving_domain("rum_vitals", {"metric_value": 1.25}) is True

    def test_rum_vitals_row_missing_metric_value_does_not_match(self):
        assert row_matches_serving_domain("rum_vitals", {"error_message": "boom"}) is False

    def test_rum_vitals_row_with_nan_metric_value_does_not_match(self):
        assert row_matches_serving_domain("rum_vitals", {"metric_value": float("nan")}) is False

    def test_rum_errors_row_with_error_message_matches(self):
        assert row_matches_serving_domain("rum_errors", {"error_message": "boom"}) is True

    def test_rum_errors_row_missing_error_message_does_not_match(self):
        assert row_matches_serving_domain("rum_errors", {"metric_value": 1.25}) is False

    def test_request_domain_rows_always_match(self):
        assert row_matches_serving_domain("request", {}) is True


def test_cmcd_rows_persist_country_and_asn_after_the_cmcd_map() -> None:
    from backend.high_scale.publication import _DOMAIN_COLUMNS, _rows_for_domain

    batch = HighScaleBatch(
        "batch-cmcd",
        "svc",
        "cmcd",
        "4",
        (
            {
                "request_event_id": "request-event-1",
                "timestamp": "2026-09-11T20:00:00Z",
                "fields": {"br": "1000"},
                "country": "US",
                "asn": 7922,
            },
            {"request_event_id": "request-event-2", "timestamp": "2026-09-11T20:00:01Z", "fields": {"br": "1"}},
        ),
    )

    first, second = _rows_for_domain(batch, "00000000-0000-0000-0000-000000000001")

    assert _DOMAIN_COLUMNS["cmcd"][-3:] == ("cmcd", "country", "asn")
    assert len(first) == len(_DOMAIN_COLUMNS["cmcd"])
    assert first[-2:] == ("US", 7922)
    assert second[-2:] == ("", None)


def _rum_batch(domain: str, row: dict) -> HighScaleBatch:
    base = {
        "event_id": f"{domain}-1",
        "timestamp": "2026-09-11T20:00:00Z",
        "source_object_key": "raw/rum/one.gz",
        "source_object_version": "v1",
        "line_ordinal": 0,
        "transform_version": "normalize.v1",
    }
    return HighScaleBatch(f"batch-{domain}", "svc", domain, "4", ({**base, **row},))


def test_rum_vitals_rows_persist_environment_columns_after_country() -> None:
    from backend.high_scale.publication import _DOMAIN_COLUMNS, _rows_for_domain

    batch = _rum_batch(
        "rum_vitals",
        {
            "metric_name": "LCP",
            "metric_value": 1.5,
            "browser": "Firefox",
            "os": "Linux",
            "device": "Desktop",
            "geo_country_code": "DE",
            "city": "Berlin",
            "region": "BE",
            "server_pop": "FRA",
            "tls": "1.3",
            "ttfb": "0.04",
        },
    )

    (row,) = _rows_for_domain(batch, "00000000-0000-0000-0000-000000000001")

    columns = _DOMAIN_COLUMNS["rum_vitals"]
    assert columns[-9:] == ("country", "browser", "os", "device", "city", "region", "pop", "tls", "ttfb")
    assert len(row) == len(columns)
    assert row[-9:] == ("DE", "Firefox", "Linux", "Desktop", "Berlin", "BE", "FRA", "1.3", 0.04)


def test_rum_error_rows_persist_error_position_and_environment() -> None:
    from backend.high_scale.publication import _DOMAIN_COLUMNS, _rows_for_domain

    batch = _rum_batch(
        "rum_errors",
        {
            "error_message": "boom",
            "error_file": "app.js",
            "rum_error_line": "12",
            "rum_error_col": "34",
            "browser": "Chrome",
            "country": "US",
        },
    )

    (row,) = _rows_for_domain(batch, "00000000-0000-0000-0000-000000000001")

    columns = _DOMAIN_COLUMNS["rum_errors"]
    assert columns[-11:] == (
        "country",
        "error_line",
        "error_col",
        "browser",
        "os",
        "device",
        "city",
        "region",
        "pop",
        "tls",
        "ttfb",
    )
    assert len(row) == len(columns)
    assert row[-11:] == ("US", 12, 34, "Chrome", "", "", "", "", "", "", None)


def test_rum_rows_without_environment_keep_empty_columns_not_defaults() -> None:
    from backend.high_scale.publication import _rows_for_domain

    (row,) = _rows_for_domain(
        _rum_batch("rum_vitals", {"metric_name": "CLS", "metric_value": 0.1}),
        "00000000-0000-0000-0000-000000000001",
    )

    assert row[-9:] == ("", "", "", "", "", "", "", "", None)


# ── Merged from test_archive_publication.py ──

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import (
    ArchivePublication,
    InMemoryObjectStore,
    PublicationState,
)
from tests.high_scale.conftest import sha256_checksum as _checksum


def _manifest() -> ArchiveManifest:
    return ArchiveManifest(
        manifest_id="manifest-1",
        source=ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 10, "v1"),
        artifact=ArchiveArtifact(
            uri="s3://archive/artifacts/manifest-1.parquet",
            checksum="sha256:artifact",
            size_bytes=4,
            row_count=2,
            byte_count=20,
            canonical_digest="sha256:events",
            schema_version="request.v1",
            transform_version="normalize.v1",
        ),
        coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        retention_deadline=datetime(2026, 9, 2, tzinfo=UTC),
        deletion_authorization_deadline=datetime(2026, 9, 3, tzinfo=UTC),
        archive_epoch=4,
    )


def test_publication_requires_verified_artifact_before_manifest_commit() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    manifest = _manifest()

    with pytest.raises(ValueError, match="artifact checksum"):
        publication.publish(manifest, b"bad")

    assert publication.state(manifest.manifest_id) is None
    assert not store.exists("archive/manifests/manifest-1.commit")


def test_publication_commits_manifest_only_after_verified_artifact() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    manifest = _manifest()
    artifact = b"data"

    manifest = replace(manifest, artifact=replace(manifest.artifact, checksum=_checksum(artifact)))

    result = publication.publish(manifest, artifact)

    assert result.state is PublicationState.MANIFEST_COMMITTED
    assert result.manifest_id == manifest.manifest_id
    assert store.exists("archive/manifests/manifest-1.commit")
    assert publication.is_replayable(manifest.manifest_id)


def test_publish_is_idempotent_even_when_a_retry_recomputes_different_timestamps() -> None:
    """Regression test: manifest_id is derived from (service_id, domain,
    object_key, checksum, artifact_checksum) — never from coverage_start/
    coverage_end — so a retried publish() for the same logical object (e.g.
    after a crash between publish and the caller's next step) computes the
    SAME manifest_id but, since coverage_start/end come from wall-clock
    `now`, a byte-DIFFERENT manifest payload. Overwriting the already-
    published key with different content breaks the immutable-artifact
    contract and, against a CDN-cached store, can make the immediate
    post-write verification read back stale bytes and fail outright —
    observed live as every retry of one real object failing here forever.
    A retry must be a no-op once the manifest is already durably published."""
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    artifact = b"data"
    manifest = replace(_manifest(), artifact=replace(_manifest().artifact, checksum=_checksum(artifact)))

    first = publication.publish(manifest, artifact)

    retried_manifest = replace(
        manifest,
        coverage_start=datetime(2026, 9, 1, 0, 5, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 1, 0, 6, tzinfo=UTC),
    )
    second = publication.publish(retried_manifest, artifact)

    assert second == first
    # The originally-published manifest content is untouched — never
    # overwritten with the retry's different coverage_start/end.
    assert publication.is_replayable(manifest.manifest_id) is True


def test_commit_marker_digest_mismatch_is_not_replayable() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    manifest = replace(_manifest(), artifact=replace(_manifest().artifact, checksum=_checksum(b"data")))
    publication.publish(manifest, b"data")
    store.put("archive/manifests/manifest-1.commit", b"tampered")

    assert publication.is_replayable(manifest.manifest_id) is False


# ── Merged from test_archive_writer.py ──


import pytest
from backend.high_scale.archive_writer import write_batch_archive_checkpoint


def test_archive_checkpoint_is_verified_and_replayable(tmp_path) -> None:
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:a", 12, "v1")
    manifest = write_batch_archive_checkpoint(
        tmp_path,
        service_id="svc",
        domain="request",
        source_batches=[(source, [{"event_id": "1", "status": 200}, {"event_id": "2", "status": 500}])],
        archive_epoch=4,
        schema_version="request.v1",
        transform_version="normalize.v1",
        coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        retention_seconds=3600,
        deletion_grace_seconds=900,
    )
    assert manifest.artifact.row_count == 2
    assert manifest.deletion_authorization_deadline > manifest.retention_deadline
    assert (tmp_path / f"{manifest.manifest_id}.manifest.json").is_file()


def test_archive_checkpoint_rejects_mismatched_source(tmp_path) -> None:
    source = ArchiveSourceObject("other", "request", "raw/request/a.gz", "sha256:a", 12)
    with pytest.raises(ValueError, match="source identity"):
        write_batch_archive_checkpoint(
            tmp_path,
            service_id="svc",
            domain="request",
            source_batches=[(source, [{"event_id": "1"}])],
            archive_epoch=1,
            schema_version="request.v1",
            transform_version="normalize.v1",
            coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        )


def test_archive_checkpoint_accepts_empty_rum_body(tmp_path) -> None:
    source = ArchiveSourceObject("svc", "rum_errors", "raw/rum/errors.gz", "sha256:a", 12)
    manifest = write_batch_archive_checkpoint(
        tmp_path,
        service_id="svc",
        domain="rum_errors",
        source_batches=[
            (
                source,
                [
                    {
                        "event_id": "1",
                        "rum_body": {},
                        "error_message": "empty body remains publishable",
                    }
                ],
            )
        ],
        archive_epoch=1,
        schema_version="rum_errors.v1",
        transform_version="normalize.v1",
        coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
    )
    assert manifest.artifact.row_count == 1


# ── Merged from test_raw_query.py ──


from backend.high_scale._common import raw_query_producer
from tests.high_scale.conftest import (
    InMemoryManifestCatalog as _Catalog,
)
from tests.high_scale.conftest import (
    make_test_archive_manifest as _raw_query_manifest,
)


def test_producer_yields_rows_from_all_manifests_covering_the_window() -> None:
    m1, bytes1 = _raw_query_manifest(
        "m1",
        datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        ({"event_id": "1", "timestamp": "2026-09-01T00:00:30Z"},),
    )
    m2, bytes2 = _raw_query_manifest(
        "m2",
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 2, tzinfo=UTC),
        ({"event_id": "2", "timestamp": "2026-09-01T00:01:30Z"},),
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(m1, bytes1)
    archive.publish(m2, bytes2)
    catalog = _Catalog((m1, m2))

    producer = raw_query_producer(
        catalog,
        archive,
        service_id="svc",
        domain="request",
        start=datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        end=datetime(2026, 9, 1, 0, 2, tzinfo=UTC),
    )

    rows = list(producer())

    assert [row["event_id"] for row in rows] == ["1", "2"]


def test_producer_excludes_rows_outside_the_requested_window() -> None:
    manifest, artifact_bytes = _raw_query_manifest(
        "m1",
        datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 2, tzinfo=UTC),
        (
            {"event_id": "early", "timestamp": "2026-09-01T00:00:10Z"},
            {"event_id": "in-window", "timestamp": "2026-09-01T00:01:00Z"},
            {"event_id": "late", "timestamp": "2026-09-01T00:01:50Z"},
        ),
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)
    catalog = _Catalog((manifest,))

    producer = raw_query_producer(
        catalog,
        archive,
        service_id="svc",
        domain="request",
        start=datetime(2026, 9, 1, 0, 0, 30, tzinfo=UTC),
        end=datetime(2026, 9, 1, 0, 1, 30, tzinfo=UTC),
    )

    rows = list(producer())

    assert [row["event_id"] for row in rows] == ["in-window"]


def test_producer_yields_nothing_when_no_manifest_covers_the_window() -> None:
    catalog = _Catalog(())
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)

    producer = raw_query_producer(
        catalog,
        archive,
        service_id="svc",
        domain="request",
        start=datetime(2026, 9, 1, tzinfo=UTC),
        end=datetime(2026, 9, 1, 1, tzinfo=UTC),
    )

    assert list(producer()) == []
