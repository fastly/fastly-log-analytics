"""Publishing a committed archive manifest again after a failed ClickHouse or derived publication."""

from __future__ import annotations

import gzip
import logging
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest

from backend.high_scale.archive import (
    ArchivePublication,
    ClickHousePublication,
    HighScaleBatch,
    InMemoryObjectStore,
    InsertReceipt,
    PostgresBatchManifestStore,
    PublicationStatus,
)
from backend.high_scale.archive.manifests import build_batch_id_from_manifest_id
from backend.high_scale.ingest_controller import (
    HighScaleIngestController,
    HighScaleWorkerCoordinator,
    SourceObjectDescriptor,
    SourceObjectMissingError,
    SourceObjectPage,
)
from backend.high_scale.postgres_control import PostgresControlPlane

_PG_TARGET = "backend.core.metadata.base.get_pg_readonly_connection"
_BOT_ROW = {"waf_req_id": "req-bot", "bot_name": "bingbot", "category": "search", "wellknown_bot_name": "bing"}


class _Pg:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def execute(self, sql: str, params: list[str]) -> _Pg:
        return self

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows

    def close(self) -> None:
        return None


class _ClickHouse:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []
        self.failing: set[str] = set()
        self.attempts: list[str] = []

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.attempts.append(batch.domain)
        if batch.domain in self.failing:
            raise RuntimeError(f"clickhouse rejected {batch.domain}")
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    def batch(self, batch_id: str) -> HighScaleBatch:
        return next(b for b in self.batches if b.batch_id == batch_id)


class _Lister:
    def __init__(self, objects: list[SourceObjectDescriptor]) -> None:
        self.objects = objects

    def list_source_objects(self, service_id: str, domain: str, *, page_size: int, cursor: str | None, **_: Any):
        return SourceObjectPage(tuple(self.objects), None)


class _Reader:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads

    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes:
        if object_key not in self.payloads:
            raise SourceObjectMissingError(f"source object is missing from storage: {object_key}")
        return self.payloads[object_key]


class _FailingArchive(ArchivePublication):
    def publish(self, manifest, artifact):
        raise RuntimeError("fos unavailable")


class _Harness:
    def __init__(self, control: PostgresControlPlane, sid: str, *, archive: ArchivePublication | None = None) -> None:
        self.control, self.sid = control, sid
        self.objects = InMemoryObjectStore()
        self.facts, self.derived = _ClickHouse(), _ClickHouse()
        self.archive = archive or ArchivePublication(self.objects)
        self.reader = _Reader({})
        self.lister = _Lister([])
        control.initialize_owner(sid, owner="high_scale", source_cursor="__initial__")
        self.controller = HighScaleIngestController(
            ownership=control,
            ledger=control,
            control_plane=control,
            archive=self.archive,
            serving=ClickHousePublication(PostgresBatchManifestStore(control), self.facts),
            aggregates=ClickHousePublication(PostgresBatchManifestStore(control), self.derived),
            worker_id="worker-1",
            lease_seconds=60,
            deletion_grace_seconds=0,
        )
        self.coordinator = HighScaleWorkerCoordinator(
            lister=self.lister,
            reader=self.reader,
            controller=self.controller,
            ownership=control,
            ledger=control,
            worker_id="worker-1",
            control_plane=control,
            page_size=10,
            lease_seconds=60,
        )

    def add_source(self, key: str, body: bytes) -> SourceObjectDescriptor:
        payload = gzip.compress(body)
        descriptor = SourceObjectDescriptor(key, f"etag-{key}", len(payload), "v1")
        self.lister.objects.append(descriptor)
        self.reader.payloads[key] = payload
        return descriptor

    def run_page(self, domain: str = "request"):
        return self.coordinator.run_page(service_id=self.sid, domain=domain)

    def retry(self, **kwargs: Any):
        kwargs.setdefault("min_idle_seconds", 0)
        return self.controller.retry_archived_publications(self.sid, **kwargs)

    def source(self, key: str):
        return self.control.source(self.sid, key)

    def publication_status(self, batch_id: str) -> PublicationStatus | None:
        manifest = self.control.get_batch_manifest(batch_id)
        return None if manifest is None else manifest.status


@pytest.fixture
def harness(request):
    control = PostgresControlPlane()
    control.create_schema()
    sid = f"svc-pubretry-{uuid4().hex[:10]}"
    yield lambda **kwargs: _Harness(control, sid, **kwargs)
    with control.transaction() as connection:
        tables = connection.execute(
            "SELECT table_name FROM information_schema.columns WHERE table_schema = current_schema() "
            "AND column_name = 'service_id' AND table_name LIKE 'high_scale_%'"
        ).fetchall()
        for (table,) in tables:
            connection.execute(f"DELETE FROM {table} WHERE service_id = %s", (sid,))


def _pass_deletion_grace(h: _Harness) -> None:
    with h.control.transaction() as connection:
        connection.execute(
            "UPDATE high_scale_archive_manifests SET coverage_start = coverage_start - interval '1 hour', "
            "coverage_end = coverage_end - interval '1 hour', retention_deadline = retention_deadline - interval '1 hour', "
            "deletion_authorization_deadline = deletion_authorization_deadline - interval '1 hour' WHERE service_id = %s",
            (h.sid,),
        )


def _request_body(prefix: str, count: int, *, bot: bool = False, dead_letter: bool = False) -> bytes:
    lines = [
        (
            f'{{"timestamp":"2026-10-09T12:00:{i % 60:02d}Z","url":"/{prefix}/{i}","status":200,'
            f'"client_ip":"192.0.2.{i % 250}","sid":"session-{prefix}",'
            f'"cmcd":{{"br":{1000 + i},"d":4000}}}}'
        ).encode()
        for i in range(count)
    ]
    if bot:
        lines.append(b'{"timestamp":"2026-10-09T12:01:00Z","url":"/bot","status":200,"waf_req_id":"req-bot"}')
        lines.append(b'{"timestamp":"2026-10-09T12:01:01Z","url":"/human","status":200,"waf_req_id":"req-human"}')
    if dead_letter:
        lines.append(b"")
        lines.append(b"this is not json")
    return b"\n".join(lines) + b"\n"


def _rum_body(count: int) -> bytes:
    lines = [
        f'{{"timestamp":"2026-10-09T12:00:{i:02d}Z","metric_name":"LCP","metric_value":{1 + i / 10}}}'.encode()
        for i in range(count)
    ]
    lines.append(b'{"timestamp":"2026-10-09T12:00:59Z","error_message":"boom"}')
    return b"\n".join(lines) + b"\n"


def test_post_commit_clickhouse_failure_binds_sources_to_the_committed_archive(harness) -> None:
    h = harness()
    first = h.add_source("raw/request/a.log.gz", _request_body("a", 25))
    second = h.add_source("raw/request/b.log.gz", _request_body("b", 25))
    h.facts.failing.add("request")

    page = h.run_page()

    assert page.failed == 2
    bound = [h.source(first.object_key), h.source(second.object_key)]
    assert {s.status for s in bound} == {"archived"}
    manifest_id = bound[0].archive_manifest_id
    assert manifest_id and bound[1].archive_manifest_id == manifest_id
    assert all(s.owner is None and s.lease_until is None and s.last_error for s in bound)
    assert h.control.archive_manifest(manifest_id).state.value == "manifest_committed"
    assert h.publication_status(build_batch_id_from_manifest_id(manifest_id)) is PublicationStatus.PENDING
    assert (
        h.control.claim_sources_batch(
            h.sid, [first.object_key, second.object_key], "worker-2", lease_seconds=60, expected_owner_epoch=1
        )
        == ()
    )
    _pass_deletion_grace(h)
    assert h.control.deletable_sources(h.sid) == ()

    h.facts.failing.clear()
    rerun = h.run_page()
    assert (rerun.duplicates, rerun.processed, rerun.failed) == (2, 0, 0)
    assert {h.source(k).status for k in (first.object_key, second.object_key)} == {"archived"}


def test_retry_publishes_committed_rows_with_the_original_batch_ids_after_raw_is_gone(harness) -> None:
    h = harness()
    first = h.add_source("raw/request/a.log.gz", _request_body("a", 30, bot=True, dead_letter=True))
    second = h.add_source("raw/request/b.log.gz", _request_body("b", 20))
    h.facts.failing.add("request")
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        h.run_page()
    manifest_id = h.source(first.object_key).archive_manifest_id
    facts_id = build_batch_id_from_manifest_id(manifest_id)
    cmcd_id = build_batch_id_from_manifest_id(f"{manifest_id}:cmcd")
    aggregate_id = build_batch_id_from_manifest_id(f"{manifest_id}:request_aggregate")
    pending_digest = h.control.get_batch_manifest(facts_id).digest
    h.reader.payloads.clear()
    h.facts.failing.clear()

    with patch(_PG_TARGET, return_value=_Pg([])):
        result = h.retry(limit=5)

    assert (result.acknowledged, result.failed, result.released_to_raw) == (1, 0, 0)
    for key in (first.object_key, second.object_key):
        source = h.source(key)
        assert (source.status, source.archive_manifest_id, source.last_error) == ("acknowledged", manifest_id, None)
    published = h.facts.batch(facts_id)
    assert published.digest == pending_digest and published.generation == "1"
    assert len(published.rows) == 52
    bot_rows = [r for r in published.rows if r.get("waf_req_id") == "req-bot"]
    assert [(r["ngwaf_bot"], r["ngwaf_bot_category"]) for r in bot_rows] == [("bingbot", "search")]
    assert not any("ngwaf_bot" in r for r in published.rows if r.get("waf_req_id") == "req-human")
    assert not any("_ngwaf_enrichment" in r for r in published.rows)
    assert len(h.facts.batch(cmcd_id).rows) == 50
    for batch_id in (facts_id, cmcd_id, aggregate_id):
        assert h.publication_status(batch_id) is PublicationStatus.VISIBLE
    security = [b for b in h.derived.batches if b.domain == "security_dimensions"]
    assert [b.batch_id for b in security] == [build_batch_id_from_manifest_id(f"{manifest_id}:security_dimensions")]
    assert any(r.get("value") == "bingbot" for r in security[0].rows)
    _pass_deletion_grace(h)
    assert {s.object_key for s in h.control.deletable_sources(h.sid)} == {first.object_key, second.object_key}

    rerun = h.run_page()
    assert (rerun.duplicates, rerun.missing, rerun.failed) == (2, 0, 0)
    assert h.retry(limit=5).acknowledged == 0


def test_retry_after_a_derived_failure_reuses_the_visible_fact_batch(harness) -> None:
    h = harness()
    source = h.add_source("raw/request/a.log.gz", _request_body("a", 10))
    h.derived.failing.add("request_aggregate")
    h.run_page()
    manifest_id = h.source(source.object_key).archive_manifest_id
    facts_id = build_batch_id_from_manifest_id(manifest_id)
    assert h.source(source.object_key).status == "archived"
    assert h.publication_status(facts_id) is PublicationStatus.VISIBLE
    h.reader.payloads.clear()
    h.derived.failing.clear()

    result = h.retry(limit=5)

    assert result.acknowledged == 1 and h.source(source.object_key).status == "acknowledged"
    assert [b.batch_id for b in h.facts.batches].count(facts_id) == 1
    aggregate_id = build_batch_id_from_manifest_id(f"{manifest_id}:request_aggregate")
    assert h.publication_status(aggregate_id) is PublicationStatus.VISIBLE


def test_retry_republishes_the_rum_sibling_domain_from_the_archive(harness) -> None:
    h = harness()
    source = h.add_source("raw/rum/a.log.gz", _rum_body(30))
    h.facts.failing.add("rum_errors")
    h.run_page("rum_vitals")
    bound = h.source(source.object_key)
    assert bound.status == "archived"
    manifest_id = bound.archive_manifest_id
    sibling_id = build_batch_id_from_manifest_id(f"{manifest_id}:rum_errors")
    assert h.publication_status(sibling_id) is PublicationStatus.PENDING
    h.reader.payloads.clear()
    h.facts.failing.clear()

    result = h.retry(limit=5)

    assert result.acknowledged == 1 and h.source(source.object_key).status == "acknowledged"
    assert h.publication_status(build_batch_id_from_manifest_id(manifest_id)) is PublicationStatus.VISIBLE
    assert h.publication_status(sibling_id) is PublicationStatus.VISIBLE
    assert [r["error_message"] for r in h.facts.batch(sibling_id).rows] == ["boom"]
    sibling_aggregate = build_batch_id_from_manifest_id(f"{manifest_id}:rum_errors_aggregate")
    assert h.publication_status(sibling_aggregate) is PublicationStatus.VISIBLE


def test_failure_before_the_archive_commit_returns_sources_to_discovered(harness) -> None:
    h = harness(archive=_FailingArchive(InMemoryObjectStore()))
    source = h.add_source("raw/request/a.log.gz", _request_body("a", 10))

    page = h.run_page()

    record = h.source(source.object_key)
    assert page.failed == 1
    assert (record.status, record.archive_manifest_id, record.owner) == ("discovered", None, None)
    assert "fos unavailable" in record.last_error
    assert h.retry(limit=5).acknowledged == 0
    assert h.facts.attempts == []


def test_an_unreadable_archive_releases_sources_back_to_raw_ingest(harness, caplog) -> None:
    h = harness()
    source = h.add_source("raw/request/a.log.gz", _request_body("a", 10))
    h.facts.failing.add("request")
    h.run_page()
    manifest_id = h.source(source.object_key).archive_manifest_id
    h.objects.put(f"archive/{manifest_id}.parquet", b"corrupted")
    h.facts.failing.clear()

    with caplog.at_level(logging.ERROR, logger="backend.high_scale.ingest_controller"):
        result = h.retry(limit=5)

    record = h.source(source.object_key)
    assert (result.acknowledged, result.released_to_raw) == (0, 1)
    assert (record.status, record.archive_manifest_id, record.owner) == ("discovered", None, None)
    assert "not replayable" in record.last_error
    assert "archive publication retry cannot read" in caplog.text
    assert h.facts.attempts == ["request"]

    h.run_page()
    assert h.source(source.object_key).status == "acknowledged"


def test_a_failed_retry_keeps_the_binding_and_backs_off(harness) -> None:
    h = harness()
    source = h.add_source("raw/request/a.log.gz", _request_body("a", 10))
    h.facts.failing.add("request")
    h.run_page()
    manifest_id = h.source(source.object_key).archive_manifest_id

    failed = h.retry(limit=5)
    again = h.retry(limit=5)

    record = h.source(source.object_key)
    assert (failed.acknowledged, failed.failed) == (0, 1)
    assert (again.acknowledged, again.failed) == (0, 0)
    assert (record.status, record.archive_manifest_id, record.owner, record.lease_until) == (
        "archived",
        manifest_id,
        None,
        None,
    )
    assert "clickhouse rejected request" in record.last_error
    assert h.facts.attempts == ["request", "request"]


def test_retry_is_fenced_to_the_current_high_scale_owner(harness) -> None:
    h = harness()
    source = h.add_source("raw/request/a.log.gz", _request_body("a", 10))
    h.facts.failing.add("request")
    h.run_page()
    h.facts.failing.clear()
    with h.control.transaction() as connection:
        connection.execute("UPDATE high_scale_ownership SET current_owner='legacy' WHERE service_id=%s", (h.sid,))

    result = h.retry(limit=5)

    assert result.acknowledged == 0 and h.source(source.object_key).status == "archived"
    assert h.facts.attempts == ["request"]
