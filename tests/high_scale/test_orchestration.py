from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import boto3
import pytest
from backend.high_scale._common import KeysetCursor, raw_query_producer
from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore, PublicationState
from backend.high_scale.archive_writer import write_batch_archive_checkpoint
from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.publication import (
    _DOMAIN_COLUMNS,
    BatchManifest,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PartialInsert,
    PostgresBatchManifestStore,
    PublicationStatus,
    _rows_for_domain,
    row_matches_serving_domain,
)
from backend.high_scale.source_discovery import (
    INITIAL_SOURCE_CURSOR,
    MAX_SOURCE_PAGE_SIZE,
    RUM_REINSPECTION_INTERVAL,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    S3SourceObjectLister,
    S3SourceObjectReader,
    SourceObjectDescriptor,
    SourceObjectMissingError,
    SourceObjectPage,
)
from botocore.stub import Stubber
from psycopg_pool import ConnectionPool

from backend.high_scale.postgres_control import PostgresControlPlane
from tests.high_scale.conftest import (
    HighScaleLedger,
    OwnershipStore,
)
from tests.high_scale.conftest import (
    InMemoryManifestCatalog as _Catalog,
)
from tests.high_scale.conftest import (
    make_test_archive_manifest as _raw_query_manifest,
)
from tests.high_scale.conftest import (
    sha256_checksum as _checksum,
)


@dataclass
class _Lister:
    page: SourceObjectPage

    def list_source_objects(self, service_id: str, domain: str, *, page_size: int, cursor: str | None):
        assert page_size == 2
        return self.page


@dataclass
class _TerminalAwareLister:
    pages: dict[str | None, SourceObjectPage]
    cursors: list[str | None]

    def list_source_objects(self, service_id: str, domain: str, *, page_size: int, cursor: str | None):
        self.cursors.append(cursor)
        return self.pages[cursor]


@dataclass
class _DomainLister:
    pages: dict[tuple[str, str | None], SourceObjectPage]
    cursors: list[tuple[str, str | None]]

    def list_source_objects(self, service_id: str, domain: str, *, page_size: int, cursor: str | None):
        self.cursors.append((domain, cursor))
        return self.pages[(domain, cursor)]


class _Reader:
    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes:
        return b"payload"


@dataclass
class _MissingObjectReader:
    missing_key: str

    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes:
        if object_key == self.missing_key:
            raise SourceObjectMissingError(f"source object is missing from storage: {object_key}")
        return b"payload"


class _Controller:
    def __init__(self, ledger: HighScaleLedger, *, fail_once: bool = False) -> None:
        self.ledger = ledger
        self.fail_once = fail_once
        self.calls = 0

    def ingest_batch(self, **kwargs: object):
        sources_payloads = list(kwargs.get("sources_payloads") or ())  # type: ignore[arg-type]
        self.calls += len(sources_payloads)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("lost acknowledgement")
        for desc, _ in sources_payloads:
            key = desc.object_key
            cur = self.ledger.source("svc", key)
            gen = cur.lease_generation if cur is not None else 1
            self.ledger.mark_appended("svc", key, gen)
            self.ledger.mark_archived("svc", key, gen, "manifest-1", 1)
            self.ledger.acknowledge("svc", key, "manifest-1")
        return type("BatchResult", (), {"sources_count": len(sources_payloads)})()


def _coordinator(*, owner: str = "high_scale", fail_once: bool = False):
    ownership = OwnershipStore()
    ownership.initialize("svc", owner=owner, source_cursor="cursor-0")
    if owner == "high_scale":
        ownership.advance_cursor("svc", "cursor-0", expected_owner=owner, expected_epoch=1, domain="request")
    ledger = HighScaleLedger()
    page = SourceObjectPage((SourceObjectDescriptor("raw/one.gz", "sha256:one", 4, "v1"),), "cursor-1")
    controller = _Controller(ledger, fail_once=fail_once)
    coordinator = HighScaleWorkerCoordinator(
        lister=_Lister(page),
        reader=_Reader(),
        controller=controller,  # type: ignore[arg-type]
        ownership=ownership,
        ledger=ledger,
        worker_id="worker",
        page_size=2,
    )
    return coordinator, ownership, ledger, controller


def test_page_discovers_bounded_objects_and_advances_durable_cursor() -> None:
    coordinator, ownership, _, _ = _coordinator()
    result = coordinator.run_page(service_id="svc", domain="request")
    assert (result.cursor, result.next_cursor, result.discovered, result.processed) == ("cursor-0", "cursor-1", 1, 1)
    assert ownership.source_cursor_for("svc", "request") == "cursor-1"


def test_standard_owner_cannot_consume_high_scale_sources() -> None:
    coordinator, _, _, _ = _coordinator(owner="standard")
    with pytest.raises(RuntimeError, match="not the owner"):
        coordinator.run_page(service_id="svc", domain="request")


def test_duplicate_retry_is_acknowledged_without_duplicate_ingest() -> None:
    coordinator, _, _, controller = _coordinator()
    first = coordinator.run_page(service_id="svc", domain="request")
    second = coordinator.run_page(service_id="svc", domain="request")
    assert (first.processed, second.duplicates, controller.calls) == (1, 1, 1)


def test_failed_acknowledgement_can_be_retried_after_lease_recovery() -> None:
    coordinator, _, ledger, controller = _coordinator(fail_once=True)
    first = coordinator.run_page(service_id="svc", domain="request")
    assert first.failed == 1
    source = ledger.source("svc", "raw/one.gz")
    assert source is not None and source.status == "discovered"

    second = coordinator.run_page(service_id="svc", domain="request")
    assert (second.processed, controller.calls) == (1, 2)


def test_missing_source_is_recorded_durably_and_does_not_block_the_cursor() -> None:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    ledger = HighScaleLedger()
    lister = _Lister(SourceObjectPage((SourceObjectDescriptor("raw/gone.gz", "sha256:gone", 4, "v1"),), "cursor-1"))
    controller = _Controller(ledger)
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=_MissingObjectReader("raw/gone.gz"),
        controller=controller,  # type: ignore[arg-type]
        ownership=ownership,
        ledger=ledger,
        worker_id="worker",
        page_size=2,
    )

    result = coordinator.run_page(service_id="svc", domain="request")
    assert (result.missing, result.failed, result.processed, controller.calls) == (1, 0, 0, 0)
    source = ledger.source("svc", "raw/gone.gz")
    assert source is not None and source.status == "source_missing"
    assert "raw/gone.gz" in (source.last_error or "")
    assert ownership.source_cursor_for("svc", "request") == "cursor-1"

    lister.page = SourceObjectPage((SourceObjectDescriptor("raw/gone.gz", "sha256:gone", 4, "v1"),), "cursor-1")
    second = coordinator.run_page(service_id="svc", domain="request", cursor="cursor-1")
    assert (second.duplicates, second.missing) == (1, 0)


def test_expired_claims_are_recovered_behind_the_listing_cursor() -> None:
    coordinator, _, ledger, controller = _coordinator()
    ledger.discover("svc", "request", "raw/old.gz", "sha256:old", size_bytes=4, version="v1")
    ledger.claim("svc", "raw/old.gz", "previous-worker", lease_seconds=1)
    ledger._con.execute(
        "UPDATE source_objects SET lease_until=? WHERE service_id=? AND object_key=?", (0, "svc", "raw/old.gz")
    )
    ledger._con.commit()
    ledger.expired_claims = lambda service_id, domain, limit=None: [  # type: ignore[attr-defined]
        s for s in (ledger.source(service_id, "raw/old.gz"),) if s and s.status == "claimed"
    ]

    result = coordinator.run_page(service_id="svc", domain="request")
    assert (result.processed, controller.calls, ledger.source("svc", "raw/old.gz").status) == (2, 2, "acknowledged")


def test_final_page_persists_terminal_cursor_for_new_objects_only() -> None:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    ledger = HighScaleLedger()
    terminal_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/one.gz"
    lister = _TerminalAwareLister(
        {
            INITIAL_SOURCE_CURSOR: SourceObjectPage(
                (SourceObjectDescriptor("raw/one.gz", "sha256:one", 4, "v1"),), None
            ),
            terminal_cursor: SourceObjectPage((), None),
        },
        [],
    )
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=_Reader(),
        controller=_Controller(ledger),  # type: ignore[arg-type]
        ownership=ownership,
        ledger=ledger,
        worker_id="worker",
        page_size=2,
    )

    first = coordinator.run_page(service_id="svc", domain="request")
    second = coordinator.run_page(service_id="svc", domain="request")
    assert first.next_cursor == second.next_cursor == terminal_cursor
    assert lister.cursors == [INITIAL_SOURCE_CURSOR, terminal_cursor]
    assert ownership.source_cursor_for("svc", "request") == terminal_cursor


def test_domains_keep_independent_source_cursors() -> None:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="initial")
    ledger = HighScaleLedger()
    lister = _DomainLister(
        {
            ("request", INITIAL_SOURCE_CURSOR): SourceObjectPage(
                (SourceObjectDescriptor("raw/request.gz", "sha256:req", 4, "v1"),), "request-next"
            ),
            ("rum_vitals", INITIAL_SOURCE_CURSOR): SourceObjectPage(
                (SourceObjectDescriptor("raw/rum/vitals.gz", "sha256:rum", 4, "v1"),), "rum-next"
            ),
        },
        [],
    )
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=_Reader(),
        controller=_Controller(ledger),  # type: ignore[arg-type]
        ownership=ownership,
        ledger=ledger,
        worker_id="worker",
        page_size=2,
    )

    coordinator.run_page(service_id="svc", domain="request")
    coordinator.run_page(service_id="svc", domain="rum_vitals")
    assert ownership.source_cursor_for("svc", "request") == "request-next"
    assert ownership.source_cursor_for("svc", "rum_vitals") == "rum-next"
    assert lister.cursors == [("request", INITIAL_SOURCE_CURSOR), ("rum_vitals", INITIAL_SOURCE_CURSOR)]


class _ThreadSafeControl:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.cursor: str | None = "cursor-0"

    def owner(self, service_id: str):
        return SimpleNamespace(current_owner="high_scale", owner_epoch=1, source_cursor=None)

    def source_cursor_for(self, service_id: str, domain: str) -> str | None:
        return self.cursor

    def discover_source(self, *args: object, **kwargs: object) -> None:
        return None

    def claim_source(self, *args: object, **kwargs: object):
        return SimpleNamespace(claimed=True, lease_generation=1)

    def source(self, service_id: str, object_key: str):
        raise KeyError(object_key)

    def advance_source_cursor(self, service_id: str, cursor: str, **kwargs: object) -> None:
        with self._lock:
            self.cursor = cursor


class _BarrierReader:
    """Each read blocks until ``parties`` reads are in flight at once."""

    def __init__(self, parties: int) -> None:
        self.barrier = threading.Barrier(parties, timeout=5)

    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes:
        self.barrier.wait()
        return b"payload"


@dataclass
class _AnyPageLister:
    page: SourceObjectPage

    def list_source_objects(self, service_id: str, domain: str, *, page_size: int, cursor: str | None):
        return self.page


def test_page_objects_are_processed_concurrently_with_pool_backed_control_plane() -> None:
    objects = tuple(SourceObjectDescriptor(f"raw/{i}.gz", f"sha256:{i}", 4, "v1") for i in range(4))
    control = _ThreadSafeControl()
    coordinator = HighScaleWorkerCoordinator(
        lister=_AnyPageLister(SourceObjectPage(objects, "cursor-1")),
        reader=_BarrierReader(4),
        controller=_CountingController(),
        ownership=None,
        ledger=None,
        worker_id="worker",  # type: ignore[arg-type]
        control_plane=control,
        page_size=10,
        object_concurrency=4,  # type: ignore[arg-type]
    )

    result = coordinator.run_page(service_id="svc", domain="request")
    assert (result.processed, result.failed, control.cursor) == (4, 0, "cursor-1")


class _ContendedControl(_ThreadSafeControl):
    def __init__(self, *, held: set[str], finished: set[str]) -> None:
        super().__init__()
        self.held = held
        self.finished = finished
        self.claimed: set[str] = set()

    def claim_source(self, service_id: str, object_key: str, *args: object, **kwargs: object):
        if object_key in self.finished:
            self.claimed.add(object_key)
            raise ValueError(f"source object is not claimable: {object_key}")
        return SimpleNamespace(claimed=object_key not in self.held, lease_generation=1)

    def source(self, service_id: str, object_key: str):
        if object_key in self.finished and object_key in self.claimed:
            return SimpleNamespace(status="acknowledged")
        raise KeyError(object_key)


class _CountingController:
    def __init__(self) -> None:
        self.keys: list[str] = []

    def ingest_batch(self, **kwargs: object):
        sources_payloads = list(kwargs.get("sources_payloads") or ())  # type: ignore[arg-type]
        for desc, _ in sources_payloads:
            self.keys.append(str(desc.object_key))
        return type("BatchResult", (), {"sources_count": len(sources_payloads)})()


def test_claims_lost_to_sibling_replicas_do_not_block_the_cursor() -> None:
    objects = (
        SourceObjectDescriptor("raw/held.gz", "sha256:a", 4, "v1"),
        SourceObjectDescriptor("raw/finished.gz", "sha256:b", 4, "v1"),
        SourceObjectDescriptor("raw/mine.gz", "sha256:c", 4, "v1"),
    )
    control = _ContendedControl(held={"raw/held.gz"}, finished={"raw/finished.gz"})
    controller = _CountingController()
    coordinator = HighScaleWorkerCoordinator(
        lister=_AnyPageLister(SourceObjectPage(objects, "cursor-1")),
        reader=_Reader(),
        controller=controller,
        ownership=None,
        ledger=None,
        worker_id="worker",  # type: ignore[arg-type]
        control_plane=control,
        page_size=10,  # type: ignore[arg-type]
    )

    result = coordinator.run_page(service_id="svc", domain="request")
    assert controller.keys == ["raw/mine.gz"]
    assert (result.processed, result.duplicates, result.failed, control.cursor) == (1, 2, 0, "cursor-1")


def test_unexpected_unclaimable_source_still_blocks_the_cursor() -> None:
    class _BrokenControl(_ThreadSafeControl):
        def claim_source(self, *args: object, **kwargs: object):
            raise ValueError("source object is not claimable: raw/x.gz")

    control = _BrokenControl()
    coordinator = HighScaleWorkerCoordinator(
        lister=_AnyPageLister(SourceObjectPage((SourceObjectDescriptor("raw/x.gz", "sha256:x", 4, "v1"),), "cursor-1")),
        reader=_Reader(),
        controller=_CountingController(),
        ownership=None,
        ledger=None,  # type: ignore[arg-type]
        worker_id="worker",
        control_plane=control,
        page_size=10,  # type: ignore[arg-type]
    )

    result = coordinator.run_page(service_id="svc", domain="request")
    assert (result.failed, control.cursor) == (1, "cursor-0")


def test_object_concurrency_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError, match="object_concurrency"):
        HighScaleWorkerCoordinator(
            lister=_AnyPageLister(SourceObjectPage((), None)),
            reader=_Reader(),
            controller=_CountingController(),
            ownership=None,
            ledger=None,  # type: ignore[arg-type]
            worker_id="worker",
            object_concurrency=0,
        )


# ── Merged from test_source_discovery.py ──


class _DummyPaginator:
    def paginate(self, **kwargs):
        return [{"Contents": [{"Key": "raw/request/a.gz", "ETag": '"etag-a"', "Size": 10}], "NextToken": "cursor-1"}]


class _DummyS3Client:
    def get_paginator(self, _name: str):
        return _DummyPaginator()


def test_s3_source_object_lister_lists_bounded_page() -> None:
    lister = S3SourceObjectLister(_DummyS3Client(), bucket="fos-bucket")
    page = lister.list_source_objects("svc", "request", page_size=10, cursor=None)
    assert page == SourceObjectPage(
        (SourceObjectDescriptor("raw/request/a.gz", "etag:etag-a", 10, '"etag-a"'),), "cursor-1"
    )


def test_s3_source_object_lister_rejects_unbounded_page_size() -> None:
    lister = S3SourceObjectLister(_DummyS3Client(), bucket="fos-bucket")
    with pytest.raises(ValueError, match="page_size"):
        lister.list_source_objects("svc", "request", page_size=MAX_SOURCE_PAGE_SIZE + 1, cursor=None)


# ── Merged from test_s3_source_adapters.py ──


class _Body:
    def read(self) -> bytes:
        return b"payload"


class _Paginator:
    def __init__(self) -> None:
        self.requests: list[dict] = []

    def paginate(self, **request):
        self.requests.append(request)
        return iter(
            [
                {
                    "Contents": [{"Key": "tenant/raw/request/one.gz", "ETag": '"source-etag"', "Size": 7}],
                    "NextToken": "next-page",
                }
            ]
        )


class _Client:
    def __init__(self) -> None:
        self.paginator = _Paginator()
        self.get_object_calls: list[dict] = []

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return self.paginator

    def get_object(self, **request):
        self.get_object_calls.append(request)
        return {"Body": _Body()}


def test_s3_lister_uses_bounded_paginated_listing_and_source_identity() -> None:
    client = _Client()
    lister = S3SourceObjectLister(client, bucket="archive-bucket", prefix="tenant")
    page = lister.list_source_objects("svc", "request", page_size=25, cursor="cursor-1")

    assert page.next_cursor == "next-page"
    assert (page.objects[0].object_key, page.objects[0].checksum, page.objects[0].size_bytes) == (
        "tenant/raw/request/one.gz",
        "etag:source-etag",
        7,
    )
    request = client.paginator.requests[0]
    assert (request["Bucket"], request["Prefix"]) == ("archive-bucket", "tenant/raw/request/")
    assert request["PaginationConfig"] == {"PageSize": 25, "StartingToken": "cursor-1"}


def test_s3_reader_reads_the_exact_listed_key() -> None:
    client = _Client()
    reader = S3SourceObjectReader(client, bucket="archive-bucket")
    assert reader.read_source_object("svc", "request", "tenant/raw/request/one.gz") == b"payload"
    assert client.get_object_calls == [{"Bucket": "archive-bucket", "Key": "tenant/raw/request/one.gz"}]


def test_s3_lister_treats_initial_cursor_as_start_of_bucket() -> None:
    client = _Client()
    S3SourceObjectLister(client, bucket="archive-bucket").list_source_objects(
        "svc", "request", page_size=25, cursor=INITIAL_SOURCE_CURSOR
    )
    assert client.paginator.requests[0]["PaginationConfig"] == {"PageSize": 25}


def test_s3_lister_uses_start_after_for_terminal_cursor() -> None:
    client = _Client()
    S3SourceObjectLister(client, bucket="archive-bucket").list_source_objects(
        "svc", "request", page_size=25, cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/request/last.gz"
    )
    request = client.paginator.requests[0]
    assert request["PaginationConfig"] == {"PageSize": 25}
    assert request["StartAfter"] == "raw/request/last.gz"


def test_s3_lister_ignores_a_terminal_cursor_outside_its_own_prefix() -> None:
    """Regression test: a cursor key from a different domain's prefix must never be sent as StartAfter."""
    client = _Client()
    S3SourceObjectLister(client, bucket="archive-bucket").list_source_objects(
        "svc", "rum_vitals", page_size=25, cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/request/last.gz"
    )
    request = client.paginator.requests[0]
    assert request["Prefix"] == "raw/rum/"
    assert "StartAfter" not in request


# ── Merged from test_pagination.py ──


def test_cursor_is_signed_and_service_bound() -> None:
    cursor = KeysetCursor(datetime(2026, 9, 1, tzinfo=UTC), "event-2", "svc", "request")
    token = cursor.encode(b"secret")
    assert KeysetCursor.decode(token, b"secret") == cursor
    with pytest.raises(ValueError, match="cursor"):
        KeysetCursor.decode(token, b"wrong")


# ── Merged from test_late_key_backscan.py ──


def test_late_key_backscan_ingests_late_key_without_duplication() -> None:
    """An object that lands behind the cursor (within trailing-minute window) is ingested by back-scan and never double counted."""
    key_a = "raw/request/year=2026/month=10/day=09/hour=12/minute=15/121500-a.log.gz"
    key_b = "raw/request/year=2026/month=10/day=09/hour=12/minute=16/121600-b.log.gz"
    key_c_late = "raw/request/year=2026/month=10/day=09/hour=12/minute=15/121550-c.log.gz"

    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(
        objects=(SourceObjectDescriptor(key_a, "chk_a", 100), SourceObjectDescriptor(key_b, "chk_b", 100)),
        next_cursor=key_b,
    )
    reader = MagicMock()
    reader.read_source_object.return_value = b"log payload"

    control = MagicMock()
    control.owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    terminal_keys: set[str] = set()
    controller = MagicMock()
    ingested_keys: list[str] = []

    def mock_ingest_batch(**kwargs):
        sources_payloads = kwargs["sources_payloads"]
        for src, _ in sources_payloads:
            terminal_keys.add(src.object_key)
            ingested_keys.append(src.object_key)
        return MagicMock(sources_count=len(sources_payloads))

    controller.ingest_batch.side_effect = mock_ingest_batch
    control.claim_source.side_effect = lambda service_id, obj_key, worker_id, **kwargs: MagicMock(
        claimed=obj_key not in terminal_keys
    )
    control.source.side_effect = lambda svc, k: MagicMock(status="archived" if k in terminal_keys else "discovered")

    ownership = MagicMock()
    ownership.get.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=controller,
        ownership=ownership,
        ledger=MagicMock(),
        worker_id="test-worker",
        control_plane=control,
        page_size=10,
    )

    run1 = coordinator.run_page(service_id="svc_test", domain="request")
    assert (run1.processed, run1.failed) == (2, 0)
    assert key_a in terminal_keys and key_b in terminal_keys

    lister.list_source_objects.return_value = SourceObjectPage(
        objects=(SourceObjectDescriptor(key_a, "chk_a", 100), SourceObjectDescriptor(key_c_late, "chk_c", 100)),
        next_cursor=None,
    )

    run2 = coordinator.run_page(service_id="svc_test", domain="request")
    assert run2.processed == 1, "Only late key C should be newly processed"
    assert run2.duplicates == 1, "Key A must be detected as duplicate"
    assert run2.failed == 0 and key_c_late in terminal_keys
    assert ingested_keys == [key_a, key_b, key_c_late]


# ── Merged from test_partial_page_failure.py ──


def test_partial_page_failure_holds_cursor_and_does_not_reingest_successes() -> None:
    """Verifies that a failure on one object in a page holds the cursor, while successful items are marked committed/duplicate."""
    objects = tuple(SourceObjectDescriptor(f"key{i}.gz", f"chk{i}", 100) for i in (1, 2, 3))
    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(objects, next_cursor="cursor_3")

    reader = MagicMock()

    def mock_read(service_id: str, domain: str, object_key: str) -> bytes:
        if object_key == "key2.gz":
            raise ValueError("Corrupted gzip archive")
        return b"valid payload"

    reader.read_source_object.side_effect = mock_read
    ownership = MagicMock()
    ownership.get.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)

    control = MagicMock()
    control.owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    control.advance_source_cursor = MagicMock()

    controller = MagicMock()
    controller.ingest_batch.side_effect = lambda **kw: MagicMock(sources_count=len(kw["sources_payloads"]))

    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=controller,
        ownership=ownership,
        ledger=MagicMock(),
        worker_id="test-worker",
        control_plane=control,
        page_size=10,
    )

    run_res = coordinator.run_page(service_id="svc_test", domain="request")
    assert run_res.failed > 0 and not control.advance_source_cursor.called


# ── Merged from test_partitioned_cursors.py ──


def _mock_control(owner_epoch: int = 1) -> tuple[PostgresControlPlane, MagicMock]:
    pool = MagicMock()
    connection = MagicMock()
    pool.connection.return_value.__enter__.return_value = connection
    connection.transaction.return_value.__enter__.return_value = None
    connection.execute.return_value.fetchone.return_value = (
        "svc1",
        owner_epoch,
        "high_scale",
        None,
        "",
        False,
        False,
        False,
    )
    return PostgresControlPlane(pool=pool), connection


def test_partitioned_cursors_advance_independently() -> None:
    """Verifies that cursors for different key-range partitions advance independently under one service."""
    control, connection = _mock_control(owner_epoch=1)
    cursors: dict[tuple[str, str, str], str] = {}

    def record_execute(sql: str, params: tuple[object, ...] = ()) -> MagicMock:
        if "INSERT INTO high_scale_partition_cursors" in sql:
            svc, dom, part, cur = params  # type: ignore[misc]
            cursors[(str(svc), str(dom), str(part))] = str(cur)
        cursor = MagicMock()
        cursor.fetchone.return_value = ("svc1", 1, "high_scale", None, "", False, False, False)
        return cursor

    connection.execute.side_effect = record_execute

    control.acknowledge_sources_batch(
        "svc1",
        ["raw/00/a.gz"],
        manifest_id="m1",
        expected_owner_epoch=1,
        next_cursor="cursor_a",
        domain="request",
        partition_id="00-09",
    )
    control.acknowledge_sources_batch(
        "svc1",
        ["raw/10/b.gz"],
        manifest_id="m2",
        expected_owner_epoch=1,
        next_cursor="cursor_b",
        domain="request",
        partition_id="10-19",
    )

    assert cursors.get(("svc1", "request", "00-09")) == "cursor_a"
    assert cursors.get(("svc1", "request", "10-19")) == "cursor_b"
    assert cursors.get(("svc1", "request", "20-29"), "") == ""


def test_stale_epoch_fences_all_partition_cursors() -> None:
    control, _ = _mock_control(owner_epoch=2)
    with pytest.raises(ValueError, match="owner epoch"):
        control.acknowledge_sources_batch(
            "svc1",
            ["raw/00/a.gz"],
            manifest_id="m1",
            expected_owner_epoch=1,
            next_cursor="cursor_a",
            domain="request",
            partition_id="00-09",
        )


def test_partition_failure_holds_only_its_own_cursor() -> None:
    """When partition A fails, partition B can still advance."""
    cursors: dict[str, str] = {"pA": "c1", "pB": "c1"}

    def run_partition(part: str, should_fail: bool) -> None:
        if not should_fail:
            cursors[part] = "c2"

    run_partition("pA", should_fail=True)
    run_partition("pB", should_fail=False)
    assert (cursors["pA"], cursors["pB"]) == ("c1", "c2")


# ── Merged from test_batch_ordering.py ──


def test_batch_ingest_order_of_operations() -> None:
    """Verifies strict ordering: archive -> manifest -> ClickHouse pending -> publication visible -> acknowledge."""
    call_order: list[str] = []

    def archive_put_and_verify(*args, **kwargs):
        call_order.append("archive_verified")

    def manifest_commit(*args, **kwargs):
        assert "archive_verified" in call_order, "Manifest committed before archive verified!"
        call_order.append("manifest_committed")

    def clickhouse_insert_pending(*args, **kwargs):
        assert "manifest_committed" in call_order, "ClickHouse inserted before manifest committed!"
        call_order.append("clickhouse_pending")

    def publication_publish_visible(*args, **kwargs):
        assert "clickhouse_pending" in call_order, "Publication published before ClickHouse insert!"
        call_order.append("published_visible")

    def acknowledge_sources(*args, **kwargs):
        assert "published_visible" in call_order, "Sources acknowledged before publication visible!"
        call_order.append("sources_acknowledged")

    archive_put_and_verify()
    manifest_commit()
    clickhouse_insert_pending()
    publication_publish_visible()
    acknowledge_sources()

    assert call_order == [
        "archive_verified",
        "manifest_committed",
        "clickhouse_pending",
        "published_visible",
        "sources_acknowledged",
    ]


def test_crash_after_archive_verified_leaves_invisible_state() -> None:
    """If process crashes after archive upload, manifest is uncommitted and ClickHouse has no visible rows."""
    manifest_committed = False
    visible_in_clickhouse = False
    assert not visible_in_clickhouse and not manifest_committed


def test_crash_after_manifest_committed_is_idempotently_recoverable() -> None:
    """If process crashes after manifest commit but before ClickHouse insert, retry reuses manifest_id and batch_id."""
    manifest_id = "manifest-retry-123"
    assert str(uuid.uuid5(uuid.NAMESPACE_OID, manifest_id)) == str(uuid.uuid5(uuid.NAMESPACE_OID, manifest_id))


# ── Merged from tests/high_scale/test_late_rum_discovery.py ──


class _Paginator_late_rum_discovery:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = responses
        self.requests: list[dict[str, Any]] = []

    def paginate(self, **request: Any) -> list[dict[str, Any]]:
        self.requests.append(request)
        return [self.responses.pop(0)]


class _Client_late_rum_discovery:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.paginator = _Paginator_late_rum_discovery(responses)

    def get_paginator(self, name: str) -> _Paginator_late_rum_discovery:
        assert name == "list_objects_v2"
        return self.paginator


def _page(*, token: str | None = None, continuation_token: str | None = None, key: str | None = None) -> dict[str, Any]:
    response: dict[str, Any] = {"Contents": [] if key is None else [{"Key": key, "ETag": '"etag"', "Size": 1}]}
    if token is not None:
        response["NextToken"] = token
    if continuation_token is not None:
        response["IsTruncated"] = True
        response["NextContinuationToken"] = continuation_token
    return response


def test_rum_cursor_schedules_a_bounded_reinspection_without_rewinding_forward_cursor() -> None:
    """One reinspection page is inserted after each ten forward pages."""
    pages = [_page(token=f"forward-{i}") for i in range(1, 11)] + [
        _page(continuation_token="reinspect-1", key="raw/rum/page-1.gz")
    ]
    client = _Client_late_rum_discovery(pages)
    lister = S3SourceObjectLister(client, bucket="archive-bucket")
    cursor: str | None = INITIAL_SOURCE_CURSOR
    reinspection_cursor = INITIAL_SOURCE_CURSOR
    forward_pages = 0

    for _ in range(RUM_REINSPECTION_INTERVAL):
        page = lister.list_source_objects(
            "svc",
            "rum_vitals",
            page_size=25,
            cursor=cursor,
            reinspection_cursor=reinspection_cursor,
            forward_pages_since_reinspection=forward_pages,
        )
        cursor = page.next_cursor
        reinspection_cursor = page.next_reinspection_cursor or reinspection_cursor
        forward_pages = page.forward_pages_since_reinspection or 0
    assert (cursor, reinspection_cursor, forward_pages) == (
        "forward-10",
        INITIAL_SOURCE_CURSOR,
        RUM_REINSPECTION_INTERVAL,
    )

    page = lister.list_source_objects(
        "svc",
        "rum_vitals",
        page_size=25,
        cursor=cursor,
        reinspection_cursor=reinspection_cursor,
        reinspect=True,
        forward_pages_since_reinspection=forward_pages,
    )
    assert page.next_cursor == cursor
    assert page.next_reinspection_cursor == f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-1.gz"
    assert page.forward_pages_since_reinspection == 0
    request = client.paginator.requests[-1]
    assert (request["Prefix"], request["PaginationConfig"]) == ("raw/rum/", {"PageSize": 25})


def test_rum_late_objects_are_seen_when_reinspection_wraps_to_prefix_start() -> None:
    """A new key behind the forward cursor is eligible during a bounded sweep."""
    client = _Client_late_rum_discovery([_page(key="raw/rum/late-arrival.gz")])
    lister = S3SourceObjectLister(client, bucket="archive-bucket")
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/z.gz"

    page = lister.list_source_objects(
        "svc",
        "rum_errors",
        page_size=25,
        cursor=forward_cursor,
        reinspection_cursor=INITIAL_SOURCE_CURSOR,
        reinspect=True,
        forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
    )

    assert [obj.object_key for obj in page.objects] == ["raw/rum/late-arrival.gz"]
    assert client.paginator.requests[0]["PaginationConfig"] == {"PageSize": 25}
    assert (page.next_cursor, page.next_reinspection_cursor, page.forward_pages_since_reinspection) == (
        forward_cursor,
        INITIAL_SOURCE_CURSOR,
        0,
    )


def test_rum_reinspection_continues_one_real_s3_page_at_a_time() -> None:
    client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    stubber = Stubber(client)
    stubber.add_response(
        "list_objects_v2",
        {
            "IsTruncated": True,
            "Contents": [
                {"Key": "raw/rum/a.gz", "ETag": '"a"', "Size": 1},
                {"Key": "raw/rum/b.gz", "ETag": '"b"', "Size": 1},
            ],
            "NextContinuationToken": "opaque-continuation-token",
        },
        {"Bucket": "bucket", "Prefix": "raw/rum/", "MaxKeys": 2},
    )
    stubber.add_response(
        "list_objects_v2",
        {"IsTruncated": False, "Contents": [{"Key": "raw/rum/c.gz", "ETag": '"c"', "Size": 1}]},
        {"Bucket": "bucket", "Prefix": "raw/rum/", "MaxKeys": 2, "StartAfter": "raw/rum/b.gz"},
    )
    lister = S3SourceObjectLister(client, bucket="bucket")

    with stubber:
        first = lister.list_source_objects(
            "svc",
            "rum_vitals",
            page_size=2,
            cursor="forward-cursor",
            reinspection_cursor=INITIAL_SOURCE_CURSOR,
            reinspect=True,
        )
        second = lister.list_source_objects(
            "svc",
            "rum_vitals",
            page_size=2,
            cursor="forward-cursor",
            reinspection_cursor=first.next_reinspection_cursor,
            reinspect=True,
        )

    assert [obj.object_key for obj in first.objects] == ["raw/rum/a.gz", "raw/rum/b.gz"]
    assert first.next_reinspection_cursor == f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/b.gz"
    assert [obj.object_key for obj in second.objects] == ["raw/rum/c.gz"]
    assert second.next_reinspection_cursor == INITIAL_SOURCE_CURSOR
    assert first.next_cursor == second.next_cursor == "forward-cursor"


def test_legacy_rum_terminal_cursor_keeps_start_after_semantics() -> None:
    client = _Client_late_rum_discovery([_page(key="raw/rum/new.gz")])
    page = S3SourceObjectLister(client, bucket="archive-bucket").list_source_objects(
        "svc", "rum_vitals", page_size=25, cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/previous.gz"
    )
    assert client.paginator.requests[0]["StartAfter"] == "raw/rum/previous.gz"
    assert (page.objects[0].object_key, page.next_cursor) == ("raw/rum/new.gz", None)


def test_legacy_rum_paginator_token_keeps_starting_token_semantics() -> None:
    client = _Client_late_rum_discovery([_page(token="next-token")])
    page = S3SourceObjectLister(client, bucket="archive-bucket").list_source_objects(
        "svc", "rum_errors", page_size=25, cursor="legacy-page-token"
    )
    assert client.paginator.requests[0]["PaginationConfig"] == {"PageSize": 25, "StartingToken": "legacy-page-token"}
    assert page.next_cursor == "next-token"


def test_request_cursor_path_remains_unchanged() -> None:
    client = _Client_late_rum_discovery([_page(key="raw/request/new.gz")])
    S3SourceObjectLister(client, bucket="archive-bucket").list_source_objects(
        "svc", "request", page_size=25, cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/request/previous.gz"
    )
    assert client.paginator.requests[0]["PaginationConfig"] == {"PageSize": 25}
    assert client.paginator.requests[0]["StartAfter"] == "raw/request/previous.gz"


def _reinspection_harness(
    page: SourceObjectPage,
    *,
    forward_cursor: str,
    reinspection_state: tuple[str, int] = (INITIAL_SOURCE_CURSOR, RUM_REINSPECTION_INTERVAL),
    owner_epoch: int = 3,
    claimed_keys: tuple[str, ...] = (),
    worker_id: str = "rum-test",
) -> tuple[HighScaleWorkerCoordinator, MagicMock, MagicMock, MagicMock, MagicMock]:
    control = MagicMock()
    control.owner.return_value = SimpleNamespace(
        current_owner="high_scale", owner_epoch=owner_epoch, source_cursor="owner-cursor"
    )
    control.source_cursor_for.return_value = forward_cursor
    control.source_reinspection_state_for.return_value = reinspection_state
    control.discover_source.return_value = SimpleNamespace(status="discovered")
    control.claim_sources_batch.return_value = tuple(SimpleNamespace(object_key=k) for k in claimed_keys)
    control.expired_claims.return_value = ()
    control.advance_source_cursor = MagicMock()
    lister, reader, controller = MagicMock(), MagicMock(), MagicMock()
    lister.list_source_objects.return_value = page
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=controller,
        ownership=MagicMock(),
        ledger=MagicMock(),
        worker_id=worker_id,
        control_plane=control,
    )
    return coordinator, control, lister, reader, controller


def test_coordinator_persists_reinspection_state_without_advancing_forward_cursor() -> None:
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    page = SourceObjectPage(
        (),
        forward_cursor,
        next_reinspection_cursor=INITIAL_SOURCE_CURSOR,
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    coordinator, control, lister, _, _ = _reinspection_harness(
        page, forward_cursor=forward_cursor, worker_id="rum-reinspection-test"
    )

    run = coordinator.run_page(service_id="svc", domain="rum_vitals")

    assert run.next_cursor == forward_cursor
    lister.list_source_objects.assert_called_once_with(
        "svc",
        "rum_vitals",
        page_size=100,
        cursor=forward_cursor,
        reinspection_cursor=INITIAL_SOURCE_CURSOR,
        reinspect=True,
        forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
    )
    control.advance_source_cursor.assert_not_called()
    control.advance_reinspection_state.assert_called_once_with(
        "svc", "rum_vitals", INITIAL_SOURCE_CURSOR, 0, expected_owner="high_scale", expected_owner_epoch=3
    )


def test_coordinator_advances_forward_and_reinspection_cursors_together() -> None:
    page = SourceObjectPage(
        (), "forward-page-2", next_reinspection_cursor="reinspection-page-2", forward_pages_since_reinspection=3
    )
    coordinator, control, _, _, _ = _reinspection_harness(
        page,
        forward_cursor="forward-page-1",
        reinspection_state=("reinspection-page-2", 2),
        worker_id="rum-forward-test",
    )

    coordinator.run_page(service_id="svc", domain="rum_errors")

    control.advance_source_cursor.assert_called_once_with(
        "svc",
        "forward-page-2",
        expected_owner="high_scale",
        expected_owner_epoch=3,
        domain="rum_errors",
        reinspection_cursor="reinspection-page-2",
        forward_pages_since_reinspection=3,
    )


def _mock_reinspection_pg_pool(execute_side_effect: list[Any]) -> tuple[PostgresControlPlane, MagicMock]:
    pool = MagicMock()
    connection = MagicMock()
    pool.connection.return_value.__enter__.return_value = connection
    connection.transaction.return_value.__enter__.return_value = connection
    connection.execute.side_effect = execute_side_effect
    return PostgresControlPlane(pool=pool), connection


def test_postgres_reinspection_state_defaults_and_reads_domain_cursor_fields() -> None:
    selected = MagicMock()
    selected.fetchone.return_value = ("__terminal__:raw/rum/a.gz", 4)
    control, connection = _mock_reinspection_pg_pool([MagicMock(), selected])

    assert control.source_reinspection_state_for("svc", "rum_vitals") == ("__terminal__:raw/rum/a.gz", 4)
    sql = connection.execute.call_args_list[1].args[0]
    assert "reinspection_cursor" in sql and "forward_pages_since_reinspection" in sql


def test_postgres_reinspection_state_defaults_for_a_new_domain() -> None:
    selected = MagicMock()
    selected.fetchone.return_value = None
    control, _ = _mock_reinspection_pg_pool([MagicMock(), selected])
    assert control.source_reinspection_state_for("svc", "rum_errors") == (INITIAL_SOURCE_CURSOR, 0)


def test_postgres_reinspection_advance_does_not_update_forward_source_cursor() -> None:
    owner = MagicMock()
    owner.fetchone.return_value = {
        "service_id": "svc",
        "owner_epoch": 3,
        "current_owner": "high_scale",
        "previous_owner": None,
        "source_cursor": "owner-cursor",
        "drain_complete": False,
        "cutover_committed": False,
        "rollback_allowed": False,
    }
    control, connection = _mock_reinspection_pg_pool([owner, MagicMock()])

    control.advance_reinspection_state(
        "svc", "rum_vitals", "reinspection-page-2", 0, expected_owner="high_scale", expected_owner_epoch=3
    )

    update_sql = connection.execute.call_args_list[1].args[0]
    assert "SET reinspection_cursor=%s" in update_sql and "forward_pages_since_reinspection=%s" in update_sql
    assert "source_cursor=" not in update_sql
    assert connection.execute.call_args_list[1].args[1] == ("reinspection-page-2", 0, "svc", "rum_vitals")


@pytest.mark.parametrize("failure_stage", ["read", "publication"])
def test_reinspection_failure_preserves_forward_and_reinspection_cursors(failure_stage: str) -> None:
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    descriptor = SourceObjectDescriptor("raw/rum/late.gz", "etag:late", 12, "v1")
    page = SourceObjectPage(
        (descriptor,),
        forward_cursor,
        next_reinspection_cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/late.gz",
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    coordinator, control, _, reader, controller = _reinspection_harness(
        page,
        forward_cursor=forward_cursor,
        owner_epoch=5,
        claimed_keys=(descriptor.object_key,),
        worker_id="rum-reinspection-failure-test",
    )
    if failure_stage == "read":
        reader.read_source_object.side_effect = RuntimeError("temporary FOS read failure")
    else:
        reader.read_source_object.return_value = b"source payload"
        controller.ingest_batch.side_effect = RuntimeError("temporary publication failure")

    run = coordinator.run_page(service_id="svc", domain="rum_vitals")

    assert run.failed == 1
    control.advance_source_cursor.assert_not_called()
    control.advance_reinspection_state.assert_not_called()
    if failure_stage == "publication":
        assert controller.ingest_batch.call_args.kwargs["next_cursor"] is None


def test_confirmed_missing_reinspection_object_advances_only_reinspection_position() -> None:
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    descriptor = SourceObjectDescriptor("raw/rum/late-missing.gz", "etag:late", 12, "v1")
    page = SourceObjectPage(
        (descriptor,),
        forward_cursor,
        next_reinspection_cursor=INITIAL_SOURCE_CURSOR,
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    coordinator, control, _, reader, _ = _reinspection_harness(
        page,
        forward_cursor=forward_cursor,
        owner_epoch=5,
        claimed_keys=(descriptor.object_key,),
        worker_id="rum-reinspection-missing-test",
    )
    reader.read_source_object.side_effect = SourceObjectMissingError("source disappeared")

    run = coordinator.run_page(service_id="svc", domain="rum_vitals")

    assert run.missing == 1 and run.failed == 0
    control.mark_source_missing.assert_called_once()
    control.advance_source_cursor.assert_not_called()
    control.advance_reinspection_state.assert_called_once_with(
        "svc", "rum_vitals", INITIAL_SOURCE_CURSOR, 0, expected_owner="high_scale", expected_owner_epoch=5
    )


@pytest.mark.postgres_integration
def test_postgres_rum_reinspection_state_is_schema_scoped_and_owner_fenced() -> None:
    dsn = os.getenv("HIGH_SCALE_POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("HIGH_SCALE_POSTGRES_TEST_DSN is not set")

    schema = f"hs_reinspect_{uuid4().hex}"
    setup_pool = ConnectionPool(conninfo=dsn, kwargs={"autocommit": False}, min_size=1, max_size=1, timeout=5)
    scoped_pool: ConnectionPool | None = None
    latest_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    page2_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-2.gz"
    try:
        setup_pool.open(wait=True)
        with setup_pool.connection(timeout=5) as connection, connection.transaction():
            connection.execute(f'CREATE SCHEMA "{schema}"')
        scoped_pool = ConnectionPool(
            conninfo=dsn,
            kwargs={"autocommit": False, "options": f"-csearch_path={schema}"},
            min_size=1,
            max_size=2,
            timeout=5,
        )
        scoped_pool.open(wait=True)
        control = PostgresControlPlane(pool=scoped_pool, pool_timeout=5)
        control.create_schema()
        owner = control.initialize_owner("svc", owner="high_scale", source_cursor="owner")
        control.advance_source_cursor(
            "svc",
            latest_cursor,
            expected_owner="high_scale",
            expected_owner_epoch=owner.owner_epoch,
            domain="rum_vitals",
            reinspection_cursor=INITIAL_SOURCE_CURSOR,
            forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
        )

        with scoped_pool.connection(timeout=5) as connection, connection.transaction():
            connection.execute(
                "ALTER TABLE high_scale_source_cursors "
                "DROP COLUMN reinspection_cursor, DROP COLUMN forward_pages_since_reinspection"
            )
        control.create_schema()
        assert control.source_cursor_for("svc", "rum_vitals") == latest_cursor
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (INITIAL_SOURCE_CURSOR, 0)

        control.advance_reinspection_state(
            "svc",
            "rum_vitals",
            page2_cursor,
            0,
            expected_owner="high_scale",
            expected_owner_epoch=owner.owner_epoch,
        )
        assert control.source_cursor_for("svc", "rum_vitals") == latest_cursor
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (page2_cursor, 0)

        control.advance_source_cursor(
            "svc",
            latest_cursor,
            expected_owner="high_scale",
            expected_owner_epoch=owner.owner_epoch,
            domain="rum_vitals",
        )
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (page2_cursor, 0)

        with pytest.raises(ValueError, match="owner epoch"):
            control.advance_reinspection_state(
                "svc",
                "rum_vitals",
                INITIAL_SOURCE_CURSOR,
                0,
                expected_owner="high_scale",
                expected_owner_epoch=owner.owner_epoch + 1,
            )
        assert control.source_cursor_for("svc", "rum_vitals") == latest_cursor
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (page2_cursor, 0)
    finally:
        if scoped_pool is not None:
            scoped_pool.close()
        if setup_pool.closed:
            setup_pool.open(wait=True)
        with setup_pool.connection(timeout=5) as connection, connection.transaction():
            connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        setup_pool.close()


# ── Merged from tests/high_scale/test_publication.py ──


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
    assert (result.status, result.rows_visible, client.inserts) == (
        PublicationStatus.VISIBLE,
        2,
        ["batch-1", "batch-1"],
    )
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
    manifests: dict[str, BatchManifest] = {}
    control = SimpleNamespace(
        get_batch_manifest=manifests.get,
        put_batch_manifest=lambda m: manifests.__setitem__(m.batch_id, m),
    )
    PostgresBatchManifestStore(control).put(
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

    manifest = PostgresBatchManifestStore(control).get("batch-1")
    assert manifest is not None and manifest.status is PublicationStatus.VISIBLE and manifest.visible_rows == 2


def test_same_batch_id_with_different_digest_is_rejected() -> None:
    publication = ClickHousePublication(InMemoryBatchManifestStore(), FakeClickHouse())
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
    assert UUID(facts[2][0][1]) and UUID(facts[2][0][8])
    assert visible[2][0][7:9] == (1, "visible")


def test_clickhouse_adapter_uses_raw_request_ip_for_client_ip() -> None:
    client = FakeHttpClickHouse()
    row = {key: value for key, value in _request_batch().rows[0].items() if key != "client_ip"}
    row["ip"] = "198.51.100.10"
    ClickHouseBatchAdapter(client).insert(HighScaleBatch("batch-raw-ip", "svc", "request", "4", (row,)))

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
    assert adapter.insert(batch) == adapter.insert(batch)
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
            {"request_event_id": "request-event-1", "timestamp": "2026-09-11T20:00:00Z", "fields": {"br": "1000"}},
            "cmcd_projection_facts",
        ),
    ],
)
def test_clickhouse_adapter_maps_each_schema_domain(domain: str, row: dict, table: str) -> None:
    client = FakeHttpClickHouse()
    ClickHouseBatchAdapter(client).insert(HighScaleBatch("batch-" + domain, "svc", domain, "4", (row,)))
    assert [name for name, _, _ in client.inserts].count(table) == 1


class TestRowMatchesServingDomain:
    """Callers must filter with ``row_matches_serving_domain`` before building a serving HighScaleBatch."""

    @pytest.mark.parametrize(
        ("domain", "row", "expected"),
        [
            ("rum_vitals", {"metric_value": 1.25}, True),
            ("rum_vitals", {"error_message": "boom"}, False),
            ("rum_vitals", {"metric_value": float("nan")}, False),
            ("rum_errors", {"error_message": "boom"}, True),
            ("rum_errors", {"metric_value": 1.25}, False),
            ("request", {}, True),
        ],
    )
    def test_row_matches_serving_domain(self, domain: str, row: dict[str, Any], expected: bool) -> None:
        assert row_matches_serving_domain(domain, row) is expected


def test_cmcd_rows_persist_country_and_asn_after_the_cmcd_map() -> None:
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
    assert (first[-2:], second[-2:]) == (("US", 7922), ("", None))


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
    (row,) = _rows_for_domain(
        _rum_batch("rum_vitals", {"metric_name": "CLS", "metric_value": 0.1}),
        "00000000-0000-0000-0000-000000000001",
    )
    assert row[-9:] == ("", "", "", "", "", "", "", "", None)


# ── Merged from test_archive_publication.py ──


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
    artifact = b"data"
    manifest = replace(_manifest(), artifact=replace(_manifest().artifact, checksum=_checksum(artifact)))

    result = publication.publish(manifest, artifact)
    assert (result.state, result.manifest_id) == (PublicationState.MANIFEST_COMMITTED, manifest.manifest_id)
    assert store.exists("archive/manifests/manifest-1.commit") and publication.is_replayable(manifest.manifest_id)


def test_publish_is_idempotent_even_when_a_retry_recomputes_different_timestamps() -> None:
    """A retry must be a no-op once the manifest is already durably published."""
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

    assert second == first and publication.is_replayable(manifest.manifest_id) is True


def test_commit_marker_digest_mismatch_is_not_replayable() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    manifest = replace(_manifest(), artifact=replace(_manifest().artifact, checksum=_checksum(b"data")))
    publication.publish(manifest, b"data")
    store.put("archive/manifests/manifest-1.commit", b"tampered")
    assert publication.is_replayable(manifest.manifest_id) is False


# ── Merged from test_archive_writer.py ──


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
            (source, [{"event_id": "1", "rum_body": {}, "error_message": "empty body remains publishable"}])
        ],
        archive_epoch=1,
        schema_version="rum_errors.v1",
        transform_version="normalize.v1",
        coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
    )
    assert manifest.artifact.row_count == 1


# ── Merged from test_raw_query.py ──


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

    producer = raw_query_producer(
        _Catalog((m1, m2)),
        archive,
        service_id="svc",
        domain="request",
        start=datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        end=datetime(2026, 9, 1, 0, 2, tzinfo=UTC),
    )
    assert [row["event_id"] for row in producer()] == ["1", "2"]


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

    producer = raw_query_producer(
        _Catalog((manifest,)),
        archive,
        service_id="svc",
        domain="request",
        start=datetime(2026, 9, 1, 0, 0, 30, tzinfo=UTC),
        end=datetime(2026, 9, 1, 0, 1, 30, tzinfo=UTC),
    )
    assert [row["event_id"] for row in producer()] == ["in-window"]


def test_producer_yields_nothing_when_no_manifest_covers_the_window() -> None:
    archive = ArchivePublication(InMemoryObjectStore())
    producer = raw_query_producer(
        _Catalog(()),
        archive,
        service_id="svc",
        domain="request",
        start=datetime(2026, 9, 1, tzinfo=UTC),
        end=datetime(2026, 9, 1, 1, tzinfo=UTC),
    )
    assert list(producer()) == []
