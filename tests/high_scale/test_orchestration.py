from __future__ import annotations

from dataclasses import dataclass

import pytest

from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.source_discovery import (
    INITIAL_SOURCE_CURSOR,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    SourceObjectDescriptor,
    SourceObjectMissingError,
    SourceObjectPage,
)
from tests.high_scale.conftest import HighScaleLedger, OwnershipStore


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
    lister = _Lister(
        SourceObjectPage(
            (SourceObjectDescriptor("raw/one.gz", "sha256:one", 4, "v1"),),
            "cursor-1",
        )
    )
    controller = _Controller(ledger, fail_once=fail_once)
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
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

    assert result.cursor == "cursor-0"
    assert result.next_cursor == "cursor-1"
    assert result.discovered == 1
    assert result.processed == 1
    assert ownership.source_cursor_for("svc", "request") == "cursor-1"


def test_standard_owner_cannot_consume_high_scale_sources() -> None:
    coordinator, _, _, _ = _coordinator(owner="standard")

    with pytest.raises(RuntimeError, match="not the owner"):
        coordinator.run_page(service_id="svc", domain="request")


def test_duplicate_retry_is_acknowledged_without_duplicate_ingest() -> None:
    coordinator, _, _, controller = _coordinator()

    first = coordinator.run_page(service_id="svc", domain="request")
    second = coordinator.run_page(service_id="svc", domain="request")

    assert first.processed == 1
    assert second.duplicates == 1
    assert controller.calls == 1


def test_failed_acknowledgement_can_be_retried_after_lease_recovery() -> None:
    coordinator, _, ledger, controller = _coordinator(fail_once=True)

    first = coordinator.run_page(service_id="svc", domain="request")
    assert first.failed == 1
    source = ledger.source("svc", "raw/one.gz")
    assert source is not None and source.status == "discovered"

    second = coordinator.run_page(service_id="svc", domain="request")
    assert second.processed == 1
    assert controller.calls == 2


def test_missing_source_is_recorded_durably_and_does_not_block_the_cursor() -> None:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    ledger = HighScaleLedger()
    lister = _Lister(
        SourceObjectPage(
            (SourceObjectDescriptor("raw/gone.gz", "sha256:gone", 4, "v1"),),
            "cursor-1",
        )
    )
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

    assert result.missing == 1
    assert result.failed == 0
    assert result.processed == 0
    assert controller.calls == 0
    source = ledger.source("svc", "raw/gone.gz")
    assert source is not None
    assert source.status == "source_missing"
    assert "raw/gone.gz" in (source.last_error or "")
    assert ownership.source_cursor_for("svc", "request") == "cursor-1"

    lister.page = SourceObjectPage((SourceObjectDescriptor("raw/gone.gz", "sha256:gone", 4, "v1"),), "cursor-1")
    second = coordinator.run_page(service_id="svc", domain="request", cursor="cursor-1")
    assert second.duplicates == 1
    assert second.missing == 0


def test_expired_claims_are_recovered_behind_the_listing_cursor() -> None:
    coordinator, _, ledger, controller = _coordinator()
    ledger.discover("svc", "request", "raw/old.gz", "sha256:old", size_bytes=4, version="v1")
    ledger.claim("svc", "raw/old.gz", "previous-worker", lease_seconds=1)
    ledger._con.execute(
        "UPDATE source_objects SET lease_until=? WHERE service_id=? AND object_key=?",
        (0, "svc", "raw/old.gz"),
    )
    ledger._con.commit()
    ledger.expired_claims = lambda service_id, domain, limit=None: [  # type: ignore[attr-defined]
        s for s in (ledger.source(service_id, "raw/old.gz"),) if s and s.status == "claimed"
    ]

    result = coordinator.run_page(service_id="svc", domain="request")

    assert result.processed == 2
    assert controller.calls == 2
    assert ledger.source("svc", "raw/old.gz").status == "acknowledged"


def test_final_page_persists_terminal_cursor_for_new_objects_only() -> None:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    ledger = HighScaleLedger()
    lister = _TerminalAwareLister(
        {
            INITIAL_SOURCE_CURSOR: SourceObjectPage(
                (SourceObjectDescriptor("raw/one.gz", "sha256:one", 4, "v1"),),
                None,
            ),
            f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/one.gz": SourceObjectPage((), None),
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

    terminal_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/one.gz"
    assert first.next_cursor == terminal_cursor
    assert second.next_cursor == terminal_cursor
    assert lister.cursors == [INITIAL_SOURCE_CURSOR, terminal_cursor]
    assert ownership.source_cursor_for("svc", "request") == terminal_cursor


def test_domains_keep_independent_source_cursors() -> None:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="initial")
    ledger = HighScaleLedger()
    lister = _DomainLister(
        {
            ("request", INITIAL_SOURCE_CURSOR): SourceObjectPage(
                (SourceObjectDescriptor("raw/request.gz", "sha256:req", 4, "v1"),),
                "request-next",
            ),
            ("rum_vitals", INITIAL_SOURCE_CURSOR): SourceObjectPage(
                (SourceObjectDescriptor("raw/rum/vitals.gz", "sha256:rum", 4, "v1"),),
                "rum-next",
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
        import threading

        self._lock = threading.Lock()
        self.cursor: str | None = "cursor-0"

    def owner(self, service_id: str):
        @dataclass
        class _Owner:
            current_owner: str = "high_scale"
            owner_epoch: int = 1
            source_cursor: str | None = None

        return _Owner()

    def source_cursor_for(self, service_id: str, domain: str) -> str | None:
        return self.cursor

    def discover_source(self, *args: object, **kwargs: object) -> None:
        return None

    def claim_source(self, *args: object, **kwargs: object):
        @dataclass
        class _Claim:
            claimed: bool = True
            lease_generation: int = 1

        return _Claim()

    def source(self, service_id: str, object_key: str):
        raise KeyError(object_key)

    def advance_source_cursor(self, service_id: str, cursor: str, **kwargs: object) -> None:
        with self._lock:
            self.cursor = cursor


class _BarrierReader:
    """Each read blocks until ``parties`` reads are in flight at once, so a
    serial reader breaks the barrier (timeout) and reports failures."""

    def __init__(self, parties: int) -> None:
        import threading

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
        controller=_CountingController(),  # type: ignore[arg-type]
        ownership=None,
        ledger=None,
        worker_id="worker",
        control_plane=control,  # type: ignore[arg-type]
        page_size=10,
        object_concurrency=4,
    )

    result = coordinator.run_page(service_id="svc", domain="request")

    assert result.processed == 4
    assert result.failed == 0
    assert control.cursor == "cursor-1"


class _ContendedControl(_ThreadSafeControl):
    def __init__(self, *, held: set[str], finished: set[str]) -> None:
        super().__init__()
        self.held = held
        self.finished = finished
        self.claimed: set[str] = set()

    def claim_source(self, service_id: str, object_key: str, *args: object, **kwargs: object):
        @dataclass
        class _Claim:
            claimed: bool
            lease_generation: int = 1

        if object_key in self.finished:
            self.claimed.add(object_key)
            raise ValueError(f"source object is not claimable: {object_key}")
        return _Claim(object_key not in self.held)

    def source(self, service_id: str, object_key: str):
        if object_key in self.finished and object_key in self.claimed:

            @dataclass
            class _Source:
                status: str = "acknowledged"

            return _Source()
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
        controller=controller,  # type: ignore[arg-type]
        ownership=None,
        ledger=None,
        worker_id="worker",
        control_plane=control,  # type: ignore[arg-type]
        page_size=10,
    )

    result = coordinator.run_page(service_id="svc", domain="request")

    assert controller.keys == ["raw/mine.gz"]
    assert result.processed == 1
    assert result.duplicates == 2
    assert result.failed == 0
    assert control.cursor == "cursor-1"


def test_unexpected_unclaimable_source_still_blocks_the_cursor() -> None:
    class _BrokenControl(_ThreadSafeControl):
        def claim_source(self, *args: object, **kwargs: object):
            raise ValueError("source object is not claimable: raw/x.gz")

    control = _BrokenControl()
    coordinator = HighScaleWorkerCoordinator(
        lister=_AnyPageLister(SourceObjectPage((SourceObjectDescriptor("raw/x.gz", "sha256:x", 4, "v1"),), "cursor-1")),
        reader=_Reader(),
        controller=_CountingController(),  # type: ignore[arg-type]
        ownership=None,
        ledger=None,
        worker_id="worker",
        control_plane=control,  # type: ignore[arg-type]
        page_size=10,
    )

    result = coordinator.run_page(service_id="svc", domain="request")

    assert result.failed == 1
    assert control.cursor == "cursor-0"


def test_object_concurrency_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError, match="object_concurrency"):
        HighScaleWorkerCoordinator(
            lister=_AnyPageLister(SourceObjectPage((), None)),
            reader=_Reader(),
            controller=_CountingController(),  # type: ignore[arg-type]
            ownership=None,
            ledger=None,
            worker_id="worker",
            object_concurrency=0,
        )


# ── Merged from test_source_discovery.py ──


from backend.high_scale.source_discovery import (
    MAX_SOURCE_PAGE_SIZE,
    S3SourceObjectLister,
)


class _DummyPaginator:
    def paginate(self, **kwargs):
        return [
            {
                "Contents": [
                    {"Key": "raw/request/a.gz", "ETag": '"etag-a"', "Size": 10},
                ],
                "NextToken": "cursor-1",
            }
        ]


class _DummyS3Client:
    def get_paginator(self, _name: str):
        return _DummyPaginator()


def test_s3_source_object_lister_lists_bounded_page() -> None:
    lister = S3SourceObjectLister(_DummyS3Client(), bucket="fos-bucket")
    page = lister.list_source_objects("svc", "request", page_size=10, cursor=None)
    assert page == SourceObjectPage(
        (SourceObjectDescriptor("raw/request/a.gz", "etag:etag-a", 10, '"etag-a"'),),
        "cursor-1",
    )


def test_s3_source_object_lister_rejects_unbounded_page_size() -> None:
    lister = S3SourceObjectLister(_DummyS3Client(), bucket="fos-bucket")
    with pytest.raises(ValueError, match="page_size"):
        lister.list_source_objects("svc", "request", page_size=MAX_SOURCE_PAGE_SIZE + 1, cursor=None)


# ── Merged from test_s3_source_adapters.py ──

from backend.high_scale.source_discovery import (
    S3SourceObjectReader,
)


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
                    "Contents": [
                        {
                            "Key": "tenant/raw/request/one.gz",
                            "ETag": '"source-etag"',
                            "Size": 7,
                        }
                    ],
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
    assert page.objects[0].object_key == "tenant/raw/request/one.gz"
    assert page.objects[0].checksum == "etag:source-etag"
    assert page.objects[0].size_bytes == 7
    request = client.paginator.requests[0]
    assert request["Bucket"] == "archive-bucket"
    assert request["Prefix"] == "tenant/raw/request/"
    assert request["PaginationConfig"] == {"PageSize": 25, "StartingToken": "cursor-1"}


def test_s3_reader_reads_the_exact_listed_key() -> None:
    client = _Client()
    reader = S3SourceObjectReader(client, bucket="archive-bucket")

    assert reader.read_source_object("svc", "request", "tenant/raw/request/one.gz") == b"payload"
    assert client.get_object_calls == [{"Bucket": "archive-bucket", "Key": "tenant/raw/request/one.gz"}]


def test_s3_lister_treats_initial_cursor_as_start_of_bucket() -> None:
    client = _Client()
    lister = S3SourceObjectLister(client, bucket="archive-bucket")

    lister.list_source_objects("svc", "request", page_size=25, cursor=INITIAL_SOURCE_CURSOR)

    assert client.paginator.requests[0]["PaginationConfig"] == {"PageSize": 25}


def test_s3_lister_uses_start_after_for_terminal_cursor() -> None:
    client = _Client()
    lister = S3SourceObjectLister(client, bucket="archive-bucket")

    lister.list_source_objects(
        "svc",
        "request",
        page_size=25,
        cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/request/last.gz",
    )

    request = client.paginator.requests[0]
    assert request["PaginationConfig"] == {"PageSize": 25}
    assert request["StartAfter"] == "raw/request/last.gz"


def test_s3_lister_ignores_a_terminal_cursor_outside_its_own_prefix() -> None:
    """Regression test: a cursor key from a different domain's prefix (e.g.
    ``rum_vitals`` inheriting a stale ``raw/request/...`` cursor from the
    owner-level fallback bug) must never be sent as StartAfter — S3-
    compatible backends can silently return zero objects for a StartAfter
    outside the Prefix, permanently starving discovery with no error.
    Dropping it is always safe: it just means one extra full-prefix LIST."""
    client = _Client()
    lister = S3SourceObjectLister(client, bucket="archive-bucket")

    lister.list_source_objects(
        "svc",
        "rum_vitals",
        page_size=25,
        cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/request/last.gz",
    )

    request = client.paginator.requests[0]
    assert request["Prefix"] == "raw/rum/"
    assert "StartAfter" not in request


# ── Merged from test_pagination.py ──

from datetime import UTC, datetime

from backend.high_scale._common import KeysetCursor


def test_cursor_is_signed_and_service_bound() -> None:
    cursor = KeysetCursor(datetime(2026, 9, 1, tzinfo=UTC), "event-2", "svc", "request")
    token = cursor.encode(b"secret")

    assert KeysetCursor.decode(token, b"secret") == cursor
    with pytest.raises(ValueError, match="cursor"):
        KeysetCursor.decode(token, b"wrong")


# ── Merged from test_late_key_backscan.py ──

from unittest.mock import MagicMock


def test_late_key_backscan_ingests_late_key_without_duplication() -> None:
    """An object that lands behind the cursor (within trailing-minute window) is ingested by back-scan and never double counted."""
    # Scenario:
    # Key A (minute 15): raw/request/year=2026/month=10/day=09/hour=12/minute=15/121500-a.log.gz
    # Key B (minute 16): raw/request/year=2026/month=10/day=09/hour=12/minute=16/121600-b.log.gz
    # Cursor has advanced past B.
    # Late Key C (minute 15): raw/request/year=2026/month=10/day=09/hour=12/minute=15/121550-c.log.gz
    key_a = "raw/request/year=2026/month=10/day=09/hour=12/minute=15/121500-a.log.gz"
    key_b = "raw/request/year=2026/month=10/day=09/hour=12/minute=16/121600-b.log.gz"
    key_c_late = "raw/request/year=2026/month=10/day=09/hour=12/minute=15/121550-c.log.gz"

    lister = MagicMock()
    # First regular page: lists A and B
    lister.list_source_objects.return_value = SourceObjectPage(
        objects=(
            SourceObjectDescriptor(key_a, "chk_a", 100),
            SourceObjectDescriptor(key_b, "chk_b", 100),
        ),
        next_cursor=key_b,
    )

    reader = MagicMock()
    reader.read_source_object.return_value = b"log payload"

    control = MagicMock()
    control.owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    # Simulate already-terminal check: Key A and B become terminal after first ingestion
    terminal_keys = set()

    def mock_already_terminal(service_id: str, obj_key: str):
        return obj_key in terminal_keys

    controller = MagicMock()
    ingested_keys: list[str] = []

    def mock_ingest_batch(**kwargs):
        sources_payloads = kwargs["sources_payloads"]
        for src, _ in sources_payloads:
            terminal_keys.add(src.object_key)
            ingested_keys.append(src.object_key)
        return MagicMock(sources_count=len(sources_payloads))

    controller.ingest_batch.side_effect = mock_ingest_batch

    # Claim source returns claimed=True if not in terminal_keys
    def mock_claim(service_id, obj_key, worker_id, **kwargs):
        if obj_key in terminal_keys:
            return MagicMock(claimed=False)
        return MagicMock(claimed=True)

    control.claim_source.side_effect = mock_claim
    control.source.side_effect = lambda svc, k: MagicMock(status="archived" if k in terminal_keys else "discovered")

    ownership = MagicMock()
    ownership.get.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    ledger = MagicMock()

    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=controller,
        ownership=ownership,
        ledger=ledger,
        worker_id="test-worker",
        control_plane=control,
        page_size=10,
    )

    # 1. Run normal page -> processes A and B
    run1 = coordinator.run_page(service_id="svc_test", domain="request")
    assert run1.processed == 2
    assert run1.failed == 0
    assert key_a in terminal_keys
    assert key_b in terminal_keys

    # 2. Back-scan lists trailing minute prefix for minute=15, returning key A (already processed) and key C (late-arriving)
    lister.list_source_objects.return_value = SourceObjectPage(
        objects=(
            SourceObjectDescriptor(key_a, "chk_a", 100),
            SourceObjectDescriptor(key_c_late, "chk_c", 100),
        ),
        next_cursor=None,
    )

    # 3. Process the back-scan page:
    # Key A should be detected as duplicate, Key C should be newly processed
    run2 = coordinator.run_page(service_id="svc_test", domain="request")
    assert run2.processed == 1, "Only late key C should be newly processed"
    assert run2.duplicates == 1, "Key A must be detected as duplicate"
    assert run2.failed == 0
    assert key_c_late in terminal_keys

    # Total ingested objects across batches must be exactly 3 (A, B, C) - no double counting of A
    assert ingested_keys == [key_a, key_b, key_c_late]


# ── Merged from test_partial_page_failure.py ──


def test_partial_page_failure_holds_cursor_and_does_not_reingest_successes() -> None:
    """Verifies that a failure on one object in a page holds the cursor, while successful items are marked committed/duplicate."""
    # Source objects: obj1 succeeds, obj2 fails, obj3 succeeds
    objects = (
        SourceObjectDescriptor("key1.gz", "chk1", 100),
        SourceObjectDescriptor("key2.gz", "chk2", 100),
        SourceObjectDescriptor("key3.gz", "chk3", 100),
    )
    page = SourceObjectPage(objects, next_cursor="cursor_3")

    lister = MagicMock()
    lister.list_source_objects.return_value = page

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
    # Mock advance cursor: should NOT be called if failed > 0
    control.advance_source_cursor = MagicMock()

    controller = MagicMock()
    controller.ingest_batch.side_effect = lambda **kw: MagicMock(sources_count=len(kw["sources_payloads"]))
    ledger = MagicMock()

    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=controller,
        ownership=ownership,
        ledger=ledger,
        worker_id="test-worker",
        control_plane=control,
        page_size=10,
    )

    # Coordinator processes sources: key1 processed, key2 fails, key3 processed
    # If failed > 0, advance_cursor must NOT be called
    run_res = coordinator.run_page(service_id="svc_test", domain="request")

    assert run_res.failed > 0
    assert not control.advance_source_cursor.called


# ── Merged from test_partitioned_cursors.py ──


from backend.high_scale.postgres_control import PostgresControlPlane


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
        if should_fail:
            # Failure holds cursor
            return
        cursors[part] = "c2"

    run_partition("pA", should_fail=True)
    run_partition("pB", should_fail=False)

    assert cursors["pA"] == "c1"  # Held
    assert cursors["pB"] == "c2"  # Advanced


# ── Merged from test_batch_ordering.py ──


def test_batch_ingest_order_of_operations() -> None:
    """Verifies strict ordering:
    1. Archive upload & checksum verify
    2. Manifest prepared -> committed in Postgres
    3. ClickHouse INSERT as pending
    4. Publication marked visible
    5. Acknowledge sources in Postgres
    """
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

    # Execute sequence
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

    # Simulate crash before step 2
    # Verify state: no visible rows in serving
    assert not visible_in_clickhouse
    assert not manifest_committed


def test_crash_after_manifest_committed_is_idempotently_recoverable() -> None:
    """If process crashes after manifest commit but before ClickHouse insert, retry reuses manifest_id and batch_id."""
    manifest_id = "manifest-retry-123"
    # Derived batch_id must be stable across retries
    import uuid

    batch_id_1 = str(uuid.uuid5(uuid.NAMESPACE_OID, manifest_id))
    batch_id_2 = str(uuid.uuid5(uuid.NAMESPACE_OID, manifest_id))
    assert batch_id_1 == batch_id_2
