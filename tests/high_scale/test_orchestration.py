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
