from __future__ import annotations

from dataclasses import dataclass

import pytest

from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.source_discovery import SourceObjectDescriptor, SourceObjectPage


@dataclass
class _Lister:
    page: SourceObjectPage

    def list_source_objects(self, service_id: str, domain: str, *, page_size: int, cursor: str | None):
        assert page_size == 2
        return self.page


class _Reader:
    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes:
        return b"payload"


class _Controller:
    def __init__(self, ledger: HighScaleLedger, *, fail_once: bool = False) -> None:
        self.ledger = ledger
        self.fail_once = fail_once
        self.calls = 0

    def ingest(self, **kwargs: object) -> None:
        self.calls += 1
        key = str(kwargs["object_key"])
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("lost acknowledgement")
        claim = self.ledger.claim(key, "worker")
        self.ledger.mark_appended(key, claim.lease_generation)
        self.ledger.mark_archived(key, claim.lease_generation, "manifest-1", 1)
        self.ledger.acknowledge(key, "manifest-1")


def _coordinator(*, owner: str = "high_scale", fail_once: bool = False):
    ownership = OwnershipStore()
    ownership.initialize("svc", owner=owner, source_cursor="cursor-0")
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
    assert ownership.get("svc").source_cursor == "cursor-1"


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
    source = ledger.source("raw/one.gz")
    assert source is not None and source.status == "discovered"

    second = coordinator.run_page(service_id="svc", domain="request")
    assert second.processed == 1
    assert controller.calls == 2
