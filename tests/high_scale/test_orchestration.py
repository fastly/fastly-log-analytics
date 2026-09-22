from __future__ import annotations

from dataclasses import dataclass

import pytest

from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.source_discovery import (
    INITIAL_SOURCE_CURSOR,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    SourceObjectDescriptor,
    SourceObjectMissingError,
    SourceObjectPage,
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
    """Raises ``SourceObjectMissingError`` for one key, as if the raw object
    had already been deleted from storage (e.g. by a legacy raw-deletion
    path racing high-scale ownership)."""

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

    def ingest(self, **kwargs: object) -> None:
        self.calls += 1
        key = str(kwargs["object_key"])
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("lost acknowledgement")
        claim = self.ledger.claim("svc", key, "worker")
        self.ledger.mark_appended("svc", key, claim.lease_generation)
        self.ledger.mark_archived("svc", key, claim.lease_generation, "manifest-1", 1)
        self.ledger.acknowledge("svc", key, "manifest-1")


def _coordinator(*, owner: str = "high_scale", fail_once: bool = False):
    ownership = OwnershipStore()
    ownership.initialize("svc", owner=owner, source_cursor="cursor-0")
    # source_cursor_for no longer falls back to the owner-level cursor for a
    # domain that hasn't advanced yet (see test_ownership.py for why) — seed
    # the "request" domain's own cursor explicitly instead of relying on
    # that removed fallback.
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
    """The regression this exists for: a claimed source whose raw object was
    already deleted (e.g. by legacy cleanup racing high-scale ownership)
    must not be silently skipped, and must not retry forever blocking every
    later object in the stream behind it. It gets recorded as a durable
    ``source_missing`` failure and the cursor still advances."""
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
    # Cursor advances despite the missing object — the data is unrecoverable
    # regardless of retries, so blocking discovery behind it is strictly worse.
    assert ownership.source_cursor_for("svc", "request") == "cursor-1"

    # A later page must treat it as terminal (skipped as a duplicate), not
    # re-attempt it forever.
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
    # The "request" domain has no per-domain cursor yet, so it resolves to
    # its own initial cursor — never the owner-level "cursor-0" seed above
    # (see test_ownership.py for why that fallback was removed).
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
    # Neither domain has its own cursor yet, so both resolve to the shared
    # INITIAL_SOURCE_CURSOR constant — never the owner-level "initial" seed
    # above (see test_ownership.py for why that fallback was removed). The
    # point of this test is that they then advance independently.
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
