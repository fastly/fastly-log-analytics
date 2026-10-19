import pytest

from backend.high_scale.ledger import ChecksumMismatch, HighScaleLedger
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.source_discovery import (
    DiscoveryResult,
    HighScaleSourceDiscovery,
    SourceObjectDescriptor,
    SourceObjectPage,
)


class _Lister:
    def __init__(self, pages: dict[str | None, SourceObjectPage]) -> None:
        self.pages = pages
        self.calls: list[tuple[str, str, int, str | None]] = []

    def list_source_objects(
        self,
        service_id: str,
        domain: str,
        *,
        page_size: int,
        cursor: str | None,
    ) -> SourceObjectPage:
        self.calls.append((service_id, domain, page_size, cursor))
        return self.pages[cursor]


@pytest.fixture
def ownership() -> OwnershipStore:
    store = OwnershipStore()
    store.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    yield store
    store.close()


@pytest.fixture
def ledger() -> HighScaleLedger:
    value = HighScaleLedger()
    yield value
    value.close()


def test_discover_once_bounds_listing_page_and_returns_cursor_progression(
    ownership: OwnershipStore,
    ledger: HighScaleLedger,
) -> None:
    lister = _Lister(
        {
            "cursor-0": SourceObjectPage(
                (
                    SourceObjectDescriptor("raw/request/a.gz", "sha256:a", 10, "v1"),
                    SourceObjectDescriptor("raw/request/b.gz", "sha256:b", 20, "v1"),
                ),
                "cursor-1",
            ),
            "cursor-1": SourceObjectPage(
                (SourceObjectDescriptor("raw/request/c.gz", "sha256:c", 30, "v2"),),
                None,
            ),
        }
    )
    discovery = HighScaleSourceDiscovery(
        ownership=ownership,
        ledger=ledger,
        lister=lister,
        page_size=2,
    )

    first = discovery.discover_once(service_id="svc", domain="request")
    second = discovery.discover_once(service_id="svc", domain="request", cursor=first.next_cursor)

    assert first == DiscoveryResult(next_cursor="cursor-1", discovered_count=2)
    assert second == DiscoveryResult(next_cursor=None, discovered_count=1)
    assert lister.calls == [
        ("svc", "request", 2, "cursor-0"),
        ("svc", "request", 2, "cursor-1"),
    ]
    assert ledger.count() == 3


@pytest.mark.parametrize(
    ("checksum", "version"),
    [("sha256:b", "v1"), ("sha256:a", "v2")],
)
def test_discover_once_rejects_checksum_or_version_changes(
    ownership: OwnershipStore,
    ledger: HighScaleLedger,
    checksum: str,
    version: str,
) -> None:
    lister = _Lister(
        {
            "cursor-0": SourceObjectPage(
                (SourceObjectDescriptor("raw/request/a.gz", "sha256:a", 10, "v1"),),
                None,
            )
        }
    )
    discovery = HighScaleSourceDiscovery(ownership=ownership, ledger=ledger, lister=lister)
    discovery.discover_once(service_id="svc", domain="request")

    lister.pages["cursor-0"] = SourceObjectPage(
        (SourceObjectDescriptor("raw/request/a.gz", checksum, 10, version),),
        None,
    )
    with pytest.raises(ChecksumMismatch):
        discovery.discover_once(service_id="svc", domain="request")


def test_discover_once_refuses_standard_ownership(
    ledger: HighScaleLedger,
) -> None:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="standard", source_cursor="cursor-0")
    lister = _Lister({"cursor-0": SourceObjectPage((), None)})
    discovery = HighScaleSourceDiscovery(ownership=ownership, ledger=ledger, lister=lister)

    try:
        with pytest.raises(RuntimeError, match="not the owner"):
            discovery.discover_once(service_id="svc", domain="request")
        assert lister.calls == []
        assert ledger.count() == 0
    finally:
        ownership.close()


def test_discover_once_enforces_expected_owner_epoch(
    ownership: OwnershipStore,
    ledger: HighScaleLedger,
) -> None:
    lister = _Lister({"cursor-0": SourceObjectPage((), None)})
    discovery = HighScaleSourceDiscovery(
        ownership=ownership,
        ledger=ledger,
        lister=lister,
        owner_epoch=2,
    )

    with pytest.raises(RuntimeError, match="owner epoch"):
        discovery.discover_once(service_id="svc", domain="request")
    assert lister.calls == []


def test_discover_once_keeps_repeated_objects_idempotent(
    ownership: OwnershipStore,
    ledger: HighScaleLedger,
) -> None:
    lister = _Lister(
        {
            "cursor-0": SourceObjectPage(
                (SourceObjectDescriptor("raw/request/a.gz", "sha256:a", 10, "v1"),),
                None,
            )
        }
    )
    discovery = HighScaleSourceDiscovery(ownership=ownership, ledger=ledger, lister=lister)

    first = discovery.discover_once(service_id="svc", domain="request")
    second = discovery.discover_once(service_id="svc", domain="request")

    assert first.discovered_count == second.discovered_count == 1
    assert ledger.count() == 1


def test_source_page_size_must_be_bounded(
    ownership: OwnershipStore,
    ledger: HighScaleLedger,
) -> None:
    lister = _Lister({"cursor-0": SourceObjectPage((), None)})

    with pytest.raises(ValueError, match="page_size"):
        HighScaleSourceDiscovery(
            ownership=ownership,
            ledger=ledger,
            lister=lister,
            page_size=501,
        )
