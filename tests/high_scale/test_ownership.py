import pytest

from backend.high_scale.ownership import OwnershipStore


def test_cutover_is_atomic_and_epoch_fenced() -> None:
    store = OwnershipStore()
    try:
        store.initialize("svc", owner="standard", source_cursor="cursor-1")
        draining = store.begin_drain("svc", expected_owner="standard")
        assert draining.drain_complete is False
        store.mark_drained("svc", expected_epoch=draining.owner_epoch, source_cursor="cursor-9")
        cutover = store.commit_cutover("svc", expected_epoch=draining.owner_epoch, next_owner="high_scale")

        assert cutover.current_owner == "high_scale"
        assert cutover.cutover_committed is True
        with pytest.raises(ValueError, match="epoch"):
            store.commit_cutover("svc", expected_epoch=draining.owner_epoch, next_owner="other")
    finally:
        store.close()


def test_rollback_returns_to_previous_owner_only_after_cutover() -> None:
    store = OwnershipStore()
    try:
        store.initialize("svc", owner="standard", source_cursor="cursor-1")
        draining = store.begin_drain("svc", expected_owner="standard")
        store.mark_drained("svc", expected_epoch=draining.owner_epoch, source_cursor="cursor-2")
        cutover = store.commit_cutover("svc", expected_epoch=draining.owner_epoch, next_owner="high_scale")

        rolled_back = store.rollback("svc", expected_epoch=cutover.owner_epoch)

        assert rolled_back.current_owner == "standard"
        assert rolled_back.rollback_allowed is False
        assert rolled_back.cutover_committed is False
    finally:
        store.close()
