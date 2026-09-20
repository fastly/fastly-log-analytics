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


def test_source_cursor_for_never_falls_back_to_a_different_domains_cursor() -> None:
    """Regression test: different domains list different FOS prefixes
    (request vs. rum), so a cursor another domain advanced to is never a
    valid starting point for this one — it can reference a key entirely
    outside this domain's prefix. Observed live: the owner-level cursor
    (request domain's own value) leaked into rum_vitals/rum_errors as their
    fallback starting point, and the resulting StartAfter-outside-Prefix
    silently returned zero objects forever, permanently stalling RUM
    discovery with no error. A domain with no cursor of its own must start
    from its own beginning instead."""
    store = OwnershipStore()
    try:
        store.initialize("svc", owner="high_scale", source_cursor="__terminal__:raw/request/a.gz")
        store.advance_cursor(
            "svc", "__terminal__:raw/request/b.gz", expected_owner="high_scale", expected_epoch=1, domain="request"
        )

        # request has its own advanced cursor.
        assert store.source_cursor_for("svc", "request") == "__terminal__:raw/request/b.gz"
        # rum_vitals has never advanced — must get its own initial cursor,
        # not the owner-level (request-shaped) value.
        assert store.source_cursor_for("svc", "rum_vitals") == "__initial__"
    finally:
        store.close()
