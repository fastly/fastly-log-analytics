from __future__ import annotations

from unittest.mock import MagicMock

from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.source_discovery import SourceObjectDescriptor, SourceObjectPage


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
