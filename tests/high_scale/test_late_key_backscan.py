from __future__ import annotations

from unittest.mock import MagicMock

from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.source_discovery import (
    SourceObjectDescriptor,
    SourceObjectPage,
)


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

    def mock_ingest(**kwargs):
        terminal_keys.add(kwargs["object_key"])
        return MagicMock()

    controller.ingest.side_effect = mock_ingest

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

    # Total controller ingest calls must be exactly 3 (A, B, C) - no double counting of A
    assert controller.ingest.call_count == 3
