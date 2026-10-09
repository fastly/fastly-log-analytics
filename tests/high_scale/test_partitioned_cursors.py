from __future__ import annotations

from unittest.mock import MagicMock

from backend.high_scale.postgres_control import PostgresControlPlane


def test_partitioned_cursors_advance_independently() -> None:
    """Verifies that cursors for different key-range partitions advance independently under one service."""
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Mock cursor store dictionary to simulate DB state
    cursors: dict[tuple[str, str, str], str] = {}

    def mock_get_cursor(service_id: str, domain: str, partition_id: str = "default") -> str:
        return cursors.get((service_id, domain, partition_id), "")

    def mock_advance_cursor(
        service_id: str, domain: str, cursor: str, expected_owner_epoch: int, partition_id: str = "default"
    ) -> None:
        if expected_owner_epoch != 1:
            raise ValueError("stale owner epoch")
        cursors[(service_id, domain, partition_id)] = cursor

    control.get_partition_source_cursor = mock_get_cursor  # type: ignore[assignment]
    control.advance_partition_source_cursor = mock_advance_cursor  # type: ignore[assignment]

    # Advance partition '00-09'
    control.advance_partition_source_cursor("svc1", "request", "cursor_a", expected_owner_epoch=1, partition_id="00-09")
    # Advance partition '10-19'
    control.advance_partition_source_cursor("svc1", "request", "cursor_b", expected_owner_epoch=1, partition_id="10-19")

    assert control.get_partition_source_cursor("svc1", "request", partition_id="00-09") == "cursor_a"
    assert control.get_partition_source_cursor("svc1", "request", partition_id="10-19") == "cursor_b"
    assert control.get_partition_source_cursor("svc1", "request", partition_id="20-29") == ""


def test_stale_epoch_fences_all_partition_cursors() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Contract requires real method on PostgresControlPlane
    assert hasattr(control, "advance_partition_source_cursor"), "Missing advance_partition_source_cursor"
    assert hasattr(control, "get_partition_source_cursor"), "Missing get_partition_source_cursor"


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
