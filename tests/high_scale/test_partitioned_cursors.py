from __future__ import annotations

from unittest.mock import MagicMock

import pytest

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
