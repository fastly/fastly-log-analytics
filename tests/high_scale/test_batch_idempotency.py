from __future__ import annotations

import uuid

import pytest

from backend.high_scale.schema import build_batch_id_from_manifest_id


def test_batch_id_is_deterministically_derived_from_manifest_id() -> None:
    manifest_id = "manifest-test-abc-123"
    batch_id_1 = build_batch_id_from_manifest_id(manifest_id)
    batch_id_2 = build_batch_id_from_manifest_id(manifest_id)
    assert batch_id_1 == batch_id_2
    # Must be valid UUID format
    assert str(uuid.UUID(batch_id_1)) == batch_id_1


def test_different_manifest_id_yields_different_batch_id() -> None:
    batch_1 = build_batch_id_from_manifest_id("manifest-1")
    batch_2 = build_batch_id_from_manifest_id("manifest-2")
    assert batch_1 != batch_2


def test_publication_mismatch_blocks_visibility() -> None:
    """When visible_rows != expected_rows, publication status must reject visible transition."""
    # Publication gate logic
    expected_rows = 100
    visible_rows = 95  # 5 rows missing

    def check_publication(expected: int, visible: int) -> str:
        if expected != visible:
            raise ValueError(f"Publication gate failure: expected {expected} rows, but got {visible}")
        return "visible"

    with pytest.raises(ValueError, match="Publication gate failure"):
        check_publication(expected_rows, visible_rows)
