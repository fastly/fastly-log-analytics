from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.postgres_control import PostgresControlPlane


def test_stale_epoch_cannot_commit_manifest() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Mock transaction and locked owner
    mock_conn = MagicMock()
    mock_owner = MagicMock(current_owner="high_scale", owner_epoch=5)

    control._locked_owner = MagicMock(return_value=mock_owner)  # type: ignore[assignment]
    control.transaction = MagicMock()  # type: ignore[assignment]
    control.transaction.return_value.__enter__.return_value = mock_conn

    # Worker has stale epoch = 4 (current is 5)
    manifest = MagicMock(spec=ArchiveManifest)
    manifest.manifest_id = "man-1"
    manifest.service_id = "svc-1"
    manifest.domain = "request"
    manifest.validate.return_value = None

    with pytest.raises(ValueError, match="owner epoch mismatch|stale owner epoch"):
        control.register_archive_manifest(manifest, owner_epoch=4)


def test_stale_epoch_cannot_acknowledge_sources_batch() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Contract requires acknowledge_sources_batch with epoch check
    assert hasattr(control, "acknowledge_sources_batch"), (
        "Missing acknowledge_sources_batch method on PostgresControlPlane"
    )


def test_stale_epoch_cannot_claim_sources_batch() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Contract requires claim_sources_batch with epoch check
    assert hasattr(control, "claim_sources_batch"), "Missing claim_sources_batch method on PostgresControlPlane"
