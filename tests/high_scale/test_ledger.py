import pytest

from backend.high_scale.ledger import ArchiveNotVerified, ChecksumMismatch, HighScaleLedger


@pytest.fixture
def ledger():
    value = HighScaleLedger()
    yield value
    value.close()


def test_duplicate_discovery_is_idempotent(ledger: HighScaleLedger) -> None:
    first = ledger.discover("svc", "request", "raw/request/a.gz", "sha256:a")
    second = ledger.discover("svc", "request", "raw/request/a.gz", "sha256:a")
    assert first.object_id == second.object_id
    assert ledger.count() == 1


def test_changed_source_identity_is_rejected(ledger: HighScaleLedger) -> None:
    ledger.discover("svc", "request", "raw/request/a.gz", "sha256:a")
    with pytest.raises(ChecksumMismatch):
        ledger.discover("svc", "request", "raw/request/a.gz", "sha256:b")


def test_claim_recovery_increments_lease_generation(ledger: HighScaleLedger) -> None:
    ledger.discover("svc", "request", "raw/request/a.gz", "sha256:a")
    first = ledger.claim("svc", "raw/request/a.gz", "worker-a", lease_seconds=0)
    second = ledger.claim("svc", "raw/request/a.gz", "worker-b", lease_seconds=30)
    assert first.claimed is True
    assert second.claimed is True
    assert second.lease_generation == first.lease_generation + 1


def test_source_cannot_be_deleted_before_verified_archive(ledger: HighScaleLedger) -> None:
    ledger.discover("svc", "request", "raw/request/a.gz", "sha256:a")
    claim = ledger.claim("svc", "raw/request/a.gz", "worker")
    ledger.mark_appended("svc", "raw/request/a.gz", claim.lease_generation)
    with pytest.raises(ArchiveNotVerified):
        ledger.authorize_source_delete("svc", "raw/request/a.gz", "manifest-1", current_owner_epoch=1)


def test_archive_acknowledgement_and_delete_are_fenced(ledger: HighScaleLedger) -> None:
    ledger.discover("svc", "request", "raw/request/a.gz", "sha256:a")
    claim = ledger.claim("svc", "raw/request/a.gz", "worker")
    ledger.record_counts("svc", "raw/request/a.gz", accepted_rows=2, malformed_rows=0)
    ledger.mark_appended("svc", "raw/request/a.gz", claim.lease_generation)
    ledger.mark_archived("svc", "raw/request/a.gz", claim.lease_generation, "manifest-1", 3)
    ledger.acknowledge("svc", "raw/request/a.gz", "manifest-1")
    auth = ledger.authorize_source_delete("svc", "raw/request/a.gz", "manifest-1", current_owner_epoch=3)
    assert auth.archive_manifest_id == "manifest-1"
    with pytest.raises(ArchiveNotVerified):
        ledger.authorize_source_delete("svc", "raw/request/a.gz", "manifest-1", current_owner_epoch=4)


def test_archived_malformed_rows_do_not_block_acknowledgement(ledger: HighScaleLedger) -> None:
    ledger.discover("svc", "request", "raw/request/a.gz", "sha256:a")
    claim = ledger.claim("svc", "raw/request/a.gz", "worker")
    ledger.record_counts("svc", "raw/request/a.gz", accepted_rows=2, malformed_rows=1)
    ledger.mark_appended("svc", "raw/request/a.gz", claim.lease_generation)
    ledger.mark_archived("svc", "raw/request/a.gz", claim.lease_generation, "manifest-1", 3)

    ledger.acknowledge("svc", "raw/request/a.gz", "manifest-1")

    assert ledger.authorize_source_delete("svc", "raw/request/a.gz", "manifest-1", current_owner_epoch=3)
