from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from psycopg_pool import ConnectionPool

from backend.core.high_scale_contracts import ArchiveState
from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.source_discovery import INITIAL_SOURCE_CURSOR


def _fake_store() -> tuple[PostgresControlPlane, MagicMock, MagicMock]:
    pool = MagicMock()
    connection = MagicMock()
    pool.connection.return_value.__enter__.return_value = connection
    connection.transaction.return_value.__enter__.return_value = connection
    return PostgresControlPlane(pool=pool), pool, connection


def _owner_row(
    *,
    service_id: str = "svc",
    epoch: int = 1,
    owner: str = "standard",
    previous: str | None = None,
    cursor: str = "cursor-1",
    drained: bool = False,
    committed: bool = False,
    rollback_allowed: bool = False,
) -> dict[str, object]:
    return {
        "service_id": service_id,
        "owner_epoch": epoch,
        "current_owner": owner,
        "previous_owner": previous,
        "source_cursor": cursor,
        "drain_complete": drained,
        "cutover_committed": committed,
        "rollback_allowed": rollback_allowed,
    }


def _source_row(
    *,
    object_id: str = "object-1",
    service_id: str = "svc",
    object_key: str = "raw/one.gz",
    status: str = "discovered",
    lease_until: datetime | None = None,
    generation: int = 0,
    owner_epoch: int | None = None,
    last_error: str | None = None,
) -> dict[str, object]:
    return {
        "object_id": object_id,
        "service_id": service_id,
        "domain": "request",
        "object_key": object_key,
        "checksum": "sha256:source",
        "size_bytes": 12,
        "version": "v1",
        "status": status,
        "owner": None,
        "lease_until": lease_until,
        "lease_generation": generation,
        "archive_manifest_id": None,
        "owner_epoch": owner_epoch,
        "malformed_rows": 0,
        "accepted_rows": 3,
        "last_error": last_error,
    }


def _archive_row(
    *,
    manifest_id: str = "manifest-1",
    service_id: str = "svc",
    state: str = "manifest_prepared",
    owner_epoch: int = 1,
) -> dict[str, object]:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    return {
        "manifest_id": manifest_id,
        "service_id": service_id,
        "domain": "request",
        "source_key": "raw/one.gz",
        "source_checksum": "sha256:source",
        "source_size": 12,
        "source_version": "v1",
        "artifact_uri": "s3://archive/manifest-1.parquet",
        "artifact_checksum": "sha256:artifact",
        "artifact_size": 128,
        "row_count": 3,
        "byte_count": 256,
        "canonical_digest": "digest",
        "schema_version": "archive.v1",
        "transform_version": "normalize.v1",
        "coverage_start": start,
        "coverage_end": start + timedelta(minutes=1),
        "retention_deadline": start + timedelta(days=1),
        "deletion_authorization_deadline": start + timedelta(days=2),
        "archive_epoch": 1,
        "owner_epoch": owner_epoch,
        "state": state,
    }


def test_schema_is_explicit_and_transactional() -> None:
    store, pool, connection = _fake_store()

    store.create_schema()

    pool.connection.assert_called_once_with()
    connection.transaction.assert_called_once_with()
    ddl = connection.execute.call_args.args[0]
    assert "CREATE TABLE IF NOT EXISTS high_scale_ownership" in ddl
    assert "CREATE TABLE IF NOT EXISTS high_scale_source_objects" in ddl
    assert "CREATE TABLE IF NOT EXISTS high_scale_archive_manifests" in ddl
    assert "CREATE TABLE IF NOT EXISTS high_scale_batch_claims" in ddl
    assert "CREATE TABLE IF NOT EXISTS high_scale_deletion_authorizations" in ddl


def test_operational_snapshot_aggregates_source_and_publication_state() -> None:
    store, _, connection = _fake_store()
    source_counts = MagicMock(fetchall=MagicMock(return_value=[("request", "claimed", 4)]))
    source_age = MagicMock(fetchall=MagicMock(return_value=[("request", 9.5)]))
    publications = MagicMock(fetchall=MagicMock(return_value=[("request", 2, 120, 3.25)]))
    connection.execute.side_effect = [MagicMock(), source_counts, source_age, publications]

    snapshot = store.operational_snapshot("svc")

    assert snapshot == {
        "source_objects": [{"domain": "request", "status": "claimed", "count": 4}],
        "source_age": [{"domain": "request", "age_seconds": 9.5}],
        "publications": [
            {
                "domain": "request",
                "pending": 2,
                "published_rows": 120,
                "lag_seconds": 3.25,
            }
        ],
    }
    assert connection.execute.call_count == 4


def test_batch_claim_is_fenced_and_starts_at_generation_one() -> None:
    store, _, connection = _fake_store()
    owner_row = _owner_row(owner="high_scale")
    connection.execute.side_effect = [
        MagicMock(fetchone=MagicMock(return_value=owner_row)),
        MagicMock(fetchone=MagicMock(return_value=None)),
        MagicMock(),
    ]

    claim = store.claim_batch(
        "batch-1",
        "svc",
        "request",
        "worker-1",
        expected_owner="high_scale",
        expected_owner_epoch=1,
        now=datetime(2026, 9, 11, 12, tzinfo=UTC),
    )

    assert claim.claimed is True
    assert claim.lease_generation == 1
    assert claim.state == "claimed"


def test_batch_claim_rejects_wrong_owner_epoch() -> None:
    store, _, connection = _fake_store()
    connection.execute.return_value.fetchone.return_value = _owner_row(owner="standard", epoch=2)

    with pytest.raises(ValueError, match="owner"):
        store.claim_batch(
            "batch-1",
            "svc",
            "request",
            "worker-1",
            expected_owner="high_scale",
            expected_owner_epoch=1,
        )


def test_discover_source_tolerates_a_sibling_domain_discovering_the_same_object() -> None:
    """Regression test: rum_vitals and rum_errors both list the FOS prefix
    raw/rum/, so the same object is legitimately discovered by both
    domains' pages — whichever wins the race keeps the row. Rejecting the
    second domain's discovery as a "changed identity" made every subsequent
    tick of the losing domain's page raise and abort, permanently starving
    it (observed live during RUM qualification). Only a genuine
    checksum/version change is a real identity conflict."""
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    existing = _source_row(status="discovered")  # discovered under domain="request" per _source_row's default
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = existing
    connection.execute.side_effect = cursors

    result = store.discover_source(
        "svc",
        "rum_errors",
        "raw/one.gz",
        "sha256:source",  # matches _source_row's checksum
        size_bytes=12,
        version="v1",  # matches _source_row's version
        expected_owner="high_scale",
        expected_owner_epoch=1,
    )

    assert result.object_id == "object-1"
    # Only the two locking SELECTs ran — no INSERT attempted, no error.
    assert len(connection.execute.call_args_list) == 2


def test_discover_source_rejects_a_real_checksum_change() -> None:
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    existing = _source_row(status="discovered")
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = existing
    connection.execute.side_effect = cursors

    with pytest.raises(ValueError, match="identity changed"):
        store.discover_source(
            "svc",
            "request",
            "raw/one.gz",
            "sha256:different",
            size_bytes=12,
            version="v1",
            expected_owner="high_scale",
            expected_owner_epoch=1,
        )


def test_claim_returns_existing_lease_without_stealing_it() -> None:
    store, _, connection = _fake_store()
    now = datetime(2026, 9, 11, tzinfo=UTC)
    owner = _owner_row()
    source = _source_row(lease_until=now + timedelta(seconds=30), generation=4)
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    result = store.claim_source(
        "svc",
        "raw/one.gz",
        "worker-2",
        expected_owner="standard",
        expected_owner_epoch=1,
        now=now,
    )

    assert result.object_id == "object-1"
    assert result.claimed is False
    assert result.lease_generation == 4
    assert len(connection.execute.call_args_list) == 2


def test_claim_is_idempotent_for_the_current_worker() -> None:
    store, _, connection = _fake_store()
    now = datetime(2026, 9, 11, tzinfo=UTC)
    owner = _owner_row()
    source = _source_row(
        lease_until=now + timedelta(seconds=30),
        generation=4,
    )
    source["owner"] = "worker-1"
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    result = store.claim_source(
        "svc",
        "raw/one.gz",
        "worker-1",
        expected_owner="standard",
        expected_owner_epoch=1,
        now=now,
    )

    assert result.object_id == "object-1"
    assert result.claimed is True
    assert result.lease_generation == 4
    assert len(connection.execute.call_args_list) == 2


def test_claim_fences_stale_owner_epoch_before_source_update() -> None:
    store, _, connection = _fake_store()
    connection.execute.return_value.fetchone.return_value = _owner_row(epoch=2)

    with pytest.raises(ValueError, match="owner epoch"):
        store.claim_source(
            "svc",
            "raw/one.gz",
            "worker-1",
            expected_owner="standard",
            expected_owner_epoch=1,
        )

    assert len(connection.execute.call_args_list) == 1


def test_mark_source_missing_stamps_terminal_status_and_error() -> None:
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    source = _source_row(status="claimed", owner_epoch=1)
    cursors = [MagicMock(), MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    store.mark_source_missing(
        "svc",
        "raw/one.gz",
        expected_owner="high_scale",
        expected_owner_epoch=1,
        error="source object is missing from storage: raw/one.gz",
    )

    update_sql, update_params = connection.execute.call_args_list[2].args
    assert "status='source_missing'" in update_sql
    assert update_params[0] == "source object is missing from storage: raw/one.gz"
    assert update_params[-1] == source["object_id"]


def test_mark_source_missing_is_a_no_op_when_already_missing() -> None:
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    source = _source_row(status="source_missing", owner_epoch=1)
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    store.mark_source_missing(
        "svc",
        "raw/one.gz",
        expected_owner="high_scale",
        expected_owner_epoch=1,
        error="retry",
    )

    # Only the two locking SELECTs — no UPDATE issued for an already-terminal row.
    assert len(connection.execute.call_args_list) == 2


def test_mark_source_missing_refuses_to_override_acknowledged_source() -> None:
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    source = _source_row(status="acknowledged", owner_epoch=1)
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    with pytest.raises(ValueError, match="durably archived"):
        store.mark_source_missing(
            "svc",
            "raw/one.gz",
            expected_owner="high_scale",
            expected_owner_epoch=1,
            error="source object is missing from storage: raw/one.gz",
        )


def test_mark_source_missing_fences_stale_owner_epoch() -> None:
    store, _, connection = _fake_store()
    connection.execute.return_value.fetchone.return_value = _owner_row(owner="high_scale", epoch=2)

    with pytest.raises(ValueError, match="owner epoch"):
        store.mark_source_missing(
            "svc",
            "raw/one.gz",
            expected_owner="high_scale",
            expected_owner_epoch=1,
            error="source object is missing from storage: raw/one.gz",
        )

    assert len(connection.execute.call_args_list) == 1


def test_cursor_advancement_is_owner_epoch_fenced() -> None:
    store, _, connection = _fake_store()
    current = _owner_row(owner="high_scale", epoch=4, cursor="cursor-1")
    updated = _owner_row(owner="high_scale", epoch=4, cursor="cursor-2")
    cursors = [MagicMock(), MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = current
    cursors[2].fetchone.return_value = updated
    connection.execute.side_effect = cursors

    result = store.advance_source_cursor(
        "svc",
        "cursor-2",
        expected_owner="high_scale",
        expected_owner_epoch=4,
    )

    assert result.source_cursor == "cursor-2"
    assert connection.execute.call_args_list[1].args[1] == (
        "cursor-2",
        "svc",
        4,
        "high_scale",
    )


def test_source_cursor_for_never_falls_back_to_owner_level_cursor() -> None:
    """Regression test: different domains list different FOS prefixes
    (request vs. rum). Falling back to the shared owner-level cursor for a
    domain that has never advanced can hand it a StartAfter key entirely
    outside its own Prefix — observed live as rum_vitals/rum_errors
    permanently stuck at zero discovered objects, silently, because the
    inherited request-domain cursor made every listing return empty. A
    domain with no cursor of its own must get its own initial cursor."""
    store, _, connection = _fake_store()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    connection.execute.return_value = cursor

    result = store.source_cursor_for("svc", "rum_vitals")

    assert result == INITIAL_SOURCE_CURSOR
    # Only the per-domain lookup ran (plus the read-only transaction set-up)
    # — no owner-row fallback query.
    executed_sql = [c.args[0] for c in connection.execute.call_args_list]
    assert not any("high_scale_ownership" in sql for sql in executed_sql)


def test_source_cursor_for_returns_the_domains_own_persisted_cursor() -> None:
    store, _, connection = _fake_store()
    cursor = MagicMock()
    cursor.fetchone.return_value = ("__terminal__:raw/rum/a.gz",)
    connection.execute.return_value = cursor

    assert store.source_cursor_for("svc", "rum_vitals") == "__terminal__:raw/rum/a.gz"


def test_cutover_increments_epoch_and_preserves_previous_owner() -> None:
    store, _, connection = _fake_store()
    current = _owner_row(drained=True)
    updated = _owner_row(
        epoch=2,
        owner="high_scale",
        previous="standard",
        drained=True,
        committed=True,
        rollback_allowed=True,
    )
    cursors = [MagicMock(), MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = current
    cursors[2].fetchone.return_value = updated
    connection.execute.side_effect = cursors

    result = store.commit_cutover("svc", expected_epoch=1, next_owner="high_scale")

    assert result.owner_epoch == 2
    assert result.previous_owner == "standard"
    assert result.current_owner == "high_scale"
    update_sql = connection.execute.call_args_list[1].args[0]
    assert "owner_epoch=owner_epoch+1" in update_sql
    assert "previous_owner=current_owner" in update_sql


def test_database_errors_propagate_from_transaction() -> None:
    store, _, connection = _fake_store()
    connection.execute.side_effect = RuntimeError("database unavailable")

    with pytest.raises(RuntimeError, match="database unavailable"):
        store.create_schema()


def test_archive_transition_requires_owner_epoch_and_adjacent_state() -> None:
    store, _, connection = _fake_store()
    current = _archive_row()
    updated = _archive_row(state="manifest_committed")
    cursors = [MagicMock(), MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = current
    cursors[2].fetchone.return_value = updated
    connection.execute.side_effect = cursors

    result = store.transition_archive_manifest(
        "manifest-1",
        expected_state=ArchiveState.MANIFEST_PREPARED,
        next_state=ArchiveState.MANIFEST_COMMITTED,
        expected_owner_epoch=1,
    )

    assert result.state is ArchiveState.MANIFEST_COMMITTED
    assert connection.execute.call_args_list[1].args[1] == (
        "manifest_committed",
        "manifest-1",
        1,
        "manifest_prepared",
    )

    connection.execute.reset_mock()
    stale_cursor = MagicMock()
    stale_cursor.fetchone.return_value = current
    connection.execute.side_effect = [stale_cursor]
    with pytest.raises(ValueError, match="owner epoch"):
        store.transition_archive_manifest(
            "manifest-1",
            expected_state=ArchiveState.MANIFEST_PREPARED,
            next_state=ArchiveState.MANIFEST_COMMITTED,
            expected_owner_epoch=2,
        )


def test_deletion_authorization_rejects_active_replay_lease_without_database_call() -> None:
    store, _, connection = _fake_store()

    with pytest.raises(ValueError, match="active replay lease"):
        store.authorize_source_delete(
            "svc",
            "raw/one.gz",
            manifest_id="manifest-1",
            expected_owner_epoch=1,
            replay_lease_active=True,
        )

    connection.execute.assert_not_called()


def test_deletable_sources_queries_committed_and_eligible_manifests_past_deadline() -> None:
    store, _, connection = _fake_store()
    cursor = MagicMock()
    cursor.fetchall.return_value = [_source_row(status="acknowledged"), _source_row(status="archived")]
    connection.execute.return_value = cursor

    result = store.deletable_sources("svc", limit=50)

    assert len(result) == 2
    assert all(r.object_id == "object-1" for r in result)
    sql, params = connection.execute.call_args.args
    assert "high_scale_source_objects" in sql
    assert "high_scale_archive_manifests" in sql
    assert "'archived', 'acknowledged'" in sql
    assert "'manifest_committed', 'deletion_eligible'" in sql
    assert params == ("svc", 50)


def test_deletable_sources_rejects_non_positive_limit() -> None:
    store, _, _ = _fake_store()
    with pytest.raises(ValueError, match="limit"):
        store.deletable_sources("svc", limit=0)


def test_manifests_covering_queries_by_service_domain_and_overlap() -> None:
    store, _, connection = _fake_store()
    cursor = MagicMock()
    cursor.fetchall.return_value = [_archive_row(manifest_id="m1"), _archive_row(manifest_id="m2")]
    connection.execute.return_value = cursor

    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = datetime(2026, 9, 1, 1, tzinfo=UTC)
    result = store.manifests_covering("svc", "request", start, end)

    assert [m.manifest_id for m in result] == ["m1", "m2"]
    sql, params = connection.execute.call_args.args
    assert "high_scale_archive_manifests" in sql
    assert "coverage_end > %s" in sql
    assert "coverage_start < %s" in sql
    assert params == ("svc", "request", start, end)


def test_manifests_covering_rejects_inverted_range() -> None:
    store, _, _ = _fake_store()
    start = datetime(2026, 9, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="range"):
        store.manifests_covering("svc", "request", start, start - timedelta(minutes=1))


def _manifest(*, service_id: str = "svc", manifest_id: str = "manifest-1") -> ArchiveManifest:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    source = ArchiveSourceObject(service_id, "request", "raw/one.gz", "sha256:source", 12, "v1")
    artifact = ArchiveArtifact(
        "s3://archive/manifest-1.parquet",
        "sha256:artifact",
        128,
        3,
        256,
        "digest",
        "archive.v1",
        "normalize.v1",
    )
    return ArchiveManifest(
        manifest_id,
        source,
        artifact,
        start,
        start + timedelta(minutes=1),
        start + timedelta(days=1),
        start + timedelta(days=2),
        1,
    )


@pytest.mark.postgres_integration
def test_postgres_control_plane_end_to_end() -> None:
    dsn = os.getenv("HIGH_SCALE_POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("HIGH_SCALE_POSTGRES_TEST_DSN is not set")

    service_id = f"test-high-scale-{uuid4()}"
    pool = ConnectionPool(
        conninfo=dsn,
        kwargs={"autocommit": False},
        min_size=1,
        max_size=2,
        timeout=5,
    )
    store = PostgresControlPlane(pool=pool, pool_timeout=5)
    manifest = _manifest(service_id=service_id)
    try:
        pool.open(wait=True)
        store.create_schema()
        owner = store.initialize_owner(service_id, owner="standard", source_cursor="cursor-0")
        source = store.discover_source(
            service_id,
            "request",
            "raw/one.gz",
            "sha256:source",
            size_bytes=12,
            version="v1",
            expected_owner="standard",
            expected_owner_epoch=owner.owner_epoch,
        )
        claim = store.claim_source(
            service_id,
            source.object_key,
            "worker-1",
            expected_owner="standard",
            expected_owner_epoch=owner.owner_epoch,
        )
        store.mark_source_appended(
            service_id,
            source.object_key,
            lease_generation=claim.lease_generation,
            expected_owner_epoch=owner.owner_epoch,
        )
        store.register_archive_manifest(manifest, owner_epoch=owner.owner_epoch)
        for current, next_state in (
            (ArchiveState.ARTIFACT_UPLOADING, ArchiveState.ARTIFACT_VERIFIED),
            (ArchiveState.ARTIFACT_VERIFIED, ArchiveState.MANIFEST_PREPARED),
            (ArchiveState.MANIFEST_PREPARED, ArchiveState.MANIFEST_COMMITTED),
        ):
            store.transition_archive_manifest(
                manifest.manifest_id,
                expected_state=current,
                next_state=next_state,
                expected_owner_epoch=owner.owner_epoch,
            )
        store.mark_source_archived(
            service_id,
            source.object_key,
            lease_generation=claim.lease_generation,
            manifest_id=manifest.manifest_id,
            owner_epoch=owner.owner_epoch,
        )
        store.acknowledge_source(service_id, source.object_key, manifest_id=manifest.manifest_id)
        store.transition_archive_manifest(
            manifest.manifest_id,
            expected_state=ArchiveState.MANIFEST_COMMITTED,
            next_state=ArchiveState.DELETION_ELIGIBLE,
            expected_owner_epoch=owner.owner_epoch,
        )
        store.authorize_source_delete(
            service_id,
            source.object_key,
            manifest_id=manifest.manifest_id,
            expected_owner_epoch=owner.owner_epoch,
            now=manifest.deletion_authorization_deadline + timedelta(seconds=1),
        )
        store.mark_source_deleted(
            service_id,
            source.object_key,
            manifest_id=manifest.manifest_id,
            expected_owner_epoch=owner.owner_epoch,
        )
        assert store.archive_manifest(manifest.manifest_id).state is ArchiveState.SOURCE_DELETED
    finally:
        try:
            with pool.connection(timeout=5) as connection, connection.transaction():
                connection.execute(
                    "DELETE FROM high_scale_deletion_authorizations WHERE object_id IN "
                    "(SELECT object_id FROM high_scale_source_objects WHERE service_id=%s)",
                    (service_id,),
                )
                connection.execute(
                    "DELETE FROM high_scale_source_objects WHERE service_id=%s",
                    (service_id,),
                )
                connection.execute(
                    "DELETE FROM high_scale_archive_manifests WHERE service_id=%s",
                    (service_id,),
                )
                connection.execute(
                    "DELETE FROM high_scale_ownership WHERE service_id=%s",
                    (service_id,),
                )
        finally:
            pool.close()
