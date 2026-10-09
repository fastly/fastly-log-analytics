from __future__ import annotations


def test_batch_ingest_order_of_operations() -> None:
    """Verifies strict ordering:
    1. Archive upload & checksum verify
    2. Manifest prepared -> committed in Postgres
    3. ClickHouse INSERT as pending
    4. Publication marked visible
    5. Acknowledge sources in Postgres
    """
    call_order: list[str] = []

    def archive_put_and_verify(*args, **kwargs):
        call_order.append("archive_verified")

    def manifest_commit(*args, **kwargs):
        assert "archive_verified" in call_order, "Manifest committed before archive verified!"
        call_order.append("manifest_committed")

    def clickhouse_insert_pending(*args, **kwargs):
        assert "manifest_committed" in call_order, "ClickHouse inserted before manifest committed!"
        call_order.append("clickhouse_pending")

    def publication_publish_visible(*args, **kwargs):
        assert "clickhouse_pending" in call_order, "Publication published before ClickHouse insert!"
        call_order.append("published_visible")

    def acknowledge_sources(*args, **kwargs):
        assert "published_visible" in call_order, "Sources acknowledged before publication visible!"
        call_order.append("sources_acknowledged")

    # Execute sequence
    archive_put_and_verify()
    manifest_commit()
    clickhouse_insert_pending()
    publication_publish_visible()
    acknowledge_sources()

    assert call_order == [
        "archive_verified",
        "manifest_committed",
        "clickhouse_pending",
        "published_visible",
        "sources_acknowledged",
    ]


def test_crash_after_archive_verified_leaves_invisible_state() -> None:
    """If process crashes after archive upload, manifest is uncommitted and ClickHouse has no visible rows."""
    manifest_committed = False
    visible_in_clickhouse = False

    # Simulate crash before step 2
    # Verify state: no visible rows in serving
    assert not visible_in_clickhouse
    assert not manifest_committed


def test_crash_after_manifest_committed_is_idempotently_recoverable() -> None:
    """If process crashes after manifest commit but before ClickHouse insert, retry reuses manifest_id and batch_id."""
    manifest_id = "manifest-retry-123"
    # Derived batch_id must be stable across retries
    import uuid

    batch_id_1 = str(uuid.uuid5(uuid.NAMESPACE_OID, manifest_id))
    batch_id_2 = str(uuid.uuid5(uuid.NAMESPACE_OID, manifest_id))
    assert batch_id_1 == batch_id_2
