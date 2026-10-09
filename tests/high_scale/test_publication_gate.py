from __future__ import annotations

from datetime import UTC
from unittest.mock import MagicMock

import pytest

from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    PartialInsert,
    PublicationStatus,
)


def test_publication_attempt_with_mismatched_row_count_rejected() -> None:
    """A publication attempt with mismatched row count is rejected and never transitions to visible."""
    store = InMemoryBatchManifestStore()
    client = MagicMock()
    # Mock ClickHouse inserting 95 rows when batch has 100
    batch = HighScaleBatch(
        batch_id="batch-mismatch",
        service_id="svc-test",
        domain="request",
        generation="epoch-1",
        rows=tuple({"event_id": f"evt-{i}"} for i in range(100)),
    )

    client.insert.side_effect = PartialInsert(95)
    publication = ClickHousePublication(store, client)

    with pytest.raises(PartialInsert) as exc_info:
        publication.publish(batch)

    assert exc_info.value.rows_inserted == 95
    assert not publication.is_visible("batch-mismatch")
    manifest = store.get("batch-mismatch")
    assert manifest is not None
    assert manifest.status is PublicationStatus.PENDING
    assert manifest.visible_rows == 0


def test_query_filters_visible_state_and_ignores_pending_rows() -> None:
    """Queries must filter on publication_state='visible' or _high_scale_visible=1 and never see pending rows."""
    from datetime import datetime, timedelta

    from backend.high_scale.archive_models import ServingWatermark
    from backend.high_scale.query_service import query_request_facts

    client = MagicMock()
    # ClickHouse query returns rows only when publication_state = 'visible'
    client.execute.return_value = [
        {"event_id": "00000000-0000-0000-0000-000000000001", "timestamp": "2026-10-09T12:00:00Z"},
    ]

    now = datetime(2026, 10, 9, 12, 5, tzinfo=UTC)
    watermark = ServingWatermark(
        service_id="svc-test",
        domain="request",
        owner_epoch=1,
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        last_accepted_cursor="cur-1",
        last_archived_event_id="evt-1",
        last_visible_event_id="evt-1",
        exact=True,
    )

    # Run query_request_facts
    results = query_request_facts(
        client,
        service_id="svc-test",
        start=now - timedelta(minutes=5),
        end=now,
        cursor_secret=b"test-secret-123456",
        watermark=watermark,
        limit=10,
    )
    assert len(results.rows) == 1

    # Verify execute was called with a query enforcing publication_state = 'visible'
    assert client.execute.called
    query_sql, params = client.execute.call_args[0]
    assert "publication_state = 'visible'" in query_sql or "_high_scale_visible = 1" in query_sql
