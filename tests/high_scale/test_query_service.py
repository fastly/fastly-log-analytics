from datetime import UTC, datetime

import pytest

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.pagination import KeysetCursor
from backend.high_scale.query_service import query_cmcd_facts, query_request_facts, query_rum_facts


class _Client:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.params = None

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params
        return self.rows


def _watermark() -> ServingWatermark:
    return ServingWatermark(
        "svc",
        "request",
        1,
        datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        datetime(2026, 9, 11, 20, 5, tzinfo=UTC),
        "cursor",
        "event-2",
        "event-2",
        True,
    )


def _row(second: int, event_id: str) -> dict:
    return {"event_id": event_id, "timestamp": datetime(2026, 9, 11, 20, 0, second, tzinfo=UTC)}


def test_request_facts_use_visibility_fence_and_keyset_cursor() -> None:
    client = _Client([_row(1, "event-1"), _row(2, "event-2")])
    page = query_request_facts(
        client,
        service_id="svc",
        start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
        cursor_secret=b"secret",
        watermark=_watermark(),
        limit=1,
        now=datetime(2026, 9, 11, 20, 6, tzinfo=UTC),
    )

    assert "publication_state = 'visible'" in client.sql
    assert page.rows == (client.rows[0],)
    assert page.next_cursor
    decoded = KeysetCursor.decode(page.next_cursor, b"secret")
    assert decoded.event_id == "event-1"
    assert page.metadata.exact is True
    assert page.metadata.freshness_lag_seconds == 60


def test_request_facts_reject_cursor_for_other_domain() -> None:
    cursor = KeysetCursor(
        datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        "event-1",
        "svc",
        "rum_vitals",
    ).encode(b"secret")

    with pytest.raises(ValueError, match="service and domain"):
        query_request_facts(
            _Client([]),
            service_id="svc",
            start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
            end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
            cursor_secret=b"secret",
            watermark=_watermark(),
            cursor=cursor,
        )


def test_request_facts_reject_invalid_window_and_limit() -> None:
    with pytest.raises(ValueError, match="query end"):
        query_request_facts(
            _Client([]),
            service_id="svc",
            start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
            end=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
            cursor_secret=b"secret",
            watermark=_watermark(),
        )
    with pytest.raises(ValueError, match="between"):
        query_request_facts(
            _Client([]),
            service_id="svc",
            start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
            end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
            cursor_secret=b"secret",
            watermark=_watermark(),
            limit=501,
        )


def test_rum_domains_use_separate_fact_tables_and_watermarks() -> None:
    client = _Client([_row(1, "event-1")])
    watermark = _watermark()
    watermark = ServingWatermark(
        watermark.service_id,
        "rum_vitals",
        watermark.owner_epoch,
        watermark.coverage_start,
        watermark.coverage_end,
        watermark.last_accepted_cursor,
        watermark.last_archived_event_id,
        watermark.last_visible_event_id,
        watermark.exact,
    )

    query_rum_facts(
        client,
        service_id="svc",
        domain="rum_vitals",
        start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
        cursor_secret=b"secret",
        watermark=watermark,
    )

    assert "FROM rum_vitals_facts" in client.sql


def test_cmcd_facts_use_projection_identity_for_keyset() -> None:
    client = _Client([_row(1, "projection-1")])
    watermark = _watermark()
    watermark = ServingWatermark(
        watermark.service_id,
        "cmcd",
        watermark.owner_epoch,
        watermark.coverage_start,
        watermark.coverage_end,
        watermark.last_accepted_cursor,
        watermark.last_archived_event_id,
        watermark.last_visible_event_id,
        watermark.exact,
    )

    query_cmcd_facts(
        client,
        service_id="svc",
        start=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
        cursor_secret=b"secret",
        watermark=watermark,
    )

    assert "FROM cmcd_projection_facts" in client.sql
    assert "projection_key AS event_id" in client.sql
