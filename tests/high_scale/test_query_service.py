from datetime import UTC, datetime

import pytest

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.pagination import KeysetCursor
from backend.high_scale.query_service import query_request_facts


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
