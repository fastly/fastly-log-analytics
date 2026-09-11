from datetime import UTC, datetime

import pytest

from backend.high_scale.pagination import KeysetCursor, paginate_rows


def test_cursor_is_signed_and_service_bound() -> None:
    cursor = KeysetCursor(datetime(2026, 9, 1, tzinfo=UTC), "event-2", "svc", "request")
    token = cursor.encode(b"secret")

    assert KeysetCursor.decode(token, b"secret") == cursor
    with pytest.raises(ValueError, match="cursor"):
        KeysetCursor.decode(token, b"wrong")


def test_paginate_rows_uses_timestamp_and_event_id_keyset() -> None:
    rows = (
        {"timestamp": "2026-09-01T00:00:00+00:00", "event_id": "event-1"},
        {"timestamp": "2026-09-01T00:00:00+00:00", "event_id": "event-2"},
        {"timestamp": "2026-09-01T00:01:00+00:00", "event_id": "event-3"},
    )

    first = paginate_rows(rows, limit=2, secret=b"secret", service_id="svc", domain="request")
    second = paginate_rows(
        rows,
        limit=2,
        secret=b"secret",
        service_id="svc",
        domain="request",
        cursor=first.next_cursor,
    )

    assert [row["event_id"] for row in first.rows] == ["event-1", "event-2"]
    assert [row["event_id"] for row in second.rows] == ["event-3"]
    assert second.next_cursor is None


def test_page_limit_is_bounded() -> None:
    with pytest.raises(ValueError, match="limit"):
        paginate_rows([], limit=501, secret=b"secret", service_id="svc", domain="request")
