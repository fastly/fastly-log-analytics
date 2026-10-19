from datetime import UTC, datetime

import pytest

from backend.high_scale.pagination import KeysetCursor


def test_cursor_is_signed_and_service_bound() -> None:
    cursor = KeysetCursor(datetime(2026, 9, 1, tzinfo=UTC), "event-2", "svc", "request")
    token = cursor.encode(b"secret")

    assert KeysetCursor.decode(token, b"secret") == cursor
    with pytest.raises(ValueError, match="cursor"):
        KeysetCursor.decode(token, b"wrong")
