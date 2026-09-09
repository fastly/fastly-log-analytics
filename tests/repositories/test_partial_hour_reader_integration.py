from datetime import UTC, datetime
from unittest.mock import patch


def test_execute_top_n_rollups_merges_partial_hour_and_narrows_live_start():
    from backend.repositories._base import QueryRunner

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    with (
        patch(
            "backend.core.rollups.partial_hour.read_partial_hour_watermark",
            return_value=1780272000.0,
        ),
        patch(
            "backend.core.rollups.partial_hour.read_partial_hour_all_fields",
            return_value=[("country", "US", 42)],
        ),
        patch.object(QueryRunner, "_create_active_hour_temp_direct", return_value=None) as mock_direct,
    ):
        runner = QueryRunner.__new__(QueryRunner)
        runner.src = {"service_id": "svc-a", "name": "svc-a"}
        live_start, live_end = runner._partial_hour_adjusted_live_start(datetime(2026, 1, 1, tzinfo=UTC), active_hour)
        # The watermark (1780272000.0 epoch, 2026-06-01) is later than the
        # naive window start passed in (2026-01-01), so it should win via
        # max().
        from datetime import datetime as dt

        assert live_start == dt.fromtimestamp(1780272000.0, tz=UTC)
