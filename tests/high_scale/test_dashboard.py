from datetime import UTC, datetime

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.dashboard import aggregates
from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import AggregatesRequest


class FakeClient:
    def execute(self, sql, params=None):
        if "SELECT bucket_start" in sql:
            return [{"bucket_start": "2026-09-15 19:00:00", "value": 12}]
        return [
            {"value": "/synthetic", "aggregate_count": 12},
        ]


def test_high_scale_dashboard_uses_clickhouse_aggregate_shape():
    service = HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )
    response = aggregates(
        service,
        AggregatesRequest(fields=["url", "country"]),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )
    assert response.total_rows == 12
    assert response.data["url"].total == 12
    assert response.map_data
    assert response.time_series[0].value == 12
