from datetime import UTC, datetime

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.dashboard import aggregates, header_metrics
from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import AggregatesRequest


class FakeClient:
    def execute(self, sql, params=None):
        if "SELECT bucket_start" in sql:
            return [{"bucket_start": "2026-09-15 19:00:00", "value": 12}]
        if "max(event_timestamp)" in sql:
            domain = (params or {}).get("domain")
            return [
                {
                    "total_rows": {"request": 12, "rum_vitals": 8, "rum_errors": 2}[domain],
                    "latest_log_at": {
                        "request": datetime(2026, 9, 15, 19, 0, tzinfo=UTC),
                        "rum_vitals": datetime(2026, 9, 15, 19, 1, tzinfo=UTC),
                        "rum_errors": datetime(2026, 9, 15, 19, 2, tzinfo=UTC),
                    }[domain],
                }
            ]
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


def test_high_scale_dashboard_returns_requested_supported_card_dimensions():
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
        AggregatesRequest(fields=["host", "method", "status", "cache", "ua"]),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )

    assert set(response.data) == {"host", "method", "status", "cache", "ua"}
    assert all(field.total == 12 for field in response.data.values())


def test_high_scale_header_metrics_use_visible_rows_and_latest_events():
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

    metrics = header_metrics(service)

    assert metrics["request"] == {
        "latest_log_at": "2026-09-15T19:00:00+00:00",
        "total_rows": 12,
        "last_sync_at": None,
    }
    assert metrics["rum"] == {
        "latest_log_at": "2026-09-15T19:02:00+00:00",
        "total_rows": 10,
        "last_sync_at": None,
    }
    assert metrics["local_rows"] == 22
