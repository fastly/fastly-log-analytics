from datetime import UTC
from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import CacheCollapseDetailResponse, InsightCard, InsightItem, InsightsResponse


def insights_endpoint(
    service: HighScaleService, req: Any, clamp_start: str | None, clamp_end: str | None
) -> InsightsResponse:
    from datetime import datetime

    now = datetime.now(UTC).isoformat()

    query = """
        SELECT url, count() as errors
        FROM fastly_log_analytics.request_facts
        WHERE service_id={service_id:String}
          AND event_timestamp >= subtractDays(now(), 1)
          AND toInt32OrZero(custom_fields['status']) >= 500
        GROUP BY url ORDER BY errors DESC LIMIT 5
    """
    try:
        rows = service.client.execute(query, {"service_id": service.service_id})
    except Exception:
        rows = []

    insights = []
    if rows and any(r["errors"] > 0 for r in rows):
        items = []
        for r in rows:
            if r["errors"] > 0:
                items.append(
                    InsightItem(label=r["url"], current_val=float(r["errors"]), unit="errors", severity="high")
                )
        insights.append(
            InsightCard(
                id="high_scale_5xx",
                title="Elevated 5xx Errors",
                description="Paths with highest 5xx error counts in the last 24h.",
                severity="high",
                summary=f"Found {sum(r['errors'] for r in rows)} recent errors.",
                items=items,
                category="origin",
            )
        )

    return InsightsResponse.with_telemetry(
        insights=insights,
        window_start=now,
        window_end=now,
        baseline_start=now,
        baseline_end=now,
        computed_at=now,
        window_hours=24.0,
        baseline_hours=24.0,
    )


def cache_collapse_detail_endpoint(
    service: HighScaleService, req: Any, clamp_start: str | None, clamp_end: str | None
) -> CacheCollapseDetailResponse:
    return CacheCollapseDetailResponse.with_telemetry(
        has_data=False,
        clusters=[],
    )
