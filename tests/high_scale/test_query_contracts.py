from datetime import UTC, datetime

import pytest

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.query_contracts import QueryResponseMetadata


def test_query_metadata_requires_coverage_and_watermark_consistency() -> None:
    watermark = ServingWatermark(
        "svc",
        "request",
        2,
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        "cursor",
        "event-2",
        "event-2",
        True,
    )

    metadata = QueryResponseMetadata(
        status="complete",
        exact=True,
        coverage=1.0,
        freshness_lag_seconds=3.0,
        watermark=watermark,
        approximation_error=None,
        error=None,
    )

    metadata.validate()


def test_approximation_error_is_required_for_approximate_results() -> None:
    watermark = ServingWatermark("svc", "request", 1, None, None, None, None, None, False)
    metadata = QueryResponseMetadata("complete", False, 0.5, 0.0, watermark, None, None)

    with pytest.raises(ValueError, match="approximation"):
        metadata.validate()
