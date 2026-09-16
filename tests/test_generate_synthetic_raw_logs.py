from datetime import UTC, datetime

from backend.core.ingest import get_catalog_field_ids
from scripts.load_test.generate_synthetic_raw_logs import _synthetic_line


def test_synthetic_line_populates_dashboard_dimensions() -> None:
    line = _synthetic_line(datetime(2026, 9, 10, tzinfo=UTC), "test-service")

    expected_fields = set(get_catalog_field_ids())

    assert expected_fields <= line.keys()
    assert all(line[field] not in ("", None) for field in expected_fields)
