from datetime import UTC, datetime

import pytest

from backend.high_scale.aggregate_writer import compute_dimension_counts


def test_computes_counts_per_dimension_value_and_minute_bucket() -> None:
    rows = (
        {"timestamp": "2026-09-14T00:00:10Z", "url": "/a", "country": "US", "ip": "1.1.1.1"},
        {"timestamp": "2026-09-14T00:00:40Z", "url": "/a", "country": "US", "ip": "1.1.1.2"},
        {"timestamp": "2026-09-14T00:01:05Z", "url": "/b", "country": "CA", "ip": "1.1.1.1"},
    )

    counts = compute_dimension_counts("request", rows)

    by_key = {(row["dimension"], row["value"], row["bucket_start"]): row["count"] for row in counts}
    minute0 = datetime(2026, 9, 14, 0, 0, tzinfo=UTC).isoformat()
    minute1 = datetime(2026, 9, 14, 0, 1, tzinfo=UTC).isoformat()
    assert by_key[("url", "/a", minute0)] == 2
    assert by_key[("url", "/b", minute1)] == 1
    assert by_key[("country", "US", minute0)] == 2
    assert by_key[("country", "CA", minute1)] == 1
    assert by_key[("client_ip", "1.1.1.1", minute0)] == 1
    assert by_key[("client_ip", "1.1.1.2", minute0)] == 1


def test_request_counts_include_all_dashboard_card_dimensions() -> None:
    rows = (
        {
            "timestamp": "2026-09-14T00:00:10Z",
            "url": "/a",
            "country": "US",
            "ip": "1.1.1.1",
            "host": "logs.example.test",
            "method": "GET",
            "status": 200,
            "cache": "HIT",
            "proto": "HTTP/3",
            "ua": "Synthetic Browser",
            "referer": "https://example.test/",
            "city": "Seattle",
            "region": "WA",
            "pop": "SEA",
            "backend": "origin-a",
            "tls": "1.3",
        },
    )

    counts = compute_dimension_counts("request", rows)

    values = {(row["dimension"], row["value"]) for row in counts}
    assert {
        ("host", "logs.example.test"),
        ("method", "GET"),
        ("status", "200"),
        ("cache", "HIT"),
        ("proto", "HTTP/3"),
        ("ua", "Synthetic Browser"),
        ("referer", "https://example.test/"),
        ("city", "Seattle"),
        ("region", "WA"),
        ("pop", "SEA"),
        ("backend", "origin-a"),
        ("tls", "1.3"),
    } <= values


def test_rum_vitals_uses_metric_name_dimension() -> None:
    rows = ({"timestamp": "2026-09-14T00:00:00Z", "metric_name": "LCP"},)

    counts = compute_dimension_counts("rum_vitals", rows)

    assert len(counts) == 1
    assert counts[0]["dimension"] == "metric_name"
    assert counts[0]["value"] == "LCP"


def test_cmcd_dimension_reads_the_session_id_field() -> None:
    rows = ({"timestamp": "2026-09-14T00:00:00Z", "sid": "session-1"},)

    counts = compute_dimension_counts("cmcd", rows)

    assert counts[0]["dimension"] == "cmcd_session"
    assert counts[0]["value"] == "session-1"


def test_rows_without_a_timestamp_are_skipped() -> None:
    counts = compute_dimension_counts("request", ({"url": "/a"},))

    assert counts == ()


def test_unsupported_domain_is_rejected() -> None:
    with pytest.raises(ValueError, match="domain"):
        compute_dimension_counts("bogus", ())
