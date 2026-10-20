from copy import deepcopy

from backend.high_scale.performance_projection import (
    SCATTER_SAMPLE_SIZE,
    build_performance_projection_rows,
)


def _event(**overrides):
    event = {
        "event_id": "evt-1",
        "timestamp": "2026-09-16T14:00:01Z",
        "url": "/a",
        "asn": "7922",
        "cache": "MISS",
        "elapsed": 250_000,
        "ttfb": 0.1,
        "ottfb": 40_000,
        "ottlb": 90_000,
    }
    event.update(overrides)
    return event


def test_summary_rows_decompose_latency_into_waterfall_components_in_ms():
    events = (
        _event(),
        _event(event_id="evt-2", timestamp="2026-09-16T14:00:30Z", elapsed=150_000, ttfb=0.05, ottfb=None, ottlb=None),
        _event(event_id="evt-3", timestamp="2026-09-16T14:01:00Z"),
    )
    original = deepcopy(events)

    projection = build_performance_projection_rows(events)

    assert events == original
    assert projection.summary_rows == (
        {
            "bucket_start": "2026-09-16T14:00:00+00:00",
            "component_count": 2,
            # evt-1: 60 edge, 40 wait, 50 download, 150 client; evt-2: 50 edge, 0, 0, 100 client
            "edge_processing_sum_ms": 110.0,
            "origin_wait_sum_ms": 40.0,
            "origin_download_sum_ms": 50.0,
            "client_download_sum_ms": 250.0,
        },
        {
            "bucket_start": "2026-09-16T14:01:00+00:00",
            "component_count": 1,
            "edge_processing_sum_ms": 60.0,
            "origin_wait_sum_ms": 40.0,
            "origin_download_sum_ms": 50.0,
            "client_download_sum_ms": 150.0,
        },
    )


def test_components_are_clamped_at_zero_like_the_standard_path():
    # ottlb < ottfb and ttfb < ottfb: every derived component floors at 0.
    projection = build_performance_projection_rows((_event(elapsed=10_000, ttfb=0.001, ottfb=40_000, ottlb=20_000),))

    row = projection.summary_rows[0]
    assert row["edge_processing_sum_ms"] == 0.0
    assert row["origin_download_sum_ms"] == 0.0
    assert row["client_download_sum_ms"] == 0.0
    assert row["origin_wait_sum_ms"] == 40.0


def test_rows_without_ttfb_or_elapsed_never_enter_summary_or_scatter():
    projection = build_performance_projection_rows(
        (_event(ttfb=None), _event(event_id="e2", elapsed=""), _event(event_id="e3", ttfb="nan"))
    )

    assert projection.summary_rows == ()
    assert projection.scatter_rows == ()


def test_scatter_rows_carry_origin_edge_and_cache_and_drop_negative_edge():
    projection = build_performance_projection_rows(
        (
            _event(),
            _event(event_id="evt-hit", cache="HIT", elapsed=120_000, ttfb=0.02),
            _event(event_id="evt-neg", elapsed=1_000, ttfb=0.5),
            _event(event_id="evt-nocache", cache=None),
        )
    )

    shapes = {(row["cache"], row["origin_ms"], row["edge_ms"]) for row in projection.scatter_rows}
    assert shapes == {("MISS", 100.0, 150.0), ("HIT", 20.0, 100.0), ("", 100.0, 150.0)}
    assert all(0 <= row["sample_key"] < 2**64 for row in projection.scatter_rows)
    assert all(row["bucket_start"] == "2026-09-16T14:00:00+00:00" for row in projection.scatter_rows)


def test_scatter_keeps_the_lowest_sample_keys_so_merged_batches_stay_uniform():
    events = tuple(_event(event_id=f"evt-{i}") for i in range(SCATTER_SAMPLE_SIZE * 2))

    projection = build_performance_projection_rows(events)

    assert len(projection.scatter_rows) == SCATTER_SAMPLE_SIZE
    keys = [row["sample_key"] for row in projection.scatter_rows]
    assert keys == sorted(keys)
    again = build_performance_projection_rows(tuple(reversed(events)))
    assert [row["sample_key"] for row in again.scatter_rows] == keys


def test_dimension_rows_are_unchanged():
    projection = build_performance_projection_rows((_event(), _event(event_id="e2", url="/b")))

    assert {(r["dimension"], r["value"]) for r in projection.dimension_rows} == {
        ("url", "/a"),
        ("url", "/b"),
        ("asn", "7922"),
    }


def test_integer_asn_from_the_vcl_log_format_still_produces_an_asn_dimension():
    # The log format emits asn unquoted, so real events carry an int, not "7922".
    projection = build_performance_projection_rows((_event(asn=7922), _event(event_id="evt-2", asn=15169)))

    asn_values = {row["value"] for row in projection.dimension_rows if row["dimension"] == "asn"}

    assert asn_values == {"7922", "15169"}
