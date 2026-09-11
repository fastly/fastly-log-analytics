from datetime import UTC, datetime

from backend.high_scale.aggregates import AggregateStore, EventBatch


def batch(batch_id: str, domain: str, events: list[dict]) -> EventBatch:
    return EventBatch(
        batch_id,
        "svc",
        domain,
        tuple(events),
        owner_epoch=1,
        coverage_start=datetime(2026, 9, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
    )


def test_request_status_counts_are_exact_and_retry_is_idempotent() -> None:
    store = AggregateStore()
    request_batch = batch(
        "b1", "request", [{"event_id": "1", "status_code": 200}, {"event_id": "2", "status_code": 500}]
    )
    receipt = store.apply(request_batch)
    duplicate = store.apply(request_batch)
    assert receipt.applied is True
    assert duplicate.duplicate is True
    assert store.request_count("svc") == 2
    assert store.status_counts("svc") == {"200": 1, "500": 1}


def test_rum_and_cmcd_updates_do_not_change_request_counts() -> None:
    store = AggregateStore()
    store.apply(batch("rum", "rum_vitals", [{"event_id": "r1", "metric_name": "LCP"}]))
    store.apply(batch("cmcd", "cmcd", [{"event_id": "c1", "sid": "session"}]))
    assert store.request_count("svc") == 0


def test_response_exposes_watermark_and_freshness_metadata() -> None:
    store = AggregateStore()
    store.apply(batch("b1", "request", [{"event_id": "1", "url": "/"}]))
    response = store.response("svc", "request", now=datetime(2026, 9, 1, 0, 2, tzinfo=UTC))
    assert response.exact is True
    assert response.coverage == 1.0
    assert response.freshness_lag_seconds == 60
    assert response.watermark.last_visible_event_id == "1"


def test_minute_counts_are_separate_for_each_domain() -> None:
    store = AggregateStore()
    timestamp = "2026-09-01T00:01:42Z"
    store.apply(batch("request", "request", [{"event_id": "1", "timestamp": timestamp}]))
    store.apply(batch("rum", "rum_vitals", [{"event_id": "r1", "timestamp": timestamp}]))

    minute = datetime(2026, 9, 1, 0, 1, tzinfo=UTC)
    assert store.minute_counts("svc", "request") == {minute: 1}
    assert store.minute_counts("svc", "rum_vitals") == {minute: 1}
