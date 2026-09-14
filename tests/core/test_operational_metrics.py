"""Tests for the OTel operational metric bridge."""

from __future__ import annotations

from backend.core import operational_metrics


class _FakeGauge:
    def __init__(self, callbacks):
        self.callbacks = callbacks


class _FakeMeter:
    def __init__(self):
        self.gauges = {}

    def create_observable_gauge(self, *, name, callbacks, description, unit):
        gauge = _FakeGauge(callbacks)
        self.gauges[name] = (gauge, description, unit)
        return gauge


def test_record_exposes_latest_labeled_observation(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)

    operational_metrics.record("celery_queue_depth", 7, queue="q.ingest")

    gauge, description, unit = meter.gauges["fla_celery_queue_depth"]
    observations = gauge.callbacks[0](None)
    assert [(item.value, item.attributes) for item in observations] == [(7.0, {"queue": "q.ingest"})]
    assert description == "Celery broker queue depth"
    assert unit == "{messages}"


def test_record_keeps_distinct_label_sets(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)

    operational_metrics.record("ingest_ledger_rows", 3, status="discovered")
    operational_metrics.record("ingest_ledger_rows", 2, status="claimed")

    gauge, _, _ = meter.gauges["fla_ingest_ledger_rows"]
    observations = gauge.callbacks[0](None)
    assert {(item.value, item.attributes["status"]) for item in observations} == {
        (3.0, "discovered"),
        (2.0, "claimed"),
    }
