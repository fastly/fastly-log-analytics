"""Latest operational samples exported through the configured OTel meter."""

from __future__ import annotations

import threading
from typing import Any

from opentelemetry.metrics import CallbackOptions, Observation

from backend.core.request_telemetry import get_meter

_lock = threading.Lock()
_values: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}
_instruments: dict[str, Any] = {}

_METRICS = {
    "celery_queue_depth": ("Celery broker queue depth", "{messages}"),
    "celery_broker_reachable": ("Celery broker reachability", "1"),
    "celery_active_workers": ("Active Celery workers", "{workers}"),
    "celery_active_tasks": ("Active Celery tasks", "{tasks}"),
    "ingest_ledger_rows": ("Ingest ledger rows by state", "{rows}"),
}


def _instrument(metric: str) -> Any:
    instrument = _instruments.get(metric)
    if instrument is not None:
        return instrument

    description, unit = _METRICS[metric]

    def callback(_options: CallbackOptions, metric_name: str = metric) -> list[Observation]:
        with _lock:
            samples = dict(_values.get(metric_name, {}))
        return [Observation(value, dict(attributes)) for attributes, value in samples.items()]

    instrument = get_meter().create_observable_gauge(
        name=f"fla_{metric}",
        callbacks=[callback],
        description=description,
        unit=unit,
    )
    _instruments[metric] = instrument
    return instrument


def record(metric: str, value: float, **attributes: str) -> None:
    """Publish the latest value for an operational metric and its labels."""
    if metric not in _METRICS:
        raise ValueError(f"unsupported operational metric: {metric}")
    key = tuple(sorted((name, str(value)) for name, value in attributes.items()))
    with _lock:
        _values.setdefault(metric, {})[key] = float(value)
    _instrument(metric)


def reset_for_tests() -> None:
    """Clear cached samples and instruments between isolated unit tests."""
    with _lock:
        _values.clear()
        _instruments.clear()
