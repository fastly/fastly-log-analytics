"""Backend-only observable gauges; collected by the existing OTLP reader.

Callbacks share one short-lived sample, including unknown values on failures.
Neither an admin request nor expensive generation readiness is needed.
"""

from __future__ import annotations

import sys
import threading
import time
from dataclasses import replace
from functools import cache, partial
from typing import TYPE_CHECKING

import structlog
from opentelemetry.metrics import Observation

from backend import config
from backend.core.request_telemetry import get_meter

if TYPE_CHECKING:
    from backend.core.clickhouse_client import ClickHouseClient

logger = structlog.get_logger(__name__)


@cache
def _instrument(name: str, kind: str, unit: str = ""):
    meter = get_meter()
    create = meter.create_histogram if kind == "histogram" else meter.create_counter
    return create(f"app.clickhouse_{name}", unit=unit)


def record_operation(stats: dict) -> None:
    if stats["operation"] not in {"execute", "insert"}:
        return
    try:
        insert = stats["operation"] == "insert"
        _instrument("insert_duration_ms" if insert else "query_duration_ms", "histogram", "ms").record(
            stats["duration_ms"], {"outcome": stats["outcome"]}
        )
        for phase in ("admission_wait_ms", "network_ms", "decode_ms", "server_elapsed_ms"):
            value = stats.get(phase)
            if value is not None:
                _instrument(phase, "histogram", "ms").record(
                    value, {"operation": stats["operation"], "outcome": stats["outcome"]}
                )
        if not insert:
            _instrument("queries_total", "counter").add(1, {"outcome": stats["outcome"]})
        elif stats["outcome"] == "success":
            # Physical acknowledged rows, including identical retry copies.
            _instrument("rows_inserted_total", "counter").add(stats["rows_written"])
        _instrument("bytes_read", "counter", "By").add(stats["bytes_read"])
    except Exception:
        logger.debug("record_operation: _instrument failed (ignored)", exc_info=True)


def record_publication(outcome: str) -> None:
    try:
        _instrument("publications_total", "counter").add(1, {"outcome": outcome})
    except Exception:
        logger.debug("record_publication: _instrument failed (ignored)", exc_info=True)


sys.modules["backend.core.clickhouse_metrics"] = sys.modules[__name__]


def __getattr__(name: str):
    if name == "ClickHouseClient":
        from backend.core.clickhouse_client import ClickHouseClient

        return ClickHouseClient
    if name == "PgManifest":
        from backend.core.clickhouse_manifest import PgManifest

        return PgManifest
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class ClickHouseObserver:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._client: ClickHouseClient | None = None
        self._registered = False
        self._sampled_at = 0.0
        self._values: dict[str, float] = {}

    def start(self, client: ClickHouseClient) -> None:
        with self._lock:
            if self._client is not None:
                client.close()
                return
            self._client = client
            self._sampled_at = 0
            if not self._registered:
                meter = get_meter()
                for key, name, unit in (
                    ("up", "up", ""),
                    ("disk_free", "disk_free", "By"),
                    ("disk_total", "disk_total", "By"),
                    ("lag", "publication_lag_seconds", "s"),
                ):
                    meter.create_observable_gauge(
                        f"app.clickhouse_{name}",
                        callbacks=[partial(self.observe, key)],
                        unit=unit,
                    )
                self._registered = True

    def observe(self, key: str, options=None) -> list[Observation]:
        # All four callbacks run on the exporter, never the serving path.
        with self._lock:
            if self._client is None:
                return []
            if time.monotonic() - self._sampled_at >= 10:
                values: dict[str, float] = {"up": 0}
                try:
                    values.update(self._client.storage_health())
                except Exception as exc:
                    self._log_error("storage", exc)
                try:
                    from backend.core.clickhouse_manifest import PgManifest as _PgManifest

                    manifest_cls = globals().get("PgManifest", _PgManifest)
                    values["lag"] = manifest_cls().publication_lag_seconds()
                except Exception as exc:
                    self._log_error("manifest", exc)
                self._values = values
                self._sampled_at = time.monotonic()
            value = self._values.get(key)
            return [] if value is None else [Observation(value)]

    @staticmethod
    def _log_error(probe: str, exc: Exception) -> None:
        try:
            logger.warning("clickhouse.observe_failed", probe=probe, error_kind=type(exc).__name__)
        except Exception:
            logger.debug("_log_error: logger.warning failed (ignored)", exc_info=True)

    def stop(self) -> None:
        with self._lock:
            client, self._client = self._client, None
            self._values = {}
        if client is not None:
            client.close()


_observer = ClickHouseObserver()


def start_clickhouse_observer() -> None:
    try:
        settings = config.load_clickhouse_config()
        if settings is None:
            return
        from backend.core.clickhouse_client import ClickHouseClient as _ClickHouseClient

        client_cls = globals().get("ClickHouseClient", _ClickHouseClient)
        # Do not contend with serving admission or inherit long query timeouts.
        client = client_cls(
            replace(
                settings,
                pool_max_size=1,
                connect_timeout_s=2,
                query_timeout_s=2,
                pool_timeout_s=2,
            )
        )
        _observer.start(client)
    except Exception as exc:
        stop_clickhouse_observer()
        _observer._log_error("startup", exc)


def stop_clickhouse_observer() -> None:
    try:
        _observer.stop()
    except Exception as exc:
        _observer._log_error("shutdown", exc)
