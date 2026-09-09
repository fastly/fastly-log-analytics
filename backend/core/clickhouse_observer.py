"""Backend-only observable gauges; collected by the existing OTLP reader.

Callbacks share one short-lived sample, including unknown values on failures.
Neither an admin request nor expensive generation readiness is needed.
"""

import threading
import time
from dataclasses import replace
from functools import partial

import structlog
from opentelemetry.metrics import Observation

from backend import config
from backend.core.clickhouse_client import ClickHouseClient
from backend.core.clickhouse_manifest import PgManifest
from backend.core.request_telemetry import get_meter

logger = structlog.get_logger(__name__)


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
                    values["lag"] = PgManifest().publication_lag_seconds()
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
            pass

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
        # Do not contend with serving admission or inherit long query timeouts.
        client = ClickHouseClient(
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
